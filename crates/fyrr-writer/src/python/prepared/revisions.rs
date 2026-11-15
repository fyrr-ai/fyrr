//! Bounded published revisions, pending overlays and row counts for one source.
use super::*;
use arrow_array::StringArray;

pub(super) type RevisionStream = futures::stream::BoxStream<'static, Result<Vec<(String, String)>>>;

pub(super) type RevisionFailure = Arc<Mutex<Option<WriterError>>>;

pub(super) struct RevisionView<'a> {
    pub(super) dataset: &'a Option<Arc<lance::Dataset>>,
    pub(super) pending: Arc<PendingRevisions>,
    pub(super) revisions: Arc<Mutex<RevisionStream>>,
    pub(super) failure: &'a RevisionFailure,
}

// Per-preparation metadata only. Bound string payload and map cardinality;
// these limits do not claim a bound on the whole worker's RSS.
const REVISION_KEYS: usize = 8192;
const REVISION_STRINGS: usize = 1024 * 1024;
type PendingSummary = BTreeMap<String, (u64, Option<String>)>;

pub(super) struct PendingRevisions {
    paths: Arc<Vec<Path>>,
    source: String,
    summary: tokio::sync::OnceCell<Option<PendingSummary>>,
}
impl PendingRevisions {
    pub(super) fn new(paths: Arc<Vec<Path>>, source: String) -> Self {
        Self {
            paths,
            source,
            summary: tokio::sync::OnceCell::new(),
        }
    }
    async fn summary(&self, store: &dyn ObjectStore) -> Result<&Option<PendingSummary>> {
        self.summary
            .get_or_try_init(|| async {
                let mut result = BTreeMap::new();
                let mut strings = 0;
                for path in self.paths.iter() {
                    let envelope: PreparedEnvelope =
                        fyrr_reader::storage::read_contract(store, path).await?;
                    if envelope.source_id != self.source {
                        continue;
                    }
                    let EnvelopePayload::Content(payload) = envelope.payload else {
                        continue;
                    };
                    for doc in payload.documents {
                        let old: Option<&(u64, Option<String>)> = result.get(&doc.root_id);
                        if old.is_some_and(|(sequence, _)| *sequence >= envelope.sequence) {
                            continue;
                        }
                        let revision = if doc.intent == DocumentIntent::Delete {
                            None
                        } else {
                            doc.source_revision
                        };
                        let previous = old.map_or(0, |(_, value)| {
                            doc.root_id.len() + value.as_ref().map_or(0, String::len)
                        });
                        let bytes = strings - previous
                            + doc.root_id.len()
                            + revision.as_ref().map_or(0, String::len);
                        if bytes > REVISION_STRINGS
                            || (old.is_none() && result.len() == REVISION_KEYS)
                        {
                            // Keep the existing bounded streaming path for larger views.
                            return Ok(None);
                        }
                        result.insert(doc.root_id, (envelope.sequence, revision));
                        strings = bytes;
                    }
                }
                Ok(Some(result))
            })
            .await
    }
}

enum RootSelection<'a> {
    PublishedFirst,
    Keys(&'a [String]),
}

fn root_scan(
    dataset: &lance::Dataset,
    source: &str,
    columns: &[&str],
    selection: RootSelection<'_>,
) -> Result<lance::dataset::scanner::Scanner> {
    let mut scan = dataset.scan();
    scan.project(columns)
        .map_err(|_| WriterError::PublicationInvalid)?;
    let predicate = match selection {
        RootSelection::PublishedFirst => format!(
            concat!(
                "source_id = {} AND (record_order = 0 OR ",
                "(record_order IS NULL AND record_id = concat(root_id, ':0')))"
            ),
            fyrr_reader::storage::sql(source)
        ),
        RootSelection::Keys(roots) => fyrr_reader::storage::roots_filter(source, roots),
    };
    scan.filter(&predicate)
        .map_err(|_| WriterError::PublicationInvalid)?;
    Ok(scan)
}

fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref::<StringArray>())
        .ok_or(WriterError::PublicationInvalid)
}

/// Read only published revisions for the requested source and keys.
pub(super) async fn published_revisions(
    dataset: Option<&lance::Dataset>,
    source: &str,
    roots: &[String],
) -> Result<std::collections::BTreeMap<String, (String, u64)>> {
    let mut result = std::collections::BTreeMap::new();
    if let Some(dataset) = dataset
        && !roots.is_empty()
    {
        let scan = root_scan(
            dataset,
            source,
            &["root_id", "source_revision"],
            RootSelection::Keys(roots),
        )?;
        let batch = scan.try_into_batch().await?;
        let root = strings(&batch, "root_id")?;
        let rev = strings(&batch, "source_revision")?;
        for i in 0..batch.num_rows() {
            if !rev.value(i).is_empty() {
                result.insert(root.value(i).into(), (rev.value(i).into(), 0));
            }
        }
    }
    Ok(result)
}

/// Effective revisions, using one bounded immutable summary per preparation.
pub(super) async fn lookup_revisions(
    config: &fyrr_core::config::Config,
    dataset: Option<&lance::Dataset>,
    pending: &PendingRevisions,
    roots: &[String],
) -> Result<BTreeMap<String, (String, u64)>> {
    let source = pending.source.as_str();
    let mut result = published_revisions(dataset, source, roots).await?;
    let (store, _) = fyrr_reader::storage::namespace_store(config)?;
    if let Some(summary) = pending.summary(store.as_ref()).await? {
        for root in roots {
            if let Some((sequence, revision)) = summary.get(root) {
                if let Some(revision) = revision {
                    result.insert(root.clone(), (revision.clone(), *sequence));
                } else {
                    result.remove(root);
                }
            }
        }
        return Ok(result);
    }
    // Overflow fallback reads one envelope at a time and retains only page keys.
    let mut newest = std::collections::BTreeMap::<String, u64>::new();
    for path in pending.paths.iter() {
        let envelope: PreparedEnvelope =
            fyrr_reader::storage::read_contract(store.as_ref(), path).await?;
        if envelope.source_id != source {
            continue;
        }
        let EnvelopePayload::Content(payload) = envelope.payload else {
            continue;
        };
        for document in payload.documents {
            if !roots.contains(&document.root_id)
                || newest
                    .get(&document.root_id)
                    .is_some_and(|s| *s >= envelope.sequence)
            {
                continue;
            }
            newest.insert(document.root_id.clone(), envelope.sequence);
            if document.intent == DocumentIntent::Delete {
                result.remove(&document.root_id);
            } else if let Some(rev) = document.source_revision {
                result.insert(document.root_id, (rev, envelope.sequence));
            }
        }
    }
    Ok(result)
}

/// Counts only requested logical keys, projecting no document text or vectors.
pub(super) async fn source_counts(
    dataset: Option<&lance::Dataset>,
    source: &str,
    roots: &[String],
) -> Result<std::collections::BTreeMap<String, u64>> {
    let mut counts = std::collections::BTreeMap::new();
    let Some(dataset) = dataset else {
        return Ok(counts);
    };
    for page in roots.chunks(256) {
        let batch = root_scan(dataset, source, &["root_id"], RootSelection::Keys(page))?
            .try_into_batch()
            .await?;
        let roots = strings(&batch, "root_id")?;
        for root in roots.iter() {
            *counts
                .entry(root.ok_or(WriterError::PublicationInvalid)?.to_owned())
                .or_default() += 1;
        }
    }
    Ok(counts)
}

impl Preparation<'_> {
    pub(super) fn revision_stream(
        &self,
        dataset: &Option<Arc<lance::Dataset>>,
        pending: &Arc<PendingRevisions>,
        store: &Arc<dyn ObjectStore>,
    ) -> Result<RevisionStream> {
        let revision_paths = &pending.paths;
        let mut revision_batches: RevisionStream = if let Some(dataset) = dataset.clone() {
            let mut scan = root_scan(
                &dataset,
                &self.source.id,
                &["root_id", "source_revision"],
                RootSelection::PublishedFirst,
            )?;
            scan.batch_size(256);
            self.rt
                .block_on(scan.try_into_stream())?
                .map(|batch| {
                    let batch = batch?;
                    let roots = strings(&batch, "root_id")?;
                    let rev = strings(&batch, "source_revision")?;
                    Ok((0..batch.num_rows())
                        .map(|i| (roots.value(i).into(), rev.value(i).into()))
                        .collect())
                })
                .boxed()
        } else {
            futures::stream::empty().boxed()
        };
        let config_pending = self.config.clone();
        let source_pending = self.source.id.clone();
        let dataset_pending = dataset.clone();
        let overlay_config = self.config.clone();
        let overlay_dataset = dataset.clone();
        let overlay_pending = pending.clone();
        revision_batches = revision_batches
            .then(move |page| {
                let config = overlay_config.clone();
                let dataset = overlay_dataset.clone();
                let pending = overlay_pending.clone();
                async move {
                    let page = page?;
                    if pending.paths.is_empty() {
                        return Ok(page);
                    }
                    let roots: Vec<_> = page.into_iter().map(|(root, _)| root).collect();
                    Ok(
                        lookup_revisions(&config, dataset.as_deref(), &pending, &roots)
                            .await?
                            .into_iter()
                            .map(|(root, (revision, _))| (root, revision))
                            .collect(),
                    )
                }
            })
            .boxed();
        let effective_pending = pending.clone();
        let store_pending = store.clone();
        let pending_stream = futures::stream::iter(
            revision_paths.iter().cloned().collect::<Vec<_>>(),
        )
        .then(move |path| {
            let config = config_pending.clone();
            let source = source_pending.clone();
            let dataset = dataset_pending.clone();
            let pending = effective_pending.clone();
            let store = store_pending.clone();
            async move {
                let envelope: PreparedEnvelope =
                    fyrr_reader::storage::read_contract(store.as_ref(), &path).await?;
                if envelope.source_id != source {
                    return Ok(Vec::new());
                }
                let EnvelopePayload::Content(payload) = envelope.payload else {
                    return Ok(Vec::new());
                };
                let roots: Vec<_> = payload
                    .documents
                    .iter()
                    .map(|d| d.root_id.clone())
                    .collect();
                let published = published_revisions(dataset.as_deref(), &source, &roots).await?;
                let effective =
                    lookup_revisions(&config, dataset.as_deref(), &pending, &roots).await?;
                Ok(payload
                    .documents
                    .into_iter()
                    .filter(|d| {
                        d.intent == DocumentIntent::Replace
                            && !published.contains_key(&d.root_id)
                            && effective
                                .get(&d.root_id)
                                .is_some_and(|(_, sequence)| *sequence == envelope.sequence)
                    })
                    .filter_map(|d| d.source_revision.map(|rev| (d.root_id, rev)))
                    .collect())
            }
        });
        Ok(revision_batches.chain(pending_stream).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;

    #[test]
    #[ignore = "real S3 pending revision view; scripts/ci"]
    fn pending_revision_view_reads_each_envelope_once_on_real_s3() {
        let path = std::env::var_os("FYRR_TEST_REVISION_CONFIG").unwrap();
        let config = fyrr_core::config::load(std::path::Path::new(&path))
            .unwrap()
            .config;
        let source = config.sources.iter().find(|s| s.id == "allowed").unwrap();
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
        crate::sync_lease::CursorLease::with(
            &rt,
            &config,
            &source.id,
            SyncKind::Content,
            |lease| {
                let preparation = Preparation {
                    config: &config,
                    source,
                    kind: SyncKind::Content,
                    rt: &rt,
                    lease,
                };
                let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
                let head = rt
                    .block_on(fyrr_reader::storage::optional_head(&config))
                    .unwrap()
                    .unwrap();
                let dataset = Some(Arc::new(
                    rt.block_on(fyrr_reader::storage::pinned(&config, &head))
                        .unwrap(),
                ));
                let objects = rt
                    .block_on(
                        store
                            .list(Some(&namespace.clone().join("envelopes")))
                            .try_collect::<Vec<_>>(),
                    )
                    .unwrap();
                let mut seed = None;
                for object in objects {
                    let e: PreparedEnvelope = rt
                        .block_on(fyrr_reader::storage::read_contract(
                            store.as_ref(),
                            &object.location,
                        ))
                        .unwrap();
                    if e.source_id == source.id && matches!(e.payload, EnvelopePayload::Content(_))
                    {
                        seed = Some(e);
                        break;
                    }
                }
                let seed = seed.unwrap();
                let document = |root: &str, revision: Option<&str>| EnvelopeDocument {
                    root_id: root.into(),
                    intent: if revision.is_some() {
                        DocumentIntent::Replace
                    } else {
                        DocumentIntent::Delete
                    },
                    source_revision: revision.map(str::to_owned),
                    revision_order: None,
                    expected_prior_revision: revision.map(str::to_owned),
                    permission_revision: None,
                    records: 0,
                };
                let batches = [
                    vec![
                        document("r0", Some("pending-a")),
                        document("new-one", Some("first")),
                    ],
                    vec![
                        document("r0", None),
                        document("new-one", Some("second")),
                        document("r1", Some("r1")),
                    ],
                    vec![
                        document("r0", Some("restored")),
                        document("new-one", None),
                        document("r2", None),
                        document("new-two", Some("fresh")),
                    ],
                    vec![document("r0", Some("foreign"))],
                ];
                let mut paths = Vec::new();
                for (i, documents) in batches.into_iter().enumerate() {
                    let mut e = seed.clone();
                    e.sequence = 100 + i as u64;
                    if i == 3 {
                        e.source_id = "other".into();
                    }
                    e.envelope_id = fyrr_core::envelope::envelope_id(
                        &e.namespace,
                        &e.source_id,
                        e.kind,
                        e.sequence,
                        e.rejection_generation,
                        e.cursor.start.as_deref(),
                    );
                    let EnvelopePayload::Content(payload) = &mut e.payload else {
                        unreachable!()
                    };
                    payload.documents = documents;
                    payload.fragments.clear();
                    payload.quarantined.clear();
                    let bytes = fyrr_core::write_contract(&e);
                    fyrr_core::parse_contract::<PreparedEnvelope>(&bytes).unwrap();
                    let path = namespace
                        .clone()
                        .join("envelopes")
                        .join(format!("{}.json", e.envelope_id));
                    rt.block_on(store.put(&path, bytes.into())).unwrap();
                    paths.push(path);
                }
                let paths = Arc::new(PendingRevisions::new(Arc::new(paths), source.id.clone()));
                let mut stream = preparation
                    .revision_stream(&dataset, &paths, &store)
                    .unwrap();
                let marker = std::path::PathBuf::from(
                    std::env::var_os("FYRR_TEST_REVISION_MARKER").unwrap(),
                );
                std::fs::write(&marker, "count").unwrap();
                let actual = rt.block_on(async {
                    let mut actual = BTreeMap::new();
                    while let Some(page) = stream.next().await {
                        actual.extend(page.unwrap());
                    }
                    actual
                });
                let mut expected: BTreeMap<String, String> =
                    (0..8).map(|i| (format!("r{i}"), "r1".into())).collect();
                expected.extend([
                    ("restricted".into(), "r1".into()),
                    ("denied".into(), "r1".into()),
                    ("split".into(), "r1".into()),
                    ("new-two".into(), "fresh".into()),
                ]);
                expected.insert("r0".into(), "restored".into());
                expected.remove("r2");
                assert_eq!(
                    actual, expected,
                    "pending stream preserves deletes, replacements, presence and source isolation"
                );
                let roots = ["r0", "r1", "r2", "new-one", "new-two"].map(str::to_owned);
                for _ in 0..3 {
                    let found = rt
                        .block_on(lookup_revisions(
                            &config,
                            dataset.as_deref(),
                            &paths,
                            &roots,
                        ))
                        .unwrap();
                    assert_eq!(
                        found,
                        [
                            ("r0".into(), ("restored".into(), 102)),
                            ("r1".into(), ("r1".into(), 101)),
                            ("new-two".into(), ("fresh".into(), 102))
                        ]
                        .into_iter()
                        .collect()
                    );
                }
                std::fs::remove_file(marker).unwrap();
                assert!(paths.summary.get().unwrap().is_some());
                // Real storage overflow must preserve streaming results and stop
                // retaining the partial index, including on repeated lookups.
                for (label, documents) in [
                    (
                        "keys",
                        (0..=REVISION_KEYS)
                            .map(|i| document(&format!("overflow-{i}"), Some("bounded")))
                            .collect(),
                    ),
                    (
                        "strings",
                        (0..=(REVISION_STRINGS / 256))
                            .map(|i| document(&format!("overflow-{i}"), Some(&"x".repeat(256))))
                            .collect(),
                    ),
                ] {
                    let mut e = seed.clone();
                    e.sequence = 200;
                    e.envelope_id = fyrr_core::envelope::envelope_id(
                        &e.namespace,
                        &e.source_id,
                        e.kind,
                        e.sequence,
                        e.rejection_generation,
                        e.cursor.start.as_deref(),
                    );
                    let EnvelopePayload::Content(payload) = &mut e.payload else {
                        unreachable!()
                    };
                    payload.documents = documents;
                    payload.fragments.clear();
                    payload.quarantined.clear();
                    let path = namespace
                        .clone()
                        .join("revision-overflow")
                        .join(format!("{label}.json"));
                    let bytes = fyrr_core::write_contract(&e);
                    fyrr_core::parse_contract::<PreparedEnvelope>(&bytes).unwrap();
                    rt.block_on(store.put(&path, bytes.into())).unwrap();
                    let pending = PendingRevisions::new(Arc::new(vec![path]), source.id.clone());
                    let roots = ["overflow-0".to_owned(), "r0".to_owned()];
                    let first = rt
                        .block_on(lookup_revisions(
                            &config,
                            dataset.as_deref(),
                            &pending,
                            &roots,
                        ))
                        .unwrap();
                    assert_eq!(first.get("r0"), Some(&("r1".to_owned(), 0)));
                    assert!(first.contains_key("overflow-0"));
                    assert!(
                        pending.summary.get().unwrap().is_none(),
                        "{label} overflow must discard the index"
                    );
                    let second = rt
                        .block_on(lookup_revisions(
                            &config,
                            dataset.as_deref(),
                            &pending,
                            &roots,
                        ))
                        .unwrap();
                    assert_eq!(first, second);
                }
                Ok::<_, WriterError>(())
            },
        )
        .unwrap();
    }
}
