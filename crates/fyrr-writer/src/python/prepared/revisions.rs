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
