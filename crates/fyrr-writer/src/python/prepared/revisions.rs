//! Bounded published revisions, pending overlays and row counts for one source.
use super::*;
use arrow_array::StringArray;

pub(super) type RevisionStream = futures::stream::BoxStream<'static, Result<Vec<(String, String)>>>;

pub(super) type RevisionFailure = Arc<Mutex<Option<WriterError>>>;

pub(super) struct RevisionView<'a> {
    pub(super) dataset: &'a Option<Arc<lance::Dataset>>,
    pub(super) pending: Arc<PendingRevisions>,
    pub(super) revisions: RevisionStream,
    pub(super) failure: &'a RevisionFailure,
}

use crate::commit::history::index::{Evidence, Index};

pub(super) struct PendingRevisions {
    paths: Arc<Vec<Path>>,
    source: String,
    summary: tokio::sync::OnceCell<Index>,
}
impl PendingRevisions {
    pub(super) fn new(paths: Arc<Vec<Path>>, source: String) -> Self {
        Self {
            paths,
            source,
            summary: tokio::sync::OnceCell::new(),
        }
    }

    async fn summary(
        &self,
        config: &fyrr_core::config::Config,
        store: &dyn ObjectStore,
    ) -> Result<&Index> {
        self.summary
            .get_or_try_init(|| async {
                let index = Index::command(config)?;
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
                        let old: Option<(u64, Option<String>)> =
                            index.get(Evidence::PendingRevision, &doc.root_id)?;
                        if old.is_some_and(|(sequence, _)| sequence >= envelope.sequence) {
                            continue;
                        }
                        let revision = if doc.intent == DocumentIntent::Delete {
                            None
                        } else {
                            doc.source_revision
                        };
                        // Store unsigned sequences as JSON, never narrowed to a signed SQL integer.
                        index.replace(
                            Evidence::PendingRevision,
                            &doc.root_id,
                            &(envelope.sequence, revision),
                        )?;
                    }
                }
                Ok(index)
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

/// Effective revisions from one published lookup and an exact disposable overlay.
pub(super) async fn lookup_revisions(
    config: &fyrr_core::config::Config,
    dataset: Option<&lance::Dataset>,
    pending: &PendingRevisions,
    roots: &[String],
) -> Result<BTreeMap<String, (String, u64)>> {
    let published = published_revisions(dataset, &pending.source, roots).await?;
    overlay_revisions(config, pending, roots, published).await
}

pub(super) async fn revision_views(
    config: &fyrr_core::config::Config,
    dataset: Option<&lance::Dataset>,
    pending: &PendingRevisions,
    roots: &[String],
) -> Result<(
    BTreeMap<String, (String, u64)>,
    BTreeMap<String, (String, u64)>,
)> {
    let published = published_revisions(dataset, &pending.source, roots).await?;
    let effective = overlay_revisions(config, pending, roots, published.clone()).await?;
    Ok((published, effective))
}

async fn overlay_revisions(
    config: &fyrr_core::config::Config,
    pending: &PendingRevisions,
    roots: &[String],
    mut result: BTreeMap<String, (String, u64)>,
) -> Result<BTreeMap<String, (String, u64)>> {
    if pending.paths.is_empty() {
        return Ok(result);
    }
    let (store, _) = fyrr_reader::storage::namespace_store(config)?;
    let index = pending.summary(config, store.as_ref()).await?;
    for root in roots {
        if let Some((sequence, revision)) =
            index.get::<(u64, Option<String>)>(Evidence::PendingRevision, root)?
        {
            if let Some(revision) = revision {
                result.insert(root.clone(), (revision, sequence));
            } else {
                result.remove(root);
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
        let overlay_pending = pending.clone();
        revision_batches = revision_batches
            .then(move |page| {
                let config = overlay_config.clone();
                let pending = overlay_pending.clone();
                async move {
                    let page = page?;
                    if pending.paths.is_empty() {
                        return Ok(page);
                    }
                    let roots: Vec<_> = page.iter().map(|(root, _)| root.clone()).collect();
                    let published = page
                        .into_iter()
                        .filter(|(_, revision)| !revision.is_empty())
                        .map(|(root, revision)| (root, (revision, 0)))
                        .collect();
                    Ok(overlay_revisions(&config, &pending, &roots, published)
                        .await?
                        .into_iter()
                        .map(|(root, (revision, _))| (root, revision))
                        .collect())
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
                let (published, effective) =
                    revision_views(&config, dataset.as_deref(), &pending, &roots).await?;
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
    use fyrr_core::Contract;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn pending_revision_overflow_keeps_exact_unsigned_overlays_without_rescans() {
        let (config, root) = crate::lease::tests::local_config("revision-overflow");
        let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
        let template: PreparedEnvelope = serde_json::from_str(include_str!(
            "../../../../../fixtures/contracts/prepared-envelope/content.json"
        ))
        .unwrap();
        let mut paths = Vec::new();
        let mut seed = Vec::new();
        // Cross both the old 8,192-key and 1 MiB string limits.
        for page in 0..40 {
            let mut envelope = template.clone();
            envelope.namespace.clone_from(&config.namespace.name);
            envelope.sequence = page + 1;
            envelope.envelope_id = fyrr_core::envelope::envelope_id(
                &envelope.namespace,
                &envelope.source_id,
                envelope.kind,
                envelope.sequence,
                envelope.rejection_generation,
                envelope.cursor.start.as_deref(),
            );
            let EnvelopePayload::Content(payload) = &mut envelope.payload else {
                unreachable!()
            };
            let mut document = payload.documents[0].clone();
            document.records = 0;
            payload.fragments.clear();
            payload.documents = (0..256)
                .map(|row| {
                    let mut doc = document.clone();
                    doc.root_id = format!("root-{:05}", page * 256 + row);
                    doc.source_revision = Some("r".repeat(160));
                    doc
                })
                .collect();
            envelope.validate().unwrap();
            let path = namespace
                .clone()
                .join("envelopes")
                .join(format!("{}.json", envelope.envelope_id));
            seed.push((path.clone(), fyrr_core::write_contract(&envelope)));
            paths.push(path);
        }
        let mut tail = template;
        tail.namespace.clone_from(&config.namespace.name);
        tail.sequence = u64::MAX;
        tail.envelope_id = fyrr_core::envelope::envelope_id(
            &tail.namespace,
            &tail.source_id,
            tail.kind,
            tail.sequence,
            tail.rejection_generation,
            tail.cursor.start.as_deref(),
        );
        let EnvelopePayload::Content(payload) = &mut tail.payload else {
            unreachable!()
        };
        payload.fragments.clear();
        payload.documents[0].root_id = "root-00001".into();
        payload.documents[0].source_revision = Some("newest".into());
        payload.documents[0].records = 0;
        payload.documents[1].root_id = "root-00000".into();
        tail.validate().unwrap();
        let path = namespace
            .clone()
            .join("envelopes")
            .join(format!("{}.json", tail.envelope_id));
        seed.push((path.clone(), fyrr_core::write_contract(&tail)));
        paths.push(path);
        fyrr_reader::storage::test_store::seed_local_fixture(&config, seed).unwrap();
        let pending = PendingRevisions::new(Arc::new(paths.clone()), tail.source_id);
        let roots: Vec<_> = (0..256).map(|row| format!("root-{row:05}")).collect();
        for _ in 0..20 {
            let view = lookup_revisions(&config, None, &pending, &roots)
                .await
                .unwrap();
            assert!(!view.contains_key("root-00000"));
            assert_eq!(view["root-00001"], ("newest".into(), u64::MAX));
            assert_eq!(view["root-00002"].0, "r".repeat(160));
        }
        // The immutable command snapshot is exact even after its source objects
        // are retired; another lookup must not perform another envelope scan.
        for path in paths {
            store.delete(&path).await.unwrap();
        }
        let view = lookup_revisions(&config, None, &pending, &roots)
            .await
            .unwrap();
        assert!(!view.contains_key("root-00000"));
        assert_eq!(view["root-00001"], ("newest".into(), u64::MAX));
        drop(pending);
        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }
}
