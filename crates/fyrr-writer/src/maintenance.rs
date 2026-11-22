//! Bounded native work under the namespace lease; manifests remain the only visibility point.
use crate::{Result, WriterError};
pub(crate) mod local_staging;
mod restoration;
pub use restoration::restore;
use restoration::*;

use crate::{apply, commit, lease::LeaseGuard, prepare};
mod restore_fragments;
mod restore_index;
use chrono::Utc;
use futures::TryStreamExt;
use fyrr_core::config::Config;
use fyrr_core::envelope::{EnvelopePayload, RejectionMarker};
use fyrr_core::publication::{CommitLineage, ContentManifest, PolicyArtifact, ServingHead};
use fyrr_reader::storage::{dataset_uri, namespace_store, pinned, read_contract};
use lance::Dataset;
use lance::dataset::cleanup::CleanupPolicy;
use lance::dataset::optimize::{CompactionOptions, plan_compaction};
use lance::dataset::transaction::{Operation, RewriteGroup};
use lance::index::DatasetIndexExt;
use lance_index::optimize::OptimizeOptions;
use object_store::{ObjectStoreExt, path::Path};
use std::collections::{BTreeMap, BTreeSet};

// Bound catalog-wide index opens; merge half the budget to amortize each consolidation.
const INDEX_SEGMENT_LIMIT: usize = 64;

pub(crate) fn lineage(lease: &LeaseGuard, head: &ServingHead) -> CommitLineage {
    CommitLineage {
        holder: lease.holder().holder.clone(),
        epoch: lease.holder().epoch,
        base_version: head.content.as_ref().map_or(0, |p| p.lance_version),
        recovery_version: None,
        transactions: Vec::new(),
    }
}

/// Every native step starts from the head after fresh authorization discovery.
/// Orphan recovery runs once at command start and again after a failed native step.
async fn boundary(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
) -> Result<ServingHead> {
    cached_drain_authorization(config, lease, history, false).await?;
    lease.head().await
}

pub async fn maintain(config: &Config) -> Result<serde_json::Value> {
    crate::lease::LeaseGuard::with(config, |lease| async move {
        let mut history = commit::RestoreHistory::new(config)?;
        let mut result = maintain_held(config, &lease, &mut history).await;
        // Every expired native step yields to authorization, including storage
        // reads during purge/compaction/index planning before cleanup begins.
        if result
            .as_ref()
            .is_err_and(|error| *error == WriterError::MaintenanceStepDeadline)
            && let Err(error) = command_recover(config, &lease, &mut history, false).await
        {
            result = Err(error);
        }
        result
    })
    .await
}

async fn maintain_held(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
) -> Result<serde_json::Value> {
    // Obsolete applied objects must not exhaust history's resident allowance
    // before the cleanup that can remove them becomes reachable.
    let mut envelopes = crate::envelopes::cleanup_held(config, lease).await?;
    // Recover the previous command's private suffix and reconcile configuration
    // once. Quiet native boundaries need only fresh authorization discovery.
    command_start(config, lease, history, false).await?;
    let mut compacted = 0;
    let mut merged = 0;
    let limit = config.writer.maintenance.step_max_duration.as_duration();
    loop {
        let head = boundary(config, lease, history).await?;
        if head.content.is_none() {
            break;
        }
        let dataset = pinned(config, &head).await?;
        let oversized = tokio::time::timeout(limit, async {
            let indexes = dataset.load_indices().await?;
            let mut counts = BTreeMap::new();
            for index in indexes.iter() {
                *counts.entry(index.name.clone()).or_insert(0usize) += 1;
            }
            Ok::<_, WriterError>(
                counts
                    .into_iter()
                    .find_map(|(name, count)| (count > INDEX_SEGMENT_LIMIT).then_some(name)),
            )
        })
        .await
        .map_err(|_| WriterError::MaintenanceStepDeadline)??;
        if let Some(name) = oversized {
            if !merge_index(config, lease, dataset, head, name, INDEX_SEGMENT_LIMIT / 2).await? {
                return Err(WriterError::IndexMergeNoProgress);
            }
            merged += 1;
            continue;
        }
        if tokio::time::timeout(limit, purge_denied(config, lease, &head))
            .await
            .map_err(|_| WriterError::MaintenanceStepDeadline)??
        {
            continue;
        }
        if !compact_one(config, lease, dataset, head).await? {
            break;
        }
        compacted += 1;
    }
    // One index name per interruptible step. Merge existing centroids, never retrain.
    let head = boundary(config, lease, history).await?;
    if head.content.is_some() {
        let dataset = pinned(config, &head).await?;
        let names: BTreeSet<_> = dataset
            .load_indices()
            .await?
            .iter()
            .map(|i| i.name.clone())
            .collect();
        for name in names {
            let head = boundary(config, lease, history).await?;
            let dataset = pinned(config, &head).await?;
            let indexes = dataset.load_indices().await?;
            let same: Vec<_> = indexes.iter().filter(|i| i.name == name).collect();
            // A merged bitmap may still name retired fragment IDs. It is a coverage
            // hint, not evidence that the segment needs another rewrite.
            if same.len() <= 1 {
                continue;
            }
            if merge_index(config, lease, dataset, head, name, usize::MAX).await? {
                merged += 1;
            }
        }
    }
    boundary(config, lease, history).await?;
    let (mut removed, protected, unverified_protected) =
        tokio::time::timeout(limit, cleanup_versions(config, lease, history))
            .await
            .map_err(|_| WriterError::MaintenanceStepDeadline)??;
    let staging = local_staging::plan(config).await?;
    removed += local_staging::remove(config, lease, staging).await?;
    envelopes += crate::envelopes::cleanup_held(config, lease).await?;
    removed += superseded_history(config, lease, history).await?;
    let head = boundary(config, lease, history).await?;
    if head.content.is_some() {
        let dataset = pinned(config, &head).await?;
        let work = lineage(lease, &head);
        commit::publish(
            config,
            lease,
            &dataset,
            head,
            work,
            Vec::new(),
            false,
            Vec::new(),
            true,
        )
        .await?;
    }
    Ok(
        serde_json::json!({"compaction_steps":compacted,"index_merge_steps":merged,"deleted_files":removed,"protected_fragment_files":protected,"unverified_protected_fragment_files":unverified_protected,"deleted_envelopes":envelopes}),
    )
}

async fn merge_index(
    config: &Config,
    lease: &LeaseGuard,
    mut dataset: Dataset,
    head: ServingHead,
    name: String,
    segments: usize,
) -> Result<bool> {
    let step = async {
        let before = dataset.version().version;
        let mut work = lineage(lease, &head);
        let mut options = OptimizeOptions::merge(segments);
        options.index_names = Some(vec![name]);
        options.retrain = false;
        commit::native_step(
            lease,
            dataset.optimize_indices(&options),
            WriterError::IndexMergeFailed,
        )
        .await?;
        commit::record_transaction(&dataset, &mut work, before).await?;
        if dataset.version().version == before {
            return Ok(false);
        }
        commit::publish(
            config,
            lease,
            &dataset,
            head,
            work,
            Vec::new(),
            false,
            Vec::new(),
            false,
        )
        .await?;
        Ok(true)
    };
    tokio::time::timeout(
        config.writer.maintenance.step_max_duration.as_duration(),
        step,
    )
    .await
    .map_err(|_| WriterError::MaintenanceStepDeadline)?
}

/// Remove unreadable chunks; wholly unreadable content becomes a row-local state marker.
/// A later grant requires content refetch; a stale content envelope keeps the marker.
async fn purge_denied(config: &Config, lease: &LeaseGuard, head: &ServingHead) -> Result<bool> {
    let mut dataset = pinned(config, head).await?;
    for source in &config.sources {
        let filter = if source.permissions == fyrr_core::config::PermissionsMode::Fixed {
            if !source.readers.is_empty() {
                continue;
            }
            format!("source_id = {}", fyrr_reader::storage::sql(&source.id))
        } else {
            format!(
                "source_id = {} AND array_length(readers) = 0",
                fyrr_reader::storage::sql(&source.id)
            )
        };
        let rows = dataset
            .scan()
            .project(&["root_id"])
            .map_err(|_| WriterError::ChangedRowsInvalid)?
            .filter(&filter)
            .map_err(|_| WriterError::ChangedRowsInvalid)?
            .limit(Some(1), None)
            .map_err(|_| WriterError::ChangedRowsInvalid)?
            .try_into_batch()
            .await
            .map_err(|_| WriterError::ChangedRowScanFailed)?;
        if rows.num_rows() == 0 {
            continue;
        }
        let root = apply::strings(&rows, "root_id")?.value(0).to_owned();
        let filter = apply::keys_filter(&source.id, std::slice::from_ref(&root));
        let rows = dataset
            .scan()
            .filter(&filter)
            .map_err(|_| WriterError::ChangedRowsInvalid)?
            .try_into_batch()
            .await
            .map_err(|_| WriterError::ChangedRowScanFailed)?;
        let mut readable = Vec::new();
        if source.permissions != fyrr_core::config::PermissionsMode::Fixed {
            for row in 0..rows.num_rows() {
                if !apply::tokens(&rows, row)?.is_empty() {
                    readable.push(rows.slice(row, 1));
                }
            }
        }
        if readable.is_empty() {
            let mut state = apply::state(&rows, 0)?;
            state.content_purged = true;
            readable.push(apply::marker_row(
                config.embedding.dimensions,
                &source.id,
                &root,
                &state,
            )?);
        }
        // A partial section revoke retains admitted chunks and their canonical state.
        let output = apply::write_rows(config, readable).await?;
        let mut work = lineage(lease, head);
        commit::apply_staged(config, &mut dataset, lease, &mut work, vec![filter], output).await?;
        let before = dataset.version().version;
        crate::commit::native_step(
            lease,
            dataset.optimize_indices(&OptimizeOptions::append()),
            WriterError::IndexBuildFailed,
        )
        .await?;
        commit::record_transaction(&dataset, &mut work, before).await?;
        commit::publish(
            config,
            lease,
            &dataset,
            head.clone(),
            work,
            Vec::new(),
            true,
            Vec::new(),
            false,
        )
        .await?;
        return Ok(true);
    }
    Ok(false)
}

/// Keep the publication window and, independently, the grace after retirement.
async fn retained(config: &Config, head: &ServingHead) -> Result<Vec<ContentManifest>> {
    let (store, root) = namespace_store(config)?;
    let mut chain = fyrr_reader::storage::manifest_chain(
        store,
        root,
        head.content.as_ref().map(|p| p.manifest.clone()),
    );
    let mut retired: Option<chrono::DateTime<Utc>> = None;
    let mut keep = Vec::new();
    while let Some(manifest) = chain.try_next().await? {
        if retired.is_some_and(|at| {
            (fyrr_core::clock::now() - at)
                .to_std()
                .is_ok_and(|age| age >= config.writer.version_grace.as_duration())
                && (fyrr_core::clock::now() - manifest.committed_at)
                    .to_std()
                    .is_ok_and(|age| age >= config.writer.purge_window.as_duration())
        }) {
            break;
        }
        retired = Some(manifest.committed_at);
        keep.push(manifest);
    }
    Ok(keep)
}

/// Delete the publication manifests and envelope checkpoints that no retained
/// manifest names. A chain walk ends at the oldest manifest left, and a walk
/// that starts at a manifest older than the window finds its checkpoint gone
/// and reads the manifests instead, so what is deleted is history no restore
/// or receipt reaches. Like the other steps, the pass is bounded by
/// `step_max_duration`, listing included, meets the revocation boundary every
/// 64 deletes and ends when the head publishes; what it leaves, the next
/// `writer maintain` removes.
async fn superseded_history(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
) -> Result<usize> {
    let limit = config.writer.maintenance.step_max_duration.as_duration();
    let start = std::time::Instant::now();
    tokio::time::timeout(limit, async {
        let head = boundary(config, lease, history).await?;
        if head.content.is_none() {
            return Ok(0);
        }
        let retained = retained(config, &head).await?;
        // Only a served manifest with evidence lets a walk stop before the
        // older ones; the publication that ends this command adds it.
        if retained
            .first()
            .is_none_or(|manifest| manifest.envelope_evidence.is_none())
        {
            return Ok(0);
        }
        let named: BTreeSet<_> = retained
            .iter()
            .flat_map(|manifest| {
                [format!("publications/{}.json", manifest.publication_id)]
                    .into_iter()
                    .chain(
                        manifest
                            .envelope_evidence
                            .iter()
                            .map(|evidence| evidence.checkpoint.object.clone()),
                    )
            })
            .collect();
        let served = head.content.as_ref().map(|p| p.publication_id.clone());
        let (store, root, durability) = fyrr_reader::storage::cleanup_namespace_store(config)?;
        let mut deleted = 0;
        for prefix in ["publications", "evidence"] {
            let mut listing = store.list(Some(&root.clone().join(prefix)));
            while let Some(object) = listing.try_next().await? {
                if named.contains(&fyrr_reader::storage::relative_path(
                    &object.location,
                    &root,
                )) {
                    continue;
                }
                if start.elapsed() >= limit {
                    break;
                }
                let current = if deleted % 64 == 0 {
                    boundary(config, lease, history).await?
                } else {
                    lease.head().await?
                };
                // The references above are those of the head this pass began with.
                if current.content.as_ref().map(|p| &p.publication_id) != served.as_ref() {
                    durability.finish().await?;
                    return Ok(deleted);
                }
                match store.delete(&object.location).await {
                    Ok(()) => deleted += 1,
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(_) => return Err(WriterError::StorageUnavailable),
                }
            }
        }
        durability.finish().await?;
        Ok(deleted)
    })
    .await
    .map_err(|_| WriterError::MaintenanceStepDeadline)?
}

async fn cleanup_versions(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
) -> Result<(usize, usize, usize)> {
    let head = boundary(config, lease, history).await?;
    if head.content.is_none() {
        return Ok((0, 0, 0));
    }
    let dataset = pinned(config, &head).await?;
    let versions: BTreeSet<_> = retained(config, &head)
        .await?
        .iter()
        .map(|m| m.lance_version)
        .collect();
    let oldest = *versions
        .first()
        .ok_or(WriterError::PublicationUnavailable)?;
    let policy = CleanupPolicy {
        before_version: Some(oldest),
        delete_unverified: false,
        ..Default::default()
    };
    // Native cleanup doesn't know prepared envelopes. Filter its candidates under
    // the namespace lease; protect every remaining log fragment, including rejects.
    let (store, root, durability) = fyrr_reader::storage::cleanup_namespace_store(config)?;
    let commit::CleanupEvidence {
        applied,
        unresolved,
        envelopes,
        ..
    } = commit::cleanup_evidence(config, &head).await?;
    let mut protected = BTreeSet::new();
    for (_, envelope) in envelopes {
        if applied.contains(&envelope.envelope_id) && !unresolved.contains(&envelope.envelope_id) {
            continue;
        }
        if let EnvelopePayload::Content(payload) = envelope.payload {
            protected.extend(
                payload
                    .fragments
                    .iter()
                    .flat_map(|f| f.files.iter().map(|file| file.path.clone())),
            );
        }
    }
    let plan = dataset
        .cleanup(policy)
        .with_max_candidate_files(100_000)
        .explain()
        .await
        .map_err(|_| WriterError::VersionCleanupFailed)?;
    if !plan.referenced_branches.is_empty() {
        return Err(WriterError::CleanupBranchUnavailable);
    }
    let dataset_root = root.join("dataset.lance");
    let start = std::time::Instant::now();
    let mut removed = 0;
    let mut protected_count = 0;
    let mut unverified_protected = 0;
    let mut candidates = Vec::new();
    for candidate in plan.candidate_files {
        let path = Path::from(candidate.path);
        let relative = path.prefix_match(&dataset_root).map_or_else(
            || path.to_string(),
            |p| {
                p.map(|p| p.as_ref().to_owned())
                    .collect::<Vec<_>>()
                    .join("/")
            },
        );
        if protected.contains(&relative) {
            protected_count += 1;
            unverified_protected += usize::from(candidate.unverified);
            continue;
        }
        candidates.push(relative);
    }
    let deletion_store = store.as_ref();
    for chunk in candidates.chunks(crate::io::CONCURRENCY) {
        let remaining =
            crate::narrow_usize(config.writer.commit.group_max_rows).saturating_sub(removed);
        if remaining == 0
            || start.elapsed() >= config.writer.maintenance.step_max_duration.as_duration()
        {
            break;
        }
        // Keep the 64-delete revocation boundary and the row/time budgets.
        let current = if removed % 64 == 0 {
            boundary(config, lease, history).await?
        } else {
            lease.head().await?
        };
        if current.content.as_ref().map(|p| p.lance_version) != Some(plan.read_version) {
            break;
        }
        let dataset_root = &dataset_root;
        let read_version = plan.read_version;
        let results = crate::io::bounded(
            &chunk[..chunk.len().min(remaining)],
            |relative| async move {
                let current = lease.head().await?;
                if current.content.as_ref().map(|p| p.lance_version) != Some(read_version) {
                    return Ok(false);
                }
                deletion_store
                    .delete(&Path::from(format!("{dataset_root}/{relative}")))
                    .await?;
                Ok(true)
            },
        )
        .await?;
        let changed = results.iter().any(|deleted| !deleted);
        removed += results.into_iter().filter(|deleted| *deleted).count();
        if changed {
            break;
        }
    }
    durability.finish().await?;
    Ok((removed, protected_count, unverified_protected))
}

/// Explicit full-content scan; ordinary commits still verify only changed rows.
pub async fn verify(config: &Config) -> Result<serde_json::Value> {
    let head = fyrr_reader::storage::optional_head(config)
        .await?
        .ok_or(WriterError::NamespaceUnavailable)?;
    let (store, root) = namespace_store(config)?;
    let manifest: ContentManifest = read_contract(
        store.as_ref(),
        &Path::from(format!(
            "{root}/{}",
            head.content
                .as_ref()
                .ok_or(WriterError::PublicationUnavailable)?
                .manifest
        )),
    )
    .await?;
    // The same check as the reader's: the publication matches the namespace's settings.
    let policy: PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    if !manifest.built_with(&policy.settings) {
        return Err(WriterError::IncompatibleIndex);
    }
    let dataset = pinned(config, &head).await?;
    let mut stream = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|_| WriterError::VerificationFailed)?;
    let mut keys = BTreeSet::new();
    let mut states = BTreeMap::new();
    let mut rows = 0u64;
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|_| WriterError::VerificationFailed)?
    {
        for row in 0..batch.num_rows() {
            let source = apply::strings(&batch, "source_id")?.value(row);
            let root = apply::strings(&batch, "root_id")?.value(row);
            let record = apply::strings(&batch, "record_id")?.value(row);
            if !keys.insert((source.to_owned(), root.to_owned(), record.to_owned())) {
                return Err(WriterError::DuplicateChangedRows);
            }
            let vector = batch
                .column_by_name("vector")
                .and_then(|a| a.as_any().downcast_ref::<arrow_array::FixedSizeListArray>())
                .ok_or(WriterError::ChangedRowsInvalid)?
                .value(row);
            let vector = vector
                .as_any()
                .downcast_ref::<arrow_array::Float16Array>()
                .ok_or(WriterError::ChangedRowsInvalid)?;
            if !apply::valid_vector(vector, config.embedding.dimensions) {
                return Err(WriterError::ChangedRowsInvalid);
            }
            let state = apply::state(&batch, row)?;
            let key = (source.to_owned(), root.to_owned());
            if states.get(&key).is_some_and(|old| old != &state) {
                return Err(WriterError::DocumentStateInvalid);
            }
            states.insert(key, state.clone());
            if source.starts_with("__fyrr_state_") {
                if !apply::tokens(&batch, row)?.is_empty()
                    || !apply::strings(&batch, "searchable_text")?
                        .value(row)
                        .is_empty()
                {
                    return Err(WriterError::TombstoneInvalid);
                }
            } else if state.deleted
                || state.content_purged
                || state.revision.as_deref()
                    != Some(apply::strings(&batch, "source_revision")?.value(row))
                || apply::strings(&batch, "searchable_text")?
                    .value(row)
                    .is_empty()
            {
                return Err(WriterError::ChangedRowsInvalid);
            }
            rows += 1;
        }
    }
    // Use the production reader's native index-kind and coverage verification.
    fyrr_reader::search::verify_indices(&dataset, &manifest, config)
        .await
        .map_err(WriterError::from)?;
    Ok(serde_json::json!({"verified":true,"lance_version":dataset.version().version,"rows":rows}))
}

#[cfg(test)]
type RestoreTestHook = std::sync::Arc<dyn Fn() + Send + Sync>;
#[cfg(test)]
static RESTORE_TEST_GROUP: std::sync::Mutex<Option<RestoreTestHook>> = std::sync::Mutex::new(None);
#[cfg(test)]
static RESTORE_TEST_PUBLICATION: std::sync::Mutex<Option<RestoreTestHook>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
static RESTORE_TEST_DRAIN: std::sync::Mutex<Option<RestoreTestHook>> = std::sync::Mutex::new(None);
#[cfg(test)]
type RestoreScanTestHook = std::sync::Arc<dyn Fn(&mut RestoreCadence) + Send + Sync>;
#[cfg(test)]
static RESTORE_TEST_SCAN: std::sync::Mutex<Option<RestoreScanTestHook>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
fn restore_test_scan(cadence: &mut RestoreCadence) {
    let hook = RESTORE_TEST_SCAN.lock().unwrap().clone();
    if let Some(hook) = hook {
        hook(cadence);
    }
}
#[cfg(test)]
pub(crate) fn restore_test_publication() {
    let hook = RESTORE_TEST_PUBLICATION.lock().unwrap().clone();
    if let Some(hook) = hook {
        hook();
    }
}
#[cfg(test)]
pub(crate) fn restore_test_drain() {
    let hook = RESTORE_TEST_DRAIN.lock().unwrap().clone();
    if let Some(hook) = hook {
        hook();
    }
}
#[cfg(test)]
fn restore_test_group() {
    let hook = RESTORE_TEST_GROUP
        .lock()
        .expect("restore hook lock")
        .clone();
    if let Some(hook) = hook {
        hook();
    }
}

/// A bounded, conservative warning, with no source/document identifiers or counts.
pub(crate) async fn purge_warnings(
    config: &Config,
    head: &ServingHead,
) -> Result<Vec<&'static str>> {
    let check = async {
        let Some(pointer) = &head.content else {
            return Ok(false);
        };
        let (store, root) = namespace_store(config)?;
        let current: ContentManifest = read_contract(
            store.as_ref(),
            &Path::from(format!("{root}/{}", pointer.manifest)),
        )
        .await?;
        let threshold = config
            .writer
            .purge_window
            .as_duration()
            .saturating_sub(config.writer.version_grace.as_duration());
        let old = |instant: chrono::DateTime<Utc>| {
            (fyrr_core::clock::now() - instant)
                .to_std()
                .is_ok_and(|age| age >= threshold)
        };
        if old(current
            .maintenance_completed_at
            .unwrap_or(current.committed_at))
        {
            return Ok(true);
        }
        let dataset = pinned(config, head).await?;
        let rows = dataset
            .scan()
            .project(&["metadata_json", "source_revision"])?
            .filter("array_length(readers) = 0 AND source_id NOT LIKE '__fyrr_state_%'")?
            .limit(Some(64), None)?
            .try_into_batch()
            .await?;
        if rows.num_rows() == 64 {
            return Ok(true);
        }
        let revoked_at: BTreeSet<_> = (0..rows.num_rows())
            .map(|row| apply::state(&rows, row).map(|s| s.publication_sequence))
            .collect::<Result<_, _>>()?;
        let mut probes = 0;
        let files: BTreeSet<_> = current.files.iter().map(|f| f.path.clone()).collect();
        let mut previous = current.previous_publication_id;
        for _ in 0..64 {
            let Some(id) = previous else {
                return Ok(false);
            };
            // Maintenance deleted the history outside the purge window.
            let Some(manifest) = fyrr_reader::storage::read_older_manifest(
                store.as_ref(),
                &root,
                &format!("publications/{id}.json"),
            )
            .await?
            else {
                return Ok(false);
            };
            if old(manifest.committed_at) {
                if revoked_at.contains(&manifest.sequence) {
                    return Ok(true);
                }
                // Old payloads retained for restore also need to be removed before
                // the deadline. Check existence, never count their rows or files.
                for file in manifest.files.iter().filter(|f| !files.contains(&f.path)) {
                    if probes == 64 {
                        return Ok(true);
                    }
                    probes += 1;
                    match store
                        .head(&Path::from(format!("{root}/dataset.lance/{}", file.path)))
                        .await
                    {
                        Ok(_) => return Ok(true),
                        Err(object_store::Error::NotFound { .. }) => {}
                        Err(_) => return Err(WriterError::StorageUnavailable),
                    }
                }
            }
            previous = manifest.previous_publication_id;
        }
        // More history than the bounded probe can certify is conservatively warned.
        Ok::<_, WriterError>(previous.is_some())
    };
    let risk = match tokio::time::timeout(
        config.writer.maintenance.step_max_duration.as_duration(),
        check,
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => true,
    };
    Ok(if risk {
        vec!["physical_purge_deadline_at_risk"]
    } else {
        Vec::new()
    })
}

#[cfg(test)]
mod restore_tests;

async fn compact_one(
    config: &Config,
    lease: &LeaseGuard,
    mut dataset: Dataset,
    head: ServingHead,
) -> Result<bool> {
    let limit = config.writer.maintenance.step_max_duration.as_duration();
    // Limit the actual source set, rather than asking the native planner to
    // discard an oversized bin. Deleted fragments take priority regardless of size.
    let fragments = &dataset.manifest().fragments;
    let selected: BTreeSet<_> = if let Some(fragment) =
        fragments.iter().find(|f| f.deletion_file.is_some())
    {
        [fragment.id].into_iter().collect()
    } else if let Some(pair) = fragments.windows(2).find(|pair| {
        pair.iter()
            .all(|f| (f.physical_rows.unwrap_or(0) as u64) < config.writer.commit.group_max_rows)
    }) {
        pair.iter().map(|f| f.id).collect()
    } else {
        return Ok(false);
    };
    let options = CompactionOptions {
        max_source_fragments: Some(2),
        materialize_deletions: true,
        defer_index_remap: true,
        materialize_deletions_threshold: 0.0,
        excluded_fragment_ids: fragments
            .iter()
            .filter(|f| !selected.contains(&f.id))
            .map(|f| crate::narrow_u32(f.id))
            .collect(),
        ..Default::default()
    };
    let plan = plan_compaction(&dataset, &options)
        .await
        .map_err(|_| WriterError::CompactionPlanFailed)?;
    let Some(task) = plan.compaction_tasks().next() else {
        return Ok(false);
    };
    let mut work = lineage(lease, &head);
    let step = async {
        let rewritten = task
            .execute(&dataset)
            .await
            .map_err(|_| WriterError::CompactionFailed)?;
        let before = dataset.version().version;
        // A rewrite leaves changed fragments unindexed. Append deltas below,
        // without the remapper's extra reservation commit or stale rebase.
        dataset = commit::native_step(
            lease,
            Dataset::commit(
                dataset_uri(config).as_str(),
                Operation::Rewrite {
                    groups: vec![RewriteGroup {
                        old_fragments: rewritten.original_fragments,
                        new_fragments: rewritten.new_fragments,
                    }],
                    rewritten_indices: Vec::new(),
                    frag_reuse_index: None,
                },
                Some(before),
                Some(prepare::store_params(config)?),
                None,
                dataset.session(),
                false,
            ),
            WriterError::CompactionCommitFailed,
        )
        .await?;
        commit::record_transaction(&dataset, &mut work, before).await?;
        let before = dataset.version().version;
        crate::commit::native_step(
            lease,
            dataset.optimize_indices(&OptimizeOptions::append()),
            WriterError::IndexBuildFailed,
        )
        .await?;
        commit::record_transaction(&dataset, &mut work, before).await?;
        commit::publish(
            config,
            lease,
            &dataset,
            head,
            work,
            Vec::new(),
            false,
            Vec::new(),
            false,
        )
        .await
    };
    tokio::time::timeout(limit, step)
        .await
        .map_err(|_| WriterError::MaintenanceStepDeadline)??;
    Ok(true)
}
