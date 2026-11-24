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
use fyrr_core::Contract;
use fyrr_core::attributes::stale_analyzers;
use fyrr_core::config::Config;
use fyrr_core::envelope::{EnvelopePayload, RejectionMarker};
use fyrr_core::publication::{
    CommitLineage, ContentManifest, HISTORY_CUT_OBJECT, HistoryCut, IndexIdentity, PolicyArtifact,
    ServingHead,
};
use fyrr_reader::storage::{dataset_uri, namespace_store, pinned, read_contract};
use lance::Dataset;
use lance::dataset::cleanup::CleanupPolicy;
use lance::dataset::optimize::{CompactionOptions, plan_compaction};
use lance::dataset::transaction::{Operation, RewriteGroup};
use lance::index::DatasetIndexExt;
use lance::index::optimize::{commit_optimize_indices, plan_optimize_indices};
use lance_index::optimize::OptimizeOptions;
use object_store::{ObjectStoreExt, PutMode, path::Path};
use std::collections::{BTreeMap, BTreeSet};

// Bound catalog-wide index opens, while the indexes stay below `merge_tier_rows` per segment.
const INDEX_SEGMENT_LIMIT: usize = 64;
// Merge throughput measured on #317: 3,000,000 rows in 2,147 s on `writer maintain`, cores 0-3,
// `writer.threads = 2`. Re-measure with `scripts/bench-scale --maintain` when the merge changes.
const MERGE_ROWS_PER_SECOND: f64 = 1_400.0;

/// The rows one merge step may rewrite: half of `step_max_duration` at the measured throughput,
/// 210,000 rows by default. A step also opens segments and writes the version.
fn merge_tier_rows(config: &Config) -> u64 {
    let budget = config
        .writer
        .maintenance
        .step_max_duration
        .as_duration()
        .as_secs_f64()
        / 2.0;
    ((budget * MERGE_ROWS_PER_SECOND) as u64).max(1)
}

/// Whether `merge_tier` has a run for the segments of one index: two or more neighbors, in
/// fragment order, whose rows add up to at most `target`. It packs next-fit, as Lance does.
fn merge_run_pending(segments: &[&IndexIdentity], target: u64) -> bool {
    let mut ordered: Vec<_> = segments
        .iter()
        .filter_map(|segment| Some((segment.first_fragment?, segment.rows?)))
        .collect();
    ordered.sort_unstable();
    let mut bin: Option<(usize, u64)> = None;
    for (_, rows) in ordered {
        match &mut bin {
            Some((count, used)) if used.saturating_add(rows) <= target => {
                *count += 1;
                *used += rows;
                if *count > 1 {
                    return true;
                }
            }
            _ => bin = Some((1, rows)),
        }
    }
    false
}

/// Whether `maintain` has work, judged from the served manifest alone, so that
/// an idle `writer serve` tick asks storage for nothing more. An index with
/// more segments than the limit is due at once if a merge has a run to do, so that an index of
/// full segments never makes every tick run `maintain`. Everything else waits for
/// `writer.version_grace` since the last maintenance, so that steady upserts,
/// which leave a deletion in a fragment each, or a short `max_envelope_age` do
/// not run it on every publication: then a fragment with deletions is due, and
/// so is the span reaching the window after which a version is purgeable or an
/// applied envelope is old. A namespace never maintained is due. `maintain`
/// ends by recording its completion, which restarts both spans. An attribute
/// index that the settings ask for and the manifest does not hold is due at once,
/// and so is a full-text index the manifest records with another analyzer.
pub(crate) fn due(config: &Config, manifest: &ContentManifest) -> bool {
    let writer = &config.writer;
    let grace = writer.version_grace.as_duration();
    let window = writer
        .purge_window
        .as_duration()
        .saturating_sub(grace)
        .min(writer.max_envelope_age.as_duration());
    let mut segments: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for index in &manifest.indexes {
        segments.entry(&index.name).or_default().push(index);
    }
    let target = merge_tier_rows(config);
    if segments
        .values()
        .any(|same| same.len() > INDEX_SEGMENT_LIMIT && merge_run_pending(same, target))
        || fyrr_reader::search::attribute_indexes(
            &config.namespace.attributes,
            &config.namespace.attribute_indexes,
        )
        .any(|(_, name, _)| !segments.contains_key(&name))
        || stale_analyzers(
            &config.namespace.attribute_indexes,
            &manifest.attribute_indexes,
        )
        .next()
        .is_some()
    {
        return true;
    }
    let deletions = manifest
        .files
        .iter()
        .any(|file| file.path.starts_with("_deletions/"));
    manifest.maintenance_completed_at.is_none_or(|at| {
        (fyrr_core::clock::now() - at)
            .to_std()
            .is_ok_and(|age| age >= grace && (deletions || age >= window))
    })
}

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

/// `rebalance` also re-partitions the vector index. Splitting a partition
/// trains on up to 262,144 vectors in memory and cannot be interrupted, so
/// only the operator command asks for it; `writer serve` merges segments only.
pub async fn maintain(config: &Config, rebalance: bool) -> Result<serde_json::Value> {
    crate::lease::LeaseGuard::with(config, |lease| async move {
        let mut history = commit::RestoreHistory::new(config)?;
        let mut result = maintain_held(config, &lease, &mut history, rebalance).await;
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
    rebalance: bool,
) -> Result<serde_json::Value> {
    // Obsolete applied objects must not exhaust history's resident allowance
    // before the cleanup that can remove them becomes reachable.
    let mut envelopes = crate::envelopes::cleanup_held(config, lease).await?;
    // Recover the previous command's private suffix and reconcile configuration
    // once. Quiet native boundaries need only fresh authorization discovery.
    command_start(config, lease, history, false).await?;
    let mut compacted = 0;
    let mut merged = 0;
    let mut settled = BTreeSet::new();
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
            Ok::<_, WriterError>(counts.into_iter().find_map(|(name, count)| {
                (count > INDEX_SEGMENT_LIMIT && !settled.contains(&name)).then_some(name)
            }))
        })
        .await
        .map_err(|_| WriterError::MaintenanceStepDeadline)??;
        if let Some(name) = oversized {
            // A namespace of more than 64 segments of `merge_tier_rows` rows has no merge left.
            if !merge_index(config, lease, dataset, head, name.clone(), rebalance).await? {
                settled.insert(name);
                continue;
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
    // One index name per interruptible step. Merge the segments, never retrain;
    // with `rebalance` Lance also splits and joins partitions toward the index's
    // target size.
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
            // Each step merges the newest segments only, so the next one finds
            // the step's output among them.
            loop {
                let head = boundary(config, lease, history).await?;
                let dataset = pinned(config, &head).await?;
                let segments = dataset
                    .load_indices()
                    .await?
                    .iter()
                    .filter(|i| i.name == name)
                    .count();
                // A merged bitmap may still name retired fragment IDs. It is a coverage
                // hint, not evidence that the segment needs another rewrite.
                if segments <= 1
                    || !merge_index(config, lease, dataset, head, name.clone(), rebalance).await?
                {
                    break;
                }
                merged += 1;
            }
        }
    }
    // One retained set for both cleanups: a manifest goes only with the
    // version files that this set protects.
    let head = boundary(config, lease, history).await?;
    let kept = if head.content.is_some() {
        tokio::time::timeout(limit, retained(config, &head))
            .await
            .map_err(|_| WriterError::MaintenanceStepDeadline)??
    } else {
        Vec::new()
    };
    let (mut removed, protected, unverified_protected, cleaned) = tokio::time::timeout(
        limit,
        cleanup_versions(config, lease, history, &head, &kept),
    )
    .await
    .map_err(|_| WriterError::MaintenanceStepDeadline)??;
    let staging = local_staging::plan(config).await?;
    removed += local_staging::remove(config, lease, staging).await?;
    envelopes += crate::envelopes::cleanup_held(config, lease).await?;
    // A manifest is the proof that its version's files were removed: it goes
    // only once the version cleanup of this run has left none behind.
    if cleaned && protected == 0 {
        removed += superseded_history(config, lease, history, &head, &kept).await?;
    }
    let head = boundary(config, lease, history).await?;
    if head.content.is_some() {
        let mut dataset = pinned(config, &head).await?;
        let mut work = lineage(lease, &head);
        // The attribute indexes that were switched on appear with this publication,
        // and so do the full-text ones built again for another analyzer: the old
        // index serves until then.
        let (store, root) = namespace_store(config)?;
        let mut built = commit::previous_manifest(store.as_ref(), &root, &head)
            .await?
            .map(|manifest| manifest.attribute_indexes)
            .unwrap_or_default();
        let rebuilt = Box::pin(commit::build_attribute_indexes(
            config,
            lease,
            &mut dataset,
            &mut work,
            &built,
        ))
        .await?;
        // What was built again carries the analyzer of the settings.
        built.retain(|name, _| !rebuilt.contains(name));
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
            Some(&built),
        )
        .await?;
    }
    Ok(
        serde_json::json!({"compaction_steps":compacted,"index_merge_steps":merged,"deleted_files":removed,"protected_fragment_files":protected,"unverified_protected_fragment_files":unverified_protected,"deleted_envelopes":envelopes}),
    )
}

/// Merge the first group of the plan that packs the segments of an index into runs of at most
/// `max_rows_per_segment` rows. A run is one step: it rewrites no more than that, and a segment
/// that is that large already stays. Segments that stay are never rewritten.
async fn merge_tier(dataset: &mut Dataset, options: &OptimizeOptions) -> lance::Result<()> {
    let plan = plan_optimize_indices(dataset, options).await?;
    for merge in &plan.merges {
        let mut built = Vec::new();
        for task in plan.tasks.iter().filter(|task| task.group == merge.group) {
            built.extend(task.execute(dataset).await?);
        }
        if let Some(result) = merge.execute(dataset, &built).await? {
            return commit_optimize_indices(dataset, vec![result], options).await;
        }
    }
    Ok(())
}

async fn merge_index(
    config: &Config,
    lease: &LeaseGuard,
    dataset: Dataset,
    head: ServingHead,
    name: String,
    rebalance: bool,
) -> Result<bool> {
    tokio::time::timeout(
        config.writer.maintenance.step_max_duration.as_duration(),
        merge_step(config, lease, dataset, head, name, rebalance),
    )
    .await
    .map_err(|_| WriterError::MaintenanceStepDeadline)?
}

async fn merge_step(
    config: &Config,
    lease: &LeaseGuard,
    mut dataset: Dataset,
    head: ServingHead,
    name: String,
    rebalance: bool,
) -> Result<bool> {
    let before = dataset.version().version;
    let mut work = lineage(lease, &head);
    let mut options = OptimizeOptions::merge(usize::MAX);
    options.index_names = Some(vec![name]);
    options.retrain = false;
    let target = merge_tier_rows(config);
    if dataset.count_rows(None).await? as u64 <= target {
        if rebalance {
            options = options.with_partition_rebalancing();
        }
        commit::native_step(
            lease,
            dataset.optimize_indices(&options),
            WriterError::IndexMergeFailed,
        )
        .await?;
    } else {
        // shortcut: indexes above the step target are merged in tiers without partition
        // rebalancing; rebalance per segment when needed.
        options.max_rows_per_segment = Some(target);
        commit::native_step(
            lease,
            merge_tier(&mut dataset, &options),
            WriterError::IndexMergeFailed,
        )
        .await?;
    }
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
        None,
    )
    .await?;
    Ok(true)
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
                config.namespace.dense_element,
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
            None,
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

/// Walk the whole chain, so a manifest missing before the cut is found before
/// anything is deleted: cleanup reads the evidence of the head only.
pub(crate) async fn verify_history(config: &Config, head: &ServingHead) -> Result<()> {
    let (store, root) = namespace_store(config)?;
    let mut chain = fyrr_reader::storage::manifest_chain(
        store,
        root,
        head.content.as_ref().map(|p| p.manifest.clone()),
    );
    while chain.try_next().await?.is_some() {}
    Ok(())
}

/// Delete the publication manifests and envelope checkpoints that no retained
/// manifest names, and the policy and identity generations older than any a
/// retained manifest or the head names. The oldest retained manifest is recorded as the cut first:
/// the walks of the chain end there, and a manifest missing anywhere else is a
/// lost one. A walk that starts at a manifest older than the window finds its
/// checkpoint gone and reads the manifests instead, so what is deleted is
/// history no restore or receipt reaches. Like the other steps, the pass is
/// bounded by `step_max_duration`, listing included, meets the revocation
/// boundary every 64 deletes and ends when the head publishes; what it leaves,
/// the next `writer maintain` removes.
async fn superseded_history(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
    head: &ServingHead,
    kept: &[ContentManifest],
) -> Result<usize> {
    let limit = config.writer.maintenance.step_max_duration.as_duration();
    let start = std::time::Instant::now();
    tokio::time::timeout(limit, async {
        // The cut manifest's evidence stands for the manifests deleted before
        // it; a manifest older than the evidence has none to stand for.
        if head.content.is_none()
            || kept
                .last()
                .is_none_or(|manifest| manifest.envelope_evidence.is_none())
        {
            return Ok(0);
        }
        let mut named: BTreeSet<_> = kept
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
        // Record the cut before anything goes: a crash leaves older manifests
        // that the next pass deletes, never a chain with a hole.
        if let Some(oldest) = kept.last()
            && oldest.previous_publication_id.is_some()
        {
            let (store, root) = namespace_store(config)?;
            // The cut manifest's evidence stands for the manifests before it:
            // an earlier pass with a shorter window may have deleted it, and a
            // checkpoint that does not read is no stand-in either.
            if let Some(evidence) = &oldest.envelope_evidence
                && commit::read_checkpoint(store.as_ref(), &root, &evidence.checkpoint)
                    .await?
                    .is_none()
            {
                return Ok(0);
            }
            let path = root.join(HISTORY_CUT_OBJECT);
            // Read with its version: the write replaces exactly this cut, so a
            // writer that lost the lease cannot move a newer one back.
            let (current, mode) = match store.get(&path).await {
                Ok(object) => {
                    let version = crate::lease::version_of(&object.meta);
                    let bytes = object
                        .bytes()
                        .await
                        .map_err(|_| WriterError::StorageUnavailable)?;
                    let cut: HistoryCut = fyrr_core::parse_contract(&bytes)
                        .map_err(|_| WriterError::PublicationInvalid)?;
                    (Some(cut), PutMode::Update(version))
                }
                Err(object_store::Error::NotFound { .. }) => (None, PutMode::Create),
                Err(_) => return Err(WriterError::StorageUnavailable),
            };
            // The cut only moves forward; a pass that knows less deletes nothing.
            if current
                .as_ref()
                .is_some_and(|cut| cut.sequence > oldest.sequence)
            {
                return Ok(0);
            }
            if current.as_ref().map(|cut| &cut.publication_id) != Some(&oldest.publication_id) {
                let cut = HistoryCut {
                    schema_version: <HistoryCut as Contract>::VERSION,
                    namespace: config.namespace.name.as_str().to_owned(),
                    publication_id: oldest.publication_id.clone(),
                    sequence: oldest.sequence,
                };
                lease.head().await?;
                match crate::lease::cas_put(store.as_ref(), &path, &cut, mode).await {
                    Ok(_) => {}
                    // Another writer moved the cut: delete nothing from here.
                    Err(
                        object_store::Error::Precondition { .. }
                        | object_store::Error::AlreadyExists { .. },
                    ) => return Ok(0),
                    Err(_) => return Err(WriterError::StorageUnavailable),
                }
            }
        }
        // A policy or identity generation is named by the head and by each
        // retained manifest, and generations only grow along the head. Only
        // one below the oldest generation they name is deleted; higher ones,
        // orphans of a failed publication included, stay until the head passes
        // them. A reader that loaded an older head-only generation (a
        // directory pull, a retire, the read-only switch publish no manifest)
        // can read it gone once and fails closed with `StorageUnavailable`; a
        // fresh head fixes that.
        let policy_floor = kept
            .iter()
            .map(|manifest| manifest.policy.generation)
            .fold(head.policy.generation, u64::min);
        let identity_floor = kept
            .iter()
            .map(|manifest| manifest.identity.generation)
            .fold(head.identity.generation, u64::min);
        let (store, root, durability) = fyrr_reader::storage::cleanup_namespace_store(config)?;
        // The object a pointer names is kept whatever its key says: the
        // contract does not make the key the canonical name of the generation.
        // The key is the one the reader resolves, `//` and all.
        named.extend(
            kept.iter()
                .flat_map(|manifest| [&manifest.policy, &manifest.identity])
                .chain([&head.policy, &head.identity])
                .map(|pointer| {
                    fyrr_reader::storage::relative_path(
                        &Path::from(format!("{root}/{}", pointer.object)),
                        &root,
                    )
                }),
        );
        let served = head.content.as_ref().map(|p| p.publication_id.clone());
        let mut deleted = 0;
        for (prefix, floor) in [
            ("publications", None),
            ("evidence", None),
            ("policy", Some(policy_floor)),
            ("identity", Some(identity_floor)),
        ] {
            let mut listing = store.list(Some(&root.clone().join(prefix)));
            while let Some(object) = listing.try_next().await? {
                let relative = fyrr_reader::storage::relative_path(&object.location, &root);
                let superseded = match floor {
                    None => !named.contains(&relative),
                    Some(floor) => {
                        !named.contains(&relative)
                            && generation_of(&relative, prefix).is_some_and(|g| g < floor)
                    }
                };
                if !superseded {
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

/// The generation of an object whose key is exactly what the writer gives a
/// generation, `<prefix>/<generation>-<64 hex digits>.json`; any other key has
/// none, and is kept.
fn generation_of(relative: &str, prefix: &str) -> Option<u64> {
    let name = relative.strip_prefix(prefix)?.strip_prefix('/')?;
    let (generation, digest) = name.strip_suffix(".json")?.split_once('-')?;
    let number: u64 = generation.parse().ok()?;
    (number.to_string() == generation
        && digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
    .then_some(number)
}

#[cfg(test)]
mod generation_key_tests {
    use super::generation_of;

    #[test]
    fn only_the_writers_own_key_has_a_generation() {
        let hex = "0123456789abcdef".repeat(4);
        assert_eq!(
            generation_of(&format!("policy/9-{hex}.json"), "policy"),
            Some(9)
        );
        for key in [
            "policy/1-backup.json".to_owned(),
            format!("policy/09-{hex}.json"),
            format!("policy/+9-{hex}.json"),
            format!("policy/9-{}.json", hex.to_uppercase()),
            format!("policy/9-{hex}.bak"),
            format!("policy/inputs/9-{hex}.json"),
            format!("identity/9-{hex}.json"),
        ] {
            assert_eq!(generation_of(&key, "policy"), None, "{key}");
        }
    }
}

async fn cleanup_versions(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
    head: &ServingHead,
    kept: &[ContentManifest],
) -> Result<(usize, usize, usize, bool)> {
    if head.content.is_none() {
        return Ok((0, 0, 0, true));
    }
    let dataset = pinned(config, head).await?;
    let versions: BTreeSet<_> = kept.iter().map(|m| m.lance_version).collect();
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
    } = commit::cleanup_evidence(config, head).await?;
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
    // The plan lists at most 100,000 files; a full list may be a cut one.
    let listed_all = plan.candidate_files.len() < 100_000;
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
    // Complete: nothing the plan found remains, protected files aside.
    Ok((
        removed,
        protected_count,
        unverified_protected,
        listed_all && removed == candidates.len(),
    ))
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
            if !apply::valid_vector(vector.as_ref(), config.embedding.dimensions) {
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
        let cut = fyrr_reader::storage::history_cut(store.as_ref(), &root).await?;
        let mut previous = if cut.as_ref() == Some(&current.publication_id) {
            None
        } else {
            current.previous_publication_id
        };
        for _ in 0..64 {
            let Some(id) = previous else {
                return Ok(false);
            };
            // A maintenance that cut the history meanwhile ends the walk.
            let Some(manifest) = fyrr_reader::storage::read_older_manifest(
                store.as_ref(),
                &root,
                &format!("publications/{id}.json"),
                cut.as_ref(),
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
            // Maintenance deleted the history before the cut.
            previous = if cut.as_ref() == Some(&manifest.publication_id) {
                None
            } else {
                manifest.previous_publication_id
            };
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

#[cfg(test)]
mod merge_run_tests {
    use super::*;

    fn segment(first_fragment: u32, rows: u64) -> IndexIdentity {
        IndexIdentity {
            name: "vector_idx".into(),
            kind: "ivf_rq".into(),
            column: "vector".into(),
            uuid: first_fragment.to_string(),
            partitions: None,
            rows: Some(rows),
            first_fragment: Some(first_fragment),
        }
    }

    fn pending(segments: &[IndexIdentity], target: u64) -> bool {
        merge_run_pending(&segments.iter().collect::<Vec<_>>(), target)
    }

    #[test]
    fn a_run_needs_two_neighbors_that_fit_one_step() {
        // Every segment is at the target: nothing to merge, however many there are.
        let full: Vec<_> = (0..100).map(|i| segment(i, 100)).collect();
        assert!(!pending(&full, 100));
        // Two that fit.
        assert!(pending(
            &[segment(0, 100), segment(1, 40), segment(2, 60)],
            100
        ));
        // Neighbors in fragment order, whatever the manifest order: the big segment is between.
        assert!(!pending(
            &[segment(0, 40), segment(2, 40), segment(1, 100)],
            100
        ));
        // No two neighbors fit.
        assert!(!pending(
            &[segment(0, 60), segment(1, 60), segment(2, 60)],
            100
        ));
        // A segment without rows or fragments takes no part.
        let mut unknown = segment(1, 10);
        unknown.rows = None;
        assert!(!pending(&[segment(0, 10), unknown], 100));
    }
}

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
            None,
        )
        .await
    };
    tokio::time::timeout(limit, step)
        .await
        .map_err(|_| WriterError::MaintenanceStepDeadline)??;
    Ok(true)
}
