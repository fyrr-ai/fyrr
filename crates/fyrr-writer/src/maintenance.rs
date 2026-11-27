//! Bounded native work under the namespace lease; manifests remain the only visibility point.
use crate::{Result, WriterError};
pub(crate) mod local_staging;

use crate::{apply, commit, lease::LeaseGuard, prepare};
use arrow_array::Array;
use chrono::Utc;
use futures::TryStreamExt;
use fyrr_core::Contract;
use fyrr_core::attributes::stale_analyzers;
use fyrr_core::config::Config;
use fyrr_core::envelope::EnvelopePayload;
use fyrr_core::publication::{
    CommitLineage, ContentManifest, HISTORY_CUT_OBJECT, HistoryCut, PolicyArtifact, ServingHead,
};
use fyrr_reader::storage::{dataset_uri, namespace_store, pinned, read_contract};
use lance::Dataset;
use lance::dataset::cleanup::CleanupPolicy;
use lance::dataset::optimize::{CompactionOptions, plan_compaction};
use lance::dataset::transaction::{Operation, RewriteGroup};
use lance::index::DatasetIndexExt;
use lance::index::DatasetIndexInternalExt;
use lance::index::optimize::{
    IndexSegmentResult, OptimizeIndexTask, commit_optimize_indices, plan_optimize_indices,
};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::optimize::OptimizeOptions;
use lance_index::vector::ivf::builder::recommended_num_partitions;
use object_store::{ObjectStoreExt, PutMode, path::Path};
use std::collections::{BTreeMap, BTreeSet};

// Bound catalog-wide index opens.
pub(crate) const INDEX_SEGMENT_LIMIT: usize = 64;
// Rows × partitions × dimensions that a step encodes per second: a row is compared with every
// centroid of the model. The slowest step measured on #319, its own work included: 150,000 rows
// of 128 dimensions against 4,096 partitions in 104.0 s (350,000 rows took 80-108 s), on `writer
// maintain`, cores 0-3, `writer.threads = 2`, a host at load 10 to 20. Re-measure with
// `scripts/bench-scale` when the step changes.
const ENCODE_DISTANCES_PER_SECOND: f64 = 750e6;
// Partitions² × `VECTOR_SAMPLE_RATE` × dimensions that training computes per second: k-means over
// the sample and its loss against every centroid. Measured on #319: 4,096 partitions × 64 × 128
// dimensions in 66-101 s, and 1,090 × 128 × 128 in 14.2 s.
const TRAIN_DISTANCES_PER_SECOND: f64 = 1.3e9;
// Rows × dimensions that a step copies per second into one segment. The slowest measured on #319:
// 3,000,000 rows of 128 dimensions in 37.7 s on a host at load 20.
const COPY_DIMENSIONS_PER_SECOND: f64 = 9.6e6;
// A vector index gets a new model when its rows ask for this many times the partitions of the
// model it has, or for Lance's ceiling: each model serves until the rows about double.
const MODEL_GROWTH: usize = 2;

/// The work of one maintenance step, priced by `step_cost`.
#[derive(Clone, Copy)]
enum Work<'a> {
    /// Rows encoded into every vector index of `models`, each `(partitions, dimensions)`: an
    /// encode step, the encoding of a step that trains, or a compaction.
    Encode {
        rows: u64,
        models: &'a [(usize, usize)],
    },
    /// Rows of `dimensions` copied into one segment.
    Copy { rows: u64, dimensions: usize },
    /// Training a model of `partitions` of `dimensions` on `VECTOR_SAMPLE_RATE` vectors each.
    Train {
        partitions: usize,
        dimensions: usize,
    },
}

/// The seconds `work` takes at the slowest rates measured, the one estimate every step, the
/// compaction and `due` decide by. With `FYRR_TEST_STEP_ROWS` (debug builds) an encoded or copied
/// row costs one and training nothing, so that a test does not depend on how fast its host is.
fn step_cost(work: Work) -> f64 {
    let test = test_step_rows().is_some();
    match work {
        Work::Encode { rows, models } if test => (rows * models.len().max(1) as u64) as f64,
        Work::Encode { rows, models } => {
            models
                .iter()
                .map(|&(partitions, dimensions)| rows as f64 * (partitions * dimensions) as f64)
                .sum::<f64>()
                / ENCODE_DISTANCES_PER_SECOND
        }
        Work::Copy { rows, .. } if test => rows as f64,
        Work::Train { .. } if test => 0.0,
        Work::Copy { rows, dimensions } => {
            rows as f64 * dimensions as f64 / COPY_DIMENSIONS_PER_SECOND
        }
        Work::Train {
            partitions,
            dimensions,
        } => {
            (partitions * partitions * crate::commit::VECTOR_SAMPLE_RATE * dimensions) as f64
                / TRAIN_DISTANCES_PER_SECOND
        }
    }
}

/// What a step's main work may cost: half of `step_max_duration`, in the units of `step_cost`.
/// The other half covers opening the segments, writing the version and publishing it.
fn step_budget(config: &Config) -> f64 {
    test_step_rows().map_or_else(
        || {
            config
                .writer
                .maintenance
                .step_max_duration
                .as_duration()
                .as_secs_f64()
                / 2.0
        },
        |rows| rows as f64,
    )
}

/// Debug builds only: `FYRR_TEST_STEP_ROWS` fixes the rows a step encodes, so that a test of
/// small namespaces does not depend on how fast its host is.
fn test_step_rows() -> Option<u64> {
    #[cfg(debug_assertions)]
    if let Some(rows) = std::env::var("FYRR_TEST_STEP_ROWS")
        .ok()
        .and_then(|rows| rows.parse().ok())
    {
        return Some(rows);
    }
    None
}

/// Debug builds only: `FYRR_TEST_VECTOR_STEPS` ends a run's vector index steps after that many, so
/// that a test can leave an index between two models.
fn test_vector_steps() -> Option<usize> {
    #[cfg(debug_assertions)]
    if let Some(steps) = std::env::var("FYRR_TEST_VECTOR_STEPS")
        .ok()
        .and_then(|steps| steps.parse().ok())
    {
        return Some(steps);
    }
    None
}

/// The most rows of which `work` (given for one row) fits `budget`, at least 1.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn rows_within(budget: f64, one_row: f64) -> u64 {
    if one_row <= 0.0 {
        return u64::MAX;
    }
    (budget / one_row).clamp(1.0, u64::MAX as f64) as u64
}

/// The rows a step copies into one segment of `dimensions`: 11,250,000 of 128 dimensions by
/// default.
fn copy_rows(config: &Config, dimensions: usize) -> u64 {
    rows_within(
        step_budget(config),
        step_cost(Work::Copy {
            rows: 1,
            dimensions,
        }),
    )
}

/// The partitions a model of `dimensions` grows to: Lance's ceiling of 4,096, or fewer where its
/// training must fit a step's main work (about 1,720 of 1,024 dimensions by default).
fn largest_model(config: &Config, dimensions: usize) -> usize {
    let ceiling = recommended_num_partitions(usize::MAX, 1);
    let one = step_cost(Work::Train {
        partitions: 1,
        dimensions,
    });
    let fits = rows_within(step_budget(config), one) as f64;
    usize::try_from(rows_within(fits.sqrt(), 1.0))
        .unwrap_or(ceiling)
        .min(ceiling)
}

/// The vector columns of the namespace, each with the largest model it grows to.
fn largest_models(config: &Config) -> Vec<(usize, usize)> {
    config
        .namespace
        .settings
        .vectors
        .values()
        .map(|vector| vector.dimensions as usize)
        .chain([config.embedding.dimensions as usize])
        .map(|dimensions| (largest_model(config, dimensions), dimensions))
        .collect()
}

/// Whether a compaction may rewrite `rows`. Lance encodes them again into every vector index, so
/// they must fit half a step against the largest models of all of them at once: no fragment it
/// makes outgrows a later encode step, nor the half of one that the step that trains encodes.
/// 107,000 rows for one column of 128 dimensions by default.
// shortcut: a row of a multi-vector attribute counts as one vector, and the other indexes' work
// is not priced (#385).
fn compaction_fits(config: &Config, rows: u64) -> bool {
    step_cost(Work::Encode {
        rows,
        models: &largest_models(config),
    }) <= step_budget(config) / 2.0
}

/// The widest vector column of the namespace.
fn max_dimensions(config: &Config) -> usize {
    config
        .namespace
        .settings
        .vectors
        .values()
        .map(|vector| vector.dimensions)
        .chain([config.embedding.dimensions])
        .max()
        .unwrap_or(1) as usize
}

/// The indexes whose merge step ran out of time, by namespace and name, with the head version the
/// run that skipped them ended at. In memory: a `writer serve` keeps it between runs. Until the
/// head is another version (a new publication) neither `maintain` nor `due` tries them again, so a
/// step that does not fit its deadline is wasted once per publication, never on every tick. A skip
/// ends only with a new content version: an identity-only publication keeps `lance_version`, and
/// the data it would merge is the same.
static MERGE_SKIPPED: std::sync::Mutex<BTreeMap<(String, String), u64>> =
    std::sync::Mutex::new(BTreeMap::new());

fn merge_skipped(namespace: &str, index: &str, version: u64) -> bool {
    MERGE_SKIPPED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&(namespace.to_owned(), index.to_owned()))
        .is_some_and(|&skipped| skipped == version)
}

fn skip_merge(namespace: &str, index: &str, version: u64) {
    MERGE_SKIPPED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert((namespace.to_owned(), index.to_owned()), version);
}

/// Whether `name` is a vector index: the one that `merge_tier` packs, as against the scalar
/// ones, which Lance merges in one step.
fn is_vector_index(config: &Config, name: &str) -> bool {
    name == "vector_idx"
        || config
            .namespace
            .settings
            .vectors
            .keys()
            .any(|vector| fyrr_core::attributes::vector_index_name(vector) == name)
}

/// For a vector index of more segments than `INDEX_SEGMENT_LIMIT`, from each live segment's first
/// fragment and rows: the smallest sum of the rows of two neighbors in fragment order. `merge_tier`
/// has a run to execute exactly when this is at most the rows one step may rewrite: next-fit
/// packing makes a run of two or more segments iff some neighbors fit together.
pub(crate) fn crowded_pair_rows(mut segments: Vec<(u32, u64)>) -> Option<u64> {
    if segments.len() <= INDEX_SEGMENT_LIMIT {
        return None;
    }
    segments.sort_unstable();
    segments
        .windows(2)
        .map(|pair| pair[0].1.saturating_add(pair[1].1))
        .min()
}

/// Whether `maintain` has work, judged from the served manifest alone, so that
/// an idle `writer serve` tick asks storage for nothing more. An index with
/// more live vector segments than the limit is due at once if two neighbors fit one merge step,
/// so that an index of full segments never makes every tick run `maintain`. Everything else waits for
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
    let mut segments = BTreeMap::new();
    for index in &manifest.indexes {
        *segments.entry(&index.name).or_insert(0usize) += 1;
    }
    // Two neighbors of a crowded index are copied into one segment.
    let target = copy_rows(config, max_dimensions(config));
    if manifest.merge_pair_rows.iter().any(|(index, &pair)| {
        pair <= target && !merge_skipped(&config.namespace.name, index, manifest.lance_version)
    }) || fyrr_reader::search::attribute_indexes(
        &config.namespace.settings.attributes,
        &config.namespace.settings.attribute_indexes,
    )
    .any(|(_, name, _)| !segments.contains_key(&name))
        || stale_analyzers(
            &config.namespace.settings.attribute_indexes,
            &manifest.settings.namespace.attribute_indexes,
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
    cached_drain_authorization(config, lease, history).await?;
    lease.head().await
}

// A fresh complete listing is mandatory, even when a cursor advance failed.
// Quiet boundaries stop here, before history, markers, grouping or publication.
async fn cached_drain_authorization(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
) -> Result<usize> {
    let pending = history.pending(config, &lease.head().await?, true).await?;
    if pending.is_empty() {
        return Ok(0);
    }
    commit::drain_cached_authorization(config, lease, history, pending).await
}

async fn command_start(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
) -> Result<ServingHead> {
    history.refresh(config, &lease.head().await?).await?;
    command_recover(config, lease, history).await
}

// A failed maintenance step must yield to fresh authorization discovery before
// reading new publication history or recovering its unpublished native suffix.
async fn command_recover(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
) -> Result<ServingHead> {
    cached_drain_authorization(config, lease, history).await?;
    commit::commit_one(config, false, lease, Vec::new(), Some(history)).await?;
    lease.head().await
}

/// Publishes the version of a retained publication again, or of the one before
/// the head's with `None`: a plain Lance restore under the namespace lease. The
/// rows come back as they were then, with the readers they had. Only a
/// namespace written through the write API alone restores; one that holds a
/// connector source fails with `restore_unsupported` and recovers by a sync or
/// `writer namespace copy`.
pub async fn restore(config: &Config, publication: Option<&str>) -> Result<serde_json::Value> {
    if publication.is_some_and(|id| !fyrr_core::limits::prefixed_hex(id, "pub-", 32)) {
        return Err(WriterError::PublicationIdInvalid);
    }
    crate::lease::LeaseGuard::with(config, |lease| async move {
        // Keep the native publication future off the caller's stack.
        Box::pin(restore_held(config, &lease, publication)).await
    })
    .await
}

async fn restore_held(
    config: &Config,
    lease: &LeaseGuard,
    publication: Option<&str>,
) -> Result<serde_json::Value> {
    // A source that ever published names its chain in every later manifest.
    let (store, root) = namespace_store(config)?;
    let head = lease.head().await?;
    let connector = !config.sources.is_empty()
        || commit::previous_manifest(store.as_ref(), &root, &head)
            .await?
            .is_some_and(|m| {
                m.chain_applied
                    .keys()
                    .any(|source| source != fyrr_core::connector::API_SOURCE_ID)
            });
    if connector {
        return Err(WriterError::RestoreUnsupported);
    }
    // Recover an unpublished native suffix, so the restore follows the head.
    commit::commit_one(config, false, lease, Vec::new(), None).await?;
    let head = lease.head().await?;
    let retained = retained(config, &head).await?;
    let target = if let Some(id) = publication {
        retained.iter().find(|m| m.publication_id == id)
    } else {
        retained.get(1)
    }
    .filter(|m| {
        (fyrr_core::clock::now() - m.committed_at)
            .to_std()
            .is_ok_and(|age| age < config.writer.purge_window.as_duration())
    })
    .ok_or(WriterError::RestoreHorizonExpired)?;
    let current = pinned(config, &head).await?;
    let mut dataset = current
        .checkout_version(target.lance_version)
        .await
        .map_err(|_| WriterError::RestoreHorizonExpired)?;
    let mut work = lineage(lease, &head);
    commit::native_step(lease, dataset.restore(), WriterError::RestoreCommitFailed).await?;
    commit::record_transaction(&dataset, &mut work, current.version().version).await?;
    // The settings hold the attributes of now: a version from before one was
    // added has no column for it, and gets one that reads null.
    Box::pin(commit::add_attribute_columns(
        config,
        lease,
        &mut dataset,
        &mut work,
    ))
    .await?;
    let version = commit::publish(
        config,
        lease,
        &dataset,
        head,
        work,
        Vec::new(),
        Default::default(),
        true,
        false,
        // The indexes stand as the restored publication built them.
        Some(&target.settings.namespace.attribute_indexes),
    )
    .await?;
    Ok(serde_json::json!({
        "restored": true,
        "lance_version": version,
        "from_publication": target.publication_id,
    }))
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
            && let Err(error) = command_recover(config, &lease, &mut history).await
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
    command_start(config, lease, history).await?;
    let version = |head: &ServingHead| head.content.as_ref().map_or(0, |p| p.lance_version);
    let mut skipped = Vec::new();
    let mut compacted = 0;
    let mut merged = 0;
    let mut vector_steps = 0;
    let limit = config.writer.maintenance.step_max_duration.as_duration();
    loop {
        let head = boundary(config, lease, history).await?;
        if head.content.is_none() {
            break;
        }
        let dataset = pinned(config, &head).await?;
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
    // One index name per interruptible step: `vector_step` for a vector index, a merge of all
    // the segments for any other.
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
            // An index that ran out of time on this version waits for a new publication.
            if merge_skipped(&config.namespace.name, &name, version(&head)) {
                continue;
            }
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
                if segments <= 1 {
                    break;
                }
                let start = std::time::Instant::now();
                let at = version(&head);
                match merge_index(config, lease, dataset, head, name.clone()).await {
                    Ok(Some((kind, rows))) => {
                        merged += 1;
                        crate::telemetry::index_merge_done(kind, rows, start.elapsed());
                        vector_steps += usize::from(kind != "merge");
                        if test_vector_steps().is_some_and(|most| vector_steps >= most) {
                            break;
                        }
                    }
                    // A crowded index that no step can merge waits for a new publication, so that
                    // `due` does not run maintenance for it again on every tick.
                    Ok(None) => {
                        if segments > INDEX_SEGMENT_LIMIT {
                            skip_merge(&config.namespace.name, &name, at);
                            skipped.push(name.clone());
                        }
                        break;
                    }
                    // The other indexes and the steps after the merges still run.
                    Err(WriterError::MaintenanceStepDeadline) => {
                        // Recorded now: a later step of this run may fail before the run ends.
                        skip_merge(&config.namespace.name, &name, at);
                        crate::telemetry::index_merge_skipped(&name);
                        command_recover(config, lease, history).await?;
                        skipped.push(name);
                        break;
                    }
                    Err(error) => return Err(error),
                }
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
            .map(|manifest| manifest.settings.namespace.attribute_indexes)
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
            Default::default(),
            false,
            true,
            Some(&built),
        )
        .await?;
    }
    // The run's own publications moved the head on: the skip follows it.
    let end = version(&lease.head().await?);
    for index in skipped {
        skip_merge(&config.namespace.name, &index, end);
    }
    Ok(
        serde_json::json!({"compaction_steps":compacted,"index_merge_steps":merged,"deleted_files":removed,"protected_fragment_files":protected,"unverified_protected_fragment_files":unverified_protected,"deleted_envelopes":envelopes}),
    )
}

/// Merge the first group of the plan that packs the segments of an index into runs of at most
/// `max_rows_per_segment` rows, and return the rows it rewrote; none if there is no run. A run is
/// one step: it rewrites no more than that, and a segment that is that large already stays.
/// Segments that stay are never rewritten.
async fn merge_tier(dataset: &mut Dataset, options: &OptimizeOptions) -> lance::Result<u64> {
    // Planning opens every segment of the index, so its cost grows with their number.
    let plan = plan_optimize_indices(dataset, options).await?;
    for merge in &plan.merges {
        let mut built = Vec::new();
        for task in plan.tasks.iter().filter(|task| task.group == merge.group) {
            built.extend(task.execute(dataset).await?);
        }
        if let Some(result) = merge.execute(dataset, &built).await? {
            let rows = result
                .segment
                .fragment_bitmap
                .iter()
                .flat_map(|bitmap| bitmap.iter())
                .filter_map(|id| dataset.get_fragment(id as usize))
                .map(|fragment| fragment.metadata().num_rows().unwrap_or(0) as u64)
                .sum::<u64>();
            commit_optimize_indices(dataset, vec![result], options).await?;
            return Ok(rows);
        }
    }
    Ok(0)
}

/// The rows of each fragment of the dataset.
fn fragment_rows(dataset: &Dataset) -> BTreeMap<u32, u64> {
    dataset
        .manifest()
        .fragments
        .iter()
        .map(|f| (crate::narrow_u32(f.id), f.num_rows().unwrap_or(0) as u64))
        .collect()
}

/// The segments of vector index `name` in the order of the manifest, the order they were
/// committed in, each with the fragments it still answers for.
fn vector_segments(
    indices: &[lance::table::format::IndexMetadata],
    name: &str,
    fragments: &BTreeMap<u32, u64>,
) -> Vec<(lance::table::format::IndexMetadata, Vec<u32>)> {
    indices
        .iter()
        .filter(|segment| segment.name == name)
        .map(|segment| {
            let covered = segment
                .fragment_bitmap
                .iter()
                .flat_map(|bitmap| bitmap.iter())
                .filter(|id| fragments.contains_key(id))
                .collect();
            (segment.clone(), covered)
        })
        .collect()
}

/// The newest segment that answers for a fragment, its column and its model: the one Lance
/// encodes the next write against. None for an index without one.
// shortcut: Lance drops a segment once every row it answers for is gone, in the commit that
// deletes them. A model lives only in its segments, so if the rows of the one segment a model
// was trained into are all deleted before another step encodes against it (a run that stopped
// right after training), the model is lost and a later step trains it again (#385).
async fn newest_model(
    dataset: &Dataset,
    segments: &[(lance::table::format::IndexMetadata, Vec<u32>)],
) -> lance::Result<
    Option<(
        lance::table::format::IndexMetadata,
        String,
        std::sync::Arc<dyn lance_index::vector::VectorIndex>,
    )>,
> {
    let Some((segment, _)) = segments
        .iter()
        .rev()
        .find(|(_, covered)| !covered.is_empty())
    else {
        return Ok(None);
    };
    let column = dataset
        .schema()
        .field_by_id(segment.fields[0])
        .map(|field| field.name.clone())
        .unwrap_or_default();
    let model = dataset
        .open_vector_index(&column, &segment.uuid, &NoOpMetricsCollector)
        .await?;
    Ok(Some((segment.clone(), column, model)))
}

/// One step of a vector index: its kind and the rows it rewrote, 0 if the index is as it should
/// be.
///
/// Every segment is encoded against an IVF model, and a write encodes against the model of the
/// newest segment. The model of the first write has the partitions of that write's rows, so an
/// index that grew is searched in partitions far above `dense_partition_rows`, and recall falls.
/// The steps:
///
/// 1. `train`: the rows ask for `MODEL_GROWTH` times the partitions of the newest model, or for
///    Lance's ceiling, and training them fits a step (`step_cost`). Lance trains a new model on a
///    sample of the whole table, and the step encodes the smallest segment against it, at most
///    half an encode step. The new segment is the newest, so the writes follow the new model.
/// 2. `encode`: a segment has another model than the newest. The step encodes the oldest such
///    segments again against the newest model, at most an encode step, as one new segment.
/// 3. `copy`: every segment has the newest model. The step copies neighbors into one segment of
///    at most `copy_rows`. Of an index larger than an encode step, the largest segment joins only
///    once the others together hold half its rows; until then they are copied into segments of at
///    most twice their rows.
///
/// A step that reaches only part of a segment encodes its first fragments, at least one, and
/// commits the segment again answering for the others. Each step is one Lance version, so a run
/// that stops continues from the table alone. Segments of two models are searched each with its
/// own, so every state in between serves.
async fn vector_step(
    config: &Config,
    dataset: &mut Dataset,
    name: &str,
    mut options: OptimizeOptions,
) -> lance::Result<(&'static str, u64)> {
    let fragments = fragment_rows(dataset);
    let segments = vector_segments(&dataset.load_indices().await?, name, &fragments);
    let Some((newest, column, model)) = newest_model(dataset, &segments).await? else {
        return Ok(("copy", 0));
    };
    let dimensions = model.ivf_model().dimension();
    // shortcut: a row is one vector. A row of a multi-vector attribute holds several, which a
    // step encodes each (#385).
    let encode_rows = |partitions: usize, share: f64| {
        rows_within(
            step_budget(config) * share,
            step_cost(Work::Encode {
                rows: 1,
                models: &[(partitions, dimensions)],
            }),
        )
    };
    // shortcut: Lance trains at most 4,096 partitions (`recommended_num_partitions`), and a model
    // grows only while training fits half a step. The model so stays at 4,096 partitions above
    // about 2,250,000 rows of 128 dimensions, and at about 1,720 of 1,024 dimensions by default;
    // a partition then holds more than twice `dense_partition_rows` above 4,500,000 and
    // 1,900,000 rows. Lance's retrain sizes a model by the rows, so if the rows ask for more
    // than fits between two runs, the model keeps its partitions: #385 trains with an explicit
    // count.
    let ceiling = recommended_num_partitions(usize::MAX, 1);
    let wanted = recommended_num_partitions(
        crate::narrow_usize(fragments.values().sum()),
        config.namespace.settings.dense_partition_rows as usize,
    );
    let current = model.total_partitions();
    let mut grow = wanted > current
        && wanted >= (MODEL_GROWTH * current).min(ceiling)
        && step_cost(Work::Train {
            partitions: wanted,
            dimensions,
        }) <= step_budget(config);
    // shortcut: Lance sizes the model by all the rows of the table, also those that leave the
    // column null, so a column of which fewer than half the rows hold a vector keeps its model:
    // its vectors would fill fewer than half the partitions' target rows. The vectors are the
    // rows its segments index (a segment may still hold some of rows since deleted). #385 sizes
    // the model by the vectors.
    if grow {
        let mut vectors = 0;
        for (segment, covered) in &segments {
            if !covered.is_empty() {
                vectors += dataset
                    .open_vector_index(&column, &segment.uuid, &NoOpMetricsCollector)
                    .await?
                    .num_rows();
            }
        }
        grow = MODEL_GROWTH as u64 * vectors
            >= (wanted * config.namespace.settings.dense_partition_rows as usize) as u64;
    }
    // shortcut: training runs in the step's process and cannot stop at the deadline. It holds
    // the sample, 4,096 × 64 vectors (1.07 GB of 1,024 dimensions); `step_cost` keeps it within
    // half a step on the measured host.
    let (kind, reference, mut stale, budget) = if grow {
        let start = std::time::Instant::now();
        let mut retrain = options.clone();
        retrain.retrain = true;
        retrain.max_rows_per_task = Some(encode_rows(wanted, 1.0));
        // The planner of a split rebuild trains the model and plans tasks against it. Only the
        // model is taken: the task below follows the segments instead, so that the step commits
        // alone.
        let plan = plan_optimize_indices(dataset, &retrain).await?;
        let Some(task) = plan.tasks.into_iter().next() else {
            return Ok(("train", 0));
        };
        crate::telemetry::index_model_trained(name, wanted, start.elapsed());
        // Training spent up to half the step's main work: the step encodes half as many rows.
        let budget = encode_rows(wanted, 0.5);
        (
            "train",
            task.reference,
            segments.iter().collect::<Vec<_>>(),
            budget,
        )
    } else {
        let mut stale = Vec::new();
        for segment in &segments {
            if segment.0.uuid == newest.uuid || segment.1.is_empty() {
                continue;
            }
            let other = dataset
                .open_vector_index(&column, &segment.0.uuid, &NoOpMetricsCollector)
                .await?;
            if other.ivf_model().centroids_array().map(|c| c.to_data())
                != model.ivf_model().centroids_array().map(|c| c.to_data())
            {
                stale.push(segment);
            }
        }
        let budget = encode_rows(current, 1.0);
        ("encode", newest.clone(), stale, budget)
    };
    stale.retain(|(_, covered)| !covered.is_empty());
    let rows_of = |covered: &[u32]| covered.iter().map(|id| fragments[id]).sum::<u64>();
    if stale.is_empty() {
        let sizes: Vec<u64> = segments
            .iter()
            .map(|(_, covered)| rows_of(covered))
            .collect();
        let largest = sizes.iter().copied().max().unwrap_or(0);
        let others = sizes.iter().sum::<u64>() - largest;
        let run = if 2 * others >= largest || largest + others <= budget {
            largest + others
        } else {
            2 * others
        };
        options.max_rows_per_segment = Some(copy_rows(config, dimensions).min(run).max(1));
        return Ok(("copy", merge_tier(dataset, &options).await?));
    }
    // The step that trained takes the smallest segment, the newest of them, which a later step
    // can copy into the run before it.
    if kind == "train" {
        stale = stale
            .into_iter()
            .rev()
            .min_by_key(|(_, covered)| rows_of(covered))
            .into_iter()
            .collect();
    }
    // Whole segments, oldest first, while they fit the step; of a larger first one, its first
    // fragments.
    // shortcut: a fragment is encoded whole. A compacted one holds at most what `compaction_fits`,
    // half of what a step encodes against the largest model, and a write's one at most
    // `envelope_max_rows` (50,000 by default, within an encode step up to 1,024 dimensions); a
    // larger `envelope_max_rows`, or a wider vector column, can make one step encode more than
    // its budget (#385).
    let (mut encode, mut replaces, mut rows) = (Vec::new(), Vec::new(), 0u64);
    let mut rest = None;
    for (segment, covered) in stale {
        let more = rows_of(covered);
        if rows + more <= budget {
            encode.extend(covered.iter().copied());
            replaces.push(segment.uuid);
            rows += more;
            continue;
        }
        if replaces.is_empty() {
            let mut taken = 0;
            for id in covered {
                if taken > 0 && rows + fragments[id] > budget {
                    break;
                }
                encode.push(*id);
                rows += fragments[id];
                taken += 1;
            }
            if taken == covered.len() {
                replaces.push(segment.uuid);
            } else {
                let mut left = segment.clone();
                left.fragment_bitmap = Some(covered[taken..].iter().copied().collect());
                rest = Some(left);
            }
        }
        break;
    }
    // A segment none of whose fragments is left answers for nothing: it goes with the step.
    replaces.extend(
        segments
            .iter()
            .filter(|(_, covered)| covered.is_empty())
            .map(|(segment, _)| segment.uuid),
    );
    encode.sort_unstable();
    let task = OptimizeIndexTask {
        plan_id: reference.uuid,
        read_version: dataset.version().version,
        group: 0,
        fragments: encode,
        reference,
        old_segments: Vec::new(),
        num_indices_to_merge: Some(0),
        enable_partition_rebalancing: false,
        retrain: false,
    };
    let Some(mut result) = task.execute(dataset).await? else {
        return Ok((kind, 0));
    };
    result.replaces = replaces;
    // The segment with the rest of its fragments comes first, so that the encoded one stays the
    // newest: the one the next write is encoded against.
    let mut results = Vec::new();
    if let Some(left) = rest {
        results.push(IndexSegmentResult {
            plan_id: result.plan_id,
            read_version: result.read_version,
            group: result.group,
            replaces: vec![left.uuid],
            segment: left,
        });
    }
    results.push(result);
    commit_optimize_indices(dataset, results, &options).await?;
    Ok((kind, rows))
}

async fn merge_index(
    config: &Config,
    lease: &LeaseGuard,
    dataset: Dataset,
    head: ServingHead,
    name: String,
) -> Result<Option<(&'static str, u64)>> {
    tokio::time::timeout(
        config.writer.maintenance.step_max_duration.as_duration(),
        merge_step(config, lease, dataset, head, name),
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
) -> Result<Option<(&'static str, u64)>> {
    let before = dataset.version().version;
    let mut work = lineage(lease, &head);
    let mut options = OptimizeOptions::merge(usize::MAX);
    options.index_names = Some(vec![name.clone()]);
    options.retrain = false;
    let rows = if is_vector_index(config, &name) {
        commit::native_step(
            lease,
            Box::pin(vector_step(config, &mut dataset, &name, options)),
            WriterError::IndexMergeFailed,
        )
        .await?
    } else {
        // shortcut: any other index merges all its segments in one step. The full-text one took
        // 98-111 s at 3,000,000 rows (#319), so it outgrows a 5-minute step at about 8,000,000
        // rows: #381 merges it in steps.
        commit::native_step(
            lease,
            dataset.optimize_indices(&options),
            WriterError::IndexMergeFailed,
        )
        .await?;
        ("merge", dataset.count_rows(None).await? as u64)
    };
    commit::record_transaction(&dataset, &mut work, before).await?;
    if dataset.version().version == before {
        return Ok(None);
    }
    commit::publish(
        config,
        lease,
        &dataset,
        head,
        work,
        Vec::new(),
        Default::default(),
        false,
        false,
        None,
    )
    .await?;
    Ok(Some(rows))
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
                config.namespace.settings.dense_element,
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
            Default::default(),
            true,
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
        if head.content.is_none() || kept.is_empty() {
            return Ok(0);
        }
        let mut named: BTreeSet<_> = kept
            .iter()
            .flat_map(|manifest| {
                [format!("publications/{}.json", manifest.publication_id)]
                    .into_iter()
                    .chain([manifest.envelope_evidence.checkpoint.object.clone()])
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
            if commit::read_checkpoint(store.as_ref(), &root, &oldest.envelope_evidence.checkpoint)
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
                    let cut: HistoryCut = fyrr_reader::storage::parse_stored(
                        &bytes,
                        fyrr_reader::storage::StorageError::PublicationInvalid,
                    )?;
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
mod merge_run_tests {
    use super::{
        INDEX_SEGMENT_LIMIT, compaction_fits, crowded_pair_rows, due, merge_skipped, skip_merge,
    };

    #[test]
    fn a_compaction_prices_every_vector_index_it_encodes_again() {
        let config = |vectors: &str| {
            fyrr_core::config::parse(&format!(
                "[namespace]\nname='compaction'\n[namespace.write_api]\n{vectors}[storage]\n\
                 uri='s3://compaction'\n[embedding]\nendpoint='https://cache.invalid'\n\
                 dimensions=128\n"
            ))
            .unwrap()
            .config
        };
        // One column of 128 dimensions: a pair of 107,288 rows fits half a 5-minute step against
        // a model of 4,096 partitions.
        let one = config("");
        assert!(compaction_fits(&one, 107_288));
        assert!(!compaction_fits(&one, 107_289));
        // Eight such columns: Lance encodes the pair into each, so an eighth of it fits.
        let eight = config(
            &(0..7)
                .map(|i| format!("[namespace.vectors.v{i}]\ndimensions=128\n"))
                .collect::<String>(),
        );
        assert!(compaction_fits(&eight, 13_411));
        assert!(!compaction_fits(&eight, 13_412));
    }

    #[test]
    fn the_smallest_pair_is_of_neighbors_in_fragment_order_and_of_a_crowded_index_only() {
        let crowded = |extra: Vec<(u32, u64)>| {
            // Past the limit, in fragment order from 1000 on, each of a full step of 100 rows.
            let full = (0..INDEX_SEGMENT_LIMIT as u32).map(|i| (1000 + i, 100));
            full.chain(extra).collect::<Vec<_>>()
        };
        assert_eq!(crowded_pair_rows(vec![(0, 40), (1, 40)]), None);
        assert_eq!(crowded_pair_rows(crowded(vec![])), None);
        // Every pair is above a step of 100 rows: no merge is due.
        assert_eq!(crowded_pair_rows(crowded(vec![(2000, 100)])), Some(200));
        // The manifest order is not the fragment order: the 40s are not neighbors.
        assert_eq!(
            crowded_pair_rows(crowded(vec![(500, 40), (550, 100), (600, 40)])),
            Some(140)
        );
        assert_eq!(
            crowded_pair_rows(crowded(vec![(2000, 40), (2001, 60)])),
            Some(100)
        );
    }

    #[test]
    fn a_skip_recorded_before_the_run_publishes_keeps_due_false_for_that_version() {
        let config = fyrr_core::config::parse(
            "[namespace]\nname='due-skip'\n[storage]\nuri='s3://due-skip'\n[embedding]\n\
                endpoint='https://cache.invalid'\n",
        )
        .unwrap()
        .config;
        let mut manifest: fyrr_core::publication::ContentManifest = serde_json::from_str(
            include_str!("../../../fixtures/contracts/content-manifest/manifest.json"),
        )
        .unwrap();
        // Just maintained, and holding every index the settings ask for, so that only a crowded
        // vector index with a pair that fits one step is due.
        manifest.maintenance_completed_at = Some(fyrr_core::clock::now());
        manifest.indexes = fyrr_reader::search::attribute_indexes(
            &config.namespace.settings.attributes,
            &config.namespace.settings.attribute_indexes,
        )
        .map(|(column, name, _)| fyrr_core::publication::IndexIdentity {
            name,
            kind: "btree".into(),
            column,
            uuid: "00000000-0000-0000-0000-000000000000".into(),
            partitions: None,
        })
        .collect();
        manifest.settings.namespace.attribute_indexes.clear();
        assert!(!due(&config, &manifest));
        manifest.merge_pair_rows.insert("vector_idx".into(), 1);
        assert!(due(&config, &manifest));
        // The merge ran out of time at the version the run began with, and the run then failed
        // before it published: the manifest is still that version.
        skip_merge("due-skip", "vector_idx", manifest.lance_version);
        assert!(!due(&config, &manifest));
        manifest.lance_version += 1;
        assert!(due(&config, &manifest));
    }

    #[test]
    fn a_skipped_index_waits_for_a_new_version() {
        assert!(!merge_skipped("skip-ns", "vector_idx", 7));
        skip_merge("skip-ns", "vector_idx", 7);
        assert!(merge_skipped("skip-ns", "vector_idx", 7));
        assert!(!merge_skipped("skip-ns", "vector_idx", 8));
        assert!(!merge_skipped("skip-ns", "readers_idx", 7));
        assert!(!merge_skipped("other-ns", "vector_idx", 7));
    }
}

async fn compact_one(
    config: &Config,
    lease: &LeaseGuard,
    mut dataset: Dataset,
    head: ServingHead,
) -> Result<bool> {
    let limit = config.writer.maintenance.step_max_duration.as_duration();
    // The rewritten rows are encoded again against the vector models.
    // Limit the actual source set, rather than asking the native planner to
    // discard an oversized bin. Deleted fragments take priority regardless of size.
    let fragments = &dataset.manifest().fragments;
    let rows = |f: &lance::table::format::Fragment| f.physical_rows.unwrap_or(0) as u64;
    // shortcut: a fragment with deletions is rewritten whole, since its deleted rows must leave
    // storage. It holds at most `envelope_max_rows` (50,000 by default) or, once compacted,
    // `compaction_fits`; a larger `envelope_max_rows` can make this step longer.
    let selected: BTreeSet<_> =
        if let Some(fragment) = fragments.iter().find(|f| f.deletion_file.is_some()) {
            [fragment.id].into_iter().collect()
        } else if let Some(pair) = fragments.windows(2).find(|pair| {
            pair.iter()
                .all(|f| rows(f) < config.writer.commit.group_max_rows)
                && compaction_fits(config, rows(&pair[0]) + rows(&pair[1]))
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
    let rewritten = fragments
        .iter()
        .filter(|f| selected.contains(&f.id))
        .map(rows)
        .sum();
    let start = std::time::Instant::now();
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
            Default::default(),
            false,
            false,
            None,
        )
        .await
    };
    tokio::time::timeout(limit, step)
        .await
        .map_err(|_| WriterError::MaintenanceStepDeadline)??;
    crate::telemetry::compaction_done(rewritten, start.elapsed());
    Ok(true)
}
