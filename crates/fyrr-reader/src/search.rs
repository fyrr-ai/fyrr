//! Permission-aware partition selection with Lance native search and refinement.
use crate::telemetry::{emit, micros};
use crate::tools::revision as record_revision;
use fyrr_core::layout::NamespaceLayout as Layout;

use crate::embedding::{self, digest};
use crate::error::*;
use crate::generation::{GenerationState, MemoryPool, NativeSession, PreparationReservation};
use crate::mirror::{MirrorLease, MirrorRequest, MirrorStore};
use crate::retrieval::{Candidate, compose};
use crate::routing::{CentroidRouter, RoutingFallback, strategy};
use crate::storage::*;
use arrow_array::{Array, Float32Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
use futures::TryStreamExt;
use fyrr_core::config::{Config, ProcessEnvironment, RoutingStrategy};
use fyrr_core::identity::IdentityArtifact;
use fyrr_core::mcp::*;
use fyrr_core::publication::*;
use lance::Dataset;
use lance::dataset::scanner::{RowAddrMask, RowAddrTreeMap};
use lance::index::prefilter::DatasetPreFilter;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_index::{
    IndexType,
    metrics::NoOpMetricsCollector,
    vector::{ApproxMode, Query, VectorIndex},
};
use lance_io::object_store::WrappingObjectStore;
use object_store::path::Path;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

/// A surviving hit carries the winning query's text policy through hydration.
pub(crate) struct SelectedHit<'a> {
    pub candidate: Candidate,
    pub query: &'a str,
    pub query_index: Option<u32>,
    pub output: OutputMode,
    pub excerpt_chars: u32,
}

pub(crate) const AUTHORIZATION_CACHE_MAX_BYTES: usize = 128 * 1024 * 1024;

fn routing_telemetry(telemetry: &mut Telemetry, fallback: Option<RoutingFallback>, rabitq: bool) {
    let method = if rabitq {
        RoutingMethod::Rabitq
    } else {
        RoutingMethod::Exact
    };
    telemetry.routing_strategy = Some(match telemetry.routing_strategy {
        Some(previous) if previous != method => RoutingMethod::Mixed,
        _ => method,
    });
    telemetry.routing_fallback = telemetry.routing_fallback.or(fallback);
}

/// Per-partition admission flags of one vector segment. The map names its
/// segment but does not own the root: a cached admission must not keep a
/// generation's native index (and the memory its reservation pays for) alive.
#[derive(Debug)]
pub struct PartitionMap {
    segment: String,
    allowed: Vec<bool>,
}

// Completion order selects the sample. Retain the existing embedding and
// predicate with 600 bytes of row-ID scratch until the successful reply.
pub(crate) type DenseCompletion = (
    Vec<f32>,
    [u64; fyrr_core::limits::mcp::MAX_SEARCH_LIMIT as usize],
    usize,
    String,
);

fn select_hits<'q>(
    ranked: Vec<Vec<Candidate>>,
    queries: &[&'q str],
    args: &SearchInput,
    output: OutputMode,
    k: usize,
    max_documents: usize,
) -> Vec<SelectedHit<'q>> {
    let selected = ranked
        .into_iter()
        .enumerate()
        .map(|(query_index, mut rows)| {
            rows.truncate(k);
            rows.into_iter()
                .enumerate()
                .map(|(rank, candidate)| SelectedHit {
                    candidate,
                    query: queries[query_index],
                    query_index: args.queries.as_ref().map(|_| {
                        let Ok(index) = u32::try_from(query_index) else {
                            unreachable!("validated query batch index");
                        };
                        index
                    }),
                    output: if rank < args.text_hits.map_or(k, |count| count as usize) {
                        output
                    } else {
                        OutputMode::Metadata
                    },
                    excerpt_chars: args
                        .excerpt_chars
                        .unwrap_or(fyrr_core::limits::mcp::EXCERPT_CODEPOINTS),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    interleave(selected, max_documents)
}

fn interleave(queries: Vec<Vec<SelectedHit<'_>>>, cap: usize) -> Vec<SelectedHit<'_>> {
    let mut queries: Vec<_> = queries.into_iter().map(Vec::into_iter).collect();
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    loop {
        let mut more = false;
        for query in &mut queries {
            if let Some(hit) = query.next() {
                more = true;
                if seen.insert((hit.candidate.source.clone(), hit.candidate.root.clone())) {
                    selected.push(hit);
                    if selected.len() == cap {
                        return selected;
                    }
                }
            }
        }
        if !more {
            return selected;
        }
    }
}

fn recall_at_k(rows: &[u64], reference: &UInt64Array, distances: &Float32Array, k: usize) -> f64 {
    let denominator = reference.len().min(k);
    let hits = rows[..rows.len().min(k)]
        .iter()
        .filter(|id| {
            reference
                .values()
                .iter()
                .position(|candidate| candidate == *id)
                .is_some_and(|rank| {
                    // @10 accepts any exact neighbour tied at its distance boundary.
                    // @75 remains membership: the reference ends there, so ties
                    // beyond rank 75 can still make the measured recall read low.
                    k == 75 || distances.value(rank) <= distances.value(denominator - 1)
                })
        })
        .count();
    hits as f64 / denominator as f64
}

pub(crate) struct RecallSample {
    completion: DenseCompletion,
    identity: GenerationPointer,
    policy: GenerationPointer,
    expires: Option<i64>,
}

/// Authorized data never crosses a subject or publication boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthorizationCacheKey {
    namespace: String,
    subject: String,
    identity: String,
    identity_generation: u64,
    policy: String,
    policy_generation: u64,
    content: u64,
    hidden: Vec<String>,
}
impl AuthorizationCacheKey {
    fn memory_bound(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.namespace.capacity()
            + self.subject.capacity()
            + self.identity.capacity()
            + self.policy.capacity()
            + self.hidden.capacity() * std::mem::size_of::<String>()
            + self.hidden.iter().map(String::capacity).sum::<usize>()
    }
}
/// One completed admission: the rows a caller may read and the routing hints
/// derived from them. It is immutable once built. The subject cache and every
/// prepared reader that binds the subject share one `Arc` of it, so a request
/// can neither alter what another request sees nor pay for a private copy.
#[derive(Debug)]
pub(crate) struct Admission {
    pub(crate) eligible: RowAddrMask,
    /// Eligible rows of fragments outside the vector index coverage.
    unindexed: RowAddrTreeMap,
    maps: Option<Vec<PartitionMap>>,
    reported_hidden_sources: Vec<String>,
    fallback_probes: usize,
}
impl Admission {
    /// Charged per roaring container on top of the serialized bitmap size.
    const CONTAINER_OVERHEAD_BYTES: usize = 128;

    /// Admits nothing; the state of a reader before its first binding.
    pub(crate) fn nothing() -> Self {
        Self {
            eligible: RowAddrMask::allow_nothing(),
            unindexed: RowAddrTreeMap::new(),
            maps: None,
            reported_hidden_sources: Vec::new(),
            fallback_probes: 0,
        }
    }
    fn bitmap_bytes(&self) -> usize {
        use lance::cache::DeepSizeOf;
        self.eligible.deep_size_of() + self.unindexed.deep_size_of()
    }
    fn partition_bytes(&self) -> usize {
        self.maps.as_ref().map_or(0, |m| {
            std::mem::size_of_val(m.as_slice())
                + m.iter()
                    .map(|p| p.allowed.capacity() + p.segment.capacity())
                    .sum::<usize>()
        })
    }
    fn hidden_bytes(&self) -> usize {
        self.reported_hidden_sources.capacity() * std::mem::size_of::<String>()
            + self
                .reported_hidden_sources
                .iter()
                .map(String::capacity)
                .sum::<usize>()
    }
    /// Lance bitmap accounting and owned buffers, excluding shared native indexes.
    fn estimated_private_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.bitmap_bytes()
            + self.partition_bytes()
            + self.hidden_bytes()
    }
    /// Roaring containers of both bitmaps. Each costs a container struct, its
    /// own allocation and allocator slack that the serialized size omits; a
    /// sparse mask (a few rows per 65,536-row chunk) is mostly that overhead.
    fn bitmap_containers(&self) -> usize {
        let containers = |rows: &RowAddrTreeMap| {
            rows.iter()
                .map(|(fragment, _)| {
                    rows.get_fragment_bitmap(*fragment)
                        .map_or(1, |bitmap| bitmap.statistics().n_containers as usize)
                })
                .sum::<usize>()
        };
        self.eligible.allow_list().map_or(0, containers) + containers(&self.unindexed)
    }
    /// What the subject cache charges for retaining this admission: an
    /// estimate above its own size. The bitmaps are counted twice because
    /// Lance's serialized size omits the spare capacity of array containers,
    /// plus a fixed charge per container for the overhead it also omits.
    pub(crate) fn memory_bound(&self) -> usize {
        1024 + 2 * self.bitmap_bytes()
            + Self::CONTAINER_OVERHEAD_BYTES * self.bitmap_containers()
            + self.partition_bytes()
            + self.maps.as_ref().map_or(0, |m| m.len() * 128)
            + self.hidden_bytes()
            + self.reported_hidden_sources.len() * 32
    }
}
#[derive(Debug)]
struct CachedAdmission {
    _cache_claim: Arc<tokio::sync::OwnedSemaphorePermit>,
    admission: Arc<Admission>,
}
impl CachedAdmission {
    fn estimated_private_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.admission.estimated_private_bytes()
    }
}

#[derive(Debug)]
pub(crate) struct AuthorizationCache {
    entries: std::collections::VecDeque<(AuthorizationCacheKey, Arc<CachedAdmission>)>,
    budget: Arc<tokio::sync::Semaphore>,
}
impl Default for AuthorizationCache {
    fn default() -> Self {
        Self {
            entries: Default::default(),
            budget: Arc::new(tokio::sync::Semaphore::new(AUTHORIZATION_CACHE_MAX_BYTES)),
        }
    }
}
impl AuthorizationCache {
    pub(crate) fn retain_content(&mut self, version: u64) {
        self.entries.retain(|(key, _)| key.content == version);
        self.entries.shrink_to_fit();
    }
    fn get(&mut self, key: &AuthorizationCacheKey) -> Option<Arc<CachedAdmission>> {
        let i = self.entries.iter().position(|(k, _)| k == key)?;
        let entry = self.entries.remove(i)?;
        let result = entry.1.clone();
        self.entries.push_back(entry);
        Some(result)
    }
    /// Evict before reserving; held entries keep their permits after eviction.
    /// The caller must not insert the entry until this succeeds.
    fn reserve(
        &mut self,
        key: &AuthorizationCacheKey,
        incoming: usize,
        cap: usize,
    ) -> Option<Arc<tokio::sync::OwnedSemaphorePermit>> {
        if cap == 0 || incoming > AUTHORIZATION_CACHE_MAX_BYTES {
            return None;
        }
        self.entries.retain(|(k, _)| k != key);
        self.entries.shrink_to_fit();
        loop {
            if self.entries.len() < cap
                && let Ok(claim) = self
                    .budget
                    .clone()
                    .try_acquire_many_owned(u32::try_from(incoming).ok()?)
            {
                return Some(Arc::new(claim));
            }
            self.entries.pop_front()?;
            self.entries.shrink_to_fit();
        }
    }
    fn insert(&mut self, key: AuthorizationCacheKey, value: CachedAdmission, cap: usize) {
        self.entries.push_back((key, Arc::new(value)));
        let estimated = self
            .entries
            .iter()
            .map(|(k, e)| k.memory_bound() + e.estimated_private_bytes())
            .sum::<usize>();
        let accounted = AUTHORIZATION_CACHE_MAX_BYTES - self.budget.available_permits();
        // Fixed numeric operator metrics; no identity labels.
        crate::telemetry::emit(format_args!(
            "authorization_cache subjects={} estimated_private_bytes={estimated} \
                accounted_bytes={accounted} capacity_subjects={cap} capacity_bytes={}",
            self.entries.len(),
            AUTHORIZATION_CACHE_MAX_BYTES
        ));
    }
}

/// Required adapter for Lance's native prefilter ABI; no authorization logic lives here.
struct RowAdmission(RowAddrMask);
#[async_trait::async_trait]
impl lance_index::prefilter::FilterLoader for RowAdmission {
    async fn load(self: Box<Self>) -> lance::Result<RowAddrMask> {
        Ok(self.0)
    }
}

// Shared host resources: one session/cache envelope, mirror and process caches.
type SharedReaderResources = (
    std::sync::Weak<NativeSession>,
    Arc<MirrorStore>,
    Arc<std::sync::Mutex<AuthorizationCache>>,
    Arc<embedding::QueryEmbeddings>,
    crate::storage::ArtifactCache<PolicyArtifact>,
    crate::storage::ArtifactCache<IdentityArtifact>,
);

/// The SQL predicate and mask belong to one completed request-filter scan.
#[derive(Debug)]
pub(crate) struct FilterSelection {
    pub(crate) predicate: String,
    pub(crate) eligible: RowAddrMask,
}

/// A content pin and the authorization artifacts used to admit its queries.
#[derive(Debug)]
pub struct PreparedReader {
    #[cfg(test)]
    pub(crate) test_hooks: Arc<crate::serve::TestHooks>,
    /// Per-lane stage keys of the current search while recording
    /// (`record_stages`); `None` otherwise.
    #[cfg(test)]
    stage_trace: Option<serde_json::Value>,
    pub(crate) config: Config,
    pub(crate) dataset: Arc<Dataset>,
    pub manifest: ContentManifest,
    pub(crate) head: ServingHead,
    policy: Arc<PolicyArtifact>,
    pub(crate) policy_cache: crate::storage::ArtifactCache<PolicyArtifact>,
    identity: Arc<IdentityArtifact>,
    pub(crate) identity_cache: crate::storage::ArtifactCache<IdentityArtifact>,
    scope: scope::RequestScope,
    pub(crate) admission_cache: Arc<std::sync::Mutex<AuthorizationCache>>,
    pub(crate) admission_build_us: u64,
    /// The binding's admission, shared with the subject cache.
    pub(crate) admission: Arc<Admission>,
    /// Whether this reader may route by the admission's partition maps.
    maps_enabled: bool,
    indexes: Arc<Vec<lance_table::format::IndexMetadata>>,
    fallback_probes: usize,
    // Native v2 reconstruction allocates scratch per root. Subjects and workers
    // share these publication-bound roots; admissions are immutable and shared.
    pub(crate) vector_roots: Arc<tokio::sync::OnceCell<Vec<Arc<dyn VectorIndex>>>>,
    pub(crate) centroid_routers:
        Arc<tokio::sync::OnceCell<Vec<Result<CentroidRouter, RoutingFallback>>>>,
    pub(crate) mirror: Arc<MirrorStore>,
    pub(crate) mirror_pin: Arc<MirrorLease>,
    pub(crate) generation_state: GenerationState,
    pub(crate) generation_id: Arc<()>,
    pub(crate) native_session: Arc<NativeSession>,
    pub(crate) preparation_budget: crate::generation::MemoryPool,
    pub(crate) preparation_reservation: Arc<PreparationReservation>,
    pub(crate) reranker: Option<Arc<crate::rerank::Reranker>>,
    pub(crate) query_embeddings: Arc<embedding::QueryEmbeddings>,
    pub(crate) control_store: Arc<dyn object_store::ObjectStore>,
    pub(crate) control_backend: Arc<dyn object_store::ObjectStore>,
    pub(crate) control_root: Path,
    pub(crate) filter_selection: Option<FilterSelection>,
    pub(crate) response_claim: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    pub(crate) execution_slot: Option<Arc<crate::serve::ExecutionSlot>>,
    pub(crate) hidden_sources: BTreeSet<String>,
}

#[allow(clippy::cast_possible_truncation)] // Preserve native fragment-key bit extraction.
pub(crate) const fn coverage_fragment(id: u64) -> u32 {
    id as u32
}

pub(crate) fn column<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
    error: ReaderError,
) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref())
        .ok_or(error)
}
pub(crate) fn ids(batch: &RecordBatch) -> Result<&UInt64Array> {
    column(batch, "_rowid", IndexRowsInvalid)
}
pub(crate) fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    column(batch, name, PublicationRowsInvalid)
}

#[path = "reader/scope.rs"]
mod scope;

#[path = "reader/authorization.rs"]
mod authorization;
#[path = "reader/dense.rs"]
mod dense;
#[path = "reader/hydrate.rs"]
mod hydrate;
#[path = "reader/lexical.rs"]
mod lexical;
#[path = "reader/open.rs"]
mod open;
#[path = "reader/query.rs"]
mod query;
#[path = "reader/recall.rs"]
mod recall;
#[path = "reader/routing.rs"]
mod routing;

/// Index opens kept in flight at once. Each is a few small reads (footers and
/// lookup tables), and Lance bounds its own I/O below them.
const INDEX_OPEN_CONCURRENCY: usize = 16;

/// The index kinds every namespace carries, in the order they are verified.
const INDEX_KINDS: [(&str, &str, IndexType); 8] = [
    ("readers", "readers_idx", IndexType::LabelList),
    ("searchable_text", Layout::FTS_INDEX, IndexType::Inverted),
    ("source_uri", "source_uri_idx", IndexType::BTree),
    ("source_type", "source_type_idx", IndexType::Bitmap),
    ("project", "project_idx", IndexType::Bitmap),
    ("author", "author_idx", IndexType::Bitmap),
    ("updated_at", "updated_at_idx", IndexType::BTree),
    ("attributes", "attributes_idx", IndexType::LabelList),
];

/// Verify native index kinds and coverage without opening a query embedding cache.
pub async fn verify_indices(
    dataset: &Dataset,
    manifest: &ContentManifest,
    config: &Config,
) -> Result<Arc<Vec<lance_table::format::IndexMetadata>>, IndexVerificationError> {
    let indexes = dataset
        .load_indices()
        .await
        .map_err(|_| IndexVerificationError::StorageUnavailable)?;
    let fts_compatible = manifest.fts == config.namespace.fts_settings()
        && manifest.retrieval_policy_digest == config.namespace.retrieval_digest();
    let fragments: Vec<u32> = dataset
        .manifest()
        .fragments
        .iter()
        .map(|f| coverage_fragment(f.id))
        .collect();
    verify_loaded_indices(
        &indexes,
        &fragments,
        fts_compatible,
        |column, index| async move {
            dataset
                .open_generic_index(column, &index.uuid, &NoOpMetricsCollector)
                .await
                .map(|opened| opened.index_type())
                .map_err(|_| IndexVerificationError::StorageUnavailable)
        },
    )
    .await?;
    Ok(indexes)
}

/// The checks of `verify_indices` over loaded index metadata. Opening is the
/// only I/O: up to `INDEX_OPEN_CONCURRENCY` opens run at once, in the order the
/// checks consume them, and the checks run in the fixed kind order. The error
/// reported is therefore the one a strictly sequential pass reports: an open or
/// kind failure of an earlier index, or a coverage or compatibility failure of
/// an earlier kind, always wins over anything later.
async fn verify_loaded_indices<F, Fut>(
    indexes: &[lance_table::format::IndexMetadata],
    fragments: &[u32],
    fts_compatible: bool,
    open: F,
) -> Result<(), IndexVerificationError>
where
    F: Fn(&'static str, lance_table::format::IndexMetadata) -> Fut,
    Fut: std::future::Future<Output = Result<IndexType, IndexVerificationError>>,
{
    use futures::StreamExt;
    let mut queued = std::collections::VecDeque::new();
    for (column, name, _) in INDEX_KINDS {
        for index in indexes.iter().filter(|i| i.name == name) {
            queued.push_back((column, index.clone()));
        }
    }
    let mut opened = futures::stream::FuturesOrdered::new();
    for (column, name, kind) in INDEX_KINDS {
        let error = if column == "readers" {
            IndexVerificationError::ReadersIndexMissing
        } else {
            IndexVerificationError::IncompatibleIndex
        };
        if name == Layout::FTS_INDEX && !fts_compatible {
            return Err(IndexVerificationError::IncompatibleIndex);
        }
        for _ in indexes.iter().filter(|i| i.name == name) {
            while opened.len() < INDEX_OPEN_CONCURRENCY
                && let Some((column, index)) = queued.pop_front()
            {
                opened.push_back(open(column, index));
            }
            let Some(actual) = opened.next().await else {
                unreachable!("one open is queued for every index of every kind");
            };
            if actual? != kind {
                return Err(error);
            }
        }
        let coverage = coverage(indexes, name);
        if fragments
            .iter()
            .any(|fragment| !coverage.contains(fragment))
        {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "reader/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "reader/tests/fts_partition_tests.rs"]
mod fts_partition_tests;

#[cfg(test)]
#[path = "reader/tests/filter_selection_tests.rs"]
mod filter_selection_tests;

#[cfg(test)]
#[path = "reader/tests/routing_tests.rs"]
mod routing_tests;

#[cfg(test)]
#[path = "reader/tests/authorization_cache_tests.rs"]
mod authorization_cache_tests;

#[cfg(all(test, feature = "test-store"))]
#[path = "reader/tests/open_tests.rs"]
mod open_tests;
