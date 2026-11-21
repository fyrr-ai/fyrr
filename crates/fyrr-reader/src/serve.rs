//! MCP SDK transports over one bounded, transport-independent request scheduler.
use crate::error::*;
use crate::search::PreparedReader;
use crate::telemetry::{emit, micros};
use futures::{StreamExt, stream::FuturesUnordered};
use fyrr_core::mcp::{FetchInput, ListFiltersInput, SearchInput, ToolError};
use lance_io::object_store::WrappingObjectStore;
use rmcp::{RoleServer, ServerHandler, ServiceExt, model::*, service::RequestContext};
use serde_json::{Value, json};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

type VersionPreparation =
    tokio::task::JoinHandle<Result<Option<(PreparedReader, VecDeque<PreparedReader>)>>>;

#[derive(Debug)]
struct ExecutionState {
    held: Option<tokio::sync::OwnedSemaphorePermit>,
    provider_waiting: bool,
    lexical_finished: bool,
}
/// One request's native turn. The reader and its content pin remain owned by
/// the executing future while only this permit is released for provider wait.
#[derive(Debug)]
pub(crate) struct ExecutionSlot {
    pool: Arc<tokio::sync::Semaphore>,
    state: std::sync::Mutex<ExecutionState>,
}
impl ExecutionSlot {
    fn new(
        pool: Arc<tokio::sync::Semaphore>,
        held: tokio::sync::OwnedSemaphorePermit,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool,
            state: std::sync::Mutex::new(ExecutionState {
                held: Some(held),
                provider_waiting: false,
                lexical_finished: false,
            }),
        })
    }
    pub(crate) fn begin_provider_wait(&self) {
        let mut state = crate::sync::lock(&self.state);
        state.provider_waiting = true;
        if state.lexical_finished {
            state.held.take();
        }
    }
    pub(crate) fn lexical_finished(&self) {
        let mut state = crate::sync::lock(&self.state);
        state.lexical_finished = true;
        if state.provider_waiting {
            state.held.take();
        }
    }
    pub(crate) fn held(&self) -> bool {
        crate::sync::lock(&self.state).held.is_some()
    }
    pub(crate) fn try_resume(&self) -> bool {
        let mut state = crate::sync::lock(&self.state);
        state.provider_waiting = false;
        if state.held.is_some() {
            return true;
        }
        if let Ok(permit) = self.pool.clone().try_acquire_owned() {
            state.held = Some(permit);
            return true;
        }
        false
    }
    pub(crate) async fn resume(&self) -> Result<()> {
        let acquire = {
            let mut state = crate::sync::lock(&self.state);
            state.provider_waiting = false;
            state.held.is_none()
        };
        if acquire {
            let permit = self.pool.clone().acquire_owned().await.map_err(|_| Busy)?;
            crate::sync::lock(&self.state).held = Some(permit);
        }
        Ok(())
    }
}
/// Shared local resources, without ownership of a serving generation's claim.
#[derive(Clone)]
struct PreparationContext {
    reranker: Option<Arc<crate::rerank::Reranker>>,
    #[cfg(test)]
    test_hooks: Arc<TestHooks>,
    config: fyrr_core::config::Config,
    mirror: Arc<crate::mirror::MirrorStore>,
    query_embeddings: Arc<crate::embedding::QueryEmbeddings>,
    cache: Arc<std::sync::Mutex<crate::search::AuthorizationCache>>,
    budget: crate::generation::MemoryPool,
    control_backend: Arc<dyn object_store::ObjectStore>,
    control_root: object_store::path::Path,
    policy_cache: crate::storage::ArtifactCache<fyrr_core::publication::PolicyArtifact>,
    identity_cache: crate::storage::ArtifactCache<fyrr_core::identity::IdentityArtifact>,
    head_cache: crate::storage::HeadCache,
}
struct RequestTiming {
    arrived: Instant,
    deadline: Instant,
    cancelled: CancellationToken,
    version_wait_until: Instant,
    authorization_build_us: u64,
}
/// Reserve stdio capacity at decoded arrival, before SDK task scheduling can reorder it.
#[derive(Clone)]
struct StdioAdmissionSlot {
    permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    cancellation: CancellationToken,
}
type CancellationRegistry =
    Arc<std::sync::Mutex<std::collections::HashMap<(String, RequestId), CancellationToken>>>;
/// Remove completed/disconnected requests without retaining identity-scoped cancellation state.
struct CancellationRegistration {
    key: (String, RequestId),
    registry: CancellationRegistry,
    token: CancellationToken,
}
impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        self.token.cancel();
        if let Ok(mut registry) = self.registry.lock() {
            registry.remove(&self.key);
        }
    }
}
/// Account a response until the transport sends it or drops the disconnected body.
#[derive(Default)]
pub(crate) struct ResponseClaims {
    admission: std::sync::Mutex<Option<Arc<tokio::sync::OwnedSemaphorePermit>>>,
    scratch: std::sync::Mutex<Option<Arc<tokio::sync::OwnedSemaphorePermit>>>,
}
type StdioResponseClaims =
    Arc<std::sync::Mutex<std::collections::HashMap<RequestId, Arc<ResponseClaims>>>>;

struct Job {
    request: ToolCall,
    reply: oneshot::Sender<CallToolResult>,
    timing: RequestTiming,
    subject: Option<crate::auth::AuthenticatedSubject>,
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    response_claims: Arc<ResponseClaims>,
    recall_query_index: usize,
}
#[derive(Clone)]
pub(crate) struct McpService {
    #[cfg(test)]
    sample_events: tokio::sync::broadcast::Sender<Value>,
    #[cfg(test)]
    native_slots: Arc<tokio::sync::Semaphore>,
    http: bool,
    instructions: Option<String>,
    sender: mpsc::Sender<Job>,
    cancellations: CancellationRegistry,
    slots: Arc<tokio::sync::Semaphore>,
    reload: mpsc::Sender<oneshot::Sender<Result<Value>>>,
    publication: Arc<tokio::sync::RwLock<crate::readiness::Readiness>>,
}

// These gates observe the real scheduler and native scan, without product knobs.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestGate {
    pub(crate) enabled: std::sync::atomic::AtomicBool,
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Semaphore,
}
#[cfg(test)]
impl Default for TestGate {
    fn default() -> Self {
        Self {
            enabled: false.into(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}
#[cfg(test)]
impl TestGate {
    pub(crate) async fn wait(&self) {
        if self.enabled.load(std::sync::atomic::Ordering::SeqCst) {
            self.hold().await;
        }
    }
    pub(crate) async fn wait_once(&self) {
        if self
            .enabled
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.hold().await;
        }
    }
    async fn hold(&self) {
        self.entered.notify_one();
        self.release
            .acquire()
            .await
            .expect("test gate open")
            .forget();
    }
}
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct TestHooks {
    pub(crate) scheduler_failed: tokio::sync::Notify,
    pub(crate) embedding_parked: tokio::sync::Notify,
    pub(crate) request_finished: tokio::sync::Notify,
    pub(crate) native_work: TestGate,
    pub(crate) sample_plan: std::sync::atomic::AtomicBool,
    pub(crate) completion: TestGate,
    pub(crate) authorization_output: TestGate,
    pub(crate) admission_scan: TestGate,
    pub(crate) filter_scan: TestGate,
    pub(crate) admission_probe: std::sync::Mutex<Option<AdmissionProbe>>,
    pub(crate) cache_warming: TestGate,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct AdmissionProbe {
    row: u64,
    observations: Vec<AdmissionObservation>,
}
#[cfg(test)]
#[derive(Debug)]
struct AdmissionObservation {
    before_cleanup: bool,
    worker: usize,
    head: fyrr_core::publication::ServingHead,
    selected: bool,
}
#[cfg(test)]
impl TestHooks {
    pub(crate) fn observe_admission(&self, reader: &PreparedReader, before_cleanup: bool) {
        if let Some(probe) = crate::sync::lock(&self.admission_probe).as_mut() {
            probe.observations.push(AdmissionObservation {
                before_cleanup,
                worker: Arc::as_ptr(&reader.mirror) as usize,
                head: reader.head.clone(),
                selected: reader.admission.eligible.selected(probe.row),
            });
        }
    }
}

async fn bounded<T>(
    timing: &RequestTiming,
    work: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! { biased;
        () = timing.cancelled.cancelled() => Err(Cancelled),
        () = tokio::time::sleep_until(timing.deadline) => Err(DeadlineExceeded),
        result = work => result,
    }
}
async fn settle<T>(reader: &mut PreparedReader, result: Result<T>) -> Result<T> {
    if matches!(result, Err(Cancelled | DeadlineExceeded)) {
        // A parked request must not queue behind native work merely to cancel.
        // interrupted refuses changed admission without rebuilding it if no
        // native permit is held; the next request performs its normal binding.
        let cleanup = reader.interrupted();
        match tokio::time::timeout(Duration::from_secs(1), cleanup).await {
            Ok(Ok(false)) => result,
            _ => Err(AuthorizationUnavailable),
        }
    } else {
        result
    }
}

#[cfg(test)]
pub(crate) async fn cache_test_enqueue(
    service: &McpService,
    request: ToolCall,
    subject: crate::auth::AuthenticatedSubject,
) -> oneshot::Receiver<CallToolResult> {
    let permit = Arc::new(service.slots.clone().try_acquire_owned().unwrap());
    let (reply, receiver) = oneshot::channel();
    let now = Instant::now();
    service
        .sender
        .send(Job {
            request,
            reply,
            subject: Some(subject),
            _permit: permit,
            response_claims: Arc::default(),
            recall_query_index: 0,
            timing: RequestTiming {
                arrived: now,
                deadline: now + Duration::from_secs(300),
                cancelled: CancellationToken::new(),
                version_wait_until: now,
                authorization_build_us: 0,
            },
        })
        .await
        .unwrap();
    receiver
}

#[cfg(test)]
#[path = "serve/tests.rs"]
mod tests;

mod mcp;
mod preparation;
mod recall;
mod scheduler;
mod stdio;
pub(crate) use mcp::ToolCall;
use mcp::{execute, tool_error};
use preparation::*;
pub(crate) use recall::SampleSkip;
use recall::{SampleResult, SampleWork, recall_telemetry};
pub(crate) use scheduler::start;
pub(crate) use stdio::serve;

/// The reply an expired subject's tool call gets in place of its result, as
/// the JSON-RPC response the transport sends.
pub(crate) fn expired_reply(id: &RequestId) -> Value {
    let mut result = tool_error(AuthorizationUnavailable);
    result.result_type = None;
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

#[cfg(test)]
use stdio::StdioTransport;
