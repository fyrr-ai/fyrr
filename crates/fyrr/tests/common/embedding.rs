//! Owned loopback embedding service. Inputs are keyed by role and exact UTF-8 bytes.
use axum::response::IntoResponse;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug, Default)]
pub struct State {
    pub models: AtomicUsize,
    pub requests: Mutex<Vec<Value>>,
    pub mode: AtomicUsize,
    pub fail_after: AtomicUsize,
    vectors: Mutex<BTreeMap<(String, String), Vec<f32>>>,
    rejected: Mutex<BTreeSet<String>>,
}

#[derive(Debug)]
pub struct Provider {
    pub endpoint: String,
    pub state: Arc<State>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Provider {
    pub async fn open() -> Self {
        Self::start("BAAI/bge-m3", false)
    }

    pub fn start(model: &str, registered_only: bool) -> Self {
        let state = Arc::new(State::default());
        let shared = state.clone();
        let model = model.to_owned();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/v1", socket.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let app = axum::Router::new()
                    .route("/v1/embeddings/models", axum::routing::get(models))
                    .route("/v1/embeddings", axum::routing::post(embeddings))
                    .with_state(Arc::new(Service {
                        state: shared,
                        model,
                        registered_only,
                    }));
                let socket = tokio::net::TcpListener::from_std(socket).unwrap();
                tokio::select! {
                    result = axum::serve(socket, app).into_future() => result.unwrap(),
                    _ = stopped => {},
                }
            });
        });
        Self {
            endpoint,
            state,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    pub fn calls(&self) -> usize {
        self.state.models.load(Ordering::SeqCst) + self.state.requests.lock().unwrap().len()
    }

    pub fn reject(&self, text: &str) {
        self.state.rejected.lock().unwrap().insert(text.into());
    }

    pub fn insert(&self, role: &str, text: &str, vector: Vec<f32>) {
        self.state
            .vectors
            .lock()
            .unwrap()
            .insert((role.into(), text.into()), vector);
    }

    pub fn insert_both(&self, text: &str, vector: Vec<f32>) {
        for role in ["", "document", "query"] {
            self.insert(role, text, vector.clone());
        }
    }

    pub fn vector(&self, text: &str) -> Vec<f32> {
        self.state.vectors.lock().unwrap()[&(String::new(), text.into())].clone()
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.thread.take().unwrap().join().unwrap();
    }
}

struct Service {
    state: Arc<State>,
    model: String,
    registered_only: bool,
}

/// The fixture's configured credential: the S3 fixture's or, for local
/// storage, `FYRR_TEST_LOCAL_SECRET`.
fn bearer(headers: &axum::http::HeaderMap) {
    let sent = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok());
    assert!(
        ["FYRR_TEST_S3_SECRET_ACCESS_KEY", "FYRR_TEST_LOCAL_SECRET"]
            .into_iter()
            .filter_map(|variable| std::env::var(variable).ok())
            .any(|secret| sent == Some(format!("Bearer {secret}").as_str())),
        "embedding credential mismatch"
    );
}

async fn models(
    axum::extract::State(service): axum::extract::State<Arc<Service>>,
    headers: axum::http::HeaderMap,
) -> axum::Json<Value> {
    bearer(&headers);
    service.state.models.fetch_add(1, Ordering::SeqCst);
    let model = if service.state.mode.load(Ordering::SeqCst) == 1 {
        "other-model"
    } else {
        &service.model
    };
    axum::Json(json!({"data":[{"id":model}]}))
}

async fn embeddings(
    axum::extract::State(service): axum::extract::State<Arc<Service>>,
    headers: axum::http::HeaderMap,
    axum::Json(request): axum::Json<Value>,
) -> axum::response::Response {
    bearer(&headers);
    assert_eq!(request["model"], service.model);
    let inputs = request["input"].as_array().unwrap();
    let role = request
        .get("input_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(matches!(role, "" | "document" | "query"));
    let requested_dimension = request["dimensions"].as_u64().unwrap_or(1024) as usize;
    service.state.requests.lock().unwrap().push(request.clone());
    let fail_after = service.state.fail_after.load(Ordering::SeqCst);
    let mode = service.state.mode.load(Ordering::SeqCst);
    if mode == 3 || (fail_after > 0 && service.state.requests.lock().unwrap().len() > fail_after) {
        return (
            axum::http::StatusCode::BAD_GATEWAY,
            "PRIVATE_EMBEDDING_RESPONSE",
        )
            .into_response();
    }
    if mode == 4 {
        return axum::response::Response::builder()
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from_stream(futures::stream::pending::<
                Result<String, std::io::Error>,
            >()))
            .unwrap();
    }
    let dimension = if mode == 2 { 512 } else { requested_dimension };
    let mut rows = Vec::new();
    for index in (0..inputs.len()).rev() {
        let text = inputs[index].as_str().unwrap();
        if service.state.rejected.lock().unwrap().contains(text) {
            return (
                axum::http::StatusCode::BAD_GATEWAY,
                "fixture_input_rejected",
            )
                .into_response();
        }
        let registered = service
            .state
            .vectors
            .lock()
            .unwrap()
            .get(&(role.into(), text.into()))
            .cloned();
        let embedding = match registered {
            Some(vector) => vector,
            None if service.registered_only => {
                return (
                    axum::http::StatusCode::BAD_GATEWAY,
                    "fixture_input_unregistered",
                )
                    .into_response();
            }
            None if requested_dimension == 512 => {
                let hash = fyrr_reader::embedding::digest(text.as_bytes());
                let mut seed = u32::from_str_radix(&hash[7..15], 16).unwrap();
                (0..dimension)
                    .map(|_| {
                        seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
                        (seed >> 8) as f32 / 16777216.0 * 2.0 - 1.0
                    })
                    .collect()
            }
            None => (0..dimension).map(|d| ((d + 7) as f32).sin()).collect(),
        };
        rows.push(json!({"index":index,"embedding":embedding}));
    }
    axum::Json(json!({"model":service.model,"data":rows})).into_response()
}

/// The root owns the service so child CLIs can use it until the test releases its fixture.
#[derive(Debug)]
pub struct FixtureRoot {
    path: PathBuf,
    pub embedding: Provider,
}
impl FixtureRoot {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            embedding: Provider::start("BAAI/bge-m3", true),
        }
    }
}
impl std::ops::Deref for FixtureRoot {
    type Target = Path;
    fn deref(&self) -> &Self::Target {
        &self.path
    }
}
impl AsRef<Path> for FixtureRoot {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

/// Replace only the embedding section, preserving the fixture's other configuration.
pub fn configure(text: &str, settings: &str) -> String {
    let (before, embedding) = text.split_once("[embedding]\n").unwrap();
    let (_, after) = embedding.split_once("\n[").unwrap();
    format!("{before}[embedding]\n{settings}\n[{after}")
}
