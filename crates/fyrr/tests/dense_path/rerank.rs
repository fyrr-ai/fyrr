//! Fake HTTP reranker over the real executable and a real S3 publication.
use super::tools::body;
use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tools::Mcp;

#[derive(Default)]
struct State {
    requests: Mutex<Vec<Value>>,
    mode: AtomicUsize,
}
struct Provider {
    endpoint: String,
    state: Arc<State>,
    task: tokio::task::JoinHandle<()>,
}
impl Provider {
    async fn open() -> Self {
        let state = Arc::new(State::default());
        let app = axum::Router::new()
            .route("/rerank", axum::routing::post(provider))
            .with_state(state.clone());
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/rerank", socket.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(socket, app).await.unwrap();
        });
        Self {
            endpoint,
            state,
            task,
        }
    }
    fn calls(&self) -> usize {
        self.state.requests.lock().unwrap().len()
    }
}
impl Drop for Provider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn provider(
    axum::extract::State(state): axum::extract::State<Arc<State>>,
    headers: axum::http::HeaderMap,
    axum::Json(request): axum::Json<Value>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    assert!(
        headers
            .get(axum::http::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap()
            == format!("Bearer {}", std::env::var("FYRR_TEST_SECRET").unwrap()),
        "expected test bearer authorization"
    );
    let count = request["documents"].as_array().unwrap().len();
    state.requests.lock().unwrap().push(request);
    let mode = state.mode.load(Ordering::SeqCst);
    let rows: Vec<_> = (0..count)
        .rev()
        .map(|index| json!({"index":index,"relevance_score":index as f64}))
        .collect();
    if mode == 4 {
        axum::Json(json!({"results": rows})).into_response()
    } else {
        axum::Json(json!({"data": rows})).into_response()
    }
}

fn configured(
    config: &Path,
    provider: &Provider,
    weight: f64,
    cap: u64,
    deadline: &str,
) -> PathBuf {
    let text = std::fs::read_to_string(config)
        .unwrap()
        .replace("[reader]\n", "[reader]\nconcurrency=1\n");
    let path = config.with_file_name(format!("rerank-{weight}-{cap}-{deadline}.toml"));
    std::fs::write(&path, format!("{text}\n[namespace.rerank]\nendpoint='{}'\nmodel='fixture-rerank'\napi_key={{env='FYRR_TEST_SECRET'}}\ncandidates=10\nweight={weight}\ndeadline='{deadline}'\nspend_limit_microusd={cap}\nmax_call_cost_microusd=10\n", provider.endpoint)).unwrap();
    // Retrieval settings are publication policy, including optional reranking.
    success(&path, &["writer", "commit"]);
    path
}
fn query() -> Value {
    json!({"query":"exactneedle","limit":5,"output":"metadata"})
}
fn roots(body: &Value) -> Vec<String> {
    body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable and local storage; scripts/ci"]
async fn tools_rerank_text_window_and_response_budget_are_authorized_on_storage() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let configured = configured(&config, &provider, 1.0, 1000, "5s");
    let text = std::fs::read_to_string(&configured)
        .unwrap()
        .replace("[reader]\n", "[reader]\nmax_response_bytes=4096\n");
    std::fs::write(&configured, format!("{text}\ntext_chars=200\n")).unwrap();
    success(&configured, &["writer", "commit"]);
    let log = root.join("bounded-rerank.log");
    let mut reader = Mcp::open_logged(&configured, &log);
    for output in ["metadata", "excerpt", "content"] {
        let result = reader.call(
            "search",
            json!({
                "queries":["exactneedle","exactneedle","exactneedle"],
                "limit":10,"output":output
            }),
        );
        assert_eq!(result["isError"], false, "bounded search failed");
        assert!(serde_json::to_vec(&result).unwrap().len() <= 4096);
        let body = &result["structuredContent"];
        let counts = &body["truncated"];
        assert!(counts["hits_dropped"].as_u64().unwrap() > 0);
        assert!(counts["text_dropped"].as_u64().unwrap() <= 30);
        assert!(counts["hits_dropped"].as_u64().unwrap() <= 30);
        assert!(!result.to_string().contains("HIDDEN_LENGTH_AND_TEXT"));
        let text_copy: Value =
            serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(&text_copy, body);
    }
    assert!(provider.calls() > 0);
    for request in provider.state.requests.lock().unwrap().iter() {
        assert!(!request.to_string().contains("HIDDEN_LENGTH_AND_TEXT"));
        for text in request["documents"].as_array().unwrap() {
            assert!(text.as_str().unwrap().chars().count() <= 200);
        }
    }
    drop(reader);
    let log = std::fs::read_to_string(log).unwrap();
    for marker in ["exactneedle", "HIDDEN_LENGTH_AND_TEXT", "fixture document"] {
        assert!(!log.contains(marker));
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable and local storage; scripts/ci"]
async fn tools_rerank_default_and_success_on_storage() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let mut plain = Mcp::open(&config);
    let baseline = body(plain.call("search", query()));
    let again = body(plain.call("search", query()));
    assert_eq!(baseline["hits"], again["hits"]);
    assert!(baseline["metadata"]["telemetry"].get("rerank_us").is_none());
    assert_eq!(provider.calls(), 0);
    let extended = body(plain.call(
        "search",
        json!({"query":"exactneedle","limit":10,"output":"metadata"}),
    ));
    let log = root.join("rerank.log");
    let mut enabled = Mcp::open_logged(&configured(&config, &provider, 1.0, 1000, "5s"), &log);
    for mode in [0, 4] {
        provider.state.mode.store(mode, Ordering::SeqCst);
        let ranked = body(enabled.call("search", query()));
        assert_eq!(
            roots(&ranked),
            roots(&extended)
                .into_iter()
                .rev()
                .take(5)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            ranked["metadata"]["degraded"],
            baseline["metadata"]["degraded"]
        );
        assert!(
            ranked["metadata"]["telemetry"]["rerank_us"]
                .as_u64()
                .unwrap()
                > 0
        );
    }
    for request in provider.state.requests.lock().unwrap().iter() {
        assert_eq!(request.as_object().unwrap().len(), 3);
        assert_eq!(request["model"], "fixture-rerank");
        assert_eq!(request["query"], "exactneedle");
        assert!(!request.to_string().contains("HIDDEN_LENGTH_AND_TEXT"));
        assert_eq!(
            request["documents"].as_array().unwrap().len(),
            extended["hits"].as_array().unwrap().len()
        );
    }
    let mut zero = Mcp::open(&configured(&config, &provider, 0.0, 1000, "5s"));
    assert_eq!(body(zero.call("search", query()))["hits"], baseline["hits"]);
    drop(enabled);
    let log = std::fs::read_to_string(log).unwrap();
    for marker in [
        "exactneedle",
        "HIDDEN_LENGTH_AND_TEXT",
        "PRIVATE_PROVIDER_RESPONSE",
        "fixture document",
    ] {
        assert!(!log.contains(marker));
    }
    drop(zero);
    drop(plain);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable and local storage; scripts/ci"]
async fn tools_rerank_denied_and_listing_make_no_calls_on_storage() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let enabled = configured(&config, &provider, 1.0, 1000, "5s");
    let mut client = Mcp::open(&enabled);
    assert!(
        body(client.call("search", json!({"order":"updated_at","limit":5})))["hits"].is_array()
    );
    assert!(
        body(client.call(
            "search",
            json!({"query":"exactneedle","sources":["unknown"],"output":"metadata"})
        ))["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    drop(client);
    // Published identity owns the caller's grants; change document permissions
    // through the writer, rather than mutating a reader's local group list.
    let documents = root.join("documents.json");
    let mut input: Value = serde_json::from_slice(&std::fs::read(&documents).unwrap()).unwrap();
    for document in input["documents"].as_array_mut().unwrap() {
        document["readers"] = json!(["grp:deny"]);
        document["revision"] = json!("r2");
    }
    std::fs::write(documents, serde_json::to_vec(&input).unwrap()).unwrap();
    coordination::worker_sync(&enabled, &["--full"]);
    success(&enabled, &["writer", "commit"]);
    let mut client = Mcp::open(&enabled);
    assert!(
        body(client.call("search", query()))["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(provider.calls(), 0);
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable, gated provider connection and local storage; scripts/ci"]
async fn tools_rerank_revoke_before_body_emission_sends_no_text_on_storage() {
    use std::sync::atomic::AtomicBool;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    provider.endpoint = format!("http://{}/rerank", socket.local_addr().unwrap());
    let connected = Arc::new(AtomicBool::new(false));
    let accepted = Arc::new(tokio::sync::Notify::new());
    let read_body = Arc::new(tokio::sync::Semaphore::new(0));
    let receive = tokio::spawn({
        let connected = connected.clone();
        let accepted = accepted.clone();
        let read_body = read_body.clone();
        async move {
            let (mut socket, _) = socket.accept().await.unwrap();
            connected.store(true, Ordering::SeqCst);
            accepted.notify_one();
            // The connection is established, but the provider does not read yet.
            read_body.acquire().await.unwrap().forget();
            let _ = socket
                .write_all(
                    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await;
            let mut received = Vec::new();
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                socket.read_to_end(&mut received),
            )
            .await
            .unwrap()
            .unwrap();
            received
        }
    });
    let enabled = configured(&config, &provider, 1.0, 1000, "20s");
    let stop = Arc::new(AtomicBool::new(false));
    let resume = Arc::new(AtomicBool::new(false));
    let held = Arc::new(tokio::sync::Notify::new());
    let hook: corrections::BoundaryHook = Arc::new({
        let (connected, stop, resume, held) = (
            connected.clone(),
            stop.clone(),
            resume.clone(),
            held.clone(),
        );
        move |request, _, after| {
            if !after
                && request.starts_with("GET ")
                && request.contains("/head.json ")
                && connected.load(Ordering::SeqCst)
            {
                held.notify_one();
                while !resume.load(Ordering::SeqCst) && !stop.load(Ordering::SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
            false
        }
    });
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, mut guard) = corrections::proxy_at_boundary(endpoint.clone(), stop, hook);
    guard.resume = resume.clone(); // Also release held reads on assertion unwind.
    let proxied = root.join("body-disclosure.toml");
    std::fs::write(
        &proxied,
        std::fs::read_to_string(&enabled)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let mut client = Mcp::open(&proxied);
    let task = tokio::task::spawn_blocking(move || {
        let result = client.call("search", query());
        (client, result)
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), accepted.notified())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), held.notified())
        .await
        .unwrap();
    let path = root.join("documents.json");
    let mut input: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    for document in input["documents"].as_array_mut().unwrap() {
        document["readers"] = json!(["grp:deny"]);
        document["revision"] = json!("r2");
    }
    std::fs::write(path, serde_json::to_vec(&input).unwrap()).unwrap();
    coordination::worker_sync(&enabled, &["--full"]);
    success(&enabled, &["writer", "commit"]);
    resume.store(true, Ordering::SeqCst);
    read_body.add_permits(1);
    let received = receive.await.unwrap();
    assert!(
        !String::from_utf8_lossy(&received).contains("fixture document"),
        "revoked document text must not leave the reader"
    );
    assert!(
        !String::from_utf8_lossy(&received).contains("exactneedle"),
        "the rejected payload emits no query either"
    );
    let (client, result) = task.await.unwrap();
    assert_eq!(
        result["structuredContent"]["code"],
        "authorization_unavailable"
    );
    drop(client);
    drop(guard);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local storage, MCP stdio and in-request admission rebuilding"]
async fn tools_rerank_refresh_preserves_rebuilt_admission_on_storage() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let config = configured(&config, &provider, 1.0, 1000, "5s");
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    let output = Command::new(binary)
        .args([
            "serve::tests::embedding::rerank_refresh_preserves_rebuilt_admission_on_storage",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("FYRR_TEST_EMBED_CONFIG", &config)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    assert_eq!(
        provider.calls(),
        2,
        "only the two authorized controls reach the reranker"
    );
    std::fs::remove_dir_all(root).unwrap();
}
