//! Fake HTTP reranker over the real executable and a real S3 publication.
use super::tools::body;
use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tools::Mcp;

struct State {
    requests: Mutex<Vec<Value>>,
    mode: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    started: tokio::sync::Notify,
    body_started: tokio::sync::Notify,
    gate: tokio::sync::Semaphore,
}
impl Default for State {
    fn default() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            mode: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            started: tokio::sync::Notify::new(),
            body_started: tokio::sync::Notify::new(),
            gate: tokio::sync::Semaphore::new(0),
        }
    }
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
    async fn wait_calls(&self, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let notified = self.state.started.notified();
                if self.calls() >= count {
                    break;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
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
            == format!(
                "Bearer {}",
                std::env::var("FYRR_TEST_S3_SECRET_ACCESS_KEY").unwrap()
            ),
        "expected test bearer authorization"
    );
    let count = request["documents"].as_array().unwrap().len();
    let forward = request["query"].as_str().unwrap().ends_with('0');
    state.requests.lock().unwrap().push(request);
    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
    state.peak.fetch_max(active, Ordering::SeqCst);
    state.started.notify_one();
    let mode = state.mode.load(Ordering::SeqCst);
    if mode == 3 {
        state.gate.acquire().await.unwrap().forget();
    }
    state.active.fetch_sub(1, Ordering::SeqCst);
    let rows: Vec<_> = (0..count)
        .rev()
        .map(|index| json!({"index":index,"relevance_score":if mode == 8 && forward { (count-index) as f64 } else { index as f64 }}))
        .collect();
    match mode {
        1 => (
            axum::http::StatusCode::BAD_GATEWAY,
            "PRIVATE_PROVIDER_RESPONSE",
        )
            .into_response(),
        2 => axum::Json(json!({"results":[{"index":count,"relevance_score":0.5}]})).into_response(),
        4 => axum::Json(json!({"results":rows})).into_response(),
        5 => (
            axum::http::StatusCode::TEMPORARY_REDIRECT,
            [(axum::http::header::LOCATION, "/rerank")],
        )
            .into_response(),
        6 => axum::Json(json!({"data":rows,"padding":"PRIVATE_PROVIDER_RESPONSE".repeat(4000)}))
            .into_response(),
        7 => {
            let first = futures::stream::once(async move {
                state.body_started.notify_one();
                Ok::<_, std::io::Error>("{")
            });
            let stalled = futures::StreamExt::chain(first, futures::stream::pending());
            axum::response::Response::builder()
                .header("Content-Type", "application/json")
                .body(axum::body::Body::from_stream(stalled))
                .unwrap()
        }
        _ => axum::Json(json!({"data":rows})).into_response(),
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
    std::fs::write(&path, format!("{text}\n[namespace.rerank]\nendpoint='{}'\nmodel='fixture-rerank'\napi_key={{env='FYRR_TEST_S3_SECRET_ACCESS_KEY'}}\ncandidates=10\nweight={weight}\ndeadline='{deadline}'\nspend_limit_microusd={cap}\nmax_call_cost_microusd=10\n", provider.endpoint)).unwrap();
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
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_text_window_and_response_budget_are_authorized_on_real_s3() {
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
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_default_and_success_on_real_s3() {
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
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_failures_and_shared_deadline_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let baseline = body(Mcp::open(&config).call("search", query()));
    let batch = json!({"queries":vec!["exactneedle";16],"limit":5,"output":"metadata"});
    let baseline_batch = body(Mcp::open(&config).call("search", batch.clone()));
    let log = root.join("rerank-failures.log");
    let mut client = Mcp::open_logged(&configured(&config, &provider, 1.0, 1000, "5s"), &log);
    for mode in [1, 2, 5, 6] {
        provider.state.mode.store(mode, Ordering::SeqCst);
        let before = provider.calls();
        let result = body(client.call("search", query()));
        assert_eq!(result["hits"], baseline["hits"]);
        assert!(
            result["metadata"]["degraded"]
                .as_array()
                .unwrap()
                .contains(&json!("rerank"))
        );
        assert_eq!(provider.calls(), before + 1, "no retry or redirect");
    }
    provider.state.mode.store(3, Ordering::SeqCst);
    let timed = configured(&config, &provider, 1.0, 1000, "200ms");
    let mut timed_client = Mcp::open(&timed);
    let before = provider.calls();
    let result = body(timed_client.call("search", batch));
    assert_eq!(result["hits"], baseline_batch["hits"]);
    assert!(
        result["metadata"]["degraded"]
            .as_array()
            .unwrap()
            .contains(&json!("rerank"))
    );
    assert!(
        provider.calls() <= before + 4,
        "queued calls cannot reset the deadline"
    );
    assert!(
        result["metadata"]["telemetry"]["rerank_us"]
            .as_u64()
            .unwrap()
            < 1_000_000
    );
    drop(client);
    drop(timed_client);
    let log = std::fs::read_to_string(log).unwrap();
    assert!(!log.contains("PRIVATE_PROVIDER_RESPONSE"));
    assert!(!log.contains("exactneedle"));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_denied_and_listing_make_no_calls_on_real_s3() {
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
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_spend_is_shared_between_queries_and_requests_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let queries: Vec<_> = (0..4)
        .map(|i| format!("exactneedle fixture document {i}"))
        .collect();
    let batch = json!({"queries":queries,"limit":5,"output":"metadata"});
    let baseline = body(Mcp::open(&config).call("search", batch.clone()));
    let mut client = Mcp::open(&configured(&config, &provider, 1.0, 20, "5s"));
    let first = body(client.call("search", batch.clone()));
    assert_eq!(provider.calls(), 2);
    assert_eq!(
        provider
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r["query"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "exactneedle fixture document 0".to_owned(),
            "exactneedle fixture document 1".to_owned()
        ]
        .into_iter()
        .collect()
    );
    assert!(
        first["metadata"]["degraded"]
            .as_array()
            .unwrap()
            .contains(&json!("rerank"))
    );
    let second = body(client.call("search", batch));
    assert_eq!(provider.calls(), 2);
    assert_eq!(second["hits"], baseline["hits"]);
    assert!(
        second["metadata"]["degraded"]
            .as_array()
            .unwrap()
            .contains(&json!("rerank"))
    );
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_batch_has_four_calls_in_flight_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    provider.state.mode.store(3, Ordering::SeqCst);
    let mut client = Mcp::open(&configured(&config, &provider, 1.0, 1000, "10s"));
    client.send(json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"search","arguments":{"queries":vec!["exactneedle";8],"limit":5,"text_hits":2}}}));
    provider.wait_calls(4).await;
    client.send(json!({"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"search","arguments":{"order":"updated_at","limit":1}}}));
    let (response_sent, mut response_received) = tokio::sync::oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        let first = client.read();
        let _ = response_sent.send(());
        let second = client.read();
        (client, first, second)
    });
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            &mut response_received
        )
        .await
        .is_err(),
        "both requests wait while the reranker holds the sole worker"
    );
    assert_eq!(provider.calls(), 4);
    assert_eq!(provider.state.peak.load(Ordering::SeqCst), 4);
    provider.state.gate.add_permits(4);
    provider.wait_calls(8).await;
    assert_eq!(provider.state.peak.load(Ordering::SeqCst), 4);
    provider.state.gate.add_permits(4);
    let (client, first, second) = task.await.unwrap();
    let replies: std::collections::BTreeMap<_, _> = [first, second]
        .into_iter()
        .map(|reply| (reply["id"].as_u64().unwrap(), reply["result"].clone()))
        .collect();
    let result = body(replies[&7].clone());
    let listing = body(replies[&8].clone());
    assert!(
        listing["metadata"]["telemetry"]["queue_wait_ms"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        !result["metadata"]["degraded"]
            .as_array()
            .unwrap()
            .contains(&json!("rerank"))
    );
    assert_eq!(provider.calls(), 8);
    assert_eq!(roots(&result).len(), 5, "batch dedup follows reranking");
    assert_eq!(
        result["hits"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|hit| !hit["excerpt"].is_null())
            .count(),
        2
    );
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_revoke_cancels_queued_provider_calls_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    provider.state.mode.store(3, Ordering::SeqCst);
    let enabled = configured(&config, &provider, 1.0, 1000, "20s");
    let mut client = Mcp::open(&enabled);
    let task = tokio::task::spawn_blocking(move || {
        let result = client.call("search", json!({"queries":vec!["exactneedle";8],"limit":5,"output":"metadata","deadline_ms":30000}));
        (client, result)
    });
    provider.wait_calls(4).await;
    let path = root.join("documents.json");
    let mut input: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    for document in input["documents"].as_array_mut().unwrap() {
        document["readers"] = json!(["grp:deny"]);
        document["revision"] = json!("r2");
    }
    std::fs::write(path, serde_json::to_vec(&input).unwrap()).unwrap();
    coordination::worker_sync(&enabled, &["--full"]);
    success(&enabled, &["writer", "commit"]);
    provider.state.gate.add_permits(4);
    let (client, result) = task.await.unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(
        result["structuredContent"]["code"],
        "authorization_unavailable"
    );
    assert_eq!(
        provider.calls(),
        4,
        "revoked text is not sent by queued calls"
    );
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_stalled_response_body_releases_worker_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let baseline = body(Mcp::open(&config).call("search", query()));
    provider.state.mode.store(7, Ordering::SeqCst);
    let mut client = Mcp::open(&configured(&config, &provider, 1.0, 1000, "250ms"));
    client.send(json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"search","arguments":query()}}));
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        provider.state.body_started.notified(),
    )
    .await
    .unwrap();
    client.send(json!({"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"search","arguments":{"order":"updated_at","limit":1}}}));
    let task = tokio::task::spawn_blocking(move || {
        let replies = [client.read(), client.read()];
        (client, replies)
    });
    let (client, replies) = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    let replies: std::collections::BTreeMap<_, _> = replies
        .into_iter()
        .map(|reply| (reply["id"].as_u64().unwrap(), body(reply["result"].clone())))
        .collect();
    assert_eq!(replies[&7]["hits"], baseline["hits"]);
    assert!(
        replies[&7]["metadata"]["degraded"]
            .as_array()
            .unwrap()
            .contains(&json!("rerank"))
    );
    assert!(
        replies[&7]["metadata"]["telemetry"]["rerank_us"]
            .as_u64()
            .unwrap()
            < 1_000_000
    );
    assert!(
        replies[&8]["metadata"]["telemetry"]["queue_wait_ms"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(provider.calls(), 1);
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_distinct_queries_keep_their_own_indices_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let queries = [
        "exactneedle fixture document 0",
        "exactneedle fixture document 1",
    ];
    let mut plain = Mcp::open(&config);
    let fused: Vec<_> = queries
        .iter()
        .map(|query| {
            body(plain.call(
                "search",
                json!({"query":query,"limit":10,"output":"metadata"}),
            ))
        })
        .collect();
    drop(plain);
    provider.state.mode.store(8, Ordering::SeqCst);
    let mut client = Mcp::open(&configured(&config, &provider, 1.0, 1000, "5s"));
    let result = body(client.call(
        "search",
        json!({"queries":queries,"limit":5,"output":"metadata"}),
    ));
    let expected = [
        roots(&fused[0]).into_iter().take(5).collect::<Vec<_>>(),
        roots(&fused[1])
            .into_iter()
            .rev()
            .take(5)
            .collect::<Vec<_>>(),
    ];
    let mut seen = std::collections::BTreeSet::new();
    let mut interleaved = Vec::new();
    for rank in 0..5 {
        for (index, rows) in expected.iter().enumerate() {
            if let Some(root) = rows.get(rank)
                && seen.insert(root.clone())
            {
                interleaved.push((root.clone(), index as u64));
            }
        }
    }
    assert_eq!(
        result["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| (
                hit["root_id"].as_str().unwrap().to_owned(),
                hit["query_index"].as_u64().unwrap()
            ))
            .collect::<Vec<_>>(),
        interleaved
    );
    assert_eq!(provider.calls(), 2);
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_rerank_unconfigured_publication_is_stale_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let enabled = configured(&config, &provider, 1.0, 1000, "5s");
    // Publish with the original digest, then start a differently configured reader.
    success(&config, &["writer", "commit"]);
    let refused = run(&enabled, &["reader", "check"]);
    assert!(!refused.status.success());
    assert!(
        String::from_utf8(refused.stderr)
            .unwrap()
            .contains("incompatible_index")
    );
    assert_eq!(provider.calls(), 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable, gated provider connection and RustFS; scripts/ci"]
async fn tools_rerank_revoke_before_body_emission_sends_no_text_on_real_s3() {
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
#[ignore = "two HTTP MCP clients, fake reranker and real RustFS; scripts/ci"]
async fn tools_rerank_two_clients_share_spend_and_cancellation_does_not_refund_on_real_s3() {
    let provider = Provider::open().await;
    let (_, issuer_root) = setup();
    let issuer = http_mcp::Issuer::new(&issuer_root).await;
    let (worker, _, root, _) = http_mcp::fixture_for_issuer(&issuer.url).await;
    // The committer fixture deliberately has no worker replay/cache files.
    let enabled = configured(&worker, &provider, 1.0, 10, "20s");
    std::fs::write(
        &enabled,
        std::fs::read_to_string(&enabled)
            .unwrap()
            .replace("concurrency=1", "concurrency=2"),
    )
    .unwrap();
    let mut reader =
        http_mcp::Reader::start_configured(&enabled, &root, &issuer, 100, 100, 32, false, true)
            .await;
    provider.state.mode.store(3, Ordering::SeqCst);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let tokens: Vec<_> = (0..2)
        .map(|index| issuer.token(&format!("subject-{index}"), json!({}), json!({})))
        .collect();
    let clients = [reqwest::Client::new(), reqwest::Client::new()];
    let url = format!("{}/mcp", reader.base);
    let mut tasks: Vec<_> = (0..2).map(|index| {
        let (barrier, client, url, token) = (barrier.clone(), clients[index].clone(), url.clone(), tokens[index].clone());
        tokio::spawn(async move {
            barrier.wait().await;
            let response = client.post(url).bearer_auth(token)
                .header("Accept", "application/json, text/event-stream")
                .header("MCP-Protocol-Version", "2025-11-25")
                .json(&json!({"jsonrpc":"2.0","id":101+index,"method":"tools/call","params":{"name":"search","arguments":{"query":format!("fixture document {index}"),"limit":5,"output":"metadata","deadline_ms":30000}}}))
                .send().await.unwrap();
            assert_eq!(response.status(), 200);
            response.json::<Value>().await.unwrap()["result"].clone()
        })
    }).collect();
    barrier.wait().await;
    provider.wait_calls(1).await;
    let winning_query = provider.state.requests.lock().unwrap()[0]["query"]
        .as_str()
        .unwrap()
        .to_owned();
    let winner = usize::from(winning_query.ends_with('1'));
    let loser = 1 - winner;
    let losing_task = tasks.swap_remove(loser);
    let losing_result = tokio::time::timeout(std::time::Duration::from_secs(10), losing_task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        body(losing_result)["metadata"]["degraded"]
            .as_array()
            .unwrap()
            .contains(&json!("rerank"))
    );
    let notification = clients[winner].post(&url).bearer_auth(&tokens[winner])
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":101+winner}}))
        .send().await.unwrap();
    assert_eq!(notification.status(), 202);
    let cancelled = tokio::time::timeout(std::time::Duration::from_secs(5), tasks.pop().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled["structuredContent"]["code"], "cancelled");
    provider.state.mode.store(0, Ordering::SeqCst);
    let next = clients[winner].post(&url).bearer_auth(&tokens[winner])
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .json(&json!({"jsonrpc":"2.0","id":103,"method":"tools/call","params":{"name":"search","arguments":{"query":winning_query,"limit":5,"output":"metadata"}}}))
        .send().await.unwrap().json::<Value>().await.unwrap();
    assert!(
        body(next["result"].clone())["metadata"]["degraded"]
            .as_array()
            .unwrap()
            .contains(&json!("rerank"))
    );
    assert_eq!(
        provider.calls(),
        1,
        "cancellation after dispatch never refunds the reservation"
    );
    provider.state.gate.add_permits(1);
    reader.stop().await;
    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(issuer_root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3, MCP stdio and in-request admission rebuilding"]
async fn tools_rerank_refresh_preserves_rebuilt_admission_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let config = configured(&config, &provider, 1.0, 1000, "5s");
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    let output = Command::new(binary)
        .args([
            "serve::tests::embedding::rerank_refresh_preserves_rebuilt_admission_on_real_s3",
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
