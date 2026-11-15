use super::*;
use crate::embedding::EmbeddingError;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use fyrr_core::config::SpendConfig;
use std::sync::Mutex;

struct Fake {
    case: AtomicU64,
    no_catalog: std::sync::atomic::AtomicBool,
    posts: Mutex<Vec<serde_json::Value>>,
    probes: AtomicU64,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
impl Default for Fake {
    fn default() -> Self {
        Self {
            case: AtomicU64::new(0),
            no_catalog: false.into(),
            posts: Mutex::default(),
            probes: AtomicU64::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}
struct Server {
    fake: Arc<Fake>,
    task: tokio::task::JoinHandle<()>,
    endpoint: String,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server() -> Server {
    async fn embeddings(
        State(fake): State<Arc<Fake>>,
        Json(body): Json<serde_json::Value>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        fake.posts.lock().unwrap().push(body.clone());
        let case = fake.case.load(Ordering::Relaxed);
        if case == 10 {
            fake.entered.notify_one();
            let permit = fake.release.acquire().await.unwrap();
            permit.forget();
        }
        if case == 8 {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error":"private provider body"})),
            );
        }
        let count = body["input"].as_array().unwrap().len();
        let mut data = (0..count)
            .map(|index| {
                let dimension = if case == 3 { 511 } else { 512 };
                let value = match case {
                    4 => 0.,
                    5 => f32::MAX,
                    6 => f32::MIN_POSITIVE,
                    _ => (index + 1) as f32 * 0.125,
                };
                serde_json::json!({"index":if case == 1 {0} else {index},
                 "embedding":vec![value;dimension]})
            })
            .collect::<Vec<_>>();
        data.reverse();
        if case == 9 {
            data[0]["embedding"][0] = serde_json::Value::Null;
        }
        if case == 11 {
            data[0]["index"] = serde_json::json!(count);
        }
        if case == 2 {
            data.pop();
        }
        let mut response = serde_json::json!({
            "model": if case == 7 {"wrong/model"} else {"test/embed"}, "data": data
        });
        if case == 13 {
            response.as_object_mut().unwrap().remove("model");
        }
        (StatusCode::OK, Json(response))
    }

    async fn models(State(fake): State<Arc<Fake>>) -> (StatusCode, Json<serde_json::Value>) {
        fake.probes.fetch_add(1, Ordering::Relaxed);
        if fake.no_catalog.load(Ordering::Relaxed) {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({})));
        }
        let model = if fake.case.load(Ordering::Relaxed) == 12 {
            "foreign/embed"
        } else {
            "test/embed"
        };
        (
            StatusCode::OK,
            Json(serde_json::json!({"data":[{"id":model}]})),
        )
    }

    let fake = Arc::new(Fake::default());

    let router = Router::new()
        .route("/embeddings/models", get(models))
        .route("/models", get(models))
        .route("/embeddings", post(embeddings))
        .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server {
        fake,
        task,
        endpoint,
    }
}
fn config(server: &Server) -> EmbeddingConfig {
    let mut config = fyrr_core::config::parse(
        "[namespace]\nname='fake'\n[storage]\nuri='s3://fake'\n[embedding]\n\
            endpoint='https://fake.invalid'\n",
    )
    .unwrap()
    .config
    .embedding;
    config.endpoint = server.endpoint.clone();
    config.model = "test/embed".into();
    config.dimensions = 512;
    config.send_dimensions = true;
    config.reader_live = true;
    config.max_retries = 0;
    config.roles.query.prefix = Some("Q: ".into());
    config.roles.query.input_type = Some("query".into());
    config.roles.document.prefix = Some("D: ".into());
    config.roles.document.input_type = Some("document".into());
    config.spend = Some(SpendConfig {
        limit_microusd: 1000,
        max_call_microusd: 1,
    });
    config
}

#[tokio::test]
async fn live_no_catalog_probe_checks_optional_model_vectors_and_spend() {
    let server = server().await;
    server.fake.no_catalog.store(true, Ordering::Relaxed);
    let mut config = config(&server);
    for (case, error) in [
        (7, EmbeddingError::EmbeddingModelMismatch),
        (3, EmbeddingError::EmbeddingInvalid),
        (4, EmbeddingError::EmbeddingInvalid),
        (5, EmbeddingError::EmbeddingInvalid),
        (9, EmbeddingError::EmbeddingResponseInvalid),
    ] {
        server.fake.case.store(case, Ordering::Relaxed);
        let client = EmbeddingClient::new(&config, None).unwrap();
        let mut stats = ProviderStats::default();
        assert_eq!(client.verify_model(&mut stats).await.unwrap_err(), error);
        assert_eq!(
            (stats.calls, stats.embedding_calls, stats.reserved_microusd),
            (3, 1, 3)
        );
    }
    server.fake.case.store(13, Ordering::Relaxed);
    let client = EmbeddingClient::new(&config, None).unwrap();
    assert_eq!(
        client
            .embed(
                &["query without response model"],
                Role::Query,
                &mut ProviderStats::default()
            )
            .await
            .unwrap(),
        [vec![0.125; 512]]
    );
    config.spend.as_mut().unwrap().limit_microusd = 2;
    let before = server.fake.posts.lock().unwrap().len();
    let mut stats = ProviderStats::default();
    assert_eq!(
        EmbeddingClient::new(&config, None)
            .unwrap()
            .verify_model(&mut stats)
            .await
            .unwrap_err(),
        EmbeddingError::EmbeddingSpendLimitExceeded
    );
    assert_eq!(
        (stats.calls, stats.embedding_calls, stats.reserved_microusd),
        (2, 0, 2)
    );
    assert_eq!(server.fake.posts.lock().unwrap().len(), before);
}

#[tokio::test]
async fn live_provider_roles_order_validation_and_no_uncapped_calls() {
    let server = server().await;
    let config = config(&server);
    let client = EmbeddingClient::new(&config, None).unwrap();
    let mut stats = ProviderStats::default();
    let vectors = client
        .embed(&["first", "second"], Role::Query, &mut stats)
        .await
        .unwrap();
    assert_eq!(vectors, [vec![0.125; 512], vec![0.25; 512]]);
    let body = server.fake.posts.lock().unwrap()[0].clone();
    assert_eq!(body["input"], serde_json::json!(["Q: first", "Q: second"]));
    assert_eq!(body["input_type"], "query");
    assert_eq!(body["dimensions"], 512);
    assert_eq!(stats.calls, 2);
    assert_eq!(stats.embedding_calls, 1);
    for case in [1, 2, 3, 4, 5, 6, 7, 9, 11] {
        server.fake.case.store(case, Ordering::Relaxed);
        assert!(
            client
                .embed(&["first", "second"], Role::Query, &mut stats)
                .await
                .is_err(),
            "case {case}"
        );
    }
    let before = server.fake.posts.lock().unwrap().len();
    let mut uncapped = config.clone();
    uncapped.spend = None;
    assert_eq!(
        EmbeddingClient::new(&uncapped, None)
            .unwrap()
            .embed(&["first"], Role::Query, &mut stats)
            .await
            .unwrap_err(),
        EmbeddingError::EmbeddingSpendLimitRequired
    );
    assert_eq!(server.fake.posts.lock().unwrap().len(), before);
    assert_eq!(server.fake.probes.load(Ordering::Relaxed), 1);
    for (limit_microusd, max_call_microusd) in [(0, 1), (2, 0), (1, 2)] {
        let mut invalid = config.clone();
        invalid.spend = Some(SpendConfig {
            limit_microusd,
            max_call_microusd,
        });
        assert_eq!(
            EmbeddingClient::new(&invalid, None).unwrap_err(),
            EmbeddingError::EmbeddingSpendConfigurationInvalid
        );
    }
    server.fake.case.store(12, Ordering::Relaxed);
    assert_eq!(
        EmbeddingClient::new(&config, None)
            .unwrap()
            .verify_model(&mut stats)
            .await
            .unwrap_err(),
        EmbeddingError::EmbeddingModelMismatch
    );
    assert_eq!(server.fake.posts.lock().unwrap().len(), before);
    server.fake.case.store(0, Ordering::Relaxed);
    client
        .embed(&["document"], Role::Document, &mut stats)
        .await
        .unwrap();
    let body = server.fake.posts.lock().unwrap().last().unwrap().clone();
    assert_eq!(body["input"], serde_json::json!(["D: document"]));
    assert_eq!(body["input_type"], "document");
    let mut optional = config.clone();
    optional.send_dimensions = false;
    optional.roles = Default::default();
    EmbeddingClient::new(&optional, None)
        .unwrap()
        .embed(&["unprefixed"], Role::Query, &mut stats)
        .await
        .unwrap();
    let body = server.fake.posts.lock().unwrap().last().unwrap().clone();
    assert_eq!(body["input"], serde_json::json!(["unprefixed"]));
    assert!(body.get("input_type").is_none());
    assert!(body.get("dimensions").is_none());
}

#[tokio::test]
async fn live_spend_reserves_every_retry_and_refuses_the_next_call() {
    let server = server().await;
    let mut config = config(&server);
    config.max_retries = 2;
    config.spend.as_mut().unwrap().limit_microusd = 4;
    let client = EmbeddingClient::new(&config, None).unwrap();
    server.fake.case.store(8, Ordering::Relaxed);
    let mut stats = ProviderStats::default();
    assert_eq!(
        client
            .embed(&["retry"], Role::Query, &mut stats)
            .await
            .unwrap_err(),
        EmbeddingError::EmbeddingProviderUnavailable
    );
    assert_eq!(stats.calls, 4);
    assert_eq!(stats.embedding_calls, 3);
    assert_eq!(stats.reserved_microusd, 4);
    let before = server.fake.posts.lock().unwrap().len();
    assert_eq!(
        client
            .embed(&["over cap"], Role::Query, &mut stats)
            .await
            .unwrap_err(),
        EmbeddingError::EmbeddingSpendLimitExceeded
    );
    assert_eq!(server.fake.posts.lock().unwrap().len(), before);
}

#[tokio::test]
async fn live_concurrent_calls_share_one_monotone_spend_limit() {
    let server = server().await;
    let mut config = config(&server);
    config.spend.as_mut().unwrap().limit_microusd = 3;
    let client = Arc::new(EmbeddingClient::new(&config, None).unwrap());
    client
        .verify_model(&mut ProviderStats::default())
        .await
        .unwrap();
    let calls = (0..4).map(|_| {
        let client = client.clone();
        async move {
            client
                .embed(&["concurrent"], Role::Query, &mut ProviderStats::default())
                .await
        }
    });
    let results = futures::future::join_all(calls).await;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(EmbeddingError::EmbeddingSpendLimitExceeded)))
            .count(),
        2
    );
    assert_eq!(server.fake.posts.lock().unwrap().len(), 2);
    assert_eq!(server.fake.probes.load(Ordering::Relaxed), 1);
    assert_eq!(client.stats().reserved_microusd, 3);
}

#[tokio::test]
async fn live_deadline_retains_reservation_and_never_returns_provider_errors() {
    let server = server().await;
    let mut config = config(&server);
    config.timeout = serde_json::from_value(serde_json::json!("1s")).unwrap();
    let client = Arc::new(EmbeddingClient::new(&config, None).unwrap());
    client
        .verify_model(&mut ProviderStats::default())
        .await
        .unwrap();
    server.fake.case.store(10, Ordering::Relaxed);
    let mut pending = tokio::spawn({
        let client = client.clone();
        async move {
            let mut stats = ProviderStats::default();
            let result = client.embed(&["deadline"], Role::Query, &mut stats).await;
            (result, stats)
        }
    });
    tokio::select! {
        () = server.fake.entered.notified() => {},
        completed = &mut pending => panic!("provider never reached its gate: {completed:?}"),
    }
    let (result, stats) = pending.await.unwrap();
    assert_eq!(result.unwrap_err(), EmbeddingError::EmbeddingTimeout);
    assert_eq!(stats.embedding_calls, 1);
    assert_eq!(stats.reserved_microusd, 1);
    assert_eq!(client.stats().reserved_microusd, 2);
    server.fake.release.add_permits(1);
}
