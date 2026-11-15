use super::*;
use crate::serve::TestGate;
use std::sync::atomic::Ordering;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real publication supplied by tools_embedding_provider_wait fixture"]
async fn provider_wait_overlaps_native_lexical_on_real_s3() {
    async fn embed(
        axum::extract::State(gate): axum::extract::State<Arc<TestGate>>,
        axum::Json(request): axum::Json<serde_json::Value>,
    ) -> axum::Json<serde_json::Value> {
        let count = request["input"].as_array().unwrap().len();
        assert_eq!(count, 2);
        gate.entered.notify_one();
        gate.release.acquire().await.unwrap().forget();
        let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
        let data: Vec<_> = (0..count)
            .map(|index| serde_json::json!({"index":index,"embedding":vector}))
            .collect();
        axum::Json(serde_json::json!({"model":"BAAI/bge-m3","data":data}))
    }

    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("embedding fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    let provider = Arc::new(TestGate::default());

    let app = axum::Router::new()
        .route(
            "/embeddings/models",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"data":[{"id":"BAAI/bge-m3"}]}))
            }),
        )
        .route("/embeddings", axum::routing::post(embed))
        .with_state(provider.clone());
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.embedding.endpoint = format!("http://{}", socket.local_addr().unwrap());
    config.embedding.reader_live = true;
    config.embedding.timeout = serde_json::from_value(serde_json::json!("5s")).unwrap();
    config.embedding.spend = Some(fyrr_core::config::SpendConfig {
        limit_microusd: 100,
        max_call_microusd: 10,
    });
    let server = tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    let template = PreparedReader::open(&config).await.unwrap();
    let mut reader = template.worker().await.unwrap();
    let hooks = reader.test_hooks.clone();
    let args: SearchInput = serde_json::from_value(serde_json::json!({
        "queries":["fixture uncached overlap one", "fixture uncached overlap two"],
        "limit":4, "output":"metadata"
    }))
    .unwrap();
    let proof = async {
        provider.entered.notified().await;
        loop {
            let completed = hooks.lexical_complete.notified();
            if hooks.lexical_queries_completed.load(Ordering::SeqCst) == 2 {
                break;
            }
            completed.await;
        }
        // Both native lexical scans finish while the provider response
        // is still gated. No duration comparison or sleep proves this claim.
        assert_eq!(provider.release.available_permits(), 0);
        provider.release.add_permits(1);
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::join!(reader.search_input(&args, None, None), proof)
    })
    .await
    .unwrap();
    let result = result.unwrap();
    assert!(result.metadata.degraded.is_empty());
    assert_eq!(result.metadata.telemetry.embedding_provider_calls, 2);
    assert!(!result.hits.is_empty());
    server.abort();
}
