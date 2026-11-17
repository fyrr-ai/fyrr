use super::*;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};

fn sink() -> (Arc<Sink>, std::path::PathBuf) {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "fyrr-diagnostic-{}-{}.jsonl",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    (Sink::open(&path).unwrap(), path)
}

#[tokio::test]
async fn boundary_records_operations_without_keys_payloads_or_backend_errors() {
    let (sink, path) = sink();
    let store = Store {
        inner: Arc::new(object_store::memory::InMemory::new()),
        sink,
        local: true,
        scope: Path::default(),
    };
    let key = Path::from("PRIVATE_KEY/PRIVATE_IDENTITY");
    store
        .put(&key, b"PRIVATE_BODY".to_vec().into())
        .await
        .unwrap();
    assert_eq!(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"PRIVATE_BODY"
    );
    store.head(&key).await.unwrap();
    assert_eq!(
        store.get_ranges(&key, &[0..3, 3..7]).await.unwrap()[0].as_ref(),
        b"PRI"
    );
    store.copy(&key, &Path::from("PRIVATE_COPY")).await.unwrap();
    store
        .rename(&Path::from("PRIVATE_COPY"), &Path::from("PRIVATE_RENAMED"))
        .await
        .unwrap();
    let objects: Vec<_> = futures::TryStreamExt::try_collect(store.list(None))
        .await
        .unwrap();
    assert_eq!(objects.len(), 2);
    let offset: Vec<_> =
        futures::TryStreamExt::try_collect(store.list_with_offset(None, &Path::default()))
            .await
            .unwrap();
    assert_eq!(offset.len(), 2);
    store.list_with_delimiter(None).await.unwrap();
    store.delete(&key).await.unwrap();
    assert!(matches!(store.get(&key).await, Err(Error::NotFound { .. })));
    let mut upload = store.put_multipart(&key).await.unwrap();
    upload
        .put_part(b"PRIVATE_MULTIPART_BODY".to_vec().into())
        .await
        .unwrap();
    upload.complete().await.unwrap();
    let mut upload = store.put_multipart(&key).await.unwrap();
    upload.abort().await.unwrap();
    drop(store.get(&key).await.unwrap());
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.contains("PRIVATE"));
    let records: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for method in [
        "PUT",
        "GET",
        "HEAD",
        "COPY",
        "RENAME",
        "LIST",
        "LIST_OFFSET",
        "LIST_DELIMITER",
        "DELETE_STREAM",
        "MULTIPART_CREATE",
        "MULTIPART_PART",
        "MULTIPART_COMPLETE",
        "MULTIPART_ABORT",
    ] {
        assert!(
            records.iter().any(|r| r["method"] == method),
            "missing {method}"
        );
    }
    assert!(records.iter().any(|r| r["error_code"] == "not_found"));
    assert!(records.iter().any(|r| r["error_code"] == "cancelled"));
    for record in records {
        assert_eq!(record["key_sha256"].as_str().unwrap().len(), 64);
        assert_eq!(record["retry_count"], 0);
        assert!(record["http_status"].is_null());
        assert!(record["latency_us"].is_u64());
    }
    std::fs::remove_file(path).unwrap();
}

#[derive(Debug)]
struct Connector;
impl HttpConnector for Connector {
    fn connect(&self, options: &ClientOptions) -> Result<HttpClient> {
        Ok(HttpClient::new(Transport(
            ReqwestConnector::default().connect(options)?,
        )))
    }
}

#[tokio::test]
async fn boundary_counts_real_http_retries_and_never_formats_remote_errors() {
    use axum::{Router, extract::State, http::StatusCode, routing::get};
    async fn respond(
        State(requests): State<Arc<AtomicUsize>>,
        uri: axum::http::Uri,
    ) -> (StatusCode, [(&'static str, &'static str); 2], &'static str) {
        let headers = [
            ("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
            ("etag", "\"private-etag\""),
        ];
        if uri.path().ends_with("DENIED") {
            return (StatusCode::FORBIDDEN, headers, "PRIVATE_REMOTE_ERROR");
        }
        if requests.fetch_add(1, Ordering::SeqCst) == 0 {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                headers,
                "PRIVATE_REMOTE_ERROR",
            )
        } else {
            (StatusCode::OK, headers, "PRIVATE_DOCUMENT")
        }
    }
    let requests = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/{*key}", get(respond))
        .with_state(requests.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let inner = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name("private-bucket")
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_skip_signature(true)
        .with_http_connector(Connector)
        .with_retry(RetryConfig {
            max_retries: 1,
            ..Default::default()
        })
        .build()
        .unwrap();
    let (sink, path) = sink();
    let store = Store {
        inner: Arc::new(inner),
        sink,
        local: false,
        scope: Path::default(),
    };
    let result = store
        .get(&Path::from("PRIVATE_KEY"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(result.as_ref(), b"PRIVATE_DOCUMENT");
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    assert!(matches!(
        store.get(&Path::from("PRIVATE_DENIED")).await,
        Err(Error::PermissionDenied { .. })
    ));
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.contains("PRIVATE"));
    let record: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(record["method"], "GET");
    assert_eq!(record["http_status"], 200);
    assert_eq!(record["retry_count"], 1);
    assert_eq!(record["error_code"], "ok");
    let denied: serde_json::Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    assert_eq!(denied["http_status"], 403);
    assert_eq!(denied["retry_count"], 0);
    assert_eq!(denied["error_code"], "permission_denied");
    server.abort();
    std::fs::remove_file(path).unwrap();
}

#[test]
fn diagnostic_sink_refuses_symlinks_and_public_files() {
    let (sink, path) = sink();
    drop(sink);
    let link = path.with_extension("link");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(Sink::open(&link).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Sink::open(&path).is_err());
    assert!(Sink::open(std::path::Path::new("relative-log")).is_err());
    std::fs::remove_file(link).unwrap();
    std::fs::remove_file(path).unwrap();
}
