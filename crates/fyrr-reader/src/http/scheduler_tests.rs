use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication, HTTP readiness and a failed native request task"]
async fn readiness_refuses_failed_scheduler_on_storage() {
    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("publication fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = 1;
    config.reader.version_check = fyrr_core::config::VersionCheck::Off;
    config.embedding.reader_live = false;
    let reader = crate::search::PreparedReader::open(&config).await.unwrap();
    let hooks = reader.test_hooks.clone();
    let subject = crate::auth::AuthenticatedSubject {
        key: std::env::var("FYRR_TEST_SCHEDULER_SUBJECT").unwrap(),
        identity: Some(reader.head.identity.clone()),
        expires: fyrr_core::clock::now().timestamp() + 600,
        api_key: false,
    };
    let admission = Arc::new(admission::Admission::new(
        reader.control_store.clone(),
        reader.control_root.clone(),
        reader.identity_cache.clone(),
    ));
    let service = crate::serve::start(reader).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    config.reader.http.as_mut().unwrap().public_url = format!("{url}/mcp");
    let state = HttpState {
        admission,
        service: service.clone(),
        public: config
            .reader
            .http
            .as_ref()
            .unwrap()
            .public_url
            .parse()
            .unwrap(),
        auth: crate::auth::Authenticator::new(&config).unwrap(),
        config,
        permits: Arc::new(Semaphore::new(MAX_REQUESTS_IN_FLIGHT)),
        controls: Arc::new(Semaphore::new(MAX_CONTROLS_IN_FLIGHT)),
        control_limits: Arc::new(Mutex::new(ControlLimits {
            subjects: BTreeMap::new(),
            tokens: BTreeMap::new(),
            attempts: (Instant::now(), 0),
        })),
        limits: Arc::new(Mutex::new(BTreeMap::new())),
        key_limits: Arc::default(),
        refresh: Arc::new(Mutex::new(None)),
    };
    let router = Router::new()
        .route("/readyz", get(readiness))
        .layer(middleware::from_fn_with_state(state.clone(), boundary))
        .with_state(state);
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let before = client.get(format!("{url}/readyz")).send().await.unwrap();
    assert_eq!(before.status(), StatusCode::OK);
    assert_eq!(
        before.json::<serde_json::Value>().await.unwrap()["ready"],
        true
    );
    hooks
        .native_work
        .enabled
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut reply = crate::serve::cache_test_enqueue(
        &service,
        crate::serve::ToolCall::Search(
            serde_json::from_value(json!({
                "query":"exactneedle", "limit":1, "output":"metadata"
            }))
            .unwrap(),
        ),
        subject,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            () = hooks.native_work.entered.notified() => {},
            response = &mut reply => panic!("request failed before native work: {response:?}"),
        }
    })
    .await
    .expect("the bound request reaches native work");
    // Closing the existing gate panics its parked request task, without a product panic knob.
    hooks.native_work.release.close();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), reply)
            .await
            .unwrap()
            .is_err()
    );
    // A dropped reply observes the request panic, not the scheduler's handling
    // of its JoinError. Wait for admission to close at that recorded boundary.
    tokio::time::timeout(Duration::from_secs(10), hooks.scheduler_failed.notified())
        .await
        .expect("scheduler records the failed request and closes admission");
    let after = client.get(format!("{url}/readyz")).send().await.unwrap();
    assert_eq!(
        after.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "HTTP readiness must refuse service after the scheduler stops"
    );
    assert_eq!(
        after.json::<serde_json::Value>().await.unwrap()["ready"],
        false
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(10), service.refresh())
            .await
            .unwrap()
            .is_err()
    );
    server.abort();
}
