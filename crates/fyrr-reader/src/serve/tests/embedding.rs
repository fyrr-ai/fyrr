use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real publication supplied by the embedding worker fixture"]
async fn embedding_wait_releases_native_slot_and_keeps_retired_generation_pin_on_real_s3() {
    async fn embed(
        axum::extract::State(gate): axum::extract::State<Arc<TestGate>>,
        axum::Json(request): axum::Json<Value>,
    ) -> axum::Json<Value> {
        assert_eq!(request["input"].as_array().unwrap().len(), 1);
        gate.entered.notify_one();
        gate.release.acquire().await.unwrap().forget();
        let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
        axum::Json(json!({"model":"BAAI/bge-m3","data":[{"index":0,"embedding":vector}]}))
    }

    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("embedding fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    let provider = Arc::new(TestGate::default());

    let app = axum::Router::new()
        .route(
            "/embeddings/models",
            axum::routing::get(|| async { axum::Json(json!({"data":[{"id":"BAAI/bge-m3"}]})) }),
        )
        .route("/embeddings", axum::routing::post(embed))
        .with_state(provider.clone());
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.embedding.endpoint = format!("http://{}", socket.local_addr().unwrap());
    config.embedding.reader_live = true;
    config.embedding.timeout = serde_json::from_value(json!("10s")).unwrap();
    config.embedding.spend = Some(fyrr_core::config::SpendConfig {
        limit_microusd: 100,
        max_call_microusd: 10,
    });
    config.reader.concurrency = 1;
    config.reader.version_check = fyrr_core::config::VersionCheck::Off;
    let server = tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    let mut head = reader.head.clone();
    let mut manifest = reader.manifest.clone();
    let old_generation = Arc::downgrade(&reader.generation_id);
    let old_pin = Arc::downgrade(&reader.mirror_pin);
    let hooks = reader.test_hooks.clone();
    let service = start(reader).await.unwrap();
    let args: SearchInput = serde_json::from_value(
        json!({"query":"fixture parked embedding", "limit":4, "output":"metadata"}),
    )
    .unwrap();
    let waiting = enqueue(&service, ToolCall::Search(args), false).await;
    tokio::time::timeout(Duration::from_secs(10), provider.entered.notified())
        .await
        .unwrap();
    loop {
        let completed = hooks.lexical_complete.notified();
        if hooks.lexical_queries_completed.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::timeout(Duration::from_secs(10), completed)
            .await
            .unwrap();
    }
    let fetch = serde_json::from_value(json!({"source_id":"allowed","root_id":"r0"})).unwrap();
    let fetched = tokio::time::timeout(
        Duration::from_secs(5),
        enqueue(&service, ToolCall::Fetch(fetch), false).await,
    )
    .await
    .unwrap()
    .unwrap();
    assert_ne!(
        fetched.is_error,
        Some(true),
        "fetch runs at cap one while the embedding remains gated"
    );
    assert_eq!(provider.release.available_permits(), 0);
    generation_publish(&config, &mut head, &mut manifest).await;
    tokio::time::timeout(Duration::from_secs(5), service.refresh())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        old_generation.strong_count(),
        1,
        "only the in-flight reader retains the retired generation"
    );
    assert!(
        old_pin.upgrade().is_some(),
        "retirement keeps the old request's content lease"
    );
    provider.release.add_permits(1);
    let reply = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(reply.is_error, Some(true));
    assert_eq!(
        reply.structured_content.unwrap()["metadata"]["degraded"],
        json!([])
    );
    drop(service);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real publication supplied by the embedding worker fixture"]
async fn embedding_wait_all_drivers_leave_mirror_io_headroom_on_real_s3() {
    async fn embed(
        axum::extract::State(gate): axum::extract::State<Arc<TestGate>>,
    ) -> axum::Json<Value> {
        gate.entered.notify_one();
        gate.release.acquire().await.unwrap().forget();
        let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
        axum::Json(json!({"model":"BAAI/bge-m3","data":[{"index":0,"embedding":vector}]}))
    }

    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("embedding fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    let provider = Arc::new(TestGate::default());

    let app = axum::Router::new()
        .route(
            "/embeddings/models",
            axum::routing::get(|| async { axum::Json(json!({"data":[{"id":"BAAI/bge-m3"}]})) }),
        )
        .route("/embeddings", axum::routing::post(embed))
        .with_state(provider.clone());
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.embedding.endpoint = format!("http://{}", socket.local_addr().unwrap());
    config.embedding.reader_live = true;
    config.embedding.timeout = serde_json::from_value(json!("10s")).unwrap();
    config.embedding.spend = Some(fyrr_core::config::SpendConfig {
        limit_microusd: 100,
        max_call_microusd: 10,
    });
    config.reader.concurrency = 2;
    config.reader.version_check = fyrr_core::config::VersionCheck::Off;
    let server = tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    let hooks = reader.test_hooks.clone();
    let service = start(reader).await.unwrap();
    let mut replies = Vec::new();
    for index in 0..4 {
        let args = serde_json::from_value(json!({
            "query":format!("fixture parked headroom {index}"), "limit":4,
        }))
        .unwrap();
        replies.push(enqueue(&service, ToolCall::Search(args), false).await);
        tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
            .await
            .expect("every live driver reaches the slow provider");
        // No provider can finish yet. The third and fourth lexical scans
        // require local mirror I/O while earlier drivers remain parked.
        loop {
            let completed = hooks.lexical_complete.notified();
            if hooks.lexical_queries_completed.load(Ordering::SeqCst) > index {
                break;
            }
            tokio::time::timeout(Duration::from_secs(5), completed)
                .await
                .expect("mirror reads progress with parked drivers");
        }
    }
    assert_eq!(provider.release.available_permits(), 0);
    provider.release.add_permits(4);
    for reply in replies {
        let reply = tokio::time::timeout(Duration::from_secs(5), reply)
            .await
            .expect("dense mirror reads finish before provider deadlines")
            .unwrap();
        assert_ne!(reply.is_error, Some(true));
        assert_eq!(
            reply.structured_content.unwrap()["metadata"]["degraded"],
            json!([])
        );
    }
    drop(service);
    server.abort();
}

struct EmbeddingWire {
    write: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    lines: tokio::io::Lines<tokio::io::BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    serving: tokio::task::JoinHandle<Result<()>>,
}
impl EmbeddingWire {
    async fn open(service: McpService) -> Self {
        use tokio::io::AsyncBufReadExt;
        let (client, server) = tokio::io::duplex(1024 * 1024);
        let (read, write) = tokio::io::split(server);
        let transport = StdioTransport {
            inner: rmcp::transport::async_rw::AsyncRwTransport::new_server(read, write),
            slots: service.slots.clone(),
            cancellations: service.cancellations.clone(),
            response_claims: Arc::default(),
        };
        let serving = tokio::spawn(service.run_stdio(transport));
        let (read, write) = tokio::io::split(client);
        let mut wire = Self {
            write,
            lines: tokio::io::BufReader::new(read).lines(),
            serving,
        };
        wire.send(json!({"jsonrpc":"2.0",
            "id":0,
            "method":"initialize",
            "params":{"protocolVersion":"2025-03-26",
            "capabilities":{},
            "clientInfo":{"name":"parked-embedding-test",
            "version":"1"}}}))
            .await;
        assert_eq!(wire.reply().await["id"], 0);
        wire.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await;
        wire
    }
    async fn send(&mut self, value: Value) {
        use tokio::io::AsyncWriteExt;
        self.write
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
        self.write.flush().await.unwrap();
    }
    async fn call(&mut self, id: u32, name: &str, args: Value) {
        self.send(json!({"jsonrpc":"2.0",
            "id":id,
            "method":"tools/call",
            "params":{"name":name,
            "arguments":args}}))
            .await;
    }
    async fn reply(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(10), self.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(&line).unwrap()
    }
    async fn cancel(&mut self, id: u32) {
        self.send(json!({"jsonrpc":"2.0",
            "method":"notifications/cancelled",
            "params":{"requestId":id,
            "reason":"test cancellation"}}))
            .await;
    }
    async fn close(self) {
        drop(self.write);
        drop(self.lines);
        self.serving.await.unwrap().unwrap();
    }
}
struct ParkedEmbedding {
    wire: EmbeddingWire,
    service: McpService,
    provider: Arc<TestGate>,
    hooks: Arc<TestHooks>,
    memory: Arc<tokio::sync::Semaphore>,
    baseline_memory: usize,
    probe_row: u64,
    pin: Arc<crate::mirror::MirrorLease>,
    config: fyrr_core::config::Config,
    head: fyrr_core::publication::ServingHead,
    policy: fyrr_core::publication::PolicyArtifact,
    server: tokio::task::JoinHandle<()>,
}
impl ParkedEmbedding {
    async fn open(timeout: &str) -> Self {
        async fn embed(
            axum::extract::State(gate): axum::extract::State<Arc<TestGate>>,
            axum::Json(request): axum::Json<Value>,
        ) -> axum::Json<Value> {
            gate.entered.notify_one();
            gate.release.acquire().await.unwrap().forget();
            let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
            let data: Vec<_> = request["input"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(index, _)| json!({"index":index,"embedding":vector}))
                .collect();
            axum::Json(json!({"model":"BAAI/bge-m3","data":data}))
        }

        let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("embedding fixture config");
        let mut config = fyrr_core::config::load(std::path::Path::new(&path))
            .unwrap()
            .config;
        let provider = Arc::new(TestGate::default());

        let app = axum::Router::new()
            .route(
                "/embeddings/models",
                axum::routing::get(|| async { axum::Json(json!({"data":[{"id":"BAAI/bge-m3"}]})) }),
            )
            .route("/embeddings", axum::routing::post(embed))
            .with_state(provider.clone());
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        config.embedding.endpoint = format!("http://{}", socket.local_addr().unwrap());
        config.embedding.reader_live = true;
        config.embedding.query_cache_max_bytes = 0;
        config.embedding.timeout = serde_json::from_value(json!(timeout)).unwrap();
        config.embedding.spend = Some(fyrr_core::config::SpendConfig {
            limit_microusd: 1000,
            max_call_microusd: 10,
        });
        config.reader.concurrency = 1;
        config.reader.version_check = fyrr_core::config::VersionCheck::Off;
        let server = tokio::spawn(async move {
            axum::serve(socket, app).await.unwrap();
        });
        let mut reader = PreparedReader::open(&config).await.unwrap();
        reader.prewarm().await.unwrap();
        let probe_row = reader.eligible.iter_addrs().unwrap().next().unwrap().into();
        let hooks = reader.test_hooks.clone();
        let memory = reader.preparation_budget.permits.clone();
        let pin = reader.mirror_pin.clone();
        let head = reader.head.clone();
        let (store, root) = crate::storage::namespace_store(&config).unwrap();
        let policy = crate::storage::artifact(store.as_ref(), &root, &head.policy)
            .await
            .unwrap();
        let service = start(reader).await.unwrap();
        let baseline_memory = memory.available_permits();
        let wire = EmbeddingWire::open(service.clone()).await;
        Self {
            wire,
            service,
            provider,
            hooks,
            memory,
            baseline_memory,
            probe_row,
            pin,
            config,
            head,
            policy,
            server,
        }
    }
    async fn parked(&mut self, id: u32) {
        let args = json!({"query":"exactneedle","limit":4,"output":"metadata","deadline_ms":8000});
        ToolCall::parse("search", args.clone()).unwrap();
        self.wire.call(id, "search", args).await;
        tokio::time::timeout(Duration::from_secs(8), self.provider.entered.notified())
            .await
            .unwrap();
        self.wait_free_native().await;
    }
    async fn wait_free_native(&self) {
        loop {
            let parked = self.hooks.embedding_parked.notified();
            if self.service.native_slots.available_permits() == 1 {
                break;
            }
            tokio::time::timeout(Duration::from_secs(8), parked)
                .await
                .unwrap();
        }
    }
    async fn hold_second(&mut self, id: u32) {
        self.hooks.native_work.enabled.store(true, Ordering::SeqCst);
        self.wire
            .call(
                id,
                "fetch",
                json!({"source_id":"allowed","root_id":"r0","deadline_ms":10000}),
            )
            .await;
        tokio::time::timeout(
            Duration::from_secs(8),
            self.hooks.native_work.entered.notified(),
        )
        .await
        .unwrap();
        assert_eq!(self.service.native_slots.available_permits(), 0);
    }
    async fn settled(&mut self, pins: usize, native: usize, admission: usize) {
        // A subsequent SDK response is a transport flush barrier: the
        // previous response's admission/scratch claims have been dropped.
        self.wire
            .send(json!({"jsonrpc":"2.0","id":900,"method":"ping"}))
            .await;
        assert_eq!(self.wire.reply().await["id"], 900);
        loop {
            let done = self.hooks.request_finished.notified();
            if self.pin.request_count() == pins
                && self.service.native_slots.available_permits() == native
                && self.service.slots.available_permits() == admission
                && self.memory.available_permits() == self.baseline_memory
            {
                break;
            }
            tokio::time::timeout(Duration::from_secs(8), done)
                .await
                .unwrap();
        }
        assert_eq!(self.pin.request_count(), pins);
        assert_eq!(self.memory.available_permits(), self.baseline_memory);
        assert_eq!(self.service.native_slots.available_permits(), native);
        assert_eq!(self.service.slots.available_permits(), admission);
    }
    async fn close(self) {
        self.wire.close().await;
        self.server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication, native scheduler, SDK transport and fake provider"]
async fn embedding_timeout_keeps_lexical_answer_while_native_slot_is_occupied_on_real_s3() {
    let mut fixture = ParkedEmbedding::open("3s").await;
    fixture.parked(1).await;
    fixture.hold_second(2).await;
    let reply = fixture.wire.reply().await;
    assert_eq!(
        reply["id"], 1,
        "fallback completes while the second native request remains gated"
    );
    assert_eq!(reply["result"]["isError"], false);
    assert_eq!(
        reply["result"]["structuredContent"]["metadata"]["degraded"],
        json!(["dense"])
    );
    assert!(
        !reply["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.service.native_slots.available_permits(), 0);
    assert!(
        reply["result"]["structuredContent"]["metadata"]["telemetry"]["stage_us"]["total"]
            .as_u64()
            .unwrap()
            < 8_000_000
    );
    fixture.hooks.native_work.release.add_permits(1);
    assert_eq!(fixture.wire.reply().await["id"], 2);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication, native scheduler, SDK transport and fake provider"]
async fn embedding_wait_revocation_refuses_old_authorization_on_real_s3() {
    use object_store::ObjectStoreExt;
    let mut fixture = ParkedEmbedding::open("10s").await;
    fixture.parked(1).await;
    let mut policy = fixture.policy.clone();
    policy.generation += 1;
    policy.fixed_grants = std::collections::BTreeMap::from([("allowed".into(), Vec::new())]);
    let bytes = fyrr_core::write_contract(&policy);
    let (store, root) = crate::storage::namespace_store(&fixture.config).unwrap();
    let object = "policies/embedding-wait-revoke.json";
    store
        .put(
            &object_store::path::Path::from(format!("{root}/{object}")),
            bytes.clone().into(),
        )
        .await
        .unwrap();
    fixture.head.policy = fyrr_core::publication::GenerationPointer {
        generation: policy.generation,
        object: object.into(),
        digest: crate::embedding::digest(&bytes),
    };
    store
        .put(
            &root.join("head.json"),
            fyrr_core::write_contract(&fixture.head).into(),
        )
        .await
        .unwrap();
    fixture.provider.release.add_permits(1);
    let reply = fixture.wire.reply().await;
    assert_eq!(reply["result"]["isError"], true);
    assert_eq!(
        reply["result"]["structuredContent"]["code"],
        "authorization_unavailable"
    );
    assert!(reply["result"]["structuredContent"]["hits"].is_null());
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication, native scheduler, SDK transport and fake provider"]
async fn embedding_parked_cancellation_releases_counted_resources_on_real_s3() {
    let mut fixture = ParkedEmbedding::open("10s").await;
    fixture.parked(1).await;
    assert!(fixture.memory.available_permits() < fixture.baseline_memory);
    assert_eq!(fixture.pin.request_count(), 2);
    fixture.wire.cancel(1).await;
    assert_eq!(
        fixture.wire.reply().await["result"]["structuredContent"]["code"],
        "cancelled"
    );
    fixture.settled(0, 1, 2).await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication, native scheduler, SDK transport and fake provider"]
async fn embedding_reacquire_cancellation_releases_counted_resources_on_real_s3() {
    let mut fixture = ParkedEmbedding::open("10s").await;
    fixture.parked(1).await;
    fixture.hold_second(2).await;
    fixture.provider.release.add_permits(1);
    tokio::time::timeout(
        Duration::from_secs(8),
        fixture.hooks.embedding_reacquiring.notified(),
    )
    .await
    .unwrap();
    fixture.wire.cancel(1).await;
    let reply = fixture.wire.reply().await;
    assert_eq!(reply["id"], 1);
    assert_eq!(reply["result"]["structuredContent"]["code"], "cancelled");
    fixture.settled(1, 0, 1).await;
    fixture.wire.cancel(2).await;
    fixture.hooks.native_work.release.add_permits(1);
    assert_eq!(fixture.wire.reply().await["id"], 2);
    fixture.settled(0, 1, 2).await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication, native scheduler, SDK transport and fake provider"]
async fn embedding_parked_admission_saturation_returns_busy_on_real_s3() {
    let mut fixture = ParkedEmbedding::open("10s").await;
    fixture.parked(1).await;
    fixture.parked(2).await;
    assert_eq!(fixture.service.slots.available_permits(), 0);
    fixture
        .wire
        .call(3, "fetch", json!({"source_id":"allowed","root_id":"r0"}))
        .await;
    let reply = fixture.wire.reply().await;
    assert_eq!(reply["id"], 3);
    assert_eq!(reply["result"]["structuredContent"]["code"], "busy");
    fixture.wire.cancel(1).await;
    fixture.wire.cancel(2).await;
    for _ in 0..2 {
        assert_eq!(
            fixture.wire.reply().await["result"]["structuredContent"]["code"],
            "cancelled"
        );
    }
    fixture.settled(0, 1, 2).await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication and stdio cancellation during admission"]
async fn cancelled_admission_rebuild_cannot_reuse_stale_stdio_mask_on_real_s3() {
    interrupted_admission_rebuild(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication and stdio deadline during admission"]
async fn timed_out_admission_rebuild_cannot_reuse_stale_stdio_mask_on_real_s3() {
    interrupted_admission_rebuild(true).await;
}

async fn interrupted_admission_rebuild(deadline: bool) {
    use object_store::ObjectStoreExt;

    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("publication fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = 1;
    config.reader.version_check = fyrr_core::config::VersionCheck::Off;
    config.embedding.reader_live = false;
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    let mut head = reader.head.clone();
    let hooks = reader.test_hooks.clone();
    let service = start(reader).await.unwrap();
    let mut wire = EmbeddingWire::open(service).await;
    let args = json!({"query":"exactneedle","limit":4,"output":"metadata"});
    wire.call(1, "search", args.clone()).await;
    let first = wire.reply().await;
    assert_ne!(first["result"]["isError"], true, "{first}");
    assert!(
        !first["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(config.sources.iter().any(|source| source.id == "allowed"
        && source.permissions == fyrr_core::config::PermissionsMode::Sync));
    let freshness = head
        .freshness
        .sources
        .iter_mut()
        .find(|source| source.source_id == "allowed")
        .unwrap();
    freshness.permissions_read_at = chrono::DateTime::UNIX_EPOCH;
    let (store, root) = crate::storage::namespace_store(&config).unwrap();
    store
        .put(
            &root.join("head.json"),
            fyrr_core::write_contract(&head).into(),
        )
        .await
        .unwrap();
    hooks.admission_scan.enabled.store(true, Ordering::SeqCst);
    let mut interrupted = args.clone();
    interrupted["deadline_ms"] = json!(if deadline { 1000 } else { 8000 });
    wire.call(2, "search", interrupted).await;
    tokio::time::timeout(
        Duration::from_secs(8),
        hooks.admission_scan.entered.notified(),
    )
    .await
    .expect("the expired-source rebuild reaches the native scan boundary");
    if !deadline {
        wire.cancel(2).await;
    }
    let stopped = wire.reply().await;
    assert_eq!(stopped["id"], 2);
    let code = stopped["result"]["structuredContent"]["code"]
        .as_str()
        .unwrap();
    assert!(
        code == "authorization_unavailable"
            || code
                == if deadline {
                    "deadline_exceeded"
                } else {
                    "cancelled"
                },
        "{stopped}"
    );
    wire.call(3, "search", args).await;
    let next = wire.reply().await;
    assert_eq!(next["id"], 3);
    assert_ne!(next["result"]["isError"], true, "{next}");
    assert_eq!(
        next["result"]["structuredContent"]["hits"],
        json!([]),
        "a reused stdio worker must not expose rows from the expired source"
    );
    assert_eq!(
        next["result"]["structuredContent"]["metadata"]["hidden_sources"],
        json!(["allowed"])
    );
    wire.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 and stdio directory freshness recovery"]
async fn failed_refresh_cannot_admit_a_stale_mask_after_directory_recovery_on_real_s3() {
    use crate::storage::read_contract;
    use fyrr_core::publication::ServingHead;
    use object_store::ObjectStoreExt;

    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("publication fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = 1;
    config.reader.version_check = fyrr_core::config::VersionCheck::Off;
    config.embedding.reader_live = false;
    config.identity.directory.provider = fyrr_core::config::DirectoryProvider::Snapshot;
    let (store, root) = crate::storage::namespace_store(&config).unwrap();
    let head_path = root.join("head.json");
    let mut head: ServingHead = read_contract(store.as_ref(), &head_path).await.unwrap();
    head.freshness.directory = Some(chrono::Utc::now());
    store
        .put(&head_path, fyrr_core::write_contract(&head).into())
        .await
        .unwrap();
    let admitted = head.clone();
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    let service = start(reader).await.unwrap();
    let mut wire = EmbeddingWire::open(service).await;
    let args = json!({"query":"exactneedle","limit":4,"output":"metadata"});
    wire.call(1, "search", args.clone()).await;
    let first = wire.reply().await;
    assert_ne!(first["result"]["isError"], true, "{first}");
    let hits = first["result"]["structuredContent"]["hits"]
        .as_array()
        .unwrap();
    assert!(!hits.is_empty());
    assert!(hits.iter().all(|hit| hit["source_id"] == "allowed"));
    assert!(config.sources.iter().any(|source| source.id == "allowed"
        && source.permissions == fyrr_core::config::PermissionsMode::Sync));

    head.freshness.directory = Some(chrono::DateTime::UNIX_EPOCH);
    head.freshness
        .sources
        .iter_mut()
        .find(|source| source.source_id == "allowed")
        .unwrap()
        .permissions_read_at = chrono::DateTime::UNIX_EPOCH;
    store
        .put(&head_path, fyrr_core::write_contract(&head).into())
        .await
        .unwrap();
    wire.call(2, "search", args.clone()).await;
    let denied = wire.reply().await;
    assert_eq!(denied["id"], 2);
    assert_eq!(denied["result"]["isError"], true);
    assert_eq!(
        denied["result"]["structuredContent"]["code"],
        "authorization_unavailable"
    );
    assert!(denied["result"]["structuredContent"]["hits"].is_null());

    // An unchanged directory read can refresh its proof without changing identity.
    // Publish that exact state transition through the real fixture object store.
    head.freshness.directory = Some(chrono::Utc::now());
    assert_eq!(head.identity, admitted.identity);
    assert_eq!(head.policy, admitted.policy);
    assert_eq!(head.content, admitted.content);
    assert_eq!(
        PreparedReader::expired_sources(&config, &head).unwrap(),
        std::collections::BTreeSet::from(["allowed".into()])
    );
    assert!(crate::storage::directory_fresh(
        &config,
        head.freshness.directory,
        chrono::Utc::now()
    ));
    store
        .put(&head_path, fyrr_core::write_contract(&head).into())
        .await
        .unwrap();
    wire.call(3, "search", args).await;
    let recovered = wire.reply().await;
    assert_eq!(recovered["id"], 3);
    assert_ne!(recovered["result"]["isError"], true, "{recovered}");
    assert_eq!(
        recovered["result"]["structuredContent"]["hits"],
        json!([]),
        "directory recovery must not reuse admission from before the source expired"
    );
    assert_eq!(
        recovered["result"]["structuredContent"]["metadata"]["hidden_sources"],
        json!(["allowed"])
    );
    wire.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3, MCP stdio lifecycle and a failed native task"]
async fn stdio_terminates_after_scheduler_failure_on_real_s3() {
    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("publication fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = 1;
    config.reader.version_check = fyrr_core::config::VersionCheck::Off;
    config.embedding.reader_live = false;
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    let hooks = reader.test_hooks.clone();
    let service = start(reader).await.unwrap();
    // Ordinary client EOF remains a successful shutdown, with the same lifecycle adapter.
    tokio::time::timeout(
        Duration::from_secs(5),
        EmbeddingWire::open(service.clone()).await.close(),
    )
    .await
    .unwrap();
    let mut wire = EmbeddingWire::open(service.clone()).await;
    let args = json!({"query":"exactneedle","limit":4,"output":"metadata"});
    wire.call(1, "search", args.clone()).await;
    let first = wire.reply().await;
    assert_ne!(first["result"]["isError"], true, "{first}");
    assert!(
        !first["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    hooks.native_work.enabled.store(true, Ordering::SeqCst);
    wire.call(2, "search", args.clone()).await;
    tokio::time::timeout(Duration::from_secs(5), hooks.native_work.entered.notified())
        .await
        .expect("request reaches the existing native-work boundary");
    // Closing this existing test gate panics only the parked request task.
    hooks.native_work.release.close();
    tokio::time::timeout(Duration::from_secs(5), service.sender.closed())
        .await
        .expect("the failed request task stops the scheduler");
    let stopped = tokio::time::timeout(Duration::from_secs(5), &mut wire.serving).await;
    if stopped.is_err() {
        // Confirm the actual wrong response, rather than inferring failure from a timer.
        wire.call(3, "search", args).await;
        for _ in 0..2 {
            let reply = tokio::time::timeout(Duration::from_secs(5), wire.reply())
                .await
                .unwrap();
            if reply["id"] == 3 {
                assert_eq!(reply["result"]["structuredContent"]["code"], "busy");
                panic!("stdio remained open and returned retryable busy after scheduler failure");
            }
        }
        panic!("failed scheduler produced no observable reply");
    }
    assert_eq!(stopped.unwrap().unwrap(), Err(TransportUnavailable));
    tokio::time::timeout(Duration::from_secs(5), async {
        while wire.lines.next_line().await.unwrap().is_some() {}
    })
    .await
    .expect("failed stdio server closes its output while the client stays connected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication, SDK transport and gated embedding before rerank refresh"]
async fn rerank_refresh_preserves_rebuilt_admission_on_real_s3() {
    use object_store::ObjectStoreExt;

    let mut fixture = ParkedEmbedding::open("10s").await;
    assert!(fixture.config.namespace.rerank.is_some());
    *crate::sync::lock(&fixture.hooks.admission_probe) = Some(AdmissionProbe {
        row: fixture.probe_row,
        observations: Vec::new(),
    });
    let readiness = fixture.service.publication().await;
    // Live embeddings reserve two drivers at concurrency one. Exercise both
    // before the change, then verify their identities rather than assuming reuse.
    for id in 1..=2 {
        fixture.parked(id).await;
        fixture.provider.release.add_permits(1);
        let reply = fixture.wire.reply().await;
        assert_eq!(reply["id"], id);
        assert_eq!(reply["result"]["isError"], false);
        assert!(
            reply["result"]["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hit| hit["source_id"] == "allowed")
        );
        assert_eq!(
            reply["result"]["structuredContent"]["metadata"]["degraded"],
            json!([])
        );
        fixture.settled(0, 1, 2).await;
    }
    fixture.parked(3).await;
    fixture
        .hooks
        .admission_scan
        .enabled
        .store(true, Ordering::SeqCst);
    fixture
        .hooks
        .authorization_output
        .enabled
        .store(true, Ordering::SeqCst);
    fixture
        .head
        .freshness
        .sources
        .iter_mut()
        .find(|source| source.source_id == "allowed")
        .unwrap()
        .permissions_read_at = chrono::DateTime::UNIX_EPOCH;
    let (store, root) = crate::storage::namespace_store(&fixture.config).unwrap();
    store
        .put(
            &root.join("head.json"),
            fyrr_core::write_contract(&fixture.head).into(),
        )
        .await
        .unwrap();
    fixture.provider.release.add_permits(1);
    tokio::time::timeout(
        Duration::from_secs(8),
        fixture.hooks.admission_scan.entered.notified(),
    )
    .await
    .expect("the in-request refresh reaches admission rebuilding");
    fixture.hooks.admission_scan.release.add_permits(1);
    tokio::time::timeout(
        Duration::from_secs(8),
        fixture.hooks.authorization_output.entered.notified(),
    )
    .await
    .expect("the rebuild finishes before final request cleanup");
    {
        let probe = crate::sync::lock(&fixture.hooks.admission_probe);
        let before = probe.as_ref().unwrap().observations.last().unwrap();
        assert!(before.before_cleanup);
        assert_eq!(before.head, fixture.head);
        assert!(
            !before.selected,
            "the mid-rerank rebuild must first remove the expired source"
        );
    }
    fixture.hooks.authorization_output.release.add_permits(1);
    let refused = fixture.wire.reply().await;
    assert_eq!(refused["id"], 3);
    assert_eq!(
        refused["result"]["structuredContent"]["code"],
        "authorization_unavailable"
    );
    fixture.settled(0, 1, 2).await;
    assert_eq!(fixture.service.publication().await, readiness);

    let mut visible = Vec::new();
    for id in 4..=5 {
        fixture.provider.release.add_permits(1);
        fixture
            .wire
            .call(
                id,
                "search",
                json!({
                    "query":"exactneedle", "limit":4, "output":"metadata", "deadline_ms":8000
                }),
            )
            .await;
        let reply = fixture.wire.reply().await;
        assert_eq!(reply["id"], id);
        visible.push(
            reply["result"]["structuredContent"]["hits"]
                .as_array()
                .map(Vec::len),
        );
        fixture.settled(0, 1, 2).await;
    }
    {
        let probe = crate::sync::lock(&fixture.hooks.admission_probe);
        let observations = &probe.as_ref().unwrap().observations;
        assert_eq!(observations.len(), 10);
        let completed: Vec<_> = observations
            .iter()
            .filter(|state| !state.before_cleanup)
            .collect();
        assert_ne!(completed[0].worker, completed[1].worker);
        assert_eq!(completed[0].worker, completed[2].worker);
        assert_eq!(completed[1].worker, completed[3].worker);
        assert_eq!(completed[2].worker, completed[4].worker);
        assert_eq!(completed[2].head, fixture.head);
        assert!(
            !completed[2].selected,
            "final cleanup must preserve rebuilt admission; follow-up hit counts: {visible:?}"
        );
    }
    assert_eq!(
        visible,
        vec![Some(0), Some(0)],
        "both reused workers must answer successfully without expired-source rows"
    );
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3, MCP filters and cancellation before filter installation"]
async fn request_filters_preserve_base_admission_and_worker_reuse_on_real_s3() {
    let mut fixture = ParkedEmbedding::open("10s").await;
    *crate::sync::lock(&fixture.hooks.admission_probe) = Some(AdmissionProbe {
        row: fixture.probe_row,
        observations: Vec::new(),
    });
    // An empty selection is a successful filtered request, not a change to
    // the worker's base admission. It also avoids the embedding provider.
    fixture
        .wire
        .call(
            1,
            "search",
            json!({
                "query":"exactneedle", "authors":["no-such-author"],
                "limit":4, "output":"metadata", "deadline_ms":8000
            }),
        )
        .await;
    let filtered = fixture.wire.reply().await;
    assert_eq!(filtered["id"], 1);
    assert_eq!(filtered["result"]["isError"], false);
    assert_eq!(filtered["result"]["structuredContent"]["hits"], json!([]));
    fixture.settled(0, 1, 2).await;

    fixture.parked(2).await;
    fixture.provider.release.add_permits(1);
    let control = fixture.wire.reply().await;
    assert_eq!(control["id"], 2);
    assert_eq!(control["result"]["isError"], false);
    assert!(
        !control["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    fixture.settled(0, 1, 2).await;

    fixture
        .hooks
        .filter_scan
        .enabled
        .store(true, Ordering::SeqCst);
    fixture
        .wire
        .call(
            3,
            "search",
            json!({
                "query":"exactneedle", "authors":["Ada"],
                "limit":4, "output":"metadata", "deadline_ms":8000
            }),
        )
        .await;
    tokio::time::timeout(
        Duration::from_secs(8),
        fixture.hooks.filter_scan.entered.notified(),
    )
    .await
    .expect("filter construction parks before installing its selection");
    fixture.wire.cancel(3).await;
    let cancelled = fixture.wire.reply().await;
    assert_eq!(cancelled["id"], 3);
    assert_eq!(
        cancelled["result"]["structuredContent"]["code"],
        "cancelled"
    );
    fixture.settled(0, 1, 2).await;
    for id in 4..=5 {
        fixture.parked(id).await;
        fixture.provider.release.add_permits(1);
        let reply = fixture.wire.reply().await;
        assert_eq!(reply["id"], id);
        assert_eq!(reply["result"]["isError"], false);
        assert!(
            !reply["result"]["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        fixture.settled(0, 1, 2).await;
    }
    {
        let probe = crate::sync::lock(&fixture.hooks.admission_probe);
        let observations = &probe.as_ref().unwrap().observations;
        assert_eq!(observations.len(), 9);
        assert!(
            observations.iter().all(|state| state.selected),
            "filtered success and cancellation must never narrow the base admission mask"
        );
        let completed: Vec<_> = observations
            .iter()
            .filter(|state| !state.before_cleanup)
            .collect();
        assert_eq!(completed.len(), 5);
        assert_ne!(completed[0].worker, completed[1].worker);
        assert_eq!(completed[0].worker, completed[2].worker);
        assert_eq!(completed[1].worker, completed[3].worker);
        assert_eq!(completed[2].worker, completed[4].worker);
    }
    fixture.close().await;
}
