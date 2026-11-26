use super::*;

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
        Self::open_with_cache(timeout, 0).await
    }
    async fn open_with_cache(timeout: &str, cache_bytes: u64) -> Self {
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
        config.embedding.query_cache_max_bytes = cache_bytes;
        config.embedding.timeout = serde_json::from_value(json!(timeout)).unwrap();
        config.embedding.spend = Some(fyrr_core::config::SpendConfig {
            limit_microusd: 1000,
            max_call_microusd: 10,
        });
        config.reader.concurrency = 1;
        let server = tokio::spawn(async move {
            axum::serve(socket, app).await.unwrap();
        });
        let mut reader = PreparedReader::open(&config).await.unwrap();
        reader.prewarm().await.unwrap();
        let probe_row = reader
            .admission
            .eligible
            .iter_addrs()
            .unwrap()
            .next()
            .unwrap()
            .into();
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

    async fn settled(&self, pins: usize, native: usize, admission: usize) {
        let counts = || {
            (
                self.pin.request_count(),
                self.service.native_slots.available_permits(),
                self.service.slots.available_permits(),
                self.memory.available_permits(),
            )
        };
        let expected = (pins, native, admission, self.baseline_memory);
        tokio::time::timeout(Duration::from_secs(8), async {
            // Worker return and response-claim release happen on separate
            // tasks. Only worker return signals request_finished.
            self.hooks.request_finished.notified().await;
            while counts() != expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "request did not settle (pins, native, admission, memory): expected {expected:?}, observed {:?}",
                counts()
            )
        });
    }
    async fn close(self) {
        self.wire.close().await;
        self.server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication, native scheduler, SDK transport and fake provider"]
async fn embedding_wait_revocation_refuses_old_authorization_on_storage() {
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

/// A policy published while a request waits on its embedding, without a
/// native slot, refuses the request when it is then cancelled: its check finds
/// the change before the reader's head records it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication, fake provider and stdio cancellation"]
async fn a_policy_change_before_a_cancellation_refuses_the_request_on_storage() {
    let mut fixture = ParkedEmbedding::open("10s").await;
    fixture.parked(1).await;
    let config = fixture.config.clone();
    retire_allowed_source(&config, &mut fixture.head).await;
    fixture.wire.cancel(1).await;
    let reply = fixture.wire.reply().await;
    assert_eq!(reply["id"], 1);
    assert_eq!(
        reply["result"]["structuredContent"]["code"], "authorization_unavailable",
        "{reply}"
    );
    assert!(reply["result"]["structuredContent"]["hits"].is_null());
    fixture.close().await;
}

/// Publishes a policy generation that retires the permission-synced `allowed`
/// source: the grant change that makes a reader rebuild its admission.
async fn retire_allowed_source(
    config: &fyrr_core::config::Config,
    head: &mut fyrr_core::publication::ServingHead,
) {
    use fyrr_core::publication::{GenerationPointer, PolicyArtifact, RetiredSource};
    use object_store::ObjectStoreExt;

    assert!(config.sources.iter().any(|source| source.id == "allowed"
        && source.permissions == fyrr_core::config::PermissionsMode::Sync));
    let (store, root) = crate::storage::namespace_store(config).unwrap();
    let mut policy: PolicyArtifact = crate::storage::artifact(store.as_ref(), &root, &head.policy)
        .await
        .unwrap();
    policy.generation += 1;
    policy.retired_sources.insert(
        "allowed".into(),
        RetiredSource {
            retired_at: fyrr_core::clock::now(),
            purged_at: None,
        },
    );
    let bytes = fyrr_core::write_contract(&policy);
    let digest = crate::embedding::digest(&bytes);
    let object = format!("policy/{}-{}.json", policy.generation, &digest[7..]);
    store
        .put(
            &object_store::path::Path::from(format!("{root}/{object}")),
            bytes.into(),
        )
        .await
        .unwrap();
    head.policy = GenerationPointer {
        generation: policy.generation,
        object,
        digest,
    };
    store
        .put(
            &root.join("head.json"),
            fyrr_core::write_contract(head).into(),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication and stdio cancellation during admission"]
async fn cancelled_admission_rebuild_cannot_reuse_stale_stdio_mask_on_storage() {
    interrupted_admission_rebuild(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication and stdio deadline during admission"]
async fn timed_out_admission_rebuild_cannot_reuse_stale_stdio_mask_on_storage() {
    interrupted_admission_rebuild(true).await;
}

async fn interrupted_admission_rebuild(deadline: bool) {
    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("publication fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = 1;
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
    retire_allowed_source(&config, &mut head).await;
    hooks.admission_scan.enabled.store(true, Ordering::SeqCst);
    let mut interrupted = args.clone();
    interrupted["deadline_ms"] = json!(if deadline { 1000 } else { 8000 });
    wire.call(2, "search", interrupted).await;
    tokio::time::timeout(
        Duration::from_secs(8),
        hooks.admission_scan.entered.notified(),
    )
    .await
    .expect("the retired-source rebuild reaches the native scan boundary");
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
        "a reused stdio worker must not expose rows from the retired source"
    );
    wire.close().await;
}

/// A request past its deadline whose cleanup does not end within its 1 s, as
/// on a loaded host, answers `deadline_exceeded`, not
/// `authorization_unavailable`, and the reader serves the next request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 publication and a stdio deadline with a slow cleanup"]
async fn a_slow_cleanup_after_a_deadline_answers_deadline_exceeded_on_storage() {
    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("publication fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = 1;
    config.embedding.reader_live = false;
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    let hooks = reader.test_hooks.clone();
    let service = start(reader).await.unwrap();
    let mut wire = EmbeddingWire::open(service).await;
    let args = json!({"query":"exactneedle","limit":4,"output":"metadata"});
    // The native work outlasts the deadline; the cleanup after it waits on a
    // gate that is never opened.
    hooks.native_work.enabled.store(true, Ordering::SeqCst);
    hooks.cleanup.enabled.store(true, Ordering::SeqCst);
    let mut late = args.clone();
    late["deadline_ms"] = json!(1000);
    wire.call(1, "search", late).await;
    let stopped = wire.reply().await;
    assert_eq!(stopped["id"], 1);
    assert_eq!(
        stopped["result"]["structuredContent"]["code"], "deadline_exceeded",
        "{stopped}"
    );
    assert!(!hooks.cleanup.enabled.load(Ordering::SeqCst));
    wire.call(2, "search", args).await;
    let next = wire.reply().await;
    assert_eq!(next["id"], 2);
    assert_ne!(next["result"]["isError"], true, "{next}");
    wire.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3, MCP stdio lifecycle and a failed native task"]
async fn stdio_terminates_after_scheduler_failure_on_storage() {
    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("publication fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = 1;
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
async fn rerank_refresh_preserves_rebuilt_admission_on_storage() {
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
    let config = fixture.config.clone();
    retire_allowed_source(&config, &mut fixture.head).await;
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
            "the mid-rerank rebuild must first remove the retired source"
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
        "both reused workers must answer successfully without retired-source rows"
    );
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3, MCP filters and cancellation before filter installation"]
async fn request_filters_preserve_base_admission_and_worker_reuse_on_storage() {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real publication and shared embedding cache over MCP"]
async fn empty_selection_metadata_does_not_reveal_query_cache_on_storage() {
    let mut fixture = ParkedEmbedding::open_with_cache("10s", 1024 * 1024).await;
    fixture.parked(1).await;
    fixture.provider.release.add_permits(1);
    let primed = fixture.wire.reply().await;
    assert_eq!(primed["result"]["isError"], false);
    fixture
        .wire
        .send(json!({"jsonrpc":"2.0","id":900,"method":"ping"}))
        .await;
    assert_eq!(fixture.wire.reply().await["id"], 900);
    let mut metadata = Vec::new();
    for (id, query) in [(2, "exactneedle"), (3, "never-cached-probe")] {
        fixture
            .wire
            .call(
                id,
                "search",
                json!({
                    "query":query, "authors":["no-such-author"],
                    "limit":4, "output":"metadata", "deadline_ms":8000
                }),
            )
            .await;
        let reply = fixture.wire.reply().await;
        assert_eq!(reply["result"]["isError"], false);
        assert_eq!(reply["result"]["structuredContent"]["hits"], json!([]));
        metadata.push(reply["result"]["structuredContent"]["metadata"].clone());
        fixture
            .wire
            .send(json!({"jsonrpc":"2.0","id":900,"method":"ping"}))
            .await;
        assert_eq!(fixture.wire.reply().await["id"], 900);
    }
    assert_eq!(metadata[0]["degraded"], metadata[1]["degraded"]);
    assert_eq!(
        serde_json::to_vec(&metadata[0]).unwrap(),
        serde_json::to_vec(&metadata[1]).unwrap()
    );
    fixture
        .wire
        .call(
            4,
            "search",
            json!({
                "query":"exactneedle", "limit":4, "output":"metadata", "deadline_ms":8000
            }),
        )
        .await;
    let cached = fixture.wire.reply().await;
    assert_eq!(cached["result"]["isError"], false);
    for response in [&primed, &cached] {
        let telemetry = &response["result"]["structuredContent"]["metadata"]["telemetry"];
        assert!(
            telemetry.get("stage_us").is_none(),
            "search durations reveal provider waiting: {telemetry}"
        );
        assert!(telemetry.get("rerank_us").is_none());
        assert_eq!(telemetry["queue_wait_ms"], 0);
    }
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real publication, MCP transport and eight independent native requests"]
async fn default_concurrency_serves_eight_simultaneous_clients_on_storage() {
    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("publication fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = fyrr_core::config::ReaderConfig::default().concurrency;
    config.embedding.reader_live = false;
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    let hooks = reader.test_hooks.clone();
    hooks.native_work.enabled.store(true, Ordering::SeqCst);
    let service = start(reader).await.unwrap();
    let mut clients = Vec::new();
    for id in 1..=8 {
        let mut wire = EmbeddingWire::open(service.clone()).await;
        hooks.native_work.enabled.store(true, Ordering::SeqCst);
        wire.call(
            id,
            "search",
            json!({"query":"exactneedle","limit":4,"output":"metadata"}),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), hooks.native_work.entered.notified())
            .await
            .expect("each client gets an independent prepared native reader");
        clients.push(wire);
    }
    hooks.native_work.enabled.store(false, Ordering::SeqCst);
    hooks.native_work.release.add_permits(8);
    for mut wire in clients {
        let reply = wire.reply().await;
        assert_ne!(reply["result"]["isError"], true, "{reply}");
        assert!(
            !reply["result"]["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        wire.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real publication, MCP transport and one namespace saturated by 32 clients"]
async fn saturated_namespace_leaves_another_namespace_every_slot_on_storage() {
    const CLIENTS: u32 = 32;
    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("publication fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = fyrr_core::config::ReaderConfig::default().concurrency;
    config.embedding.reader_live = false;
    let cap = u32::try_from(config.reader.concurrency).unwrap();
    // Two schedulers of one process, as two namespaces have: each its own slots.
    let mut saturated = PreparedReader::open(&config).await.unwrap();
    saturated.prewarm().await.unwrap();
    let hooks = saturated.test_hooks.clone();
    let mut other = PreparedReader::open(&config).await.unwrap();
    other.prewarm().await.unwrap();
    let (saturated, other) = (start(saturated).await.unwrap(), start(other).await.unwrap());
    let query = json!({"query":"exactneedle","limit":4,"output":"metadata"});
    let mut clients = Vec::new();
    // The first `cap` requests execute and stay in native work.
    for id in 1..=cap {
        let mut wire = EmbeddingWire::open(saturated.clone()).await;
        hooks.native_work.enabled.store(true, Ordering::SeqCst);
        wire.call(id, "search", query.clone()).await;
        tokio::time::timeout(Duration::from_secs(5), hooks.native_work.entered.notified())
            .await
            .expect("the saturated namespace executes `cap` requests");
        clients.push(wire);
    }
    // The next `cap` wait in its queue; the rest are refused at once.
    for id in cap + 1..=CLIENTS {
        let mut wire = EmbeddingWire::open(saturated.clone()).await;
        wire.call(id, "search", query.clone()).await;
        clients.push(wire);
    }
    // Every request of the other namespace is answered while the first is full.
    let mut served = Vec::new();
    for id in 1..=cap {
        let mut wire = EmbeddingWire::open(other.clone()).await;
        wire.call(id, "search", query.clone()).await;
        served.push(wire);
    }
    for mut wire in served {
        let reply = wire.reply().await;
        assert_ne!(reply["result"]["isError"], true, "{reply}");
        assert!(
            !reply["result"]["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        wire.close().await;
    }
    hooks.native_work.release.add_permits(cap as usize);
    let (mut answered, mut busy) = (0, 0);
    for mut wire in clients {
        let reply = wire.reply().await;
        if reply["result"]["isError"] == true {
            assert_eq!(reply["result"]["structuredContent"]["code"], "busy");
            busy += 1;
        } else {
            answered += 1;
        }
        wire.close().await;
    }
    assert_eq!((answered, busy), (2 * cap, CLIENTS - 2 * cap));
}
