use super::*;
use std::sync::atomic::Ordering;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "synthetic publication; cancellation while real excerpt CPU work is parked"]
async fn excerpt_cancellation_retains_capacity_and_batch_scratch_on_real_s3() {
    struct Release(Arc<TestHooks>);

    impl Drop for Release {
        fn drop(&mut self) {
            self.0.excerpt_cpu.release.add_permits(1);
        }
    }

    let path = std::env::var_os("FYRR_TEST_EMBED_CONFIG").expect("embedding fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.concurrency = 1;
    config.reader.version_check = fyrr_core::config::VersionCheck::Off;
    let query = "fixture query";
    let args: SearchInput = serde_json::from_value(json!({
        "queries":[query,query], "limit":10, "output":"excerpt"
    }))
    .unwrap();
    args.validate().unwrap();
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    let budget = reader.preparation_budget.permits.clone();
    let hooks = reader.test_hooks.clone();
    let service = start(reader).await.unwrap();
    let before = budget.available_permits();
    hooks.excerpt_cpu.enabled.store(true, Ordering::SeqCst);
    // Release even if an assertion fails, so the test cannot strand a CPU
    // pool thread during panic cleanup.

    let release = Release(hooks.clone());
    let token = CancellationToken::new();
    let (reply, receiver) = oneshot::channel();
    let permit = Arc::new(service.slots.clone().try_acquire_owned().unwrap());
    let now = Instant::now();
    service
        .sender
        .send(Job {
            request: ToolCall::Search(args),
            reply,
            subject: None,
            _permit: permit,
            response_claims: Arc::default(),
            recall_query_index: 0,
            timing: RequestTiming {
                arrived: now,
                deadline: now + Duration::from_secs(10),
                cancelled: token.clone(),
                version_wait_until: now,
                authorization_build_us: 0,
            },
        })
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        hooks.excerpt_cpu.entered.notified(),
    )
    .await
    .unwrap();
    let held = budget.available_permits();
    assert!(
        held < before,
        "batch scratch was reserved before CPU selection"
    );
    token.cancel();
    let cancelled = tokio::time::timeout(Duration::from_secs(5), receiver)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled.structured_content.unwrap()["code"], "cancelled");
    let scratch = hooks.excerpt_scratch.lock().unwrap().clone();
    assert!(
        scratch.strong_count() > 0,
        "detached CPU work retains the exact batch permit"
    );
    let completed = hooks.lexical_queries_completed.load(Ordering::SeqCst);
    let mut second = enqueue(
        &service,
        ToolCall::Search(
            serde_json::from_value(json!({
                "query":query,"limit":1,"output":"metadata"
            }))
            .unwrap(),
        ),
        false,
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut second)
            .await
            .is_err(),
        "the next native request cannot bypass detached CPU work at cap one"
    );
    assert_eq!(
        hooks.lexical_queries_completed.load(Ordering::SeqCst),
        completed
    );
    drop(release);
    let second = tokio::time::timeout(Duration::from_secs(10), second)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(second.is_error, Some(true));
    assert_eq!(
        scratch.strong_count(),
        0,
        "completed CPU work releases its exact batch permit"
    );
}

async fn generation_publish(
    config: &fyrr_core::config::Config,
    head: &mut fyrr_core::publication::ServingHead,
    manifest: &mut fyrr_core::publication::ContentManifest,
) {
    use object_store::ObjectStoreExt;
    let mut dataset = crate::storage::open_version(
        &crate::storage::dataset_uri(config),
        config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        manifest.lance_version,
        None,
    )
    .await
    .unwrap();
    // Real native versions retire the preceding admission cache. Merely
    // changing publication IDs would keep its paid generation claim alive.
    dataset.restore().await.unwrap();
    manifest.lance_version = dataset.version().version;
    // The fixture claims no leased transaction: its lineage ends where it starts.
    manifest.lineage.base_version = manifest.lance_version;
    manifest.lineage.recovery_version = None;
    manifest.lineage.transactions.clear();
    let (store, root) = crate::storage::namespace_store(config).unwrap();
    let dataset_root = root.clone().join("dataset.lance");
    let mut paths = vec![dataset.manifest_location().path.clone()];
    if let Some(transaction) = &dataset.manifest().transaction_file {
        paths.push(
            dataset_root
                .clone()
                .join("_transactions")
                .join(transaction.as_str()),
        );
    }
    for path in paths {
        let relative = crate::storage::relative_path(&path, &dataset_root);
        if !manifest.files.iter().any(|file| file.path == relative) {
            manifest.files.push(fyrr_core::envelope::FileRef {
                path: relative,
                size_bytes: store.head(&path).await.unwrap().size,
            });
        }
    }
    manifest.previous_publication_id = Some(manifest.publication_id.clone());
    manifest.sequence += 1;
    manifest.publication_id = format!("generation-proof-{}", manifest.sequence);
    manifest.committed_at = chrono::Utc::now();
    let path = format!("publications/{}.json", manifest.publication_id);
    store
        .put(
            &object_store::path::Path::from(format!("{root}/{path}")),
            fyrr_core::write_contract(manifest).into(),
        )
        .await
        .unwrap();
    head.content = Some(fyrr_core::publication::ContentPointer {
        publication_id: manifest.publication_id.clone(),
        sequence: manifest.sequence,
        lance_version: manifest.lance_version,
        manifest: path,
        committed_at: manifest.committed_at,
    });
    store
        .put(
            &root.join("head.json"),
            fyrr_core::write_contract(head).into(),
        )
        .await
        .unwrap();
}

#[path = "tests/embedding.rs"]
mod embedding;
#[path = "tests/generations.rs"]
mod generations;

async fn reader() -> (PreparedReader, SearchInput, Vec<f32>) {
    let config = fyrr_core::config::load(std::path::Path::new(
        &std::env::var("FYRR_TEST_RECALL_CONFIG").unwrap(),
    ))
    .unwrap()
    .config;
    let query = std::env::var("FYRR_TEST_RECALL_QUERY").unwrap();
    let client = crate::embedding::EmbeddingClient::new(&config.embedding, None).unwrap();
    let vector = client
        .embed(
            &[&query],
            crate::embedding::Role::Query,
            &mut Default::default(),
        )
        .await
        .unwrap()
        .remove(0);
    let args = serde_json::from_value(json!({"query":query,"limit":1})).unwrap();
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    assert_eq!(
        reader.generation_state,
        crate::generation::GenerationState::WarmReady
    );
    (reader, args, vector)
}
async fn enqueue(
    service: &McpService,
    request: ToolCall,
    cancelled: bool,
) -> oneshot::Receiver<CallToolResult> {
    if let ToolCall::Search(args) = &request {
        args.validate().unwrap();
    }
    let permit = Arc::new(service.slots.clone().try_acquire_owned().unwrap());
    let (reply, receiver) = oneshot::channel();
    let now = Instant::now();
    let token = CancellationToken::new();
    if cancelled {
        token.cancel();
    }
    service
        .sender
        .send(Job {
            request,
            reply,
            subject: None,
            _permit: permit,
            response_claims: Arc::default(),
            recall_query_index: 0,
            timing: RequestTiming {
                arrived: now,
                deadline: now + Duration::from_secs(10),
                cancelled: token,
                version_wait_until: now,
                authorization_build_us: 0,
            },
        })
        .await
        .unwrap();
    receiver
}
async fn search(service: &McpService, args: &SearchInput) {
    let reply = enqueue(service, ToolCall::Search(args.clone()), false)
        .await
        .await
        .unwrap();
    assert_ne!(
        reply.is_error,
        Some(true),
        "tool error code: {}",
        reply
            .structured_content
            .as_ref()
            .map_or(Value::Null, |value| value["code"].clone())
    );
}
async fn hundred(service: &McpService, args: &SearchInput) {
    for _ in 0..100 {
        search(service, args).await;
    }
}
async fn event(events: &mut tokio::sync::broadcast::Receiver<Value>, key: &str) -> Value {
    loop {
        let event = tokio::time::timeout(Duration::from_secs(30), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(event.as_object().unwrap().values().all(Value::is_number));
        if event.get(key).is_some() {
            return event;
        }
    }
}
async fn reference_rows(reader: &PreparedReader, vector: &[f32]) -> Vec<(String, f32)> {
    let mut scan = reader.dataset.scan();
    let query = arrow_array::FixedSizeListArray::new(
        Arc::new(arrow_schema::Field::new(
            "item",
            arrow_schema::DataType::Float32,
            false,
        )),
        i32::try_from(vector.len()).unwrap(),
        Arc::new(arrow_array::Float32Array::from(vector.to_vec())),
        None,
    );
    scan.nearest("vector", &query, 75)
        .unwrap()
        .use_index(false)
        .distance_metric(lance_linalg::distance::DistanceType::Cosine)
        .filter(&reader.request_predicate().unwrap())
        .unwrap()
        .prefilter(true)
        .with_row_id_prefilter(reader.eligible.clone());
    scan.project(&["root_id", "_distance"]).unwrap();
    let batch = scan.try_into_batch().await.unwrap();
    let distances = batch
        .column_by_name("_distance")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Float32Array>()
        .unwrap();
    crate::search::strings(&batch, "root_id")
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, root)| (root.unwrap().to_owned(), distances.value(i)))
        .collect()
}

#[tokio::test]
#[ignore = "real S3 publication supplied by the sampler fixture"]
async fn recall_sampler_duplicate_id_preserves_original_sdk_response() {
    async fn send(write: &mut tokio::io::WriteHalf<tokio::io::DuplexStream>, value: Value) {
        write
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
        write.flush().await.unwrap();
    }

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let (reader, args, _) = reader().await;
    let hooks = reader.test_hooks.clone();
    let service = start(reader).await.unwrap();
    let claims: StdioResponseClaims = Arc::default();
    let (client, server) = tokio::io::duplex(1024 * 1024);
    let (server_read, server_write) = tokio::io::split(server);
    let transport = StdioTransport {
        inner: rmcp::transport::async_rw::AsyncRwTransport::new_server(server_read, server_write),
        slots: service.slots.clone(),
        cancellations: service.cancellations.clone(),
        response_claims: claims.clone(),
    };
    let serving = tokio::spawn(async move {
        service
            .serve(transport)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let (client_read, mut client_write) = tokio::io::split(client);
    let mut lines = tokio::io::BufReader::new(client_read).lines();

    send(
        &mut client_write,
        json!({"jsonrpc":"2.0",
        "id":0,
        "method":"initialize",
        "params":{"protocolVersion":"2025-03-26",
        "capabilities":{},
        "clientInfo":{"name":"duplicate-test",
        "version":"1"}}}),
    )
    .await;
    assert_eq!(
        serde_json::from_str::<Value>(&lines.next_line().await.unwrap().unwrap()).unwrap()["id"],
        0
    );
    send(
        &mut client_write,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await;
    hooks
        .completion
        .enabled
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let query = args.query.unwrap();
    let call = json!({"jsonrpc":"2.0",
        "id":7,
        "method":"tools/call",
        "params":{"name":"search",
        "arguments":{"queries":[query,
        query],
        "limit":1,
        "text_hits":0}}});
    send(&mut client_write, call.clone()).await;
    hooks.completion.entered.notified().await;
    send(&mut client_write, call).await;
    let duplicate: Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(duplicate["id"], 7);
    assert_eq!(duplicate["result"]["structuredContent"]["code"], "busy");
    assert_eq!(claims.lock().unwrap().len(), 1);
    send(
        &mut client_write,
        json!({"jsonrpc":"2.0", "id":"7", "method":"tools/list"}),
    )
    .await;
    let distinct: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(distinct["id"], "7");
    assert!(distinct["result"]["tools"].is_array());
    assert_eq!(claims.lock().unwrap().len(), 1);
    hooks
        .completion
        .enabled
        .store(false, std::sync::atomic::Ordering::SeqCst);
    hooks.completion.release.add_permits(1);
    let original: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(original["id"], 7);
    assert!(original["result"]["structuredContent"]["hits"].is_array());
    assert!(claims.lock().unwrap().is_empty());
    drop(client_write);
    drop(lines);
    serving.await.unwrap();
}

#[tokio::test]
#[ignore = "real S3 publication supplied by the sampler fixture"]
async fn recall_sampler_multiquery_counts_once_and_retains_one_slot() {
    let (reader, args, _) = reader().await;
    let hooks = reader.test_hooks.clone();
    let query = args.query.as_ref().unwrap();
    let batch: SearchInput = serde_json::from_value(
        json!({"queries":[query,query,query,query],"limit":1,"text_hits":0}),
    )
    .unwrap();
    let service = start(reader).await.unwrap();
    let mut events = service.sample_events.subscribe();
    let initial_slots = service.slots.available_permits();
    hooks
        .completion
        .enabled
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let response = enqueue(&service, ToolCall::Search(batch.clone()), false).await;
    hooks.completion.entered.notified().await;
    assert_eq!(
        service.slots.available_permits(),
        initial_slots - 1,
        "four queries consume one whole-call slot"
    );
    hooks
        .completion
        .enabled
        .store(false, std::sync::atomic::Ordering::SeqCst);
    hooks.completion.release.add_permits(1);
    assert_ne!(response.await.unwrap().is_error, Some(true));
    for _ in 1..100 {
        search(&service, &batch).await;
    }
    let measured = event(&mut events, "recall_at_75").await;
    assert_eq!(measured["recall_successful"], 100);
    assert_eq!(measured["recall_selected"], 1);
    assert_eq!(measured["recall_completed"], 1);
}

#[tokio::test]
#[ignore = "real S3 publication supplied by the sampler fixture"]
async fn recall_sampler_cadence_poor_ann_and_same_filtered_reference() {
    let (mut reader, args, vector) = reader().await;
    let reference = reference_rows(&reader, &vector).await;
    assert_eq!(reference.len(), 75);
    reader.record_stages();
    reader.search(&args).await.unwrap();
    let stages = reader.take_stages().unwrap();
    let dense = stages["dense"].as_array().unwrap();
    assert_eq!(dense.len(), 75);
    assert!(dense.iter().all(|row| row[0] == "allowed"));
    let approximate: Vec<_> = dense
        .iter()
        .map(|row| row[1].as_str().unwrap().to_owned())
        .collect();
    let recall = |k: usize| {
        approximate[..k]
            .iter()
            .filter(|root| {
                reference.iter().any(|(exact, distance)| {
                    exact == *root && (k == 75 || *distance <= reference[k - 1].1)
                })
            })
            .count() as f64
            / k as f64
    };
    assert!(
        recall(75) < 0.8,
        "fixture must deliberately have poor ANN recall: {}",
        recall(75)
    );
    let service = start(reader).await.unwrap();
    let mut events = service.sample_events.subscribe();
    for _ in 0..99 {
        search(&service, &args).await;
    }
    let cancelled = enqueue(&service, ToolCall::Search(args.clone()), true)
        .await
        .await
        .unwrap();
    assert_eq!(cancelled.structured_content.unwrap()["code"], "cancelled");
    let lexical: SearchInput =
        serde_json::from_value(json!({"query":"uncached-lexical-only","limit":1})).unwrap();
    search(&service, &lexical).await;
    let listing: SearchInput =
        serde_json::from_value(json!({"sources":["allowed"],"order":"updated_at","limit":1}))
            .unwrap();
    search(&service, &listing).await;
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    search(&service, &args).await;
    let measured = event(&mut events, "recall_at_75").await;
    assert_eq!(measured["recall_successful"], 100);
    assert_eq!(measured["recall_selected"], 1);
    assert_eq!(measured["recall_completed"], 1);
    assert_eq!(measured["recall_coverage"], 0.01);
    for (key, k) in [("recall_at_10", 10), ("recall_at_75", 75)] {
        assert!((measured[key].as_f64().unwrap() - recall(k)).abs() < 1e-12);
    }
    let excluded: Vec<_> = reference[..10]
        .iter()
        .map(|(root, _)| json!({"source_id":"allowed","root_id":root}))
        .collect();
    let filtered: SearchInput = serde_json::from_value(
        json!({"query":args.query,"limit":1,"sources":["allowed"],"exclude":excluded}),
    )
    .unwrap();
    hundred(&service, &filtered).await;
    let measured = event(&mut events, "recall_at_75").await;
    assert_eq!(measured["recall_successful"], 200);
    assert_eq!(measured["recall_completed"], 2);
    assert_eq!(measured["recall_at_10"], 1.0);
    assert_eq!(measured["recall_at_75"], 1.0);
}

#[tokio::test]
#[ignore = "real S3 publication supplied by the sampler fixture"]
async fn recall_sampler_cancellation_during_native_scan_reuses_worker() {
    let (reader, args, _) = reader().await;
    let hooks = reader.test_hooks.clone();
    let read = reader.mirror.sample_read.clone();
    hooks.sample_plan.store(true, Ordering::SeqCst);
    let service = start(reader).await.unwrap();
    let mut events = service.sample_events.subscribe();
    for _ in 0..99 {
        search(&service, &args).await;
    }
    // Finish dense foreground I/O before arming the one-shot read boundary.
    hooks.completion.enabled.store(true, Ordering::SeqCst);
    let hundredth = enqueue(&service, ToolCall::Search(args.clone()), false).await;
    hooks.completion.entered.notified().await;
    read.enabled.store(true, Ordering::SeqCst);
    hooks.completion.enabled.store(false, Ordering::SeqCst);
    hooks.completion.release.add_permits(1);
    assert_ne!(hundredth.await.unwrap().is_error, Some(true));
    let started = event(&mut events, "recall_started").await;
    assert_eq!(started["recall_selected"], 1);
    tokio::time::timeout(Duration::from_secs(30), read.entered.notified())
        .await
        .unwrap();
    // The native flat scan is held at a data read, before it can finish.
    // Keep that read held while the same worker completes foreground work.
    let listing: SearchInput =
        serde_json::from_value(json!({"sources":["allowed"],"order":"updated_at","limit":1}))
            .unwrap();
    let foreground = enqueue(&service, ToolCall::Search(listing), false).await;
    let skipped = event(&mut events, "recall_skip_cancelled").await;
    assert_eq!(skipped["recall_selected"], 1);
    assert_eq!(skipped["recall_completed"], 0);
    assert!(skipped.get("recall_at_10").is_none());
    assert!(skipped.get("recall_at_75").is_none());
    assert_ne!(foreground.await.unwrap().is_error, Some(true));
    assert_eq!(read.release.available_permits(), 0);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    read.release.add_permits(1);
}

#[tokio::test]
#[ignore = "real S3 publication supplied by the sampler fixture"]
async fn recall_sampler_cancellation_and_time_cap_discard_completed_scan() {
    for reason in ["recall_skip_cancelled", "recall_skip_timeout"] {
        let (reader, args, _) = reader().await;
        let hooks = reader.test_hooks.clone();
        hooks.sample.enabled.store(true, Ordering::SeqCst);
        let service = start(reader).await.unwrap();
        let mut events = service.sample_events.subscribe();
        hundred(&service, &args).await;
        let started = event(&mut events, "recall_started").await;
        assert_eq!(started["recall_in_flight"], 1);
        tokio::time::timeout(Duration::from_secs(30), hooks.sample.entered.notified())
            .await
            .unwrap(); // Real flat scan finished; recording remains held.
        if reason == "recall_skip_cancelled" {
            let listing: SearchInput = serde_json::from_value(
                json!({"sources":["allowed"],"order":"updated_at","limit":1}),
            )
            .unwrap();
            search(&service, &listing).await;
        }
        let skipped = event(&mut events, reason).await;
        assert_eq!(skipped["recall_completed"], 0);
        assert_eq!(skipped["recall_coverage"], 0.0);
        assert!(skipped.get("recall_at_75").is_none());
        // A new successful foreground request proves the worker and pin were returned.
        hooks.sample.enabled.store(false, Ordering::SeqCst);
        search(&service, &args).await;
    }
}

#[tokio::test]
#[ignore = "real S3 publication supplied by the sampler fixture"]
async fn recall_sampler_skips_queued_foreground_and_preparation_without_queue() {
    for reason in ["recall_skip_foreground_queued", "recall_skip_preparation"] {
        let (reader, args, _) = reader().await;
        let hooks = reader.test_hooks.clone();
        let service = start(reader).await.unwrap();
        let mut events = service.sample_events.subscribe();
        for _ in 0..99 {
            search(&service, &args).await;
        }
        if reason == "recall_skip_preparation" {
            hooks.preparation.enabled.store(true, Ordering::SeqCst);
            let refresh = {
                let service = service.clone();
                tokio::spawn(async move { service.refresh().await })
            };
            hooks.preparation.entered.notified().await;
            search(&service, &args).await;
            let skipped = event(&mut events, reason).await;
            assert_eq!(skipped["recall_selected"], 1);
            assert_eq!(skipped["recall_completed"], 0);
            hooks.preparation.enabled.store(false, Ordering::SeqCst);
            hooks.preparation.release.add_permits(1);
            refresh.await.unwrap().unwrap();
        } else {
            hooks.completion.enabled.store(true, Ordering::SeqCst);
            let completing = enqueue(&service, ToolCall::Search(args.clone()), false).await;
            hooks.completion.entered.notified().await;
            let mut queued_events = service.queue_events.subscribe();
            let listing: SearchInput = serde_json::from_value(
                json!({"sources":["allowed"],"order":"updated_at","limit":1}),
            )
            .unwrap();
            let queued = enqueue(&service, ToolCall::Search(listing), false).await;
            queued_events.recv().await.unwrap();
            assert_eq!(service.slots.available_permits(), 0);
            hooks.completion.enabled.store(false, Ordering::SeqCst);
            hooks.completion.release.add_permits(1);
            assert_ne!(completing.await.unwrap().is_error, Some(true));
            assert_ne!(queued.await.unwrap().is_error, Some(true));
            let skipped = event(&mut events, reason).await;
            assert_eq!(skipped["recall_successful"], 100);
            assert_eq!(skipped["recall_completed"], 0);
        }
    }
}
