use super::*;

#[tokio::test]
#[ignore = "real publications supplied by the dense_path mirror fixture"]
async fn generation_inflight_pin_and_mirror_eviction_on_real_s3() {
    async fn fetch(service: &McpService, root: &str) -> oneshot::Receiver<CallToolResult> {
        let (reply, response) = oneshot::channel();
        let now = Instant::now();
        service
            .sender
            .send(Job {
                request: ToolCall::Fetch(
                    serde_json::from_value(json!({
                        "source_id":"incremental", "root_id":root, "deadline_ms":300000
                    }))
                    .unwrap(),
                ),
                reply,
                subject: None,
                response_claims: Arc::default(),
                recall_query_index: 0,
                _permit: Arc::new(service.slots.clone().try_acquire_owned().unwrap()),
                timing: RequestTiming {
                    arrived: now,
                    deadline: now + Duration::from_secs(300),
                    cancelled: CancellationToken::new(),
                    version_wait_until: now,
                    authorization_build_us: 0,
                },
            })
            .await
            .unwrap();
        response
    }

    use object_store::ObjectStoreExt;
    let config = fyrr_core::config::load(std::path::Path::new(
        &std::env::var("FYRR_TEST_MIRROR_CONFIG").unwrap(),
    ))
    .unwrap()
    .config;
    let bundle: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("FYRR_TEST_MIRROR_PUBLICATIONS").unwrap()).unwrap(),
    )
    .unwrap();
    let old: fyrr_core::publication::ServingHead =
        serde_json::from_value(bundle["old"].clone()).unwrap();
    let latest: fyrr_core::publication::ServingHead =
        serde_json::from_value(bundle["latest"].clone()).unwrap();
    let hashes: std::collections::BTreeSet<String> =
        serde_json::from_value(bundle["obsolete_hashes"].clone()).unwrap();
    let (store, root) = crate::storage::namespace_store(&config).unwrap();
    let head_path = root.join("head.json");
    // Only this fixture's mutable head is rewound; immutable objects stay intact.
    store
        .put(&head_path, fyrr_core::write_contract(&old).into())
        .await
        .unwrap();
    let reader = PreparedReader::open(&config).await.unwrap();
    let hooks = reader.test_hooks.clone();
    let service = start(reader).await.unwrap();

    for root in ["a", "b"] {
        let result = fetch(&service, root).await.await.unwrap();
        assert_ne!(result.is_error, Some(true));
        assert!(
            !result.structured_content.unwrap()["documents"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    // Initial warming must finish before holding the sole old foreground pin.
    while service.publication().await["warm_ready"] != true {
        tokio::task::yield_now().await;
    }
    let old_blobs: Vec<_> = std::fs::read_dir(config.reader.mirror_path.as_deref().unwrap())
        .unwrap()
        .flat_map(|dir| std::fs::read_dir(dir.unwrap().path()).unwrap())
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            path.is_file()
                .then(|| std::fs::read(&path).unwrap())
                .filter(|bytes| hashes.contains(&crate::embedding::digest(bytes)))
                .map(|_| path)
        })
        .collect();
    assert!(!old_blobs.is_empty(), "old content is physically cached");
    hooks.completion.enabled.store(true, Ordering::SeqCst);
    let mut held = fetch(&service, "a").await;
    hooks.completion.entered.notified().await;
    assert!(
        matches!(
            held.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "old response is held on its generation pin"
    );
    // Future requests pass the gate, while this request stays held.
    hooks.completion.enabled.store(false, Ordering::SeqCst);
    store
        .put(&head_path, fyrr_core::write_contract(&latest).into())
        .await
        .unwrap();
    service.refresh().await.unwrap();
    assert_eq!(
        service.publication().await["publication_id"],
        latest.content.as_ref().unwrap().publication_id
    );
    let current = fetch(&service, "b").await.await.unwrap();
    assert_ne!(current.is_error, Some(true));
    let current = current.structured_content.unwrap();
    assert!(!current["documents"].as_array().unwrap().is_empty());
    assert_eq!(
        current["metadata"]["publication_id"],
        latest.content.as_ref().unwrap().publication_id
    );
    assert!(
        old_blobs.iter().all(|path| path.exists()),
        "held old pin protects every obsolete blob"
    );
    hooks.completion.release.add_permits(1);
    let previous = held.await.unwrap();
    assert_ne!(previous.is_error, Some(true));
    let previous = previous.structured_content.unwrap();
    assert!(!previous["documents"].as_array().unwrap().is_empty());
    assert_eq!(
        previous["metadata"]["publication_id"],
        old.content.as_ref().unwrap().publication_id
    );
    // Observe automatic deletion itself; no elapsed-time bound or manual eviction.
    while old_blobs.iter().any(|path| path.exists()) {
        tokio::task::yield_now().await;
    }
    assert!(old_blobs.iter().all(|path| !path.exists()));
    let current = fetch(&service, "a").await.await.unwrap();
    assert_ne!(current.is_error, Some(true));
    assert_eq!(
        current.structured_content.unwrap()["metadata"]["publication_id"],
        latest.content.as_ref().unwrap().publication_id
    );
}

struct GenerationProof {
    config: fyrr_core::config::Config,
    head: fyrr_core::publication::ServingHead,
    manifest: fyrr_core::publication::ContentManifest,
    original_head: fyrr_core::publication::ServingHead,
    args: SearchInput,
    expected: Value,
    store: Arc<dyn object_store::ObjectStore>,
    root: object_store::path::Path,
    pool: Arc<tokio::sync::Semaphore>,
    bytes: u64,
    needed: (u64, u64),
}
impl GenerationProof {
    async fn new(curative: bool) -> Self {
        use crate::generation::PreparationReservation;
        use object_store::ObjectStoreExt;
        let mut config = fyrr_core::config::load(std::path::Path::new(
            &std::env::var("FYRR_TEST_GENERATION_CONFIG").unwrap(),
        ))
        .unwrap()
        .config;
        config.reader.routing = fyrr_core::config::RoutingStrategy::Exact;
        config.reader.concurrency = 1;
        config.reader.mirror_max_bytes = 0;
        // Explicit refresh controls switches; no background interval can race them.
        config.reader.version_check = fyrr_core::config::VersionCheck::Off;
        let initial = PreparedReader::open(&config).await.unwrap();
        let mut head = initial.head.clone();
        let original_head = head.clone();
        let mut manifest = initial.manifest.clone();
        // The synthetic nprobes=1 ANN is intentionally imperfect. Compare the
        // known authorized cohort, as the cold-fallback CLI proof does, rather
        // than turning a dispatcher proof into a broad recall measurement.
        let args: SearchInput = serde_json::from_value(
            json!({"query":"fixture query","sources":["incremental"],"limit":10}),
        )
        .unwrap();
        let mut reference = initial;
        let expected = serde_json::to_value(
            reference
                .search_input(&args, None, None)
                .await
                .unwrap()
                .hits,
        )
        .unwrap();
        assert_eq!(expected.as_array().unwrap().len(), 1);
        assert_eq!(expected[0]["root_id"], "a");
        let rows = reference.dataset.count_rows(None).await.unwrap() as u64;
        let cold = PreparationReservation::cold_bytes(&config, rows).div_ceil(4096) * 4096;
        let index = manifest
            .indexes
            .iter()
            .find(|index| (index.name == "vector_idx") != curative)
            .unwrap();
        // A real immutable inventory object grows cache capacity. Only the cold
        // proof grows vector input too; the curative proof leaves its warm claim unchanged.
        let path = format!("_indices/{}/generation-proof.bin", index.uuid);
        let size = 64 * 1024 * 1024;
        let (store, root) = crate::storage::namespace_store(&config).unwrap();
        store
            .put(
                &object_store::path::Path::from(format!("{root}/dataset.lance/{path}")),
                vec![0u8; size].into(),
            )
            .await
            .unwrap();
        manifest.files.push(fyrr_core::envelope::FileRef {
            path,
            size_bytes: size as u64,
        });
        let estimate = PreparationReservation::acquire(
            &config,
            &manifest,
            rows,
            Arc::new(tokio::sync::Semaphore::new(u32::MAX as usize)),
        )
        .unwrap();
        let warm = estimate.warm_bytes();
        drop(estimate);
        let needed = PreparationReservation::cache_bytes(&manifest);
        let bytes = if curative {
            2 * warm + needed.0 + needed.1 + 8 * 1024 * 1024
        } else {
            2 * cold + 16 * 1024 * 1024
        };
        assert!(curative || warm > bytes);
        drop(reference);
        if !curative {
            generation_publish(&config, &mut head, &mut manifest).await;
        }
        let pool = Arc::new(tokio::sync::Semaphore::new((bytes / 4096) as usize));
        Self {
            config,
            head,
            manifest,
            original_head,
            args,
            expected,
            store,
            root,
            pool,
            bytes,
            needed,
        }
    }
    async fn assert_refusals(&mut self) {
        for switch in 0..5 {
            if switch != 0 {
                generation_publish(&self.config, &mut self.head, &mut self.manifest).await;
            }
            assert_eq!(
                PreparedReader::open_shared(
                    &self.config,
                    None,
                    crate::generation::MemoryPool {
                        cap: self.bytes,
                        permits: self.pool.clone()
                    }
                )
                .await
                .err(),
                Some(ReaderError::PreparationBudgetImpossible)
            );
            assert_eq!(self.pool.available_permits() as u64 * 4096, self.bytes);
        }
    }
    async fn assert_handover(&mut self) {
        use crate::generation::PreparationReservation;
        use std::sync::atomic::Ordering;

        let reader = PreparedReader::open_shared(
            &self.config,
            None,
            crate::generation::MemoryPool {
                cap: self.bytes,
                permits: self.pool.clone(),
            },
        )
        .await
        .unwrap();

        let old_needed = PreparationReservation::cache_bytes(&reader.manifest);
        assert!(reader.native_session.capacity.0 >= old_needed.0);
        assert!(reader.native_session.capacity.1 >= old_needed.1);
        assert!(
            reader.native_session.capacity.0 < self.needed.0
                || reader.native_session.capacity.1 < self.needed.1
        );

        let hooks = reader.test_hooks.clone();
        let service = Arc::new(start(reader).await.unwrap());

        // Hold a completed foreground request on its old pin during activation/drain.
        hooks.completion.enabled.store(true, Ordering::SeqCst);
        let old_id = service.publication().await["publication_id"].clone();
        let old_version = service.publication().await["lance_version"].clone();
        let old = enqueue(&service, ToolCall::Search(self.args.clone()), false).await;
        hooks.completion.entered.notified().await;
        generation_publish(&self.config, &mut self.head, &mut self.manifest).await;
        let refreshing = {
            let service = service.clone();
            tokio::spawn(async move { service.refresh().await })
        };

        tokio::time::timeout(Duration::from_secs(60), hooks.handover_entered.notified())
            .await
            .unwrap();
        assert_eq!(hooks.handovers.load(Ordering::SeqCst), 1);
        assert!(
            !refreshing.is_finished(),
            "handover must drain the held foreground pin"
        );

        hooks.completion.enabled.store(false, Ordering::SeqCst);
        hooks.completion.release.add_permits(1);
        let old = old.await.unwrap();
        assert_ne!(old.is_error, Some(true));
        let old_result = old.structured_content.unwrap();
        assert_eq!(old_result["metadata"]["publication_id"], old_id);
        assert_eq!(old_result["metadata"]["content_version"], old_version);
        tokio::time::timeout(Duration::from_secs(60), refreshing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let switches = 3;
        for switch in 0..switches {
            if switch != 0 {
                generation_publish(&self.config, &mut self.head, &mut self.manifest).await;
                tokio::time::timeout(Duration::from_secs(60), service.refresh())
                    .await
                    .unwrap()
                    .unwrap();
            }
            let ready = service.publication().await;
            assert_eq!(ready["publication_id"], self.manifest.publication_id);
            assert_eq!(ready["ready"], true);

            assert!(
                ready["native_cache_available_bytes"].as_u64().unwrap()
                    >= self.needed.0 + self.needed.1
            );

            assert!(ready["preparation_reserved_bytes"].as_u64().unwrap() <= self.bytes);
            let reply = enqueue(&service, ToolCall::Search(self.args.clone()), false)
                .await
                .await
                .unwrap();
            assert_ne!(reply.is_error, Some(true));
            let result = reply.structured_content.unwrap();
            assert_eq!(
                result["metadata"]["publication_id"],
                self.manifest.publication_id
            );
            assert_eq!(
                result["metadata"]["content_version"],
                self.manifest.lance_version
            );
            assert_eq!(result["hits"], self.expected);
            let denied = serde_json::from_value(
                json!({"query":"fixture query","sources":["denied"],"limit":10}),
            )
            .unwrap();
            let reply = enqueue(&service, ToolCall::Search(denied), false)
                .await
                .await
                .unwrap();
            assert_ne!(reply.is_error, Some(true));
            assert_eq!(reply.structured_content.unwrap()["hits"], json!([]));
            let revoked = serde_json::from_value(json!({"query":"fixture query",
            "sources":["incremental"],
            "exclude":[{"source_id":"incremental",
            "root_id":"a"}],
            "limit":10}))
            .unwrap();
            let reply = enqueue(&service, ToolCall::Search(revoked), false)
                .await
                .await
                .unwrap();
            assert_ne!(reply.is_error, Some(true));
            assert_eq!(reply.structured_content.unwrap()["hits"], json!([]));
            assert_eq!(hooks.handovers.load(Ordering::SeqCst), 1);
        }
        {
            let sessions = hooks.sessions.lock().unwrap();
            assert_eq!(sessions.len(), switches + 1);

            assert!(!sessions[0].ptr_eq(&sessions[1]));
            assert!(
                sessions[0].upgrade().is_none(),
                "the old session released its claims"
            );
            assert!(
                sessions[1..]
                    .windows(2)
                    .all(|pair| pair[0].ptr_eq(&pair[1]))
            );
        }
        drop(service);
    }
    async fn restore(self) {
        use object_store::ObjectStoreExt;
        self.store
            .put(
                &self.root.join("head.json"),
                fyrr_core::write_contract(&self.original_head).into(),
            )
            .await
            .unwrap();
    }
}
async fn generation_proof(curative: bool) {
    let mut proof = GenerationProof::new(curative).await;
    if curative {
        proof.assert_handover().await;
    } else {
        proof.assert_refusals().await;
    }
    proof.restore().await;
}

#[tokio::test]
#[ignore = "real publication supplied by the dense_path generation fixture"]
async fn generation_undersized_five_publications_refused_on_real_s3() {
    generation_proof(false).await;
}

#[tokio::test]
#[ignore = "real publication supplied by the dense_path generation fixture"]
async fn generation_exactly_one_curative_handover_on_real_s3() {
    generation_proof(true).await;
}

#[tokio::test]
#[ignore = "real publication supplied by the dense_path routing fixture"]
async fn routing_reserve_switch_on_real_s3() {
    use fyrr_core::{
        config::RoutingStrategy, mcp::RoutingFallback, publication::*, write_contract,
    };
    use object_store::ObjectStoreExt;
    let mut config = fyrr_core::config::load(std::path::Path::new(
        &std::env::var("FYRR_TEST_ROUTING_CONFIG").unwrap(),
    ))
    .unwrap()
    .config;
    config.reader.routing = RoutingStrategy::Exact;
    let mut reader = PreparedReader::open(&config).await.unwrap();
    let args: SearchInput =
        serde_json::from_value(json!({"query":"routing-only query","limit":10})).unwrap();
    let exact = reader.search_input(&args, None, None).await.unwrap().hits;
    let warm = reader.preparation_reservation.warm_bytes() / 4096;
    let pool = reader.preparation_budget.permits.clone();
    assert!(pool.available_permits() as u64 > warm);
    // Model other paid allocations: precisely one next warm claim remains.
    let _held = pool
        .clone()
        .try_acquire_many_owned(u32::try_from(pool.available_permits() as u64 - warm).unwrap())
        .unwrap();
    let centroids = reader.vector_roots.get().unwrap()[0]
        .ivf_model()
        .centroids_array()
        .unwrap();
    // Establish that router residency + scratch fits if the reserve is ignored.
    drop(crate::routing::CentroidRouter::build(centroids, pool.clone(), 0).unwrap());
    assert_eq!(pool.available_permits() as u64, warm);
    reader.config.reader.routing = RoutingStrategy::Rabitq;
    reader.prepare_routing().await.unwrap();
    let result = reader.search_input(&args, None, None).await.unwrap();
    assert_eq!(
        result.metadata.telemetry.routing_fallback,
        Some(RoutingFallback::MemoryBudget)
    );
    assert_eq!(result.hits, exact);

    // Publish through the fixture's writable namespace store.
    let (store, root) = crate::storage::namespace_store(&config).unwrap();
    let mut head = reader.head.clone();
    let mut manifest = reader.manifest.clone();
    let hooks = reader.test_hooks.clone();
    let service = start(reader).await.unwrap();
    // New immutable publication, identical native files and rows: its full
    // warm claim is exactly equal, without estimating a writer's index growth.
    manifest.previous_publication_id = Some(manifest.publication_id.clone());
    manifest.sequence += 1;
    manifest.publication_id = format!("routing-reserve-{}", manifest.sequence);
    manifest.committed_at = chrono::Utc::now();
    let path = format!("publications/{}.json", manifest.publication_id);
    store
        .put(
            &object_store::path::Path::from(format!("{root}/{path}")),
            write_contract(&manifest).into(),
        )
        .await
        .unwrap();
    head.content = Some(ContentPointer {
        publication_id: manifest.publication_id.clone(),
        sequence: manifest.sequence,
        lance_version: manifest.lance_version,
        manifest: path,
        committed_at: manifest.committed_at,
    });
    store
        .put(&root.join("head.json"), write_contract(&head).into())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), service.refresh())
        .await
        .unwrap()
        .unwrap();
    let reply = enqueue(&service, ToolCall::Search(args), false)
        .await
        .await
        .unwrap();
    assert_ne!(reply.is_error, Some(true));
    let result = reply.structured_content.unwrap();
    assert_eq!(
        result["metadata"]["publication_id"],
        manifest.publication_id
    );
    assert_eq!(result["hits"], serde_json::to_value(exact).unwrap());
    assert_eq!(hooks.handovers.load(Ordering::SeqCst), 0);
}
