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
                strong: false,
                _permit: Arc::new(service.slots.clone().try_acquire_owned().unwrap()),
                timing: RequestTiming {
                    arrived: now,
                    deadline: now + Duration::from_secs(300),
                    cancelled: CancellationToken::new(),
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
