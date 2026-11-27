use super::*;
use std::sync::atomic::Ordering;

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
            strong: false,
            timing: RequestTiming {
                arrived: now,
                deadline: now + Duration::from_secs(10),
                cancelled: token,
                authorization_build_us: 0,
            },
        })
        .await
        .unwrap();
    receiver
}
async fn search(service: &McpService, args: &SearchInput) {
    let reply = enqueue(service, ToolCall::Search(Box::new(args.clone())), false)
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
/// Root ids of the dense candidates `search` ranks, nearest first.
async fn dense_roots(reader: &mut PreparedReader, args: &SearchInput) -> Vec<String> {
    reader.record_stages();
    reader.search(args).await.unwrap();
    let stages = reader.take_stages().unwrap();
    let dense = stages["dense"].as_array().unwrap();
    assert_eq!(dense.len(), 75);
    assert!(dense.iter().all(|row| row[0] == "allowed"));
    dense
        .iter()
        .map(|row| row[1].as_str().unwrap().to_owned())
        .collect()
}
async fn reference_rows(
    reader: &PreparedReader,
    vector: &[f32],
    rows: usize,
) -> Vec<(String, f32)> {
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
    scan.nearest("vector", &query, rows)
        .unwrap()
        .use_index(false)
        .distance_metric(lance_linalg::distance::DistanceType::Cosine)
        .filter(&reader.request_predicate().unwrap())
        .unwrap()
        .prefilter(true)
        .with_row_id_prefilter(reader.admission.eligible.clone());
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
async fn recall_sampler_cadence_poor_ann_and_same_filtered_reference() {
    let (mut reader, args, vector) = reader().await;
    // The exact order of the subject's rows: 75 for the plain request, and
    // ten more for the request that excludes the ten nearest.
    let exact = reference_rows(&reader, &vector, 85).await;
    assert_eq!(exact.len(), 85);
    let reference = &exact[..75];
    let approximate = dense_roots(&mut reader, &args).await;
    // `k == 75` compares the sets; a smaller `k` compares by distance, so ties
    // at the cutoff count as found.
    let recall = |approximate: &[String], reference: &[(String, f32)], k: usize| {
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
    // The filtered index path probes until it has seen as many selected rows
    // as an unfiltered query, so recall on this fixture is high but the
    // RaBitQ shortlist still loses rows. The measured recall below only
    // proves something if it is not trivially 1.
    assert!(
        recall(&approximate, reference, 75) < 1.0,
        "fixture must have imperfect ANN recall: {}",
        recall(&approximate, reference, 75)
    );
    // The same request excluding the ten nearest: its reference is the same
    // filtered scan, so the sampler must measure against `exact[10..]`.
    let excluded: Vec<_> = exact[..10]
        .iter()
        .map(|(root, _)| json!({"source_id":"allowed","root_id":root}))
        .collect();
    let filtered: SearchInput = serde_json::from_value(
        json!({"query":args.query,"limit":1,"sources":["allowed"],"exclude":excluded}),
    )
    .unwrap();
    let filtered_approximate = dense_roots(&mut reader, &filtered).await;
    assert!(
        exact[..10]
            .iter()
            .all(|(root, _)| !filtered_approximate.contains(root)),
        "an excluded root was returned"
    );
    let service = start(reader).await.unwrap();
    let mut events = service.sample_events.subscribe();
    for _ in 0..99 {
        search(&service, &args).await;
    }
    let cancelled = enqueue(&service, ToolCall::Search(Box::new(args.clone())), true)
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
        let expected = recall(&approximate, reference, k);
        assert!((measured[key].as_f64().unwrap() - expected).abs() < 1e-12);
    }
    hundred(&service, &filtered).await;
    let measured = event(&mut events, "recall_at_75").await;
    assert_eq!(measured["recall_successful"], 200);
    assert_eq!(measured["recall_completed"], 2);
    for (key, k) in [("recall_at_10", 10), ("recall_at_75", 75)] {
        let expected = recall(&filtered_approximate, &exact[10..], k);
        assert!((measured[key].as_f64().unwrap() - expected).abs() < 1e-12);
    }
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
    let hundredth = enqueue(&service, ToolCall::Search(Box::new(args.clone())), false).await;
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
    let foreground = enqueue(&service, ToolCall::Search(Box::new(listing)), false).await;
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
async fn warming_waits_for_a_pin_that_outlives_its_request() {
    let mut config = fyrr_core::config::load(std::path::Path::new(
        &std::env::var("FYRR_TEST_RECALL_CONFIG").unwrap(),
    ))
    .unwrap()
    .config;
    // An empty mirror, so that warming has files to download.
    let mirror = std::env::temp_dir().join(format!("fyrr-warming-pin-{}", std::process::id()));
    config.reader.mirror_path = Some(mirror.to_string_lossy().into_owned());
    let reader = PreparedReader::open(&config).await.unwrap();
    let lease = reader.mirror_pin.clone();
    let hooks = reader.test_hooks.clone();
    hooks.cache_warming.enabled.store(true, Ordering::SeqCst);
    hooks.completion.enabled.store(true, Ordering::SeqCst);
    let service = start(reader).await.unwrap();
    let exercise = async {
        hooks.cache_warming.entered.notified().await;
        // A request runs, and a pin outlives it, as excerpt work can.
        let reply = enqueue(&service, ToolCall::Report, false).await;
        hooks.completion.entered.notified().await;
        let residue = lease.request().unwrap();
        hooks.cache_warming.release.add_permits(1);
        hooks.warming_busy.notified().await;
        hooks.completion.release.add_permits(1);
        reply.await.unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(2),
                hooks.cache_warming.entered.notified()
            )
            .await
            .is_err(),
            "warming started again while a pin was held"
        );
        drop(residue);
        hooks.cache_warming.entered.notified().await;
        hooks.cache_warming.enabled.store(false, Ordering::SeqCst);
        hooks.cache_warming.release.add_permits(1);
        while service.publication().await["warm_ready"] != true {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(45), exercise)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(mirror);
}
