use super::*;

fn assert_fallback(result: SearchOutput, exact: &[SearchHit], reason: RoutingFallback) {
    assert_eq!(result.hits, exact);
    assert_eq!(
        result.metadata.telemetry.routing_strategy,
        Some(RoutingMethod::Exact)
    );
    assert_eq!(result.metadata.telemetry.routing_fallback, Some(reason));
}
#[tokio::test]
#[ignore = "real publication supplied by the dense_path generation fixture"]
async fn serving_minimum_admission_on_real_s3() {
    let path = std::env::var_os("FYRR_TEST_GENERATION_CONFIG").unwrap();
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    let mut reference = PreparedReader::open(&config).await.unwrap();
    let args: SearchInput = serde_json::from_value(
        serde_json::json!({"query":"fixture query","sources":["incremental"],"limit":10}),
    )
    .unwrap();
    let expected = reference
        .search_input(&args, None, None)
        .await
        .unwrap()
        .hits;
    assert!(!expected.is_empty());
    let warm = reference.preparation_reservation.warm_bytes();
    let cache = PreparationReservation::cache_bytes(&reference.manifest);
    let overhead = config.reader.memory_max_bytes.as_u64() - reference.preparation_budget.cap;
    let minimum = PreparationReservation::minimum_bytes(overhead, warm, cache);
    drop(reference);
    // Freeze the startup envelope, as the serving dispatcher does. Opening
    // separate processes would change measured RSS and hence the boundary.
    for (budget, accepted) in [(minimum - 1, false), (minimum, true)] {
        config.reader.memory_max_bytes = fyrr_core::units::ByteSize::bytes(budget);
        let bytes = (budget - overhead) / 4096 * 4096;
        let pool = Arc::new(tokio::sync::Semaphore::new((bytes / 4096) as usize));
        let opened = PreparedReader::open_shared(
            &config,
            None,
            crate::generation::MemoryPool {
                cap: bytes,
                permits: pool.clone(),
            },
        )
        .await;
        if accepted {
            let mut reader = opened.unwrap();
            assert_eq!(reader.readiness()["ready"], true);
            assert_eq!(
                reader.readiness()["native_cache_degradation"],
                serde_json::Value::Null
            );
            assert_eq!(
                reader.search_input(&args, None, None).await.unwrap().hits,
                expected
            );
        } else {
            assert_eq!(opened.err(), Some(ReaderError::PreparationBudgetImpossible));
            assert_eq!(
                pool.available_permits() as u64 * 4096,
                bytes,
                "refusal precedes generation/cache claims"
            );
        }
    }
}

#[tokio::test]
#[ignore = "real publication supplied by the dense_path routing fixture"]
async fn routing_fallbacks_on_real_s3() {
    let path = std::env::var_os("FYRR_TEST_ROUTING_CONFIG").expect("routing fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.routing = RoutingStrategy::Exact;
    let args: SearchInput =
        serde_json::from_value(serde_json::json!({"query":"routing-only query","limit":10}))
            .unwrap();
    let mut reader = PreparedReader::open(&config).await.unwrap();
    let exact = reader.search_input(&args, None, None).await.unwrap().hits;
    // An exact generation owns no router. Exercise the absent lookup on
    // that real pin/admission, without injecting a fake router state.
    reader.config.reader.routing = RoutingStrategy::Rabitq;
    assert!(reader.centroid_routers.get().is_none());
    assert_fallback(
        reader.search_input(&args, None, None).await.unwrap(),
        &exact,
        RoutingFallback::Absent,
    );
    // Existing claims stay paid; exhaust only the remaining shared pool.
    let pool = reader.preparation_budget.permits.clone();
    let held = pool
        .clone()
        .try_acquire_many_owned(u32::try_from(pool.available_permits()).unwrap())
        .unwrap();
    reader.prepare_routing().await.unwrap();
    assert!(
        reader
            .centroid_routers
            .get()
            .unwrap()
            .iter()
            .all(|r| matches!(r, Err(RoutingFallback::MemoryBudget)))
    );
    // Routing failure is retained by the OnceCell. Release the artificial
    // pressure before search reserves its own native execution memory.
    drop(held);
    assert_fallback(
        reader.search_input(&args, None, None).await.unwrap(),
        &exact,
        RoutingFallback::MemoryBudget,
    );
    drop(reader);

    config.reader.routing = RoutingStrategy::Rabitq;
    let mut reader = PreparedReader::open(&config).await.unwrap();
    assert!(
        reader
            .centroid_routers
            .get()
            .unwrap()
            .iter()
            .all(Result::is_ok)
    );
    // A nonempty filter still chooses the filtered native path when it matches
    // every row. Equal masks do not permit unfiltered partition-map routing.
    let all_sources: Vec<_> = config.sources.iter().map(|source| &source.id).collect();
    let filtered: SearchInput = serde_json::from_value(serde_json::json!({
        "query":"routing-only query", "sources":all_sources, "limit":10
    }))
    .unwrap();
    let base_rows = reader.eligible.max_len();
    let selected = reader.search_input(&filtered, None, None).await.unwrap();
    assert_eq!(selected.hits, exact);
    assert_eq!(
        selected.metadata.telemetry.routing_strategy,
        Some(RoutingMethod::Exact)
    );
    assert_eq!(selected.metadata.telemetry.routing_fallback, None);
    assert_eq!(reader.eligible.max_len(), base_rows);
    assert!(reader.filter_selection.is_none());

    reader.config.reader.nprobes = 65536;
    let mut reference = config.clone();
    reference.reader.routing = RoutingStrategy::Exact;
    reference.reader.nprobes = 65536;
    let mut exact_reader = PreparedReader::open(&reference).await.unwrap();
    let exact = exact_reader
        .search_input(&args, None, None)
        .await
        .unwrap()
        .hits;
    assert_fallback(
        reader.search_input(&args, None, None).await.unwrap(),
        &exact,
        RoutingFallback::EligibleShortlist,
    );
    // The only injected state is failure at the optional build boundary;
    // roots, index executor, admission and storage are still the real ones.
    reader.centroid_routers = Arc::new(tokio::sync::OnceCell::new());
    reader
        .centroid_routers
        .set(
            reader
                .vector_roots
                .get()
                .unwrap()
                .iter()
                .map(|_| Err(RoutingFallback::BuildFailed))
                .collect(),
        )
        .unwrap();
    assert_fallback(
        reader.search_input(&args, None, None).await.unwrap(),
        &exact,
        RoutingFallback::BuildFailed,
    );
}
