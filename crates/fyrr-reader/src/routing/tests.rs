use super::*;
#[test]
fn router_yields_to_next_warm_claim() {
    let centroids = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        8,
        Arc::new(Float32Array::from(vec![0.25; 16])),
        None,
    );
    // This build fits in two pages, but must leave one warm page free.
    let pool = Arc::new(tokio::sync::Semaphore::new(2));
    assert!(matches!(
        CentroidRouter::build(&centroids, pool.clone(), 4096),
        Err(RoutingFallback::MemoryBudget)
    ));
    assert_eq!(pool.available_permits(), 2);
    let next = pool.try_acquire_many_owned(1).unwrap();
    assert_eq!(next.num_permits(), 1);
}
#[test]
fn auto_threshold_is_only_a_strategy_decision() {
    for (n, expected) in [
        (8191, RoutingStrategy::Exact),
        (8192, RoutingStrategy::Rabitq),
    ] {
        assert_eq!(strategy(RoutingStrategy::Auto, n), expected);
        assert_eq!(strategy(RoutingStrategy::Exact, n), RoutingStrategy::Exact);
        assert_eq!(
            strategy(RoutingStrategy::Rabitq, n),
            RoutingStrategy::Rabitq
        );
    }
}
