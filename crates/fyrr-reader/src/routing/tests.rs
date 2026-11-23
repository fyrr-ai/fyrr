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
        let cosine = DenseMetric::Cosine;
        assert_eq!(strategy(RoutingStrategy::Auto, n, cosine), expected);
        assert_eq!(
            strategy(RoutingStrategy::Exact, n, cosine),
            RoutingStrategy::Exact
        );
        assert_eq!(
            strategy(RoutingStrategy::Rabitq, n, DenseMetric::SquaredL2),
            RoutingStrategy::Rabitq
        );
        // The centroid scan does not rank by a dot product: Lance routes.
        assert_eq!(
            strategy(RoutingStrategy::Rabitq, n, DenseMetric::Dot),
            RoutingStrategy::Exact
        );
    }
}

#[test]
fn probes_stop_at_nprobes_for_full_partitions_and_widen_for_sparse_ones() {
    let ranked = |n: u32| (0..n).map(|p| (p, p as f32));
    // Unrestricted subject, partitions of average size: exactly nprobes.
    let full = vec![10; 8];
    assert_eq!(take_probes(ranked(8), &full, 3, 30).0, vec![0, 1, 2]);
    // Sparse subject: one admitted row per partition, every other partition
    // empty. It keeps probing until it has seen as many rows as the target.
    let sparse: Vec<u32> = (0..40).map(|p| u32::from(p % 2 == 0)).collect();
    let (ids, distances, consumed) = take_probes(ranked(40), &sparse, 3, 5);
    assert_eq!(ids, vec![0, 2, 4, 6, 8]);
    assert_eq!(distances, vec![0.0, 2.0, 4.0, 6.0, 8.0]);
    assert_eq!(consumed, 9);
    // A subject with fewer rows than the target probes all its partitions.
    let (ids, _, consumed) = take_probes(ranked(40), &sparse, 3, 1_000);
    assert_eq!(ids.len(), 20);
    assert_eq!(consumed, 40);
    // Large admitted partitions never cut the probe count below nprobes.
    assert_eq!(take_probes(ranked(8), &[1_000; 8], 3, 30).0, vec![0, 1, 2]);
}

#[test]
fn router_takes_the_nearest_admitted_partitions_until_the_target() {
    let (n, dim) = (64u32, 16i32);
    let (rows, width) = (n as usize, dim as usize);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        ((state >> 33) as f32 / (1u64 << 31) as f32) - 0.5
    };
    let values: Vec<f32> = (0..rows * width).map(|_| next()).collect();
    let centroids = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        dim,
        Arc::new(Float32Array::from(values.clone())),
        None,
    );
    let pool = Arc::new(tokio::sync::Semaphore::new(4096));
    let router = CentroidRouter::build(&centroids, pool, 0).unwrap();
    for query_no in 0..8 {
        let query: Vec<f32> = (0..width).map(|_| next()).collect();
        let exact = |admitted: &[u32]| {
            let mut ranked: Vec<_> = (0..n)
                .filter(|p| admitted[*p as usize] > 0)
                .map(|p| {
                    let at = p as usize * width;
                    (
                        OrderedFloat(l2_distance(&query, &values[at..at + width])),
                        p,
                    )
                })
                .collect();
            ranked.sort_unstable();
            ranked.into_iter().map(|r| r.1).collect::<Vec<_>>()
        };
        let query = Arc::new(Float32Array::from(query.clone()));
        // Every partition admitted and of the target size: nprobes nearest.
        let full = vec![10; rows];
        let (ids, _) = router.select(query.clone(), &full, 4, 40).unwrap();
        assert_eq!(ids.len(), 4, "query {query_no}");
        // Half the partitions admitted with one row each: the router widens
        // to 20 partitions, all of them admitted and nearest first.
        let sparse: Vec<u32> = (0..n).map(|p| u32::from(p % 2 == 1)).collect();
        let (ids, distances) = router.select(query.clone(), &sparse, 2, 20).unwrap();
        assert_eq!(ids, exact(&sparse)[..20].to_vec(), "query {query_no}");
        assert!(distances.windows(2).all(|w| w[0] <= w[1]));
        // Too few admitted partitions for the shortlist: native routing.
        let few: Vec<u32> = (0..n).map(|p| u32::from(p < 7)).collect();
        assert!(matches!(
            router.select(query, &few, 2, 20),
            Err(RoutingFallback::EligibleShortlist)
        ));
    }
}

/// Under squared L2 the query is not normalized: the router ranks the centroids
/// by their Euclidean distance to a query of any length, as the namespace's
/// metric does, and not by the angle that a cosine namespace's normalized query
/// gives.
#[test]
fn router_ranks_a_query_of_any_length_by_euclidean_distance() {
    let (n, dim) = (64u32, 16i32);
    let (rows, width) = (n as usize, dim as usize);
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut next = || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        ((state >> 33) as f32 / (1u64 << 31) as f32) - 0.5
    };
    // Centroids of different lengths, so that the angle and the distance differ.
    let values: Vec<f32> = (0..rows)
        .flat_map(|p| {
            let length = 0.25 + (p % 8) as f32;
            (0..width)
                .map(|_| length * 2.0 * next())
                .collect::<Vec<_>>()
        })
        .collect();
    let centroids = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        dim,
        Arc::new(Float32Array::from(values.clone())),
        None,
    );
    let router =
        CentroidRouter::build(&centroids, Arc::new(tokio::sync::Semaphore::new(4096)), 0).unwrap();
    let sparse: Vec<u32> = (0..n).map(|p| u32::from(p % 2 == 1)).collect();
    for (query_no, scale) in [0.05f32, 1.0, 20.0].into_iter().enumerate() {
        let query: Vec<f32> = (0..width).map(|_| scale * next()).collect();
        let mut exact: Vec<_> = (0..n)
            .filter(|p| sparse[*p as usize] > 0)
            .map(|p| {
                let at = p as usize * width;
                (
                    OrderedFloat(l2_distance(&query, &values[at..at + width])),
                    p,
                )
            })
            .collect();
        exact.sort_unstable();
        let (ids, distances) = router
            .select(Arc::new(Float32Array::from(query)), &sparse, 2, 20)
            .unwrap();
        assert_eq!(
            ids,
            exact.iter().take(20).map(|r| r.1).collect::<Vec<_>>(),
            "query {query_no} of scale {scale}"
        );
        // The distances are the squared Euclidean ones, not an angle.
        assert!(distances.iter().zip(&exact).all(|(d, e)| *d == e.0.0));
    }
}
