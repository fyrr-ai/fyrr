use super::*;
use std::collections::BTreeSet;
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

/// A router over `n` deterministic pseudo-random 16-dimensional centroids.
fn synthetic_router(n: usize) -> (CentroidRouter, impl FnMut() -> Arc<Float32Array>) {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((state >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    };
    let values: Vec<f32> = (0..n * 16).map(|_| next()).collect();
    let centroids = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        16,
        Arc::new(Float32Array::from(values)),
        None,
    );
    let pool = Arc::new(tokio::sync::Semaphore::new(1 << 20));
    let router = CentroidRouter::build(&centroids, pool, 0).unwrap();
    (router, move || {
        Arc::new(Float32Array::from_iter_values((0..16).map(|_| next())))
    })
}

/// Which partitions an admission pattern admits, out of `n`.
fn admission_patterns(n: usize, shortlist: usize) -> Vec<(&'static str, Vec<bool>)> {
    vec![
        ("all", vec![true; n]),
        ("none", vec![false; n]),
        ("scattered", (0..n).map(|i| i % 3 == 0).collect()),
        ("sparse", (0..n).map(|i| i % 11 == 5).collect()),
        (
            "block",
            (0..n).map(|i| (n / 2..n / 2 + 9).contains(&i)).collect(),
        ),
        ("one short", (0..n).map(|i| i < shortlist - 1).collect()),
        ("exact", (0..n).map(|i| i >= n - shortlist).collect()),
        ("single", (0..n).map(|i| i == 17).collect()),
    ]
}

#[tokio::test]
async fn lazy_router_selection_equals_the_selection_over_complete_flags() {
    let n = 200;
    let (router, mut query) = synthetic_router(n);
    for nprobes in [1, 2, 3, 7] {
        for (name, allowed) in admission_patterns(n, 4 * nprobes) {
            for _ in 0..25 {
                let key = query();
                let eager = router.select(key.clone(), &allowed, nprobes);
                let asked = std::cell::RefCell::new(BTreeSet::new());
                let lazy = router
                    .select_lazily(&key, n, nprobes, |id| {
                        asked.borrow_mut().insert(id);
                        std::future::ready(Ok::<_, ()>(allowed[id]))
                    })
                    .await
                    .unwrap();
                assert_eq!(lazy, eager, "{name} nprobes={nprobes}");
                // A subject who reads many partitions near the query is
                // admitted without asking about the rest.
                if matches!(name, "all" | "scattered") {
                    assert!(asked.borrow().len() < n, "{name}: asked every partition");
                }
                if lazy == Err(RoutingFallback::EligibleShortlist) {
                    // Only a complete walk proves too few readable partitions.
                    assert!(asked.borrow().len() == n || n < 4 * nprobes);
                }
            }
        }
    }
    // A router over fewer partitions than the shortlist never asks.
    let (small, mut query) = synthetic_router(5);
    let lazy = small
        .select_lazily(
            &query(),
            5,
            2,
            |_| -> std::future::Ready<Result<bool, ()>> { panic!("asked") },
        )
        .await
        .unwrap();
    assert_eq!(lazy, Err(RoutingFallback::EligibleShortlist));
    // A refused answer is an error, never an admission.
    let refused = router
        .select_lazily(&query(), n, 1, |_| {
            std::future::ready(Err::<bool, _>("unreadable"))
        })
        .await;
    assert_eq!(refused, Err("unreadable"));
}

#[tokio::test]
async fn lazy_exact_walk_equals_the_walk_over_complete_flags() {
    let n = 200usize;
    let ranked: Vec<(u32, f32)> = (0..n)
        .map(|i| (u32::try_from((i * 37) % n).unwrap(), i as f32 * 0.5))
        .collect();
    for nprobes in [1, 2, 3, 7, 16, 65536] {
        for (name, allowed) in admission_patterns(n, 4) {
            let mut selected = Vec::new();
            let mut distances = Vec::new();
            for (id, distance) in &ranked {
                if allowed[*id as usize] {
                    selected.push(*id);
                    distances.push(*distance);
                    if selected.len() == nprobes {
                        break;
                    }
                }
            }
            let asked = std::cell::RefCell::new(0usize);
            let lazy = first_admitted(ranked.iter().copied(), nprobes, |id| {
                *asked.borrow_mut() += 1;
                std::future::ready(Ok::<_, ()>(allowed[id]))
            })
            .await
            .unwrap();
            assert_eq!(lazy, (selected, distances), "{name} nprobes={nprobes}");
            if name == "all" {
                assert_eq!(*asked.borrow(), nprobes.min(n));
            }
        }
    }
}
