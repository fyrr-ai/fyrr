use super::*;
use crate::ReaderError;
fn row(source: &str, root: &str, record: &str, score: f64) -> Candidate {
    Candidate {
        row_id: 0,
        source: source.into(),
        root: root.into(),
        record: record.into(),
        score,
        lexical: None,
        distance: None,
    }
}
#[test]
fn a_search_without_text_keeps_the_order_by_distance_whatever_the_weights() {
    let dense = vec![
        row("a", "z", "1", 0.2),
        row("a", "y", "1", 0.9),
        row("a", "y", "2", 0.8),
        row("a", "x", "1", 0.5),
    ];
    let order = |rows: Vec<Candidate>| {
        rows.iter()
            .map(|r| (r.root.clone(), r.score))
            .collect::<Vec<_>>()
    };
    // One row per document, best first, scores untouched.
    assert_eq!(
        order(dense_only(dense.clone(), 10, &Bonus::new()).unwrap()),
        [("y".into(), 0.9), ("x".into(), 0.5), ("z".into(), 0.2)]
    );
    assert_eq!(dense_only(dense, 2, &Bonus::new()).unwrap().len(), 2);
    // The fusion with a dense weight of zero loses that order.
    let none = RetrievalConfig {
        dense_weight: 0.0,
        ..RetrievalConfig::default()
    };
    let fused = compose(
        vec![row("a", "z", "1", 0.2), row("a", "y", "1", 0.9)],
        vec![],
        &Composition::default(),
        10,
        &none,
        &Bonus::new(),
    )
    .unwrap();
    assert!(fused.iter().all(|r| r.score == 0.0));
}

#[test]
fn a_fused_hit_keeps_the_raw_lexical_score_and_distance_of_its_lanes() {
    // The dense lane scores a hit by minus its distance.
    let dense = vec![row("a", "x", "1", -0.25), row("a", "y", "1", -0.5)];
    let lexical = vec![row("a", "x", "1", 7.5), row("a", "z", "1", 3.0)];
    let fused = compose(
        dense,
        lexical,
        &Composition::default(),
        10,
        &RetrievalConfig::default(),
        &Bonus::new(),
    )
    .unwrap();
    let raw = |root: &str| {
        let hit = fused.iter().find(|r| r.root == root).unwrap();
        (hit.lexical, hit.distance)
    };
    assert_eq!(raw("x"), (Some(7.5), Some(0.25)));
    assert_eq!(raw("y"), (None, Some(0.5)));
    assert_eq!(raw("z"), (Some(3.0), None));
}

#[test]
fn composition_cases() {
    let slots = Composition {
        fusion: 1,
        dense: 3,
        lexical: 0,
    };
    for (name, dense, lexical, config, expected, scores) in [
        (
            "constant lanes and union before grouping",
            vec![row("a", "x", "1", 4.0), row("b", "y", "1", 4.0)],
            vec![row("a", "x", "2", 9.0)],
            Composition::default(),
            Ok(vec![("a", "x", "1"), ("b", "y", "1")]),
            vec![0.25, 0.25],
        ),
        (
            "source cap and unique document slots",
            vec![
                row("a", "same", "same", 3.0),
                row("a", "two", "1", 2.0),
                row("a", "three", "1", 1.0),
                row("b", "same", "same", 0.0),
            ],
            vec![],
            slots.clone(),
            Ok(vec![
                ("a", "same", "same"),
                ("a", "two", "1"),
                ("a", "three", "1"),
                ("b", "same", "same"),
            ]),
            vec![],
        ),
        (
            "nonfinite score",
            vec![row("a", "x", "1", f64::NAN)],
            vec![],
            slots,
            Err(ReaderError::StorageUnavailable),
            vec![],
        ),
    ] {
        let result = compose(
            dense,
            lexical,
            &config,
            if expected.is_err() { 1 } else { 75 },
            &RetrievalConfig::default(),
            &Bonus::new(),
        );
        match expected {
            Err(error) => assert_eq!(result.unwrap_err(), error, "{name}"),
            Ok(keys) => {
                let result = result.unwrap();
                assert_eq!(
                    result.iter().map(Candidate::key).collect::<Vec<_>>(),
                    keys,
                    "{name}"
                );
                for (row, score) in result.iter().zip(scores) {
                    assert_eq!(row.score, score, "{name}");
                }
            }
        }
    }
    let rows: Vec<_> = (0..50)
        .map(|i| row("a", &format!("{i:02}"), "0", 50.0 - f64::from(i)))
        .collect();
    let single = compose(
        rows.clone(),
        vec![],
        &Composition::default(),
        50,
        &RetrievalConfig::default(),
        &Bonus::new(),
    )
    .unwrap();
    assert_eq!(single.len(), 50);
    assert!(
        single
            .iter()
            .enumerate()
            .all(|(i, r)| r.root == format!("{i:02}"))
    );
    let mut multi = rows;
    multi.extend([row("b", "b1", "0", -1.0), row("b", "b2", "0", -2.0)]);
    for (name, rows, limit) in [
        ("spread and refill", multi, 50),
        ("limit after composition", single, 3),
    ] {
        let result = compose(
            rows,
            vec![],
            &Composition::default(),
            limit,
            &RetrievalConfig::default(),
            &Bonus::new(),
        )
        .unwrap();
        assert_eq!(result.len(), limit, "{name}");
        if limit == 50 {
            assert_eq!(
                result
                    .iter()
                    .take(4)
                    .map(|r| r.source.as_str())
                    .collect::<Vec<_>>(),
                ["a", "a", "b", "b"]
            );
            assert!(
                result
                    .iter()
                    .skip(4)
                    .enumerate()
                    .all(|(i, r)| r.root == format!("{:02}", i + 2))
            );
        }
    }
    for (weight, first) in [
        (1.0, "dense"),
        (0.0, "lexical"),
        (0.25, "lexical"),
        (0.5, "dense"),
    ] {
        let dense = vec![row("a", "dense", "0", 2.0), row("a", "shared", "0", 1.0)];
        let lexical = vec![row("b", "lexical", "0", 2.0), row("a", "shared", "0", 1.0)];
        let config = Composition {
            fusion: 75,
            dense: 0,
            lexical: 0,
        };
        let retrieval = RetrievalConfig {
            source_cap: 0,
            dense_weight: weight,
            ..RetrievalConfig::default()
        };
        let result = compose(dense, lexical, &config, 75, &retrieval, &Bonus::new()).unwrap();
        assert_eq!(result[0].root, first, "weight={weight}");
        if weight == 0.5 {
            assert_eq!(result[0].score, result[1].score);
            assert_eq!(result.len(), 3);
        }
    }
}

#[test]
fn reciprocal_rank_fusion_sums_weight_over_rank_constant_plus_rank() {
    use fyrr_core::mcp::FusionMethod;
    let fusion = |rank_constant, weights| Fusion {
        method: FusionMethod::Rrf,
        rank_constant,
        weights,
    };
    let lists = || {
        vec![
            vec![row("a", "x", "1", 9.0), row("a", "y", "1", 8.0)],
            vec![row("a", "y", "1", 3.0), row("a", "z", "1", 2.0)],
        ]
    };
    let scores = |fusion: &Fusion| {
        reciprocal_rank_fusion(lists(), fusion)
            .into_iter()
            .map(|(list, hit)| (hit.root, list, hit.score))
            .collect::<Vec<_>>()
    };
    // Rank constant 60 and weight 1 by default: y is second in the first list
    // and first in the second, x and z are first in one list only.
    let default = scores(&fusion(None, None));
    let (x, y, z) = (1.0 / 61.0, 1.0 / 62.0 + 1.0 / 61.0, 1.0 / 62.0);
    assert_eq!(
        default,
        [("y".into(), 1, y), ("x".into(), 0, x), ("z".into(), 1, z)]
    );
    // A weight scales the part of its list; y keeps the list that gave it most.
    let weighted = scores(&fusion(Some(1), Some(vec![3.0, 0.5])));
    assert_eq!(
        weighted,
        [
            ("x".into(), 0, 3.0 / 2.0),
            ("y".into(), 0, 3.0 / 3.0 + 0.5 / 2.0),
            ("z".into(), 1, 0.5 / 3.0),
        ]
    );
}

#[test]
fn a_bonus_lifts_a_row_outside_the_limit_and_zero_bonuses_change_nothing() {
    let roots = |rows: &[Candidate]| rows.iter().map(|r| r.root.clone()).collect::<Vec<_>>();
    let lexical = || {
        // Two sources of three documents each, `w` the weakest of all.
        vec![
            row("s", "a", "1", 9.0),
            row("s", "b", "1", 8.0),
            row("s", "c", "1", 7.0),
            row("t", "x", "1", 3.0),
            row("t", "y", "1", 2.0),
            row("t", "w", "1", 1.0),
        ]
        .into_iter()
        .enumerate()
        .map(|(row_id, row)| Candidate {
            row_id: row_id as u64,
            ..row
        })
        .collect::<Vec<_>>()
    };
    let capped = RetrievalConfig {
        source_cap: 2,
        ..RetrievalConfig::default()
    };
    let run = |bonus: &Bonus, limit| {
        compose(
            vec![],
            lexical(),
            &Composition::default(),
            limit,
            &capped,
            bonus,
        )
        .unwrap()
    };
    // Without a bonus, `w` is outside the top 3.
    let plain = run(&Bonus::new(), 3);
    assert!(!roots(&plain).contains(&"w".to_owned()));
    // A bonus of zero everywhere, however it is spelled, leaves the answer as it was.
    let zeros: Bonus = (0..6).map(|id| (id, 0.0)).collect();
    assert_eq!(roots(&run(&zeros, 3)), roots(&plain));
    assert_eq!(roots(&run(&zeros, 6)), roots(&run(&Bonus::new(), 6)));
    // A bonus of 1 puts `w` first, over the whole candidate set.
    let lifted = run(&Bonus::from([(5, 1.0)]), 3);
    assert_eq!(roots(&lifted)[0], "w");
}

#[test]
fn a_distance_score_is_normalized_before_a_bonus_is_added() {
    // Scores are negated distances: a bonus of 1 would swamp them as they are.
    let dense = || {
        vec![
            row("a", "x", "1", -100.0),
            row("a", "y", "1", -300.0),
            row("a", "z", "1", -500.0),
        ]
        .into_iter()
        .enumerate()
        .map(|(row_id, row)| Candidate {
            row_id: row_id as u64,
            ..row
        })
        .collect::<Vec<_>>()
    };
    let roots = |rows: &[Candidate]| rows.iter().map(|r| r.root.clone()).collect::<Vec<_>>();
    // A small bonus does not reorder, and the reported distance stays raw.
    let small = dense_only(dense(), 3, &Bonus::from([(2, 0.05)])).unwrap();
    assert_eq!(roots(&small), ["x", "y", "z"]);
    assert!(small.iter().all(|r| (0.0..=1.05).contains(&r.score)));
    assert_eq!(small[0].distance, Some(100.0));
    let lifted = dense_only(dense(), 3, &Bonus::from([(2, 1.0)])).unwrap();
    assert_eq!(roots(&lifted), ["z", "x", "y"]);
}
