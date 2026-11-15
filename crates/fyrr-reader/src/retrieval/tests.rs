use super::*;
use crate::ReaderError;
fn row(source: &str, root: &str, record: &str, score: f64) -> Candidate {
    Candidate {
        row_id: 0,
        source: source.into(),
        root: root.into(),
        record: record.into(),
        score,
    }
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
            Ok(vec![("a", "x", "2"), ("b", "y", "1")]),
            vec![0.375, 0.125],
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
    for (weight, first) in [(1.0, "dense"), (0.0, "lexical"), (0.5, "dense")] {
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
        let result = compose(dense, lexical, &config, 75, &retrieval).unwrap();
        assert_eq!(result[0].root, first, "weight={weight}");
        if weight == 0.5 {
            assert_eq!(result[0].score, result[1].score);
            assert_eq!(result.len(), 3);
        }
    }
}
