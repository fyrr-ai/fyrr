use super::*;
use crate::ReaderError;

fn row(index: usize, score: f64) -> Candidate {
    Candidate {
        row_id: index as u64,
        source: "a".into(),
        root: index.to_string(),
        record: "0".into(),
        score,
        lexical: None,
        distance: None,
    }
}

#[test]
fn both_envelopes_require_exact_finite_index_coverage() {
    for envelope in ["data", "results"] {
        let valid = serde_json::json!({envelope:[{"index":1,
            "relevance_score":0.9},
            {"index":0,
            "relevance_score":0.1}],
            "usage":{"tokens":42}});
        assert_eq!(
            parse_scores(&serde_json::to_vec(&valid).unwrap(), 2).unwrap(),
            [0.1, 0.9]
        );
        for rows in [
            serde_json::json!([]),
            serde_json::json!([{"index":0,"relevance_score":1}]),
            serde_json::json!([{"index":0,"relevance_score":1},{"index":0,"relevance_score":2}]),
            serde_json::json!([{"index":0,"relevance_score":1},{"index":2,"relevance_score":2}]),
            serde_json::json!([{"index":0,"relevance_score":1},{"index":-1,"relevance_score":2}]),
            serde_json::json!([{"index":0,"relevance_score":1},{"index":1,"relevance_score":null}]),
            serde_json::json!([{"index":0,
                "relevance_score":1},
                {"index":1,
                "relevance_score":"NaN"}]),
        ] {
            assert_eq!(
                parse_scores(
                    &serde_json::to_vec(&serde_json::json!({envelope:rows})).unwrap(),
                    2
                ),
                Err(ReaderError::RerankResponseInvalid)
            );
        }
    }
    for bytes in [
        b"[]".as_slice(),
        b"{}",
        b"{\"data\":[],\"results\":[]}",
        b"{\"items\":[]}",
        b"{\"data\":[{\"index\":0,\"relevance_score\":1e999}]}",
    ] {
        assert_eq!(
            parse_scores(bytes, 1),
            Err(ReaderError::RerankResponseInvalid)
        );
    }
}

#[test]
fn pilot_score_blend_and_raw_endpoints_have_stable_ties() {
    let fusion = vec![row(0, 0.9), row(1, 0.6), row(2, 0.1)];
    let scores = [0.2, 0.8, 0.9];
    let mut rows = fusion.clone();
    blend(&mut rows, &scores, 0.5);
    // Pilot analyze.z: zf=[.752331956,.550341648,.212074818],
    // zr=[.197545423,.632664646,.703685971]; mixing puts index 1 first.
    assert_eq!(rows.iter().map(|r| r.row_id).collect::<Vec<_>>(), [1, 0, 2]);
    blend(&mut rows, &[0.2, 0.8, 0.9], 0.0);
    assert_eq!(rows[0].row_id, 1);
    let mut rows = fusion.clone();
    blend(&mut rows, &scores, 1.0);
    assert_eq!(rows.iter().map(|r| r.row_id).collect::<Vec<_>>(), [2, 1, 0]);
    let mut rows = fusion.clone();
    assert!(
        normalized(&[f64::MAX, f64::MAX, -f64::MAX])
            .iter()
            .all(|score| score.is_finite())
    );
    blend(&mut rows, &[f64::MAX, f64::MAX, -f64::MAX], 0.5);
    assert_eq!(rows[0].row_id, 0);
    let mut rows = fusion;
    blend(&mut rows, &[1.0, 1.0, 1.0], 1.0);
    assert_eq!(rows.iter().map(|r| r.row_id).collect::<Vec<_>>(), [0, 1, 2]);
    blend(&mut rows, &[0.0, -0.0, 0.0], 1.0);
    assert_eq!(rows.iter().map(|r| r.row_id).collect::<Vec<_>>(), [0, 1, 2]);
}

#[test]
fn text_window_is_bounded_term_seeking_and_utf8_safe() {
    assert_eq!(window("short text", "text", Some(200)), "short text");
    let long = format!("{}needle haystack{}", "a ".repeat(400), " b".repeat(400));
    assert_eq!(window(&long, "needle", None), long);
    let chosen = window(&long, "Needle", Some(300));
    assert_eq!(chosen.chars().count(), 300);
    assert!(chosen.contains("needle"));
    // No query term anywhere: the head is kept.
    assert_eq!(window(&long, "absent", Some(300)), &long[..300]);
    let wide = "é".repeat(500) + " cible " + &"ü".repeat(500);
    let chosen = window(&wide, "cible", Some(240));
    assert_eq!(chosen.chars().count(), 240);
    assert!(chosen.contains("cible"));
    let tail = "x".repeat(300) + " needle";
    assert!(window(&tail, "needle", Some(200)).ends_with("needle"));
}

#[test]
fn concurrent_reservations_are_capped_and_never_overflow() {
    let config: RerankConfig =
        serde_json::from_value(serde_json::json!({"endpoint":"http://127.0.0.1/rerank",
        "model":"test",
        "candidates":1,
        "weight":1.0,
        "deadline":"1s",
        "spend_limit_microusd":100,
        "max_call_cost_microusd":10}))
        .unwrap();
    let reranker = Reranker::new().unwrap();
    let admitted = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..32 {
            scope.spawn(|| {
                if reranker.reserve(&config).is_ok() {
                    admitted.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(admitted.load(Ordering::Relaxed), 10);
    assert_eq!(reranker.spent.load(Ordering::Relaxed), 100);
    reranker.spent.store(u64::MAX - 1, Ordering::Relaxed);
    assert_eq!(
        reranker.reserve(&config),
        Err(ReaderError::RerankSpendExhausted)
    );
}

#[tokio::test]
async fn payload_and_deadline_skip_before_spend_or_http() {
    let config: RerankConfig =
        serde_json::from_value(serde_json::json!({"endpoint":"http://127.0.0.1:9/rerank",
        "model":"test",
        "candidates":1,
        "weight":1.0,
        "deadline":"1s",
        "spend_limit_microusd":10,
        "max_call_cost_microusd":10}))
        .unwrap();
    let reranker = Reranker::new().unwrap();
    assert_eq!(
        reranker
            .scores(
                &config,
                "q",
                vec!["x".repeat(MAX_TEXT_BYTES)],
                Instant::now() + std::time::Duration::from_secs(1),
                std::future::ready(Ok(())),
            )
            .await,
        Err(ReaderError::RerankPayloadLimit)
    );
    assert_eq!(
        reranker
            .scores(
                &config,
                "q",
                vec!["text".into()],
                Instant::now() - std::time::Duration::from_secs(1),
                std::future::ready(Ok(())),
            )
            .await,
        Err(ReaderError::RerankDeadline)
    );
    assert_eq!(reranker.spent.load(Ordering::Relaxed), 0);
}

#[test]
fn clients_that_share_a_counter_share_the_limit() {
    let config: RerankConfig =
        serde_json::from_value(serde_json::json!({"endpoint":"http://127.0.0.1/rerank",
        "model":"test",
        "candidates":1,
        "weight":1.0,
        "deadline":"1s",
        "spend_limit_microusd":30,
        "max_call_cost_microusd":10}))
        .unwrap();
    let spent = Arc::new(AtomicU64::new(0));
    let (first, second) = (
        Reranker::charging(spent.clone()).unwrap(),
        Reranker::charging(spent).unwrap(),
    );
    assert!(first.reserve(&config).is_ok());
    assert!(second.reserve(&config).is_ok());
    assert!(first.reserve(&config).is_ok());
    assert_eq!(
        second.reserve(&config),
        Err(ReaderError::RerankSpendExhausted)
    );
}
