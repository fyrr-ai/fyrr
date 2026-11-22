use super::*;
use crate::ReaderError;
use fyrr_core::limits::mcp::RESPONSE_MIN_HITS_PER_QUERY as FLOOR;

/// `per_query[q]` hits of query `q` in round-robin order; the first
/// `texted` hits of each query carry a 400-byte excerpt.
fn output(per_query: &[usize], texted: usize) -> SearchOutput {
    let mut out: SearchOutput = serde_json::from_str(include_str!(
        "../../../../fixtures/mcp/examples/search-output.json"
    ))
    .unwrap();
    let hit = out.hits[0].clone();
    out.hits.clear();
    for rank in 0..per_query.iter().copied().max().unwrap_or(0) {
        for (query, count) in per_query.iter().enumerate() {
            if rank < *count {
                let mut hit = hit.clone();
                hit.query_index = Some(u32::try_from(query).unwrap());
                hit.root_id = format!("q{query}r{rank}");
                if rank < texted {
                    hit.excerpt.as_mut().unwrap().text = "x".repeat(400);
                } else {
                    hit.excerpt = None;
                }
                out.hits.push(hit);
            }
        }
    }
    out
}

fn per_query(out: &SearchOutput, query: u32) -> usize {
    out.hits
        .iter()
        .filter(|hit| hit.query_index == Some(query))
        .count()
}

fn texted(out: &SearchOutput) -> usize {
    out.hits.iter().filter(|hit| hit.excerpt.is_some()).count()
}

#[test]
fn wire_size_accounting_matches_serde_with_escaped_text_and_counters() {
    for count in [0, 1, 2, 9, 10, 20] {
        let mut out = output(&[count, count], count);
        for hit in &mut out.hits {
            if let Some(excerpt) = &mut hit.excerpt {
                excerpt.text = "\"\\\n😃é".repeat(7);
            }
        }
        for dropped in [0, 9, 10, 99, 100, u32::MAX] {
            out.truncated = Some(Truncation {
                text_dropped: dropped,
                hits_dropped: dropped,
            });
            out.metadata.omitted_bytes = u64::from(dropped);
            let bytes: usize = out.hits.iter().map(|hit| hit_wire_len(hit).unwrap()).sum();
            assert_eq!(
                sized_response_len(&out, bytes).unwrap(),
                response_len(&out).unwrap()
            );
        }
    }
}

#[test]
fn counters_include_hits_removed_when_their_digits_grow() {
    let full = output(&[20, 20], 4);
    for max in [4096, 8192, 12000, 16000, response_len(&full).unwrap() - 1] {
        let mut cut = full.clone();
        fit_response(&mut cut, max).unwrap();
        let removed: u64 = full
            .hits
            .iter()
            .filter(|hit| !cut.hits.iter().any(|kept| kept.root_id == hit.root_id))
            .map(|hit| serialized_len(hit).unwrap() as u64)
            .sum();
        assert!(cut.metadata.omitted_bytes >= full.metadata.omitted_bytes + removed);
        assert!(response_len(&cut).unwrap() <= max);
    }
}

#[test]
fn response_budget_cases() {
    #[derive(Clone, Copy, Debug)]
    enum Case {
        Untouched,
        Impossible,
        FairTail,
        Asymmetric,
        DropText,
        YieldFloor,
        MaximumBatch,
    }
    let full = output(&[8, 8, 8], 4);
    let size = response_len(&full).unwrap();
    let mut shorter = full.clone();
    shorter.hits.pop();
    let tail_hit = size - response_len(&shorter).unwrap();
    let asymmetric = output(&[12, FLOOR], 4);
    let asymmetric_max = response_len(&asymmetric).unwrap()
        - 5 * serialized_len(asymmetric.hits.last().unwrap()).unwrap();
    let mut maximum = output(&[20; 16], 4);
    for hit in &mut maximum.hits {
        if let Some(excerpt) = &mut hit.excerpt {
            excerpt.text = "\"\\\n😃".repeat(512);
        }
    }
    for (case, full, max) in [
        (Case::Untouched, full.clone(), size),
        (Case::Impossible, output(&[8], 4), 10),
        (Case::FairTail, full.clone(), size - 6 * tail_hit),
        (Case::Asymmetric, asymmetric, asymmetric_max),
        (
            Case::DropText,
            full.clone(),
            response_len(&output(&[FLOOR, FLOOR, FLOOR], 4)).unwrap() - 300,
        ),
        (
            Case::YieldFloor,
            full,
            response_len(&output(&[2, 2, 2], 0)).unwrap(),
        ),
        (Case::MaximumBatch, maximum, 48_000),
    ] {
        let mut cut = full.clone();
        let input_bytes = serde_json::to_vec(&full).unwrap().len();
        BUDGET_SERIALIZED_BYTES.with(|count| count.set(0));
        let result = fit_response(&mut cut, max);
        let serialized = BUDGET_SERIALIZED_BYTES.with(|count| count.get());
        if matches!(case, Case::MaximumBatch) {
            assert!(serialized > 0, "the serialization observer must run");
            assert!(
                serialized <= 8 * input_bytes,
                "trimming serialized {serialized} bytes for {input_bytes} input bytes"
            );
        }
        if matches!(case, Case::Impossible) {
            assert_eq!(result, Err(ReaderError::Busy));
            assert!(cut.hits.is_empty());
            continue;
        }
        result.unwrap();
        assert!(response_len(&cut).unwrap() <= max, "{case:?}");
        assert!(
            cut.hits.windows(2).all(|pair| {
                let position =
                    |hit: &SearchHit| full.hits.iter().position(|h| h.root_id == hit.root_id);
                position(&pair[0]) < position(&pair[1])
            }),
            "{case:?}"
        );
        match case {
            Case::Untouched => {
                assert_eq!(cut, full);
                assert!(cut.truncated.is_none());
            }
            Case::FairTail => {
                let truncated = cut.truncated.as_ref().unwrap();
                assert_eq!(truncated.text_dropped, 0);
                assert!(truncated.hits_dropped >= 6);
                assert_eq!(texted(&cut), texted(&full));
                assert_eq!(cut.hits[0].root_id, "q0r0");
                let counts = [per_query(&cut, 0), per_query(&cut, 1), per_query(&cut, 2)];
                assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
                assert!(counts.iter().all(|count| *count >= FLOOR));
                assert!(cut.metadata.omitted_bytes > full.metadata.omitted_bytes);
            }
            Case::Asymmetric => {
                assert_eq!(per_query(&cut, 1), FLOOR);
                assert!((FLOOR..12).contains(&per_query(&cut, 0)));
                assert_eq!(cut.truncated.as_ref().unwrap().text_dropped, 0);
            }
            Case::DropText => {
                let truncated = cut.truncated.as_ref().unwrap();
                assert_eq!(truncated.hits_dropped as usize, 24 - 3 * FLOOR);
                assert!((1..3 * FLOOR).contains(&(truncated.text_dropped as usize)));
                assert_eq!(cut.hits.len(), 3 * FLOOR);
                assert!(cut.hits[..3].iter().all(|hit| hit.excerpt.is_some()));
                assert!(cut.hits.last().unwrap().excerpt.is_none());
            }
            Case::YieldFloor => {
                assert_eq!(
                    cut.truncated.as_ref().unwrap().text_dropped as usize,
                    3 * FLOOR
                );
                assert!(cut.hits.len() < 3 * FLOOR && !cut.hits.is_empty());
                assert_eq!(cut.hits[0].root_id, "q0r0");
                let mut none = full;
                assert_eq!(fit_response(&mut none, 10), Err(ReaderError::Busy));
                assert!(none.hits.is_empty());
                // A budget that holds the envelope but not one hit is an error,
                // not an empty page.
                let mut one = output(&[1], 0);
                let envelope = SearchOutput {
                    hits: vec![],
                    truncated: Some(Truncation {
                        text_dropped: 0,
                        hits_dropped: 1,
                    }),
                    metadata: one.metadata.clone(),
                };
                let max = response_len(&envelope).unwrap();
                assert_eq!(fit_response(&mut one, max), Err(ReaderError::Busy));
            }
            Case::MaximumBatch => {}
            Case::Impossible => unreachable!(),
        }
    }
}
