//! Candidate-cutoff determinism through separate production reader processes.
use super::coordination::{command, worker_sync};
use super::*;

pub(super) fn twins(count: usize) -> (PathBuf, FixtureRoot) {
    let (config, root) = setup();
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("[storage]", "[namespace.composition]\nfusion=75\n[storage]");
    // Reverse physical insertion order so a row-address cutoff cannot stand in
    // for the contractual (source_id, root_id, record_id) order.
    let documents: Vec<_> = (0..count).rev().map(|i| json!({"id":format!("twin-{i:04}"),"revision":"r1","readers":["grp:allow"],"texts":["fixture document 0"]})).collect();
    let batches: Vec<_> = documents.chunks(128).map(|batch| batch.to_vec()).collect();
    std::fs::write(
        root.join("input.json"),
        serde_json::to_vec(&json!({"epoch":1,"content":batches})).unwrap(),
    )
    .unwrap();
    std::fs::write(&config, format!("{}\n[[sources]]\nid='twins'\ntype='incremental-test'\npermissions='fixed'\nreaders=['grp:allow']\n[sources.scope]\nfixture='{}'\n", text.split("[[sources]]").next().unwrap(), root.join("input.json").display())).unwrap();
    command(&config, &["writer", "namespace", "create"]);
    worker_sync(&config, &["twins", "--kind", "content"]);
    command(&config, &["writer", "commit"]);
    (config, root)
}

fn roots(response: &Value) -> Vec<String> {
    let hits = response["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 50, "lexical lane must fill its candidate limit");
    assert!(hits.iter().all(|hit| hit["source_id"] == "twins"));
    assert!(
        hits.iter().all(|hit| hit["score"] == hits[0]["score"]),
        "fixture twins must tie exactly"
    );
    hits.iter()
        .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
#[ignore = "real CLI/MCP reader processes and RustFS; scripts/ci tools selection"]
async fn tools_lexical_cutoff_twins_are_stable_between_reader_processes_on_real_s3() {
    let (config, _root) = twins(76);
    // The provider rejects this query so only lexical candidates reach the result.
    let mut observed = Vec::new();
    let mut version = None;
    for _ in 0..2 {
        for _ in 0..12 {
            let response = success(
                &config,
                &[
                    "reader", "search", "document", "--limit", "75", "--output", "metadata",
                ],
            );
            assert_eq!(response["metadata"]["degraded"], json!(["dense"]));
            let seen = response["metadata"]["content_version"].as_u64().unwrap();
            assert!(
                version.is_none_or(|v| v == seen),
                "publication changed during cutoff proof"
            );
            version = Some(seen);
            observed.push(roots(&response));
        }
        let mut mcp = super::tools::Mcp::open(&config);
        for _ in 0..12 {
            let result = mcp.call(
                "search",
                json!({"query":"document","sources":["twins"],"limit":75,"output":"metadata"}),
            );
            assert_eq!(result["isError"], false);
            let response = &result["structuredContent"];
            assert_eq!(
                response["metadata"]["content_version"],
                json!(version.unwrap())
            );
            assert_eq!(response["metadata"]["degraded"], json!(["dense"]));
            observed.push(roots(response));
        }
    }
    assert_eq!(
        observed.len(),
        48,
        "all independent process/path repetitions must run"
    );
    let expected: Vec<_> = (0..50).map(|i| format!("twin-{i:04}")).collect();
    assert!(
        observed.iter().all(|hits| hits == &expected),
        "lexical cutoff chose a physical/arbitrary twin instead of the stable-key first 50"
    );
}

#[tokio::test]
#[ignore = "real CLI/MCP reader processes and RustFS; scripts/ci"]
async fn tools_dense_cutoff_twins_are_stable_between_reader_processes_on_real_s3() {
    let (config, _root) = twins(76);
    let mut observed = Vec::new();
    let mut version = None;
    let mut adapter_seen = false;
    for _ in 0..2 {
        let mut mcp = super::tools::Mcp::open(&config);
        for _ in 0..12 {
            let cli = success(
                &config,
                &[
                    "reader",
                    "search",
                    "dense-only query",
                    "--limit",
                    "75",
                    "--output",
                    "metadata",
                ],
            );
            let result = mcp.call("search", json!({"query":"dense-only query","sources":["twins"],"limit":75,"output":"metadata"}));
            assert_eq!(result["isError"], false, "{result}");
            for response in [&cli, &result["structuredContent"]] {
                assert_eq!(response["metadata"]["degraded"], json!([]));
                adapter_seen |=
                    response["metadata"]["telemetry"]["stage_us"]["float16_rescore"].is_u64();
                let seen = response["metadata"]["content_version"].as_u64().unwrap();
                assert!(version.is_none_or(|v| v == seen));
                version = Some(seen);
                let hits = response["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 75);
                assert!(
                    hits.iter().all(|hit| hit["score"] == hits[0]["score"]),
                    "vector twins must tie exactly"
                );
                observed.push(
                    hits.iter()
                        .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
                        .collect::<Vec<_>>(),
                );
            }
        }
    }
    assert_eq!(observed.len(), 48);
    assert!(adapter_seen, "warm adapter cutoff must be exercised");
    let expected: Vec<_> = (0..75).map(|i| format!("twin-{i:04}")).collect();
    assert!(
        observed.iter().all(|hits| hits == &expected),
        "dense cutoff must use the stable-key first 75"
    );
}

#[tokio::test]
#[ignore = "real fixed-bound reader telemetry and RustFS; scripts/ci tools selection"]
async fn tools_cutoff_fetch_bounds_are_explicit_on_real_s3() {
    let (config, _root) = twins(1201);
    let mut mcp = super::tools::Mcp::open(&config);
    let result = mcp.call(
        "search",
        json!({"query":"document","sources":["twins"],"limit":75,"output":"metadata"}),
    );
    assert_eq!(result["isError"], false);
    let response = &result["structuredContent"];
    let telemetry = &response["metadata"]["telemetry"];
    assert_eq!(telemetry["lexical_tie_bound_hits"], 1);
    assert_eq!(telemetry["lexical_candidates_fetched"], 2605);
    let fetched_keys = roots(response);
    assert!(fetched_keys.windows(2).all(|pair| pair[0] < pair[1]));
    let result = mcp.call(
        "search",
        json!({"query":"dense-only query","sources":["twins"],"limit":75,"output":"metadata"}),
    );
    assert_eq!(result["isError"], false, "{result}");
    let response = &result["structuredContent"];
    assert_eq!(response["metadata"]["telemetry"]["dense_tie_bound_hits"], 1);
    assert_eq!(
        response["metadata"]["telemetry"]["dense_candidates_fetched"],
        2340
    );
    let keys: Vec<_> = response["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["root_id"].as_str().unwrap())
        .collect();
    assert_eq!(keys.len(), 75);
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
}
