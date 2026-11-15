//! Hybrid retrieval through the executable, native indexes and real RustFS.
use super::*;

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn retrieval_publication_rescore_depths_execute_and_adaptive_expands_on_real_s3() {
    let (config, root) = setup();
    success(&config, &["writer", "namespace", "create"]);
    assert!(run(&config, &["writer", "sync"]).status.success());
    success(&config, &["writer", "commit"]);
    let original = std::fs::read_to_string(&config).unwrap();
    let mut legacy = None;
    let mut legacy_fetched = 0;
    for (name, policy) in [
        ("legacy", "{mode='legacy'}"),
        ("fixed", "{mode='fixed',candidates=75}"),
        (
            "adaptive",
            "{mode='adaptive',min_candidates=100,max_candidates=300,min_gap=2.0}",
        ),
    ] {
        let variant = root.join(format!("rescore-{name}.toml"));
        std::fs::write(
            &variant,
            format!("{original}\n[namespace.retrieval]\nrescore_depth={policy}\n"),
        )
        .unwrap();
        success(&variant, &["writer", "commit"]);
        let log = root.join(format!("rescore-{name}.log"));
        let mut client = super::tools::Mcp::open_logged(&variant, &log);
        let start = std::time::Instant::now();
        while !std::fs::read_to_string(&log)
            .unwrap_or_default()
            .contains("reader_refresh status=ready ")
        {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(60),
                "rescore warm-ready timed out"
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        let response = client.call("search", json!({"query":"fixture query","limit":10}));
        assert_eq!(response["isError"], false);
        let result = &response["structuredContent"];
        assert_eq!(result["hits"].as_array().unwrap().len(), 10);
        assert!(
            result["hits"]
                .as_array()
                .unwrap()
                .iter()
                .all(|h| h["source_id"] == "allowed")
        );
        let fetched = result["metadata"]["telemetry"]["dense_candidates_fetched"]
            .as_u64()
            .unwrap();
        match name {
            "legacy" => {
                legacy = Some(result["hits"].clone());
                legacy_fetched = fetched;
                assert!(fetched > 75);
            }
            "fixed" => assert!((75..legacy_fetched).contains(&fetched)),
            "adaptive" => {
                assert!(
                    fetched > legacy_fetched,
                    "small-gap policy must execute another refinement pass"
                );
                assert_eq!(Some(result["hits"].clone()), legacy);
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn retrieval_publication_analyzer_and_excerpt_on_real_s3() {
    let (config, root) = setup();
    let short = "TicketA0123456789";
    let long = format!("TicketA{}", "9".repeat(33));
    assert_eq!(long.len(), 40);
    let text = format!(
        "{} Résumé 👩🏽‍💻 Straße. Running the runners. {short} {long}. Final sentence.",
        "opening ".repeat(190)
    );
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both(&text, vector);
    for row in 0..768 {
        std::fs::remove_file(root.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    std::fs::write(root.join("sources").join("unique.txt"), &text).unwrap();
    success(&config, &["writer", "namespace", "create"]);
    assert!(run(&config, &["writer", "sync"]).status.success());
    let first = success(&config, &["writer", "commit"]);
    assert!(
        success(&config, &["reader", "search", "run"])["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        success(&config, &["reader", "search", "the"])["hits"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let result = success(&config, &["reader", "search", short]);
    assert_eq!(result["metadata"]["degraded"], json!(["dense"]));
    assert_eq!(result["hits"].as_array().unwrap().len(), 1);
    let hit = &result["hits"][0];
    assert_eq!(hit["source_id"], "allowed");
    assert!(hit["content"].is_null());
    let start = hit["excerpt"]["start"].as_u64().unwrap() as usize;
    let end = hit["excerpt"]["end"].as_u64().unwrap() as usize;
    assert!(start > 0 && end - start <= 1200);
    assert_eq!(
        text.chars()
            .skip(start)
            .take(end - start)
            .collect::<String>(),
        hit["excerpt"]["text"]
    );
    assert!(hit["excerpt"]["text"].as_str().unwrap().contains(short));
    assert!(
        success(&config, &["reader", "search", &long])["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let content = success(&config, &["reader", "search", short, "--output", "content"]);
    assert_eq!(content["hits"][0]["content"], text);
    assert_eq!(content["hits"][0]["revision"], hit["revision"]);
    assert!(content["hits"][0]["excerpt"].is_null());
    // Updating an analyzer with no envelopes still builds a new index/publication.
    let override_config = root.join("override.toml");
    let original = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &override_config,
        original.replace(
            "name = \"dense-fixture\"",
            "name = \"dense-fixture\"\nfts_max_token_length = 128",
        ),
    )
    .unwrap();
    let rebuilt = success(&override_config, &["writer", "commit"]);
    assert_ne!(first["lance_version"], rebuilt["lance_version"]);
    assert!(!run(&config, &["reader", "check"]).status.success());
    let long_result = success(&override_config, &["reader", "search", &long]);
    assert_eq!(long_result["hits"].as_array().unwrap().len(), 1);
    assert_eq!(long_result["hits"][0]["revision"], hit["revision"]);
    assert_eq!(
        success(&override_config, &["writer", "commit"])["lance_version"],
        rebuilt["lance_version"]
    );
    // Composition changes policy/publication while leaving immutable index/text intact.
    let composed = root.join("composed.toml");
    std::fs::write(
        &composed,
        std::fs::read_to_string(&override_config).unwrap().replace(
            "fts_max_token_length = 128",
            "fts_max_token_length = 128\ncomposition = { fusion = 1, dense = 1, lexical = 0 }",
        ),
    )
    .unwrap();
    assert_eq!(
        success(&composed, &["writer", "commit"])["lance_version"],
        rebuilt["lance_version"]
    );
    assert!(!run(&override_config, &["reader", "check"]).status.success());
    let new_result = success(&composed, &["reader", "search", &long]);
    assert_ne!(
        new_result["metadata"]["publication_id"],
        long_result["metadata"]["publication_id"]
    );
    // A repeated build has stable identities, scoring, text and codepoint offsets.
    assert_eq!(new_result["hits"], long_result["hits"]);
    let fresh = root.join("fresh.toml");
    std::fs::write(
        &fresh,
        std::fs::read_to_string(&composed)
            .unwrap()
            .replace("name = \"dense-fixture\"", "name = \"fresh-fixture\""),
    )
    .unwrap();
    success(&fresh, &["writer", "namespace", "create"]);
    assert!(run(&fresh, &["writer", "sync"]).status.success());
    success(&fresh, &["writer", "commit"]);
    assert_eq!(
        success(&fresh, &["reader", "search", &long])["hits"],
        new_result["hits"]
    );
    let analyzer = root.join("analyzer.toml");
    std::fs::write(
        &analyzer,
        std::fs::read_to_string(&fresh).unwrap().replace(
            "name = \"fresh-fixture\"",
            "name = \"fresh-fixture\"\nfts_stem=true\nfts_remove_stop_words=true",
        ),
    )
    .unwrap();
    let republished = success(&analyzer, &["writer", "commit"]);
    assert!(!run(&fresh, &["reader", "check"]).status.success());
    assert_eq!(
        success(&analyzer, &["reader", "search", "run"])["hits"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        success(&analyzer, &["reader", "search", "the"])["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        success(&analyzer, &["writer", "commit"])["lance_version"],
        republished["lance_version"]
    );
    // Folding is a separate lever; Lance applies it after its stemmer.
    let folded = root.join("folded.toml");
    std::fs::write(
        &folded,
        std::fs::read_to_string(&fresh).unwrap().replace(
            "name = \"fresh-fixture\"",
            "name = \"fresh-fixture\"\nfts_ascii_folding=true",
        ),
    )
    .unwrap();
    success(&folded, &["writer", "commit"]);
    assert_eq!(
        success(&folded, &["reader", "search", "resume"])["hits"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn retrieval_publication_rabitq_authorized_routing_and_fallbacks_on_real_s3() {
    let (config, root) = setup();
    // 2000 + 2000 + 400 = 4400 rows, with distinct dense distances and
    // no lexical query matches. One probe needs four eligible partitions.
    for row in 768..2000 {
        let text = format!("fixture document {row}");
        std::fs::write(root.join("sources").join(format!("{row}.txt")), &text).unwrap();
        let vector: Vec<f32> = (0..1024)
            .map(|d| (((row + 1) * (d + 7)) as f32).sin())
            .collect();
        root.embedding.insert_both(&text, vector);
    }
    let correlated = root.join("correlated");
    std::fs::create_dir(&correlated).unwrap();
    for row in 0..=400 {
        let text = format!("correlated document {row}");
        let vector: Vec<f32> = (0..1024)
            .map(|d| {
                ((d + 7) as f32).sin()
                    + (row + 1) as f32 * 0.0003 * (((row + 17) * (d + 11)) as f32).sin()
            })
            .collect();
        root.embedding.insert_both(&text, vector);
        if row < 400 {
            std::fs::write(correlated.join(format!("{row}.txt")), text).unwrap();
        }
    }
    root.embedding
        .insert_both("routing-only query", root.embedding.vector("fixture query"));
    let text = std::fs::read_to_string(&config).unwrap();
    let text = format!(
        "{text}\n[[sources]]\nid='correlated'\ntype='local-files'\npermissions='fixed'\nreaders=['grp:correlated']\n[sources.scope]\npaths=['{}']\n",
        correlated.display()
    );
    std::fs::write(&config, &text).unwrap();
    success(&config, &["writer", "namespace", "create"]);
    use fyrr_core::directory::{DirectoryGroup, DirectoryUser};
    let loaded = fyrr_core::config::load(&config).unwrap();
    let mut directory = super::identity::snapshot(&loaded.config, "scim");
    for cohort in ["broad", "selective", "correlated"] {
        let name = format!("fixture-{cohort}@example.test");
        directory.users.push(DirectoryUser {
            key: principal_hash(&loaded.config, "key", &name),
            token: principal_hash(&loaded.config, "tok", &format!("usr:{name}")),
            active: true,
            aliases: Vec::new(),
            subjects: Vec::new(),
        });
    }
    for (token, members) in [
        ("grp:allow", vec!["broad", "selective"]),
        ("grp:deny", vec!["broad"]),
        ("grp:correlated", vec!["broad", "correlated"]),
    ] {
        directory.groups.push(DirectoryGroup {
            token: principal_hash(&loaded.config, "tok", token),
            members: members
                .iter()
                .map(|cohort| {
                    principal_hash(
                        &loaded.config,
                        "key",
                        &format!("fixture-{cohort}@example.test"),
                    )
                })
                .collect(),
        });
    }
    let snapshot = root.join("routing-directory.json");
    std::fs::write(&snapshot, write_contract(&directory)).unwrap();
    success(
        &config,
        &[
            "writer",
            "directory",
            "--snapshot",
            snapshot.to_str().unwrap(),
        ],
    );
    assert!(run(&config, &["writer", "sync"]).status.success());
    success(&config, &["writer", "commit"]);
    let mut live = Vec::<(super::tools::Mcp, PathBuf)>::new();
    for (cohort, permitted) in [
        ("broad", vec!["allowed", "denied", "correlated"]),
        ("selective", vec!["allowed"]),
        ("correlated", vec!["correlated"]),
    ] {
        let cohort_text = text.replace(
            "stdio_user = \"fixture-subject\"",
            &format!("stdio_user = \"fixture-{cohort}@example.test\""),
        );
        for publication in 0..2 {
            if publication == 1 && cohort == "broad" {
                std::fs::write(correlated.join("new.txt"), "correlated document 400").unwrap();
                let sync = run(&config, &["writer", "sync", "correlated"]);
                assert!(
                    sync.status.success(),
                    "writer sync correlated: status={} stdout={} stderr={}",
                    sync.status,
                    String::from_utf8_lossy(&sync.stdout),
                    String::from_utf8_lossy(&sync.stderr)
                );
                success(&config, &["writer", "commit"]);
                let (head, _) = super::coordination::publication(&config).await;
                let publication = head.content.unwrap().publication_id;
                let mut switched_hits = None;
                for (client, log) in &mut live {
                    assert!(
                        Command::new("kill")
                            .args(["-HUP", &client.pid().to_string()])
                            .status()
                            .unwrap()
                            .success()
                    );
                    let started = std::time::Instant::now();
                    loop {
                        let result = client
                            .call("search", json!({"query":"routing-only query", "limit":10}));
                        assert_eq!(result["isError"], false, "{result}");
                        if result["structuredContent"]["metadata"]["publication_id"] == publication
                        {
                            if log.file_name().unwrap() == "live-rabitq.log" {
                                // Mixed means RaBitQ executed on at least one delta,
                                // with exact routing on another (e.g. a tiny new index).
                                assert!(matches!(
                                    result["structuredContent"]["metadata"]["telemetry"]["routing_strategy"].as_str(),
                                    Some("rabitq" | "mixed")
                                ), "{result}");
                            }
                            let hits = result["structuredContent"]["hits"].clone();
                            if let Some(expected) = &switched_hits {
                                assert_eq!(&hits, expected);
                            } else {
                                switched_hits = Some(hits);
                            }
                            break;
                        }
                        assert!(
                            started.elapsed() < std::time::Duration::from_secs(60),
                            "live generation did not switch"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(25));
                    }
                }
            }
            let mut expected = None;
            for route in ["exact", "auto", "rabitq"] {
                let route_config = root.join(format!("{cohort}-{route}.toml"));
                std::fs::write(
                    &route_config,
                    cohort_text.replace("[reader]", &format!("[reader]\nrouting='{route}'")),
                )
                .unwrap();
                let result = run(
                    &route_config,
                    &["reader", "search", "routing-only query", "--limit", "10"],
                );
                assert!(
                    result.status.success(),
                    "{}",
                    String::from_utf8_lossy(&result.stderr)
                );
                let value: Value = serde_json::from_slice(&result.stdout).unwrap();
                let hits = &value["hits"];
                let take_config = root.join(format!("{cohort}-{route}-take.toml"));
                std::fs::write(
                    &take_config,
                    std::fs::read_to_string(&route_config)
                        .unwrap()
                        .replace("[reader]", "[reader]\nsparse_read='take_rows'"),
                )
                .unwrap();
                let take = success(
                    &take_config,
                    &["reader", "search", "routing-only query", "--limit", "10"],
                );
                assert_eq!(
                    &take["hits"], hits,
                    "masked scan/take parity across grants and incremental tail"
                );
                assert!(!hits.as_array().unwrap().is_empty());
                assert!(
                    hits.as_array()
                        .unwrap()
                        .iter()
                        .all(|h| permitted.contains(&h["source_id"].as_str().unwrap()))
                );
                if let Some(expected) = &expected {
                    assert_eq!(
                        hits, expected,
                        "{cohort}, {route}, publication {publication}"
                    );
                } else {
                    expected = Some(hits.clone());
                }
                let telemetry = &value["metadata"]["telemetry"];
                if route == "auto" {
                    assert_eq!(telemetry["routing_strategy"], "exact", "{telemetry}");
                }
                if route == "rabitq" && cohort == "broad" {
                    assert!(
                        matches!(
                            telemetry["routing_strategy"].as_str(),
                            Some("rabitq" | "mixed")
                        ),
                        "{telemetry}"
                    );
                    if publication == 0 {
                        assert!(telemetry.get("routing_fallback").is_none(), "{telemetry}");
                    }
                }
            }
            if cohort == "broad" && publication == 0 {
                for route in ["exact", "rabitq"] {
                    let path = root.join(format!("broad-{route}.toml"));
                    let log = root.join(format!("live-{route}.log"));
                    let mut client = super::tools::Mcp::open_logged(&path, &log);
                    let result =
                        client.call("search", json!({"query":"routing-only query", "limit":10}));
                    assert_eq!(result["isError"], false, "{result}");
                    assert_eq!(
                        result["structuredContent"]["hits"],
                        *expected.as_ref().unwrap()
                    );
                    live.push((client, log));
                }
            }
        }
    }
    for (_, log) in &live {
        let telemetry = std::fs::read_to_string(log).unwrap();
        assert!(!telemetry.contains("routing-only query") && !telemetry.contains("fixture-broad"));
    }
    drop(live);
    let broad_config = root.join("broad-exact.toml");
    let test_binary = std::env::var_os("FYRR_ROUTING_TEST_BINARY")
        .expect("reader test binary built by real-S3 setup");
    let fallbacks = Command::new(test_binary)
        .args([
            "search::routing_tests::routing_fallbacks_on_real_s3",
            "serve::tests::generations::routing_reserve_switch_on_real_s3",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("FYRR_TEST_ROUTING_CONFIG", &broad_config)
        .output()
        .unwrap();
    std::fs::write(
        root.join("routing-fallbacks.log"),
        format!(
            "{}{}",
            String::from_utf8_lossy(&fallbacks.stdout),
            String::from_utf8_lossy(&fallbacks.stderr)
        ),
    )
    .unwrap();
    assert!(
        fallbacks.status.success(),
        "{}{}",
        String::from_utf8_lossy(&fallbacks.stdout),
        String::from_utf8_lossy(&fallbacks.stderr)
    );
    assert!(String::from_utf8_lossy(&fallbacks.stdout).contains("2 passed; 0 failed"));
    let stderr = String::from_utf8_lossy(&fallbacks.stderr);
    // Bounded routing diagnostics contain none of the fixture's private data.
    assert!(
        !stderr.contains("routing-only query")
            && !stderr.contains("fixture-subject")
            && !stderr.contains("fixture-broad")
            && !stderr.contains("fixture document")
    );
}

#[tokio::test]
#[ignore = "real writer, dispatcher and RustFS; scripts/ci"]
async fn retrieval_publication_recall_sampler_on_real_s3() {
    let (config, root) = setup();
    let mut sentinels = common::Sentinels::new();
    let query = sentinels.mark("sampler-query");
    let subject = sentinels.mark("sampler-subject");
    let mut seed = 0xabad1deau32;
    let vector: Vec<f32> = (0..1024)
        .map(|_| {
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            (seed >> 8) as f32 / 16777216.0 * 2.0 - 1.0
        })
        .collect();
    root.embedding.insert_both(&query, vector);
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("fixture-subject", &subject)
        .replace(
            "[reader]",
            "[reader]\nconcurrency=1\nversion_check_interval='30s'",
        )
        .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 67108864");
    std::fs::write(&config, text).unwrap();
    success(&config, &["writer", "namespace", "create"]);
    let sync = run(&config, &["writer", "sync"]);
    assert!(
        sync.status.success(),
        "{}",
        String::from_utf8_lossy(&sync.stderr)
    );
    success(&config, &["writer", "commit"]);
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    let output = Command::new(binary)
        .args(["serve::tests::recall_sampler_", "--ignored", "--nocapture"])
        .env("FYRR_TEST_RECALL_CONFIG", &config)
        .env("FYRR_TEST_RECALL_QUERY", &query)
        .output()
        .unwrap();
    for log in [&output.stdout, &output.stderr] {
        assert!(
            sentinels.found_in(&String::from_utf8_lossy(log)).is_empty(),
            "sampler diagnostic leaked a private marker"
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    std::fs::write(
        root.join("recall-sampler.log"),
        format!("{stdout}\n{stderr}"),
    )
    .unwrap();
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("6 passed; 0 failed"));
    for metric in [
        "recall_at_10",
        "recall_at_75",
        "recall_skip_cancelled",
        "recall_skip_timeout",
        "recall_skip_preparation",
        "recall_skip_foreground_queued",
    ] {
        assert!(
            stderr.contains(metric),
            "missing dispatcher proof: {metric}"
        );
    }
}

#[tokio::test]
#[ignore = "real executable, reader libtest and RustFS fixture; scripts/ci"]
async fn retrieval_publication_authorization_cache_budget_on_real_s3() {
    use fyrr_core::directory::{DirectoryGroup, DirectoryUser};
    let (config, root) = setup();
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config, text.replace("[reader]", "[reader]\nconcurrency=2")).unwrap();
    success(&config, &["writer", "namespace", "create"]);
    let loaded = fyrr_core::config::load(&config).unwrap().config;
    let mut directory = super::identity::snapshot(&loaded, "scim");
    for i in 0..3 {
        let name = format!("cache-user-{i}");
        directory.users.push(DirectoryUser {
            key: principal_hash(&loaded, "key", &name),
            token: principal_hash(&loaded, "tok", &format!("usr:{name}")),
            active: true,
            aliases: Vec::new(),
            subjects: vec![principal_hash(
                &loaded,
                "key",
                &format!("cache-subject-{i}"),
            )],
        });
    }
    // Keep the configured stdio identity valid during generation preparation.
    directory.users.push(DirectoryUser {
        key: principal_hash(&loaded, "key", "fixture-subject"),
        token: principal_hash(&loaded, "tok", "usr:fixture-subject"),
        active: true,
        aliases: Vec::new(),
        subjects: Vec::new(),
    });
    directory.groups = vec![
        DirectoryGroup {
            token: principal_hash(&loaded, "tok", "grp:allow"),
            members: [0, 2, 3].map(|i| directory.users[i].key.clone()).to_vec(),
        },
        DirectoryGroup {
            token: principal_hash(&loaded, "tok", "grp:deny"),
            members: vec![directory.users[1].key.clone()],
        },
    ];
    directory.validate().unwrap();
    let input = root.join("cache-directory.json");
    std::fs::write(&input, write_contract(&directory)).unwrap();
    success(
        &config,
        &["writer", "directory", "--snapshot", input.to_str().unwrap()],
    );
    assert!(run(&config, &["writer", "sync"]).status.success());
    success(&config, &["writer", "commit"]);
    directory.built_at = chrono::Utc::now();
    directory.users[0].active = false;
    directory.groups[0].members.remove(0);
    std::fs::write(root.join("cache-revoke.json"), write_contract(&directory)).unwrap();
    let binary = std::env::var_os("FYRR_ROUTING_TEST_BINARY")
        .expect("reader libtest built by real-S3 setup");
    let output = Command::new(binary)
        .args([
            "search::authorization_cache_tests::authorization_cache_budget_on_real_s3",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("FYRR_TEST_CACHE_CONFIG", &config)
        .env("FYRR_TEST_CACHE_WRITER", env!("CARGO_BIN_EXE_fyrr"))
        .env(
            "VIRTUAL_ENV",
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../connectors/.venv"),
        )
        .env(
            "PYTHONPATH",
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/providers"),
        )
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    std::fs::write(
        root.join("authorization-cache.log"),
        format!("{stdout}\n{stderr}"),
    )
    .unwrap();
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("1 passed; 0 failed"));
    for marker in [
        "fixture query",
        "fixture document",
        "cache-user-",
        "cache-subject-",
    ] {
        assert!(
            !stderr.contains(marker),
            "cache diagnostic leaked a private marker"
        );
    }
}
