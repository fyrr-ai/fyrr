//! Hybrid retrieval through the executable, native indexes and real RustFS.
use super::*;

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn retrieval_publication_rabitq_authorized_routing_and_fallbacks_on_storage() {
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
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
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
                assert!(result.status.success(), "{}", report(&result));
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
    let fallbacks = Command::new(&test_binary)
        .args([
            "search::routing_tests::routing_fallbacks_on_storage",
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
    assert!(String::from_utf8_lossy(&fallbacks.stdout).contains("1 passed; 0 failed"));
    let stderr = String::from_utf8_lossy(&fallbacks.stderr);
    // Bounded routing diagnostics contain none of the fixture's private data.
    assert!(
        !stderr.contains("routing-only query")
            && !stderr.contains("fixture-subject")
            && !stderr.contains("fixture-broad")
            && !stderr.contains("fixture document")
    );
    // The request filter's index route and the lazily resolved partition
    // admission, per grant cohort of the same publication; the fixture's
    // partitions are too few for the filter's index route by default.
    for (test, log) in [
        ("filtered_ann_on_storage", "filtered-ann"),
        ("lazy_partition_admission_on_storage", "lazy-partitions"),
    ] {
        for cohort in ["broad", "selective", "correlated"] {
            let output = Command::new(&test_binary)
                .args([
                    &format!("search::routing_tests::{test}"),
                    "--exact",
                    "--ignored",
                    "--nocapture",
                ])
                .env(
                    "FYRR_TEST_ROUTING_CONFIG",
                    root.join(format!("{cohort}-exact.toml")),
                )
                .output()
                .unwrap();
            std::fs::write(
                root.join(format!("{log}-{cohort}.log")),
                format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ),
            )
            .unwrap();
            assert!(
                output.status.success(),
                "{test} {cohort}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        }
    }
}

#[tokio::test]
#[ignore = "real executable, reader libtest and local storage; scripts/ci"]
async fn retrieval_publication_authorization_cache_budget_on_storage() {
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
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    success(&config, &["writer", "commit"]);
    directory.built_at = fyrr_core::clock::now();
    directory.users[0].active = false;
    directory.groups[0].members.remove(0);
    std::fs::write(root.join("cache-revoke.json"), write_contract(&directory)).unwrap();
    let binary = std::env::var_os("FYRR_ROUTING_TEST_BINARY")
        .expect("reader libtest built by real-S3 setup");
    let output = Command::new(binary)
        .args([
            "search::authorization_cache_tests::authorization_cache_budget_on_storage",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("FYRR_TEST_CACHE_CONFIG", &config)
        .env("FYRR_TEST_CACHE_WRITER", crate::common::binary())
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
