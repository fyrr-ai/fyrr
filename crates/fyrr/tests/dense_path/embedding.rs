//! Native reader and MCP requests over real S3, using only a local fake provider.
use super::tools::body;
use super::*;
use common::embedding::Provider;
use std::sync::atomic::Ordering;
use tools::Mcp;

fn configured(config: &Path, provider: &Provider, cap: u64) -> PathBuf {
    let text = configure_embedding(
        &std::fs::read_to_string(config).unwrap(),
        &format!(
            "endpoint = '{}'\nreader_live = true\napi_key = {{env='FYRR_TEST_S3_SECRET_ACCESS_KEY'}}\ntimeout = '1s'\nmax_retries = 0\nspend = {{limit_microusd={cap},max_call_microusd=10}}",
            provider.endpoint
        ),
    );
    let path = config.with_file_name(format!("embedding-{cap}.toml"));
    std::fs::write(&path, text).unwrap();
    path
}
fn query(text: &str) -> Value {
    json!({"query":text,"limit":4,"output":"metadata"})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable, local fake embedding provider and RustFS; scripts/ci"]
async fn tools_embedding_live_cache_and_batched_queries_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let config = configured(&config, &provider, 1000);
    let log = root.join("reader-embedding.log");
    let mut client = Mcp::open_logged(&config, &log);
    let initial = body(client.call("search", query("fixture query")));
    assert_eq!(
        initial["metadata"]["telemetry"]["embedding_provider_calls"],
        2
    );
    assert_eq!(provider.calls(), 2, "the first query probes and embeds");
    let memory = body(client.call("search", query("fixture query")));
    assert_eq!(memory["hits"], initial["hits"]);
    assert_eq!(memory["metadata"]["telemetry"]["embedding_memory_hits"], 1);
    assert_eq!(provider.calls(), 2);
    let queries = json!([
        "fixture QUERY_EMBEDDING_SENTINEL_A",
        "fixture QUERY_EMBEDDING_SENTINEL_B"
    ]);
    let args = json!({"queries":queries,"limit":4,"text_hits":0,"output":"content"});
    let live = body(client.call("search", args.clone()));
    assert_eq!(live["metadata"]["degraded"], json!([]));
    assert_eq!(live["metadata"]["telemetry"]["embedding_provider_calls"], 1);
    assert!(live["metadata"]["telemetry"]["stage_us"]["embedding_provider"].is_u64());
    assert_eq!(provider.state.models.load(Ordering::SeqCst), 1);
    assert_eq!(provider.state.requests.lock().unwrap().len(), 2);
    assert_eq!(
        provider.state.requests.lock().unwrap()[1]["input"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let hits = live["hits"].as_array().unwrap();
    assert!(!hits.is_empty());
    assert!(hits.iter().all(|hit| hit["content"].is_null()));
    let keys: std::collections::BTreeSet<_> = hits
        .iter()
        .map(|hit| (hit["source_id"].to_string(), hit["root_id"].to_string()))
        .collect();
    assert_eq!(keys.len(), hits.len());
    let repeated = body(client.call("search", args));
    assert_eq!(repeated["hits"], live["hits"]);
    assert_eq!(
        repeated["metadata"]["telemetry"]["embedding_memory_hits"],
        2
    );
    assert_eq!(provider.calls(), 3);
    let duplicate = body(client.call("search", json!({"queries":["fixture duplicated", "fixture duplicated"],"limit":4,"output":"metadata"})));
    assert_eq!(duplicate["metadata"]["degraded"], json!([]));
    assert_eq!(provider.state.requests.lock().unwrap().len(), 3);
    assert_eq!(
        provider.state.requests.lock().unwrap()[2]["input"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(provider.state.models.load(Ordering::SeqCst), 1);
    drop(client);
    let diagnostics = std::fs::read_to_string(log).unwrap();
    for sentinel in [
        "QUERY_EMBEDDING_SENTINEL_A",
        "QUERY_EMBEDDING_SENTINEL_B",
        "PRIVATE_EMBEDDING_RESPONSE",
    ] {
        assert!(!diagnostics.contains(sentinel));
        assert!(!live.to_string().contains(sentinel));
    }
    assert!(!diagnostics.contains(&std::env::var("FYRR_TEST_S3_SECRET_ACCESS_KEY").unwrap()));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable, local fake embedding provider and RustFS; scripts/ci"]
async fn tools_embedding_failures_and_spend_degrade_entire_batch_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let live = configured(&config, &provider, 1000);
    for mode in 1..=4 {
        provider.state.mode.store(mode, Ordering::SeqCst);
        let log = root.join(format!("embedding-failure-{mode}.log"));
        let mut client = Mcp::open_logged(&live, &log);
        // A memory-cached sibling must degrade with a failed live sibling.
        // A model mismatch is checked on a fresh client, before any embedding.
        if mode != 1 {
            provider.state.mode.store(0, Ordering::SeqCst);
            let initial = body(client.call("search", query("fixture query")));
            assert_eq!(initial["metadata"]["degraded"], json!([]));
            provider.state.mode.store(mode, Ordering::SeqCst);
        }
        let result = body(client.call("search", json!({"queries":["fixture query",format!("fixture failed {mode}")],"limit":4,"output":"metadata"})));
        assert_eq!(result["metadata"]["degraded"], json!(["dense"]));
        assert!(!result["hits"].as_array().unwrap().is_empty());
        assert!(
            result["metadata"]["telemetry"]["embedding_provider_calls"]
                .as_u64()
                .unwrap()
                >= 1
        );
        drop(client);
        assert!(
            !std::fs::read_to_string(log)
                .unwrap()
                .contains("PRIVATE_EMBEDDING_RESPONSE")
        );
    }
    provider.state.mode.store(0, Ordering::SeqCst);
    let capped = configured(&config, &provider, 10);
    let before = provider.calls();
    let posts = provider.state.requests.lock().unwrap().len();
    let mut client = Mcp::open(&capped);
    let result = body(client.call("search", query("fixture exhausted first")));
    assert_eq!(result["metadata"]["degraded"], json!(["dense"]));
    assert_eq!(
        provider.calls(),
        before + 1,
        "only the model probe fits the cap"
    );
    assert_eq!(provider.state.requests.lock().unwrap().len(), posts);
    let result = body(client.call("search", query("fixture exhausted second")));
    assert_eq!(result["metadata"]["degraded"], json!(["dense"]));
    assert_eq!(
        provider.calls(),
        before + 1,
        "spend is process-wide and is not refunded"
    );
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real executable, local fake embedding provider and RustFS; scripts/ci"]
async fn tools_embedding_empty_admission_and_profile_mismatch_make_no_calls_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let live = configured(&config, &provider, 1000);
    let mut client = Mcp::open(&live);
    let empty = body(client.call("search", json!({"queries":["fixture private one", "fixture private two"],"sources":["denied"],"limit":4})));
    assert_eq!(empty["hits"], json!([]));
    assert_eq!(
        provider.calls(),
        0,
        "not even a model probe before nonempty authorization"
    );
    drop(client);
    let mismatch = root.join("embedding-mismatch.toml");
    std::fs::write(
        &mismatch,
        std::fs::read_to_string(&live)
            .unwrap()
            .replace("[embedding]\n", "[embedding]\ndimensions=512\n"),
    )
    .unwrap();
    let rejected = run(&mismatch, &["reader", "search", "fixture private mismatch"]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("embedding_profile_mismatch"));
    assert_eq!(provider.calls(), 0);
    let uncapped = root.join("embedding-uncapped.toml");
    std::fs::write(
        &uncapped,
        std::fs::read_to_string(&live)
            .unwrap()
            .replace("spend = {limit_microusd=1000,max_call_microusd=10}", ""),
    )
    .unwrap();
    let rejected = run(&uncapped, &["reader", "search", "fixture uncapped"]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("embedding_spend_limit_required"));
    assert_eq!(provider.calls(), 0);
    let denied = root.join("embedding-no-document-subject.toml");
    std::fs::write(
        &denied,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("fixture-subject", "no-document-subject")
            .replace(
                "stdio_readers = [\"grp:allow\"]",
                "stdio_readers = [\"grp:nobody\"]",
            ),
    )
    .unwrap();
    success(&denied, &["writer", "commit"]);
    let denied = configured(&denied, &provider, 1000);
    let mut client = Mcp::open(&denied);
    let empty = body(client.call(
        "search",
        json!({"queries":["fixture private one", "fixture private two"],"limit":4}),
    ));
    assert_eq!(empty["hits"], json!([]));
    assert_eq!(
        provider.calls(),
        0,
        "a subject with no document grants never probes or embeds"
    );
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real native reader, gated fake provider and RustFS; scripts/ci"]
async fn tools_embedding_provider_wait_overlaps_native_lexical_on_real_s3() {
    let (config, root, _) = tools::fixture();
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    let output = Command::new(binary)
        .args([
            "search::live_embedding_tests::provider_wait_overlaps_native_lexical_on_real_s3",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("FYRR_TEST_EMBED_CONFIG", config)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real native scheduler, gated fake provider and RustFS; scripts/ci"]
async fn tools_embedding_wait_releases_worker_and_keeps_retired_pin_on_real_s3() {
    let (config, root, _) = tools::fixture();
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    let output = Command::new(binary).args([
        "serve::tests::embedding::embedding_wait_releases_native_slot_and_keeps_retired_generation_pin_on_real_s3", "--exact", "--ignored", "--nocapture",
    ]).env("FYRR_TEST_EMBED_CONFIG", config).output().unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    std::fs::remove_dir_all(root).unwrap();
}

fn writer_config(
    config: &Path,
    root: &Path,
    provider: &Provider,
    dimensions: usize,
    cap: u64,
    batch: usize,
) -> PathBuf {
    let text = std::fs::read_to_string(config).unwrap();
    let text = configure_embedding(
        &text,
        &format!(
            "endpoint='{}'\ndimensions={dimensions}\nsend_dimensions=true\nbatch_size={batch}\nreader_live=true\nlocal_cache='{}'\napi_key={{env='FYRR_TEST_S3_SECRET_ACCESS_KEY'}}\nmax_retries=1\ntimeout='1s'\nspend={{limit_microusd={cap},max_call_microusd=10}}\nroles={{document={{input_type='document'}},query={{input_type='query'}}}}",
            provider.endpoint,
            root.join("writer-cache").display()
        ),
    );
    let text = if dimensions == 1024 {
        text.replace("send_dimensions=true\n", "").replace(
            "roles={document={input_type='document'},query={input_type='query'}}",
            "",
        )
    } else {
        text
    };
    let path = root.join(format!("writer-{dimensions}-{cap}.toml"));
    std::fs::write(&path, text).unwrap();
    path
}
fn writer_sync(config: &Path, args: &[&str]) -> Value {
    let output = run(config, args);
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let reports = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<Value>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    if reports.len() == 1 {
        reports.into_iter().next().unwrap()
    } else {
        json!(reports)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real CLI worker, fake HTTP provider, RustFS and native reader sampler; scripts/ci"]
async fn tools_writer_embedding_512_batches_restart_mcp_recall_and_routing_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root) = setup();
    let config = writer_config(&config, &root, &provider, 512, 100000, 32);
    success(&config, &["writer", "namespace", "create"]);
    let first = writer_sync(&config, &["writer", "sync"]);
    assert!(first.to_string().contains("paid_calls"));
    let posts = provider.state.requests.lock().unwrap().clone();
    let calls = provider.calls();
    assert_eq!(
        posts.len(),
        48,
        "768 novel texts per source, in batches of 32"
    );
    assert!(
        posts
            .iter()
            .all(|post| post["input"].as_array().unwrap().len() == 32
                && post["input_type"] == "document"
                && post["dimensions"] == 512)
    );
    success(&config, &["writer", "commit"]);
    let (_, manifest) = super::coordination::publication(&config).await;
    assert_eq!(manifest.embedding.dimensions, 512);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    let batch = dataset
        .scan()
        .project(&["vector"])
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    use arrow_array::Array;
    let vectors = batch
        .column_by_name("vector")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeListArray>()
        .unwrap();
    assert_eq!(vectors.value_length(), 512);
    assert_eq!(vectors.len(), 1536);
    // New root, unchanged text: only a host-local cache can satisfy this restart.
    std::fs::copy(
        root.join("sources/0.txt"),
        root.join("sources/restarted.txt"),
    )
    .unwrap();
    writer_sync(&config, &["writer", "sync", "--full"]);
    assert_eq!(provider.state.requests.lock().unwrap().len(), posts.len());
    assert_eq!(
        provider.calls(),
        calls + 2,
        "restart makes only the required startup model probes"
    );
    success(&config, &["writer", "commit"]);
    std::fs::remove_dir_all(root.join("writer-cache")).unwrap();
    writer_sync(&config, &["writer", "sync", "--full"]);
    assert_eq!(
        provider.state.requests.lock().unwrap().len(),
        posts.len(),
        "empty local cache must not re-embed published revisions"
    );
    success(&config, &["writer", "commit"]);
    assert_eq!(
        provider.calls(),
        calls + 4,
        "unchanged revisions with an empty cache make only startup probes"
    );
    // The same HTTP profile supplies document and query embeddings.
    let mut client = Mcp::open(&config);
    let result = body(client.call("search", query("fixture novel writer query")));
    assert_eq!(result["metadata"]["degraded"], json!([]));
    assert!(!result["hits"].as_array().unwrap().is_empty());
    drop(client);
    let query = "routing-only query";
    provider.reject("uncached-lexical-only");
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace(
            "[reader]",
            "[reader]\nconcurrency=1\nversion_check_interval='30s'",
        )
        .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 67108864");
    std::fs::write(&config, text).unwrap();
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    for (selection, count) in [
        ("serve::tests::recall_sampler_", 6),
        (
            "serve::tests::generations::routing_reserve_switch_on_real_s3",
            1,
        ),
    ] {
        let output = Command::new(&binary)
            .args([selection, "--ignored", "--nocapture"])
            .env("FYRR_TEST_RECALL_CONFIG", &config)
            .env("FYRR_TEST_RECALL_QUERY", query)
            .env("FYRR_TEST_ROUTING_CONFIG", &config)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{stdout} {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            stdout.contains(&format!("{count} passed; 0 failed")),
            "{stdout}"
        );
    }
    let before_delete = provider.calls();
    std::fs::remove_file(root.join("sources/restarted.txt")).unwrap();
    writer_sync(&config, &["writer", "sync", "--full"]);
    success(&config, &["writer", "commit"]);
    success(&config, &["writer", "verify"]);
    assert_eq!(
        provider.calls(),
        before_delete + 2,
        "deletes need only startup probes, never text embedding calls"
    );
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    let markers = dataset
        .scan()
        .filter("source_id LIKE '__fyrr_state_%'")
        .unwrap()
        .project(&["vector"])
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(markers.num_rows(), 2);
    let markers = markers
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeListArray>()
        .unwrap();
    assert_eq!(markers.value_length(), 512);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real CLI worker, fake HTTP provider and RustFS; scripts/ci"]
async fn tools_writer_embedding_forced_unchanged_skip_and_cache_restart_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root, _) = tools::fixture();
    let config = writer_config(&config, &root, &provider, 1024, 10000, 2);
    let (_, legacy_manifest) = super::coordination::publication(&config).await;
    let legacy_identity = serde_json::to_vec(&legacy_manifest.embedding).unwrap();
    assert!(legacy_manifest.embedding.settings.is_none());
    let loaded = fyrr_core::config::load(&config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let legacy_vectors = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap()
        .scan()
        .project(&["vector"])
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();

    let foreign = writer_config(&root.join("fyrr.toml"), &root, &provider, 512, 10000, 2);
    assert_eq!(
        fyrr_core::config::load(&foreign)
            .unwrap()
            .config
            .embedding
            .dimensions,
        512
    );
    let rejected = run(
        &foreign,
        &["writer", "sync", "allowed", "--kind", "content"],
    );
    assert!(!rejected.status.success());
    let report: Value = serde_json::from_slice(&rejected.stdout).unwrap();
    assert_eq!(report["error"], "embedding_profile_mismatch");
    let rejected = run(&foreign, &["writer", "commit"]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("embedding_profile_mismatch"));
    assert_eq!(
        provider.calls(),
        0,
        "foreign-profile writers do not reach the provider"
    );
    let mut documents: Value =
        serde_json::from_slice(&std::fs::read(root.join("documents.json")).unwrap()).unwrap();
    // Unlike local-files this provider always emits all documents, regardless
    // of the host's revision view. The writer itself must skip the unchanged ones.
    for document in documents["documents"].as_array_mut().unwrap() {
        document["text"] = json!("WRITER_UNCHANGED_TEXT_SENTINEL never embedded");
        document["first_text"] = document["text"].clone();
    }
    std::fs::write(
        root.join("documents.json"),
        serde_json::to_vec(&documents).unwrap(),
    )
    .unwrap();
    let report = writer_sync(&config, &["writer", "sync", "allowed", "--full"]);
    assert_eq!(report["cache_hits"], 0);
    assert_eq!(report["cache_misses"], 0);
    assert_eq!(report["paid_calls"], 0);
    assert_eq!(
        provider.calls(),
        1,
        "forced unchanged revisions make only the startup model probe"
    );
    assert!(provider.state.requests.lock().unwrap().is_empty());
    assert!(
        !report
            .to_string()
            .contains("WRITER_UNCHANGED_TEXT_SENTINEL")
    );
    success(&config, &["writer", "commit"]);
    let (_, unchanged_manifest) = super::coordination::publication(&config).await;
    assert_eq!(
        serde_json::to_vec(&unchanged_manifest.embedding).unwrap(),
        legacy_identity
    );
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let unchanged_vectors = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap()
        .scan()
        .project(&["vector"])
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        unchanged_vectors, legacy_vectors,
        "legacy float16 schema and bytes stay unchanged"
    );
    // A new revision is embedded; a further revision of identical content is
    // satisfied by the persisted local cache in a freshly spawned worker.
    let document = &mut documents["documents"][0];
    document["revision"] = json!("r2");
    document["text"] = json!("WRITER_NEW_TEXT_SENTINEL novel document");
    document["first_text"] = document["text"].clone();
    documents["cursor"] = json!("two");
    std::fs::write(
        root.join("documents.json"),
        serde_json::to_vec(&documents).unwrap(),
    )
    .unwrap();
    let report = writer_sync(
        &config,
        &["writer", "sync", "allowed", "--kind", "content", "--full"],
    );
    assert_eq!(report["paid_calls"], 1);
    assert_eq!(
        provider.state.requests.lock().unwrap().len(),
        1,
        "duplicate chunks in one document are embedded once"
    );
    success(&config, &["writer", "commit"]);
    documents["documents"][0]["revision"] = json!("r3");
    documents["cursor"] = json!("three");
    std::fs::write(
        root.join("documents.json"),
        serde_json::to_vec(&documents).unwrap(),
    )
    .unwrap();
    let report = writer_sync(
        &config,
        &["writer", "sync", "allowed", "--kind", "content", "--full"],
    );
    assert_eq!(report["paid_calls"], 0);
    assert_eq!(report["cache_hits"], 1);
    assert_eq!(provider.state.requests.lock().unwrap().len(), 1);
    assert_eq!(
        provider.calls(),
        4,
        "each invocation probes its model, but cache hits never repeat text calls"
    );
    assert!(!report.to_string().contains("WRITER_NEW_TEXT_SENTINEL"));
    success(&config, &["writer", "commit"]);
    std::fs::remove_dir_all(root).unwrap();
}

async fn envelopes(config: &Path) -> Vec<fyrr_core::envelope::PreparedEnvelope> {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, root) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let listed = store
        .list_with_delimiter(Some(&root.join("envelopes")))
        .await
        .unwrap();
    let mut result = Vec::new();
    for object in listed.objects {
        result.push(
            fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                .await
                .unwrap(),
        );
    }
    result
}
fn small_writer_fixture(provider: &Provider, cap: u64) -> (PathBuf, FixtureRoot) {
    let (config, root) = setup();
    for row in 4..768 {
        std::fs::remove_file(root.join(format!("sources/{row}.txt"))).unwrap();
    }
    let config = writer_config(&config, &root, provider, 1024, cap, 2);
    let text = std::fs::read_to_string(&config).unwrap();
    let (prefix, sources) = text.split_once("[[sources]]").unwrap();
    let source = sources.split("[[sources]]").next().unwrap();
    std::fs::write(&config, format!("{prefix}\n[[sources]]{source}")).unwrap();
    success(&config, &["writer", "namespace", "create"]);
    (config, root)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real CLI worker, fake HTTP provider and RustFS; scripts/ci"]
async fn tools_writer_embedding_failures_deadline_spend_and_no_partial_stage_on_real_s3() {
    let provider = Provider::open().await;
    for (mode, cap, code, calls) in [
        (3, 1000, "embedding_provider_unavailable", 2),
        (4, 1000, "embedding_timeout", 1),
        (2, 1000, "embedding_invalid", 1),
        (1, 1000, "embedding_model_mismatch", 0),
        (0, 10, "embedding_spend_limit_exceeded", 0),
        (0, 1000, "embedding_spend_limit_required", 0),
    ] {
        let (config, root) = small_writer_fixture(&provider, cap);
        let mut text = std::fs::read_to_string(&config).unwrap();
        if code == "embedding_timeout" {
            text = text.replace("timeout='1s'", "timeout='100ms'");
        }
        if code == "embedding_spend_limit_required" {
            text = text.replace("spend={limit_microusd=1000,max_call_microusd=10}", "");
        }
        // Disable reader_live validation so the writer runtime itself must
        // refuse an uncapped live document path before a provider request.
        text = text.replace("reader_live=true", "reader_live=false");
        std::fs::write(&config, text).unwrap();
        provider.state.mode.store(mode, Ordering::SeqCst);
        let before = provider.state.requests.lock().unwrap().len();
        let model_before = provider.state.models.load(Ordering::SeqCst);
        let output = run(
            &config,
            &["writer", "sync", "allowed", "--kind", "content", "--full"],
        );
        assert!(!output.status.success());
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["error"], code, "{report}");
        assert_eq!(
            report["paid_calls"], calls,
            "failed attempts remain accounted for"
        );
        if calls > 0 {
            assert!(report["reserved_microusd"].as_u64().unwrap() >= calls as u64 * 10);
        }
        assert_eq!(
            provider.state.requests.lock().unwrap().len() - before,
            calls
        );
        if code == "embedding_spend_limit_required" {
            assert_eq!(provider.state.models.load(Ordering::SeqCst), model_before);
        }
        assert!(
            envelopes(&config).await.is_empty(),
            "no partially embedded envelope"
        );
        let loaded = fyrr_core::config::load(&config).unwrap();
        let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        assert!(
            store
                .list_with_delimiter(Some(&ns.clone().join("document-metadata")))
                .await
                .unwrap()
                .objects
                .is_empty()
        );
        let staged = store
            .list_with_delimiter(Some(&ns.join("dataset.lance").join("data")))
            .await
            .unwrap();
        assert!(
            staged.objects.is_empty() && staged.common_prefixes.is_empty(),
            "failed embeddings never stage Lance data files"
        );
        for bytes in [&output.stdout, &output.stderr] {
            assert!(!String::from_utf8_lossy(bytes).contains("PRIVATE_EMBEDDING_RESPONSE"));
            assert!(
                !String::from_utf8_lossy(bytes)
                    .contains(&std::env::var("FYRR_TEST_S3_SECRET_ACCESS_KEY").unwrap())
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real CLI worker, fake HTTP provider and RustFS; scripts/ci"]
async fn tools_writer_embedding_failure_preserves_earlier_completed_envelope_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root) = small_writer_fixture(&provider, 1000);
    let text = std::fs::read_to_string(&config).unwrap().replace(
        "[[sources]]",
        "[writer]\nenvelope_max_rows=2\n[writer.commit]\ngroup_max_rows=2\n[[sources]]",
    );
    std::fs::write(&config, text).unwrap();
    provider.state.fail_after.store(1, Ordering::SeqCst);
    let output = run(
        &config,
        &["writer", "sync", "allowed", "--kind", "content", "--full"],
    );
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["error"], "embedding_provider_unavailable");
    assert_eq!(
        provider.state.requests.lock().unwrap().len(),
        3,
        "one completed batch and two failing attempts"
    );
    let durable = envelopes(&config).await;
    assert_eq!(durable.len(), 1);
    assert!(
        matches!(&durable[0].payload, fyrr_core::envelope::EnvelopePayload::Content(payload) if payload.documents.len() == 2 && payload.fragments.len() == 1)
    );
    success(&config, &["writer", "commit"]);
    let (_, manifest) = super::coordination::publication(&config).await;
    let loaded = fyrr_core::config::load(&config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        manifest.lance_version,
        head.content.as_ref().unwrap().lance_version
    );
    let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 2);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "parallel real CLI workers, fake HTTP provider and RustFS; scripts/ci"]
async fn tools_writer_embedding_spend_is_partitioned_across_source_workers_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root) = setup();
    for row in 4..768 {
        std::fs::remove_file(root.join(format!("sources/{row}.txt"))).unwrap();
    }
    let config = writer_config(&config, &root, &provider, 1024, 30, 2);
    success(&config, &["writer", "namespace", "create"]);
    let output = run(
        &config,
        &["writer", "sync", "--kind", "content", "--jobs", "2"],
    );
    assert!(!output.status.success());
    let reports = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<Value>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(reports.len(), 2);
    assert!(
        reports
            .iter()
            .all(|report| report["error"] == "embedding_spend_limit_exceeded"
                && report["failed"] == true)
    );
    assert_eq!(provider.state.models.load(Ordering::SeqCst), 2);
    assert!(provider.state.requests.lock().unwrap().is_empty());
    assert_eq!(
        reports
            .iter()
            .map(|report| report["reserved_microusd"].as_u64().unwrap())
            .sum::<u64>(),
        20
    );
    assert!(envelopes(&config).await.is_empty());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real worker, fake provider and RustFS; scripts/ci"]
async fn tools_writer_embedding_unchanged_revision_refuses_foreign_chunk_profile_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root) = small_writer_fixture(&provider, 10000);
    writer_sync(&config, &["writer", "sync", "--full"]);
    success(&config, &["writer", "commit"]);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (head, mut manifest) = super::coordination::publication(&config).await;
    manifest.chunk_profile.digest = digest(b"different chunk boundaries");
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = object_store::path::Path::from(format!(
        "{namespace}/{}",
        head.content.as_ref().unwrap().manifest
    ));
    store
        .put(&path, write_contract(&manifest).into())
        .await
        .unwrap();
    let before = provider.calls();
    let result = run(&config, &["writer", "sync", "--full"]);
    assert!(
        !result.status.success(),
        "unchanged revisions cannot bypass a chunk-profile mismatch"
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["error"], "chunk_profile_mismatch");
    // Non-content work must not relabel the old vectors/chunks at publication.
    writer_sync(
        &config,
        &["writer", "sync", "allowed", "--kind", "reconcile", "--full"],
    );
    let committed = run(&config, &["writer", "commit"]);
    assert!(
        !committed.status.success(),
        "commit must not relabel a foreign chunk profile"
    );
    assert!(String::from_utf8_lossy(&committed.stderr).contains("chunk_profile_mismatch"));
    assert_eq!(
        provider.calls(),
        before,
        "mismatched publications are refused before provider calls"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real worker, fake provider and RustFS; scripts/ci"]
async fn tools_writer_embedding_live_start_verifies_model_even_for_unchanged_revisions_on_real_s3()
{
    let provider = Provider::open().await;
    let (config, root) = small_writer_fixture(&provider, 10000);
    writer_sync(
        &config,
        &["writer", "sync", "allowed", "--kind", "content", "--full"],
    );
    success(&config, &["writer", "commit"]);
    let posts = provider.state.requests.lock().unwrap().len();
    let models = provider.state.models.load(Ordering::SeqCst);
    provider.state.mode.store(1, Ordering::SeqCst);
    let result = run(
        &config,
        &["writer", "sync", "allowed", "--kind", "content", "--full"],
    );
    assert!(
        !result.status.success(),
        "a configured live writer must verify its model before skipping unchanged documents"
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["error"], "embedding_model_mismatch");
    assert_eq!(report["provider_calls"], 1);
    assert_eq!(report["reserved_microusd"], 10);
    assert_eq!(provider.state.models.load(Ordering::SeqCst), models + 1);
    assert_eq!(provider.state.requests.lock().unwrap().len(), posts);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real worker, fake provider and RustFS; scripts/ci"]
async fn tools_writer_embedding_corrupt_local_float_is_never_published_on_real_s3() {
    let provider = Provider::open().await;
    let (config, root) = small_writer_fixture(&provider, 10000);
    writer_sync(
        &config,
        &["writer", "sync", "allowed", "--kind", "content", "--full"],
    );
    success(&config, &["writer", "commit"]);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let key = fyrr_reader::embedding::cache_key(
        &loaded.config.embedding,
        "fixture document 0",
        fyrr_reader::embedding::Role::Document,
    );
    let scope = std::fs::read_dir(root.join("writer-cache"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let path = scope.join(&key[7..]);
    let mut bytes = std::fs::read(&path).unwrap();
    // Alter one finite float, preserving the file's size and all framing.
    let offset = bytes.len() - 4;
    bytes[offset..].copy_from_slice(&0.375f32.to_le_bytes());
    std::fs::write(path, &bytes).unwrap();
    std::fs::copy(
        root.join("sources/0.txt"),
        root.join("sources/from-cache.txt"),
    )
    .unwrap();
    let before = envelopes(&config).await.len();
    let posts = provider.state.requests.lock().unwrap().len();
    let result = run(
        &config,
        &["writer", "sync", "allowed", "--kind", "content", "--full"],
    );
    assert!(
        !result.status.success(),
        "a changed cached float must not be staged as a valid vector"
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["error"], "embedding_local_cache_invalid");
    assert_eq!(envelopes(&config).await.len(), before);
    assert_eq!(provider.state.requests.lock().unwrap().len(), posts);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real S3, native reader and SDK; parked embedding deadline regression"]
async fn tools_embedding_timeout_lexical_under_native_contention_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::embedding_timeout_keeps_lexical_answer_while_native_slot_is_occupied_on_real_s3",
    );
}
#[tokio::test]
#[ignore = "real S3, native reader and SDK; revocation during embedding"]
async fn tools_embedding_wait_revocation_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::embedding_wait_revocation_refuses_old_authorization_on_real_s3",
    );
}
#[tokio::test]
#[ignore = "real S3, native reader and SDK; parked cancellation resource accounting"]
async fn tools_embedding_parked_cancellation_resources_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::embedding_parked_cancellation_releases_counted_resources_on_real_s3",
    );
}
#[tokio::test]
#[ignore = "real S3, native reader and SDK; reacquisition cancellation resource accounting"]
async fn tools_embedding_reacquire_cancellation_resources_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::embedding_reacquire_cancellation_releases_counted_resources_on_real_s3",
    );
}
#[tokio::test]
#[ignore = "real S3, native reader and SDK; parked admission saturation"]
async fn tools_embedding_parked_admission_bound_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::embedding_parked_admission_saturation_returns_busy_on_real_s3",
    );
}
fn parked_reader_test(selection: &str) {
    let (config, root, _) = tools::fixture();
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    let output = Command::new(binary)
        .args([selection, "--exact", "--ignored", "--nocapture"])
        .env("FYRR_TEST_EMBED_CONFIG", config)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real native scheduler, four gated provider waits and RustFS; scripts/ci"]
async fn tools_embedding_wait_all_drivers_leave_mirror_io_headroom_on_real_s3() {
    let (config, root, _) = tools::fixture();
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    let output = Command::new(binary)
        .args([
            "serve::tests::embedding::embedding_wait_all_drivers_leave_mirror_io_headroom_on_real_s3",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("FYRR_TEST_EMBED_CONFIG", config)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "real excerpt scheduler, cancellation and RustFS; scripts/ci"]
fn tools_excerpt_cancellation_retains_capacity_and_scratch_on_real_s3() {
    parked_reader_test(
        "serve::tests::excerpt_cancellation_retains_capacity_and_batch_scratch_on_real_s3",
    );
}

#[test]
#[ignore = "real S3 and stdio admission cancellation"]
fn tools_cancelled_admission_rebuild_refuses_stale_mask_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::cancelled_admission_rebuild_cannot_reuse_stale_stdio_mask_on_real_s3",
    );
}

#[test]
#[ignore = "real S3 and stdio admission deadline"]
fn tools_timed_out_admission_rebuild_refuses_stale_mask_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::timed_out_admission_rebuild_cannot_reuse_stale_stdio_mask_on_real_s3",
    );
}

#[test]
#[ignore = "real S3 and stdio directory freshness recovery"]
fn tools_directory_recovery_refuses_admission_after_failed_refresh_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::failed_refresh_cannot_admit_a_stale_mask_after_directory_recovery_on_real_s3",
    );
}

#[test]
#[ignore = "real S3 and stdio failure lifecycle"]
fn tools_failed_scheduler_terminates_stdio_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::stdio_terminates_after_scheduler_failure_on_real_s3",
    );
}

#[test]
#[ignore = "real S3 and MCP request-filter cancellation and worker reuse"]
fn tools_request_filters_preserve_base_admission_on_real_s3() {
    parked_reader_test(
        "serve::tests::embedding::request_filters_preserve_base_admission_and_worker_reuse_on_real_s3",
    );
}
