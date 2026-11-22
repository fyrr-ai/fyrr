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
            "endpoint = '{}'\nreader_live = true\napi_key = {{env='FYRR_TEST_SECRET'}}\ntimeout = '1s'\nmax_retries = 0\nspend = {{limit_microusd={cap},max_call_microusd=10}}",
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
#[ignore = "real executable, local fake embedding provider and local storage; scripts/ci"]
async fn tools_embedding_empty_admission_and_namespace_profile_make_no_calls_on_storage() {
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
    // A process configured for another embedding profile serves the namespace
    // with the namespace's own.
    let mismatch = root.join("embedding-mismatch.toml");
    std::fs::write(
        &mismatch,
        std::fs::read_to_string(&live)
            .unwrap()
            .replace("[embedding]\n", "[embedding]\ndimensions=512\n"),
    )
    .unwrap();
    let mut client = Mcp::open(&mismatch);
    let served = body(client.call("search", json!({"queries":["fixture private one", "fixture private two"],"sources":["denied"],"limit":4})));
    assert_eq!(served["hits"], json!([]));
    assert_eq!(provider.calls(), 0);
    drop(client);
    let committed = run(&mismatch, &["writer", "commit"]);
    assert!(
        committed.status.success(),
        "{}",
        String::from_utf8_lossy(&committed.stderr)
    );
    // Creating the namespace again with another profile is refused by name.
    let rejected = run(&mismatch, &["writer", "namespace", "create"]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("namespace_settings_fixed"));
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
            "endpoint='{}'\ndimensions={dimensions}\nsend_dimensions=true\nbatch_size={batch}\nreader_live=true\nlocal_cache='{}'\napi_key={{env='FYRR_TEST_SECRET'}}\nmax_retries=1\ntimeout='1s'\nspend={{limit_microusd={cap},max_call_microusd=10}}\nroles={{document={{input_type='document'}},query={{input_type='query'}}}}",
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
#[ignore = "real CLI worker, fake HTTP provider, local storage and native reader sampler; scripts/ci"]
async fn tools_writer_embedding_512_batches_restart_mcp_recall_and_routing_on_storage() {
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
        (
            "serve::tests::recall_sampler_cadence_poor_ann_and_same_filtered_reference",
            1,
        ),
        (
            "serve::tests::recall_sampler_cancellation_during_native_scan_reuses_worker",
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
#[ignore = "real CLI worker, fake HTTP provider and local storage; scripts/ci"]
async fn tools_writer_embedding_failures_deadline_spend_and_no_partial_stage_on_storage() {
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
                    .contains(&std::env::var("FYRR_TEST_SECRET").unwrap())
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
#[ignore = "real S3, native reader and SDK; revocation during embedding"]
async fn tools_embedding_wait_revocation_on_storage() {
    parked_reader_test(
        "serve::tests::embedding::embedding_wait_revocation_refuses_old_authorization_on_storage",
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

#[test]
#[ignore = "real S3 and stdio admission cancellation"]
fn tools_cancelled_admission_rebuild_refuses_stale_mask_on_storage() {
    parked_reader_test(
        "serve::tests::embedding::cancelled_admission_rebuild_cannot_reuse_stale_stdio_mask_on_storage",
    );
}

#[test]
#[ignore = "real S3 and stdio admission deadline"]
fn tools_timed_out_admission_rebuild_refuses_stale_mask_on_storage() {
    parked_reader_test(
        "serve::tests::embedding::timed_out_admission_rebuild_cannot_reuse_stale_stdio_mask_on_storage",
    );
}

#[test]
#[ignore = "real S3 and stdio failure lifecycle"]
fn tools_failed_scheduler_terminates_stdio_on_storage() {
    parked_reader_test(
        "serve::tests::embedding::stdio_terminates_after_scheduler_failure_on_storage",
    );
}

#[test]
#[ignore = "real S3 and MCP request-filter cancellation and worker reuse"]
fn tools_request_filters_preserve_base_admission_on_storage() {
    parked_reader_test(
        "serve::tests::embedding::request_filters_preserve_base_admission_and_worker_reuse_on_storage",
    );
}

#[test]
#[ignore = "real publication and shared embedding cache over MCP"]
fn tools_empty_selection_hides_query_cache_on_storage() {
    parked_reader_test(
        "serve::tests::embedding::empty_selection_metadata_does_not_reveal_query_cache_on_storage",
    );
}

#[test]
#[ignore = "real publication and eight simultaneous MCP clients"]
fn tools_default_concurrency_serves_eight_clients_on_storage() {
    parked_reader_test(
        "serve::tests::embedding::default_concurrency_serves_eight_simultaneous_clients_on_storage",
    );
}

#[test]
#[ignore = "real publication and one namespace saturated by 32 MCP clients"]
fn tools_saturated_namespace_leaves_another_every_slot_on_storage() {
    parked_reader_test(
        "serve::tests::embedding::saturated_namespace_leaves_another_namespace_every_slot_on_storage",
    );
}
