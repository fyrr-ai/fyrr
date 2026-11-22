//! Real executable + real S3 or local storage: namespace, ingest, pinned
//! serving and failure boundaries.
#![cfg(feature = "writer")]

mod common;

#[path = "dense_path/api_keys.rs"]
mod api_keys;
#[path = "dense_path/coordination.rs"]
mod coordination;
#[path = "dense_path/corrections.rs"]
mod corrections;
#[path = "dense_path/directory_ops.rs"]
mod directory_ops;
#[path = "dense_path/embedding.rs"]
mod embedding;
#[path = "dense_path/github.rs"]
mod github;
#[path = "dense_path/google_drive.rs"]
mod google_drive;
#[path = "dense_path/head_diagnostics.rs"]
mod head_diagnostics;
#[path = "dense_path/history_integrity.rs"]
mod history_integrity;
#[path = "dense_path/http_json.rs"]
mod http_json;
#[path = "dense_path/http_mcp.rs"]
mod http_mcp;
#[path = "dense_path/identity.rs"]
mod identity;
#[path = "dense_path/incremental.rs"]
mod incremental;
#[path = "dense_path/index_errors.rs"]
mod index_errors;
#[path = "dense_path/jira.rs"]
mod jira;
#[path = "dense_path/local_storage.rs"]
mod local_storage;
#[path = "dense_path/maintenance.rs"]
mod maintenance;
#[path = "dense_path/mirror_identity.rs"]
mod mirror_identity;
#[path = "dense_path/namespace_delete.rs"]
mod namespace_delete;
#[path = "dense_path/notion.rs"]
mod notion;
#[path = "dense_path/publication_lifecycle.rs"]
mod publication_lifecycle;
#[path = "dense_path/rerank.rs"]
mod rerank;
#[path = "dense_path/retrieval.rs"]
mod retrieval;
#[path = "dense_path/slack.rs"]
mod slack;
#[path = "dense_path/storage_budget.rs"]
mod storage_budget;
#[path = "dense_path/synthetic.rs"]
mod synthetic;
#[path = "dense_path/tools.rs"]
mod tools;
#[path = "dense_path/write_api.rs"]
mod write_api;

use common::embedding::{FixtureRoot, configure as configure_embedding};
use fyrr_core::publication::{ContentManifest, PolicyArtifact, ServingHead};
use fyrr_core::{Contract, parse_contract, write_contract};
use fyrr_reader::embedding::digest;
use object_store::ObjectStoreExt;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

fn run(config: &Path, args: &[&str]) -> Output {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    Command::new("bash")
        .args(["-c", "umask 022; exec \"$@\"", "fyrr-test"])
        .arg(crate::common::binary())
        .arg("--config")
        .arg(config)
        .args(args)
        .env("VIRTUAL_ENV", root.join("connectors/.venv"))
        .env("PYTHONPATH", root.join("crates/fyrr/tests/providers"))
        .output()
        .expect("execute fyrr")
}
/// Everything a failed command reported. Commands such as `writer sync` name
/// their error on stdout, so a failure message needs both streams.
fn report(output: &Output) -> String {
    format!(
        "status={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
fn success(config: &Path, args: &[&str]) -> Value {
    let output = run(config, args);
    assert!(output.status.success(), "{args:?}: {}", report(&output));
    serde_json::from_slice(&output.stdout).expect("JSON output")
}

fn initialize_stdio(input: &mut impl Write, output: &mut impl BufRead) {
    writeln!(input, "{}", json!({"jsonrpc":"2.0","id":"initialize","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"fyrr-tests","version":"1"}}})).unwrap();
    input.flush().unwrap();
    let mut line = String::new();
    output.read_line(&mut line).unwrap();
    assert!(serde_json::from_str::<Value>(&line).unwrap()["result"].is_object());
    writeln!(
        input,
        "{}",
        json!({"jsonrpc":"2.0","method":"notifications/initialized"})
    )
    .unwrap();
    input.flush().unwrap();
}

/// The storage a fixture publishes to; scenarios that hold for both take it
/// as a parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    /// The RustFS fixture of `scripts/s3-fixture`.
    S3,
    /// A `file://` directory next to the fixture root, created by fyrr.
    Local,
}

impl Backend {
    fn selected() -> Self {
        match std::env::var("FYRR_TEST_BACKEND").as_deref() {
            Ok("s3") => Self::S3,
            Ok("local") | Err(_) => Self::Local,
            Ok(value) => panic!("unknown test storage backend: {value}"),
        }
    }
}

fn setup() -> (PathBuf, FixtureRoot) {
    setup_on(Backend::selected())
}

fn setup_on(backend: Backend) -> (PathBuf, FixtureRoot) {
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "dense-{}-{}-{:?}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_micros(),
        std::thread::current().id()
    ));
    setup_at_on(root, backend)
}

fn setup_at_on(root: PathBuf, backend: Backend) -> (PathBuf, FixtureRoot) {
    let root = FixtureRoot::new(root);
    let sources = root.join("sources");
    std::fs::create_dir_all(&sources).expect("fixture directory");
    for row in 0..768 {
        let text = format!("fixture document {row}");
        std::fs::write(sources.join(format!("{row}.txt")), &text).expect("fixture text");
        let vector: Vec<f32> = (0..1024)
            .map(|d| (((row + 1) * (d + 7)) as f32).sin())
            .collect();
        root.embedding.insert_both(&text, vector.clone());
        if row == 0 {
            root.embedding.insert_both("fixture query", vector.clone());
            root.embedding.insert_both("dense-only query", vector);
        }
    }
    let run_id = root.file_name().expect("run id").to_string_lossy();
    // Local storage lives outside the root, so the per-host copies of the
    // configuration, which move paths under the root, share it.
    let (storage, secret) = match backend {
        Backend::S3 => {
            let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").expect("scripts/s3-fixture env");
            let bucket = std::env::var("FYRR_TEST_S3_BUCKET").expect("scripts/s3-fixture env");
            (
                format!(
                    "uri = \"s3://{bucket}/dense-{run_id}\"\nendpoint = \"{endpoint}\"\n\
                     allow_http = true\n\
                     access_key_id = {{ env = \"FYRR_TEST_S3_ACCESS_KEY_ID\" }}\n\
                     secret_access_key = {{ env = \"FYRR_TEST_S3_SECRET_ACCESS_KEY\" }}\n\
                     # fyrr-test-head-check = every"
                ),
                "FYRR_TEST_SECRET",
            )
        }
        Backend::Local => (
            format!(
                "uri = \"file://{}\"\n# fyrr-test-store = local-storage\n# fyrr-test-head-check = every",
                root.with_file_name(format!("storage-{run_id}")).display()
            ),
            "FYRR_TEST_SECRET",
        ),
    };
    let config = root.join("fyrr.toml");
    let text = format!(
        r#"
[namespace]
name = "dense-fixture"
[storage]
{storage}
[embedding]
endpoint = "{}"
reader_live = true
api_key = {{ env = "{secret}" }}
local_cache = "{}"
max_retries = 0
spend = {{ limit_microusd = 1000000, max_call_microusd = 10 }}
[identity]
stdio_user = "fixture-subject"
stdio_readers = ["grp:allow"]
hashing_key = {{ env = "{secret}" }}
encryption_key = {{ env = "FYRR_TEST_ENCRYPTION_KEY" }}
[reader]
nprobes = 1
mirror_path = "{}"
mirror_max_bytes = 16777216
[[sources]]
id = "allowed"
type = "local-files"
permissions = "fixed"
readers = ["grp:allow"]
[sources.scope]
paths = ["{}"]
[[sources]]
id = "denied"
type = "local-files"
permissions = "fixed"
readers = ["grp:deny"]
[sources.scope]
paths = ["{}"]
"#,
        root.embedding.endpoint,
        root.join("fixture-writer-cache").display(),
        root.join("mirror").display(),
        sources.display(),
        sources.display()
    );
    std::fs::write(&config, &text).expect("config");
    (config, root)
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn dense_path_and_namespace_contracts_on_real_s3() {
    dense_path_and_namespace_contracts(Backend::S3).await;
}

#[tokio::test]
#[ignore = "real CLI on local storage; scripts/ci"]
async fn dense_path_and_namespace_contracts_on_local_storage() {
    dense_path_and_namespace_contracts(Backend::Local).await;
}

async fn dense_path_and_namespace_contracts(backend: Backend) {
    let (config, root) = setup_on(backend);
    success(&config, &["writer", "namespace", "create"]);
    // Creation's policy and head must pass the same parser as reader admission.
    let loaded = fyrr_core::config::load(&config).expect("config");
    let (store, path) = fyrr_reader::storage::namespace_store(&loaded.config).expect("storage");
    let head: ServingHead =
        fyrr_reader::storage::read_contract(store.as_ref(), &path.clone().join("head.json"))
            .await
            .expect("created head validates");
    let policy: PolicyArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{path}/{}", head.policy.object)),
    )
    .await
    .expect("created policy validates");
    head.validate().expect("head");
    policy.validate().expect("policy");
    let sync = run(&config, &["writer", "sync"]);
    assert!(
        sync.status.success(),
        "{}{}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&sync.stdout).lines().count(), 2);
    success(&config, &["writer", "commit"]);
    let head: ServingHead =
        fyrr_reader::storage::read_contract(store.as_ref(), &path.clone().join("head.json"))
            .await
            .expect("published head");
    let pointer = head.content.as_ref().expect("content");
    let manifest: ContentManifest = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{path}/{}", pointer.manifest)),
    )
    .await
    .expect("created manifest validates");
    manifest.validate().expect("manifest");
    head.validate().expect("published head");
    success(&config, &["reader", "check"]);
    let search = success(
        &config,
        &["reader", "search", "fixture query", "--limit", "75"],
    );
    let hits = search["hits"].as_array().expect("hits");
    assert_eq!(
        hits.len(),
        50,
        "the soft source cap refills the fusion bucket"
    );
    assert!(
        hits.iter()
            .all(|h| h["content"].is_null() && h["excerpt"]["text"].is_string())
    );
    assert!(
        search["metadata"]["degraded"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        manifest
            .indexes
            .iter()
            .any(|i| i.name == "fts_idx" && i.kind == "inverted")
    );
    assert!(!manifest.fts.with_position);
    assert!(hits.iter().all(|h| h["source_id"] == "allowed"));
    let mirror_directory = root.join("mirror").join(digest(
        format!(
            "{}:{}",
            loaded.config.storage.endpoint.as_deref().unwrap_or(""),
            fyrr_reader::storage::dataset_uri(&loaded.config)
        )
        .as_bytes(),
    ));
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(root.join("mirror"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&mirror_directory)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let mirror_bytes: u64 = std::fs::read_dir(&mirror_directory)
        .expect("mirror directory")
        .map(|entry| {
            let meta = entry
                .expect("mirror entry")
                .metadata()
                .expect("mirror metadata");
            assert_eq!(meta.permissions().mode() & 0o777, 0o600);
            meta.len()
        })
        .sum();
    assert!(
        mirror_bytes > 0 && mirror_bytes <= 16_777_216,
        "read-through mirror remains within its configured byte bound"
    );
    let config_text = std::fs::read_to_string(&config).expect("config");
    // Equal roots/record IDs in two sources survive publication independently.
    let both = root.join("both.toml");
    std::fs::write(
        &both,
        config_text.replace(
            "stdio_readers = [\"grp:allow\"]",
            "stdio_readers = [\"grp:allow\",\"grp:deny\"]",
        ),
    )
    .expect("both-token identity config");
    assert_eq!(
        success(&both, &["writer", "commit"])["lance_version"],
        manifest.lance_version
    );
    let collisions = success(
        &both,
        &["reader", "search", "dense-only query", "--limit", "2"],
    );
    let hits = collisions["hits"].as_array().expect("colliding hits");
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0]["source_id"], "allowed");
    assert_eq!(hits[1]["source_id"], "denied");
    assert_eq!(hits[0]["root_id"], hits[1]["root_id"]);
    assert_eq!(hits[0]["record_id"], hits[1]["record_id"]);
    let lexical_collisions = success(&both, &["reader", "search", "document 0", "--limit", "2"]);
    assert_eq!(lexical_collisions["hits"][0]["source_id"], "allowed");
    assert_eq!(lexical_collisions["hits"][1]["source_id"], "denied");
    assert_eq!(
        lexical_collisions["hits"][0]["root_id"],
        lexical_collisions["hits"][1]["root_id"]
    );
    assert_eq!(
        success(&config, &["writer", "commit"])["lance_version"],
        manifest.lance_version
    );
    let denied = root.join("denied.toml");
    std::fs::write(
        &denied,
        config_text.replace(
            "stdio_user = \"fixture-subject\"",
            "stdio_user = \"unknown-subject\"",
        ),
    )
    .expect("denied config");
    for query in ["fixture query", "cache miss"] {
        let output = run(&denied, &["reader", "search", query]);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("authorization_unavailable"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("embedding_unavailable"));
    }
    let isolated = root.join("isolated.toml");
    std::fs::write(
        &isolated,
        config_text.replace("name = \"dense-fixture\"", "name = \"isolated-fixture\""),
    )
    .expect("isolated");
    assert!(!run(&isolated, &["reader", "check"]).status.success());
    // No-op and reset rescan publish no new rows or versions.
    for args in [&["writer", "sync"][..], &["writer", "sync", "--full"]] {
        let output = run(&config, args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let report: Value = serde_json::from_str(line).expect("report");
            assert_eq!(report["stats"]["upserts"], 0);
            assert_eq!(report["cache_misses"], 0);
        }
        assert_eq!(
            success(&config, &["writer", "commit"])["lance_version"],
            manifest.lance_version
        );
    }
    // A fresh policy generation without a readers index cannot become ready.
    let mut missing = manifest.clone();
    missing.indexes.retain(|index| index.column != "readers");
    assert!(parse_contract::<ContentManifest>(&write_contract(&missing)).is_err());
    let rejected = path
        .clone()
        .join("publications")
        .join("missing-readers.json");
    assert_eq!(
        fyrr_writer::prepare::put_immutable(store.as_ref(), &rejected, &missing)
            .await
            .map_err(fyrr_writer::WriterError::as_str),
        Err("publication_invalid")
    );
    assert!(matches!(
        store.get(&rejected).await,
        Err(object_store::Error::NotFound { .. })
    ));
    success(&config, &["writer", "commit"]);
    let continued = success(
        &config,
        &["reader", "search", "fixture document 3", "--limit", "1"],
    );
    assert_eq!(continued["hits"][0]["source_id"], "allowed");
    assert_eq!(continued["hits"][0]["root_id"], "3.txt");
    let head: ServingHead =
        fyrr_reader::storage::read_contract(store.as_ref(), &path.clone().join("head.json"))
            .await
            .expect("current head");
    let identity_path = object_store::path::Path::from(format!("{path}/{}", head.identity.object));
    let identity: fyrr_core::identity::IdentityArtifact =
        fyrr_reader::storage::read_contract(store.as_ref(), &identity_path)
            .await
            .expect("identity");
    let mut altered_identity = identity.clone();
    altered_identity.generation += 1;
    store
        .put(&identity_path, write_contract(&altered_identity).into())
        .await
        .expect("altered identity injection");
    // A valid artifact with altered bytes must also fail closed in the writer.
    let output = run(&config, &["writer", "status"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("authorization_unavailable"));
    store
        .put(&identity_path, write_contract(&identity).into())
        .await
        .expect("restore identity");
    write_head(store.as_ref(), &path, &head).await;
    // Identity-only commits move only the identity pointer; the content pin stays exact.
    let revoked = root.join("revoked.toml");
    std::fs::write(
        &revoked,
        config_text.replace("stdio_readers = [\"grp:allow\"]", "stdio_readers = []"),
    )
    .expect("revoked config");
    success(&revoked, &["writer", "commit"]);
    let revoked_head: ServingHead =
        fyrr_reader::storage::read_contract(store.as_ref(), &path.clone().join("head.json"))
            .await
            .expect("identity-only head");
    assert_eq!(revoked_head.content, head.content);
    assert_ne!(revoked_head.identity, head.identity);
    for query in ["fixture query", "cache miss"] {
        let output = run(&revoked, &["reader", "search", query]);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("authorization_unavailable"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("embedding_unavailable"));
    }
    success(&config, &["writer", "commit"]);
    let binary = Command::new(crate::common::binary())
        .arg("--config")
        .arg(&config)
        .args(["reader", "serve"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("warm MCP");
    let mut binary = binary;
    let mut stdin = binary.stdin.take().expect("stdin");
    let mut stdout = BufReader::new(binary.stdout.take().expect("stdout"));
    initialize_stdio(&mut stdin, &mut stdout);
    let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{"query":"fixture query","limit":75}}});
    writeln!(stdin, "{request}").expect("warm request");
    let mut line = String::new();
    stdout.read_line(&mut line).expect("warm response");
    assert_eq!(
        serde_json::from_str::<Value>(&line).expect("warm JSON")["result"]["isError"],
        false
    );
    // A no-op commit refreshes authorization without changing the pinned content.
    success(&config, &["writer", "commit"]);
    writeln!(stdin, "{request}").unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap()["result"]["isError"],
        false
    );
    // The same process adopts a new token set and rebuilds row/partition admission.
    let deny_only = root.join("deny-only.toml");
    std::fs::write(
        &deny_only,
        config_text.replace(
            "stdio_readers = [\"grp:allow\"]",
            "stdio_readers = [\"grp:deny\"]",
        ),
    )
    .unwrap();
    success(&deny_only, &["writer", "commit"]);
    writeln!(stdin, "{request}").unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let response: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(response["result"]["isError"], false);
    assert!(
        response["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|hit| hit["source_id"] == "denied")
    );
    success(&revoked, &["writer", "commit"]);
    for query in ["fixture query", "cache miss"] {
        writeln!(stdin, "{}", json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search","arguments":{"query":query}}})).unwrap();
        line.clear();
        stdout.read_line(&mut line).unwrap();
        assert!(line.contains("authorization_unavailable"));
        assert!(!line.contains("embedding_unavailable"));
    }
    success(&config, &["writer", "commit"]);
    // Valid policy-generation replacement is adopted by this live reader too.
    let mut head: ServingHead =
        fyrr_reader::storage::read_contract(store.as_ref(), &path.clone().join("head.json"))
            .await
            .unwrap();
    let mut policy: PolicyArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{path}/{}", head.policy.object)),
    )
    .await
    .unwrap();
    policy.generation += 1;
    let object = format!(
        "policy/{}-{}.json",
        policy.generation,
        &digest(&write_contract(&policy))[7..]
    );
    fyrr_writer::prepare::put_immutable(
        store.as_ref(),
        &object_store::path::Path::from(format!("{path}/{object}")),
        &policy,
    )
    .await
    .unwrap();
    head.policy = fyrr_core::publication::GenerationPointer {
        generation: policy.generation,
        object,
        digest: digest(&write_contract(&policy)),
    };
    store
        .put(
            &path.clone().join("head.json"),
            write_contract(&head).into(),
        )
        .await
        .unwrap();
    writeln!(stdin, "{request}").unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap()["result"]["isError"],
        false
    );
    let mut stale = head.clone();
    stale.policy.generation += 1;
    write_head(store.as_ref(), &path, &stale).await;
    writeln!(stdin, "{request}").expect("stale request");
    line.clear();
    stdout.read_line(&mut line).expect("stale response");
    assert!(line.contains("authorization_unavailable"));
    assert!(!line.contains("fixture document"));
    store
        .delete(&path.clone().join("head.json"))
        .await
        .expect("storage loss injection");
    writeln!(stdin, "{request}").expect("storage-loss request");
    line.clear();
    stdout.read_line(&mut line).expect("storage-loss response");
    assert!(line.contains("storage_unavailable"));
    assert!(!line.contains("fixture document"));
    write_head(store.as_ref(), &path, &head).await;
    drop(stdin);
    assert!(binary.wait().expect("MCP exit").success());
    let plan = success(&config, &["writer", "namespace", "delete"]);
    assert_eq!(plan["dry_run"], true);
    assert!(plan["objects"].as_u64().expect("object count") > 0);
    let export = root.join("verified-export");
    use futures::TryStreamExt;
    let mut objects = store.list(Some(&path));
    while let Some(object) = objects.try_next().await.expect("export inventory") {
        let relative = object
            .location
            .prefix_match(&path)
            .expect("namespace prefix")
            .map(|part| part.as_ref().to_owned())
            .collect::<Vec<_>>()
            .join("/");
        let file = export.join(relative);
        std::fs::create_dir_all(file.parent().expect("export parent")).expect("export directory");
        let bytes = store
            .get(&object.location)
            .await
            .expect("export object")
            .bytes()
            .await
            .expect("export bytes");
        std::fs::write(file, bytes).expect("export file");
    }
    let deleted = success(
        &config,
        &[
            "writer",
            "namespace",
            "delete",
            "--export",
            export.to_str().expect("export path"),
        ],
    );
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["retained_data"], true);
    assert!(!run(&config, &["reader", "check"]).status.success());
    assert_eq!(
        success(&config, &["writer", "namespace", "delete"])["deleted"],
        true
    );
}

fn principal_hash(config: &fyrr_core::config::Config, kind: &str, value: &str) -> String {
    fyrr_reader::storage::hash_principal(config, kind, value).unwrap()
}

fn fixture_hits(client: &mut tools::Mcp) -> Value {
    client.call(
        "search",
        json!({"query":"fixture query","sources":["incremental"],"limit":5}),
    )
}

fn free_address() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn write_head(
    store: &dyn object_store::ObjectStore,
    root: &object_store::path::Path,
    head: &ServingHead,
) {
    store
        .put(&root.clone().join("head.json"), write_contract(head).into())
        .await
        .expect("write fixture head");
}

async fn ready_request(
    child: &mut std::process::Child,
    stderr: &Path,
    request: reqwest::RequestBuilder,
) -> Option<reqwest::StatusCode> {
    assert!(
        child.try_wait().unwrap().is_none(),
        "{}",
        std::fs::read_to_string(stderr).unwrap()
    );
    request.send().await.ok().map(|response| response.status())
}

#[path = "dense_path/preparation_evidence.rs"]
mod preparation_evidence;

#[path = "dense_path/metadata_io.rs"]
mod metadata_io;

#[path = "dense_path/worker_io_probe.rs"]
mod worker_io_probe;

#[path = "dense_path/durability_syncs.rs"]
mod durability_syncs;
