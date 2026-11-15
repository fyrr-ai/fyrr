//! The single-host flow on a `file://` storage root, with no S3 and no Docker:
//! namespace creation, sync, commit, reader check, CLI and MCP retrieval with
//! permission filtering, maintenance and verification.
use super::*;
use fyrr_core::config::StorageBackend;
use std::os::unix::fs::PermissionsExt;

fn mode(path: &Path) -> u32 {
    std::fs::symlink_metadata(path)
        .unwrap()
        .permissions()
        .mode()
        & 0o777
}

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn local_storage_publishes_and_serves_with_permission_filtering() {
    let (config, root) = setup_on(Backend::Local);
    // Exercise URI decoding all the way through control storage, Lance and
    // mirror cache keys, rather than testing only paths without spaces.
    let text = std::fs::read_to_string(&config).unwrap();
    let text = text
        .lines()
        .map(|line| {
            if let Some(uri) = line.strip_prefix("uri = \"file://") {
                format!("uri = \"file://{}%20storage\"", uri.trim_end_matches('"'))
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&config, text).unwrap();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let Some(StorageBackend::Local { root: storage }) = loaded.config.storage.backend() else {
        panic!("the fixture names local storage");
    };
    assert!(!storage.exists());
    let check = run(&config, &["config", "check"]);
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );

    success(&config, &["writer", "namespace", "create"]);
    assert_eq!(mode(&storage), 0o700, "fyrr creates the root private");
    let sync = run(&config, &["writer", "sync"]);
    assert!(
        sync.status.success(),
        "{}",
        String::from_utf8_lossy(&sync.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&sync.stdout).lines().count(), 2);
    success(&config, &["writer", "commit"]);

    // The control objects and the Lance dataset share the root.
    let (head, manifest) = super::coordination::publication(&config).await;
    head.validate().unwrap();
    manifest.validate().unwrap();
    let namespace = storage.join("namespaces/dense-fixture");
    assert!(namespace.join("head.json").is_file());
    let versions = namespace.join("dataset.lance/_versions");
    assert!(
        std::fs::read_dir(&versions).unwrap().count() >= 1,
        "Lance committed its versions locally"
    );
    assert_eq!(
        super::coordination::pinned_count(&config).await,
        1536,
        "both sources' rows are published"
    );
    assert!(storage.join(".fyrr-storage.lock").is_file());
    assert_eq!(mode(&storage.join(".fyrr-storage.lock")), 0o600);

    success(&config, &["reader", "check"]);
    let search = success(
        &config,
        &["reader", "search", "fixture query", "--limit", "20"],
    );
    let hits = search["hits"].as_array().unwrap();
    assert!(!hits.is_empty());
    assert!(
        hits.iter().all(|hit| hit["source_id"] == "allowed"),
        "grp:allow never sees the denied source: {search}"
    );
    assert!(
        search["metadata"]["degraded"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let mut client = super::tools::Mcp::open(&config);
    let found = client.call("search", json!({"query": "fixture query", "limit": 20}));
    assert_eq!(found["isError"], false, "{found}");
    assert_eq!(found["structuredContent"]["hits"], search["hits"]);
    let allowed = client.call("fetch", json!({"source_id": "allowed", "root_id": "0.txt"}));
    let documents = allowed["structuredContent"]["documents"]
        .as_array()
        .unwrap();
    assert_eq!(documents.len(), 1, "{allowed}");
    assert!(allowed.to_string().contains("fixture document 0"));
    let denied = client.call("fetch", json!({"source_id": "denied", "root_id": "0.txt"}));
    assert_eq!(denied["isError"], false, "{denied}");
    assert_eq!(
        denied["structuredContent"]["documents"],
        json!([]),
        "a denied document looks like an unknown one"
    );
    drop(client);

    // A commit with nothing pending, maintenance and verification converge.
    success(&config, &["writer", "commit"]);
    let maintain = run(&config, &["writer", "maintain"]);
    assert!(
        maintain.status.success(),
        "{}",
        String::from_utf8_lossy(&maintain.stderr)
    );
    assert_eq!(success(&config, &["writer", "verify"])["verified"], true);
    success(&config, &["reader", "check"]);

    // A file URI takes no S3 key; `config check` names the key, not its value.
    let s3_key = root.join("s3-key.toml");
    std::fs::write(
        &s3_key,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("[embedding]", "region = \"eu-central-1\"\n[embedding]"),
    )
    .unwrap();
    let refused = run(&s3_key, &["config", "check"]);
    assert_eq!(refused.status.code(), Some(78));
    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&refused.stdout),
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(report.contains("storage.region"), "{report}");
    assert!(!report.contains("eu-central-1"), "{report}");
}

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn local_storage_reaps_abandoned_uploads_but_protects_live_staging() {
    use std::time::{Duration, SystemTime};
    let (config, _root) = setup_on(Backend::Local);
    let text = std::fs::read_to_string(&config).unwrap();
    // Keep this lifecycle proof small while writing real Lance document data.
    std::fs::write(
        &config,
        text.replace("[sources.scope]", "[sources.scope]\ninclude = [\"0.txt\"]"),
    )
    .unwrap();
    success(&config, &["writer", "namespace", "create"]);
    assert!(run(&config, &["writer", "sync"]).status.success());
    success(&config, &["writer", "commit"]);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let Some(StorageBackend::Local { root: storage }) = loaded.config.storage.backend() else {
        panic!("local fixture");
    };
    let data = storage.join("namespaces/dense-fixture/dataset.lance/data");
    let original = std::fs::read_dir(&data)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "lance")
        })
        .unwrap();
    let abandoned = data.join("killed-upload.lance#123");
    let young = data.join("live-upload.lance#456");
    let ordinary = data.join("keep#not-digits");
    let old = SystemTime::now() - Duration::from_secs(1200);
    for path in [&abandoned, &young, &ordinary] {
        std::fs::copy(&original, path).unwrap();
    }
    let age = |path: &Path| {
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old))
            .unwrap();
    };
    age(&abandoned);
    age(&ordinary);
    // This is precisely the hidden namespace: the native object listing omits it.
    use futures::TryStreamExt;
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let listed: Vec<_> = store
        .list(Some(&namespace.join("dataset.lance/data")))
        .try_collect()
        .await
        .unwrap();
    assert!(
        !listed
            .iter()
            .any(|meta| meta.location.to_string().contains("killed-upload"))
    );

    // A long-running worker's cursor lease protects even an old staging file.
    let worker = fyrr_writer::sync_lease::CursorLease::acquire(
        &loaded.config,
        "allowed",
        fyrr_core::connector::SyncKind::Content,
    )
    .await
    .unwrap();
    success(&config, &["writer", "maintain"]);
    assert!(abandoned.exists(), "a live upload must not be reaped");
    worker.release().await.unwrap();
    let cleaned = success(&config, &["writer", "maintain"]);
    assert!(
        !abandoned.exists(),
        "maintenance removes unlisted document data"
    );
    assert!(cleaned["deleted_files"].as_u64().unwrap() >= 1);
    assert!(young.exists(), "recent uploads remain protected");
    assert!(ordinary.exists(), "only multipart staging names are reaped");

    std::fs::copy(&original, &abandoned).unwrap();
    age(&abandoned);
    let plan = success(&config, &["writer", "source", "purge", "allowed"]);
    assert_eq!(plan["namespace_staging_files"], 1);
    assert!(abandoned.exists(), "the purge plan is read-only");
    let purged = success(
        &config,
        &["writer", "source", "purge", "allowed", "--confirm"],
    );
    assert_eq!(purged["deleted_staging_files"], 1);
    assert!(
        !abandoned.exists(),
        "purge removes the abandoned upload's text"
    );
    assert!(young.exists(), "purge also protects recent uploads");
    age(&young);
    success(&config, &["writer", "maintain"]);
    assert!(
        !young.exists(),
        "later maintenance converges after the safety window"
    );
}
