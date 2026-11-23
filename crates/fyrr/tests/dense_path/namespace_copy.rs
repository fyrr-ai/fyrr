//! `writer namespace copy` on local storage: the copy is one version of a
//! namespace that is being written, it finds the same documents with the same
//! access, it is independent and writable, an interrupted copy leaves no
//! namespace and the next run completes it, and a key that is not the source's
//! is refused.
use super::*;
use fyrr_core::config::StorageBackend;
use fyrr_core::connector::CursorState;

fn storage_of(config: &Path) -> PathBuf {
    let loaded = fyrr_core::config::load(config).unwrap();
    let Some(StorageBackend::Local { root }) = loaded.config.storage.backend() else {
        panic!("local fixture");
    };
    root
}

/// The configuration of the same namespace in another local storage, with the
/// key of `config` unless `key` names another variable.
fn elsewhere(config: &Path, suffix: &str, key: Option<&str>) -> PathBuf {
    let storage = storage_of(config).display().to_string();
    let mut text = std::fs::read_to_string(config)
        .unwrap()
        .replace(&storage, &format!("{storage}-{suffix}"));
    if let Some(key) = key {
        text = text.replace(
            "hashing_key = { env = \"FYRR_TEST_SECRET\" }",
            &format!("hashing_key = {{ env = \"{key}\" }}"),
        );
    }
    let path = config.with_file_name(format!("{suffix}.toml"));
    std::fs::write(&path, text).unwrap();
    path
}

fn copy(config: &Path, to: &Path) -> Output {
    std::process::Command::new(crate::common::binary())
        .arg("--config")
        .arg(config)
        .args(["writer", "namespace", "copy", "--to-config"])
        .arg(to)
        .env("FYRR_TEST_OTHER_KEY", "another key")
        .output()
        .unwrap()
}

fn copied(config: &Path, to: &Path) -> Value {
    let output = copy(config, to);
    assert!(
        output.status.success(),
        "copy to {}: {}",
        to.display(),
        report(&output)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// `writer sync` prints a line for each source.
fn sync(config: &Path) {
    let output = run(config, &["writer", "sync"]);
    assert!(output.status.success(), "{}", report(&output));
}

fn refused(output: &Output, code: &str) {
    assert!(!output.status.success(), "{code}: {}", report(output));
    assert!(report(output).contains(code), "{code}: {}", report(output));
}

/// Every object of a namespace with its size and time of change.
fn objects(storage: &Path) -> Vec<(String, u64, std::time::SystemTime)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let root = storage.join("namespaces/dense-fixture");
    let mut files = Vec::new();
    walk(&root, &mut files);
    let mut listed: Vec<_> = files
        .into_iter()
        .map(|file| {
            let meta = std::fs::metadata(&file).unwrap();
            (
                file.strip_prefix(&root).unwrap().display().to_string(),
                meta.len(),
                meta.modified().unwrap(),
            )
        })
        .collect();
    listed.sort();
    listed
}

/// What the stdio reader of `config` sees: only the documents its tokens allow.
fn visible(config: &Path) {
    let search = success(
        config,
        &["reader", "search", "fixture query", "--limit", "20"],
    );
    let hits = search["hits"].as_array().unwrap();
    assert!(!hits.is_empty(), "the copy finds documents: {search}");
    assert!(
        hits.iter().all(|hit| hit["source_id"] != "denied"),
        "the access of the copy is the source's: {search}"
    );
    let mut client = super::tools::Mcp::open(config);
    let denied = client.call("fetch", json!({"source_id": "denied", "root_id": "0.txt"}));
    assert_eq!(
        denied["structuredContent"]["documents"],
        json!([]),
        "{denied}"
    );
    // A document with a connector map, which is read from beside the dataset.
    let map = client.call(
        "fetch",
        json!({"source_id": "metadata", "root_id": "doc-0"}),
    );
    assert_eq!(map["isError"], false, "{map}");
    assert_eq!(
        map["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "{map}"
    );
    assert!(map.to_string().contains("note-0"), "{map}");
}

fn fetched(config: &Path, root_id: &str) -> String {
    let mut client = super::tools::Mcp::open(config);
    let found = client.call("fetch", json!({"source_id": "allowed", "root_id": root_id}));
    found["structuredContent"]["documents"].to_string()
}

#[allow(clippy::print_stderr)]
#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn local_copy_is_one_version_independent_and_completes_after_interruption() {
    let (config, root) = setup_on(Backend::Local);
    let storage = storage_of(&config);
    // A source whose documents carry a connector map, which a reader loads
    // from beside the dataset.
    let mapped = root.join("documents.json");
    let documents: Vec<_> = (0..2)
        .map(|n| json!({"id": format!("doc-{n}"), "title": "document", "author": "Ada", "readers": ["grp:allow"], "note": format!("note-{n}"), "text": format!("fixture document {n}")}))
        .collect();
    std::fs::write(
        &mapped,
        serde_json::to_vec(&json!({"documents": documents, "cursor": "one"})).unwrap(),
    )
    .unwrap();
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!(
            "{text}\n[[sources]]\nid='metadata'\ntype='sentinel-docs'\npermissions='fixed'\nreaders=['grp:allow']\n[sources.scope]\nfixture='{}'\n",
            mapped.display()
        ),
    )
    .unwrap();
    success(&config, &["writer", "namespace", "create"]);
    sync(&config);
    success(&config, &["writer", "commit"]);

    // The same storage would need another name, and another name other hashes.
    refused(&copy(&config, &config), "copy_same_storage");
    // The destination file is validated like `config check`: a key on a local
    // storage is refused before anything is written.
    let keyed = elsewhere(&config, "keyed", None);
    let text = std::fs::read_to_string(&keyed).unwrap();
    std::fs::write(
        &keyed,
        text.replacen("[storage]\n", "[storage]\nkms_key_id = \"alias/x\"\n", 1),
    )
    .unwrap();
    refused(&copy(&config, &keyed), "storage.kms_key_id");
    assert!(
        !storage_of(&keyed).exists(),
        "nothing is written to a refused destination"
    );

    // The source is quiet: a copy reads it and changes none of its objects.
    let before = objects(&storage);
    let (source_head, source_manifest) = super::coordination::publication(&config).await;
    let backup = elsewhere(&config, "b", None);
    let done = copied(&config, &backup);
    assert_eq!(done["copied"], true, "{done}");
    assert_eq!(
        objects(&storage),
        before,
        "the copy never modifies the source"
    );
    // Measured on the native binary; docs/operations.md records the figure.
    let megabytes = done["bytes"].as_f64().unwrap() / 1e6;
    let seconds = done["seconds"].as_f64().unwrap();
    eprintln!(
        "namespace copy: {megabytes:.1} MB in {seconds:.2} s = {:.1} MB/s",
        megabytes / seconds
    );

    // One version, found with the same access, and as whole as the source.
    let (head, manifest) = super::coordination::publication(&backup).await;
    assert_eq!(manifest.sequence, source_manifest.sequence);
    assert_ne!(head.incarnation, source_head.incarnation);
    success(&backup, &["reader", "check"]);
    assert_eq!(success(&backup, &["writer", "verify"])["verified"], true);
    assert_eq!(
        super::coordination::pinned_count(&backup).await,
        super::coordination::pinned_count(&config).await
    );
    visible(&backup);

    // The sequences of the cursors go along, so that the next write of a
    // source is newer than every row the copy holds; their positions do not.
    let state: CursorState = serde_json::from_slice(
        &std::fs::read(
            storage_of(&backup).join("namespaces/dense-fixture/cursors/allowed-content.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let origin: CursorState = serde_json::from_slice(
        &std::fs::read(storage.join("namespaces/dense-fixture/cursors/allowed-content.json"))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(state.sequence, origin.sequence);
    assert!(state.sequence > 0 && state.cursor.is_none() && state.lease.is_none());

    // Repeating a copy that finished is refused: the destination exists.
    refused(&copy(&config, &backup), "namespace_exists");

    // The copy is writable and independent: a revised and a new document are
    // published and found, and the source does not change.
    root.embedding.insert_both(
        "fixture document 0 revised",
        root.embedding.vector("fixture document 0"),
    );
    std::fs::write(root.join("sources/0.txt"), "fixture document 0 revised").unwrap();
    sync(&backup);
    success(&backup, &["writer", "commit"]);
    assert!(
        fetched(&backup, "0.txt").contains("revised"),
        "a revision of a copied document is not refused"
    );
    assert_eq!(
        objects(&storage),
        before,
        "a write to the copy leaves the source alone"
    );
    std::fs::write(root.join("sources/0.txt"), "fixture document 0").unwrap();

    // A copy of a namespace that is written is one of its versions.
    for revision in 1..6 {
        let text = format!("fixture document {revision} revised");
        root.embedding.insert_both(
            &text,
            root.embedding
                .vector(&format!("fixture document {revision}")),
        );
    }
    let writer = {
        let (config, sources) = (config.clone(), root.join("sources"));
        std::thread::spawn(move || {
            for revision in 1..6 {
                let text = format!("fixture document {revision} revised");
                std::fs::write(sources.join(format!("{revision}.txt")), &text).unwrap();
                let sync = run(&config, &["writer", "sync"]);
                assert!(sync.status.success(), "{}", report(&sync));
                let commit = run(&config, &["writer", "commit"]);
                assert!(commit.status.success(), "{}", report(&commit));
            }
        })
    };
    let live = elsewhere(&config, "live", None);
    copied(&config, &live);
    writer.join().unwrap();
    success(&live, &["reader", "check"]);
    assert_eq!(success(&live, &["writer", "verify"])["verified"], true);
    visible(&live);

    // A copy that stopped before its head leaves no readable namespace, and
    // running the command again completes it.
    let stopped = storage_of(&live).join("namespaces/dense-fixture");
    std::fs::remove_file(stopped.join("head.json")).unwrap();
    let data = std::fs::read_dir(stopped.join("dataset.lance/data"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(data).unwrap();
    let unreadable = run(&live, &["reader", "check"]);
    assert!(!unreadable.status.success(), "{}", report(&unreadable));
    // What is not a copy's own debris is not removed.
    let loaded = fyrr_core::config::load(&live).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let kept = namespace.join("fence/kept.json");
    store.put(&kept, "{}".into()).await.unwrap();
    refused(&copy(&config, &live), "namespace_deleting");
    assert!(store.head(&kept).await.is_ok());
    store.delete(&kept).await.unwrap();
    copied(&config, &live);
    success(&live, &["reader", "check"]);
    assert_eq!(success(&live, &["writer", "verify"])["verified"], true);
    visible(&live);

    // A destination with another hashing key could not read the rows: refused,
    // and nothing is written there.
    let other = elsewhere(&config, "other", Some("FYRR_TEST_OTHER_KEY"));
    refused(&copy(&config, &other), "hashing_key_mismatch");
    assert!(!storage_of(&other).join("namespaces").exists());

    for suffix in ["b", "live", "other"] {
        let _ = std::fs::remove_dir_all(format!("{}-{suffix}", storage.display()));
    }
    std::fs::remove_dir_all(storage).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
