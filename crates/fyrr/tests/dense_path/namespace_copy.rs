//! `writer namespace copy` on local storage: the copy is one version of a
//! namespace that is being written, it hashes under the name of its source, an
//! interrupted copy leaves no namespace and the next run completes it, and a
//! key that is not the source's is refused.
use super::*;
use fyrr_core::config::StorageBackend;

/// The configuration of `name`, in the storage and with the key of `config`.
fn renamed(config: &Path, name: &str) -> PathBuf {
    let text = std::fs::read_to_string(config).unwrap();
    let path = config.with_file_name(format!("{name}.toml"));
    std::fs::write(
        &path,
        text.replace("name = \"dense-fixture\"", &format!("name = \"{name}\"")),
    )
    .unwrap();
    path
}

/// A file that is replaced whole, so that a command never reads half of it.
fn replace(path: &Path, text: &str) {
    let part = path.with_extension("part");
    std::fs::write(&part, text).unwrap();
    std::fs::rename(part, path).unwrap();
}

fn storage_of(config: &Path) -> PathBuf {
    let loaded = fyrr_core::config::load(config).unwrap();
    let Some(StorageBackend::Local { root }) = loaded.config.storage.backend() else {
        panic!("local fixture");
    };
    root
}

/// Every object of a namespace with its size and time of change.
fn objects(storage: &Path, name: &str) -> Vec<(String, u64, std::time::SystemTime)> {
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
    let root = storage.join("namespaces").join(name);
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

fn refused(output: &Output, code: &str) {
    assert!(!output.status.success(), "{}", report(output));
    assert!(report(output).contains(code), "{}", report(output));
}

/// The first search hits and fetch of a namespace, as the stdio reader sees it.
fn visible(config: &Path) -> (Vec<String>, usize) {
    let search = success(
        config,
        &["reader", "search", "fixture query", "--limit", "20"],
    );
    let hits = search["hits"].as_array().unwrap();
    assert!(!hits.is_empty(), "the copy finds documents: {search}");
    assert!(
        hits.iter().all(|hit| hit["source_id"] == "allowed"),
        "the ACL of the copy is the source's: {search}"
    );
    let mut client = super::tools::Mcp::open(config);
    let denied = client.call("fetch", json!({"source_id": "denied", "root_id": "0.txt"}));
    assert_eq!(
        denied["structuredContent"]["documents"],
        json!([]),
        "{denied}"
    );
    let allowed = client.call("fetch", json!({"source_id": "allowed", "root_id": "0.txt"}));
    let documents = allowed["structuredContent"]["documents"]
        .as_array()
        .unwrap();
    (
        hits.iter()
            .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
            .collect(),
        documents.len(),
    )
}

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn local_copy_is_one_version_hashes_as_its_source_and_completes_after_interruption() {
    let (config, root) = setup_on(Backend::Local);
    let storage = storage_of(&config);
    success(&config, &["writer", "namespace", "create"]);
    success(&config, &["writer", "sync"]);
    success(&config, &["writer", "commit"]);

    // The source is quiet: a copy reads it and changes none of its objects.
    let before = objects(&storage, "dense-fixture");
    let copied = success(
        &config,
        &["writer", "namespace", "copy", "dense-fixture", "copy-quiet"],
    );
    assert_eq!(copied["copied"], true, "{copied}");
    assert_eq!(
        objects(&storage, "dense-fixture"),
        before,
        "the copy never modifies the source"
    );
    // Measured on the native binary; docs/operations.md records the figure.
    let megabytes = copied["bytes"].as_f64().unwrap() / 1e6;
    let seconds = copied["seconds"].as_f64().unwrap();
    eprintln!(
        "namespace copy: {megabytes:.1} MB in {seconds:.2} s = {:.1} MB/s",
        megabytes / seconds
    );

    // The copy is a namespace of its own that records the name of its hashes:
    // readers find the documents the source's reader tokens allow.
    let quiet = renamed(&config, "copy-quiet");
    let loaded = fyrr_core::config::load(&quiet).unwrap();
    let (store, path) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let (head, _) = super::coordination::publication(&quiet).await;
    let policy: PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &path, &head.policy)
            .await
            .unwrap();
    assert_eq!(policy.settings.hash_name.as_deref(), Some("dense-fixture"));
    assert_eq!(
        policy.schema_version.to_string(),
        "4.0",
        "an old build refuses it"
    );
    assert!(!policy.read_only);
    success(&quiet, &["reader", "check"]);
    assert_eq!(success(&quiet, &["writer", "verify"])["verified"], true);
    assert_eq!(
        super::coordination::pinned_count(&quiet).await,
        super::coordination::pinned_count(&config).await
    );
    visible(&quiet);

    // The copy is writable: a document of its own, hashed as the source's
    // readers are, is found by the same reader.
    std::fs::write(root.join("sources/fresh.txt"), "fixture document fresh").unwrap();
    root.embedding.insert_both(
        "fixture document fresh",
        root.embedding.vector("fixture document 0"),
    );
    success(&quiet, &["writer", "sync"]);
    success(&quiet, &["writer", "commit"]);
    let mut client = super::tools::Mcp::open(&quiet);
    let fresh = client.call(
        "fetch",
        json!({"source_id": "allowed", "root_id": "fresh.txt"}),
    );
    assert_eq!(
        fresh["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "{fresh}"
    );
    let denied = client.call(
        "fetch",
        json!({"source_id": "denied", "root_id": "fresh.txt"}),
    );
    assert_eq!(
        denied["structuredContent"]["documents"],
        json!([]),
        "{denied}"
    );
    drop(client);
    assert_eq!(
        objects(&storage, "dense-fixture"),
        before,
        "a write to the copy leaves the source alone"
    );
    std::fs::remove_file(root.join("sources/fresh.txt")).unwrap();

    // A copy of a namespace that is written is one of its versions.
    for revision in 0..6 {
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
            for revision in 0..6 {
                let text = format!("fixture document {revision} revised");
                std::fs::write(sources.join(format!("{revision}.txt")), &text).unwrap();
                let sync = run(&config, &["writer", "sync"]);
                assert!(sync.status.success(), "{}", report(&sync));
                let commit = run(&config, &["writer", "commit"]);
                assert!(commit.status.success(), "{}", report(&commit));
            }
        })
    };
    let live = success(
        &config,
        &["writer", "namespace", "copy", "dense-fixture", "copy-live"],
    );
    writer.join().unwrap();
    let live_config = renamed(&config, "copy-live");
    assert_eq!(live["copied"], true, "{live}");
    success(&live_config, &["reader", "check"]);
    assert_eq!(
        success(&live_config, &["writer", "verify"])["verified"],
        true
    );
    visible(&live_config);
    assert_eq!(
        success(&live_config, &["writer", "commit"])["envelope_ids"],
        json!([]),
        "the copy has no pending envelopes of the source"
    );

    // Repeating a copy that finished is refused: the destination exists.
    refused(
        &run(
            &config,
            &["writer", "namespace", "copy", "dense-fixture", "copy-quiet"],
        ),
        "namespace_exists",
    );

    // A copy that stopped before its head leaves no readable namespace, and
    // running the command again completes it.
    let stopped = storage.join("namespaces/copy-quiet");
    std::fs::remove_file(stopped.join("head.json")).unwrap();
    let data = std::fs::read_dir(stopped.join("dataset.lance/data"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(data).unwrap();
    let unreadable = run(&quiet, &["reader", "check"]);
    assert!(!unreadable.status.success(), "{}", report(&unreadable));
    let again = success(
        &config,
        &["writer", "namespace", "copy", "dense-fixture", "copy-quiet"],
    );
    assert_eq!(again["copied"], true, "{again}");
    success(&quiet, &["reader", "check"]);
    assert_eq!(success(&quiet, &["writer", "verify"])["verified"], true);
    visible(&quiet);

    // What is not a copy's own debris is not removed.
    let foreign = storage.join("namespaces/copy-foreign/cursors");
    std::fs::create_dir_all(&foreign).unwrap();
    std::fs::write(foreign.join("kept.json"), "{}").unwrap();
    refused(
        &run(
            &config,
            &[
                "writer",
                "namespace",
                "copy",
                "dense-fixture",
                "copy-foreign",
            ],
        ),
        "namespace_deleting",
    );
    assert!(foreign.join("kept.json").is_file());

    // A destination with another hashing key could not read the rows: refused,
    // and nothing is written there.
    let other = root.join("other-key.toml");
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &other,
        text.replace(
            "hashing_key = { env = \"FYRR_TEST_SECRET\" }",
            "hashing_key = { env = \"FYRR_TEST_OTHER_KEY\" }",
        )
        .replace(
            &storage.display().to_string(),
            &format!("{}-other", storage.display()),
        ),
    )
    .unwrap();
    let mismatch = std::process::Command::new(crate::common::binary())
        .arg("--config")
        .arg(&config)
        .args(["writer", "namespace", "copy", "dense-fixture", "other"])
        .arg("--to-config")
        .arg(&other)
        .env("FYRR_TEST_OTHER_KEY", "another key")
        .output()
        .unwrap();
    refused(&mismatch, "hashing_key_mismatch");
    assert!(!PathBuf::from(format!("{}-other/namespaces/other", storage.display())).exists());

    // The same key in another storage: a copy there, readable and verified.
    let elsewhere = root.join("elsewhere.toml");
    std::fs::write(
        &elsewhere,
        text.replace(
            &storage.display().to_string(),
            &format!("{}-b", storage.display()),
        )
        .replace("name = \"dense-fixture\"", "name = \"remote\""),
    )
    .unwrap();
    let moved = std::process::Command::new(crate::common::binary())
        .arg("--config")
        .arg(&config)
        .args(["writer", "namespace", "copy", "dense-fixture", "remote"])
        .arg("--to-config")
        .arg(&elsewhere)
        .output()
        .unwrap();
    assert!(moved.status.success(), "{}", report(&moved));
    success(&elsewhere, &["reader", "check"]);
    assert_eq!(success(&elsewhere, &["writer", "verify"])["verified"], true);
    visible(&elsewhere);

    for suffix in ["-b", "-other"] {
        let _ = std::fs::remove_dir_all(format!("{}{suffix}", storage.display()));
    }
    std::fs::remove_dir_all(storage).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
