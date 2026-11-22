//! The reader mirror across a namespace recreated at the same storage URI.
use super::*;
use futures::TryStreamExt;

const FIRST: &str = "first incarnation ledger entry";
const SECOND: &str = "second incarnation ledger entry";

/// Publishes one document as a fresh namespace at the configured URI.
fn publish(config: &Path, root: &FixtureRoot, text: &str) {
    let sources = root.join("sources");
    std::fs::remove_dir_all(&sources).unwrap();
    std::fs::create_dir_all(&sources).unwrap();
    std::fs::write(sources.join("ledger.txt"), text).unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both(text, vector);
    success(config, &["writer", "namespace", "create"]);
    let sync = run(config, &["writer", "sync"]);
    assert!(sync.status.success(), "{}", report(&sync));
    success(config, &["writer", "commit"]);
}

fn mirror_holds(mirror: &Path, text: &str) -> bool {
    std::fs::read_dir(mirror)
        .unwrap()
        .flat_map(|directory| std::fs::read_dir(directory.unwrap().path()).unwrap())
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "blob"))
        .any(|path| {
            std::fs::read(path)
                .unwrap()
                .windows(text.len())
                .any(|window| window == text.as_bytes())
        })
}

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn recreated_namespace_at_same_uri_never_serves_old_mirror_entries_on_local_storage() {
    recreated_namespace_at_same_uri_never_serves_old_mirror_entries(Backend::Local).await;
}

async fn recreated_namespace_at_same_uri_never_serves_old_mirror_entries(backend: Backend) {
    let (config, root) = setup_on(backend);
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    let mirror = root.join("mirror");

    publish(&config, &root, FIRST);
    let first = super::coordination::publication(&config).await.1;
    success(&config, &["reader", "prewarm"]);
    let search = success(&config, &["reader", "search", "ledger"]);
    assert!(search.to_string().contains(FIRST), "{search}");
    assert!(
        mirror_holds(&mirror, FIRST),
        "the first namespace's rows are physically in the mirror"
    );

    // A new namespace at the same URI: the bucket contents are replaced, the
    // reader's mirror directory is kept.
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let objects: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
    assert!(!objects.is_empty());
    for object in objects {
        store.delete(&object.location).await.unwrap();
    }
    publish(&config, &root, SECOND);
    let second = super::coordination::publication(&config).await.1;
    assert_ne!(first.publication_id, second.publication_id);
    let versions = |files: &[fyrr_core::envelope::FileRef]| -> std::collections::BTreeSet<String> {
        files
            .iter()
            .filter(|file| file.path.starts_with("_versions/"))
            .map(|file| file.path.clone())
            .collect()
    };
    assert_eq!(
        versions(&first.files),
        versions(&second.files),
        "both namespaces pin the same native version manifest name"
    );

    let check = run(&config, &["reader", "check"]);
    assert!(
        check.status.success(),
        "the reader must refresh a mirror entry of another namespace: {}",
        String::from_utf8_lossy(&check.stderr)
    );
    let search = success(&config, &["reader", "search", "ledger"]);
    let text = search.to_string();
    assert!(text.contains(SECOND), "{search}");
    assert!(
        !text.contains(FIRST),
        "rows of the replaced namespace: {search}"
    );
    assert!(
        !mirror_holds(&mirror, FIRST),
        "mirror entries of the replaced namespace are discarded"
    );
    std::fs::remove_dir_all(root).unwrap();
}

pub(super) const KEPT: &str = "kept ledger entry of both incarnations";
const DOOMED: &str = "doomed ledger entry of the first incarnation only";
pub(super) const EXTRA: &str = "extra ledger entry that advances the first sequence";

/// Replaces the source files and syncs them; `commit` publishes them.
pub(super) fn sync_texts(config: &Path, root: &FixtureRoot, texts: &[(&str, &str)]) {
    let sources = root.join("sources");
    std::fs::remove_dir_all(&sources).unwrap();
    std::fs::create_dir_all(&sources).unwrap();
    for (name, text) in texts {
        std::fs::write(sources.join(name), text).unwrap();
        let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
        root.embedding.insert_both(text, vector);
    }
    let sync = run(config, &["writer", "sync"]);
    assert!(sync.status.success(), "{}", report(&sync));
}

/// `namespace delete --export` after writing the verified export it demands.
async fn export_and_delete(config: &Path, root: &FixtureRoot) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let export = root.join(format!(
        "export-{}",
        fyrr_core::clock::now().timestamp_micros()
    ));
    let objects: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
    for object in objects {
        let file = export.join(fyrr_reader::storage::relative_path(
            &object.location,
            &namespace,
        ));
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        let bytes = store
            .get(&object.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        std::fs::write(file, bytes).unwrap();
    }
    let deleted = success(
        config,
        &[
            "writer",
            "namespace",
            "delete",
            "--export",
            export.to_str().unwrap(),
        ],
    );
    assert_eq!(deleted["deleted"], true, "{deleted}");
}

/// One search on the running reader: its publication and hits, or the
/// refusal code.
pub(super) fn served(client: &mut super::tools::Mcp) -> Result<(String, String), String> {
    let result = client.call("search", json!({"query":"ledger"}));
    if result["isError"] == true {
        return Err(result["structuredContent"]["code"]
            .as_str()
            .unwrap_or_default()
            .to_owned());
    }
    let content = &result["structuredContent"];
    Ok((
        content["metadata"]["publication_id"]
            .as_str()
            .unwrap()
            .to_owned(),
        content["hits"].to_string(),
    ))
}

/// Polls the running reader until it serves `publication`. Every answer on
/// the way is a refusal; none comes from another publication.
fn await_publication(
    client: &mut super::tools::Mcp,
    publication: &str,
    forbidden: &[&str],
) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        match served(client) {
            Ok((id, hits)) => {
                for text in forbidden {
                    assert!(
                        !hits.contains(text),
                        "the running reader served {text:?} from publication {id} \
                         while the head names {publication}: {hits}"
                    );
                }
                assert_eq!(
                    id, publication,
                    "the running reader served another publication"
                );
                return hits;
            }
            Err(code) => assert!(
                std::time::Instant::now() < deadline,
                "the running reader never switched to {publication}: {code}"
            ),
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn running_reader_never_serves_a_replaced_namespace_incarnation_on_local_storage() {
    running_reader_never_serves_a_replaced_namespace_incarnation(Backend::Local).await;
}

async fn running_reader_never_serves_a_replaced_namespace_incarnation(backend: Backend) {
    let (config, root) = setup_on(backend);
    std::fs::write(
        &config,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
    )
    .unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);

    // The first incarnation reaches sequence 2 and holds the doomed document.
    success(&config, &["writer", "namespace", "create"]);
    sync_texts(
        &config,
        &root,
        &[("kept.txt", KEPT), ("doomed.txt", DOOMED)],
    );
    success(&config, &["writer", "commit"]);
    let restorable = super::coordination::publication(&config).await.1;
    sync_texts(
        &config,
        &root,
        &[
            ("kept.txt", KEPT),
            ("doomed.txt", DOOMED),
            ("extra.txt", EXTRA),
        ],
    );
    success(&config, &["writer", "commit"]);
    let first = super::coordination::publication(&config).await.1;
    assert_eq!(first.sequence, 2);

    let mut client = super::tools::Mcp::open(&config);
    let (id, hits) = served(&mut client).unwrap();
    assert_eq!(id, first.publication_id);
    assert!(hits.contains(DOOMED) && hits.contains(EXTRA), "{hits}");

    // A restore stays in the lineage: a higher sequence that the running
    // reader switches to, without the rows the restore removed.
    success(&config, &["writer", "restore", &restorable.publication_id]);
    let restored = super::coordination::publication(&config).await.1;
    assert_eq!(restored.sequence, 3);
    let hits = await_publication(&mut client, &restored.publication_id, &[EXTRA]);
    assert!(hits.contains(DOOMED), "{hits}");

    // An unpublished namespace serves nothing.
    export_and_delete(&config, &root).await;
    assert_eq!(served(&mut client), Err("storage_unavailable".to_owned()));

    // The namespace is created again at the same URI. Before its first
    // commit it has no content; afterwards it restarts at sequence 1, below
    // the pinned publication's sequence 3.
    success(&config, &["writer", "namespace", "create"]);
    let created = served(&mut client);
    assert_eq!(
        created,
        Err("authorization_unavailable".to_owned()),
        "a namespace without content served the replaced incarnation"
    );
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    success(&config, &["writer", "commit"]);
    let second = super::coordination::publication(&config).await.1;
    assert_eq!(second.sequence, 1);
    assert_ne!(second.publication_id, restored.publication_id);
    let hits = await_publication(&mut client, &second.publication_id, &[DOOMED, EXTRA]);
    assert!(hits.contains(KEPT), "{hits}");
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

/// The shipped behaviour: requests check the serving head the reader's last
/// version check read. A running reader may answer from the publication it
/// holds until its next check, then follows the head: to a new publication,
/// and to a refusal once the namespace is gone.
#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn running_reader_with_a_shared_head_check_follows_the_head_within_its_interval() {
    let (config, root) = setup_on(Backend::Local);
    std::fs::write(
        &config,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("# fyrr-test-head-check = every", "")
            .replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
    )
    .unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    success(&config, &["writer", "namespace", "create"]);
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    success(&config, &["writer", "commit"]);
    let first = super::coordination::publication(&config).await.1;
    let mut client = super::tools::Mcp::open(&config);
    assert_eq!(served(&mut client).unwrap().0, first.publication_id);

    sync_texts(&config, &root, &[("kept.txt", KEPT), ("extra.txt", EXTRA)]);
    success(&config, &["writer", "commit"]);
    let second = super::coordination::publication(&config).await.1;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        // Until the reader has switched, an answer is a refusal or comes
        // whole from the publication it holds.
        match served(&mut client) {
            Ok((id, hits)) if id == second.publication_id => {
                assert!(hits.contains(EXTRA), "{hits}");
                break;
            }
            Ok((id, hits)) => {
                assert_eq!(id, first.publication_id);
                assert!(!hits.contains(EXTRA), "{hits}");
            }
            Err(_) => {}
        }
        assert!(std::time::Instant::now() < deadline, "never switched");
        std::thread::sleep(std::time::Duration::from_millis(250));
    }

    export_and_delete(&config, &root).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while served(&mut client) != Err("storage_unavailable".to_owned()) {
        assert!(
            std::time::Instant::now() < deadline,
            "the reader kept serving a deleted namespace"
        );
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

/// A reader that did not run while its version check came due, as in an
/// environment frozen between requests, checks before it answers: the first
/// request after the pause is served from the publication made during it.
#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn reader_paused_past_its_version_check_answers_from_the_new_publication() {
    let (config, root) = setup_on(Backend::Local);
    std::fs::write(
        &config,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("# fyrr-test-head-check = every", "")
            .replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
    )
    .unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    success(&config, &["writer", "namespace", "create"]);
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    success(&config, &["writer", "commit"]);
    let first = super::coordination::publication(&config).await.1;
    let mut client = super::tools::Mcp::open(&config);
    assert_eq!(served(&mut client).unwrap().0, first.publication_id);

    client.signal("STOP");
    sync_texts(&config, &root, &[("kept.txt", KEPT), ("extra.txt", EXTRA)]);
    success(&config, &["writer", "commit"]);
    let second = super::coordination::publication(&config).await.1;
    // Longer than the check interval and the gap the reader takes for a pause.
    std::thread::sleep(std::time::Duration::from_secs(3));
    client.signal("CONT");
    let (publication, hits) = served(&mut client).unwrap();
    assert_eq!(publication, second.publication_id);
    assert!(hits.contains(EXTRA), "{hits}");
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real HTTP MCP executable and local storage; scripts/ci"]
async fn running_http_reader_never_serves_a_revoked_grant_of_a_replaced_namespace_on_local_storage()
{
    running_http_replacement(Backend::Local).await;
}

async fn running_http_replacement(backend: Backend) {
    use super::coordination::worker_sync;
    use super::http_mcp::{Issuer, Reader, fixture_for_issuer};
    use super::incremental::{doc, input, publish};
    let (_, issuer_root) = setup_on(backend);
    let issuer = Issuer::new(&issuer_root).await;
    let (worker, commit, root, _) = fixture_for_issuer(&issuer.url, backend).await;
    std::fs::write(
        &worker,
        std::fs::read_to_string(&worker)
            .unwrap()
            .replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
    )
    .unwrap();
    let namespace = fyrr_core::config::load(&worker)
        .unwrap()
        .config
        .namespace
        .name;
    let root_id = format!("{namespace}-http-0");
    let first = super::coordination::publication(&commit).await.1;
    let mut reader =
        Reader::start_configured(&worker, &root, &issuer, 1000, 1000, 2, false, true).await;
    let holder = issuer.token("subject-0", json!({}), json!({}));
    let grantee = issuer.token("subject-1", json!({}), json!({}));
    let fetch = json!({"source_id":"incremental","root_id":root_id});
    let documents = |answer: &Value| {
        answer["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .len()
    };
    let granted = reader.call(&holder, 1, "fetch", fetch.clone()).await;
    assert_eq!(granted["isError"], false, "{granted}");
    assert_eq!(
        granted["structuredContent"]["metadata"]["publication_id"],
        first.publication_id
    );
    assert_eq!(documents(&granted), 1, "{granted}");

    // The namespace is deleted with its prefix, and created again with the
    // document granted to another subject instead.
    export_and_delete(&commit, &root).await;
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, prefix) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let objects: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
    assert!(objects.is_empty(), "{objects:?}");
    success(&commit, &["writer", "namespace", "create"]);
    worker_sync(&worker, &["allowed", "denied", "--full"]);
    let mut moved = doc(&root_id, "r2", "fixture document 0");
    moved["readers"] = json!(["usr:u1"]);
    let users: Vec<_> = (0..4)
        .map(|i| json!({"id":format!("u{i}"),"email":format!("http-{i}@example.test"),"email_verification":"verified_corporate","active":true}))
        .collect();
    input(
        &root,
        json!({"epoch":2,"content":[[moved]],"permissions":[[{"id":root_id,"revision":"p2","readers":["usr:u1"]}]],"members":[{"users":users,"groups":[]}]}),
    );
    worker_sync(
        &worker,
        &[
            "incremental",
            "--kind",
            "content",
            "--kind",
            "permissions",
            "--kind",
            "members",
        ],
    );
    publish(&commit);
    let pull = super::coordination::host_command(&commit)
        .args(["writer", "sync", "--directory"])
        .output()
        .unwrap();
    assert!(pull.status.success(), "{}", report(&pull));
    let second = super::coordination::publication(&commit).await.1;
    assert_ne!(second.publication_id, first.publication_id);

    // Until the reader serves the new namespace it refuses; afterwards the
    // revoked holder reads nothing and the new grantee reads the document.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut id = 10;
    loop {
        id += 1;
        let answer = reader.call(&holder, id, "fetch", fetch.clone()).await;
        if answer["isError"] == false {
            assert_eq!(
                documents(&answer),
                0,
                "a revoked grant of the replaced namespace was served: {answer}"
            );
            assert_eq!(
                answer["structuredContent"]["metadata"]["publication_id"],
                second.publication_id
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the running reader never switched to the new namespace: {answer}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    let moved = reader.call(&grantee, id + 1, "fetch", fetch).await;
    assert_eq!(moved["isError"], false, "{moved}");
    assert_eq!(documents(&moved), 1, "{moved}");
    reader.stop().await;
}
