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
    use std::time::Duration;
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
    let old = std::time::SystemTime::from(fyrr_core::clock::now()) - Duration::from_secs(1200);
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

#[tokio::test]
#[ignore = "real executable and long local publication history; scripts/ci"]
async fn lifetime_history_keeps_maintenance_restore_and_receipts_available() {
    use super::coordination::{command, publication, worker_sync};
    use super::incremental::{envelopes, input, publish, publish_initial, roots, rows};
    use fyrr_core::connector::SyncKind;
    use object_store::ObjectStoreExt;

    let fixture_dir = std::env::var_os("FYRR_TEST_HISTORY_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR")));
    let setup = setup_at_on(
        fixture_dir.join(format!(
            "fyrr-history-{}-{}",
            std::process::id(),
            fyrr_core::clock::now().timestamp_micros()
        )),
        Backend::Local,
    );
    for row in 1..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (worker, commit, root) =
        super::incremental::fixture_host_at(1, true, "dense-fixture", setup);
    publish_initial(&worker, &commit, &root);
    let (mut head, template) = publication(&commit).await;
    let oldest = template.envelope_ids.first().unwrap().clone();
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let Some(StorageBackend::Local { root: storage }) = loaded.config.storage.backend() else {
        panic!("local history fixture");
    };
    let member = envelopes(&worker)
        .await
        .into_iter()
        .find(|(_, e)| {
            e.source_id == "incremental"
                && e.kind == SyncKind::Members
                && fyrr_writer::apply::complete_read(e)
        })
        .unwrap()
        .1;
    let mut cleaned_replay = None;
    let mut latest_applied = None;
    let mut previous = template.publication_id.clone();
    // Valid immutable history, without thousands of unnecessary native writes.
    // Lifetime receipt evidence exceeds the former aggregate 65,536-entry cap.
    // The invariant is the aggregate receipt count, not publication depth.
    // Four individually valid manifests remain below the former 65,536-ID
    // limit, but their 70,400 distinct IDs cross it together. Keep all 33,000
    // real envelope objects to cross the 32,768 active-object boundary too.
    const PUBLICATIONS: u64 = 4;
    const RECEIPTS_PER_PUBLICATION: u64 = 17_600;
    const { assert!(PUBLICATIONS * RECEIPTS_PER_PUBLICATION > 65_536) };
    let mut seed = Vec::with_capacity(33_000 + PUBLICATIONS as usize);
    for offset in 1..=PUBLICATIONS {
        let mut manifest = template.clone();
        manifest.sequence += offset;
        manifest.publication_id = format!("pub-{:032x}", 1_000_000 + offset);
        manifest.previous_publication_id = Some(previous);
        manifest.envelope_ids.clear();
        for id in 0..RECEIPTS_PER_PUBLICATION {
            let mut envelope = member.clone();
            envelope.sequence = 1_000_000 + offset * RECEIPTS_PER_PUBLICATION + id;
            envelope.envelope_id = fyrr_core::envelope::envelope_id(
                &envelope.namespace,
                &envelope.source_id,
                envelope.kind,
                envelope.sequence,
                envelope.rejection_generation,
                envelope.cursor.start.as_deref(),
            );
            envelope.created_at = fyrr_core::clock::now() - chrono::Duration::days(4);
            if let fyrr_core::envelope::EnvelopePayload::Members(payload) = &mut envelope.payload {
                payload.read_at = envelope.created_at;
            }
            envelope.validate().unwrap();
            if (offset - 1) * RECEIPTS_PER_PUBLICATION + id < 33_000 {
                let path = namespace
                    .clone()
                    .join("envelopes")
                    .join(format!("{}.json", envelope.envelope_id));
                seed.push((path.clone(), write_contract(&envelope)));
                if cleaned_replay.is_none() {
                    cleaned_replay = Some((
                        path.clone(),
                        envelope.clone(),
                        manifest.publication_id.clone(),
                    ));
                }
                latest_applied = Some(path);
            }
            manifest.envelope_ids.push(envelope.envelope_id);
        }
        manifest.validate().unwrap();
        seed.push((
            object_store::path::Path::from(format!(
                "{namespace}/publications/{}.json",
                manifest.publication_id
            )),
            write_contract(&manifest),
        ));
        previous = manifest.publication_id;
        let pointer = head.content.as_mut().unwrap();
        pointer.publication_id = previous.clone();
        pointer.manifest = format!("publications/{previous}.json");
        pointer.sequence = manifest.sequence;
    }
    assert_eq!(seed.len(), 33_000 + PUBLICATIONS as usize);
    fyrr_reader::storage::test_store::seed_local_fixture(&loaded.config, seed).unwrap();
    store
        .put(&namespace.join("head.json"), write_contract(&head).into())
        .await
        .unwrap();
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions", "--full"]);
    for attempt in 0..2 {
        let result = command(&commit, &["writer", "maintain"]);
        if attempt == 0 {
            assert!(result["deleted_envelopes"].as_u64().unwrap() >= 32_999);
        }
    }
    store.head(&latest_applied.unwrap()).await.unwrap();
    let (replay_path, replay, replay_publication) = cleaned_replay.unwrap();
    assert!(matches!(
        store.head(&replay_path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(
        roots(&rows(&worker).await),
        ["b"],
        "startup cleanup preserves the queued revoke and maintenance applies it"
    );
    let receipt = command(&commit, &["writer", "commit", "--receipt", &oldest]);
    assert_eq!(receipt["publication_id"], template.publication_id);
    let before_replay = publication(&commit).await.0.content.unwrap().sequence;
    store
        .put(&replay_path, write_contract(&replay).into())
        .await
        .unwrap();
    publish(&commit);
    assert_eq!(
        publication(&commit).await.0.content.unwrap().sequence,
        before_replay,
        "a cleaned envelope reappearing in the log is still already published"
    );
    assert_eq!(
        command(
            &commit,
            &["writer", "commit", "--receipt", &replay.envelope_id]
        )["publication_id"],
        replay_publication
    );

    command(&commit, &["writer", "restore", &template.publication_id]);
    assert_eq!(
        roots(&rows(&worker).await),
        ["b"],
        "restore preserves the revoke"
    );
    assert_eq!(
        command(&commit, &["writer", "commit", "--receipt", &oldest])["publication_id"],
        template.publication_id
    );
    std::fs::remove_dir_all(storage).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
