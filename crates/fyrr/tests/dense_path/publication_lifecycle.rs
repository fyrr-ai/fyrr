//! Publication lifecycle through separate CLI hosts and the owned RustFS.
use super::coordination::{host_command, publication, worker_sync};
use super::incremental::{fixture, input, publish};
use super::*;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};

fn doc(id: &str, revision: &str, text: &str) -> Value {
    let mut value = super::incremental::doc(id, revision, text);
    value["readers"] = json!(["grp:allow"]);
    value["revision_order"] = json!(revision.trim_start_matches("r").parse::<u64>().unwrap());
    value
}

// Compare logical rows across native versions independently of index layout.
async fn audit(config: &Path, before: u64, after: u64) -> Value {
    use futures::TryStreamExt;
    use std::collections::BTreeMap;
    let loaded = fyrr_core::config::load(config).unwrap();
    let mut versions = Vec::new();
    for version in [before, after] {
        let dataset = fyrr_reader::storage::open_version(
            &fyrr_reader::storage::dataset_uri(&loaded.config),
            loaded
                .config
                .storage
                .object_store_options(&fyrr_core::config::ProcessEnvironment)
                .unwrap(),
            version,
            None,
        )
        .await
        .unwrap();
        let mut stream = dataset.scan().try_into_stream().await.unwrap();
        let mut rows = BTreeMap::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            for i in 0..batch.num_rows() {
                let string = |name| {
                    fyrr_writer::apply::strings(&batch, name)
                        .unwrap()
                        .value(i)
                        .to_owned()
                };
                let key = (string("source_id"), string("root_id"), string("record_id"));
                let metadata: Value = serde_json::from_str(&string("metadata_json")).unwrap();
                let content = (
                    string("source_revision"),
                    string("searchable_text"),
                    metadata.clone(),
                );
                let mut readers = fyrr_writer::apply::tokens(&batch, i).unwrap();
                readers.sort_unstable();
                readers.dedup();
                assert!(
                    rows.insert(
                        key,
                        (content, (readers, metadata["_fyrr"]["permission"].clone()))
                    )
                    .is_none(),
                    "duplicate logical row"
                );
            }
        }
        versions.push(rows);
    }
    let before = &versions[0];
    let after = &versions[1];
    let missing = before
        .keys()
        .filter(|key| !after.contains_key(*key))
        .count();
    let added = after
        .keys()
        .filter(|key| !before.contains_key(*key))
        .count();
    let mut content = 0;
    let mut grants = 0;
    for (key, (old_content, old_grants)) in before {
        if let Some((new_content, new_grants)) = after.get(key) {
            content += usize::from(old_content != new_content);
            grants += usize::from(old_grants != new_grants);
        }
    }
    json!({"status":if missing + added + content + grants == 0 {"PASS"} else {"FAIL"},
        "missing_rows":missing,"content_revision_or_state_changes":content,"grant_changes":grants})
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn broad_audit_detects_content_grant_and_key_changes_on_storage() {
    let (worker, commit, root) = fixture();
    super::incremental::initial(&root);
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
    // Establish a baseline after the fixture's initially denied content is purged.
    success(&commit, &["writer", "maintain"]);
    let before = publication(&commit).await.1.lance_version;
    success(&commit, &["writer", "maintain"]);
    let compacted = publication(&commit).await.1.lance_version;
    assert_eq!(audit(&commit, before, compacted).await["status"], "PASS");

    input(
        &root,
        json!({"epoch":2,"content":[[super::incremental::doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let changed = publication(&commit).await.1.lance_version;
    let content = audit(&commit, compacted, changed).await;
    assert_eq!(content["status"], "FAIL");
    assert!(
        content["content_revision_or_state_changes"]
            .as_u64()
            .unwrap()
            > 0
    );

    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    let revoked = publication(&commit).await.1.lance_version;
    let grants = audit(&commit, changed, revoked).await;
    assert_eq!(grants["status"], "FAIL");
    assert!(grants["grant_changes"].as_u64().unwrap() > 0);

    input(
        &root,
        json!({"epoch":4,"content":[[{"id":"b","revision":"r2","delete":true}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    success(&commit, &["writer", "maintain"]);
    let removed = publication(&commit).await.1.lance_version;
    let keys = audit(&commit, revoked, removed).await;
    assert_eq!(keys["status"], "FAIL");
    assert!(keys["missing_rows"].as_u64().unwrap() > 0);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn committer_indexes_and_stale_prepared_acl_on_storage() {
    {
        let (config, commit, root) = fixture();
        worker_sync(
            &config,
            &["incremental", "--kind", "permissions", "--kind", "members"],
        );
        publish(&commit);
        input(
            &root,
            json!({"epoch":1,"content":[[doc("a","r1","fixture document 1"),doc("b","r1","fixture document 2")]]}),
        );
        worker_sync(&config, &["incremental", "--kind", "content"]);
        let result = host_command(&commit)
            .args(["writer", "commit", "--stage-timings"])
            .output()
            .unwrap();
        assert!(result.status.success(), "{}", report(&result));
        success(&commit, &["writer", "verify"]);
        let mut client = super::tools::Mcp::open(&config);
        let result = client.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
        assert_eq!(result["isError"], false);
        assert_eq!(
            result["structuredContent"]["documents"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        {
            input(
                &root,
                json!({"epoch":2,"content":[[doc("a","r2","fixture document 3")]]}),
            );
            worker_sync(&config, &["incremental", "--kind", "content"]);
            // Authorization publishes first, invalidating prepared base and ACLs.
            input(
                &root,
                json!({"epoch":3,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
            );
            worker_sync(&config, &["incremental", "--kind", "permissions"]);
            let result = host_command(&commit)
                .args(["writer", "commit", "--stage-timings"])
                .output()
                .unwrap();
            assert!(result.status.success(), "{}", report(&result));
            success(&commit, &["writer", "verify"]);
            let mut client = super::tools::Mcp::open(&config);
            let result = client.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
            assert!(
                result["structuredContent"]["documents"]
                    .as_array()
                    .is_none_or(|d| d.is_empty()),
                "{result}"
            );
        }
        let loaded = fyrr_core::config::load(&commit).unwrap();
        let (_, manifest) = publication(&commit).await;
        let ds = fyrr_reader::storage::open_version(
            &fyrr_reader::storage::dataset_uri(&loaded.config),
            loaded
                .config
                .storage
                .object_store_options(&fyrr_core::config::ProcessEnvironment)
                .unwrap(),
            manifest.lance_version,
            None,
        )
        .await
        .unwrap();
        let indexes = ds.load_indices().await.unwrap();
        for name in [
            "vector_idx",
            "readers_idx",
            "record_id_idx",
            "root_id_idx",
            "source_id_idx",
            "source_uri_idx",
            "source_type_idx",
            "project_idx",
            "author_idx",
            "updated_at_idx",
            "attributes_idx",
            "fts_idx",
        ] {
            assert!(indexes.iter().any(|i| i.name == name), "missing {name}");
            let mut coverage = std::collections::BTreeSet::new();
            for index in indexes.iter().filter(|i| i.name == name) {
                let column = if name == "fts_idx" {
                    "searchable_text"
                } else {
                    name.trim_end_matches("_idx")
                };
                ds.open_generic_index(
                    column,
                    &index.uuid,
                    &lance_index::metrics::NoOpMetricsCollector,
                )
                .await
                .unwrap();
                coverage.extend(index.fragment_bitmap.iter().flat_map(|b| b.iter()));
            }
            assert!(
                ds.manifest
                    .fragments
                    .iter()
                    .all(|f| coverage.contains(&(f.id as u32))),
                "incomplete {name}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "real CLI 60-second service timer and RustFS; scripts/ci"]
async fn serve_timer_disabled_committer_on_storage() {
    let (config, commit, root) = fixture();
    let initial = publication(&commit).await.0.content.unwrap();
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1","fixture document 1")]]}),
    );
    worker_sync(&config, &["incremental", "--kind", "content"]);
    // No source polling is needed for this timer proof; the backlog lives only on S3.
    let mut text = std::fs::read_to_string(&commit).unwrap();
    text.truncate(text.find("[[sources]]").unwrap());
    text.push_str("\n[writer.serve]\ncommitter=false\n");
    let service = root.join("committer-host/service.toml");
    std::fs::write(&service, &text).unwrap();
    let mut child = host_command(&service)
        .args(["writer", "serve"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert_eq!(
        publication(&commit).await.0.content.unwrap().publication_id,
        initial.publication_id
    );
    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(child.wait().unwrap().success());
    std::fs::write(&service, text.replace("committer=false", "committer=true")).unwrap();
    let began = std::time::Instant::now();
    let mut child = host_command(&service)
        .args(["writer", "serve"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut observed = false;
    for _ in 0..95 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if publication(&commit)
            .await
            .0
            .content
            .as_ref()
            .unwrap()
            .publication_id
            != initial.publication_id
        {
            observed = true;
            break;
        }
    }
    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(child.wait().unwrap().success());
    assert!(observed);
    assert!(began.elapsed().as_secs() >= 60);
    publish(&commit);
}

#[tokio::test]
#[ignore = "real CLI service worker isolation and RustFS; scripts/ci"]
async fn serve_isolates_failed_providers_while_publishing_on_storage() {
    use std::sync::{Arc, atomic::AtomicBool};
    let (worker, commit, root) = fixture();
    worker_sync(
        &worker,
        &["incremental", "--kind", "permissions", "--kind", "members"],
    );
    publish(&commit);
    let mut text = std::fs::read_to_string(&worker).unwrap();
    text.push_str("\n[writer.commit]\ncoalescing_timer='1s'\n");
    for (id, mode) in [("hanger", "hang"), ("crasher", "crash")] {
        let host = root.join(id);
        std::fs::create_dir(&host).unwrap();
        input(&host, json!({"mode":mode}));
        text.push_str(&format!("\n[[sources]]\nid='{id}'\ntype='incremental-test'\npermissions='fixed'\nreaders=['grp:allow']\n[sources.scope]\nfixture='{}'\n", host.join("input.json").display()));
    }
    let service = root.join("service.toml");
    std::fs::write(&service, text).unwrap();
    input(
        &root,
        json!({"epoch":1,"content":[[doc("healthy","r1","fixture document 1")]]}),
    );
    let mut command = super::coordination::worker_command(&service);
    command.args(["writer", "serve", "--jobs", "5", "--timeout", "4s"]);
    let mut process =
        super::coordination::CommitProcess::from_command(command, Arc::new(AtomicBool::new(true)));
    let mut observed = false;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let rows = super::incremental::rows(&commit).await;
        if super::incremental::roots(&rows).contains(&"healthy".to_owned()) {
            observed = true;
            break;
        }
    }
    assert!(observed, "a failed provider blocked healthy publication");
    assert!(root.join("hanger/entered").exists());
    Command::new("kill")
        .args(["-TERM", &process.child().id().to_string()])
        .status()
        .unwrap();
    let result = process.finish();
    assert!(result.status.success(), "{}", report(&result));
    success(&commit, &["writer", "verify"]);
}

#[tokio::test]
#[ignore = "real CLI continuously refilled backlog, SIGTERM and RustFS; scripts/ci"]
async fn delta_shutdown_finishes_only_current_group_on_storage() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    use std::time::{Duration, Instant};
    let (worker, commit, root) = fixture();
    let text = std::fs::read_to_string(&worker).unwrap();
    std::fs::write(&worker, format!("{text}\n[writer]\nenvelope_max_rows=1\n[writer.commit]\ngroup_max_rows=1\ncoalescing_timer='1s'\n")).unwrap();
    input(
        &root,
        json!({"epoch":1,"content":(0..10).map(|i| vec![doc(&format!("queued-{i}"), "r1", "fixture document 1")]).collect::<Vec<_>>()}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let before = publication(&commit).await.0.content.unwrap().sequence;
    let fired = Arc::new(AtomicBool::new(false));
    let resume = Arc::new(AtomicBool::new(false));
    let observed = fired.clone();
    let release = resume.clone();
    let hook: super::corrections::BoundaryHook = Arc::new(move |request, body, after| {
        let selected = !after
            && request.starts_with("PUT ")
            && request.contains("/head.json")
            && serde_json::from_slice::<Value>(body).is_ok_and(|v| {
                v["content"]["sequence"]
                    .as_u64()
                    .is_some_and(|s| s > before)
            })
            && !observed.swap(true, Ordering::SeqCst);
        if selected {
            while !release.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        false // Pause only; forward the current group publication normally.
    });
    let proxy_stop = Arc::new(AtomicBool::new(false));
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, thread) =
        super::corrections::proxy_at_boundary(endpoint.clone(), proxy_stop.clone(), hook);
    let mut _guard = thread;
    _guard.resume = resume.clone();
    let service = root.join("committer-host/shutdown.toml");
    let mut text = std::fs::read_to_string(&commit).unwrap();
    text.truncate(text.find("[[sources]]").unwrap());
    std::fs::write(&service, format!("{}\n[writer]\nenvelope_max_rows=1\n[writer.commit]\ngroup_max_rows=1\ncoalescing_timer='1s'\n", text.replace(&endpoint, &proxy))).unwrap();
    let mut process = super::coordination::CommitProcess::spawn_command(
        &service,
        &["writer", "serve"],
        resume.clone(),
    );
    let started = Instant::now();
    while !fired.load(Ordering::SeqCst) {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "first group never reached publication"
        );
        assert!(process.child().try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The source keeps appending envelopes from its own host while publication is blocked.
    struct Refiller {
        stop: Arc<AtomicBool>,
        task: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for Refiller {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Some(task) = self.task.take() {
                task.join().unwrap();
            }
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicU64::new(0));
    let stopped = stop.clone();
    let counted = count.clone();
    let worker_host = worker.clone();
    let worker_root = root.to_path_buf();
    let _refiller = Refiller {
        stop,
        task: Some(std::thread::spawn(move || {
            let mut n = 2;
            while !stopped.load(Ordering::SeqCst) {
                input(
                    &worker_root,
                    json!({"epoch":n,"content":[[doc(&format!("refilled-{n}"), "r1", "fixture document 1")]]}),
                );
                worker_sync(
                    &worker_host,
                    &["incremental", "--kind", "content", "--timeout", "30s"],
                );
                counted.fetch_add(1, Ordering::SeqCst);
                n += 1;
            }
        })),
    };
    let started = Instant::now();
    while count.load(Ordering::SeqCst) < 2 {
        assert!(
            started.elapsed() < Duration::from_secs(45),
            "backlog was not refilled"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Command::new("kill")
        .args(["-TERM", &process.child().id().to_string()])
        .status()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        process.child().try_wait().unwrap().is_none(),
        "current group was abandoned"
    );
    resume.store(true, Ordering::SeqCst);
    let released = Instant::now();
    loop {
        if process.child().try_wait().unwrap().is_some() {
            break;
        }
        assert!(
            released.elapsed() < Duration::from_secs(20),
            "shutdown drained the continuously refilled backlog"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(process.finish().status.success());
    let head = publication(&commit).await.0;
    assert_eq!(
        head.content.unwrap().sequence,
        before + 1,
        "shutdown may publish only the current group"
    );
    assert!(
        head.lease
            .is_none_or(|lease| lease.expires_at <= fyrr_core::clock::now()),
        "shutdown must release the namespace lease"
    );
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let takeover = fyrr_writer::lease::LeaseGuard::acquire(&loaded.config)
        .await
        .unwrap();
    takeover.release().await.unwrap();
    assert!(
        !fyrr_writer::commit::pending(&loaded.config, &publication(&commit).await.0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "real CLI service and local storage; scripts/ci"]
async fn serve_runs_a_source_again_when_a_rejection_is_published_on_storage() {
    use super::incremental::{envelopes, initial, publish_initial};
    use fyrr_core::envelope::EnvelopePayload;
    use object_store::ObjectStoreExt;
    use std::sync::{Arc, atomic::AtomicBool};
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    initial(&root);
    let mut next =
        serde_json::from_slice::<Value>(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    next["epoch"] = json!(2);
    next["permissions"][0][0]["revision"] = json!("p2");
    next["permissions"][0][0]["readers"] = json!([]);
    input(&root, next);
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    // The committer rejects an envelope that stayed unapplied for days. The
    // permission interval is five minutes and was restarted by the sync above:
    // only the published rejection can make the source read again within the test.
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, _) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    for (path, mut envelope) in envelopes(&worker).await {
        if matches!(&envelope.payload, EnvelopePayload::Permissions(p) if p.documents.iter().any(|d| d.permission_revision == "p2"))
        {
            envelope.created_at = fyrr_core::clock::now() - chrono::Duration::days(4);
            store
                .put(&path, fyrr_core::write_contract(&envelope).into())
                .await
                .unwrap();
        }
    }
    let mut text = std::fs::read_to_string(&worker).unwrap();
    text.push_str("\n[writer.commit]\ncoalescing_timer='1s'\n");
    let service = root.join("service.toml");
    std::fs::write(&service, text).unwrap();
    let mut command = super::coordination::worker_command(&service);
    command.args(["writer", "serve"]);
    let mut process =
        super::coordination::CommitProcess::from_command(command, Arc::new(AtomicBool::new(true)));
    // A replacement read ends with a complete-read envelope of a later generation.
    let mut observed = false;
    for _ in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        observed = envelopes(&worker).await.iter().any(|(_, e)| {
            e.source_id == "incremental"
                && e.kind == fyrr_core::connector::SyncKind::Permissions
                && e.rejection_generation > 0
                && fyrr_writer::apply::complete_read(e)
        });
        if observed {
            break;
        }
    }
    Command::new("kill")
        .args(["-TERM", &process.child().id().to_string()])
        .status()
        .unwrap();
    let result = process.finish();
    assert!(observed, "the rejection did not make the source read again");
    assert!(result.status.success(), "{}", report(&result));
}
