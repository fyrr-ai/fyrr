//! Maintenance and restore lifecycle proofs through independent CLI hosts and owned RustFS.
use super::coordination::{command, host_command, publication, worker_sync};
use super::corrections::proxied;
use super::incremental::{
    doc, envelopes, fixture, fixture_on, initial, input, publish, publish_initial, rows,
    source_rows,
};
use super::*;
use object_store::ObjectStoreExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

fn reader_config(worker: &Path, root: &Path, mode: &str) -> PathBuf {
    let host = root.join(format!("reader-{mode}"));
    std::fs::create_dir(&host).unwrap();
    let config = host.join("fyrr.toml");
    // `interval` is the shipped behavior; the others are test-only switches.
    let hook = if mode == "interval" {
        String::new()
    } else {
        format!("# fyrr-test-version-check = {mode}\n")
    };
    let text = std::fs::read_to_string(worker)
        .unwrap()
        .replace(
            "[reader]",
            &format!("{hook}[reader]\nversion_check_interval='100ms'\n"),
        )
        .replace(
            &root.join("mirror").display().to_string(),
            &host.join("mirror").display().to_string(),
        )
        .replace(
            &root.join("sources").display().to_string(),
            &host.join("no-worker-files").display().to_string(),
        )
        .replace(
            &root.join("input.json").display().to_string(),
            &host.join("no-worker-input").display().to_string(),
        );
    std::fs::write(&config, text).unwrap();
    config
}
fn replacement(id: &str, revision: &str, prior: &str, text: &str) -> Value {
    let mut value = doc(id, revision, text);
    value["expected_prior_revision"] = json!(prior);
    value
}
fn readable(result: &Value) -> bool {
    result["structuredContent"]["documents"]
        .as_array()
        .is_some_and(|d| !d.is_empty())
}

fn fetch(client: &mut tools::Mcp, root: &str) -> Value {
    client.call("fetch", json!({"source_id":"incremental","root_id":root}))
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_pending_revoke_purge_and_verify_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    std::fs::remove_file(root.join("sources/0.txt")).unwrap();
    worker_sync(&worker, &["allowed"]);
    publish(&commit);
    let (_, before) = publication(&commit).await;
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let ds = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        loaded
            .config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        before.lance_version,
        None,
    )
    .await
    .unwrap();
    assert!(
        ds.manifest()
            .fragments
            .iter()
            .any(|f| f.deletion_file.is_some()),
        "fixture needs a partial-fragment delete; metadata={:?}",
        ds.manifest()
            .fragments
            .iter()
            .map(|f| (
                f.id,
                f.physical_rows,
                f.deletion_file.as_ref().map(|d| d.num_deleted_rows)
            ))
            .collect::<Vec<_>>()
    );
    let old_files: Vec<_> = ds
        .manifest()
        .fragments
        .iter()
        .flat_map(|f| f.files.iter().map(|f| format!("data/{}", f.path)))
        .collect();
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]],"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    // Queue another revoke while a native maintenance transaction is already in flight.
    let versions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fired = Arc::new(AtomicBool::new(false));
    let observed = fired.clone();
    let writer_host = worker.clone();
    let input_root = root.to_path_buf();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if after
            && request.starts_with("PUT ")
            && request.contains("/_versions/")
            && versions.fetch_add(1, Ordering::SeqCst) >= 2
            && !observed.swap(true, Ordering::SeqCst)
        {
            input(
                &input_root,
                json!({"epoch":3,"permissions":[[{"id":"b","revision":"p3","readers":[]}]]}),
            );
            worker_sync(&writer_host, &["incremental", "--kind", "permissions"]);
        }
        false
    });
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, thread) = corrections::proxy_at_boundary(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        hook,
    );
    let mut guard = thread;
    guard.resume = Arc::new(AtomicBool::new(true));
    let fault = proxied(&commit, &endpoint);
    let maintained = command(&fault, &["writer", "maintain"]);
    assert!(
        fired.load(Ordering::SeqCst),
        "mid-step revoke was actually queued"
    );
    drop(guard);
    assert!(
        maintained["compaction_steps"].as_u64().unwrap() > 0,
        "{maintained}"
    );
    let (head, after) = publication(&commit).await;
    assert!(after.sequence > before.sequence);
    let pending = fyrr_writer::commit::pending(&loaded.config, &head)
        .await
        .unwrap();
    assert!(
        pending
            .iter()
            .any(|e| e.kind == fyrr_core::connector::SyncKind::Content),
        "maintenance must not drain content backlog"
    );
    assert!(
        !pending
            .iter()
            .any(|e| e.kind == fyrr_core::connector::SyncKind::Permissions)
    );
    let ds = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        loaded
            .config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        after.lance_version,
        None,
    )
    .await
    .unwrap();
    assert!(
        ds.manifest()
            .fragments
            .iter()
            .all(|f| f.deletion_file.is_none())
    );
    let rows = ds
        .scan()
        .filter("source_id = 'incremental' AND root_id = 'a'")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        rows.num_rows(),
        0,
        "revoked text is physically absent from the current version"
    );
    assert!(
        old_files
            .iter()
            .any(|p| !after.files.iter().any(|f| f.path == *p)),
        "compacted files are absent from the new inventory"
    );
    command(&commit, &["writer", "verify"]);
    let reader = reader_config(&worker, &root, "interval");
    let mut mcp = tools::Mcp::open(&reader);
    assert!(!readable(&fetch(&mut mcp, "a")));
    assert!(!readable(&fetch(&mut mcp, "b")));
    let healthy = mcp.call(
        "search",
        json!({"order":"updated_at","sources":["allowed"],"limit":10}),
    );
    assert_eq!(healthy["isError"], false, "{healthy}");
    assert!(
        !healthy["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    publish(&commit);
    assert_eq!(
        incremental::rows(&worker).await.num_rows(),
        0,
        "pending content doesn't refill purged revoked text"
    );
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn maintenance_restore_rollback_current_rights_deletes_and_cursor_recovery_on_local_storage()
{
    maintenance_restore_rollback_current_rights_deletes_and_cursor_recovery(Backend::Local).await;
}

async fn maintenance_restore_rollback_current_rights_deletes_and_cursor_recovery(backend: Backend) {
    let (worker, commit, root) = fixture_on(backend);
    publish_initial(&worker, &commit, &root);
    let (_, target) = publication(&commit).await;
    input(
        &root,
        json!({"epoch":2,"content":[[{"id":"b","delete":true,"revision":"r2","expected_prior_revision":"r1"}]],"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    publish(&commit);
    let (_, current) = publication(&commit).await;
    let restored = command(&commit, &["writer", "restore", &target.publication_id]);
    assert!(restored["lance_version"].as_u64().unwrap() > current.lance_version);
    let (_, after) = publication(&commit).await;
    assert!(!after.cursor_rewinds.is_empty());
    let config = reader_config(&worker, &root, "every");
    let mut mcp = tools::Mcp::open_logged(&config, &root.join("restore-reader.stderr"));
    assert!(
        !readable(&fetch(&mut mcp, "a")),
        "current revoke survives older content"
    );
    assert!(
        !readable(&fetch(&mut mcp, "b")),
        "current delete survives older content"
    );
    assert!(
        mcp.call("search", json!({"query":"fixture query","limit":1}))["structuredContent"]["hits"]
            .as_array()
            .is_some_and(|h| !h.is_empty())
    );
    command(&commit, &["writer", "verify"]);
    command(&commit, &["writer", "rollback"]);
    let (_, rollback) = publication(&commit).await;
    assert!(rollback.lance_version > after.lance_version);
    input(
        &root,
        json!({"epoch":3,"capture_cursors":true,"content":[[doc("c","r1","fixture document 3")]]}),
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
    let captured: Value =
        serde_json::from_slice(&std::fs::read(root.join("content-cursor.json")).unwrap()).unwrap();
    assert!(
        captured.is_null(),
        "restore/rollback force full content rescan"
    );
    publish(&commit);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, path) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let cursor_path = path.join("cursors").join("incremental-content.json");
    let cursor: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    assert_eq!(cursor.restore_sequence, rollback.sequence);
    store.delete(&cursor_path).await.unwrap();
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let captured: Value =
        serde_json::from_slice(&std::fs::read(root.join("content-cursor.json")).unwrap()).unwrap();
    assert_eq!(
        captured,
        json!("3-0"),
        "reset floor recovers from envelopes after cursor-cache loss"
    );
    let healthy = fetch(&mut mcp, "c");
    assert!(readable(&healthy), "{healthy}");
    assert!(!readable(&fetch(&mut mcp, "a")));
    assert!(!readable(&fetch(&mut mcp, "b")));
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn maintenance_restore_crash_boundaries_on_storage() {
    // Distinct storage boundaries, shared by the two native maintenance operations.
    // This is deliberately not a cross product of actions and failure windows.
    let cases = [
        (false, "/_versions/", false),
        (true, "/_versions/", true),
        (false, "/publications/", false),
        (true, "/head.json", false),
        (false, "/head.json", true),
    ];
    for (restore, boundary, after) in cases {
        let (worker, commit, root) = fixture();
        publish_initial(&worker, &commit, &root);
        let target = publication(&commit).await.1;
        input(
            &root,
            json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]],"permissions":[[{"id":"b","revision":"p2","readers":[]}]]}),
        );
        worker_sync(
            &worker,
            &["incremental", "--kind", "content", "--kind", "permissions"],
        );
        publish(&commit);
        let before = publication(&commit).await.1;
        let fired = Arc::new(AtomicBool::new(false));
        let resume = Arc::new(AtomicBool::new(false));
        let observed = fired.clone();
        let release = resume.clone();
        let minimum = before.sequence;
        let hook: corrections::BoundaryHook = Arc::new(move |request, body, response| {
            let content_change = boundary != "/head.json"
                || serde_json::from_slice::<Value>(body).is_ok_and(|v| {
                    v["content"]["sequence"]
                        .as_u64()
                        .is_some_and(|s| s > minimum)
                });
            let selected = request.starts_with("PUT ")
                && request.contains(boundary)
                && content_change
                && response == after
                && !observed.swap(true, Ordering::SeqCst);
            if selected {
                while !release.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            selected
        });
        let stop = Arc::new(AtomicBool::new(false));
        let (endpoint, thread) = corrections::proxy_at_boundary(
            std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
            stop.clone(),
            hook,
        );
        let mut guard = thread;
        guard.resume = resume.clone();
        let config = proxied(&commit, &endpoint);
        let args = if restore {
            vec!["writer", "restore", &target.publication_id]
        } else {
            vec!["writer", "maintain"]
        };
        let mut process =
            coordination::CommitProcess::spawn_command(&config, &args, resume.clone());
        let started = Instant::now();
        while !fired.load(Ordering::SeqCst) {
            assert!(
                started.elapsed() < Duration::from_secs(90),
                "boundary {restore}/{boundary}/{after} was not reached"
            );
            if process.child().try_wait().unwrap().is_some() {
                let output = process.finish();
                panic!(
                    "operation {restore}/{boundary}/{after} exited before boundary: status={}, stdout={}, stderr={}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                );
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        process.child().kill().unwrap();
        assert!(!process.finish().status.success());
        resume.store(true, Ordering::SeqCst);
        drop(guard);
        let (head, failed) = publication(&commit).await;
        assert_eq!(
            failed.sequence > before.sequence,
            boundary == "/head.json" && after
        );
        let remaining = (head.lease.unwrap().expires_at - fyrr_core::clock::now())
            .to_std()
            .unwrap_or_default();
        // The crashed holder is gone; advance its persisted lease deadline.
        fyrr_core::clock::advance(remaining + Duration::from_millis(50)).unwrap();
        // A different host recovers any orphan through Restore, then does the requested work.
        command(&commit, &args);
        assert_eq!(command(&commit, &["writer", "verify"])["verified"], true);
        let config = reader_config(&worker, &root, "interval");
        let mut mcp = tools::Mcp::open(&config);
        assert!(readable(&fetch(&mut mcp, "a")));
        assert!(
            !readable(&fetch(&mut mcp, "b")),
            "current revoke survives {restore}/{boundary}/{after}"
        );
        let fixed = mcp.call(
            "search",
            json!({"order":"updated_at","sources":["allowed"],"limit":10}),
        );
        assert!(
            !fixed["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn restore_and_rollback_preserve_current_section_revocation() {
    for rollback in [false, true] {
        let (worker, commit, root) = fixture();
        initial(&root);
        let mut value: Value =
            serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
        value["content"][0][0]["texts"] = json!(["fixture document 0", "fixture document 2"]);
        value["content"][0][0]["section_readers"] = json!([["grp:group"], ["grp:group"]]);
        input(&root, value.clone());
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
        let old = publication(&commit).await.1;
        value["epoch"] = json!(2);
        value["content"][0][0]["revision"] = json!("r2");
        value["content"][0][0]["expected_prior_revision"] = json!("r1");
        value["content"][0][0]["section_readers"] = json!([[], ["grp:group"]]);
        input(&root, value.clone());
        worker_sync(&worker, &["incremental", "--kind", "content"]);
        publish(&commit);
        let mut before = tools::Mcp::open(&worker);
        let denied = fetch(&mut before, "a");
        assert!(
            !denied.to_string().contains("fixture document 0"),
            "precondition revoke did not apply"
        );
        drop(before);
        if rollback {
            command(&commit, &["writer", "rollback"]);
        } else {
            command(&commit, &["writer", "restore", &old.publication_id]);
        }
        command(&commit, &["writer", "verify"]);
        let mut after = tools::Mcp::open(&worker);
        let result = fetch(&mut after, "a");
        assert!(
            !result.to_string().contains("fixture document 0"),
            "RESTORE REGRANTED REVOKED SECTION: {}",
            result
        );
        drop(after);

        // A fresh complete sync can legitimately restate section grants.
        value["epoch"] = json!(3);
        value["content"][0][0]["revision"] = json!("r3");
        value["content"][0][0]["expected_prior_revision"] = json!("r1");
        value["content"][0][0]["section_readers"] = json!([["grp:group"], ["grp:group"]]);
        for permission in value["permissions"][0].as_array_mut().unwrap() {
            permission["revision"] = json!("p3");
        }
        input(&root, value);
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
        let loaded = fyrr_core::config::load(&worker).unwrap();
        let restated = fyrr_writer::apply::permission_readers(
            &loaded.config,
            "incremental",
            &["grp:group".into()],
            &[],
        )
        .unwrap();
        let batch = rows(&worker).await;
        let roots = fyrr_writer::apply::strings(&batch, "root_id").unwrap();
        let metadata = fyrr_writer::apply::strings(&batch, "metadata_json").unwrap();
        let mut changed = 0;
        for row in 0..batch.num_rows() {
            if roots.value(row) != "a" {
                continue;
            }
            changed += 1;
            let metadata: Value = serde_json::from_str(metadata.value(row)).unwrap();
            assert_eq!(metadata["_fyrr"]["revision"], "r3");
            let own: Vec<String> =
                serde_json::from_value(metadata["_fyrr_own_readers"].clone()).unwrap();
            assert_eq!(own, restated);
            let permission = fyrr_writer::apply::state(&batch, row)
                .unwrap()
                .permission
                .unwrap();
            let intersection: Vec<_> = own
                .into_iter()
                .filter(|token| permission.readers.contains(token))
                .collect();
            assert_eq!(
                fyrr_writer::apply::tokens(&batch, row).unwrap(),
                intersection
            );
        }
        assert_eq!(changed, 2);
        let mut restated_reader = tools::Mcp::open(&worker);
        assert!(
            fetch(&mut restated_reader, "a")
                .to_string()
                .contains("fixture document 0")
        );
    }
}

async fn retirement_fixture() -> (PathBuf, PathBuf, FixtureRoot) {
    retirement_fixture_on(Backend::selected()).await
}

/// The incremental fixture with its two documents published and readable.
async fn retirement_fixture_on(backend: Backend) -> (PathBuf, PathBuf, FixtureRoot) {
    let (worker, commit, root) = fixture_on(backend);
    publish_initial(&worker, &commit, &root);
    assert_eq!(rows(&worker).await.num_rows(), 2);
    (worker, commit, root)
}

/// Every row the namespace holds of the source: content and state markers.
async fn held_rows(config: &Path) -> usize {
    source_rows(config, "incremental").await.num_rows()
        + source_rows(config, &fyrr_writer::apply::state_source("incremental"))
            .await
            .num_rows()
}

/// Retiring a source fences its further commits.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn maintenance_retired_source_commits_are_fenced_on_storage() {
    let (worker, commit, root) = retirement_fixture().await;
    // An envelope that is pending when the source is retired.
    input(
        &root,
        json!({"epoch":2,"content":[[doc("c","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let pending = envelopes(&worker)
        .await
        .into_iter()
        .find_map(|(_, envelope)| match &envelope.payload {
            fyrr_core::envelope::EnvelopePayload::Content(payload)
                if envelope.source_id == "incremental"
                    && payload.documents.iter().any(|d| d.root_id == "c") =>
            {
                Some(envelope.envelope_id.clone())
            }
            _ => None,
        })
        .unwrap();

    let warned = |status: &Value| {
        status["warnings"]
            .as_array()
            .unwrap()
            .contains(&json!("retired_source_configured"))
    };
    assert!(!warned(&command(&commit, &["writer", "status"])));
    let retired = command(&commit, &["writer", "source", "retire", "incremental"]);
    assert_eq!(
        retired,
        json!({"source_id":"incremental","retired":true,"changed":true})
    );
    // The configuration still names the retired source: status says so.
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["retired_sources_configured"], json!(["incremental"]));
    assert!(warned(&status));
    let again = command(&commit, &["writer", "source", "retire", "incremental"]);
    assert_eq!(again["changed"], false, "retiring twice changes nothing");

    publish(&commit);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    assert!(
        fyrr_writer::apply::rejected(&loaded.config, &pending)
            .await
            .unwrap(),
        "the pending envelope is rejected"
    );
    assert_eq!(
        rows(&worker).await.num_rows(),
        2,
        "nothing of it was applied"
    );
    let refused = super::coordination::worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "content"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stdout).contains("source_retired"),
        "{}",
        String::from_utf8_lossy(&refused.stdout)
    );
    // The other sources still sync and commit.
    let before = source_rows(&worker, "allowed").await.num_rows();
    std::fs::write(root.join("sources").join("new.txt"), "fixture document 9").unwrap();
    worker_sync(&worker, &["allowed"]);
    publish(&commit);
    assert_eq!(source_rows(&worker, "allowed").await.num_rows(), before + 1);
    // A mistyped ID takes no retirement entry.
    let unknown = host_command(&commit)
        .args(["writer", "source", "retire", "no-such-source"])
        .output()
        .unwrap();
    assert!(!unknown.status.success());
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("source_unknown"));
}

/// A retired source remains denied after restore.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn maintenance_retired_source_survives_restore_and_rollback_on_storage() {
    let (worker, commit, root) = retirement_fixture().await;
    let first = publication(&worker).await.1.publication_id;
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    command(&commit, &["writer", "source", "retire", "incremental"]);
    assert!(held_rows(&worker).await > 0, "retiring deletes nothing");

    command(&commit, &["writer", "rollback"]);
    assert_eq!(held_rows(&worker).await, 0, "a rollback brings no row back");
    assert!(!readable(&fetch(&mut tools::Mcp::open(&worker), "a")));
    command(&commit, &["writer", "restore", &first]);
    assert_eq!(held_rows(&worker).await, 0, "nor does a restore");
    assert!(!readable(&fetch(&mut tools::Mcp::open(&worker), "a")));
    assert!(readable(&client_fetch_allowed(&worker)));
}

fn client_fetch_allowed(config: &Path) -> Value {
    tools::Mcp::open(config).call("fetch", json!({"source_id":"allowed","root_id":"0.txt"}))
}

/// The sources and directory whose snapshots the published identity is built from.
async fn identity_inputs(config: &Path) -> std::collections::BTreeSet<String> {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let head = publication(config).await.0;
    let identity: fyrr_core::identity::IdentityArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{namespace}/{}", head.identity.object)),
    )
    .await
    .unwrap();
    identity.inputs.into_iter().map(|input| input.id).collect()
}

fn envelopes_of_incremental(
    all: Vec<(
        object_store::path::Path,
        fyrr_core::envelope::PreparedEnvelope,
    )>,
) -> usize {
    all.into_iter()
        .filter(|(_, envelope)| envelope.source_id == "incremental")
        .count()
}

/// Recovery starts from the serving head after an interrupted native delete.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn maintenance_purge_killed_before_its_head_publication_converges_on_storage() {
    let (worker, commit, _root) = retirement_fixture().await;
    let before_version = publication(&worker).await.1.lance_version;
    let before_rows = held_rows(&worker).await;
    let stop = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicBool::new(false));
    let resume = Arc::new(AtomicBool::new(false));
    let (hook_fired, hook_resume) = (fired.clone(), resume.clone());
    // The native delete has committed, but its head CAS has not reached S3.
    let hook: super::corrections::BoundaryHook = Arc::new(move |request, body, response| {
        let publication = !response
            && request.starts_with("PUT ")
            && request.contains("/head.json ")
            && serde_json::from_slice::<Value>(body).is_ok_and(|head| {
                head["content"]["lance_version"]
                    .as_u64()
                    .is_some_and(|v| v > before_version)
            });
        let selected = publication && !hook_fired.swap(true, Ordering::SeqCst);
        if selected {
            while !hook_resume.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        selected
    });
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, mut guard) =
        super::corrections::proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
    guard.resume = resume.clone();
    let faulty = commit.parent().unwrap().join("fault.toml");
    // The crash victim uses the default TTL so it reaches the intended boundary.
    std::fs::write(
        &faulty,
        std::fs::read_to_string(&commit)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let mut purge = super::coordination::CommitProcess::spawn_command(
        &faulty,
        &["writer", "source", "purge", "incremental", "--confirm"],
        resume.clone(),
    );
    let began = Instant::now();
    while !fired.load(Ordering::SeqCst) {
        assert!(
            began.elapsed() < Duration::from_secs(120),
            "native publication not reached"
        );
        if purge.child().try_wait().unwrap().is_some() {
            let ended = purge.finish();
            panic!(
                "the purge ended before publishing its native delete: {} {}",
                String::from_utf8_lossy(&ended.stdout),
                String::from_utf8_lossy(&ended.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    purge.child().kill().unwrap();
    assert!(!purge.finish().status.success());
    resume.store(true, Ordering::SeqCst);
    stop.store(true, Ordering::SeqCst);
    drop(guard);

    assert_eq!(
        held_rows(&worker).await,
        before_rows,
        "the serving head still pins the rows before the unpublished native delete"
    );
    assert!(
        envelopes_of_incremental(envelopes(&worker).await) > 0,
        "the cleanup had not happened"
    );
    assert!(!readable(&fetch(&mut tools::Mcp::open(&worker), "a")));
    // Wait for the recorded lease, including any renewal before the crash.
    let head = publication(&commit).await.0;
    let remaining = (head.lease.unwrap().expires_at - fyrr_core::clock::now())
        .to_std()
        .unwrap_or_default();
    // The killed purge holder must expire before recovery takes ownership.
    fyrr_core::clock::advance(remaining + Duration::from_millis(50)).unwrap();
    let again = command(
        &commit,
        &["writer", "source", "purge", "incremental", "--confirm"],
    );
    assert_eq!(again["purged"], true);
    assert_eq!(again["rows"], before_rows);
    assert_eq!(held_rows(&worker).await, 0);
    assert_eq!(envelopes_of_incremental(envelopes(&worker).await), 0);
    assert!(!identity_inputs(&worker).await.contains("incremental"));
    command(&commit, &["writer", "verify"]);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_restore_discovers_authorization_after_failed_cursor_on_storage() {
    discovers_authorization_after_failed_cursor("restore").await;
}

async fn discovers_authorization_after_failed_cursor(operation: &str) {
    for kind in ["permissions", "members"] {
        let (worker, commit, root) = fixture();
        publish_initial(&worker, &commit, &root);
        let target = publication(&commit).await.1;
        let final_cursor = if kind == "permissions" { "2-0" } else { "2-1" };
        input(
            &root,
            if kind == "permissions" {
                json!({"epoch":2,"permissions":[[{"id":"a","revision":"failed-cursor-revoke","readers":[]}]]})
            } else {
                json!({"epoch":2,"members":[
                    {"last":false,"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":false}],"groups":[]},
                    {"last":true,"users":[],"groups":[{"id":"child","members":[]},{"id":"group","members":["grp:child"]}]}
                ]})
            },
        );
        let loaded = fyrr_core::config::load(&commit).unwrap();
        let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        let cursor_path =
            object_store::path::Path::from(format!("{ns}/cursors/incremental-{kind}.json"));
        let before: fyrr_core::connector::CursorState =
            fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
                .await
                .unwrap();
        let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
        let failed_cursor = Arc::new(AtomicBool::new(false));
        let observed = failed_cursor.clone();
        let hook: super::corrections::BoundaryHook = Arc::new(move |request, body, after| {
            let reject = !after
                && request.starts_with("PUT ")
                && request.contains(&format!("/cursors/incremental-{kind}.json"))
                && serde_json::from_slice::<Value>(body).is_ok_and(|cursor| {
                    cursor["sequence"]
                        .as_u64()
                        .is_some_and(|sequence| sequence > before.sequence)
                        && cursor["cursor"].as_str() == Some(final_cursor)
                });
            if reject {
                observed.store(true, Ordering::SeqCst);
            }
            reject
        });
        let (proxy, guard) = super::corrections::proxy_at_boundary(
            endpoint.clone(),
            Arc::new(AtomicBool::new(false)),
            hook,
        );
        let proxied = root.join("failed-cursor.toml");
        std::fs::write(
            &proxied,
            std::fs::read_to_string(&worker)
                .unwrap()
                .replace(&endpoint, &proxy),
        )
        .unwrap();
        let output = super::coordination::worker_command(&proxied)
            .args(["writer", "sync", "incremental", "--kind", kind])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            failed_cursor.load(Ordering::SeqCst),
            "only the final cursor advance is faulted, after durable authorization"
        );
        drop(guard);
        let head = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        assert!(
            fyrr_writer::commit::pending(&loaded.config, &head)
                .await
                .unwrap()
                .iter()
                .any(|e| e.kind.is_authorization())
        );
        let after: fyrr_core::connector::CursorState =
            fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
                .await
                .unwrap();
        assert_ne!(after.cursor.as_deref(), Some(final_cursor));
        if operation == "restore" {
            command(&commit, &["writer", "restore", &target.publication_id]);
        } else {
            command(&commit, &["writer", "maintain"]);
        }
        let config = reader_config(&worker, &root, "every");
        let mut reader = tools::Mcp::open(&config);
        assert!(
            !readable(&fetch(&mut reader, "a")),
            "{kind}: fresh discovery must apply the durable revoke despite failed cursor advancement"
        );
        assert_eq!(readable(&fetch(&mut reader, "b")), kind == "permissions");
    }
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn maintenance_restore_changed_envelope_identity_fails_closed_on_storage() {
    use std::sync::atomic::AtomicUsize;
    let (_, commit, root) = fixture();
    let target = publication(&commit).await.1;
    let lists = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(AtomicBool::new(false));
    let altered = changed.clone();
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    // Cleanup lists twice, then history records its first snapshot.
    // Change the next history listing, after that identity is cached.
    let transform: super::corrections::ResponseTransform = Arc::new(move |request, response| {
        if request.contains("list-type=2")
            && request.contains("envelopes")
            && lists.fetch_add(1, Ordering::SeqCst) == 3
        {
            super::corrections::rewrite_ok_body(response, |body| {
                if let Ok(mut objects) = serde_json::from_slice::<
                    Vec<fyrr_reader::storage::test_store::boundary::Metadata>,
                >(body)
                {
                    objects.first_mut().unwrap().e_tag =
                        Some(r#""changed-immutable-object""#.into());
                    *body = serde_json::to_vec(&objects).unwrap();
                } else {
                    let mut text = String::from_utf8(body.clone()).unwrap();
                    let start = text.find("<ETag>").unwrap() + 6;
                    let end = start + text[start..].find("</ETag>").unwrap();
                    text.replace_range(start..end, "&quot;changed-immutable-object&quot;");
                    *body = text.into_bytes();
                }
                altered.store(true, Ordering::SeqCst);
                true
            });
        }
    });
    let (proxy, guard) = super::corrections::proxy_transform(
        endpoint.clone(),
        Arc::new(AtomicBool::new(false)),
        transform,
        None,
    );
    let metered = root.join("changed-identity.toml");
    std::fs::write(
        &metered,
        std::fs::read_to_string(&commit)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let output = host_command(&metered)
        .args(["writer", "restore", &target.publication_id])
        .output()
        .unwrap();
    drop(guard);
    assert!(
        changed.load(Ordering::SeqCst),
        "second history LIST must expose the changed immutable identity"
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("restore_history_identity_changed"));
    assert_eq!(
        publication(&commit).await.1.publication_id,
        target.publication_id
    );
}

#[tokio::test]
#[ignore = "real S3 conditional restore history read; scripts/ci"]
async fn maintenance_restore_refuses_envelope_replaced_after_listing_on_real_s3() {
    let (worker, commit, root) = fixture();
    let target = publication(&commit).await.1;
    // A valid, unchanged envelope must permit the same restore operation.
    command(&commit, &["writer", "restore", &target.publication_id]);
    let before = publication(&commit).await.0;
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, _) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let (path, mut envelope) = envelopes(&worker).await.into_iter().next().unwrap();
    let original = store.head(&path).await.unwrap().e_tag;
    let get = format!(
        "GET /{}/{} HTTP/1.1\r\n",
        std::env::var("FYRR_TEST_S3_BUCKET").unwrap(),
        path.as_ref().replace('(', "%28").replace(')', "%29")
    );
    let gate_get = get.clone();
    // Cleanup reads this envelope unconditionally. Select history's conditional
    // read itself, regardless of how many cleanup enumerations precede it.
    let selected = Arc::new(AtomicBool::new(false));
    let gate_selected = selected.clone();
    let (gate, hook) = super::corrections::RequestGate::new(move |request, headers, after| {
        let matches = !after
            && request == gate_get
            && String::from_utf8_lossy(headers)
                .lines()
                .any(|line| line.to_ascii_lowercase().starts_with("if-match:"));
        if matches {
            gate_selected.store(true, Ordering::SeqCst);
        }
        matches
    });
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let statuses = observed.clone();
    let transform = Arc::new(move |request: &str, response: &mut Vec<u8>| {
        if request == get && selected.load(Ordering::SeqCst) {
            statuses.lock().unwrap().push(
                String::from_utf8_lossy(response)
                    .lines()
                    .next()
                    .unwrap()
                    .to_owned(),
            );
        }
    });
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, _proxy_guard) = super::corrections::proxy_transform(
        endpoint.clone(),
        Arc::new(AtomicBool::new(false)),
        transform,
        Some(hook),
    );
    let gate = gate;
    let proxied = root.join("conditional-restore.toml");
    std::fs::write(
        &proxied,
        std::fs::read_to_string(&commit)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let process = super::coordination::CommitProcess::spawn_command(
        &proxied,
        &["writer", "restore", &target.publication_id],
        Arc::new(AtomicBool::new(false)),
    );
    gate.entered();
    envelope.created_at -= chrono::Duration::seconds(1);
    let replacement = write_contract(&envelope);
    parse_contract::<fyrr_core::envelope::PreparedEnvelope>(&replacement).unwrap();
    store.put(&path, replacement.into()).await.unwrap();
    assert_ne!(store.head(&path).await.unwrap().e_tag, original);
    gate.release();
    let output = process.finish_bounded();
    assert_eq!(
        *observed.lock().unwrap(),
        ["HTTP/1.1 412 Precondition Failed"]
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("storage_unavailable"), "{stderr}");
    assert_eq!(publication(&commit).await.0.content, before.content);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_reader_switch_keeps_inflight_pin_and_evicts_old_mirror_on_real_s3() {
    maintenance_reader_switch_keeps_inflight_pin_and_evicts_old_mirror(Backend::S3).await;
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn maintenance_reader_switch_keeps_inflight_pin_and_evicts_old_mirror_on_local_storage() {
    maintenance_reader_switch_keeps_inflight_pin_and_evicts_old_mirror(Backend::Local).await;
}

async fn maintenance_reader_switch_keeps_inflight_pin_and_evicts_old_mirror(backend: Backend) {
    let (worker, commit, root) = fixture_on(backend);
    publish_initial(&worker, &commit, &root);
    let (old_head, old) = publication(&commit).await;
    let config = reader_config(&worker, &root, "held");
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2"),replacement("b","r2","r1","fixture document 3")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let (latest_head, latest) = publication(&commit).await;
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let mut obsolete_hashes = std::collections::BTreeSet::new();
    for file in &old.files {
        if file.path.starts_with("data/") && !latest.files.iter().any(|f| f.path == file.path) {
            let bytes = store
                .get(&object_store::path::Path::from(format!(
                    "{ns}/dataset.lance/{}",
                    file.path
                )))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            obsolete_hashes.insert(digest(&bytes));
        }
    }
    let publications = root.join("mirror-publications.json");
    // The child uses the dispatcher completion hook to hold exactly the old
    // foreground pin; background S3 checks cannot steal that boundary.
    std::fs::write(
        &publications,
        serde_json::to_vec(&json!({
            "old": old_head, "latest": latest_head, "obsolete_hashes": obsolete_hashes
        }))
        .unwrap(),
    )
    .unwrap();
    let output = Command::new(
        std::env::var_os("FYRR_ROUTING_TEST_BINARY")
            .expect("reader libtest built for the real-S3 mirror proof"),
    )
    .arg("serve::tests::generations::generation_inflight_pin_and_mirror_eviction_on_real_s3")
    .args(["--exact", "--ignored", "--nocapture"])
    .env("FYRR_TEST_MIRROR_CONFIG", &config)
    .env("FYRR_TEST_MIRROR_PUBLICATIONS", &publications)
    .output()
    .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    std::fs::write(root.join("mirror-proof.log"), format!("{stdout}\n{stderr}")).unwrap();
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("1 passed; 0 failed"));
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
#[allow(clippy::print_stderr)]
async fn maintenance_restore_batched_authorization_boundaries_on_storage() {
    let (worker, commit, root) = fixture();
    initial(&root);
    let mut seed: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    let members_path = root.join("restore-members.json");
    std::fs::write(&members_path, serde_json::to_vec(&seed["members"]).unwrap()).unwrap();
    let documents: Vec<_> = (0..300)
        .map(|n| {
            let mut value = doc(
                &format!("restore-{n}"),
                "r1",
                &format!("fixture document {n}"),
            );
            if n == 0 {
                value["texts"] = json!(["fixture document 0", "fixture document 1"]);
                value["section_readers"] = json!([["grp:group"], ["grp:group"]]);
            }
            value
        })
        .collect();
    seed["content"] = json!(documents.chunks(100).collect::<Vec<_>>());
    seed.as_object_mut().unwrap().remove("permissions");
    input(&root, seed);
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
    let target = publication(&commit).await.1;
    let changes = |epoch: usize| {
        let documents: Vec<_> = (0..300).map(|n| {
            if n == 299 {
                return json!({"id":"restore-299","delete":true,"revision":format!("r{epoch}"),"expected_prior_revision":"r1"});
            }
            let mut value = doc(&format!("restore-{n}"), &format!("r{epoch}"), &format!("fixture document {n}"));
            value["expected_prior_revision"] = json!("r1");
            if n == 0 {
                value["texts"] = json!(["fixture document 0", "fixture document 1"]);
                value["section_readers"] = json!([["grp:group"], ["grp:group"]]);
            }
            value
        }).collect();
        // Keep the content publication granted, then revoke every root through
        // the permission stream so both restore targets need two patch groups.
        let permissions: Vec<_> = (0..300)
            .map(|n| json!({"id":format!("restore-{n}"),"revision":format!("p{epoch}"),"readers":[]}))
            .collect();
        json!({"epoch":epoch,"content":documents.chunks(100).collect::<Vec<_>>(),
            "permissions":permissions.chunks(100).collect::<Vec<_>>()})
    };
    let config = reader_config(&worker, &root, "every");
    let mut reader = tools::Mcp::open(&config);
    let granted = fetch(&mut reader, "restore-0");
    assert!(readable(&granted), "initial grant is live: {granted}");
    input(&root, changes(2));
    // Publish content before its separate permission stream so rollback's
    // predecessor still has grants to patch across both root groups.
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let predecessor = publication(&commit).await.1.publication_id;
    let content_grant = fetch(&mut reader, "restore-260");
    assert!(
        readable(&content_grant),
        "content must retain the grant until the permission stream: {content_grant}"
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    let permission_deny = fetch(&mut reader, "restore-260");
    assert!(
        !readable(&permission_deny),
        "the permission publication must revoke the grant: {permission_deny}"
    );
    // Roll back while the preceding snapshot still has broad grants, then
    // explicitly restore the original snapshot: each must patch all 300 roots.
    for rollback in [true, false] {
        let before = publication(&commit).await.1;
        let result = if rollback {
            command(&commit, &["writer", "rollback"])
        } else {
            command(&commit, &["writer", "restore", &target.publication_id])
        };
        if rollback {
            assert_eq!(result["from_publication"], predecessor);
        }
        let (head, after) = publication(&commit).await;
        assert_eq!(after.sequence, before.sequence + 1, "one final publication");
        assert!(result["restored"].as_bool().unwrap());
        let loaded = fyrr_core::config::load(&commit).unwrap();
        let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
            .await
            .unwrap();
        let transactions = &after.lineage.transactions;
        let mut updates = 0;
        let mut restores = 0;
        for recorded in transactions {
            let tx = dataset
                .read_version_transaction(recorded.version)
                .await
                .unwrap()
                .transaction
                .unwrap();
            match tx.operation {
                lance::dataset::transaction::Operation::Update { .. } => updates += 1,
                lance::dataset::transaction::Operation::Restore { .. } => restores += 1,
                _ => {}
            }
        }
        let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        let restored_from: fyrr_core::publication::ContentManifest =
            fyrr_reader::storage::read_contract(
                store.as_ref(),
                &object_store::path::Path::from(format!(
                    "{namespace}/publications/{}.json",
                    result["from_publication"].as_str().unwrap()
                )),
            )
            .await
            .unwrap();
        let historical = dataset
            .checkout_version(restored_from.lance_version)
            .await
            .unwrap();
        let granted_rows = historical
            .count_rows(Some(
                "source_id = 'incremental' AND array_length(readers) > 0".into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            granted_rows,
            if rollback { 300 } else { 301 },
            "the selected snapshot must retain grants across both root groups"
        );
        assert_eq!(updates, 2, "300 changed roots must use two groups");
        assert_eq!(restores, 1);
        eprintln!(
            "restore_cost rollback={rollback} roots=300 groups={updates} native_commits={} prior_root_updates=300",
            transactions.len()
        );
        for key in ["restore-0", "restore-260", "restore-299"] {
            assert!(
                !readable(&fetch(&mut reader, key)),
                "restore must preserve current deny/delete: {key}"
            );
        }
        command(&commit, &["writer", "verify"]);
    }
    let cases = std::env::var("FYRR_TEST_RESTORE_REVIEW_CASE")
        .map(|case| vec![Some(case)])
        .unwrap_or_else(|_| {
            vec![
                None,
                Some("a1".into()),
                Some("a2".into()),
                Some("cadence".into()),
            ]
        });
    for case in cases {
        use std::os::unix::process::ExitStatusExt;
        let started = Instant::now();
        // Bound a hung child, including the real CLI commands its hooks invoke.
        // Event order comes from the acknowledged restore callbacks, not this watchdog.
        let mut probe = Command::new("timeout");
        probe.args(["--kill-after=5s", "180s"]).arg(
            std::env::var_os("FYRR_WRITER_TEST_BINARY")
                .expect("writer libtest built by scripts/ci"),
        );
        if let Some(case) = &case {
            probe.env("FYRR_TEST_RESTORE_REVIEW_CASE", case);
        } else {
            probe.env_remove("FYRR_TEST_RESTORE_REVIEW_CASE");
        }
        let output = probe
            .args([
                "maintenance::restore_tests::restore_drains_restarts_and_caps_attempts_on_storage",
                "--exact",
                "--ignored",
                "--nocapture",
            ])
            .env("FYRR_TEST_RESTORE_WORKER", &worker)
            .env("FYRR_TEST_RESTORE_CONFIG", &commit)
            .env("FYRR_TEST_RESTORE_INPUT", root.join("input.json"))
            .env("FYRR_TEST_RESTORE_MEMBERS", &members_path)
            .env("FYRR_TEST_RESTORE_TARGET", &target.publication_id)
            .env(
                "FYRR_TEST_RESTORE_BINARY",
                host_command(&commit).get_program(),
            )
            // Nested real writer commands need the same provider environment
            // as worker_command; recorded CI does not export it globally.
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
        eprintln!(
            "restore_probe case={case:?} status={} code={:?} signal={:?} elapsed={:?}",
            output.status,
            output.status.code(),
            output.status.signal(),
            started.elapsed(),
        );
        assert!(
            output.status.success(),
            "case={case:?} status={} code={:?} signal={:?} elapsed={:?}\n{}\n{}",
            output.status,
            output.status.code(),
            output.status.signal(),
            started.elapsed(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    }
    for key in ["restore-0", "restore-260", "restore-299"] {
        assert!(
            !readable(&fetch(&mut reader, key)),
            "live retained reader must not widen after retries: {key}"
        );
    }
    command(&commit, &["writer", "verify"]);
}

/// Rewrite only test-clock observations. Preserve the real fixture's objects and payload bytes.
fn age_observations(request: &str, response: &mut Vec<u8>, expired: Option<&str>) {
    if !request.starts_with("GET ") {
        return;
    }
    super::corrections::rewrite_ok_body(response, |body| {
        let mut changed = false;
        if request.contains("/publications/") && !request.contains("list-type=") {
            if let Ok(mut value) = serde_json::from_slice::<Value>(body) {
                value["committed_at"] =
                    json!((fyrr_core::clock::now() - chrono::Duration::hours(25)).to_rfc3339());
                *body = serde_json::to_vec(&value).unwrap();
                changed = true;
            }
        } else if expired.is_some_and(|id| request.contains(id)) {
            if let Ok(mut value) = serde_json::from_slice::<Value>(body) {
                value["created_at"] =
                    json!((fyrr_core::clock::now() - chrono::Duration::days(4)).to_rfc3339());
                *body = serde_json::to_vec(&value).unwrap();
                changed = true;
            }
        } else if request.contains("list-type=")
            && let Ok(mut objects) = serde_json::from_slice::<
                Vec<fyrr_reader::storage::test_store::boundary::Metadata>,
            >(body)
        {
            for object in &mut objects {
                object.last_modified = "2010-01-01T00:00:00Z".parse().unwrap();
            }
            *body = serde_json::to_vec(&objects).unwrap();
            changed = true;
        } else if request.contains("list-type=")
            && let Ok(mut text) = String::from_utf8(body.clone())
        {
            let mut from = 0;
            while let Some(start) = text[from..].find("<LastModified>") {
                let start = from + start + 14;
                let end = text[start..].find("</LastModified>").unwrap() + start;
                text.replace_range(start..end, "2010-01-01T00:00:00Z");
                from = start + 20;
                changed = true;
            }
            *body = text.into_bytes();
        }
        changed
    });
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_purge_window_and_restore_horizon_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let old = publication(&commit).await.1;
    let old_files: Vec<_> = old
        .files
        .iter()
        .filter(|f| f.path.starts_with("data/") || f.path.starts_with("_indices/"))
        .map(|f| f.path.clone())
        .collect();
    input(
        &root,
        json!({"epoch":2,"content":[[{"delete":true,"id":"b","revision":"r2"}]],"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    publish(&commit);
    command(&commit, &["writer", "maintain"]);
    let current = publication(&commit).await.1;
    let removed: Vec<_> = old_files
        .into_iter()
        .filter(|p| !current.files.iter().any(|f| &f.path == p))
        .collect();
    assert!(!removed.is_empty());
    let stop = Arc::new(AtomicBool::new(false));
    let transform: corrections::ResponseTransform =
        Arc::new(|request, response| age_observations(request, response, None));
    let (endpoint, thread) = corrections::proxy_transform(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        transform,
        None,
    );
    let mut guard = thread;
    guard.resume = Arc::new(AtomicBool::new(true));
    let config = proxied(&commit, &endpoint);
    let expired = host_command(&config)
        .args(["writer", "restore", &old.publication_id])
        .output()
        .unwrap();
    assert!(!expired.status.success());
    assert!(String::from_utf8_lossy(&expired.stderr).contains("restore_horizon_expired"));
    command(&config, &["writer", "maintain"]);
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    // Payloads are removed before manifests so partial cleanup never loses the
    // native verification proof. Continue the bounded cleanup until it drains.
    let old_manifest = old
        .files
        .iter()
        .find(|f| f.path.starts_with("_versions/"))
        .unwrap();
    for _ in 0..10 {
        if matches!(
            store
                .head(&object_store::path::Path::from(format!(
                    "{ns}/dataset.lance/{}",
                    old_manifest.path
                )))
                .await,
            Err(object_store::Error::NotFound { .. })
        ) {
            break;
        }
        command(&config, &["writer", "maintain"]);
    }
    assert!(
        matches!(
            store
                .head(&object_store::path::Path::from(format!(
                    "{ns}/dataset.lance/{}",
                    old_manifest.path
                )))
                .await,
            Err(object_store::Error::NotFound { .. })
        ),
        "expired native manifest remains"
    );
    for path in &removed {
        assert!(
            matches!(
                store
                    .head(&object_store::path::Path::from(format!(
                        "{ns}/dataset.lance/{path}"
                    )))
                    .await,
                Err(object_store::Error::NotFound { .. })
            ),
            "retired payload remains: {path}"
        );
    }
    assert_eq!(command(&commit, &["writer", "verify"])["verified"], true);
    let config = reader_config(&worker, &root, "held");
    let mut mcp = tools::Mcp::open(&config);
    assert!(!readable(&fetch(&mut mcp, "a")));
    assert!(!readable(&fetch(&mut mcp, "b")));
    let healthy = mcp.call(
        "search",
        json!({"order":"updated_at","sources":["allowed"],"limit":10}),
    );
    assert!(
        !healthy["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    drop(mcp);
    drop(guard);
}

/// Objects the namespace holds under one control prefix.
async fn control_objects(config: &Path, prefix: &str) -> usize {
    use futures::TryStreamExt;
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    store
        .list(Some(&namespace.join(prefix)))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len()
}

/// One round of the history: new content and a new member, so the publication
/// is a publication of its own.
fn history_round(worker: &Path, root: &Path, round: u64) {
    let mut users = vec![
        json!({"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}),
    ];
    users.extend((2..=round).map(|n| json!({"id":format!("u{n}"),"email":format!("u{n}@example.test"),"email_verification":"verified_corporate","active":true})));
    input(
        root,
        json!({"epoch":round,
            "content":[[replacement("a", &format!("r{round}"), &format!("r{}", round - 1), &format!("fixture document {round}"))]],
            "members":[{"users":users,"groups":[{"id":"child","members":["usr:u"]},{"id":"group","members":["grp:child"]}]}]}),
    );
    worker_sync(
        worker,
        &["incremental", "--kind", "content", "--kind", "members"],
    );
}

#[tokio::test]
#[ignore = "real CLI and storage; scripts/ci"]
async fn maintenance_deletes_history_outside_the_purge_window_on_storage() {
    use futures::TryStreamExt;
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    for round in 2..=10 {
        history_round(&worker, &root, round);
        publish(&commit);
    }
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let (head, _) = publication(&commit).await;
    let chain = fyrr_reader::storage::manifest_chain(
        store.clone(),
        namespace.clone(),
        head.content.as_ref().map(|p| p.manifest.clone()),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert!(chain.len() >= 10, "{} publications", chain.len());
    let oldest = chain
        .iter()
        .rev()
        .find_map(|m| m.envelope_ids.first().cloned())
        .unwrap();
    let pending = command(&commit, &["writer", "status"])["pending_envelopes"].clone();
    // A manifest lost inside the window is not where the history ends:
    // maintenance refuses and deletes nothing.
    let lost = chain[3].publication_id.clone();
    let path =
        |id: &str| object_store::path::Path::from(format!("{namespace}/publications/{id}.json"));
    let bytes = store
        .get(&path(&lost))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    store.delete(&path(&lost)).await.unwrap();
    let refused = host_command(&commit)
        .args(["writer", "maintain"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("publication_invalid"));
    // So does every other reader of the chain, `writer status` among them.
    let status = host_command(&commit)
        .args(["writer", "status"])
        .output()
        .unwrap();
    assert!(!status.status.success());
    assert!(String::from_utf8_lossy(&status.stderr).contains("publication_invalid"));
    for manifest in chain.iter().filter(|m| m.publication_id != lost) {
        store.head(&path(&manifest.publication_id)).await.unwrap();
    }
    store.put(&path(&lost), bytes.into()).await.unwrap();
    // Inside the window nothing is deleted, and every receipt is there.
    command(&commit, &["writer", "maintain"]);
    assert!(control_objects(&commit, "publications").await >= 10);
    assert!(
        command(&commit, &["writer", "commit", "--receipt", &oldest])["publication_id"].is_string()
    );

    // Past the window, but not past the age the envelopes are kept for.
    fyrr_core::clock::advance(Duration::from_secs(25 * 3600)).unwrap();
    command(&commit, &["writer", "maintain"]);
    command(&commit, &["writer", "maintain"]);
    // Every maintenance step publishes, so those manifests are retained.
    let (head, _) = publication(&commit).await;
    let kept = fyrr_reader::storage::manifest_chain(
        store.clone(),
        namespace.clone(),
        head.content.as_ref().map(|p| p.manifest.clone()),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let publications = control_objects(&commit, "publications").await;
    assert_eq!(kept.len(), publications, "unreachable manifests remain");
    // Past the window only the cut stays, kept for the grace after its
    // retirement: the history before it, the original publications
    // included, is gone.
    let window = chrono::Duration::hours(24);
    let outside: Vec<_> = kept
        .iter()
        .filter(|m| fyrr_core::clock::now() - m.committed_at >= window)
        .map(|m| m.publication_id.clone())
        .collect();
    assert_eq!(outside.len(), 1, "{outside:?}");
    for old in &chain {
        assert!(
            !kept.iter().any(|m| m.publication_id == old.publication_id),
            "{} remains",
            old.publication_id
        );
    }
    let checkpoints = control_objects(&commit, "evidence").await;
    assert!(
        checkpoints <= publications,
        "{checkpoints} checkpoints for {publications} retained publications"
    );
    // Retained publications keep their receipts; the deleted part answers from
    // the evidence, without its publication; an envelope never applied does not.
    for id in kept.iter().flat_map(|m| &m.envelope_ids) {
        assert!(
            command(&commit, &["writer", "commit", "--receipt", id])["publication_id"].is_string()
        );
    }
    let lost = command(&commit, &["writer", "commit", "--receipt", &oldest]);
    assert!(lost["publication_id"].is_null());
    assert_eq!(lost["envelope_ids"], json!([oldest]));
    let unknown = host_command(&commit)
        .args([
            "writer",
            "commit",
            "--receipt",
            &format!("env-{}", "0".repeat(32)),
        ])
        .output()
        .unwrap();
    assert!(!unknown.status.success());
    // The envelopes still in the log are still known to have been applied.
    assert_eq!(
        command(&commit, &["writer", "status"])["pending_envelopes"],
        pending
    );

    // The window moved on: the cut moves to the oldest manifest it keeps.
    fyrr_core::clock::advance(Duration::from_secs(2 * 3600)).unwrap();
    command(&commit, &["writer", "maintain"]);
    assert!(matches!(
        store.head(&path(&outside[0])).await,
        Err(object_store::Error::NotFound { .. })
    ));
    // A longer window than the history left is no lost manifest: the walks end
    // at the cut.
    let mut text = std::fs::read_to_string(&commit).unwrap();
    text.push_str("\n[writer]\npurge_window='7d'\n");
    text.parse::<toml::Table>().expect("one [writer] table");
    std::fs::write(&commit, text).unwrap();
    command(&commit, &["writer", "maintain"]);
    command(&commit, &["writer", "status"]);

    // A fresh writer, restore and rollback work from what is left.
    history_round(&worker, &root, 11);
    publish(&commit);
    command(&commit, &["writer", "rollback"]);
    assert_eq!(command(&commit, &["writer", "verify"])["verified"], true);
    std::fs::remove_dir_all(root).unwrap();
}
