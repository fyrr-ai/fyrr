//! Incremental contracts through real executable processes and the owned RustFS.
use super::coordination::{committer_host, host_command, publication, worker_sync};
use super::*;
use arrow_array::{Array, StringArray};
use fyrr_core::connector::SyncKind;
use fyrr_core::envelope::{EnvelopePayload, PreparedEnvelope};
use object_store::ObjectStoreExt;
use std::collections::BTreeSet;

#[tokio::test]
#[ignore = "real worker and RustFS; scripts/ci"]
async fn incremental_unchanged_upsert_still_protects_pending_reconciliation_on_real_s3() {
    let setup = setup();
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (worker, commit, root) = fixture_host_at(8, true, "presence-fixture", setup);
    input(
        &root,
        json!({"epoch":1,"content":[[doc("present","r1","fixture document 0")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content", "--full"]);
    publish(&commit);
    assert_eq!(roots(&rows(&worker).await), ["present"]);
    input(
        &root,
        json!({"epoch":2,"listing":[{"last":true,"complete":true,"roots":[]}]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "reconcile", "--full"]);
    // The connector observes the document after the listing started, unchanged.
    input(
        &root,
        json!({"epoch":3,"content":[[doc("present","r1","fixture document 0")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content", "--full"]);
    publish(&commit);
    assert_eq!(
        roots(&rows(&worker).await),
        ["present"],
        "an unchanged observed root must not be reconciled away"
    );
    std::fs::remove_dir_all(root).unwrap();
}

pub(super) fn fixture() -> (PathBuf, PathBuf, FixtureRoot) {
    fixture_size(768)
}
pub(super) fn fixture_on(backend: Backend) -> (PathBuf, PathBuf, FixtureRoot) {
    fixture_host_at(768, true, "dense-fixture", setup_on(backend))
}
fn fixture_size(documents: usize) -> (PathBuf, PathBuf, FixtureRoot) {
    fixture_host(documents, true)
}
pub(super) fn fixture_host(
    documents: usize,
    publish_initial: bool,
) -> (PathBuf, PathBuf, FixtureRoot) {
    fixture_host_in_namespace(documents, publish_initial, "dense-fixture")
}
pub(super) fn fixture_host_in_namespace(
    documents: usize,
    publish_initial: bool,
    namespace: &str,
) -> (PathBuf, PathBuf, FixtureRoot) {
    fixture_host_at(documents, publish_initial, namespace, setup())
}

pub(super) fn fixture_host_at(
    documents: usize,
    publish_initial: bool,
    namespace: &str,
    (config, root): (PathBuf, FixtureRoot),
) -> (PathBuf, PathBuf, FixtureRoot) {
    for row in 768..documents {
        std::fs::write(
            root.join("sources").join(format!("{row}.txt")),
            format!("fixture document {}", row % 768),
        )
        .unwrap();
    }
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace(
            "name = \"dense-fixture\"",
            &format!("name = \"{namespace}\""),
        )
        .replace(
            "stdio_user = \"fixture-subject\"",
            "stdio_user = \"fixture@example.test\"",
        );
    std::fs::write(&config, format!("{text}\n[[sources]]\nid='incremental'\ntype='incremental-test'\npermissions='sync'\ndocument_max_bytes='64B'\n[sources.intervals]\npermissions='5m'\nmembers='5m'\n[sources.scope]\nfixture='{}'\n", root.join("input.json").display())).unwrap();
    input(&root, json!({"epoch":0}));
    let output = host_command(&config)
        .args(["writer", "namespace", "create"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    worker_sync(&config, &["allowed", "denied", "--full"]);
    let commit = committer_host(&config, &root);
    if publish_initial {
        publish(&commit);
    }
    (config, commit, root)
}
fn isolated(text: &str, root: &Path, host: &Path) -> String {
    text.replace(&root.display().to_string(), &host.display().to_string())
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_refused_provider_never_prepares_on_real_s3() {
    use futures::TryStreamExt;
    let (config, root) = setup();
    let fixture = root.join("future.json");
    std::fs::write(
        &fixture,
        serde_json::to_vec(&json!({"cursor":"future-1","documents":[{
            "id":"future","title":"fixture","author":"fixture",
            "text":"fixture document 0","readers":["grp:allow"]
        }]}))
        .unwrap(),
    )
    .unwrap();
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config, format!("{text}\n[[sources]]\nid='future'\ntype='sentinel-future-sdk'\npermissions='fixed'\nreaders=['grp:allow']\n[sources.scope]\nfixture='{}'\n", fixture.display())).unwrap();
    super::coordination::command(&config, &["writer", "namespace", "create"]);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    // Explicit kinds and defaults must all refuse invalid providers before dispatch.
    for args in [
        vec!["writer", "sync", "future", "--kind", "content"],
        vec!["writer", "sync", "future", "--kind", "permissions"],
        vec!["writer", "sync", "future", "--kind", "members"],
        vec!["writer", "sync", "future", "--kind", "reconcile"],
        vec!["writer", "sync", "future"],
        vec!["writer", "sync", "future", "--due"],
    ] {
        let output = super::coordination::worker_command(&config)
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(78), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("its connector provider is refused"),
            "{args:?}"
        );
        for prefix in ["envelopes", "cursors"] {
            let objects: Vec<_> = store
                .list(Some(&ns.clone().join(prefix)))
                .try_collect()
                .await
                .unwrap();
            assert!(objects.is_empty(), "{args:?} wrote {prefix}");
        }
        assert!(
            !output.status.success(),
            "{args:?} executed a refused provider"
        );
    }
}
pub(super) fn input(root: &Path, value: Value) {
    std::fs::write(root.join("input.json"), serde_json::to_vec(&value).unwrap()).unwrap();
}
pub(super) fn doc(id: &str, revision: &str, text: &str) -> Value {
    json!({"id":id,"revision":revision,"texts":[text]})
}
pub(super) fn publish(config: &Path) -> Value {
    let output = host_command(config)
        .args(["writer", "commit"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "commit: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
pub(super) async fn rows(config: &Path) -> arrow_array::RecordBatch {
    source_rows(config, "incremental").await
}
/// The published rows of one source.
pub(super) async fn source_rows(config: &Path, source: &str) -> arrow_array::RecordBatch {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (head, _) = publication(config).await;
    let ds = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        loaded
            .config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        head.content.unwrap().lance_version,
        None,
    )
    .await
    .unwrap();
    ds.scan()
        .filter(&format!("source_id = '{source}'"))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap()
}
pub(super) fn roots(batch: &arrow_array::RecordBatch) -> Vec<String> {
    batch
        .column_by_name("root_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap().to_owned())
        .collect()
}
pub(super) async fn envelopes(config: &Path) -> Vec<(object_store::path::Path, PreparedEnvelope)> {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (head, _) = publication(config).await;
    let (store, _) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let (paths, _) = fyrr_writer::commit::envelope_paths(&loaded.config, &head)
        .await
        .unwrap();
    let mut out = Vec::new();
    for path in paths {
        let envelope = fyrr_reader::storage::read_contract(store.as_ref(), &path)
            .await
            .unwrap();
        out.push((path, envelope));
    }
    out
}
pub(super) fn initial(root: &Path) {
    input(
        root,
        json!({"epoch":1,"content":[[doc("a","r1","fixture document 0"),doc("b","r1","fixture document 1")]],"permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]},{"id":"b","revision":"p1","readers":["grp:group"]}]],"members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"child","members":["usr:u"]},{"id":"group","members":["grp:child"]}]}]}),
    );
}
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_priority_backpressure_revoke_and_pending_readers_on_real_s3() {
    let (worker, commit, root) = fixture();
    initial(&root);
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
    assert_eq!(roots(&rows(&worker).await), ["a", "b"]);
    let mut reader = super::tools::Mcp::open(&worker);
    let visible = reader.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert!(!visible["isError"].as_bool().unwrap_or(false), "{visible}");
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    // A different process/host supplies only the permission payload and has no worker corpus.
    let permission_dir = root.join("permission-host");
    std::fs::create_dir(&permission_dir).unwrap();
    let permission_config = permission_dir.join("fyrr.toml");
    let worker_text = std::fs::read_to_string(&worker).unwrap();
    let bounded = worker_text.replace(
        "[reader]",
        "[writer.backpressure]\nmax_pending_content_envelopes=1\n[reader]",
    );
    std::fs::write(&worker, &bounded).unwrap();
    std::fs::write(
        &permission_config,
        isolated(&bounded, &root, &permission_dir),
    )
    .unwrap();
    input(
        &permission_dir,
        json!({"epoch":3,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    let blocked = super::coordination::worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "content"])
        .output()
        .unwrap();
    assert!(!blocked.status.success());
    assert!(
        String::from_utf8_lossy(&blocked.stdout).contains("content_backpressure"),
        "{} {}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    worker_sync(
        &permission_config,
        &["incremental", "--kind", "permissions"],
    );
    publish(&commit);
    let denied = reader.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert!(
        denied["isError"].as_bool().unwrap_or(false) || denied.to_string().contains("not_found"),
        "{denied}"
    );
    let batch = rows(&worker).await;
    let ids = roots(&batch);
    let row = ids.iter().position(|id| id == "a").unwrap();
    let acl = batch
        .column_by_name("readers")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap();
    assert_eq!(
        acl.value(row).len(),
        0,
        "pending content regranted a revoked reader"
    );
    let values = envelopes(&worker).await;
    let content = values
        .iter()
        .find(|(_, e)| {
            e.source_id == "incremental" && e.kind == SyncKind::Content && e.sequence == 2
        })
        .unwrap();
    let receipt = host_command(&commit)
        .args(["writer", "commit", "--receipt", &content.1.envelope_id])
        .output()
        .unwrap();
    assert!(receipt.status.success());
    let latest = publication(&worker).await.1;
    let earlier: Value = serde_json::from_slice(&receipt.stdout).unwrap();
    assert_eq!(earlier["publication_id"], latest.publication_id);
    let mut fresh = super::tools::Mcp::open(&worker);
    let denied = fresh.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert!(
        denied["isError"].as_bool().unwrap_or(false)
            || denied["structuredContent"]["documents"]
                .as_array()
                .is_some_and(Vec::is_empty),
        "{denied}"
    );
    let before = fresh.call("fetch", json!({"source_id":"incremental","root_id":"b"}));
    assert!(!before["isError"].as_bool().unwrap_or(false), "{before}");
    input(
        &permission_dir,
        json!({"epoch":4,"members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"group","members":[]}]}]}),
    );
    worker_sync(&permission_config, &["incremental", "--kind", "members"]);
    publish(&commit);
    let removed = fresh.call("fetch", json!({"source_id":"incremental","root_id":"b"}));
    assert!(
        removed["isError"].as_bool().unwrap_or(false)
            || removed["structuredContent"]["documents"]
                .as_array()
                .is_some_and(Vec::is_empty),
        "{removed}"
    );
    let mut remaining = roots(&rows(&worker).await);
    remaining.sort();
    assert_eq!(remaining, ["a", "b"]);
    let read_at = publication(&worker)
        .await
        .0
        .freshness
        .sources
        .iter()
        .find(|s| s.source_id == "incremental")
        .unwrap()
        .members_read_at;
    for data in [
        json!({"epoch":5,"members":[{"last":false,"users":[],"groups":[]}]}),
        json!({"epoch":6}),
    ] {
        input(&permission_dir, data);
        let refused = super::coordination::worker_command(&permission_config)
            .args(["writer", "sync", "incremental", "--kind", "members"])
            .output()
            .unwrap();
        assert!(!refused.status.success());
        assert!(
            String::from_utf8_lossy(&refused.stdout).contains("membership_snapshot_incomplete")
        );
        publish(&commit);
        assert_eq!(
            publication(&worker)
                .await
                .0
                .freshness
                .sources
                .iter()
                .find(|s| s.source_id == "incremental")
                .unwrap()
                .members_read_at,
            read_at
        );
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_revisions_quarantine_listing_and_two_old_fragment_replacements_on_real_s3() {
    let (worker, commit, root) = fixture();
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    // Two worker invocations start from the same publication and modify disjoint logical keys
    // in the same physical fragment. Their local homes are distinct and committer has neither.
    let second_dir = root.join("second-worker");
    std::fs::create_dir(&second_dir).unwrap();
    let second = second_dir.join("fyrr.toml");
    std::fs::write(
        &second,
        isolated(
            &std::fs::read_to_string(&worker).unwrap(),
            &root,
            &second_dir,
        ),
    )
    .unwrap();
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    input(
        &second_dir,
        json!({"epoch":3,"content":[[doc("b","r2","fixture document 3")]]}),
    );
    // Exercise the exact bounded lookup against the real pin, surfacing native diagnostics
    // only for these synthetic test values.
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (head, _) = publication(&worker).await;
    let ds = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        loaded
            .config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        head.content.unwrap().lance_version,
        None,
    )
    .await
    .unwrap();
    ds.scan()
        .project(&["root_id", "source_revision"])
        .unwrap()
        .filter(&fyrr_writer::apply::keys_filter(
            "incremental",
            &["b".into()],
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    worker_sync(&second, &["incremental", "--kind", "content"]);
    publish(&commit);
    let batch = rows(&worker).await;
    assert_eq!(batch.num_rows(), 2);
    let revisions = batch
        .column_by_name("source_revision")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(revisions.iter().all(|r| r == Some("r2")));
    input(
        &root,
        json!({"epoch":4,"content":[[{"id":"a","revision":"bad","texts":[""]},doc("b","r3","fixture document 4")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let batch = rows(&worker).await;
    let revisions = batch
        .column_by_name("source_revision")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        revisions.value(roots(&batch).iter().position(|id| id == "a").unwrap()),
        "r2"
    );
    input(
        &root,
        json!({"epoch":"oversized","content":[[doc("a","oversized",&"x".repeat(65))]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let status = host_command(&worker)
        .args(["writer", "status"])
        .output()
        .unwrap();
    assert!(status.status.success());
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["quarantine"]["document_invalid"], 1);
    assert_eq!(status["quarantine"]["document_oversized"], 1);
    // One stale key conflicts; the disjoint good key still publishes, and refetch rewinds
    // only the opaque cursor while sequence remains monotonic with a new rejection generation.
    let mut stale = doc("a", "r4", "fixture document 5");
    stale["expected_prior_revision"] = json!("foreign");
    input(
        &root,
        json!({"epoch":5,"content":[[stale,doc("b","r4","fixture document 6")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let (_, manifest) = publication(&worker).await;
    assert!(!manifest.rejected_envelope_ids.is_empty());
    let batch = rows(&worker).await;
    assert_eq!(batch.num_rows(), 2);
    input(
        &root,
        json!({"epoch":6,"content":[[doc("a","r4","fixture document 5")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let values = envelopes(&worker).await;
    assert!(
        values
            .iter()
            .any(|(_, e)| e.source_id == "incremental" && e.rejection_generation == 1)
    );
    // Partial listing is incapable of deleting. A complete listing bound to the old
    // watermark cannot delete a later replacement prepared while listing is pending.
    input(
        &root,
        json!({"epoch":7,"listing":[{"last":true,"complete":false,"roots":[]}]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "reconcile"]);
    publish(&commit);
    assert_eq!(rows(&worker).await.num_rows(), 2);
    input(
        &root,
        json!({"epoch":8,"listing":[{"last":true,"complete":true,"roots":[]}]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "reconcile"]);
    input(
        &root,
        json!({"epoch":9,"content":[[doc("a","r5","fixture document 7")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert_eq!(roots(&rows(&worker).await), ["a"]);
    // A delete becomes a retained row in the SAME published version. An envelope
    // prepared before that delete cannot resurrect its old content.
    input(
        &root,
        json!({"epoch":10,"content":[[{"id":"a","revision":"r6","delete":true}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert_eq!(rows(&worker).await.num_rows(), 0);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let entries = envelopes(&worker).await;
    let mut stale=entries.iter().find(|(_,e)|matches!(&e.payload,EnvelopePayload::Content(p) if e.source_id=="incremental" && p.documents.len()==1 && p.documents[0].source_revision.as_deref()==Some("r5"))).unwrap().1.clone();
    stale.rejection_generation += 1;
    stale.envelope_id = fyrr_core::envelope::envelope_id(
        &stale.namespace,
        &stale.source_id,
        stale.kind,
        stale.sequence,
        stale.rejection_generation,
        stale.cursor.start.as_deref(),
    );
    let EnvelopePayload::Content(payload) = &mut stale.payload else {
        unreachable!()
    };
    payload.documents[0].expected_prior_revision = None;
    store
        .put(
            &ns.join("envelopes")
                .join(format!("{}.json", stale.envelope_id)),
            fyrr_core::write_contract(&stale).into(),
        )
        .await
        .unwrap();
    publish(&commit);
    assert_eq!(rows(&worker).await.num_rows(), 0);
    assert!(
        publication(&worker)
            .await
            .1
            .rejected_envelope_ids
            .contains(&stale.envelope_id)
    );
    let output = host_command(&worker)
        .args(["writer", "maintain"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_per_kind_leases_due_intervals_and_provider_isolation_on_real_s3() {
    let (worker, commit, root) = fixture();
    let text = std::fs::read_to_string(&worker)
        .unwrap()
        .replace("[reader]", "[writer.commit]\nlease_ttl='3s'\n[reader]");
    let crash_dir = root.join("crash-host");
    let hang_dir = root.join("hang-host");
    std::fs::create_dir(&crash_dir).unwrap();
    std::fs::create_dir(&hang_dir).unwrap();
    input(&crash_dir, json!({"mode":"crash"}));
    input(&hang_dir, json!({"mode":"hang"}));
    let text = format!(
        "{text}\n[[sources]]\nid='crasher'\ntype='incremental-test'\npermissions='fixed'\nreaders=['grp:allow']\n[sources.scope]\nfixture='{}'\n[[sources]]\nid='hanger'\ntype='incremental-test'\npermissions='fixed'\nreaders=['grp:allow']\n[sources.scope]\nfixture='{}'\n",
        crash_dir.join("input.json").display(),
        hang_dir.join("input.json").display()
    );
    std::fs::write(&worker, &text).unwrap();
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1","fixture document 0")]]}),
    );
    let started = std::time::Instant::now();
    let output = super::coordination::worker_command(&worker)
        .args([
            "writer",
            "sync",
            "incremental",
            "crasher",
            "hanger",
            "--kind",
            "content",
            "--jobs",
            "3",
            "--timeout",
            "4s",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(started.elapsed() < std::time::Duration::from_secs(8));
    let reports = String::from_utf8(output.stdout).unwrap();
    assert!(
        reports.contains("\"source_id\":\"incremental\"") && reports.contains("\"failed\":false"),
        "{reports}"
    );
    publish(&commit);
    assert_eq!(roots(&rows(&worker).await), ["a"]);
    // Another process may own permissions while a content process is hung. The
    // same-kind contender fails promptly rather than waiting for the first holder.
    input(&root, json!({"mode":"hang"}));
    let mut holder = super::coordination::worker_command(&worker)
        .args([
            "writer",
            "sync",
            "incremental",
            "--kind",
            "content",
            "--timeout",
            "4s",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns.clone().join("cursors").join("incremental-content.json");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let state: fyrr_core::connector::CursorState =
            fyrr_reader::storage::read_contract(store.as_ref(), &path)
                .await
                .unwrap();
        if root.join("entered").exists()
            && state
                .lease
                .is_some_and(|l| l.expires_at > chrono::Utc::now())
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p1","readers":[]}]]}),
    );
    let contender = super::coordination::worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "content"])
        .output()
        .unwrap();
    assert!(!contender.status.success());
    assert!(String::from_utf8_lossy(&contender.stdout).contains("sync_busy"));
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    let due = super::coordination::worker_command(&worker)
        .args([
            "writer",
            "sync",
            "incremental",
            "--kind",
            "permissions",
            "--due",
        ])
        .output()
        .unwrap();
    assert!(due.status.success());
    assert!(String::from_utf8_lossy(&due.stdout).contains("interval_not_due"));
    assert!(!holder.wait().unwrap().success());
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_damaged_rows_rejected_and_cleanup_retains_log_authority_on_real_s3() {
    let (worker, commit, root) = fixture();
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let entries = envelopes(&worker).await;
    let (_, pending) = entries
        .iter()
        .find(|(_, e)| e.source_id == "incremental" && e.sequence == 2)
        .unwrap();
    let EnvelopePayload::Content(payload) = &pending.payload else {
        unreachable!()
    };
    let file = &payload.fragments[0].files[0];
    let path = object_store::path::Path::from(format!("{ns}/dataset.lance/{}", file.path));
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    let mut damage = bytes.to_vec();
    let offset = damage.len() / 2;
    damage[offset] ^= 1;
    store.put(&path, damage.into()).await.unwrap();
    publish(&commit);
    let (_, manifest) = publication(&worker).await;
    assert!(
        manifest
            .rejected_envelope_ids
            .contains(&pending.envelope_id)
    );
    let batch = rows(&worker).await;
    assert!(
        batch
            .column_by_name("source_revision")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .all(|r| r == Some("r1"))
    );
    input(
        &root,
        json!({"epoch":3,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    for (case, revision) in [("declared-count", "r3"), ("duplicate-records", "r4")] {
        input(
            &root,
            json!({"epoch":case,"content":[[doc("a",revision,"fixture document 2")]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "content"]);
        let values = envelopes(&worker).await;
        let (path, mut envelope) = values
            .into_iter()
            .filter(|(_, e)| e.source_id == "incremental" && e.kind == SyncKind::Content)
            .max_by_key(|(_, e)| e.sequence)
            .unwrap();
        let EnvelopePayload::Content(payload) = &mut envelope.payload else {
            unreachable!()
        };
        payload.documents[0].records += 1;
        if case == "duplicate-records" {
            payload.fragments.push(payload.fragments[0].clone());
        }
        store
            .put(&path, fyrr_core::write_contract(&envelope).into())
            .await
            .unwrap();
        publish(&commit);
        let (_, manifest) = publication(&worker).await;
        assert!(
            manifest
                .rejected_envelope_ids
                .contains(&envelope.envelope_id),
            "{case}"
        );
        assert_eq!(rows(&worker).await.num_rows(), 2);
        input(
            &root,
            json!({"epoch":format!("{case}-repair"),"content":[[doc("a",revision,"fixture document 2")]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "content"]);
        publish(&commit);
    }
    // Age only our own synthetic immutable test envelopes; cleanup is exercised
    // through the public command, and cursor cache loss is then repaired from S3.
    let entries = envelopes(&worker).await;
    let mut latest = std::collections::BTreeMap::new();
    for (_, e) in &entries {
        latest
            .entry((e.source_id.clone(), e.kind.as_str()))
            .and_modify(|s: &mut u64| *s = (*s).max(e.sequence))
            .or_insert(e.sequence);
    }
    for (path, mut envelope) in entries {
        envelope.created_at = chrono::Utc::now() - chrono::Duration::days(8);
        store
            .put(&path, fyrr_core::write_contract(&envelope).into())
            .await
            .unwrap();
    }
    let output = host_command(&worker)
        .args(["writer", "maintain"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(report["deleted_envelopes"].as_u64().unwrap() > 0);
    let entries = envelopes(&worker).await;
    for (key, seq) in latest {
        assert!(
            entries.iter().any(|(_, e)| e.source_id == key.0
                && e.kind.as_str() == key.1
                && e.sequence == seq)
        );
    }
    store
        .delete(&ns.join("cursors").join("incremental-content.json"))
        .await
        .unwrap();
    input(
        &root,
        json!({"epoch":4,"content":[[doc("b","r2","fixture document 3")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert_eq!(rows(&worker).await.num_rows(), 2);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_sub_envelope_cursor_recovery_and_atomic_chunk_groups_on_real_s3() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    for applied_without_response in [false, true] {
        let (worker, commit, root) = fixture();
        initial(&root);
        worker_sync(&worker, &["incremental", "--kind", "content"]);
        publish(&commit);
        for config in [&worker, &commit] {
            let text = std::fs::read_to_string(config)
                .unwrap()
                .replace("document_max_bytes='64B'", "document_max_bytes='64KiB'")
                .replace(
                    "[reader]",
                    "[writer]\nenvelope_max_rows=2\n[writer.commit]\ngroup_max_rows=2\n[reader]",
                );
            std::fs::write(config, text).unwrap();
        }
        let text = "A".repeat(16384) + &"B".repeat(1000);
        let vector = root.embedding.vector("fixture query");
        for chunk in fyrr_writer::chunk::chunks(&text) {
            root.embedding.insert_both(chunk, vector.clone());
        }

        input(
            &root,
            json!({"epoch":2,"content":[[doc("a","r2",&text),doc("b","r2","fixture document 2")]]}),
        );
        let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let hook: super::corrections::BoundaryHook = Arc::new(move |request, body, after| {
            request.starts_with("PUT ")
                && request.contains("/envelopes/")
                && after == applied_without_response
                && serde_json::from_slice::<PreparedEnvelope>(body)
                    .ok()
                    .is_some_and(|e| {
                        e.source_id == "incremental"
                            && e.kind == SyncKind::Content
                            && e.sequence == 3
                    })
        });
        let (proxy, thread) =
            super::corrections::proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
        let proxied = root.join("fault.toml");
        std::fs::write(
            &proxied,
            std::fs::read_to_string(&worker)
                .unwrap()
                .replace(&endpoint, &proxy),
        )
        .unwrap();
        let output = super::coordination::worker_command(&proxied)
            .args(["writer", "sync", "incremental", "--kind", "content"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        stop.store(true, Ordering::SeqCst);
        drop(thread);
        let values = envelopes(&worker).await;
        let first = values
            .iter()
            .find(|(_, e)| {
                e.source_id == "incremental" && e.sequence == 2 && e.kind == SyncKind::Content
            })
            .unwrap();
        assert!(!first.1.advances_cursor);
        assert_eq!(first.1.cursor.start.as_deref(), Some("1-0"));
        publish(&commit);
        let batch = rows(&worker).await;
        assert_eq!(
            roots(&batch).iter().filter(|r| r.as_str() == "a").count(),
            2,
            "the replacement is atomic at each publication"
        );
        worker_sync(&worker, &["incremental", "--kind", "content"]);
        publish(&commit);
        let batch = rows(&worker).await;
        assert_eq!(batch.num_rows(), 3);
        assert!(
            batch
                .column_by_name("source_revision")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .all(|r| r == Some("r2"))
        );
        for (_, envelope) in envelopes(&worker).await.into_iter().filter(|(_, e)| {
            e.source_id == "incremental" && e.kind == SyncKind::Content && e.sequence > 1
        }) {
            let EnvelopePayload::Content(p) = envelope.payload else {
                unreachable!()
            };
            let work = p.fragments.iter().map(|f| f.rows).sum::<u64>()
                + p.documents
                    .iter()
                    .filter(|d| d.intent == fyrr_core::envelope::DocumentIntent::Delete)
                    .count() as u64;
            assert!(work <= 2);
        }
        input(
            &root,
            json!({"epoch":3,"permissions":[[{"id":"a","revision":"p2","readers":[]},{"id":"b","revision":"p2","readers":[]}]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "permissions"]);
        publish(&commit);
        let mut publications = std::collections::BTreeSet::new();
        for (_, envelope) in envelopes(&worker)
            .await
            .into_iter()
            .filter(|(_, e)| e.source_id == "incremental" && e.kind == SyncKind::Permissions)
        {
            let EnvelopePayload::Permissions(p) = &envelope.payload else {
                unreachable!()
            };
            if p.documents.is_empty() {
                continue;
            }
            assert_eq!(p.documents.len(), 1);
            let receipt = host_command(&commit)
                .args(["writer", "commit", "--receipt", &envelope.envelope_id])
                .output()
                .unwrap();
            assert!(receipt.status.success());
            let receipt: Value = serde_json::from_slice(&receipt.stdout).unwrap();
            publications.insert(receipt["publication_id"].as_str().unwrap().to_owned());
        }
        assert_eq!(
            publications.len(),
            2,
            "two-chunk and one-chunk permission replacements exceed one group"
        );
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_source_limits_and_retention_precede_embedding_on_real_s3() {
    let (worker, commit, root) = fixture();
    let text = std::fs::read_to_string(&worker).unwrap().replace(
        "document_max_bytes='64B'",
        "document_max_bytes='64B'\nrow_warning=1\nrow_error=2\nretention_days=1",
    );
    std::fs::write(&worker, &text).unwrap();
    initial(&root);
    let warning = super::coordination::worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "content"])
        .output()
        .unwrap();
    assert!(warning.status.success());
    let report: Value = serde_json::from_slice(&warning.stdout).unwrap();
    assert_eq!(report["stats"]["row_warning"], true);
    assert_eq!(report["cache_misses"], 2);
    assert_eq!(
        report["paid_calls"], 1,
        "the accepted pair is embedded in one batch"
    );
    let embeddings = root.embedding.state.requests.lock().unwrap().len();
    publish(&commit);
    assert_eq!(rows(&worker).await.num_rows(), 2);
    assert_eq!(
        root.embedding.state.requests.lock().unwrap().len(),
        embeddings,
        "limits and retention must precede embedding"
    );
    input(
        &root,
        json!({"epoch":2,"content":[[doc("c","r1","uncached quota text")]]}),
    );
    let refused = super::coordination::worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "content"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stdout).contains("source_row_limit"),
        "{} {}",
        String::from_utf8_lossy(&refused.stdout),
        String::from_utf8_lossy(&refused.stderr)
    );
    assert_eq!(rows(&worker).await.num_rows(), 2);
    assert_eq!(
        root.embedding.state.requests.lock().unwrap().len(),
        embeddings,
        "limits and retention must precede embedding"
    );
    input(
        &root,
        json!({"epoch":3,"content":[[{"id":"c","revision":"r1","texts":["uncached expired text"],"updated_at":"2000-01-01T00:00:00Z"}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert_eq!(rows(&worker).await.num_rows(), 2);
    assert_eq!(
        root.embedding.state.requests.lock().unwrap().len(),
        embeddings,
        "limits and retention must precede embedding"
    );
    let entries = envelopes(&worker).await;
    let latest = entries
        .iter()
        .filter(|(_, e)| e.source_id == "incremental" && e.kind == SyncKind::Content)
        .max_by_key(|(_, e)| e.sequence)
        .unwrap();
    let EnvelopePayload::Content(payload) = &latest.1.payload else {
        unreachable!()
    };
    assert!(payload.fragments.is_empty());
    assert_eq!(
        payload.documents[0].intent,
        fyrr_core::envelope::DocumentIntent::Delete
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_container_grants_publish_without_rewriting_content_on_real_s3() {
    let (worker, commit, root) = fixture();
    input(
        &root,
        json!({"epoch":1,"content":[[{"id":"a","revision":"r1","texts":["fixture document 0"],"readers":[],"containers":["folder"]}]],
        "permissions":[[{"id":"a","revision":"p1","readers":[],"containers":["folder"]}]],
        "container_grants":[[{"container_id":"folder","container_kind":"folder","permission_revision":"g1","readers":["grp:group"]}]],
        "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"group","members":["usr:u"]}]}]}),
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
    let mut reader = super::tools::Mcp::open(&worker);
    let result = reader.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert!(!result["isError"].as_bool().unwrap_or(false), "{result}");
    assert!(
        !result["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{result}"
    );
    let before = publication(&worker).await.0.content.unwrap().lance_version;
    input(
        &root,
        json!({"epoch":2,"container_grants":[[{"container_id":"folder","container_kind":"folder","permission_revision":"g2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    assert_eq!(
        publication(&worker).await.0.content.unwrap().lance_version,
        before
    );
    let result = reader.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert!(
        result["isError"].as_bool().unwrap_or(false)
            || result["structuredContent"]["documents"]
                .as_array()
                .is_some_and(Vec::is_empty),
        "{result}"
    );
    assert_eq!(roots(&rows(&worker).await), ["a"]);
}

pub(super) fn row_readers(batch: &arrow_array::RecordBatch, root: &str) -> Vec<String> {
    let row = roots(batch).iter().position(|id| id == root).unwrap();
    let readers = batch
        .column_by_name("readers")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap()
        .value(row);
    let readers = readers.as_any().downcast_ref::<StringArray>().unwrap();
    readers.iter().map(|v| v.unwrap().to_owned()).collect()
}

/// A sync source whose content carries no permission revision, readers or
/// containers makes no permission statement: the permission stream alone
/// decides who reads the document.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_content_without_a_permission_statement_keeps_applied_readers_on_real_s3() {
    let (worker, commit, root) = fixture();
    let silent = |id: &str, revision: &str, text: &str| json!({"id":id,"revision":revision,"texts":[text],"readers":[]});
    // New documents: "a" has a permission envelope in the same commit, "c" has
    // none yet, "d" still states its readers in content.
    input(
        &root,
        json!({"epoch":1,
        "content":[[silent("a","r1","fixture document 0"),silent("c","r1","fixture document 2"),doc("d","r1","fixture document 3")]],
        "permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]},{"id":"d","revision":"p1","readers":["grp:group"]}]],
        "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"group","members":["usr:u"]}]}]}),
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
    // A reader opened after the publication: an open one refuses its first
    // request after the published identity moved.
    let readable = |root: &str| {
        let mut reader = super::tools::Mcp::open(&worker);
        let result = reader.call("fetch", json!({"source_id":"incremental","root_id":root}));
        !result["isError"].as_bool().unwrap_or(false)
            && result["structuredContent"]["documents"]
                .as_array()
                .is_some_and(|documents| !documents.is_empty())
    };
    let batch = rows(&worker).await;
    let granted = row_readers(&batch, "d");
    assert_eq!(
        granted.len(),
        1,
        "a reader-carrying change is published as before"
    );
    assert_eq!(
        row_readers(&batch, "a"),
        granted,
        "the permission envelope decides the readers of a new document"
    );
    assert!(row_readers(&batch, "c").is_empty(), "no state: no readers");
    assert!(readable("a") && readable("d") && !readable("c"));

    // An existing document: a content edit keeps the applied readers. A
    // reader-carrying edit still narrows and never widens.
    let mut edit = silent("a", "r2", "fixture document 1");
    edit["expected_prior_revision"] = json!("r1");
    let mut narrowing = doc("d", "r2", "fixture document 4");
    narrowing["expected_prior_revision"] = json!("r1");
    narrowing["readers"] = json!(["grp:other"]);
    input(&root, json!({"epoch":2,"content":[[edit, narrowing]]}));
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let batch = rows(&worker).await;
    assert_eq!(
        row_readers(&batch, "a"),
        granted,
        "a content edit without a permission statement keeps the readers"
    );
    assert!(
        row_readers(&batch, "d").is_empty(),
        "content that states readers narrows as before"
    );
    assert!(readable("a") && !readable("d") && !readable("c"));

    // The permission envelope of "c" arrives after its content.
    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"c","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    assert_eq!(row_readers(&rows(&worker).await, "c"), granted);
    assert!(readable("c") && readable("a"));
}

/// The readers of the plain and of the restricted section of the two-section
/// documents below.
fn section_readers(batch: &arrow_array::RecordBatch, id: &str) -> [BTreeSet<String>; 2] {
    let texts = batch
        .column_by_name("searchable_text")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let acl = batch
        .column_by_name("readers")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap();
    ["fixture document 5", "fixture document 6"].map(|text| {
        let row = (0..batch.num_rows())
            .find(|row| roots(batch)[*row] == id && texts.value(*row) == text)
            .unwrap();
        let readers = acl.value(row);
        let readers = readers.as_any().downcast_ref::<StringArray>().unwrap();
        readers
            .iter()
            .map(|v| v.unwrap().to_owned())
            .collect::<BTreeSet<_>>()
    })
}

/// A content statement that narrows the applied readers no longer stands for
/// the permission revision it narrowed: the same authoritative envelope
/// applies again, while an unchanged state still skips an equal revision.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_narrowing_content_releases_the_permission_revision_on_real_s3() {
    let (worker, commit, root) = fixture();
    let granting = json!([[{"id":"a","revision":"p1","readers":["grp:group"]}]]);
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1","fixture document 0")]],"permissions":granting}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    publish(&commit);
    let granted = row_readers(&rows(&worker).await, "a");
    assert_eq!(granted.len(), 1);
    // A newer content change states another reader: nobody is left.
    let mut narrowing = doc("a", "r2", "fixture document 1");
    narrowing["expected_prior_revision"] = json!("r1");
    narrowing["readers"] = json!(["grp:other"]);
    input(&root, json!({"epoch":2,"content":[[narrowing]]}));
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert!(row_readers(&rows(&worker).await, "a").is_empty());
    // The source sends its permission state again, at the same revision.
    input(&root, json!({"epoch":3,"permissions":granting}));
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    assert_eq!(
        row_readers(&rows(&worker).await, "a"),
        granted,
        "the authoritative envelope applies again"
    );
    let published = publication(&worker).await.1.lance_version;
    input(&root, json!({"epoch":4,"permissions":granting}));
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    assert_eq!(
        publication(&worker).await.1.lance_version,
        published,
        "an equal revision on an unchanged state rewrites nothing"
    );
    assert_eq!(row_readers(&rows(&worker).await, "a"), granted);
}

/// Content that makes no permission statement, as the first commit of an
/// empty namespace, is published as the committer would write it: nothing of
/// the staging form, and no row readable before a permission state exists.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_silent_content_at_genesis_is_published_like_any_other_on_real_s3() {
    let (worker, commit, root) = fixture_host(768, false);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let token = |reader: &str| {
        fyrr_writer::apply::reader_token(&loaded.config, "incremental", reader).unwrap()
    };
    let (group, other) = (token("grp:group"), token("grp:other"));
    let content = |revision: &str| {
        json!({"id":"s","revision":revision,"readers":[],
            "texts":["fixture document 5","fixture document 6"],
            "section_readers":[null,["grp:other"]]})
    };
    input(&root, json!({"epoch":1,"content":[[content("r1")]]}));
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let batch = rows(&worker).await;
    let metadata = batch
        .column_by_name("metadata_json")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        metadata
            .iter()
            .all(|row| !row.unwrap().contains("no_permission_statement")),
        "the staging marker is not published"
    );
    assert_eq!(
        section_readers(&batch, "s"),
        [BTreeSet::new(), BTreeSet::new()],
        "no permission state: nobody reads any section"
    );
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"s","revision":"p1","readers":["grp:group","grp:other"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    let both = [
        BTreeSet::from([group.clone(), other.clone()]),
        BTreeSet::from([other.clone()]),
    ];
    assert_eq!(section_readers(&rows(&worker).await, "s"), both);
    let mut edit = content("r2");
    edit["expected_prior_revision"] = json!("r1");
    input(&root, json!({"epoch":3,"content":[[edit]]}));
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert_eq!(section_readers(&rows(&worker).await, "s"), both);
}

/// A section with its own readers stays restricted to them within the
/// document's readers, whichever of content and permissions arrives first.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_section_readers_survive_a_later_permission_state_on_real_s3() {
    let (worker, commit, root) = fixture();
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let token = |reader: &str| {
        fyrr_writer::apply::reader_token(&loaded.config, "incremental", reader).unwrap()
    };
    let (group, other) = (token("grp:group"), token("grp:other"));
    let set = |tokens: &[&String]| tokens.iter().map(|t| (*t).clone()).collect::<BTreeSet<_>>();
    let content = |id: &str, revision: &str, prior: Option<&str>| {
        let mut change = json!({"id":id,"revision":revision,"readers":[],
            "texts":["fixture document 5","fixture document 6"],
            "section_readers":[null,["grp:other"]]});
        if let Some(prior) = prior {
            change["expected_prior_revision"] = json!(prior);
        }
        change
    };
    let permission = |id: &str, revision: &str, readers: Value| json!({"id":id,"revision":revision,"readers":readers});
    let apply = |epoch: u64, kind: &str, value: Value| {
        let mut data = json!({"epoch":epoch});
        data[kind] = json!([[value]]);
        input(&root, data);
        worker_sync(&worker, &["incremental", "--kind", kind]);
        publish(&commit);
    };

    // Content first, while the document has no permission state.
    apply(1, "content", content("s", "r1", None));
    assert_eq!(
        section_readers(&rows(&worker).await, "s"),
        [set(&[]), set(&[])]
    );
    apply(
        2,
        "permissions",
        permission("s", "p1", json!(["grp:group", "grp:other"])),
    );
    let both = [set(&[&group, &other]), set(&[&other])];
    assert_eq!(
        section_readers(&rows(&worker).await, "s"),
        both,
        "the restricted section keeps its own readers"
    );
    // The document loses and regains the section's reader.
    apply(
        3,
        "permissions",
        permission("s", "p2", json!(["grp:group"])),
    );
    assert_eq!(
        section_readers(&rows(&worker).await, "s"),
        [set(&[&group]), set(&[])]
    );
    apply(
        4,
        "permissions",
        permission("s", "p3", json!(["grp:group", "grp:other"])),
    );
    assert_eq!(section_readers(&rows(&worker).await, "s"), both);
    // A content edit states nothing about permissions.
    apply(5, "content", content("s", "r2", Some("r1")));
    assert_eq!(section_readers(&rows(&worker).await, "s"), both);
    // Permissions first, then content.
    apply(
        6,
        "permissions",
        permission("t", "p1", json!(["grp:group", "grp:other"])),
    );
    apply(7, "content", content("t", "r1", None));
    assert_eq!(section_readers(&rows(&worker).await, "t"), both);
    // A restore keeps the current rights of every row.
    let rollback = host_command(&commit)
        .args(["writer", "rollback"])
        .output()
        .unwrap();
    assert!(
        rollback.status.success(),
        "{}",
        String::from_utf8_lossy(&rollback.stderr)
    );
    assert_eq!(section_readers(&rows(&worker).await, "s"), both);
}

/// A sync provider without a permission stream states readers only in
/// content: there an empty reader set means nobody, and content never widens
/// the applied state again. Only a provider with a permission
/// stream makes no statement that way.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_content_only_provider_empty_readers_narrow_to_nobody_on_real_s3() {
    let (worker, commit, root) = fixture();
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config).unwrap();
        assert!(text.contains("type='incremental-test'"));
        std::fs::write(
            config,
            text.replace("type='incremental-test'", "type='incremental-content-only'"),
        )
        .unwrap();
    }
    let step = |epoch: u64, revision: &str, prior: Option<&str>, readers: Option<Value>| {
        let mut change = doc("a", revision, "fixture document 0");
        if let Some(prior) = prior {
            change["expected_prior_revision"] = json!(prior);
        }
        if let Some(readers) = readers {
            change["readers"] = readers;
        }
        input(&root, json!({"epoch":epoch,"content":[[change]]}));
        worker_sync(&worker, &["incremental", "--kind", "content"]);
        publish(&commit);
    };
    step(1, "r1", None, None);
    assert_eq!(
        row_readers(&rows(&worker).await, "a").len(),
        1,
        "the reader stated in content"
    );
    step(2, "r2", Some("r1"), Some(json!([])));
    assert!(
        row_readers(&rows(&worker).await, "a").is_empty(),
        "empty content readers mean nobody"
    );
    step(3, "r3", Some("r2"), None);
    assert!(
        row_readers(&rows(&worker).await, "a").is_empty(),
        "content readers never widen the applied state"
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_genesis_count_verification_and_delete_authority_on_real_s3() {
    use futures::TryStreamExt;
    let (worker, commit, root) = fixture_host(768, false);
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1","fixture document 0")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let objects: Vec<_> = store
        .list(Some(&ns.clone().join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    for object in objects {
        let mut envelope: PreparedEnvelope =
            fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                .await
                .unwrap();
        if envelope.source_id == "incremental" {
            let EnvelopePayload::Content(payload) = &mut envelope.payload else {
                continue;
            };
            payload.documents[0].records += 1;
            store
                .put(
                    &object.location,
                    fyrr_core::write_contract(&envelope).into(),
                )
                .await
                .unwrap();
        }
    }
    let rejected = publish(&commit);
    assert!(rejected["publication_id"].is_null());
    assert!(
        fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap()
            .content
            .is_none()
    );
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r1","fixture document 0"),{"id":"ghost","delete":true,"revision":"r0"}]]}),
    );
    worker_sync(
        &worker,
        &[
            "allowed",
            "denied",
            "incremental",
            "--kind",
            "content",
            "--full",
        ],
    );
    publish(&commit);
    assert_eq!(roots(&rows(&worker).await), ["a"]);
    input(
        &root,
        json!({"epoch":3,"content":[[doc("ghost","r1","fixture document 1")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let (path, mut stale) = envelopes(&worker)
        .await
        .into_iter()
        .filter(|(_, e)| e.source_id == "incremental" && e.kind == SyncKind::Content)
        .max_by_key(|(_, e)| e.sequence)
        .unwrap();
    store.delete(&path).await.unwrap();
    stale.sequence = 1;
    stale.rejection_generation += 1;
    stale.base_head = None;
    stale.cursor.start = None;
    stale.envelope_id = fyrr_core::envelope::envelope_id(
        &stale.namespace,
        &stale.source_id,
        stale.kind,
        stale.sequence,
        stale.rejection_generation,
        None,
    );
    store
        .put(
            &ns.join("envelopes")
                .join(format!("{}.json", stale.envelope_id)),
            fyrr_core::write_contract(&stale).into(),
        )
        .await
        .unwrap();
    publish(&commit);
    assert_eq!(
        roots(&rows(&worker).await),
        ["a"],
        "genesis deletion must fence older prepared content"
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_delta_genesis_undeclared_row_rejected_on_real_s3() {
    let (worker, commit, root) = fixture_host(768, false);
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let paths = fyrr_writer::commit::envelope_paths(&loaded.config, &head)
        .await
        .unwrap()
        .0;
    for path in paths {
        let mut envelope: PreparedEnvelope =
            fyrr_reader::storage::read_contract(store.as_ref(), &path)
                .await
                .unwrap();
        if envelope.source_id != "incremental" {
            continue;
        }
        let EnvelopePayload::Content(payload) = &mut envelope.payload else {
            continue;
        };
        assert_eq!(payload.documents.len(), 2);
        payload.documents.retain(|d| d.root_id == "a");
        store
            .put(&path, write_contract(&envelope).into())
            .await
            .unwrap();
    }
    let _ = ns;
    let result = publish(&commit);
    assert!(result["publication_id"].is_null());
    let after = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.content, after.content, "no staged row becomes visible");
    assert!(!after.rejected_envelope_ids.is_empty());
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_delta_cleanup_preserves_unconsumed_rejection_on_real_s3() {
    let (worker, commit, root) = fixture();
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r2","fixture document 2")],[doc("b","r2","fixture document 3")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, _) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let (path, mut victim) = envelopes(&worker)
        .await
        .into_iter()
        .find(|(_, e)| e.source_id == "incremental" && e.sequence == 2)
        .unwrap();
    victim.created_at = chrono::Utc::now() - chrono::Duration::days(8);
    store
        .put(&path, fyrr_core::write_contract(&victim).into())
        .await
        .unwrap();
    let EnvelopePayload::Content(p) = &victim.payload else {
        unreachable!()
    };
    let uri = object_store::path::Path::from(
        fyrr_reader::storage::dataset_uri(&loaded.config)
            .splitn(4, '/')
            .nth(3)
            .unwrap(),
    );
    let files: Vec<_> = p
        .fragments
        .iter()
        .flat_map(|f| &f.files)
        .map(|f| object_store::path::Path::from(format!("{uri}/{}", f.path)))
        .collect();
    publish(&commit);
    assert!(
        fyrr_writer::commit::published_ids(&loaded.config, &publication(&worker).await.0)
            .await
            .unwrap()
            .1
            .contains(&victim.envelope_id)
    );
    for consume in [false, true] {
        if consume {
            input(
                &root,
                json!({"epoch":3,"content":[[doc("a","r2","fixture document 2")]]}),
            );
            worker_sync(&worker, &["incremental", "--kind", "content"]);
            // A durable refetch alone cannot release the rejected input.
            let output = host_command(&worker)
                .args(["writer", "maintain"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(store.head(&path).await.is_ok());
            publish(&commit);
        }
        let output = host_command(&worker)
            .args(["writer", "maintain"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(store.head(&path).await.is_ok(), !consume);
        // This block's cleanup never deletes native fragments; block 3 extends that proof.
        for file in &files {
            assert!(store.head(file).await.is_ok(), "{file}");
        }
    }
    let (proof_path, mut proof) = envelopes(&worker)
        .await
        .into_iter()
        .filter(|(_, e)| e.source_id == "incremental" && e.kind == SyncKind::Content)
        .max_by_key(|(_, e)| e.sequence)
        .unwrap();
    proof.created_at = chrono::Utc::now() - chrono::Duration::days(8);
    store
        .put(&proof_path, fyrr_core::write_contract(&proof).into())
        .await
        .unwrap();
    // A late fenced writer can leave an immutable marker without a publishing CAS.
    // Its physical existence cannot invalidate this applied consumption proof.
    fyrr_writer::apply::reject(&loaded.config, &proof, "changed_rows_invalid")
        .await
        .unwrap();
    input(
        &root,
        json!({"epoch":4,"content":[[doc("b","r3","fixture document 4")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let output = host_command(&worker)
        .args(["writer", "maintain"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        store.head(&proof_path).await.is_ok(),
        "the latest applied complete read remains pinned beside a newer pending read"
    );
    assert!(
        fyrr_writer::commit::unconsumed_rejections(
            &loaded.config,
            &publication(&worker).await.0,
            &[]
        )
        .await
        .unwrap()
        .is_empty()
    );
    publish(&commit);
}

async fn rejected_authorization(kind: SyncKind) {
    let (worker, commit, root) = fixture();
    initial(&root);
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
    let mut late = envelopes(&worker)
        .await
        .into_iter()
        .map(|(_, e)| e)
        .filter(|e| {
            e.source_id == "incremental"
                && e.kind == kind
                && !matches!(&e.payload,
            EnvelopePayload::Permissions(p) if p.complete_read)
        })
        .min_by_key(|e| e.sequence)
        .unwrap();
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    input(
        &root,
        if kind == SyncKind::Permissions {
            json!({"epoch":3,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]})
        } else {
            json!({"epoch":3,"members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"child","members":[]},{"id":"group","members":["grp:child"]}]}]})
        },
    );
    worker_sync(&worker, &["incremental", "--kind", kind.as_str()]);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let mut rejected_id = String::new();
    for (path, mut envelope) in envelopes(&worker).await {
        if (kind == SyncKind::Permissions
            && matches!(&envelope.payload, EnvelopePayload::Permissions(p) if p.documents.iter().any(|d| d.permission_revision == "p2")))
            || (kind == SyncKind::Members
                && envelope.kind == kind
                && envelope.sequence > 2
                && matches!(&envelope.payload, EnvelopePayload::Members(p) if !p.complete_read))
        {
            rejected_id = envelope.envelope_id.clone();
            envelope.created_at = chrono::Utc::now() - chrono::Duration::days(4);
            store
                .put(&path, fyrr_core::write_contract(&envelope).into())
                .await
                .unwrap();
        }
    }
    assert!(!rejected_id.is_empty());
    input(&root, json!({"epoch":4}));
    worker_sync(
        &worker,
        &["incremental", "--kind", "permissions", "--kind", "members"],
    );
    // Time compression: expire only this source's head freshness, keeping identity/directory live.
    let mut before = publication(&worker).await.0;
    let old = before
        .freshness
        .sources
        .iter_mut()
        .find(|f| f.source_id == "incremental")
        .unwrap();
    old.permissions_read_at = chrono::Utc::now() - chrono::Duration::hours(2);
    old.members_read_at = Some(old.permissions_read_at);
    let old = old.clone();
    store
        .put(
            &ns.clone().join("head.json"),
            fyrr_core::write_contract(&before).into(),
        )
        .await
        .unwrap();
    publish(&commit);
    let (head, _) = publication(&worker).await;
    assert_eq!(
        head.freshness
            .sources
            .iter()
            .find(|f| f.source_id == "incremental")
            .unwrap(),
        &old
    );
    assert!(
        fyrr_writer::commit::published_ids(&loaded.config, &head)
            .await
            .unwrap()
            .1
            .contains(&rejected_id)
    );
    let batch = rows(&worker).await;
    assert!(
        fyrr_writer::apply::strings(&batch, "source_revision")
            .unwrap()
            .iter()
            .all(|r| r == Some("r1"))
    );
    let mut client = super::tools::Mcp::open(&worker);
    let denied = client.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert_eq!(
        denied["structuredContent"]["documents"],
        json!([]),
        "{denied}"
    );
    // Re-fetch both kinds from the rewound generation, then separate partial and complete reads.
    initial(&root);
    let mut repaired: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    repaired["epoch"] = json!(5);
    if kind == SyncKind::Permissions {
        repaired["permissions"][0][0]["revision"] = json!("p2");
        repaired["permissions"][0][0]["readers"] = json!([]);
    } else {
        repaired["members"][0]["groups"][0]["members"] = json!([]);
    }
    let before_repair: BTreeSet<_> = envelopes(&worker)
        .await
        .into_iter()
        .map(|(_, e)| e.envelope_id)
        .collect();
    input(&root, repaired);
    worker_sync(
        &worker,
        &["incremental", "--kind", "permissions", "--kind", "members"],
    );
    // Keep both terminal attestations for the original completion control.
    // Only the rejected kind has a marker that this partial read could consume.
    let marker: fyrr_core::envelope::RejectionMarker = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &ns.clone()
            .join("rejections")
            .join(format!("{rejected_id}.json")),
    )
    .await
    .unwrap();
    let mut terminals = Vec::new();
    let mut partial_ids = Vec::new();
    let content_id = envelopes(&worker)
        .await
        .into_iter()
        .map(|(_, envelope)| envelope)
        .filter(|envelope| {
            envelope.source_id == "incremental" && envelope.kind == SyncKind::Content
        })
        .max_by_key(|envelope| envelope.sequence)
        .unwrap()
        .envelope_id;
    for (path, envelope) in envelopes(&worker).await {
        if envelope.source_id != marker.source_id || before_repair.contains(&envelope.envelope_id) {
            continue;
        }
        if envelope.kind == kind {
            assert!(
                envelope.rejection_generation > marker.rejection_generation,
                "{kind:?}: replacement generation"
            );
            assert!(
                envelope.restore_sequence >= marker.restore_sequence,
                "{kind:?}: replacement restore sequence"
            );
        }
        let complete = match &envelope.payload {
            EnvelopePayload::Permissions(payload) => payload.complete_read,
            EnvelopePayload::Members(payload) => payload.complete_read,
            _ => unreachable!(),
        };
        if complete {
            let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
            terminals.push((path.clone(), bytes));
            store.delete(&path).await.unwrap();
        } else if envelope.kind == kind {
            partial_ids.push(envelope.envelope_id);
        }
    }
    assert!(!terminals.is_empty(), "{kind:?}: terminal control");
    assert!(!partial_ids.is_empty(), "{kind:?}: partial replacement");
    assert!(envelopes(&worker).await.iter().all(|(_, envelope)| {
        envelope.source_id != marker.source_id
            || envelope.kind != kind
            || envelope.rejection_generation <= marker.rejection_generation
            || match &envelope.payload {
                EnvelopePayload::Permissions(payload) => !payload.complete_read,
                EnvelopePayload::Members(payload) => !payload.complete_read,
                _ => false,
            }
    }));
    publish(&commit);
    let applied = fyrr_writer::commit::published_ids(&loaded.config, &publication(&worker).await.0)
        .await
        .unwrap()
        .0;
    assert!(
        partial_ids.iter().all(|id| applied.contains(id)),
        "{kind:?}: partial replacement must be durably applied"
    );
    // A later drain must still refuse the affected content, while publishing another source.
    std::fs::write(root.join("sources/0.txt"), "fixture document 3").unwrap();
    worker_sync(&worker, &["allowed", "--kind", "content"]);
    publish(&commit);
    let receipt = host_command(&commit)
        .args(["writer", "commit", "--receipt", &content_id])
        .output()
        .unwrap();
    assert!(
        !receipt.status.success(),
        "{kind:?}: partial_replacement published affected content"
    );
    assert!(
        String::from_utf8_lossy(&receipt.stderr).contains("receipt_unpublished"),
        "{kind:?}: partial_replacement receipt"
    );
    let batch = rows(&worker).await;
    let row = roots(&batch).iter().position(|id| id == "a").unwrap();
    assert_eq!(
        fyrr_writer::apply::strings(&batch, "source_revision")
            .unwrap()
            .value(row),
        "r1",
        "{kind:?}: partial_replacement"
    );
    let partial_head = publication(&worker).await.0;
    assert_eq!(
        partial_head
            .freshness
            .sources
            .iter()
            .find(|f| f.source_id == "incremental")
            .unwrap(),
        &old,
        "{kind:?}: partial_replacement freshness"
    );
    let status = super::coordination::command(&commit, &["writer", "status"]);
    assert_eq!(
        status["freshness"],
        serde_json::to_value(&partial_head.freshness).unwrap()
    );
    let control = source_rows(&worker, "allowed").await;
    let row = roots(&control).iter().position(|id| id == "0.txt").unwrap();
    assert_eq!(
        fyrr_writer::apply::strings(&control, "searchable_text")
            .unwrap()
            .value(row),
        "fixture document 3"
    );
    for (path, bytes) in terminals {
        store.put(&path, bytes.into()).await.unwrap();
    }
    std::fs::write(root.join("sources/0.txt"), "fixture document 4").unwrap();
    worker_sync(&worker, &["allowed", "--kind", "content"]);
    publish(&commit);
    let receipt =
        super::coordination::command(&commit, &["writer", "commit", "--receipt", &content_id]);
    assert!(
        receipt.to_string().contains(&content_id),
        "{kind:?}: complete replacement receipt"
    );
    let control = source_rows(&worker, "allowed").await;
    let row = roots(&control).iter().position(|id| id == "0.txt").unwrap();
    assert_eq!(
        fyrr_writer::apply::strings(&control, "searchable_text")
            .unwrap()
            .value(row),
        "fixture document 4"
    );
    let recovered = publication(&worker).await.0;
    let status = super::coordination::command(&commit, &["writer", "status"]);
    assert_eq!(
        status["freshness"],
        serde_json::to_value(&recovered.freshness).unwrap()
    );

    let fresh = recovered
        .freshness
        .sources
        .iter()
        .find(|f| f.source_id == "incremental")
        .unwrap();
    assert!(fresh.permissions_read_at > old.permissions_read_at);
    assert!(fresh.members_read_at > old.members_read_at);
    let batch = rows(&worker).await;
    let row = roots(&batch).iter().position(|id| id == "a").unwrap();
    assert_eq!(
        fyrr_writer::apply::strings(&batch, "source_revision")
            .unwrap()
            .value(row),
        "r2"
    );
    if kind == SyncKind::Permissions {
        assert!(fyrr_writer::apply::tokens(&batch, row).unwrap().is_empty());
    }
    let denied = super::tools::Mcp::open(&worker)
        .call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert_eq!(
        denied["structuredContent"]["metadata"]["hidden_sources"],
        json!([]),
        "access is the applied revoke, not stale-source hiding"
    );
    assert_eq!(
        denied["structuredContent"]["documents"],
        json!([]),
        "{denied}"
    );
    if kind == SyncKind::Permissions {
        let allowed = super::tools::Mcp::open(&worker)
            .call("fetch", json!({"source_id":"incremental","root_id":"b"}));
        assert_eq!(
            allowed["structuredContent"]["documents"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    // A holder prepared under the rejected generation may finish after recovery.
    // Its old grant must not reapply, even though the rejection is now consumed.
    late.sequence = envelopes(&worker)
        .await
        .into_iter()
        .map(|(_, e)| e)
        .filter(|e| e.source_id == "incremental" && e.kind == kind)
        .map(|e| e.sequence)
        .max()
        .unwrap()
        + 1;
    late.created_at = chrono::Utc::now();
    late.envelope_id = fyrr_core::envelope::envelope_id(
        &late.namespace,
        &late.source_id,
        late.kind,
        late.sequence,
        late.rejection_generation,
        late.cursor.start.as_deref(),
    );
    store
        .put(
            &ns.clone()
                .join("envelopes")
                .join(format!("{}.json", late.envelope_id)),
            fyrr_core::write_contract(&late).into(),
        )
        .await
        .unwrap();
    publish(&commit);
    assert!(
        fyrr_writer::commit::published_ids(&loaded.config, &publication(&worker).await.0)
            .await
            .unwrap()
            .1
            .contains(&late.envelope_id)
    );
    let denied = super::tools::Mcp::open(&worker)
        .call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert_eq!(
        denied["structuredContent"]["metadata"]["hidden_sources"],
        json!([])
    );
    assert_eq!(denied["structuredContent"]["documents"], json!([]));
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_members_alone_never_attest_permissions_on_real_s3() {
    let (worker, commit, root) = fixture();
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert_eq!(roots(&rows(&worker).await), ["a", "b"]);
    assert!(
        publication(&worker)
            .await
            .0
            .freshness
            .sources
            .iter()
            .all(|f| f.source_id != "incremental")
    );

    worker_sync(&worker, &["incremental", "--kind", "members"]);
    let pending = envelopes(&worker).await;
    assert!(!pending.is_empty());
    assert!(pending.iter().any(|(_, e)| matches!(
        &e.payload,
        EnvelopePayload::Members(p) if p.complete_read
    )));
    assert!(pending.iter().all(|(_, e)| e.kind != SyncKind::Permissions));
    publish(&commit);
    let head = publication(&worker).await.0;
    let freshness = head
        .freshness
        .sources
        .iter()
        .find(|f| f.source_id == "incremental")
        .unwrap();
    assert_eq!(freshness.permissions_read_at, chrono::DateTime::UNIX_EPOCH);
    assert!(freshness.members_read_at.is_some());
    let hidden = success(
        &worker,
        &["reader", "search", "fixture query", "--limit", "75"],
    );
    assert_eq!(hidden["metadata"]["hidden_sources"], json!(["incremental"]));
    assert!(
        hidden["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|h| h["source_id"] != "incremental")
    );
    let listing = super::tools::Mcp::open(&worker).call(
        "search",
        json!({"sources":["incremental"],"order":"updated_at","limit":75}),
    );
    assert_eq!(listing["isError"], false);
    assert_eq!(listing["structuredContent"]["hits"], json!([]));
    assert_eq!(
        listing["structuredContent"]["metadata"]["hidden_sources"],
        json!(["incremental"])
    );

    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    let head = publication(&worker).await.0;
    assert!(
        head.freshness
            .sources
            .iter()
            .any(|f| f.source_id == "incremental"
                && f.permissions_read_at > chrono::DateTime::UNIX_EPOCH
                && f.members_read_at == freshness.members_read_at)
    );
    let admitted = super::tools::Mcp::open(&worker).call(
        "search",
        json!({"sources":["incremental"],"order":"updated_at","limit":75}),
    );
    assert_eq!(admitted["isError"], false);
    assert_eq!(
        admitted["structuredContent"]["metadata"]["hidden_sources"],
        json!([])
    );
    assert_eq!(
        admitted["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(
        admitted["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|h| h["source_id"] == "incremental")
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_delta_expired_source_isolated_on_real_s3() {
    let (worker, commit, root) = fixture();
    initial(&root);
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
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let (mut head, _) = publication(&worker).await;
    let mut identity: fyrr_core::identity::IdentityArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{ns}/{}", head.identity.object)),
    )
    .await
    .unwrap();
    let fixed_token =
        fyrr_reader::storage::hash_principal(&loaded.config, "tok", "grp:allow").unwrap();
    let fixed_id = identity
        .tokens
        .iter()
        .position(|t| t == &fixed_token)
        .unwrap() as u32;
    identity.principals.push(fyrr_core::identity::Principal {
        groups: Vec::new(),
        key: fyrr_reader::storage::hash_principal(&loaded.config, "key", "fixed-only@example.test")
            .unwrap(),
        tokens: vec![fixed_id],
    });
    identity.principals.sort_by(|a, b| a.key.cmp(&b.key));
    identity.generation += 1;
    identity.built_at = chrono::Utc::now();
    let hash = digest(&fyrr_core::write_contract(&identity));
    let object = format!("identity/{}-{}.json", identity.generation, &hash[7..]);
    fyrr_writer::prepare::put_immutable(
        store.as_ref(),
        &object_store::path::Path::from(format!("{ns}/{object}")),
        &identity,
    )
    .await
    .unwrap();
    head.identity = fyrr_core::publication::GenerationPointer {
        generation: identity.generation,
        object,
        digest: hash,
    };
    store
        .put(
            &ns.clone().join("head.json"),
            fyrr_core::write_contract(&head).into(),
        )
        .await
        .unwrap();
    let denied_config = worker.parent().unwrap().join("fixed-only.toml");
    std::fs::write(
        &denied_config,
        std::fs::read_to_string(&worker)
            .unwrap()
            .replace("fixture@example.test", "fixed-only@example.test"),
    )
    .unwrap();
    let mut fixed_only = super::tools::Mcp::open(&denied_config);
    let mut client = super::tools::Mcp::open(&worker);
    let initial = client.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert_eq!(
        initial["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    for kind in [SyncKind::Permissions, SyncKind::Members] {
        let mut head = publication(&worker).await.0;
        let fresh = head
            .freshness
            .sources
            .iter_mut()
            .find(|f| f.source_id == "incremental")
            .unwrap();
        fresh.permissions_read_at = chrono::Utc::now();
        fresh.members_read_at = Some(chrono::Utc::now());
        let expired = chrono::Utc::now() - chrono::Duration::hours(2);
        match kind {
            SyncKind::Permissions => fresh.permissions_read_at = expired,
            _ => fresh.members_read_at = Some(expired),
        }
        store
            .put(
                &ns.clone().join("head.json"),
                fyrr_core::write_contract(&head).into(),
            )
            .await
            .unwrap();
        let out = host_command(&worker)
            .args(["reader", "search", "fixture query", "--limit", "2"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let search: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(search["metadata"]["hidden_sources"], json!(["incremental"]));
        assert!(
            search["hits"]
                .as_array()
                .unwrap()
                .iter()
                .all(|h| h["source_id"] == "allowed")
        );
        assert_eq!(search["hits"].as_array().unwrap().len(), 2);
        for (tool, args) in [
            (
                "search",
                json!({"sources":["incremental"],"order":"updated_at"}),
            ),
            ("fetch", json!({"source_id":"incremental","root_id":"a"})),
            ("fetch", json!({"uri":"test://a"})),
        ] {
            let result = client.call(tool, args);
            assert!(!result["isError"].as_bool().unwrap_or(false), "{result}");
            let body = &result["structuredContent"];
            assert_eq!(body["metadata"]["hidden_sources"], json!(["incremental"]));
            let key = if tool == "search" {
                "hits"
            } else {
                "documents"
            };
            assert_eq!(body[key], json!([]), "{result}");
        }
        let listing = client.call(
            "search",
            json!({"sources":["allowed"],"limit":75,"order":"updated_at"}),
        );
        let body = &listing["structuredContent"];
        assert_eq!(body["hits"].as_array().unwrap().len(), 75);
        assert!(
            body["hits"]
                .as_array()
                .unwrap()
                .iter()
                .all(|h| h["source_id"] == "allowed")
        );
        let counts = client.call("list_filters", json!({}));
        assert_eq!(
            counts["structuredContent"]["source_types"]["values"],
            json!([{ "value":"local-files", "count":768 }])
        );
        assert_eq!(
            counts["structuredContent"]["metadata"]["hidden_sources"],
            json!(["incremental"])
        );
        for (tool, args) in [
            ("search", json!({"query":"fixture query","limit":2})),
            (
                "search",
                json!({"sources":["allowed"],"order":"updated_at","limit":75}),
            ),
            ("fetch", json!({"source_id":"allowed","root_id":"0.txt"})),
            ("fetch", json!({"source_id":"incremental","root_id":"a"})),
            ("list_filters", json!({})),
        ] {
            let result = fixed_only.call(tool, args);
            assert!(!result["isError"].as_bool().unwrap_or(false), "{result}");
            let body = &result["structuredContent"];
            assert_eq!(body["metadata"]["hidden_sources"], json!([]), "{result}");
            if tool == "search" {
                assert!(!body["hits"].as_array().unwrap().is_empty());
            }
            if tool == "list_filters" {
                assert_eq!(
                    body["source_types"]["values"],
                    json!([{ "value":"local-files", "count":768 }])
                );
            }
        }
    }
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn incremental_opt_in_fts_merge_publishes_one_segment_on_real_s3() {
    use lance::index::DatasetIndexExt;
    async fn segments(config: &Path) -> usize {
        let loaded = fyrr_core::config::load(config).unwrap();
        let (head, _) = publication(config).await;
        let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
            .await
            .unwrap();
        dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .filter(|i| i.name == "fts_idx")
            .count()
    }
    let (config, root) = setup();
    success(&config, &["writer", "namespace", "create"]);
    assert!(run(&config, &["writer", "sync"]).status.success());
    success(&config, &["writer", "commit"]);
    let initial = segments(&config).await;
    std::fs::write(root.join("sources/extra-a.txt"), "fixture document 0").unwrap();
    assert!(run(&config, &["writer", "sync"]).status.success());
    success(&config, &["writer", "commit"]);
    assert!(
        segments(&config).await > initial,
        "default preserves lexical delta segments"
    );
    let merged = root.join("merged.toml");
    std::fs::write(
        &merged,
        format!(
            "{}\n[writer.commit]\nmerge_fts_segments=true\n",
            std::fs::read_to_string(&config).unwrap()
        ),
    )
    .unwrap();
    // Enabling the step after an already published incremental commit must
    // close existing debt even when there are no new envelopes.
    let repaired = success(&merged, &["writer", "commit"]);
    assert_eq!(segments(&merged).await, 1);
    assert_eq!(
        success(&merged, &["writer", "commit"])["lance_version"],
        repaired["lance_version"]
    );
    std::fs::write(root.join("sources/extra-b.txt"), "fixture document 0").unwrap();
    assert!(run(&merged, &["writer", "sync"]).status.success());
    let receipt = success(&merged, &["writer", "commit"]);
    assert_eq!(segments(&merged).await, 1);
    let hits = success(&merged, &["reader", "search", "fixture query"]);
    assert!(!hits["hits"].as_array().unwrap().is_empty());
    assert!(
        hits["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|h| h["source_id"] == "allowed")
    );
    assert_eq!(
        success(&merged, &["writer", "commit"])["lance_version"],
        receipt["lance_version"]
    );
    success(&merged, &["reader", "check"]);
}

/// Catching a failed host revision callback cannot attest an incomplete read.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_caught_revision_failure_never_attests_on_real_s3() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let mut outcomes = Vec::new();
    for kind in [SyncKind::Permissions, SyncKind::Members] {
        for callback in ["lookup", "next"] {
            let (worker, commit, root) = fixture();
            initial(&root);
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
            let before = publication(&worker)
                .await
                .0
                .freshness
                .sources
                .into_iter()
                .find(|f| f.source_id == "incremental")
                .unwrap();
            input(
                &root,
                json!({"epoch":2,"content":[[doc("a","r2","fixture document 2")]]}),
            );
            worker_sync(&worker, &["incremental", "--kind", "content"]);
            let staged = envelopes(&worker).await.len();
            input(&root, json!({"epoch":3,"catch_revision_failure":callback}));
            let marker = root.join("revision-callback");
            let marker_hook = marker.clone();
            let faults = Arc::new(AtomicUsize::new(0));
            let hook_faults = faults.clone();
            // Arm only when the provider calls the revision view, after admission
            // and recovery reads. The pending content envelope is part of that view.
            let hook: super::corrections::BoundaryHook = Arc::new(move |request, _, after| {
                if !after
                    && request.starts_with("GET ")
                    && request.contains("/envelopes/")
                    && marker_hook.exists()
                {
                    hook_faults.fetch_add(1, Ordering::SeqCst);
                    true
                } else {
                    false
                }
            });
            let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
            let (proxy, guard) = super::corrections::proxy_at_boundary(
                endpoint.clone(),
                Arc::new(AtomicBool::new(false)),
                hook,
            );
            let proxied = root.join("callback-fault.toml");
            std::fs::write(
                &proxied,
                std::fs::read_to_string(&worker)
                    .unwrap()
                    .replace(&endpoint, &proxy),
            )
            .unwrap();
            let output = super::coordination::worker_command(&proxied)
                .args(["writer", "sync", "incremental", "--kind", kind.as_str()])
                .output()
                .unwrap();
            drop(guard);
            assert!(
                faults.load(Ordering::SeqCst) > 0,
                "fault was not exercised: {kind:?}/{callback}"
            );
            assert_eq!(std::fs::read_to_string(&marker).unwrap(), "caught");
            let report: Value = serde_json::from_slice(&output.stdout).unwrap();
            let no_attestation = envelopes(&worker).await.len() == staged;
            publish(&commit);
            let after = publication(&worker)
                .await
                .0
                .freshness
                .sources
                .into_iter()
                .find(|f| f.source_id == "incremental")
                .unwrap();
            outcomes.push(json!({"kind":kind,"callback":callback,"failed":!output.status.success(),"error":report["kinds"][0]["error"],"no_attestation":no_attestation,"freshness_unchanged":after == before}));
            // The failed view must not poison a later complete read.
            input(&root, json!({"epoch":4}));
            worker_sync(&worker, &["incremental", "--kind", kind.as_str()]);
            assert!(
                envelopes(&worker)
                    .await
                    .iter()
                    .any(|(_, e)| match &e.payload {
                        EnvelopePayload::Permissions(p) =>
                            kind == SyncKind::Permissions && p.complete_read,
                        EnvelopePayload::Members(p) => kind == SyncKind::Members && p.complete_read,
                        _ => false,
                    })
            );
            publish(&commit);
            let good = publication(&worker)
                .await
                .0
                .freshness
                .sources
                .into_iter()
                .find(|f| f.source_id == "incremental")
                .unwrap();
            if kind == SyncKind::Permissions {
                assert!(good.permissions_read_at > after.permissions_read_at);
            } else {
                assert!(good.members_read_at > after.members_read_at);
            }
        }
    }
    for outcome in outcomes {
        assert_eq!(outcome["failed"], true, "{outcome}");
        assert_eq!(outcome["error"], "storage_unavailable", "{outcome}");
        assert_eq!(outcome["no_attestation"], true, "{outcome}");
        assert_eq!(outcome["freshness_unchanged"], true, "{outcome}");
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_rejected_authorization_requires_complete_replacement_on_real_s3() {
    for kind in [SyncKind::Permissions, SyncKind::Members] {
        rejected_authorization(kind).await;
    }
}

#[tokio::test]
#[ignore = "real CLI, provider entry marker and RustFS; scripts/ci"]
async fn incremental_invalid_settings_stop_all_sources_before_provider_calls_on_real_s3() {
    let (worker, _, root) = fixture();
    input(&root, json!({"epoch":1,"capture_cursors":true}));
    let marker = root.join("content-cursor.json");
    let original = std::fs::read_to_string(&worker).unwrap();
    std::fs::write(&worker, format!("{original}\n[[sources]]\nid='invalid-settings'\ntype='incremental-test'\npermissions='sync'\n[sources.intervals]\npermissions='5m'\n[sources.settings]\nunexpected=true\n[sources.scope]\nfixture='{}'\n", root.join("input.json").display())).unwrap();
    let before = envelopes(&worker).await.len();
    for dry_run in [false, true] {
        let mut command = super::coordination::worker_command(&worker);
        command.args(["writer", "sync", "--kind", "content", "--jobs", "1"]);
        if dry_run {
            command.arg("--dry-run");
        }
        let output = command.output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(78), "{dry_run}: {stderr}");
        assert!(
            stderr.contains("sources[3].settings") && stderr.contains("nothing ran"),
            "{stderr}"
        );
        assert!(output.stdout.is_empty());
        assert!(
            !marker.exists(),
            "a valid earlier provider ran before validation finished"
        );
        assert_eq!(envelopes(&worker).await.len(), before);
    }
    std::fs::write(&worker, original).unwrap();
    worker_sync(&worker, &["incremental", "--kind", "content", "--full"]);
    assert!(
        marker.exists(),
        "the provider-entry probe must run with valid settings"
    );
}

#[test]
#[ignore = "real S3 revision view request counts; scripts/ci"]
fn pending_revision_view_has_linear_storage_reads_on_real_s3() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let (config, root, _) = super::tools::fixture();
    let marker = root.join("revision-counting");
    let watching = marker.clone();
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let hook: super::corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if !after
            && request.starts_with("GET ")
            && request.contains("/envelopes/")
            && watching.exists()
        {
            observed.fetch_add(1, Ordering::SeqCst);
        }
        false
    });
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, _guard) = super::corrections::proxy_at_boundary(
        endpoint.clone(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let proxied = root.join("revision-probe.toml");
    std::fs::write(
        &proxied,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let output = Command::new(std::env::var_os("FYRR_SCIM_HEALTH_TEST_BINARY").unwrap())
        .args(["python::prepared::revisions::tests::pending_revision_view_reads_each_envelope_once_on_real_s3", "--exact", "--ignored", "--nocapture"])
        .env("FYRR_TEST_REVISION_CONFIG", &proxied)
        .env("FYRR_TEST_REVISION_MARKER", &marker)
        .output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    let actual = reads.load(Ordering::SeqCst);
    if let Some(path) = std::env::var_os("FYRR_TEST_REVISION_RESULT") {
        std::fs::write(path, serde_json::to_vec(&json!({"pending_envelopes":4,"observed_GETs":actual,"semantic_and_overflow_checks":"passed"})).unwrap()).unwrap();
    }
    assert!(actual > 0, "real envelope GETs must be observed");
    assert!(
        actual <= 8,
        "four pending envelopes must use at most two linear passes, observed {actual} GETs"
    );
}
