//! Incremental contracts through real executable processes and the owned RustFS.
use super::coordination::{
    command, committer_host, host_command, publication, worker_command, worker_sync,
};
use super::*;
use arrow_array::{Array, StringArray};
use fyrr_core::connector::SyncKind;
use fyrr_core::envelope::{EnvelopePayload, PreparedEnvelope};
use object_store::ObjectStoreExt;
use std::collections::BTreeSet;

#[tokio::test]
#[ignore = "real worker and local storage; scripts/ci"]
async fn incremental_unchanged_upsert_still_protects_pending_reconciliation_on_storage() {
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

/// The documents of the connector fields cases: one with every field, one with
/// a team only.
fn field_documents(root: &Path) {
    input(
        root,
        json!({"epoch":1,
            "content":[[
                {"id":"a","revision":"r1","texts":["fixture document 0"],"title":"Alpha",
                    "team":"core","status":"open","labels":["bug","ui"],
                    "created_at":"2026-01-02T03:04:05.123456+00:00"},
                {"id":"b","revision":"r1","texts":["fixture document 1"],"team":"infra"},
            ]],
            "permissions":[[
                {"id":"a","revision":"p1","readers":["grp:group"]},
                {"id":"b","revision":"p1","readers":["grp:group"]},
            ]],
            "members":[{"users":[{"id":"u","email":"fixture@example.test",
                    "email_verification":"verified_corporate","active":true}],
                "groups":[{"id":"child","members":["usr:u"]},{"id":"group","members":["grp:child"]}]}]}),
    );
}

/// The tools return a document's fields, filtering by `filter`.
fn read_fields(worker: &Path, filter: Value) {
    let mut client = super::tools::Mcp::open(worker);
    let found = client.call(
        "search",
        json!({"order":"updated_at","output":"metadata","filter":filter}),
    );
    let hits = found["structuredContent"]["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1, "{found}");
    let hit = &hits[0];
    assert_eq!(hit["root_id"], "a");
    assert_eq!(hit["title"], "Alpha");
    assert_eq!(hit["team"], "core");
    assert_eq!(hit["status"], "open");
    assert_eq!(hit["labels"], json!(["bug", "ui"]));
    assert_eq!(hit["created_at"], "2026-01-02T03:04:05.123456Z");
    let fetched = client.call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    let document = &fetched["structuredContent"]["documents"][0];
    for field in ["title", "team", "status", "labels", "created_at"] {
        assert_eq!(document[field], hit[field], "{field}");
    }
    let other = client.call("fetch", json!({"source_id":"incremental","root_id":"b"}));
    let other = &other["structuredContent"]["documents"][0];
    assert!(
        other["title"].is_null() && other["created_at"].is_null(),
        "{other}"
    );
    assert_eq!(other["labels"], json!([]));
}

fn sync_fields(worker: &Path) {
    worker_sync(
        worker,
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
}

/// A connector document's title, team, status, labels and creation time are
/// typed columns of its namespace, not part of its stored metadata. The tools
/// filter by them and return them as before.
#[tokio::test]
#[ignore = "real worker and local storage; scripts/ci"]
async fn incremental_connector_fields_are_typed_columns_on_storage() {
    use arrow_array::{ListArray, TimestampMicrosecondArray};
    let setup = setup();
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (worker, commit, root) = fixture_host_at(8, true, "typed-fixture", setup);
    field_documents(&root);
    sync_fields(&worker);
    publish(&commit);

    // The stored rows hold the fields in their own columns and no longer in
    // their metadata.
    let batch = rows(&worker).await;
    let row_of = |root: &str| roots(&batch).iter().position(|r| r == root).unwrap();
    let text = |column: &str, root: &str| {
        let array = fyrr_writer::apply::strings(&batch, column).unwrap();
        let row = row_of(root);
        (!array.is_null(row)).then(|| array.value(row).to_owned())
    };
    assert_eq!(text("title", "a").as_deref(), Some("Alpha"));
    assert_eq!(text("team", "a").as_deref(), Some("core"));
    assert_eq!(text("status", "a").as_deref(), Some("open"));
    assert_eq!(text("title", "b"), None);
    assert_eq!(text("team", "b").as_deref(), Some("infra"));
    let labels = batch
        .column_by_name("labels")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let labels_of = |root: &str| {
        let items = labels.value(row_of(root));
        let items = items.as_any().downcast_ref::<StringArray>().unwrap();
        items
            .iter()
            .map(|v| v.unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(labels_of("a"), ["bug", "ui"]);
    assert!(labels_of("b").is_empty());
    let created = batch
        .column_by_name("created_at")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(created.value(row_of("a")), 1_767_323_045_123_456);
    assert!(created.is_null(row_of("b")));
    let metadata = fyrr_writer::apply::strings(&batch, "metadata_json").unwrap();
    assert!(
        !metadata.value(row_of("a")).contains("Alpha"),
        "the title is stored once, in its column"
    );
    let (_, manifest) = publication(&worker).await;
    assert_eq!(manifest.schema_version.to_string(), "6.0");

    read_fields(
        &worker,
        json!({"and":[
            {"field":"team","op":"eq","value":"core"},
            {"field":"labels","op":"contains","value":"bug"},
            {"field":"created_at","op":"gte","value":"2026-01-01T00:00:00Z"}]}),
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// The title of a connector document has a full-text index, so a query ranks
/// by it with `query_attributes`: the document whose title matches comes up on
/// the text lane, and the ones with another or no title do not.
#[tokio::test]
#[ignore = "real worker, HTTP reader and local storage; scripts/ci"]
async fn incremental_connector_title_has_a_full_text_index_on_storage() {
    let setup = setup();
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let table = "\n[[identity.api_keys]]\nname='title'\nkey={env='FYRR_TEST_TITLE_KEY'}\nreaders=['grp:group']\n";
    let text = std::fs::read_to_string(&setup.0).unwrap();
    std::fs::write(&setup.0, format!("{text}{table}")).unwrap();
    let (worker, commit, root) = fixture_host_at(8, false, "title-fixture", setup);
    let key = format!("fyrr_key_{:064}", 77);
    let key = key.as_str();
    field_documents(&root);
    sync_fields(&worker);
    // The keys are in the configuration of the commit that binds them.
    let committed = host_command(&commit)
        .args(["writer", "commit"])
        .env("FYRR_TEST_TITLE_KEY", key)
        .output()
        .unwrap();
    assert!(committed.status.success(), "{committed:?}");

    let (_, manifest) = publication(&worker).await;
    assert!(
        manifest
            .indexes
            .iter()
            .any(|index| index.name == "attrfts_title_idx" && index.column == "title"),
        "the title has a full-text index"
    );

    super::write_api::embed(&root, &["alpha"]);
    let keys = host_command(&commit)
        .args(["writer", "keys"])
        .env("FYRR_TEST_TITLE_KEY", key)
        .output()
        .unwrap();
    assert!(keys.status.success(), "{keys:?}");
    let mut reader = super::http_mcp::Reader::start_appended(&worker, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let response = reader
        .client
        .post(format!("{}/v1/namespaces/title-fixture/query", reader.base))
        .bearer_auth(key)
        .json(
            &json!({"query": "alpha", "output": "metadata", "consistency": "strong",
            "fields": {"scores": true},
            "query_attributes": [{"attribute": "title"}]}),
        )
        .send()
        .await
        .unwrap();
    let status = response.status();
    let answer: Value = response.json().await.unwrap_or(Value::Null);
    assert_eq!(status, 200, "{answer} {}", reader.stderr_text());
    let scored: Vec<(&str, f64)> = answer["hits"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|hit| {
            Some((
                hit["root_id"].as_str()?,
                hit.get("lexical_score")?.as_f64()?,
            ))
        })
        .collect();
    assert_eq!(scored.len(), 1, "{answer}");
    assert_eq!(scored[0].0, "a", "{answer}");
    reader.stop().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// A reader refuses a published namespace whose policy or manifest an earlier
/// fyrr wrote, by name: the policy 2.2 of v0.1.1, a current policy stamped 3.1
/// and a current manifest stamped 4.1.
#[tokio::test]
#[ignore = "real worker and local storage; scripts/ci"]
async fn incremental_reader_refuses_a_policy_of_an_earlier_fyrr_by_name_on_storage() {
    let setup = setup();
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (worker, _commit, root) = fixture_host_at(8, true, "earlier-fixture", setup);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let Some(fyrr_core::config::StorageBackend::Local { root: storage }) =
        loaded.config.storage.backend()
    else {
        panic!("local storage")
    };
    let namespace = storage.join("namespaces/earlier-fixture");
    let head_path = namespace.join("head.json");
    let mut head: Value = serde_json::from_slice(&std::fs::read(&head_path).unwrap()).unwrap();
    let policy_path = namespace.join(head["policy"]["object"].as_str().unwrap());
    let current = std::fs::read(&policy_path).unwrap();
    let mut plain: Value = serde_json::from_slice(&current).unwrap();
    plain["schema_version"] = "3.1".into();
    let v011 = std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/contracts/compat/v0.1.1-policy-artifact.json"),
    )
    .unwrap();
    // Overwritten in place: the store keeps the version tag the files carry.
    for bytes in [v011, serde_json::to_vec_pretty(&plain).unwrap()] {
        std::fs::write(&policy_path, &bytes).unwrap();
        head["policy"]["digest"] = fyrr_core::limits::digest(&bytes).into();
        std::fs::write(&head_path, serde_json::to_vec_pretty(&head).unwrap()).unwrap();
        let output = host_command(&worker)
            .args(["reader", "check"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("error: namespace_layout_old"), "{stderr}");
    }
    // The current policy again, over a manifest stamped with the plain major.
    std::fs::write(&policy_path, &current).unwrap();
    head["policy"]["digest"] = fyrr_core::limits::digest(&current).into();
    std::fs::write(&head_path, serde_json::to_vec_pretty(&head).unwrap()).unwrap();
    let manifest_path = namespace.join(head["content"]["manifest"].as_str().unwrap());
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["schema_version"] = "4.1".into();
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let output = host_command(&worker)
        .args(["reader", "check"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("error: namespace_layout_old"), "{stderr}");
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
    setup: (PathBuf, FixtureRoot),
) -> (PathBuf, PathBuf, FixtureRoot) {
    let (config, root) = setup;
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
    super::coordination::command(&config, &["writer", "namespace", "create"]);
    worker_sync(&config, &["allowed", "denied", "--full"]);
    let commit = committer_host(&config, &root);
    if publish_initial {
        publish(&commit);
    }
    (config, commit, root)
}

pub(super) fn input(root: &Path, value: Value) {
    std::fs::write(root.join("input.json"), serde_json::to_vec(&value).unwrap()).unwrap();
}
pub(super) fn doc(id: &str, revision: &str, text: &str) -> Value {
    json!({"id":id,"revision":revision,"texts":[text]})
}
pub(super) fn publish(config: &Path) -> Value {
    command(config, &["writer", "commit"])
}

pub(super) async fn rows(config: &Path) -> arrow_array::RecordBatch {
    source_rows(config, "incremental").await
}
/// The published rows of one source.
pub(super) async fn source_rows(config: &Path, source: &str) -> arrow_array::RecordBatch {
    let (head, _) = publication(config).await;
    source_rows_at(config, source, head.content.unwrap().lance_version).await
}
/// The rows of one source in the Lance version `version`.
async fn source_rows_at(config: &Path, source: &str, version: u64) -> arrow_array::RecordBatch {
    let loaded = fyrr_core::config::load(config).unwrap();
    let ds = fyrr_reader::storage::open_version(
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
pub(super) async fn source_roots(config: &Path, source: &str) -> Vec<String> {
    let mut ids = roots(&source_rows(config, source).await);
    ids.sort();
    ids.dedup();
    ids
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
pub(super) fn publish_initial(worker: &Path, commit: &Path, root: &Path) {
    initial(root);
    worker_sync(
        worker,
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
    publish(commit);
}

/// A frozen namespace takes no content sync, whatever the source, and still
/// commits the envelope that was prepared before the switch; thawed, it syncs
/// again.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_read_only_namespace_refuses_syncs_and_commits_what_was_prepared_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    assert_eq!(source_roots(&worker, "incremental").await, ["a", "b"]);
    // An envelope that is prepared and not published when the namespace is frozen.
    input(
        &root,
        json!({"epoch":2,"content":[[doc("c","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    assert_eq!(
        command(&commit, &["writer", "namespace", "read-only", "true"]),
        json!({"read_only":true,"changed":true})
    );
    assert_eq!(
        command(&commit, &["writer", "namespace", "read-only", "true"])["changed"],
        false,
        "freezing twice changes nothing"
    );

    // A content sync of any source is refused with one named error, before the
    // provider reads anything.
    input(
        &root,
        json!({"epoch":3,"content":[[doc("d","r1","fixture document 3")]]}),
    );
    let before = envelopes(&worker).await.len();
    for source in ["incremental", "allowed"] {
        let refused = worker_command(&worker)
            .args(["writer", "sync", source, "--kind", "content", "--full"])
            .output()
            .unwrap();
        assert!(!refused.status.success(), "{source}");
        assert!(
            String::from_utf8_lossy(&refused.stdout).contains("namespace_read_only"),
            "{source}: {}",
            String::from_utf8_lossy(&refused.stdout)
        );
    }
    assert_eq!(envelopes(&worker).await.len(), before, "nothing is written");

    // The envelope prepared before the switch is committed after it, and the
    // frozen namespace stays readable and maintainable.
    publish(&commit);
    assert_eq!(source_roots(&worker, "incremental").await, ["a", "b", "c"]);
    command(&commit, &["writer", "maintain"]);
    let (head, _) = publication(&worker).await;
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, namespace_root) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let policy: fyrr_core::publication::PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &namespace_root, &head.policy)
            .await
            .unwrap();
    assert!(policy.read_only);
    assert_eq!(policy.schema_version.to_string(), "5.0");

    // Thawed, the next sync prepares and commits.
    assert_eq!(
        command(&commit, &["writer", "namespace", "read-only", "false"]),
        json!({"read_only":false,"changed":true})
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert_eq!(
        source_roots(&worker, "incremental").await,
        ["a", "b", "c", "d"]
    );
}

/// A confirmed envelope that does not parse blocks only its source: another
/// source still publishes, and `writer status` names the blocked one (#370).
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_an_unreadable_envelope_blocks_only_its_source_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":[]},{"id":"b","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    // The envelope the cursor confirmed is truncated on storage.
    let last = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns.join("envelopes").join(format!("{}.json", last.id));
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    store
        .put(&path, bytes.slice(..bytes.len() / 2).into())
        .await
        .unwrap();
    std::fs::write(root.join("sources/0.txt"), "fixture document other 1").unwrap();
    root.embedding.insert_both(
        "fixture document other 1",
        root.embedding.vector("fixture document 0"),
    );
    worker_sync(&worker, &["allowed", "--kind", "content"]);
    publish(&commit);
    let control = source_rows(&worker, "allowed").await;
    let row = roots(&control).iter().position(|id| id == "0.txt").unwrap();
    assert_eq!(
        fyrr_writer::apply::strings(&control, "searchable_text")
            .unwrap()
            .value(row),
        "fixture document other 1",
        "the other source publishes"
    );
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(
        status["blocked_sources"],
        json!(["incremental"]),
        "{status}"
    );
    assert!(
        !row_readers(&rows(&worker).await, "b").is_empty(),
        "the blocked source keeps its rights"
    );
}

/// `writer status` names a source whose permissions cursor is lost from what
/// the publication applied, whether its configuration lists the source or not.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_status_names_a_lost_cursor_of_an_unlisted_source_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    // The committer's configuration does not list `incremental`.
    let text = std::fs::read_to_string(&commit).unwrap();
    let block = format!(
        "[[sources]]\nid='incremental'\ntype='incremental-test'\npermissions='sync'\ndocument_max_bytes='64B'\n[sources.intervals]\npermissions='5m'\nmembers='5m'\n[sources.scope]\nfixture='{}'\n",
        root.join("input.json").display()
    );
    assert!(text.contains(&block));
    std::fs::write(&commit, text.replace(&block, "")).unwrap();
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    store
        .delete(&ns.join("cursors").join("incremental-permissions.json"))
        .await
        .unwrap();
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(
        status["blocked_sources"],
        json!(["incremental"]),
        "{status}"
    );
}

/// An unreadable members envelope is named by no chain and could hide a revoke:
/// the commit and `writer status` fail closed, nothing is published, and once
/// it is put back the revoke is (#370).
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_an_unreadable_members_envelope_fails_the_commit_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let fetch = || {
        super::tools::Mcp::open(&worker)
            .call("fetch", json!({"source_id":"incremental","root_id":"a"}))["structuredContent"]
            ["documents"]
            .as_array()
            .is_some_and(|d| !d.is_empty())
    };
    assert!(fetch(), "a is readable before the revoke");
    let before = publication(&commit).await.0;
    // The snapshot removes `u` from `child`, and its envelope is truncated.
    input(
        &root,
        json!({"epoch":2,"members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"child","members":[]},{"id":"group","members":["grp:child"]}]}]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "members"]);
    let (path, _) = envelopes(&worker)
        .await
        .into_iter()
        .filter(|(_, e)| e.source_id == "incremental" && e.kind == SyncKind::Members)
        .max_by_key(|(_, e)| e.sequence)
        .unwrap();
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, _) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    store
        .put(&path, bytes.slice(..bytes.len() / 2).into())
        .await
        .unwrap();
    for args in [["writer", "commit"], ["writer", "status"]] {
        let refused = host_command(&commit).args(args).output().unwrap();
        assert!(!refused.status.success(), "{args:?} fails closed");
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("publication_invalid"),
            "{args:?}: {}",
            String::from_utf8_lossy(&refused.stderr)
        );
    }
    let after = publication(&commit).await.0;
    assert_eq!(after.content, before.content, "nothing is published");
    assert_eq!(after.identity, before.identity, "no identity is published");
    // Put back, the snapshot publishes, and `u` loses `a`.
    store.put(&path, bytes.into()).await.unwrap();
    publish(&commit);
    assert!(!fetch(), "the revoke is published");
}

/// Purging the source that an unreadable envelope blocks deletes that envelope
/// too, which its chain names: the namespace commits again (#370).
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_purge_of_the_blocked_source_deletes_its_unreadable_envelope_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":[]},{"id":"b","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let last = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns.join("envelopes").join(format!("{}.json", last.id));
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    store
        .put(&path, bytes.slice(..bytes.len() / 2).into())
        .await
        .unwrap();
    publish(&commit);
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(
        status["blocked_sources"],
        json!(["incremental"]),
        "{status}"
    );
    let purged = command(
        &commit,
        &["writer", "source", "purge", "incremental", "--confirm"],
    );
    assert_eq!(purged["purged"], true, "{purged}");
    assert!(
        store.head(&path).await.is_err(),
        "the unreadable envelope is deleted"
    );
    publish(&commit);
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["blocked_sources"], json!([]), "{status}");
    // A second purge changes nothing and the namespace still commits.
    command(
        &commit,
        &["writer", "source", "purge", "incremental", "--confirm"],
    );
    publish(&commit);
    assert!(source_roots(&worker, "incremental").await.is_empty());
}

/// An unreadable envelope of one source does not stop the members snapshot nor
/// the complete listing of another, which read the whole log (#370).
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_an_unreadable_envelope_leaves_members_and_listings_of_others_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    // A confirmed content envelope of `allowed` is truncated on storage.
    std::fs::write(root.join("sources/0.txt"), "fixture document other 1").unwrap();
    root.embedding.insert_both(
        "fixture document other 1",
        root.embedding.vector("fixture document 0"),
    );
    worker_sync(&worker, &["allowed", "--kind", "content"]);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let cursor: fyrr_core::connector::CursorState = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &ns.clone().join("cursors").join("allowed-content.json"),
    )
    .await
    .unwrap();
    let path = ns
        .join("envelopes")
        .join(format!("{}.json", cursor.last.unwrap().id));
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    store
        .put(&path, bytes.slice(..bytes.len() / 2).into())
        .await
        .unwrap();
    // `incremental` has a members snapshot and a complete listing pending.
    let before = command(&commit, &["writer", "status"])["directory"]["identity_generation"]
        .as_u64()
        .unwrap();
    input(
        &root,
        json!({"epoch":2,
            "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"child","members":[]},{"id":"group","members":["grp:child"]}]}],
            "listing":[{"last":true,"complete":true,"roots":["a"]}]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "members"]);
    worker_sync(&worker, &["incremental", "--kind", "reconcile", "--full"]);
    publish(&commit);
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["blocked_sources"], json!(["allowed"]), "{status}");
    assert!(
        status["directory"]["identity_generation"].as_u64().unwrap() > before,
        "the members snapshot is published: {status}"
    );
    assert_eq!(
        source_roots(&worker, "incremental").await,
        ["a"],
        "the listing is published"
    );
}

/// A frozen namespace still takes a revoke: a permission sync is prepared and
/// committed, and readers no longer see the document; content stays frozen
/// (#163).
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_read_only_namespace_publishes_a_revoke_and_refuses_content_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let fetch = |root_id: &str| {
        super::tools::Mcp::open(&worker).call(
            "fetch",
            json!({"source_id":"incremental","root_id":root_id}),
        )
    };
    let readable = |result: &Value| {
        result["structuredContent"]["documents"]
            .as_array()
            .is_some_and(|d| !d.is_empty())
    };
    assert!(readable(&fetch("a")), "a is readable before the revoke");
    assert_eq!(
        command(&commit, &["writer", "namespace", "read-only", "true"]),
        json!({"read_only":true,"changed":true})
    );
    input(
        &root,
        json!({"epoch":2,
            "content":[[doc("c","r1","fixture document 2")]],
            "permissions":[[{"id":"a","revision":"p2","readers":[]},{"id":"b","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    assert!(
        row_readers(&rows(&worker).await, "a").is_empty(),
        "the revoke is published"
    );
    assert!(!readable(&fetch("a")), "readers no longer see a");
    assert!(readable(&fetch("b")), "b keeps its readers");
    let refused = worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "content"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stdout).contains("namespace_read_only"),
        "{}",
        String::from_utf8_lossy(&refused.stdout)
    );
    publish(&commit);
    assert_eq!(source_roots(&worker, "incremental").await, ["a", "b"]);
}

/// A member read that repeats the published snapshot writes no envelope and
/// publishes nothing.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_idle_member_reread_publishes_nothing_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let before = publication(&commit).await;
    let staged = envelopes(&worker).await.len();
    let mut repeated: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    repeated["epoch"] = json!(2);
    input(&root, repeated);
    worker_sync(&worker, &["incremental", "--kind", "members"]);
    assert_eq!(envelopes(&worker).await.len(), staged, "no envelope");
    publish(&commit);
    let after = publication(&commit).await;
    assert_eq!(after.0.content, before.0.content);
    assert_eq!(after.0.identity, before.0.identity);
    assert_eq!(after.1.publication_id, before.1.publication_id);
}

/// A permission read that repeats the published rights, its documents and its
/// container grants, writes no envelope and publishes nothing; its cursor still
/// moves. A read that repeats only some of them writes the others.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_idle_permission_reread_publishes_nothing_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let read = |epoch: u64, readers: Value| {
        json!({"epoch":epoch,
            "permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]},{"id":"b","revision":"p1","readers":["grp:group"]}]],
            "container_grants":[[{"container_id":"folder","container_kind":"folder","permission_revision":"c1","readers":readers}]]})
    };
    // The documents repeat what is published, the grant is new.
    let staged = envelopes(&worker).await.len();
    input(&root, read(2, json!(["grp:group", "grp:child"])));
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let written: Vec<_> = envelopes(&worker)
        .await
        .into_iter()
        .filter(|(_, e)| e.kind == SyncKind::Permissions)
        .filter_map(|(_, e)| match e.payload {
            fyrr_core::envelope::EnvelopePayload::Permissions(p) => Some(p),
            _ => None,
        })
        .filter(|p| !p.complete_read)
        .collect();
    assert_eq!(
        envelopes(&worker).await.len(),
        staged + 2,
        "a part and its end"
    );
    assert!(
        written
            .iter()
            .any(|p| p.documents.is_empty() && p.container_grants.len() == 1),
        "only the grant is written"
    );
    publish(&commit);
    let before = publication(&commit).await;
    let staged = envelopes(&worker).await.len();
    let cursor = cursor_of(&worker, SyncKind::Permissions).await;
    // The same read again, its grant readers in another order, changes nothing.
    input(
        &root,
        read(3, json!(["grp:child", "grp:group", "grp:child"])),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    assert_eq!(envelopes(&worker).await.len(), staged, "no envelope");
    let moved = cursor_of(&worker, SyncKind::Permissions).await;
    assert_ne!(moved.cursor, cursor.cursor, "the cursor moves");
    assert_eq!(moved.last, cursor.last);
    publish(&commit);
    let after = publication(&commit).await;
    assert_eq!(after.0.content, before.0.content);
    assert_eq!(after.0.identity, before.0.identity);
    assert_eq!(after.1.publication_id, before.1.publication_id);
}

/// A grant and its removal inside one read: the removal names the published
/// revision again, and still publishes, since it follows the grant.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_grant_removed_inside_one_read_publishes_the_removal_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p0","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    assert!(row_readers(&rows(&worker).await, "a").is_empty());
    input(
        &root,
        json!({"epoch":3,"permissions":[
            [{"id":"a","revision":"p1","readers":["grp:group"]}],
            [{"id":"a","revision":"p0","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    assert!(
        row_readers(&rows(&worker).await, "a").is_empty(),
        "the removal is published"
    );
}

/// A removal after the envelopes of the reads before it were cleaned up: the
/// rights it is compared with are the published rows, not the log.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_removal_after_cleanup_publishes_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let granted = row_readers(&rows(&worker).await, "a");
    let read = |epoch: u64, revision: &str, readers: Value| {
        input(
            &root,
            json!({"epoch":epoch,"permissions":[[{"id":"a","revision":revision,"readers":readers}]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "permissions"]);
        publish(&commit);
    };
    read(2, "p2", json!([]));
    assert!(row_readers(&rows(&worker).await, "a").is_empty());
    read(3, "p3", json!(["grp:group"]));
    assert_eq!(row_readers(&rows(&worker).await, "a"), granted);
    fyrr_core::clock::advance(std::time::Duration::from_secs(4 * 86_400)).unwrap();
    command(&commit, &["writer", "maintain"]);
    read(4, "p2", json!([]));
    assert!(
        row_readers(&rows(&worker).await, "a").is_empty(),
        "the removal is published"
    );
}

/// Content that narrows the readers of a document leaves it without a
/// permission revision: a permission read that sends the same revision again
/// restores its rights.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_same_revision_restores_rights_narrowed_by_content_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let granted = row_readers(&rows(&worker).await, "a");
    assert!(!granted.is_empty());
    // Readers that share none of the published ones narrow them to nobody.
    let mut narrowing = doc("a", "r2", "fixture document 0");
    narrowing["readers"] = json!(["grp:child"]);
    input(&root, json!({"epoch":2,"content":[[narrowing]]}));
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert!(row_readers(&rows(&worker).await, "a").is_empty());
    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    assert_eq!(
        row_readers(&rows(&worker).await, "a"),
        granted,
        "the rights are restored"
    );
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_sub_envelope_cursor_recovery_and_atomic_chunk_groups_on_storage() {
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
            assert_eq!(p.documents.len(), 2, "two roots fit one envelope");
            let receipt = command(
                &commit,
                &["writer", "commit", "--receipt", &envelope.envelope_id],
            );
            publications.insert(receipt["publication_id"].as_str().unwrap().to_owned());
        }
        assert_eq!(
            publications.len(),
            1,
            "two-chunk and one-chunk permission replacements exceed one group, \
             and their read is published whole, its rows written in steps"
        );
    }
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

/// A section with its own readers stays restricted to them within the
/// document's readers, whichever of content and permissions arrives first.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_section_readers_survive_a_later_permission_state_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
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

    // Content first in an empty namespace, with no permission state.
    apply(1, "content", content("s", "r1", None));
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
    assert_eq!(section_readers(&batch, "s"), [set(&[]), set(&[])]);
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
    assert!(rollback.status.success(), "{}", report(&rollback));
    assert_eq!(section_readers(&rows(&worker).await, "s"), both);
}

/// A sync provider without a permission stream states readers only in
/// content: there an empty reader set means nobody, and content never widens
/// the applied state again. Only a provider with a permission
/// stream makes no statement that way.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_content_only_provider_empty_readers_narrow_to_nobody_on_storage() {
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

async fn rejected_authorization(kind: SyncKind) {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let mut late = envelopes(&worker)
        .await
        .into_iter()
        .map(|(_, e)| e)
        .filter(|e| e.source_id == "incremental" && e.kind == kind)
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
    let newest_members = envelopes(&worker)
        .await
        .iter()
        .filter(|(_, e)| e.source_id == "incremental" && e.kind == SyncKind::Members)
        .map(|(_, e)| e.sequence)
        .max();
    for (path, mut envelope) in envelopes(&worker).await {
        if (kind == SyncKind::Permissions
            && matches!(&envelope.payload, EnvelopePayload::Permissions(p) if p.documents.iter().any(|d| d.permission_revision == "p2")))
            || (kind == SyncKind::Members
                && envelope.source_id == "incremental"
                && envelope.kind == kind
                && Some(envelope.sequence) == newest_members
                && matches!(&envelope.payload, EnvelopePayload::Members(p) if !p.complete_read))
        {
            rejected_id = envelope.envelope_id.clone();
            envelope.created_at = fyrr_core::clock::now() - chrono::Duration::days(4);
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
    publish(&commit);
    let (head, _) = publication(&worker).await;
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
    // Re-fetch both kinds from the rewound generation, then separate partial and complete reads.
    // A first replacement attempt fails after the generation moved on. The
    // next one must still end with the completion envelope that consumes the marker.
    input(
        &root,
        json!({"epoch":41,"catch_revision_failure":"lookup_many"}),
    );
    let failed = super::coordination::worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", kind.as_str()])
        .output()
        .unwrap();
    assert!(!failed.status.success(), "{kind:?}: failed attempt");
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
    // Only the rejected kind replaces a rejection, so only it ends with a
    // terminal completion envelope. Keep it for the original completion control.
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
    let mut tip = None;
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
        if envelope.kind != kind {
            continue;
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
            tip = Some(envelope.previous.clone());
        } else {
            partial_ids.push(envelope.envelope_id);
        }
    }
    // The cursor does not hold the permissions terminal yet: the chain ends at the
    // link before it. A members terminal is simply not in the log.
    let cursor_path = ns
        .clone()
        .join("cursors")
        .join(format!("incremental-{}.json", kind.as_str()));
    let confirmed: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    if let (SyncKind::Permissions, Some(previous)) = (kind, tip) {
        let mut held = confirmed.clone();
        held.last = match previous {
            Some(id) => {
                let sequence = envelopes(&worker)
                    .await
                    .iter()
                    .find(|(_, e)| e.envelope_id == id)
                    .map(|(_, e)| e.sequence)
                    .unwrap_or(held.sequence);
                Some(fyrr_core::connector::LastEnvelope { sequence, id })
            }
            None => None,
        };
        store
            .put(&cursor_path, write_contract(&held).into())
            .await
            .unwrap();
    }
    assert!(!terminals.is_empty(), "{kind:?}: terminal control");
    // A permission read is one envelope, which ends it: only a members read has
    // parts before its completion.
    assert!(
        kind == SyncKind::Permissions || !partial_ids.is_empty(),
        "{kind:?}: partial replacement"
    );
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
    store
        .put(&cursor_path, write_contract(&confirmed).into())
        .await
        .unwrap();
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
    late.created_at = fyrr_core::clock::now();
    late.envelope_id = fyrr_core::envelope::leased_envelope_id(
        &late.namespace,
        &late.source_id,
        late.kind,
        late.sequence,
        late.rejection_generation,
        late.cursor.start.as_deref(),
        late.lease_generation,
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
    let (applied, rejected) =
        fyrr_writer::commit::published_ids(&loaded.config, &publication(&worker).await.0)
            .await
            .unwrap();
    if kind == SyncKind::Permissions {
        // No cursor confirmed it: it is neither applied nor rejected, and inert.
        assert!(!applied.contains(&late.envelope_id) && !rejected.contains(&late.envelope_id));
    } else {
        assert!(rejected.contains(&late.envelope_id));
    }
    let denied = super::tools::Mcp::open(&worker)
        .call("fetch", json!({"source_id":"incremental","root_id":"a"}));
    assert_eq!(denied["structuredContent"]["documents"], json!([]));
}

/// A non-content batch is a header with no rows and may be as large as the
/// configured envelope: one members part (at most 256 users and groups) whose
/// groups list a thousand members each is a header of over 2 MiB, which both
/// the dry run and a prepared run, in real workers with the real Python host,
/// accept.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_large_members_snapshot_passes_dry_run_and_a_prepared_run_on_storage() {
    let (worker, commit, root) = fixture();
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let users: Vec<Value> = (0..50)
        .map(|n| {
            json!({
                "id": format!("user-{n:06}"),
                "email": format!("person.number.{n:06}@example.test"),
                "active": true,
            })
        })
        .collect();
    let groups: Vec<Value> = (0..150)
        .map(|group| {
            let members: Vec<String> = (0..1000)
                .map(|member| format!("usr:user-{:06}", group * 1000 + member))
                .collect();
            json!({"id": format!("group-{group:03}"), "members": members})
        })
        .collect();
    let part = json!({"users": users, "groups": groups, "last": true});
    assert!(
        part.to_string().len() > 2 << 20,
        "the header must exceed 2 MiB"
    );
    input(&root, json!({"epoch": 3, "members": [part]}));

    let dry = super::coordination::worker_command(&worker)
        .args([
            "writer",
            "sync",
            "--dry-run",
            "incremental",
            "--kind",
            "members",
        ])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&dry.stdout);
    assert!(
        dry.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&dry.stderr)
    );
    assert!(stdout.contains("users=50 groups=150"), "{stdout}");

    worker_sync(&worker, &["incremental", "--kind", "members"]);
    let pending = envelopes(&worker).await;
    assert!(pending.iter().any(|(_, e)| matches!(
        &e.payload,
        EnvelopePayload::Members(p) if p.last
    )));
}

/// A prepared content run's provider asks the worker for stored revisions, in a
/// real worker with the real Python host, through both callbacks, and gets
/// answers: the second sync sees the revisions the first stored and published.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_provider_receives_stored_revisions_through_the_host_on_storage() {
    let (worker, commit, root) = fixture();
    let seen = || -> Value {
        serde_json::from_str(&std::fs::read_to_string(root.join("revisions-seen.json")).unwrap())
            .unwrap()
    };
    let asked = json!(["a", "b", "missing"]);
    input(
        &root,
        json!({"epoch":1,"record_revisions":asked,"content":[[
            doc("a","r1","fixture document 0"),doc("b","r1","fixture document 1")
        ]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    assert_eq!(seen(), json!({"lookup": {}, "next": []}));
    publish(&commit);
    input(
        &root,
        json!({"epoch":2,"record_revisions":asked,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let second = seen();
    assert_eq!(second["lookup"], json!({"a": "r1", "b": "r1"}));
    let mut pages = second["next"].as_array().unwrap().clone();
    pages.sort_by_key(ToString::to_string);
    assert_eq!(pages, [json!(["a", "r1"]), json!(["b", "r1"])]);
}

/// A provider that catches a failed host revision callback still fails its read.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_caught_revision_failure_fails_the_read_on_storage() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let mut outcomes = Vec::new();
    for kind in [SyncKind::Permissions, SyncKind::Members] {
        for callback in ["lookup", "next"] {
            let (worker, commit, root) = fixture();
            publish_initial(&worker, &commit, &root);
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
            let unstaged = envelopes(&worker).await.len() == staged;
            publish(&commit);
            outcomes.push(json!({"kind":kind,"callback":callback,"failed":!output.status.success(),"error":report["kinds"][0]["error"],"unstaged":unstaged}));
            // The failed view must not poison a later complete read.
            input(&root, json!({"epoch":4}));
            worker_sync(&worker, &["incremental", "--kind", kind.as_str()]);
            publish(&commit);
        }
    }
    for outcome in outcomes {
        assert_eq!(outcome["failed"], true, "{outcome}");
        assert_eq!(outcome["error"], "storage_unavailable", "{outcome}");
        assert_eq!(outcome["unstaged"], true, "{outcome}");
    }
}

/// A provider that asks for more roots than allowed, catches the refusal and
/// finishes still fails its read.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_caught_refused_revision_request_fails_the_read_on_storage() {
    for kind in [SyncKind::Permissions, SyncKind::Members] {
        let (worker, commit, root) = fixture();
        publish_initial(&worker, &commit, &root);
        let staged = envelopes(&worker).await.len();
        input(
            &root,
            json!({"epoch":3,"catch_revision_failure":"lookup_many"}),
        );
        let output = super::coordination::worker_command(&worker)
            .args(["writer", "sync", "incremental", "--kind", kind.as_str()])
            .output()
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("revision-callback")).unwrap(),
            "caught",
            "{kind:?}"
        );
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(!output.status.success(), "{kind:?}: {report}");
        assert_eq!(report["kinds"][0]["error"], "host_callback_failed");
        assert_eq!(envelopes(&worker).await.len(), staged, "{kind:?}");
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn incremental_rejected_authorization_requires_complete_replacement_on_storage() {
    for kind in [SyncKind::Permissions, SyncKind::Members] {
        rejected_authorization(kind).await;
    }
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_damaged_rows_rejected_and_cleanup_retains_log_authority_on_storage() {
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
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_delta_genesis_undeclared_row_rejected_on_storage() {
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

/// The stored cursor of the `incremental` source's `kind`.
async fn cursor_of(config: &Path, kind: SyncKind) -> fyrr_core::connector::CursorState {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    fyrr_reader::storage::read_contract(
        store.as_ref(),
        &ns.join("cursors")
            .join(format!("incremental-{}.json", kind.as_str())),
    )
    .await
    .unwrap()
}

/// The cursor of the permissions of `incremental`, or a blank one before any worker took it.
async fn cursor_or_blank(config: &Path) -> fyrr_core::connector::CursorState {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    match fyrr_reader::storage::read_contract(
        store.as_ref(),
        &ns.join("cursors").join("incremental-permissions.json"),
    )
    .await
    {
        Ok(state) => state,
        Err(_) => fyrr_writer::prepare::initial_cursor(
            &loaded.config,
            "incremental",
            SyncKind::Permissions,
        ),
    }
}

/// The permission envelopes of the `incremental` source, oldest first.
async fn permission_envelopes(config: &Path) -> Vec<PreparedEnvelope> {
    let mut found: Vec<_> = envelopes(config)
        .await
        .into_iter()
        .map(|(_, envelope)| envelope)
        .filter(|e| e.source_id == "incremental" && e.kind == SyncKind::Permissions)
        .collect();
    found.sort_by_key(|e| (e.sequence, e.lease_generation));
    found
}

/// A permission envelope put as a worker would put it, but that no cursor
/// confirmed: `previous` and `lease_generation` as given, the roots in
/// `documents` revoked (no readers) under `revision`. Without
/// `lease_generation` it is one written before permissions envelopes were chained.
async fn put_permission_envelope(
    config: &Path,
    sequence: u64,
    previous: Option<String>,
    lease_generation: u64,
    documents: &[(&str, &str)],
) -> PreparedEnvelope {
    let documents = documents
        .iter()
        .map(
            |(root, revision)| fyrr_core::connector::DocumentPermission {
                root_id: (*root).into(),
                permission_revision: (*revision).into(),
                readers: Vec::new(),
                containers: Vec::new(),
            },
        )
        .collect();
    put_envelope_of(config, sequence, previous, lease_generation, documents).await
}

/// The same with the documents as they are.
async fn put_envelope_of(
    config: &Path,
    sequence: u64,
    previous: Option<String>,
    lease_generation: u64,
    documents: Vec<fyrr_core::connector::DocumentPermission>,
) -> PreparedEnvelope {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let mut envelope: PreparedEnvelope = fyrr_core::parse_contract(include_bytes!(
        "../../../../fixtures/contracts/prepared-envelope/permissions.json"
    ))
    .unwrap();
    envelope.namespace.clone_from(&loaded.config.namespace.name);
    envelope.source_id = "incremental".into();
    envelope.rejection_generation = 0;
    envelope.restore_sequence = 0;
    envelope.cursor.start = None;
    envelope.sequence = sequence;
    envelope.created_at = fyrr_core::clock::now();
    envelope.previous = previous;
    envelope.lease_generation = lease_generation;
    envelope.advances_cursor = true;
    let EnvelopePayload::Permissions(payload) = &mut envelope.payload else {
        unreachable!()
    };
    payload.complete_read = true;
    payload.documents = documents;
    envelope.envelope_id = fyrr_core::envelope::leased_envelope_id(
        &envelope.namespace,
        &envelope.source_id,
        envelope.kind,
        envelope.sequence,
        envelope.rejection_generation,
        envelope.cursor.start.as_deref(),
        envelope.lease_generation,
    );
    store
        .put(
            &ns.join("envelopes")
                .join(format!("{}.json", envelope.envelope_id)),
            write_contract(&envelope).into(),
        )
        .await
        .unwrap();
    envelope
}

/// Whether the committer applied or rejected `id`.
async fn resolved(config: &Path, id: &str) -> (bool, bool) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let (applied, rejected) = fyrr_writer::commit::published_ids(&loaded.config, &head)
        .await
        .unwrap();
    (applied.contains(id), rejected.contains(id))
}

/// A permission read is published whole or not at all: one that fails on a
/// later batch writes nothing and leaves the stored cursor, and the next read
/// publishes the whole change as one envelope that follows the one before.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_failed_permission_read_keeps_rights_and_the_next_read_publishes_whole_on_storage()
 {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let readers = row_readers(&rows(&worker).await, "a");
    assert_eq!(readers.len(), 1);
    let staged = permission_envelopes(&worker).await;
    let before = cursor_of(&worker, SyncKind::Permissions).await;
    assert_eq!(
        before.last.as_ref().map(|last| &last.id),
        staged.last().map(|e| &e.envelope_id)
    );
    let revoke = |epoch: u64, fail: Option<u64>| {
        let mut input = json!({"epoch":epoch,"permissions":[
            [{"id":"a","revision":"p2","readers":[]}],
            [{"id":"b","revision":"p2","readers":[]}]]});
        if let Some(batch) = fail {
            input["fail_permissions_at"] = json!(batch);
        }
        input
    };
    input(&root, revoke(2, Some(1)));
    let failed = worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "permissions"])
        .output()
        .unwrap();
    assert!(!failed.status.success());
    assert_eq!(permission_envelopes(&worker).await.len(), staged.len());
    let kept = cursor_of(&worker, SyncKind::Permissions).await;
    assert_eq!(
        (&kept.cursor, kept.sequence, &kept.last),
        (&before.cursor, before.sequence, &before.last)
    );
    publish(&commit);
    let batch = rows(&worker).await;
    assert_eq!(row_readers(&batch, "a"), readers);
    assert_eq!(row_readers(&batch, "b"), readers);

    input(&root, revoke(3, None));
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let all = permission_envelopes(&worker).await;
    assert_eq!(
        all.len(),
        staged.len() + 2,
        "a part and the envelope that ends the read"
    );
    let [.., part, read] = &all[..] else {
        unreachable!()
    };
    assert_eq!(
        part_shape(&all[all.len() - 2..]),
        [(2, false, false), (0, true, true)]
    );
    assert_eq!(read.cursor.start, before.cursor);
    assert_eq!(part.previous.as_ref(), before.last.as_ref().map(|l| &l.id));
    assert_eq!(read.previous.as_ref(), Some(&part.envelope_id));
    let confirmed = cursor_of(&worker, SyncKind::Permissions).await;
    assert_eq!(confirmed.cursor.as_deref(), Some("3-1"));
    assert_eq!(confirmed.last.unwrap().id, read.envelope_id);
    publish(&commit);
    let batch = rows(&worker).await;
    assert!(row_readers(&batch, "a").is_empty());
    assert!(row_readers(&batch, "b").is_empty());
}

/// An envelope that no cursor confirmed is inert, however late it is put and
/// whatever it changes: a worker that lost its lease before an empty `--full`
/// read of a new one cannot publish, and nor can a crash between a put and the
/// cursor write leave an envelope that is applied.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_permission_envelope_no_cursor_confirmed_is_inert_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let readers = row_readers(&rows(&worker).await, "b");
    assert_eq!(readers.len(), 1);
    // A worker takes the lease, prepares a revoke of `b`, and loses the lease
    // before it puts it.
    let old = fyrr_writer::sync_lease::CursorLease::acquire(
        &loaded.config,
        "incremental",
        SyncKind::Permissions,
    )
    .await
    .unwrap();
    let held = old.state().await.unwrap();
    old.release().await.unwrap();
    // A new worker reads nothing, with `--full`: it writes no envelope.
    input(&root, json!({"epoch":2}));
    worker_sync(&worker, &["incremental", "--kind", "permissions", "--full"]);
    let confirmed = cursor_of(&worker, SyncKind::Permissions).await;
    assert_eq!(confirmed.last, held.last);

    // The late put has the sequence a read of the new worker would take, its own
    // lease generation, and follows what the cursor held before.
    let late = put_permission_envelope(
        &worker,
        held.sequence + 1,
        held.last.as_ref().map(|last| last.id.clone()),
        held.lease.as_ref().unwrap().epoch,
        &[("b", "p9")],
    )
    .await;
    publish(&commit);
    assert_eq!(resolved(&commit, &late.envelope_id).await, (false, false));
    assert_eq!(row_readers(&rows(&worker).await, "b"), readers);

    // A crash between the put and the cursor write: the envelope that was put
    // is not the cursor's, and the repeated read writes its own beside it.
    let orphan = put_permission_envelope(
        &worker,
        confirmed.sequence + 1,
        confirmed.last.as_ref().map(|last| last.id.clone()),
        confirmed.lease.as_ref().map_or(1, |lease| lease.epoch),
        &[("b", "p8")],
    )
    .await;
    // An envelope no cursor confirmed is inert: it does not block its source.
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["blocked_sources"], json!([]), "{status}");
    // Neither the late put nor the orphan is pending work, to status as to the
    // committer.
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let pending = fyrr_writer::commit::pending(&loaded.config, &head)
        .await
        .unwrap();
    assert!(
        pending
            .iter()
            .all(|e| e.envelope_id != late.envelope_id && e.envelope_id != orphan.envelope_id)
    );
    assert_eq!(
        status["pending_envelopes"],
        json!(pending.len()),
        "{status}"
    );
    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"a","revision":"p4","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let next = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    assert_eq!(
        next.sequence,
        confirmed.sequence + 2,
        "a part and the envelope that ends the read"
    );
    let envelopes = permission_envelopes(&worker).await;
    let ending = envelopes.iter().find(|e| e.envelope_id == next.id).unwrap();
    let part = envelopes
        .iter()
        .find(|e| Some(&e.envelope_id) == ending.previous.as_ref())
        .unwrap();
    assert_eq!(part.sequence, orphan.sequence);
    assert_ne!(
        part.envelope_id, orphan.envelope_id,
        "the lease generation is in the ID"
    );
    publish(&commit);
    assert_eq!(resolved(&commit, &orphan.envelope_id).await, (false, false));
    assert_eq!(resolved(&commit, &next.id).await, (true, false));
    let batch = rows(&worker).await;
    assert!(row_readers(&batch, "a").is_empty());
    assert_eq!(row_readers(&batch, "b"), readers);
}

/// An empty read that replaces a rejection keeps the connector cursor.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_an_empty_replacement_keeps_the_connector_cursor_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"b","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, _) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let (path, mut aged) = envelopes(&worker)
        .await
        .into_iter()
        .filter(|(_, e)| e.source_id == "incremental" && e.kind == SyncKind::Permissions)
        .max_by_key(|(_, e)| e.sequence)
        .unwrap();
    aged.created_at = fyrr_core::clock::now() - chrono::Duration::days(4);
    store
        .put(&path, write_contract(&aged).into())
        .await
        .unwrap();
    publish(&commit);
    assert!(
        resolved(&commit, &aged.envelope_id).await.1,
        "rejected for its age"
    );
    input(&root, json!({"epoch":4}));
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let replacement = permission_envelopes(&worker).await.pop().unwrap();
    let EnvelopePayload::Permissions(payload) = &replacement.payload else {
        unreachable!()
    };
    assert!(payload.documents.is_empty() && payload.complete_read);
    assert!(!replacement.advances_cursor);
    assert_eq!(replacement.previous.as_ref(), Some(&aged.envelope_id));
    let after = cursor_of(&worker, SyncKind::Permissions).await;
    assert_eq!(
        after.cursor, aged.cursor.start,
        "the connector position stays"
    );
    assert_ne!(after.cursor.as_deref(), Some("complete"));
    assert_eq!(after.last.unwrap().id, replacement.envelope_id);
    publish(&commit);
    assert_eq!(
        resolved(&commit, &replacement.envelope_id).await,
        (true, false)
    );
}

/// The shape of each permissions envelope of `parts`: its roots, whether it
/// ends the read, and whether it moves the connector cursor.
fn part_shape(parts: &[PreparedEnvelope]) -> Vec<(usize, bool, bool)> {
    parts
        .iter()
        .map(|e| match &e.payload {
            EnvelopePayload::Permissions(p) => (
                p.documents.len() + p.container_grants.len(),
                p.complete_read,
                e.advances_cursor,
            ),
            _ => unreachable!(),
        })
        .collect()
}

/// Publishes once, and checks that `parts`, a whole read, are all applied by
/// that one publication: one head compare-and-swap.
async fn published_in_one(commit: &Path, parts: &[PreparedEnvelope]) {
    let before = publication(commit).await.1.sequence;
    publish(commit);
    let (_, manifest) = publication(commit).await;
    assert_eq!(manifest.sequence, before + 1, "one publication");
    for part in parts {
        assert!(
            manifest.envelope_ids.contains(&part.envelope_id),
            "{} is applied by the same publication",
            part.envelope_id
        );
    }
    // Nothing of the read is left: the next round publishes nothing.
    publish(commit);
    assert_eq!(publication(commit).await.1.sequence, before + 1);
}

/// A permission read of more roots than an envelope holds is put in parts,
/// each within them, chained and confirmed by the cursor only with the envelope
/// that ends the read, and the committer publishes the read whole, in one
/// publication, though it is over the bounds of a group. The rows it rewrites
/// are the committer's to bound: two roots of two rows each, more than a group
/// together, and a root of three rows, more than a group alone, are each
/// published in one publication, written in steps.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_permission_read_over_the_roots_of_an_envelope_publishes_whole_in_one_publication_on_storage()
 {
    let (worker, commit, root) = fixture();
    initial(&root);
    let text = "A".repeat(16384) + &"B".repeat(1000);
    let vector = root.embedding.vector("fixture query");
    // A root of three chunks, more rows than a group of two holds.
    let long = "A".repeat(16384) + &"B".repeat(16384) + &"C".repeat(1000);
    for chunk in fyrr_writer::chunk::chunks(&text)
        .into_iter()
        .chain(fyrr_writer::chunk::chunks(&long))
    {
        root.embedding.insert_both(chunk, vector.clone());
    }
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config)
            .unwrap()
            .replace("document_max_bytes='64B'", "document_max_bytes='64KiB'");
        std::fs::write(config, text).unwrap();
    }
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1",&text),doc("b","r1",&text),doc("c","r1",&long)]],
            "permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]},{"id":"b","revision":"p1","readers":["grp:group"]},{"id":"c","revision":"p1","readers":["grp:group"]}]],
            "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"child","members":["usr:u"]},{"id":"group","members":["grp:child"]}]}]}),
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
    let granted = row_readers(&rows(&worker).await, "a");
    assert!(!granted.is_empty());

    // Two roots to an envelope, two rows to a group.
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config).unwrap().replace(
            "[reader]",
            "[writer]\nenvelope_max_rows=2\n[writer.commit]\ngroup_max_rows=2\n[reader]",
        );
        std::fs::write(config, text).unwrap();
    }
    // Five roots of a marker row each are three envelopes.
    let before = permission_envelopes(&worker).await.len();
    let batches: Vec<Value> = (0..5)
        .map(|n| json!([{"id":format!("r{n}"),"revision":"p1","readers":[]}]))
        .collect();
    input(&root, json!({"epoch":2,"permissions":batches}));
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts: Vec<_> = permission_envelopes(&worker).await.split_off(before);
    assert_eq!(
        part_shape(&parts),
        [
            (2, false, false),
            (2, false, false),
            (1, false, false),
            (0, true, true)
        ]
    );
    assert!(
        parts
            .windows(2)
            .all(|pair| pair[1].previous.as_ref() == Some(&pair[0].envelope_id))
    );
    assert!(
        parts
            .iter()
            .all(|e| e.cursor.start.as_deref() == Some("1-0"))
    );
    assert_eq!(
        cursor_of(&worker, SyncKind::Permissions)
            .await
            .last
            .unwrap()
            .id,
        parts[3].envelope_id
    );
    published_in_one(&commit, &parts).await;

    // `a` and `b` have two rows each: each fits a group, together they do not.
    // One batch revokes both, as one envelope; the revocation is published
    // once, whole, its rows written in two steps.
    let before = permission_envelopes(&worker).await.len();
    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"a","revision":"p2","readers":[]},{"id":"b","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts: Vec<_> = permission_envelopes(&worker).await.split_off(before);
    assert_eq!(part_shape(&parts), [(2, false, false), (0, true, true)]);
    let batch = rows(&worker).await;
    assert_eq!(row_readers(&batch, "a"), granted);
    assert_eq!(row_readers(&batch, "b"), granted);
    published_in_one(&commit, &parts).await;
    let batch = rows(&worker).await;
    assert!(row_readers(&batch, "a").is_empty() && row_readers(&batch, "b").is_empty());

    // `c` alone has more rows than a group: its rows are written in steps.
    let before = permission_envelopes(&worker).await.len();
    input(
        &root,
        json!({"epoch":4,"permissions":[[{"id":"c","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts: Vec<_> = permission_envelopes(&worker).await.split_off(before);
    assert_eq!(part_shape(&parts), [(1, false, false), (0, true, true)]);
    published_in_one(&commit, &parts).await;
    assert!(row_readers(&rows(&worker).await, "c").is_empty());
}

/// Runs a committer that is killed once it wrote its first Lance version, and
/// passes the lease it held: the head is as it was.
async fn crash_after_the_first_version(commit: &Path) {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let before = publication(commit).await.1.sequence;
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let stop = Arc::new(AtomicBool::new(false));
    let (gate, hook) = super::corrections::RequestGate::new(|request, _, after| {
        after && request.starts_with("PUT ") && request.contains("/_versions/")
    });
    let (proxy, guard) =
        super::corrections::proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
    let gate = gate; // Release a blocked request before joining the proxy on unwind.
    let proxied = commit.parent().unwrap().join("crash.toml");
    std::fs::write(
        &proxied,
        std::fs::read_to_string(commit)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let mut process = super::coordination::CommitProcess::spawn_command(
        &proxied,
        &["writer", "commit"],
        Arc::new(AtomicBool::new(false)),
    );
    gate.entered();
    process.child().kill().unwrap();
    let killed = process.finish_bounded();
    gate.release();
    assert!(!killed.status.success(), "the committer was killed");
    stop.store(true, Ordering::SeqCst);
    drop(guard);
    let (head, manifest) = publication(commit).await;
    assert_eq!(manifest.sequence, before, "nothing was published");
    // The killed holder cannot release its lease: pass its deadline.
    let remaining = (head.lease.unwrap().expires_at - fyrr_core::clock::now())
        .to_std()
        .unwrap_or_default();
    fyrr_core::clock::advance(remaining + std::time::Duration::from_millis(50)).unwrap();
}

/// A permission read over the bytes of an envelope (two pages of a root of
/// 1,700 readers each, which fit 256 KiB, an envelope and a group, apart and not
/// together) is put in two parts, and published whole in one publication; each
/// rewritten row, over the bytes of a group with its readers twice, is a step
/// of its own. A read that fails after its first part was put, as a worker that
/// dies between its parts, publishes none of it: the part is not confirmed by
/// the cursor, and the next read publishes the change; a committer that dies
/// after the first Lance version of the read publishes none of it. A root
/// whose readers alone are over the bytes of an envelope refuses its read.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_permission_read_over_the_bytes_of_a_group_publishes_whole_or_none_on_storage()
{
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let granted = row_readers(&rows(&worker).await, "a");
    assert_eq!(granted.len(), 1);
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config).unwrap().replace(
            "[reader]",
            "[writer]\ndocument_max_bytes='64KiB'\nenvelope_max_bytes='256KiB'\n[writer.commit]\ngroup_max_bytes='256KiB'\n[reader]",
        );
        std::fs::write(config, text).unwrap();
    }
    let readers =
        |tag: &str| -> Vec<String> { (0..1700).map(|n| format!("grp:{tag}-{n:04}")).collect() };
    let read = |epoch: u64, fail: Option<u64>| {
        let mut value = json!({"epoch":epoch,"permissions":[
            [{"id":"a","revision":format!("p{epoch}"),"readers":readers("a")}],
            [{"id":"b","revision":format!("p{epoch}"),"readers":readers("b")}],
            []]});
        if let Some(at) = fail {
            value["fail_permissions_at"] = json!(at);
        }
        value
    };
    let before = permission_envelopes(&worker).await.len();
    let cursor = cursor_of(&worker, SyncKind::Permissions).await;
    input(&root, read(2, Some(2)));
    let failed = worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "permissions"])
        .output()
        .unwrap();
    assert!(!failed.status.success(), "the read was to fail");
    let put: Vec<_> = permission_envelopes(&worker).await.split_off(before);
    assert_eq!(
        part_shape(&put),
        [(1, false, false)],
        "its first part was put"
    );
    let kept = cursor_of(&worker, SyncKind::Permissions).await;
    assert_eq!(
        (&kept.cursor, kept.sequence, &kept.last),
        (&cursor.cursor, cursor.sequence, &cursor.last)
    );
    publish(&commit);
    assert_eq!(resolved(&commit, &put[0].envelope_id).await, (false, false));
    let batch = rows(&worker).await;
    assert_eq!(row_readers(&batch, "a"), granted);
    assert_eq!(row_readers(&batch, "b"), granted);

    input(&root, read(3, None));
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts: Vec<_> = permission_envelopes(&worker)
        .await
        .split_off(before + put.len());
    assert_eq!(
        part_shape(&parts),
        [(1, false, false), (1, false, false), (0, true, true)]
    );
    let loaded = fyrr_core::config::load(&commit).unwrap();
    for part in &parts {
        let bytes = write_contract(part).len() as u64;
        assert!(bytes <= loaded.config.writer.commit.group_max_bytes.as_u64());
    }
    // A committer that dies once it wrote the first Lance version of the read
    // publishes none of it: the rights stay, and the next commit publishes the
    // whole read once.
    crash_after_the_first_version(&commit).await;
    let batch = rows(&worker).await;
    assert_eq!(row_readers(&batch, "a"), granted);
    assert_eq!(row_readers(&batch, "b"), granted);
    published_in_one(&commit, &parts).await;
    let batch = rows(&worker).await;
    assert_eq!(row_readers(&batch, "a").len(), 1700);
    assert_eq!(row_readers(&batch, "b").len(), 1700);
    assert!(!row_readers(&batch, "a").contains(&granted[0]), "revoked");
    assert_eq!(resolved(&commit, &put[0].envelope_id).await, (false, false));

    // A root of 3,400 readers: its entry alone is over the bytes of an envelope.
    let before = permission_envelopes(&worker).await.len();
    let mut many = readers("a");
    many.extend(readers("c"));
    input(
        &root,
        json!({"epoch":4,"permissions":[[{"id":"a","revision":"p4","readers":many}]]}),
    );
    let refused = worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "permissions"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    let said = String::from_utf8_lossy(&refused.stdout).into_owned()
        + &String::from_utf8_lossy(&refused.stderr);
    assert!(said.contains("permission_root_oversized"), "{said}");
    assert_eq!(permission_envelopes(&worker).await.len(), before);
}

/// The rows a read rewrites, text and all, are the committer's to bound: two
/// roots of 150,000 characters each, whose rows fit a 256 KiB group apart, not
/// together, are one part, and the committer writes their rows in steps of one
/// publication.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_permission_read_whose_rewritten_rows_are_over_a_group_publishes_in_steps_on_storage()
 {
    let (worker, commit, root) = published_large_rows(150_000, "192KiB").await;
    assert!(!row_readers(&rows(&worker).await, "a").is_empty());
    for config in [&worker, &commit] {
        let config_text = std::fs::read_to_string(config).unwrap().replace(
            "[reader]",
            "[writer]\ndocument_max_bytes='192KiB'\nenvelope_max_bytes='256KiB'\n[writer.commit]\ngroup_max_bytes='256KiB'\n[reader]",
        );
        std::fs::write(config, config_text).unwrap();
    }
    let before = permission_envelopes(&worker).await.len();
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":[]},{"id":"b","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts: Vec<_> = permission_envelopes(&worker).await.split_off(before);
    assert_eq!(part_shape(&parts), [(2, false, false), (0, true, true)]);
    published_in_one(&commit, &parts).await;
    let batch = rows(&worker).await;
    assert!(row_readers(&batch, "a").is_empty() && row_readers(&batch, "b").is_empty());
}

/// A content change writes a root over a group in steps too: a replacement of
/// `a` whose rows are over the bytes of the committer's group, its files
/// within them, publishes in one publication.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_replacement_of_a_root_over_a_group_publishes_in_steps_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    large_documents(&worker);
    small_group(&commit);
    replace_a(&worker, &commit, &root, 2, "r2", 'A').await;
}

/// `writer restore` writes a root over a group in steps too: back to a
/// replacement of `a`, whose rights changed since, its rows over the bytes of
/// the group the committer has by then, in one publication.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_restore_of_a_root_over_a_group_publishes_in_steps_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    large_documents(&worker);
    large_documents(&commit);
    let target = replace_a(&worker, &commit, &root, 2, "r2", 'A').await;
    replace_a(&worker, &commit, &root, 3, "r3", 'C').await;
    // The rights of `a` change after the target: a restore rewrites its rows.
    input(
        &root,
        json!({"epoch":4,"permissions":[[{"id":"a","revision":"p2","readers":["grp:group","grp:other"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    let text = std::fs::read_to_string(&commit)
        .unwrap()
        .replace("[writer]\ndocument_max_bytes='512KiB'\nenvelope_max_bytes='1MiB'\n[writer.commit]\ngroup_max_bytes='1MiB'\n", "")
        .replace("document_max_bytes='512KiB'", "document_max_bytes='64B'");
    std::fs::write(&commit, text).unwrap();
    small_group(&commit);
    let before = publication(&commit).await.1.sequence;
    command(&commit, &["writer", "restore", &target]);
    assert_eq!(publication(&commit).await.1.sequence, before + 1);
    assert_eq!(revisions(&rows(&worker).await, "a"), ["r2"; 25]);
}

/// Lets `config` prepare documents of 400,000 characters, whose files are small
/// (the text is one letter) and whose rows are over 256 KiB.
fn large_documents(config: &Path) {
    let text = std::fs::read_to_string(config)
        .unwrap()
        .replace("document_max_bytes='64B'", "document_max_bytes='512KiB'")
        .replace(
            "[reader]",
            "[writer]\ndocument_max_bytes='512KiB'\nenvelope_max_bytes='1MiB'\n[writer.commit]\ngroup_max_bytes='1MiB'\n[reader]",
        );
    std::fs::write(config, text).unwrap();
}

/// Gives the committer of `config` a group of 256 KiB.
fn small_group(config: &Path) {
    let text = std::fs::read_to_string(config).unwrap().replace(
        "[reader]",
        "[writer]\ndocument_max_bytes='64KiB'\nenvelope_max_bytes='256KiB'\n[writer.commit]\ngroup_max_bytes='256KiB'\n[reader]",
    );
    std::fs::write(config, text).unwrap();
}

/// Replaces `a` at `revision` with 400,000 `letter`s, 25 records, in one
/// publication; gives its ID.
async fn replace_a(
    worker: &Path,
    commit: &Path,
    root: &FixtureRoot,
    epoch: u64,
    revision: &str,
    letter: char,
) -> String {
    let text = letter.to_string().repeat(400_000);
    let vector = root.embedding.vector("fixture query");
    for chunk in fyrr_writer::chunk::chunks(&text) {
        root.embedding.insert_both(chunk, vector.clone());
    }
    input(
        root,
        json!({"epoch":epoch,"content":[[doc("a",revision,&text)]]}),
    );
    worker_sync(worker, &["incremental", "--kind", "content"]);
    let before = publication(commit).await.1.sequence;
    publish(commit);
    let (_, manifest) = publication(commit).await;
    assert_eq!(manifest.sequence, before + 1, "one publication");
    assert_eq!(revisions(&rows(worker).await, "a"), [revision; 25]);
    manifest.publication_id
}

/// The revision of each row of `root`.
fn revisions(batch: &arrow_array::RecordBatch, root: &str) -> Vec<String> {
    let revisions = batch
        .column_by_name("source_revision")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    roots(batch)
        .iter()
        .enumerate()
        .filter(|(_, id)| *id == root)
        .map(|(row, _)| revisions.value(row).to_owned())
        .collect()
}

/// A source of `a` and `b`, one row of `chars` characters each, read by
/// `grp:group`, published once, its documents up to `document_max`.
async fn published_large_rows(chars: usize, document_max: &str) -> (PathBuf, PathBuf, FixtureRoot) {
    let (worker, commit, root) = fixture();
    let vector = root.embedding.vector("fixture query");
    for c in ["A", "B"] {
        for chunk in fyrr_writer::chunk::chunks(&c.repeat(chars)) {
            root.embedding.insert_both(chunk, vector.clone());
        }
    }
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config).unwrap().replace(
            "document_max_bytes='64B'",
            &format!("document_max_bytes='{document_max}'"),
        );
        std::fs::write(config, text).unwrap();
    }
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1",&"A".repeat(chars)),doc("b","r1",&"B".repeat(chars))]],
            "permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]},{"id":"b","revision":"p1","readers":["grp:group"]}]],
            "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"child","members":["usr:u"]},{"id":"group","members":["grp:child"]}]}]}),
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
    (worker, commit, root)
}

/// A root named twice in a split read, back to the revision that is published
/// (`a`: p1, then p2, then p1), with a revocation on `b`, at 256 KiB: the first
/// part sets `a` to p2, the next sets it back and revokes `b`. The committer
/// rewrites what each part changes, in steps, and publishes the read once,
/// the revocation with it.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_root_named_twice_in_a_split_read_publishes_with_its_revocation_on_storage() {
    let (worker, commit, root) = published_large_rows(3000, "8KiB").await;
    let granted = row_readers(&rows(&worker).await, "a");
    assert!(!granted.is_empty());
    equal_limits(&[&worker, &commit], "256KiB");
    let others = |tag: &str, count: usize| -> Vec<String> {
        (0..count).map(|n| format!("grp:{tag}-{n:04}")).collect()
    };
    let before = permission_envelopes(&worker).await.len();
    input(
        &root,
        json!({"epoch":2,"permissions":[
            [{"id":"a","revision":"p2","readers":others("a", 2000)}],
            [{"id":"b","revision":"p2","readers":others("b", 1300)}],
            [{"id":"a","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts: Vec<_> = permission_envelopes(&worker).await.split_off(before);
    assert_eq!(
        part_shape(&parts),
        [(1, false, false), (2, false, false), (0, true, true)]
    );
    published_in_one(&commit, &parts).await;
    let batch = rows(&worker).await;
    assert_eq!(row_readers(&batch, "a"), granted, "back to p1");
    let b = row_readers(&batch, "b");
    assert_eq!(b.len(), 1300);
    assert!(!b.contains(&granted[0]), "revoked");
}

/// Before the first publication the committer writes the permissions of the
/// content it publishes in steps too: two documents of 150,000 characters at
/// 256 KiB, with the read that grants them, are one first publication.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_the_first_publication_writes_the_rows_of_a_permission_read_in_steps_on_storage()
 {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    let vector = root.embedding.vector("fixture query");
    for c in ["A", "B"] {
        for chunk in fyrr_writer::chunk::chunks(&c.repeat(150_000)) {
            root.embedding.insert_both(chunk, vector.clone());
        }
    }
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config)
            .unwrap()
            .replace("document_max_bytes='64B'", "document_max_bytes='192KiB'")
            .replace(
                "[reader]",
                "[writer]\ndocument_max_bytes='192KiB'\nenvelope_max_bytes='256KiB'\n[writer.commit]\ngroup_max_bytes='256KiB'\n[reader]",
            );
        std::fs::write(config, text).unwrap();
    }
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1",&"A".repeat(150_000))],[doc("b","r1",&"B".repeat(150_000))]],
            "permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]},{"id":"b","revision":"p1","readers":["grp:group"]}]],
            "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"child","members":["usr:u"]},{"id":"group","members":["grp:child"]}]}]}),
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
    let read = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    let receipt = publish(&commit);
    assert!(receipt["publication_id"].is_string(), "{receipt}");
    assert_eq!(resolved(&commit, &read.id).await, (true, false));
    let batch = rows(&worker).await;
    let a = row_readers(&batch, "a");
    assert!(!a.is_empty());
    assert_eq!(row_readers(&batch, "b"), a);
}

/// The configuration of `configs` with an envelope and a group of `limit`.
fn equal_limits(configs: &[&PathBuf], limit: &str) {
    for config in configs {
        let text = std::fs::read_to_string(config).unwrap().replace(
            "[reader]",
            &format!(
                "[writer]\ndocument_max_bytes='8KiB'\nenvelope_max_bytes='{limit}'\n[writer.commit]\ngroup_max_bytes='{limit}'\n[reader]"
            ),
        );
        std::fs::write(config, text).unwrap();
    }
}

/// Cursors count: with an envelope and a group of 256 KiB and connector cursors
/// of 16,000 bytes, two roots that fit a part, start cursor and all, apart and
/// not together are two parts, which carry no end cursor; the envelope that
/// ends the read carries both cursors and no root. Each is within the bound as
/// it encodes, and the read is published in one publication.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_read_with_long_cursors_is_split_as_it_encodes_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    equal_limits(&[&worker, &commit], "256KiB");
    let readers =
        |tag: &str| -> Vec<String> { (0..1500).map(|n| format!("grp:{tag}-{n:04}")).collect() };
    // A read that leaves a long cursor, from which the next one starts.
    input(
        &root,
        json!({"epoch":"x".repeat(15998),"permissions":[[{"id":"a","revision":"p2","readers":readers("x")}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    let before = permission_envelopes(&worker).await.len();
    input(
        &root,
        json!({"epoch":"y".repeat(15998),"permissions":[
            [{"id":"a","revision":"p3","readers":readers("a")}],
            [{"id":"b","revision":"p3","readers":readers("b")}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts: Vec<_> = permission_envelopes(&worker).await.split_off(before);
    assert_eq!(
        part_shape(&parts),
        [(1, false, false), (1, false, false), (0, true, true)]
    );
    assert!(
        parts[0].cursor.end.is_empty()
            && parts[1].cursor.end.is_empty()
            && parts[2].cursor.end.len() >= 16000
    );
    let loaded = fyrr_core::config::load(&worker).unwrap();
    for part in &parts {
        assert!(part.cursor.start.as_ref().unwrap().len() >= 16000);
        assert!(
            write_contract(part).len() as u64 <= loaded.config.writer.envelope_max_bytes.as_u64()
        );
    }
    published_in_one(&commit, &parts).await;
    let batch = rows(&worker).await;
    assert_eq!(row_readers(&batch, "a").len(), 1500);
    assert_eq!(row_readers(&batch, "b").len(), 1500);
}

/// The requests of `rounds` commits of `commit` that write to storage, as an
/// observer between the committer and its storage sees them.
fn observed_writes(commit: &Path, rounds: usize) -> Vec<String> {
    use std::sync::{Arc, Mutex, atomic::AtomicBool};
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let writes = Arc::new(Mutex::new(Vec::new()));
    let seen = writes.clone();
    let hook: super::corrections::BoundaryHook = Arc::new(move |request, _, after| {
        let line = request.lines().next().unwrap_or_default();
        if !after
            && ["PUT ", "POST ", "DELETE ", "PATCH "]
                .iter()
                .any(|m| line.starts_with(m))
        {
            seen.lock().unwrap().push(line.to_owned());
        }
        false
    });
    let stop = Arc::new(AtomicBool::new(false));
    let (proxy, guard) =
        super::corrections::proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
    let proxied = commit.parent().unwrap().join("observed.toml");
    std::fs::write(
        &proxied,
        std::fs::read_to_string(commit)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    for _ in 0..rounds {
        let receipt = publish(&proxied);
        assert!(receipt["publication_id"].is_null(), "{receipt}");
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    drop(guard);
    writes.lock().unwrap().clone()
}

/// Before the first content, permissions envelopes wait for rows to be checked
/// against: a commit with nothing else to do writes nothing to storage, however
/// often it runs. One that is to be rejected, over the bytes of the
/// committer's group, is judged with the others: its rejection is published
/// once, and the commits after it write nothing.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_commit_with_only_waiting_permissions_writes_nothing_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    input(
        &root,
        json!({"epoch":1,"permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    let writes = observed_writes(&commit, 2);
    assert!(writes.is_empty(), "an idle commit wrote {writes:?}");
    let waiting = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    assert_eq!(
        resolved(&commit, &waiting.id).await,
        (false, false),
        "it waits"
    );

    // A newer read, 270 KB under the worker's bounds, over the committer's 256 KiB.
    let readers: Vec<String> = (0..3300).map(|n| format!("grp:wide-{n:04}")).collect();
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"b","revision":"p1","readers":readers}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let over = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    equal_limits(&[&commit], "256KiB");
    publish(&commit);
    assert!(
        resolved(&commit, &over.id).await.1,
        "its rejection is published"
    );
    let writes = observed_writes(&commit, 2);
    assert!(
        writes.is_empty(),
        "a commit after the rejection wrote {writes:?}"
    );

    // A read that replaces it waits too, beside a members snapshot that is
    // over the age of an envelope: the snapshot is rejected in the same commit.
    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"a","revision":"p2","readers":["grp:group"]}]],
            "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"group","members":["usr:u"]}]}]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "permissions", "--kind", "members"],
    );
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let listed: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(
        object_store::ObjectStore::list(store.as_ref(), Some(&ns.join("envelopes"))),
    )
    .await
    .unwrap();
    let mut members = None;
    for object in listed {
        let mut envelope: PreparedEnvelope =
            fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                .await
                .unwrap();
        if envelope.kind == SyncKind::Members && envelope.source_id == "incremental" {
            envelope.created_at = fyrr_core::clock::now() - chrono::Duration::days(4);
            store
                .put(&object.location, write_contract(&envelope).into())
                .await
                .unwrap();
            members = Some(envelope.envelope_id);
        }
    }
    let members = members.expect("a members envelope");
    publish(&commit);
    assert!(
        resolved(&commit, &members).await.1,
        "the old snapshot is rejected"
    );
    let writes = observed_writes(&commit, 2);
    assert!(
        writes.is_empty(),
        "a commit after the rejections wrote {writes:?}"
    );
}

/// Waiting permissions keep no other kind from being committed before the
/// first content: a valid members snapshot behind them is applied, its input
/// in the identity the head serves. (Before the first content no manifest
/// records it as applied: it stays pending, as on main.)
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_members_behind_waiting_permissions_are_applied_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    input(
        &root,
        json!({"epoch":1,
            "permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]}]],
            "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"group","members":["usr:u"]}]}]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "members"]);
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    assert!(
        !identity_inputs_of(&commit)
            .await
            .contains(&"incremental".to_owned())
    );
    publish(&commit);
    assert!(
        identity_inputs_of(&commit)
            .await
            .contains(&"incremental".to_owned()),
        "the snapshot is applied"
    );
}

/// Before the first content, waiting permissions and a members snapshot change
/// nothing a commit publishes once the identity is: the commits after the
/// first take no lease and write nothing, and the envelopes stay pending.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_commit_with_waiting_permissions_and_members_writes_nothing_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    input(
        &root,
        json!({"epoch":1,
            "permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]}]],
            "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"group","members":["usr:u"]}]}]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "permissions", "--kind", "members"],
    );
    publish(&commit);
    assert!(
        identity_inputs_of(&commit)
            .await
            .contains(&"incremental".to_owned()),
        "the snapshot is applied to the identity"
    );
    let writes = observed_writes(&commit, 3);
    assert!(writes.is_empty(), "an idle commit wrote {writes:?}");
}

/// A content envelope that only deletes waits behind the rejection of the
/// permissions of its source, before the first content: once the rejection is
/// published, the commits after it take no lease and write nothing.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_commit_with_only_waiting_deletes_writes_nothing_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    input(
        &root,
        json!({"epoch":1,"content":[[{"id":"gone","delete":true,"revision":"r1"}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let readers: Vec<String> = (0..3300).map(|n| format!("grp:wide-{n:04}")).collect();
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"b","revision":"p1","readers":readers}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let over = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    equal_limits(&[&commit], "256KiB");
    publish(&commit);
    assert!(
        resolved(&commit, &over.id).await.1,
        "the envelope over the bounds is rejected"
    );
    let writes = observed_writes(&commit, 3);
    assert!(writes.is_empty(), "an idle commit wrote {writes:?}");

    // Older than an envelope may be, the deletes still wait for the read that
    // replaces the rejected one: nothing in a commit changes.
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let listed: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(
        object_store::ObjectStore::list(store.as_ref(), Some(&ns.join("envelopes"))),
    )
    .await
    .unwrap();
    for object in listed {
        let mut envelope: PreparedEnvelope =
            fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                .await
                .unwrap();
        if envelope.kind == SyncKind::Content {
            envelope.created_at = fyrr_core::clock::now() - chrono::Duration::days(4);
            store
                .put(&object.location, write_contract(&envelope).into())
                .await
                .unwrap();
        }
    }
    let writes = observed_writes(&commit, 3);
    assert!(writes.is_empty(), "a commit past the age wrote {writes:?}");
}

/// A complete listing waits for the first content, which it reconciles
/// against: commits write nothing after the first.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_commit_with_only_a_waiting_listing_writes_nothing_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    input(
        &root,
        json!({"epoch":1,"listing":[{"last":true,"complete":true,"roots":[]}]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "reconcile", "--full"]);
    publish(&commit);
    let writes = observed_writes(&commit, 3);
    assert!(writes.is_empty(), "an idle commit wrote {writes:?}");
}

/// A content envelope that only deletes, beside waiting permissions and one
/// over the committer's 256 KiB group, before the first content: the envelope
/// over the bounds is rejected in the first commit. (The deletes then wait for
/// a read that replaces the rejected one, as a rejection of the permissions of
/// a source holds its content; without the rejection they are published with
/// the waiting permissions in the first commit.)
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_deletes_beside_waiting_permissions_keep_no_rejection_back_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    input(
        &root,
        json!({"epoch":1,
            "content":[[{"id":"gone","delete":true,"revision":"r1"}]],
            "permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let readers: Vec<String> = (0..3300).map(|n| format!("grp:wide-{n:04}")).collect();
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"b","revision":"p1","readers":readers}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let over = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    equal_limits(&[&commit], "256KiB");
    publish(&commit);
    assert!(
        resolved(&commit, &over.id).await.1,
        "the envelope over the bounds is rejected"
    );
}

/// Waiting permissions are judged whatever else is chosen: before the first
/// content, beside a members snapshot over the committer's one-row group, a
/// permissions part of two roots, over it too, is rejected in the first
/// commit.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_waiting_permissions_are_judged_beside_an_oversized_snapshot_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    input(
        &root,
        json!({"epoch":1,
            "permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]},{"id":"b","revision":"p1","readers":["grp:group"]}]],
            "members":[{"users":[{"id":"u","email":"fixture@example.test","email_verification":"verified_corporate","active":true}],"groups":[{"id":"group","members":["usr:u"]}]}]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "members"]);
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let read = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    let text = std::fs::read_to_string(&commit).unwrap().replace(
        "[reader]",
        "[writer]\nenvelope_max_rows=1\n[writer.commit]\ngroup_max_rows=1\n[reader]",
    );
    std::fs::write(&commit, text).unwrap();
    publish(&commit);
    assert!(
        resolved(&commit, &read.id).await.1,
        "the read over the group is rejected in the first commit"
    );
}

/// The bound of an envelope is the committer's, inclusive, on the envelope as
/// it is put, and a root is judged when it is seen by the largest part of its
/// read: a `previous` and a sequence of the most digits. Two roots seen in one
/// batch, before any part of the read, each fit that bound exactly: the first
/// part has no `previous`, the second has one, and both are put and publish.
/// At one byte less the read is refused when the first root is seen, and
/// nothing is put.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_permission_envelope_of_exactly_the_bound_publishes_on_storage() {
    let (worker, commit, root) = fixture();
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    equal_limits(&[&worker, &commit], "1MiB");
    // Roots of the same size: the same revision, ids and readers of one length.
    let readers = |tag: &str| -> Vec<String> {
        (0..2500)
            .map(|n| format!("grp:exact-{tag}-{n:04}"))
            .collect()
    };
    let entry = |id: &str| json!({"id":id,"revision":"p2","readers":readers(id)});
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = |id: &str| ns.clone().join("envelopes").join(format!("{id}.json"));
    let cursor = ns
        .clone()
        .join("cursors")
        .join("incremental-permissions.json");
    // The parts of the read the cursor ends, oldest first, and the ending.
    let read = || async {
        let mut id = cursor_of(&worker, SyncKind::Permissions)
            .await
            .last
            .map(|last| last.id);
        let mut chain = Vec::new();
        while let Some(next) = id {
            let envelope: PreparedEnvelope =
                fyrr_reader::storage::read_contract(store.as_ref(), &path(&next))
                    .await
                    .unwrap();
            id = envelope.previous.clone();
            chain.push(envelope);
        }
        chain.reverse();
        chain
    };
    // The largest part of a read with one root: the part a first read puts,
    // with a `previous` and a sequence of the most digits.
    input(&root, json!({"epoch":2,"permissions":[[entry("a")]]}));
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let first = read().await;
    let mut largest = first[0].clone();
    largest.sequence = u64::MAX;
    largest.previous = Some(first[1].envelope_id.clone());
    let bound = write_contract(&largest).len();
    // The read is undone, and made again under bounds of that size.
    for envelope in &first {
        store.delete(&path(&envelope.envelope_id)).await.unwrap();
    }
    store.delete(&cursor).await.unwrap();
    let limit = |limit: usize| {
        for config in [&worker, &commit] {
            let text = std::fs::read_to_string(config).unwrap();
            let text = text
                .lines()
                .map(|line| {
                    if line.starts_with("envelope_max_bytes=")
                        || line.starts_with("group_max_bytes=")
                    {
                        let key = line.split('=').next().unwrap();
                        format!("{key}='{limit}B'")
                    } else {
                        line.to_owned()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            std::fs::write(config, text + "\n").unwrap();
        }
    };
    input(
        &root,
        json!({"epoch":3,"permissions":[[entry("a"), entry("b")]]}),
    );
    limit(bound - 1);
    let refused = worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "permissions"])
        .output()
        .unwrap();
    assert!(!refused.status.success(), "one byte less refuses the read");
    let said = String::from_utf8_lossy(&refused.stdout).into_owned()
        + &String::from_utf8_lossy(&refused.stderr);
    assert!(said.contains("permission_root_oversized"), "{said}");
    let listed: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(
        object_store::ObjectStore::list(store.as_ref(), Some(&ns.clone().join("envelopes"))),
    )
    .await
    .unwrap();
    for object in listed {
        let envelope: PreparedEnvelope =
            fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                .await
                .unwrap();
        assert!(
            envelope.source_id != "incremental" || envelope.kind != SyncKind::Permissions,
            "nothing of the refused read is put"
        );
    }
    limit(bound);
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts = read().await;
    assert_eq!(
        part_shape(&parts),
        [(1, false, false), (1, false, false), (0, true, true)]
    );
    assert!(parts[0].previous.is_none() && parts[1].previous.is_some());
    for part in &parts {
        assert!(write_contract(part).len() <= bound);
    }
    published_in_one(&commit, &parts).await;
    let batch = rows(&worker).await;
    assert_eq!(row_readers(&batch, "a").len(), 2500);
    assert_eq!(row_readers(&batch, "b").len(), 2500);
}

/// A missing cursor is not an empty one: a source whose cursor is gone while
/// envelopes it chained wait, before the first publication, is blocked, its
/// content waits for the permission change, and `writer status` lists it; a
/// worker that starts meanwhile makes no new cursor. The supported recovery,
/// a purge and a fresh sync under a new source ID, publishes. After a
/// publication, a cursor that goes blocks its source in `writer status` with
/// nothing pending, a worker refuses it and writes nothing, and purging the
/// source ends the block.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_missing_cursor_blocks_its_source_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    initial(&root);
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let cursor = |source: &str| {
        ns.clone()
            .join("cursors")
            .join(format!("{source}-permissions.json"))
    };
    store.delete(&cursor("incremental")).await.unwrap();
    // The envelopes in the log, before any publication too.
    let logged = async || {
        use futures::TryStreamExt;
        let listed: Vec<_> = store
            .list(Some(&ns.clone().join("envelopes")))
            .try_collect()
            .await
            .unwrap();
        listed.len()
    };
    let blocked = |source: &str| {
        let status = command(&commit, &["writer", "status"]);
        assert_eq!(status["blocked_sources"], json!([source]), "{status}");
    };
    blocked("incremental");
    // A worker refuses the source: it makes no new cursor and writes nothing,
    // and the source stays blocked.
    let refused = |source: &str| {
        let output = worker_command(&worker)
            .args(["writer", "sync", source, "--kind", "permissions"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        let said = String::from_utf8_lossy(&output.stdout).into_owned()
            + &String::from_utf8_lossy(&output.stderr);
        assert!(said.contains("permission_cursor_lost"), "{said}");
    };
    let before = logged().await;
    refused("incremental");
    assert!(
        store.head(&cursor("incremental")).await.is_err(),
        "no cursor stands in for the lost one"
    );
    assert_eq!(logged().await, before);
    blocked("incremental");
    publish(&commit);
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    assert!(
        head.content.is_none(),
        "the content waits for its permissions"
    );
    // The recovery: purge the source and sync it afresh under a new ID.
    command(
        &commit,
        &["writer", "source", "purge", "incremental", "--confirm"],
    );
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["blocked_sources"], json!([]), "{status}");
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config).unwrap();
        assert!(text.contains("id='incremental'"));
        std::fs::write(config, text.replace("id='incremental'", "id='fresh'")).unwrap();
    }
    worker_sync(
        &worker,
        &["fresh", "--kind", "content", "--kind", "permissions"],
    );
    publish(&commit);
    assert!(!row_readers(&source_rows(&worker, "fresh").await, "a").is_empty());
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["blocked_sources"], json!([]), "{status}");
    // After a publication, with nothing pending.
    store.delete(&cursor("fresh")).await.unwrap();
    blocked("fresh");
    let before = logged().await;
    refused("fresh");
    assert!(
        store.head(&cursor("fresh")).await.is_err(),
        "no cursor is made"
    );
    assert_eq!(logged().await, before);
    // Purging the source ends the block.
    command(
        &commit,
        &["writer", "source", "purge", "fresh", "--confirm"],
    );
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["blocked_sources"], json!([]), "{status}");
}

/// A restore between a confirmed split read and the next commit: the restore
/// keeps the rights of every row, the read's revocation is published whole or
/// not at all, the chain stays whole (the source is not blocked, nothing waits),
/// and the next read publishes whole, in one publication.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_restore_after_a_confirmed_split_read_keeps_the_chain_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let granted = row_readers(&rows(&worker).await, "a");
    assert!(!granted.is_empty());
    let target = publication(&commit).await.1.publication_id;
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    // One root to a part: the revocation of `a` and `b` is a split read.
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config)
            .unwrap()
            .replace("[reader]", "[writer]\nenvelope_max_rows=1\n[reader]");
        std::fs::write(config, text).unwrap();
    }
    let before = permission_envelopes(&worker).await.len();
    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"a","revision":"p2","readers":[]}],[{"id":"b","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts: Vec<_> = permission_envelopes(&worker).await.split_off(before);
    assert_eq!(
        part_shape(&parts),
        [(1, false, false), (1, false, false), (0, true, true)]
    );
    command(&commit, &["writer", "restore", &target]);
    let rights =
        |batch: &arrow_array::RecordBatch| (row_readers(batch, "a"), row_readers(batch, "b"));
    let after = rights(&rows(&worker).await);
    assert!(
        after == (granted.clone(), granted.clone()) || after == (Vec::new(), Vec::new()),
        "never a part of the read: {after:?}"
    );
    publish(&commit);
    assert_eq!(
        rights(&rows(&worker).await),
        (Vec::new(), Vec::new()),
        "the revocation is published whole"
    );
    for part in &parts {
        assert_eq!(resolved(&commit, &part.envelope_id).await, (true, false));
    }
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["blocked_sources"], json!([]), "{status}");
    assert_eq!(status["pending_envelopes"], json!(0), "{status}");
    // The chain goes on from the restored head: a read after it publishes whole.
    let before = permission_envelopes(&worker).await.len();
    input(
        &root,
        json!({"epoch":4,"permissions":[[{"id":"a","revision":"p3","readers":["grp:group"]}],[{"id":"b","revision":"p3","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let parts: Vec<_> = permission_envelopes(&worker).await.split_off(before);
    published_in_one(&commit, &parts).await;
    assert_eq!(rights(&rows(&worker).await), (granted.clone(), granted));
}

/// A new cursor follows every lease generation of the log: after the cursor of
/// a source whose read was rejected is gone, with no chain left to confirm, a
/// read again takes a generation above the old one, so its envelopes are never
/// those of the old chain, and is put.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_new_cursor_follows_the_lease_generations_of_the_log_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    input(
        &root,
        json!({"epoch":1,"permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]},{"id":"b","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let old = cursor_of(&worker, SyncKind::Permissions).await;
    let old_generation = old.lease.as_ref().unwrap().epoch;
    let text = std::fs::read_to_string(&commit).unwrap().replace(
        "[reader]",
        "[writer]\nenvelope_max_rows=1\n[writer.commit]\ngroup_max_rows=1\n[reader]",
    );
    std::fs::write(&commit, text).unwrap();
    publish(&commit);
    let old_last = old.last.unwrap();
    assert!(
        resolved(&commit, &old_last.id).await.1,
        "the read is rejected"
    );
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    store
        .delete(&ns.join("cursors").join("incremental-permissions.json"))
        .await
        .unwrap();
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let new = cursor_of(&worker, SyncKind::Permissions).await;
    assert!(
        new.lease.as_ref().unwrap().epoch > old_generation,
        "{} after {old_generation}",
        new.lease.as_ref().unwrap().epoch
    );
    assert_ne!(new.last.unwrap().id, old_last.id);
}

/// A copy has no chain and nothing applied: its sources read again from the start.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_copy_clears_the_chain_of_the_cursors_on_storage() {
    let (worker, commit, root) = fixture();
    // A namespace with identity inputs, which members make, is not copied.
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1","fixture document 0")]],"permissions":[[{"id":"a","revision":"p1","readers":["grp:group"]}]]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    publish(&commit);
    assert!(
        cursor_of(&worker, SyncKind::Permissions)
            .await
            .last
            .is_some()
    );
    assert!(!publication(&commit).await.1.chain_applied.is_empty());
    let to = super::namespace_copy::elsewhere(&worker, "chain-copy", None);
    super::namespace_copy::copied(&worker, &to);
    let copied = cursor_of(&to, SyncKind::Permissions).await;
    assert!(copied.last.is_none() && copied.cursor.is_none());
    assert!(copied.sequence > 0, "the numeric sequence is kept");
    assert!(publication(&to).await.1.chain_applied.is_empty());
    let _ = root;
}

/// The cursor of the `incremental` source's `kind`, changed by `change` and
/// written back as it was before a worker held it.
async fn change_cursor(
    config: &Path,
    kind: SyncKind,
    change: impl FnOnce(&mut fyrr_core::connector::CursorState),
) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns
        .join("cursors")
        .join(format!("incremental-{}.json", kind.as_str()));
    let mut state = cursor_of(config, kind).await;
    change(&mut state);
    store
        .put(&path, write_contract(&state).into())
        .await
        .unwrap();
}

/// `id` as old as `days`.
async fn age(config: &Path, id: &str, days: i64) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, _) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let (path, mut envelope) = envelopes(config)
        .await
        .into_iter()
        .find(|(_, e)| e.envelope_id == id)
        .unwrap();
    envelope.created_at = fyrr_core::clock::now() - chrono::Duration::days(days);
    store
        .put(&path, write_contract(&envelope).into())
        .await
        .unwrap();
}

/// A source whose chain is broken holds its content too: a revision does not
/// publish with rights that a missing link may have revoked, and it does once
/// the chain is whole.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_broken_chain_holds_the_content_of_its_source_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"b","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let links = permission_envelopes(&worker).await;
    let [.., first, _second] = &links[..] else {
        unreachable!()
    };
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns
        .clone()
        .join("envelopes")
        .join(format!("{}.json", first.envelope_id));
    let kept = store.get(&path).await.unwrap().bytes().await.unwrap();
    store.delete(&path).await.unwrap();
    input(
        &root,
        json!({"epoch":4,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let batch = rows(&worker).await;
    assert!(
        fyrr_writer::apply::strings(&batch, "source_revision")
            .unwrap()
            .iter()
            .all(|r| r == Some("r1")),
        "the content waits for the chain"
    );
    assert!(!row_readers(&batch, "a").is_empty());

    store.put(&path, kept.into()).await.unwrap();
    publish(&commit);
    let batch = rows(&worker).await;
    let row = roots(&batch).iter().position(|id| id == "a").unwrap();
    assert_eq!(
        fyrr_writer::apply::strings(&batch, "source_revision")
            .unwrap()
            .value(row),
        "r2"
    );
    assert!(row_readers(&batch, "a").is_empty() && row_readers(&batch, "b").is_empty());
}

/// Cleanup keeps what the committer walks: the chain the cursor held when
/// cleanup read it, and an envelope confirmed after that. An envelope off the
/// chain, of an older lease generation, goes.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_cleanup_keeps_the_chain_and_what_a_cursor_confirmed_after_it_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    for (epoch, id) in [(2, "a"), (3, "b")] {
        input(
            &root,
            json!({"epoch":epoch,"permissions":[[{"id":id,"revision":"p2","readers":[]}]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    }
    let links = permission_envelopes(&worker).await;
    let [.., first, second] = &links[..] else {
        unreachable!()
    };
    let cursor = cursor_of(&worker, SyncKind::Permissions).await;
    assert!(second.lease_generation >= cursor.lease.as_ref().unwrap().epoch);
    // The cursor as cleanup read it, before the worker confirmed the second.
    let held = first.clone();
    change_cursor(&worker, SyncKind::Permissions, |state| {
        state.last = Some(fyrr_core::connector::LastEnvelope {
            sequence: held.sequence,
            id: held.envelope_id.clone(),
        });
    })
    .await;
    let dead = put_permission_envelope(&worker, cursor.sequence + 5, None, 1, &[("b", "p7")]).await;
    for id in [&first.envelope_id, &second.envelope_id, &dead.envelope_id] {
        age(&worker, id, 4).await;
    }
    command(&commit, &["writer", "maintain"]);
    let left: Vec<_> = envelopes(&worker)
        .await
        .into_iter()
        .map(|(_, e)| e.envelope_id)
        .collect();
    assert!(
        left.contains(&first.envelope_id),
        "on the walk of the cursor"
    );
    assert!(
        left.contains(&second.envelope_id),
        "of the lease generation of the cursor"
    );
    assert!(
        !left.contains(&dead.envelope_id),
        "off the chain, of an older generation"
    );
}

/// A source with content pending is checked against its cursor before its first
/// permissions envelope is applied too: the revoke that its cursor confirmed is
/// not there, and the content does not publish with the rights it may have revoked.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_content_waits_for_a_confirmed_revoke_before_any_envelope_was_applied_on_storage()
 {
    let (worker, commit, root) = fixture();
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1","fixture document 0")]],
            "permissions":[[{"id":"a","revision":"p1","readers":[]}]]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    let revoke = permission_envelopes(&worker).await.pop().unwrap();
    assert!(
        publication(&commit)
            .await
            .1
            .chain_applied
            .get("incremental")
            .is_none_or(|kinds| !kinds.contains_key(&SyncKind::Permissions))
    );
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns
        .join("envelopes")
        .join(format!("{}.json", revoke.envelope_id));
    let kept = store.get(&path).await.unwrap().bytes().await.unwrap();
    store.delete(&path).await.unwrap();
    publish(&commit);
    assert!(
        source_roots(&worker, "incremental").await.is_empty(),
        "the content waits for the revoke its cursor confirmed"
    );
    // What waits behind a chain with no tail is not pending work: status says what
    // the committer does.
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        command(&commit, &["writer", "status"])["pending_envelopes"],
        fyrr_writer::commit::pending(&loaded.config, &head)
            .await
            .unwrap()
            .len()
    );
    store.put(&path, kept.into()).await.unwrap();
    publish(&commit);
    let batch = rows(&worker).await;
    assert!(row_readers(&batch, "a").is_empty());
}

/// A source whose chain is broken holds its listings too: a listing that does
/// not name a document does not delete it, and does once the chain is whole.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_broken_chain_holds_the_listings_of_its_source_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    for (epoch, id) in [(2, "a"), (3, "b")] {
        input(
            &root,
            json!({"epoch":epoch,"permissions":[[{"id":id,"revision":"p2","readers":[]}]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    }
    let first = permission_envelopes(&worker)
        .await
        .into_iter()
        .rev()
        .nth(1)
        .unwrap();
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns
        .join("envelopes")
        .join(format!("{}.json", first.envelope_id));
    let kept = store.get(&path).await.unwrap().bytes().await.unwrap();
    store.delete(&path).await.unwrap();
    input(
        &root,
        json!({"epoch":4,"listing":[{"last":true,"complete":true,"roots":[]}]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "reconcile", "--full"]);
    publish(&commit);
    assert_eq!(source_roots(&worker, "incremental").await, ["a", "b"]);
    store.put(&path, kept.into()).await.unwrap();
    publish(&commit);
    assert!(source_roots(&worker, "incremental").await.is_empty());
}

/// An envelope that is refused contributes nothing: a permission read whose row
/// is over the bytes of a group is rejected before its container grants or rows
/// are prepared, so the commit publishes none of its ACL, and goes on.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_refused_permission_envelope_publishes_none_of_its_grants_on_storage() {
    let (worker, commit, root) = fixture();
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    small_groups(&[&commit]);
    // Readers that, as the row of `a` states them, are over the bytes of a group.
    let readers = many_readers();
    input(
        &root,
        json!({"epoch":2,
            "permissions":[[{"id":"a","revision":"p2","readers":readers}]],
            "container_grants":[[{"container_id":"folder","container_kind":"folder","permission_revision":"c1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let read = permission_envelopes(&worker).await.pop().unwrap();
    let before = publication(&commit).await.0.identity;
    publish(&commit);
    assert!(
        resolved(&commit, &read.envelope_id).await.1,
        "refused with a marker"
    );
    let (head, _) = publication(&commit).await;
    assert_eq!(
        head.identity, before,
        "none of its container grants was published"
    );
    assert!(!row_readers(&rows(&worker).await, "a").is_empty());
    // The commit goes on: another source publishes.
    std::fs::write(root.join("sources/0.txt"), "fixture document 6").unwrap();
    worker_sync(&worker, &["allowed", "--kind", "content"]);
    publish(&commit);
    let control = source_rows(&worker, "allowed").await;
    let row = roots(&control).iter().position(|id| id == "0.txt").unwrap();
    assert_eq!(
        fyrr_writer::apply::strings(&control, "searchable_text")
            .unwrap()
            .value(row),
        "fixture document 6"
    );
}

/// Content waits for a source whose chain has a tail that is not there, even
/// when no permissions envelope of it is pending: the cursor holds an envelope
/// that is not yet visible, and a revision does not publish with its rights.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_content_waits_for_a_chain_whose_tail_is_missing_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let tail = permission_envelopes(&worker).await.pop().unwrap();
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns
        .join("envelopes")
        .join(format!("{}.json", tail.envelope_id));
    let kept = store.get(&path).await.unwrap().bytes().await.unwrap();
    store.delete(&path).await.unwrap();
    input(
        &root,
        json!({"epoch":3,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let batch = rows(&worker).await;
    assert!(
        fyrr_writer::apply::strings(&batch, "source_revision")
            .unwrap()
            .iter()
            .all(|r| r == Some("r1")),
        "the content waits for the tail"
    );
    store.put(&path, kept.into()).await.unwrap();
    publish(&commit);
    let batch = rows(&worker).await;
    let row = roots(&batch).iter().position(|id| id == "a").unwrap();
    assert_eq!(
        fyrr_writer::apply::strings(&batch, "source_revision")
            .unwrap()
            .value(row),
        "r2"
    );
    assert!(row_readers(&batch, "a").is_empty());
}

/// The same on the first publication, which has no dataset to check before the
/// envelopes are prepared: the refused permissions envelope contributes none of
/// its container grants, and the commit goes on without it.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_permission_envelope_refused_in_the_first_publication_publishes_no_grant_on_storage()
 {
    let (worker, commit, root) = fixture_host(768, false);
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    small_groups(&[&commit]);
    let readers = many_readers();
    input(
        &root,
        json!({"epoch":2,
            "permissions":[[{"id":"a","revision":"p2","readers":readers}]],
            "container_grants":[[{"container_id":"folder","container_kind":"folder","permission_revision":"c1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let read = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    // The first round publishes the rejection alone; the next, the rest.
    publish(&commit);
    publish(&commit);
    assert!(resolved(&commit, &read.id).await.1, "refused with a marker");
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let head = publication(&commit).await.0;
    let identity: fyrr_core::identity::IdentityArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{ns}/{}", head.identity.object)),
    )
    .await
    .unwrap();
    assert!(
        !identity
            .inputs
            .iter()
            .any(|input| input.id == "permissions:incremental"),
        "none of its container grants was published"
    );
    // The commit went on: the other sources are published, and the content of
    // this one waits for a read that replaces the refused one.
    assert!(!source_roots(&worker, "allowed").await.is_empty());
}

/// A cursor that lost its `last` while the manifest has a chain holds the
/// content of its source, and does not open it.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_content_waits_for_a_cursor_that_lost_its_chain_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let kept = cursor_of(&worker, SyncKind::Permissions).await.last;
    assert!(kept.is_some());
    change_cursor(&worker, SyncKind::Permissions, |state| state.last = None).await;
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r2","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let revision = |batch: &arrow_array::RecordBatch| {
        let row = roots(batch).iter().position(|id| id == "a").unwrap();
        fyrr_writer::apply::strings(batch, "source_revision")
            .unwrap()
            .value(row)
            .to_owned()
    };
    assert_eq!(revision(&rows(&worker).await), "r1", "the content waits");
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(
        status["blocked_sources"],
        json!(["incremental"]),
        "{status}"
    );
    change_cursor(&worker, SyncKind::Permissions, |state| state.last = kept).await;
    publish(&commit);
    assert_eq!(revision(&rows(&worker).await), "r2");
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["blocked_sources"], json!([]), "{status}");
}

/// The envelopes of the sources other than `incremental` are removed from the
/// log of a namespace that has no publication, which leaves it one source.
async fn drop_other_sources(config: &Path) {
    use futures::TryStreamExt;
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let listed: Vec<_> = store
        .list(Some(&ns.join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    for object in listed {
        let envelope: PreparedEnvelope =
            fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                .await
                .unwrap();
        if envelope.source_id != "incremental" {
            store.delete(&object.location).await.unwrap();
        }
    }
}

/// The identity inputs of the head of a namespace, whether or not it has a publication.
async fn identity_inputs_of(config: &Path) -> Vec<String> {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let identity: fyrr_core::identity::IdentityArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{ns}/{}", head.identity.object)),
    )
    .await
    .unwrap();
    identity.inputs.into_iter().map(|input| input.id).collect()
}

/// Readers that, as the row of `a` states them, are over the bytes of a group.
fn many_readers() -> Vec<String> {
    (0..4000).map(|n| format!("grp:reader-{n:05}")).collect()
}

/// The configuration that makes `many_readers` a row over the bytes of a group,
/// for the committer: the worker, under the default bounds, writes the envelope
/// that the committer refuses, as one prepared under larger bounds.
fn small_groups(configs: &[&PathBuf]) {
    for config in configs {
        let text = std::fs::read_to_string(config).unwrap().replace(
            "[reader]",
            "[writer]\ndocument_max_bytes='64KiB'\nenvelope_max_bytes='256KiB'\n[writer.commit]\ngroup_max_bytes='256KiB'\n[reader]",
        );
        std::fs::write(config, text).unwrap();
    }
}

/// A source alone in a namespace without a publication: the permissions envelope
/// of its first read is refused, and a valid replacement still lets the first
/// publication happen, with the content that waited for it.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_valid_replacement_lets_the_first_publication_of_a_lone_source_happen_on_storage()
 {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    initial(&root);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    small_groups(&[&commit]);
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":many_readers()}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let refused = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    publish(&commit);
    publish(&commit);
    assert!(resolved(&commit, &refused.id).await.1);
    assert!(
        fyrr_reader::storage::optional_head(&fyrr_core::config::load(&commit).unwrap().config)
            .await
            .unwrap()
            .unwrap()
            .content
            .is_none(),
        "the content waits for a replacement"
    );
    input(
        &root,
        json!({"epoch":3,"permissions":[[{"id":"a","revision":"p3","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    publish(&commit);
    let batch = rows(&worker).await;
    assert!(row_readers(&batch, "a").is_empty());
    assert_eq!(source_roots(&worker, "incremental").await, ["a", "b"]);
}

/// Permissions that arrive before the first content have no rows to be checked
/// against: their container grants are not published, and are not once the
/// envelope is refused at the first publication.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_container_grant_before_the_first_content_is_not_published_on_storage() {
    let (worker, commit, root) = fixture_host(768, false);
    drop_other_sources(&worker).await;
    initial(&root);
    small_groups(&[&commit]);
    input(
        &root,
        json!({"epoch":2,
            "permissions":[[{"id":"a","revision":"p2","readers":many_readers()}]],
            "container_grants":[[{"container_id":"folder","container_kind":"folder","permission_revision":"c1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let read = cursor_of(&worker, SyncKind::Permissions)
        .await
        .last
        .unwrap();
    publish(&commit);
    assert!(
        !identity_inputs_of(&commit)
            .await
            .contains(&"permissions:incremental".to_owned()),
        "no grant before there are rows to check"
    );
    // The content arrives, and the envelope is refused when its rows are written.
    input(
        &root,
        json!({"epoch":3,"content":[[doc("a","r1","fixture document 0")]],
            "permissions":[[{"id":"a","revision":"p2","readers":many_readers()}]],
            "container_grants":[[{"container_id":"folder","container_kind":"folder","permission_revision":"c1","readers":["grp:group"]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    publish(&commit);
    assert!(resolved(&commit, &read.id).await.1, "refused with a marker");
    assert!(
        !identity_inputs_of(&commit)
            .await
            .contains(&"permissions:incremental".to_owned())
    );
}

/// The rejection marker of `envelope`, as a committer writes it.
async fn put_marker(config: &Path, envelope: &PreparedEnvelope) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let marker = fyrr_core::envelope::RejectionMarker {
        schema_version: fyrr_core::envelope::ENVELOPE_CONTRACT_VERSION,
        envelope_id: envelope.envelope_id.clone(),
        source_id: envelope.source_id.clone(),
        kind: envelope.kind,
        sequence: envelope.sequence,
        rejection_generation: envelope.rejection_generation,
        restore_sequence: envelope.restore_sequence,
        start_cursor: envelope.cursor.start.clone(),
        reason: "authorization_rewind_required".into(),
    };
    store
        .put(
            &ns.join("rejections")
                .join(format!("{}.json", envelope.envelope_id)),
            write_contract(&marker).into(),
        )
        .await
        .unwrap();
}

/// No publication rejected a permissions envelope and left an older one of its
/// source to be resolved by a later one: the worker may read between them, from
/// the start of the one that was rejected, and the older read would be lost.
async fn assert_rejections_whole(config: &Path) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let mut chain: Vec<_> =
        futures::TryStreamExt::try_collect(fyrr_reader::storage::manifest_chain(
            store.clone(),
            ns.clone(),
            head.content.as_ref().map(|p| p.manifest.clone()),
        ))
        .await
        .unwrap();
    chain.reverse();
    let mut resolved_at = std::collections::BTreeMap::new();
    for (index, manifest) in chain.iter().enumerate() {
        for id in manifest
            .envelope_ids
            .iter()
            .chain(&manifest.rejected_envelope_ids)
        {
            resolved_at.entry(id.clone()).or_insert(index);
        }
    }
    for id in &head.rejected_envelope_ids {
        resolved_at.entry(id.clone()).or_insert(chain.len());
    }
    for (index, manifest) in chain.iter().enumerate() {
        for id in &manifest.rejected_envelope_ids {
            let Ok(rejected) = fyrr_reader::storage::read_contract::<PreparedEnvelope>(
                store.as_ref(),
                &ns.clone().join("envelopes").join(format!("{id}.json")),
            )
            .await
            else {
                continue;
            };
            if rejected.kind != SyncKind::Permissions {
                continue;
            }
            // Its older links were resolved before, by construction, or here.
            if let Some(older) = &rejected.previous
                && resolved_at.get(older).is_some_and(|at| *at > index)
            {
                panic!(
                    "{older} was left behind the rejected {id}: resolved later than the publication that rejected it"
                );
            }
        }
    }
}

/// The reads of a source are committed oldest first, whatever their age: a
/// newer read that is over the age of an envelope never goes before the older
/// one. With a group of one row the older is published alone, then the newer
/// is rejected, and the replacement starts where it did. (A group that holds
/// both rejects the newer and drops the older with it.)
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_newer_read_rejected_for_its_age_never_goes_before_the_older_one_on_storage()
{
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config).unwrap().replace(
            "[reader]",
            "[writer]\nenvelope_max_rows=1\n[writer.commit]\ngroup_max_rows=1\n[reader]",
        );
        std::fs::write(config, text).unwrap();
    }
    for (epoch, id) in [(2, "a"), (3, "b")] {
        input(
            &root,
            json!({"epoch":epoch,"permissions":[[{"id":id,"revision":"p2","readers":[]}]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    }
    let links = permission_envelopes(&worker).await;
    // A part and the envelope that ends each read.
    let [.., older, _, newer, _] = &links[..] else {
        unreachable!()
    };
    // The newer is over the age of an envelope; the older read goes first.
    age(&worker, &newer.envelope_id, 4).await;
    publish(&commit);
    publish(&commit);
    assert_eq!(resolved(&commit, &older.envelope_id).await, (true, false));
    assert_eq!(resolved(&commit, &newer.envelope_id).await, (false, true));
    assert_rejections_whole(&commit).await;
    input(
        &root,
        json!({"epoch":4,"permissions":[[{"id":"a","revision":"p3","readers":[]}],[{"id":"b","revision":"p3","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let replacement = permission_envelopes(&worker)
        .await
        .into_iter()
        .rev()
        .nth(1)
        .unwrap();
    assert_eq!(
        replacement.cursor.start, newer.cursor.start,
        "the replacement starts where the rejected read did"
    );
    publish(&commit);
    let batch = rows(&worker).await;
    assert!(row_readers(&batch, "a").is_empty() && row_readers(&batch, "b").is_empty());
}

/// The head decides what is rejected, not a marker on storage: the marker of an
/// envelope that no publication names, as a committer that lost its lease or
/// died before it published may leave, rejects nothing. The envelope is
/// applied as its commit judges it.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_marker_no_publication_names_rejects_nothing_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let read = permission_envelopes(&worker).await.pop().unwrap();
    put_marker(&worker, &read).await;
    publish(&commit);
    assert_eq!(
        resolved(&commit, &read.envelope_id).await,
        (true, false),
        "applied, not rejected"
    );
    assert!(
        row_readers(&rows(&worker).await, "a").is_empty(),
        "its revoke was published"
    );
}

/// A rejection that a commit wrote and did not live to publish stands: the next
/// commit, which would reject the envelope for another reason, finds the marker
/// and goes on.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_rejection_marker_written_before_a_crash_is_reused_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let read = permission_envelopes(&worker).await.pop().unwrap();
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns
        .join("rejections")
        .join(format!("{}.json", read.envelope_id));
    let written = fyrr_core::envelope::RejectionMarker {
        schema_version: fyrr_core::envelope::ENVELOPE_CONTRACT_VERSION,
        envelope_id: read.envelope_id.clone(),
        source_id: read.source_id.clone(),
        kind: read.kind,
        sequence: read.sequence,
        rejection_generation: read.rejection_generation,
        restore_sequence: read.restore_sequence,
        start_cursor: read.cursor.start.clone(),
        reason: "authorization_rewind_required".into(),
    };
    store
        .put(&path, write_contract(&written).into())
        .await
        .unwrap();
    age(&worker, &read.envelope_id, 4).await;
    publish(&commit);
    assert!(resolved(&commit, &read.envelope_id).await.1);
    let kept: fyrr_core::envelope::RejectionMarker =
        fyrr_reader::storage::read_contract(store.as_ref(), &path)
            .await
            .unwrap();
    assert_eq!(kept.reason, "authorization_rewind_required");
}

/// A permissions envelope without its lease generation, as 0.1 wrote it, is not
/// on a chain: it is never applied, and it does not stop the commit, in which
/// another source still publishes.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_an_unchained_permissions_envelope_is_inert_and_blocks_no_commit_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let readers = row_readers(&rows(&worker).await, "a");
    let sequence = cursor_of(&worker, SyncKind::Permissions).await.sequence;
    let debris = put_permission_envelope(&worker, sequence + 1, None, 0, &[("a", "p5")]).await;
    std::fs::write(root.join("sources/0.txt"), "fixture document 7").unwrap();
    worker_sync(&worker, &["allowed", "--kind", "content"]);
    publish(&commit);
    assert_eq!(resolved(&commit, &debris.envelope_id).await, (false, false));
    assert_eq!(row_readers(&rows(&worker).await, "a"), readers);
    let control = source_rows(&worker, "allowed").await;
    let row = roots(&control).iter().position(|id| id == "0.txt").unwrap();
    assert_eq!(
        fyrr_writer::apply::strings(&control, "searchable_text")
            .unwrap()
            .value(row),
        "fixture document 7",
        "another source publishes in the same commit"
    );
}

// ---- The schedule test of the permissions pipeline (#325) ----------------------
//
// Short schedules, drawn from a seed, run the real worker, committer, reader and
// cleanup on local storage. A small oracle, independent of the writer, knows what
// each completed read states; after every publication the rights a reader sees are
// checked against it, and what the committer needs of the log must be there.

/// The readers that the `i`th change of the source states: odd changes are of
/// `a`, even ones of `b`, and no two prefixes of the log state the same rights.
const COUNTS: [usize; 16] = [0, 0, 1, 1, 0, 2, 1, 3, 0, 4, 2, 0, 3, 1, 2, 2];

/// The rights after the first `length` changes of the source: the readers of
/// `a` and of `b`, which the content of the documents states, five each, until a
/// change does.
fn states_of(length: usize) -> (usize, usize) {
    let (mut a, mut b) = (5, 5);
    for (index, count) in COUNTS.iter().enumerate().take(length) {
        if index % 2 == 0 {
            a = *count;
        } else {
            b = *count;
        }
    }
    (a, b)
}

/// The reader tokens a reader sees of `a` and of `b`, sorted.
type Rights = (Vec<String>, Vec<String>);

/// A deterministic generator: the same seed, the same schedule.
struct Dice(u64);
impl Dice {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)) % n
    }
}

#[derive(Clone, Copy, Debug)]
enum Step {
    ReadOk,
    ReadFails,
    ReadCrashAfterCursor,
    CrashAfterPut,
    CrashAfterRejection,
    Debris,
    Takeover,
    LatePut,
    Oversize,
    AgeAndClean,
    Copy,
    OtherContent,
    Publish,
    /// A content read of the source, published.
    ContentRead,
    /// A content read whose worker dies before the cursor write that confirms
    /// its envelope: it never publishes, and the next read, of the same
    /// revisions, publishes them.
    ContentCrashAfterPut,
}

const STEPS: [(Step, u64); 13] = [
    (Step::ReadOk, 4),
    (Step::ReadFails, 1),
    (Step::ReadCrashAfterCursor, 1),
    (Step::CrashAfterPut, 1),
    (Step::CrashAfterRejection, 1),
    (Step::Debris, 1),
    (Step::Takeover, 1),
    (Step::LatePut, 1),
    (Step::Oversize, 2),
    (Step::AgeAndClean, 1),
    (Step::Copy, 1),
    (Step::OtherContent, 1),
    (Step::Publish, 3),
];

struct Run {
    seed: u64,
    genesis: bool,
    trace: Vec<String>,
    worker: PathBuf,
    commit: PathBuf,
    root: FixtureRoot,
    epoch: u64,
    content: u32,
    other: u32,
    /// The changes of the source so far, as the readers each states.
    log: Vec<usize>,
    /// How much of the log the newest read that completed covers, and the oldest
    /// prefix that may still be visible: rights never go back.
    completed: usize,
    seen: usize,
    /// The prefixes of the log that a read ended at, when it completed: a read
    /// is published whole, so no other prefix is ever visible.
    ends: Vec<usize>,
    /// The sequence of the newest publication checked.
    checked: u64,
    /// Which of `a` and `b` a checked publication had.
    shown: std::cell::Cell<[bool; 2]>,
    /// Whether the cursor holds a read last, that nothing has resolved.
    last_valid: bool,
    /// Where that read ends, when it is one of `ends`.
    last_end: Option<usize>,
    /// The documents maintenance purged, until their content is fetched again.
    purged: [bool; 2],
    held: Option<fyrr_core::connector::CursorState>,
    /// The content envelopes that no cursor confirmed: no publication names them.
    lost_content: Vec<String>,
    /// Whether the next content read repeats the revisions of a lost one.
    redo: bool,
}

impl Drop for Run {
    #[allow(clippy::print_stderr)]
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "schedule seed {} (genesis: {}) failed; trace:\n  {}",
                self.seed,
                self.genesis,
                self.trace.join("\n  ")
            );
        }
    }
}

impl Run {
    fn append(&mut self) {
        let index = self.log.len();
        self.log.push(COUNTS[index]);
    }

    /// Writes the source: its log of changes, and its documents. `oversize` is a
    /// change whose readers, this once, make a row over the bytes of a group.
    fn input(&mut self, oversize: Option<usize>, fail_at: Option<u64>) {
        self.epoch += 1;
        let revision = format!("r{}", self.content);
        let log: Vec<Value> = self
            .log
            .iter()
            .enumerate()
            .map(|(index, count)| {
                let (id, tag) = if index % 2 == 0 { ("a", "ra") } else { ("b", "rb") };
                let big = oversize == Some(index);
                json!({"id":id,
                    "revision":format!("p{index}-{}", if big { "big".to_owned() } else { count.to_string() }),
                    "readers": if big { many_readers() } else { (0..*count).map(|n| format!("grp:{tag}{n}")).collect() }})
            })
            .collect();
        let mut value = json!({"epoch":self.epoch,
            "content":[[
                {"id":"a","revision":&revision,"texts":["fixture document 0"],"readers":(0..5).map(|n| format!("grp:ra{n}")).collect::<Vec<_>>()},
                {"id":"b","revision":&revision,"texts":["fixture document 1"],"readers":(0..5).map(|n| format!("grp:rb{n}")).collect::<Vec<_>>()}]],
            "permission_log":log});
        if let Some(at) = fail_at {
            value["fail_permissions_at"] = json!(at);
        }
        input(&self.root, value);
    }

    fn read(&mut self, label: &str) {
        self.append();
        self.trace
            .push(format!("{label} change {}", self.log.len()));
        self.input(None, None);
        worker_sync(&self.worker, &["incremental", "--kind", "permissions"]);
        self.completed = self.log.len();
        self.ends.push(self.completed);
        self.last_valid = true;
        self.last_end = Some(self.completed);
    }

    /// One round of the committer, and the invariants after it.
    async fn publish(&mut self) {
        publish(&self.commit);
        self.check().await;
    }

    async fn published(&self) -> bool {
        let loaded = fyrr_core::config::load(&self.commit).unwrap();
        fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .is_some_and(|head| head.content.is_some())
    }

    async fn step(&mut self, step: Step) {
        let published = self.published().await;
        match step {
            Step::ReadOk => self.read("read_ok"),
            Step::ReadCrashAfterCursor => {
                self.read("read_ok_then_crash_after_the_cursor");
                // The worker dies before the write that stamps the read.
                change_cursor(&self.worker, SyncKind::Permissions, |state| {
                    state.updated_at = chrono::DateTime::UNIX_EPOCH;
                })
                .await;
            }
            Step::ReadFails => {
                self.trace.push("read_fails_midway".into());
                if self.log.len() + 2 <= COUNTS.len() {
                    self.append();
                    self.append();
                }
                self.input(None, Some(1));
                let output = worker_command(&self.worker)
                    .args(["writer", "sync", "incremental", "--kind", "permissions"])
                    .output()
                    .unwrap();
                assert!(!output.status.success(), "the read was to fail");
            }
            Step::Oversize => {
                self.trace.push("read_with_a_row_over_the_group".into());
                self.append();
                self.input(Some(self.log.len() - 1), None);
                worker_sync(&self.worker, &["incremental", "--kind", "permissions"]);
                // The cursor holds it, and nothing has resolved it yet.
                self.last_valid = true;
                self.last_end = None;
            }
            Step::CrashAfterRejection => {
                // A commit rejected the newest read, as too old, wrote its marker
                // and died before it published: a retry rejects it again and
                // reuses the marker.
                let cursor = cursor_or_blank(&self.worker).await;
                let (true, Some(last)) = (self.last_valid, cursor.last) else {
                    self.trace
                        .push("crash_after_rejection (nothing to reject)".into());
                    return;
                };
                let loaded = fyrr_core::config::load(&self.worker).unwrap();
                let head = fyrr_reader::storage::optional_head(&loaded.config)
                    .await
                    .unwrap()
                    .unwrap();
                let (applied, rejected) = fyrr_writer::commit::published_ids(&loaded.config, &head)
                    .await
                    .unwrap();
                if applied.contains(&last.id) || rejected.contains(&last.id) {
                    self.trace
                        .push("crash_after_rejection (already resolved)".into());
                    return;
                }
                self.trace.push("crash_after_rejection".into());
                let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
                let path = ns.join("envelopes").join(format!("{}.json", last.id));
                let mut envelope: PreparedEnvelope =
                    fyrr_reader::storage::read_contract(store.as_ref(), &path)
                        .await
                        .unwrap();
                envelope.created_at = fyrr_core::clock::now() - chrono::Duration::days(4);
                store
                    .put(&path, write_contract(&envelope).into())
                    .await
                    .unwrap();
                put_marker(&self.worker, &envelope).await;
                self.last_valid = false;
                // A rejected read is never visible.
                if let Some(end) = self.last_end.take() {
                    self.ends.retain(|e| *e != end);
                }
            }
            Step::Debris => {
                self.trace
                    .push("envelope_of_0.1_without_its_lease_generation".into());
                let cursor = cursor_or_blank(&self.worker).await;
                let loaded = fyrr_core::config::load(&self.worker).unwrap();
                let readers = (0..9)
                    .map(|n| {
                        fyrr_writer::apply::reader_token(
                            &loaded.config,
                            "incremental",
                            &format!("grp:debris{n}"),
                        )
                        .unwrap()
                    })
                    .collect();
                put_envelope_of(
                    &self.worker,
                    cursor.sequence + 1,
                    None,
                    0,
                    vec![fyrr_core::connector::DocumentPermission {
                        root_id: "a".into(),
                        permission_revision: "debris".into(),
                        readers,
                        containers: Vec::new(),
                    }],
                )
                .await;
            }
            Step::CrashAfterPut => {
                self.trace.push("crash_after_put_before_cursor".into());
                let cursor = cursor_or_blank(&self.worker).await;
                self.unconfirmed(
                    cursor.sequence + 1,
                    cursor.last.map(|last| last.id),
                    cursor.lease.map_or(1, |lease| lease.epoch),
                )
                .await;
            }
            Step::Takeover => {
                self.trace.push("lease_takeover".into());
                let loaded = fyrr_core::config::load(&self.worker).unwrap();
                let old = fyrr_writer::sync_lease::CursorLease::acquire(
                    &loaded.config,
                    "incremental",
                    SyncKind::Permissions,
                )
                .await
                .unwrap();
                self.held = Some(old.state().await.unwrap());
                old.release().await.unwrap();
            }
            Step::LatePut => {
                self.trace.push("late_put_of_the_old_worker".into());
                if let Some(held) = self.held.take() {
                    self.unconfirmed(
                        held.sequence + 1,
                        held.last.map(|last| last.id),
                        held.lease.map_or(1, |lease| lease.epoch),
                    )
                    .await;
                }
            }
            Step::AgeAndClean if published => {
                self.trace
                    .push("clock_passes_max_envelope_age_then_cleanup".into());
                // The rights before maintenance are those the oracle accepts.
                self.check().await;
                let before = self.visible().await;
                fyrr_core::clock::advance(std::time::Duration::from_secs(4 * 86_400)).unwrap();
                command(&self.commit, &["writer", "maintain"]);
                // Reads nothing published by now are expired and rejected, and
                // maintenance purges a document nobody may read, by design: a
                // later grant needs its content fetched again. A document the
                // oracle says somebody may read is never purged.
                let present = roots(&rows(&self.worker).await);
                for (index, root) in ["a", "b"].iter().enumerate() {
                    if !present.iter().any(|id| id == root) {
                        let readers = if index == 0 { &before.0 } else { &before.1 };
                        assert!(
                            readers.is_empty(),
                            "maintenance purged the document {root} that {} readers may read",
                            readers.len()
                        );
                        self.purged[index] = true;
                    }
                }
                self.last_valid = false;
                self.last_end = None;
                self.check().await;
            }
            Step::Copy if published => {
                self.trace.push("copy".into());
                self.check().await;
                self.epoch += 1;
                let to = super::namespace_copy::elsewhere(
                    &self.worker,
                    &format!("schedule-{}-{}", self.seed, self.epoch),
                    None,
                );
                super::namespace_copy::copied(&self.worker, &to);
                let here = self.visible().await;
                let batch = rows(&to).await;
                let readers = |root: &str| {
                    if roots(&batch).iter().any(|id| id == root) {
                        let mut readers = row_readers(&batch, root);
                        readers.sort();
                        readers
                    } else {
                        Vec::new()
                    }
                };
                assert_eq!(
                    (readers("a"), readers("b")),
                    here,
                    "the copy holds the rights that were published"
                );
                let copied = cursor_of(&to, SyncKind::Permissions).await;
                assert!(copied.last.is_none(), "a copy has no chain");
                std::fs::remove_dir_all(super::namespace_copy::storage_of(&to)).unwrap();
                std::fs::remove_file(&to).unwrap();
            }
            Step::OtherContent if published && !self.genesis => {
                self.trace.push("other_source_content".into());
                self.other += 1;
                std::fs::write(
                    self.root.join("sources/0.txt"),
                    format!("fixture document other {}", self.other),
                )
                .unwrap();
                worker_sync(&self.worker, &["allowed", "--kind", "content"]);
                self.publish().await;
                let control = source_rows(&self.worker, "allowed").await;
                let row = roots(&control).iter().position(|id| id == "0.txt").unwrap();
                assert_eq!(
                    fyrr_writer::apply::strings(&control, "searchable_text")
                        .unwrap()
                        .value(row),
                    format!("fixture document other {}", self.other),
                    "another source publishes whatever the permissions of this one are"
                );
                self.check().await;
            }
            Step::ContentRead => {
                if !std::mem::take(&mut self.redo) {
                    self.content += 1;
                }
                self.trace.push(format!("content_read r{}", self.content));
                self.input(None, None);
                worker_sync(&self.worker, &["incremental", "--kind", "content"]);
                self.publish().await;
                if self.published().await {
                    let batch = rows(&self.worker).await;
                    let revisions = fyrr_writer::apply::strings(&batch, "source_revision").unwrap();
                    for row in 0..batch.num_rows() {
                        assert_eq!(
                            revisions.value(row),
                            format!("r{}", self.content),
                            "the content read publishes"
                        );
                    }
                }
            }
            Step::ContentCrashAfterPut => {
                self.content += 1;
                self.trace.push(format!(
                    "content_crash_after_put_before_cursor r{}",
                    self.content
                ));
                self.input(None, None);
                let before = cursor_of(&self.worker, SyncKind::Content).await;
                let known = envelope_ids(&self.worker).await;
                worker_sync(&self.worker, &["incremental", "--kind", "content"]);
                let mut after = cursor_of(&self.worker, SyncKind::Content).await;
                let written: Vec<_> = envelope_ids(&self.worker)
                    .await
                    .into_iter()
                    .filter(|id| !known.contains(id))
                    .collect();
                assert!(!written.is_empty(), "the read wrote its content");
                self.lost_content.extend(written);
                after.sequence = before.sequence;
                after.cursor = before.cursor;
                after.last = before.last;
                after.updated_at = before.updated_at;
                put_cursor(&self.worker, &after).await;
                self.redo = true;
                self.publish().await;
            }
            Step::Publish | Step::AgeAndClean | Step::Copy | Step::OtherContent => {
                self.trace.push("publish".into());
                self.publish().await;
            }
        }
    }

    /// An envelope no cursor confirmed, that grants `a` nine readers.
    async fn unconfirmed(&mut self, sequence: u64, previous: Option<String>, generation: u64) {
        let loaded = fyrr_core::config::load(&self.worker).unwrap();
        let readers = (0..9)
            .map(|n| {
                fyrr_writer::apply::reader_token(
                    &loaded.config,
                    "incremental",
                    &format!("grp:unconfirmed{n}"),
                )
                .unwrap()
            })
            .collect();
        put_envelope_of(
            &self.worker,
            sequence,
            previous,
            generation,
            vec![fyrr_core::connector::DocumentPermission {
                root_id: "a".into(),
                permission_revision: format!("unconfirmed-{sequence}"),
                readers,
                containers: Vec::new(),
            }],
        )
        .await;
    }

    /// The reader tokens of `count` readers `grp:{tag}0`.., sorted: what the
    /// oracle expects a reader to see, whatever the writer wrote.
    fn tokens(&self, tag: &str, count: usize) -> Vec<String> {
        let loaded = fyrr_core::config::load(&self.worker).unwrap();
        let mut tokens: Vec<String> = (0..count)
            .map(|n| {
                fyrr_writer::apply::reader_token(
                    &loaded.config,
                    "incremental",
                    &format!("grp:{tag}{n}"),
                )
                .unwrap()
            })
            .collect();
        tokens.sort();
        tokens
    }

    /// The readers of `a` and of `b` that a state of `counts` readers each states.
    fn principals(&self, counts: (usize, usize)) -> Rights {
        (self.tokens("ra", counts.0), self.tokens("rb", counts.1))
    }

    async fn visible(&self) -> Rights {
        let (head, _) = publication(&self.worker).await;
        let (rights, unpublished) = self.visible_at(head.content.unwrap().lance_version).await;
        assert_eq!(unpublished, [false, false], "the documents are published");
        rights
    }

    /// The rights a reader of the Lance version `version` sees, and which of
    /// `a` and `b` no publication had yet, which nobody reads: a publication
    /// before the content of the source can hold its rights alone.
    async fn visible_at(&self, version: u64) -> (Rights, [bool; 2]) {
        let batch = source_rows_at(&self.worker, "incremental", version).await;
        let mut unpublished = [false; 2];
        let mut readers = |index: usize, root: &str| {
            let mut shown = self.shown.get();
            if roots(&batch).iter().any(|id| id == root) {
                assert!(!self.purged[index], "the purged document {root} is back");
                shown[index] = true;
                self.shown.set(shown);
                let mut readers = row_readers(&batch, root);
                readers.sort();
                readers
            } else {
                assert!(
                    self.purged[index] || !shown[index],
                    "the document {root} is not published: {:?}",
                    roots(&batch)
                );
                unpublished[index] = !self.purged[index];
                Vec::new()
            }
        };
        let rights = (readers(0, "a"), readers(1, "b"));
        (rights, unpublished)
    }

    /// Whether the rights seen are `wanted`, a purged document aside: it has no
    /// rights until its content is fetched again.
    fn same(&self, seen: &Rights, wanted: &Rights) -> bool {
        (self.purged[0] || seen.0 == wanted.0) && (self.purged[1] || seen.1 == wanted.1)
    }

    /// The invariants after a publication.
    async fn check(&mut self) {
        let loaded = fyrr_core::config::load(&self.commit).unwrap();
        let head = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        // 1. The rights are the state after some read that completed, never one
        // older than what was seen, never a partial, unconfirmed or refused one.
        // Every publication since the last check is checked, not only the
        // last: one commit can publish several, and none may show a wrong state.
        if let Some(content) = &head.content {
            let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
            let mut chain =
                fyrr_reader::storage::manifest_chain(store, ns, Some(content.manifest.clone()));
            let mut published = Vec::new();
            while let Some(manifest) = futures::TryStreamExt::try_next(&mut chain).await.unwrap() {
                if manifest.sequence <= self.checked {
                    break;
                }
                for id in &self.lost_content {
                    assert!(
                        !manifest.envelope_ids.contains(id),
                        "publication {} names {id}, which no cursor confirmed",
                        manifest.sequence
                    );
                }
                // A publication of maintenance, with no envelope, changes no right.
                if !manifest.envelope_ids.is_empty() || manifest.sequence == content.sequence {
                    published.push((manifest.sequence, manifest.lance_version));
                }
            }
            for (sequence, lance_version) in published.into_iter().rev() {
                let (seen, unpublished) = self.visible_at(lance_version).await;
                let version = self
                    .ends
                    .iter()
                    .copied()
                    .filter(|v| *v >= self.seen)
                    .find(|v| {
                        let wanted = self.principals(states_of(*v));
                        (self.purged[0] || unpublished[0] || seen.0 == wanted.0)
                            && (self.purged[1] || unpublished[1] || seen.1 == wanted.1)
                    });
                let version = version.unwrap_or_else(|| {
                    panic!(
                        "publication {sequence}: the rights {seen:?} are not those after a completed read of {:?}, from {} changes",
                        self.ends, self.seen
                    )
                });
                self.seen = version;
            }
            self.checked = content.sequence;
        }
        // 2. What the committer needs of the log is there: every link of the chain
        // from the cursor back to the envelope that was applied.
        let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        let cursor = cursor_or_blank(&self.worker).await;
        let applied = match &head.content {
            Some(_) => publication(&self.commit)
                .await
                .1
                .chain_applied
                .get("incremental")
                .and_then(|kinds| kinds.get(&SyncKind::Permissions))
                .cloned(),
            None => None,
        };
        // No envelope is both applied and rejected, and no read waits behind a
        // newer one that was rejected: the replacement would start after it.
        let (applied_ids, rejected_ids) = fyrr_writer::commit::published_ids(&loaded.config, &head)
            .await
            .unwrap();
        let mut next = cursor.last.map(|last| last.id);
        let mut dropped = false;
        while let Some(id) = next.take() {
            if applied.as_ref() == Some(&id) {
                break;
            }
            let link: PreparedEnvelope = fyrr_reader::storage::read_contract(
                store.as_ref(),
                &ns.clone().join("envelopes").join(format!("{id}.json")),
            )
            .await
            .unwrap_or_else(|_| panic!("the link {id} of the chain is not in the log"));
            let resolved = applied_ids.contains(&id) || rejected_ids.contains(&id);
            assert!(
                !dropped || resolved,
                "the read {id} waits behind a newer one that was rejected"
            );
            dropped |= rejected_ids.contains(&id);
            next = link.previous;
        }
        // So is every link of the content chain.
        let content_applied = match &head.content {
            Some(_) => publication(&self.commit)
                .await
                .1
                .chain_applied
                .get("incremental")
                .and_then(|kinds| kinds.get(&SyncKind::Content))
                .cloned(),
            None => None,
        };
        let content_cursor: Option<fyrr_core::connector::CursorState> =
            fyrr_reader::storage::read_contract(
                store.as_ref(),
                &ns.clone().join("cursors").join("incremental-content.json"),
            )
            .await
            .ok();
        let mut next = content_cursor
            .and_then(|state| state.last)
            .map(|last| last.id);
        while let Some(id) = next.take() {
            if content_applied.as_ref() == Some(&id) {
                break;
            }
            let link: PreparedEnvelope = fyrr_reader::storage::read_contract(
                store.as_ref(),
                &ns.clone().join("envelopes").join(format!("{id}.json")),
            )
            .await
            .unwrap_or_else(|_| panic!("the content link {id} is not in the log"));
            next = link.previous;
        }
        let listed: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(
            object_store::ObjectStore::list(store.as_ref(), Some(&ns.join("envelopes"))),
        )
        .await
        .unwrap();
        for object in listed {
            let envelope: PreparedEnvelope =
                fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                    .await
                    .unwrap();
            let both = envelope.kind == SyncKind::Permissions
                && rejected_ids.contains(&envelope.envelope_id)
                && applied_ids.contains(&envelope.envelope_id);
            assert!(!both, "{} is applied and rejected", envelope.envelope_id);
        }
        assert_rejections_whole(&self.commit).await;
        // 4. `writer status` counts what the committer has pending.
        let status = command(&self.commit, &["writer", "status"]);
        let pending = fyrr_writer::commit::pending(&loaded.config, &head)
            .await
            .unwrap()
            .len();
        assert_eq!(
            status["pending_envelopes"].as_u64().unwrap() as usize,
            pending,
            "writer status and the committer disagree on what is pending"
        );
    }

    /// 3. Once the faults stop, a valid read unblocks the source.
    async fn recover(&mut self) {
        // A rejection the commit made is seen by the worker only at its next
        // read: a valid read, repeated, always ends in the rights it states.
        let mut rounds = 0;
        loop {
            self.read("final_valid_read");
            for _ in 0..3 {
                self.publish().await;
            }
            rounds += 1;
            if self.published().await
                && self.same(
                    &self.visible().await,
                    &self.principals(states_of(self.completed)),
                )
            {
                break;
            }
            assert!(rounds < 3, "a valid re-read did not unblock the source");
        }
        self.check().await;
        // A document nobody may read stays purged: its content brings nothing back.
        let wanted = states_of(self.completed);
        self.purged = [
            self.purged[0] && wanted.0 == 0,
            self.purged[1] && wanted.1 == 0,
        ];
        self.content += 1;
        let revision = format!("r{}", self.content);
        self.input(None, None);
        worker_sync(&self.worker, &["incremental", "--kind", "content"]);
        for _ in 0..3 {
            self.publish().await;
        }
        let batch = rows(&self.worker).await;
        let revisions = fyrr_writer::apply::strings(&batch, "source_revision").unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(
                revisions.value(row),
                revision,
                "the content of the source publishes"
            );
        }
        assert_eq!(
            self.visible().await,
            self.principals(wanted),
            "a document purged comes back with the rights of the source"
        );
        self.check().await;
        let loaded = fyrr_core::config::load(&self.commit).unwrap();
        let head = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        assert!(
            fyrr_writer::commit::pending(&loaded.config, &head)
                .await
                .unwrap()
                .is_empty(),
            "nothing is left pending"
        );
    }
}

/// The steps the dice of `seed` pick.
fn dealt(seed: u64, length: usize) -> Vec<Step> {
    let mut dice = Dice(seed);
    let total: u64 = STEPS.iter().map(|(_, weight)| weight).sum();
    (0..length)
        .map(|_| {
            let mut pick = dice.below(total);
            STEPS
                .iter()
                .find(|(_, weight)| {
                    let hit = pick < *weight;
                    pick = pick.saturating_sub(*weight);
                    hit
                })
                .map(|(step, _)| *step)
                .unwrap()
        })
        .collect()
}

async fn schedule(seed: u64, genesis: bool, steps: &[Step], purge: bool) {
    let (worker, commit, root) = if genesis {
        fixture_host(768, false)
    } else {
        fixture()
    };
    if genesis {
        drop_other_sources(&worker).await;
    }
    let mut run = Run {
        seed,
        genesis,
        trace: Vec::new(),
        worker,
        commit,
        root,
        epoch: 0,
        content: 0,
        other: 0,
        completed: 0,
        seen: 0,
        ends: vec![0],
        checked: 0,
        shown: Default::default(),
        log: Vec::new(),
        last_valid: false,
        last_end: None,
        purged: [false; 2],
        held: None,
        lost_content: Vec::new(),
        redo: false,
    };
    small_groups(&[&run.commit.clone()]);
    // The publications before the schedule are of the fixture.
    if run.published().await {
        run.checked = publication(&run.commit).await.1.sequence;
    }
    // A namespace that is published has had a read: the first change of the source.
    if genesis {
        run.input(None, None);
        worker_sync(&run.worker, &["incremental", "--kind", "content"]);
    } else {
        run.append();
        run.input(None, None);
        worker_sync(
            &run.worker,
            &["incremental", "--kind", "content", "--kind", "permissions"],
        );
        run.completed = 1;
        run.seen = 1;
        run.ends.push(1);
        run.publish().await;
    }
    for step in steps {
        run.step(*step).await;
    }
    if purge {
        run.trace.push("purge".into());
        publish(&run.commit);
        if run.published().await {
            run.check().await;
        }
        command(
            &run.commit,
            &["writer", "source", "purge", "incremental", "--confirm"],
        );
        assert!(source_roots(&run.worker, "incremental").await.is_empty());
        if !genesis {
            assert!(!source_roots(&run.worker, "allowed").await.is_empty());
        }
    } else {
        run.recover().await;
    }
    // The storage of the fixture lives beside its root.
    let storage = super::namespace_copy::storage_of(&run.worker);
    std::fs::remove_dir_all(storage).unwrap();
    std::fs::remove_dir_all(&*run.root).unwrap();
}

/// Bounded schedules of reads that succeed, fail, crash or are refused, of lease
/// takeovers with late puts, of cleanup after the clock passes the age of an
/// envelope, of copy and of purge, over the real worker, committer and reader
/// (#325); and the same faults of the write API's content (#331). A failure
/// prints its seed and trace.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_scheduled_reads_faults_cleanup_and_copy_keep_the_rights_exact_on_storage() {
    for (seed, genesis, steps, purge) in SCRIPTS {
        schedule(seed, genesis, steps, purge).await;
    }
    for (seed, purge) in SCHEDULES {
        schedule(seed, seed % 2 == 1, &dealt(seed, 6), purge).await;
    }
    for (seed, steps) in API_SCRIPTS {
        api_schedule(seed, steps).await;
    }
    for seed in API_SCHEDULES {
        api_schedule(seed, &api_dealt(seed, 5)).await;
    }
}

/// The sequences each fault of the log needs, as one short schedule each.
#[rustfmt::skip]
const SCRIPTS: [(u64, bool, &[Step], bool); 11] = [
    // A revoke waits when a read after it is refused for a row over the group.
    (101, false, &[Step::ReadOk, Step::Oversize, Step::Publish], false),
    (102, true, &[Step::ReadOk, Step::Oversize, Step::Publish], false),
    // A commit wrote the marker of the read that was refused and died: the older
    // read, accepted, is dropped with it, though it is held back with the content.
    (108, true, &[Step::ReadOk, Step::Oversize, Step::CrashAfterRejection, Step::Publish], false),
    (109, false, &[Step::ReadOk, Step::Oversize, Step::CrashAfterRejection, Step::Publish], false),
    // A marker a commit wrote and died before it published.
    (103, false, &[Step::ReadOk, Step::CrashAfterRejection, Step::Publish, Step::ReadOk, Step::Publish], false),
    // An envelope of 0.1, that no chain confirms.
    (104, false, &[Step::Debris, Step::Publish, Step::ReadOk, Step::Publish], false),
    // Reads nobody published when the clock passes their age, and a purge.
    (105, false, &[Step::ReadOk, Step::ReadOk, Step::ReadFails, Step::AgeAndClean, Step::LatePut], false),
    (106, false, &[Step::ReadOk, Step::Takeover, Step::LatePut, Step::CrashAfterPut, Step::Publish, Step::ReadCrashAfterCursor, Step::Publish, Step::Copy], false),
    // Content that no cursor confirmed, then a read of the same revisions (#331).
    (110, false, &[Step::ContentCrashAfterPut, Step::Publish, Step::ContentRead, Step::ReadOk, Step::Publish], false),
    (111, true, &[Step::ContentCrashAfterPut, Step::ContentCrashAfterPut, Step::ContentRead, Step::Publish], false),
    (112, false, &[Step::ReadOk, Step::ContentCrashAfterPut, Step::CrashAfterPut, Step::ContentRead, Step::AgeAndClean, Step::ContentRead], false),
];

const SCHEDULES: [(u64, bool); 4] = [(2, false), (3, false), (4, false), (5, true)];

// ---- The write API in the schedule test (#331) ---------------------------------
//
// The content of the write API is confirmed by its cursor, as permissions are:
// an envelope that no cursor write confirmed never publishes, and the next write
// takes its sequence under a new lease generation.

#[derive(Clone, Copy, Debug)]
enum ApiStep {
    Write,
    /// A write whose envelope is put, and whose process dies before the cursor
    /// write that confirms it.
    CrashAfterPut,
    /// An envelope of 0.1 at the next sequence, without a lease generation.
    Debris,
    Takeover,
    /// A put of the worker whose lease was taken over, after the takeover.
    LatePut,
    AgeAndClean,
}

const API_STEPS: [(ApiStep, u64); 6] = [
    (ApiStep::Write, 3),
    (ApiStep::CrashAfterPut, 2),
    (ApiStep::Debris, 1),
    (ApiStep::Takeover, 1),
    (ApiStep::LatePut, 1),
    (ApiStep::AgeAndClean, 1),
];

/// The sequences each fault needs, as one short schedule each.
#[rustfmt::skip]
const API_SCRIPTS: [(u64, &[ApiStep]); 5] = [
    // A write that died before its cursor write, then the one that takes its sequence.
    (201, &[ApiStep::CrashAfterPut, ApiStep::Write, ApiStep::CrashAfterPut, ApiStep::CrashAfterPut, ApiStep::Write]),
    // An envelope at the ID the next write had before lease generations.
    (202, &[ApiStep::Debris, ApiStep::Write, ApiStep::AgeAndClean, ApiStep::Write]),
    // A lease taken and given back around a crash, and cleanup of what it left.
    (203, &[ApiStep::Takeover, ApiStep::CrashAfterPut, ApiStep::Takeover, ApiStep::Write, ApiStep::AgeAndClean]),
    // A crash in the generation of the cursor, kept by cleanup until a write follows.
    (204, &[ApiStep::CrashAfterPut, ApiStep::AgeAndClean, ApiStep::Write, ApiStep::AgeAndClean]),
    // A late put of the worker whose lease was taken over.
    (205, &[ApiStep::Takeover, ApiStep::CrashAfterPut, ApiStep::LatePut, ApiStep::Write, ApiStep::LatePut, ApiStep::AgeAndClean]),
];

const API_SCHEDULES: [u64; 2] = [6, 7];

fn api_dealt(seed: u64, length: usize) -> Vec<ApiStep> {
    let mut dice = Dice(seed);
    let total: u64 = API_STEPS.iter().map(|(_, weight)| weight).sum();
    (0..length)
        .map(|_| {
            let mut pick = dice.below(total);
            API_STEPS
                .iter()
                .find(|(_, weight)| {
                    let hit = pick < *weight;
                    pick = pick.saturating_sub(*weight);
                    hit
                })
                .map(|(step, _)| *step)
                .unwrap()
        })
        .collect()
}

/// The texts the writes of a schedule state, one each, embedded beforehand.
const API_TEXTS: usize = 24;

struct ApiRun {
    seed: u64,
    trace: Vec<String>,
    config: PathBuf,
    root: FixtureRoot,
    address: std::net::SocketAddr,
    writer: Option<super::write_api::Writer>,
    texts: usize,
    /// The texts of `d` that an answered write stated, oldest first.
    answered: Vec<String>,
    /// The texts of writes that died before their cursor write: never visible.
    lost: Vec<String>,
    /// Their envelopes, which no publication names.
    lost_ids: Vec<String>,
    /// The cursor as the worker whose lease was taken over last saw it.
    held: Option<fyrr_core::connector::CursorState>,
    checked: u64,
}

impl Drop for ApiRun {
    #[allow(clippy::print_stderr)]
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "write API schedule seed {} failed; trace:\n  {}",
                self.seed,
                self.trace.join("\n  ")
            );
        }
    }
}

fn api_text(n: usize) -> String {
    format!("schedule text {n}")
}

/// The IDs of the envelopes in the log.
async fn envelope_ids(config: &Path) -> BTreeSet<String> {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    futures::TryStreamExt::try_collect::<Vec<_>>(object_store::ObjectStore::list(
        store.as_ref(),
        Some(&ns.join("envelopes")),
    ))
    .await
    .unwrap()
    .into_iter()
    .filter_map(|object| {
        object
            .location
            .filename()
            .and_then(|name| name.strip_suffix(".json"))
            .map(str::to_owned)
    })
    .collect()
}

/// Puts `state` as the stored cursor of its source and kind, without its
/// lease: what a worker that died before a cursor write leaves.
async fn put_cursor(config: &Path, state: &fyrr_core::connector::CursorState) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    store
        .put(
            &ns.join("cursors")
                .join(format!("{}-{}.json", state.source_id, state.kind.as_str())),
            write_contract(state).into(),
        )
        .await
        .unwrap();
}

impl ApiRun {
    fn next_text(&mut self) -> String {
        self.texts += 1;
        assert!(
            self.texts <= API_TEXTS,
            "the schedule writes more than it embedded"
        );
        api_text(self.texts)
    }

    fn writer(&self) -> &super::write_api::Writer {
        self.writer.as_ref().unwrap()
    }

    async fn cursor(&self) -> fyrr_core::connector::CursorState {
        let loaded = fyrr_core::config::load(&self.config).unwrap();
        let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        fyrr_reader::storage::read_contract(
            store.as_ref(),
            &ns.join("cursors").join("_api-content.json"),
        )
        .await
        .unwrap()
    }

    async fn write(&mut self) {
        let text = self.next_text();
        self.trace.push(format!("write {text}"));
        let answer = self
            .writer()
            .post(
                super::write_api::WRITE_KEY,
                "dense-fixture",
                &json!({"upserts": [{"id": "d", "text": &text}]}),
            )
            .await;
        assert_eq!(answer, (200, json!({"upserted": 1, "deleted": 0})));
        self.answered.push(text);
        self.check().await;
    }

    async fn step(&mut self, step: ApiStep) {
        let loaded = fyrr_core::config::load(&self.config).unwrap();
        match step {
            ApiStep::Write => self.write().await,
            ApiStep::CrashAfterPut => {
                let text = self.next_text();
                self.trace
                    .push(format!("crash_after_put_before_cursor {text}"));
                // The committer waits while the namespace lease is held: the
                // envelope is put and confirmed, and nothing publishes it.
                let lease = loop {
                    match fyrr_writer::lease::LeaseGuard::acquire(&loaded.config).await {
                        Ok(lease) => break lease,
                        Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
                    }
                };
                let before = self.cursor().await;
                let body = json!({"upserts": [{"id": "d", "text": &text}]});
                let request =
                    self.writer()
                        .post(super::write_api::WRITE_KEY, "dense-fixture", &body);
                let confirmed = async {
                    while self.cursor().await.sequence == before.sequence {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                };
                tokio::select! {
                    answer = request => panic!("a write was answered under a held lease: {answer:?}"),
                    () = confirmed => {}
                }
                // The cursor write never happened: the cursor is as before, in
                // the generation of the lease the envelope was written under.
                // The worker releases its lease first, as a crash would leave it
                // to expire.
                while self
                    .cursor()
                    .await
                    .lease
                    .is_some_and(|lease| lease.expires_at > fyrr_core::clock::now())
                {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                let mut after = self.cursor().await;
                self.lost_ids.push(after.last.take().unwrap().id);
                after.sequence = before.sequence;
                after.last = before.last;
                put_cursor(&self.config, &after).await;
                lease.release().await.unwrap();
                self.lost.push(text);
                self.check().await;
            }
            ApiStep::Debris => {
                self.trace
                    .push("envelope_of_0.1_without_its_lease_generation".into());
                let cursor = self.cursor().await;
                let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
                let last = cursor.last.as_ref().unwrap();
                let mut envelope: PreparedEnvelope = fyrr_reader::storage::read_contract(
                    store.as_ref(),
                    &ns.clone()
                        .join("envelopes")
                        .join(format!("{}.json", last.id)),
                )
                .await
                .unwrap();
                envelope.sequence = cursor.sequence + 1;
                envelope.envelope_id = fyrr_core::envelope::envelope_id(
                    &loaded.config.namespace.name,
                    fyrr_core::connector::API_SOURCE_ID,
                    SyncKind::Content,
                    envelope.sequence,
                    cursor.rejection_generation,
                    cursor.cursor.as_deref(),
                );
                envelope.lease_generation = 0;
                envelope.previous = None;
                envelope.created_at = fyrr_core::clock::now();
                store
                    .put(
                        &ns.join("envelopes")
                            .join(format!("{}.json", envelope.envelope_id)),
                        write_contract(&envelope).into(),
                    )
                    .await
                    .unwrap();
            }
            ApiStep::Takeover => {
                self.trace.push("lease_takeover".into());
                self.held = Some(self.cursor().await);
                let cursor = fyrr_writer::sync_lease::CursorLease::acquire(
                    &loaded.config,
                    fyrr_core::connector::API_SOURCE_ID,
                    SyncKind::Content,
                )
                .await
                .unwrap();
                cursor.release().await.unwrap();
            }
            ApiStep::LatePut => {
                let (Some(held), Some(lost)) = (self.held.take(), self.lost_ids.last().cloned())
                else {
                    self.trace.push("late_put (nothing to put)".into());
                    return;
                };
                let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
                let Ok(mut envelope) = fyrr_reader::storage::read_contract::<PreparedEnvelope>(
                    store.as_ref(),
                    &ns.clone().join("envelopes").join(format!("{lost}.json")),
                )
                .await
                else {
                    self.trace.push("late_put (its body was cleaned up)".into());
                    return;
                };
                self.trace.push("late_put_of_the_old_worker".into());
                // The body of a lost write, put as the next envelope of the old lease.
                let generation = held.lease.as_ref().map_or(0, |lease| lease.epoch);
                envelope.sequence = held.sequence + 1;
                envelope.lease_generation = generation;
                envelope.previous = held.last.map(|last| last.id);
                envelope.envelope_id = fyrr_core::envelope::leased_envelope_id(
                    &loaded.config.namespace.name,
                    fyrr_core::connector::API_SOURCE_ID,
                    SyncKind::Content,
                    envelope.sequence,
                    held.rejection_generation,
                    held.cursor.as_deref(),
                    generation,
                );
                envelope.created_at = fyrr_core::clock::now();
                store
                    .put(
                        &ns.join("envelopes")
                            .join(format!("{}.json", envelope.envelope_id)),
                        write_contract(&envelope).into(),
                    )
                    .await
                    .unwrap();
                self.lost_ids.push(envelope.envelope_id);
                self.check().await;
            }
            ApiStep::AgeAndClean => {
                self.trace
                    .push("clock_passes_max_envelope_age_then_cleanup".into());
                self.writer = None;
                fyrr_core::clock::advance(std::time::Duration::from_secs(4 * 86_400)).unwrap();
                command(&self.config, &["writer", "maintain"]);
                // What no cursor confirmed is gone; what the chain holds stays.
                let head = fyrr_reader::storage::optional_head(&loaded.config)
                    .await
                    .unwrap()
                    .unwrap();
                let (applied, rejected) = fyrr_writer::commit::published_ids(&loaded.config, &head)
                    .await
                    .unwrap();
                let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
                let listed: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(
                    object_store::ObjectStore::list(store.as_ref(), Some(&ns.join("envelopes"))),
                )
                .await
                .unwrap();
                // An envelope of the cursor's own generation may still be
                // confirmed by it, and stays.
                let generation = self.cursor().await.lease.map_or(0, |lease| lease.epoch);
                for object in listed {
                    let envelope: PreparedEnvelope =
                        fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                            .await
                            .unwrap();
                    assert!(
                        applied.contains(&envelope.envelope_id)
                            || rejected.contains(&envelope.envelope_id)
                            || envelope.lease_generation == generation,
                        "cleanup kept {}, which no cursor confirms",
                        envelope.envelope_id
                    );
                }
                self.writer = Some(
                    super::write_api::Writer::start(&self.config, &self.root, self.address).await,
                );
                self.check().await;
            }
        }
    }

    /// The invariants after a publication.
    async fn check(&mut self) {
        let loaded = fyrr_core::config::load(&self.config).unwrap();
        let head = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        let content = head.content.clone().unwrap();
        let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        // 1. Every publication since the last check shows a text that an answered
        // write stated, never an older one than before nor a lost one.
        let mut chain =
            fyrr_reader::storage::manifest_chain(store.clone(), ns.clone(), Some(content.manifest));
        let mut versions = Vec::new();
        while let Some(manifest) = futures::TryStreamExt::try_next(&mut chain).await.unwrap() {
            if manifest.sequence <= self.checked {
                break;
            }
            for id in &self.lost_ids {
                assert!(
                    !manifest.envelope_ids.contains(id),
                    "publication {} names {id}, which no cursor confirmed",
                    manifest.sequence
                );
            }
            versions.push(manifest.lance_version);
        }
        for version in versions.into_iter().rev() {
            let batch =
                source_rows_at(&self.config, fyrr_core::connector::API_SOURCE_ID, version).await;
            // The publication that created the namespace holds no document.
            let Some(row) = roots(&batch).iter().position(|id| id == "d") else {
                continue;
            };
            let text = fyrr_writer::apply::strings(&batch, "searchable_text")
                .unwrap()
                .value(row)
                .to_owned();
            assert!(
                !self.lost.contains(&text),
                "a write no cursor confirmed published: {text}"
            );
            assert!(
                self.answered.contains(&text),
                "the text {text} is no write's"
            );
        }
        self.checked = content.sequence;
        let batch = source_rows_at(
            &self.config,
            fyrr_core::connector::API_SOURCE_ID,
            content.lance_version,
        )
        .await;
        let row = roots(&batch).iter().position(|id| id == "d").unwrap();
        assert_eq!(
            fyrr_writer::apply::strings(&batch, "searchable_text")
                .unwrap()
                .value(row),
            self.answered.last().unwrap(),
            "the newest answered write is published"
        );
        // 2. Every link of the chain from the cursor back to what was applied is
        // in the log.
        let applied = publication(&self.config)
            .await
            .1
            .chain_applied
            .get(fyrr_core::connector::API_SOURCE_ID)
            .and_then(|kinds| kinds.get(&SyncKind::Content))
            .cloned();
        let mut next = self.cursor().await.last.map(|last| last.id);
        while let Some(id) = next.take() {
            if applied.as_ref() == Some(&id) {
                break;
            }
            let link: PreparedEnvelope = fyrr_reader::storage::read_contract(
                store.as_ref(),
                &ns.clone().join("envelopes").join(format!("{id}.json")),
            )
            .await
            .unwrap_or_else(|_| panic!("the link {id} of the chain is not in the log"));
            next = link.previous;
        }
        // 3. `writer status` counts what the committer has pending: nothing, as
        // what no cursor confirmed is not.
        let pending = fyrr_writer::commit::pending(&loaded.config, &head)
            .await
            .unwrap()
            .len();
        assert_eq!(pending, 0, "an answered write leaves nothing pending");
        let status = command(&self.config, &["writer", "status"]);
        assert_eq!(status["pending_envelopes"].as_u64(), Some(0));
    }
}

async fn api_schedule(seed: u64, steps: &[ApiStep]) {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    super::write_api::configure(&config, &address, "open = true");
    let texts: Vec<String> = (1..=API_TEXTS).map(api_text).collect();
    super::write_api::embed(&root, &texts.iter().map(String::as_str).collect::<Vec<_>>());
    let writer = super::write_api::Writer::start(&config, &root, address).await;
    let mut run = ApiRun {
        seed,
        trace: Vec::new(),
        config,
        root,
        address,
        writer: Some(writer),
        texts: 0,
        answered: Vec::new(),
        lost: Vec::new(),
        lost_ids: Vec::new(),
        held: None,
        checked: 0,
    };
    // A namespace that is published has had a write.
    run.write().await;
    for step in steps {
        run.step(*step).await;
    }
    // Once the faults stop, a write publishes.
    run.write().await;
    run.writer = None;
    let storage = super::namespace_copy::storage_of(&run.config);
    std::fs::remove_dir_all(storage).unwrap();
    std::fs::remove_dir_all(&*run.root).unwrap();
}

/// Removes the content cursor of `incremental`, as an operator who lost it.
async fn lose_content_cursor(config: &Path) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    store
        .delete(&ns.join("cursors").join("incremental-content.json"))
        .await
        .unwrap();
}

/// A content cursor lost while a publication is in flight is made again only
/// once it is published: the new cursor confirms what that publication
/// applied, and the source is not blocked by a chain that misses it (#422).
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_lost_content_cursor_waits_for_a_publication_in_flight_on_storage() {
    let (worker, commit, root) = fixture();
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1","fixture document 0")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r2","fixture document 0")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    // The committer stops once the manifest of r2 is written, before the head
    // names it, holding the namespace lease.
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (gate, hook) = super::corrections::RequestGate::new(|request, _, after| {
        after && request.starts_with("PUT /local/") && request.contains("/publications/")
    });
    let (proxied_endpoint, proxy) = super::corrections::proxy_at_boundary(
        endpoint,
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        hook,
    );
    let proxied = super::corrections::proxied(&commit, &proxied_endpoint);
    let committer = std::thread::spawn(move || command(&proxied, &["writer", "commit"]));
    gate.entered();
    lose_content_cursor(&worker).await;
    input(
        &root,
        json!({"epoch":3,"content":[[doc("a","r3","fixture document 0")]]}),
    );
    let refused = worker_command(&worker)
        .args(["writer", "sync", "incremental", "--kind", "content"])
        .output()
        .unwrap();
    assert!(
        !refused.status.success(),
        "the cursor is made under the namespace lease"
    );
    assert!(
        String::from_utf8_lossy(&refused.stdout).contains("committer_busy"),
        "{}",
        String::from_utf8_lossy(&refused.stdout)
    );
    gate.release();
    committer.join().unwrap();
    drop(gate);
    drop(proxy);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let status = command(&commit, &["writer", "status"]);
    assert_eq!(status["blocked_sources"], json!([]), "{status}");
    assert_eq!(status["pending_envelopes"], 0, "{status}");
    let batch = rows(&worker).await;
    let revisions = fyrr_writer::apply::strings(&batch, "source_revision").unwrap();
    let row = roots(&batch).iter().position(|id| id == "a").unwrap();
    assert_eq!(revisions.value(row), "r3");
}

/// A content cursor lost after a read of it was rejected reads the source from
/// the start: the marker of the rejection does not rewind the new cursor to
/// where the rejected read started (#422).
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_a_lost_content_cursor_after_a_rejection_reads_from_the_start_on_storage() {
    let (worker, commit, root) = fixture();
    input(
        &root,
        json!({"epoch":1,"content":[[doc("a","r1","fixture document 0")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    input(
        &root,
        json!({"epoch":2,"content":[[doc("a","r2","fixture document 0")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    fyrr_core::clock::advance(std::time::Duration::from_secs(4 * 86_400)).unwrap();
    publish(&commit);
    let (head, _) = publication(&commit).await;
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (_, rejected) = fyrr_writer::commit::published_ids(&loaded.config, &head)
        .await
        .unwrap();
    assert!(
        !rejected.is_empty(),
        "the read of r2 is rejected as expired"
    );
    lose_content_cursor(&worker).await;
    input(
        &root,
        json!({"epoch":3,"capture_cursors":true,"content":[[doc("a","r3","fixture document 0")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let captured: Value =
        serde_json::from_slice(&std::fs::read(root.join("content-cursor.json")).unwrap()).unwrap();
    assert!(
        captured.is_null(),
        "a lost content cursor reads from the start: {captured}"
    );
    publish(&commit);
    let batch = rows(&worker).await;
    let revisions = fyrr_writer::apply::strings(&batch, "source_revision").unwrap();
    let row = roots(&batch).iter().position(|id| id == "a").unwrap();
    assert_eq!(revisions.value(row), "r3");
}
