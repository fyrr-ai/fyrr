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
    // An older build would read these columns as null: it refuses the namespace.
    let (_, manifest) = publication(&worker).await;
    assert_eq!(manifest.schema_version.to_string(), "5.0");

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

/// A namespace created before the connector columns keeps its layout: its
/// documents keep the fields in their metadata, an older publication version
/// stays, and the tools return the fields as before. Creating it again with a
/// file that asks for the columns repeats it.
#[tokio::test]
#[ignore = "real worker and local storage; scripts/ci"]
async fn incremental_namespace_created_before_the_connector_columns_keeps_its_layout_on_storage() {
    let setup = setup();
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (worker, commit, root) = fixture_host_created(8, true, "legacy-fixture", setup, |config| {
        let loaded = fyrr_core::config::load(config).unwrap();
        let mut settings = fyrr_writer::commit::namespace_settings(&loaded.config);
        for (name, _) in fyrr_core::attributes::CONNECTOR_COLUMNS {
            settings.attributes.remove(name);
        }
        settings.attribute_indexes.remove("title");
        let config = loaded.config.clone();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(fyrr_writer::commit::create_with(&config, settings))
                .unwrap();
        })
        .join()
        .unwrap();
    });
    field_documents(&root);
    sync_fields(&worker);
    publish(&commit);

    let batch = rows(&worker).await;
    assert!(batch.column_by_name("title").is_none(), "no typed columns");
    let metadata = fyrr_writer::apply::strings(&batch, "metadata_json").unwrap();
    assert!(
        (0..metadata.len()).any(|row| metadata.value(row).contains("Alpha")),
        "the fields stay in the metadata"
    );
    let (_, manifest) = publication(&worker).await;
    assert_eq!(manifest.schema_version.to_string(), "4.1");
    command(&worker, &["writer", "namespace", "create"]);

    // Filtering by the fields is what the new layout adds.
    read_fields(&worker, json!({"field":"root_id","op":"eq","value":"a"}));
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
    fixture_host_created(documents, publish_initial, namespace, setup, |config| {
        super::coordination::command(config, &["writer", "namespace", "create"]);
    })
}

/// `fixture_host_at` with its own way to create the namespace.
fn fixture_host_created(
    documents: usize,
    publish_initial: bool,
    namespace: &str,
    (config, root): (PathBuf, FixtureRoot),
    create: impl FnOnce(&Path),
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
    create(&config);
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

/// A frozen namespace takes no sync, whatever the source, and still commits the
/// envelope that was prepared before the switch; thawed, it syncs again.
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

    // A sync of any source is refused with one named error, before the provider
    // reads anything.
    input(
        &root,
        json!({"epoch":3,"content":[[doc("d","r1","fixture document 3")]]}),
    );
    let before = envelopes(&worker).await.len();
    for source in ["incremental", "allowed"] {
        let refused = worker_command(&worker)
            .args(["writer", "sync", source, "--full"])
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
    assert_eq!(policy.schema_version.to_string(), "4.0");

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
            assert_eq!(p.documents.len(), 1);
            let receipt = command(
                &commit,
                &["writer", "commit", "--receipt", &envelope.envelope_id],
            );
            publications.insert(receipt["publication_id"].as_str().unwrap().to_owned());
        }
        assert_eq!(
            publications.len(),
            2,
            "two-chunk and one-chunk permission replacements exceed one group"
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
