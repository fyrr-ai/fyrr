//! Incremental contracts through real executable processes and the owned RustFS.
use super::coordination::{command, committer_host, host_command, publication, worker_sync};
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
    // Time compression: expire only this source's head freshness, keeping identity/directory live.
    let mut before = publication(&worker).await.0;
    let old = before
        .freshness
        .sources
        .iter_mut()
        .find(|f| f.source_id == "incremental")
        .unwrap();
    old.permissions_read_at = fyrr_core::clock::now() - chrono::Duration::hours(2);
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
    assert_eq!(
        denied["structuredContent"]["metadata"]["hidden_sources"],
        json!([])
    );
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
        EnvelopePayload::Members(p) if p.complete_read
    )));
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_members_alone_never_attest_permissions_on_storage() {
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
async fn incremental_delta_expired_source_isolated_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
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
    identity.built_at = fyrr_core::clock::now();
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
        fresh.permissions_read_at = fyrr_core::clock::now();
        fresh.members_read_at = Some(fyrr_core::clock::now());
        let expired = fyrr_core::clock::now() - chrono::Duration::hours(2);
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
        let search = command(
            &worker,
            &["reader", "search", "fixture query", "--limit", "2"],
        );
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

/// Catching a failed host revision callback cannot attest an incomplete read.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn incremental_caught_revision_failure_never_attests_on_storage() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let mut outcomes = Vec::new();
    for kind in [SyncKind::Permissions, SyncKind::Members] {
        for callback in ["lookup", "next"] {
            let (worker, commit, root) = fixture();
            publish_initial(&worker, &commit, &root);
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
