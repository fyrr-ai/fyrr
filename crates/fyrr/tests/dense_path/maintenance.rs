//! Slice-3 lifecycle proofs through independent CLI hosts and the owned RustFS.
use super::coordination::{command, host_command, publication, worker_sync};
use super::corrections::proxied;
use super::incremental::{
    doc, envelopes, fixture, fixture_on, initial, input, publish, rows, source_rows,
};
use super::*;
use object_store::ObjectStoreExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

fn restore_fragment_keys(dataset: &lance::Dataset) -> Vec<String> {
    dataset
        .manifest()
        .fragments
        .iter()
        .map(|fragment| {
            let mut files: Vec<_> = fragment.files.iter().map(|file| &file.path).collect();
            files.sort();
            json!([files, fragment.deletion_file]).to_string()
        })
        .collect()
}
async fn restore_fragment_dataset(config: &Path) -> lance::Dataset {
    let loaded = fyrr_core::config::load(config).unwrap();
    let head = publication(config).await.0;
    fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap()
}
async fn restore_differing_fragment_roots(
    before: &lance::Dataset,
    after: &lance::Dataset,
) -> std::collections::BTreeSet<String> {
    let old = restore_fragment_keys(before);
    let new = restore_fragment_keys(after);
    let shared: std::collections::BTreeSet<_> =
        old.iter().filter(|key| new.contains(key)).collect();
    let mut roots = std::collections::BTreeSet::new();
    for (dataset, keys) in [(before, &old), (after, &new)] {
        let selected: Vec<_> = dataset
            .manifest()
            .fragments
            .iter()
            .zip(keys)
            .filter(|(_, key)| !shared.contains(key))
            .map(|(fragment, _)| fragment.clone())
            .collect();
        if selected.is_empty() {
            continue;
        }
        let rows = dataset
            .scan()
            .with_fragments(selected)
            .project(&["root_id"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        roots.extend(
            fyrr_writer::apply::strings(&rows, "root_id")
                .unwrap()
                .iter()
                .map(|root| root.unwrap().to_owned()),
        );
    }
    roots
}

#[tokio::test]
#[ignore = "native Restore, whole-root mutations, purge, compaction and index metadata on real S3"]
async fn maintenance_restore_fragment_metadata_premises_on_real_s3() {
    use futures::TryStreamExt;
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
    let target = publication(&commit).await.1;
    let target_dataset = restore_fragment_dataset(&commit).await;
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]],"permissions":[[{"id":"a","revision":"p2","readers":["grp:group"]}]]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    publish(&commit);
    let changed = restore_fragment_dataset(&commit).await;
    assert!(
        restore_differing_fragment_roots(&target_dataset, &changed)
            .await
            .contains("a")
    );
    input(
        &root,
        json!({"epoch":3,"content":[[{"id":"b","revision":"r2","delete":true,"expected_prior_revision":"r1"}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let tombstone = restore_fragment_dataset(&commit).await;
    assert!(
        restore_differing_fragment_roots(&changed, &tombstone)
            .await
            .contains("b")
    );
    input(
        &root,
        json!({"epoch":4,"content":[[doc("b","r3","fixture document 3")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let recreated = restore_fragment_dataset(&commit).await;
    assert!(
        restore_differing_fragment_roots(&tombstone, &recreated)
            .await
            .contains("b")
    );
    input(
        &root,
        json!({"epoch":5,"permissions":[[{"id":"a","revision":"p3","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    publish(&commit);
    let revoked = restore_fragment_dataset(&commit).await;
    let before_maintenance = publication(&commit).await.1;
    let maintenance = command(&commit, &["writer", "maintain"]);
    assert!(maintenance["compaction_steps"].as_u64().unwrap() > 0);
    assert!(maintenance["index_merge_steps"].as_u64().unwrap() > 0);
    let compacted = restore_fragment_dataset(&commit).await;
    assert!(
        restore_differing_fragment_roots(&revoked, &compacted)
            .await
            .contains("a")
    );
    let rows = compacted
        .scan()
        .filter(&fyrr_writer::apply::keys_filter(
            "incremental",
            &["a".into()],
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert!(fyrr_writer::apply::state(&rows, 0).unwrap().content_purged);
    // Index commits are metadata-only: they do not invalidate immutable row
    // evidence. Observe this separately from the root-rewriting mutations.
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let head = publication(&commit).await.0;
    let mut chain =
        fyrr_reader::storage::manifest_chain(store, ns, Some(head.content.unwrap().manifest));
    let mut index_only = 0;
    let mut index_target = None;
    while let Some(manifest) = chain.try_next().await.unwrap() {
        if manifest.publication_id == before_maintenance.publication_id {
            break;
        }
        for recorded in &manifest.lineage.transactions {
            let tx = compacted
                .read_version_transaction(recorded.version)
                .await
                .unwrap()
                .transaction
                .unwrap();
            if matches!(
                tx.operation,
                lance::dataset::transaction::Operation::CreateIndex { .. }
            ) {
                let before = compacted.checkout_version(tx.read_version).await.unwrap();
                let after = compacted.checkout_version(recorded.version).await.unwrap();
                assert_eq!(
                    restore_fragment_keys(&before),
                    restore_fragment_keys(&after)
                );
                index_only += 1;
                if manifest.lineage.transactions.len() == 1 {
                    index_target = manifest.previous_publication_id.clone();
                }
            }
        }
    }
    assert!(index_only > 0);
    let index_restore = command(
        &commit,
        &[
            "writer",
            "restore",
            &index_target.expect("published index-only transition"),
        ],
    );
    assert_eq!(
        index_restore["restore_fragments"]["full_fallback"],
        "metadata_only_version_change"
    );
    command(&commit, &["writer", "restore", &target.publication_id]);
    let restored = restore_fragment_dataset(&commit).await;
    let manifest = publication(&commit).await.1;
    let mut native_restores = 0;
    for recorded in &manifest.lineage.transactions {
        let tx = restored
            .read_version_transaction(recorded.version)
            .await
            .unwrap()
            .transaction
            .unwrap();
        if matches!(
            tx.operation,
            lance::dataset::transaction::Operation::Restore { .. }
        ) {
            let native = restored.checkout_version(recorded.version).await.unwrap();
            assert_eq!(
                restore_fragment_keys(&native),
                restore_fragment_keys(&target_dataset),
                "native Restore must retain target paths and deletion metadata"
            );
            native_restores += 1;
        }
    }
    assert_eq!(native_restores, 1);
    let rows = restored
        .scan()
        .filter(&fyrr_writer::apply::keys_filter(
            "incremental",
            &["a".into()],
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert!(
        fyrr_writer::apply::state(&rows, 0).unwrap().content_purged,
        "restore after compaction/purge keeps current purged state"
    );
    command(&commit, &["writer", "verify"]);
}

#[tokio::test]
#[ignore = "real sync/commit/restore, namespace counting store and RustFS; scripts/ci"]
#[allow(clippy::print_stdout)]
async fn maintenance_restore_history_request_counts_on_real_s3() {
    use futures::TryStreamExt;
    use fyrr_core::envelope::{PreparedEnvelope, envelope_id};
    use fyrr_core::publication::ContentPointer;
    let mut reports = Vec::new();
    for roots in [1024usize, 4096] {
        let (worker, root) = setup();
        for n in 768..roots - 1 {
            std::fs::write(
                root.join("sources").join(format!("{n}.txt")),
                format!("fixture document {}", n % 768),
            )
            .unwrap();
        }
        let original = std::fs::read_to_string(&worker).unwrap();
        let mut sections = original.split("[[sources]]");
        let header = sections.next().unwrap();
        let allowed = sections.next().unwrap();
        std::fs::write(&worker, format!("{header}[[sources]]{allowed}\n[[sources]]\nid='incremental'\ntype='incremental-test'\npermissions='sync'\ndocument_max_bytes='64B'\n[sources.intervals]\npermissions='5m'\nmembers='5m'\n[sources.scope]\nfixture='{}'\n",root.join("input.json").display())).unwrap();
        initial(&root);
        let mut initial_input: Value =
            serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
        initial_input["content"] = json!([[doc("delta", "r1", "fixture document 0")]]);
        initial_input["permissions"] =
            json!([[{"id":"delta","revision":"p1","readers":["grp:group"]}]]);
        input(&root, initial_input);
        command(&worker, &["writer", "namespace", "create"]);
        worker_sync(&worker, &["allowed", "--full"]);
        let commit = super::coordination::committer_host(&worker, &root);
        publish(&commit);
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
        let mut changed = doc("delta", "r2", "fixture document 1");
        changed["expected_prior_revision"] = json!("r1");
        input(
            &root,
            json!({"epoch":2,"content":[[changed]],"permissions":[[{"id":"delta","revision":"p2","readers":["grp:group"]}]]}),
        );
        worker_sync(
            &worker,
            &["incremental", "--kind", "content", "--kind", "permissions"],
        );
        publish(&commit);
        let (mut head, current) = publication(&commit).await;
        let loaded = fyrr_core::config::load(&commit).unwrap();
        let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        let prefix = ns.clone().join("envelopes");
        let objects: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
        let mut ids = Vec::new();
        let mut seed = None;
        for object in objects {
            let e: PreparedEnvelope =
                fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                    .await
                    .unwrap();
            ids.push(e.envelope_id.clone());
            if e.kind == fyrr_core::connector::SyncKind::Permissions {
                seed = Some(e);
            }
        }
        let seed = seed.unwrap();
        for sequence in 1000..1000 + (1100 - ids.len()) as u64 {
            let mut e = seed.clone();
            e.sequence = sequence;
            e.envelope_id = envelope_id(
                &e.namespace,
                &e.source_id,
                e.kind,
                e.sequence,
                e.rejection_generation,
                e.cursor.start.as_deref(),
            );
            store
                .put(
                    &prefix.clone().join(format!("{}.json", e.envelope_id)),
                    write_contract(&e).into(),
                )
                .await
                .unwrap();
            ids.push(e.envelope_id);
        }
        assert_eq!(ids.len(), 1100);
        // Synthetic consumed receipts vary E without changing the real native
        // sync/commit delta or introducing unapplied authorization.
        let mut receipt = current.clone();
        receipt.previous_publication_id = Some(current.publication_id);
        receipt.sequence += 1;
        receipt.publication_id = format!("pub-{:032x}", 10000 + roots);
        receipt.envelope_ids = ids;
        let path = format!("publications/{}.json", receipt.publication_id);
        store
            .put(
                &object_store::path::Path::from(format!("{ns}/{path}")),
                write_contract(&receipt).into(),
            )
            .await
            .unwrap();
        head.content = Some(ContentPointer {
            publication_id: receipt.publication_id,
            sequence: receipt.sequence,
            lance_version: receipt.lance_version,
            manifest: path,
            committed_at: receipt.committed_at,
        });
        store
            .put(&ns.clone().join("head.json"), write_contract(&head).into())
            .await
            .unwrap();
        let report = root.join("restore-counts.json");
        let output = Command::new(
            std::env::var_os("FYRR_SCIM_HEALTH_TEST_BINARY")
                .expect("writer libtest built by scripts/ci"),
        )
        .args([
            "maintenance::counting_store::restore_counting_probe_on_real_s3",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("FYRR_TEST_RESTORE_CONFIG", &commit)
        .env("FYRR_TEST_RESTORE_TARGET", &target.publication_id)
        .env("FYRR_TEST_RESTORE_COUNTS", &report)
        .output()
        .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        let counts: Value = serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
        assert_eq!(counts["result"]["restored"], true);
        println!("restore_counts N={roots} E=1100 P=2 LIST_delay_ms=200 {counts}");
        reports.push(counts);
    }
    assert_eq!(
        reports[0]["namespace"], reports[1]["namespace"],
        "namespace requests must not grow with unchanged roots"
    );
    assert_eq!(
        reports[0]["result"]["restore_scan_reads"], reports[1]["result"]["restore_scan_reads"],
        "Lance scan requests must not grow with unchanged roots"
    );
    assert_eq!(
        reports[0]["lance"], reports[1]["lance"],
        "Lance requests across all native stores must not grow with unchanged roots"
    );
    for report in reports {
        assert_eq!(
            report["result"]["restore_roots"], 1,
            "only the differing root enters the overlay"
        );
        assert_eq!(
            report["result"]["restore_body_reads"], 1,
            "the one-root delta exercises changed bodies"
        );
        assert_eq!(
            report["result"]["restore_scans"],
            json!({"authority":1,"historical":1,"verification":1})
        );
        assert!(report["namespace"]["scans"]["list_pages"].as_u64().unwrap() <= 2);
        assert_eq!(report["namespace"]["scans"]["gets"], 0);
        assert_eq!(
            report["namespace"]["pass"]["list_pages"], 2,
            "one unconditional changed-group drain"
        );
        assert_eq!(
            report["namespace"]["verification"]["list_pages"], 2,
            "the post-verification boundary remains unconditional"
        );
    }
}
fn reader_config(worker: &Path, root: &Path, mode: &str) -> PathBuf {
    let host = root.join(format!("reader-{mode}"));
    std::fs::create_dir(&host).unwrap();
    let config = host.join("fyrr.toml");
    let text = std::fs::read_to_string(worker)
        .unwrap()
        .replace(
            "[reader]",
            &format!("[reader]\nversion_check='{mode}'\nversion_check_interval='100ms'\n"),
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

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
#[allow(clippy::print_stdout)]
async fn maintenance_bounded_catalog_restart_and_retrieval_on_real_s3() {
    use arrow_array::RecordBatchIterator;
    use futures::TryStreamExt;
    use fyrr_core::publication::ContentManifest;
    use lance::dataset::{
        WriteParams,
        fragment::write::FragmentCreateBuilder,
        transaction::{Operation, RewriteGroup},
    };
    use lance::index::DatasetIndexExt;
    use lance_index::{IndexType, optimize::OptimizeOptions, scalar::ScalarIndexParams};
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;
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
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config).unwrap().replace(
            "mirror_max_bytes = 16777216",
            "mirror_max_bytes = 268435456",
        );
        std::fs::write(
            config,
            format!("{text}\n[writer.commit]\ngroup_max_rows=100000\n"),
        )
        .unwrap();
    }
    // Capture the real sync/commit results before fabricating the oversized
    // catalog. Its cold preparation can exceed the ordinary request deadline;
    // the regression proves consolidation preserves these rows and results.
    let mut reader = tools::Mcp::open(&worker);
    let fetched = fetch(&mut reader, "a");
    assert_ne!(fetched["isError"], true, "baseline fetch: {fetched}");
    let original = fetched["structuredContent"]["documents"].clone();
    assert!(!original.as_array().unwrap().is_empty());
    let query = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    let searched = reader.call("search", query.clone());
    assert_ne!(searched["isError"], true, "baseline search: {searched}");
    let original_hits = searched["structuredContent"]["hits"].clone();
    assert!(!original_hits.as_array().unwrap().is_empty());
    drop(reader);
    // Seed real, disjoint native index segments in one commit, rather than
    // spending the regression's slot in hundreds of one-root writer commits.
    // Row bytes and authority come from the real sync/commit above.
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (mut head, mut seed) = publication(&commit).await;
    let mut dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    let rows = dataset.scan().try_into_batch().await.unwrap();
    let params = WriteParams {
        max_rows_per_file: 4,
        store_params: Some(fyrr_writer::prepare::store_params(&loaded.config).unwrap()),
        ..Default::default()
    };
    let fragments = FragmentCreateBuilder::new(&fyrr_reader::storage::dataset_uri(&loaded.config))
        .write_params(&params)
        .write_fragments(RecordBatchIterator::new(
            vec![Ok(rows.clone())].into_iter(),
            rows.schema(),
        ))
        .await
        .unwrap();
    dataset = lance::Dataset::commit(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        Operation::Rewrite {
            groups: vec![RewriteGroup {
                old_fragments: dataset.manifest().fragments.iter().cloned().collect(),
                new_fragments: fragments,
            }],
            rewritten_indices: Vec::new(),
            frag_reuse_index: None,
        },
        Some(dataset.version().version),
        Some(fyrr_writer::prepare::store_params(&loaded.config).unwrap()),
        None,
        dataset.session(),
        false,
    )
    .await
    .unwrap();
    dataset
        .optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    let ids: Vec<_> = dataset
        .get_fragments()
        .iter()
        .map(|f| f.id() as u32)
        .collect();
    let mut segments = Vec::new();
    let scalar = ScalarIndexParams::default();
    for id in ids {
        segments.push(
            dataset
                .create_index_builder(&["root_id"], IndexType::BTree, &scalar)
                .name("root_id_idx".into())
                .replace(true)
                .fragments(vec![id])
                .execute_uncommitted()
                .await
                .unwrap(),
        );
    }
    dataset
        .commit_existing_index_segments("root_id_idx", "root_id", segments)
        .await
        .unwrap();
    let templates = seed.indexes.clone();
    seed.indexes = dataset
        .load_indices()
        .await
        .unwrap()
        .iter()
        .map(|index| {
            let mut identity = templates
                .iter()
                .find(|i| i.name == index.name)
                .unwrap()
                .clone();
            identity.uuid = index.uuid.to_string();
            identity
        })
        .collect();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let prefix = ns.clone().join("dataset.lance");
    let referenced = fyrr_reader::storage::data_paths(&dataset);
    let uuids: BTreeSet<_> = seed.indexes.iter().map(|i| i.uuid.as_str()).collect();
    let native_manifest = dataset.manifest_location().path.to_string();
    let transaction = dataset
        .manifest()
        .transaction_file
        .as_ref()
        .map(|t| format!("_transactions/{t}"));
    seed.files = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|object| {
            let path = fyrr_reader::storage::relative_path(&object.location, &prefix);
            let live = referenced.contains(&path)
                || native_manifest.ends_with(&format!("/{path}"))
                || transaction.as_ref() == Some(&path)
                || path
                    .strip_prefix("_indices/")
                    .and_then(|p| p.split_once('/'))
                    .is_some_and(|(uuid, _)| uuids.contains(uuid));
            live.then_some(fyrr_core::envelope::FileRef {
                path,
                size_bytes: object.size,
            })
        })
        .collect();
    seed.previous_publication_id = Some(seed.publication_id.clone());
    seed.publication_id = format!("pub-{:032x}", 999001);
    seed.sequence += 1;
    seed.lance_version = dataset.version().version;
    seed.committed_at = chrono::Utc::now();
    // The fixture publication claims no leased transaction.
    seed.lineage.base_version = seed.lance_version;
    seed.lineage.recovery_version = None;
    seed.lineage.transactions.clear();
    seed.envelope_ids.clear();
    let path = format!("publications/{}.json", seed.publication_id);
    store
        .put(
            &object_store::path::Path::from(format!("{ns}/{path}")),
            write_contract(&seed).into(),
        )
        .await
        .unwrap();
    head.content = Some(fyrr_core::publication::ContentPointer {
        publication_id: seed.publication_id.clone(),
        sequence: seed.sequence,
        lance_version: seed.lance_version,
        manifest: path,
        committed_at: seed.committed_at,
    });
    store
        .put(&ns.clone().join("head.json"), write_contract(&head).into())
        .await
        .unwrap();
    let before = publication(&commit).await.1;
    let counts = |manifest: &ContentManifest| {
        let mut counts = BTreeMap::new();
        for index in &manifest.indexes {
            *counts.entry(index.name.clone()).or_insert(0usize) += 1;
        }
        counts
    };
    assert!(
        counts(&before).values().any(|n| *n > 128),
        "oversized initial catalog required"
    );
    let records = Arc::new(Mutex::new(BTreeMap::<String, ContentManifest>::new()));
    let observed = records.clone();
    let published = Arc::new(Mutex::new(BTreeSet::new()));
    let visible = published.clone();
    let fired = Arc::new(AtomicBool::new(false));
    let gate = fired.clone();
    let resume = Arc::new(AtomicBool::new(false));
    let release = resume.clone();
    let minimum = before.sequence;
    let initial_root_segments = before
        .indexes
        .iter()
        .filter(|i| i.name == "root_id_idx")
        .count();
    let hook: corrections::BoundaryHook = Arc::new(move |request, body, after| {
        if after && request.starts_with("PUT ") && request.contains("/publications/") {
            let manifest: ContentManifest = serde_json::from_slice(body).unwrap();
            observed
                .lock()
                .unwrap()
                .insert(manifest.publication_id.clone(), manifest);
        }
        if after && request.starts_with("PUT ") && request.contains("/head.json") {
            let head: fyrr_core::publication::ServingHead = serde_json::from_slice(body).unwrap();
            if let Some(content) = head.content {
                visible.lock().unwrap().insert(content.publication_id);
            }
        }
        // Kill with the first root-index merge durably published but its
        // response withheld. Restart must continue from that published step.
        if after
            && request.starts_with("PUT ")
            && request.contains("/head.json")
            && serde_json::from_slice::<fyrr_core::publication::ServingHead>(body).is_ok_and(
                |head| {
                    head.content.is_some_and(|content| {
                        content.sequence > minimum
                            && observed
                                .lock()
                                .unwrap()
                                .get(&content.publication_id)
                                .is_some_and(|m| {
                                    m.indexes.iter().filter(|i| i.name == "root_id_idx").count()
                                        < initial_root_segments
                                })
                    })
                },
            )
            && !gate.swap(true, Ordering::SeqCst)
        {
            while !release.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        false
    });
    let (endpoint, mut guard) = corrections::proxy_at_boundary(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    guard.resume = resume.clone();
    let config = proxied(&commit, &endpoint);
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("group_max_rows=1", "group_max_rows=100000");
    std::fs::write(&config, text).unwrap();
    let mut process = coordination::CommitProcess::spawn_command(
        &config,
        &["writer", "maintain"],
        resume.clone(),
    );
    let started = Instant::now();
    while !fired.load(Ordering::SeqCst) {
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "consolidation publication not reached"
        );
        if process.child().try_wait().unwrap().is_some() {
            let output = process.finish();
            panic!(
                "maintenance exited before gate: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    process.child().kill().unwrap();
    assert!(!process.finish().status.success());
    resume.store(true, Ordering::SeqCst);
    let (head, first_published) = publication(&commit).await;
    let first_counts = counts(&first_published);
    for (name, old) in counts(&before) {
        let new = first_counts.get(&name).copied().unwrap_or(0);
        assert!(
            old <= new + 31,
            "oversized catalog must drain in bounded steps: {old}->{new}"
        );
    }
    let remaining = (head.lease.unwrap().expires_at - chrono::Utc::now())
        .to_std()
        .unwrap_or_default();
    std::thread::sleep(remaining + Duration::from_millis(50));
    let first = command(&config, &["writer", "maintain"]);
    let second = command(&config, &["writer", "maintain"]);
    drop(guard);
    assert!(
        second["compaction_steps"].as_u64().unwrap() >= 128,
        "repeated threshold crossings required: {second}"
    );
    let published = published.lock().unwrap();
    let mut ordered: Vec<_> = records
        .lock()
        .unwrap()
        .values()
        .filter(|m| published.contains(&m.publication_id))
        .cloned()
        .collect();
    ordered.sort_by_key(|m| m.sequence);
    ordered.dedup_by_key(|m| m.sequence);
    let mut previous = counts(&before);
    let mut bounded = false;
    let mut crossings = BTreeMap::<String, usize>::new();
    for manifest in &ordered {
        let current = counts(manifest);
        let rewrite = manifest.lineage.transactions.len() == 2;
        if rewrite {
            bounded = true;
            assert!(
                current.values().all(|n| *n <= 65),
                "unbounded rewrite catalog: {current:?}"
            );
        } else if !bounded && previous.values().any(|n| *n > 64) {
            for (name, old) in &previous {
                let new = current.get(name).copied().unwrap_or(0);
                assert!(
                    *old <= new + 31,
                    "oversized catalog must drain in bounded steps: {old}->{new}"
                );
            }
        }
        if bounded {
            assert!(current.values().all(|n| *n <= 65));
            for (name, old) in &previous {
                if *old == 65 && current.get(name) == Some(&34) {
                    *crossings.entry(name.clone()).or_default() += 1;
                }
            }
        }
        previous = current;
    }
    assert!(
        crossings.values().any(|n| *n >= 2),
        "repeated consolidation crossings of the same index required: {crossings:?}"
    );
    let mut reader = tools::Mcp::open(&worker);
    assert_eq!(
        fetch(&mut reader, "a")["structuredContent"]["documents"],
        original
    );
    assert_eq!(
        reader.call("search", query)["structuredContent"]["hits"],
        original_hits
    );
    assert_eq!(command(&commit, &["writer", "verify"])["verified"], true);
    println!(
        "catalog_proof initial={:?} first={first} second={second} crossings={crossings:?}",
        counts(&before)
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_restore_recreated_root_and_changed_bodies_on_real_s3() {
    use arrow_array::{Array, StringArray};
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
    input(
        &root,
        json!({"epoch":2,"content":[[{"id":"a","delete":true,"revision":"r2","expected_prior_revision":"r1"}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let target = publication(&commit).await.1;
    input(
        &root,
        json!({"epoch":3,"content":[[doc("a","r3","fixture document 3"),replacement("b","r2","r1","fixture document 2")]],"permissions":[[{"id":"b","revision":"p2","readers":["grp:group"]}]]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    publish(&commit);
    let current = super::incremental::rows(&worker).await;
    let strings = |batch: &arrow_array::RecordBatch, name: &str| -> Vec<String> {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_owned())
            .collect()
    };
    let row = strings(&current, "root_id")
        .iter()
        .position(|r| r == "a")
        .unwrap();
    let mut expected: Value =
        serde_json::from_str(&strings(&current, "metadata_json")[row]).unwrap();
    assert_eq!(expected["_fyrr"]["deleted"], false);
    expected["_fyrr"]["content_purged"] = json!(true);
    let result = command(&commit, &["writer", "restore", &target.publication_id]);
    let config = fyrr_core::config::load(&worker).unwrap().config;
    let head = publication(&commit).await.0;
    let dataset = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&config),
        config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        head.content.unwrap().lance_version,
        None,
    )
    .await
    .unwrap();
    let marker = dataset
        .scan()
        .filter("root_id = 'a'")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(marker.num_rows(), 1);
    let actual: Value = serde_json::from_str(&strings(&marker, "metadata_json")[0]).unwrap();
    assert_eq!(
        actual["_fyrr"], expected["_fyrr"],
        "a recreated root must keep current revision, flags and sequence"
    );
    assert!(result["restore_body_reads"].as_u64().unwrap() > 0);
    let rows = super::incremental::rows(&worker).await;
    assert_eq!(strings(&rows, "root_id"), ["b"]);
    assert_eq!(strings(&rows, "searchable_text"), ["fixture document 1"]);
    assert!(!marker.column_by_name("readers").unwrap().is_null(0));

    // A genuine multi-record root fits acquisition/commit limits, then exceeds
    // a smaller restore byte budget despite its small indexed metadata.
    std::fs::write(
        &worker,
        std::fs::read_to_string(&worker)
            .unwrap()
            .replace("document_max_bytes='64B'", "document_max_bytes='128KiB'"),
    )
    .unwrap();
    let texts: Vec<_> = (0..40).map(|n| format!("fixture document {n}")).collect();
    input(
        &root,
        json!({"epoch":4,"content":[[{"id":"wide","revision":"r1","texts":texts,"section_readers":vec![None::<Vec<String>>;40]}]],"permissions":[[{"id":"wide","revision":"p1","readers":["grp:group"]}]]}),
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
    let wide_target = publication(&commit).await.1;
    assert_eq!(
        strings(&super::incremental::rows(&worker).await, "root_id")
            .iter()
            .filter(|r| *r == "wide")
            .count(),
        40,
        "the target fixture must contain forty content records"
    );
    input(
        &root,
        json!({"epoch":5,"content":[[replacement("wide","r2","r1","fixture document 2")]],"permissions":[[{"id":"wide","revision":"p2","readers":["grp:group"]}]]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    publish(&commit);
    let wide_overlay = command(&commit, &["writer", "restore", &wide_target.publication_id]);
    assert!(
        wide_overlay["restore_body_reads"].as_u64().unwrap() >= 40,
        "{wide_overlay}"
    );
    assert_eq!(
        strings(&super::incremental::rows(&worker).await, "root_id")
            .iter()
            .filter(|r| *r == "wide")
            .count(),
        40
    );
    let before = publication(&commit).await.1;
    let bounded_path = root.join("bounded-restore.toml");
    std::fs::write(
        &bounded_path,
        format!(
            "{}\n[writer]\ndocument_max_bytes='64KiB'\nenvelope_max_bytes='80KiB'\n[writer.commit]\ngroup_max_bytes='80KiB'\n",
            std::fs::read_to_string(&commit).unwrap()
        ),
    )
    .unwrap();
    let failed = host_command(&bounded_path)
        .args(["writer", "restore", &wide_target.publication_id])
        .output()
        .unwrap();
    assert!(!failed.status.success());
    assert!(
        String::from_utf8_lossy(&failed.stderr).contains("envelope_bounds_invalid"),
        "{}",
        String::from_utf8_lossy(&failed.stderr)
    );
    assert_eq!(
        publication(&commit).await.1.publication_id,
        before.publication_id
    );
}

#[test]
#[ignore = "real CLI and RustFS; scripts/ci"]
fn maintenance_generation_live_proofs_on_real_s3() {
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    for name in [
        "serving_minimum_admission_on_real_s3",
        "generation_undersized_five_publications_refused_on_real_s3",
        "generation_exactly_one_curative_handover_on_real_s3",
    ] {
        // Independent namespaces preserve immutable publications between proofs.
        let (worker, commit, root) = fixture();
        initial(&root);
        let mut seed: Value =
            serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
        seed["permissions"][0][1]["readers"] = json!([]);
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
        let config = reader_config(&worker, &root, "off");
        let output = Command::new(&binary)
            .arg(if name == "serving_minimum_admission_on_real_s3" {
                format!("search::routing_tests::{name}")
            } else {
                format!("serve::tests::generations::{name}")
            })
            .args(["--exact", "--ignored", "--nocapture"])
            .env("FYRR_TEST_GENERATION_CONFIG", &config)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        std::fs::write(
            root.join(format!("{name}.log")),
            format!("{stdout}\n{stderr}"),
        )
        .unwrap();
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(stdout.contains("1 passed; 0 failed"));
    }
}

/// Rewrite only test-clock observations. Preserve the real fixture's objects and payload bytes.
fn age_observations(request: &str, response: &mut Vec<u8>, expired: Option<&str>) {
    if !request.starts_with("GET ") {
        return;
    }
    super::corrections::rewrite_ok_body(response, |body| {
        let mut changed = false;
        if request.contains("/publications/") {
            if let Ok(mut value) = serde_json::from_slice::<Value>(body) {
                value["committed_at"] =
                    json!((chrono::Utc::now() - chrono::Duration::hours(25)).to_rfc3339());
                *body = serde_json::to_vec(&value).unwrap();
                changed = true;
            }
        } else if expired.is_some_and(|id| request.contains(id)) {
            if let Ok(mut value) = serde_json::from_slice::<Value>(body) {
                value["created_at"] =
                    json!((chrono::Utc::now() - chrono::Duration::days(4)).to_rfc3339());
                *body = serde_json::to_vec(&value).unwrap();
                changed = true;
            }
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

fn fetch(client: &mut tools::Mcp, root: &str) -> Value {
    client.call("fetch", json!({"source_id":"incremental","root_id":root}))
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_pending_revoke_purge_and_verify_on_real_s3() {
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
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_reader_modes_and_sighup_on_real_s3() {
    for mode in ["interval", "off", "per-request"] {
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
        let original = reader_config(&worker, &root, mode);
        let armed = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let hook: corrections::BoundaryHook = {
            let armed = armed.clone();
            let entered = entered.clone();
            let released = released.clone();
            Arc::new(move |request, _, after| {
                if !after
                    && request.starts_with("GET ")
                    && request.contains("/head.json ")
                    && armed.swap(false, Ordering::SeqCst)
                {
                    entered.store(true, Ordering::SeqCst);
                    let start = Instant::now();
                    while !released.load(Ordering::SeqCst)
                        && start.elapsed() < Duration::from_secs(5)
                    {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                false
            })
        };
        let (endpoint, proxy) = corrections::proxy_at_boundary(
            std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
            stop.clone(),
            hook,
        );
        let config = proxied(&original, &endpoint);
        let mut mcp = tools::Mcp::open(&config);
        let before =
            fetch(&mut mcp, "a")["structuredContent"]["metadata"]["publication_id"].clone();
        input(
            &root,
            json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "content"]);
        publish(&commit);
        if mode == "off" {
            std::thread::sleep(Duration::from_millis(300));
            let old = fetch(&mut mcp, "a");
            assert_eq!(
                old["structuredContent"]["metadata"]["publication_id"], before,
                "off keeps ordinary content pinned"
            );
            // No tool request is running. Stall the coordinator's non-forced
            // head check, then deliver HUP while its preparation is pending.
            armed.store(true, Ordering::SeqCst);
            let gate_started = Instant::now();
            while !entered.load(Ordering::SeqCst) {
                assert!(
                    gate_started.elapsed() < Duration::from_secs(5),
                    "background head check reached"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                Command::new("kill")
                    .args(["-HUP", &mcp.pid().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
            std::thread::sleep(Duration::from_millis(500));
            released.store(true, Ordering::SeqCst);
        }
        let started = Instant::now();
        loop {
            let result = fetch(&mut mcp, "a");
            if result["structuredContent"]["metadata"]["publication_id"] != before
                && !result["isError"].as_bool().unwrap_or(false)
            {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "{mode}: {result}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        input(
            &root,
            json!({"epoch":3,"permissions":[[{"id":"a","revision":"p3","readers":[]}]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "permissions"]);
        publish(&commit);
        let started = Instant::now();
        loop {
            let denied = fetch(&mut mcp, "a");
            assert!(
                !readable(&denied),
                "revoke never leaks during preparation: {denied}"
            );
            let healthy = fetch(&mut mcp, "b");
            if readable(&healthy) {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "mandatory switch: {healthy}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        drop(mcp);
        stop.store(true, Ordering::SeqCst);
        drop(proxy);
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_restore_rollback_current_rights_deletes_and_cursor_recovery_on_real_s3() {
    maintenance_restore_rollback_current_rights_deletes_and_cursor_recovery(Backend::S3).await;
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn maintenance_restore_rollback_current_rights_deletes_and_cursor_recovery_on_local_storage()
{
    maintenance_restore_rollback_current_rights_deletes_and_cursor_recovery(Backend::Local).await;
}

async fn maintenance_restore_rollback_current_rights_deletes_and_cursor_recovery(backend: Backend) {
    let (worker, commit, root) = fixture_on(backend);
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
    let config = reader_config(&worker, &root, "per-request");
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
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_per_request_waits_two_seconds_then_flagged_on_real_s3() {
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
    let (head, old) = publication(&commit).await;
    let blocked = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let gate = blocked.clone();
    let resume = release.clone();
    let observed = fired.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if !after
            && gate.load(Ordering::SeqCst)
            && request.starts_with("GET ")
            && request.contains("/_versions/")
        {
            observed.store(true, Ordering::SeqCst);
            while !resume.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        false
    });
    let (endpoint, thread) = corrections::proxy_at_boundary(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        hook,
    );
    let mut guard = thread;
    guard.resume = release.clone();
    let config = reader_config(&worker, &root, "per-request");
    let config = proxied(&config, &endpoint);
    let mut mcp = tools::Mcp::open(&config);
    assert!(readable(&fetch(&mut mcp, "a")));
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    // Background preparation may observe the head CAS before writer stdout is
    // delivered. Arm the reader-only boundary before publishing the new head.
    blocked.store(true, Ordering::SeqCst);
    publish(&commit);
    let began = Instant::now();
    let fallback = fetch(&mut mcp, "a");
    let elapsed = began.elapsed();
    assert!(
        fired.load(Ordering::SeqCst),
        "native generation preparation was paused"
    );
    assert!(
        elapsed >= Duration::from_millis(1800) && elapsed < Duration::from_millis(3500),
        "wait={elapsed:?}"
    );
    assert!(readable(&fallback), "{fallback}");
    assert_eq!(
        fallback["structuredContent"]["metadata"]["publication_id"],
        old.publication_id
    );
    assert_eq!(
        fallback["structuredContent"]["metadata"]["stale_version"],
        true
    );
    release.store(true, Ordering::SeqCst);
    let started = Instant::now();
    loop {
        let latest = fetch(&mut mcp, "a");
        if latest["structuredContent"]["metadata"]["publication_id"]
            != head.content.as_ref().unwrap().publication_id
            && readable(&latest)
        {
            assert_eq!(
                latest["structuredContent"]["metadata"]["stale_version"],
                false
            );
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(10));
    }
    drop(mcp);
    drop(guard);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_pending_rejected_fragments_survive_seven_day_cleanup_on_real_s3() {
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
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    input(
        &root,
        json!({"epoch":3,"content":[[replacement("b","r2","r1","fixture document 3")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let entries = incremental::envelopes(&worker).await;
    let rejected = entries
        .iter()
        .find(|(_, e)| {
            e.source_id == "incremental"
                && e.kind == fyrr_core::connector::SyncKind::Content
                && e.sequence == 2
        })
        .unwrap()
        .1
        .clone();
    let pending = entries
        .iter()
        .find(|(_, e)| {
            e.source_id == "incremental"
                && e.kind == fyrr_core::connector::SyncKind::Content
                && e.sequence == 3
        })
        .unwrap()
        .1
        .clone();
    let id = rejected.envelope_id.clone();
    let transform: corrections::ResponseTransform =
        Arc::new(move |request, response| age_observations(request, response, Some(&id)));
    let sequence = publication(&commit).await.1.sequence + 1;
    let fired = Arc::new(AtomicBool::new(false));
    let observed = fired.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, body, after| {
        after
            && request.starts_with("PUT ")
            && request.contains("/head.json")
            && serde_json::from_slice::<Value>(body)
                .is_ok_and(|v| v["content"]["sequence"] == sequence)
            && !observed.swap(true, Ordering::SeqCst)
    });
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, thread) = corrections::proxy_transform(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        transform,
        Some(hook),
    );
    let mut guard = thread;
    guard.resume = Arc::new(AtomicBool::new(true));
    let config = proxied(&commit, &endpoint);
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config,format!("{text}\n[writer]\nenvelope_max_rows=1\nmax_envelope_age='1h'\n[writer.commit]\ngroup_max_rows=1\n")).unwrap();
    let lost = host_command(&config)
        .args(["writer", "commit"])
        .output()
        .unwrap();
    assert!(!lost.status.success(), "first publication response is lost");
    assert!(fired.load(Ordering::SeqCst));
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let head = publication(&commit).await.0;
    assert!(
        fyrr_writer::commit::unconsumed_rejections(&loaded.config, &head, &[])
            .await
            .unwrap()
            .iter()
            .any(|m| m.envelope_id == rejected.envelope_id)
    );
    assert!(
        fyrr_writer::commit::pending(&loaded.config, &head)
            .await
            .unwrap()
            .iter()
            .any(|e| e.envelope_id == pending.envelope_id)
    );
    let files: Vec<_> = [&rejected, &pending]
        .into_iter()
        .flat_map(|e| match &e.payload {
            fyrr_core::envelope::EnvelopePayload::Content(p) => p
                .fragments
                .iter()
                .flat_map(|f| f.files.iter().map(|f| f.path.clone()))
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect();
    let maintained = command(&config, &["writer", "maintain"]);
    assert!(
        maintained["unverified_protected_fragment_files"]
            .as_u64()
            .unwrap()
            >= files.len() as u64,
        "7-day native candidates must actually be protected: {maintained}"
    );
    for file in &files {
        assert!(
            store
                .head(&object_store::path::Path::from(format!(
                    "{ns}/dataset.lance/{file}"
                )))
                .await
                .is_ok()
        );
    }
    for e in [&rejected, &pending] {
        assert!(
            store
                .head(
                    &ns.clone()
                        .join("envelopes")
                        .join(format!("{}.json", e.envelope_id))
                )
                .await
                .is_ok()
        );
    }
    // Refetch consumes the immutable rejection, then old worker fragment files
    // become eligible. A larger deletion group completes the real cleanup.
    input(
        &root,
        json!({"epoch":4,"content":[[replacement("a","r2","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert!(
        fyrr_writer::commit::unconsumed_rejections(
            &loaded.config,
            &publication(&commit).await.0,
            &[]
        )
        .await
        .unwrap()
        .is_empty()
    );
    std::fs::write(
        &config,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("group_max_rows=1", "group_max_rows=10000"),
    )
    .unwrap();
    command(&config, &["writer", "maintain"]);
    for file in &files {
        assert!(
            matches!(
                store
                    .head(&object_store::path::Path::from(format!(
                        "{ns}/dataset.lance/{file}"
                    )))
                    .await,
                Err(object_store::Error::NotFound { .. })
            ),
            "consumed file was not released: {file}"
        );
    }
    drop(guard);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_restore_crash_boundaries_on_real_s3() {
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
        let remaining = (head.lease.unwrap().expires_at - chrono::Utc::now())
            .to_std()
            .unwrap_or_default();
        std::thread::sleep(remaining + Duration::from_millis(50));
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
async fn maintenance_purge_window_and_restore_horizon_on_real_s3() {
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
    let config = reader_config(&worker, &root, "off");
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
    let (old_head, old) = publication(&commit).await;
    let config = reader_config(&worker, &root, "off");
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
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_partial_unreadable_chunk_purge_on_real_s3() {
    let (worker, commit, root) = fixture();
    initial(&root);
    let mut value: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    value["content"][0][0]["texts"] = json!(["fixture document 0", "fixture document 1"]);
    value["content"][0][0]["section_readers"] = json!([[], ["grp:group"]]);
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
    assert_eq!(incremental::rows(&worker).await.num_rows(), 3);
    command(&commit, &["writer", "maintain"]);
    assert_eq!(
        incremental::rows(&worker).await.num_rows(),
        2,
        "only the unreadable chunk is physically removed"
    );
    assert_eq!(command(&commit, &["writer", "verify"])["verified"], true);
    let reader = reader_config(&worker, &root, "interval");
    let mut mcp = tools::Mcp::open(&reader);
    assert!(readable(&fetch(&mut mcp, "a")));
    assert!(readable(&fetch(&mut mcp, "b")));
    value["epoch"] = json!(2);
    input(&root, value);
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert_eq!(
        incremental::rows(&worker).await.num_rows(),
        2,
        "same-revision retry does not refill an unreadable chunk"
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_off_aged_pin_reload_and_verify_damage_on_real_s3() {
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
    let old = publication(&commit).await.1;
    let old_id = old.publication_id.clone();
    let transform: corrections::ResponseTransform = Arc::new(move |request, response| {
        if request.contains(&old_id) {
            age_observations(request, response, None);
        }
    });
    let blocked = Arc::new(AtomicBool::new(false));
    let gate = blocked.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        !after
            && gate.load(Ordering::SeqCst)
            && request.starts_with("GET ")
            && request.contains("/_versions/")
    });
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, thread) = corrections::proxy_transform(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        transform,
        Some(hook),
    );
    let mut guard = thread;
    guard.resume = Arc::new(AtomicBool::new(true));
    let config = reader_config(&worker, &root, "off");
    let config = proxied(&config, &endpoint);
    let mut mcp = tools::Mcp::open(&config);
    assert_eq!(
        fetch(&mut mcp, "a")["structuredContent"]["metadata"]["publication_id"],
        old.publication_id
    );
    let mirror = fyrr_core::config::load(&config)
        .unwrap()
        .config
        .reader
        .mirror_path
        .unwrap();
    let blobs: Vec<_> = std::fs::read_dir(&mirror)
        .unwrap()
        .flat_map(|d| std::fs::read_dir(d.unwrap().path()).unwrap())
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "blob"))
        .collect();
    assert!(!blobs.is_empty());
    blocked.store(true, Ordering::SeqCst);
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let latest = publication(&commit).await.1;
    let unavailable = fetch(&mut mcp, "a");
    assert_eq!(
        unavailable["isError"], true,
        "expired pin cannot serve through preparation failure: {unavailable}"
    );
    assert_eq!(
        unavailable["structuredContent"]["code"],
        "storage_unavailable"
    );
    let start = Instant::now();
    while blobs.iter().any(|p| p.exists()) {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "expired generation must release its mirror"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    blocked.store(false, Ordering::SeqCst);
    let start = Instant::now();
    loop {
        let result = fetch(&mut mcp, "a");
        if result["structuredContent"]["metadata"]["publication_id"] == latest.publication_id {
            assert!(readable(&result));
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "aged off pin must reload: {result}"
        );
    }
    drop(mcp);
    drop(guard);
    let stop = Arc::new(AtomicBool::new(false));
    let hook: corrections::BoundaryHook = Arc::new(|request, _, after| {
        !after && request.starts_with("GET ") && request.contains("/data/")
    });
    let (endpoint, thread) = corrections::proxy_at_boundary(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        hook,
    );
    let mut guard = thread;
    guard.resume = Arc::new(AtomicBool::new(true));
    let config = proxied(&commit, &endpoint);
    let invalid = host_command(&config)
        .args(["writer", "verify"])
        .output()
        .unwrap();
    assert!(
        !invalid.status.success(),
        "verify must read content, not trust index metadata"
    );
    assert_eq!(
        publication(&commit).await.1.publication_id,
        latest.publication_id
    );
    drop(guard);
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
        input(&root, value);
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
    }
}
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_delta_cleanup_step_exceeds_deadline() {
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
    command(&commit, &["writer", "maintain"]);
    input(
        &root,
        json!({"epoch":2,"permissions":[[{"id":"a","revision":"p2","readers":[]}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "permissions"]);
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, _) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let (before, _) = publication(&commit).await;
    let (_, applied) = fyrr_writer::commit::envelope_paths(&loaded.config, &before)
        .await
        .unwrap();
    let pending: Vec<_> = super::incremental::envelopes(&worker)
        .await
        .into_iter()
        .filter(|(_, e)| {
            e.kind == fyrr_core::connector::SyncKind::Permissions
                && !applied.contains(&e.envelope_id)
        })
        .collect();
    assert!(pending.iter().any(|(_, e)| matches!(&e.payload, fyrr_core::envelope::EnvelopePayload::Permissions(p) if p.documents.iter().any(|d| d.root_id == "a" && d.readers.is_empty()))));
    let mut prepared = Vec::new();
    // Hide every part of the prepared read, including its final attestation.
    // Provider startup is outside the five-second maintenance proof.
    for (path, _) in pending {
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        store.delete(&path).await.unwrap();
        prepared.push((path, bytes));
    }
    // Stage delivery outside the timed step. Hide it only from discovery
    // until cleanup reaches the stalled request; no client/runtime startup or
    // network PUT consumes the five-second deadline window.
    for (path, bytes) in &prepared {
        store.put(path, bytes.clone().into()).await.unwrap();
    }
    let fired = Arc::new(AtomicBool::new(false));
    let observed = fired.clone();
    let stalled_at = Arc::new(std::sync::Mutex::new(None));
    let observed_at = stalled_at.clone();
    let authorization_boundary = Arc::new(std::sync::Mutex::new(None));
    let observed_boundary = authorization_boundary.clone();
    let permission_reads: Vec<_> = prepared
        .iter()
        .map(|(path, _)| path.filename().unwrap().to_owned())
        .collect();
    let visible = fired.clone();
    let hidden_reads = permission_reads.clone();
    let transform: corrections::ResponseTransform = Arc::new(move |request, body| {
        if request.starts_with("GET ")
            && request.contains("list-type=2")
            && request.contains("envelopes")
            && !visible.load(Ordering::SeqCst)
        {
            corrections::rewrite_ok_body(body, |bytes| {
                let mut text = String::from_utf8(bytes.clone()).unwrap();
                let mut offset = 0;
                let mut changed = false;
                while let Some(start) = text[offset..].find("<Contents>") {
                    let start = start + offset;
                    let end = start + text[start..].find("</Contents>").unwrap() + 11;
                    if hidden_reads
                        .iter()
                        .any(|name| text[start..end].contains(name))
                    {
                        text.replace_range(start..end, "");
                        offset = start;
                        changed = true;
                    } else {
                        offset = end;
                    }
                }
                *bytes = text.into_bytes();
                changed
            });
        }
    });
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if !after
            && request.starts_with("GET ")
            && permission_reads.iter().any(|path| request.contains(path))
            && observed_at.lock().unwrap().is_some()
        {
            observed_boundary
                .lock()
                .unwrap()
                .get_or_insert_with(Instant::now);
        }
        if !after
            && request.starts_with("GET ")
            && request.contains("list-type=2")
            && request.contains("_transactions")
            && !observed.swap(true, Ordering::SeqCst)
        {
            *observed_at.lock().unwrap() = Some(Instant::now());
            std::thread::sleep(Duration::from_secs(8));
        }
        false
    });
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, thread) = corrections::proxy_transform(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        transform,
        Some(hook),
    );
    let mut guard = thread;
    guard.resume = Arc::new(AtomicBool::new(true));
    let cfg = proxied(&commit, &endpoint);
    let text = std::fs::read_to_string(&cfg).unwrap();
    std::fs::write(
        &cfg,
        format!("{text}\n[writer.maintenance]\nstep_max_duration='5s'\n"),
    )
    .unwrap();
    let result = host_command(&cfg)
        .args(["writer", "maintain"])
        .output()
        .unwrap();
    // The deadline bounds cleanup planning, not the subsequent atomic auth
    // publication. Observe its first new permission read before the stalled
    // listing can return; keep checking publication, denial and lease release.
    let started = stalled_at.lock().unwrap().unwrap_or_else(|| {
        panic!(
            "listing hook not reached: status={}, stderr={}",
            result.status,
            String::from_utf8_lossy(&result.stderr)
        )
    });
    let boundary = authorization_boundary.lock().unwrap().unwrap_or_else(|| {
        panic!(
            "authorization boundary not reached: status={}, stderr={}",
            result.status,
            String::from_utf8_lossy(&result.stderr)
        )
    });
    let elapsed = boundary.duration_since(started);
    assert!(
        elapsed < Duration::from_millis(7500),
        "cleanup did not yield to authorization before stalled listing returned: boundary={elapsed:?}, CLI={:?}",
        started.elapsed()
    );
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("maintenance_step_deadline"));
    let (_, head) = publication(&commit).await;
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let actual = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    assert!(
        actual
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at <= chrono::Utc::now()),
        "deadline must release lease while retaining its epoch"
    );
    let mut mcp = tools::Mcp::open(&worker);
    assert!(
        !readable(&fetch(&mut mcp, "a")),
        "pending revoke must publish at timeout boundary: status={}, manifest={head:?}",
        command(&commit, &["writer", "status"])
    );
    assert!(readable(&fetch(&mut mcp, "b")));
    assert!(head.sequence > 0);
    drop(mcp);
    drop(guard);
    command(&commit, &["writer", "maintain"]);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_delta_idle_expired_mirror_survives_failed_preparation() {
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
    let old = publication(&commit).await.1;
    let old_id = old.publication_id.clone();
    let transform: corrections::ResponseTransform = Arc::new(move |request, response| {
        if request.contains(&old_id) {
            age_observations(request, response, None);
        }
    });
    let blocked = Arc::new(AtomicBool::new(false));
    let gate = blocked.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        !after
            && gate.load(Ordering::SeqCst)
            && request.starts_with("GET ")
            && request.contains("/_versions/")
    });
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, thread) = corrections::proxy_transform(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        transform,
        Some(hook),
    );
    let mut guard = thread;
    guard.resume = Arc::new(AtomicBool::new(true));
    let config = reader_config(&worker, &root, "off");
    let config = proxied(&config, &endpoint);
    let mut mcp = tools::Mcp::open(&config);
    assert_eq!(
        fetch(&mut mcp, "a")["structuredContent"]["metadata"]["publication_id"],
        old.publication_id
    );
    let mirror = fyrr_core::config::load(&config)
        .unwrap()
        .config
        .reader
        .mirror_path
        .unwrap();
    let blobs: Vec<_> = std::fs::read_dir(&mirror)
        .unwrap()
        .flat_map(|d| std::fs::read_dir(d.unwrap().path()).unwrap())
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "blob"))
        .collect();
    assert!(!blobs.is_empty());
    blocked.store(true, Ordering::SeqCst);
    input(
        &root,
        json!({"epoch":2,"content":[[{"delete":true,"id":"a","revision":"r2"}]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    command(&commit, &["writer", "maintain"]);
    std::thread::sleep(Duration::from_secs(3));
    let remaining = blobs.iter().filter(|p| p.exists()).count();
    drop(mcp);
    drop(guard);
    assert_eq!(
        remaining, 0,
        "idle reader retains expired generation cache without an MCP request"
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_delta_status_purge_risk_is_bounded_and_value_free() {
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
    assert_eq!(
        command(&commit, &["writer", "status"])["warnings"],
        json!([])
    );
    let transform: corrections::ResponseTransform =
        Arc::new(|request, response| age_observations(request, response, None));
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, thread) = corrections::proxy_transform(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        transform,
        None,
    );
    let mut guard = thread;
    guard.resume = Arc::new(AtomicBool::new(true));
    let config = proxied(&commit, &endpoint);
    assert_eq!(
        command(&config, &["writer", "status"])["warnings"],
        json!(["physical_purge_deadline_at_risk"])
    );
    drop(guard);
    command(&commit, &["writer", "maintain"]);
    let (_, maintained) = publication(&commit).await;
    assert!(maintained.maintenance_completed_at.is_some());
    assert_eq!(
        command(&commit, &["writer", "status"])["warnings"],
        json!([])
    );
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    assert_eq!(
        publication(&commit).await.1.maintenance_completed_at,
        maintained.maintenance_completed_at,
        "ordinary commits must not reset maintenance age"
    );
    let transform: corrections::ResponseTransform =
        Arc::new(|request, response| age_observations(request, response, None));
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, thread) = corrections::proxy_transform(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop.clone(),
        transform,
        None,
    );
    let mut guard = thread;
    guard.resume = Arc::new(AtomicBool::new(true));
    let config = proxied(&commit, &endpoint);
    assert_eq!(
        command(&config, &["writer", "status"])["warnings"],
        json!(["physical_purge_deadline_at_risk"]),
        "old physical payloads must warn even after recently completed maintenance"
    );
    drop(guard);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_shared_mirror_repair_and_crashed_holder_on_real_s3() {
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
    let original = publication(&commit).await.1;
    let config = reader_config(&worker, &root, "off");
    std::fs::write(
        &config,
        std::fs::read_to_string(&config).unwrap().replace(
            "mirror_max_bytes = 16777216",
            "mirror_max_bytes = 536870912",
        ),
    )
    .unwrap();
    let cold = command(&config, &["reader", "check"]);
    assert_eq!(cold["state"], "serving", "{cold}");
    assert!(
        cold["s3_requests"].as_u64().unwrap() > 0,
        "cold opening must perform real storage reads"
    );
    let warm = command(&config, &["reader", "prewarm"]);
    assert_eq!(warm["state"], "warm-ready", "{warm}");
    let mut first = tools::Mcp::open(&config);
    let mut second = tools::Mcp::open(&config);
    let mut expected = fetch(&mut first, "a");
    assert!(readable(&expected));
    assert_eq!(
        fetch(&mut second, "a")["structuredContent"]["documents"],
        expected["structuredContent"]["documents"]
    );
    let loaded = fyrr_core::config::load(&config).unwrap();
    let directory = std::fs::read_dir(loaded.config.reader.mirror_path.as_deref().unwrap())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let blobs: Vec<_> = std::fs::read_dir(&directory)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "blob"))
        .collect();
    assert!(!blobs.is_empty());
    for path in &blobs {
        let meta = std::fs::symlink_metadata(path).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        assert!(!meta.file_type().is_symlink());
    }
    assert_eq!(
        std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    // SIGKILL closes the kernel lock. A new reader sweeps the crashed inventory,
    // while the second live reader's independently held pin protects every file.
    assert!(
        Command::new("kill")
            .args(["-KILL", &first.pid().to_string()])
            .status()
            .unwrap()
            .success()
    );
    drop(first);
    let mut third = tools::Mcp::open(&config);
    assert!(
        blobs.iter().all(|p| p.exists()),
        "live cross-process pins protect the shared files"
    );
    assert_eq!(
        fetch(&mut third, "a")["structuredContent"]["documents"],
        expected["structuredContent"]["documents"]
    );
    let active_leases = std::fs::read_dir(&directory)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|e| e == "lease")
        })
        .count();
    assert_eq!(
        active_leases, 2,
        "crashed inventory expires during the new reader's sweep"
    );
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let latest = publication(&commit).await.1;
    let uri = fyrr_reader::storage::dataset_uri(&loaded.config);
    let (bucket, dataset_root) = uri.strip_prefix("s3://").unwrap().split_once('/').unwrap();
    // Version manifest entries are bound to the publication that pins them.
    let original_binding = digest(&write_contract(&original));
    let blob = |path: &str| {
        let mut key = format!("s3${bucket}:{dataset_root}/{path}");
        if path.starts_with("_versions/") {
            key = format!("{key}:{original_binding}");
        }
        directory.join(format!("{}.blob", &digest(key.as_bytes())[7..]))
    };
    let unchanged: Vec<_> = original
        .files
        .iter()
        .filter(|f| latest.files.iter().any(|n| n.path == f.path))
        .map(|f| {
            let path = blob(&f.path);
            let metadata = std::fs::metadata(&path).unwrap();
            (path, metadata.modified().unwrap())
        })
        .collect();
    let obsolete: Vec<_> = original
        .files
        .iter()
        .filter(|f| !latest.files.iter().any(|n| n.path == f.path))
        .map(|f| blob(&f.path))
        .collect();
    assert!(!unchanged.is_empty());
    assert!(!obsolete.is_empty());
    let mut new_reader = tools::Mcp::open(&config);
    expected = fetch(&mut new_reader, "a");
    assert!(readable(&expected));
    assert_eq!(
        expected["structuredContent"]["metadata"]["publication_id"],
        latest.publication_id
    );
    assert!(
        obsolete.iter().all(|p| p.exists()),
        "the other processes still pin the old generation"
    );
    assert!(
        unchanged
            .iter()
            .all(|(p, at)| std::fs::metadata(p).unwrap().modified().unwrap() == *at),
        "unchanged files are reused, not rewritten"
    );
    drop(second);
    drop(third);
    let wait = Instant::now();
    while obsolete.iter().any(|p| p.exists()) {
        assert!(
            wait.elapsed() < Duration::from_secs(30),
            "crashed old holders must expire and retire their obsolete files"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(unchanged.iter().all(|(p, _)| p.exists()));
    drop(new_reader);
    // Bit damage and missing files are repaired from S3 before warm readiness.
    std::fs::write(&unchanged[0].0, b"damaged immutable cache").unwrap();
    if unchanged.len() > 1 {
        std::fs::remove_file(&unchanged[1].0).unwrap();
    }
    let repaired = command(&config, &["reader", "prewarm"]);
    assert_eq!(repaired["state"], "warm-ready", "{repaired}");
    assert!(repaired["s3_requests"].as_u64().unwrap() > 0);
    let mut repaired_reader = tools::Mcp::open(&config);
    assert_eq!(
        fetch(&mut repaired_reader, "a")["structuredContent"]["documents"],
        expected["structuredContent"]["documents"]
    );
    drop(repaired_reader);
    let empty = root.join("empty-mirror.toml");
    std::fs::write(
        &empty,
        std::fs::read_to_string(&config).unwrap().replace(
            loaded.config.reader.mirror_path.as_deref().unwrap(),
            &root.join("empty-mirror").display().to_string(),
        ),
    )
    .unwrap();
    let mut empty_reader = tools::Mcp::open(&empty);
    assert_eq!(
        fetch(&mut empty_reader, "a")["structuredContent"]["documents"],
        expected["structuredContent"]["documents"]
    );
    drop(empty_reader);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_preparation_budget_refusal_preserves_authorization_on_real_s3() {
    let (worker, commit, root) = fixture();
    initial(&root);
    let mut seed: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    seed["permissions"][0][1]["readers"] = json!([]);
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
    let config = reader_config(&worker, &root, "off");
    let bounded = root.join("budget-reader.toml");
    std::fs::write(
        &bounded,
        std::fs::read_to_string(&config).unwrap().replace(
            "[reader]",
            "[reader]\nmemory_max_bytes='512MiB'\nmemory_headroom_bytes='500MiB'\n",
        ),
    )
    .unwrap();
    for verb in ["check", "prewarm", "serve"] {
        let output = host_command(&bounded)
            .args(["reader", verb])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            output.stdout.is_empty(),
            "refused reader never announces serving"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .lines()
                .any(|l| l == "error: preparation_budget_impossible")
        );
    }
    let mut valid = tools::Mcp::open(&config);
    assert!(readable(&fetch(&mut valid, "a")));
    assert!(!readable(&fetch(&mut valid, "b")));
}
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_warming_does_not_block_per_request_version_adoption_on_real_s3() {
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
    let original = publication(&commit).await.1;
    let text_uuid = &original
        .indexes
        .iter()
        .find(|i| i.name == "fts_idx")
        .unwrap()
        .uuid;
    let held_file = original
        .files
        .iter()
        .filter(|f| f.path.starts_with(&format!("_indices/{text_uuid}/")))
        .max_by_key(|f| f.size_bytes)
        .unwrap()
        .path
        .clone();
    let reached = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let head_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (r, s, h) = (reached.clone(), release.clone(), head_reads.clone());
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if !after && request.starts_with("GET ") && request.contains("/head.json") {
            h.fetch_add(1, Ordering::SeqCst);
        }
        if !after
            && request.starts_with("GET ")
            && request.contains(&held_file)
            && !r.swap(true, Ordering::SeqCst)
        {
            while !s.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        false
    });
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, mut guard) =
        corrections::proxy_at_boundary(std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(), stop, hook);
    guard.resume = release.clone();
    let config = reader_config(&worker, &root, "per-request");
    let config = proxied(&config, &endpoint);
    std::fs::write(
        &config,
        std::fs::read_to_string(&config).unwrap().replace(
            "mirror_max_bytes = 16777216",
            "mirror_max_bytes = 536870912",
        ),
    )
    .unwrap();
    let log = root.join("warming-reader.log");
    let mut reader = tools::Mcp::open_logged(&config, &log);
    let wait = Instant::now();
    while !reached.load(Ordering::SeqCst) {
        assert!(
            wait.elapsed() < Duration::from_secs(30),
            "warming download must reach the held proxy"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let before = head_reads.load(Ordering::SeqCst);
    let result = fetch(&mut reader, "a");
    assert!(readable(&result));
    assert!(
        head_reads.load(Ordering::SeqCst) > before,
        "per-request checks continue while physical warming is held"
    );
    assert!(!release.load(Ordering::SeqCst));
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let latest = publication(&commit).await.1;
    let wait = Instant::now();
    loop {
        let result = fetch(&mut reader, "a");
        assert!(readable(&result));
        if result["structuredContent"]["metadata"]["publication_id"] == latest.publication_id {
            break;
        }
        assert!(
            wait.elapsed() < Duration::from_secs(30),
            "new generation must not wait for the warming download"
        );
    }
    assert!(
        !release.load(Ordering::SeqCst),
        "the new generation is adopted with the old download still held"
    );
    release.store(true, Ordering::SeqCst);
    drop(reader);
    drop(guard);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_small_mirror_stays_serving_without_rewarming_each_interval_on_real_s3() {
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
    let immutable = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = immutable.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if !after && !request.contains("/head.json") {
            count.fetch_add(1, Ordering::SeqCst);
        }
        false
    });
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, guard) =
        corrections::proxy_at_boundary(std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(), stop, hook);
    let config = reader_config(&worker, &root, "interval");
    let config = proxied(&config, &endpoint);
    std::fs::write(
        &config,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 1"),
    )
    .unwrap();
    let log = root.join("small-mirror-reader.log");
    let mut reader = tools::Mcp::open_logged(&config, &log);
    assert!(readable(&fetch(&mut reader, "a")));
    let wait = Instant::now();
    loop {
        if std::fs::read_to_string(&log)
            .unwrap()
            .contains("reason=mirror_capacity")
        {
            break;
        }
        assert!(wait.elapsed() < Duration::from_secs(30));
        std::thread::sleep(Duration::from_millis(10));
    }
    let before = immutable.load(Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(
        immutable.load(Ordering::SeqCst),
        before,
        "unchanged serving generation makes no repeated worker/storage preparation over fifteen check intervals"
    );
    assert_eq!(
        std::fs::read_to_string(&log)
            .unwrap()
            .matches("reason=mirror_capacity")
            .count(),
        1,
        "one loss-site telemetry event for the serving degradation"
    );
    assert!(readable(&fetch(&mut reader, "a")));
    drop(reader);
    drop(guard);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_per_request_checks_collect_only_on_the_periodic_interval_on_real_s3() {
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
    let config = reader_config(&worker, &root, "per-request");
    std::fs::write(
        &config,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace(
                "version_check_interval='100ms'",
                "version_check_interval='30s'",
            )
            .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 0"),
    )
    .unwrap();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let mut reader = tools::Mcp::open(&config);
    let started = Instant::now();
    let directories = std::fs::read_dir(loaded.config.reader.mirror_path.as_deref().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    assert_eq!(directories.len(), 1);
    let orphan = directories[0].join(format!("{}.partial", "a".repeat(64)));
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&orphan)
        .unwrap()
        .write_all(b"completed test orphan")
        .unwrap();
    for _ in 0..20 {
        assert!(readable(&fetch(&mut reader, "a")));
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the per-request proof must precede the first interval"
        );
        assert!(
            orphan.exists(),
            "per-request head checks must not collect the mirror"
        );
    }
    let wait = Instant::now();
    while orphan.exists() {
        assert!(
            wait.elapsed() < Duration::from_secs(70),
            "periodic collection must still retire the orphan"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        started.elapsed() >= Duration::from_secs(25),
        "collection occurred on the interval, not a request"
    );
    drop(reader);
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_native_cache_budget_refuses_shortfall_and_preserves_results_on_real_s3() {
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
    let config = reader_config(&worker, &root, "interval");
    let loaded = fyrr_core::config::load(&config).unwrap();
    let text = std::fs::read_to_string(&config).unwrap();
    let mut expected = tools::Mcp::open(&config);
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    let rows = expected.call("search", search.clone());
    assert_eq!(rows["isError"], false);
    assert!(
        !rows["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let documents = fetch(&mut expected, "a");
    assert_eq!(documents["isError"], false);
    drop(expected);
    let bounded = root.join("native-cache-budget.toml");
    std::fs::write(
        &bounded,
        text.replace(
            "[reader]",
            "[reader]\nmemory_max_bytes='512MiB'\nmemory_headroom_bytes='256MiB'\n",
        ),
    )
    .unwrap();
    for verb in ["check", "prewarm"] {
        let result = host_command(&bounded)
            .args(["reader", verb])
            .output()
            .unwrap();
        std::fs::write(
            root.join(format!("native-cache-{verb}.stderr")),
            &result.stderr,
        )
        .unwrap();
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&result.stderr)
                .lines()
                .any(|l| l == "error: preparation_budget_impossible")
        );
    }
    let log = root.join("native-cache-reader.stderr");
    // Successor/supersession checks retain a funded cache at the default budget.
    let live = root.join("native-cache-overlap.toml");
    std::fs::write(&live, &text).unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let reached = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (a, r, s) = (armed.clone(), reached.clone(), release.clone());
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if after
            && request.starts_with("GET ")
            && request.contains("/publications/")
            && a.load(Ordering::SeqCst)
            && !r.swap(true, Ordering::SeqCst)
        {
            // ProxyGuard resumes this boundary on unwind as well as success.
            // Keep it held through the successor's real fenced publication.
            while !s.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        false
    });
    let (endpoint, mut guard) = corrections::proxy_at_boundary(
        loaded.config.storage.endpoint.clone().unwrap(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    guard.resume = release.clone();
    let proxied = proxied(&live, &endpoint);
    let mut reader = tools::Mcp::open_logged(&proxied, &log);
    for _ in 0..3 {
        let actual = reader.call("search", search.clone());
        assert_eq!(actual["isError"], false, "{actual}");
        assert_eq!(
            actual["structuredContent"]["hits"],
            rows["structuredContent"]["hits"]
        );
        assert_eq!(
            fetch(&mut reader, "a")["structuredContent"]["documents"],
            documents["structuredContent"]["documents"]
        );
    }
    // Hold a real publication manifest after the storage response, then publish
    // its successor. The abandoned preparation must not announce a transition.
    let retained = publication(&commit).await.1;
    armed.store(true, Ordering::SeqCst);
    command(&commit, &["writer", "restore", &retained.publication_id]);
    let superseded = publication(&commit).await.1;
    let wait = Instant::now();
    while !reached.load(Ordering::SeqCst) {
        assert!(
            wait.elapsed() < Duration::from_secs(30),
            "preparation must reach the real manifest boundary"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for _ in 0..5 {
        command(&commit, &["writer", "restore", &retained.publication_id]);
        let latest = publication(&commit).await.1;
        assert_eq!(
            latest.indexes, retained.indexes,
            "equal-size switches retain native index identities"
        );
        release.store(true, Ordering::SeqCst);
        let wait = Instant::now();
        loop {
            let result = fetch(&mut reader, "a");
            if result["isError"] == true {
                assert_eq!(
                    result["structuredContent"]["code"], "authorization_unavailable",
                    "restore fencing must fail closed until adoption: {result}"
                );
            } else if result["structuredContent"]["metadata"]["publication_id"]
                == latest.publication_id
            {
                break;
            }
            assert!(
                wait.elapsed() < Duration::from_secs(30),
                "each publication must activate"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let fresh = host_command(&live)
            .args(["reader", "check"])
            .output()
            .unwrap();
        assert!(
            fresh.status.success(),
            "{}",
            String::from_utf8_lossy(&fresh.stderr)
        );
        let readiness: Value = serde_json::from_slice(&fresh.stdout).unwrap();
        assert_eq!(readiness["native_cache_degradation"], Value::Null);
        assert!(
            readiness["native_cache_available_bytes"].as_u64().unwrap()
                >= readiness["native_cache_needed_bytes"].as_u64().unwrap()
        );
    }
    drop(reader);
    drop(guard);
    let diagnostics = std::fs::read_to_string(&log).unwrap();
    assert!(
        !diagnostics.contains("reason=sequential_handover"),
        "no pointless cache drain: {diagnostics}"
    );
    assert!(
        !diagnostics.contains(&format!(
            "reader_refresh status=ready content_sequence={}\n",
            superseded.sequence
        )),
        "superseded preparation must never activate"
    );
    assert_eq!(
        std::fs::read_to_string(&log)
            .unwrap()
            .matches("native_cache=degraded reason=cache_budget")
            .count(),
        0
    );
}

async fn retirement_fixture() -> (PathBuf, PathBuf, FixtureRoot) {
    retirement_fixture_on(Backend::S3).await
}

/// The incremental fixture with its two documents published and readable.
async fn retirement_fixture_on(backend: Backend) -> (PathBuf, PathBuf, FixtureRoot) {
    let (worker, commit, root) = fixture_on(backend);
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
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_retired_source_commits_are_fenced_on_real_s3() {
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

/// Retiring a source installs an immediate denial in the policy object.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_retired_source_is_denied_at_once_on_real_s3() {
    let (worker, commit, root) = retirement_fixture().await;
    // A reader that never checks for new content versions.
    let off = reader_config(&worker, &root, "off");
    let mut client = tools::Mcp::open(&off);
    assert!(readable(&fetch(&mut client, "a")));
    let lanes =
        |config: &Path, readable| super::google_drive::lanes(config, "incremental", readable);
    assert!(lanes(&off, true).values().all(|found| !found.is_empty()));
    let sequence = |head: ServingHead| head.content.map(|pointer| pointer.sequence);
    let content = sequence(publication(&worker).await.0);

    command(&commit, &["writer", "source", "retire", "incremental"]);
    assert_eq!(
        sequence(publication(&worker).await.0),
        content,
        "no content publication was needed"
    );
    // The very next request of the open reader.
    let denied = fetch(&mut client, "a");
    assert!(!denied["isError"].as_bool().unwrap_or(false), "{denied}");
    assert_eq!(denied["structuredContent"]["documents"], json!([]));
    assert_eq!(
        denied["structuredContent"]["metadata"]["hidden_sources"],
        json!([]),
        "a retired source is not reported"
    );
    assert!(lanes(&off, false).values().all(|found| found.is_empty()));
    let others = client.call("search", json!({"query":"fixture query","limit":5}));
    let hits = others["structuredContent"]["hits"].as_array().unwrap();
    assert!(!hits.is_empty() && hits.iter().all(|hit| hit["source_id"] == "allowed"));
    // Readers opened later are denied as well.
    assert!(!readable(&fetch(&mut tools::Mcp::open(&worker), "a")));
}

/// A retired source remains denied after restore.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_retired_source_survives_restore_and_rollback_on_real_s3() {
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

/// Past retention, a purged source leaves neither old native versions nor reader mirror text.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_purged_source_expires_native_versions_and_mirror_on_real_s3() {
    use futures::TryStreamExt;

    let (worker, commit, root) = retirement_fixture().await;
    let marker = "purge-only fixture text";
    root.embedding
        .insert_both(marker, root.embedding.vector("fixture document 0"));
    input(
        &root,
        json!({"epoch":2,"content":[[replacement("a","r2","r1",marker)]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    let head = publication(&commit).await.0;
    let transform: corrections::ResponseTransform =
        Arc::new(|request, response| age_observations(request, response, None));
    let stop = Arc::new(AtomicBool::new(false));
    let (endpoint, mut guard) = corrections::proxy_transform(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        stop,
        transform,
        None,
    );
    guard.resume = Arc::new(AtomicBool::new(true));
    let reader = proxied(&reader_config(&worker, &root, "off"), &endpoint);
    let telemetry = root.join("purge-mirror-reader.log");
    let mut client = tools::Mcp::open_logged(&reader, &telemetry);
    assert!(readable(&fetch(&mut client, "a")));
    assert!(readable(&fetch(&mut client, "b")));
    let warming = Instant::now();
    while !std::fs::read_to_string(&telemetry)
        .unwrap()
        .contains("reader_refresh status=ready")
    {
        assert!(warming.elapsed() < Duration::from_secs(30));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let native_root = ns.join("dataset.lance");
    let old_versions: Vec<_> = store
        .list(Some(&native_root.clone().join("_versions")))
        .try_collect()
        .await
        .unwrap();
    assert!(!old_versions.is_empty());
    // Identify actual published source fragments, not envelopes or matching shared text.
    let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    let mut source_files = std::collections::BTreeSet::new();
    for fragment in dataset.get_fragments() {
        let rows = fragment
            .scan()
            .project(&["source_id", "searchable_text"])
            .unwrap()
            .filter("source_id = 'incremental'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        if rows.num_rows() > 0 {
            source_files.extend(
                fragment
                    .metadata()
                    .files
                    .iter()
                    .map(|file| native_root.clone().join("data").join(file.path.as_str())),
            );
        }
    }
    drop(dataset);
    assert!(!source_files.is_empty());
    let mut source_hashes = std::collections::BTreeSet::new();
    for path in &source_files {
        let bytes = store.get(path).await.unwrap().bytes().await.unwrap();
        source_hashes.insert(digest(&bytes));
    }
    let mirror = fyrr_core::config::load(&reader)
        .unwrap()
        .config
        .reader
        .mirror_path
        .unwrap();
    let blobs = || {
        std::fs::read_dir(&mirror)
            .unwrap()
            .flat_map(|dir| std::fs::read_dir(dir.unwrap().path()).unwrap())
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "blob")
            })
            .collect::<Vec<_>>()
    };
    let holds_source = || {
        blobs().iter().any(|path| match std::fs::read(path) {
            Ok(bytes) => source_hashes.contains(&digest(&bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => panic!("mirror read failed: {error}"),
        })
    };
    assert!(
        holds_source(),
        "source payloads were warmed into the mirror"
    );
    let holds_marker = || {
        blobs().iter().any(|path| match std::fs::read(path) {
            Ok(bytes) => bytes
                .windows(marker.len())
                .any(|text| text == marker.as_bytes()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => panic!("mirror read failed: {error}"),
        })
    };
    assert!(
        holds_marker(),
        "unique source text is physically present before purge"
    );

    command(
        &commit,
        &["writer", "source", "purge", "incremental", "--confirm"],
    );
    assert_eq!(held_rows(&commit).await, 0);
    // The cached pin has crossed the same purge window observed by maintenance.
    assert!(!readable(&fetch(&mut client, "a")));
    let cleanup = proxied(&commit, &endpoint);
    for _ in 0..10 {
        command(&cleanup, &["writer", "maintain"]);
        let mut remains = false;
        for file in old_versions
            .iter()
            .map(|file| &file.location)
            .chain(source_files.iter())
        {
            remains |= store.head(file).await.is_ok();
        }
        if !remains {
            break;
        }
    }
    for file in old_versions
        .iter()
        .map(|file| &file.location)
        .chain(source_files.iter())
    {
        assert!(
            matches!(
                store.head(file).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "purged source's retained version or payload remains: {file}"
        );
    }
    let current = publication(&commit).await.1;
    let current_versions: std::collections::BTreeSet<_> = current
        .files
        .iter()
        .filter(|file| file.path.starts_with("_versions/"))
        .map(|file| object_store::path::Path::from(format!("{native_root}/{}", file.path)))
        .collect();
    let remaining_versions: Vec<_> = store
        .list(Some(&native_root.clone().join("_versions")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        remaining_versions
            .into_iter()
            .map(|file| file.location)
            .collect::<std::collections::BTreeSet<_>>(),
        current_versions,
        "only the current publication's native manifest remains"
    );
    // Expiring an old pin starts asynchronous preparation; wait for the serving handover.
    let handover = Instant::now();
    loop {
        let result = client.call("fetch", json!({"source_id":"allowed","root_id":"0.txt"}));
        if readable(&result) {
            assert_eq!(
                result["structuredContent"]["metadata"]["publication_id"],
                current.publication_id
            );
            break;
        }
        let code = result["structuredContent"]["code"]
            .as_str()
            .unwrap_or("missing");
        assert!(
            matches!(
                code,
                "busy" | "storage_unavailable" | "authorization_unavailable"
            ),
            "unexpected reader handover error: {code}"
        );
        assert!(
            handover.elapsed() < Duration::from_secs(15),
            "reader handover did not finish: {code}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let evicting = Instant::now();
    while holds_source() || holds_marker() {
        assert!(
            evicting.elapsed() < Duration::from_secs(10),
            "purged text remains in the private mirror"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(command(&commit, &["writer", "verify"])["verified"], true);
    assert_eq!(held_rows(&commit).await, 0);
    drop(client);
    drop(guard);
}

/// Purge a disconnected source.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_disconnected_source_purges_on_real_s3() {
    maintenance_disconnected_source_purges(Backend::S3).await;
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn maintenance_disconnected_source_purges_on_local_storage() {
    maintenance_disconnected_source_purges(Backend::Local).await;
}

async fn maintenance_disconnected_source_purges(backend: Backend) {
    let (worker, commit, root) = retirement_fixture_on(backend).await;
    let before = publication(&worker).await.1.publication_id;
    assert!(readable(&fetch(&mut tools::Mcp::open(&worker), "a")));
    input(
        &root,
        json!({"epoch":2,"content":[[doc("c","r1","fixture document 2")]]}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    // Disconnected: the configuration no longer names the source.
    for config in [&worker, &commit] {
        let text = std::fs::read_to_string(config).unwrap();
        let (kept, dropped) = text.split_once("\n[[sources]]\nid='incremental'").unwrap();
        assert!(!dropped.contains("[[sources]]"));
        std::fs::write(config, kept).unwrap();
    }
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let cursor = namespace
        .clone()
        .join("cursors")
        .join("incremental-content.json");
    let of_source = |all: Vec<(
        object_store::path::Path,
        fyrr_core::envelope::PreparedEnvelope,
    )>| {
        all.into_iter()
            .filter(|(_, envelope)| envelope.source_id == "incremental")
            .count()
    };

    assert!(identity_inputs(&commit).await.contains("incremental"));
    // The dry run names what would go and changes nothing.
    let plan = command(&commit, &["writer", "source", "purge", "incremental"]);
    assert_eq!(plan["dry_run"], true);
    assert_eq!(plan["rows"], 2);
    assert!(plan["envelopes"].as_u64().unwrap() >= 4 && plan["cursors"].as_u64().unwrap() >= 3);
    assert_eq!(held_rows(&commit).await, 2);
    assert!(of_source(envelopes(&commit).await) >= 4);
    assert!(store.head(&cursor).await.is_ok());

    let purged = command(
        &commit,
        &["writer", "source", "purge", "incremental", "--confirm"],
    );
    assert_eq!(purged["purged"], true);
    assert_eq!(held_rows(&commit).await, 0);
    assert!(
        !identity_inputs(&commit).await.contains("incremental"),
        "its principal snapshot is no identity input any more"
    );
    assert_eq!(of_source(envelopes(&commit).await), 0);
    assert!(matches!(
        store.head(&cursor).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(!readable(&fetch(&mut tools::Mcp::open(&worker), "a")));
    assert!(readable(&client_fetch_allowed(&worker)));
    command(&commit, &["writer", "verify"]);
    // Running it again converges, and the commit after it is clean.
    let again = command(
        &commit,
        &["writer", "source", "purge", "incremental", "--confirm"],
    );
    assert_eq!(again["rows"], 0);
    publish(&commit);
    // A restore to the publication that still held the source brings nothing back.
    command(&commit, &["writer", "restore", &before]);
    assert_eq!(held_rows(&commit).await, 0);
    assert!(!readable(&fetch(&mut tools::Mcp::open(&worker), "a")));
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

/// A purge that dies between deleting the rows and cleaning up the source's
/// envelopes and cursors converges when it is run again.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_purge_killed_before_its_cleanup_converges_on_real_s3() {
    let (worker, commit, _root) = retirement_fixture().await;
    let stop = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicBool::new(false));
    let resume = Arc::new(AtomicBool::new(false));
    let (hook_fired, hook_resume) = (fired.clone(), resume.clone());
    // The first envelope delete: the rows are gone and published by then.
    let hook: super::corrections::BoundaryHook = Arc::new(move |request, body, response| {
        // One object, or the batch form with the keys in the body.
        let envelope_delete = (request.starts_with("DELETE ") && request.contains("/envelopes/"))
            || (request.starts_with("POST ")
                && request.contains("delete")
                && String::from_utf8_lossy(body).contains("/envelopes/"));
        let selected = envelope_delete && !response && !hook_fired.swap(true, Ordering::SeqCst);
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
            "cleanup not reached"
        );
        if purge.child().try_wait().unwrap().is_some() {
            let ended = purge.finish();
            panic!(
                "the purge ended before its cleanup: {} {}",
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
        0,
        "the rows were deleted and published"
    );
    assert!(
        envelopes_of_incremental(envelopes(&worker).await) > 0,
        "the cleanup had not happened"
    );
    assert!(!readable(&fetch(&mut tools::Mcp::open(&worker), "a")));
    // Wait for the recorded lease, including any renewal before the crash.
    let head = publication(&commit).await.0;
    let remaining = (head.lease.unwrap().expires_at - chrono::Utc::now())
        .to_std()
        .unwrap_or_default();
    tokio::time::sleep(remaining + Duration::from_millis(50)).await;
    let again = command(
        &commit,
        &["writer", "source", "purge", "incremental", "--confirm"],
    );
    assert_eq!(again["purged"], true);
    assert_eq!(again["rows"], 0);
    assert_eq!(envelopes_of_incremental(envelopes(&worker).await), 0);
    assert!(!identity_inputs(&worker).await.contains("incremental"));
    command(&commit, &["writer", "verify"]);
}

/// Recovery starts from the serving head after an interrupted native delete.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_purge_killed_before_its_head_publication_converges_on_real_s3() {
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
    let remaining = (head.lease.unwrap().expires_at - chrono::Utc::now())
        .to_std()
        .unwrap_or_default();
    tokio::time::sleep(remaining + Duration::from_millis(50)).await;
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

/// A worker finishing after the purge inventory cannot retain a new envelope.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_purged_source_late_envelope_is_deleted_on_real_s3() {
    for action in ["commit", "maintain"] {
        let (worker, commit, root) = retirement_fixture().await;
        input(
            &root,
            json!({"epoch":2,"content":[[doc("late","r1","fixture document 2")]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "content"]);
        let loaded = fyrr_core::config::load(&worker).unwrap();
        let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        let (path, late) = envelopes(&worker)
            .await
            .into_iter()
            .filter(|(_, e)| e.source_id == "incremental" && e.kind.as_str() == "content")
            .max_by_key(|(_, e)| e.sequence)
            .unwrap();
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        if let fyrr_core::envelope::EnvelopePayload::Content(payload) = &late.payload {
            for file in payload
                .fragments
                .iter()
                .flat_map(|fragment| &fragment.files)
            {
                assert!(
                    store
                        .head(&object_store::path::Path::from(format!(
                            "{namespace}/dataset.lance/{}",
                            file.path
                        )))
                        .await
                        .is_ok()
                );
            }
        }
        // Withhold only the envelope publication, as a worker stalled at its PUT would.
        store.delete(&path).await.unwrap();
        command(
            &commit,
            &["writer", "source", "purge", "incremental", "--confirm"],
        );
        store.put(&path, bytes.into()).await.unwrap();
        command(&commit, &["writer", action]);
        assert!(
            matches!(
                store.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "{action} retained an envelope published after its source was purged"
        );
        if let fyrr_core::envelope::EnvelopePayload::Content(payload) = late.payload {
            assert!(!payload.fragments.is_empty());
            for file in payload
                .fragments
                .iter()
                .flat_map(|fragment| &fragment.files)
            {
                assert!(
                    matches!(
                        store
                            .head(&object_store::path::Path::from(format!(
                                "{namespace}/dataset.lance/{}",
                                file.path
                            )))
                            .await,
                        Err(object_store::Error::NotFound { .. })
                    ),
                    "{action} retained the late fragment"
                );
            }
        }
        command(&commit, &["writer", "verify"]);
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
#[allow(clippy::print_stderr)]
async fn maintenance_restore_batched_authorization_boundaries_on_real_s3() {
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
    let config = reader_config(&worker, &root, "per-request");
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
        // Bound a hung child, including the real CLI commands its hooks invoke.
        // Event order comes from the acknowledged restore callbacks, not this watchdog.
        let mut probe = Command::new("timeout");
        probe.args(["--kill-after=5s", "180s"]).arg(
            std::env::var_os("FYRR_SCIM_HEALTH_TEST_BINARY")
                .expect("writer libtest built by scripts/ci"),
        );
        if let Some(case) = &case {
            probe.env("FYRR_TEST_RESTORE_REVIEW_CASE", case);
        } else {
            probe.env_remove("FYRR_TEST_RESTORE_REVIEW_CASE");
        }
        let output = probe
            .args([
                "maintenance::restore_tests::restore_drains_restarts_and_caps_attempts_on_real_s3",
                "--exact",
                "--ignored",
                "--nocapture",
            ])
            .env("FYRR_TEST_RESTORE_WORKER", &worker)
            .env("FYRR_TEST_RESTORE_CONFIG", &commit)
            .env("FYRR_TEST_RESTORE_INPUT", root.join("input.json"))
            .env("FYRR_TEST_RESTORE_MEMBERS", &members_path)
            .env("FYRR_TEST_RESTORE_TARGET", &target.publication_id)
            .env("FYRR_TEST_RESTORE_BINARY", env!("CARGO_BIN_EXE_fyrr"))
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
        assert!(
            output.status.success(),
            "{}\n{}",
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

#[tokio::test]
#[ignore = "real CLI request counts and RustFS; scripts/ci"]
#[allow(clippy::print_stdout)]
async fn maintenance_boundary_history_request_counts_on_real_s3() {
    use futures::TryStreamExt;
    use fyrr_core::{
        Contract,
        envelope::{PreparedEnvelope, RejectionMarker, envelope_id},
        publication::ContentPointer,
        write_contract,
    };
    use std::sync::Mutex;
    // Vary each scaling axis independently, then combine their maxima.
    // Envelope counts span one, two and three discovery pages.
    for (envelopes, depth) in [
        (256usize, 2usize),
        (1064, 2),
        (2064, 2),
        (256, 4),
        (256, 8),
        (2064, 8),
    ] {
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
        // Real append publications create enough small fragments to
        // traverse many boundaries, independently of retained history.
        for revision in 2..=1 + depth * 4 {
            let id = format!("extra-{revision}");
            input(
                &root,
                json!({"epoch":revision,
                    "content":[[doc(&id,"r1","fixture document 0")]],
                    "permissions":[[{"id":id,"revision":"p1","readers":["grp:group"]}]]
                }),
            );
            worker_sync(
                &worker,
                &["incremental", "--kind", "content", "--kind", "permissions"],
            );
            publish(&commit);
        }
        // Append indexes have different coverage until the first
        // merge. The metered command then compacts that shared coverage.
        command(&commit, &["writer", "maintain"]);
        let (mut head, target) = publication(&commit).await;
        let loaded = fyrr_core::config::load(&commit).unwrap();
        let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        let base_depth = fyrr_reader::storage::manifest_chain(
            store.clone(),
            ns.clone(),
            head.content.as_ref().map(|p| p.manifest.clone()),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len();
        let prefix = ns.clone().join("envelopes");
        let objects: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
        let mut ids = Vec::new();
        let mut seed = None;
        for object in objects {
            let e: PreparedEnvelope =
                fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                    .await
                    .unwrap();
            ids.push(e.envelope_id.clone());
            if e.kind.is_authorization() {
                seed = Some(e);
            }
        }
        let seed = seed.unwrap();
        for sequence in 1000..1000 + (envelopes - ids.len()) as u64 {
            let mut e = seed.clone();
            e.sequence = sequence;
            e.envelope_id = envelope_id(
                &e.namespace,
                &e.source_id,
                e.kind,
                e.sequence,
                e.rejection_generation,
                e.cursor.start.as_deref(),
            );
            e.validate().unwrap();
            store
                .put(
                    &prefix.clone().join(format!("{}.json", e.envelope_id)),
                    write_contract(&e).into(),
                )
                .await
                .unwrap();
            ids.push(e.envelope_id);
        }
        let rejected = ids.pop().unwrap();
        let marker = RejectionMarker {
            schema_version: fyrr_core::envelope::ENVELOPE_CONTRACT_VERSION,
            envelope_id: rejected.clone(),
            source_id: seed.source_id.clone(),
            kind: seed.kind,
            sequence: 9999,
            rejection_generation: 0,
            restore_sequence: 0,
            start_cursor: None,
            reason: "envelope_expired".into(),
        };
        store
            .put(
                &object_store::path::Path::from(format!("{ns}/rejections/{rejected}.json")),
                write_contract(&marker).into(),
            )
            .await
            .unwrap();
        let mut manifest = target.clone();
        for index in 1..depth {
            manifest.previous_publication_id = Some(manifest.publication_id.clone());
            manifest.sequence += 1;
            manifest.publication_id = format!("pub-{:032x}", 1000 + index);
            manifest.envelope_ids = ids.clone();
            manifest.rejected_envelope_ids = vec![rejected.clone()];
            let path = format!("publications/{}.json", manifest.publication_id);
            store
                .put(
                    &object_store::path::Path::from(format!("{ns}/{path}")),
                    write_contract(&manifest).into(),
                )
                .await
                .unwrap();
            head.content = Some(ContentPointer {
                publication_id: manifest.publication_id.clone(),
                sequence: manifest.sequence,
                lance_version: manifest.lance_version,
                manifest: path,
                committed_at: manifest.committed_at,
            });
        }
        store
            .put(&ns.clone().join("head.json"), write_contract(&head).into())
            .await
            .unwrap();
        // Count retained history separately from the one predecessor GET
        // required by each native publication. A publication's new manifest
        // is not an ancestral-history reread at a quiet boundary.
        let historical: std::collections::BTreeSet<_> = fyrr_reader::storage::manifest_chain(
            store.clone(),
            ns.clone(),
            head.content.as_ref().map(|p| p.manifest.clone()),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .filter(|m| Some(&m.publication_id) != head.content.as_ref().map(|p| &p.publication_id))
        .map(|m| format!("/publications/{}.json", m.publication_id))
        .collect();
        // One cache fill plus eight existing cleanup history walks. Native
        // boundaries must contribute none, regardless of their number.
        let history_limit = historical.len() * 9;
        // Each fresh LIST (not a continuation page) begins a boundary interval.
        let counts = Arc::new(Mutex::new(vec![[0usize; 4]]));
        let observed = counts.clone();
        let hook: super::corrections::BoundaryHook = Arc::new(move |request, _, after| {
            if after {
                return false;
            }
            let mut counts = observed.lock().unwrap();
            let listing = request.starts_with("GET ")
                && request.contains("list-type=2")
                && request.contains("envelopes");
            if listing && !request.contains("continuation-token=") {
                counts.push([0; 4]);
            }
            let category = if listing {
                Some(3)
            } else if request.starts_with("GET ") {
                if request.contains("/envelopes/") {
                    Some(0)
                } else if request.contains("/rejections/") {
                    Some(1)
                } else if request.contains("/publications/")
                    && historical.iter().any(|path| request.contains(path))
                {
                    Some(2)
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(category) = category {
                counts.last_mut().unwrap()[category] += 1;
            }
            false
        });
        let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
        let (proxy, guard) = super::corrections::proxy_at_boundary(
            endpoint.clone(),
            Arc::new(AtomicBool::new(false)),
            hook,
        );
        let metered = root.join("restore-metered.toml");
        std::fs::write(
            &metered,
            std::fs::read_to_string(&commit)
                .unwrap()
                .replace(&endpoint, &proxy),
        )
        .unwrap();
        let mut command = host_command(&metered);
        command.args(["writer", "maintain"]);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        drop(guard);
        let counts = counts.lock().unwrap();
        let mut total = [0; 4];
        for interval in counts.iter() {
            for i in 0..4 {
                total[i] += interval[i];
            }
        }
        let steps = result["compaction_steps"].as_u64().unwrap() as usize;
        assert!(
            steps >= 6,
            "maintenance must exercise many native boundaries: {result}"
        );
        // First LIST seeds summaries; the next begins the first native
        // step. Cleanup may read bodies once and is outside this range.
        let quiet = &counts[2..2 + steps];
        println!(
            "maintain_counts E={envelopes} H={} N={} total_body_marker_history_list={total:?} quiet_boundaries={} quiet_first={:?}",
            base_depth + depth - 1,
            1538 + depth * 4,
            quiet.len(),
            quiet.first()
        );
        assert!(
            quiet.len() >= 6,
            "several groups must traverse quiet boundaries"
        );
        for boundary in quiet {
            assert_eq!(
                &boundary[..3],
                &[0, 0, 0],
                "quiet boundary must read no bodies, markers or history"
            );
            assert_eq!(
                boundary[3],
                envelopes.div_ceil(1000),
                "fresh complete paginated discovery at every boundary"
            );
        }
        assert!(
            total[0] <= envelopes * 5,
            "body reads must not multiply by boundaries"
        );
        assert!(
            total[1] <= depth + 4,
            "marker reads must not multiply by boundaries"
        );
        assert!(
            total[2] <= history_limit,
            "history reads must not multiply by boundaries"
        );
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_restore_discovers_authorization_after_failed_cursor_on_real_s3() {
    discovers_authorization_after_failed_cursor("restore").await;
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_boundary_discovers_authorization_after_failed_cursor_on_real_s3() {
    discovers_authorization_after_failed_cursor("maintain").await;
}

async fn discovers_authorization_after_failed_cursor(operation: &str) {
    for kind in ["permissions", "members"] {
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
        let config = reader_config(&worker, &root, "per-request");
        let mut reader = tools::Mcp::open(&config);
        assert!(
            !readable(&fetch(&mut reader, "a")),
            "{kind}: fresh discovery must apply the durable revoke despite failed cursor advancement"
        );
        assert_eq!(readable(&fetch(&mut reader, "b")), kind == "permissions");
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_restore_changed_envelope_identity_fails_closed_on_real_s3() {
    use std::sync::atomic::AtomicUsize;
    let (_, commit, root) = fixture();
    let target = publication(&commit).await.1;
    let lists = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(AtomicBool::new(false));
    let altered = changed.clone();
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let transform: super::corrections::ResponseTransform = Arc::new(move |request, response| {
        if request.contains("list-type=2")
            && request.contains("envelopes")
            && lists.fetch_add(1, Ordering::SeqCst) == 1
        {
            super::corrections::rewrite_ok_body(response, |body| {
                let mut text = String::from_utf8(body.clone()).unwrap();
                let start = text.find("<ETag>").unwrap() + 6;
                let end = start + text[start..].find("</ETag>").unwrap();
                text.replace_range(start..end, "&quot;changed-immutable-object&quot;");
                *body = text.into_bytes();
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
        "second complete LIST must expose the changed immutable identity"
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("restore_history_identity_changed"));
    assert_eq!(
        publication(&commit).await.1.publication_id,
        target.publication_id
    );
}

#[tokio::test]
#[ignore = "requires owned real-S3 fixture"]
async fn maintenance_restore_rejected_authorization_freezes_freshness_on_real_s3() {
    use super::incremental::{envelopes, rows};
    use fyrr_core::connector::SyncKind;
    use fyrr_core::envelope::EnvelopePayload;
    for kind in [SyncKind::Permissions, SyncKind::Members] {
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
        let target = publication(&worker).await.1;
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
        command(&commit, &["writer", "restore", &target.publication_id]);
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
    }
}

/// Both reader statements and retired rows remain authoritative across root pages.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn maintenance_restore_batched_own_readers_and_retired_rows_on_real_s3() {
    let (worker, commit, root) = fixture();
    initial(&root);
    let mut seed: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    let documents: Vec<_> = (0..300)
        .map(|n| {
            let mut value = doc(&format!("own-{n}"), "r1", "fixture document 0");
            value["texts"] = json!(["fixture document 0", "fixture document 1"]);
            value["section_readers"] = json!([null, null]);
            value
        })
        .collect();
    seed["content"] = json!(documents.chunks(100).collect::<Vec<_>>());
    let permissions: Vec<_> = (0..300)
        .map(|n| json!({"id":format!("own-{n}"),"revision":"p1","readers":["grp:group"]}))
        .collect();
    seed["permissions"] = json!(permissions.chunks(100).collect::<Vec<_>>());
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
    let target = publication(&commit).await.1.publication_id;
    let changes: Vec<_> = (0..300)
        .map(|n| {
            let mut value = replacement(&format!("own-{n}"), "r2", "r1", "fixture document 0");
            value["texts"] = json!(["fixture document 0", "fixture document 1"]);
            value["section_readers"] = json!([null, []]);
            value
        })
        .collect();
    input(
        &root,
        json!({"epoch":2,"content":changes.chunks(100).collect::<Vec<_>>()}),
    );
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    publish(&commit);
    command(&commit, &["writer", "source", "retire", "allowed"]);
    assert_eq!(source_rows(&worker, "allowed").await.num_rows(), 768);

    for (iteration, rollback) in [true, false].into_iter().enumerate() {
        if rollback {
            command(&commit, &["writer", "rollback"]);
        } else {
            command(&commit, &["writer", "restore", &target]);
        }
        assert_eq!(source_rows(&worker, "allowed").await.num_rows(), 0);
        assert_eq!(
            source_rows(&worker, &fyrr_writer::apply::state_source("allowed"))
                .await
                .num_rows(),
            0
        );
        let (head, manifest) = publication(&commit).await;
        let loaded = fyrr_core::config::load(&commit).unwrap();
        let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
            .await
            .unwrap();
        let mut updates = 0;
        for recorded in &manifest.lineage.transactions {
            let tx = dataset
                .read_version_transaction(recorded.version)
                .await
                .unwrap()
                .transaction
                .unwrap();
            if matches!(
                tx.operation,
                lance::dataset::transaction::Operation::Update { .. }
            ) {
                updates += 1;
            }
        }
        assert!(
            updates >= 5,
            "300 active and 768 retired roots require multiple groups: {updates}"
        );
        // A later authoritative permission grant exercises the stored statement,
        // not just the effective reader set written during restore.
        let permissions: Vec<_> = (0..300)
            .map(|n| json!({"id":format!("own-{n}"),"revision":format!("p{}",iteration+2),"readers":["grp:group"]}))
            .collect();
        input(
            &root,
            json!({"epoch":iteration+3,"permissions":permissions.chunks(100).collect::<Vec<_>>()}),
        );
        worker_sync(&worker, &["incremental", "--kind", "permissions"]);
        publish(&commit);
        let batch = rows(&worker).await;
        assert_eq!(batch.num_rows(), 600);
        let texts = fyrr_writer::apply::strings(&batch, "searchable_text").unwrap();
        let metadata = fyrr_writer::apply::strings(&batch, "metadata_json").unwrap();
        for row in 0..batch.num_rows() {
            let own: Value = serde_json::from_str(metadata.value(row)).unwrap();
            let tokens = fyrr_writer::apply::tokens(&batch, row).unwrap();
            if texts.value(row) == "fixture document 1" {
                assert_eq!(own["_fyrr_own_readers"], json!([]));
                assert!(tokens.is_empty(), "restored section regained a grant");
            } else {
                assert!(!tokens.is_empty(), "inherited section lost its grant");
            }
        }
        command(&commit, &["writer", "verify"]);
    }
}

/// Public CLI reproduction of a section restatement after restore or rollback.
#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
#[allow(clippy::print_stderr)]
async fn maintenance_section_restatement_after_restore_on_real_s3() {
    let mut failures = Vec::new();
    for rollback in [false, true] {
        let variant = if rollback { "rollback" } else { "restore" };
        let (worker, commit, root) = fixture();
        initial(&root);
        let mut seed: Value =
            serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
        let mut document = doc("section", "r1", "fixture document 0");
        document["texts"] = json!(["fixture document 0", "fixture document 1"]);
        document["section_readers"] = json!([["grp:group"], ["grp:group"]]);
        seed["content"] = json!([[document]]);
        seed["permissions"] = json!([[{"id":"section","revision":"p1","readers":["grp:group"]}]]);
        input(&root, seed);
        // This document-only reproduction has no membership read to rewind.
        worker_sync(
            &worker,
            &["incremental", "--kind", "content", "--kind", "permissions"],
        );
        publish(&commit);
        let target = publication(&commit).await.1.publication_id;
        let trace = |step: &str, batch: &arrow_array::RecordBatch| {
            assert_eq!(
                batch.num_rows(),
                2,
                "{variant} {step}: two current sections"
            );
            for row in 0..batch.num_rows() {
                let metadata: Value = serde_json::from_str(
                    fyrr_writer::apply::strings(batch, "metadata_json")
                        .unwrap()
                        .value(row),
                )
                .unwrap();
                let own = metadata
                    .get("_fyrr_own_readers")
                    .map(|v| v.as_array().unwrap());
                let own_state = match own {
                    None => "absent",
                    Some(readers) if readers.is_empty() => "empty",
                    Some(_) => "present",
                };
                let state = fyrr_writer::apply::state(batch, row).unwrap();
                eprintln!(
                    "section_trace variant={variant} step={step} row={row} revision={} own={own_state} own_count={} row_readers={} document_readers={}",
                    state.revision.as_deref().unwrap_or("absent"),
                    own.map_or(0, Vec::len),
                    fyrr_writer::apply::tokens(batch, row).unwrap().len(),
                    state.permission.as_ref().map_or(0, |p| p.readers.len())
                );
            }
        };
        trace("publish", &rows(&worker).await);
        input(
            &root,
            json!({"epoch":2,"permissions":[[{"id":"section","revision":"p2","readers":[]}]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "permissions"]);
        publish(&commit);
        trace("revoke", &rows(&worker).await);
        if rollback {
            command(&commit, &["writer", "rollback"]);
        } else {
            command(&commit, &["writer", "restore", &target]);
        }
        trace("restore", &rows(&worker).await);
        input(
            &root,
            json!({"epoch":3,"permissions":[[{"id":"section","revision":"p3","readers":["grp:group"]}]]}),
        );
        worker_sync(&worker, &["incremental", "--kind", "permissions"]);
        publish(&commit);
        trace("grant", &rows(&worker).await);
        document["revision"] = json!("r2");
        document["expected_prior_revision"] = json!("r1");
        input(&root, json!({"epoch":4,"content":[[document]]}));
        worker_sync(&worker, &["incremental", "--kind", "content"]);
        trace("prepare", &rows(&worker).await);
        let loaded = fyrr_core::config::load(&worker).unwrap();
        let head = publication(&worker).await.0;
        let unresolved = fyrr_writer::commit::unconsumed_rejections(&loaded.config, &head, &[])
            .await
            .unwrap();
        let pending = fyrr_writer::commit::pending(&loaded.config, &head)
            .await
            .unwrap();
        let prepared = pending
            .iter()
            .find(|e| {
                e.source_id == "incremental" && e.kind == fyrr_core::connector::SyncKind::Content
            })
            .unwrap();
        let fyrr_core::envelope::EnvelopePayload::Content(payload) = &prepared.payload else {
            unreachable!()
        };
        assert_eq!(payload.documents[0].source_revision.as_deref(), Some("r2"));
        assert_eq!(
            payload.documents[0].expected_prior_revision.as_deref(),
            Some("r1")
        );
        eprintln!(
            "section_prepared variant={variant} revision=r2 prior=r1 unresolved_members={}",
            unresolved
                .iter()
                .filter(|m| m.source_id == "incremental"
                    && m.kind == fyrr_core::connector::SyncKind::Members)
                .count()
        );
        publish(&commit);
        let current = rows(&worker).await;
        trace("commit", &current);
        let restated = fyrr_writer::apply::permission_readers(
            &loaded.config,
            "incremental",
            &["grp:group".into()],
            &[],
        )
        .unwrap();
        for row in 0..current.num_rows() {
            let metadata: Value = serde_json::from_str(
                fyrr_writer::apply::strings(&current, "metadata_json")
                    .unwrap()
                    .value(row),
            )
            .unwrap();
            if metadata["_fyrr"]["revision"] != json!("r2") {
                failures.push(format!("{variant} row {row}: new revision was not applied"));
            }
            let own: Vec<String> =
                serde_json::from_value(metadata["_fyrr_own_readers"].clone()).unwrap();
            assert_eq!(
                own, restated,
                "{variant}: apply the restated section readers"
            );
            let permission = fyrr_writer::apply::state(&current, row)
                .unwrap()
                .permission
                .unwrap();
            let intersection: Vec<_> = own
                .into_iter()
                .filter(|token| permission.readers.contains(token))
                .collect();
            assert_eq!(
                fyrr_writer::apply::tokens(&current, row).unwrap(),
                intersection,
                "{variant}: remain within document authority"
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("; "));
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
    let listed = Arc::new(AtomicBool::new(false));
    let gate_listed = listed.clone();
    let (gate, hook) = super::corrections::RequestGate::new(move |request, _, after| {
        if !after && request == gate_get {
            assert!(
                gate_listed.load(Ordering::SeqCst),
                "envelope GET must follow its real LIST"
            );
            return true;
        }
        false
    });
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let statuses = observed.clone();
    let key = path.to_string();
    let transform = Arc::new(move |request: &str, response: &mut Vec<u8>| {
        let text = String::from_utf8_lossy(response);
        if request.contains("list-type=2") && text.contains(&key) {
            assert!(text.starts_with("HTTP/1.1 200 "));
            listed.store(true, Ordering::SeqCst);
        }
        if request == get {
            statuses
                .lock()
                .unwrap()
                .push(text.lines().next().unwrap().to_owned());
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
