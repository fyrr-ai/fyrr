//! Observe actual fsyncs at real prepare and publication boundaries.
use super::*;
use std::collections::BTreeMap;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

fn command(config: &Path, args: &[&str], log: &Path, worker: bool) {
    let mut command = if worker {
        coordination::worker_command(config)
    } else {
        coordination::host_command(config)
    };
    let output = command
        .env("FYRR_TEST_SYNC_LOG", log)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Checks every acknowledgement in a recorder log against the fsyncs before
/// it and returns the acknowledged puts per parent directory name.
///
/// - A put is acknowledged only after its staged inode was synced before the
///   publish, the object's directory after the publish, and every ancestor
///   up to the storage root at some earlier point.
/// - A delete is acknowledged only after its directory was synced after the
///   unlink. A cleanup batch owes that sync when the batch is acknowledged.
/// - Nothing is synced while an object's lock is held, including the
///   staging lock a published inode carries until its writer drops it.
/// - A control object lives in its namespace directory or one directory
///   below it, which is as far as the flush of observed objects reaches.
fn acknowledged_puts(log: &Path, storage: &Path) -> BTreeMap<String, usize> {
    let mut synced_files = BTreeMap::<PathBuf, usize>::new();
    let mut synced_directories = BTreeMap::<PathBuf, usize>::new();
    let mut published = BTreeMap::<PathBuf, usize>::new();
    let mut unlinked = BTreeMap::<PathBuf, usize>::new();
    let mut deferred = Vec::<PathBuf>::new();
    let mut puts = BTreeMap::<String, usize>::new();
    // One blocking mutation runs on one thread of one process.
    let mut held = std::collections::BTreeSet::<(u64, String)>::new();
    let after = |directories: &BTreeMap<PathBuf, usize>, path: &Path, event: usize| {
        directories
            .get(path.parent().unwrap())
            .is_some_and(|sync| *sync > event)
    };
    for (index, line) in std::fs::read_to_string(log).unwrap().lines().enumerate() {
        let event: Value = serde_json::from_str(line).unwrap();
        let path = PathBuf::from(event["path"].as_str().unwrap());
        let thread = (
            event["pid"].as_u64().unwrap(),
            event["thread"].as_str().unwrap().to_owned(),
        );
        match (
            event["event"].as_str().unwrap(),
            event["operation"].as_str(),
        ) {
            ("lock" | "stage-lock", _) => {
                assert!(held.insert(thread), "one lock at a time per mutation");
            }
            ("unlock" | "stage-unlock", _) => {
                assert!(held.remove(&thread), "unlock without lock");
            }
            ("sync", _) if held.contains(&thread) => {
                panic!("synced under an object's lock: {}", path.display())
            }
            ("sync", _) if event["directory"] == true => {
                synced_directories.insert(path, index);
            }
            ("sync", _) => {
                synced_files.insert(path, index);
            }
            ("publish", _) => {
                let staged = PathBuf::from(event["staged"].as_str().unwrap());
                assert!(
                    synced_files.contains_key(&staged),
                    "published before its bytes and tag were synced: {}",
                    path.display()
                );
                published.insert(path, index);
            }
            ("unlink", _) => {
                unlinked.insert(path, index);
            }
            ("ack", Some("put")) => {
                let namespace = storage.join("namespaces/dense-fixture");
                let below = path.strip_prefix(&namespace).unwrap().components().count();
                assert!(below <= 2, "control object too deep: {}", path.display());
                let publish = *published
                    .get(&path)
                    .unwrap_or_else(|| panic!("acknowledged unpublished {}", path.display()));
                assert!(
                    after(&synced_directories, &path, publish),
                    "put acknowledged before its directory was synced: {}",
                    path.display()
                );
                for ancestor in path
                    .parent()
                    .unwrap()
                    .ancestors()
                    .take_while(|ancestor| ancestor.starts_with(storage))
                {
                    assert!(
                        synced_directories.contains_key(ancestor),
                        "put acknowledged with an unsynced ancestor {}: {}",
                        ancestor.display(),
                        path.display()
                    );
                }
                let directory = path.parent().unwrap().file_name().unwrap();
                *puts
                    .entry(directory.to_string_lossy().into_owned())
                    .or_default() += 1;
            }
            ("ack", Some("delete")) => {
                let unlink = *unlinked.get(&path).unwrap_or_else(|| {
                    panic!("acknowledged delete without unlink {}", path.display())
                });
                assert!(
                    after(&synced_directories, &path, unlink),
                    "delete acknowledged before its directory was synced: {}",
                    path.display()
                );
            }
            ("exists", _) => {}
            ("ack", Some("deferred")) => deferred.push(path),
            ("ack", Some("cleanup")) => {
                for path in deferred.drain(..) {
                    assert!(
                        after(&synced_directories, &path, unlinked[&path]),
                        "cleanup acknowledged before a directory was synced: {}",
                        path.display()
                    );
                }
            }
            other => panic!("unknown recorder event {other:?}"),
        }
    }
    puts
}

#[test]
#[ignore = "real prepare/publication fsync recorder, including existing ancestor names"]
fn references_are_synced_before_real_prepare_and_publication() {
    let (config, root) = setup_on(Backend::Local);
    for n in 8..768 {
        std::fs::remove_file(root.join("sources").join(format!("{n}.txt"))).unwrap();
    }
    // A connector source beside the local files.
    let documents = |revision: &str| {
        let documents: Vec<_> = (0..8)
            .map(|n| {
                json!({"id":format!("doc-{n}"),"title":"document","author":"Ada",
                    "readers":["grp:allow"],"note":format!("note-{n}"),
                    "revision":revision,"text":format!("fixture document {n}")})
            })
            .collect();
        std::fs::write(
            root.join("documents.json"),
            serde_json::to_vec(&json!({"documents":documents,"cursor":revision})).unwrap(),
        )
        .unwrap();
    };
    documents("r1");
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!(
            "{text}\n[[sources]]\nid='metadata'\ntype='sentinel-docs'\npermissions='fixed'\n\
             readers=['grp:allow']\n[sources.scope]\nfixture='{}'\n",
            root.join("documents.json").display()
        ),
    )
    .unwrap();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let Some(fyrr_core::config::StorageBackend::Local { root: storage }) =
        loaded.config.storage.backend()
    else {
        unreachable!()
    };
    // Existing names may have been created by a killed writer or native Lance.
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(storage.join("namespaces/dense-fixture/envelopes"))
        .unwrap();
    let log = root.join("syncs.jsonl");
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&log)
        .unwrap();
    command(&config, &["writer", "namespace", "create"], &log, false);
    command(
        &config,
        &["writer", "sync", "allowed", "denied", "metadata", "--full"],
        &log,
        true,
    );
    command(&config, &["writer", "commit"], &log, false);
    std::fs::write(root.join("sources/0.txt"), "fixture document 1").unwrap();
    // New revisions: a second prepare and publication.
    documents("r2");
    command(
        &config,
        &[
            "writer", "sync", "allowed", "metadata", "--kind", "content", "--full",
        ],
        &log,
        true,
    );
    command(&config, &["writer", "commit"], &log, false);
    let mut files = BTreeMap::<PathBuf, (usize, bool)>::new();
    let mut directories = BTreeMap::<PathBuf, usize>::new();
    let mut prepares = 0;
    let mut publications = 0;
    let mut heads = 0;
    // The latest directory sync per process, and what each manifest owes.
    let mut own_syncs = BTreeMap::<(u64, PathBuf), usize>::new();
    let mut manifest_envelopes = BTreeMap::<PathBuf, Vec<PathBuf>>::new();
    let mut reachable = std::collections::BTreeSet::<PathBuf>::new();
    let mut observed_envelopes = 0;
    for (index, line) in std::fs::read_to_string(&log).unwrap().lines().enumerate() {
        let event: Value = serde_json::from_str(line).unwrap();
        let path = PathBuf::from(event["path"].as_str().unwrap());
        let pid = event["pid"].as_u64().unwrap();
        match event["event"].as_str().unwrap() {
            "exists" => {
                // "It exists" is an acknowledgement: the process that says so
                // flushed the name itself, after the winner published it.
                let published = files.get(&path).map_or(0, |(publish, _)| *publish);
                assert!(
                    own_syncs
                        .get(&(pid, path.parent().unwrap().to_path_buf()))
                        .is_some_and(|sync| *sync > published),
                    "existing object reported before its directory was synced: {}",
                    path.display()
                );
            }
            "sync" => {
                if event["directory"] == true {
                    own_syncs.insert((pid, path.clone()), index);
                    directories.insert(path, index);
                } else {
                    files.insert(path, (index, true));
                }
            }
            "publish" => {
                assert!(
                    files.contains_key(&PathBuf::from(event["staged"].as_str().unwrap())),
                    "staged bytes/xattr not synced: {}",
                    path.display()
                );
                for reference in event["references"].as_array().unwrap() {
                    let reference = PathBuf::from(reference.as_str().unwrap());
                    let (synced, native) = files.get(&reference).unwrap_or_else(|| {
                        panic!("unsynced referenced file {}", reference.display())
                    });
                    for (level, directory) in reference
                        .parent()
                        .unwrap()
                        .ancestors()
                        .take_while(|p| p.starts_with(&storage))
                        .enumerate()
                    {
                        let directory_sync = directories.get(directory).unwrap_or_else(|| {
                            panic!("unsynced referenced ancestor {}", directory.display())
                        });
                        // Native persistence flushes the complete chain after file
                        // sync. Atomic control publication needs a later parent sync.
                        if *native || level == 0 {
                            assert!(
                                directory_sync > synced,
                                "stale directory sync for {}",
                                reference.display()
                            );
                        }
                    }
                }
                if path.parent().unwrap().ends_with("envelopes")
                    && !event["references"].as_array().unwrap().is_empty()
                {
                    prepares += 1;
                }
                if path.parent().unwrap().ends_with("publications") {
                    publications += 1;
                    let envelopes = event["envelopes"].as_array().unwrap();
                    manifest_envelopes.insert(
                        path.clone(),
                        envelopes
                            .iter()
                            .map(|envelope| PathBuf::from(envelope.as_str().unwrap()))
                            .collect(),
                    );
                }
                if path.file_name().unwrap() == "head.json" {
                    heads += 1;
                    // Workers wrote the envelopes its manifest records. The
                    // publisher flushes their names itself before a head
                    // first makes them reachable; lease renewals repeat it.
                    for reference in event["references"].as_array().unwrap() {
                        let reference = PathBuf::from(reference.as_str().unwrap());
                        if !reachable.insert(reference.clone()) {
                            continue;
                        }
                        for envelope in manifest_envelopes.get(&reference).into_iter().flatten() {
                            let (written, _) = files.get(envelope).unwrap_or_else(|| {
                                panic!("unpublished envelope {}", envelope.display())
                            });
                            assert!(
                                own_syncs
                                    .get(&(pid, envelope.parent().unwrap().to_path_buf()))
                                    .is_some_and(|sync| sync > written),
                                "head published before its process synced {}",
                                envelope.display()
                            );
                            observed_envelopes += 1;
                        }
                    }
                }
                files.insert(path, (index, false));
            }
            _ => {}
        }
    }
    assert!(
        prepares >= 2 && publications >= 2 && heads >= 2,
        "real prepare and publication paths were not exercised"
    );
    assert!(
        observed_envelopes >= 2,
        "observed envelopes {observed_envelopes}"
    );
    // The page path (envelopes) and the publication path (manifests, the
    // head) each acknowledged durable puts.
    let puts = acknowledged_puts(&log, &storage);
    for (directory, least) in [("envelopes", 2), ("publications", 2), ("dense-fixture", 2)] {
        assert!(
            puts.get(directory).copied().unwrap_or(0) >= least,
            "{directory}: too few acknowledged puts in {puts:?}"
        );
    }
}

#[tokio::test]
#[ignore = "real purge recorder: fragment directories precede envelope unlink"]
async fn purged_fragments_are_durable_before_their_envelope_is_unlinked() {
    let (config, root) = setup_on(Backend::Local);
    for n in 8..768 {
        std::fs::remove_file(root.join("sources").join(format!("{n}.txt"))).unwrap();
    }
    let log = root.join("purge-syncs.jsonl");
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&log)
        .unwrap();
    command(&config, &["writer", "namespace", "create"], &log, false);
    command(
        &config,
        &["writer", "sync", "allowed", "denied", "--full"],
        &log,
        true,
    );
    command(&config, &["writer", "commit"], &log, false);
    let mut envelope = incremental::envelopes(&config)
        .await
        .into_iter()
        .find(|(_, e)| {
            e.source_id == "allowed" && e.kind == fyrr_core::connector::SyncKind::Content
        })
        .unwrap()
        .1;
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let Some(fyrr_core::config::StorageBackend::Local { root: storage }) =
        loaded.config.storage.backend()
    else {
        unreachable!()
    };
    let dataset = storage
        .join("namespaces")
        .join(&loaded.config.namespace.name)
        .join("dataset.lance");
    let fyrr_core::envelope::EnvelopePayload::Content(payload) = &mut envelope.payload else {
        unreachable!()
    };
    let mut fragments = Vec::new();
    for (n, file) in payload
        .fragments
        .iter_mut()
        .flat_map(|fragment| &mut fragment.files)
        .enumerate()
    {
        let path = format!("data/retired-review-{n}.lance");
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(dataset.join(&path))
            .unwrap()
            .set_len(file.size_bytes)
            .unwrap();
        file.path = path;
        fragments.push(dataset.join(&file.path));
    }
    command(
        &config,
        &["writer", "source", "purge", "allowed", "--confirm"],
        &log,
        false,
    );
    envelope.sequence += 100;
    envelope.envelope_id = fyrr_core::envelope::leased_envelope_id(
        &envelope.namespace,
        &envelope.source_id,
        envelope.kind,
        envelope.sequence,
        envelope.rejection_generation,
        envelope.cursor.start.as_deref(),
        envelope.lease_generation,
    );
    let path = namespace
        .clone()
        .join("envelopes")
        .join(format!("{}.json", envelope.envelope_id));
    store
        .put(&path, write_contract(&envelope).into())
        .await
        .unwrap();
    let envelope_file = dataset
        .parent()
        .unwrap()
        .join("envelopes")
        .join(format!("{}.json", envelope.envelope_id));
    command(&config, &["writer", "maintain"], &log, false);
    let mut unlinked = BTreeMap::new();
    let mut synced = BTreeMap::new();
    let mut checked = false;
    for (index, line) in std::fs::read_to_string(&log).unwrap().lines().enumerate() {
        let event: Value = serde_json::from_str(line).unwrap();
        let target = PathBuf::from(event["path"].as_str().unwrap());
        if event["event"] == "sync" && event["directory"] == true {
            synced.insert(target.clone(), index);
        }
        if event["event"] == "unlink" {
            if target == envelope_file {
                for fragment in &fragments {
                    let removed = unlinked.get(fragment).expect("fragment unlink missing");
                    assert!(
                        synced
                            .get(fragment.parent().unwrap())
                            .is_some_and(|sync| sync > removed),
                        "envelope removed before fragment directory sync"
                    );
                }
                checked = true;
            }
            unlinked.insert(target, index);
        }
    }
    assert!(checked && !fragments.is_empty());
    // Purge and maintenance acknowledge deletions only after their syncs.
    acknowledged_puts(&log, &storage);
    for fragment in fragments {
        assert!(!fragment.exists());
    }
    assert!(!envelope_file.exists());
}
