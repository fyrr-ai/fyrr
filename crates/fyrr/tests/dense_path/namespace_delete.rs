//! Verified export deletion must not remove a concurrently published head.
use super::coordination::{CommitProcess, publication};
use super::*;
use fyrr_core::publication::{ContentManifest, ContentPointer};
use object_store::{PutMode, UpdateVersion};
use std::sync::{Arc, atomic::AtomicBool};

async fn export_namespace(
    store: &dyn object_store::ObjectStore,
    namespace: &object_store::path::Path,
    export: &Path,
) {
    use futures::TryStreamExt;
    let objects: Vec<_> = store.list(Some(namespace)).try_collect().await.unwrap();
    for object in objects {
        let relative = fyrr_reader::storage::relative_path(&object.location, namespace);
        let path = export.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
            store
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "real CLI namespace deletion race on local storage"]
async fn delete_rejects_publication_after_verification_on_local_storage() {
    rejects_publication_after_verification(Backend::Local).await;
}

#[tokio::test]
#[ignore = "real CLI namespace deletion race on owned RustFS"]
async fn delete_rejects_publication_after_verification_on_real_s3() {
    rejects_publication_after_verification(Backend::S3).await;
}

async fn rejects_publication_after_verification(backend: Backend) {
    let setup = setup_on(backend);
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (_, commit, root) = incremental::fixture_host_at(8, true, "dense-fixture", setup);
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let export = root.join("export");
    export_namespace(store.as_ref(), &namespace, &export).await;
    let (gate, hook) = corrections::RequestGate::new(|request, body, after| {
        !after
            && ((request.starts_with("DELETE ") && request.contains("/head.json"))
                || (request.starts_with("POST ")
                    && request.contains("?delete")
                    && body
                        .windows(b"head.json".len())
                        .any(|part| part == b"head.json")))
    });
    let target = match backend {
        Backend::Local => "local-storage".into(),
        Backend::S3 => std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
    };
    let (endpoint, proxy) =
        corrections::proxy_at_boundary(target, Arc::new(AtomicBool::new(false)), hook);
    // Drop the gate before joining proxy workers, including on assertion failure.
    let deletion_boundary = gate;
    let fault = corrections::proxied(&commit, &endpoint);
    let process = CommitProcess::spawn_command(
        &fault,
        &[
            "writer",
            "namespace",
            "delete",
            "--export",
            export.to_str().unwrap(),
        ],
        Arc::new(AtomicBool::new(false)),
    );
    deletion_boundary.entered();
    let object = store
        .get(&namespace.clone().join("head.json"))
        .await
        .unwrap();
    let expected = UpdateVersion {
        e_tag: object.meta.e_tag.clone(),
        version: object.meta.version.clone(),
    };
    let mut head: ServingHead = fyrr_core::parse_contract(&object.bytes().await.unwrap()).unwrap();
    let mut manifest: ContentManifest = publication(&commit).await.1;
    manifest.previous_publication_id = Some(manifest.publication_id.clone());
    manifest.sequence += 1;
    manifest.publication_id = format!("pub-{:032x}", 9_000_000 + manifest.sequence);
    manifest.committed_at = fyrr_core::clock::now();
    let manifest_path = format!("publications/{}.json", manifest.publication_id);
    fyrr_writer::prepare::put_immutable(
        store.as_ref(),
        &object_store::path::Path::from(format!("{namespace}/{manifest_path}")),
        &manifest,
    )
    .await
    .unwrap();
    head.content = Some(ContentPointer {
        publication_id: manifest.publication_id.clone(),
        sequence: manifest.sequence,
        lance_version: manifest.lance_version,
        manifest: manifest_path,
        committed_at: manifest.committed_at,
    });
    head.lease = None;
    store
        .put_opts(
            &namespace.clone().join("head.json"),
            fyrr_core::write_contract(&head).into(),
            PutMode::Update(expected).into(),
        )
        .await
        .unwrap();
    deletion_boundary.release();
    let output = process.finish_bounded();
    assert!(
        !output.status.success(),
        "deletion accepted a head newer than its verified export"
    );
    let surviving = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        surviving.content.unwrap().publication_id,
        manifest.publication_id
    );
    drop(deletion_boundary);
    drop(proxy);
}

#[tokio::test]
#[ignore = "real CLI deletion excludes source workers"]
async fn delete_blocks_source_workers_during_verification_on_local_storage() {
    let setup = setup_on(Backend::Local);
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (_, commit, root) = incremental::fixture_host_at(8, true, "dense-fixture", setup);
    let config = fyrr_core::config::load(&commit).unwrap().config;
    let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
    // A cut left by maintenance is exported and goes with the head.
    let cut_path = namespace
        .clone()
        .join(fyrr_core::publication::HISTORY_CUT_OBJECT);
    store
        .put(
            &cut_path,
            fyrr_core::write_contract(&fyrr_core::publication::HistoryCut {
                schema_version:
                    <fyrr_core::publication::HistoryCut as fyrr_core::Contract>::VERSION,
                namespace: config.namespace.name.as_str().to_owned(),
                publication_id: "pub-00000000000000000000000000000041".into(),
                sequence: 41,
            })
            .into(),
        )
        .await
        .unwrap();
    let export = root.join("export");
    export_namespace(store.as_ref(), &namespace, &export).await;
    let (gate, hook) = corrections::RequestGate::new(|request, _, after| {
        after && request.starts_with("PUT ") && request.contains("/.namespace-delete.json")
    });
    let (endpoint, proxy) = corrections::proxy_at_boundary(
        "local-storage".into(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let deletion_boundary = gate;
    let fault = corrections::proxied(&commit, &endpoint);
    let process = CommitProcess::spawn_command(
        &fault,
        &[
            "writer",
            "namespace",
            "delete",
            "--export",
            export.to_str().unwrap(),
        ],
        Arc::new(AtomicBool::new(false)),
    );
    deletion_boundary.entered();
    let source = fyrr_writer::sync_lease::CursorLease::acquire(
        &config,
        "incremental",
        fyrr_core::connector::SyncKind::Content,
    )
    .await;
    assert!(matches!(source, Err(fyrr_writer::WriterError::SyncBusy)));
    deletion_boundary.release();
    let output = process.finish_bounded();
    assert!(output.status.success(), "{}", report(&output));
    assert!(
        fyrr_reader::storage::optional_head(&config)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store.head(&cut_path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    drop(deletion_boundary);
    drop(proxy);
}

#[tokio::test]
#[ignore = "real CLI stale deletion fence and source recovery"]
async fn stale_delete_fence_advances_head_before_source_write_on_local_storage() {
    let setup = setup_on(Backend::Local);
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (_, commit, root) = incremental::fixture_host_at(8, true, "dense-fixture", setup);
    let output = coordination::host_command(&commit)
        .args([
            "writer",
            "namespace",
            "delete",
            "--export",
            root.join("missing-export").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let config = fyrr_core::config::load(&commit).unwrap().config;
    let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
    store
        .head(&namespace.clone().join(".namespace-delete.json"))
        .await
        .unwrap();
    let before = store
        .get(&namespace.clone().join("head.json"))
        .await
        .unwrap();
    let old_version = UpdateVersion {
        e_tag: before.meta.e_tag.clone(),
        version: before.meta.version.clone(),
    };
    let before: ServingHead = fyrr_core::parse_contract(&before.bytes().await.unwrap()).unwrap();
    let source = fyrr_writer::sync_lease::CursorLease::acquire(
        &config,
        "incremental",
        fyrr_core::connector::SyncKind::Content,
    )
    .await
    .unwrap();
    let after = fyrr_reader::storage::optional_head(&config)
        .await
        .unwrap()
        .unwrap();
    assert!(after.lease.unwrap().epoch > before.lease.unwrap().epoch);
    assert!(matches!(
        fyrr_reader::storage::delete_head_if_match(&config, &old_version).await,
        Err(object_store::Error::Precondition { .. })
    ));
    assert!(
        fyrr_reader::storage::optional_head(&config)
            .await
            .unwrap()
            .is_some()
    );
    source.release().await.unwrap();
}

#[tokio::test]
#[ignore = "real storage namespace deletion refuses an active source"]
async fn delete_refuses_active_source_worker_on_local_storage() {
    let setup = setup_on(Backend::Local);
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (_, commit, root) = incremental::fixture_host_at(8, true, "dense-fixture", setup);
    let config = fyrr_core::config::load(&commit).unwrap().config;
    let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
    let export = root.join("export");
    export_namespace(store.as_ref(), &namespace, &export).await;
    let source = fyrr_writer::sync_lease::CursorLease::acquire(
        &config,
        "incremental",
        fyrr_core::connector::SyncKind::Content,
    )
    .await
    .unwrap();
    assert!(matches!(
        fyrr_writer::commit::delete(&config, Some(&export)).await,
        Err(fyrr_writer::WriterError::SyncBusy)
    ));
    assert!(
        fyrr_reader::storage::optional_head(&config)
            .await
            .unwrap()
            .is_some()
    );
    source.release().await.unwrap();
}

#[tokio::test]
#[ignore = "real CLI detects a late worker PUT after export listing"]
async fn delete_refuses_envelope_arriving_after_export_listing() {
    use futures::TryStreamExt;
    let setup = setup_on(Backend::Local);
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (worker, commit, root) = incremental::fixture_host_at(8, true, "dense-fixture", setup);
    let config = fyrr_core::config::load(&commit).unwrap().config;
    let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
    let export = root.join("export");
    export_namespace(store.as_ref(), &namespace, &export).await;
    let (gate, hook) = corrections::RequestGate::new(|request, _, after| {
        after && request.starts_with("GET ") && request.contains("/dataset.lance/data/")
    });
    let (endpoint, proxy) = corrections::proxy_at_boundary(
        "local-storage".into(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let fault = corrections::proxied(&commit, &endpoint);
    let process = CommitProcess::spawn_command(
        &fault,
        &[
            "writer",
            "namespace",
            "delete",
            "--export",
            export.to_str().unwrap(),
        ],
        Arc::new(AtomicBool::new(false)),
    );
    gate.entered();
    let mut envelope = incremental::envelopes(&worker)
        .await
        .into_iter()
        .next()
        .unwrap()
        .1;
    envelope.sequence += 100;
    envelope.envelope_id = fyrr_core::envelope::envelope_id(
        &envelope.namespace,
        &envelope.source_id,
        envelope.kind,
        envelope.sequence,
        envelope.rejection_generation,
        envelope.cursor.start.as_deref(),
    );
    store
        .put(
            &namespace
                .clone()
                .join("envelopes")
                .join(format!("{}.json", envelope.envelope_id)),
            write_contract(&envelope).into(),
        )
        .await
        .unwrap();
    gate.release();
    assert!(!process.finish_bounded().status.success());
    assert!(
        fyrr_reader::storage::optional_head(&config)
            .await
            .unwrap()
            .is_some()
    );
    let remaining: Vec<_> = store
        .list(Some(&namespace.join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    assert!(remaining.iter().any(|entry| entry.location.filename()
        == Some(format!("{}.json", envelope.envelope_id).as_str())));
    drop(gate);
    drop(proxy);
}
