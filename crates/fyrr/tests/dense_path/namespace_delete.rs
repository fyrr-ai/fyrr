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
    let commit = quick_lease(&commit);
    let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
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
        fyrr_reader::storage::delete_if_match(&config, "head.json", &old_version).await,
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
        fyrr_writer::commit::delete(&config, Some(&export), None).await,
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

/// A copy of `config` whose leases expire after 3 s: a deletion waits for the
/// leases that may still write, by default a minute.
pub(super) fn quick_lease(config: &Path) -> PathBuf {
    let text = std::fs::read_to_string(config).unwrap();
    let text = if text.contains("[writer.commit]\n") {
        text.replace("[writer.commit]\n", "[writer.commit]\nlease_ttl='3s'\n")
    } else {
        format!("{text}\n[writer.commit]\nlease_ttl='3s'\n")
    };
    let copy = config.with_file_name("quick-lease.toml");
    std::fs::write(&copy, text).unwrap();
    copy
}

fn without_sources(setup: (PathBuf, FixtureRoot)) -> (PathBuf, PathBuf, FixtureRoot) {
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    incremental::fixture_host_at(8, true, "dense-fixture", setup)
}

/// A running `writer serve`, stopped on drop, also when an assertion fails.
struct Service(std::process::Child);
impl Drop for Service {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while std::time::Instant::now() < until {
            if self.0.try_wait().unwrap().is_some() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn delete_command(commit: &Path, args: &[&str]) -> Output {
    coordination::host_command(commit)
        .args(["writer", "namespace"])
        .args(args)
        .output()
        .unwrap()
}

#[tokio::test]
#[ignore = "real CLI namespace deletion removes the data on local storage"]
async fn delete_data_removes_every_object_and_the_name_is_created_again_on_local_storage() {
    use futures::TryStreamExt;
    let (_, commit, _root) = without_sources(setup_on(Backend::Local));
    let config = fyrr_core::config::load(&commit).unwrap().config;
    let commit = quick_lease(&commit);
    let name = config.namespace.name.clone();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
    let before = fyrr_reader::storage::optional_head(&config)
        .await
        .unwrap()
        .unwrap()
        .incarnation;
    // Another name than the namespace's confirms nothing.
    let wrong = delete_command(&commit, &["delete", "--delete-data", "not-this-one"]);
    assert!(!wrong.status.success());
    assert!(
        String::from_utf8_lossy(&wrong.stderr).contains("delete_confirmation_invalid"),
        "{}",
        report(&wrong)
    );
    assert!(
        fyrr_reader::storage::optional_head(&config)
            .await
            .unwrap()
            .is_some()
    );
    let deleted = delete_command(&commit, &["delete", "--delete-data", &name]);
    assert!(deleted.status.success(), "{}", report(&deleted));
    assert!(
        fyrr_reader::storage::optional_head(&config)
            .await
            .unwrap()
            .is_none()
    );
    let left: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
    assert!(left.is_empty(), "{left:?}");
    // No source writes a cursor into the deleted namespace.
    let source = fyrr_writer::sync_lease::CursorLease::acquire(
        &config,
        "incremental",
        fyrr_core::connector::SyncKind::Content,
    )
    .await;
    assert!(matches!(
        source,
        Err(fyrr_writer::WriterError::NamespaceUnavailable)
    ));
    let left: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
    assert!(left.is_empty(), "{left:?}");
    // An object a late writer left under the prefix refuses the creation.
    let stray = namespace.clone().join("stray");
    store.put(&stray, "late".into()).await.unwrap();
    let refused = delete_command(&commit, &["create"]);
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("namespace_deleting"),
        "{}",
        report(&refused)
    );
    // With no head and no fence, the deletion still removes what remains.
    let cleaned = delete_command(&commit, &["delete", "--delete-data", &name]);
    assert!(cleaned.status.success(), "{}", report(&cleaned));
    let left: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
    assert!(left.is_empty(), "{left:?}");
    let created = delete_command(&commit, &["create"]);
    assert!(created.status.success(), "{}", report(&created));
    let after = fyrr_reader::storage::optional_head(&config)
        .await
        .unwrap()
        .unwrap()
        .incarnation;
    assert_ne!(before, after);
    // A creation interrupted before its head leaves only identity and policy:
    // repeating it succeeds.
    store
        .delete(&namespace.clone().join("head.json"))
        .await
        .unwrap();
    let repeated = delete_command(&commit, &["create"]);
    assert!(repeated.status.success(), "{}", report(&repeated));
    assert!(
        fyrr_reader::storage::optional_head(&config)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
#[ignore = "real CLI interrupted namespace deletion resumes on local storage"]
async fn interrupted_delete_finishes_when_run_again_on_local_storage() {
    use futures::TryStreamExt;
    let (_, commit, root) = without_sources(setup_on(Backend::Local));
    let config = fyrr_core::config::load(&commit).unwrap().config;
    let commit = quick_lease(&commit);
    let name = config.namespace.name.clone();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
    // A refused export leaves the fence behind; removing the head by hand is a
    // deletion that stopped right after unpublishing the namespace.
    let missing = root.join("missing-export");
    let refused = delete_command(&commit, &["delete", "--export", missing.to_str().unwrap()]);
    assert!(!refused.status.success());
    store
        .delete(&namespace.clone().join("head.json"))
        .await
        .unwrap();
    // A new head would be removed with the objects of the deletion.
    let created = delete_command(&commit, &["create"]);
    assert!(!created.status.success());
    assert!(
        String::from_utf8_lossy(&created.stderr).contains("namespace_deleting"),
        "{}",
        report(&created)
    );
    // Without a confirmation the command only prints its plan.
    let plan = delete_command(&commit, &["delete"]);
    assert!(plan.status.success(), "{}", report(&plan));
    assert!(String::from_utf8_lossy(&plan.stdout).contains("\"dry_run\":true"));
    let left: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
    assert!(left.len() > 1);
    // The resumed deletion asks for the confirmation of a new one: an export
    // that does not hold the remaining objects deletes nothing.
    let refused = delete_command(&commit, &["delete", "--export", missing.to_str().unwrap()]);
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("export_invalid"),
        "{}",
        report(&refused)
    );
    let kept: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
    assert_eq!(kept.len(), left.len());
    // The namespace is listed while any object remains under it.
    let listed = |prefix: &str| {
        let output = delete_command(&commit, &["list", "--prefix", prefix]);
        assert!(output.status.success(), "{}", report(&output));
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    assert_eq!(listed(&name), json!({"namespaces":[name]}));
    assert_eq!(listed(&format!("{name}-")), json!({"namespaces":[]}));
    assert!(
        !delete_command(&commit, &["list", "--prefix", ""])
            .status
            .success()
    );
    for _ in 0..2 {
        let resumed = delete_command(&commit, &["delete", "--delete-data", &name]);
        assert!(resumed.status.success(), "{}", report(&resumed));
        let left: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
        assert!(left.is_empty(), "{left:?}");
    }
    let created = delete_command(&commit, &["create"]);
    assert!(created.status.success(), "{}", report(&created));
}

#[tokio::test]
#[ignore = "real writer HTTP namespace deletion on local storage"]
async fn http_delete_needs_an_admin_key_and_the_repeated_name_on_local_storage() {
    use futures::TryStreamExt;
    use std::time::{Duration, Instant};
    let (_, commit, root) = without_sources(setup_on(Backend::Local));
    let config = fyrr_core::config::load(&commit).unwrap().config;
    let name = config.namespace.name.clone();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
    let admin = format!("fyrr_key_{:064}", 1);
    let reader = format!("fyrr_key_{:064}", 2);
    let address = free_address();
    let text = std::fs::read_to_string(&commit).unwrap();
    std::fs::write(
        &commit,
        format!(
            "{text}\n[[identity.api_keys]]\nname='delete-admin-q7x'\nkey={{env='FYRR_TEST_DELETE_ADMIN'}}\nroles=['admin']\n\
             [[identity.api_keys]]\nname='delete-read-q7x'\nkey={{env='FYRR_TEST_DELETE_READ'}}\nreaders=['usr:u0']\n\
             [writer.commit]\ncoalescing_timer='10m'\nlease_ttl='3s'\n[writer.serve.admin]\nlisten='{address}'\n\
             operator_token={{env='FYRR_TEST_DELETE_OPERATOR'}}\n"
        ),
    )
    .unwrap();
    let keys = |command: &mut Command| {
        command
            .env("FYRR_TEST_DELETE_ADMIN", &admin)
            .env("FYRR_TEST_DELETE_READ", &reader)
            .env(
                "FYRR_TEST_DELETE_OPERATOR",
                "owned-fixture-operator-token-only",
            )
            .output()
            .unwrap()
    };
    let published = keys(coordination::host_command(&commit).args(["writer", "keys"]));
    assert!(published.status.success(), "{}", report(&published));
    let stderr = root.join("service.stderr");
    let mut service = Service(
        coordination::worker_command(&commit)
            .args(["writer", "serve", "--jobs", "1"])
            .env("FYRR_TEST_DELETE_ADMIN", &admin)
            .env("FYRR_TEST_DELETE_READ", &reader)
            .env(
                "FYRR_TEST_DELETE_OPERATOR",
                "owned-fixture-operator-token-only",
            )
            .stdout(std::fs::File::create(root.join("service.stdout")).unwrap())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(40))
        .build()
        .unwrap();
    let url = format!("http://{address}/v1/namespaces/{name}");
    let mut ready = false;
    for _ in 0..100 {
        let probe = client
            .get(format!("http://{address}/admin/status"))
            .bearer_auth("owned-fixture-operator-token-only");
        if ready_request(&mut service.0, &stderr, probe)
            .await
            .is_some_and(|status| status.is_success())
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "writer HTTP startup deadline");
    let body = json!({"namespace": name});
    let status = |request: reqwest::RequestBuilder| async move {
        request.send().await.unwrap().status().as_u16()
    };
    // Every refusal is the standard error shape: `{"error": "<code>"}`.
    let refused = |request: reqwest::RequestBuilder| async move {
        let answer = request.send().await.unwrap();
        let code = answer.status().as_u16();
        (code, answer.json::<serde_json::Value>().await.unwrap())
    };
    assert_eq!(
        refused(client.delete(&url).json(&body)).await,
        (401, json!({"error": "unauthorized"}))
    );
    let unauthorized = client.delete(&url).json(&body).send().await.unwrap();
    assert_eq!(
        unauthorized.headers().get("www-authenticate").unwrap(),
        "Bearer"
    );
    assert_eq!(
        refused(client.delete(&url).bearer_auth(&reader).json(&body)).await,
        (403, json!({"error": "forbidden"}))
    );
    assert_eq!(
        refused(client.delete(&url).bearer_auth(&admin)).await,
        (400, json!({"error": "body_invalid"})),
        "no name in the body"
    );
    assert_eq!(
        refused(
            client
                .delete(&url)
                .bearer_auth(&admin)
                .json(&json!({"namespace": "another"}))
        )
        .await,
        (400, json!({"error": "body_invalid"}))
    );
    assert!(
        fyrr_reader::storage::optional_head(&config)
            .await
            .unwrap()
            .is_some()
    );
    // A source worker of the service may hold its cursor for a moment.
    let until = Instant::now() + Duration::from_secs(60);
    loop {
        let code = status(client.delete(&url).bearer_auth(&admin).json(&body)).await;
        if code == 200 {
            break;
        }
        assert!(code == 503 && Instant::now() < until, "answered {code}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let left: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
    assert!(left.is_empty(), "{left:?}");
}
