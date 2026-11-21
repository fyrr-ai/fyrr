//! Operation events from the actual local store, adapted to existing boundary assertions.
use super::*;
use base64::Engine;
use fyrr_reader::storage::test_store::boundary::{Event, Reply};
use std::io::Read;
use std::os::unix::net::{UnixListener, UnixStream};

pub(super) fn controller(state: Arc<BoundaryState>, stop: Arc<AtomicBool>) -> (String, ProxyGuard) {
    use std::os::unix::fs::DirBuilderExt;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "fyrr-observe-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    ));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .unwrap();
    let path = directory.join("socket");
    let listener = UnixListener::bind(&path).unwrap();
    let endpoint = format!("local-store:{}", path.display());
    let stopped = stop.clone();
    let thread = std::thread::spawn(move || {
        let mut workers = Vec::new();
        loop {
            let (stream, _) = listener.accept().unwrap();
            if stopped.load(Ordering::SeqCst) {
                break;
            }
            let state = state.clone();
            let stopped = stopped.clone();
            workers.push(std::thread::spawn(move || serve(stream, &state, &stopped)));
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    (
        endpoint,
        ProxyGuard {
            wake: Some(Wake::Local(path)),
            stop,
            resume: Arc::new(AtomicBool::new(false)),
            thread: Some(thread),
        },
    )
}

fn read_event(stream: &mut UnixStream, stopped: &AtomicBool) -> std::io::Result<Option<Event>> {
    let mut length = [0; 4];
    if stream.read_exact(&mut length).is_err() {
        return Ok(None);
    }
    let length = u32::from_be_bytes(length) as usize;
    assert!(
        length <= 64 * 1024 * 1024,
        "oversized operation observation"
    );
    let mut bytes = vec![0; length];
    if let Err(error) = stream.read_exact(&mut bytes) {
        // Killing the observed process can interrupt a frame between its
        // length and payload. Only explicit teardown permits that disconnect.
        if error.kind() == std::io::ErrorKind::UnexpectedEof && stopped.load(Ordering::SeqCst) {
            return Ok(None);
        }
        return Err(error);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(std::io::Error::other)
}

fn serve(mut stream: UnixStream, state: &BoundaryState, stopped: &AtomicBool) {
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    loop {
        let Some(event) = read_event(&mut stream, stopped).unwrap() else {
            return;
        };
        // These are operation labels, not evidence of an S3 HTTP exchange. A local
        // enumeration has one completion event and never invents continuation pages.
        let request = if event.method == "LIST" {
            format!("GET /local?list-type=2&prefix={}/ HTTP/1.1\r\n", event.path)
        } else {
            format!("{} /local/{} HTTP/1.1\r\n", event.method, event.path)
        };
        let body = base64::engine::general_purpose::STANDARD
            .decode(&event.body)
            .unwrap();
        let mut reply = Reply::default();
        if !event.after {
            reply.fail = state.before(&request, &body);
            reply.capture_objects = event.method == "LIST" && state.transform.is_some();
        } else {
            let observed = match &event.objects {
                Some(objects) => serde_json::to_vec(objects).unwrap(),
                None => body.clone(),
            };
            let status = match event.status {
                200 => "200 OK",
                404 => "404 Not Found",
                412 => "412 Precondition Failed",
                403 => "403 Forbidden",
                _ => "500 Internal Server Error",
            };
            let mut response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                observed.len(),
            )
            .into_bytes();
            response.extend_from_slice(&observed);
            reply.fail = state.after(&request, &body, &mut response);
            let start = response.windows(4).position(|b| b == b"\r\n\r\n").unwrap() + 4;
            let changed = &response[start..];
            if changed != observed {
                if event.objects.is_some() {
                    reply.objects = Some(serde_json::from_slice(changed).unwrap());
                } else {
                    reply.body = Some(base64::engine::general_purpose::STANDARD.encode(changed));
                }
            }
        }
        let bytes = serde_json::to_vec(&reply).unwrap();
        // A cancelled/killed CLI can disconnect while a gate is held.
        if stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .is_err()
            || stream.write_all(&bytes).is_err()
            || event.after
        {
            return;
        }
    }
}

#[tokio::test]
#[ignore = "real CLI operation boundaries on local storage; scripts/ci"]
async fn local_operation_boundaries_preserve_durability_and_release_killed_writers() {
    use object_store::{PutMode, PutOptions};
    let (config, root) = setup_on(Backend::Local);
    let original = std::fs::read_to_string(&config).unwrap();
    let (gate, hook) = RequestGate::rejecting(|request, _, after| {
        after && request.starts_with("PUT ") && request.contains("/head.json ")
    });
    let (proxy, controller) = proxy_at_boundary(
        "local-storage".into(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let observed = root.join("observed.toml");
    std::fs::write(&observed, original.replace("local-storage", &proxy)).unwrap();
    let gate = gate;
    let child = super::super::coordination::CommitProcess::spawn_command(
        &observed,
        &["writer", "namespace", "create"],
        controller.resume.clone(),
    );
    let written = gate.entered();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = namespace.join("head.json");
    // This process is uninstrumented and the child is still at the after-write
    // gate. Reading and retrying prove that the actual durable lock was released.
    let saved = store.get(&path).await.unwrap();
    let version = saved.meta.e_tag.clone();
    assert_eq!(saved.bytes().await.unwrap().as_ref(), written);
    assert!(matches!(
        store
            .put_opts(
                &path,
                written.clone().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                }
            )
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    gate.release();
    assert!(
        !child.finish_bounded().status.success(),
        "lost response must remain a reported failure"
    );
    assert_eq!(store.head(&path).await.unwrap().e_tag, version);
    drop(gate);
    drop(controller);

    // A commit with nothing to commit never takes the lease: give it a document.
    super::super::mirror_identity::sync_texts(&config, &root, &[("kept.txt", "kept text")]);
    let (gate, hook) = RequestGate::new(|request, _, after| {
        !after && request.starts_with("PUT ") && request.contains("/head.json ")
    });
    let (proxy, controller) = proxy_at_boundary(
        "local-storage".into(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    std::fs::write(&observed, original.replace("local-storage", &proxy)).unwrap();
    let gate = gate;
    let mut child = super::super::coordination::CommitProcess::spawn_command(
        &observed,
        &["writer", "commit"],
        controller.resume.clone(),
    );
    gate.entered();
    child.child().kill().unwrap();
    assert!(!child.finish_bounded().status.success());
    gate.release();
    drop(gate);
    drop(controller);
    let saved = store.get(&path).await.unwrap();
    assert_eq!(saved.meta.e_tag, version);
    assert_eq!(saved.bytes().await.unwrap().as_ref(), written);
    success(&config, &["writer", "commit"]);
}

#[tokio::test]
#[ignore = "native local Lance reads through the real mirror; scripts/ci"]
async fn local_native_observer_preserves_warm_mirror_reads() {
    let (worker, commit, root) = super::super::incremental::fixture_on(Backend::Local);
    super::super::incremental::initial(&root);
    super::super::coordination::worker_sync(
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
    super::super::incremental::publish(&commit);
    let native = Arc::new(Mutex::new(Vec::new()));
    let count = native.clone();
    let hook: BoundaryHook = Arc::new(move |request, _, after| {
        if !after && request.starts_with("GET ") && request.contains("/dataset.lance/") {
            count.lock().unwrap().push(request.to_owned());
        }
        false
    });
    let (proxy, controller) = proxy_at_boundary(
        "local-storage".into(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let config = root.join("observed-reader.toml");
    std::fs::write(
        &config,
        std::fs::read_to_string(&worker)
            .unwrap()
            .replace("local-storage", &proxy),
    )
    .unwrap();
    let mut reader = super::super::tools::Mcp::open(&config);
    let first = reader.call("search", json!({"query":"fixture query","limit":10}));
    let first = super::super::tools::body(first);
    assert!(!first["hits"].as_array().unwrap().is_empty());
    let initial = native.lock().unwrap().clone();
    assert!(
        !initial.is_empty(),
        "the observer must see actual native backing reads"
    );
    let second = super::super::tools::body(
        reader.call("search", json!({"query":"fixture query","limit":10})),
    );
    assert_eq!(second["hits"], first["hits"]);
    // Scope the assertion to this request, as the existing mirror proofs do.
    // The independent version checker can still open native index metadata.
    assert_eq!(
        second["metadata"]["telemetry"]["s3_requests"],
        2,
        "warm query must perform only its two authority reads; native observations: {:?}",
        &native.lock().unwrap()[initial.len()..],
    );
    // Background version checks may still be emitting native observations.
    // Mark teardown before Mcp::drop kills their process mid-frame.
    controller.stop.store(true, Ordering::SeqCst);
    drop(reader);
    drop(controller);
}

#[tokio::test]
#[ignore = "genuine bulk fixture files and subsequent LocalStore CAS; scripts/ci"]
async fn bulk_fixture_versions_remain_private_and_support_later_cas() {
    use object_store::{PutMode, PutOptions, UpdateVersion};
    use std::os::unix::fs::PermissionsExt;
    let (config, _root) = setup_on(Backend::Local);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let a = namespace.clone().join("fixture").join("a.json");
    let b = namespace.join("fixture").join("b.json");
    fyrr_reader::storage::test_store::seed_local_fixture(
        &loaded.config,
        vec![
            (a.clone(), b"first".to_vec()),
            (b.clone(), b"second".to_vec()),
        ],
    )
    .unwrap();
    let first = store.get(&a).await.unwrap();
    let original = first.meta.e_tag.clone();
    assert_eq!(first.bytes().await.unwrap().as_ref(), b"first");
    let second = store.get(&b).await.unwrap();
    let other = second.meta.e_tag.clone();
    assert_ne!(original, other);
    assert_eq!(second.bytes().await.unwrap().as_ref(), b"second");
    let Some(fyrr_core::config::StorageBackend::Local { root }) = loaded.config.storage.backend()
    else {
        unreachable!();
    };
    let directory = root.join("namespaces/dense-fixture/fixture");
    assert_eq!(
        std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for name in ["a.json", "b.json"] {
        assert_eq!(
            std::fs::metadata(directory.join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let changed = store
        .put_opts(
            &a,
            b"changed".to_vec().into(),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: original.clone(),
                    version: None,
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_ne!(changed.e_tag, original);
    assert_ne!(changed.e_tag, other);
    assert!(matches!(
        store
            .put_opts(
                &a,
                b"stale".to_vec().into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: original,
                        version: None
                    }),
                    ..Default::default()
                }
            )
            .await,
        Err(object_store::Error::Precondition { .. })
    ));
    store.delete(&b).await.unwrap();
    let recreated = store
        .put_opts(
            &b,
            b"second".to_vec().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_ne!(
        recreated.e_tag, other,
        "normal puts must not reuse a reserved version"
    );
    assert_eq!(
        store.get(&a).await.unwrap().bytes().await.unwrap().as_ref(),
        b"changed"
    );
}

#[test]
fn local_observer_only_allows_truncated_payloads_during_teardown() {
    for stopping in [false, true] {
        let (mut sender, mut receiver) = UnixStream::pair().unwrap();
        sender.write_all(&16_u32.to_be_bytes()).unwrap();
        sender.write_all(b"partial").unwrap();
        drop(sender);
        let result = read_event(&mut receiver, &AtomicBool::new(stopping));
        if stopping {
            assert!(result.unwrap().is_none());
        } else {
            assert_eq!(
                result.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        }
    }
}
