//! Independent metadata writes still publish complete, immutable page evidence.
use super::*;
use futures::TryStreamExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

fn fixture() -> (PathBuf, FixtureRoot) {
    let (config, root) = setup_on(Backend::Local);
    let prefix = std::fs::read_to_string(&config)
        .unwrap()
        .split("[[sources]]")
        .next()
        .unwrap()
        .to_owned();
    std::fs::write(&config, format!("{prefix}\n[[sources]]\nid='metadata'\ntype='sentinel-docs'\npermissions='fixed'\nreaders=['grp:allow']\n[sources.scope]\nfixture='{}'\n", root.join("documents.json").display())).unwrap();
    let documents: Vec<_> = (0..32).map(|n| json!({"id":format!("doc-{n}"),"title":"document","author":"Ada","readers":["grp:allow"],"note":format!("shared-note-{}",n%8),"text":format!("fixture document {}",n%8),"sections":[{"text":format!("fixture document {}",n%8),"readers":["grp:allow"],"location":{"kind":"heading","label":"body"}}]})).collect();
    std::fs::write(
        root.join("documents.json"),
        serde_json::to_vec(&json!({"documents":documents,"cursor":"one"})).unwrap(),
    )
    .unwrap();
    coordination::command(&config, &["writer", "namespace", "create"]);
    (config, root)
}

#[allow(clippy::print_stdout)] // Request counts are benchmark evidence.
#[tokio::test]
#[ignore = "real CLI page metadata I/O on local storage"]
async fn page_metadata_coalesces_equal_keys_and_bounds_independent_puts() {
    let (config, _root) = fixture();
    let puts = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let seen_puts = puts.clone();
    let seen_active = active.clone();
    let seen_peak = peak.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if request.starts_with("PUT ") && request.contains("/document-metadata/") {
            if after {
                seen_active.fetch_sub(1, Ordering::SeqCst);
            } else {
                seen_puts.fetch_add(1, Ordering::SeqCst);
                let pending = seen_active.fetch_add(1, Ordering::SeqCst) + 1;
                seen_peak.fetch_max(pending, Ordering::SeqCst);
                // Fixed observer latency exposes independent requests; all writes
                // still reach the real local store through its operation boundary.
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
        }
        false
    });
    let (endpoint, proxy) = corrections::proxy_at_boundary(
        "local-storage".into(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let proxied = corrections::proxied(&config, &endpoint);
    let output = coordination::worker_command(&proxied)
        .args(["writer", "sync", "metadata", "--kind", "content", "--full"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let count = puts.load(Ordering::SeqCst);
    let peak = peak.load(Ordering::SeqCst);
    println!("metadata-io: puts={count}, peak={peak}");
    assert_eq!(count, 8, "equal page keys must be coalesced");
    assert!(
        (2..=8).contains(&peak),
        "independent writes were not bounded: {peak}"
    );
    assert_eq!(active.load(Ordering::SeqCst), 0);
    drop(proxy);
    coordination::command(&config, &["writer", "commit"]);
    let rows = incremental::source_rows(&config, "metadata").await;
    assert_eq!(rows.num_rows(), 32);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    for n in 0..8 {
        let bytes = serde_json::to_vec(&json!({"note":format!("shared-note-{n}")})).unwrap();
        let digest = fyrr_reader::embedding::digest(&bytes);
        let path = namespace
            .clone()
            .join("document-metadata")
            .join(format!("{}.json", &digest[7..]));
        assert_eq!(
            store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            bytes
        );
    }
}

#[tokio::test]
#[ignore = "real metadata failure must drain admitted writes before lease release"]
async fn metadata_failure_keeps_cursor_lease_until_admitted_write_finishes() {
    let (config, _root) = fixture();
    let key = |n| {
        let bytes = serde_json::to_vec(&json!({"note":format!("shared-note-{n}")})).unwrap();
        fyrr_reader::embedding::digest(&bytes)[7..].to_owned()
    };
    let blocked_key = key(1);
    let failed_key = key(0);
    let blocked = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let (gate, block) = corrections::RequestGate::new(move |request, _, after| {
        !after
            && request.starts_with("PUT ")
            && request.contains("/document-metadata/")
            && request.contains(&blocked_key)
    });
    let hook_blocked = blocked.clone();
    let hook_failed = failed.clone();
    let hook_released = released.clone();
    let waiting_key = key(1);
    let hook: corrections::BoundaryHook = Arc::new(move |request, body, after| {
        if !after && request.starts_with("PUT ") && request.contains("/document-metadata/") {
            if request.contains(&waiting_key) {
                hook_blocked.store(true, Ordering::SeqCst);
            }
            if request.contains(&failed_key) {
                let start = std::time::Instant::now();
                while !hook_blocked.load(Ordering::SeqCst)
                    && start.elapsed() < std::time::Duration::from_secs(5)
                {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                hook_failed.store(true, Ordering::SeqCst);
                return true;
            }
        }
        if after
            && request.starts_with("PUT ")
            && request.contains("/cursors/metadata-content.json")
            && serde_json::from_slice::<fyrr_core::connector::CursorState>(body).is_ok_and(
                |state| {
                    state
                        .lease
                        .is_some_and(|lease| lease.expires_at <= fyrr_core::clock::now())
                },
            )
        {
            hook_released.store(true, Ordering::SeqCst);
        }
        block(request, body, after)
    });
    let (endpoint, proxy) = corrections::proxy_at_boundary(
        "local-storage".into(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let proxied = corrections::proxied(&config, &endpoint);
    let mut command = coordination::worker_command(&proxied);
    command.args(["writer", "sync", "metadata", "--kind", "content", "--full"]);
    let process =
        coordination::CommitProcess::from_command(command, Arc::new(AtomicBool::new(false)));
    gate.entered();
    let start = std::time::Instant::now();
    while !failed.load(Ordering::SeqCst) && start.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(failed.load(Ordering::SeqCst));
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        !released.load(Ordering::SeqCst),
        "lease released with an admitted write still blocked"
    );
    let loaded = fyrr_core::config::load(&config).unwrap();
    let competing = fyrr_writer::sync_lease::CursorLease::acquire(
        &loaded.config,
        "metadata",
        fyrr_core::connector::SyncKind::Content,
    )
    .await;
    assert!(matches!(competing, Err(fyrr_writer::WriterError::SyncBusy)));
    gate.release();
    let output = process.finish_bounded();
    assert!(!output.status.success());
    assert!(released.load(Ordering::SeqCst));
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let envelopes: Vec<_> = store
        .list(Some(&namespace.join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    assert!(
        envelopes.is_empty(),
        "a failed metadata page published an envelope"
    );
    drop(gate);
    drop(proxy);
    coordination::worker_sync(&config, &["metadata", "--kind", "content", "--full"]);
    coordination::command(&config, &["writer", "commit"]);
    assert_eq!(
        incremental::source_rows(&config, "metadata")
            .await
            .num_rows(),
        32
    );
}
