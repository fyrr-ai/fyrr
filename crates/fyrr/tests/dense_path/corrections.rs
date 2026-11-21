//! Durability, ACL revision and private-mirror regressions on RustFS.
use super::*;
#[path = "corrections/local.rs"]
mod local;
use std::net::{TcpListener, TcpStream};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;

// Count head requests observed by the proxy.
#[derive(Default)]
pub(super) struct HeadReads {
    started: AtomicUsize,
}

impl HeadReads {
    pub(super) fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    fn start(&self) {
        self.started.fetch_add(1, Ordering::SeqCst);
    }
}

// A test-only HTTP fault boundary. Requests still reach the owned real S3
// fixture; selected cursor, head or admitted retrieval reads can be faulted.
pub(super) fn proxy(
    target: String,
    reject_cursor: Arc<AtomicBool>,
    revoke_on_head: Arc<AtomicUsize>,
    revoke_config: PathBuf,
    stop: Arc<AtomicBool>,
    retrieval_fault: Option<(Arc<HeadReads>, Arc<Mutex<usize>>)>,
    heads: Option<Arc<HeadReads>>,
) -> (String, ProxyGuard) {
    proxy_with_hook(
        target,
        reject_cursor,
        revoke_on_head,
        revoke_config,
        stop,
        retrieval_fault,
        heads,
        None,
        None,
    )
}

// A request hook runs before dispatch and after the real RustFS response. It
// may pause the boundary or replace a response with a nonretryable failure.
// After a GET, bytes contain the real HTTP response for read-cost measurements;
// write hooks retain the request body on both sides of their mutation boundary.
pub(super) struct ProxyGuard {
    wake: Option<Wake>,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) resume: Arc<AtomicBool>,
    pub(super) thread: Option<std::thread::JoinHandle<()>>,
}
enum Wake {
    Tcp(std::net::SocketAddr),
    Local(PathBuf),
}
impl Drop for ProxyGuard {
    fn drop(&mut self) {
        self.resume.store(true, Ordering::SeqCst);
        self.stop.store(true, Ordering::SeqCst);
        match &self.wake {
            Some(Wake::Tcp(address)) => {
                let _ = TcpStream::connect_timeout(address, Duration::from_secs(5));
            }
            Some(Wake::Local(path)) => {
                let _ = std::os::unix::net::UnixStream::connect(path);
            }
            None => {}
        }
        let panic = self.thread.take().and_then(|thread| thread.join().err());
        if let Some(Wake::Local(path)) = &self.wake {
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_dir(path.parent().unwrap());
        }
        if let Some(panic) = panic
            && !std::thread::panicking()
        {
            std::panic::resume_unwind(panic);
        }
    }
}

pub(super) type BoundaryHook = Arc<dyn Fn(&str, &[u8], bool) -> bool + Send + Sync>;

/// One acknowledged request, with a watchdog on both sides of the boundary.
/// Drop this guard before the proxy so a failed assertion releases its worker.
pub(super) struct RequestGate {
    entered: std::sync::mpsc::Receiver<Vec<u8>>,
    release: std::sync::mpsc::SyncSender<()>,
}

impl RequestGate {
    pub(super) fn new(
        matches: impl Fn(&str, &[u8], bool) -> bool + Send + Sync + 'static,
    ) -> (Self, BoundaryHook) {
        Self::with_response(matches, false)
    }

    pub(super) fn rejecting(
        matches: impl Fn(&str, &[u8], bool) -> bool + Send + Sync + 'static,
    ) -> (Self, BoundaryHook) {
        Self::with_response(matches, true)
    }

    fn with_response(
        matches: impl Fn(&str, &[u8], bool) -> bool + Send + Sync + 'static,
        reject: bool,
    ) -> (Self, BoundaryHook) {
        let (entered_tx, entered) = std::sync::mpsc::sync_channel(1);
        let (release, release_rx) = std::sync::mpsc::sync_channel(1);
        let release_rx = Mutex::new(release_rx);
        let fired = AtomicBool::new(false);
        let hook = Arc::new(move |request: &str, body: &[u8], after: bool| {
            if matches(request, body, after) && !fired.swap(true, Ordering::SeqCst) {
                entered_tx.send(body.to_vec()).expect("gate observer");
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(120))
                    .expect("request gate release watchdog");
                return reject;
            }
            false
        });
        (Self { entered, release }, hook)
    }

    pub(super) fn entered(&self) -> Vec<u8> {
        self.entered
            .recv_timeout(Duration::from_secs(120))
            .expect("request gate entry watchdog")
    }

    pub(super) fn release(&self) {
        let _ = self.release.try_send(());
    }
}

impl Drop for RequestGate {
    fn drop(&mut self) {
        self.release();
    }
}

#[test]
#[ignore = "proxy guard invariant included in scripts/ci correction durability selection"]
fn correction_durability_proxy_guard_preserves_panics() {
    let guard = || ProxyGuard {
        wake: None,
        stop: Arc::new(AtomicBool::new(false)),
        resume: Arc::new(AtomicBool::new(false)),
        thread: Some(std::thread::spawn(|| panic!("proxy panic"))),
    };
    let proxy_panic = std::panic::catch_unwind(|| drop(guard())).unwrap_err();
    assert_eq!(proxy_panic.downcast_ref::<&str>(), Some(&"proxy panic"));
    let caller_panic = std::panic::catch_unwind(|| {
        let _guard = guard();
        panic!("caller panic");
    })
    .unwrap_err();
    assert_eq!(caller_panic.downcast_ref::<&str>(), Some(&"caller panic"));
}

pub(super) fn proxy_at_boundary(
    target: String,
    stop: Arc<AtomicBool>,
    hook: BoundaryHook,
) -> (String, ProxyGuard) {
    proxy_with_hook(
        target,
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
        PathBuf::new(),
        stop.clone(),
        None,
        None,
        Some(hook),
        None,
    )
}

/// Time-compressed storage observations; no RustFS object is changed in place.
pub(super) type ResponseTransform = Arc<dyn Fn(&str, &mut Vec<u8>) + Send + Sync>;
pub(super) fn proxy_transform(
    target: String,
    stop: Arc<AtomicBool>,
    transform: ResponseTransform,
    hook: Option<BoundaryHook>,
) -> (String, ProxyGuard) {
    proxy_with_hook(
        target,
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
        PathBuf::new(),
        stop.clone(),
        None,
        None,
        hook,
        Some(transform),
    )
}

struct BoundaryState {
    reject_cursor: Arc<AtomicBool>,
    revoke_on_head: Arc<AtomicUsize>,
    revoke_config: PathBuf,
    retrieval_fault: Option<(Arc<HeadReads>, Arc<Mutex<usize>>)>,
    heads: Option<Arc<HeadReads>>,
    hook: Option<BoundaryHook>,
    transform: Option<ResponseTransform>,
    cursor_sequence: AtomicU64,
}
impl BoundaryState {
    fn before(&self, request: &str, body: &[u8]) -> bool {
        if self
            .hook
            .as_ref()
            .is_some_and(|hook| hook(request, body, false))
        {
            return true;
        }
        if request.starts_with("PUT ")
            && request.contains("/cursors/allowed-content.json")
            && self.reject_cursor.load(Ordering::Relaxed)
            && serde_json::from_slice::<Value>(body)
                .ok()
                .and_then(|v| v["sequence"].as_u64())
                .is_some_and(|sequence| sequence > self.cursor_sequence.load(Ordering::SeqCst))
        {
            return true;
        }
        if request.starts_with("GET ") && request.contains("/head.json ") {
            if let Some(heads) = &self.heads {
                heads.start();
            }
            if let Some((heads, _)) = &self.retrieval_fault {
                heads.start();
            }
            if self.revoke_on_head.load(Ordering::Relaxed) != 0
                && self.revoke_on_head.fetch_sub(1, Ordering::Relaxed) == 1
            {
                success(&self.revoke_config, &["writer", "commit"]);
            }
        }
        if let Some((heads, failures)) = &self.retrieval_fault
            && (request.starts_with("GET ") || request.starts_with("HEAD "))
            && request.contains("/dataset.lance/")
            && heads.started() >= 2
        {
            let mut failures = failures.lock().unwrap();
            if *failures == 0 {
                success(&self.revoke_config, &["writer", "commit"]);
            }
            *failures += 1;
            return true;
        }
        false
    }
    fn after(&self, request: &str, body: &[u8], response: &mut Vec<u8>) -> bool {
        if request.starts_with("GET ")
            && request.contains("/cursors/allowed-content.json")
            && let Some(position) = response.windows(4).position(|b| b == b"\r\n\r\n")
            && let Ok(value) = serde_json::from_slice::<Value>(&response[position + 4..])
            && let Some(sequence) = value["sequence"].as_u64()
        {
            self.cursor_sequence.store(sequence, Ordering::SeqCst);
        }
        if self.hook.as_ref().is_some_and(|hook| {
            hook(
                request,
                if request.starts_with("GET ") {
                    response
                } else {
                    body
                },
                true,
            )
        }) {
            return true;
        }
        if let Some(transform) = &self.transform {
            transform(request, response);
        }
        false
    }
}

#[allow(clippy::too_many_arguments)]
fn proxy_with_hook(
    target: String,
    reject_cursor: Arc<AtomicBool>,
    revoke_on_head: Arc<AtomicUsize>,
    revoke_config: PathBuf,
    stop: Arc<AtomicBool>,
    retrieval_fault: Option<(Arc<HeadReads>, Arc<Mutex<usize>>)>,
    heads: Option<Arc<HeadReads>>,
    hook: Option<BoundaryHook>,
    transform: Option<ResponseTransform>,
) -> (String, ProxyGuard) {
    let state = Arc::new(BoundaryState {
        reject_cursor,
        revoke_on_head,
        revoke_config,
        retrieval_fault,
        heads,
        hook,
        transform,
        cursor_sequence: AtomicU64::new(0),
    });
    if target == "local-storage" {
        return local::controller(state, stop);
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("http://{address}");
    let worker_stop = stop.clone();
    let thread = std::thread::spawn(move || {
        let mut workers = Vec::new();
        loop {
            let (socket, _) = listener.accept().expect("proxy accept");
            if worker_stop.load(Ordering::SeqCst) {
                break;
            }
            let target = target.clone();
            let state = state.clone();
            workers.push(std::thread::spawn(move || {
                socket.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                let mut client = BufReader::new(socket);
                let mut headers = Vec::new();
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    match client.read_line(&mut line) {
                        Ok(0) => return,
                        Ok(_) => {},
                        // HTTP clients can preconnect without issuing a request.
                        // Expire that idle connection with the existing watchdog;
                        // partial request headers remain an actual proxy failure.
                        Err(error) if headers.is_empty() && line.is_empty() && matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => return,
                        Err(error) => panic!("proxy request headers: {error}"),
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") { length = value.trim().parse().unwrap(); }
                    let end = line == "\r\n";
                    headers.push(line);
                    if end { break; }
                }
                let request = &headers[0];
                let mut body = vec![0; length];
                std::io::Read::read_exact(&mut client, &mut body).unwrap();
                // GET hooks inspect request headers before dispatch and the real
                // response afterwards; write hooks retain their request body.
                let observation = if request.starts_with("GET ") {
                    headers.concat().into_bytes()
                } else {
                    body.clone()
                };
                if state.before(request, &observation) {
                    let _ = client.get_mut().write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    return;
                }
                let mut upstream = TcpStream::connect(target.trim_start_matches("http://")).unwrap();
                upstream.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                for header in &headers { if !header.to_ascii_lowercase().starts_with("connection:") && header != "\r\n" { upstream.write_all(header.as_bytes()).unwrap(); } }
                upstream.write_all(b"Connection: close\r\n\r\n").unwrap();
                upstream.write_all(&body).unwrap();
                if state.transform.is_some() || state.hook.is_some() || (request.starts_with("GET ") && request.contains("/cursors/allowed-content.json")) {
                    let mut response = Vec::new();
                    std::io::Read::read_to_end(&mut upstream, &mut response).unwrap();
                    if state.after(request, &body, &mut response) {
                        let _ = client.get_mut().write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    } else { let _ = client.get_mut().write_all(&response); }
                } else { let _ = std::io::copy(&mut upstream, client.get_mut()); }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    (
        endpoint,
        ProxyGuard {
            wake: Some(Wake::Tcp(address)),
            stop,
            resume: Arc::new(AtomicBool::new(false)),
            thread: Some(thread),
        },
    )
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn correction_durability_acl_and_private_mirror_on_storage() {
    let (config, root) = setup();
    let original = std::fs::read_to_string(&config).unwrap();
    let text = original
        .split("[[sources]]")
        .take(2)
        .collect::<Vec<_>>()
        .join("[[sources]]");
    std::fs::write(&config, &text).unwrap();
    let sources = root.join("sources");
    // This directory was created solely by setup in this test.
    std::fs::remove_dir_all(&sources).unwrap();
    std::fs::create_dir(&sources).unwrap();
    let file = sources.join("0.txt");
    std::fs::write(&file, "fixture document 0").unwrap();
    success(&config, &["writer", "namespace", "create"]);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let revoked = root.join("revoked.toml");
    std::fs::write(
        &revoked,
        text.replace("stdio_readers = [\"grp:allow\"]", "stdio_readers = []"),
    )
    .unwrap();
    let reject = Arc::new(AtomicBool::new(true));
    let revoke = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (proxy_endpoint, thread) = proxy(
        endpoint.clone(),
        reject.clone(),
        revoke.clone(),
        revoked.clone(),
        stop.clone(),
        None,
        None,
    );
    let proxied = root.join("proxy.toml");
    std::fs::write(&proxied, text.replace(&endpoint, &proxy_endpoint)).unwrap();
    let failed = run(&proxied, &["writer", "sync"]);
    assert!(
        !failed.status.success(),
        "cursor PUT must fail after envelope durability"
    );
    assert!(String::from_utf8_lossy(&failed.stdout).contains("sync_lease_lost"));
    assert!(!String::from_utf8_lossy(&failed.stdout).contains("batch_rejected"));
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, path) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let pending = fyrr_writer::commit::pending(
        &loaded.config,
        &fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap(),
    )
    .await
    .unwrap()
    .into_iter()
    .filter(|e| e.kind == fyrr_core::connector::SyncKind::Content)
    .collect::<Vec<_>>();
    assert_eq!(pending.len(), 1, "envelope PUT succeeded");
    let cursor: fyrr_core::connector::CursorState = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &path.clone().join("cursors").join("allowed-content.json"),
    )
    .await
    .unwrap();
    assert_eq!(
        cursor.sequence, 0,
        "the lease exists, but no cursor progress was saved"
    );
    assert_eq!(cursor.cursor, None);
    reject.store(false, Ordering::Relaxed);
    let resumed = run(&config, &["writer", "sync"]);
    assert!(resumed.status.success(), "{}", report(&resumed));
    assert!(String::from_utf8_lossy(&resumed.stdout).contains("\"upserts\":0"));
    // A -> B -> A before one publication must contain only the final A.
    std::fs::write(&file, "fixture document 1").unwrap();
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    std::fs::write(&file, "fixture document 0").unwrap();
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    success(&config, &["writer", "commit"]);
    assert_eq!(
        success(
            &config,
            &["reader", "search", "fixture query", "--limit", "75"]
        )["hits"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "A->B->A logical uniqueness"
    );
    // A reset lineage must be recovered by sequence even when S3 still has C.
    let cursor_path = path.clone().join("cursors").join("allowed-content.json");
    let before_reset: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    assert!(before_reset.cursor.is_some());
    std::fs::write(&file, "fixture document 1").unwrap();
    reject.store(true, Ordering::Relaxed);
    assert!(
        !run(&proxied, &["writer", "sync", "--full"])
            .status
            .success()
    );
    let retained: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    assert_eq!(retained.sequence, before_reset.sequence);
    assert_eq!(retained.cursor, before_reset.cursor);
    assert_eq!(
        retained.rejection_generation,
        before_reset.rejection_generation
    );
    assert!(retained.lease.as_ref().unwrap().epoch > before_reset.lease.as_ref().unwrap().epoch);
    reject.store(false, Ordering::Relaxed);
    let restarted = run(&config, &["writer", "sync"]);
    assert!(restarted.status.success(), "{}", report(&restarted));
    assert!(String::from_utf8_lossy(&restarted.stdout).contains("\"upserts\":0"));
    let recovered: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    assert_eq!(recovered.sequence, before_reset.sequence + 1);
    std::fs::write(&file, "fixture document 0").unwrap();
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    let pending = fyrr_writer::commit::pending(
        &loaded.config,
        &fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap(),
    )
    .await
    .unwrap()
    .into_iter()
    .filter(|e| e.kind == fyrr_core::connector::SyncKind::Content)
    .collect::<Vec<_>>();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].sequence, before_reset.sequence + 1);
    assert_eq!(pending[0].cursor.start, None);
    assert_eq!(pending[1].sequence, before_reset.sequence + 2);
    assert_eq!(pending[1].cursor.start, recovered.cursor);
    let fyrr_core::envelope::EnvelopePayload::Content(restored) = &pending[1].payload else {
        panic!("content envelope expected");
    };
    assert_eq!(
        restored.documents[0].records, 1,
        "published A with pending B needs materialized A, not presence"
    );
    let fyrr_core::envelope::EnvelopePayload::Content(replacement) = &pending[0].payload else {
        panic!("content envelope expected");
    };
    assert_eq!(
        restored.documents[0].expected_prior_revision, replacement.documents[0].source_revision,
        "restored A follows the durable pending B revision"
    );
    success(&config, &["writer", "commit"]);
    let latest = success(
        &config,
        &["reader", "search", "fixture query", "--output", "content"],
    );
    assert_eq!(latest["hits"].as_array().unwrap().len(), 1);
    assert_eq!(latest["hits"][0]["content"], "fixture document 0");
    // Cron may commit the durable envelope before restarting the failed sync.
    // Applied envelopes still advance the cursor cache; revision lookup alone
    // must not conceal the gap and reuse the already-applied sequence/id.
    let before_applied: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    std::fs::write(&file, "fixture document 1").unwrap();
    reject.store(true, Ordering::Relaxed);
    assert!(!run(&proxied, &["writer", "sync"]).status.success());
    success(&config, &["writer", "commit"]);
    let retained: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    assert_eq!(retained.sequence, before_applied.sequence);
    assert_eq!(retained.cursor, before_applied.cursor);
    assert_eq!(
        retained.rejection_generation,
        before_applied.rejection_generation
    );
    reject.store(false, Ordering::Relaxed);
    let restarted = run(&config, &["writer", "sync"]);
    assert!(restarted.status.success(), "{}", report(&restarted));
    assert!(String::from_utf8_lossy(&restarted.stdout).contains("\"upserts\":0"));
    let recovered: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    assert_eq!(recovered.sequence, before_applied.sequence + 1);
    std::fs::write(&file, "fixture document 0").unwrap();
    success(&config, &["writer", "sync"]);
    let pending = fyrr_writer::commit::pending(
        &loaded.config,
        &fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sequence, before_applied.sequence + 2);
    assert_eq!(pending[0].cursor.start, recovered.cursor);
    success(&config, &["writer", "commit"]);
    let latest = success(
        &config,
        &["reader", "search", "fixture query", "--output", "content"],
    );
    assert_eq!(latest["hits"].as_array().unwrap().len(), 1);
    assert_eq!(latest["hits"][0]["content"], "fixture document 0");
    // Establish B, then A -> delete -> A before the next single commit.
    std::fs::write(&file, "fixture document 1").unwrap();
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    success(&config, &["writer", "commit"]);
    std::fs::write(&file, "fixture document 0").unwrap();
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    std::fs::remove_file(&file).unwrap();
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    std::fs::write(&file, "fixture document 0").unwrap();
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    success(&config, &["writer", "commit"]);
    assert_eq!(
        success(
            &config,
            &["reader", "search", "fixture query", "--limit", "75"]
        )["hits"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "A->delete->A logical uniqueness"
    );

    // Revoke between admission/embedding and the error recheck. A cache miss
    // must become authorization_unavailable in this still-running MCP process.
    let mut child = Command::new(crate::common::binary())
        .arg("--config")
        .arg(&proxied)
        .args(["reader", "serve"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    writeln!(
        input,
        "{}",
        json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"fyrr-tests","version":"1"}}})
    )
    .unwrap();
    output.read_line(&mut line).unwrap();
    writeln!(
        input,
        "{}",
        json!({"jsonrpc":"2.0","method":"notifications/initialized"})
    )
    .unwrap();
    revoke.store(2, Ordering::Relaxed);
    writeln!(input,"{}",json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{"query":"cache miss"}}})).unwrap();
    line.clear();
    output.read_line(&mut line).unwrap();
    assert!(line.contains("authorization_unavailable"), "{line}");
    assert!(!line.contains("embedding_unavailable"));
    drop(input);
    assert!(child.wait().unwrap().success());
    stop.store(true, Ordering::Relaxed);
    drop(thread);

    // Only the configured reader token changed; text did not. Every file is
    // republished, and the new principal can read it while the old is refused.
    let changed = root.join("changed-token.toml");
    let changed_text = text
        .replace("fixture-subject", "new-subject")
        .replace("grp:allow", "grp:new");
    std::fs::write(&changed, &changed_text).unwrap();
    let update = run(&changed, &["writer", "sync", "--full"]);
    assert!(update.status.success(), "{}", report(&update));
    assert!(String::from_utf8_lossy(&update.stdout).contains("\"upserts\":1"));
    assert!(
        String::from_utf8_lossy(&update.stdout).contains("\"cache_misses\":0,\"paid_calls\":0")
    );
    success(&changed, &["writer", "commit"]);
    assert_eq!(
        success(
            &changed,
            &["reader", "search", "fixture query", "--output", "content"]
        )["hits"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let denied = run(
        &config,
        &["reader", "search", "fixture query", "--output", "content"],
    );
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stderr).contains("authorization_unavailable"));
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let dataset = fyrr_reader::storage::open_version(
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
    assert_eq!(dataset.count_rows(None).await.unwrap(), 1);
    let new_token = fyrr_reader::storage::hash_principal(&loaded.config, "tok", "grp:new").unwrap();
    assert_eq!(
        dataset
            .count_rows(Some(format!("array_has(readers, '{}')", new_token)))
            .await
            .unwrap(),
        1
    );

    use std::os::unix::fs::{PermissionsExt, symlink};
    // Simulate interrupted mirror renames. A fresh real reader must remove
    // every owned partial and fetch the immutable bytes again from RustFS.
    let mut abandoned = Vec::new();
    for directory in std::fs::read_dir(root.join("mirror")).unwrap() {
        for entry in std::fs::read_dir(directory.unwrap().path()).unwrap() {
            let blob = entry.unwrap().path();
            if blob.extension().is_some_and(|e| e == "blob") {
                let partial = blob.with_extension("partial");
                std::fs::rename(&blob, &partial).unwrap();
                std::fs::write(&partial, b"interrupted write").unwrap();
                abandoned.push(partial);
            }
        }
    }
    assert!(!abandoned.is_empty());
    let hydrated = success(
        &changed,
        &["reader", "search", "fixture query", "--output", "content"],
    );
    assert_eq!(hydrated["hits"][0]["content"], "fixture document 0");
    // Only this reader's endpoint/namespace directory is opened at restart.
    let mirror_directory = root.join("mirror").join(fyrr_reader::embedding::digest(
        format!(
            "{}:{}",
            loaded.config.storage.endpoint.as_deref().unwrap_or(""),
            fyrr_reader::storage::dataset_uri(&loaded.config)
        )
        .as_bytes(),
    ));
    for partial in abandoned
        .iter()
        .filter(|p| p.parent() == Some(mirror_directory.as_path()))
    {
        assert!(!partial.exists(), "startup removes abandoned partials");
    }
    let mut files = 0;
    for entry in std::fs::read_dir(&mirror_directory).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path).unwrap();
        assert!(metadata.is_file() && !metadata.file_type().is_symlink());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        let extension = path.extension().and_then(|s| s.to_str());
        assert!(
            entry.file_name() == "mirror.lock"
                || matches!(extension, Some("blob" | "checksum" | "lease"))
        );
        files += usize::from(extension == Some("blob"));
    }
    assert!(files > 0, "read-through restored private Lance files");
    let unsafe_mirror = root.join("unsafe-mirror");
    std::fs::create_dir(&unsafe_mirror).unwrap();
    std::fs::set_permissions(&unsafe_mirror, std::fs::Permissions::from_mode(0o755)).unwrap();
    let unsafe_config = root.join("unsafe.toml");
    std::fs::write(
        &unsafe_config,
        changed_text.replace(
            &root.join("mirror").display().to_string(),
            &unsafe_mirror.display().to_string(),
        ),
    )
    .unwrap();
    assert!(
        !run(&unsafe_config, &["reader", "check"]).status.success(),
        "public mirror refused"
    );
    let linked = root.join("linked-mirror");
    symlink(&unsafe_mirror, &linked).unwrap();
    std::fs::write(
        &unsafe_config,
        changed_text.replace(
            &root.join("mirror").display().to_string(),
            &linked.display().to_string(),
        ),
    )
    .unwrap();
    assert!(
        !run(&unsafe_config, &["reader", "check"]).status.success(),
        "symlink mirror refused"
    );
    // A stale cache can lag several applied envelopes, including reset lineage.
    let latest_cursor: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    store
        .put(
            &cursor_path,
            fyrr_core::write_contract(&before_applied).into(),
        )
        .await
        .unwrap();
    let resumed = run(&changed, &["writer", "sync"]);
    assert!(resumed.status.success(), "{}", report(&resumed));
    assert!(String::from_utf8_lossy(&resumed.stdout).contains("\"upserts\":0"));
    let repaired: fyrr_core::connector::CursorState =
        fyrr_reader::storage::read_contract(store.as_ref(), &cursor_path)
            .await
            .unwrap();
    assert_eq!(repaired.sequence, latest_cursor.sequence);
    assert_eq!(repaired.cursor, latest_cursor.cursor);

    // Conflicting applied/pending lineages at an old sequence are refused even
    // when the cached cursor is already ahead of them.
    use futures::TryStreamExt;
    let envelopes: Vec<_> = store
        .list(Some(&path.clone().join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    let mut duplicate: fyrr_core::envelope::PreparedEnvelope =
        fyrr_reader::storage::read_contract(store.as_ref(), &envelopes[0].location)
            .await
            .unwrap();
    duplicate.rejection_generation += 1;
    duplicate.envelope_id = fyrr_core::envelope::envelope_id(
        &duplicate.namespace,
        &duplicate.source_id,
        duplicate.kind,
        duplicate.sequence,
        duplicate.rejection_generation,
        duplicate.cursor.start.as_deref(),
    );
    fyrr_writer::prepare::put_immutable(
        store.as_ref(),
        &path
            .join("envelopes")
            .join(format!("{}.json", duplicate.envelope_id)),
        &duplicate,
    )
    .await
    .unwrap();
    let conflict = run(&changed, &["writer", "sync"]);
    assert!(!conflict.status.success());
    assert!(String::from_utf8_lossy(&conflict.stdout).contains("immutable_object_conflict"));
}

#[test]
#[ignore = "final authorization on real executable/local-storage; scripts/ci"]
fn correction_inflight_identity_change_on_storage() {
    let (config, root) = setup();
    let text = std::fs::read_to_string(&config).unwrap();
    let text = text
        .split("[[sources]]")
        .take(2)
        .collect::<Vec<_>>()
        .join("[[sources]]");
    std::fs::write(&config, &text).unwrap();
    let sources = root.join("sources");
    std::fs::remove_dir_all(&sources).unwrap();
    std::fs::create_dir(&sources).unwrap();
    std::fs::write(sources.join("0.txt"), "fixture document 0").unwrap();
    success(&config, &["writer", "namespace", "create"]);
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    success(&config, &["writer", "commit"]);
    let revoked = root.join("partial-revoke.toml");
    std::fs::write(
        &revoked,
        text.replace(
            "stdio_readers = [\"grp:allow\"]",
            "stdio_readers = [\"grp:deny\"]",
        ),
    )
    .unwrap();
    let revoke = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let armed = revoke.clone();
    let hook: BoundaryHook = Arc::new(move |request, _, after| {
        if !after && request.starts_with("AUTHORIZE ") && armed.swap(0, Ordering::SeqCst) != 0 {
            success(&revoked, &["writer", "commit"]);
        }
        false
    });
    let (proxied_endpoint, thread) = proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
    let proxied = root.join("proxy.toml");
    std::fs::write(&proxied, text.replace(&endpoint, &proxied_endpoint)).unwrap();
    let mut child = Command::new(crate::common::binary())
        .arg("--config")
        .arg(&proxied)
        .args(["reader", "serve"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{"query":"fixture query"}}});
    initialize_stdio(&mut input, &mut output);
    let mut line = String::new();
    writeln!(input, "{request}").unwrap();
    output.read_line(&mut line).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap()["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    // Publish only after this request completes retrieval, before its final live check.
    // Background warm preparation can read the head but cannot consume this boundary.
    revoke.store(1, Ordering::SeqCst);
    writeln!(input, "{request}").unwrap();
    line.clear();
    output.read_line(&mut line).unwrap();
    assert_eq!(revoke.load(Ordering::SeqCst), 0, "revocation hook fired");
    assert!(line.contains("authorization_unavailable"), "{line}");
    writeln!(input, "{request}").unwrap();
    line.clear();
    output.read_line(&mut line).unwrap();
    let following: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(following["result"]["isError"], false);
    assert_eq!(
        following["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    // A no-op commit during final refresh changes nothing served and succeeds.
    revoke.store(1, Ordering::SeqCst);
    writeln!(input, "{request}").unwrap();
    line.clear();
    output.read_line(&mut line).unwrap();
    assert_eq!(revoke.load(Ordering::SeqCst), 0, "no-op commit hook fired");
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap()["result"]["isError"],
        false
    );
    drop(input);
    assert!(child.wait().unwrap().success());
    success(&config, &["writer", "commit"]);
    let arguments = ["reader", "search", "dense-only query"];
    // The CLI uses the same semantic output boundary.
    revoke.store(1, Ordering::SeqCst);
    let refused = run(&proxied, &arguments);
    assert_eq!(
        revoke.load(Ordering::SeqCst),
        0,
        "CLI revocation hook fired"
    );
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("authorization_unavailable"));
    assert!(refused.stdout.is_empty(), "no stale CLI result is printed");
    assert!(
        success(&proxied, &arguments)["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    stop.store(true, Ordering::Relaxed);
    drop(thread);
}

// The mirror is deliberately too small to retain immutable files, ensuring a
// real retrieval read reaches the proxy after the admission head read.
fn retrieval_error_during_identity_change(query: Option<&str>) {
    let (config, root) = setup();
    let text = std::fs::read_to_string(&config).unwrap();
    let text = text
        .split("[[sources]]")
        .take(2)
        .collect::<Vec<_>>()
        .join("[[sources]]")
        .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 1");
    std::fs::write(&config, &text).unwrap();
    let sources = root.join("sources");
    std::fs::remove_dir_all(&sources).unwrap();
    std::fs::create_dir(&sources).unwrap();
    std::fs::write(sources.join("0.txt"), "fixture document 0").unwrap();
    success(&config, &["writer", "namespace", "create"]);
    {
        let output = run(&config, &["writer", "sync"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    success(&config, &["writer", "commit"]);
    let revoked = root.join("revoked.toml");
    std::fs::write(
        &revoked,
        text.replace(
            "stdio_readers = [\"grp:allow\"]",
            "stdio_readers = [\"grp:deny\"]",
        ),
    )
    .unwrap();
    let heads = Arc::new(HeadReads::default());
    let failures = Arc::new(Mutex::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxied_endpoint, thread) = proxy(
        endpoint.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
        revoked,
        stop.clone(),
        Some((heads.clone(), failures.clone())),
        None,
    );
    let proxied = root.join("proxy.toml");
    std::fs::write(&proxied, text.replace(&endpoint, &proxied_endpoint)).unwrap();
    let response = if let Some(query) = query {
        let mut child = Command::new(crate::common::binary())
            .arg("--config")
            .arg(&proxied)
            .args(["reader", "serve"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        initialize_stdio(&mut input, &mut output);
        writeln!(input, "{}", json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{"query":query}}})).unwrap();
        let mut line = String::new();
        output.read_line(&mut line).unwrap();
        drop(input);
        assert!(child.wait().unwrap().success());
        line
    } else {
        let output = run(&proxied, &["reader", "search", "dense-only query"]);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        String::from_utf8(output.stderr).unwrap()
    };
    stop.store(true, Ordering::Relaxed);
    drop(thread);
    assert!(
        *failures.lock().unwrap() > 0,
        "retrieval must fail after admission"
    );
    assert_eq!(
        heads.started(),
        3,
        "open, admission and final current-head reads are required"
    );
    assert!(response.contains("authorization_unavailable"), "{response}");
    assert!(
        !response.contains("storage_unavailable"),
        "the retrieval error must be suppressed"
    );
}

pub(super) fn proxied(config: &Path, endpoint: &str) -> PathBuf {
    let path = config.parent().unwrap().join("fault.toml");
    std::fs::write(
        &path,
        std::fs::read_to_string(config)
            .unwrap()
            .replace(&std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(), endpoint),
    )
    .unwrap();
    path
}

pub(super) fn rewrite_ok_body(response: &mut Vec<u8>, rewrite: impl FnOnce(&mut Vec<u8>) -> bool) {
    let Some(split) = response.windows(4).position(|p| p == b"\r\n\r\n") else {
        return;
    };
    let header = String::from_utf8_lossy(&response[..split]).to_string();
    if !header.starts_with("HTTP/1.1 200") {
        return;
    }
    let mut body = response[split + 4..].to_vec();
    if header
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut plain = Vec::new();
        let mut position = 0;
        loop {
            let end = body[position..]
                .windows(2)
                .position(|p| p == b"\r\n")
                .unwrap()
                + position;
            let size = usize::from_str_radix(
                std::str::from_utf8(&body[position..end])
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap(),
                16,
            )
            .unwrap();
            if size == 0 {
                break;
            }
            position = end + 2;
            plain.extend_from_slice(&body[position..position + size]);
            position += size + 2;
        }
        body = plain;
    }
    if rewrite(&mut body) {
        let mut next = header
            .split("\r\n")
            .filter(|line| {
                let lower = line.to_ascii_lowercase();
                !lower.starts_with("content-length:")
                    && !lower.starts_with("transfer-encoding:")
                    && !lower.starts_with("trailer:")
            })
            .collect::<Vec<_>>()
            .join("\r\n");
        next.push_str(&format!("\r\nContent-Length: {}\r\n\r\n", body.len()));
        *response = next.into_bytes();
        response.extend_from_slice(&body);
    }
}

#[test]
#[ignore = "retrieval errors and identity changes on real MCP/RustFS; scripts/ci"]
fn retrieval_error_revocation_cases() {
    for query in [Some("fixture query"), Some("fixture document"), None] {
        retrieval_error_during_identity_change(query);
    }
}

#[test]
#[ignore = "real S3 endpoint identity in the private mirror; scripts/ci"]
fn mirror_endpoint_isolation_on_real_s3() {
    let (config, root) = setup_on(Backend::S3);
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .split("[[sources]]")
        .take(2)
        .collect::<Vec<_>>()
        .join("[[sources]]");
    std::fs::write(&config, &text).unwrap();
    let sources = root.join("sources");
    std::fs::remove_dir_all(&sources).unwrap();
    std::fs::create_dir(&sources).unwrap();
    std::fs::write(sources.join("0.txt"), "fixture document 0").unwrap();
    success(&config, &["writer", "namespace", "create"]);
    {
        let output = run(&config, &["writer", "sync", "--full"]);
        assert!(output.status.success(), "{}", report(&output));
    }
    success(&config, &["writer", "commit"]);
    success(
        &config,
        &["reader", "search", "fixture query", "--output", "content"],
    );
    // Same namespace and bucket at another endpoint gets a different cache.
    let before = std::fs::read_dir(root.join("mirror")).unwrap().count();
    let alternate = root.join("alternate.toml");
    std::fs::write(&alternate, text.replace("127.0.0.1", "localhost")).unwrap();
    success(
        &alternate,
        &["reader", "search", "fixture query", "--output", "content"],
    );
    assert_eq!(
        std::fs::read_dir(root.join("mirror")).unwrap().count(),
        before + 1,
        "endpoint isolates mirror keys"
    );
}
