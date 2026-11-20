//! The host client against scripted peers: closed diagnostics for every way a
//! peer can misbehave, row and error policies, bounds, callbacks and cleanup.
//! The peers are small Python scripts that speak the protocol by hand; the
//! real host is covered by its own tests and by the CLI tests.

use super::driver::{ErrMap, Request, RowPolicy, drive_provider};
use super::host::{Failure, Host, Revisions, RowLimits};
use super::*;
use crate::worker::Diagnostic;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// The connectors environment that `scripts/doctor` prepares.
fn python() -> PathBuf {
    let python =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../connectors/.venv/bin/python");
    assert!(
        python.exists(),
        "{} is missing; run scripts/doctor",
        python.display()
    );
    python
}

/// A peer that plays `FAKE_MODE` and logs what it was asked to `FAKE_LOG`.
const PEER: &str = r#"
import json, os, socket, struct, sys, time
sock = socket.socket(fileno=int(sys.argv[1]))
mode = os.environ["FAKE_MODE"]
log = open(os.environ["FAKE_LOG"], "a")
def exact(n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            os._exit(0)
        data += chunk
    return data
def send(kind, payload):
    sock.sendall(struct.pack(">cI", kind, len(payload)) + payload)
def say(message):
    send(b"J", json.dumps(message).encode())
def request():
    kind, length = struct.unpack(">cI", exact(5))
    return json.loads(exact(length))
def ipc(rows):
    import pyarrow as pa
    batch = pa.RecordBatch.from_pylist([{"a": "x" * 100} for _ in range(rows)])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()
protocol = 99 if mode == "other-protocol" else 1
say({"t": "ready", "protocol": protocol, "sdk_version": "9.9.9"})
batches = 0
while True:
    message = request()
    kind = message["t"]
    log.write(kind + "\n"); log.flush()
    if kind == "run":
        say({"t": "started"})
    elif kind == "close":
        if mode == "hang-on-close":
            log.write("pid %d\n" % os.getpid()); log.flush()
            time.sleep(3600)
        say({"t": "closed"})
    elif kind == "next":
        batches += 1
        if mode == "empty":
            say({"t": "done"})
        elif mode in ("provider-error", "hang-on-close"):
            say({"t": "failed"})
        elif mode == "oversize":
            say({"t": "oversize"})
        elif mode == "bad-kind":
            send(b"X", b"")
        elif mode == "huge-frame":
            sock.sendall(b"H" + struct.pack(">I", 0xFFFFFFFF))
        elif mode == "invalid-batch":
            send(b"H", b"not-json")
        elif mode == "bad-rows":
            send(b"B", b"not-json"); send(b"A", b"garbage")
        elif mode == "two-batches":
            send(b"B", b"not-json"); send(b"A", b"garbage")
        elif mode == "many-rows":
            send(b"B", b"not-json"); send(b"A", ipc(5))
        elif mode == "rows-then-nothing":
            send(b"B", b"not-json"); say({"t": "done"})
        elif mode == "bytes":
            send(b"B", b"not-json"); send(b"A", ipc(5))
        elif mode == "exit-early":
            os._exit(3)
        elif mode == "stray-start":
            say({"t": "started"})
        elif mode == "non-utf8-header":
            send(b"H", b"\xff\xfe")
        elif mode == "lookup":
            say({"t": "lookup_revisions", "roots": ["a", "b"]})
            reply = request()
            log.write(json.dumps(reply) + "\n"); log.flush()
            say({"t": "next_revisions"})
            log.write(json.dumps(request()) + "\n"); log.flush()
            say({"t": "lookup_revisions", "roots": ["r%d" % n for n in range(257)]})
            log.write(json.dumps(request()) + "\n"); log.flush()
            say({"t": "done"})
        elif mode == "unsolicited-lookup":
            say({"t": "lookup_revisions", "roots": ["a"]})
"#;

struct Scripted {
    host: Host,
    log: PathBuf,
}

impl Scripted {
    fn start(mode: &str) -> Result<Self, String> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let log = std::env::temp_dir().join(format!(
            "fyrr-peer-{}-{}.log",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&log);
        let credentials = vec![
            ("FAKE_MODE".to_owned(), mode.to_owned()),
            ("FAKE_LOG".to_owned(), log.display().to_string()),
        ];
        let host = Host::launch(python(), &["-P", "-c", PEER], &credentials)?;
        Ok(Self { host, log })
    }

    /// What the peer was asked, in order, once the host is gone.
    fn requests(self) -> Vec<String> {
        let Self { host, log } = self;
        drop(host);
        std::fs::read_to_string(&log)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

fn checker(kind: SyncKind, max_rows: u64) -> BatchChecker {
    let limits = crate::batch::Limits {
        max_rows,
        max_bytes: 1 << 20,
        document_max_bytes: 1 << 20,
        row_warning: None,
        row_error: None,
        retention_days: None,
    };
    BatchChecker::new("fixture", kind, limits, None)
}

#[test]
fn the_handshake_refuses_another_protocol_and_names_the_sdk() {
    let error = Scripted::start("other-protocol").err().unwrap();
    assert!(
        error.contains("protocol 99") && error.contains("9.9.9"),
        "{error}"
    );
    assert!(error.contains("this fyrr speaks 1"), "{error}");
}

#[test]
fn a_run_closes_its_generator_unless_the_read_succeeds() {
    for (mode, failure) in [
        ("provider-error", Failure::Provider),
        ("oversize", Failure::Batch),
        ("bad-kind", Failure::Tuple),
        ("exit-early", Failure::Provider),
    ] {
        let mut peer = Scripted::start(mode).unwrap();
        let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
        let limits = Some(RowLimits {
            max_bytes: 1 << 20,
            max_rows: None,
        });
        assert_eq!(run.next(limits, None).unwrap_err(), failure, "{mode}");
        drop(run);
        let requests = peer.requests();
        // A peer that is gone or out of step is not asked to close.
        let closed = matches!(mode, "provider-error" | "oversize");
        assert_eq!(
            requests.iter().filter(|r| *r == "close").count(),
            usize::from(closed),
            "{mode}: {requests:?}"
        );
    }
    let mut peer = Scripted::start("empty").unwrap();
    let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
    assert!(run.next(None, None).unwrap().is_none());
    run.finish();
    assert_eq!(peer.requests(), ["run", "next"]);
}

#[test]
fn frames_over_their_bound_and_unusable_rows_are_batch_failures() {
    let limits = |max_bytes, max_rows| {
        Some(RowLimits {
            max_bytes,
            max_rows,
        })
    };
    for (mode, limits, failure) in [
        // A length of four gigabytes is refused without reading or allocating it.
        ("huge-frame", limits(1 << 20, None), Failure::Batch),
        ("bad-rows", limits(1 << 20, None), Failure::Batch),
        // Rows are not allowed here: the batch is refused, not decoded.
        ("bad-rows", None, Failure::Batch),
        // Five rows of 100 bytes exceed 200 bytes, and exceed two rows.
        ("many-rows", limits(200, None), Failure::Batch),
        ("many-rows", limits(1 << 20, Some(2)), Failure::Batch),
        ("non-utf8-header", limits(1 << 20, None), Failure::Tuple),
        ("rows-then-nothing", limits(1 << 20, None), Failure::Tuple),
        ("stray-start", limits(1 << 20, None), Failure::Tuple),
        ("unsolicited-lookup", limits(1 << 20, None), Failure::Tuple),
    ] {
        let mut peer = Scripted::start(mode).unwrap();
        let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
        assert_eq!(run.next(limits, None).unwrap_err(), failure, "{mode}");
    }
    // Within both limits the same rows are accepted, and decode to one batch.
    let mut peer = Scripted::start("many-rows").unwrap();
    let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
    let item = run.next(limits(1 << 20, Some(5)), None).unwrap().unwrap();
    assert_eq!(item.rows.unwrap().num_rows(), 5);
    assert_eq!(item.header, "not-json");
}

struct Answers(Vec<String>);

impl Revisions for Answers {
    fn lookup(&mut self, roots: &[String]) -> Option<BTreeMap<String, String>> {
        self.0.push(format!("lookup {}", roots.len()));
        Some(
            roots
                .iter()
                .map(|root| (root.clone(), "r1".to_owned()))
                .collect(),
        )
    }
    fn next(&mut self) -> Option<Vec<(String, String)>> {
        self.0.push("next".into());
        None
    }
}

#[test]
fn revision_requests_are_answered_while_a_batch_is_produced() {
    let mut peer = Scripted::start("lookup").unwrap();
    let mut run = peer.host.run("{}", SyncKind::Content, None, true).unwrap();
    let mut answers = Answers(Vec::new());
    assert!(run.next(None, Some(&mut answers)).unwrap().is_none());
    run.finish();
    // The 257-root request is refused by the cap and never reaches the answers.
    assert_eq!(answers.0, ["lookup 2", "next"]);
    let requests = peer.requests();
    assert_eq!(
        requests[2..5],
        [
            r#"{"t": "revisions", "map": {"a": "r1", "b": "r1"}}"#,
            r#"{"t": "error"}"#,
            r#"{"t": "error"}"#,
        ]
    );
}

#[test]
fn the_row_and_error_policies_map_each_failure_to_its_own_diagnostic() {
    use WriterError as W;
    for (policy, errors, mode, expected) in [
        (
            RowPolicy::Forbidden,
            ErrMap::NON_CONTENT,
            "bad-kind",
            W::BatchInvalid,
        ),
        (
            RowPolicy::Required,
            ErrMap::CONTENT,
            "bad-kind",
            W::ProviderFailure,
        ),
        (
            RowPolicy::Forbidden,
            ErrMap::NON_CONTENT,
            "bad-rows",
            W::BatchInvalid,
        ),
        (
            RowPolicy::Required,
            ErrMap::CONTENT,
            "bad-rows",
            W::BatchInvalid,
        ),
        (
            RowPolicy::Forbidden,
            ErrMap::NON_CONTENT,
            "invalid-batch",
            W::BatchRejected,
        ),
        (
            RowPolicy::Required,
            ErrMap::CONTENT,
            "invalid-batch",
            W::BatchInvalid,
        ),
        (
            RowPolicy::Forbidden,
            ErrMap::NON_CONTENT,
            "provider-error",
            W::ProviderFailure,
        ),
        (
            RowPolicy::Required,
            ErrMap::CONTENT,
            "provider-error",
            W::ProviderFailure,
        ),
        (
            RowPolicy::Required,
            ErrMap::CONTENT,
            "exit-early",
            W::ProviderFailure,
        ),
    ] {
        let mut peer = Scripted::start(mode).unwrap();
        let kind = if matches!(policy, RowPolicy::Required) {
            SyncKind::Content
        } else {
            SyncKind::Permissions
        };
        let mut checker = checker(kind, 1);
        let mut delivered = 0;
        let request = Request {
            source_json: "{}",
            kind,
            cursor: None,
        };
        let result = drive_provider(
            &mut peer.host,
            &request,
            None,
            &mut checker,
            policy,
            errors,
            |_, _, _| {
                delivered += 1;
                Ok(())
            },
        );
        let Err(error) = result else {
            panic!("{mode}: an invalid provider batch was accepted");
        };
        assert_eq!(error, expected, "{mode}");
        assert_eq!(delivered, 0, "invalid batches must never reach staging");
    }
}

#[test]
fn a_dry_run_maps_failures_to_worker_diagnostics_and_runs_empty_providers() {
    for (mode, expected) in [
        ("invalid-batch", Err(Diagnostic::BatchRejected)),
        ("provider-error", Err(Diagnostic::ProviderFailure)),
        ("exit-early", Err(Diagnostic::ProviderFailure)),
        ("bad-kind", Err(Diagnostic::BatchRejected)),
        ("bad-rows", Err(Diagnostic::BatchRejected)),
        ("huge-frame", Err(Diagnostic::BatchRejected)),
        ("empty", Ok(())),
    ] {
        let mut peer = Scripted::start(mode).unwrap();
        let mut checker = checker(SyncKind::Content, 1);
        assert_eq!(
            run_kind_in(&mut peer.host, "{}", SyncKind::Content, &mut checker),
            expected,
            "{mode}"
        );
    }
}

#[test]
fn a_host_that_hangs_on_close_is_killed_and_leaves_no_process() {
    let mut peer = Scripted::start("hang-on-close").unwrap();
    let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
    // Dropping the run asks the peer to close; it never answers.
    let started = std::time::Instant::now();
    let _ = run.next(None, None);
    drop(run);
    let log = peer.log.clone();
    drop(peer.host);
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
    let pid: u32 = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("pid ")?.parse().ok())
        .expect("the peer recorded its pid");
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "the peer outlived its host"
    );
}

#[test]
fn prepared_callback_origin_keeps_host_codes_closed() {
    for code in crate::worker::codes() {
        assert!(WriterError::parse(code).is_some(), "{code}");
    }
    for (text, expected) in [
        ("sync_lease_lost", WriterError::BatchRejected),
        ("storage_unavailable", WriterError::StorageUnavailable),
        ("private-exception", WriterError::BatchRejected),
        ("host_callback_failed", WriterError::BatchRejected),
    ] {
        assert_eq!(WriterError::from_provider(text.to_owned()), expected);
    }
    assert_eq!(
        WriterError::parse("sync_lease_lost"),
        Some(WriterError::SyncLeaseLost)
    );
    assert!(WriterError::parse("private-exception").is_none());
}
