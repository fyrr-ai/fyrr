//! The host client against scripted peers: closed diagnostics for every way a
//! peer can misbehave, row and error policies, bounds, callbacks and cleanup.
//! The peers are small Python scripts that speak the protocol by hand; the
//! real host is covered by its own tests and by the CLI tests.

use super::driver::{ErrMap, Request, RowPolicy, drive_provider};
use super::host::{BOOTSTRAP, Bounds, Failure, Host, Revisions, Rows, StartError};
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
sock = socket.socket(fileno=os.dup(0))
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
version = "9.9.9 <script>" if mode == "garbled-version" else "9.9.9"
if mode in ("garbled-version",):
    protocol = 99
say({"t": "ready", "protocol": protocol, "sdk_version": version})
batches = 0
seen = 0
while True:
    message = request()
    seen += 1
    kind = message["t"]
    log.write(kind + "\n"); log.flush()
    if kind == "environment":
        # The message is never echoed; what it carried is checked from the log.
        log.write("initial-environment-has-it %s\n" % ("SECRET-VALUE" in open("/proc/self/environ").read()))
        log.write("first-request %s\n" % (seen == 1))
        log.write("variables %s\n" % json.dumps(message["variables"], sort_keys=True))
        log.flush()
        say({"t": "environment_set"})
    elif kind == "run":
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
            batches_sent = batches
            if batches_sent <= 2:
                send(b"H", [b"first", b"second"][batches_sent - 1])
            else:
                say({"t": "done"})
        elif mode == "many-rows":
            send(b"B", b"not-json"); send(b"A", ipc(5))
        elif mode == "header-2mib":
            send(b"H", b"x" * (2 << 20))
        elif mode == "header-over":
            send(b"H", b"x" * ((4 << 20) + (1 << 20) + 1))
        elif mode == "compressed":
            import pyarrow as pa
            batch = pa.RecordBatch.from_pylist([{"a": "x" * 100} for _ in range(5)])
            sink = pa.BufferOutputStream()
            options = pa.ipc.IpcWriteOptions(compression="lz4")
            with pa.ipc.new_stream(sink, batch.schema, options=options) as writer:
                writer.write_batch(batch)
            send(b"B", b"not-json"); send(b"A", sink.getvalue().to_pybytes())
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
    fn start(mode: &str) -> Result<Self, StartError> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let log = std::env::temp_dir().join(format!(
            "fyrr-peer-{}-{}.log",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&log);
        let environment = vec![
            ("FAKE_MODE".to_owned(), mode.to_owned()),
            ("FAKE_LOG".to_owned(), log.display().to_string()),
        ];
        let host = Host::launch(python(), &["-P", "-c", PEER], &environment)?;
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

const MIB: u64 = 1 << 20;

fn bounds(max_bytes: u64, rows: Rows) -> Bounds {
    Bounds { max_bytes, rows }
}

#[test]
fn the_handshake_refuses_another_protocol_and_names_the_sdk_as_a_closed_code() {
    let error = Scripted::start("other-protocol").err().unwrap();
    assert_eq!(
        error,
        StartError::ProtocolMismatch {
            sdk_version: Some("9.9.9".into())
        }
    );
    assert_eq!(error.code(), "protocol_mismatch");
    let message = error.to_string();
    assert!(
        message.contains("9.9.9") && message.contains("provider-host protocol 1"),
        "{message}"
    );
    // A version that does not look like one is never printed.
    let error = Scripted::start("garbled-version").err().unwrap();
    assert_eq!(
        error,
        StartError::ProtocolMismatch {
            sdk_version: Some("<invalid>".into())
        }
    );
    assert!(!error.to_string().contains("script"), "{error}");
    // The code survives the worker's result channel, and only as a code.
    let sent = super::host_error_result(&error);
    assert_eq!(super::host_error_from_result(&sent), Some(error));
    assert_eq!(
        super::host_error_from_result(br#"{"host_error":"x\u0000","sdk_version":"a b"}"#),
        Some(StartError::Failed)
    );
    assert_eq!(
        super::host_error_from_result(br#"{"host_error":"protocol_mismatch","sdk_version":"1 2"}"#),
        Some(StartError::ProtocolMismatch {
            sdk_version: Some("<invalid>".into())
        })
    );
    assert_eq!(super::host_error_from_result(br#"{"providers":[]}"#), None);
}

/// Starts the real host's bootstrap in an environment whose `fyrr_connectors`
/// is the package in `files` (a directory of module sources) instead of the SDK.
fn launch_with_sdk(name: &str, files: &[(&str, &str)]) -> Result<Host, StartError> {
    let dir = std::env::temp_dir().join(format!("fyrr-sdk-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let package = dir.join("fyrr_connectors");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(package.join("__init__.py"), "").unwrap();
    for (file, source) in files {
        std::fs::write(package.join(file), source).unwrap();
    }
    let result = Host::launch(
        python(),
        &["-P", "-c", BOOTSTRAP],
        &[("PYTHONPATH".to_owned(), dir.display().to_string())],
    );
    std::fs::remove_dir_all(dir).unwrap();
    result
}

#[test]
fn an_sdk_without_the_host_module_asks_for_the_matching_wheel() {
    // An SDK of the embedded-interpreter generation: it has `runtime` but no `host`.
    let error = launch_with_sdk("old", &[("runtime.py", "")]).err().unwrap();
    assert_eq!(error, StartError::ProtocolMismatch { sdk_version: None });
    let message = error.to_string();
    assert!(message.contains("does not match this fyrr"), "{message}");
    assert!(
        message.contains("wheel") && message.contains("VIRTUAL_ENV"),
        "{message}"
    );
    // No SDK, or one that cannot import its own runtime (a missing pyarrow).
    let error = launch_with_sdk("none", &[("runtime.py", "import no_such_module_anywhere")])
        .err()
        .unwrap();
    assert_eq!(error, StartError::SdkUnavailable);
    assert!(error.to_string().contains("cannot be imported"), "{error}");
    // Any other failure to start says nothing about the peer.
    let error = launch_with_sdk(
        "broken",
        &[
            ("runtime.py", ""),
            ("host.py", "def main(argv):\n    raise SystemExit(5)\n"),
        ],
    )
    .err()
    .unwrap();
    assert_eq!(error, StartError::Failed);
}

#[test]
fn credentials_are_sent_after_the_host_is_running_and_never_echoed() {
    let mut peer = Scripted::start("empty").unwrap();
    peer.host
        .set_environment(&[("SECRET_NAME".to_owned(), "SECRET-VALUE".to_owned())])
        .unwrap();
    let requests = peer.requests();
    assert_eq!(
        requests,
        [
            "environment",
            "initial-environment-has-it False",
            "first-request True",
            r#"variables {"SECRET_NAME": "SECRET-VALUE"}"#,
        ]
    );
    // What the host would refuse is refused before it is sent, and not named.
    for (name, value) in [
        ("", "SECRET-VALUE"),
        ("1BAD", "SECRET-VALUE"),
        ("A=B", "SECRET-VALUE"),
        ("NAME", "SECRET-VALUE\0nul"),
    ] {
        let mut peer = Scripted::start("empty").unwrap();
        let error = peer
            .host
            .set_environment(&[(name.to_owned(), value.to_owned())])
            .unwrap_err();
        assert_eq!(error, StartError::Failed);
        assert!(!error.to_string().contains("SECRET"), "{value:?}");
        assert!(peer.requests().is_empty());
    }
}

#[test]
fn credentials_are_not_in_the_real_hosts_initial_environment() {
    let mut host = Host::launch(python(), &["-P", "-c", BOOTSTRAP], &[]).unwrap();
    host.set_environment(&[("FYRR_TEST_CREDENTIAL".to_owned(), "SECRET-VALUE".to_owned())])
        .unwrap();
    let environ = std::fs::read(format!("/proc/{}/environ", host.pid())).unwrap();
    assert!(!environ.windows(12).any(|window| window == b"SECRET-VALUE"));
    assert!(!String::from_utf8_lossy(&environ).contains("FYRR_TEST_CREDENTIAL"));
    // The host is alive and serves requests after it took them.
    assert!(
        host.document(&serde_json::json!({"t": "inventory"}))
            .is_ok()
    );
}

#[test]
fn a_run_closes_its_generator_unless_the_read_succeeds_or_the_peer_is_out_of_step() {
    let any = bounds(MIB, Rows::Any);
    for (mode, bounds, failure, closed) in [
        ("provider-error", any, Failure::Provider, true),
        ("oversize", any, Failure::Batch, true),
        // The whole frame was read and then refused for its size or its rows:
        // the host is in step and is asked to close its generator.
        ("many-rows", bounds(200, Rows::Any), Failure::Batch, true),
        (
            "many-rows",
            bounds(MIB, Rows::AtMost(2)),
            Failure::Batch,
            true,
        ),
        ("compressed", any, Failure::Batch, true),
        ("bad-rows", any, Failure::Batch, true),
        // Out of step or gone: nothing more is sent.
        ("huge-frame", any, Failure::Batch, false),
        (
            "many-rows",
            bounds(MIB, Rows::Forbidden),
            Failure::Batch,
            false,
        ),
        ("bad-kind", any, Failure::Tuple, false),
        ("exit-early", any, Failure::Provider, false),
    ] {
        let mut peer = Scripted::start(mode).unwrap();
        let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
        assert_eq!(run.next(bounds, None).unwrap_err(), failure, "{mode}");
        drop(run);
        let requests = peer.requests();
        assert_eq!(
            requests.iter().filter(|r| *r == "close").count(),
            usize::from(closed),
            "{mode}: {requests:?}"
        );
    }
    let mut peer = Scripted::start("empty").unwrap();
    let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
    assert!(
        run.next(bounds(MIB, Rows::Forbidden), None)
            .unwrap()
            .is_none()
    );
    run.finish();
    assert_eq!(peer.requests(), ["run", "next"]);
}

#[test]
fn a_batch_header_may_be_as_large_as_the_envelope_whether_or_not_rows_are_allowed() {
    // A members snapshot is a header and no rows: 2 MiB under a 4 MiB envelope.
    for rows in [Rows::Forbidden, Rows::Any, Rows::AtMost(10)] {
        let mut peer = Scripted::start("header-2mib").unwrap();
        let mut run = peer.host.run("{}", SyncKind::Members, None, false).unwrap();
        let item = run.next(bounds(4 * MIB, rows), None).unwrap().unwrap();
        assert_eq!(item.header.len() as u64, 2 * MIB, "{rows:?}");
    }
    // Over the envelope and its framing slack, the frame is refused unread.
    let mut peer = Scripted::start("header-over").unwrap();
    let mut run = peer.host.run("{}", SyncKind::Members, None, false).unwrap();
    assert_eq!(
        run.next(bounds(4 * MIB, Rows::Forbidden), None)
            .unwrap_err(),
        Failure::Batch
    );
}

#[test]
fn frames_over_their_bound_and_unusable_rows_are_batch_failures() {
    for (mode, bounds, failure) in [
        // A length of four gigabytes is refused without reading or allocating it.
        ("huge-frame", bounds(MIB, Rows::Any), Failure::Batch),
        ("bad-rows", bounds(MIB, Rows::Any), Failure::Batch),
        // Rows are not allowed here: the batch is refused, not decoded.
        ("bad-rows", bounds(MIB, Rows::Forbidden), Failure::Batch),
        // Five rows of 100 bytes exceed 200 bytes, and exceed two rows.
        ("many-rows", bounds(200, Rows::Any), Failure::Batch),
        ("many-rows", bounds(MIB, Rows::AtMost(2)), Failure::Batch),
        // A compressed stream is refused before the decoder can inflate it.
        ("compressed", bounds(MIB, Rows::Any), Failure::Batch),
        ("non-utf8-header", bounds(MIB, Rows::Any), Failure::Tuple),
        ("rows-then-nothing", bounds(MIB, Rows::Any), Failure::Tuple),
        ("stray-start", bounds(MIB, Rows::Any), Failure::Tuple),
        ("unsolicited-lookup", bounds(MIB, Rows::Any), Failure::Tuple),
    ] {
        let mut peer = Scripted::start(mode).unwrap();
        let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
        assert_eq!(run.next(bounds, None).unwrap_err(), failure, "{mode}");
    }
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
    assert!(
        run.next(bounds(MIB, Rows::Forbidden), Some(&mut answers))
            .unwrap()
            .is_none()
    );
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
fn two_batches_of_one_run_arrive_in_order_over_one_conversation() {
    let mut peer = Scripted::start("two-batches").unwrap();
    let mut run = peer.host.run("{}", SyncKind::Members, None, false).unwrap();
    for expected in ["first", "second"] {
        let item = run
            .next(bounds(MIB, Rows::Forbidden), None)
            .unwrap()
            .unwrap();
        assert_eq!(item.header, expected);
        assert!(item.rows.is_none());
    }
    assert!(
        run.next(bounds(MIB, Rows::Forbidden), None)
            .unwrap()
            .is_none()
    );
    run.finish();
    assert_eq!(peer.requests(), ["run", "next", "next", "next"]);
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
        let request = Request {
            source_json: "{}",
            kind: SyncKind::Content,
            cursor: None,
        };
        let outcome = drive_provider(
            &mut peer.host,
            &request,
            None,
            &mut checker,
            RowPolicy::Optional,
            ErrMap::DRY_RUN,
            |_, _, _| Ok(()),
        )
        .map(|run| run.finish());
        assert_eq!(outcome, expected, "{mode}");
    }
}

#[test]
fn a_host_that_hangs_on_close_is_killed_and_leaves_no_process() {
    let mut peer = Scripted::start("hang-on-close").unwrap();
    let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
    // Dropping the run asks the peer to close; it never answers.
    let started = std::time::Instant::now();
    let _ = run.next(bounds(MIB, Rows::Forbidden), None);
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
