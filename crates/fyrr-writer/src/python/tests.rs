//! The host client against scripted peers: closed diagnostics for every way a
//! peer can misbehave, row and error policies, bounds and cleanup.
//! The peers are small Python scripts that speak the protocol by hand; the
//! real host is covered by its own tests and by the CLI tests.

use super::driver::{ErrMap, Request, RowPolicy, drive_provider};
use super::host::{Bounds, Failure, Host, MIN_PYTHON, Rows, StartError, entry};
use super::*;
use crate::worker::Diagnostic;
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
import json, os, socket, struct, time
sock = socket.socket(fileno=os.dup(0))
mode = os.environ["FAKE_MODE"]
log = open(os.environ["FAKE_LOG"], "a")
def exact(n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            if mode == "hang-on-eof":
                log.write("pid %d\n" % os.getpid()); log.flush()
                time.sleep(3600)
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
protocol = 99 if mode in ("other-protocol", "garbled-version") else 1
version = "9.9.9 <script>" if mode == "garbled-version" else "9.9.9"
say({"t": "ready", "protocol": protocol, "sdk_version": version})
while True:
    kind = request()["t"]
    if kind == "run":
        say({"t": "started"})
    elif kind == "next":
        if mode == "empty":
            say({"t": "done"})
        elif mode in ("provider-error", "hang-on-eof"):
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
        elif mode == "header-over":
            send(b"H", b"x" * ((4 << 20) + (1 << 20) + 1))
        elif mode == "rows-then-nothing":
            send(b"B", b"not-json"); say({"t": "done"})
        elif mode == "exit-early":
            os._exit(3)
        elif mode == "stray-start":
            say({"t": "started"})
        elif mode == "non-utf8-header":
            send(b"H", b"\xff\xfe")
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
        let host = Host::launch(python(), &["-P", "-c", PEER], &environment, &[])?;
        Ok(Self { host, log })
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

#[test]
fn credentials_reach_the_provider_only_through_the_socket_and_bad_ones_start_no_host() {
    // HOME is in this process's environment, so a host would inherit it, as it
    // would the old value of a credential whose name is also exported here.
    assert!(std::env::var_os("HOME").is_some());
    // Names of the configured lengths are taken by the real host.
    let credentials = ["HOME".to_owned(), "N".repeat(129), "N".repeat(256)]
        .map(|name| (name, "SECRET-VALUE".to_owned()));
    let mut host = Host::start(&credentials).unwrap();
    let environ = std::fs::read(format!("/proc/{}/environ", host.pid())).unwrap();
    assert!(!environ.windows(12).any(|window| window == b"SECRET-VALUE"));
    assert!(
        !environ
            .split(|b| *b == 0)
            .any(|row| row.starts_with(b"HOME="))
    );
    // The host is alive and serves requests after it took them.
    assert!(
        host.document(&serde_json::json!({"t": "inventory"}))
            .is_ok()
    );
    // What the host would refuse is refused before one is started, and not named.
    let too_long = "N".repeat(257);
    for (name, value) in [
        ("", "SECRET-VALUE"),
        ("1BAD", "SECRET-VALUE"),
        ("A=B", "SECRET-VALUE"),
        ("lower", "SECRET-VALUE"),
        (too_long.as_str(), "SECRET-VALUE"),
        ("NAME", "SECRET-VALUE\0nul"),
        // Names that change which interpreter, libraries or modules run.
        ("PATH", "SECRET-VALUE"),
        ("VIRTUAL_ENV", "SECRET-VALUE"),
        ("PYTHONPATH", "SECRET-VALUE"),
        ("LD_PRELOAD", "SECRET-VALUE"),
    ] {
        let error = Host::start(&[(name.to_owned(), value.to_owned())])
            .err()
            .unwrap();
        assert_eq!(error, StartError::Failed);
        assert!(!error.to_string().contains("SECRET"), "{value:?}");
    }
}

#[test]
fn an_interpreter_older_than_the_minimum_ends_at_the_version_check_and_says_so() {
    let error = Host::launch(python(), &entry((99, 0)), &[], &[])
        .err()
        .unwrap();
    assert_eq!(error, StartError::PythonUnavailable);
    assert_eq!(error.code(), "python_unavailable");
    let message = super::host::unavailable(Ok(PathBuf::from("/venv/bin/python")));
    assert!(
        message.contains("/venv/bin/python") && message.contains("needs Python 3.13 or newer"),
        "{message}"
    );
}

#[test]
fn a_batch_header_over_the_envelope_and_its_slack_is_refused_unread() {
    // Under the envelope a header of any size passes (`incremental.rs` sends a
    // members snapshot of several megabytes); over it, and its framing slack,
    // the frame is refused unread, whether or not rows are allowed.
    for rows in [Rows::Forbidden, Rows::AtMost(10)] {
        let mut peer = Scripted::start("header-over").unwrap();
        let mut run = peer.host.run("{}", SyncKind::Members, None, false).unwrap();
        assert_eq!(
            run.next(bounds(4 * MIB, rows), None).unwrap_err(),
            Failure::Batch
        );
    }
}

#[test]
fn frames_over_their_bound_and_unusable_rows_are_batch_failures() {
    for (mode, bounds, failure) in [
        // A length of four gigabytes is refused without reading or allocating it.
        ("huge-frame", bounds(MIB, Rows::AtMost(100)), Failure::Batch),
        ("bad-rows", bounds(MIB, Rows::AtMost(100)), Failure::Batch),
        // Rows are not allowed here: the batch is refused, not decoded.
        ("bad-rows", bounds(MIB, Rows::Forbidden), Failure::Batch),
        ("oversize", bounds(MIB, Rows::AtMost(100)), Failure::Batch),
        (
            "non-utf8-header",
            bounds(MIB, Rows::AtMost(100)),
            Failure::Tuple,
        ),
        (
            "rows-then-nothing",
            bounds(MIB, Rows::AtMost(100)),
            Failure::Tuple,
        ),
        (
            "stray-start",
            bounds(MIB, Rows::AtMost(100)),
            Failure::Tuple,
        ),
        (
            "unsolicited-lookup",
            bounds(MIB, Rows::AtMost(100)),
            Failure::Tuple,
        ),
    ] {
        let mut peer = Scripted::start(mode).unwrap();
        let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
        assert_eq!(run.next(bounds, None).unwrap_err(), failure, "{mode}");
    }
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
        );
        assert_eq!(outcome, expected, "{mode}");
    }
}

#[test]
fn a_host_that_hangs_in_cleanup_is_killed_and_leaves_no_process() {
    let mut peer = Scripted::start("hang-on-eof").unwrap();
    let mut run = peer.host.run("{}", SyncKind::Content, None, false).unwrap();
    let _ = run.next(bounds(MIB, Rows::Forbidden), None);
    // Ending the host sends end-of-file; the peer then never exits by itself.
    let started = std::time::Instant::now();
    let log = peer.log.clone();
    drop(peer.host);
    assert!(started.elapsed() < std::time::Duration::from_secs(30));
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
