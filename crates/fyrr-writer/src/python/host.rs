//! The client of the Python provider host: one child process per use, started
//! by a worker from the interpreter that [`super::interpreter`] finds, talking
//! over a socket it inherits (`fyrr_connectors.host`).
//!
//! The child stays in the worker's process group and keeps its resource
//! limits, so the supervisor's kill, timeout and orphan paths reach it. Its
//! stdin, stdout and stderr are `/dev/null`: the protocol never uses them.
//! Whatever the peer sends is untrusted: every frame is bounded before it is
//! allocated, every message belongs to a closed set, and a failure is one of
//! three closed outcomes ([`Failure`]) that carry no peer-supplied value.

use super::interpreter;
use super::wire::{Kind, PROTOCOL_VERSION, WireError, read_frame, write_frame};
use crate::worker::RESULT_MAX_BYTES;
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use fyrr_core::connector::SyncKind;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::Shutdown;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// Exception names and frames can contain supplied values.
pub(super) const PYTHON_FAILURE: &str = "Python failure";
const START_FAILED: &str = "the Python provider host could not start (Python failure)";
const EXITED_AT_START: &str = "the Python provider host exited at startup (Python failure); \
make sure fyrr-connectors, with pyarrow, is installed in the environment VIRTUAL_ENV names \
(or in the python3 on PATH)";

/// Rows and headers may exceed the configured batch bytes by their framing
/// (Arrow schema and padding, JSON syntax); the exact limits are then checked
/// on the decoded batch and by the batch checker.
const FRAME_SLACK: u64 = 1 << 20;
/// A `close` runs the provider's cleanup; it gets this long.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// A host whose socket is closed exits by itself; it gets this long.
const EXIT_GRACE: Duration = Duration::from_secs(2);
/// Control messages and documents travel on the worker's result channel size.
const CONTROL_MAX: u64 = RESULT_MAX_BYTES as u64;
/// The most roots one `lookup_revisions` may ask for.
pub(super) const MAX_ROOTS: usize = 256;
/// Environment of the worker itself that the provider process never needs.
const WORKER_ONLY_ENV: [&str; 3] = [
    "FYRR_WORKER_RESULT",
    "FYRR_WORKER_MEMORY",
    "FYRR_WRITER_EMBEDDING_CAP",
];

/// How a provider run failed, as the closed class the caller maps to its own
/// diagnostic: the provider (or its process) failed, a message was malformed
/// or out of order, or a batch was unacceptable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Failure {
    Provider,
    Tuple,
    Batch,
}

impl From<WireError> for Failure {
    fn from(error: WireError) -> Self {
        match error {
            WireError::Closed => Self::Provider,
            WireError::Malformed => Self::Tuple,
            WireError::Oversize => Self::Batch,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "t", rename_all = "snake_case", deny_unknown_fields)]
enum Message {
    Ready { protocol: u64, sdk_version: String },
    Started,
    Done,
    Failed,
    Oversize,
    Closed,
    LookupRevisions { roots: Vec<String> },
    NextRevisions,
}

/// What a prepared run lets the provider ask the worker for.
pub(super) trait Revisions {
    /// The revisions of `roots`; `None` fails the provider's call.
    fn lookup(&mut self, roots: &[String]) -> Option<BTreeMap<String, String>>;
    /// The next page of published revisions, empty at the end; `None` fails the call.
    fn next(&mut self) -> Option<Vec<(String, String)>>;
}

/// The bounds one batch must stay within.
#[derive(Debug, Clone, Copy)]
pub(super) struct RowLimits {
    pub(super) max_bytes: u64,
    /// Set when the row count is enforced as the rows arrive.
    pub(super) max_rows: Option<u64>,
}

/// One batch of a run: the contract header and, for content, its rows.
#[derive(Debug)]
pub(super) struct Item {
    pub(super) header: String,
    pub(super) rows: Option<RecordBatch>,
}

#[derive(Debug)]
pub(crate) struct Host {
    child: Child,
    stream: UnixStream,
    /// The conversation is out of step or the peer is gone: nothing more is
    /// sent, and the child is killed rather than waited for.
    broken: bool,
}

impl Host {
    /// Starts a host with `credentials` added to its environment, and waits
    /// for its handshake.
    pub(super) fn start(credentials: &[(String, String)]) -> Result<Self, String> {
        Self::launch(
            interpreter::resolve()?,
            &["-P", "-m", "fyrr_connectors.host"],
            credentials,
        )
    }

    /// Starts `python` with `entry` and the socket's descriptor number.
    pub(super) fn launch(
        python: std::path::PathBuf,
        entry: &[&str],
        credentials: &[(String, String)],
    ) -> Result<Self, String> {
        let (stream, theirs) = UnixStream::pair().map_err(|_| START_FAILED.to_owned())?;
        // The only descriptor the child inherits (std's sockets close on exec).
        rustix::io::fcntl_setfd(&theirs, rustix::io::FdFlags::empty())
            .map_err(|_| START_FAILED.to_owned())?;
        let mut command = Command::new(python);
        command
            .args(entry)
            .arg(theirs.as_raw_fd().to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for name in WORKER_ONLY_ENV {
            command.env_remove(name);
        }
        for (name, value) in credentials {
            command.env(name, value);
        }
        let child = command.spawn().map_err(|_| START_FAILED.to_owned())?;
        drop(theirs);
        let mut host = Self {
            child,
            stream,
            broken: false,
        };
        host.handshake()?;
        Ok(host)
    }

    fn handshake(&mut self) -> Result<(), String> {
        match self.read_message() {
            Ok(Message::Ready {
                protocol: PROTOCOL_VERSION,
                ..
            }) => Ok(()),
            Ok(Message::Ready {
                protocol,
                sdk_version,
            }) => Err(format!(
                "the installed fyrr-connectors {} speaks provider-host protocol {protocol}; this \
                 fyrr speaks {PROTOCOL_VERSION}: install the fyrr-connectors that matches this fyrr",
                displayable(&sdk_version)
            )),
            Err(Failure::Provider) => Err(EXITED_AT_START.to_owned()),
            Ok(_) | Err(_) => Err(PYTHON_FAILURE.to_owned()),
        }
    }

    /// The JSON document of an `inventory` or `discover` request.
    pub(crate) fn document(&mut self, request: &serde_json::Value) -> Result<String, String> {
        self.send(request).map_err(|_| PYTHON_FAILURE.to_owned())?;
        let (kind, payload) = self
            .read(|kind| match kind {
                Kind::Control | Kind::Document => CONTROL_MAX,
                _ => 0,
            })
            .map_err(|_| PYTHON_FAILURE.to_owned())?;
        match kind {
            Kind::Document => String::from_utf8(payload).map_err(|_| PYTHON_FAILURE.to_owned()),
            _ => Err(PYTHON_FAILURE.to_owned()),
        }
    }

    /// Opens a provider run of one source and kind; batches follow with
    /// [`Run::next`]. `revisions` makes the two revision callbacks available.
    pub(super) fn run(
        &mut self,
        source_json: &str,
        kind: SyncKind,
        cursor: Option<&str>,
        revisions: bool,
    ) -> Result<Run<'_>, Failure> {
        self.send(&serde_json::json!({
            "t": "run",
            "source": source_json,
            "kind": kind.as_str(),
            "cursor": cursor,
            "revisions": revisions,
        }))?;
        match self.read_message()? {
            Message::Started => Ok(Run {
                host: self,
                finished: false,
            }),
            Message::Failed => Err(Failure::Provider),
            _ => Err(self.out_of_step()),
        }
    }

    fn send(&mut self, message: &serde_json::Value) -> Result<(), Failure> {
        let payload = serde_json::to_vec(message).map_err(|_| Failure::Tuple)?;
        write_frame(&mut self.stream, Kind::Control, &payload).map_err(|_| {
            self.broken = true;
            Failure::Provider
        })
    }

    fn read(&mut self, bound: impl Fn(Kind) -> u64) -> Result<(Kind, Vec<u8>), Failure> {
        read_frame(&mut self.stream, bound).map_err(|error| {
            self.broken = true;
            error.into()
        })
    }

    /// The next frame, which must be a control message.
    fn read_message(&mut self) -> Result<Message, Failure> {
        let (kind, payload) = self.read(|kind| match kind {
            Kind::Control => CONTROL_MAX,
            _ => 0,
        })?;
        match (kind, serde_json::from_slice(&payload)) {
            (Kind::Control, Ok(message)) => Ok(message),
            _ => Err(self.out_of_step()),
        }
    }

    fn out_of_step(&mut self) -> Failure {
        self.broken = true;
        Failure::Tuple
    }

    /// Asks the host to close the running generator so that its cleanup runs.
    fn close(&mut self) {
        if self.broken {
            return;
        }
        let _ = self.stream.set_read_timeout(Some(CLOSE_TIMEOUT));
        let closed = self.send(&serde_json::json!({"t": "close"})).is_ok()
            && matches!(self.read_message(), Ok(Message::Closed));
        self.broken |= !closed;
        let _ = self.stream.set_read_timeout(None);
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        // Closing the socket ends a healthy host; anything else is killed.
        // The supervisor's process-group kill covers a worker that dies first.
        let _ = self.stream.shutdown(Shutdown::Both);
        let deadline = Instant::now()
            + if self.broken {
                Duration::ZERO
            } else {
                EXIT_GRACE
            };
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(2));
                }
                _ => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    return;
                }
            }
        }
    }
}

/// An open provider run. Dropping it closes the provider's generator unless
/// [`Run::finish`] says the read succeeded, so its cleanup always runs.
#[derive(Debug)]
pub(super) struct Run<'h> {
    host: &'h mut Host,
    finished: bool,
}

impl Run<'_> {
    /// The next batch, `None` when the provider is done. `limits` is `None`
    /// where the run takes no rows. While the provider runs it may call
    /// back; `revisions` answers it.
    pub(super) fn next(
        &mut self,
        limits: Option<RowLimits>,
        mut revisions: Option<&mut dyn Revisions>,
    ) -> Result<Option<Item>, Failure> {
        let host = &mut *self.host;
        host.send(&serde_json::json!({"t": "next"}))?;
        let batch_bound = limits.map_or(0, |limits| limits.max_bytes.saturating_add(FRAME_SLACK));
        loop {
            let (kind, payload) = host.read(|kind| match kind {
                Kind::Control => CONTROL_MAX,
                Kind::Header | Kind::Batch => batch_bound.max(FRAME_SLACK),
                Kind::Document | Kind::Rows => 0,
            })?;
            match kind {
                Kind::Header => return Ok(Some(Item::new(payload, None, host)?)),
                Kind::Batch => {
                    let Some(limits) = limits else {
                        // Rows where none are allowed; their frame stays unread.
                        host.broken = true;
                        return Err(Failure::Batch);
                    };
                    let (kind, rows) = host.read(|kind| match kind {
                        Kind::Rows => batch_bound,
                        Kind::Control => CONTROL_MAX,
                        _ => 0,
                    })?;
                    if kind != Kind::Rows {
                        return Err(host.out_of_step());
                    }
                    let rows = decode_rows(rows).ok_or_else(|| {
                        host.broken = true;
                        Failure::Batch
                    })?;
                    let over_rows = limits
                        .max_rows
                        .is_some_and(|max| rows.num_rows() as u64 > max);
                    let too_big = crate::batch::rows_bytes(&rows)
                        .is_none_or(|bytes| bytes > limits.max_bytes);
                    if over_rows || too_big {
                        host.broken = true;
                        return Err(Failure::Batch);
                    }
                    return Ok(Some(Item::new(payload, Some(rows), host)?));
                }
                Kind::Control => {}
                Kind::Document | Kind::Rows => return Err(host.out_of_step()),
            }
            let Ok(message) = serde_json::from_slice(&payload) else {
                return Err(host.out_of_step());
            };
            match message {
                Message::Done => return Ok(None),
                Message::Failed => return Err(Failure::Provider),
                Message::Oversize => return Err(Failure::Batch),
                Message::LookupRevisions { roots } => {
                    let Some(revisions) = revisions.as_deref_mut() else {
                        return Err(host.out_of_step());
                    };
                    let reply = if roots.len() > MAX_ROOTS {
                        None
                    } else {
                        revisions.lookup(&roots)
                    };
                    host.send(&reply.map_or_else(
                        || serde_json::json!({"t": "error"}),
                        |map| serde_json::json!({"t": "revisions", "map": map}),
                    ))?;
                }
                Message::NextRevisions => {
                    let Some(revisions) = revisions.as_deref_mut() else {
                        return Err(host.out_of_step());
                    };
                    host.send(&revisions.next().map_or_else(
                        || serde_json::json!({"t": "error"}),
                        |items| serde_json::json!({"t": "revisions", "items": items}),
                    ))?;
                }
                Message::Ready { .. } | Message::Started | Message::Closed => {
                    return Err(host.out_of_step());
                }
            }
        }
    }

    /// The read succeeded: the generator ran to its end.
    pub(super) fn finish(mut self) {
        self.finished = true;
    }
}

impl Drop for Run<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Cleanup failure must never replace the original diagnostic.
            self.host.close();
        }
    }
}

impl Item {
    fn new(header: Vec<u8>, rows: Option<RecordBatch>, host: &mut Host) -> Result<Self, Failure> {
        match String::from_utf8(header) {
            Ok(header) => Ok(Self { header, rows }),
            Err(_) => Err(host.out_of_step()),
        }
    }
}

/// Exactly one record batch of an Arrow IPC stream, validated by the decoder.
/// The batch's buffers are slices of the frame, not copies.
fn decode_rows(stream: Vec<u8>) -> Option<RecordBatch> {
    let mut buffer = Buffer::from_vec(stream);
    let mut decoder = StreamDecoder::new();
    let mut batch = None;
    while !buffer.is_empty() {
        if let Some(rows) = decoder.decode(&mut buffer).ok()?
            && batch.replace(rows).is_some()
        {
            return None;
        }
    }
    decoder.finish().ok()?;
    batch
}

/// A version string of the peer that is safe to print.
fn displayable(version: &str) -> &str {
    if !version.is_empty()
        && version.len() <= 40
        && version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".+-".contains(&byte))
    {
        version
    } else {
        "<invalid>"
    }
}
