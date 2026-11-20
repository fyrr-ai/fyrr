//! The client of the Python provider host: one child process per use, started
//! by a worker from the interpreter that [`super::interpreter`] finds, talking
//! over a socket (`fyrr_connectors.host`).
//!
//! The child stays in the worker's process group and keeps its resource
//! limits, so the supervisor's kill, timeout and orphan paths reach it. Its
//! stdout and stderr are `/dev/null`. Its stdin is its end of the socket,
//! which is the only descriptor it is handed. The interpreter starts without
//! site code (`-S`), and [`entry`] moves the socket to a descriptor that no
//! child of a provider inherits, points stdin at `/dev/null` and closes every
//! other inherited descriptor, all before `.pth` files, `sitecustomize` or any
//! provider import can run. Passing the socket as stdin keeps it close-on-exec
//! in every other process, so no concurrent spawn can leak it.
//!
//! Whatever the peer sends is untrusted: every frame is bounded before it is
//! allocated, Arrow streams are inspected before they are decoded
//! ([`super::ipc`]), every message belongs to a closed set, and a failure is
//! one of three closed outcomes ([`Failure`]) or, at start, one closed
//! [`StartError`]; none carries a peer-supplied value but a version string
//! that has the shape of a version.

use super::interpreter;
use super::ipc;
use super::wire::{Kind, PROTOCOL_VERSION, WireError, read_frame, write_frame};
use crate::worker::RESULT_MAX_BYTES;
use arrow_array::RecordBatch;
use fyrr_core::connector::SyncKind;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// Exception names and frames can contain supplied values.
pub(super) const PYTHON_FAILURE: &str = "Python failure";

/// Exit codes of the bootstrap that tell the reasons for a failed start apart
/// before the protocol begins.
///
/// The interpreter cannot import the SDK's runtime (a missing SDK or pyarrow):
const EXIT_NO_SDK: i32 = 90;
/// The host module cannot be imported (an SDK older than the host protocol):
const EXIT_NO_HOST: i32 = 91;
/// The interpreter is older than [`MIN_PYTHON`]:
const EXIT_OLD_PYTHON: i32 = 92;
/// The descriptors could not be set up:
const EXIT_DETACH: i32 = 93;
/// The oldest interpreter that can run the SDK, as (major, minor).
pub(super) const MIN_PYTHON: (u32, u32) = (3, 13);

/// The arguments that start the host: no site code (`-S`), then a bootstrap
/// that runs in this order, using only built-in modules until the last step:
///
/// 1. keep the current directory (the empty `sys.path` entry, which `-c`
///    adds) off `sys.path`, as `-P` would, without needing a version that has
///    `-P`;
/// 2. exit with [`EXIT_OLD_PYTHON`] when the interpreter is older than
///    `minimum`, before any other import, so that an old interpreter says why
///    instead of failing on an option or a module it lacks. `posix`, the
///    module behind `os`, is built into the interpreter on Linux, so nothing
///    from the path or the environment is imported before the next step;
/// 3. move the socket (stdin) to a non-inheritable descriptor, point stdin at
///    `/dev/null` and close every other inheritable descriptor above stderr,
///    found in `/proc/self/fd` with no numeric ceiling. Descriptors the
///    interpreter opened itself are not inheritable and stay, so objects that
///    own them keep working. A hook closes the socket in every process forked
///    from here on, so a fork by site code has already let go of it; the
///    child then forgets the number, so its own forks never close whatever
///    reuses it;
/// 4. run what start-up would have run: `site.main()` (virtual environments,
///    the user site, `.pth` files, `sitecustomize`) and `PYTHONPATH` stay
///    as documented, but only now, with nothing inherited left to leak;
/// 5. import the SDK and the host, with an exit code for each import that
///    can fail, and serve.
pub(super) fn entry(minimum: (u32, u32)) -> [String; 3] {
    let (major, minor) = minimum;
    let source = format!(
        r#"import sys
sys.path[:] = [entry for entry in sys.path if entry != '']
if sys.version_info < ({major}, {minor}):
    sys.exit({EXIT_OLD_PYTHON})
import posix
try:
    fd = posix.dup(0)
    null = posix.open('/dev/null', posix.O_RDWR)
    posix.dup2(null, 0)
    if null != 0:
        posix.close(null)
    names = posix.listdir('/proc/self/fd')
except OSError:
    sys.exit({EXIT_DETACH})
def leave():
    global fd
    if fd < 0:
        return
    try:
        posix.close(fd)
    except OSError:
        pass
    fd = -1
posix.register_at_fork(after_in_child=leave)
for name in names:
    number = int(name)
    if number > 2 and number != fd:
        try:
            if posix.get_inheritable(number):
                posix.close(number)
        except OSError:
            pass
import site
site.main()
try:
    import fyrr_connectors.runtime
except ImportError:
    sys.exit({EXIT_NO_SDK})
try:
    import fyrr_connectors.host as host
except ImportError:
    sys.exit({EXIT_NO_HOST})
sys.exit(host.main(sys.argv, fd))
"#
    );
    ["-S".to_owned(), "-c".to_owned(), source]
}

/// Frames of a batch may exceed the configured envelope by their framing
/// (JSON syntax, Arrow schema, message metadata and padding); the exact limits
/// are checked on the declared and decoded batch and by the batch checker.
const FRAME_SLACK: u64 = 1 << 20;
/// A `close` runs the provider's cleanup; the whole exchange, request and
/// reply, gets this long.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// A host whose socket is closed exits by itself; it gets this long.
const EXIT_GRACE: Duration = Duration::from_secs(2);
/// Control messages and documents travel on the worker's result channel size.
const CONTROL_MAX: u64 = RESULT_MAX_BYTES as u64;
/// What the host may set in the provider's environment: the host's own limits.
/// A name is what a configuration may reference (see
/// [`fyrr_core::config::valid_env_name`]) up to
/// [`fyrr_core::config::HOST_ENV_NAME_MAX`] bytes, and `config check` refuses
/// the names the host refuses ([`fyrr_core::config::host_env_refusal`]).
const ENV_MAX_VARIABLES: usize = 256;
const ENV_MAX_VALUE: usize = 1 << 20;
/// Environment of the worker itself that the provider process never needs.
const WORKER_ONLY_ENV: [&str; 3] = [
    "FYRR_WORKER_RESULT",
    "FYRR_WORKER_MEMORY",
    "FYRR_WRITER_EMBEDDING_CAP",
];

/// Whether the host would take these credentials: the names a configuration
/// may reference, within the host's name limit and without
/// [`fyrr_core::config::host_env_refusal`]; values within [`ENV_MAX_VALUE`]
/// without a NUL. The host applies the same rules, and configuration
/// validation refuses what this would, so a start failure here is a defect.
fn acceptable(credentials: &[(String, String)]) -> bool {
    credentials.len() <= ENV_MAX_VARIABLES
        && credentials.iter().all(|(name, value)| {
            fyrr_core::config::valid_env_name(name)
                && fyrr_core::config::host_env_refusal(name).is_none()
                && value.len() <= ENV_MAX_VALUE
                && !value.contains('\0')
        })
}

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

/// Why a host could not be started: a closed code that crosses from a worker
/// to the process that prints it. Each has one actionable message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StartError {
    /// No interpreter was found, or it could not be run, or it is older than
    /// [`MIN_PYTHON`].
    PythonUnavailable,
    /// The interpreter cannot import the connector SDK (or its pyarrow).
    SdkUnavailable,
    /// The SDK and this fyrr do not speak the same host protocol: it has no
    /// host module (older) or another protocol version. Carries the SDK's
    /// version, in the shape of a version, when it said one.
    ProtocolMismatch { sdk_version: Option<String> },
    /// Anything else: nothing about the peer is reported.
    Failed,
}

impl StartError {
    /// The closed code that names this error.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::PythonUnavailable => "python_unavailable",
            Self::SdkUnavailable => "sdk_unavailable",
            Self::ProtocolMismatch { .. } => "protocol_mismatch",
            Self::Failed => "python_failure",
        }
    }

    /// The error a code and an optional version name; unknown codes are
    /// [`StartError::Failed`], and a version is shown only if it looks like one.
    pub(crate) fn parse(code: &str, sdk_version: Option<&str>) -> Self {
        match code {
            "python_unavailable" => Self::PythonUnavailable,
            "sdk_unavailable" => Self::SdkUnavailable,
            "protocol_mismatch" => Self::ProtocolMismatch {
                sdk_version: sdk_version.map(crate::providers::version_text),
            },
            _ => Self::Failed,
        }
    }

    /// The version a mismatch names, for the code's wire form.
    pub(crate) fn sdk_version(&self) -> Option<&str> {
        match self {
            Self::ProtocolMismatch { sdk_version } => sdk_version.as_deref(),
            _ => None,
        }
    }
}

/// The message for an interpreter that could not be used: the lookup's own
/// reason when it found none, else that the one found cannot be run or is too old.
pub(super) fn unavailable(resolved: Result<std::path::PathBuf, String>) -> String {
    match resolved {
        Err(error) => error,
        Ok(python) => format!(
            "{} cannot be run, or is too old: the connector SDK needs Python {}.{} or newer; {}",
            python.display(),
            MIN_PYTHON.0,
            MIN_PYTHON.1,
            interpreter::FIX
        ),
    }
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fix = interpreter::FIX;
        match self {
            // Paths only: the same lookup that found nothing, now with its reason.
            Self::PythonUnavailable => f.write_str(&unavailable(interpreter::resolve())),
            Self::SdkUnavailable => match interpreter::resolve() {
                Ok(python) => write!(
                    f,
                    "the connector SDK (fyrr_connectors, with pyarrow) cannot be imported by {}; {fix}",
                    python.display()
                ),
                Err(_) => write!(f, "the connector SDK cannot be imported; {fix}"),
            },
            Self::ProtocolMismatch { sdk_version } => write!(
                f,
                "the installed fyrr-connectors{} does not match this fyrr (provider-host \
                 protocol {PROTOCOL_VERSION}); {fix}",
                sdk_version
                    .as_ref()
                    .map_or_else(String::new, |version| format!(" {version}"))
            ),
            Self::Failed => {
                f.write_str("the Python provider host failed to start (Python failure)")
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "t", rename_all = "snake_case", deny_unknown_fields)]
enum Message {
    Ready { protocol: u64, sdk_version: String },
    EnvironmentSet,
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

/// What a run's batches may carry.
#[derive(Debug, Clone, Copy)]
pub(super) struct Bounds {
    /// The bytes of a batch: its header, and its Arrow buffers.
    pub(super) max_bytes: u64,
    pub(super) rows: Rows,
}

/// Whether, and how many, Arrow rows a batch may carry.
#[derive(Debug, Clone, Copy)]
pub(super) enum Rows {
    /// None: a batch has only its header.
    Forbidden,
    /// At most this many rows.
    AtMost(u64),
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
    /// Starts a host and waits for its handshake. `credentials` reach the
    /// provider's environment only after the host is running, in a message
    /// that is bounded and never echoed. Their names are removed from the
    /// environment the host inherits, so neither a value this process also
    /// exports under such a name nor the credential is in the host's initial
    /// environment (`/proc/<pid>/environ`).
    pub(super) fn start(credentials: &[(String, String)]) -> Result<Self, StartError> {
        if !acceptable(credentials) {
            return Err(StartError::Failed);
        }
        let python = interpreter::resolve().map_err(|_| StartError::PythonUnavailable)?;
        let names: Vec<&str> = credentials.iter().map(|(name, _)| name.as_str()).collect();
        let mut host = Self::spawn(python, &entry(MIN_PYTHON), &[], &names)?;
        host.set_environment(credentials)?;
        Ok(host)
    }

    /// [`Host::spawn`] for tests, which script a peer and pass it
    /// `environment`; a production host gets none beyond its credentials.
    #[cfg(test)]
    pub(super) fn launch<S: AsRef<std::ffi::OsStr>>(
        python: std::path::PathBuf,
        entry: &[S],
        environment: &[(String, String)],
        unset: &[&str],
    ) -> Result<Self, StartError> {
        Self::spawn(python, entry, environment, unset)
    }

    /// Starts `python` with the arguments `entry`, whose stdin is the socket
    /// and whose environment is this process's, without the worker's own
    /// variables and without `unset`, plus `environment`.
    fn spawn<S: AsRef<std::ffi::OsStr>>(
        python: std::path::PathBuf,
        entry: &[S],
        environment: &[(String, String)],
        unset: &[&str],
    ) -> Result<Self, StartError> {
        let (stream, theirs) = UnixStream::pair().map_err(|_| StartError::Failed)?;
        let mut command = Command::new(python);
        command
            .args(entry)
            .stdin(Stdio::from(std::os::fd::OwnedFd::from(theirs)))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for name in WORKER_ONLY_ENV.iter().chain(unset) {
            command.env_remove(name);
        }
        for (name, value) in environment {
            command.env(name, value);
        }
        let child = command.spawn().map_err(|_| StartError::PythonUnavailable)?;
        // `command` held the socket's other end; it is closed with it.
        drop(command);
        let mut host = Self {
            child,
            stream,
            broken: false,
        };
        host.handshake()?;
        Ok(host)
    }

    /// The process id of the host, for tests that look at it from outside.
    #[cfg(test)]
    pub(super) fn pid(&self) -> u32 {
        self.child.id()
    }

    fn handshake(&mut self) -> Result<(), StartError> {
        match self.read_message() {
            Ok(Message::Ready {
                protocol: PROTOCOL_VERSION,
                ..
            }) => Ok(()),
            Ok(Message::Ready { sdk_version, .. }) => Err(StartError::ProtocolMismatch {
                sdk_version: Some(crate::providers::version_text(&sdk_version)),
            }),
            // The host is gone before it spoke: its exit status says why.
            Err(Failure::Provider) => Err(self.exit_at_start()),
            Ok(_) | Err(_) => Err(StartError::Failed),
        }
    }

    /// The reason a host exited before its handshake, from its exit status.
    fn exit_at_start(&mut self) -> StartError {
        let deadline = Instant::now() + EXIT_GRACE;
        let status = loop {
            match self.child.try_wait() {
                Ok(Some(status)) => break status.code(),
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(2));
                }
                _ => break None,
            }
        };
        self.broken = true;
        match status {
            Some(EXIT_NO_SDK) => StartError::SdkUnavailable,
            Some(EXIT_NO_HOST) => StartError::ProtocolMismatch { sdk_version: None },
            Some(EXIT_OLD_PYTHON) => StartError::PythonUnavailable,
            _ => StartError::Failed,
        }
    }

    /// Sets the provider's credentials in the host's `os.environ`, before any
    /// provider code can run. Names and values the host would refuse are
    /// refused here, without saying which.
    pub(super) fn set_environment(
        &mut self,
        credentials: &[(String, String)],
    ) -> Result<(), StartError> {
        if credentials.is_empty() {
            return Ok(());
        }
        if !acceptable(credentials) {
            return Err(StartError::Failed);
        }
        let variables: BTreeMap<_, _> = credentials.iter().cloned().collect();
        self.send(&serde_json::json!({"t": "environment", "variables": variables}))
            .map_err(|_| StartError::Failed)?;
        match self.read_message() {
            Ok(Message::EnvironmentSet) => Ok(()),
            _ => Err(StartError::Failed),
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
        self.read_by(None, bound)
    }

    /// Like [`Host::read`], but the whole frame must arrive by `deadline`
    /// when there is one: a peer that sends one byte at a time cannot extend it.
    fn read_by(
        &mut self,
        deadline: Option<Instant>,
        bound: impl Fn(Kind) -> u64,
    ) -> Result<(Kind, Vec<u8>), Failure> {
        let frame = match deadline {
            None => read_frame(&mut self.stream, bound),
            Some(deadline) => read_frame(
                &mut Until {
                    stream: &self.stream,
                    deadline,
                },
                bound,
            ),
        };
        frame.map_err(|error| {
            self.broken = true;
            error.into()
        })
    }

    /// The next frame, which must be a control message.
    fn read_message(&mut self) -> Result<Message, Failure> {
        self.read_message_by(None)
    }

    fn read_message_by(&mut self, deadline: Option<Instant>) -> Result<Message, Failure> {
        let (kind, payload) = self.read_by(deadline, |kind| match kind {
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
    /// One deadline covers the request and the whole reply.
    fn close(&mut self) {
        if self.broken {
            return;
        }
        let deadline = Instant::now() + CLOSE_TIMEOUT;
        // The request is one small frame, but a full send buffer must not
        // outlast the deadline either.
        let _ = self.stream.set_write_timeout(Some(CLOSE_TIMEOUT));
        let closed = self.send(&serde_json::json!({"t": "close"})).is_ok()
            && matches!(self.read_message_by(Some(deadline)), Ok(Message::Closed));
        self.broken |= !closed;
        let _ = self.stream.set_write_timeout(None);
        let _ = self.stream.set_read_timeout(None);
    }
}

/// A reader of the host's socket that gives up at an absolute deadline, not
/// after a quiet period: every read waits only for what time is left.
struct Until<'a> {
    stream: &'a UnixStream,
    deadline: Instant,
}

impl std::io::Read for Until<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let left = self
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
            .ok_or(std::io::ErrorKind::TimedOut)?;
        self.stream.set_read_timeout(Some(left))?;
        std::io::Read::read(&mut self.stream, buffer)
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
    /// The next batch, `None` when the provider is done. While the provider
    /// runs it may call back; `revisions` answers it.
    pub(super) fn next(
        &mut self,
        bounds: Bounds,
        mut revisions: Option<&mut dyn Revisions>,
    ) -> Result<Option<Item>, Failure> {
        let host = &mut *self.host;
        host.send(&serde_json::json!({"t": "next"}))?;
        // The configured envelope bounds every batch frame, whether or not the
        // batch has rows: a members snapshot is as large as it may be.
        let frame_bound = bounds.max_bytes.saturating_add(FRAME_SLACK);
        loop {
            let (kind, payload) = host.read(|kind| match kind {
                Kind::Control => CONTROL_MAX,
                Kind::Header | Kind::Batch => frame_bound,
                Kind::Document | Kind::Rows => 0,
            })?;
            match kind {
                Kind::Header => return Ok(Some(Item::new(payload, None, host)?)),
                Kind::Batch => {
                    let max_rows = match bounds.rows {
                        Rows::Forbidden => {
                            // Rows where none are allowed; their frame stays
                            // unread, so the conversation is out of step.
                            host.broken = true;
                            return Err(Failure::Batch);
                        }
                        Rows::AtMost(max) => max,
                    };
                    let (kind, rows) = host.read(|kind| match kind {
                        Kind::Rows => frame_bound,
                        Kind::Control => CONTROL_MAX,
                        _ => 0,
                    })?;
                    if kind != Kind::Rows {
                        return Err(host.out_of_step());
                    }
                    // Both frames are read: whatever is wrong with the rows,
                    // the host is in step and is asked to close its generator.
                    let rows = ipc::decode(
                        rows,
                        ipc::Bounds {
                            max_bytes: bounds.max_bytes,
                            max_rows,
                        },
                    )
                    .map_err(|_| Failure::Batch)?;
                    // The checker measures the decoded buffers against
                    // `max_bytes` (`batch.rs`); that is the one place that does.
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
                    host.send(&revisions.lookup(&roots).map_or_else(
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
                Message::Ready { .. }
                | Message::EnvironmentSet
                | Message::Started
                | Message::Closed => {
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
