//! Bounded worker result channel, process-group cleanup and memory limits.
//! Provider stdout and stderr are discarded at the OS descriptor boundary.
//!
//! macOS is a development platform with weaker worker isolation (see
//! [`platform`]): the kernel does not enforce the worker's memory limit, and
//! a provider process that leaves the worker's process group outlives it.

use crate::WriterError;
use std::collections::BTreeSet;
use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fyrr_core::connector::SyncKind;
use rustix::process::{Pid, Resource, Rlimit, Signal, kill_process_group, setrlimit};
use serde::{Deserialize, Serialize};

pub const POLL: Duration = Duration::from_millis(20);
pub const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(120);
pub const DEFAULT_MEMORY: u64 = 2 << 30;
pub const RESULT_MAX_BYTES: usize = 1 << 20;
static NEXT: AtomicU64 = AtomicU64::new(0);
// Only supervisor-created workers belong here. Hold the lock across spawn
// and registration so orphan cleanup cannot mistake a new worker for an orphan.
static LIVE_WORKERS: Mutex<BTreeSet<u32>> = Mutex::new(BTreeSet::new());

/// Closed worker diagnostics: no provider-controlled free text crosses here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Diagnostic {
    ProviderFailure,
    BatchRejected,
    InvalidSource,
    WorkerFailure,
    TimedOut,
    InvalidReport,
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ProviderFailure => "the provider failed",
            Self::BatchRejected => "the provider batch was rejected",
            Self::InvalidSource => {
                "the source does not match its provider; run `fyrr config check`"
            }
            Self::WorkerFailure => "the worker failed",
            Self::TimedOut => {
                "the worker timed out and its process group was killed; increase --timeout"
            }
            Self::InvalidReport => "the worker result was missing or invalid",
        })
    }
}

#[derive(Debug, Clone)]
pub enum Job {
    Discover,
    Metadata {
        provider: String,
    },
    Sync {
        config: PathBuf,
        source: String,
        kinds: Vec<SyncKind>,
        prepare: bool,
        embedding_cap: Option<u64>,
        full: bool,
        due: bool,
    },
}

pub fn command(job: &Job) -> io::Result<Command> {
    let mut command = Command::new(std::env::current_exe()?);
    match job {
        Job::Discover => {
            command.args(["writer", "__worker", "discover"]);
        }
        Job::Metadata { provider } => {
            command.args(["writer", "__worker", "metadata", "--provider", provider]);
        }
        Job::Sync {
            config,
            source,
            kinds,
            prepare,
            embedding_cap,
            full,
            due,
        } => {
            command
                .arg("--config")
                .arg(config)
                .args(["writer", "__worker", "sync", "--source", source]);
            for kind in kinds {
                command.args(["--kind", kind.as_str()]);
            }
            command.env_remove("FYRR_WRITER_EMBEDDING_CAP");
            if let Some(cap) = embedding_cap {
                command.env("FYRR_WRITER_EMBEDDING_CAP", cap.to_string());
            }
            if *prepare {
                command.arg("--prepare");
            }
            if *due {
                command.arg("--due");
            }
            if *full {
                command.arg("--full");
            }
        }
    }
    command
        .env_remove("FYRR_CONFIG")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);
    python_home(&mut command);
    Ok(command)
}

fn python_home(command: &mut Command) {
    if std::env::var_os("PYTHONHOME").is_some() {
        return;
    }
    let Some(venv) = std::env::var_os("VIRTUAL_ENV") else {
        #[cfg(target_os = "macos")]
        if let Some(prefix) = platform::bundled_python() {
            command.env("PYTHONHOME", prefix);
        }
        return;
    };
    let Ok(text) = std::fs::read_to_string(Path::new(&venv).join("pyvenv.cfg")) else {
        return;
    };
    let home = text.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key.trim() == "home").then(|| PathBuf::from(value.trim()))
    });
    if let Some(prefix) = home.as_deref().and_then(Path::parent) {
        command.env("PYTHONHOME", prefix);
    }
}

/// Called before initializing Python. DATA limits heap and anonymous mappings,
/// without restricting the allocator's virtual address-space reservations.
pub fn initialize() -> io::Result<()> {
    let null = std::fs::OpenOptions::new().write(true).open("/dev/null")?;
    rustix::stdio::dup2_stdout(&null)?;
    rustix::stdio::dup2_stderr(&null)?;
    let memory = std::env::var("FYRR_WORKER_MEMORY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MEMORY);
    platform::limit_memory(setrlimit(
        Resource::Data,
        Rlimit {
            current: Some(memory),
            maximum: Some(memory),
        },
    ))?;
    setrlimit(
        Resource::Core,
        Rlimit {
            current: Some(0),
            maximum: Some(0),
        },
    )?;
    Ok(())
}

/// A separate local result socket, never stdout/stderr. Linux abstract sockets
/// leave no filesystem artifact and avoid path-length and cleanup races.
pub fn send(bytes: &[u8]) -> io::Result<()> {
    if bytes.len() > RESULT_MAX_BYTES {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let name = std::env::var("FYRR_WORKER_RESULT").map_err(|_| io::ErrorKind::NotConnected)?;
    let mut stream = platform::connect(&name)?;
    stream.write_all(bytes)
}

#[cfg(not(target_os = "macos"))]
mod platform {
    use super::*;
    use rustix::process::{WaitOptions, kill_process, set_child_subreaper, waitpid};
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::net::SocketAddr;

    /// The listening end of a result socket; nothing to remove afterwards.
    #[derive(Debug)]
    pub(super) struct Endpoint;

    pub(super) fn listen(name: &str) -> io::Result<(UnixListener, String, Endpoint)> {
        let listener = UnixListener::bind_addr(&SocketAddr::from_abstract_name(name)?)?;
        Ok((listener, name.to_owned(), Endpoint))
    }

    pub(super) fn connect(name: &str) -> io::Result<UnixStream> {
        UnixStream::connect_addr(&SocketAddr::from_abstract_name(name)?)
    }

    pub(super) fn limit_memory(set: rustix::io::Result<()>) -> io::Result<()> {
        Ok(set?)
    }

    /// Descendants that outlive their parent are adopted by this process.
    pub(super) fn adopt_orphans() -> io::Result<()> {
        Ok(set_child_subreaper(Some(Pid::INIT))?)
    }

    /// Kills every child of this process that is not a registered worker and
    /// returns how many there were.
    pub(super) fn kill_orphans(live: &BTreeSet<u32>) -> io::Result<usize> {
        let mut orphans = BTreeSet::new();
        for task in std::fs::read_dir("/proc/self/task")? {
            let path = task?.path().join("children");
            let text = match std::fs::read_to_string(path) {
                Ok(text) => text,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            for pid in text.split_whitespace() {
                let pid: u32 = pid.parse().map_err(|_| io::ErrorKind::InvalidData)?;
                if !live.contains(&pid) {
                    orphans.insert(pid);
                }
            }
        }
        for pid in &orphans {
            if let Some(pid) = Pid::from_raw(*pid as i32) {
                let _ = kill_process(pid, Signal::KILL);
                match waitpid(Some(pid), WaitOptions::NOHANG) {
                    Ok(_) | Err(rustix::io::Errno::CHILD) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(orphans.len())
    }
}

/// macOS has no abstract sockets, no child subreaper and no procfs, and its
/// kernel does not enforce `RLIMIT_DATA`. The result socket lives in a
/// private directory. Cleanup kills the worker's process group only: a
/// provider process that started its own session or group is not found and
/// keeps running. Use macOS for development, not to run untrusted providers.
#[cfg(target_os = "macos")]
mod platform {
    use super::*;
    use std::os::unix::fs::DirBuilderExt;

    /// The private directory of a result socket, removed with its socket.
    #[derive(Debug)]
    pub(super) struct Endpoint(PathBuf);

    impl Drop for Endpoint {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.0.join("result"));
            let _ = std::fs::remove_dir(&self.0);
        }
    }

    pub(super) fn listen(name: &str) -> io::Result<(UnixListener, String, Endpoint)> {
        // A fresh 0700 directory: `create` fails on an existing name, so no
        // other user can have placed the socket or redirect it.
        let directory = std::env::temp_dir().join(name);
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let endpoint = Endpoint(directory);
        let path = endpoint.0.join("result");
        let listener = UnixListener::bind(&path)?;
        let path = path.into_os_string().into_string();
        let path = path.map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok((listener, path, endpoint))
    }

    pub(super) fn connect(name: &str) -> io::Result<UnixStream> {
        UnixStream::connect(name)
    }

    /// The limit is recorded where the kernel accepts it; it is not enforced.
    pub(super) fn limit_memory(_: rustix::io::Result<()>) -> io::Result<()> {
        Ok(())
    }

    pub(super) fn adopt_orphans() -> io::Result<()> {
        Ok(())
    }

    pub(super) fn kill_orphans(_: &BTreeSet<u32>) -> io::Result<usize> {
        Ok(0)
    }

    /// The Python installation of a release archive: `bin/fyrr` sits in the
    /// prefix that holds `lib/python3.*`, so the workers use that standard
    /// library and the connector SDK installed into it.
    pub(super) fn bundled_python() -> Option<PathBuf> {
        let executable = std::env::current_exe().ok()?.canonicalize().ok()?;
        let prefix = executable.parent()?.parent()?;
        std::fs::read_dir(prefix.join("lib"))
            .ok()?
            .flatten()
            .any(|entry| {
                entry.file_name().to_string_lossy().starts_with("python3.")
                    && entry.path().join("os.py").is_file()
            })
            .then(|| prefix.to_owned())
    }
}

#[derive(Debug)]
pub enum Outcome {
    Exited { status: ExitStatus, result: Vec<u8> },
    TimedOut(Duration),
}

impl Outcome {
    pub fn result_line(&self) -> Option<&[u8]> {
        match self {
            Self::Exited { result, .. } => Some(result),
            Self::TimedOut(_) => None,
        }
    }
    pub fn failure(&self) -> Option<Diagnostic> {
        match self {
            Self::Exited { status, .. } if status.success() => None,
            Self::Exited { .. } => Some(Diagnostic::WorkerFailure),
            Self::TimedOut(_) => Some(Diagnostic::TimedOut),
        }
    }
}

#[derive(Debug)]
pub struct Running {
    child: Child,
    listener: UnixListener,
    _endpoint: platform::Endpoint,
    stream: Option<UnixStream>,
    result: Vec<u8>,
    invalid: bool,
    eof: bool,
    group_killed: bool,
    cleaned: bool,
    kill_at: Instant,
    deadline: Instant,
    timeout: Duration,
}

impl Running {
    pub fn spawn_with_memory(job: &Job, timeout: Duration, memory: u64) -> io::Result<Self> {
        platform::adopt_orphans()?;
        let mut live = LIVE_WORKERS
            .lock()
            .map_err(|_| io::Error::other("worker registry unavailable"))?;
        let name = format!(
            "fyrr-worker-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let (listener, address, endpoint) = platform::listen(&name)?;
        listener.set_nonblocking(true)?;
        let child = command(job)?
            .env("FYRR_WORKER_RESULT", address)
            .env("FYRR_WORKER_MEMORY", memory.to_string())
            .spawn()?;
        live.insert(child.id());
        let deadline = Instant::now() + timeout;
        // The timeout includes termination/reaping: reserve at most 100 ms
        // so cleanup uses the same absolute deadline as result draining.
        let kill_at = deadline - timeout.min(POLL * 5);
        Ok(Self {
            child,
            listener,
            _endpoint: endpoint,
            stream: None,
            result: Vec::new(),
            invalid: false,
            eof: false,
            group_killed: false,
            cleaned: false,
            kill_at,
            deadline,
            timeout,
        })
    }

    fn kill_group(&mut self) {
        if self.group_killed {
            return;
        }
        self.group_killed = true;
        if let Some(pid) = Pid::from_raw(self.child.id() as i32) {
            let _ = kill_process_group(pid, Signal::KILL);
        }
    }

    fn drain(&mut self) -> io::Result<()> {
        let stream = match &mut self.stream {
            Some(stream) => stream,
            vacant @ None => match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    vacant.insert(stream)
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => return Err(error),
            },
        };
        let mut buffer = [0u8; 8192];
        while Instant::now() < self.deadline && !self.invalid {
            match stream.read(&mut buffer) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(count) if self.result.len() + count <= RESULT_MAX_BYTES => {
                    self.result.extend_from_slice(&buffer[..count]);
                }
                Ok(_) => self.invalid = true,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub fn poll(&mut self) -> io::Result<Option<Outcome>> {
        self.drain()?;
        if self.invalid {
            return Err(io::ErrorKind::InvalidData.into());
        }
        if let Some(status) = self.child.try_wait()? {
            // Kill descendants even after success. Never join a blocking pipe reader.
            self.cleanup()?;
            self.drain()?;
            if self.eof || self.invalid || Instant::now() >= self.deadline {
                let result = if self.invalid || !self.eof {
                    Vec::new()
                } else {
                    std::mem::take(&mut self.result)
                };
                return Ok(Some(Outcome::Exited { status, result }));
            }
            // No report connection means there is nothing left to drain.
            if self.stream.is_none() {
                return Ok(Some(Outcome::Exited {
                    status,
                    result: Vec::new(),
                }));
            }
        }
        if Instant::now() >= self.kill_at {
            self.cleanup()?;
            return Ok(Some(Outcome::TimedOut(self.timeout)));
        }
        Ok(None)
    }

    /// Reap the worker, then all adopted descendants, including those that
    /// detached with setsid. Killing an adopted parent can reveal another
    /// generation, so enumerate every thread's children again until empty.
    fn cleanup(&mut self) -> io::Result<()> {
        if self.cleaned {
            return Ok(());
        }
        self.kill_group();
        loop {
            let reaped = self.child.try_wait()?.is_some();
            let mut live = LIVE_WORKERS
                .lock()
                .map_err(|_| io::Error::other("worker registry unavailable"))?;
            if reaped {
                live.remove(&self.child.id());
            }
            let orphans = platform::kill_orphans(&live)?;
            drop(live);
            if reaped && orphans == 0 {
                self.cleaned = true;
                return Ok(());
            }
            if Instant::now() >= self.deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            std::thread::sleep(
                Duration::from_millis(1)
                    .min(self.deadline.saturating_duration_since(Instant::now())),
            );
        }
    }

    pub fn wait(mut self) -> io::Result<Outcome> {
        loop {
            if let Some(outcome) = self.poll()? {
                return Ok(outcome);
            }
            std::thread::sleep(POLL.min(self.deadline.saturating_duration_since(Instant::now())));
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // Also covers parent channel/read errors and early returns. No blocking wait.
        let _ = self.cleanup();
    }
}

#[cfg(test)]
pub(crate) fn codes() -> impl Iterator<Item = &'static str> {
    crate::WriterError::ALL
        .iter()
        .copied()
        .map(crate::WriterError::as_str)
}
pub(crate) fn closed_code(value: &serde_json::Value) -> Option<WriterError> {
    WriterError::parse(value.as_str()?)
}
#[cfg(test)]
#[path = "worker_codes_tests.rs"]
mod codes_tests;
