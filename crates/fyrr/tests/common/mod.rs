//! Helpers shared by the `fyrr` binary tests, including the reusable privacy
//! sentinel harness. Every privacy test guards logs, artifacts and error paths.
//!
//! The harness plants unique markers where private values enter fyrr
//! (secrets in the environment or misplaced in the config, document text,
//! titles, authors, reader tokens, query-like input) and asserts that no
//! marker leaves through stdout, stderr or any file a run writes. Each run
//! gets a fresh sandbox directory as its working directory, `HOME`, `TMPDIR`
//! and XDG directories, so logs, caches and artifacts land where the scan
//! looks. Later slices point their storage fixture, log files and outputs
//! into [`Sandbox::output`] and call [`assert_clean`] the same way.
#![allow(dead_code)]

#[cfg(feature = "writer")]
pub mod embedding;

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// The repository root.
pub fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root")
}

/// The example configuration.
pub fn example_config() -> PathBuf {
    repo().join("config/fyrr.example.toml")
}

/// Directory holding the test-only connector providers (static dist-info).
pub fn test_providers() -> PathBuf {
    repo().join("crates/fyrr/tests/providers")
}

/// An immutable variant selected by CI, or Cargo's binary for a direct test run.
pub fn binary() -> OsString {
    std::env::var_os("FYRR_GATE_BINARY").unwrap_or_else(|| env!("CARGO_BIN_EXE_fyrr").into())
}

/// The `fyrr` binary under test with a controlled environment: no inherited
/// `FYRR_CONFIG`, and in the full build the connectors venv (`VIRTUAL_ENV`)
/// with the test providers on `PYTHONPATH`. `scripts/doctor` creates the venv.
pub fn fyrr() -> Command {
    let mut command = Command::new(binary());
    command
        .env_remove("FYRR_CONFIG")
        .env_remove("PYTHONHOME")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("PYTHONDONTWRITEBYTECODE", "1");
    if cfg!(feature = "writer") {
        let venv = repo().join("connectors/.venv");
        assert!(
            venv.join("pyvenv.cfg").exists(),
            "{} is missing; run scripts/doctor",
            venv.display()
        );
        command
            .env("VIRTUAL_ENV", venv)
            .env("PYTHONPATH", test_providers());
    }
    command
}

/// Unique markers for one test, by category (`secret`, `title`, …).
#[derive(Debug, Default)]
pub struct Sentinels {
    planted: BTreeMap<String, String>,
}

impl Sentinels {
    pub fn new() -> Self {
        Self::default()
    }

    /// A new marker: letters, digits and dashes only, so it fits TOML,
    /// JSON, URIs and reader tokens without escaping.
    pub fn mark(&mut self, category: &str) -> String {
        let marker = format!(
            "SNT{}x{}-{category}-q7",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        self.planted.insert(marker.clone(), category.to_owned());
        marker
    }

    /// Every marker that appears in `text`, with its category.
    pub fn found_in(&self, text: &str) -> Vec<String> {
        self.planted
            .iter()
            .filter(|(marker, _)| text.contains(marker.as_str()))
            .map(|(marker, category)| format!("{category} ({marker})"))
            .collect()
    }
}

/// A per-test directory: `inputs/` for what the test plants (configs,
/// provider fixtures; never scanned) and `run/` for everything fyrr may
/// write (scanned).
#[derive(Debug)]
pub struct Sandbox {
    root: PathBuf,
}

impl Sandbox {
    pub fn new(name: &str) -> Self {
        Self::new_in(name, Path::new(env!("CARGO_TARGET_TMPDIR")))
    }

    pub fn new_in(name: &str, directory: &Path) -> Self {
        let root = directory.join(format!(
            "{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        for dir in ["inputs", "run/home", "run/tmp", "run/cwd"] {
            std::fs::create_dir_all(root.join(dir)).expect("sandbox directory");
        }
        Self { root }
    }

    /// Writes an input file and returns its path.
    pub fn input(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.root.join("inputs").join(name);
        std::fs::write(&path, contents).expect("write input");
        path
    }

    /// Where runs write; later slices put storage and logs here.
    pub fn output(&self) -> PathBuf {
        self.root.join("run")
    }

    /// Runs fyrr inside the sandbox (cwd, HOME, TMPDIR, XDG dirs).
    pub fn configured<I, S>(&self, config: &Path, args: I, env: &[(&str, &str)]) -> Output
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut all = vec![OsString::from("--config"), config.as_os_str().to_owned()];
        all.extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        self.run(all, env)
    }
    pub fn run<I, S>(&self, args: I, env: &[(&str, &str)]) -> Output
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.run_command(fyrr(), args, env)
    }

    /// Runs another binary of this package inside the sandbox.
    pub fn run_command<I, S>(&self, mut command: Command, args: I, env: &[(&str, &str)]) -> Output
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let run = self.output();
        command
            .args(args)
            .current_dir(run.join("cwd"))
            .env("HOME", run.join("home"))
            .env("TMPDIR", run.join("tmp"))
            .env("XDG_CACHE_HOME", run.join("home/.cache"))
            .env("XDG_STATE_HOME", run.join("home/.local/state"))
            .env("XDG_DATA_HOME", run.join("home/.local/share"))
            // The identity keys `config_with` references; callers may override them.
            // CLI sandboxes never use the real host's EC2 instance profile.
            .env("FYRR_IDENTITY_KEY", SANDBOX_IDENTITY_KEY)
            .env("FYRR_ENCRYPTION_KEY", SANDBOX_ENCRYPTION_KEY);
        for (key, value) in env {
            command.env(key, value);
        }
        command.output().expect("run fyrr")
    }

    /// Every file under the run directory.
    pub fn written_files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();
        let mut stack = vec![self.output()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read sandbox").flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    files.push(path);
                }
            }
        }
        files
    }
}

/// Asserts that no planted marker appears in the run's stdout, stderr or any
/// file written under the sandbox's run directory.
pub fn assert_clean(sentinels: &Sentinels, output: &Output, sandbox: &Sandbox, context: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut leaks = Vec::new();
    for (stream, text) in [("stdout", &stdout), ("stderr", &stderr)] {
        for found in sentinels.found_in(text) {
            leaks.push(format!("{stream}: {found}"));
        }
    }
    for file in sandbox.written_files() {
        let name = file.file_name().unwrap().to_string_lossy();
        assert!(
            !(name.starts_with("fyrr-writer-") && name.ends_with(".sqlite")),
            "{context}: a named writer scratch database survived: {}",
            file.display()
        );
        let bytes = std::fs::read(&file).unwrap_or_default();
        for found in sentinels.found_in(&String::from_utf8_lossy(&bytes)) {
            leaks.push(format!("{}: {found}", file.display()));
        }
    }
    assert!(
        leaks.is_empty(),
        "{context}: private values leaked: {leaks:#?}\n--- stdout\n{stdout}\n--- stderr\n{stderr}"
    );
}

/// stdout and stderr as text.
pub fn text(output: &Output) -> (String, String) {
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Fixed test values of the identity keys `config_with` references.
pub const SANDBOX_IDENTITY_KEY: &str = "sandbox-identity-key";
pub const SANDBOX_ENCRYPTION_KEY: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

/// A minimal valid configuration plus `extra` TOML.
pub fn config_with(extra: &str) -> String {
    format!(
        "[namespace]\nname = \"acme\"\n[storage]\nuri = \"s3://fyrr-data/acme\"\n[identity]\nhashing_key = {{ env = \"FYRR_IDENTITY_KEY\" }}\nencryption_key = {{ env = \"FYRR_ENCRYPTION_KEY\" }}\n[embedding]\nendpoint = \"https://embeddings.example/v1\"\n{extra}"
    )
}

/// A `[[sources]]` entry using a test provider with a fixture file.
pub fn source(id: &str, provider: &str, fixture: &Path, settings: &str) -> String {
    format!(
        "\n[[sources]]\nid = \"{id}\"\ntype = \"{provider}\"\npermissions = \"fixed\"\nreaders = [\"grp:testers\"]\n[sources.settings]\n{settings}\n[sources.scope]\nfixture = \"{}\"\n",
        fixture.display()
    )
}

/// The JSON fixture the `sentinel-*` providers read: one document whose
/// title, author, text and reader carry markers, and one user.
pub fn provider_fixture(sentinels: &mut Sentinels) -> String {
    serde_json::json!({
        "cursor": "cursor-1",
        "documents": [{
            "id": format!("doc-{}", sentinels.mark("root-id")),
            "title": sentinels.mark("title"),
            "author": sentinels.mark("author"),
            "text": format!("Body {} text", sentinels.mark("document-text")),
            "readers": [format!("usr:{}", sentinels.mark("reader-token"))],
        }],
        "users": [{ "id": sentinels.mark("user-id"), "email": format!("{}@example.com", sentinels.mark("email")), "email_verification": "verified_corporate" }],
    })
    .to_string()
}
