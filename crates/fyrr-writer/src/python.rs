//! The embedded Python interpreter, used only inside a worker
//! process (see [`crate::worker`]), never in the process that reads the
//! configuration or orchestrates sources.
//!
//! The interpreter finds its standard library from `PYTHONHOME` or its
//! build prefix (`/usr/local` in the image); the parent derives `PYTHONHOME`
//! from `VIRTUAL_ENV` for development. Providers and the SDK come from the
//! interpreter's site-packages (the image), the user site (a mounted wheel
//! directory), `PYTHONPATH`, or the virtual environment named by
//! `VIRTUAL_ENV`, whose `.pth` files are processed.
//!
//! Content rows cross from Python to Rust through the Arrow C Data
//! Interface (`__arrow_c_array__` PyCapsules, no copy); everything printed is
//! value-free: Python failures use closed diagnostic codes.

use crate::{Result, WriterError};
use fyrr_core::connector::SyncKind;
use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::batch::BatchChecker;
use crate::sync::{ReportDefaults, default_report};
use crate::worker::Diagnostic;

// Exception names and frames can contain supplied values.
const PYTHON_FAILURE: &str = "Python failure";

/// Runs before the SDK is imported: the virtual environment, if any, becomes a site dir.
/// The worker suppresses stdout/stderr at the OS boundary before Python starts.
const BOOTSTRAP: &std::ffi::CStr = c"
import os, site, sys, sysconfig
_venv = os.environ.get('VIRTUAL_ENV')
if _venv:
    site.addsitedir(sysconfig.get_path('purelib', vars={'base': _venv, 'platbase': _venv}))
del _venv
";

/// Starts the interpreter, imports the SDK runtime and runs `body` with it.
pub fn with_runtime<R>(
    body: impl for<'py> FnOnce(Python<'py>, &Bound<'py, PyModule>) -> Result<R, String>,
) -> Result<R, String> {
    with_runtime_error(body, std::convert::identity)
}

fn with_runtime_error<R, E>(
    body: impl for<'py> FnOnce(Python<'py>, &Bound<'py, PyModule>) -> Result<R, E>,
    map_error: fn(String) -> E,
) -> Result<R, E> {
    Python::initialize();
    Python::attach(|py| {
        py.run(BOOTSTRAP, None, None).map_err(|_| {
            map_error("the embedded Python could not start (Python failure)".into())
        })?;
        let runtime = py.import("fyrr_connectors.runtime").map_err(|_| {
            map_error(
                concat!(
                    "cannot import the connector SDK fyrr_connectors (Python failure); ",
                    "install fyrr-connectors, or set VIRTUAL_ENV to an environment that has it"
                )
                .into(),
            )
        })?;
        body(py, &runtime)
    })
}

/// Close a provider iterator unless the entire read succeeds.
struct ProviderGenerator<'py> {
    value: Bound<'py, PyAny>,
    finished: bool,
}
impl<'py> ProviderGenerator<'py> {
    fn new(value: Bound<'py, PyAny>) -> Self {
        Self {
            value,
            finished: false,
        }
    }
    fn finish(&mut self) {
        self.finished = true;
    }
}
impl Drop for ProviderGenerator<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Cleanup failure must never replace the original diagnostic.
            let _ = self.value.call_method0("close");
        }
    }
}

/// Runs one sync kind of one source and checks every batch at the boundary.
pub fn run_kind(
    _py: Python<'_>,
    runtime: &Bound<'_, PyModule>,
    source_json: &str,
    kind: SyncKind,
    checker: &mut BatchChecker,
) -> Result<(), Diagnostic> {
    let mut generator = driver::drive_provider(
        runtime,
        (checker, (source_json, kind.as_str(), None::<String>)),
        driver::RowPolicy::Optional,
        driver::ErrMap::DRY_RUN,
        |_, _, _| Ok(()),
    )?;
    generator.finish();
    Ok(())
}

/// Enumerates entry points without importing or executing providers.
pub fn inventory(_py: Python<'_>, runtime: &Bound<'_, PyModule>) -> Result<String, String> {
    runtime
        .call_method0("inventory")
        .and_then(|r| r.extract::<String>())
        .map_err(|_| PYTHON_FAILURE.to_owned())
}

/// Loads metadata for exactly one entry point.
pub fn discover_provider(
    _py: Python<'_>,
    runtime: &Bound<'_, PyModule>,
    name: &str,
) -> Result<String, String> {
    runtime
        .call_method1("discover", (name,))
        .and_then(|r| r.extract::<String>())
        .map_err(|_| PYTHON_FAILURE.to_owned())
}

mod driver;
mod prepared;
use prepared::prepared_kind;

/// One process loads only its own provider and runs independent leased kinds.
pub fn prepared_sync(
    loaded: &fyrr_core::config::LoadedConfig,
    source_id: &str,
    requested: &[fyrr_core::connector::SyncKind],
    full: bool,
    due: bool,
) -> std::process::ExitCode {
    use fyrr_core::connector::SyncKind;
    let result = (|| -> Result<serde_json::Value> {
        let (index, source) = loaded
            .config
            .sources
            .iter()
            .enumerate()
            .find(|(_, s)| s.id == source_id)
            .ok_or(WriterError::SourceInvalid)?;
        // Whether the provider delivers permissions through its own stream.
        let (kinds, permission_stream) = with_runtime(|py, runtime| {
            let discovery = crate::providers::parse_discovery(
                discover_provider(py, runtime, &source.provider)?.as_bytes(),
            )?;
            // Parsed metadata may belong to a refused provider. Apply the same
            // checks as dry run before dispatching any kind, including serve jobs.
            if crate::providers::check_source(index, source, &discovery)
                .iter()
                .any(|issue| issue.severity == fyrr_core::config::Severity::Error)
            {
                return Err(
                    "the source does not match its provider; run `fyrr config check`".into(),
                );
            }
            let info = discovery
                .find(&source.provider)
                .and_then(|p| p.info.as_ref())
                .ok_or(WriterError::ProviderInvalid)?;
            let defaults = crate::providers::default_kinds(info, source);
            Ok((
                crate::providers::select_kinds(defaults, requested).0,
                info.sync_kinds.contains(&SyncKind::Permissions),
            ))
        })
        .map_err(|_| WriterError::ProviderInvalid)?;
        let mut reports = Vec::new();
        let mut failed = false;
        for kind in kinds {
            match prepared_kind(
                loaded,
                source_id,
                kind,
                full,
                due || (requested.is_empty() && kind == SyncKind::Reconcile),
                permission_stream,
            ) {
                Ok(value) => {
                    failed |= value.get("error").is_some();
                    reports.push(value);
                }
                Err(error) => {
                    failed = true;
                    reports.push(serde_json::json!({"kind":kind,"error":error}));
                }
            }
        }
        let content = reports
            .iter()
            .find(|r| r["kind"] == serde_json::json!(SyncKind::Content))
            .cloned()
            .unwrap_or_else(|| default_report(ReportDefaults::Source));
        let mut report = content;
        report["source_id"] = serde_json::json!(source_id);
        report["kinds"] = serde_json::json!(reports);
        report["failed"] = serde_json::json!(failed);
        Ok(report)
    })();
    let (report, success) = match result {
        Ok(report) => {
            let success = report["failed"] == false;
            (report, success)
        }
        Err(error) => (serde_json::json!({"error":error,"paid_calls":0}), false),
    };
    let sent = serde_json::to_vec(&report)
        .ok()
        .is_some_and(|bytes| crate::worker::send(&bytes).is_ok());
    if success && sent {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(fyrr_core::exit::FAILURE)
    }
}

#[cfg(test)]
#[path = "python/tests.rs"]
mod diagnostics_tests;
