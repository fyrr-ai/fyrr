//! The connector providers, run by the Python provider host: a child process
//! of a worker (see [`crate::worker`]), never of the process that reads the
//! configuration or orchestrates sources, and never the `fyrr` executable
//! itself, which contains no Python.
//!
//! [`interpreter`] finds the Python to run: `$VIRTUAL_ENV/bin/python`, a
//! `python3` next to the `fyrr` executable, then `python3` on `PATH`. The
//! SDK and the providers come from that interpreter's environment.
//!
//! The worker and the host talk over an inherited socket ([`wire`] frames,
//! [`host`] client). Content rows cross as Arrow IPC streams, which the
//! worker bounds before it allocates and validates when it decodes them;
//! everything printed is value-free: provider failures are closed
//! diagnostics.

use crate::{Result, WriterError};
use fyrr_core::connector::SyncKind;

use crate::batch::BatchChecker;
use crate::secrets::Credentials;
use crate::sync::{ReportDefaults, default_report};
use crate::worker::Diagnostic;
use host::Host;

pub(crate) use interpreter::preflight;

/// Starts a provider host and runs `body` with it.
pub(crate) fn with_host<R>(
    credentials: &Credentials,
    body: impl FnOnce(&mut Host) -> Result<R, String>,
) -> Result<R, String> {
    body(&mut Host::start(credentials)?)
}

/// Runs one sync kind of one source in a host of its own and checks every
/// batch at the boundary.
pub(crate) fn run_kind(
    credentials: &Credentials,
    source_json: &str,
    kind: SyncKind,
    checker: &mut BatchChecker,
) -> Result<(), Diagnostic> {
    let mut host = Host::start(credentials).map_err(|_| Diagnostic::ProviderFailure)?;
    run_kind_in(&mut host, source_json, kind, checker)
}

fn run_kind_in(
    host: &mut Host,
    source_json: &str,
    kind: SyncKind,
    checker: &mut BatchChecker,
) -> Result<(), Diagnostic> {
    let request = driver::Request {
        source_json,
        kind,
        cursor: None,
    };
    let run = driver::drive_provider(
        host,
        &request,
        None,
        checker,
        driver::RowPolicy::Optional,
        driver::ErrMap::DRY_RUN,
        |_, _, _| Ok(()),
    )?;
    run.finish();
    Ok(())
}

/// Enumerates entry points without importing or executing providers.
pub(crate) fn inventory(host: &mut Host) -> Result<String, String> {
    host.document(&serde_json::json!({"t": "inventory"}))
}

/// Loads metadata for exactly one entry point.
pub(crate) fn discover_provider(host: &mut Host, name: &str) -> Result<String, String> {
    host.document(&serde_json::json!({"t": "discover", "name": name}))
}

mod driver;
mod host;
mod interpreter;
mod prepared;
mod wire;
use prepared::prepared_kind;

/// One process loads only its own provider and runs independent leased kinds.
pub fn prepared_sync(
    loaded: &fyrr_core::config::LoadedConfig,
    source_id: &str,
    requested: &[fyrr_core::connector::SyncKind],
    full: bool,
    due: bool,
    credentials: &Credentials,
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
        let (kinds, permission_stream) = with_host(credentials, |host| {
            let discovery = crate::providers::parse_discovery(
                discover_provider(host, &source.provider)?.as_bytes(),
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
        let mut evidence = prepared::CommandEvidence::default();
        let mut failed = false;
        for kind in kinds {
            match prepared_kind(
                loaded,
                source_id,
                kind,
                full,
                due || (requested.is_empty() && kind == SyncKind::Reconcile),
                permission_stream,
                &mut evidence,
                credentials,
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
