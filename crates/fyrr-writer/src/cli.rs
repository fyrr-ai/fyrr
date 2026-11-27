//! The `fyrr writer` subcommands, full build only.

use crate::{Result, WriterError};
use std::path::Path;
use std::process::ExitCode;

use clap::Subcommand;
use fyrr_core::config::{self, LoadedConfig};
use fyrr_core::connector::SyncKind;
use fyrr_core::exit;
use fyrr_core::units::ConfigDuration;

use crate::python;
use crate::sync::{self, SyncOptions};

/// Writing and administration commands.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum WriterCommand {
    /// Sync all configured sources, or the named ones, into prepared envelopes.
    Sync {
        /// Pull the configured canonical directory instead of connector sources.
        #[arg(long, conflicts_with_all = ["sources", "kinds", "full", "dry_run", "due", "jobs"])]
        directory: bool,
        /// Source IDs; all configured sources when omitted.
        sources: Vec<String>,
        /// Prepare envelopes for whole sources (bulk load).
        #[arg(long, conflicts_with = "dry_run")]
        full: bool,
        /// Run each source's provider in its own worker and validate its
        /// output against the connector contract without writing.
        /// Incompatible with --due.
        #[arg(long, conflicts_with = "due")]
        dry_run: bool,
        /// A sync kind to run (repeatable): content, permissions, members or
        /// reconcile. Default: every kind the provider implements and the
        /// source's connection type uses.
        #[arg(long = "kind", value_parser = parse_kind)]
        kinds: Vec<SyncKind>,
        /// Time limit per source; its worker is killed when it passes.
        #[arg(long, default_value = "10m")]
        timeout: ConfigDuration,
        /// Run only sync kinds whose configured interval is due.
        #[arg(long)]
        due: bool,
        /// Sources run at the same time.
        #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=256))]
        jobs: u16,
    },
    /// Publish pending envelopes. One commit process at a time.
    ///
    /// Returns the latest publication, which may be recovery-only with no envelope IDs.
    /// Look up an earlier publication's receipt with --receipt ENVELOPE_ID.
    Commit {
        /// Emit bounded writer-stage timing diagnostics to stderr.
        #[arg(long)]
        stage_timings: bool,
        /// Look up the original published receipt by envelope ID, including after recovery-only publications. Writes nothing.
        #[arg(long, conflicts_with = "stage_timings")]
        receipt: Option<String>,
    },
    /// Publish a complete hashed directory snapshot under the namespace lease.
    Directory {
        #[arg(long)]
        snapshot: std::path::PathBuf,
    },
    /// Publish the configured API keys of the deployment under the storage
    /// root, for the readers of every namespace. Replaces the published set.
    Keys,
    /// Persist a credential from an environment variable, never a CLI value.
    Credential {
        #[arg(long)]
        name: String,
        #[arg(long)]
        from_env: String,
    },
    /// Run maintenance in bounded, lease-held steps.
    Maintain,
    /// Schedule due source workers and publish on the coalescing timer (60s by default).
    Serve {
        /// Maximum simultaneously running source workers.
        #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=256))]
        jobs: u16,
        /// Time limit per isolated source worker.
        #[arg(long, default_value = "10m")]
        timeout: ConfigDuration,
    },
    /// Show publications, envelopes, quarantine, backpressure and the last permission and member read per source.
    /// Counts are relative to the printed, pinned content_sequence and exact in
    /// a quiescent namespace. Concurrent commits and arrivals can overcount pending work.
    Status,
    /// Publish a retained version of a write API namespace again.
    Restore {
        /// Retained publication ID to restore through a new fenced Lance commit.
        publication: String,
    },
    /// Restore the preceding retained publication of a write API namespace.
    Rollback,
    /// Verify the full published content on demand.
    Verify,
    /// Create or delete the namespace.
    Namespace {
        #[command(subcommand)]
        action: NamespaceAction,
    },
    /// Retire or purge a source.
    Source {
        #[command(subcommand)]
        action: SourceAction,
    },
    /// List installed connector providers and check SDK compatibility; exits
    /// 1 when any installed provider is refused.
    Providers,
    /// Internal: the worker process that runs one source's Python provider host.
    #[command(name = "__worker", hide = true, subcommand)]
    Worker(WorkerJob),
}

/// Namespace administration.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum NamespaceAction {
    /// Create the namespace.
    Create,
    /// Freeze the namespace (`true`) or thaw it (`false`): API writes and
    /// connector syncs are refused while it stays readable, and envelopes that
    /// were prepared before the switch are still committed. Maintenance and
    /// the switch itself still work. Readers older than this switch refuse the
    /// policy of a frozen namespace, so upgrade them first.
    ReadOnly {
        #[arg(action = clap::ArgAction::Set)]
        enabled: bool,
    },
    /// Delete the namespace and every object of it after a dry-run plan, with
    /// a verified export or the namespace name repeated. Stop every writer of
    /// the namespace first: the command waits `writer.commit.lease_ttl`, twice.
    /// Noncurrent versions on a versioned S3 bucket remain.
    Delete {
        /// Verified namespace export directory; checked against storage before
        /// the deletion. Without it and without `--delete-data`, the dry-run
        /// plan is printed and nothing is deleted.
        #[arg(long)]
        export: Option<std::path::PathBuf>,
        /// The namespace name, repeated: deletes without an export.
        #[arg(long)]
        delete_data: Option<String>,
    },
    /// Copy the namespace, as the version it serves now, into the `[storage]`
    /// of another configuration file, under the same name. The copy is
    /// independent and writable; the source is only read. That file's
    /// `identity.hashing_key` must be the source's. A copy that stopped is
    /// completed by running the command again.
    Copy {
        /// A configuration file of the destination deployment: its `[storage]`
        /// and `identity.hashing_key` are used, nothing else.
        #[arg(long)]
        to_config: std::path::PathBuf,
    },
    /// List the namespaces of the storage root, in name order: a name is listed
    /// while any object exists under it, so a namespace whose deletion has not
    /// finished is listed.
    List {
        /// Only the namespaces whose name starts with this prefix.
        #[arg(long, value_parser = namespace_prefix)]
        prefix: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum SourceAction {
    /// Deny the source to every reader at once and reject its further
    /// commits; final.
    Retire {
        /// Source ID.
        source: String,
    },
    /// Retire the source and delete its rows, envelopes and cursors.
    Purge {
        /// Source ID.
        source: String,
        /// Delete; omit to print the dry-run plan.
        #[arg(long)]
        confirm: bool,
    },
}

/// What a worker process does.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum WorkerJob {
    /// Enumerate installed entry points as JSON without executing metadata.
    Discover,
    /// Internal: execute only one provider metadata callable.
    Metadata {
        #[arg(long)]
        provider: String,
    },
    /// Run sync kinds of one source and send a structured report.
    Sync {
        #[arg(long)]
        source: String,
        #[arg(long = "kind", value_parser = parse_kind)]
        kinds: Vec<SyncKind>,
        #[arg(long)]
        prepare: bool,
        #[arg(long)]
        full: bool,
        #[arg(long)]
        due: bool,
    },
}

fn namespace_prefix(text: &str) -> Result<String, String> {
    fyrr_core::config::is_namespace_prefix(text)
        .then(|| text.to_owned())
        .ok_or_else(|| "expected 1 to 63 lowercase letters, digits or `-`".to_owned())
}

fn parse_kind(text: &str) -> Result<SyncKind, String> {
    [
        SyncKind::Content,
        SyncKind::Permissions,
        SyncKind::Members,
        SyncKind::Reconcile,
    ]
    .into_iter()
    .find(|kind| kind.as_str() == text)
    .ok_or_else(|| "expected content, permissions, members or reconcile".to_owned())
}

/// Runs a writer command; `config` is the configuration file path.
#[allow(clippy::print_stdout, clippy::print_stderr)]
pub fn run(command: &WriterCommand, path: &Path) -> ExitCode {
    match command {
        WriterCommand::Providers => return providers(),
        WriterCommand::Worker(job) => return worker(job, path),
        _ => {}
    }
    // Creating or deleting a namespace starts from the file's own settings.
    // A connector sync only starts workers, which take the settings themselves,
    // and a commit takes them from the policy it reads at each tick.
    let existing = !matches!(
        command,
        WriterCommand::Namespace { .. }
            | WriterCommand::Sync {
                directory: false,
                ..
            }
            | WriterCommand::Commit { receipt: None, .. }
            | WriterCommand::Serve { .. }
            | WriterCommand::Keys
    );
    with_config(path, existing, |loaded| {
        let config = &loaded.config;
        match command {
            WriterCommand::Providers | WriterCommand::Worker(_) => {
                unreachable!("handled before config")
            }
            WriterCommand::Sync {
                sources,
                full,
                dry_run,
                kinds,
                timeout,
                jobs,
                due,
                directory,
            } => {
                if *directory {
                    return runtime_command(crate::directory_pull::pull(
                        config,
                        timeout.as_duration(),
                    ));
                }
                let options = SyncOptions {
                    config_path: path,
                    sources,
                    kinds,
                    timeout: timeout.as_duration(),
                    jobs: usize::from(*jobs),
                    memory: config.writer.worker_memory_max_bytes.as_u64(),
                };
                if *dry_run {
                    sync::dry_run(loaded, &options)
                } else {
                    sync::prepared(loaded, &options, *full, *due)
                }
            }
            WriterCommand::Directory { snapshot } => runtime_command(async {
                let file = std::fs::File::open(snapshot)
                    .map_err(|_| WriterError::DirectorySnapshotUnavailable)?;
                let mut bytes = Vec::new();
                std::io::Read::read_to_end(
                    &mut std::io::Read::take(file, 16 * 1024 * 1024 + 1),
                    &mut bytes,
                )
                .map_err(|_| WriterError::DirectorySnapshotUnavailable)?;
                if bytes.len() > 16 * 1024 * 1024 {
                    return Err(WriterError::DirectorySnapshotInvalid);
                }
                let snapshot = fyrr_core::parse_contract(&bytes)
                    .map_err(|_| WriterError::DirectorySnapshotInvalid)?;
                crate::directory::publish(config, &snapshot).await
            }),
            WriterCommand::Keys => runtime_command(crate::keys::publish(config)),
            WriterCommand::Credential { name, from_env } => {
                runtime_command(crate::secrets::credential_put(config, name, from_env))
            }
            WriterCommand::Commit {
                stage_timings,
                receipt,
            } => runtime_command(async {
                let result = if let Some(id) = receipt {
                    crate::commit::receipt(config, id).await
                } else {
                    crate::commit::commit_until_stopped(config, *stage_timings, None)
                        .await
                        .map(|(receipt, _)| receipt)
                };
                result.map(|receipt| serde_json::json!(receipt))
            }),
            WriterCommand::Namespace {
                action: NamespaceAction::Create,
            } => runtime_command(async {
                crate::commit::create(config)
                    .await
                    .map(|()| serde_json::json!({"created":true}))
            }),
            WriterCommand::Namespace {
                action: NamespaceAction::ReadOnly { enabled },
            } => runtime_command(async {
                crate::commit::set_read_only(config, *enabled)
                    .await
                    .map(|changed| serde_json::json!({"read_only":enabled,"changed":changed}))
            }),
            WriterCommand::Namespace {
                action:
                    NamespaceAction::Delete {
                        export,
                        delete_data,
                    },
            } => runtime_command(crate::commit::delete(
                config,
                export.as_deref(),
                delete_data.as_deref(),
            )),
            WriterCommand::Namespace {
                action: NamespaceAction::Copy { to_config },
            } => {
                let to = match config::load(to_config) {
                    Ok(loaded) => {
                        let issues = loaded.validate();
                        for issue in &issues {
                            eprintln!("{issue}");
                        }
                        if issues.iter().any(|i| i.severity == config::Severity::Error) {
                            return ExitCode::from(exit::CONFIG);
                        }
                        loaded.config
                    }
                    Err(issue) => {
                        eprintln!("{issue}");
                        return ExitCode::from(exit::CONFIG);
                    }
                };
                runtime_command(crate::commit::copy(config, &to))
            }
            WriterCommand::Namespace {
                action: NamespaceAction::List { prefix },
            } => runtime_command(async {
                let (store, root) = fyrr_reader::storage::namespace_store(config)?;
                let root = fyrr_reader::storage::deployment_root(&root);
                let prefixes: Vec<&str> = prefix.as_deref().into_iter().collect();
                fyrr_reader::storage::list_namespaces(store.as_ref(), &root, &prefixes)
                    .await
                    .map(|namespaces| serde_json::json!({"namespaces":namespaces}))
                    .map_err(Into::into)
            }),
            WriterCommand::Maintain => runtime_command(crate::maintenance::maintain(config)),
            WriterCommand::Serve { jobs, timeout } => {
                // Source jobs need the providers' Python: say so once, up
                // front, from an inventory worker (this process runs none).
                if !config.sources.is_empty()
                    && let Err(error) =
                        sync::inventory(config.writer.worker_memory_max_bytes.as_u64())
                {
                    eprintln!("error: {error}");
                    return ExitCode::from(exit::FAILURE);
                }
                runtime_command(crate::serve::run(
                    loaded,
                    path,
                    usize::from(*jobs),
                    timeout.as_duration(),
                ))
            }
            WriterCommand::Status => runtime_command(crate::envelopes::status(config)),
            WriterCommand::Restore { publication } => {
                runtime_command(crate::maintenance::restore(config, Some(publication)))
            }
            WriterCommand::Rollback => runtime_command(crate::maintenance::restore(config, None)),
            WriterCommand::Source {
                action: SourceAction::Retire { source },
            } => runtime_command(crate::source::retire(config, source)),
            WriterCommand::Source {
                action: SourceAction::Purge { source, confirm },
            } => runtime_command(crate::source::purge(config, source, *confirm)),
            WriterCommand::Verify => runtime_command(crate::maintenance::verify(config)),
        }
    })
}

/// The closed error code, and what to do about it when it needs more than its name.
#[allow(clippy::print_stderr)]
fn report_error(error: &WriterError) {
    eprintln!("error: {error}");
    if let Some(hint) = fyrr_reader::storage::code_hint(error.as_str()) {
        eprintln!("{hint}");
    }
}

#[allow(clippy::print_stdout, clippy::print_stderr)]
fn runtime_command(
    future: impl std::future::Future<Output = Result<serde_json::Value>>,
) -> ExitCode {
    let Ok(runtime) = tokio::runtime::Runtime::new() else {
        return ExitCode::from(exit::FAILURE);
    };
    match runtime.block_on(future) {
        Ok(report) => {
            println!("{report}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            report_error(&error);
            ExitCode::from(if error == WriterError::CommitterBusy {
                exit::BUSY
            } else {
                exit::FAILURE
            })
        }
    }
}

/// Loads and validates the configuration. With `existing`, the settings the
/// namespace was created with replace the file's: the file supplies them only
/// when a namespace is created.
#[allow(clippy::print_stderr)]
fn with_config(
    path: &Path,
    existing: bool,
    body: impl FnOnce(&LoadedConfig) -> ExitCode,
) -> ExitCode {
    match config::load(path) {
        Ok(mut loaded) => {
            let issues = loaded.validate();
            for issue in &issues {
                eprintln!("{issue}");
            }
            if issues.iter().any(|i| i.severity == config::Severity::Error) {
                ExitCode::from(exit::CONFIG)
            } else {
                // No runtime, Lance call or worker thread exists yet.
                crate::threads::apply(&loaded.config.writer);
                if existing && let Err(error) = take_namespace_settings(&mut loaded) {
                    report_error(&error);
                    return ExitCode::from(exit::FAILURE);
                }
                body(&loaded)
            }
        }
        Err(issue) => {
            eprintln!("{issue}");
            ExitCode::from(exit::CONFIG)
        }
    }
}

fn take_namespace_settings(loaded: &mut LoadedConfig) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new().map_err(|_| WriterError::RuntimeUnavailable)?;
    loaded.config = runtime
        .block_on(fyrr_reader::storage::with_namespace_settings(
            &loaded.config,
        ))?
        // The namespace has a reranker and the file has no `[namespace.rerank]`.
        .ok_or(WriterError::ConfigurationInvalid)?;
    Ok(())
}

#[allow(clippy::print_stdout, clippy::print_stderr)]
fn providers() -> ExitCode {
    match sync::discover(crate::worker::DEFAULT_MEMORY) {
        Ok(discovery) => {
            print!("{}", discovery.render());
            if discovery.refused() > 0 {
                ExitCode::from(exit::FAILURE)
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(exit::FAILURE)
        }
    }
}

#[allow(clippy::print_stdout, clippy::print_stderr)]
fn worker(job: &WorkerJob, config: &Path) -> ExitCode {
    if crate::worker::initialize().is_err() {
        return ExitCode::from(exit::FAILURE);
    }
    match job {
        WorkerJob::Discover | WorkerJob::Metadata { .. } => {
            // The host is started here, under the worker's limits; a host that
            // cannot start is reported as a closed code, which the process
            // that prints turns into one message.
            let mut host = match python::start_host() {
                Ok(host) => host,
                Err(error) => {
                    let _ = crate::worker::send(&python::host_error_result(&error));
                    return ExitCode::from(exit::FAILURE);
                }
            };
            let result = match job {
                WorkerJob::Metadata { provider } => python::discover_provider(&mut host, provider),
                _ => python::inventory(&mut host),
            };
            match result {
                Ok(json) if crate::worker::send(json.as_bytes()).is_ok() => ExitCode::SUCCESS,
                _ => ExitCode::from(exit::FAILURE),
            }
        }
        WorkerJob::Sync {
            source,
            kinds,
            prepare,
            full,
            due,
        } => with_config(config, false, |loaded| {
            let Ok(credentials) = crate::secrets::worker_credentials(&loaded.config, source) else {
                return ExitCode::from(exit::FAILURE);
            };
            if *prepare {
                crate::python::prepared_sync(loaded, source, kinds, *full, *due, &credentials)
            } else {
                sync::worker_sync(loaded, source, kinds, &credentials)
            }
        }),
    }
}
