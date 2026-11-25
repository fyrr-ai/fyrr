//! The `fyrr reader` subcommands.

use crate::error::*;
use clap::Subcommand;
use fyrr_core::config::{LoadedConfig, Severity};
use fyrr_core::exit;
use fyrr_core::mcp::OutputMode;
use std::path::Path;
use std::process::ExitCode;

/// Read-only serving commands; present in both builds.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ReaderCommand {
    /// Serve MCP over stdio or Streamable HTTP.
    Serve,
    /// Run the startup path without writes or provider calls; exits nonzero
    /// when not ready.
    Check,
    /// Verify and prewarm the pinned immutable files without provider calls.
    Prewarm,
    /// Run one search and print the same JSON as MCP.
    Search {
        /// Query text; may start with `-`.
        #[arg(allow_hyphen_values = true)]
        query: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Text per hit.
        #[arg(long, value_enum, default_value_t = OutputMode::Excerpt)]
        output: OutputMode,
    },
}

/// Runs a reader command.
#[allow(clippy::print_stdout, clippy::print_stderr)]
pub fn run(command: &ReaderCommand, config: &Path) -> ExitCode {
    let loaded = match load(config) {
        Ok(loaded) => loaded,
        Err(code) => return code,
    };
    // The HTTP reader needs a way to authenticate callers; the file stays valid
    // for the writer without one (see `Config::http_authentication_missing`).
    if matches!(command, ReaderCommand::Serve) && loaded.config.http_authentication_missing() {
        eprintln!("error: configuration_invalid");
        return ExitCode::from(exit::CONFIG);
    }
    // No runtime, Lance call or worker thread exists yet.
    crate::fts_layout::apply(&loaded.config.reader);
    // SDK dispatch stays ordered on one reactor; native work uses the bounded workers.
    let runtime = if matches!(command, ReaderCommand::Serve) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    } else {
        tokio::runtime::Runtime::new()
    };
    let Ok(runtime) = runtime else {
        return ExitCode::from(exit::FAILURE);
    };
    finish(runtime.block_on(async {
        let mut reader = crate::search::PreparedReader::open(&loaded.config).await?;
        match command {
            ReaderCommand::Check => {
                println!("{}", reader.readiness());
                Ok(())
            }
            ReaderCommand::Prewarm => {
                reader.prewarm().await?;
                println!("{}", reader.readiness());
                Ok(())
            }
            ReaderCommand::Serve => crate::serve::serve(reader).await,
            ReaderCommand::Search {
                query,
                limit,
                output,
            } => {
                let args = serde_json::from_value(
                    serde_json::json!({"query": query, "limit": limit, "output": output}),
                )
                .map_err(|_| InvalidArgument)?;
                let output = reader.search(&args).await?;
                println!(
                    "{}",
                    serde_json::to_string(&output).map_err(|_| SerializationFailed)?
                );
                Ok(())
            }
        }
    }))
}

/// Loads and validates the configuration, printing only closed codes.
#[allow(clippy::print_stderr)]
pub fn load(config: &Path) -> Result<LoadedConfig, ExitCode> {
    let Ok(loaded) = fyrr_core::config::load(config) else {
        eprintln!("error: configuration_invalid");
        return Err(ExitCode::from(exit::CONFIG));
    };
    let issues = loaded.validate();
    if issues.iter().any(|i| i.severity == Severity::Error) {
        // Preserve named live-spend refusals without exposing arbitrary config
        // diagnostics, which may contain operator-provided values.
        let code = issues
            .iter()
            .find_map(|issue| {
                (issue.severity == Severity::Error
                    && matches!(
                        issue.message.as_str(),
                        "embedding_spend_limit_required" | "embedding_spend_configuration_invalid"
                    ))
                .then_some(issue.message.as_str())
            })
            .unwrap_or("configuration_invalid");
        eprintln!("error: {code}");
        return Err(ExitCode::from(exit::CONFIG));
    }
    Ok(loaded)
}

/// Maps a command result to its exit code; errors are closed codes.
#[allow(clippy::print_stderr)]
pub(crate) fn finish(result: Result<()>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            if let Some(hint) = crate::storage::code_hint(error.as_str()) {
                eprintln!("{hint}");
            }
            ExitCode::from(exit::FAILURE)
        }
    }
}
