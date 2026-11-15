//! The `fyrr` binary: `reader`, `writer` and `config` subcommands.
//!
//! Built twice: the full build (default feature `writer`) for the `fyrr`
//! image, and the reader-only build (`--no-default-features`) for the
//! `fyrr-reader` image, which links no writer code and answers `fyrr writer …`
//! by explaining that the writer is absent.
//!
//! Exit codes: 0 success, 1 failure, 2 usage error, 69 command unavailable in
//! this build or slice, 78 invalid configuration.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{CommandFactory, Parser, Subcommand};
use fyrr_core::config::{self, Issue, LoadedConfig, ProcessEnvironment, Severity};
use fyrr_core::exit;

#[cfg(feature = "writer")]
const BUILD: &str = "full";
#[cfg(not(feature = "writer"))]
const BUILD: &str = "reader-only";

const VERSION: &str = if cfg!(feature = "writer") {
    concat!(env!("CARGO_PKG_VERSION"), " (full build)")
} else {
    concat!(env!("CARGO_PKG_VERSION"), " (reader-only build)")
};

/// Authorization-aware retrieval for agents over MCP.
#[derive(Debug, Parser)]
#[command(name = "fyrr", version = VERSION)]
struct Cli {
    /// Configuration file.
    #[arg(long, global = true, env = "FYRR_CONFIG", default_value = "fyrr.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Read-only serving (the fyrr-reader image).
    #[command(subcommand)]
    Reader(fyrr_reader::cli::ReaderCommand),
    /// Sync, commit, maintenance and administration (full build only).
    #[cfg(feature = "writer")]
    #[command(subcommand)]
    Writer(fyrr_writer::cli::WriterCommand),
    /// Not part of this reader-only build; use the full `fyrr` image.
    #[cfg(not(feature = "writer"))]
    #[command(disable_help_flag = true)]
    Writer {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, hide = true)]
        args: Vec<String>,
    },
    /// Validate or show the configuration.
    #[command(subcommand)]
    Config(ConfigCommand),
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Validate the file, its cross-field rules and the referenced secrets.
    Check {
        /// Do not require the referenced environment variables to be set.
        #[arg(long)]
        no_env: bool,
    },
    /// Print every effective value with its origin (default, file or env).
    Show,
    /// Print the configuration JSON Schema; the full build joins the
    /// installed connector providers' settings and scope schemas.
    Schema,
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => return usage_error(&error),
    };
    match &cli.command {
        Command::Reader(command) => fyrr_reader::cli::run(command, &cli.config),
        #[cfg(feature = "writer")]
        Command::Writer(command) => fyrr_writer::cli::run(command, &cli.config),
        #[cfg(not(feature = "writer"))]
        Command::Writer { .. } => {
            eprintln!(
                "error: this is the reader-only fyrr build (the fyrr-reader image); it contains no writer.\n\
                 Run `fyrr writer …` with the full build (the fyrr image)."
            );
            ExitCode::from(exit::UNAVAILABLE)
        }
        Command::Config(ConfigCommand::Check { no_env }) => config_check(&cli.config, *no_env),
        Command::Config(ConfigCommand::Show) => config_show(&cli.config),
        Command::Config(ConfigCommand::Schema) => config_schema(),
    }
}

/// Prints a usage error without the user's arguments: clap's own messages
/// quote whatever was typed (a query, a secret), so the message is built from
/// the error kind and fyrr's definitions (argument names, possible values,
/// suggestions, usage) only.
fn usage_error(error: &clap::Error) -> ExitCode {
    use clap::error::{ContextKind, ErrorKind};
    if matches!(
        error.kind(),
        ErrorKind::DisplayHelp
            | ErrorKind::DisplayVersion
            | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
    ) {
        error.exit();
    }
    let mut message = format!("error: {}", error.kind());
    // These kinds name fyrr's own argument definition in InvalidArg; for
    // others (an unknown argument) it holds what the user typed.
    let names_definition = matches!(
        error.kind(),
        ErrorKind::InvalidValue
            | ErrorKind::ValueValidation
            | ErrorKind::MissingRequiredArgument
            | ErrorKind::ArgumentConflict
            | ErrorKind::TooManyValues
            | ErrorKind::TooFewValues
            | ErrorKind::WrongNumberOfValues
            | ErrorKind::NoEquals
    );
    if names_definition && let Some(argument) = error.get(ContextKind::InvalidArg) {
        message.push_str(&format!(" ('{argument}')"));
    }
    // Value parsers of fyrr report what they expect, never the value.
    if error.kind() == ErrorKind::ValueValidation
        && let Some(source) = std::error::Error::source(error)
    {
        message.push_str(&format!(": {source}"));
    }
    if let Some(valid) = error.get(ContextKind::ValidValue) {
        message.push_str(&format!("\n  [possible values: {valid}]"));
    }
    for kind in [ContextKind::SuggestedSubcommand, ContextKind::SuggestedArg] {
        if let Some(suggested) = error.get(kind) {
            message.push_str(&format!("\n  tip: a similar name exists: '{suggested}'"));
        }
    }
    let usage = error.get(ContextKind::Usage).map_or_else(
        || Cli::command().render_usage().to_string(),
        ToString::to_string,
    );
    eprintln!("{message}\n\n{usage}\n\nFor more information, try '--help'.");
    ExitCode::from(exit::USAGE)
}

fn load(path: &Path) -> Result<LoadedConfig, ExitCode> {
    config::load(path).map_err(|issue| {
        eprintln!("{issue}");
        ExitCode::from(exit::CONFIG)
    })
}

/// Prints issues to stderr; returns the number of errors.
fn report(issues: &[Issue]) -> usize {
    for issue in issues {
        eprintln!("{issue}");
    }
    issues
        .iter()
        .filter(|issue| issue.severity == Severity::Error)
        .count()
}

fn config_check(path: &Path, no_env: bool) -> ExitCode {
    let loaded = match load(path) {
        Ok(loaded) => loaded,
        Err(code) => return code,
    };
    let mut issues = loaded.validate();
    if !no_env {
        issues.extend(loaded.missing_secrets(&ProcessEnvironment));
    }
    // Sources against their installed connector providers.
    #[cfg(feature = "writer")]
    match fyrr_writer::check_providers(&loaded.config) {
        Ok(provider_issues) => issues.extend(provider_issues),
        Err(error) => {
            report(&issues);
            eprintln!("error: {error}");
            return ExitCode::from(exit::FAILURE);
        }
    }
    #[cfg(not(feature = "writer"))]
    if !loaded.config.sources.is_empty() {
        eprintln!(
            "note: sources are not checked against connector providers in the reader-only build"
        );
    }
    let errors = report(&issues);
    if errors > 0 {
        eprintln!(
            "config check failed: {errors} error(s) in {}",
            path.display()
        );
        return ExitCode::from(exit::CONFIG);
    }
    println!(
        "config OK: {} (namespace {}, {} source(s), {} warning(s))",
        path.display(),
        loaded.config.namespace.name,
        loaded.config.sources.len(),
        issues.len()
    );
    ExitCode::SUCCESS
}

/// Validates first, then prints every effective value with its origin. Values
/// the inline-secret check refuses are redacted, so a misplaced secret is never
/// printed even though the file is invalid.
fn config_show(path: &Path) -> ExitCode {
    let loaded = match load(path) {
        Ok(loaded) => loaded,
        Err(code) => return code,
    };
    let mut issues = loaded.validate();
    issues.extend(loaded.missing_secrets(&ProcessEnvironment));
    let mut errors = report(&issues);
    let mut values = loaded.effective_values(&ProcessEnvironment);
    errors += redact_provider_fields(&loaded, &mut values);
    let width = values
        .iter()
        .map(|value| value.path.len() + value.value.len() + 3)
        .max()
        .unwrap_or(0);
    println!(
        "# fyrr {} ({BUILD} build), config {}",
        env!("CARGO_PKG_VERSION"),
        path.display()
    );
    for value in &values {
        let assignment = format!("{} = {}", value.path, value.value);
        println!("{assignment:<width$}  # {}", value.origin.as_str());
    }
    if errors > 0 {
        eprintln!("config show: {errors} error(s) in {}", path.display());
        return ExitCode::from(exit::CONFIG);
    }
    ExitCode::SUCCESS
}

/// Source `settings` and `scope` values beyond the key heuristic: fields the
/// provider's schema marks secret, and, failing closed, every value of a
/// source whose provider schema is unknown (always in the reader-only build).
/// Environment references stay visible.
fn redact_provider_fields(loaded: &LoadedConfig, values: &mut [config::EffectiveValue]) -> usize {
    #[cfg(feature = "writer")]
    let (rules, issues) = fyrr_writer::config_show_redactions(&loaded.config);
    #[cfg(feature = "writer")]
    let errors = report(&issues);
    #[cfg(not(feature = "writer"))]
    let errors = 0;
    #[cfg(not(feature = "writer"))]
    let rules: Vec<(String, &str)> = (0..loaded.config.sources.len())
        .flat_map(|index| {
            ["settings", "scope"].map(|part| {
                (
                    format!("sources[{index}].{part}"),
                    "<redacted: no provider schema in the reader-only build>",
                )
            })
        })
        .collect();
    for value in values {
        let covered = rules.iter().find(|(prefix, _)| {
            value
                .path
                .strip_prefix(prefix.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with(['.', '[']))
                // Scalar arrays render at the aggregate path. Redact the
                // aggregate if any indexed child is marked secret.
                || prefix.strip_prefix(value.path.as_str())
                    .is_some_and(|rest| rest.starts_with(['.', '[']))
        });
        if let Some((_, redacted)) = covered
            && value.origin != config::Origin::Env
        {
            value.value = (*redacted).to_owned();
        }
    }
    errors
}

/// The configuration JSON Schema; the full build joins installed providers.
fn config_schema() -> ExitCode {
    #[cfg(feature = "writer")]
    let schema = match fyrr_writer::effective_config_schema() {
        Ok(schema) => schema,
        Err(error) => {
            eprintln!("error: {error}");
            return ExitCode::from(exit::FAILURE);
        }
    };
    #[cfg(not(feature = "writer"))]
    let schema = fyrr_core::schemas::config_schema();
    match serde_json::to_string_pretty(&schema) {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(_) => ExitCode::from(exit::FAILURE),
    }
}
