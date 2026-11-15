//! `fyrr-eval`: internal quality measurement over the reader's search path.
//!
//! Not part of any release. It reads only the reader's public API; usage and
//! input errors print a closed code, never the arguments or inputs.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod frozen;
mod stub;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::error::ErrorKind;
use clap::{Parser, Subcommand};
use fyrr_core::config::Config;
use fyrr_core::exit;
use fyrr_core::mcp::{OutputMode, SearchInput};
use fyrr_reader::ReaderError;
use fyrr_reader::search::PreparedReader;
use serde_json::{Value, json};

#[derive(Debug, Parser)]
#[command(name = "fyrr-eval", version)]
struct Cli {
    /// Configuration file.
    #[arg(long, global = true, env = "FYRR_CONFIG", default_value = "fyrr.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a batch of searches and print one MCP-shaped JSON line per search.
    Search {
        /// JSON array of `{"query": "...", "filters": {...}}` requests.
        #[arg(long)]
        queries: PathBuf,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Text per hit.
        #[arg(long, value_enum, default_value_t = OutputMode::Excerpt)]
        output: OutputMode,
        /// Write every search's stage keys to this new file under
        /// `FYRR_EVAL_ROOT`, outside this checkout.
        #[arg(long)]
        stages: Option<PathBuf>,
    },
    /// Serve frozen vectors as the embedding provider over loopback HTTP.
    ServeEmbeddings {
        #[command(flatten)]
        frozen: frozen::FrozenInput,
        #[arg(long, default_value = "127.0.0.1:0")]
        listen: std::net::SocketAddr,
    },
}

/// Closed error codes.
#[derive(Debug)]
enum Error {
    InvalidArgument,
    InputInvalid,
    ListenerUnavailable,
    Reader(ReaderError),
}
impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArgument => formatter.write_str("invalid_argument"),
            Self::InputInvalid => formatter.write_str("evaluation_input_invalid"),
            Self::ListenerUnavailable => formatter.write_str("evaluation_listener_unavailable"),
            Self::Reader(error) => error.fmt(formatter),
        }
    }
}
impl From<ReaderError> for Error {
    fn from(error: ReaderError) -> Self {
        Self::Reader(error)
    }
}
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp
                    | ErrorKind::DisplayVersion
                    | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            ) =>
        {
            error.exit()
        }
        Err(_) => {
            eprintln!("error: invalid_argument");
            return ExitCode::from(exit::USAGE);
        }
    };
    let loaded = match fyrr_reader::cli::load(&cli.config) {
        Ok(loaded) => loaded,
        Err(code) => return code,
    };
    let result = match &cli.command {
        Command::ServeEmbeddings { frozen, listen } => serve(&loaded.config, frozen, *listen),
        Command::Search {
            queries,
            limit,
            output,
            stages,
        } => search(&loaded.config, queries, *limit, *output, stages.as_deref()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(exit::FAILURE)
        }
    }
}

fn serve(
    config: &Config,
    frozen: &frozen::FrozenInput,
    listen: std::net::SocketAddr,
) -> Result<()> {
    if !frozen.configured() {
        return Err(Error::InvalidArgument);
    }
    let vectors = frozen.open(&config.embedding)?;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| Error::ListenerUnavailable)?;
    runtime.block_on(stub::serve(config.embedding.clone(), vectors, listen))
}

fn search(
    config: &Config,
    queries: &Path,
    limit: usize,
    mode: OutputMode,
    stages: Option<&Path>,
) -> Result<()> {
    if stages.is_some_and(|path| !stage_path_valid(path)) {
        return Err(Error::InputInvalid);
    }
    let requests = requests(queries, limit, mode)?;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| Error::InvalidArgument)?;
    let traces = runtime.block_on(async {
        let mut reader = PreparedReader::open(config).await?;
        let mut traces = Vec::new();
        for request in &requests {
            if stages.is_some() {
                reader.record_stages();
            }
            let output = reader.search(request).await?;
            if stages.is_some() {
                traces.push(reader.take_stages().unwrap_or_default());
            }
            println!(
                "{}",
                serde_json::to_string(&output)
                    .map_err(|_| Error::Reader(ReaderError::SerializationFailed))?
            );
        }
        Ok::<_, Error>(traces)
    })?;
    if let Some(path) = stages {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|_| Error::InputInvalid)?;
        let bytes = serde_json::to_vec(&traces).map_err(|_| Error::InputInvalid)?;
        file.write_all(&bytes).map_err(|_| Error::InputInvalid)?;
    }
    Ok(())
}

/// Parses the whole batch before any search runs.
fn requests(path: &Path, limit: usize, mode: OutputMode) -> Result<Vec<SearchInput>> {
    let bytes = std::fs::read(path).map_err(|_| Error::InputInvalid)?;
    let requests: Vec<Value> = serde_json::from_slice(&bytes).map_err(|_| Error::InputInvalid)?;
    requests
        .into_iter()
        .map(|request| {
            let query = request
                .get("query")
                .and_then(Value::as_str)
                .ok_or(Error::InputInvalid)?;
            let mut args = match request.get("filters") {
                None | Some(Value::Null) => json!({}),
                Some(filters @ Value::Object(_)) => filters.clone(),
                Some(_) => return Err(Error::InputInvalid),
            };
            args["query"] = json!(query);
            args["limit"] = json!(limit);
            args["output"] = json!(mode);
            serde_json::from_value(args).map_err(|_| Error::InputInvalid)
        })
        .collect()
}

/// Stage files may only be written as a new file under `FYRR_EVAL_ROOT`,
/// and neither the root nor the file may lie inside this checkout.
fn stage_path_valid(path: &Path) -> bool {
    let root = std::env::var_os("FYRR_EVAL_ROOT").and_then(|root| std::fs::canonicalize(root).ok());
    let parent = path
        .parent()
        .and_then(|parent| std::fs::canonicalize(parent).ok());
    let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).parent();
    match (root, parent, checkout) {
        (Some(root), Some(parent), Some(checkout)) => {
            !root.starts_with(checkout)
                && !parent.starts_with(checkout)
                && parent.starts_with(root)
                && !path.exists()
        }
        _ => false,
    }
}
