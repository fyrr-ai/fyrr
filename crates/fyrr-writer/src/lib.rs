//! fyrr writer: sync, commit, maintenance and administration.
//!
//! Linked only into the full build (cargo feature `writer` of the `fyrr`
//! binary); the reader-only build never depends on this crate, Python or
//! PyO3, and CI proves it. Each source's connector provider runs in an
//! embedded interpreter in its own worker process; its Arrow output is
//! validated at the boundary and written as envelopes that the namespace's
//! single committer publishes.

#![cfg_attr(
    not(test),
    warn(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::cast_possible_truncation,
        unreachable_pub,
        clippy::manual_let_else,
        clippy::map_unwrap_or,
        clippy::semicolon_if_nothing_returned,
        clippy::ignored_unit_patterns,
        clippy::assigning_clones,
        clippy::format_push_string,
        clippy::items_after_statements,
    )
)]

mod error;
mod io;
#[cfg(test)]
mod io_probe;
pub use error::{Result, WriterError};

pub mod apply;
pub mod batch;
pub mod chunk;
pub mod cli;
pub mod commit;
pub mod directory;
mod embedding;
pub mod envelopes;
pub mod lease;
pub mod maintenance;
pub mod membership;
pub mod prepare;
pub mod providers;
pub mod python;
mod scratch;
pub mod serve;
pub mod source;
pub mod sync;
pub mod sync_lease;
mod telemetry;
mod threads;
pub mod worker;

use fyrr_core::config::{Config, Issue};

/// Provider issues of every source (`fyrr config check`); discovers the
/// installed providers in a worker process. No sources, no Python.
pub fn check_providers(config: &Config) -> Result<Vec<Issue>, String> {
    if config.sources.is_empty() {
        return Ok(Vec::new());
    }
    let discovery = sync::discover(config.writer.worker_memory_max_bytes.as_u64())?;
    Ok(providers::check_sources(config, &discovery))
}

/// What `fyrr config show` redacts beyond the key heuristic, as (path
/// prefix, replacement): fields a source's provider marks secret, and every
/// `settings`/`scope` value of a source whose provider is unknown, refused or
/// undiscoverable (fail closed).
#[allow(clippy::print_stderr)]
pub fn config_show_redactions(config: &Config) -> (Vec<(String, &'static str)>, Vec<Issue>) {
    if config.sources.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let discovery = sync::discover(config.writer.worker_memory_max_bytes.as_u64())
        .inspect_err(|error| {
            eprintln!("note: {error}; every source's settings and scope are redacted");
        })
        .ok();
    let issues = discovery.as_ref().map_or_else(
        || vec![Issue::error("sources", "provider discovery failed")],
        |d| providers::check_sources(config, d),
    );
    (
        providers::show_redactions(config, discovery.as_ref()),
        issues,
    )
}

/// The configuration schema with the installed providers joined in.
pub fn effective_config_schema() -> Result<serde_json::Value, String> {
    let mut schema = fyrr_core::schemas::config_schema();
    providers::extend_config_schema(&mut schema, &sync::discover(worker::DEFAULT_MEMORY)?);
    Ok(schema)
}

pub mod secrets;

pub mod directory_pull;

mod admin;
mod scim;

/// Callback failure state cannot be recovered after a panic while updating it.
#[allow(clippy::expect_used)]
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().expect("callback failure lock")
}

// Native IDs and wire positions retain their established low-bit encodings.
#[allow(clippy::cast_possible_truncation)]
fn narrow_u32(value: u64) -> u32 {
    value as u32
}
#[allow(clippy::cast_possible_truncation)]
fn narrow_usize(value: u64) -> usize {
    value as usize
}

#[cfg(test)]
mod narrowing_tests {
    #[test]
    fn narrowing_preserves_existing_low_bits() {
        for (value, low) in [
            (0, 0),
            (u64::from(u32::MAX), u32::MAX),
            (1u64 << 32, 0),
            (u64::MAX, u32::MAX),
        ] {
            assert_eq!(super::narrow_u32(value), low);
            assert_eq!(super::narrow_usize(value), value as usize);
        }
    }
}
