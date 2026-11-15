//! Storage-independent fyrr contracts, configuration and MCP tool schemas.
//! Nothing here touches S3 or Lance.

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

pub mod config;
pub mod connector;
pub mod directory;
pub mod envelope;
pub mod identity;
pub mod limits;
pub mod mcp;
pub mod provider;
pub mod publication;
pub mod schemas;
pub mod units;
pub mod version;

pub use version::{Contract, ContractError, SchemaVersion, parse_contract, write_contract};

/// Process exit codes of the `fyrr` binary.
pub mod exit {
    /// A command or check failed (also: partial failure of a sync).
    pub const FAILURE: u8 = 1;
    /// Usage error (clap uses the same code).
    pub const USAGE: u8 = 2;
    /// The command is unavailable in this build or slice.
    pub const UNAVAILABLE: u8 = 69;
    /// A namespace committer already holds the lease; retry later.
    pub const BUSY: u8 = 75;
    /// The configuration is invalid.
    pub const CONFIG: u8 = 78;
}

pub mod secret;

pub mod layout;
