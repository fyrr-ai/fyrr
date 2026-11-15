//! fyrr reader: read-only serving over published Lance versions.
//!
//! `fyrr reader serve | check | search` share the dense serving path.
//! This crate never writes to remote storage: readers use
//! read-only credentials and pin exact versions, never `latest`.

#![deny(
    unreachable_pub,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::semicolon_if_nothing_returned,
    clippy::ignored_unit_patterns,
    clippy::assigning_clones,
    clippy::format_push_string,
    clippy::items_after_statements
)]
// Test fixtures assert successful setup and make failures stop at the assertion.
#![cfg_attr(test, allow(clippy::unwrap_used))]

mod auth;
pub mod cli;
pub mod embedding;
mod error;
mod excerpt;
mod file_digest;
mod generation;
mod http;
mod mirror;
mod provider;
mod readiness;
mod rerank;
mod response_budget;
mod retrieval;
mod routing;
pub mod search;
mod serve;
pub mod storage;
mod sync;
mod telemetry;
mod tools;

pub use error::{IndexVerificationError, ReaderError};
