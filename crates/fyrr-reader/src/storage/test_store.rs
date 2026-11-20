//! Test-only operation observation; shipping package graphs exclude this module.
#[path = "test_store/boundary.rs"]
pub mod boundary;
#[path = "test_store/latency.rs"]
pub mod latency;
#[path = "test_store/sync.rs"]
pub(crate) mod sync;
use lance_io::object_store::{ChainedWrappingObjectStore, WrappingObjectStore};
use std::sync::Arc;

pub(super) fn native_wrapper(
    mirror: Option<Arc<dyn WrappingObjectStore>>,
) -> Option<Arc<dyn WrappingObjectStore>> {
    if boundary::Client::current().is_none() {
        return mirror;
    }
    // The mirror stays outermost: observe backing I/O without bypassing cache hits.
    let mut wrappers: Vec<Arc<dyn WrappingObjectStore>> = vec![Arc::new(boundary::Native)];
    wrappers.extend(mirror);
    Some(Arc::new(ChainedWrappingObjectStore::new(wrappers)))
}

/// Whether the configuration of this process asks for a head read at every
/// authorization check (`# fyrr-test-head-check = every`). Suites that assert
/// on the request right after a publication use it; like the store observer,
/// it is a comment so that it stays out of the configuration schema. A CLI
/// process names its configuration with `--config`; a fixture test launched
/// by a suite receives it in a `FYRR_TEST_*_CONFIG` variable.
pub(crate) fn head_check_every() -> bool {
    static EVERY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *EVERY.get_or_init(|| {
        let mut arguments = std::env::args_os();
        let named = arguments
            .find(|arg| arg == "--config")
            .and_then(|_| arguments.next());
        let fixtures = std::env::vars_os()
            .filter(|(name, _)| {
                let name = name.to_string_lossy();
                name.starts_with("FYRR_TEST_") && name.ends_with("_CONFIG")
            })
            .map(|(_, path)| path);
        named.into_iter().chain(fixtures).any(|path| {
            std::fs::read_to_string(path).is_ok_and(|text| {
                text.lines()
                    .any(|line| line == "# fyrr-test-head-check = every")
            })
        })
    })
}

/// Seed new control objects in an inert fixture, before starting any CLI.
/// Each object has a genuine private file, tag xattr and contents.
pub fn seed_local_fixture(
    config: &fyrr_core::config::Config,
    entries: Vec<(object_store::path::Path, Vec<u8>)>,
) -> std::io::Result<()> {
    let Some(fyrr_core::config::StorageBackend::Local { root }) = config.storage.backend() else {
        return Err(std::io::Error::other(
            "fixture seeding requires local storage",
        ));
    };
    super::local::fixture::seed(&root, config.namespace.name.as_str(), entries)
}
