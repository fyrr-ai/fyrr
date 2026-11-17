//! Test-only operation observation; shipping package graphs exclude this module.
#[path = "test_store/boundary.rs"]
pub mod boundary;
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
