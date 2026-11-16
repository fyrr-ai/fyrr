//! Test-only operation observation; shipping package graphs exclude this module.
#[path = "test_store/boundary.rs"]
pub mod boundary;
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
