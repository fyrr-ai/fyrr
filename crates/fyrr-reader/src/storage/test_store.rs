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

/// A test-only setting of this process: the value of a `# fyrr-test-<name> =
/// <value>` line in its configuration. Like the store observer, it is a
/// comment so that it stays out of the configuration schema. A CLI process
/// names its configuration with `--config`; a fixture test launched by a
/// suite receives it in a `FYRR_TEST_*_CONFIG` variable.
fn setting(name: &str) -> Option<String> {
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
    let prefix = format!("# fyrr-test-{name} = ");
    named.into_iter().chain(fixtures).find_map(|path| {
        let text = std::fs::read_to_string(path).ok()?;
        text.lines()
            .find_map(|line| line.strip_prefix(&prefix).map(str::to_owned))
    })
}

/// Whether every authorization check reads the head
/// (`# fyrr-test-head-check = every`). Suites that assert on the request
/// right after a publication use it.
pub(crate) fn head_check_every() -> bool {
    static EVERY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *EVERY.get_or_init(|| setting("head-check").as_deref() == Some("every"))
}

/// When the reader checks for a new version, for suites that need the switch
/// at a known point (`# fyrr-test-version-check = `).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionCheck {
    /// `every`: each request starts a check and waits for it.
    Every,
    /// `held`: a check switches only when the old version may no longer be
    /// served; `SIGHUP` and `POST /admin/refresh` still switch.
    Held,
}

pub(crate) fn version_check() -> Option<VersionCheck> {
    static CHECK: std::sync::OnceLock<Option<VersionCheck>> = std::sync::OnceLock::new();
    *CHECK.get_or_init(|| match setting("version-check").as_deref() {
        Some("every") => Some(VersionCheck::Every),
        Some("held") => Some(VersionCheck::Held),
        Some(other) => panic!("unknown fyrr-test-version-check: {other}"),
        None => None,
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
