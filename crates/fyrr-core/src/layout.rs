//! Names shared by published namespaces and their native reader paths.

/// Paths within an already validated namespace; this type performs no I/O.
#[derive(Debug, Clone, Copy)]
pub struct NamespaceLayout<'a> {
    storage_uri: &'a str,
    namespace: &'a str,
}
impl<'a> NamespaceLayout<'a> {
    pub const DATASET_NAME: &'static str = "dataset.lance";
    pub const VECTOR_INDEX: &'static str = "vector_idx";
    pub const FTS_INDEX: &'static str = "fts_idx";
    /// Version manifests, named by version number rather than a unique ID.
    pub const VERSIONS: &'static str = "_versions/";
    const IMMUTABLE_PREFIXES: &'static [&'static str] = &[
        "data/",
        "_indices/",
        "_deletions/",
        Self::VERSIONS,
        "_transactions/",
    ];

    /// Borrows the configured storage URI and namespace name.
    pub const fn new(storage_uri: &'a str, namespace: &'a str) -> Self {
        Self {
            storage_uri,
            namespace,
        }
    }
    /// Dataset URI beneath the namespace root.
    pub fn dataset_uri(&self) -> String {
        format!(
            "{}/namespaces/{}/{}",
            self.storage_uri.trim_end_matches('/'),
            self.namespace,
            Self::DATASET_NAME
        )
    }
    /// Classifies Lance-relative paths for the immutable file cache.
    pub fn immutable_relative_path(relative: &str) -> bool {
        Self::IMMUTABLE_PREFIXES
            .iter()
            .any(|prefix| relative.starts_with(prefix))
    }
    /// Recognizes the same immutable files in a native object-store path.
    pub fn immutable_native_path(path: &str) -> bool {
        path.match_indices(Self::DATASET_NAME).any(|(start, _)| {
            start > 0
                && path.as_bytes()[start - 1] == b'/'
                && path[start + Self::DATASET_NAME.len()..]
                    .strip_prefix('/')
                    .is_some_and(Self::immutable_relative_path)
        })
    }
}

#[cfg(test)]
#[path = "layout/tests.rs"]
mod tests;
