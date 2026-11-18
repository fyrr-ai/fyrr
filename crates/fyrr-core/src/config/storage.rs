//! The storage backend that `storage.uri` names.

use std::path::{Component, PathBuf};

use super::{Config, StorageConfig};

/// Where a namespace lives, parsed from `storage.uri`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend<'a> {
    /// `s3://bucket[/prefix]`: S3 or an S3-compatible service.
    S3 { bucket: &'a str, prefix: &'a str },
    /// `file:///absolute/path`: a directory on this host, for single-host use.
    Local { root: PathBuf },
}

impl StorageConfig {
    /// The backend this URI names; `None` for a URI `config check` refuses.
    pub fn backend(&self) -> Option<StorageBackend<'_>> {
        if let Some(rest) = self.uri.strip_prefix("s3://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            return Some(StorageBackend::S3 { bucket, prefix });
        }
        local_root(&self.uri).map(|root| StorageBackend::Local { root })
    }
}

/// Only the canonical spelling of an absolute path below `/` is accepted: no
/// host, query, fragment, `.`/`..` or empty segment, and reserved characters
/// percent-encoded, so the URI names exactly one directory.
fn local_root(uri: &str) -> Option<PathBuf> {
    let url = url::Url::parse(uri).ok()?;
    if url.scheme() != "file"
        || url.as_str() != uri
        || url.host().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let segments: Vec<&str> = url.path_segments()?.collect();
    // A trailing slash names the same directory.
    let segments = segments.strip_suffix(&[""]).unwrap_or(&segments);
    let root = url.to_file_path().ok()?;
    let canonical = url::Url::from_file_path(&root).ok()?;
    if canonical.as_str().trim_end_matches('/') != uri.trim_end_matches('/')
        || root.as_os_str().as_encoded_bytes().contains(&0)
    {
        return None;
    }
    let mut normal = 0;
    for component in root.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(_) => normal += 1,
            _ => return None,
        }
    }
    (normal > 0 && normal == segments.len() && segments.iter().all(|s| !s.is_empty()))
        .then_some(root)
}

/// macOS keeps `/tmp`, `/var` and `/etc` as symlinks into `/private`. Local
/// storage refuses a symlink anywhere in its root path, so a root spelled
/// through one of the three is rewritten to the directory it names. Only
/// these system links are resolved; any other symlink is still refused.
#[cfg(target_os = "macos")]
pub(super) fn macos_private(uri: &str) -> Option<String> {
    let path = uri.strip_prefix("file://")?;
    ["/tmp", "/var", "/etc"]
        .iter()
        .find(|link| {
            path.strip_prefix(**link)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        })
        .map(|_| format!("file:///private{path}"))
}

/// For a `file://` URI, `[storage]` holds only `uri`: every other key
/// configures S3 access.
pub(super) fn local(config: &Config, raw: &toml::Table, error: &mut impl FnMut(&str, String)) {
    if !config.storage.uri.starts_with("file:") {
        return;
    }
    if !matches!(config.storage.backend(), Some(StorageBackend::Local { .. })) {
        error(
            "storage.uri",
            "must be a canonical absolute file URI such as file:///var/lib/fyrr \
             (no host, query, `.`, `..` or empty segments; percent-encode spaces)"
                .into(),
        );
    }
    for key in raw
        .get("storage")
        .and_then(toml::Value::as_table)
        .into_iter()
        .flat_map(|table| table.keys())
        .filter(|key| *key != "uri")
    {
        error(
            &format!("storage.{key}"),
            "applies only to s3:// storage; a file:// URI takes no other storage keys".into(),
        );
    }
}
