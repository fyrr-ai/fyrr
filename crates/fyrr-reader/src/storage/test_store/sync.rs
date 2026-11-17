//! Records successful fsyncs, publication boundaries and acknowledgements,
//! never object contents.
use std::io::Write;
use std::path::Path;

fn record(value: serde_json::Value) -> std::io::Result<()> {
    let Some(log) = std::env::var_os("FYRR_TEST_SYNC_LOG") else {
        return Ok(());
    };
    let mut line = serde_json::to_vec(&value).map_err(std::io::Error::other)?;
    line.push(b'\n');
    std::fs::OpenOptions::new()
        .append(true)
        .open(log)?
        .write_all(&line)
}
pub(crate) fn synced(path: &Path, directory: bool) -> std::io::Result<()> {
    record(serde_json::json!({"event":"sync","path":path,"directory":directory}))
}
pub(crate) fn unlinked(path: &Path) -> std::io::Result<()> {
    record(serde_json::json!({"event":"unlink","path":path}))
}
/// A mutation is about to report success: `put`, `delete`, a `deferred`
/// delete of a cleanup batch, or the `cleanup` batch itself.
pub(crate) fn acknowledged(operation: &str, path: &Path) -> std::io::Result<()> {
    record(serde_json::json!({"event":"ack","operation":operation,"path":path}))
}
pub(crate) fn publishing(
    target: &Path,
    staged: &Path,
    payload: &object_store::PutPayload,
) -> std::io::Result<()> {
    if std::env::var_os("FYRR_TEST_SYNC_LOG").is_none() {
        return Ok(());
    }
    let bytes: Vec<_> = payload
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    let value = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_default();
    let parent = target
        .parent()
        .ok_or_else(|| std::io::Error::other("missing object parent"))?;
    let namespace = if matches!(
        parent.file_name().and_then(|p| p.to_str()),
        Some("envelopes" | "publications")
    ) {
        parent.parent().unwrap_or(parent)
    } else {
        parent
    };
    let mut references = Vec::new();
    if let Some(files) = value["files"].as_array() {
        for file in files {
            if let Some(path) = file["path"].as_str() {
                references.push(namespace.join("dataset.lance").join(path));
            }
        }
    }
    if let Some(fragments) = value["payload"]["fragments"].as_array() {
        for fragment in fragments {
            if let Some(files) = fragment["files"].as_array() {
                for file in files {
                    if let Some(path) = file["path"].as_str() {
                        references.push(namespace.join("dataset.lance").join(path));
                    }
                }
            }
        }
    }
    if target.file_name().is_some_and(|name| name == "head.json") {
        for pointer in ["content", "policy", "identity"] {
            for field in ["manifest", "object"] {
                if let Some(path) = value[pointer][field].as_str() {
                    references.push(namespace.join(path));
                }
            }
        }
    }
    record(
        serde_json::json!({"event":"publish","path":target,"staged":staged,"references":references}),
    )
}
