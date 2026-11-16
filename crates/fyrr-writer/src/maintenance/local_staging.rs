//! LocalFileSystem hides multipart staging names (`name#digits`) from listings.
//! Reap abandoned uploads directly, under the namespace lease, without following
//! symlinks. A live cursor lease protects uploads even with a custom worker timeout.
use crate::{Result, WriterError, lease::LeaseGuard};
use futures::TryStreamExt;
use fyrr_core::config::{Config, StorageBackend};
use fyrr_core::connector::CursorState;
use fyrr_reader::storage::{namespace_store, read_contract};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

// The default worker timeout. Recent files remain protected after a crash;
// live cursor leases additionally protect workers with longer timeouts.
const MIN_AGE: Duration = Duration::from_secs(600);

async fn uploading(config: &Config) -> Result<bool> {
    let (store, root) = namespace_store(config)?;
    let mut cursors = store.list(Some(&root.join("cursors")));
    while let Some(meta) = cursors.try_next().await? {
        let cursor: CursorState = read_contract(store.as_ref(), &meta.location).await?;
        if cursor
            .lease
            .is_some_and(|lease| lease.expires_at > fyrr_core::clock::now())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) async fn plan(config: &Config) -> Result<Vec<PathBuf>> {
    let Some(StorageBackend::Local { root }) = config.storage.backend() else {
        return Ok(Vec::new());
    };
    if uploading(config).await? {
        return Ok(Vec::new());
    }
    let dataset = root
        .join("namespaces")
        .join(&config.namespace.name)
        .join("dataset.lance");
    let limit = config.writer.maintenance.step_max_duration.as_duration();
    let max_files = crate::narrow_usize(config.writer.commit.group_max_rows);
    tokio::task::spawn_blocking(move || {
        let start = Instant::now();
        let now = std::time::SystemTime::from(fyrr_core::clock::now());
        let mut directories = vec![dataset];
        let mut files = Vec::new();
        while let Some(directory) = directories.pop() {
            let entries = match std::fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => return Err(WriterError::StorageUnavailable),
            };
            for entry in entries {
                if start.elapsed() >= limit {
                    return Err(WriterError::MaintenanceStepDeadline);
                }
                let entry = entry.map_err(|_| WriterError::StorageUnavailable)?;
                let kind = entry
                    .file_type()
                    .map_err(|_| WriterError::StorageUnavailable)?;
                if kind.is_dir() {
                    directories.push(entry.path());
                } else if kind.is_file()
                    && entry.file_name().to_str().is_some_and(|name| {
                        name.rsplit_once('#').is_some_and(|(base, suffix)| {
                            !base.is_empty()
                                && !suffix.is_empty()
                                && suffix.bytes().all(|byte| byte.is_ascii_digit())
                        })
                    })
                    && old(&entry.path(), now)?
                {
                    files.push(entry.path());
                    if files.len() >= max_files {
                        return Ok(files);
                    }
                }
            }
        }
        Ok(files)
    })
    .await
    .map_err(|_| WriterError::StorageUnavailable)?
}

fn old(path: &std::path::Path, now: SystemTime) -> Result<bool> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(WriterError::StorageUnavailable),
    };
    let modified = metadata
        .modified()
        .map_err(|_| WriterError::StorageUnavailable)?;
    Ok(metadata.is_file() && now.duration_since(modified).is_ok_and(|age| age >= MIN_AGE))
}

pub(crate) async fn remove(
    config: &Config,
    lease: &LeaseGuard,
    files: Vec<PathBuf>,
) -> Result<usize> {
    let mut removed = 0;
    let start = Instant::now();
    for path in files {
        if start.elapsed() >= config.writer.maintenance.step_max_duration.as_duration() {
            break;
        }
        lease.head().await?;
        if uploading(config).await? {
            break;
        }
        if old(&path, std::time::SystemTime::from(fyrr_core::clock::now()))? {
            match std::fs::remove_file(path) {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(WriterError::StorageUnavailable),
            }
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    #[ignore = "diagnostic: repeated cursor discovery during local upload removal"]
    async fn staging_cursor_scan_runtime_io_measurement() {
        let (config, storage) = crate::lease::tests::local_config("staging-io-probe");
        let (store, namespace) = namespace_store(&config).unwrap();
        let mut head: fyrr_core::publication::ServingHead = serde_json::from_str(include_str!(
            "../../../../fixtures/contracts/serving-head/head.json"
        ))
        .unwrap();
        head.namespace.clone_from(&config.namespace.name);
        head.content = None;
        head.lease = None;
        store
            .put(
                &namespace.clone().join("head.json"),
                fyrr_core::write_contract(&head).into(),
            )
            .await
            .unwrap();
        let mut seed = Vec::new();
        for n in 0..8 {
            let mut cursor: CursorState = serde_json::from_str(include_str!(
                "../../../../fixtures/contracts/connector-cursor/cursor.json"
            ))
            .unwrap();
            cursor.namespace.clone_from(&config.namespace.name);
            cursor.source_id = format!("cursor-{n}");
            seed.push((
                namespace
                    .clone()
                    .join("cursors")
                    .join(format!("cursor-{n}.json")),
                fyrr_core::write_contract(&cursor),
            ));
        }
        fyrr_reader::storage::test_store::seed_local_fixture(&config, seed).unwrap();
        let dataset = storage.join(namespace.as_ref()).join("dataset.lance");
        std::fs::create_dir_all(&dataset).unwrap();
        let old = SystemTime::from(fyrr_core::clock::now()) - Duration::from_secs(1200);
        let lease = LeaseGuard::acquire(&config).await.unwrap();
        for files in [8, 32] {
            let mut paths = Vec::new();
            for n in 0..files {
                let path = dataset.join(format!("retired-{n}#1"));
                std::fs::write(&path, b"retired upload").unwrap();
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_times(std::fs::FileTimes::new().set_modified(old))
                    .unwrap();
                paths.push(path);
            }
            let removed = crate::io_probe::measure(
                &format!("staging-remove-{files}-files-8-cursors"),
                remove(&config, &lease, paths),
            )
            .await
            .unwrap();
            assert_eq!(removed, files);
        }
        lease.release().await.unwrap();
        drop(store);
        std::fs::remove_dir_all(storage).unwrap();
    }
}
