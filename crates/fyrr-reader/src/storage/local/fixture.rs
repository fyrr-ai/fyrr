//! Batch setup for inert local fixtures, before any CLI or reader starts.
//! Normal store mutations retain their individual publication/durability path.
use super::*;
use std::collections::BTreeSet;

pub(in crate::storage) fn seed(
    root: &std::path::Path,
    namespace: &str,
    entries: Vec<(Path, Vec<u8>)>,
) -> std::io::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let store = LocalStore::open(root)?;
    let lock = store.lock()?;
    store.reap_stage()?;
    let prefix = store.root.clone().join("namespaces").join(namespace);
    let mut targets = Vec::with_capacity(entries.len());
    let mut parents = BTreeSet::new();
    for (location, _) in &entries {
        if location.prefix_match(&prefix).is_none() || store.is_native_file(location) {
            return Err(std::io::Error::other(
                "fixture object is outside its control namespace",
            ));
        }
        let target = store.target(location).map_err(std::io::Error::other)?;
        let mut parent = target.parent();
        while let Some(path) = parent.filter(|path| path.starts_with(root)) {
            parents.insert(path.to_path_buf());
            parent = path.parent();
        }
        targets.push(target);
    }
    for directory in &parents {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)?;
    }
    // Reserve every version durably before exposing any fixture object. Future
    // real puts continue beyond the reservation, including after a setup error.
    let first = read_version(&lock)?;
    let last = first
        .checked_add(entries.len() as u128)
        .ok_or_else(|| std::io::Error::other("fixture version counter exhausted"))?;
    set_version(&lock, last)?;
    lock.sync_all()?;
    for (index, (target, (_, bytes))) in targets.iter().zip(entries).enumerate() {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(target)?;
        file.write_all(&bytes)?;
        set_version(&file, first + index as u128 + 1)?;
    }
    // Bound open files and flush concurrency. Keep the real root lock until
    // every file and its directory entries are durable.
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(8)
        .min(targets.len());
    std::thread::scope(|scope| -> std::io::Result<()> {
        let handles: Vec<_> = targets
            .chunks(targets.len().div_ceil(workers))
            .map(|chunk| {
                scope.spawn(move || -> std::io::Result<()> {
                    for target in chunk {
                        open_object(target)?.sync_all()?;
                    }
                    Ok(())
                })
            })
            .collect();
        for handle in handles {
            handle
                .join()
                .map_err(|_| std::io::Error::other("fixture flush thread panicked"))??;
        }
        Ok(())
    })?;
    // Descendants precede their parents in reverse path order.
    for directory in parents.into_iter().rev() {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}
