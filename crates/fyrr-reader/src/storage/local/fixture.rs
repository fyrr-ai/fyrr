//! Batch setup for inert local fixtures, before any CLI or reader starts.
//! Normal store mutations retain their individual publication/durability path.
use super::*;
use std::collections::BTreeMap;

pub(in crate::storage) fn seed(
    root: &std::path::Path,
    namespace: &str,
    entries: Vec<(Path, Vec<u8>)>,
) -> std::io::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let store = LocalStore::open(root)?;
    let root = store.root_directory()?;
    store.reap(&root)?;
    let prefix = store.root.clone().join("namespaces").join(namespace);
    // One descriptor chain per directory, however many objects it receives.
    let mut directories = BTreeMap::<Vec<String>, Vec<Directory>>::new();
    let mut targets = Vec::with_capacity(entries.len());
    for (location, _) in &entries {
        if location.prefix_match(&prefix).is_none() || store.is_native_file(location) {
            return Err(std::io::Error::other(
                "fixture object is outside its control namespace",
            ));
        }
        let mut names = store.names(location).map_err(std::io::Error::other)?;
        let name = names.pop().expect("an object has a name");
        if !directories.contains_key(&names) {
            directories.insert(names.clone(), store.chain(&names, true)?);
        }
        targets.push((names, name));
    }
    let parent = |names: &Vec<String>| {
        directories[names]
            .last()
            .expect("the chain starts at the root")
    };
    for ((names, name), (_, bytes)) in targets.iter().zip(entries) {
        let mut file = File::from(rustix::fs::openat(
            &parent(names).file,
            name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?);
        file.write_all(&bytes)?;
        set_version(&file, new_tag()?)?;
    }
    // Bound open files and flush concurrency.
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(8)
        .min(targets.len());
    std::thread::scope(|scope| -> std::io::Result<()> {
        let handles: Vec<_> = targets
            .chunks(targets.len().div_ceil(workers))
            .map(|chunk| {
                let parent = &parent;
                scope.spawn(move || -> std::io::Result<()> {
                    for (names, name) in chunk {
                        parent(names).object(name)?.0.sync_all()?;
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
    let mut flushed = BTreeMap::new();
    for directory in directories.values().flatten() {
        flushed.entry(&directory.path).or_insert(&directory.file);
    }
    for directory in flushed.values().rev() {
        directory.sync_all()?;
    }
    Ok(())
}
