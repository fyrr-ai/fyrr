//! Single-host storage in a local directory (`storage.uri = "file:///…"`).
//!
//! `object_store`'s `LocalFileSystem` publishes writes atomically but has no
//! compare-and-swap (`PutMode::Update`), and its e-tag (inode, modification
//! time in microseconds, size) can repeat: an inode freed by a replacement is
//! reused, and a same-size rewrite may land in the same timestamp tick.
//! [`LocalStore`] adds what fyrr's coordination needs on top of it:
//!
//! - every mutation holds one exclusive `flock` on the root's lock file, so a
//!   conditional write compares the current e-tag and publishes its object
//!   with no other fyrr process changing the object in between;
//! - each write gets a tag of 128 random bits from the operating system,
//!   stored as an xattr on its staging inode before publication. A tag never
//!   depends on timestamps, inode numbers or content, so a rewrite of the
//!   same bytes and an object created again after a delete get new ones;
//! - an object and its version xattr are flushed on a staging inode, then
//!   published by `rename` (replace) or `link` (create only). Native files are
//!   persisted explicitly before the contracts that reference them. Finally,
//!   its directory is flushed. A crash leaves the old object or the new one.
//!
//! - the single reserved staging file is reaped under the lock before every
//!   mutation, so a killed writer cannot leave unlisted data through a purge.
//!
//! - every object is reached by walking from the held root descriptor, one
//!   `openat` per directory with `O_NOFOLLOW`, and is then read, published or
//!   removed relative to its parent descriptor. A symlink or a replaced
//!   directory cannot redirect an operation outside the root between a check
//!   and its use. Listings find names with `LocalFileSystem` and re-read each
//!   one this way.
//!
//! Reads take no lock. The lock is advisory and local to one kernel, so the
//! directory must not be shared between hosts; network file systems are
//! refused where `statfs` names them.
//!
//! Object paths are absolute paths without their leading `/`, as Lance names
//! the files of a local dataset, so the two agree on every path the way S3
//! keys do.

use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use fyrr_core::layout::NamespaceLayout;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result,
};
use rustix::fs::{Mode, OFlags};
use std::fs::{File, Metadata};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const VERSION: &str = "user.fyrr.version";

const STORE: &str = "fyrr local storage";
/// In the root, outside `namespaces/`, so no namespace listing returns it.
const LOCK: &str = ".fyrr-storage.lock";
/// The content of a lock file that carries no version counter. Releases
/// that numbered versions from a counter on this file refuse a non-empty
/// lock file without one, so they cannot write beside this release.
const NO_COUNTER: &[u8] = b"fyrr local storage lock; object tags are random\n";
/// One reserved staging file, outside all namespaces. Holding the root lock
/// throughout a write lets the next mutation safely reap it after a crash.
const STAGE: &str = ".fyrr-storage.stage";
/// `statfs` types of network and remote file systems, whose `flock`, rename
/// and timestamps do not give single-host guarantees: NFS, SMB, CIFS, SMB2,
/// AFS, Ceph and 9p.
const REMOTE_FILE_SYSTEMS: [u64; 7] = [
    0x6969,
    0x517b,
    0xff53_4d42,
    0xfe53_4d42,
    0x5346_414f,
    0x00c3_6400,
    0x0102_1997,
];

/// The object store of a `file://` storage root.
#[derive(Debug, Clone)]
pub(super) struct LocalStore {
    inner: Arc<LocalFileSystem>,
    root: Path,
    root_path: PathBuf,
    directory: Arc<File>,
    root_identity: (u64, u64),
    cleanup_directories: Option<Arc<std::sync::Mutex<std::collections::BTreeMap<PathBuf, File>>>>,
}

impl std::fmt::Display for LocalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(STORE)
    }
}

/// A directory at or below the storage root, opened by walking from the held
/// root descriptor. `path` only names it in errors and the test recorder.
#[derive(Debug)]
struct Directory {
    file: File,
    path: PathBuf,
}

impl Directory {
    fn sync(&self) -> std::io::Result<()> {
        sync_file(&self.file, &self.path)
    }

    /// The entry `name` without following it, or `None` when it is missing.
    fn entry(&self, name: &str) -> std::io::Result<Option<rustix::fs::Stat>> {
        match rustix::fs::statat(&self.file, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW) {
            Ok(entry) => Ok(Some(entry)),
            Err(rustix::io::Errno::NOENT) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Opens the regular file `name` for reading; a symlink is refused.
    fn object(&self, name: &str) -> std::io::Result<(File, Metadata)> {
        let file = File::from(rustix::fs::openat(
            &self.file,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(std::io::Error::other("not a regular file"));
        }
        Ok((file, metadata))
    }

    fn unlink(&self, name: &str) -> std::io::Result<()> {
        Ok(rustix::fs::unlinkat(
            &self.file,
            name,
            rustix::fs::AtFlags::empty(),
        )?)
    }
}

/// Where an object lives: its directories from the root down to its parent,
/// each opened from the one above without following symlinks, and its name.
/// Every operation on the object goes through the parent descriptor, so no
/// path is resolved again between a check and its use.
#[derive(Debug)]
struct Place {
    chain: Vec<Directory>,
    name: String,
    path: PathBuf,
}

impl Place {
    fn parent(&self) -> &Directory {
        self.chain.last().expect("the chain starts at the root")
    }
}

fn is_symlink(entry: &rustix::fs::Stat) -> bool {
    rustix::fs::FileType::from_raw_mode(entry.st_mode) == rustix::fs::FileType::Symlink
}

fn is_regular(entry: &rustix::fs::Stat) -> bool {
    rustix::fs::FileType::from_raw_mode(entry.st_mode) == rustix::fs::FileType::RegularFile
}

impl LocalStore {
    /// Opens `root`, creating it private (0700) when missing. An existing root
    /// must be a directory owned by this user with no group or other access,
    /// not a symlink, and not on a network file system.
    pub(super) fn open(root: &std::path::Path) -> std::io::Result<Self> {
        if !root.is_absolute() {
            return Err(std::io::Error::other("storage root must be absolute"));
        }
        for ancestor in root.ancestors() {
            match std::fs::symlink_metadata(ancestor) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    return Err(std::io::Error::other("symlink in storage root"));
                }
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        crate::mirror::MirrorStore::private_directory(root)?;
        let kind = (rustix::fs::statfs(root)?.f_type as u64) & 0xffff_ffff;
        if REMOTE_FILE_SYSTEMS.contains(&kind) {
            return Err(std::io::Error::other(
                "network file systems are not supported",
            ));
        }
        let directory = Arc::new(File::open(root)?);
        let metadata = directory.metadata()?;
        Ok(Self {
            inner: Arc::new(LocalFileSystem::new().with_fsync(true)),
            root: Path::from_absolute_path(root).map_err(std::io::Error::other)?,
            root_path: root.to_path_buf(),
            directory,
            root_identity: (metadata.dev(), metadata.ino()),
            cleanup_directories: None,
        })
    }

    pub(super) fn persist_native(
        &self,
        dataset: &Path,
        files: &[fyrr_core::envelope::FileRef],
    ) -> std::io::Result<()> {
        let mut directories = std::collections::BTreeMap::new();
        for file in files {
            let location = Path::from(format!("{dataset}/{}", file.path));
            if location.prefix_match(dataset).is_none() || !self.is_native_file(&location) {
                return Err(std::io::Error::other("native file outside dataset"));
            }
            let place = self.place(&location, false)?;
            let (opened, metadata) = place.parent().object(&place.name)?;
            if metadata.len() != file.size_bytes {
                return Err(std::io::Error::other("native file size changed"));
            }
            sync_file(&opened, &place.path)?;
            for directory in place.chain {
                directories.entry(directory.path).or_insert(directory.file);
            }
        }
        // Descendants before ancestors, including newly created native directories.
        for (path, directory) in directories.iter().rev() {
            sync_file(directory, path)?;
        }
        Ok(())
    }

    /// Only maintenance and purge opt into deferred directory durability.
    pub(super) fn cleanup_batch(mut self) -> Self {
        self.cleanup_directories = Some(Arc::new(std::sync::Mutex::new(Default::default())));
        self
    }

    /// A successful cleanup includes durable removal of every directory entry.
    pub(super) async fn finish_cleanup(&self) -> Result<()> {
        self.blocking(|store| {
            let directories = store.cleanup_directories.as_ref().expect("cleanup batch");
            let mut directories = directories.lock().expect("cleanup directories lock");
            for (path, directory) in directories.iter() {
                sync_file(directory, path).map_err(|error| failure(path, error))?;
            }
            directories.clear();
            #[cfg(feature = "test-store")]
            super::test_store::sync::acknowledged("cleanup", &store.root_path)
                .map_err(|error| failure(&store.root_path, error))?;
            Ok(())
        })
        .await
    }

    /// The object path of the storage root.
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    fn is_internal(&self, location: &Path) -> bool {
        location.prefix_match(&self.root).is_some_and(|mut rest| {
            rest.next()
                .is_some_and(|part| matches!(part.as_ref(), LOCK | STAGE))
                && rest.next().is_none()
        })
    }

    /// The held root descriptor, after checking that the root path still
    /// names it: a replaced or redirected root must not be served.
    fn root_directory(&self) -> std::io::Result<Directory> {
        let current = std::fs::symlink_metadata(&self.root_path)?;
        if !current.is_dir() || (current.dev(), current.ino()) != self.root_identity {
            return Err(std::io::Error::other("storage root changed"));
        }
        Ok(Directory {
            file: self.directory.try_clone()?,
            path: self.root_path.clone(),
        })
    }

    /// The names of `location` below the root; only objects of this store.
    fn names(&self, location: &Path) -> Result<Vec<String>> {
        let outside = || unsupported("path outside the namespace");
        if self.is_internal(location) {
            return Err(outside());
        }
        let names: Vec<String> = location
            .prefix_match(&self.root)
            .ok_or_else(outside)?
            .map(|part| part.as_ref().to_owned())
            .collect();
        if names.is_empty()
            || names
                .iter()
                .any(|name| matches!(name.as_str(), "" | "." | "..") || name.contains('/'))
        {
            return Err(outside());
        }
        Ok(names)
    }

    /// Opens the directories `names` one below the other, starting at the
    /// held root descriptor. No component is followed when it is a symlink,
    /// so the result is beneath the root whatever happens to the tree
    /// meanwhile. `create` makes missing directories private.
    fn chain(&self, names: &[String], create: bool) -> std::io::Result<Vec<Directory>> {
        let mut chain = vec![self.root_directory()?];
        for name in names {
            let above = chain.last().expect("the chain starts at the root");
            let open = || {
                rustix::fs::openat(
                    &above.file,
                    name.as_str(),
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
            };
            let opened = match open() {
                Err(rustix::io::Errno::NOENT) if create => {
                    match rustix::fs::mkdirat(&above.file, name.as_str(), Mode::RWXU) {
                        Ok(()) | Err(rustix::io::Errno::EXIST) => open(),
                        Err(error) => Err(error),
                    }
                }
                opened => opened,
            };
            let file = match opened {
                Ok(file) => File::from(file),
                Err(error) => {
                    return Err(match above.entry(name) {
                        Ok(Some(entry)) if is_symlink(&entry) => {
                            std::io::Error::other("symlink in storage path")
                        }
                        Ok(Some(_)) => std::io::Error::from(ErrorKind::NotADirectory),
                        _ => error.into(),
                    });
                }
            };
            let path = above.path.join(name);
            chain.push(Directory { file, path });
        }
        Ok(chain)
    }

    fn place(&self, location: &Path, create: bool) -> Result<Place> {
        let mut names = self.names(location)?;
        let name = names.pop().expect("an object has a name");
        let path = self.inner.path_to_filesystem(location)?;
        let chain = self
            .chain(&names, create)
            .map_err(|error| failure(&path, error))?;
        Ok(Place { chain, name, path })
    }

    /// The exclusive lock every mutation holds; released when the file closes,
    /// also when the process dies.
    fn lock(&self) -> std::io::Result<File> {
        let root = self.root_directory()?;
        let mut file = File::from(rustix::fs::openat(
            &root.file,
            LOCK,
            OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?);
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.uid() != rustix::process::getuid().as_raw()
            || meta.mode() & 0o077 != 0
        {
            return Err(std::io::Error::other("storage lock is not private"));
        }
        // A stopped writer may retain this lock indefinitely. Never park a
        // storage worker behind it: contention must fail closed within a bound.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match file.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(std::io::Error::new(
                            ErrorKind::WouldBlock,
                            "storage lock acquisition deadline exceeded",
                        ));
                    }
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(5)));
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
        }
        self.hand_over(&root, &mut file)?;
        Ok(file)
    }

    /// Takes a root over from a release that numbered versions from a
    /// counter on the lock file, and marks a new root the same way: the lock
    /// file is non-empty and has no counter. Such a release then fails every
    /// mutation at its counter check, and its tags stay valid, because a tag
    /// is only ever compared for equality.
    fn hand_over(&self, root: &Directory, file: &mut File) -> std::io::Result<()> {
        let mut changed = false;
        if file.metadata()?.len() == 0 {
            file.write_all(NO_COUNTER)?;
            changed = true;
        }
        match rustix::fs::fremovexattr(&*file, VERSION) {
            Ok(()) => changed = true,
            Err(rustix::io::Errno::NODATA) => {}
            Err(error) => return Err(error.into()),
        }
        if changed {
            sync_file(file, &root.path.join(LOCK))?;
            root.sync()?;
        }
        Ok(())
    }

    fn put_blocking(
        &self,
        location: &Path,
        payload: PutPayload,
        mode: PutMode,
    ) -> Result<PutResult> {
        self.names(location)?;
        let _lock = self
            .lock()
            .map_err(|error| failure(&self.root_path.join(LOCK), error))?;
        let place = self.place(location, true)?;
        let io = |error| failure(&place.path, error);
        let root = &place.chain[0];
        self.reap_stage(root).map_err(io)?;
        let mut file = rustix::fs::openat(
            &root.file,
            STAGE,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map(File::from)
        .map_err(|error| io(error.into()))?;
        let result = self.publish(&place, &mut file, &payload, mode);
        if result.is_err() {
            let _ = root.unlink(STAGE);
        }
        #[cfg(feature = "test-store")]
        if result.is_ok() {
            super::test_store::sync::acknowledged("put", &place.path).map_err(io)?;
        }
        result
    }

    fn publish(
        &self,
        place: &Place,
        file: &mut File,
        payload: &PutPayload,
        mode: PutMode,
    ) -> Result<PutResult> {
        let io = |error| failure(&place.path, error);
        let (root, parent) = (&place.chain[0], place.parent());
        let staged = root.path.join(STAGE);
        for chunk in payload.iter() {
            file.write_all(chunk).map_err(io)?;
        }
        #[cfg(test)]
        tests::pause("staged");
        #[cfg(test)]
        tests::pause("locked");
        let current = match parent.entry(&place.name).map_err(io)? {
            Some(entry) if is_symlink(&entry) => {
                return Err(io(std::io::Error::other("symlink in storage path")));
            }
            Some(entry) if is_regular(&entry) => Some(entry),
            Some(_) => return Err(io(std::io::Error::other("not a regular file"))),
            None => None,
        };
        let path = place.path.display().to_string();
        if matches!(&mode, PutMode::Update(expected) if expected.version.is_some()) {
            return Err(unsupported("historical object versions"));
        }
        match (&mode, &current) {
            (PutMode::Create, Some(_)) => {
                return Err(Error::AlreadyExists {
                    path,
                    source: "object exists".into(),
                });
            }
            (PutMode::Update(_), None) => {
                return Err(Error::Precondition {
                    path,
                    source: "object does not exist".into(),
                });
            }
            (PutMode::Update(expected), Some(_)) => match &expected.e_tag {
                Some(tag)
                    if *tag
                        == version_tag(
                            read_version(&parent.object(&place.name).map_err(io)?.0).map_err(io)?,
                        ) => {}
                Some(_) => {
                    return Err(Error::Precondition {
                        path,
                        source: "e-tag does not match".into(),
                    });
                }
                None => {
                    return Err(Error::Generic {
                        store: STORE,
                        source: "a conditional update needs an e-tag".into(),
                    });
                }
            },
            _ => {}
        }
        if current.is_none() {
            // Another writer or Lance may have created these directory names
            // without persisting them. New objects depend on the whole chain.
            for directory in place.chain.iter().rev() {
                directory.sync().map_err(io)?;
            }
        }
        let next = new_tag().map_err(io)?;
        set_version(file, next).map_err(io)?;
        sync_file(file, &staged).map_err(io)?;
        #[cfg(test)]
        tests::pause("versioned");
        #[cfg(feature = "test-store")]
        super::test_store::sync::publishing(&place.path, &staged, payload).map_err(io)?;
        let name = place.name.as_str();
        if matches!(mode, PutMode::Create) {
            rustix::fs::linkat(
                &root.file,
                STAGE,
                &parent.file,
                name,
                rustix::fs::AtFlags::empty(),
            )
            .map_err(|error| io(error.into()))?;
            // Publication succeeded, but a cleanup error still fails the call.
            root.unlink(STAGE).map_err(io)?;
        } else {
            rustix::fs::renameat(&root.file, STAGE, &parent.file, name)
                .map_err(|error| io(error.into()))?;
        }
        parent.sync().map_err(io)?;
        root.sync().map_err(io)?;
        Ok(PutResult {
            e_tag: Some(version_tag(next)),
            version: None,
            extensions: Default::default(),
        })
    }

    /// Only a dead writer can leave this file while another writer holds
    /// the lock. Reap it before *any* mutation, so purge cannot leave behind
    /// unlisted temporary objects containing retired content.
    fn reap_stage(&self, root: &Directory) -> std::io::Result<()> {
        match root.entry(STAGE)? {
            Some(entry)
                if is_regular(&entry)
                    && entry.st_uid == rustix::process::getuid().as_raw()
                    && entry.st_mode & 0o077 == 0 =>
            {
                root.unlink(STAGE)?;
                root.sync()
            }
            Some(_) => Err(std::io::Error::other(
                "staging object is not a private regular file",
            )),
            None => Ok(()),
        }
    }

    /// Lance owns the files below the dataset, including native commit
    /// metadata. All other objects belong to our versioned control protocol.
    fn is_native_file(&self, location: &Path) -> bool {
        let Some(mut parts) = location.prefix_match(&self.root) else {
            return false;
        };
        parts
            .next()
            .is_some_and(|part| part.as_ref() == "namespaces")
            && parts.next().is_some()
            && parts
                .next()
                .is_some_and(|part| part.as_ref() == NamespaceLayout::DATASET_NAME)
            && parts.next().is_some()
    }

    fn get_blocking(&self, location: Path, options: GetOptions) -> Result<GetResult> {
        let place = self.place(&location, false)?;
        let path = place.path.clone();
        self.read(place.parent(), &place.name, path, location, options)
    }

    /// Reads the object `name` of `parent`: the bytes, metadata and version
    /// all come from the one descriptor opened here.
    fn read(
        &self,
        parent: &Directory,
        name: &str,
        path: PathBuf,
        location: Path,
        options: GetOptions,
    ) -> Result<GetResult> {
        let io = |error| failure(&path, error);
        let (file, metadata) = parent.object(name).map_err(io)?;
        let mut meta = ObjectMeta {
            location,
            last_modified: metadata.modified().map_err(io)?.into(),
            size: metadata.len(),
            e_tag: Some(e_tag(&metadata)),
            version: None,
        };
        // Lance's immutable files are written by LocalFileSystem directly.
        // Only our control objects carry a tag. Missing
        // metadata is valid solely for files owned by Lance.
        match read_version(&file) {
            Ok(version) => meta.e_tag = Some(version_tag(version)),
            Err(error)
                if error.raw_os_error() == Some(rustix::io::Errno::NODATA.raw_os_error())
                    && self.is_native_file(&meta.location) => {}
            Err(error) => return Err(io(error)),
        }
        options.check_preconditions(&meta)?;
        if options.version.is_some() {
            return Err(unsupported("historical object versions"));
        }
        let range = match options.range {
            Some(range) => range.as_range(meta.size).map_err(|source| Error::Generic {
                store: STORE,
                source: Box::new(source),
            })?,
            None => 0..meta.size,
        };
        Ok(GetResult {
            payload: GetResultPayload::File(file, path),
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }

    /// The listed objects with the metadata reads report. An object deleted
    /// since the directory walk is no longer part of the listing, as on S3.
    fn current(&self, listed: Vec<ObjectMeta>) -> Result<Vec<ObjectMeta>> {
        let mut objects = Vec::with_capacity(listed.len());
        // Listings arrive grouped by directory: walk each directory once.
        let mut walked: Option<(Vec<String>, Vec<Directory>)> = None;
        for object in listed {
            if self.is_internal(&object.location) {
                continue;
            }
            let mut names = self.names(&object.location)?;
            let name = names.pop().expect("an object has a name");
            let path = self.inner.path_to_filesystem(&object.location)?;
            if walked.as_ref().is_none_or(|(known, _)| *known != names) {
                walked = match self.chain(&names, false) {
                    Ok(chain) => Some((names, chain)),
                    Err(error) if error.kind() == ErrorKind::NotFound => continue,
                    Err(error) => return Err(failure(&path, error)),
                };
            }
            let parent = walked
                .as_ref()
                .and_then(|(_, chain)| chain.last())
                .expect("the chain starts at the root");
            if self.is_native_file(&object.location) {
                // Native files use the listing metadata and have no version xattr.
                // Validate containment for this listing operation, without reopening
                // every immutable file merely to reconstruct the same metadata.
                match parent.entry(&name).map_err(|error| failure(&path, error))? {
                    Some(entry) if is_symlink(&entry) => {
                        return Err(failure(
                            &path,
                            std::io::Error::other("symlink in storage path"),
                        ));
                    }
                    Some(_) => objects.push(object),
                    None => {}
                }
                continue;
            }
            match self.read(parent, &name, path, object.location, GetOptions::default()) {
                Ok(read) => objects.push(read.meta),
                Err(Error::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(objects)
    }

    pub(super) async fn delete_if_match(&self, location: Path, expected: String) -> Result<()> {
        self.blocking(move |store| {
            let place = store.place(&location, false)?;
            let io = |error| failure(&place.path, error);
            let parent = place.parent();
            let _lock = store.lock().map_err(io)?;
            store.reap_stage(&place.chain[0]).map_err(io)?;
            let (file, _) = parent.object(&place.name).map_err(io)?;
            if version_tag(read_version(&file).map_err(io)?) != expected {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: "namespace head changed".into(),
                });
            }
            parent.unlink(&place.name).map_err(io)?;
            #[cfg(feature = "test-store")]
            super::test_store::sync::unlinked(&place.path).map_err(io)?;
            parent.sync().map_err(io)?;
            #[cfg(feature = "test-store")]
            super::test_store::sync::acknowledged("delete", &place.path).map_err(io)?;
            Ok(())
        })
        .await
    }

    /// Deleting a missing object succeeds, as on S3.
    fn delete_blocking(&self, location: &Path) -> Result<()> {
        let place = match self.place(location, false) {
            Ok(place) => place,
            Err(Error::NotFound { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        let io = |error| failure(&place.path, error);
        let parent = place.parent();
        let _lock = self.lock().map_err(io)?;
        self.reap_stage(&place.chain[0]).map_err(io)?;
        match parent.entry(&place.name).map_err(io)? {
            Some(entry) if is_symlink(&entry) => {
                return Err(io(std::io::Error::other("symlink in storage path")));
            }
            Some(_) => {}
            None => return Ok(()),
        }
        match parent.unlink(&place.name) {
            Ok(()) => {
                #[cfg(feature = "test-store")]
                super::test_store::sync::unlinked(&place.path).map_err(io)?;
                if let Some(directories) = &self.cleanup_directories {
                    let directory = parent.file.try_clone().map_err(io)?;
                    directories
                        .lock()
                        .expect("cleanup directories lock")
                        .entry(parent.path.clone())
                        .or_insert(directory);
                    #[cfg(test)]
                    tests::pause("unlinked");
                    #[cfg(feature = "test-store")]
                    super::test_store::sync::acknowledged("deferred", &place.path).map_err(io)?;
                    Ok(())
                } else {
                    parent.sync().map_err(io)?;
                    #[cfg(feature = "test-store")]
                    super::test_store::sync::acknowledged("delete", &place.path).map_err(io)?;
                    Ok(())
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io(error)),
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || operation(store))
            .await
            .map_err(|error| Error::Generic {
                store: STORE,
                source: Box::new(error),
            })?
    }
}

/// A tag no other write has: 128 bits from the operating system's generator.
/// A failure to obtain them fails the write.
fn new_tag() -> std::io::Result<u128> {
    let mut bytes = [0u8; 16];
    let mut filled = 0;
    while filled < bytes.len() {
        match rustix::rand::getrandom(&mut bytes[filled..], rustix::rand::GetRandomFlags::empty()) {
            Ok(0) => return Err(std::io::Error::other("no random bytes for an object tag")),
            Ok(read) => filled += read,
            Err(rustix::io::Errno::INTR) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(u128::from_be_bytes(bytes))
}

fn read_version(file: &File) -> std::io::Result<u128> {
    let mut bytes = [0u8; 16];
    let size = rustix::fs::fgetxattr(file, VERSION, &mut bytes)?;
    if size != bytes.len() {
        return Err(std::io::Error::other("invalid storage version"));
    }
    Ok(u128::from_be_bytes(bytes))
}

fn set_version(file: &File, version: u128) -> std::io::Result<()> {
    rustix::fs::fsetxattr(
        file,
        VERSION,
        &version.to_be_bytes(),
        rustix::fs::XattrFlags::empty(),
    )?;
    Ok(())
}

fn version_tag(version: u128) -> String {
    format!("\"{version:x}\"")
}

/// The e-tag `LocalFileSystem` reports for the same file, so reads and
/// conditional writes compare equal values.
pub(crate) fn e_tag(meta: &Metadata) -> String {
    format!(
        "\"{:x}-{:x}-{:x}\"",
        meta.ino(),
        micros(modified(meta)),
        meta.len()
    )
}

fn modified(meta: &Metadata) -> SystemTime {
    meta.modified().unwrap_or(UNIX_EPOCH)
}

fn micros(at: SystemTime) -> u128 {
    at.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

fn sync_file(file: &File, path: &std::path::Path) -> std::io::Result<()> {
    file.sync_all()?;
    #[cfg(feature = "test-store")]
    if std::env::var_os("FYRR_TEST_SYNC_LOG").is_some() {
        super::test_store::sync::synced(path, file.metadata()?.is_dir())?;
    }
    #[cfg(not(feature = "test-store"))]
    let _ = path;
    Ok(())
}

fn failure(target: &std::path::Path, error: std::io::Error) -> Error {
    let path = target.display().to_string();
    let source = Box::new(error);
    match source.kind() {
        ErrorKind::NotFound => Error::NotFound { path, source },
        // `link` refuses an existing target even without the lock.
        ErrorKind::AlreadyExists => Error::AlreadyExists { path, source },
        _ => Error::Generic {
            store: STORE,
            source,
        },
    }
}

fn unsupported(operation: &str) -> Error {
    Error::NotSupported {
        source: format!("{operation} is not used on fyrr local storage").into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if !opts.attributes.is_empty() {
            return Err(Error::NotImplemented {
                operation: "`put_opts` with `opts.attributes` specified".into(),
                implementer: STORE.into(),
            });
        }
        let location = location.clone();
        self.blocking(move |store| store.put_blocking(&location, payload, opts.mode))
            .await
    }

    /// Multipart uploads bypass the conditional-write path; fyrr's control
    /// objects never use them.
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(unsupported("multipart upload"))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        self.blocking(move |store| store.get_blocking(location, options))
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    store
                        .blocking(move |store| store.delete_blocking(&location).map(|()| location))
                        .await
                }
            })
            .boxed()
    }

    /// Ordered by path, as S3 lists; no prefix lists the root.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let listing = self.inner.list(Some(prefix.unwrap_or(&self.root)));
        let store = self.clone();
        stream::once(async move {
            let mut objects = store.current(listing.try_collect().await?)?;
            objects.sort_by(|a, b| a.location.cmp(&b.location));
            Ok::<_, Error>(stream::iter(objects.into_iter().map(Ok)))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.unwrap_or(&self.root);
        let mut result = self.inner.list_with_delimiter(Some(prefix)).await?;
        result.objects = self.current(result.objects)?;
        for prefix in &result.common_prefixes {
            let names = self.names(prefix)?;
            match self.chain(&names, false) {
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(failure(&self.inner.path_to_filesystem(prefix)?, error));
                }
            }
        }
        result.objects.sort_by(|a, b| a.location.cmp(&b.location));
        result.common_prefixes.sort();
        Ok(result)
    }

    /// fyrr never copies control objects through this store.
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(unsupported("copy"))
    }
}

#[cfg(test)]
#[path = "local/tests.rs"]
mod tests;

#[cfg(feature = "test-store")]
#[path = "local/fixture.rs"]
pub(super) mod fixture;
