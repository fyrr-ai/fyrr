//! Single-host storage in a local directory (`storage.uri = "file:///…"`).
//!
//! `object_store`'s `LocalFileSystem` publishes writes atomically but has no
//! compare-and-swap (`PutMode::Update`), and its e-tag (inode, modification
//! time in microseconds, size) can repeat: an inode freed by a replacement is
//! reused, and a same-size rewrite may land in the same timestamp tick.
//! [`LocalStore`] adds what fyrr's coordination needs on top of it:
//!
//! - each write gets a tag of 128 random bits from the operating system,
//!   stored as an xattr on its staging inode before publication. A tag never
//!   depends on timestamps, inode numbers or content, so a rewrite of the
//!   same bytes and an object created again after a delete get new ones;
//! - an object and its tag are flushed on a staging inode before the object
//!   gets its name. A create publishes with `renameat2(RENAME_NOREPLACE)`,
//!   which is atomic and needs no lock. After the publication the writer
//!   flushes the object's directory, and only then acknowledges. A crash
//!   leaves the old object or the new one, each complete;
//! - a write that replaces or removes an object locks that object: an
//!   exclusive `flock` on the inode the name currently means, then a check
//!   that the name still means it. If not, someone replaced the object and
//!   the writer starts over with the new inode. Every move of a name away
//!   from an inode needs that inode's lock, so two writers cannot both
//!   compare the same tag and publish. Only the comparison and the `rename`
//!   or `unlink` happen under the lock; nothing is flushed there. Writes to
//!   different objects never wait for each other;
//! - `flock` cannot be revoked, and a waiter cannot tell a stopped holder
//!   from a slow one. A writer therefore polls for an object's lock for at
//!   most [`LOCK_WAIT`] and then fails without writing. A process stopped
//!   inside that short section blocks later replacements and removals of
//!   that one object; creates and all other objects proceed;
//! - a reader can see an object before its writer flushed the directory.
//!   Whoever publishes a reference to something it only observed makes it
//!   durable first: a create that finds its object present flushes it before
//!   reporting so, and [`LocalStore::persist_control`] covers the rest.
//!   Native files are persisted explicitly before the contracts that
//!   reference them;
//!
//! - each write stages its bytes in its own file under
//!   `.fyrr-storage.stage.d/` and holds an exclusive `flock` on it while the
//!   name exists. A staging file nobody locks belongs to a dead writer; the
//!   first mutation of a store handle, every delete outside a cleanup batch
//!   and the end of a batch remove such files, so a killed writer cannot
//!   leave unlisted data through a purge. A stopped writer keeps its lock
//!   and its file.
//!
//! - every object is reached by walking from the held root descriptor, one
//!   `openat` per directory with `O_NOFOLLOW`, and is then read, published or
//!   removed relative to its parent descriptor. A symlink or a replaced
//!   directory cannot redirect an operation outside the root between a check
//!   and its use. Listings find names with `LocalFileSystem` and re-read each
//!   one this way.
//!
//! Reads take no lock. The locks are advisory and local to one kernel, so
//! the directory must not be shared between hosts; network file systems are
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
/// Releases that serialized every mutation held a `flock` on this file and
/// kept a version counter in its xattr. Nothing locks it any more, except
/// once to take a root over from such a release.
const LOCK: &str = ".fyrr-storage.lock";
/// The content of a lock file that carries no version counter. Releases
/// that numbered versions from a counter on this file refuse a non-empty
/// lock file without one, so they cannot write beside this release.
const NO_COUNTER: &[u8] = b"fyrr local storage lock; object tags are random\n";
/// The staging directory, outside all namespaces: one file per write in
/// progress, named by the write's tag and locked by its writer.
const STAGES: &str = ".fyrr-storage.stage.d";
/// The single staging file of releases that staged under the root lock. One
/// left behind by a killed writer is removed after the handover.
const LEGACY_STAGE: &str = ".fyrr-storage.stage";
/// How long a mutation polls for a `flock` before it fails without writing.
/// Nothing is flushed under a lock, so a live holder is done in far less.
/// Only time spent waiting for a lock counts; a slow system call does not.
const LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(5);
/// How often a mutation starts over because the object changed under it.
/// Each new start means another writer got through, so this only ends a
/// writer that could otherwise lose to others indefinitely.
const ATTEMPTS: usize = 64;
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
    /// Whether this handle already removed abandoned staging files.
    reaped: Arc<std::sync::atomic::AtomicBool>,
}

/// The lock on an object: an exclusive `flock` on the inode its name means.
/// Dropping it releases the lock.
#[derive(Debug)]
struct Key {
    file: File,
    #[cfg_attr(not(feature = "test-store"), allow(dead_code))]
    path: PathBuf,
}

impl Drop for Key {
    fn drop(&mut self) {
        #[cfg(feature = "test-store")]
        let _ = super::test_store::sync::unlocked(&self.path);
    }
}

/// A write's staging file. The writer's `flock` on `file` marks it as live.
#[derive(Debug)]
struct Stage {
    directory: Directory,
    name: String,
    file: File,
    path: PathBuf,
    tag: u128,
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

    /// Removes the directories below the root that an object's removal left
    /// empty, as an object store has no empty prefix, and returns the one that
    /// remains. A write that opened one meanwhile makes it again.
    fn without_empty_directories(&self) -> &Directory {
        let mut kept = self.chain.len() - 1;
        while kept > 0
            && self.chain[kept]
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    rustix::fs::unlinkat(
                        &self.chain[kept - 1].file,
                        name,
                        rustix::fs::AtFlags::REMOVEDIR,
                    )
                    .is_ok()
                })
        {
            kept -= 1;
        }
        &self.chain[kept]
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
            reaped: Arc::default(),
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

    /// Flushes `namespace`, each directory in it and the directories above
    /// it. Control objects are flushed before they get a name, so a durable
    /// directory entry makes the whole object durable; that covers every
    /// control object visible when this starts, whoever wrote it. Control
    /// objects live in the namespace directory or one directory below it,
    /// so this reads only the namespace directory, however many objects the
    /// others hold. The dataset is Lance's and has [`Self::persist_native`].
    pub(super) async fn persist_control(&self, namespace: Path) -> Result<()> {
        self.blocking(move |store| {
            let path = store.inner.path_to_filesystem(&namespace)?;
            let names = store.names(&namespace)?;
            let chain = match store.chain(&names, false) {
                Ok(chain) => chain,
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(failure(&path, error)),
            };
            let directory = chain.last().expect("the chain starts at the root");
            sync_children(directory, NamespaceLayout::DATASET_NAME)
                .map_err(|error| failure(&path, error))?;
            for directory in chain.iter().rev() {
                directory.sync().map_err(|error| failure(&path, error))?;
            }
            Ok(())
        })
        .await
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
            let root = store
                .root_directory()
                .map_err(|error| failure(&store.root_path, error))?;
            store
                .hand_over(&root)
                .and_then(|()| store.reap(&root))
                .map_err(|error| failure(&store.root_path, error))?;
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
                .is_some_and(|part| matches!(part.as_ref(), LOCK | STAGES | LEGACY_STAGE))
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
                    // A delete that empties a directory removes it, also between
                    // this `mkdirat` and the `open`: make it once more.
                    let mut made = Err(rustix::io::Errno::NOENT);
                    for _ in 0..2 {
                        made = match rustix::fs::mkdirat(&above.file, name.as_str(), Mode::RWXU) {
                            Ok(()) | Err(rustix::io::Errno::EXIST) => open(),
                            Err(error) => Err(error),
                        };
                        if !matches!(made, Err(rustix::io::Errno::NOENT)) {
                            break;
                        }
                    }
                    made
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

    /// Makes sure no release that serialized mutations through the root lock
    /// can write to this root: the lock file is non-empty and carries no
    /// version counter, which such a release refuses at its counter check.
    /// A root in that state costs one open and two reads here. Otherwise the
    /// root lock is taken once, so that a mutation such a release has in
    /// progress finishes first; then the counter is removed. The tags it
    /// wrote stay valid, because a tag is only ever compared for equality.
    fn hand_over(&self, root: &Directory) -> std::io::Result<()> {
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
        let counted = match rustix::fs::fgetxattr(&file, VERSION, &mut [0u8; 0][..]) {
            Ok(_) => true,
            Err(rustix::io::Errno::NODATA) => false,
            Err(error) => return Err(error.into()),
        };
        if meta.len() > 0 && !counted {
            return Ok(());
        }
        lock_exclusive(&file)?;
        let mut changed = false;
        if file.metadata()?.len() == 0 {
            file.write_all(NO_COUNTER)?;
            changed = true;
        }
        match rustix::fs::fremovexattr(&file, VERSION) {
            Ok(()) => changed = true,
            Err(rustix::io::Errno::NODATA) => {}
            Err(error) => return Err(error.into()),
        }
        if changed {
            sync_file(&file, &root.path.join(LOCK))?;
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
        if matches!(&mode, PutMode::Update(expected) if expected.version.is_some()) {
            return Err(unsupported("historical object versions"));
        }
        self.names(location)?;
        let root = self
            .root_directory()
            .map_err(|error| failure(&self.root_path, error))?;
        self.hand_over(&root)
            .map_err(|error| failure(&self.root_path.join(LOCK), error))?;
        // A delete removes the directories it empties, also those this write
        // has just opened: it then writes again, once, into new ones.
        let mut again = true;
        loop {
            let place = self.place(location, true)?;
            let io = |error| failure(&place.path, error);
            if !self.reaped.swap(true, std::sync::atomic::Ordering::SeqCst) {
                self.reap(&root).map_err(io)?;
            }
            // A write that cannot succeed is refused before it stages anything.
            self.admit(&place, &mode, None)?;
            let stage = self.stage(&root).map_err(io)?;
            let result = self.publish(&place, &stage, &payload, &mode);
            if result.is_err() {
                let _ = stage.directory.unlink(&stage.name);
            }
            if matches!(result, Err(Error::NotFound { .. })) && std::mem::take(&mut again) {
                continue;
            }
            #[cfg(feature = "test-store")]
            if result.is_ok() {
                super::test_store::sync::acknowledged("put", &place.path).map_err(io)?;
            }
            return result;
        }
    }

    /// Whether `mode` can apply to the object as it is now. `locked` is the
    /// held lock of the object when the answer must stay true until the
    /// caller publishes; without it the answer is only good for refusing,
    /// which is always correct: an object that exists was created, and a tag
    /// that does not match never matches again.
    fn admit(&self, place: &Place, mode: &PutMode, locked: Option<&Key>) -> Result<()> {
        let io = |error| failure(&place.path, error);
        let path = || place.path.display().to_string();
        let missing = || Error::Precondition {
            path: path(),
            source: "object does not exist".into(),
        };
        let current = match locked {
            Some(key) => Some(key.file.try_clone().map_err(io)?),
            None => match place.parent().entry(&place.name).map_err(io)? {
                Some(entry) if is_symlink(&entry) => {
                    return Err(io(std::io::Error::other("symlink in storage path")));
                }
                Some(entry) if !is_regular(&entry) => {
                    return Err(io(std::io::Error::other("not a regular file")));
                }
                Some(_) => match place.parent().object(&place.name) {
                    Ok((file, _)) => Some(file),
                    Err(error) if error.kind() == ErrorKind::NotFound => None,
                    Err(error) => return Err(io(error)),
                },
                None => None,
            },
        };
        match (mode, current) {
            (PutMode::Create, Some(_)) => {
                self.settle_existing(place).map_err(io)?;
                Err(Error::AlreadyExists {
                    path: path(),
                    source: "object exists".into(),
                })
            }
            (PutMode::Update(_), None) => Err(missing()),
            (PutMode::Update(expected), Some(file)) => match &expected.e_tag {
                Some(tag) if *tag == version_tag(read_version(&file).map_err(io)?) => Ok(()),
                Some(_) => Err(Error::Precondition {
                    path: path(),
                    source: "e-tag does not match".into(),
                }),
                None => Err(Error::Generic {
                    store: STORE,
                    source: "a conditional update needs an e-tag".into(),
                }),
            },
            _ => Ok(()),
        }
    }

    /// Publishes the staged object: flush, give it the name, release, flush
    /// the directory. Nothing is flushed while an object's lock is held.
    fn publish(
        &self,
        place: &Place,
        stage: &Stage,
        payload: &PutPayload,
        mode: &PutMode,
    ) -> Result<PutResult> {
        let io = |error| failure(&place.path, error);
        let parent = place.parent();
        let mut file = &stage.file;
        for chunk in payload.iter() {
            file.write_all(chunk).map_err(io)?;
        }
        #[cfg(test)]
        tests::pause("staged");
        set_version(&stage.file, stage.tag).map_err(io)?;
        sync_file(&stage.file, &stage.path).map_err(io)?;
        #[cfg(test)]
        tests::pause("versioned");
        #[cfg(feature = "test-store")]
        super::test_store::sync::publishing(&place.path, &stage.path, payload).map_err(io)?;
        let replace = |key: Key| -> Result<()> {
            #[cfg(test)]
            tests::pause("locked");
            rustix::fs::renameat(
                &stage.directory.file,
                stage.name.as_str(),
                &parent.file,
                place.name.as_str(),
            )
            .map_err(|error| io(error.into()))?;
            drop(key);
            Ok(())
        };
        let mut attempts = 0..ATTEMPTS;
        let created = loop {
            if attempts.next().is_none() {
                return Err(io(contended()));
            }
            match mode {
                PutMode::Create => {
                    if self.create(place, stage).map_err(io)? {
                        break true;
                    }
                    // Lost to another creator; this reports and flushes it.
                    self.admit(place, mode, None)?;
                }
                PutMode::Update(_) => {
                    let Some(key) = self.key(place).map_err(io)? else {
                        return Err(Error::Precondition {
                            path: place.path.display().to_string(),
                            source: "object does not exist".into(),
                        });
                    };
                    self.admit(place, mode, Some(&key))?;
                    replace(key)?;
                    break false;
                }
                PutMode::Overwrite => {
                    if self.create(place, stage).map_err(io)? {
                        break true;
                    }
                    if let Some(key) = self.key(place).map_err(io)? {
                        replace(key)?;
                        break false;
                    }
                    // Removed since the create found it; create again.
                }
            }
        };
        // The published inode still carries this writer's staging lock, which
        // is now a lock on the object. Release it before anything slow.
        #[cfg(feature = "test-store")]
        super::test_store::sync::stage_locked(&place.path).map_err(io)?;
        stage.file.unlock().map_err(io)?;
        #[cfg(feature = "test-store")]
        super::test_store::sync::stage_unlocked(&place.path).map_err(io)?;
        #[cfg(test)]
        tests::pause("published");
        parent.sync().map_err(io)?;
        if created {
            // Another writer or Lance may have created these directory names
            // without persisting them. A new object depends on the whole chain.
            for directory in place.chain.iter().rev().skip(1) {
                directory.sync().map_err(io)?;
            }
        }
        Ok(PutResult {
            e_tag: Some(version_tag(stage.tag)),
            version: None,
            extensions: Default::default(),
        })
    }

    /// Gives the staged object its name unless the name is taken; atomic, so
    /// it needs no lock. `false` when the object exists.
    fn create(&self, place: &Place, stage: &Stage) -> std::io::Result<bool> {
        use rustix::io::Errno;
        let (stages, staged) = (&stage.directory, stage.name.as_str());
        let (parent, name) = (place.parent(), place.name.as_str());
        match rustix::fs::renameat_with(
            &stages.file,
            staged,
            &parent.file,
            name,
            rustix::fs::RenameFlags::NOREPLACE,
        ) {
            Ok(()) => return Ok(true),
            Err(Errno::EXIST) => return Ok(false),
            // A file system without `RENAME_NOREPLACE`: link, then drop the
            // staging name. Between the two the object has both names, and a
            // reaper that finds the second removes only that name.
            Err(Errno::INVAL | Errno::NOSYS | Errno::OPNOTSUPP) => {}
            Err(error) => return Err(error.into()),
        }
        match rustix::fs::linkat(
            &stages.file,
            staged,
            &parent.file,
            name,
            rustix::fs::AtFlags::empty(),
        ) {
            Ok(()) => {}
            Err(Errno::EXIST) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        stages.unlink(staged)?;
        Ok(true)
    }

    /// Locks the object: the inode its name means now. `None` when there is
    /// no object. A lock obtained on an inode the name no longer means is
    /// worthless, because someone replaced or removed the object while this
    /// writer waited; then the new inode is locked instead. The descriptor
    /// held while waiting keeps the old inode number from being reused, so
    /// the comparison cannot be fooled.
    fn key(&self, place: &Place) -> std::io::Result<Option<Key>> {
        let (parent, name) = (place.parent(), place.name.as_str());
        for _ in 0..ATTEMPTS {
            let file = match rustix::fs::openat(
                &parent.file,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(file) => File::from(file),
                Err(rustix::io::Errno::NOENT) => return Ok(None),
                Err(rustix::io::Errno::LOOP) => {
                    return Err(std::io::Error::other("symlink in storage path"));
                }
                Err(error) => return Err(error.into()),
            };
            let opened = rustix::fs::fstat(&file)?;
            if !is_regular(&opened) {
                return Err(std::io::Error::other("not a regular file"));
            }
            #[cfg(test)]
            tests::hold("opened");
            lock_exclusive(&file)?;
            match parent.entry(name)? {
                Some(named) if (named.st_dev, named.st_ino) == (opened.st_dev, opened.st_ino) => {
                    #[cfg(feature = "test-store")]
                    super::test_store::sync::locked(&place.path)?;
                    return Ok(Some(Key {
                        file,
                        path: place.path.clone(),
                    }));
                }
                Some(_) => {}
                None => return Ok(None),
            }
        }
        Err(contended())
    }

    /// Before a create reports that its object exists: callers treat that
    /// answer as "stored", but the writer that won may not have flushed the
    /// name yet. Flush the object and every directory down to it, so the
    /// answer holds after a power loss as well.
    fn settle_existing(&self, place: &Place) -> std::io::Result<()> {
        match place.parent().object(&place.name) {
            Ok((file, _)) => sync_file(&file, &place.path)?,
            // Deleted since; it did exist when this create looked.
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        for directory in place.chain.iter().rev() {
            directory.sync()?;
        }
        #[cfg(feature = "test-store")]
        super::test_store::sync::existing(&place.path)?;
        Ok(())
    }

    /// The private staging directory of the root; `create` makes it.
    fn stages(&self, root: &Directory, create: bool) -> std::io::Result<Option<Directory>> {
        let open = || {
            rustix::fs::openat(
                &root.file,
                STAGES,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
        };
        let file = match open() {
            Err(rustix::io::Errno::NOENT) if !create => return Ok(None),
            Err(rustix::io::Errno::NOENT) => {
                match rustix::fs::mkdirat(&root.file, STAGES, Mode::RWXU) {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
                root.sync()?;
                open()?
            }
            opened => opened?,
        };
        let file = File::from(file);
        let meta = file.metadata()?;
        if meta.uid() != rustix::process::getuid().as_raw() || meta.mode() & 0o077 != 0 {
            return Err(std::io::Error::other("staging directory is not private"));
        }
        Ok(Some(Directory {
            file,
            path: root.path.join(STAGES),
        }))
    }

    /// A new staging file, named by a new tag and locked by this writer.
    fn stage(&self, root: &Directory) -> std::io::Result<Stage> {
        let directory = self
            .stages(root, true)?
            .expect("the staging directory was created");
        loop {
            let tag = new_tag()?;
            let name = format!("{tag:032x}");
            let file = File::from(rustix::fs::openat(
                &directory.file,
                name.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?);
            // A reaper may have found the file between its creation and this
            // lock and removed it as abandoned. Then stage under a new name.
            lock_exclusive(&file)?;
            if file.metadata()?.nlink() == 0 {
                continue;
            }
            let path = directory.path.join(&name);
            return Ok(Stage {
                directory,
                name,
                file,
                path,
                tag,
            });
        }
    }

    /// Removes the staging files of dead writers: those nobody holds a lock
    /// on. A stopped writer keeps its lock, so its file stays. Anything in
    /// the staging directory that is not a private regular file fails the
    /// caller, which then changes nothing.
    fn reap(&self, root: &Directory) -> std::io::Result<()> {
        let private = |entry: &rustix::fs::Stat| {
            is_regular(entry)
                && entry.st_uid == rustix::process::getuid().as_raw()
                && entry.st_mode & 0o077 == 0
        };
        let foreign = || std::io::Error::other("staging object is not a private regular file");
        // Releases that staged under the root lock cannot write after the
        // handover, so their single staging file has no live writer.
        match root.entry(LEGACY_STAGE)? {
            Some(entry) if private(&entry) => {
                root.unlink(LEGACY_STAGE)?;
                root.sync()?;
            }
            Some(_) => return Err(foreign()),
            None => {}
        }
        let Some(stages) = self.stages(root, false)? else {
            return Ok(());
        };
        let mut removed = false;
        for entry in rustix::fs::Dir::read_from(&stages.file)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if matches!(name.as_str(), "." | "..") {
                continue;
            }
            let file = match rustix::fs::openat(
                &stages.file,
                name.as_str(),
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(file) => File::from(file),
                // Published or removed since the directory was read.
                Err(rustix::io::Errno::NOENT) => continue,
                Err(rustix::io::Errno::LOOP) => return Err(foreign()),
                Err(error) => return Err(error.into()),
            };
            let opened = rustix::fs::fstat(&file)?;
            if !private(&opened) {
                return Err(foreign());
            }
            match file.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => continue,
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
            // Only the name that still means the locked inode is removed.
            if stages
                .entry(&name)?
                .is_some_and(|named| (named.st_dev, named.st_ino) == (opened.st_dev, opened.st_ino))
            {
                match stages.unlink(&name) {
                    Ok(()) => removed = true,
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        if removed {
            stages.sync()?;
        }
        Ok(())
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
            let (root, parent) = (&place.chain[0], place.parent());
            store.hand_over(root).map_err(io)?;
            store.reap(root).map_err(io)?;
            let Some(key) = store.key(&place).map_err(io)? else {
                return Err(io(ErrorKind::NotFound.into()));
            };
            if version_tag(read_version(&key.file).map_err(io)?) != expected {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: "namespace head changed".into(),
                });
            }
            parent.unlink(&place.name).map_err(io)?;
            drop(key);
            #[cfg(feature = "test-store")]
            super::test_store::sync::unlinked(&place.path).map_err(io)?;
            place.without_empty_directories().sync().map_err(io)?;
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
        let (root, parent) = (&place.chain[0], place.parent());
        self.hand_over(root).map_err(io)?;
        if self.cleanup_directories.is_none()
            || !self.reaped.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.reap(root).map_err(io)?;
        }
        let gone = |result: std::io::Result<()>| match result {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        };
        let removed = if self.is_native_file(location) {
            // Lance's files are immutable and never replaced through this
            // store, so there is no comparison to protect.
            match parent.entry(&place.name).map_err(io)? {
                Some(entry) if is_symlink(&entry) => {
                    return Err(io(std::io::Error::other("symlink in storage path")));
                }
                Some(_) => gone(parent.unlink(&place.name)).map_err(io)?,
                None => false,
            }
        } else {
            match self.key(&place).map_err(io)? {
                Some(key) => {
                    let removed = gone(parent.unlink(&place.name)).map_err(io)?;
                    drop(key);
                    removed
                }
                None => false,
            }
        };
        if !removed {
            return Ok(());
        }
        #[cfg(test)]
        tests::pause("removed");
        #[cfg(feature = "test-store")]
        super::test_store::sync::unlinked(&place.path).map_err(io)?;
        let parent = place.without_empty_directories();
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
        } else {
            parent.sync().map_err(io)?;
            #[cfg(feature = "test-store")]
            super::test_store::sync::acknowledged("delete", &place.path).map_err(io)?;
        }
        Ok(())
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

/// Flushes each directory that is an entry of `directory`, except `skip`.
fn sync_children(directory: &Directory, skip: &str) -> std::io::Result<()> {
    for entry in rustix::fs::Dir::read_from(&directory.file)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if matches!(name.as_str(), "." | "..") || name == skip {
            continue;
        }
        match rustix::fs::openat(
            &directory.file,
            name.as_str(),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(file) => sync_file(&File::from(file), &directory.path.join(&name))?,
            // Not a directory, a symlink, or removed since the read.
            Err(rustix::io::Errno::NOTDIR | rustix::io::Errno::LOOP | rustix::io::Errno::NOENT) => {
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn contended() -> std::io::Error {
    std::io::Error::new(
        ErrorKind::WouldBlock,
        "storage object kept changing under this write",
    )
}

/// Polls for an exclusive `flock` on `file` for at most [`LOCK_WAIT`].
/// `flock` cannot be revoked and a waiter cannot tell a stopped holder from
/// a slow one, so the caller fails closed instead of waiting forever.
fn lock_exclusive(file: &File) -> std::io::Result<()> {
    let deadline = std::time::Instant::now() + LOCK_WAIT;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(()),
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
}

/// A tag no other write has: 128 bits from the operating system's generator.
/// A failure to obtain them fails the write.
pub(crate) fn new_tag() -> std::io::Result<u128> {
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
        // The staging directory is no object prefix, like the lock file.
        result
            .common_prefixes
            .retain(|prefix| !self.is_internal(prefix));
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
