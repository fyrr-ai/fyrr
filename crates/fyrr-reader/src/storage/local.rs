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
//! - a durable, monotonically increasing counter on the lock inode assigns
//!   each write a version, stored as an xattr on its staging inode before
//!   publication. Versions survive crashes and never depend on timestamps;
//! - an object and its version xattr are flushed on a staging inode, then
//!   published by `rename` (replace) or `link` (create only). Native files are
//!   persisted explicitly before the contracts that reference them. Finally,
//!   its directory is flushed. A crash leaves the old object or the new one.
//!
//! - the single reserved staging file is reaped under the lock before every
//!   mutation, so a killed writer cannot leave unlisted data through a purge.
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
use std::fs::{File, Metadata, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const VERSION: &str = "user.fyrr.version";

const STORE: &str = "fyrr local storage";
/// In the root, outside `namespaces/`, so no namespace listing returns it.
const LOCK: &str = ".fyrr-storage.lock";
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
    lock: PathBuf,
    directory: Arc<File>,
    root_identity: (u64, u64),
    cleanup_directories: Option<Arc<std::sync::Mutex<std::collections::BTreeSet<PathBuf>>>>,
}

impl std::fmt::Display for LocalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(STORE)
    }
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
            lock: root.join(LOCK),
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
        let mut directories = std::collections::BTreeSet::new();
        for file in files {
            let location = Path::from(format!("{dataset}/{}", file.path));
            if location.prefix_match(dataset).is_none() || !self.is_native_file(&location) {
                return Err(std::io::Error::other("native file outside dataset"));
            }
            let target = self.target(&location).map_err(std::io::Error::other)?;
            let (opened, metadata) = open_object(&target)?;
            if metadata.len() != file.size_bytes {
                return Err(std::io::Error::other("native file size changed"));
            }
            opened.sync_all()?;
            let mut parent = target.parent();
            while let Some(directory) =
                parent.filter(|directory| directory.starts_with(self.lock.parent().expect("root")))
            {
                directories.insert(directory.to_path_buf());
                parent = directory.parent();
            }
        }
        // Descendants before ancestors, including newly created native directories.
        for directory in directories.into_iter().rev() {
            File::open(directory)?.sync_all()?;
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
            for directory in directories.iter() {
                File::open(directory)
                    .and_then(|file| file.sync_all())
                    .map_err(|error| failure(directory, error))?;
            }
            directories.clear();
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

    /// The exclusive lock every mutation holds; released when the file closes,
    /// also when the process dies.
    fn lock(&self) -> std::io::Result<File> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(
                (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
            )
            .open(&self.lock)?;
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.uid() != rustix::process::getuid().as_raw()
            || meta.mode() & 0o077 != 0
        {
            return Err(std::io::Error::other("storage lock is not private"));
        }
        file.lock()?;
        if file.metadata()?.len() == 0 {
            match read_version(&file) {
                Ok(0) => {} // A crash before the initialization marker was written.
                Err(error)
                    if error.raw_os_error() == Some(rustix::io::Errno::NODATA.raw_os_error()) =>
                {
                    // Objects are written only after this file exists. A new
                    // counter beside them would repeat their versions.
                    if self
                        .lock
                        .with_file_name("namespaces")
                        .symlink_metadata()
                        .is_ok()
                    {
                        return Err(std::io::Error::other(
                            "storage counter is missing beside existing objects",
                        ));
                    }
                    set_version(&file, 0)?;
                }
                _ => {
                    return Err(std::io::Error::other(
                        "invalid storage counter initialization",
                    ));
                }
            }
            file.write_all(b"fyrr local version counter\n")?;
            file.sync_all()?;
            sync_parent(&self.lock)?;
        }
        // Missing or damaged counter metadata must never reset the sequence.
        read_version(&file)?;
        Ok(file)
    }

    fn put_blocking(
        &self,
        location: &Path,
        payload: PutPayload,
        mode: PutMode,
    ) -> Result<PutResult> {
        let target = self.target(location)?;
        let io = |error| failure(&target, error);
        let lock = self.lock().map_err(io)?;
        self.reap_stage().map_err(io)?;
        let parent = target
            .parent()
            .ok_or_else(|| io(std::io::Error::other("object has no directory")))?;
        let mut missing = Vec::new();
        let mut ancestor = parent;
        while !ancestor.try_exists().map_err(io)? {
            missing.push(ancestor.to_path_buf());
            ancestor = ancestor
                .parent()
                .ok_or_else(|| io(std::io::Error::other("missing storage root")))?;
        }
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)
            .map_err(io)?;
        // Make new directory names durable before publishing an object below them.
        for directory in missing.iter().rev() {
            sync_parent(directory).map_err(io)?;
        }
        let staged = self.lock.with_file_name(STAGE);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(&staged)
            .map_err(io)?;
        let result = self.publish(&target, &mut file, &staged, &payload, mode, &lock);
        if result.is_err() {
            let _ = std::fs::remove_file(&staged);
        }
        result
    }

    fn publish(
        &self,
        target: &std::path::Path,
        file: &mut File,
        staged: &std::path::Path,
        payload: &PutPayload,
        mode: PutMode,
        lock: &File,
    ) -> Result<PutResult> {
        let io = |error| failure(target, error);
        for chunk in payload.iter() {
            file.write_all(chunk).map_err(io)?;
        }
        #[cfg(test)]
        tests::pause("staged");
        #[cfg(test)]
        tests::pause("locked");
        let current = match std::fs::symlink_metadata(target) {
            Ok(meta) if meta.is_file() => Some(meta),
            Ok(_) => return Err(io(std::io::Error::other("not a regular file"))),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(io(error)),
        };
        let path = target.display().to_string();
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
                            read_version(&open_object(target).map_err(io)?.0).map_err(io)?,
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
        let next = read_version(lock)
            .map_err(io)?
            .checked_add(1)
            .ok_or_else(|| io(std::io::Error::other("version counter exhausted")))?;
        set_version(lock, next).map_err(io)?;
        // Persist the reservation before any object can expose this version.
        lock.sync_all().map_err(io)?;
        set_version(file, next).map_err(io)?;
        file.sync_all().map_err(io)?;
        #[cfg(test)]
        tests::pause("versioned");
        if matches!(mode, PutMode::Create) {
            std::fs::hard_link(staged, target).map_err(io)?;
            // Publication succeeded, but a cleanup error still fails the call.
            std::fs::remove_file(staged).map_err(io)?;
        } else {
            std::fs::rename(staged, target).map_err(io)?;
        }
        sync_parent(target).map_err(io)?;
        sync_parent(staged).map_err(io)?;
        Ok(PutResult {
            e_tag: Some(version_tag(next)),
            version: None,
            extensions: Default::default(),
        })
    }

    /// Only a dead writer can leave this file while another writer holds
    /// the lock. Reap it before *any* mutation, so purge cannot leave behind
    /// unlisted temporary objects containing retired content.
    fn reap_stage(&self) -> std::io::Result<()> {
        let staged = self.lock.with_file_name(STAGE);
        match std::fs::symlink_metadata(&staged) {
            Ok(meta)
                if meta.is_file()
                    && meta.uid() == rustix::process::getuid().as_raw()
                    && meta.mode() & 0o077 == 0 =>
            {
                std::fs::remove_file(&staged)?;
                sync_parent(&staged)
            }
            Ok(_) => Err(std::io::Error::other(
                "staging object is not a private regular file",
            )),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Keep all object paths within this root and reject symlinks in every
    /// existing component. The private root is owned by the cooperating user;
    /// external mutation of its directory tree is unsupported.
    fn target(&self, location: &Path) -> Result<PathBuf> {
        if location.prefix_match(&self.root).is_none() || self.is_internal(location) {
            return Err(unsupported("path outside the namespace"));
        }
        let target = self.inner.path_to_filesystem(location)?;
        // The handle stays bound to the opened root. Refuse path replacement;
        // absolute-path I/O must still resolve to that same private directory.
        let root_path = self.lock.parent().expect("storage root");
        let current_root =
            std::fs::symlink_metadata(root_path).map_err(|error| failure(&target, error))?;
        if !current_root.is_dir() || (current_root.dev(), current_root.ino()) != self.root_identity
        {
            return Err(failure(
                &target,
                std::io::Error::other("storage root changed"),
            ));
        }
        let relative = target
            .strip_prefix(self.lock.parent().expect("storage root"))
            .map_err(|_| unsupported("path outside the namespace"))?;
        let mut relative_ancestor = PathBuf::new();
        let mut components = relative.components().peekable();
        while let Some(component) = components.next() {
            let std::path::Component::Normal(name) = component else {
                return Err(unsupported("path outside the namespace"));
            };
            relative_ancestor.push(name);
            // Every prefix is checked in this operation. The root descriptor
            // anchors lookup, and NOFOLLOW rejects each prefix's final symlink.
            match rustix::fs::statat(
                &self.directory,
                &relative_ancestor,
                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            ) {
                Ok(metadata) => {
                    let kind = rustix::fs::FileType::from_raw_mode(metadata.st_mode);
                    if kind == rustix::fs::FileType::Symlink {
                        return Err(failure(
                            &target,
                            std::io::Error::other("symlink in storage path"),
                        ));
                    }
                    if components.peek().is_some() && kind != rustix::fs::FileType::Directory {
                        return Err(failure(
                            &target,
                            std::io::Error::from(ErrorKind::NotADirectory),
                        ));
                    }
                }
                Err(rustix::io::Errno::NOENT) => break,
                Err(error) => return Err(failure(&target, error.into())),
            }
        }
        Ok(target)
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
        let target = self.target(&location)?;
        let io = |error| failure(&target, error);
        let (file, metadata) = open_object(&target).map_err(io)?;
        let mut meta = ObjectMeta {
            location,
            last_modified: metadata.modified().map_err(io)?.into(),
            size: metadata.len(),
            e_tag: Some(e_tag(&metadata)),
            version: None,
        };
        // Lance's immutable files are written by LocalFileSystem directly.
        // Only our control objects carry the durable counter version. Missing
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
            payload: GetResultPayload::File(file, target),
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
        for object in listed {
            if self.is_internal(&object.location) {
                continue;
            }
            if self.is_native_file(&object.location) {
                // Native files use the listing metadata and have no version xattr.
                // Validate containment for this listing operation, without reopening
                // every immutable file merely to reconstruct the same metadata.
                match self.target(&object.location) {
                    Ok(_) => objects.push(object),
                    Err(Error::NotFound { .. }) => {}
                    Err(error) => return Err(error),
                }
                continue;
            }
            match self.get_blocking(object.location, GetOptions::default()) {
                Ok(read) => objects.push(read.meta),
                Err(Error::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(objects)
    }

    pub(super) async fn delete_if_match(&self, location: Path, expected: String) -> Result<()> {
        self.blocking(move |store| {
            let target = store.target(&location)?;
            let io = |error| failure(&target, error);
            let _lock = store.lock().map_err(io)?;
            store.reap_stage().map_err(io)?;
            let (file, _) = open_object(&target).map_err(io)?;
            if version_tag(read_version(&file).map_err(io)?) != expected {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: "namespace head changed".into(),
                });
            }
            std::fs::remove_file(&target).map_err(io)?;
            sync_parent(&target).map_err(io)
        })
        .await
    }

    /// Deleting a missing object succeeds, as on S3.
    fn delete_blocking(&self, location: &Path) -> Result<()> {
        let target = self.target(location)?;
        let io = |error| failure(&target, error);
        let _lock = self.lock().map_err(io)?;
        self.reap_stage().map_err(io)?;
        match std::fs::remove_file(&target) {
            Ok(()) => {
                if let Some(directories) = &self.cleanup_directories {
                    directories
                        .lock()
                        .expect("cleanup directories lock")
                        .insert(target.parent().expect("object directory").to_path_buf());
                    #[cfg(test)]
                    tests::pause("unlinked");
                    Ok(())
                } else {
                    sync_parent(&target).map_err(io)
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

fn open_object(path: &std::path::Path) -> std::io::Result<(File, Metadata)> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(std::io::Error::other("not a regular file"));
    }
    Ok((file, metadata))
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

fn sync_parent(target: &std::path::Path) -> std::io::Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| std::io::Error::other("object has no directory"))?;
    File::open(parent)?.sync_all()
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
            self.target(prefix)?;
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
