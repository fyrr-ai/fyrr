//! Bounded read-through cache of immutable Lance files. Mutable artifacts bypass it.

use crate::embedding::digest;
use crate::error::{ReaderError, ReaderError::*};
use futures::{StreamExt, stream::BoxStream};
use lance_io::object_store::WrappingObjectStore;
use object_store::Result;
use object_store::list::PaginatedListStore;
use object_store::{ObjectStore, ObjectStoreExt, path::Path, *};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

/// Both the Lance wrapper factory and its read-only wrapped object store.
#[derive(Debug, Clone)]
pub struct MirrorStore {
    #[cfg(test)]
    pub(crate) sample_read: Arc<crate::serve::TestGate>,
    directory: PathBuf,
    max_bytes: u64,
    target: Option<Arc<dyn ObjectStore>>,
    prefix: String,
    lock: Arc<tokio::sync::Mutex<()>>,
    counters: Arc<std::sync::RwLock<Arc<IoCounters>>>,
    totals: Arc<IoCounters>,
    verified: Arc<std::sync::Mutex<BTreeMap<PathBuf, CachedFile>>>,
    dataset_cache_key: Arc<std::sync::Mutex<Option<(String, String)>>>,
    /// The content publication that version manifest entries are bound to.
    publication: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct IoSnapshot {
    pub(crate) requests: u64,
    pub(crate) bytes: u64,
    pub(crate) hits: u64,
}

/// Remote requests, remote bytes and local mirror hits.
#[derive(Debug, Default)]
struct IoCounters {
    requests: AtomicU64,
    bytes: AtomicU64,
    hits: AtomicU64,
}
impl IoCounters {
    fn snapshot(&self) -> IoSnapshot {
        IoSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
        }
    }
}
#[derive(Clone, Copy)]
enum Io {
    Request,
    Bytes,
    Hit,
}

/// A checksum verification memo; changing the local inode or timestamps invalidates it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CachedFile {
    inode: u64,
    size: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl CachedFile {
    fn of(meta: &std::fs::Metadata) -> Self {
        Self {
            inode: meta.ino(),
            size: meta.len(),
            modified: (meta.mtime(), meta.mtime_nsec()),
            changed: (meta.ctime(), meta.ctime_nsec()),
        }
    }
}

type VerificationResult = std::result::Result<bool, Arc<std::io::Error>>;

// Process-wide work ownership survives cancellation of every waiting caller.
struct Verification {
    file: CachedFile,
    checksum: Option<CachedFile>,
    result: tokio::sync::watch::Receiver<Option<VerificationResult>>,
}
struct Verifications {
    permits: Arc<tokio::sync::Semaphore>,
    files: std::sync::Mutex<BTreeMap<PathBuf, Verification>>,
}
impl Verifications {
    const LIMIT: usize = 2;
    fn shared() -> &'static Self {
        static SHARED: std::sync::OnceLock<Verifications> = std::sync::OnceLock::new();
        SHARED.get_or_init(|| Self {
            permits: Arc::new(tokio::sync::Semaphore::new(Self::LIMIT)),
            files: std::sync::Mutex::default(),
        })
    }
    fn forget(path: &std::path::Path) {
        if let Ok(path) = std::path::absolute(path) {
            let mut files = crate::sync::lock(&Self::shared().files);
            // A replacement must join an old hash until its blocking task ends.
            if files
                .get(&path)
                .is_some_and(|v| v.result.borrow().is_some())
            {
                files.remove(&path);
            }
        }
    }
}

/// Derived local reference ownership, never a persistent publication authority.
#[derive(Debug)]
pub(crate) struct MirrorLease {
    mirror: Arc<MirrorStore>,
    // The kernel releases this exclusive lease on process exit, including SIGKILL.
    holder: std::sync::Mutex<Option<Arc<std::fs::File>>>,

    released: AtomicBool,
    retired: AtomicBool,
    pub(crate) expiry: tokio_util::sync::CancellationToken,
}
/// An active request retains the kernel pin independently of idle expiry.
#[derive(Debug)]
pub(crate) struct MirrorRequest {
    holder: Option<Arc<std::fs::File>>,
    lease: Arc<MirrorLease>,
}
impl Drop for MirrorRequest {
    fn drop(&mut self) {
        self.holder.take();
        if (self.lease.retired.load(Ordering::SeqCst) || self.lease.expired())
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let mirror = self.lease.mirror.clone();
            runtime.spawn(async move {
                let _ = mirror.evict_unpinned().await;
            });
        }
    }
}
impl MirrorLease {
    #[cfg(test)]
    pub(crate) fn request_count(&self) -> usize {
        crate::sync::lock(&self.holder)
            .as_ref()
            .map_or(0, |holder| Arc::strong_count(holder) - 1)
    }
    pub(crate) fn request(self: &Arc<Self>) -> Result<MirrorRequest, ReaderError> {
        let holder = self
            .holder
            .lock()
            .map_err(|_| StorageUnavailable)?
            .clone()
            .ok_or(StorageUnavailable)?;
        Ok(MirrorRequest {
            holder: Some(holder),
            lease: self.clone(),
        })
    }
    pub(crate) fn retire(&self) {
        self.retired.store(true, Ordering::SeqCst);
    }
    pub(crate) fn expired(&self) -> bool {
        self.released.load(Ordering::SeqCst)
    }
    pub(crate) fn expire(&self, age: std::time::Duration, horizon: std::time::Duration) {
        if self.retired.load(Ordering::SeqCst) && age >= horizon {
            self.release(true);
        }
    }

    /// An expired pin cannot serve again; release its cache ownership even if preparation fails.
    fn release(&self, expired: bool) {
        if self.released.swap(true, Ordering::SeqCst) {
            return;
        }
        self.expiry.cancel();
        crate::sync::lock(&self.holder).take();
        // Preserve the current publication's warm cache across a standalone CLI
        // exit. A version switch still has another live pin; expiry always evicts.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let mirror = self.mirror.clone();
            runtime.spawn(async move {
                let _ = mirror.sweep(!expired).await;
            });
        }
    }
}

impl Drop for MirrorLease {
    fn drop(&mut self) {
        // Requests retain their Arc even after retention expiry; only the final
        // reference drops the cross-process pin. Retirement is serialized.
        // Leave the immutable inventory for the next serialized sweep.
        self.release(self.expired());
    }
}

impl MirrorStore {
    fn owned_private_file(meta: &std::fs::Metadata) -> bool {
        meta.is_file()
            && !meta.file_type().is_symlink()
            && meta.nlink() == 1
            && meta.uid() == rustix::process::getuid().as_raw()
            && meta.permissions().mode() & 0o077 == 0
    }
    fn cache_name(name: &str, suffix: &str) -> bool {
        name.strip_suffix(suffix)
            .is_some_and(|hash| hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
    }
    /// Lance names version manifests by position (`_versions/{n}.manifest`), so a
    /// namespace recreated at the same URI reuses their names for other bytes. Their
    /// entries are bound to the content publication that pins them, and are not
    /// cached (`None`) without one. Every other immutable name is unique to the
    /// write that created it and is reached only through such a manifest.
    fn blob_name(&self, prefix: &str, path: &str) -> Option<String> {
        let key = if self.version_manifest(path) {
            format!("{prefix}:{path}:{}", self.publication.as_deref()?)
        } else {
            format!("{prefix}:{path}")
        };
        Some(format!("{}.blob", &digest(key.as_bytes())[7..]))
    }
    fn version_manifest(&self, path: &str) -> bool {
        let versions = fyrr_core::layout::NamespaceLayout::VERSIONS;
        match crate::sync::lock(&self.dataset_cache_key).as_ref() {
            Some((_, root)) => path
                .strip_prefix(root.as_str())
                .and_then(|relative| relative.strip_prefix('/'))
                .is_some_and(|relative| relative.starts_with(versions)),
            None => path.contains(&format!("/{versions}")),
        }
    }
    async fn pass_through(
        &self,
        location: &Path,
        options: GetOptions,
        counters: &IoCounters,
    ) -> Result<GetResult> {
        self.record(counters, Io::Request, 1);
        let head = options.head;
        let result = self.target().get_opts(location, options).await?;
        self.record(
            counters,
            Io::Bytes,
            if head {
                0
            } else {
                result.range.end - result.range.start
            },
        );
        Ok(result)
    }

    fn record(&self, counters: &IoCounters, io: Io, amount: u64) {
        for counters in [counters, &*self.totals] {
            let counter = match io {
                Io::Request => &counters.requests,
                Io::Bytes => &counters.bytes,
                Io::Hit => &counters.hits,
            };
            counter.fetch_add(amount, Ordering::Relaxed);
        }
    }
    pub(crate) fn totals(&self) -> IoSnapshot {
        self.totals.snapshot()
    }
    pub fn private_directory(directory: &std::path::Path) -> std::io::Result<()> {
        match std::fs::symlink_metadata(directory) {
            Ok(meta)
                if meta.is_dir()
                    && !meta.file_type().is_symlink()
                    && meta.uid() == rustix::process::getuid().as_raw()
                    && meta.permissions().mode() & 0o077 == 0 =>
            {
                Ok(())
            }
            Ok(_) => Err(std::io::Error::other("storage_unavailable")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(directory)?;
                Self::private_directory(directory)
            }
            Err(error) => Err(error),
        }
    }
    pub fn new(directory: PathBuf, max_bytes: u64) -> Result<Self, ReaderError> {
        Self::private_directory(&directory).map_err(|_| StorageUnavailable)?;
        let guard = Self::private_file(&directory.join("mirror.lock"), true)
            .map_err(|_| StorageUnavailable)?;
        match guard.try_lock() {
            Ok(()) => {
                for entry in std::fs::read_dir(&directory).map_err(|_| StorageUnavailable)? {
                    let entry = entry.map_err(|_| StorageUnavailable)?;
                    let meta =
                        std::fs::symlink_metadata(entry.path()).map_err(|_| StorageUnavailable)?;
                    if !Self::owned_private_file(&meta) {
                        return Err(StorageUnavailable);
                    }
                    if Self::cache_name(&entry.file_name().to_string_lossy(), ".partial") {
                        Self::remove_partial(&entry.path()).map_err(|_| StorageUnavailable)?;
                    }
                }
            }
            Err(std::fs::TryLockError::WouldBlock) => {} // A live download owns its partial.
            Err(_) => return Err(StorageUnavailable),
        }
        Ok(Self {
            #[cfg(test)]
            sample_read: Arc::default(),
            directory,
            max_bytes,
            target: None,
            prefix: String::new(),
            lock: Arc::default(),
            counters: Arc::default(),
            totals: Arc::default(),
            verified: Arc::default(),
            dataset_cache_key: Arc::default(),
            publication: None,
        })
    }
    /// The same mirror with its version manifest entries bound to `manifest`.
    pub(crate) fn for_publication(
        &self,
        manifest: &fyrr_core::publication::ContentManifest,
    ) -> Self {
        let mut bound = self.clone();
        bound.publication = Some(digest(&fyrr_core::write_contract(manifest)));
        bound
    }
    /// Bind the existing native cache filename convention, including files left by an
    /// earlier process.
    pub(crate) fn bind_dataset(&self, uri: &str) -> Result<(), ReaderError> {
        let key = match uri.strip_prefix("s3://") {
            Some(rest) => rest
                .split_once('/')
                .map(|(bucket, path)| (format!("s3${bucket}"), path.to_owned())),
            // Lance roots a local store at `/` and names it by its scheme.
            None => uri.strip_prefix("file-object-store://").and_then(|path| {
                let url = url::Url::parse(&format!("file://{path}")).ok()?;
                let path = Path::from_absolute_path(url.to_file_path().ok()?).ok()?;
                Some(("file-object-store".to_owned(), path.to_string()))
            }),
        };
        *crate::sync::lock(&self.dataset_cache_key) = Some(key.ok_or(StorageConfigurationInvalid)?);
        Ok(())
    }
    // O_NOFOLLOW and descriptor metadata checks apply to every coordination file.
    fn private_file(path: &std::path::Path, create: bool) -> std::io::Result<std::fs::File> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(path)?;
        if !Self::owned_private_file(&file.metadata()?) {
            return Err(std::io::Error::other("storage_unavailable"));
        }
        Ok(file)
    }
    async fn host_lock(&self) -> std::io::Result<std::fs::File> {
        let file = Self::private_file(&self.directory.join("mirror.lock"), true)?;
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
        }
    }
    fn live_files(&self) -> std::io::Result<std::collections::BTreeSet<String>> {
        let mut live = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            if !Self::owned_private_file(&std::fs::symlink_metadata(entry.path())?) {
                return Err(std::io::Error::other("storage_unavailable"));
            }
            if !Self::cache_name(&entry.file_name().to_string_lossy(), ".lease") {
                continue;
            }
            let mut file = Self::private_file(&entry.path(), false)?;
            match file.try_lock() {
                Ok(()) => std::fs::remove_file(entry.path())?,
                Err(std::fs::TryLockError::WouldBlock) => {
                    // Immutable, derived pin inventory, never publication or authorization state.
                    let paths: Vec<String> = serde_json::from_reader(&mut file)?;
                    if paths.iter().any(|p| !Self::cache_name(p, ".blob")) {
                        return Err(std::io::Error::other("storage_unavailable"));
                    }
                    live.extend(paths);
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
        }
        Ok(live)
    }
    pub(crate) async fn pin(
        self: &Arc<Self>,
        files: &[fyrr_core::envelope::FileRef],
    ) -> std::io::Result<Arc<MirrorLease>> {
        static SERIAL: AtomicU64 = AtomicU64::new(0);

        let _host = self.host_lock().await?;
        let (prefix, root) = crate::sync::lock(&self.dataset_cache_key)
            .clone()
            .ok_or_else(|| std::io::Error::other("storage_unavailable"))?;
        let paths: Vec<_> = files.iter().map(|f| f.path.clone()).collect();
        let blobs = paths
            .iter()
            .map(|p| self.blob_name(&prefix, &format!("{root}/{p}")))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| std::io::Error::other("storage_unavailable"))?;

        let name = digest(
            format!(
                "{}:{}:{}",
                std::process::id(),
                fyrr_core::clock::now()
                    .timestamp_nanos_opt()
                    .unwrap_or_default(),
                SERIAL.fetch_add(1, Ordering::Relaxed)
            )
            .as_bytes(),
        );
        let lease_path = self.directory.join(format!("{}.lease", &name[7..]));
        let mut holder = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(&lease_path)?;
        if !Self::owned_private_file(&holder.metadata()?) {
            return Err(std::io::Error::other("storage_unavailable"));
        }
        holder.lock()?;
        serde_json::to_writer(&mut holder, &blobs)?;
        holder.sync_all()?;
        Ok(Arc::new(MirrorLease {
            mirror: self.clone(),
            holder: std::sync::Mutex::new(Some(Arc::new(holder))),
            released: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            expiry: tokio_util::sync::CancellationToken::new(),
        }))
    }
    pub(crate) async fn evict_unpinned(&self) -> std::io::Result<()> {
        self.sweep(false).await
    }
    async fn sweep(&self, preserve_last_cache: bool) -> std::io::Result<()> {
        let _lock = self.lock.lock().await;
        let _host = self.host_lock().await?;
        let allowed = self.live_files()?;
        if preserve_last_cache && allowed.is_empty() {
            return Ok(());
        }
        let mut expired = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            // The host lock excludes every live download. Cancelled and crashed
            // partials can therefore be removed without following any links.
            if Self::cache_name(&name, ".partial") {
                Self::remove_partial(&entry.path())?;
            }
            if Self::cache_name(&name, ".blob") && !allowed.contains(&name) {
                expired.insert(entry.path());
            }
        }
        for file in expired {
            if let Ok(meta) = std::fs::symlink_metadata(&file) {
                if !Self::owned_private_file(&meta) {
                    return Err(std::io::Error::other("storage_unavailable"));
                }
                std::fs::remove_file(&file)?;
            }
            Self::remove_partial(&file.with_extension("checksum"))?;
            crate::sync::lock(&self.verified).remove(&file);
            Verifications::forget(&file);
        }
        Ok(())
    }
    fn remove_partial(file: &std::path::Path) -> std::io::Result<()> {
        match std::fs::symlink_metadata(file) {
            Ok(meta) if Self::owned_private_file(&meta) => std::fs::remove_file(file),
            Ok(_) => Err(std::io::Error::other("storage_unavailable")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
    /// Separate a worker's counters while sharing the immutable-file miss lock.
    pub fn worker(&self) -> Self {
        let mut worker = self.clone();
        worker.counters = Arc::default();
        worker
    }
    /// Keep completions of earlier reads out of the next request's receipt.
    pub fn start_request(&self) {
        *self.counters.write().expect("counter scope lock") = Arc::default();
    }
    pub(crate) fn counters(&self) -> IoSnapshot {
        self.counters.read().expect("counter scope lock").snapshot()
    }
    fn target(&self) -> &dyn ObjectStore {
        let Some(target) = self.target.as_deref() else {
            unreachable!("native storage access requires a wrapped store");
        };
        target
    }
    fn failure() -> Error {
        Error::Generic {
            store: "mirror",
            source: "storage_unavailable".into(),
        }
    }
    fn file_fingerprint(path: &std::path::Path) -> std::io::Result<Option<CachedFile>> {
        match std::fs::symlink_metadata(path) {
            Ok(meta) if Self::owned_private_file(&meta) => Ok(Some(CachedFile::of(&meta))),
            Ok(_) => Err(std::io::Error::other("storage_unavailable")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
    fn verified_file_blocking(
        &self,
        path: &std::path::Path,
        fingerprint: &CachedFile,
        checksum: Option<&CachedFile>,
    ) -> std::io::Result<bool> {
        let mut file = Self::private_file(path, false)?;
        if CachedFile::of(&file.metadata()?) != *fingerprint {
            return Ok(false);
        }
        let proof = match Self::private_file(&path.with_extension("checksum"), false) {
            Ok(mut proof) => {
                let meta = proof.metadata()?;
                if meta.len() != 64 || Some(&CachedFile::of(&meta)) != checksum {
                    return Ok(false);
                }
                let mut text = String::new();
                if proof.read_to_string(&mut text).is_err() {
                    return Ok(false);
                }
                text
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        if proof != crate::file_digest::sha256(&mut file)?
            || CachedFile::of(&file.metadata()?) != *fingerprint
            || Self::file_fingerprint(&path.with_extension("checksum"))?.as_ref() != checksum
        {
            return Ok(false);
        }
        crate::sync::lock(&self.verified).insert(path.to_owned(), fingerprint.clone());
        Ok(true)
    }
    async fn verified_file(&self, path: &std::path::Path) -> std::io::Result<bool> {
        loop {
            let Some(fingerprint) = Self::file_fingerprint(path)? else {
                return Ok(false);
            };
            // The common immutable-file hit never enters the blocking pool.
            if crate::sync::lock(&self.verified).get(path) == Some(&fingerprint) {
                return Ok(true);
            }
            let checksum = Self::file_fingerprint(&path.with_extension("checksum"))?;
            let key = std::path::absolute(path)?;
            let (mut receiver, same_fingerprint) = {
                let pool = Verifications::shared();
                let mut files = crate::sync::lock(&pool.files);
                if let Some(v) = files.get(&key)
                    && (v.result.borrow().is_none()
                        || (v.file == fingerprint && v.checksum == checksum))
                {
                    (
                        v.result.clone(),
                        v.file == fingerprint && v.checksum == checksum,
                    )
                } else {
                    let (sender, receiver) = tokio::sync::watch::channel(None);
                    files.insert(
                        key,
                        Verification {
                            file: fingerprint.clone(),
                            checksum: checksum.clone(),
                            result: receiver.clone(),
                        },
                    );
                    let mirror = self.clone();
                    let path = path.to_owned();
                    let fingerprint = fingerprint.clone();
                    let checksum = checksum.clone();
                    // This task, and then the blocking task, own the work. Dropping
                    // a caller cannot release the permit or launch another hash.
                    tokio::spawn(async move {
                        let Ok(permit) = pool.permits.clone().acquire_owned().await else {
                            unreachable!("the shared verification pool is never closed");
                        };
                        let result = tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            mirror.verified_file_blocking(&path, &fingerprint, checksum.as_ref())
                        })
                        .await
                        .map_err(std::io::Error::other)
                        .and_then(|r| r)
                        .map_err(Arc::new);
                        sender.send_replace(Some(result));
                    });
                    (receiver, true)
                }
            };
            while receiver.borrow().is_none() {
                receiver.changed().await.map_err(std::io::Error::other)?;
            }
            // A caller for a replacement joins the old task, then verifies the
            // new fingerprint rather than accepting an old result for new bytes.
            if !same_fingerprint
                || Self::file_fingerprint(path)?.as_ref() != Some(&fingerprint)
                || Self::file_fingerprint(&path.with_extension("checksum"))? != checksum
            {
                continue;
            }
            let Some(result) = receiver.borrow().clone() else {
                unreachable!("the verification task has completed");
            };
            if matches!(result, Ok(true)) {
                crate::sync::lock(&self.verified).insert(path.to_owned(), fingerprint);
            }
            return result.map_err(std::io::Error::other);
        }
    }
    pub(crate) async fn prewarm(
        &self,
        store: Arc<dyn ObjectStore>,
        files: &[fyrr_core::envelope::FileRef],
    ) -> Result<(), ReaderError> {
        let (prefix, root) = crate::sync::lock(&self.dataset_cache_key)
            .clone()
            .ok_or(StorageUnavailable)?;
        let store = self.wrap(&prefix, store);
        for file in files {
            let path = Path::from(format!("{root}/{}", file.path));
            store
                .get_opts(
                    &path,
                    GetOptions {
                        head: true,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| StorageUnavailable)?;
            let blob = self.directory.join(
                self.blob_name(&prefix, path.as_ref())
                    .ok_or(StorageUnavailable)?,
            );
            if !self
                .verified_file(&blob)
                .await
                .map_err(|_| StorageUnavailable)?
            {
                return Err(MirrorCapacityExhausted);
            }
        }
        Ok(())
    }
    fn readonly() -> Error {
        Error::NotSupported {
            source: "reader store is read-only".into(),
        }
    }
    fn immutable(&self, path: &Path) -> bool {
        if self.prefix == "control" {
            return false;
        }
        let path = path.as_ref();
        let key = crate::sync::lock(&self.dataset_cache_key);
        if let Some((_, root)) = key.as_ref() {
            return path
                .strip_prefix(root)
                .and_then(|relative| relative.strip_prefix('/'))
                .is_some_and(fyrr_core::layout::NamespaceLayout::immutable_relative_path);
        }
        fyrr_core::layout::NamespaceLayout::immutable_native_path(path)
    }
}

mod backend;

#[cfg(test)]
#[path = "mirror/tests.rs"]
mod tests;
