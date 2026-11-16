//! Private, disposable SQLite files; no scratch data is restart authority.
use crate::{Result, WriterError};
use fyrr_core::config::Config;
use rusqlite::Connection;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(crate) const ERROR: WriterError = WriterError::RestoreWorkingSpaceExhausted;

/// Called under the namespace lease; purposes have independent lifetimes.
pub(crate) fn directory(config: &Config, purpose: &str) -> Result<PathBuf> {
    let base = base(config)?;
    let scratch = base.join(format!(
        "{}-{purpose}",
        fyrr_reader::embedding::digest(fyrr_reader::storage::dataset_uri(config).as_bytes())
    ));
    match std::fs::remove_dir_all(&scratch) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(ERROR),
    }
    create(&scratch)?;
    Ok(scratch)
}

/// Worker commands do not hold a namespace lease and never remove another index.
pub(crate) fn command_directory(config: &Config) -> Result<PathBuf> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let scratch = base(config)?.join(format!(
        "command-{}-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_nanos_opt().ok_or(ERROR)?,
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    create(&scratch)?;
    Ok(scratch)
}

fn base(config: &Config) -> Result<PathBuf> {
    let base = if let Some(directory) = std::env::var_os("TMPDIR") {
        PathBuf::from(directory)
    } else {
        config
            .mirror_directory(&fyrr_core::config::ProcessEnvironment)
            .and_then(|mirror| mirror.parent().map(Path::to_owned))
            .ok_or(ERROR)?
    }
    .join("fyrr-writer");
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&base)
        .map_err(|_| ERROR)?;
    let owner = std::fs::symlink_metadata(&base).map_err(|_| ERROR)?;
    if !owner.is_dir()
        || owner.mode() & 0o777 != 0o700
        || owner.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(ERROR);
    }
    Ok(base)
}

fn create(scratch: &Path) -> Result<()> {
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(scratch)
        .map_err(|_| ERROR)
}

pub(crate) fn check_space(scratch: &Path, bytes: u64) -> Result<()> {
    let space = rustix::fs::statvfs(scratch).map_err(|_| ERROR)?;
    if space.f_bavail.saturating_mul(space.f_frsize) < bytes {
        return Err(ERROR);
    }
    Ok(())
}

pub(crate) fn open(scratch: &Path, purpose: &str, created: impl Fn(&Path)) -> Result<Connection> {
    let path = scratch.join(format!(
        "fyrr-writer-{purpose}-{}-{}.sqlite",
        std::process::id(),
        fyrr_core::clock::now().timestamp_nanos_opt().ok_or(ERROR)?
    ));
    // Do not let SQLite create a named file while probing a deleted inode.
    let anonymous = rustix::fs::open(
        scratch,
        rustix::fs::OFlags::TMPFILE | rustix::fs::OFlags::RDWR | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_bits_truncate(0o600),
    )
    .ok()
    .and_then(|fd| {
        let opened = Connection::open_with_flags(
            format!("/proc/self/fd/{}", fd.as_raw_fd()),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        );
        if opened.is_ok() {
            created(&path);
        }
        opened.ok()
    });
    if let Some(db) = anonymous {
        return Ok(db);
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .map_err(|_| ERROR)?;
    created(&path);
    let opened = Connection::open(&path);
    let removed = std::fs::remove_file(&path);
    drop(file);
    removed.map_err(|_| ERROR)?;
    opened.map_err(|_| ERROR)
}
