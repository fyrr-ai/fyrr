//! Private, disposable SQLite files; no scratch data is restart authority.
use crate::{Result, WriterError};
use fyrr_core::config::Config;
use rusqlite::Connection;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(crate) const ERROR: WriterError = WriterError::RestoreWorkingSpaceExhausted;

pub(crate) mod index;

/// A sync worker holds no namespace lease and never removes another index.
pub(crate) fn command_directory(config: &Config) -> Result<PathBuf> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let base = base(config)?;
    reap_dead_commands(&base)?;
    let scratch = base.join(format!(
        "command-{}-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_nanos_opt().ok_or(ERROR)?,
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    create(&scratch)?;
    Ok(scratch)
}

fn reap_dead_commands(base: &Path) -> Result<()> {
    for entry in std::fs::read_dir(base).map_err(|_| ERROR)? {
        let entry = entry.map_err(|_| ERROR)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let parts: Vec<_> = name.split('-').collect();
        if parts.len() != 4
            || parts[0] != "command"
            || parts[2].parse::<i64>().is_err()
            || parts[3].parse::<u64>().is_err()
        {
            continue;
        }
        let Ok(pid) = parts[1].parse::<u32>() else {
            continue;
        };
        if pid == 0 {
            continue;
        }
        // PID reuse conservatively retains a directory until that process exits.
        match std::fs::symlink_metadata(format!("/proc/{pid}")) {
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => continue,
        }
        let meta = match entry.path().symlink_metadata() {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(ERROR),
        };
        if meta.is_dir()
            && meta.uid() == rustix::process::geteuid().as_raw()
            && meta.mode() & 0o077 == 0
        {
            match std::fs::remove_dir_all(entry.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(ERROR),
            }
        }
    }
    Ok(())
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

pub(crate) fn open(scratch: &Path) -> Result<Connection> {
    let path = scratch.join(format!(
        "fyrr-writer-history-{}-{}.sqlite",
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
        Connection::open_with_flags(
            format!("/proc/self/fd/{}", fd.as_raw_fd()),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )
        .ok()
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
    let opened = Connection::open(&path);
    let removed = std::fs::remove_file(&path);
    drop(file);
    removed.map_err(|_| ERROR)?;
    opened.map_err(|_| ERROR)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn command_start_reaps_dead_owners_and_preserves_live_owners() {
        let (config, root) = crate::lease::tests::local_config("scratch-reap");
        let base = base(&config).unwrap();
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        let dead = base.join(format!("command-{pid}-1-0"));
        let live = base.join(format!("command-{}-2-0", std::process::id()));
        create(&dead).unwrap();
        create(&live).unwrap();
        let own = command_directory(&config).unwrap();
        assert!(!dead.exists());
        assert!(live.exists());
        std::fs::remove_dir(live).unwrap();
        std::fs::remove_dir(own).unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
