//! Exact command-local evidence of a sync worker. SQL failure invalidates the
//! entire disposable index.
use super::{ERROR, check_space};
use crate::Result;
use fyrr_core::config::Config;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Clone, Copy)]
pub(crate) enum Evidence {
    Preparation,
    PendingRevision,
}

#[derive(Debug)]
pub(crate) struct Index {
    db: Mutex<Option<Connection>>,
    scratch: PathBuf,
    remove_directory: bool,
}
impl Drop for Index {
    fn drop(&mut self) {
        if let Ok(db) = self.db.get_mut() {
            *db = None;
        }
        if self.remove_directory {
            let _ = std::fs::remove_dir(&self.scratch);
        }
    }
}
impl Index {
    pub(crate) fn command(config: &Config) -> Result<Self> {
        Self::open(super::command_directory(config)?, true)
    }

    fn open(scratch: PathBuf, remove_directory: bool) -> Result<Self> {
        let result = Self::open_inner(scratch.clone(), remove_directory);
        if result.is_err() && remove_directory {
            let _ = std::fs::remove_dir_all(scratch);
        }
        result
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.db.lock().is_ok_and(|db| db.is_some())
    }

    #[cfg(test)]
    pub(crate) fn invalidate(&self) {
        let _: Result<()> = self.with_db(|_| Err(ERROR));
    }

    fn open_inner(scratch: PathBuf, remove_directory: bool) -> Result<Self> {
        check_space(&scratch, 8 * 1024 * 1024)?;
        let db = super::open(&scratch)?;
        db.execute_batch(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
             PRAGMA locking_mode=EXCLUSIVE; PRAGMA page_size=4096;
             PRAGMA cache_size=-2048; PRAGMA temp_store=FILE;
             PRAGMA max_page_count=2048;
             CREATE TABLE evidence(kind INTEGER, key TEXT, value BLOB,
                 PRIMARY KEY(kind,key)) WITHOUT ROWID;",
        )
        .map_err(|_| ERROR)?;
        Ok(Self {
            db: Mutex::new(Some(db)),
            scratch,
            remove_directory,
        })
    }

    // Guards and statements never escape this synchronous boundary or cross an await.
    fn with_db<T>(&self, operation: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let mut opened = self.db.lock().map_err(|_| ERROR)?;
        let result = operation(opened.as_ref().ok_or(ERROR)?);
        if result.is_err() {
            *opened = None;
            if self.remove_directory {
                let _ = std::fs::remove_dir_all(&self.scratch);
            }
        }
        result
    }

    fn reserve(&self, db: &Connection, bytes: usize) -> Result<()> {
        let pages: u32 = db
            .pragma_query_value(None, "page_count", |row| row.get(0))
            .map_err(|_| ERROR)?;
        let cap: u32 = db
            .pragma_query_value(None, "max_page_count", |row| row.get(0))
            .map_err(|_| ERROR)?;
        let needed = u32::try_from(bytes / 4096 + 1)
            .map_err(|_| ERROR)?
            .checked_mul(2)
            .and_then(|n| n.checked_add(16))
            .ok_or(ERROR)?;
        let required = pages.checked_add(needed).ok_or(ERROR)?;
        if required >= cap {
            let next = required
                .checked_add(2048)
                .filter(|n| *n < u32::MAX)
                .ok_or(ERROR)?;
            check_space(&self.scratch, u64::from(next - cap) * 4096)?;
            db.pragma_update(None, "max_page_count", next)
                .map_err(|_| ERROR)?;
        }
        Ok(())
    }

    pub(crate) fn get<T: DeserializeOwned>(&self, kind: Evidence, key: &str) -> Result<Option<T>> {
        self.with_db(|db| {
            let value: Option<Vec<u8>> = db
                .query_row(
                    "SELECT value FROM evidence WHERE kind=?1 AND key=?2",
                    params![kind as i64, key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|_| ERROR)?;
            value
                .map(|bytes| serde_json::from_slice(&bytes).map_err(|_| ERROR))
                .transpose()
        })
    }

    pub(crate) fn put(&self, kind: Evidence, key: &str, value: &impl Serialize) -> Result<()> {
        let bytes = serde_json::to_vec(value).map_err(|_| ERROR)?;
        self.with_db(|db| {
            self.reserve(db, key.len().checked_add(bytes.len()).ok_or(ERROR)?)?;
            db.execute(
                "INSERT OR IGNORE INTO evidence VALUES (?1,?2,?3)",
                params![kind as i64, key, bytes],
            )
            .map_err(|_| ERROR)?;
            Ok(())
        })
    }

    pub(crate) fn replace(&self, kind: Evidence, key: &str, value: &impl Serialize) -> Result<()> {
        let bytes = serde_json::to_vec(value).map_err(|_| ERROR)?;
        self.with_db(|db| {
            self.reserve(db, key.len().checked_add(bytes.len()).ok_or(ERROR)?)?;
            db.execute("INSERT INTO evidence VALUES (?1,?2,?3) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value", params![kind as i64, key, bytes]).map_err(|_| ERROR)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> (Index, std::path::PathBuf) {
        let (config, root) = crate::lease::tests::local_config("scratch-index");
        (Index::command(&config).unwrap(), root)
    }

    #[test]
    fn sql_failure_discards_all_cached_evidence() {
        let (index, root) = index();
        index.put(Evidence::Preparation, "known", &()).unwrap();
        assert_eq!(
            index.with_db(|db| db.execute("invalid SQL", []).map(|_| ()).map_err(|_| ERROR)),
            Err(ERROR)
        );
        assert_eq!(index.get::<()>(Evidence::Preparation, "known"), Err(ERROR));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn unavailable_working_space_refuses_growth_and_poisons_the_index() {
        let (index, root) = index();
        index
            .with_db(|db| {
                let pages: u32 = db
                    .pragma_query_value(None, "page_count", |r| r.get(0))
                    .map_err(|_| ERROR)?;
                db.pragma_update(None, "max_page_count", pages)
                    .map_err(|_| ERROR)
            })
            .unwrap();
        std::fs::remove_dir(&index.scratch).unwrap();
        assert_eq!(index.put(Evidence::Preparation, "new", &()), Err(ERROR));
        assert_eq!(index.get::<()>(Evidence::Preparation, "new"), Err(ERROR));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn large_metadata_grows_the_page_allowance_before_insertion() {
        let (index, root) = index();
        index
            .with_db(|db| {
                let pages: u32 = db
                    .pragma_query_value(None, "page_count", |r| r.get(0))
                    .map_err(|_| ERROR)?;
                db.pragma_update(None, "max_page_count", pages + 20)
                    .map_err(|_| ERROR)
            })
            .unwrap();
        let value = "x".repeat(128 * 1024);
        index.put(Evidence::Preparation, "large", &value).unwrap();
        assert_eq!(
            index.get::<String>(Evidence::Preparation, "large").unwrap(),
            Some(value)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn command_index_errors_remove_their_private_directory() {
        use std::os::unix::fs::PermissionsExt;
        let (config, root) = crate::lease::tests::local_config("scratch-error-cleanup");
        let index = Index::command(&config).unwrap();
        let directory = index.scratch.clone();
        index.invalidate();
        assert!(!index.is_valid());
        assert!(!directory.exists());
        let directory = super::super::command_directory(&config).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o500)).unwrap();
        assert!(Index::open(directory.clone(), true).is_err());
        assert!(!directory.exists());
        let _ = std::fs::remove_dir_all(root);
    }
}
