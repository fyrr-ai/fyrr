//! Exact command-local evidence. SQL failure invalidates the entire disposable index.
use crate::Result;
use crate::scratch::{ERROR, check_space};
use fyrr_core::config::Config;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Clone, Copy)]
pub(crate) enum Evidence {
    Manifest,
    Applied,
    Rejected,
    Identity,
    Marker,
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
    pub(crate) fn new(config: &Config) -> Result<Self> {
        Self::open(crate::scratch::directory(config, "history")?, false)
    }

    pub(crate) fn command(config: &Config) -> Result<Self> {
        Self::open(crate::scratch::command_directory(config)?, true)
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
        let db = crate::scratch::open(&scratch, "history", |_| {})?;
        db.execute_batch(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
             PRAGMA locking_mode=EXCLUSIVE; PRAGMA page_size=4096;
             PRAGMA cache_size=-2048; PRAGMA temp_store=FILE;
             PRAGMA max_page_count=2048;
             CREATE TABLE evidence(kind INTEGER, key TEXT, value BLOB,
                 PRIMARY KEY(kind,key)) WITHOUT ROWID;
             CREATE TABLE staged(kind INTEGER, key TEXT, value BLOB,
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

    pub(crate) fn contains(&self, kind: Evidence, key: &str) -> Result<bool> {
        self.with_db(|db| {
            db.query_row(
                "SELECT EXISTS(SELECT 1 FROM evidence WHERE kind=?1 AND key=?2)",
                params![kind as i64, key],
                |row| row.get(0),
            )
            .map_err(|_| ERROR)
        })
    }

    pub(crate) fn put(
        &self,
        kind: Evidence,
        key: &str,
        value: &impl Serialize,
        staged: bool,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(value).map_err(|_| ERROR)?;
        self.with_db(|db| {
            self.reserve(db, key.len().checked_add(bytes.len()).ok_or(ERROR)?)?;
            let sql = if staged {
                "INSERT OR IGNORE INTO staged VALUES (?1,?2,?3)"
            } else {
                "INSERT OR IGNORE INTO evidence VALUES (?1,?2,?3)"
            };
            db.execute(sql, params![kind as i64, key, bytes])
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

    pub(crate) fn begin(&self) -> Result<()> {
        self.with_db(|db| {
            db.execute("DELETE FROM staged", [])
                .map(|_| ())
                .map_err(|_| ERROR)
        })
    }

    pub(crate) fn promote(&self) -> Result<()> {
        // No rollback with journal_mode=OFF. A failed promotion poisons the index;
        // consumers only resume after every row has been promoted successfully.
        self.with_db(|db| {
            loop {
                let rows = {
                    let mut statement = db
                        .prepare("SELECT kind,key,value FROM staged ORDER BY kind,key LIMIT 256")
                        .map_err(|_| ERROR)?;
                    statement
                        .query_map([], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, Vec<u8>>(2)?,
                            ))
                        })
                        .map_err(|_| ERROR)?
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(|_| ERROR)?
                };
                let Some((kind, key, _)) = rows.last() else {
                    return Ok(());
                };
                for (kind, key, value) in &rows {
                    self.reserve(db, key.len().checked_add(value.len()).ok_or(ERROR)?)?;
                    db.execute(
                        "INSERT OR IGNORE INTO evidence VALUES (?1,?2,?3)",
                        params![kind, key, value],
                    )
                    .map_err(|_| ERROR)?;
                }
                db.execute(
                    "DELETE FROM staged WHERE (kind,key) <= (?1,?2)",
                    params![kind, key],
                )
                .map_err(|_| ERROR)?;
            }
        })
    }

    pub(crate) fn next_rejected(&self, after: &str) -> Result<Option<String>> {
        self.with_db(|db| {
            db.query_row(
                "SELECT key FROM evidence WHERE kind=?1 AND key>?2 ORDER BY key LIMIT 1",
                params![Evidence::Rejected as i64, after],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| ERROR)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> Index {
        let mut config = fyrr_core::config::parse(
            "[namespace]\nname='history-test'\n[storage]\nuri='s3://history-test'\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n",
        ).unwrap().config;
        config.namespace.name = format!(
            "history-{}",
            &fyrr_reader::embedding::digest(std::thread::current().name().unwrap().as_bytes(),)
                [7..39]
        );
        config.reader.mirror_path = Some(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/test-reader-mirror")
                .display()
                .to_string(),
        );
        Index::new(&config).unwrap()
    }

    #[test]
    fn lifetime_receipts_exceed_the_active_summary_limit() {
        let index = index();
        index.begin().unwrap();
        for id in 0..70_000 {
            index
                .put(Evidence::Applied, &format!("env-{id:032x}"), &(), true)
                .unwrap();
        }
        assert!(
            !index
                .contains(Evidence::Applied, "env-00000000000000000000000000000000")
                .unwrap()
        );
        index.promote().unwrap();
        for id in [0, 32_768, 65_536, 69_999] {
            assert!(
                index
                    .contains(Evidence::Applied, &format!("env-{id:032x}"))
                    .unwrap()
            );
        }
        index
            .with_db(|db| {
                let cache: i32 = db
                    .pragma_query_value(None, "cache_size", |r| r.get(0))
                    .map_err(|_| ERROR)?;
                assert_eq!(cache, -2048);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            std::fs::read_dir(&index.scratch).unwrap().count(),
            0,
            "the working database is unlinked"
        );
    }

    #[test]
    fn discarded_prefix_never_becomes_a_validated_anchor() {
        let index = index();
        index.begin().unwrap();
        index
            .put(Evidence::Manifest, "valid", &1_u64, true)
            .unwrap();
        index.promote().unwrap();
        index.begin().unwrap();
        index
            .put(Evidence::Manifest, "invalid", &2_u64, true)
            .unwrap();
        index
            .put(Evidence::Rejected, "unvalidated-rejection", &(), true)
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                index.get::<u64>(Evidence::Manifest, "invalid").unwrap(),
                None
            );
            assert!(
                !index
                    .contains(Evidence::Rejected, "unvalidated-rejection")
                    .unwrap()
            );
            index.begin().unwrap();
        }
        assert_eq!(
            index.get::<u64>(Evidence::Manifest, "valid").unwrap(),
            Some(1)
        );
    }

    #[test]
    fn sql_failure_discards_all_cached_evidence() {
        let index = index();
        index.put(Evidence::Applied, "known", &(), false).unwrap();
        assert_eq!(
            index.with_db(|db| db.execute("invalid SQL", []).map(|_| ()).map_err(|_| ERROR)),
            Err(ERROR)
        );
        assert_eq!(index.contains(Evidence::Applied, "known"), Err(ERROR));
        assert_eq!(index.begin(), Err(ERROR));
    }

    #[test]
    fn unavailable_working_space_refuses_growth_and_poisons_the_index() {
        let index = index();
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
        assert_eq!(index.put(Evidence::Applied, "new", &(), false), Err(ERROR));
        assert_eq!(index.contains(Evidence::Applied, "new"), Err(ERROR));
    }

    #[test]
    fn promotion_crosses_mixed_evidence_chunk_boundaries() {
        let index = index();
        index.begin().unwrap();
        for id in 0..300_u64 {
            let key = format!("key-{id:08}");
            index.put(Evidence::Applied, &key, &(), true).unwrap();
            index.put(Evidence::Rejected, &key, &(), true).unwrap();
            index.put(Evidence::Manifest, &key, &id, true).unwrap();
        }
        index.promote().unwrap();
        for id in 0..300_u64 {
            let key = format!("key-{id:08}");
            assert!(index.contains(Evidence::Applied, &key).unwrap());
            assert!(index.contains(Evidence::Rejected, &key).unwrap());
            assert_eq!(
                index.get::<u64>(Evidence::Manifest, &key).unwrap(),
                Some(id)
            );
        }
    }

    #[test]
    fn large_metadata_grows_the_page_allowance_before_insertion() {
        let index = index();
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
        index
            .put(Evidence::Identity, "large", &value, false)
            .unwrap();
        assert_eq!(
            index.get::<String>(Evidence::Identity, "large").unwrap(),
            Some(value)
        );
    }
    #[tokio::test]
    #[ignore = "diagnostic: receipt SQLite phase on one runtime thread"]
    async fn history_sqlite_runtime_io_measurement() {
        let index = index();
        crate::io_probe::measure("history-sqlite-17600-receipts", async {
            index.begin().unwrap();
            for n in 0..17_600 {
                index
                    .put(Evidence::Applied, &format!("receipt-{n:032x}"), &(), true)
                    .unwrap();
            }
            index.promote().unwrap();
            for n in 0..17_600 {
                assert!(
                    index
                        .contains(Evidence::Applied, &format!("receipt-{n:032x}"))
                        .unwrap()
                );
            }
        })
        .await;
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
        let directory = crate::scratch::command_directory(&config).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o500)).unwrap();
        assert!(Index::open(directory.clone(), true).is_err());
        assert!(!directory.exists());
        let _ = std::fs::remove_dir_all(root);
    }
}
