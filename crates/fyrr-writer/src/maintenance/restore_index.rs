//! Disposable metadata only. The unlinked SQLite file is never restart evidence.
use super::*;
use crate::scratch::check_space;
use arrow_array::{Array, UInt64Array};
use rusqlite::{Connection, params};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

fn native_scan(dataset: &Dataset) -> (lance::dataset::scanner::Scanner, Arc<AtomicU64>) {
    let count = Arc::new(AtomicU64::new(0));
    let observed = count.clone();
    let mut scan = dataset.scan();
    scan.scan_stats_callback(Arc::new(move |_| {
        observed.fetch_add(1, Ordering::Relaxed);
    }));
    (scan, count)
}

// Full-v5 measurement: 5,335,257,088 bytes / 1,126,664 indexed rows.
// 6 KiB per physical row includes ~30% headroom; deleted rows overestimate it.
fn budget(rows: u64) -> Result<u64> {
    rows.checked_mul(6 * 1024)
        .and_then(|bytes| bytes.checked_add(8 * 1024 * 1024 + 4095))
        .map(|bytes| bytes / 4096 * 4096)
        .filter(|bytes| bytes / 4096 <= u64::from(u32::MAX - 1))
        .ok_or(ERROR)
}

const ERROR: WriterError = WriterError::RestoreWorkingSpaceExhausted;
const COLUMNS: &[&str] = &[
    "source_id",
    "root_id",
    "record_id",
    "metadata_json",
    "readers",
    "source_revision", // Marker comparison only; the verification scan omits it.
];

pub(super) struct Index {
    db: Connection,
    scratch: std::path::PathBuf,
    pub cap: u64,
    aliases: BTreeMap<String, String>,
    pub(super) retired: BTreeSet<String>,
    pub scans: [u64; 3],
    pub reads: [u64; 3],
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct Row {
    pub source: String,
    pub root: String,
    pub record: String,
    revision: String,
    pub state: apply::DocumentState,
    pub readers: Vec<String>,
    pub(super) own_readers: Option<Vec<String>>,
    pub address: u64,
    canonical_metadata: bool,
    marker_metadata: bool,
}

impl Row {
    fn read(batch: &arrow_array::RecordBatch, row: usize) -> Result<Self> {
        let state = apply::state(batch, row)?;
        let raw = apply::strings(batch, "metadata_json")?.value(row);
        let metadata: serde_json::Value =
            serde_json::from_str(raw).map_err(|_| WriterError::DocumentStateInvalid)?;
        let canonical_json = metadata.to_string();
        let canonical_metadata = canonical_json == raw
            && metadata.get("_fyrr")
                == Some(
                    &serde_json::to_value(&state).map_err(|_| WriterError::DocumentStateInvalid)?,
                );
        Ok(Self {
            source: apply::strings(batch, "source_id")?.value(row).into(),
            root: apply::strings(batch, "root_id")?.value(row).into(),
            record: apply::strings(batch, "record_id")?.value(row).into(),
            revision: apply::strings(batch, "source_revision")?.value(row).into(),
            state,
            readers: apply::tokens(batch, row)?,
            own_readers: apply::own_readers(batch, row)?,
            address: batch
                .column_by_name("_rowaddr")
                .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
                .filter(|a| !a.is_null(row))
                .ok_or(WriterError::RestoreVerificationFailed)?
                .value(row),
            canonical_metadata,
            marker_metadata: metadata.as_object().is_some_and(|m| m.len() == 1),
        })
    }
}

impl Index {
    /// Called under the namespace lease, including before history/target errors.
    pub(super) fn scratch(config: &Config) -> Result<std::path::PathBuf> {
        crate::scratch::directory(config, "restore")
    }

    pub(super) fn new(config: &Config, sources: &BTreeSet<String>, rows: u64) -> Result<Self> {
        let scratch = Self::scratch(config)?;
        let cap = budget(rows)?;
        check_space(&scratch, cap)?;
        let db = crate::scratch::open(&scratch, "restore", |_path| {
            #[cfg(test)]
            tests::creation_boundary(_path);
        })?;
        db.execute_batch(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
            PRAGMA locking_mode=EXCLUSIVE; PRAGMA cache_size=-8192;
            PRAGMA temp_store=MEMORY; PRAGMA page_size=4096;
            CREATE TABLE rows(side INTEGER, source TEXT, root TEXT, record TEXT, value BLOB,
                PRIMARY KEY(side,source,root,record)) WITHOUT ROWID;
            CREATE TABLE roots(source TEXT, root TEXT, PRIMARY KEY(source,root)) WITHOUT ROWID;",
        )
        .map_err(|_| ERROR)?;
        db.pragma_update(None, "max_page_count", (cap / 4096) as i64)
            .map_err(|_| ERROR)?;
        Ok(Self {
            db,
            scratch,
            cap,
            aliases: sources
                .iter()
                .flat_map(|s| [(s.clone(), s.clone()), (apply::state_source(s), s.clone())])
                .collect(),
            retired: BTreeSet::new(),
            scans: [0; 3],
            reads: [0; 3],
        })
    }

    /// A runtime fragment-metadata mismatch can expand the selected root set.
    /// Recheck the enlarged budget before any projected scan is executed.
    pub(super) fn reserve(&mut self, rows: u64) -> Result<()> {
        let cap = budget(rows)?;
        if cap > self.cap {
            check_space(&self.scratch, cap)?;
            self.db
                .pragma_update(None, "max_page_count", (cap / 4096) as i64)
                .map_err(|_| ERROR)?;
            self.cap = cap;
        }
        Ok(())
    }

    pub(super) async fn scan(
        &mut self,
        config: &Config,
        lease: &LeaseGuard,
        attempt: &mut RestoreAttempt<'_>,
        dataset: &Dataset,
        side: i64,
        fragments: &BTreeSet<u64>,
    ) -> Result<()> {
        attempt
            .cadence
            .maybe(config, lease, attempt.history)
            .await?;
        if fragments.is_empty() {
            return Ok(());
        }
        let store = dataset
            .object_store(None)
            .await
            .map_err(|_| WriterError::RestoreVerificationFailed)?;
        let before = store.io_stats_snapshot().read_iops;
        let (mut scan, executions) = native_scan(dataset);
        scan.project(COLUMNS)
            .map_err(|_| WriterError::RestoreVerificationFailed)?
            .with_row_address()
            .batch_size(RESTORE_ROOTS)
            .batch_readahead(1)
            .fragment_readahead(4)
            .target_parallelism(4);
        scan.with_fragments(
            dataset
                .manifest()
                .fragments
                .iter()
                .filter(|f| fragments.contains(&f.id))
                .cloned()
                .collect(),
        );
        let start = std::time::Instant::now();
        let mut stream = scan
            .try_into_stream()
            .await
            .map_err(|_| WriterError::RestoreVerificationFailed)?;
        restore_deadline(config, start)?;
        loop {
            attempt
                .cadence
                .maybe(config, lease, attempt.history)
                .await?;
            let start = std::time::Instant::now();
            let Some(batch) = stream
                .try_next()
                .await
                .map_err(|_| WriterError::RestoreVerificationFailed)?
            else {
                break;
            };
            restore_deadline(config, start)?;
            #[cfg(test)]
            restore_test_scan(&mut attempt.cadence);
            let tx = self.db.transaction().map_err(|_| ERROR)?;
            {
                let mut insert = tx
                    .prepare_cached("INSERT INTO rows VALUES(?1,?2,?3,?4,?5)")
                    .map_err(|_| ERROR)?;
                let mut root = tx
                    .prepare_cached("INSERT OR IGNORE INTO roots VALUES(?1,?2)")
                    .map_err(|_| ERROR)?;
                for i in 0..batch.num_rows() {
                    let row = Row::read(&batch, i)?;
                    let source = self
                        .aliases
                        .get(&row.source)
                        .ok_or(WriterError::RestoreVerificationFailed)?;
                    let bytes =
                        serde_json::to_vec(&row).map_err(|_| WriterError::DocumentStateInvalid)?;
                    insert
                        .execute(params![side, source, row.root, row.record, bytes])
                        .map_err(|_| ERROR)?;
                    root.execute(params![source, row.root]).map_err(|_| ERROR)?;
                }
            }
            tx.commit().map_err(|_| ERROR)?;
            restore_deadline(config, start)?;
        }
        drop(stream);
        self.scans[crate::narrow_usize(side as u64)] += executions.load(Ordering::Relaxed);
        self.reads[crate::narrow_usize(side as u64)] +=
            store.io_stats_snapshot().read_iops - before;
        Ok(())
    }

    pub(super) fn roots(&self, after: &(String, String)) -> Result<Vec<(String, String)>> {
        self.db
            .prepare_cached(
                "SELECT source,root FROM roots WHERE (source,root)>(?1,?2)
            ORDER BY source,root LIMIT 256",
            )
            .map_err(|_| ERROR)?
            .query_map(params![after.0, after.1], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|_| ERROR)?
            .collect::<Result<_, _>>()
            .map_err(|_| ERROR)
    }

    pub(super) fn rows(
        &self,
        config: &Config,
        side: i64,
        source: &str,
        root: &str,
    ) -> Result<Vec<Row>> {
        let mut statement = self
            .db
            .prepare_cached(
                "SELECT value FROM rows WHERE side=?1 AND source=?2 AND root=?3 ORDER BY record",
            )
            .map_err(|_| ERROR)?;
        let mut cursor = statement
            .query(params![side, source, root])
            .map_err(|_| ERROR)?;
        let mut output = Vec::new();
        let mut bytes = apply::ROW_OVERHEAD_BYTES;
        while let Some(row) = cursor.next().map_err(|_| ERROR)? {
            let value: Vec<u8> = row.get(0).map_err(|_| ERROR)?;
            bytes = bytes
                .checked_add(value.len() as u64)
                .ok_or(WriterError::EnvelopeBoundsInvalid)?;
            if output.len() as u64 >= config.writer.commit.group_max_rows
                || bytes > config.writer.commit.group_max_bytes.as_u64()
            {
                return Err(WriterError::EnvelopeBoundsInvalid);
            }
            output.push(
                serde_json::from_slice(&value).map_err(|_| WriterError::DocumentStateInvalid)?,
            );
        }
        Ok(output)
    }

    pub(super) fn authority(
        &self,
        config: &Config,
        source: &str,
        root: &str,
    ) -> Result<RestoreAuthority> {
        let mut result = RestoreAuthority::default();
        for row in self.rows(config, 0, source, root)? {
            if result.states.get(root).is_some_and(|s| *s != row.state) {
                return Err(WriterError::DocumentStateInvalid);
            }
            result.states.insert(root.into(), row.state);
            result
                .own_readers
                .insert((root.into(), row.record.clone()), row.own_readers);
            result
                .readers
                .insert((root.into(), row.record), row.readers);
        }
        Ok(result)
    }

    pub(super) async fn verify(
        &mut self,
        config: &Config,
        lease: &LeaseGuard,
        attempt: &mut RestoreAttempt<'_>,
        fragments: &BTreeSet<u64>,
    ) -> Result<()> {
        attempt
            .cadence
            .maybe(config, lease, attempt.history)
            .await?;
        if fragments.is_empty() {
            return Ok(());
        }
        let store = attempt
            .dataset
            .object_store(None)
            .await
            .map_err(|_| WriterError::RestoreVerificationFailed)?;
        let before = store.io_stats_snapshot().read_iops;
        let (mut scan, executions) = native_scan(&attempt.dataset);
        scan.project(&COLUMNS[..COLUMNS.len() - 1])
            .map_err(|_| WriterError::RestoreVerificationFailed)?
            .batch_size(RESTORE_ROOTS)
            .batch_readahead(1)
            .fragment_readahead(4)
            .target_parallelism(4);
        scan.with_fragments(
            attempt
                .dataset
                .manifest()
                .fragments
                .iter()
                .filter(|f| fragments.contains(&f.id))
                .cloned()
                .collect(),
        );
        let start = std::time::Instant::now();
        let mut stream = scan
            .try_into_stream()
            .await
            .map_err(|_| WriterError::RestoreVerificationFailed)?;
        restore_deadline(config, start)?;
        loop {
            attempt
                .cadence
                .maybe(config, lease, attempt.history)
                .await?;
            let start = std::time::Instant::now();
            let Some(batch) = stream
                .try_next()
                .await
                .map_err(|_| WriterError::RestoreVerificationFailed)?
            else {
                break;
            };
            restore_deadline(config, start)?;
            for row in 0..batch.num_rows() {
                let source = self
                    .aliases
                    .get(apply::strings(&batch, "source_id")?.value(row))
                    .ok_or(WriterError::RestoreVerificationFailed)?;
                let root = apply::strings(&batch, "root_id")?.value(row);
                if self.retired.contains(source) {
                    return Err(WriterError::RestoreVerificationFailed);
                }
                let authority = self.authority(config, source, root)?;
                restore_verify(source, &[batch.slice(row, 1)], &authority)?;
            }
            restore_deadline(config, start)?;
        }
        drop(stream);
        self.scans[2] += executions.load(Ordering::Relaxed);
        self.reads[2] = store.io_stats_snapshot().read_iops - before;
        Ok(())
    }

    pub(super) fn bytes(&self) -> Result<u64> {
        let pages: i64 = self
            .db
            .pragma_query_value(None, "page_count", |row| row.get(0))
            .map_err(|_| ERROR)?;
        let page_size: i64 = self
            .db
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .map_err(|_| ERROR)?;
        u64::try_from(pages)
            .map_err(|_| ERROR)?
            .checked_mul(u64::try_from(page_size).map_err(|_| ERROR)?)
            .ok_or(ERROR)
    }
}

/// Compare the complete prospective state, not merely the content revision.
pub(super) fn unchanged(
    source: &str,
    root: &str,
    rows: &[Row],
    authority: &RestoreAuthority,
) -> bool {
    let Some(current) = authority.states.get(root) else {
        return rows.is_empty();
    };
    let mut marker = current.clone();
    if !marker.deleted {
        marker.content_purged = true;
    }
    if rows.len() == 1
        && rows[0].source == apply::state_source(source)
        && rows[0].state == marker
        && rows[0].readers.is_empty()
        && rows[0].canonical_metadata
        && rows[0].marker_metadata
        && rows[0].revision == marker.revision.as_deref().unwrap_or_default()
        && rows[0].record
            == format!(
                "state-{}",
                &fyrr_reader::embedding::digest(root.as_bytes())[7..39]
            )
    {
        return true;
    }
    !current.deleted
        && !current.content_purged
        && !rows.is_empty()
        && rows.iter().all(|row| {
            if row.source != source || row.state.deleted || row.state.content_purged {
                return false;
            }
            let mut prospective = row.state.clone();
            prospective.permission.clone_from(&current.permission);
            let mut readers = authority
                .readers
                .get(&(root.into(), row.record.clone()))
                .cloned()
                .unwrap_or_default();
            readers.retain(|t| {
                current
                    .permission
                    .as_ref()
                    .is_some_and(|p| p.readers.contains(t))
            });
            let own = authority
                .own_readers
                .get(&(root.into(), row.record.clone()))
                .cloned()
                .unwrap_or(Some(Vec::new()));
            row.state == prospective && row.readers == readers && row.own_readers == own
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn config() -> Config {
        let mut config = fyrr_core::config::parse("[namespace]\nname='restore-test'\n[storage]\nuri='s3://restore-test'\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n").unwrap().config;
        // Keep one namespace per test, including child-process restarts.
        config.namespace.name = format!(
            "restore-test-{}",
            &fyrr_reader::embedding::digest(std::thread::current().name().unwrap().as_bytes())[7..]
        );
        config.reader.mirror_path = Some(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/test-reader-mirror")
                .display()
                .to_string(),
        );
        config
    }

    #[test]
    fn scratch_cleanup_does_not_touch_another_namespace() {
        let first = config();
        let second = std::thread::Builder::new()
            .name("another-restore-test".into())
            .spawn(config)
            .unwrap()
            .join()
            .unwrap();
        let first = Index::scratch(&first).unwrap();
        let marker = first.join("live.sqlite");
        std::fs::write(&marker, b"live").unwrap();
        let second = Index::scratch(&second).unwrap();
        assert!(
            marker.exists(),
            "another namespace must not clean this namespace's scratch"
        );
        assert_ne!(first, second);
        std::fs::remove_dir_all(first).unwrap();
        std::fs::remove_dir_all(second).unwrap();
    }

    pub(super) fn creation_boundary(path: &std::path::Path) {
        if let Some(marker) = std::env::var_os("FYRR_TEST_RESTORE_FILE_CREATED") {
            std::fs::write(marker, path.as_os_str().as_encoded_bytes()).unwrap();
            std::thread::park();
        }
    }

    #[test]
    fn working_file_creation_child() {
        if std::env::var_os("FYRR_TEST_RESTORE_FILE_CHILD").is_some() {
            Index::new(&config(), &BTreeSet::new(), 0).unwrap();
        }
    }

    #[test]
    fn working_file_kill_during_creation_and_restart_leave_no_file() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(format!("target/restore-file-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let ready = root.join("ready");
        let name = "maintenance::restore_index::tests::working_file_creation_child";
        let child = || {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([name, "--exact", "--nocapture"])
                .env("TMPDIR", &root)
                .env("FYRR_TEST_RESTORE_FILE_CHILD", "1");
            command
        };
        let mut killed = child()
            .env("FYRR_TEST_RESTORE_FILE_CREATED", &ready)
            .spawn()
            .unwrap();
        let start = std::time::Instant::now();
        while !ready.exists() && start.elapsed() < std::time::Duration::from_secs(20) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        killed.kill().unwrap();
        killed.wait().unwrap();
        let path = std::path::PathBuf::from(std::fs::read_to_string(&ready).unwrap());
        assert!(child().status().unwrap().success());
        assert!(
            !path.exists(),
            "restart must remove a file left by a kill at creation"
        );
        assert_eq!(
            std::fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn persisted_projection_excludes_text_vectors_and_nonstate_metadata() {
        let state = apply::DocumentState::default();
        let batch =
            apply::marker_row(config().embedding.dimensions, "source", "root", &state).unwrap();
        let mut columns = batch.columns().to_vec();
        let metadata = batch.schema().index_of("metadata_json").unwrap();
        columns[batch.schema().index_of("source_id").unwrap()] =
            std::sync::Arc::new(arrow_array::StringArray::from(vec!["source"]));
        let pretty = serde_json::json!({"_fyrr":state,"private_text":"SNT-discarded-text"});
        columns[metadata] = std::sync::Arc::new(arrow_array::StringArray::from(vec![
            serde_json::to_string_pretty(&pretty).unwrap(),
        ]));
        let mut fields = batch.schema().fields().to_vec();
        fields.push(std::sync::Arc::new(arrow_schema::Field::new(
            "_rowaddr",
            arrow_schema::DataType::UInt64,
            false,
        )));
        columns.push(std::sync::Arc::new(UInt64Array::from(vec![42])));
        let batch = arrow_array::RecordBatch::try_new(
            std::sync::Arc::new(arrow_schema::Schema::new(fields)),
            columns,
        )
        .unwrap();
        let row = Row::read(&batch, 0).unwrap();
        assert!(!row.canonical_metadata);
        let mut authority = RestoreAuthority::default();
        authority.states.insert("root".into(), state.clone());
        authority
            .own_readers
            .insert(("root".into(), row.record.clone()), None);
        assert!(unchanged(
            "source",
            "root",
            std::slice::from_ref(&row),
            &authority
        ));
        let overlay = restore_rows(
            &config(),
            "source",
            "root",
            std::slice::from_ref(&batch),
            &authority,
            true,
        )
        .unwrap()
        .unwrap();
        assert_eq!(overlay[0], batch);
        let encoded = serde_json::to_vec(&row).unwrap();
        let value = String::from_utf8(encoded.clone()).unwrap();
        for excluded in [
            "SNT-discarded-text",
            "private_text",
            "vector",
            "searchable_text",
            "metadata_json",
        ] {
            assert!(!value.contains(excluded));
        }
        let index = Index::new(&config(), &BTreeSet::new(), 0).unwrap();
        index
            .db
            .execute(
                "INSERT INTO rows VALUES(0,'source','root','record',?1)",
                params![encoded],
            )
            .unwrap();
        let config = fyrr_core::config::parse("[namespace]\nname='restore-test'\n[storage]\nuri='s3://restore-test'\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n").unwrap().config;
        let loaded = index.rows(&config, 0, "source", "root").unwrap();
        assert_eq!(loaded[0].address, 42);
        assert_eq!(loaded[0].state, state);
    }

    #[test]
    fn working_budget_rejects_overflow_and_expands_for_fallback() {
        assert_eq!(budget(u64::MAX), Err(ERROR));
        let mut index = Index::new(&config(), &BTreeSet::new(), 1).unwrap();
        let small = index.cap;
        index.reserve(4096).unwrap();
        assert!(index.cap > small);
        let pages: u32 = index
            .db
            .pragma_query_value(None, "max_page_count", |r| r.get(0))
            .unwrap();
        assert_eq!(u64::from(pages) * 4096, index.cap);
    }

    #[test]
    fn working_file_is_private_unlinked_bounded_and_disposable() {
        let index = Index::new(&config(), &BTreeSet::new(), 0).unwrap();
        let path = index.db.path().unwrap();
        assert!(!std::path::Path::new(path).exists());
        let fd = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .flatten()
            .find(|entry| {
                std::fs::read_link(entry.path())
                    .is_ok_and(|p| p.to_string_lossy() == format!("{path} (deleted)"))
            })
            .unwrap();
        assert_eq!(
            std::fs::metadata(fd.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
        index
            .db
            .execute("INSERT INTO roots VALUES('source','root')", [])
            .unwrap();
        assert_eq!(
            index.roots(&(String::new(), String::new())).unwrap().len(),
            1
        );
        let limit: u32 = index
            .db
            .query_row("PRAGMA max_page_count", [], |r| r.get(0))
            .unwrap();
        assert_eq!(u64::from(limit) * 4096, index.cap);
        assert!(index.cap < 16 * 1024 * 1024);
        drop(index);
        assert!(!fd.path().exists());
        assert!(
            Index::new(&config(), &BTreeSet::new(), 0)
                .unwrap()
                .roots(&(String::new(), String::new()))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn prospective_comparison_includes_every_state_field_and_record_grant() {
        let state = apply::DocumentState {
            publication_sequence: 7,
            permission: Some(apply::PermissionState {
                revision: Some("p1".into()),
                readers: vec!["tok:hash".into()],
                containers: Vec::new(),
            }),
            ..Default::default()
        };
        let mut row = Row {
            source: "source".into(),
            root: "root".into(),
            record: "record".into(),
            revision: String::new(),
            state: state.clone(),
            readers: vec!["tok:hash".into()],
            own_readers: None,
            address: 0,
            canonical_metadata: true,
            marker_metadata: false,
        };
        let mut authority = RestoreAuthority::default();
        authority.states.insert("root".into(), state.clone());
        authority
            .own_readers
            .insert(("root".into(), "record".into()), None);
        authority
            .readers
            .insert(("root".into(), "record".into()), row.readers.clone());
        assert!(unchanged(
            "source",
            "root",
            std::slice::from_ref(&row),
            &authority
        ));
        authority
            .own_readers
            .insert(("root".into(), "record".into()), Some(row.readers.clone()));
        assert!(!unchanged(
            "source",
            "root",
            std::slice::from_ref(&row),
            &authority
        ));
        authority
            .own_readers
            .insert(("root".into(), "record".into()), None);
        for changed in [
            apply::DocumentState {
                deleted: true,
                ..state.clone()
            },
            apply::DocumentState {
                content_purged: true,
                ..state.clone()
            },
            apply::DocumentState {
                permission: Some(apply::PermissionState {
                    revision: Some("p2".into()),
                    ..state.permission.clone().unwrap()
                }),
                ..state.clone()
            },
        ] {
            authority.states.insert("root".into(), changed);
            assert!(!unchanged(
                "source",
                "root",
                std::slice::from_ref(&row),
                &authority
            ));
        }
        authority.states.insert("root".into(), state.clone());
        authority
            .own_readers
            .insert(("root".into(), "record".into()), None);
        authority.readers.clear();
        assert!(!unchanged(
            "source",
            "root",
            std::slice::from_ref(&row),
            &authority
        ));
        row.readers.clear();
        assert!(unchanged("source", "root", &[row], &authority));
        // Marker comparison uses the entire current state, including publication sequence.
        authority.states.get_mut("root").unwrap().content_purged = true;
        let mut marker = Row {
            source: apply::state_source("source"),
            root: "root".into(),
            record: format!("state-{}", &fyrr_reader::embedding::digest(b"root")[7..39]),
            revision: String::new(),
            state: apply::DocumentState {
                content_purged: true,
                ..state
            },
            readers: Vec::new(),
            own_readers: None,
            address: 0,
            canonical_metadata: true,
            marker_metadata: true,
        };
        assert!(unchanged(
            "source",
            "root",
            std::slice::from_ref(&marker),
            &authority
        ));
        marker.state.publication_sequence += 1;
        assert!(!unchanged("source", "root", &[marker], &authority));
    }
}
