//! The request-filter selection answers `(admission) AND (filter)` from the
//! scalar indexes. It must select exactly the rows the earlier scan selected
//! with the admission mask attached, which Lance plans as a read of every
//! admitted row, and it must still refuse a row outside the admission.
use super::*;
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{RecordBatchIterator, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use lance::dataset::{WriteMode, WriteParams};

const INDEXED_ROWS: usize = 120;
const TAIL_ROWS: usize = 40;
const HOUR: i64 = 3_600_000_000;
const EPOCH: i64 = 1_700_000_000_000_000;

fn strings_list(items: &[Vec<String>]) -> arrow_array::ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for values in items {
        for value in values {
            builder.values().append_value(value);
        }
        builder.append(true);
    }
    builder.finish()
}

fn schema() -> Arc<Schema> {
    let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    Arc::new(Schema::new(vec![
        Field::new("source_id", DataType::Utf8, false),
        Field::new("root_id", DataType::Utf8, false),
        Field::new("record_id", DataType::Utf8, false),
        Field::new("readers", list.clone(), false),
        Field::new("project", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("attributes", list, true),
    ]))
}

fn rows(range: std::ops::Range<usize>) -> RecordBatch {
    let column = |f: &dyn Fn(usize) -> String| {
        Arc::new(StringArray::from(range.clone().map(f).collect::<Vec<_>>()))
    };
    let readers: Vec<Vec<String>> = range
        .clone()
        .map(|i| {
            let mut readers = vec!["g_other".to_owned()];
            if i % 2 == 0 {
                readers.push("g_pub".into());
            }
            if i % 3 == 0 {
                readers.push("g_eng".into());
            }
            if i % 13 == 0 {
                readers.push("g_rare".into());
            }
            readers
        })
        .collect();
    let attributes: Vec<Vec<String>> = range
        .clone()
        .map(|i| {
            if i % 2 == 0 {
                vec!["team=blue".into()]
            } else {
                vec!["team=red".into(), "tier=1".into()]
            }
        })
        .collect();
    RecordBatch::try_new(
        schema(),
        vec![
            column(&|i| format!("s{}", i % 3)),
            column(&|i| format!("root-{i:03}")),
            column(&|_| "record".into()),
            Arc::new(strings_list(&readers)),
            column(&|i| format!("p{}", i % 4)),
            column(&|i| format!("a{}", i % 5)),
            Arc::new(TimestampMicrosecondArray::from(
                range
                    .clone()
                    .map(|i| EPOCH + i as i64 * HOUR)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(strings_list(&attributes)),
        ],
    )
    .unwrap()
}

/// Three indexed fragments, then (when `tail`) one fragment the indexes do
/// not cover, then deletions in indexed and unindexed fragments.
async fn fixture(name: &str, tail: bool) -> (Dataset, std::path::PathBuf) {
    use lance::index::DatasetIndexExt;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/lance-pin")
        .join(format!("filter-selection-{name}-{}", std::process::id()));
    let params = |mode| WriteParams {
        mode,
        max_rows_per_file: 40,
        ..Default::default()
    };
    let write = |batch, mode| {
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema()),
            root.to_str().unwrap(),
            Some(params(mode)),
        )
    };
    let mut dataset = write(rows(0..INDEXED_ROWS), WriteMode::Create)
        .await
        .unwrap();
    for (column, name, kind) in [
        ("readers", "readers_idx", IndexType::LabelList),
        ("source_id", "source_id_idx", IndexType::Bitmap),
        ("project", "project_idx", IndexType::Bitmap),
        ("author", "author_idx", IndexType::Bitmap),
        ("updated_at", "updated_at_idx", IndexType::BTree),
        ("attributes", "attributes_idx", IndexType::LabelList),
    ] {
        dataset
            .create_index(
                &[column],
                kind,
                Some(name.into()),
                &lance_index::scalar::ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
    }
    if tail {
        dataset = write(
            rows(INDEXED_ROWS..INDEXED_ROWS + TAIL_ROWS),
            WriteMode::Append,
        )
        .await
        .unwrap();
        let covered = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .find(|i| i.name == "author_idx")
            .and_then(|i| i.fragment_bitmap.clone())
            .unwrap();
        assert!(
            dataset
                .manifest()
                .fragments
                .iter()
                .any(|f| !covered.contains(u32::try_from(f.id).unwrap())),
            "the appended fragment is not covered by the indexes"
        );
    }
    // Rows 9, 29, 49, ... fall in every fragment, the unindexed one included.
    dataset
        .delete("author = 'a4' AND project = 'p1'")
        .await
        .unwrap();
    (dataset, root)
}

/// The admission predicate of each subject, in the shapes `predicate` and
/// `admission_predicate` produce, with the hidden sources it excludes.
fn subjects() -> Vec<(&'static str, String, Vec<&'static str>)> {
    let hidden = |predicate: &str, sources: &[&str]| {
        format!(
            "({predicate}) AND source_id NOT IN ({})",
            sources
                .iter()
                .map(|s| format!("'{s}'"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let broad = "array_has_any(readers, ['g_pub','g_eng','g_other'])";
    let fixed = "(((array_has_any(readers, ['g_eng'])) AND source_id NOT IN ('s1')) \
                 OR (source_id IN ('s1')))";
    vec![
        ("broad", broad.into(), vec![]),
        (
            "narrow",
            "array_has_any(readers, ['g_rare'])".into(),
            vec![],
        ),
        ("none", "array_has_any(readers, ['g_none'])".into(), vec![]),
        ("fixed", fixed.into(), vec![]),
        ("hidden", hidden(broad, &["s2"]), vec!["s2"]),
        ("fixed_hidden", hidden(fixed, &["s0"]), vec!["s0"]),
    ]
}

fn filters() -> Vec<(&'static str, String)> {
    let after = EPOCH + 30 * HOUR;
    let before = EPOCH + 100 * HOUR;
    let stamp = |micros: i64| {
        chrono::DateTime::from_timestamp_micros(micros)
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S%.6f")
            .to_string()
    };
    vec![
        ("source", "source_id IN ('s0')".into()),
        ("author", "author IN ('a1','a4')".into()),
        (
            "time",
            format!(
                "updated_at > timestamp '{}' AND updated_at < timestamp '{}'",
                stamp(after),
                stamp(before)
            ),
        ),
        ("label", "array_has_any(attributes,['team=blue'])".into()),
        (
            "combined",
            "project IN ('p1','p2') AND author IN ('a1','a2') \
             AND array_has_any(attributes,['team=red']) \
             AND NOT ((source_id='s1' AND root_id='root-001'))"
                .into(),
        ),
        ("nothing", "author IN ('nobody')".into()),
        ("everything", "source_id IN ('s0','s1','s2')".into()),
    ]
}

/// The admission `rebuild_admission` builds: rows of the predicate scan,
/// minus the hidden sources. It reads `source_id` even without hidden
/// sources, where `rebuild_admission` projects nothing; both select the
/// same rows (compare `filter_rows`).
async fn admission(dataset: &Dataset, predicate: &str, hidden: &[&str]) -> RowAddrMask {
    let mut scan = dataset.scan();
    scan.project(&["source_id"]).unwrap();
    scan.filter(predicate).unwrap().with_row_id();
    let batch = scan.try_into_batch().await.unwrap();
    let sources = strings(&batch, "source_id").unwrap();
    let mut eligible = RowAddrTreeMap::new();
    for (row, id) in ids(&batch).unwrap().values().iter().enumerate() {
        if !hidden.contains(&sources.value(row)) {
            eligible.insert(*id);
        }
    }
    RowAddrMask::from_allowed(eligible)
}

/// The row ids of a selection, ascending.
fn rows_of(selection: &RowAddrTreeMap) -> Vec<u64> {
    RowAddrMask::from_allowed(selection.clone())
        .iter_addrs()
        .map(|rows| rows.map(u64::from).collect())
        .unwrap_or_default()
}

fn masked_scan(
    dataset: &Dataset,
    predicate: &str,
    eligible: &RowAddrMask,
) -> lance::dataset::scanner::Scanner {
    let mut scan = dataset.scan();
    scan.project::<&str>(&[]).unwrap();
    scan.filter(predicate)
        .unwrap()
        .with_row_id()
        .with_row_id_prefilter(eligible.clone());
    scan
}

/// The selection before this change: the same predicate with the mask attached.
async fn oracle(dataset: &Dataset, predicate: &str, eligible: &RowAddrMask) -> RowAddrTreeMap {
    let batch = masked_scan(dataset, predicate, eligible)
        .try_into_batch()
        .await
        .unwrap();
    RowAddrTreeMap::from_iter(ids(&batch).unwrap().values().iter().copied())
}

#[tokio::test]
async fn selection_equals_the_masked_scan_for_every_subject_and_filter() {
    for tail in [false, true] {
        let (dataset, root) = fixture(if tail { "tail" } else { "indexed" }, tail).await;
        let mut nonempty = 0;
        for (subject, admission_predicate, hidden) in subjects() {
            let eligible = admission(&dataset, &admission_predicate, &hidden).await;
            for (name, filter) in filters() {
                let predicate = format!("({admission_predicate}) AND ({filter})");
                let expected = oracle(&dataset, &predicate, &eligible).await;
                let selected = PreparedReader::filter_rows(&dataset, &predicate, &eligible)
                    .await
                    .unwrap();
                assert_eq!(
                    rows_of(&selected),
                    rows_of(&expected),
                    "tail={tail} subject={subject} filter={name}"
                );
                nonempty += usize::from(!rows_of(&expected).is_empty());
                if name == "nothing" || subject == "none" {
                    assert!(rows_of(&selected).is_empty(), "{subject} {name}");
                }
            }
        }
        assert!(nonempty > 15, "the matrix selects rows: {nonempty}");
        // The unindexed fragment and the deleted rows take part.
        let eligible = admission(&dataset, &subjects()[0].1, &[]).await;
        let everything = PreparedReader::filter_rows(
            &dataset,
            &format!("({}) AND (source_id IN ('s0','s1','s2'))", subjects()[0].1),
            &eligible,
        )
        .await
        .unwrap();
        let live = INDEXED_ROWS + if tail { TAIL_ROWS } else { 0 };
        let everything = rows_of(&everything);
        assert_eq!(everything.len(), live - live / 20);
        assert_eq!(
            everything.iter().any(|id| (id >> 32) >= 3),
            tail,
            "rows of the unindexed fragment are selected"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Without hidden sources `rebuild_admission` scans row ids only; that scan
/// selects the rows of the `source_id` scan, deleted and unindexed ones too.
#[tokio::test]
async fn a_row_id_admission_scan_selects_the_rows_of_the_source_scan() {
    for tail in [false, true] {
        let (dataset, root) = fixture(if tail { "ids-tail" } else { "ids" }, tail).await;
        for (subject, predicate, _) in subjects() {
            let mut scan = dataset.scan();
            scan.project::<&str>(&[]).unwrap();
            scan.filter(&predicate).unwrap().with_row_id();
            let batch = scan.try_into_batch().await.unwrap();
            let by_id: RowAddrTreeMap = ids(&batch).unwrap().values().iter().copied().collect();
            let by_source = admission(&dataset, &predicate, &[]).await;
            assert_eq!(
                RowAddrMask::from_allowed(by_id)
                    .iter_addrs()
                    .map(|r| r.map(u64::from).collect::<Vec<_>>()),
                by_source
                    .iter_addrs()
                    .map(|r| r.map(u64::from).collect::<Vec<_>>()),
                "tail={tail} subject={subject}"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn selection_reads_the_scalar_indexes_and_not_the_admitted_rows() {
    let (dataset, root) = fixture("plan", false).await;
    let (_, admission_predicate, hidden) = subjects().remove(0);
    let eligible = admission(&dataset, &admission_predicate, &hidden).await;
    assert!(eligible.max_len().unwrap_or(0) > 100, "a broad admission");
    let predicate = format!("({admission_predicate}) AND (author IN ('a1'))");

    let direct = PreparedReader::filter_scan(&dataset, &predicate).unwrap();
    let plan = direct.explain_plan(true).await.unwrap();
    assert!(plan.contains("ScalarIndexQuery"), "{plan}");
    assert!(plan.contains("refine_filter=--"), "{plan}");
    let analysis = direct.analyze_plan().await.unwrap();
    let result = rows_of(
        &PreparedReader::filter_rows(&dataset, &predicate, &eligible)
            .await
            .unwrap(),
    )
    .len();
    assert!(result > 0 && result < 50, "a selective filter: {result}");
    assert!(
        analysis.contains(&format!(
            "rows_scanned={result}, bytes_read=0, iops=0, requests=0"
        )),
        "the scan emits the matches only and reads no data page: {analysis}"
    );

    let masked = masked_scan(&dataset, &predicate, &eligible);
    let plan = masked.explain_plan(true).await.unwrap();
    assert!(!plan.contains("ScalarIndexQuery"), "{plan}");
    assert!(!plan.contains("refine_filter=--"), "{plan}");
    let analysis = masked.analyze_plan().await.unwrap();
    let admitted = eligible.iter_addrs().unwrap().count();
    assert!(
        analysis.contains(&format!("rows_scanned={admitted},"))
            && !analysis.contains("bytes_read=0,"),
        "the masked scan reads the filter columns of every admitted row: {analysis}"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn selection_refuses_a_row_outside_the_admission() {
    let (dataset, root) = fixture("closed", true).await;
    let (_, admission_predicate, hidden) = subjects().remove(0);
    let eligible = admission(&dataset, &admission_predicate, &hidden).await;
    let predicate = format!("({admission_predicate}) AND (source_id IN ('s0'))");
    let selected = PreparedReader::filter_rows(&dataset, &predicate, &eligible)
        .await
        .unwrap();
    let selected = rows_of(&selected);
    assert!(selected.len() > 1);
    // An admission that lacks one selected row, from a state older than the predicate's.
    let narrowed =
        RowAddrMask::from_allowed(RowAddrTreeMap::from_iter(selected[1..].iter().copied()));
    assert_eq!(
        PreparedReader::filter_rows(&dataset, &predicate, &narrowed)
            .await
            .unwrap_err(),
        AuthorizationUnavailable
    );
    assert_eq!(
        PreparedReader::filter_rows(&dataset, &predicate, &RowAddrMask::allow_nothing())
            .await
            .unwrap_err(),
        AuthorizationUnavailable
    );
    std::fs::remove_dir_all(root).unwrap();
}
