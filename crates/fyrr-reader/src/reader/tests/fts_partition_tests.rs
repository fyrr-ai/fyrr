//! Full-text indexes of several partitions: layout report and score identity.
//!
//! The same rows are indexed as one partition and as several (a one-worker
//! build with a 1 MiB target), and every query must return the same scores
//! and candidates from both, with and without an admission mask and with a
//! delta segment appended. `LANCE_FTS_SEARCH_CHUNK` is read once per process,
//! so the chunk variants run in child processes of this test binary.
use super::*;
use arrow_array::RecordBatchIterator;
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{WriteMode, WriteParams};
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::InvertedIndexParams;
use std::fmt::Write as _;

const ROWS: usize = 360;
/// Rows `0..BASE_ROWS` are the first write; the rest are appended as a delta.
const BASE_ROWS: usize = 300;
const LIMIT: usize = 20;
const CHILD: &str = "search::fts_partition_tests::identity_report_child_process";

/// Deterministic short rows. Every row carries unique tokens so that a few
/// hundred rows exceed the 1 MiB build target; rows 20..30 repeat one text so
/// that scores tie at a cut-off.
fn text(row: usize) -> String {
    let common = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
    let mut text = String::new();
    let source = if (20..30).contains(&row) { 20 } else { row };
    for k in 0..6 {
        if (source * 7 + k * 3) % 5 < 3 {
            write!(text, "{} ", common[(source + k) % common.len()]).unwrap();
        }
    }
    if source % 9 == 0 {
        text.push_str("needle ");
    }
    if (20..30).contains(&row) {
        text.push_str("tiemark ");
    }
    for k in 0..90 {
        write!(text, "u{source}x{k} ").unwrap();
    }
    text
}

fn batch(rows: std::ops::Range<usize>) -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(
        ["source_id", "root_id", "record_id", "searchable_text"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["source"; rows.len()])),
            Arc::new(StringArray::from(
                rows.clone()
                    .map(|i| format!("root-{i:03}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["record"; rows.len()])),
            Arc::new(StringArray::from(rows.map(text).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    (schema, batch)
}

fn scratch(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/lance-pin")
        .join(format!("fts-partitions-{name}-{}", std::process::id()))
}

async fn write(path: &std::path::Path, rows: std::ops::Range<usize>, mode: WriteMode) -> Dataset {
    let (schema, batch) = batch(rows);
    Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        path.to_str().unwrap(),
        Some(WriteParams {
            mode,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
}

/// `partitions` selects the layout: one worker with Lance's default target is
/// one partition; one worker with a 1 MiB target flushes several.
fn params(several: bool) -> InvertedIndexParams {
    let params = InvertedIndexParams::default()
        .with_position(false)
        .num_workers(1);
    if several {
        params.memory_limit_mb(1)
    } else {
        params
    }
}

async fn index(dataset: &mut Dataset, several: bool) {
    dataset
        .create_index(
            &["searchable_text"],
            IndexType::Inverted,
            Some(Layout::FTS_INDEX.into()),
            &params(several),
            false,
        )
        .await
        .unwrap();
}

/// Partition counts of the full-text segments, through the reader's own
/// layout probe.
async fn partitions(dataset: &Dataset) -> Vec<usize> {
    let indexes = dataset.load_indices().await.unwrap();
    let mut counts = Vec::new();
    for index in indexes.iter().filter(|i| i.name == Layout::FTS_INDEX) {
        let layout = crate::fts_layout::read(
            dataset,
            std::slice::from_ref(index),
            Layout::FTS_INDEX,
            "searchable_text",
        )
        .await
        .unwrap();
        counts.push(layout.partitions_max);
    }
    counts
}

/// Row address of row `row`: the delta rows live in a second fragment.
fn address(row: usize, delta: bool) -> u64 {
    if delta && row >= BASE_ROWS {
        (1 << 32) | (row - BASE_ROWS) as u64
    } else {
        row as u64
    }
}

fn queries() -> Vec<String> {
    let long = (0..30)
        .map(|k| match k % 3 {
            0 => ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"][k % 6].to_owned(),
            1 => format!("u{}x{}", k * 11 % ROWS, k),
            _ => "needle".to_owned(),
        })
        .collect::<Vec<_>>()
        .join(" ");
    vec![
        "needle".into(),
        "alpha beta".into(),
        "tiemark".into(),
        "tiemark alpha".into(),
        "u42x7 u200x3 gamma".into(),
        long,
    ]
}

/// Everything one layout returns, as exact text: scores as bit patterns.
async fn report(dataset: &Dataset, delta: bool) -> String {
    let masks: [(&str, Option<Vec<usize>>); 4] = [
        ("all", Some((0..ROWS).collect())),
        ("broad", Some((0..ROWS).filter(|i| i % 5 != 0).collect())),
        ("narrow", Some((0..ROWS).filter(|i| i % 20 == 3).collect())),
        ("unmasked", None),
    ];
    let mut out = String::new();
    for query in queries() {
        for (name, rows) in &masks {
            let candidates = match rows {
                Some(rows) => {
                    let eligible = RowAddrMask::from_allowed(RowAddrTreeMap::from_iter(
                        rows.iter().map(|&row| address(row, delta)),
                    ));
                    PreparedReader::lexical(
                        dataset,
                        &eligible,
                        None,
                        &query,
                        &TextMatch::default(),
                        LIMIT,
                    )
                    .await
                    .unwrap()
                    .0
                    .into_iter()
                    .map(|c| (c.root, c.score.to_bits()))
                    .collect::<Vec<_>>()
                }
                None => unmasked(dataset, &query).await,
            };
            writeln!(out, "{query:?} {name} {candidates:?}").unwrap();
        }
    }
    out
}

/// Lance's own top-k without any prefilter; ties at the cut-off are Lance's
/// pick, so only the rows strictly above the cut-off score are kept.
async fn unmasked(dataset: &Dataset, query: &str) -> Vec<(String, u64)> {
    let mut scan = dataset.scan();
    scan.project(&["root_id"]).unwrap();
    scan.full_text_search(
        lance_index::scalar::FullTextSearchQuery::new(query.into())
            .with_column("searchable_text".into())
            .unwrap()
            .limit(Some(LIMIT as i64)),
    )
    .unwrap();
    let batch = scan.try_into_batch().await.unwrap();
    let scores = column::<arrow_array::Float32Array>(&batch, "_score", SearchResultInvalid)
        .unwrap()
        .values()
        .to_vec();
    let floor = scores.iter().copied().fold(f32::INFINITY, f32::min);
    let mut rows = (0..batch.num_rows())
        .filter(|&row| scores[row] > floor)
        .map(|row| {
            (
                strings(&batch, "root_id").unwrap().value(row).to_owned(),
                u64::from(scores[row].to_bits()),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// One-partition and several-partition layouts of the same rows, with and
/// without a delta segment; returns their reports and partition counts.
async fn layouts(tag: &str) -> (String, String, String, String, Vec<Vec<usize>>) {
    let mut reports = Vec::new();
    let mut counts = Vec::new();
    for delta in [false, true] {
        for several in [false, true] {
            let path = scratch(&format!("{tag}-{delta}-{several}"));
            let mut dataset = write(
                &path,
                0..if delta { BASE_ROWS } else { ROWS },
                WriteMode::Create,
            )
            .await;
            index(&mut dataset, several).await;
            if delta {
                dataset = write(&path, BASE_ROWS..ROWS, WriteMode::Append).await;
                dataset
                    .optimize_indices(&OptimizeOptions::append())
                    .await
                    .unwrap();
                assert_eq!(
                    dataset.load_indices().await.unwrap().len(),
                    2,
                    "delta segment"
                );
            }
            counts.push(partitions(&dataset).await);
            reports.push(report(&dataset, delta).await);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
    let [one, many, one_delta, many_delta] = <[String; 4]>::try_from(reports).unwrap();
    (one, many, one_delta, many_delta, counts)
}

#[tokio::test]
async fn target_makes_several_partitions_and_scores_do_not_depend_on_them() {
    let (one, many, one_delta, many_delta, counts) = layouts("identity").await;
    // Per layout: one and several partitions, then the same with a delta.
    assert_eq!(counts[0], [1], "default target: one partition");
    assert!(counts[1][0] > 1, "1 MiB target: several partitions");
    assert_eq!(counts[2].len(), 2, "delta segment");
    assert!(counts[3][0] > 1, "the base segment keeps its partitions");
    assert!(one.contains("tiemark"));
    assert!(
        one.lines()
            .filter(|line| line.contains(" all ") || line.contains(" broad "))
            .all(|line| !line.ends_with("[]")),
        "every query matches rows"
    );
    assert_eq!(
        one, many,
        "scores and candidates, one vs several partitions"
    );
    assert_eq!(
        one_delta, many_delta,
        "scores and candidates with a delta segment"
    );
    // A tie at the cut-off is part of the fixture: ten rows share one text.
    assert!(
        one.lines().any(
            |line| line.starts_with("\"tiemark\" all") && line.matches("root-02").count() >= 10
        ),
    );
}

/// One chunk setting in its own process; writes `layouts` to the file named
/// by the parent.
#[tokio::test]
#[ignore = "child process of the chunk test"]
async fn identity_report_child_process() {
    let target = std::env::var("FYRR_TEST_FTS_REPORT").unwrap();
    let (one, many, one_delta, many_delta, ..) = layouts("child").await;
    assert_eq!(one, many);
    assert_eq!(one_delta, many_delta);
    std::fs::write(target, format!("{one}\n{one_delta}")).unwrap();
}

/// `LANCE_FTS_SEARCH_CHUNK` is a process-wide, read-once setting, so each
/// value runs in its own process. One task per partition (1), two tasks (2)
/// and Lance's default (16) return the same candidates and bit-identical
/// scores.
#[tokio::test]
async fn partition_search_width_does_not_change_scores() {
    let (one, _, one_delta, ..) = layouts("parent").await;
    let expected = format!("{one}\n{one_delta}");
    let directory = scratch("reports");
    std::fs::create_dir_all(&directory).unwrap();
    for chunk in ["1", "2"] {
        let target = directory.join(format!("chunk-{chunk}"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                CHILD,
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("LANCE_FTS_SEARCH_CHUNK", chunk)
            .env("FYRR_TEST_FTS_REPORT", &target)
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(status.success(), "child with chunk {chunk}");
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            expected,
            "chunk {chunk}"
        );
    }
    std::fs::remove_dir_all(directory).unwrap();
}
