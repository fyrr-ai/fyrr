//! `writer.fts_partition_memory_mb` against real Lance full-text builds.
//!
//! Lance reads the partition size from the environment once per process, so
//! each setting runs in a child process of this test binary that applies the
//! writer's own start-up settings first, builds the index with the writer's
//! own parameters, appends a delta segment and merges.
use super::*;
use arrow_array::{RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{WriteMode, WriteParams};
use lance::index::DatasetIndexInternalExt;
use lance_index::{IndexType, metrics::NoOpMetricsCollector, scalar::inverted::InvertedIndex};
use std::fmt::Write as _;
use std::sync::Arc;

const CHILD: &str = "commit::group::fts_partition_tests::partition_child_process";
const BASE_ROWS: usize = 300;
const ROWS: usize = 360;

fn config(writer: &str) -> Config {
    fyrr_core::config::parse(&format!(
        "[namespace]\nname = \"acme\"\n[storage]\nuri = \"s3://fyrr-data/acme\"\n\
         [identity]\nhashing_key = {{ env = \"FYRR_IDENTITY_KEY\" }}\n\
         [embedding]\nendpoint = \"https://embeddings.example/v1\"\n[writer]\n{writer}\n"
    ))
    .unwrap()
    .config
}

/// Short rows with unique tokens, so a few hundred of them exceed 1 MiB of
/// builder memory.
fn rows(range: std::ops::Range<usize>) -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "searchable_text",
        DataType::Utf8,
        false,
    )]));
    let text = range
        .map(|row| {
            let mut text = format!("common alpha{} ", row % 7);
            for k in 0..90 {
                write!(text, "u{row}x{k} ").unwrap();
            }
            text
        })
        .collect::<Vec<_>>();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(text))]).unwrap();
    (schema, batch)
}

async fn write(path: &std::path::Path, range: std::ops::Range<usize>, mode: WriteMode) -> Dataset {
    let (schema, batch) = rows(range);
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

/// Partition count of every full-text segment.
async fn partitions(dataset: &Dataset) -> Vec<usize> {
    let mut counts = Vec::new();
    for index in dataset
        .load_indices()
        .await
        .unwrap()
        .iter()
        .filter(|index| index.name == "fts_idx")
    {
        let opened = dataset
            .open_generic_index("searchable_text", &index.uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        counts.push(
            opened
                .as_any()
                .downcast_ref::<InvertedIndex>()
                .unwrap()
                .partition_count(),
        );
    }
    counts
}

/// Builds with the writer's parameters, appends a delta and merges; returns
/// the partitions per segment before and after the merge.
async fn lifecycle(config: &Config, tag: &str) -> (Vec<usize>, Vec<usize>) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/lance-pin")
        .join(format!("fts-partition-{tag}-{}", std::process::id()));
    let mut dataset = write(&path, 0..BASE_ROWS, WriteMode::Create).await;
    dataset
        .create_index(
            &["searchable_text"],
            IndexType::Inverted,
            Some("fts_idx".into()),
            &fts_params(config),
            true,
        )
        .await
        .unwrap();
    dataset = write(&path, BASE_ROWS..ROWS, WriteMode::Append).await;
    dataset
        .optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    let before = partitions(&dataset).await;
    assert_eq!(before.len(), 2, "base and delta segment");
    let mut options = OptimizeOptions::merge(usize::MAX);
    options.index_names = Some(vec!["fts_idx".into()]);
    dataset.optimize_indices(&options).await.unwrap();
    let after = partitions(&dataset).await;
    std::fs::remove_dir_all(path).unwrap();
    (before, after)
}

/// One writer configuration in its own process: apply its settings as the
/// writer's start-up does, then run the lifecycle and report to the parent.
#[test]
#[ignore = "child process of the full-text partition test"]
fn partition_child_process() {
    let writer = std::env::var("FYRR_TEST_FTS_WRITER").unwrap();
    let target = std::env::var("FYRR_TEST_FTS_REPORT").unwrap();
    let config = config(&writer);
    crate::threads::apply(&config.writer);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (before, after) = runtime.block_on(lifecycle(&config, "child"));
    std::fs::write(target, format!("{before:?} {after:?}")).unwrap();
}

/// Runs the lifecycle for one `[writer]` table in a child process.
fn child(writer: &str) -> String {
    let target = std::env::temp_dir().join(format!(
        "fyrr-fts-partition-{}-{}",
        std::process::id(),
        writer.len()
    ));
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            CHILD,
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("FYRR_TEST_FTS_WRITER", writer)
        .env("FYRR_TEST_FTS_REPORT", &target)
        .stdout(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "child for {writer:?}");
    let report = std::fs::read_to_string(&target).unwrap();
    std::fs::remove_file(target).unwrap();
    report
}

fn counts(report: &str) -> (Vec<usize>, Vec<usize>) {
    let (before, after) = report.split_once("] [").unwrap();
    let parse = |text: &str| {
        text.trim_matches(['[', ']'])
            .split(", ")
            .map(|n| n.parse().unwrap())
            .collect::<Vec<usize>>()
    };
    (parse(before), parse(after))
}

#[test]
fn absent_target_keeps_one_partition_and_a_set_target_keeps_several_through_merges() {
    // Absent: Lance's default, one partition per segment, one after a merge.
    let default = child("");
    let (before, after) = counts(&default);
    assert_eq!(before, [1, 1], "default build: {default}");
    assert_eq!(after, [1], "default merge: {default}");

    // Set: the build closes partitions at 1 MiB and the merge repacks to it.
    let target = child("fts_partition_memory_mb = 1");
    let (before, after) = counts(&target);
    assert!(before[0] > 1, "build makes several partitions: {target}");
    assert!(after[0] > 1, "merge keeps several partitions: {target}");

    // Composed with writer.threads: the workers come from threads, the size
    // from the target.
    let both = child("threads = 4\nfts_partition_memory_mb = 1");
    let (before, after) = counts(&both);
    assert!(before[0] > 1, "build with threads: {both}");
    assert!(after[0] > 1, "merge with threads: {both}");
}
