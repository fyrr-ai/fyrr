//! Real-S3 round trip through the pinned Lance on the per-worktree RustFS
//! fixture: storage options come from the fyrr configuration, the reader opens
//! exact versions, and bad credentials fail instead of reading.
//!
//! Ignored by plain `cargo test`; `scripts/ci` starts the fixture and runs it
//! with `--ignored`. Without the fixture environment it fails, never skips.

#![deny(
    unreachable_pub,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::semicolon_if_nothing_returned,
    clippy::ignored_unit_patterns,
    clippy::assigning_clones,
    clippy::format_push_string,
    clippy::items_after_statements
)]
#![allow(clippy::unwrap_used)] // Fixture setup failures are assertions.
use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fyrr_core::config::{Environment, Severity, StorageOptions, parse};
use fyrr_reader::storage::{Dataset, open_version};
use lance::dataset::{WriteMode, WriteParams};
use lance::io::{ObjectStoreParams, StorageOptionsAccessor};

const REASON: &str =
    "needs the RustFS fixture: scripts/s3-fixture start; eval \"$(scripts/s3-fixture env)\"";

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set ({REASON})"))
}

fn storage_options(bucket: &str, endpoint: &str, secret_env: &str) -> StorageOptions {
    let text = format!(
        r#"
[namespace]
name = "s3-round-trip"
[storage]
uri = "s3://{bucket}/round-trip"
endpoint = "{endpoint}"
allow_http = true
access_key_id = {{ env = "FYRR_TEST_S3_ACCESS_KEY_ID" }}
secret_access_key = {{ env = "{secret_env}" }}
[identity]
hashing_key = {{ env = "FYRR_TEST_IDENTITY_KEY" }}
[embedding]
endpoint = "http://127.0.0.1:9/v1"
"#
    );
    let loaded = parse(&text).expect("test config parses");
    let errors: Vec<_> = loaded
        .validate()
        .into_iter()
        .filter(|issue| issue.severity == Severity::Error)
        .collect();
    assert!(errors.is_empty(), "{errors:?}");
    let environment = |name: &str| match name {
        "FYRR_TEST_IDENTITY_KEY" => Some("s3-round-trip-test-key".to_owned()),
        "FYRR_TEST_WRONG_SECRET" => Some("not-the-secret".to_owned()),
        _ => std::env::var(name).ok(),
    };
    loaded
        .config
        .storage
        .object_store_options(&environment as &dyn Environment)
        .expect("secrets resolve")
}

fn batch(ids: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("record_id", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
    ]));
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        ids.iter().enumerate().map(|(row, _)| {
            Some(
                (0..4)
                    .map(|dim| Some((row * 4 + dim) as f32))
                    .collect::<Vec<_>>(),
            )
        }),
        4,
    );
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(ids.to_vec())), Arc::new(vectors)],
    )
    .expect("batch")
}

async fn write(uri: &str, options: &StorageOptions, mode: WriteMode, ids: &[&str]) -> Dataset {
    let batch = batch(ids);
    let schema = batch.schema();
    let params = WriteParams {
        mode,
        store_params: Some(ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                options.clone().into_map().into_iter().collect(),
            ))),
            ..Default::default()
        }),
        ..Default::default()
    };
    Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri,
        Some(params),
    )
    .await
    .expect("write to real S3")
}

async fn record_ids(dataset: &Dataset) -> Vec<String> {
    let batch = dataset.scan().try_into_batch().await.expect("scan");
    let column = batch
        .column_by_name("record_id")
        .expect("record_id column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8");
    let mut ids: Vec<String> = column
        .iter()
        .map(|id| id.expect("non-null").to_owned())
        .collect();
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the RustFS fixture (scripts/s3-fixture); run by scripts/ci"]
async fn lance_dataset_round_trips_through_real_s3() {
    let endpoint = required("FYRR_TEST_S3_ENDPOINT");
    let bucket = required("FYRR_TEST_S3_BUCKET");
    required("FYRR_TEST_S3_ACCESS_KEY_ID");
    required("FYRR_TEST_S3_SECRET_ACCESS_KEY");
    let options = storage_options(&bucket, &endpoint, "FYRR_TEST_S3_SECRET_ACCESS_KEY");
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    let uri = format!("s3://{bucket}/round-trip/{unique}/dataset.lance");

    let created = write(&uri, &options, WriteMode::Create, &["a#0", "b#0", "c#0"]).await;
    let first = created.version().version;
    let appended = write(&uri, &options, WriteMode::Append, &["d#0", "e#0"]).await;
    let second = appended.version().version;
    assert!(second > first, "append creates a new version");

    // The reader pins exact versions: the first version still reads as written.
    let pinned = open_version(&uri, options.clone(), first, None)
        .await
        .expect("open first version");
    assert_eq!(pinned.version().version, first);
    assert_eq!(pinned.count_rows(None).await.expect("count"), 3);
    assert_eq!(record_ids(&pinned).await, ["a#0", "b#0", "c#0"]);
    let latest = open_version(&uri, options, second, None)
        .await
        .expect("open second version");
    assert_eq!(
        record_ids(&latest).await,
        ["a#0", "b#0", "c#0", "d#0", "e#0"]
    );

    // Storage with wrong credentials fails; it never reads as empty.
    let wrong = storage_options(&bucket, &endpoint, "FYRR_TEST_WRONG_SECRET");
    assert!(
        open_version(&uri, wrong, first, None).await.is_err(),
        "wrong credentials must fail"
    );
}
