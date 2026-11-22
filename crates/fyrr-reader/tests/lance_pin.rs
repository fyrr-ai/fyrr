//! Native local Lance proofs for FTS correctness, index size and centroid routing.
//! Synthetic data only; real-S3 tests separately exercise storage and admission.
//! Pinned to the stable crates.io release, including its default data format.
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
#![allow(clippy::print_stdout)]
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int64Array,
    ListArray, RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray,
    UInt32Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use lance::{
    Dataset,
    dataset::{WriteMode, WriteParams},
    index::DatasetIndexExt,
};
use lance_index::{
    IndexType,
    optimize::OptimizeOptions,
    scalar::{FullTextSearchQuery, InvertedIndexParams, inverted::query::PhraseQuery},
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

fn directory(name: &str) -> PathBuf {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/lance-pin")
        .join(format!("{}-{name}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    path
}
async fn write(path: &Path, rows: &[(i64, String)], mode: WriteMode) -> Dataset {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("content", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.1.as_str()),
            )),
        ],
    )
    .unwrap();
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
async fn index(dataset: &mut Dataset, params: InvertedIndexParams, replace: bool) {
    dataset
        .create_index(
            &["content"],
            IndexType::Inverted,
            Some("fts".into()),
            &params,
            replace,
        )
        .await
        .unwrap();
}
async fn scores(
    dataset: &Dataset,
    query: FullTextSearchQuery,
    filter: Option<&str>,
) -> BTreeMap<i64, f32> {
    let mut scan = dataset.scan();
    scan.full_text_search(query).unwrap();
    if let Some(filter) = filter {
        scan.prefilter(true).filter(filter).unwrap();
    }
    let plan = scan.explain_plan(true).await.unwrap();
    assert!(
        plan.contains("MatchQuery") || plan.contains("PhraseQuery"),
        "native FTS plan required: {plan}"
    );
    let batch = scan.try_into_batch().await.unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let scores = batch
        .column_by_name("_score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    ids.values()
        .iter()
        .copied()
        .zip(scores.values().iter().copied())
        .collect()
}
fn index_bytes(path: &Path) -> u64 {
    if path.is_file() {
        return path.metadata().unwrap().len();
    }
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| index_bytes(&entry.unwrap().path()))
        .sum()
}
fn bm25(n: f32, df: f32, total_tokens: f32) -> f32 {
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln() * 2.2
        / (1.0 + 1.2 * (0.25 + 0.75 * 2.0 / (total_tokens / n)))
}

#[test]
fn native_rabitq_centroid_codes_and_f32_routing_order() {
    use arrow_array::{Array, FixedSizeListArray, UInt32Array, UInt64Array, types::Float32Type};
    use lance_index::vector::{
        CENTROID_DIST_COLUMN, PART_ID_COLUMN,
        bq::{
            RQRotationType, builder::RabitQuantizer, storage::RabitQuantizationStorage,
            transform::RQTransformer,
        },
        ivf::storage::IvfModel,
        quantizer::QuantizerStorage,
        storage::{DistCalculator, VectorStore},
        transform::Transformer,
    };
    use lance_linalg::distance::{DistanceType, l2::l2_distance};

    for dim in [16, 1024] {
        let matrix: Vec<f32> = (0..32)
            .flat_map(|row| {
                (0..dim).map(move |col| {
                    ((row * 13 + col * 7 + 1) as f32).sin() * (0.25 + row as f32 / 16.0)
                })
            })
            .collect();
        let vectors = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            dim,
            Arc::new(Float32Array::from(matrix.clone())),
            None,
        );
        let model = IvfModel::new(vectors.clone(), None);
        let centroids = model.centroids_array().unwrap();
        let rq = RabitQuantizer::new_with_rotation::<Float32Type>(1, dim, RQRotationType::Fast);
        let metadata = rq.metadata_ref().clone();
        let zero = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            dim,
            Arc::new(Float32Array::from(vec![0.0; dim as usize])),
            None,
        );
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(centroids.clone()),
            Arc::new(UInt64Array::from_iter_values(0..32)),
            Arc::new(UInt32Array::from(vec![0; 32])),
            Arc::new(Float32Array::from_iter_values(
                matrix
                    .chunks_exact(dim as usize)
                    .map(|v| l2_distance(v, &vec![0.0; dim as usize])),
            )),
        ];
        let schema = Arc::new(Schema::new(
            ["centroid", "_rowid", PART_ID_COLUMN, CENTROID_DIST_COLUMN]
                .into_iter()
                .zip(&arrays)
                .map(|(name, array)| Field::new(name, array.data_type().clone(), false))
                .collect::<Vec<_>>(),
        ));
        let input = RecordBatch::try_new(schema, arrays).unwrap();
        let transformer = RQTransformer::new(rq, DistanceType::L2, zero, "centroid").unwrap();
        let encoded = transformer.transform(&input).unwrap();
        let storage =
            RabitQuantizationStorage::try_from_batch(encoded, &metadata, DistanceType::L2, None)
                .unwrap();
        for offset in [0, 3, 11] {
            let query: Vec<f32> = (0..dim).map(|col| ((col + offset) as f32).cos()).collect();
            let query_array = Arc::new(Float32Array::from(query.clone()));
            let calculator =
                storage.dist_calculator(query_array.clone(), query.iter().map(|v| v * v).sum());
            for id in 0..32 {
                assert!(calculator.distance(id).is_finite(), "dim={dim}, id={id}");
            }
            let (native_ids, native_distances) = model
                .find_partitions(query_array.as_ref(), 32, DistanceType::L2)
                .unwrap();
            let mut rescored: Vec<_> = matrix
                .chunks_exact(dim as usize)
                .enumerate()
                .map(|(id, vector)| (u32::try_from(id).unwrap(), l2_distance(&query, vector)))
                .collect();
            rescored.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            assert_eq!(
                rescored.iter().map(|r| r.0).collect::<Vec<_>>(),
                native_ids.values().as_ref(),
                "dim={dim}, offset={offset}"
            );
            for ((id, exact), native) in rescored.iter().zip(native_distances.values()) {
                assert!(
                    (exact - native).abs() <= 1e-4 * exact.max(1.0),
                    "dim={dim}, id={id}, f32={exact}, native={native}"
                );
            }
            // The serving index stores float16 centroids and a normalized
            // float16 key; compare the same values after f32 conversion.
            let half_matrix: Vec<_> = matrix.iter().copied().map(half::f16::from_f32).collect();
            let half_query: Vec<_> = query.iter().copied().map(half::f16::from_f32).collect();
            let half_model = IvfModel::new(
                FixedSizeListArray::new(
                    Arc::new(Field::new("item", DataType::Float16, false)),
                    dim,
                    Arc::new(arrow_array::Float16Array::from(half_matrix.clone())),
                    None,
                ),
                None,
            );
            let (native_ids, native_distances) = half_model
                .find_partitions(
                    &arrow_array::Float16Array::from(half_query.clone()),
                    32,
                    DistanceType::L2,
                )
                .unwrap();
            let f32_query: Vec<_> = half_query.iter().map(|v| v.to_f32()).collect();
            let mut rescored: Vec<_> = half_matrix
                .chunks_exact(dim as usize)
                .enumerate()
                .map(|(id, vector)| {
                    (
                        u32::try_from(id).unwrap(),
                        l2_distance(
                            &f32_query,
                            &vector.iter().map(|v| v.to_f32()).collect::<Vec<_>>(),
                        ),
                    )
                })
                .collect();
            rescored.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            assert_eq!(
                rescored.iter().map(|r| r.0).collect::<Vec<_>>(),
                native_ids.values().as_ref(),
                "float16 dim={dim}, offset={offset}, rescored={rescored:?}, \
                    native_distances={native_distances:?}"
            );
        }
    }
}

#[tokio::test]
async fn positionless_native_fts_and_saving() {
    let root = directory("positions");
    let rows = (0..1000)
        .map(|id| (id, "alpha beta gamma delta ".repeat(16)))
        .collect::<Vec<_>>();
    let mut without = write(&root.join("without.lance"), &rows, WriteMode::Create).await;
    assert_eq!(
        without.manifest().data_storage_format.version.to_string(),
        "2.2"
    );
    let mut with = write(&root.join("with.lance"), &rows, WriteMode::Create).await;
    index(
        &mut without,
        InvertedIndexParams::default().with_position(false),
        false,
    )
    .await;
    index(
        &mut with,
        InvertedIndexParams::default().with_position(true),
        false,
    )
    .await;
    let query = FullTextSearchQuery::new("alpha beta".into());
    let a = scores(&without, query.clone(), None).await;
    let b = scores(&with, query, None).await;
    assert_eq!(a.len(), 1000);
    assert_eq!(a, b);
    let phrase = FullTextSearchQuery::new_query(PhraseQuery::new("alpha beta".into()).into());
    assert_eq!(scores(&with, phrase.clone(), None).await.len(), 1000);
    let mut scan = without.scan();
    scan.full_text_search(phrase).unwrap();
    let error = scan
        .try_into_batch()
        .await
        .expect_err("positionless index must reject phrase search");
    assert!(error.to_string().to_lowercase().contains("position"));
    let no_bytes = index_bytes(&root.join("without.lance/_indices"));
    let yes_bytes = index_bytes(&root.join("with.lance/_indices"));
    assert!(no_bytes < yes_bytes);
    println!(
        "Synthetic 1000 rows: positionless={no_bytes} bytes; positional={yes_bytes} \
            bytes; saving={} bytes; 1000 identical BM25 results",
        yes_bytes - no_bytes
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn bm25_global_statistics_deletions_merge_and_rebuild() {
    let root = directory("statistics");
    let visible = vec![
        (0, "needle filler".into()),
        (1, "filler filler".into()),
        (2, "needle filler filler filler".into()),
    ];
    let mut rows = visible.clone();
    rows.extend((3..7).map(|id| (id, "needle filler".into())));
    let path = root.join("global.lance");
    let mut global = write(&path, &rows, WriteMode::Create).await;
    index(
        &mut global,
        InvertedIndexParams::default().with_position(false),
        false,
    )
    .await;
    let extra = (7..10)
        .map(|id| (id, "needle filler".into()))
        .collect::<Vec<_>>();
    global = write(&path, &extra, WriteMode::Append).await;
    global
        .optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    assert_eq!(global.load_indices().await.unwrap().len(), 2);
    let q = FullTextSearchQuery::new("needle".into());
    let all = scores(&global, q.clone(), None).await;
    let filtered = scores(&global, q.clone(), Some("id < 3")).await;
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[&0], all[&0]);
    assert_eq!(filtered[&2], all[&2]);
    assert!((filtered[&0] - bm25(10.0, 9.0, 22.0)).abs() < 1e-5);
    let mut isolated = write(&root.join("isolated.lance"), &visible, WriteMode::Create).await;
    index(
        &mut isolated,
        InvertedIndexParams::default().with_position(false),
        false,
    )
    .await;
    let isolated_scores = scores(&isolated, q.clone(), None).await;
    assert!(
        (isolated_scores[&0] - filtered[&0]).abs() > 0.01,
        "hidden rows must affect BM25"
    );
    global.delete("id = 3 OR id = 7").await.unwrap();
    let deleted = scores(&global, q.clone(), Some("id < 3")).await;
    assert_eq!(deleted, filtered, "deleted rows still count before merge");
    global
        .optimize_indices(&OptimizeOptions::merge(2))
        .await
        .unwrap();
    assert_eq!(global.load_indices().await.unwrap().len(), 1);
    let merged = scores(&global, q, Some("id < 3")).await;
    assert_eq!(
        merged, deleted,
        "ordinary segment merge retains deleted-row statistics on this pin"
    );
    let stats: serde_json::Value =
        serde_json::from_str(&global.index_statistics("fts").await.unwrap()).unwrap();
    assert_eq!(stats["segments"][0]["num_docs"], 10);
    assert_eq!(stats["num_indexed_rows"], 8);
    index(
        &mut global,
        InvertedIndexParams::default().with_position(false),
        true,
    )
    .await;
    let rebuilt = scores(
        &global,
        FullTextSearchQuery::new("needle".into()),
        Some("id < 3"),
    )
    .await;
    assert!(
        (rebuilt[&0] - bm25(8.0, 7.0, 18.0)).abs() < 1e-5,
        "full rebuild drops deleted rows from statistics"
    );
    let stats: serde_json::Value =
        serde_json::from_str(&global.index_statistics("fts").await.unwrap()).unwrap();
    assert_eq!(stats["segments"][0]["num_docs"], 8);
    println!(
        "doc 0 scores: global/prefilter={} isolated={} deleted={} merged={} rebuilt={}; \
            prefilter returned only ids 0,2; merge retained 10 statistical docs / 8 live \
            rows; rebuild counts 8",
        filtered[&0], isolated_scores[&0], deleted[&0], merged[&0], rebuilt[&0]
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn configurable_max_token_length_default_40_and_override_128() {
    let root = directory("token-length");
    let rows = [39, 40, 41, 127, 128, 129].map(|n| (n as i64, format!("a{}", "1".repeat(n - 1))));
    let mut dataset = write(&root.join("tokens.lance"), &rows, WriteMode::Create).await;
    index(
        &mut dataset,
        InvertedIndexParams::default().with_position(false),
        false,
    )
    .await;
    for (n, token) in &rows {
        let result = scores(&dataset, FullTextSearchQuery::new(token.clone()), None).await;
        assert_eq!(
            result.len(),
            usize::from(*n < 40),
            "default token length {n}"
        );
        if *n < 40 {
            assert!(result.contains_key(n));
        }
    }
    index(
        &mut dataset,
        InvertedIndexParams::default()
            .with_position(false)
            .max_token_length(Some(128)),
        true,
    )
    .await;
    for (n, token) in &rows {
        let result = scores(&dataset, FullTextSearchQuery::new(token.clone()), None).await;
        assert_eq!(
            result.len(),
            usize::from(*n < 128),
            "override token length {n}"
        );
        if *n < 128 {
            assert!(result.contains_key(n));
        }
    }
    println!(
        "native indexed boundary hits: default 40 => 39 only; rebuilt override 128 => \
            39,40,41,127; lengths 128,129 dropped (exclusive byte bound)"
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// The attribute types a schema offers: a scalar column and an array column of
/// each element type, with the index kind `AttributeType` names, answer the
/// filters the reader compiles (`=`, `array_has`, `array_has_any`,
/// `array_has_all` and their negations) from that index. An element type
/// that fails here must not be offered.
#[tokio::test]
async fn every_offered_attribute_type_has_an_index_that_answers_its_filters() {
    use fyrr_core::attributes::{AttributeType, ElementType};
    // A new element type is not offered until it is tested here.
    let _: fn(ElementType) = |element| match element {
        ElementType::String
        | ElementType::Int
        | ElementType::Float
        | ElementType::Bool
        | ElementType::Datetime
        | ElementType::Uint
        | ElementType::Uuid => {}
    };
    // Two values of each element type, with the SQL literal of each.
    let literals = |a: &str, b: &str| [a.to_owned(), b.to_owned()];
    let samples: Vec<(ElementType, ArrayRef, [String; 2])> = vec![
        (
            ElementType::String,
            Arc::new(StringArray::from(vec!["a", "b"])),
            literals("'a'", "'b'"),
        ),
        (
            ElementType::Int,
            Arc::new(Int64Array::from(vec![-1, 2])),
            literals("-1", "2"),
        ),
        (
            ElementType::Uint,
            Arc::new(UInt64Array::from(vec![1, u64::MAX])),
            literals(
                "CAST(1 AS BIGINT UNSIGNED)",
                &format!("CAST({} AS BIGINT UNSIGNED)", u64::MAX),
            ),
        ),
        (
            ElementType::Float,
            Arc::new(Float64Array::from(vec![0.5, 1.5])),
            literals("0.5", "1.5"),
        ),
        (
            ElementType::Bool,
            Arc::new(BooleanArray::from(vec![true, false])),
            literals("true", "false"),
        ),
        (
            ElementType::Datetime,
            Arc::new(
                // 1970-01-01T00:00:01Z and 9999-12-31T00:00:00Z, which a
                // nanosecond timestamp cannot hold.
                TimestampMicrosecondArray::from(vec![1_000_000, 253_402_214_400_000_000])
                    .with_timezone("UTC"),
            ),
            literals(
                "timestamp(6) '1970-01-01 00:00:01.000000'",
                "timestamp(6) '9999-12-31 00:00:00.000000'",
            ),
        ),
        (
            ElementType::Uuid,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([[1_u8; 16], [2; 16]].into_iter()).unwrap(),
            ),
            [
                format!("arrow_cast(X'{}', 'FixedSizeBinary(16)')", "01".repeat(16)),
                format!("arrow_cast(X'{}', 'FixedSizeBinary(16)')", "02".repeat(16)),
            ],
        ),
    ];
    let take = |values: &ArrayRef, rows: Vec<Option<u32>>| {
        arrow_select::take::take(values.as_ref(), &UInt32Array::from(rows), None).unwrap()
    };
    let (mut fields, mut columns) = (Vec::new(), Vec::new());
    for (element, pair, _) in &samples {
        let name = format!("{element:?}").to_lowercase();
        // One value per row: a, b, b, a and none.
        fields.push(Field::new(
            format!("one_{name}"),
            element.scalar().arrow_type(),
            true,
        ));
        columns.push(take(pair, vec![Some(0), Some(1), Some(1), Some(0), None]));
        // The array of each row: [a, b], [b], [a], [] and none.
        let kind = AttributeType::Array(*element);
        fields.push(Field::new(format!("many_{name}"), kind.arrow_type(), true));
        let list = ListArray::new(
            Arc::new(Field::new("item", pair.data_type().clone(), false)),
            OffsetBuffer::from_lengths([2, 1, 1, 0, 0]),
            take(pair, vec![Some(0), Some(1), Some(1), Some(0)]),
            Some(NullBuffer::from(vec![true, true, true, true, false])),
        );
        assert_eq!(list.data_type(), &kind.arrow_type());
        columns.push(Arc::new(list));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let root = directory("attribute-types");
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        root.join("types.lance").to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    for (element, _, _) in &samples {
        let name = format!("{element:?}").to_lowercase();
        for (column, kind) in [
            (format!("one_{name}"), element.scalar()),
            (format!("many_{name}"), AttributeType::Array(*element)),
        ] {
            let index = match kind.index_kind().unwrap() {
                "bitmap" => IndexType::Bitmap,
                "label_list" => IndexType::LabelList,
                _ => IndexType::BTree,
            };
            dataset
                .create_index(
                    &[column.as_str()],
                    index,
                    Some(format!("{column}_idx")),
                    &lance_index::scalar::ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap_or_else(|error| panic!("{column}: {error}"));
        }
    }
    // Every filter of every type runs; the failures are reported together.
    let mut failures = Vec::new();
    for (element, _, [a, b]) in &samples {
        let name = format!("{element:?}").to_lowercase();
        let (one, many) = (format!("one_{name}"), format!("many_{name}"));
        // Rows that hold `a`: 0 and 3 of the scalar; 0 and 2 of the array.
        for (filter, rows, indexed) in [
            (format!("{one} = {a}"), 2, true),
            (format!("{one} IN ({a},{b})"), 4, true),
            (format!("array_has({many},{a})"), 2, true),
            (format!("NOT array_has({many},{a})"), 2, false),
            (format!("array_has_any({many},[{a},{b}])"), 3, true),
            (format!("array_has_all({many},[{a},{b}])"), 1, true),
            (format!("NOT array_has_any({many},[{b}])"), 2, false),
        ] {
            let mut scan = dataset.scan();
            let outcome = async {
                scan.filter(&filter).map_err(|e| e.to_string())?;
                let plan = scan.explain_plan(true).await.map_err(|e| e.to_string())?;
                let found = scan.count_rows().await.map_err(|e| e.to_string())?;
                if found != rows {
                    return Err(format!("{found} rows, not {rows}"));
                }
                if indexed && !plan.contains("ScalarIndexQuery") {
                    return Err(format!("not answered by its index: {plan}"));
                }
                Ok(())
            }
            .await;
            if let Err(error) = outcome {
                failures.push(format!("{filter}: {error}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    std::fs::remove_dir_all(root).unwrap();
}
