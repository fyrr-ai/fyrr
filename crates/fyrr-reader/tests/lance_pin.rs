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
    dataset::{
        WriteMode, WriteParams,
        scanner::{RowAddrMask, RowAddrTreeMap},
    },
    index::DatasetIndexExt,
};
use lance_index::{
    IndexType,
    optimize::OptimizeOptions,
    scalar::{
        FullTextSearchQuery, InvertedIndexParams,
        inverted::{
            DocumentGranularity,
            query::{
                BooleanQuery, FtsQuery, MatchQuery, MultiMatchQuery, Occur, Operator, PhraseQuery,
            },
        },
    },
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

/// A tokenizer's probes: each query with the ids of the rows it hits.
type Hits<'a> = &'a [(&'a str, &'a [i64])];

/// How the Unicode word tokenizer (Lance's `icu`) cuts Chinese and Japanese
/// text, against the `simple` tokenizer, which takes a run of such characters
/// as one token. Stemming and stop words are off, so that only the
/// segmentation decides the hits. The tokenizer finds words by a dictionary
/// that ships with Lance: `北京` and `天安门`, `東京` are words, and `北`, `東`
/// and `京` alone are not tokens of the texts that hold them.
#[tokio::test]
async fn unicode_word_tokenizer_segments_chinese_and_japanese_by_dictionary() {
    let root = directory("unicode-tokens");
    let rows: Vec<(i64, String)> = [
        (0, "我爱北京天安门"),
        (1, "私は東京に住んでいます"),
        (2, "Hello World"),
    ]
    .map(|(id, text)| (id, text.to_owned()))
    .into();
    let mut dataset = write(&root.join("cjk.lance"), &rows, WriteMode::Create).await;
    // (tokenizer, [(query, ids of the rows that hit)])
    let probes: [(&str, Hits); 2] = [
        (
            "simple",
            &[
                ("我爱北京天安门", &[0]),
                ("北京", &[]),
                ("私は東京に住んでいます", &[1]),
                ("東京", &[]),
                ("hello", &[2]),
            ],
        ),
        (
            "icu",
            &[
                ("北京", &[0]),
                ("天安门", &[0]),
                ("北", &[]),
                ("東京", &[1]),
                ("東", &[]),
                ("京", &[]),
                ("hello", &[2]),
            ],
        ),
    ];
    for (tokenizer, queries) in probes {
        let params = InvertedIndexParams::default()
            .base_tokenizer(tokenizer.into())
            .with_position(false)
            .stem(false)
            .remove_stop_words(false);
        index(&mut dataset, params, true).await;
        for (query, hits) in queries {
            let found: Vec<i64> = scores(&dataset, FullTextSearchQuery::new((*query).into()), None)
                .await
                .into_keys()
                .collect();
            assert_eq!(found, *hits, "{tokenizer}: {query}");
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

/// Every language the settings offer has a stemmer and a stop-word list in the
/// pinned Lance and is listed in `docs/tuning.md`. A language that a newer Lance
/// adds is not offered until it is added here.
#[test]
fn every_language_the_settings_offer_has_a_stemmer_and_stop_words_and_is_documented() {
    use fyrr_core::attributes::analyzer::Language;
    use lance_index::scalar::inverted::tokenizer::Language as Lance;
    let _: fn(Lance) = |language| match language {
        Lance::Arabic
        | Lance::Danish
        | Lance::Dutch
        | Lance::English
        | Lance::Finnish
        | Lance::French
        | Lance::German
        | Lance::Greek
        | Lance::Hungarian
        | Lance::Italian
        | Lance::Norwegian
        | Lance::Portuguese
        | Lance::Romanian
        | Lance::Russian
        | Lance::Spanish
        | Lance::Swedish
        | Lance::Tamil
        | Lance::Turkish => {}
    };
    let docs =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/tuning.md"))
            .unwrap();
    for language in Language::ALL {
        let params = InvertedIndexParams::default()
            .language(&language.lance_name())
            .unwrap()
            .stem(true)
            .remove_stop_words(true);
        params.build().unwrap();
        let name = serde_json::to_value(language).unwrap();
        assert!(
            docs.contains(&format!("`{}`", name.as_str().unwrap())),
            "{name} is not in docs/tuning.md"
        );
    }
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

/// The string filters of `filter`: a prefix is answered from a B-tree, a
/// substring and an expression with a literal of three characters from an
/// n-gram index, and the rest, the negations and the short patterns, by a scan
/// that returns the same rows as the index would. A column that has both
/// indexes is held to its rows only: Lance may scan it.
#[tokio::test]
async fn string_filters_use_the_btree_and_the_ngram_index_where_they_can() {
    let kinds = ["alpha", "bravo", "charlie"];
    let names: Vec<Option<String>> = (0..200)
        .map(|row| Some(format!("item-{row:04}-{}", kinds[row % 3])))
        .chain([None])
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("both", DataType::Utf8, true),
    ]));
    let column = || Arc::new(StringArray::from(names.clone())) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![column(), column(), column()]).unwrap();
    let root = directory("string-filters");
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        root.join("strings.lance").to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    for (column, kind, index) in [
        ("name", IndexType::BTree, "name_idx"),
        ("text", IndexType::NGram, "text_idx"),
        ("both", IndexType::BTree, "both_btree_idx"),
        ("both", IndexType::NGram, "both_ngram_idx"),
    ] {
        dataset
            .create_index(
                &[column],
                kind,
                Some(index.to_owned()),
                &lance_index::scalar::ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
    }
    let mut failures = Vec::new();
    for (filter, rows, indexed) in [
        ("starts_with(both,'item-001')", 10, None),
        ("contains(both,'bravo')", 67, None),
        (r"regexp_like(both,'(?-m)^item-001\d-alpha$','m')", 3, None),
        ("starts_with(name,'item-00')", 100, Some(true)),
        ("starts_with(name,'item-001')", 10, Some(true)),
        ("contains(text,'bravo')", 67, Some(true)),
        (
            r"regexp_like(text,'(?-m)^item-001\d-alpha$','m')",
            3,
            Some(true),
        ),
        ("regexp_like(text,'(?-m)r.vo','m')", 67, Some(false)),
        ("contains(text,'ra')", 67, Some(false)),
        ("NOT starts_with(name,'item-00')", 100, Some(false)),
        ("NOT contains(text,'bravo')", 133, Some(false)),
        (
            r"NOT regexp_like(text,'(?-m)^item-001\d-alpha$','m')",
            197,
            Some(false),
        ),
    ] {
        let mut scan = dataset.scan();
        let outcome = async {
            scan.filter(filter).map_err(|e| e.to_string())?;
            let plan = scan.explain_plan(true).await.map_err(|e| e.to_string())?;
            let found = scan.count_rows().await.map_err(|e| e.to_string())?;
            if found != rows {
                return Err(format!("{found} rows, not {rows}"));
            }
            if indexed.is_some_and(|indexed| plan.contains("ScalarIndexQuery") != indexed) {
                return Err(format!("indexed is not {indexed:?}: {plan}"));
            }
            Ok(())
        }
        .await;
        if let Err(error) = outcome {
            failures.push(format!("{filter}: {error}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    std::fs::remove_dir_all(root).unwrap();
}

/// The `_distance` of a stored vector under each metric is the one the
/// namespace documents, on both element types the namespace may store: one
/// minus the cosine similarity, the squared Euclidean distance, and one minus
/// the dot product. The rescoring and the recall sample rank by it.
#[tokio::test]
async fn flat_distance_of_each_metric_on_both_element_types() {
    use arrow_array::{FixedSizeListArray, Float16Array, types::Float16Type, types::Float32Type};
    use lance_index::vector::flat::compute_distance;
    use lance_linalg::distance::DistanceType;
    const DIM: i32 = 8;
    let rows: Vec<Vec<f32>> = (0..4)
        .map(|r| {
            (0..DIM as usize)
                .map(|d| ((r * 3 + d * 5) % 7) as f32 - 2.5)
                .collect()
        })
        .collect();
    let query: Vec<f32> = (0..DIM as usize).map(|d| (d % 3) as f32 - 0.5).collect();
    let dot = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
    let expected = |metric: DistanceType, row: &[f32]| match metric {
        DistanceType::Cosine => {
            1.0 - dot(&query, row) / (dot(&query, &query) * dot(row, row)).sqrt()
        }
        DistanceType::L2 => query.iter().zip(row).map(|(q, r)| (q - r).powi(2)).sum(),
        DistanceType::Dot => 1.0 - dot(&query, row),
        _ => unreachable!(),
    };
    for half in [true, false] {
        let vectors: ArrayRef = if half {
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float16Type, _, _>(
                    rows.iter()
                        .map(|r| Some(r.iter().map(|v| Some(half::f16::from_f32(*v))))),
                    DIM,
                ),
            )
        } else {
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    rows.iter().map(|r| Some(r.iter().map(|v| Some(*v)))),
                    DIM,
                ),
            )
        };
        let key: ArrayRef = if half {
            Arc::new(Float16Array::from_iter_values(
                query.iter().map(|v| half::f16::from_f32(*v)),
            ))
        } else {
            Arc::new(Float32Array::from(query.clone()))
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            vectors.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![vectors]).unwrap();
        for metric in [DistanceType::Cosine, DistanceType::L2, DistanceType::Dot] {
            let scored = compute_distance(key.clone(), metric, "vector", batch.clone())
                .await
                .unwrap();
            let distances = scored
                .column_by_name("_distance")
                .unwrap()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            for (row, values) in rows.iter().enumerate() {
                let want = expected(metric, values);
                assert!(
                    (distances.value(row) - want).abs() < 1e-3,
                    "{metric} half={half} row {row}: {} is not {want}",
                    distances.value(row)
                );
            }
        }
    }
}

/// Rows of three to eight words of a small vocabulary, written in three parts
/// that the inverted index covers as three segments.
async fn segmented(name: &str, rows: i64, params: &InvertedIndexParams) -> (Dataset, Vec<String>) {
    const WORDS: [&str; 8] = [
        "alpha", "beta", "gamma", "delta", "omega", "sigma", "kappa", "theta",
    ];
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let texts: Vec<String> = (0..rows)
        .map(|n| {
            (0..3 + n % 6)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    WORDS[usize::try_from(state % 8).unwrap()]
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    let path = directory(name).join("segments.lance");
    let rows_of = |range: std::ops::Range<usize>| -> Vec<(i64, String)> {
        range
            .map(|n| (i64::try_from(n).unwrap(), texts[n].clone()))
            .collect()
    };
    let third = texts.len() / 3;
    let mut dataset = write(&path, &rows_of(0..third), WriteMode::Create).await;
    index(&mut dataset, params.clone(), false).await;
    for range in [third..2 * third, 2 * third..texts.len()] {
        dataset = write(&path, &rows_of(range), WriteMode::Append).await;
        dataset
            .optimize_indices(&OptimizeOptions::append())
            .await
            .unwrap();
    }
    assert_eq!(dataset.load_indices().await.unwrap().len(), 3);
    (dataset, texts)
}
/// The score of every row `query` finds, the best `limit` of them, through a
/// row mask as the reader passes one.
async fn found(
    dataset: &Dataset,
    query: FtsQuery,
    limit: Option<i64>,
    allowed: Option<&RowAddrTreeMap>,
) -> BTreeMap<i64, f32> {
    let mut scan = dataset.scan();
    scan.prefilter(true).with_row_id();
    if let Some(allowed) = allowed {
        scan.with_row_id_prefilter(RowAddrMask::from_allowed(allowed.clone()));
    }
    scan.full_text_search(FullTextSearchQuery::new_query(query).limit(limit))
        .unwrap();
    let batch = scan.try_into_batch().await.unwrap();
    let ids = batch["id"].as_any().downcast_ref::<Int64Array>().unwrap();
    let scores = batch["_score"]
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    ids.values()
        .iter()
        .copied()
        .zip(scores.values().iter().copied())
        .collect()
}
/// The row ids of the rows with an even `id`.
async fn even_rows(dataset: &Dataset) -> RowAddrTreeMap {
    let mut scan = dataset.scan();
    scan.project(&["id"]).unwrap().with_row_id();
    let batch = scan.try_into_batch().await.unwrap();
    let ids = batch["id"].as_any().downcast_ref::<Int64Array>().unwrap();
    let rows = batch["_rowid"]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    ids.values()
        .iter()
        .zip(rows.values())
        .filter(|(id, _)| *id % 2 == 0)
        .map(|(_, row)| *row)
        .collect()
}
/// The best ten of the rows a query finds are those an unlimited search ranks
/// first: the same ten scores, and the same score for each row.
fn assert_true_top_10(top: &BTreeMap<i64, f32>, every: &BTreeMap<i64, f32>) {
    let mut ranked: Vec<f32> = every.values().copied().collect();
    ranked.sort_by(|a, b| b.total_cmp(a));
    let mut best: Vec<f32> = top.values().copied().collect();
    best.sort_by(|a, b| b.total_cmp(a));
    assert_eq!(best.len(), 10.min(every.len()));
    assert_eq!(best, ranked[..best.len()]);
    for (id, score) in top {
        assert!((every[id] - score).abs() < 1e-6, "row {id}");
    }
}

#[tokio::test]
async fn all_terms_and_prefix_queries_return_the_true_top_10_on_three_segments_and_under_a_mask() {
    let tokens = |text: &str| text.split(' ').map(str::to_owned).collect::<Vec<_>>();
    let (dataset, texts) = segmented(
        "all-terms",
        900,
        &InvertedIndexParams::default().with_position(false),
    )
    .await;
    let mask = even_rows(&dataset).await;
    let terms = "alpha beta";
    let holds_all: Vec<i64> = texts
        .iter()
        .enumerate()
        .filter(|(_, text)| tokens(terms).iter().all(|term| tokens(text).contains(term)))
        .map(|(id, _)| i64::try_from(id).unwrap())
        .collect();
    assert!(holds_all.len() > 100, "the probe needs many matches");
    let all = || MatchQuery::new(terms.into()).with_column(Some("content".into()));
    // A match query that needs every term, and the boolean query of one match
    // per term that must hold: both find the rows with all terms, and their
    // best ten are the best ten of an unlimited search.
    let queries = [
        FtsQuery::Match(all().with_operator(Operator::And)),
        FtsQuery::Boolean(BooleanQuery::new(tokens(terms).into_iter().map(|term| {
            (
                Occur::Must,
                FtsQuery::Match(MatchQuery::new(term).with_column(Some("content".into()))),
            )
        }))),
    ];
    for query in &queries {
        for allowed in [None, Some(&mask)] {
            let every = found(&dataset, query.clone(), None, allowed).await;
            let expected: Vec<i64> = holds_all
                .iter()
                .copied()
                .filter(|id| allowed.is_none() || id % 2 == 0)
                .collect();
            assert_eq!(every.keys().copied().collect::<Vec<_>>(), expected);
            let top = found(&dataset, query.clone(), Some(10), allowed).await;
            assert_true_top_10(&top, &every);
        }
    }
    // The match query without the operator finds a row with any term.
    let any = found(&dataset, FtsQuery::Match(all()), None, None).await;
    assert!(any.len() > holds_all.len());

    // An index of the n-grams of the start of a value gives every n-gram the
    // same position, which Lance takes as alternatives: the AND match query
    // finds the values that start with any one prefix, not with the whole
    // text, so `contains` refuses these indexes until the pin moves (#170).
    // This fails when Lance changes that.
    let (dataset, texts) = segmented(
        "prefix",
        900,
        &InvertedIndexParams::default()
            .base_tokenizer("ngram".into())
            .ngram_min_length(1)
            .ngram_max_length(16)
            .ngram_prefix_only(true)
            .with_position(false),
    )
    .await;
    let prefix = "alpha be";
    let query = FtsQuery::Match(
        MatchQuery::new(prefix.into())
            .with_column(Some("content".into()))
            .with_operator(Operator::And),
    );
    let every = found(&dataset, query, None, None).await;
    let starts: Vec<i64> = texts
        .iter()
        .enumerate()
        .filter(|(_, text)| text.starts_with(prefix))
        .map(|(id, _)| i64::try_from(id).unwrap())
        .collect();
    assert!(starts.iter().all(|id| every.contains_key(id)));
    assert!(
        every.len() > starts.len(),
        "{} rows for the {} that start with the text",
        every.len(),
        starts.len()
    );
}

/// A table of `id`, a string column `title`, a string column `body` and a
/// string array column `tags`, written in three parts that each text index
/// covers as one segment.
async fn attributes(name: &str, rows: i64) -> (Dataset, Vec<[String; 2]>) {
    const WORDS: [&str; 8] = [
        "alpha", "beta", "gamma", "delta", "omega", "sigma", "kappa", "theta",
    ];
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut text = |words: i64| -> String {
        (0..words)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                WORDS[usize::try_from(state % 8).unwrap()]
            })
            .collect::<Vec<_>>()
            .join(" ")
    };
    let texts: Vec<[String; 2]> = (0..rows)
        .map(|n| [text(1 + n % 3), text(3 + n % 6)])
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
    ]));
    let path = directory(name).join("attributes.lance");
    let part = |range: std::ops::Range<usize>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    range.clone().map(|n| i64::try_from(n).unwrap()),
                )),
                Arc::new(StringArray::from_iter_values(
                    range.clone().map(|n| texts[n][0].as_str()),
                )),
                Arc::new(StringArray::from_iter_values(
                    range.map(|n| texts[n][1].as_str()),
                )),
            ],
        )
        .unwrap()
    };
    let third = texts.len() / 3;
    let params = InvertedIndexParams::default().with_position(false);
    let mut dataset = None;
    for (n, range) in [0..third, third..2 * third, 2 * third..texts.len()]
        .into_iter()
        .enumerate()
    {
        let next = Dataset::write(
            RecordBatchIterator::new(vec![Ok(part(range))], schema.clone()),
            path.to_str().unwrap(),
            Some(WriteParams {
                mode: if n == 0 {
                    WriteMode::Create
                } else {
                    WriteMode::Append
                },
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let mut next = next;
        if n == 0 {
            for column in ["title", "body"] {
                next.create_index(
                    &[column],
                    IndexType::Inverted,
                    Some(format!("{column}_fts")),
                    &params,
                    false,
                )
                .await
                .unwrap();
            }
        } else {
            next.optimize_indices(&OptimizeOptions::append())
                .await
                .unwrap();
        }
        dataset = Some(next);
    }
    let dataset = dataset.unwrap();
    assert_eq!(dataset.load_indices().await.unwrap().len(), 6);
    (dataset, texts)
}

/// The text ranking over several attributes, each with its own weight: a
/// boolean query of one match per attribute scores a row by the sum of the
/// weighted scores of its attributes, and its best ten are those of an
/// unlimited search, on three segments, under a row mask, for either operator.
#[tokio::test]
async fn weighted_attribute_query_returns_the_true_top_10_on_three_segments_and_under_a_mask() {
    let (dataset, texts) = attributes("weighted", 900).await;
    let mask = even_rows(&dataset).await;
    let terms = "alpha beta";
    let one = |column: &str, boost: f32, operator: Operator| {
        FtsQuery::Match(
            MatchQuery::new(terms.into())
                .with_column(Some(column.into()))
                .with_boost(boost)
                .with_operator(operator),
        )
    };
    for operator in [Operator::Or, Operator::And] {
        let weighted = FtsQuery::Boolean(BooleanQuery::new([
            (Occur::Should, one("title", 3.0, operator)),
            (Occur::Should, one("body", 1.0, operator)),
        ]));
        for allowed in [None, Some(&mask)] {
            let every = found(&dataset, weighted.clone(), None, allowed).await;
            let title = found(&dataset, one("title", 1.0, operator), None, allowed).await;
            let body = found(&dataset, one("body", 1.0, operator), None, allowed).await;
            assert!(every.len() > 100, "the probe needs many matches");
            // A row of both attributes scores the weighted sum, and a row of
            // one attribute scores that attribute's weighted score.
            assert_eq!(
                every.keys().collect::<Vec<_>>(),
                title
                    .keys()
                    .chain(body.keys())
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
            );
            for (id, score) in &every {
                let want = 3.0 * title.get(id).unwrap_or(&0.0) + body.get(id).unwrap_or(&0.0);
                assert!(
                    (want - score).abs() < 1e-4,
                    "row {id}: {score} is not {want}"
                );
                assert!(allowed.is_none() || id % 2 == 0);
            }
            let top = found(&dataset, weighted.clone(), Some(10), allowed).await;
            assert_true_top_10(&top, &every);
            // One attribute alone is a match query: its boost scales the score.
            let boosted = found(&dataset, one("title", 3.0, operator), None, allowed).await;
            assert_eq!(
                boosted.keys().collect::<Vec<_>>(),
                title.keys().collect::<Vec<_>>()
            );
            for (id, score) in &boosted {
                assert!((3.0 * title[id] - score).abs() < 1e-4, "row {id}");
            }
        }
    }
    // The probe is not vacuous: the weight moves rows.
    assert!(
        texts
            .iter()
            .any(|[title, body]| title.contains("alpha") && !body.contains("alpha"))
    );
}

/// A string array indexed with one document per element: a match never spans
/// two elements, a row with several matching elements is returned once for
/// each of them, and a row mask applies.
#[tokio::test]
async fn string_array_indexed_per_element_never_matches_across_two_elements() {
    let tags: [&[&str]; 5] = [
        &["alpha gamma", "beta delta"],
        &["alpha beta", "gamma"],
        &["alpha", "beta"],
        &["beta alpha", "alpha beta alpha"],
        &["omega"],
    ];
    let field = Arc::new(Field::new("item", DataType::Utf8, false));
    let offsets = OffsetBuffer::from_lengths(tags.iter().map(|row| row.len()));
    let values = StringArray::from(
        tags.iter()
            .flat_map(|row| row.iter().copied())
            .collect::<Vec<_>>(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", DataType::List(field.clone()), false),
        Field::new("title", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..5)),
            Arc::new(ListArray::new(field, offsets, Arc::new(values), None)),
            Arc::new(StringArray::from(vec![
                "alpha", "alpha", "beta", "beta", "omega",
            ])),
        ],
    )
    .unwrap();
    let path = directory("tags").join("tags.lance");
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        path.to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    dataset
        .create_index(
            &["tags"],
            IndexType::Inverted,
            Some("tags_fts".into()),
            &InvertedIndexParams::default()
                .with_position(false)
                .document_granularity(DocumentGranularity::ListElement),
            false,
        )
        .await
        .unwrap();
    dataset
        .create_index(
            &["title"],
            IndexType::Inverted,
            Some("title_fts".into()),
            &InvertedIndexParams::default().with_position(false),
            false,
        )
        .await
        .unwrap();
    let ask = |operator: Operator| {
        FtsQuery::Match(
            MatchQuery::new("alpha beta".into())
                .with_column(Some("tags".into()))
                .with_operator(operator),
        )
    };
    let rows = async |query: FtsQuery, allowed: Option<&RowAddrTreeMap>| {
        let mut scan = dataset.scan();
        scan.prefilter(true).with_row_id();
        if let Some(allowed) = allowed {
            scan.with_row_id_prefilter(RowAddrMask::from_allowed(allowed.clone()));
        }
        scan.full_text_search(FullTextSearchQuery::new_query(query))
            .unwrap();
        let batch = scan.try_into_batch().await.unwrap();
        assert!(batch.column_by_name("_doc_index").is_some());
        let ids = batch["id"].as_any().downcast_ref::<Int64Array>().unwrap();
        let scores = batch["_score"]
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        ids.values()
            .iter()
            .copied()
            .zip(scores.values().iter().copied())
            .collect::<Vec<_>>()
    };
    let sorted_ids = |rows: Vec<(i64, f32)>| {
        let mut ids: Vec<i64> = rows.into_iter().map(|row| row.0).collect();
        ids.sort_unstable();
        ids
    };
    // Rows 1 and 3 hold both words in one element; rows 0 and 2 only across
    // two. A row is returned once for each element that matches: row 3 twice.
    let all = rows(ask(Operator::And), None).await;
    assert_eq!(sorted_ids(all.clone()), [1, 3, 3]);
    let mut any = sorted_ids(rows(ask(Operator::Or), None).await);
    any.dedup();
    assert_eq!(any, [0, 1, 2, 3]);
    // The boost of the match scales the score of each element.
    let boosted = FtsQuery::Match(
        MatchQuery::new("alpha beta".into())
            .with_column(Some("tags".into()))
            .with_operator(Operator::And)
            .with_boost(3.0),
    );
    let by_row = |mut rows: Vec<(i64, f32)>| {
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        rows
    };
    let (all, boosted) = (by_row(all), by_row(rows(boosted, None).await));
    assert_eq!(boosted.len(), all.len());
    for ((id, score), (boosted_id, boosted_score)) in all.iter().zip(&boosted) {
        assert_eq!(id, boosted_id);
        assert!((3.0 * score - boosted_score).abs() < 1e-4);
    }
    let mut masked = RowAddrTreeMap::new();
    let mut scan = dataset.scan();
    scan.project(&["id"]).unwrap().with_row_id();
    let batch = scan.try_into_batch().await.unwrap();
    let ids = batch["id"].as_any().downcast_ref::<Int64Array>().unwrap();
    let row_ids = batch["_rowid"]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    for (id, row) in ids.values().iter().zip(row_ids.values()) {
        if *id != 1 {
            masked.insert(*row);
        }
    }
    let masked = rows(ask(Operator::And), Some(&masked)).await;
    assert_eq!(sorted_ids(masked), [3, 3]);

    // One query over an index of one document per element and one of one per
    // row is refused, as a boolean query and as a multi-match query: an array
    // stands alone in the text ranking.
    let leaf = |column: &str| MatchQuery::new("alpha".into()).with_column(Some(column.into()));
    let mixed = [
        FtsQuery::Boolean(BooleanQuery::new([
            (Occur::Should, FtsQuery::Match(leaf("tags"))),
            (Occur::Should, FtsQuery::Match(leaf("title"))),
        ])),
        FtsQuery::MultiMatch(MultiMatchQuery {
            match_queries: vec![leaf("tags"), leaf("title")],
        }),
    ];
    for query in mixed {
        let mut scan = dataset.scan();
        scan.prefilter(true).with_row_id();
        let refused = match scan.full_text_search(FullTextSearchQuery::new_query(query)) {
            Err(_) => true,
            Ok(_) => scan.try_into_batch().await.is_err(),
        };
        assert!(refused);
    }
}
