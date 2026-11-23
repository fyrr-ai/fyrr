//! The request-filter selection answers `(admission) AND (filter)` from the
//! scalar indexes. It must select exactly the rows the earlier scan selected
//! with the admission mask attached, which Lance plans as a read of every
//! admitted row, and it must still refuse a row outside the admission.
use super::*;
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{BooleanArray, Int64Array, RecordBatchIterator, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use fyrr_core::attributes::AttributeType;
use fyrr_core::limits::mcp::MAX_REGEX_BYTES;
use lance::dataset::{WriteMode, WriteParams};
use serde_json::json;

const INDEXED_ROWS: usize = 120;
const TAIL_ROWS: usize = 40;
const HOUR: i64 = 3_600_000_000;
const EPOCH: i64 = 1_700_000_000_000_000;

/// A filter case: its label, its tree, and the rows it must select.
type Case = (&'static str, serde_json::Value, Box<dyn Fn(usize) -> bool>);

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
        // Typed attributes, as the writer declares them: nullable columns.
        Field::new("rank", DataType::Int64, true),
        // The same values under a name that is an SQL keyword when unquoted.
        Field::new("true", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new(
            "seen",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("note", DataType::Utf8, true),
    ]))
}

/// The typed attributes of the fixture; `ratio` has no column and only
/// serves the compiler.
fn attributes() -> AttributeSchema {
    AttributeSchema::from([
        ("rank".to_owned(), AttributeType::Int),
        ("true".to_owned(), AttributeType::Int),
        ("flag".to_owned(), AttributeType::Bool),
        ("seen".to_owned(), AttributeType::Datetime),
        ("ratio".to_owned(), AttributeType::Float),
        ("note".to_owned(), AttributeType::String),
    ])
}

/// Null for every fourth row, so that a missing value is common.
fn rank(row: usize) -> Option<i64> {
    (!row.is_multiple_of(4)).then_some((row % 7) as i64)
}

fn flag(row: usize) -> Option<bool> {
    (!row.is_multiple_of(3)).then_some(row.is_multiple_of(2))
}

fn seen(row: usize) -> Option<i64> {
    (!row.is_multiple_of(5)).then_some(EPOCH + row as i64 * HOUR)
}

/// Text that holds what a pattern must not take for syntax: `_`, `%`, a quote
/// and a backslash, in two cases. Null for every fourth row.
fn note(row: usize) -> Option<String> {
    match row % 4 {
        0 => None,
        1 => Some(format!("Alpha_{row}")),
        2 => Some(format!("alpha%{row}'")),
        _ => Some(format!("a\\b{row}")),
    }
}

/// The SQL the reader compiles a filter tree to, for the fixture's attributes.
fn compiled(tree: serde_json::Value) -> String {
    let tree: Filter = serde_json::from_value(tree).unwrap();
    PreparedReader::filter_sql(&tree, &attributes()).unwrap()
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
            Arc::new(Int64Array::from(
                range.clone().map(rank).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                range.clone().map(rank).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                range.clone().map(flag).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(range.clone().map(seen).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                range.clone().map(note).collect::<Vec<_>>(),
            )),
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
        ("rank", "attr_rank_idx", IndexType::BTree),
        ("true", "attr_true_idx", IndexType::BTree),
        ("flag", "attr_flag_idx", IndexType::Bitmap),
        ("seen", "attr_seen_idx", IndexType::BTree),
        ("note", "attr_note_idx", IndexType::BTree),
        ("note", "attrngram_note_idx", IndexType::NGram),
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
    let seen_after = chrono::DateTime::from_timestamp_micros(EPOCH + 60 * HOUR)
        .unwrap()
        .to_rfc3339();
    let mut filters: Vec<(&'static str, String)> = vec![
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
    ];
    // Every operator, over indexed and unindexed fields and over missing values.
    let on_rank =
        |op: &str, value: serde_json::Value| json!({"field":"rank","op":op,"value":value});
    let on_flag = |op: &str, value: bool| json!({"field":"flag","op":op,"value":value});
    let on_note = |op: &str, value: &str| json!({"field":"note","op":op,"value":value});
    for (name, tree) in [
        ("text_starts_with", on_note("starts_with", "Alpha")),
        ("text_not_starts_with", on_note("not_starts_with", "Alpha")),
        ("text_contains", on_note("contains", "lpha")),
        ("text_not_contains", on_note("not_contains", "lpha")),
        ("text_short_contains", on_note("contains", "a")),
        ("text_matches", on_note("matches", r"^Alpha_\d+$")),
        ("text_not_matches", on_note("not_matches", r"\d{2}")),
        (
            "text_on_a_label",
            json!({"field":"author","op":"starts_with","value":"a1"}),
        ),
        (
            "text_or",
            json!({"or": [on_note("contains", "lpha"), on_rank("eq", json!(1))]}),
        ),
        ("eq", on_rank("eq", json!(3))),
        ("ne", on_rank("ne", json!(3))),
        ("in", on_rank("in", json!([1, 2]))),
        ("not_in", on_rank("not_in", json!([1, 2]))),
        ("lt", on_rank("lt", json!(3))),
        ("lte", on_rank("lte", json!(3))),
        ("gt", on_rank("gt", json!(3))),
        ("gte", on_rank("gte", json!(3))),
        ("null", on_rank("eq", json!(null))),
        ("not_null", on_rank("ne", json!(null))),
        ("keyword", json!({"field":"true","op":"ne","value":3})),
        (
            "keyword_null",
            json!({"field":"true","op":"eq","value":null}),
        ),
        ("bool", on_flag("eq", true)),
        ("bool_ne", on_flag("ne", true)),
        ("not", json!({"not": on_rank("gt", json!(4))})),
        ("not_eq", json!({"not": on_rank("eq", json!(3))})),
        ("not_not", json!({"not": {"not": on_rank("lt", json!(3))}})),
        (
            "datetime",
            json!({"field":"seen","op":"gt","value":seen_after}),
        ),
        (
            "datetime_null",
            json!({"field":"seen","op":"eq","value":null}),
        ),
        (
            "datetime_in",
            json!({"field":"seen","op":"not_in","value":[seen_after]}),
        ),
        (
            "or_indexed",
            json!({"or": [on_rank("eq", json!(1)), on_flag("eq", true)]}),
        ),
        (
            "or_unindexed",
            json!({"or": [on_rank("eq", json!(1)), {"field":"root_id","op":"eq","value":"root-005"}]}),
        ),
        (
            "not_or",
            json!({"not": {"or": [on_rank("gt", json!(4)), on_flag("eq", false)]}}),
        ),
        (
            "nested",
            json!({"and": [
                {"or": [{"field":"author","op":"in","value":["a1","a2"]}, on_flag("eq", true)]},
                {"not": {"and": [on_rank("lte", json!(2)), {"field":"project","op":"ne","value":"p3"}]}},
                {"field":"author","op":"ne","value":null},
            ]}),
        ),
    ] {
        filters.push((name, compiled(tree)));
    }
    filters
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

/// The eight search arguments compile to the tree, and the tree to the rows
/// their former SQL selected.
#[tokio::test]
async fn the_search_arguments_select_the_rows_of_their_former_sql() {
    let (dataset, root) = fixture("arguments", true).await;
    let (_, admission_predicate, hidden) = subjects().remove(0);
    let eligible = admission(&dataset, &admission_predicate, &hidden).await;
    let at = |hours: i64| {
        chrono::DateTime::from_timestamp_micros(EPOCH + hours * HOUR)
            .unwrap()
            .to_rfc3339()
    };
    let former = filters();
    for (name, arguments) in [
        ("source", json!({"sources": ["s0"]})),
        ("author", json!({"authors": ["a1", "a4"]})),
        (
            "time",
            json!({"updated_after": at(30), "updated_before": at(100)}),
        ),
        (
            "label",
            json!({"attributes": [{"key":"team","values":["blue"]}]}),
        ),
        (
            "combined",
            json!({
                "projects": ["p1", "p2"],
                "authors": ["a1", "a2"],
                "attributes": [{"key":"team","values":["red"]}],
                "exclude": [{"source_id":"s1","root_id":"root-001"}],
            }),
        ),
    ] {
        let mut arguments = arguments;
        arguments["query"] = json!("x");
        let arguments: SearchInput = serde_json::from_value(arguments).unwrap();
        let tree = arguments.filter_tree().unwrap();
        let sql = PreparedReader::filter_sql(&tree, &attributes()).unwrap();
        let select = |filter: &str| {
            let predicate = format!("({admission_predicate}) AND ({filter})");
            let eligible = &eligible;
            let dataset = &dataset;
            async move {
                rows_of(
                    &PreparedReader::filter_rows(dataset, &predicate, eligible)
                        .await
                        .unwrap(),
                )
            }
        };
        let old = former.iter().find(|(old, _)| *old == name).unwrap();
        let selected = select(&sql).await;
        assert!(!selected.is_empty(), "{name}");
        assert_eq!(selected, select(&old.1).await, "{name}: {sql}");
    }
    std::fs::remove_dir_all(root).unwrap();
}

/// The rows (by their number in `rows`) that a filter selects.
async fn selected_numbers(dataset: &Dataset, filter: &str) -> BTreeSet<usize> {
    let mut scan = dataset.scan();
    scan.project(&["root_id"]).unwrap();
    scan.filter(filter).unwrap();
    let batch = scan.try_into_batch().await.unwrap();
    strings(&batch, "root_id")
        .unwrap()
        .iter()
        .map(|root| root.unwrap()["root-".len()..].parse().unwrap())
        .collect()
}

/// The rows each text test selects, whether an index answers it or the rows
/// are in a fragment the indexes do not cover, and none of them takes a
/// pattern character for syntax.
#[tokio::test]
async fn a_text_test_selects_the_rows_whose_text_it_names() {
    let (dataset, root) = fixture("text", true).await;
    let live = |row: &usize| !(row % 5 == 4 && row % 4 == 1);
    let all: Vec<usize> = (0..INDEXED_ROWS + TAIL_ROWS).filter(live).collect();
    let on = |op: &str, value: &str| json!({"field":"note","op":op,"value":value});
    let has = |row: usize, test: &dyn Fn(&str) -> bool| note(row).is_some_and(|n| test(&n));
    let cases: Vec<Case> = vec![
        (
            "starts_with is case-sensitive",
            on("starts_with", "Alpha"),
            Box::new(move |r| has(r, &|n| n.starts_with("Alpha"))),
        ),
        (
            "% is literal",
            on("starts_with", "alpha%"),
            Box::new(move |r| has(r, &|n| n.starts_with("alpha%"))),
        ),
        (
            "_ is not a wildcard",
            on("starts_with", "Alpha_1"),
            Box::new(move |r| has(r, &|n| n.starts_with("Alpha_1"))),
        ),
        (
            "not_starts_with leaves out a missing value",
            on("not_starts_with", "Alpha"),
            Box::new(move |r| has(r, &|n| !n.starts_with("Alpha"))),
        ),
        (
            "contains",
            on("contains", "lpha"),
            Box::new(move |r| has(r, &|n| n.contains("lpha"))),
        ),
        (
            "contains is case-sensitive",
            on("contains", "ALPHA"),
            Box::new(|_| false),
        ),
        (
            "contains takes a quote",
            on("contains", "'"),
            Box::new(move |r| has(r, &|n| n.contains('\''))),
        ),
        (
            "contains takes a backslash",
            on("contains", "\\"),
            Box::new(move |r| has(r, &|n| n.contains('\\'))),
        ),
        (
            "not_contains leaves out a missing value",
            on("not_contains", "lpha"),
            Box::new(move |r| has(r, &|n| !n.contains("lpha"))),
        ),
        (
            "matches",
            on("matches", r"^Alpha_\d+$"),
            Box::new(move |r| has(r, &|n| n.starts_with("Alpha_"))),
        ),
        (
            "matches without a literal of three characters",
            on("matches", r"a.b"),
            Box::new(move |r| has(r, &|n| n.contains('\\'))),
        ),
        (
            "matches takes an escaped backslash",
            on("matches", r"a\\b"),
            Box::new(move |r| has(r, &|n| n.contains('\\'))),
        ),
        (
            "matches takes a backslash",
            on("matches", r"\\"),
            Box::new(move |r| has(r, &|n| n.contains('\\'))),
        ),
        (
            "starts_with takes a backslash",
            on("starts_with", "a\\"),
            Box::new(move |r| has(r, &|n| n.starts_with("a\\"))),
        ),
        (
            "matches everything but a missing value",
            on("matches", ".*"),
            Box::new(move |r| has(r, &|_| true)),
        ),
        (
            "not_matches of everything is empty",
            on("not_matches", ".*"),
            Box::new(|_| false),
        ),
        (
            "not of a match of everything is empty",
            json!({"not": on("matches", ".*")}),
            Box::new(|_| false),
        ),
        (
            "not_matches leaves out a missing value",
            on("not_matches", "^Alpha"),
            Box::new(move |r| has(r, &|n| !n.starts_with("Alpha"))),
        ),
        (
            "not of a test leaves out a missing value",
            json!({"not": on("contains", "lpha")}),
            Box::new(move |r| has(r, &|n| !n.contains("lpha"))),
        ),
    ];
    for (name, tree, matches) in &cases {
        let sql = compiled(tree.clone());
        let expected: BTreeSet<usize> = all.iter().copied().filter(|r| matches(*r)).collect();
        assert_eq!(
            expected.is_empty(),
            [
                "contains is case-sensitive",
                "not_matches of everything is empty",
                "not of a match of everything is empty"
            ]
            .contains(name),
            "{name}"
        );
        assert_eq!(
            selected_numbers(&dataset, &sql).await,
            expected,
            "{name}: {sql}"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

/// What a text test compiles to, what it refuses, and the bounds of an
/// expression, which is the one pattern a caller writes in a language.
#[test]
fn a_text_test_compiles_to_one_call_and_refuses_what_it_cannot_bound() {
    let compile = |field: &str, op: &str, value: serde_json::Value| {
        let tree = json!({"field":field,"op":op,"value":value});
        PreparedReader::filter_sql(&serde_json::from_value(tree).unwrap(), &attributes())
    };
    for (field, op, value, sql) in [
        (
            "note",
            "starts_with",
            json!("it's"),
            "starts_with(`note`,'it''s')",
        ),
        (
            "note",
            "matches",
            json!("a"),
            "regexp_like(`note`,'(?-m)a','m')",
        ),
        (
            "note",
            "not_starts_with",
            json!(r"a\b"),
            r"NOT starts_with(`note`,'a\b')",
        ),
        ("note", "contains", json!("50%_"), "contains(`note`,'50%_')"),
        (
            "note",
            "matches",
            json!(r"^a\d+'"),
            r"regexp_like(`note`,'(?-m)^a\d+''','m')",
        ),
        (
            "note",
            "not_matches",
            json!("x|y"),
            "NOT regexp_like(`note`,'(?-m)x|y','m')",
        ),
        (
            "author",
            "starts_with",
            json!("a"),
            "starts_with(`author`,'a')",
        ),
        (
            "source_id",
            "contains",
            json!("s"),
            "contains(`source_id`,'s')",
        ),
    ] {
        assert_eq!(
            compile(field, op, value).as_deref(),
            Ok(sql),
            "{field} {op}"
        );
    }
    let near = "a".repeat(MAX_REGEX_BYTES);
    assert!(compile("note", "matches", json!(near)).is_ok());
    // A flag that is not `i`, one that turns `i` off, and a literal `(?i)`.
    for pattern in [
        "(?s)alpha",
        "(?-i)alpha",
        "(?-i:alpha)",
        "\\(\\?i\\)",
        "[(?i)]",
    ] {
        assert!(
            compile("note", "matches", json!(pattern)).is_ok(),
            "{pattern}"
        );
    }
    let long = "a".repeat(MAX_REGEX_BYTES + 1);
    // Nested repeats compile to a program far over the bound.
    let huge = "((a{100}){100}){100}";
    for (field, op, value) in [
        // An expression over its bound, one that is not one, and one whose
        // program is over the compile bound.
        ("note", "matches", json!(long)),
        ("note", "not_matches", json!(long)),
        ("note", "matches", json!("(")),
        ("note", "matches", json!("a{2,1}")),
        ("note", "matches", json!(huge)),
        ("note", "not_matches", json!(huge)),
        // Case-insensitive matching is not offered, by any inline flag.
        ("note", "matches", json!("(?i)alpha")),
        ("note", "not_matches", json!("(?i)alpha")),
        ("note", "matches", json!("(?i:alpha)")),
        ("note", "matches", json!("(?mi)alpha")),
        ("note", "matches", json!("a(?s-m:b)(?i:c)")),
        // An empty value, which DataFusion would turn into `IS NOT NULL`.
        ("note", "starts_with", json!("")),
        ("note", "not_starts_with", json!("")),
        ("note", "contains", json!("")),
        ("note", "not_contains", json!("")),
        ("note", "matches", json!("")),
        ("note", "not_matches", json!("")),
        // A value of another type, a missing value, and a field that is not a string.
        ("note", "starts_with", json!(1)),
        ("note", "starts_with", json!(null)),
        ("note", "contains", json!(null)),
        ("note", "matches", json!(["a"])),
        ("note", "not_contains", json!(true)),
        ("rank", "starts_with", json!("1")),
        ("rank", "contains", json!("1")),
        ("flag", "matches", json!("true")),
        ("seen", "not_starts_with", json!("2026")),
        ("attributes", "contains", json!("team=blue")),
        ("nope", "starts_with", json!("a")),
    ] {
        assert_eq!(
            compile(field, op, value.clone()),
            Err(InvalidArgument),
            "{field} {op}"
        );
    }
    // The operators are a closed set: no case-insensitive test is offered.
    let unknown = json!({"field":"note","op":"icontains","value":"a"});
    assert!(serde_json::from_value::<Filter>(unknown).is_err());
}

/// A missing attribute matches `eq null` and `ne null` only; `ne`, `not_in`
/// and `not` of a comparison do not turn it into a match.
#[tokio::test]
async fn a_missing_attribute_matches_only_a_null_test() {
    let (dataset, root) = fixture("nulls", true).await;
    let live = |row: &usize| !(row % 5 == 4 && row % 4 == 1);
    let all: Vec<usize> = (0..INDEXED_ROWS + TAIL_ROWS).filter(live).collect();
    let on_rank =
        |op: &str, value: serde_json::Value| json!({"field":"rank","op":op,"value":value});
    let on_flag = |op: &str, value: bool| json!({"field":"flag","op":op,"value":value});
    let after = EPOCH + 60 * HOUR;
    let after_text = chrono::DateTime::from_timestamp_micros(after)
        .unwrap()
        .to_rfc3339();
    let cases: Vec<Case> = vec![
        (
            "eq null",
            on_rank("eq", json!(null)),
            Box::new(|r| rank(r).is_none()),
        ),
        (
            "ne null",
            on_rank("ne", json!(null)),
            Box::new(|r| rank(r).is_some()),
        ),
        (
            "eq",
            on_rank("eq", json!(3)),
            Box::new(|r| rank(r) == Some(3)),
        ),
        (
            "ne",
            on_rank("ne", json!(3)),
            Box::new(|r| rank(r).is_some_and(|v| v != 3)),
        ),
        (
            "not_in",
            on_rank("not_in", json!([1, 2])),
            Box::new(|r| rank(r).is_some_and(|v| v != 1 && v != 2)),
        ),
        (
            "not eq",
            json!({"not": on_rank("eq", json!(3))}),
            Box::new(|r| rank(r).is_some_and(|v| v != 3)),
        ),
        (
            "not lt",
            json!({"not": on_rank("lt", json!(3))}),
            Box::new(|r| rank(r).is_some_and(|v| v >= 3)),
        ),
        (
            "not or",
            json!({"not": {"or": [on_rank("gt", json!(4)), on_flag("eq", false)]}}),
            Box::new(|r| rank(r).is_some_and(|v| v <= 4) && flag(r) == Some(true)),
        ),
        (
            "or null",
            json!({"or": [on_rank("eq", json!(null)), on_rank("eq", json!(6))]}),
            Box::new(|r| rank(r).is_none() || rank(r) == Some(6)),
        ),
        (
            "keyword ne",
            json!({"field":"true","op":"ne","value":3}),
            Box::new(|r| rank(r).is_some_and(|v| v != 3)),
        ),
        (
            "bool ne",
            on_flag("ne", true),
            Box::new(|r| flag(r) == Some(false)),
        ),
        (
            "datetime gt",
            json!({"field":"seen","op":"gt","value":after_text}),
            Box::new(move |r| seen(r).is_some_and(|v| v > after)),
        ),
        (
            "datetime null",
            json!({"field":"seen","op":"eq","value":null}),
            Box::new(|r| seen(r).is_none()),
        ),
    ];
    for (name, tree, matches) in &cases {
        let sql = compiled(tree.clone());
        let expected: BTreeSet<usize> = all.iter().copied().filter(|r| matches(*r)).collect();
        assert!(!expected.is_empty(), "{name}");
        assert_eq!(
            selected_numbers(&dataset, &sql).await,
            expected,
            "{name}: {sql}"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_value_of_the_wrong_type_or_field_is_invalid_argument() {
    let compile = |tree: serde_json::Value| {
        PreparedReader::filter_sql(&serde_json::from_value(tree).unwrap(), &attributes())
    };
    let on = |field: &str, op: &str, value: serde_json::Value| json!({"field":field,"op":op,"value":value});
    for (field, op, value) in [
        ("nope", "eq", json!(1)),
        ("vector", "eq", json!(1)),
        ("rank", "eq", json!("3")),
        ("rank", "eq", json!(3.5)),
        ("rank", "eq", json!(true)),
        ("rank", "in", json!(3)),
        ("rank", "in", json!([1, "2"])),
        ("rank", "in", json!([null])),
        ("rank", "lt", json!(null)),
        ("rank", "not_in", json!(null)),
        ("flag", "eq", json!(1)),
        ("seen", "eq", json!("yesterday")),
        ("seen", "eq", json!(5)),
        ("ratio", "eq", json!("1.5")),
        ("author", "eq", json!(5)),
        ("updated_at", "gt", json!("2026-13-01T00:00:00Z")),
        ("attributes", "eq", json!("team=blue")),
        ("attributes", "in", json!([1])),
        ("attributes", "in", json!(null)),
    ] {
        assert_eq!(
            compile(on(field, op, value.clone())),
            Err(InvalidArgument),
            "{field} {op} {value}"
        );
    }
    // A bad value anywhere in the tree fails the whole filter.
    assert_eq!(
        compile(json!({"not": {"or": [on("rank", "eq", json!(1)), on("rank", "eq", json!("x"))]}})),
        Err(InvalidArgument)
    );
    // Values the types accept, and the SQL they become.
    for (field, op, value, sql) in [
        ("rank", "eq", json!(-3), "`rank` = -3"),
        ("ratio", "gt", json!(2), "`ratio` > 2.0"),
        ("ratio", "lte", json!(0.5), "`ratio` <= 0.5"),
        ("flag", "eq", json!(false), "`flag` = false"),
        (
            "author",
            "not_in",
            json!(["it's"]),
            "`author` NOT IN ('it''s')",
        ),
        ("author", "ne", json!(null), "`author` IS NOT NULL"),
        (
            "seen",
            "gte",
            json!("2026-01-02T04:04:05.5+01:00"),
            "`seen` >= timestamp(6) '2026-01-02 03:04:05.500000'",
        ),
        (
            "attributes",
            "in",
            json!(["a=b", "c=d"]),
            "array_has_any(attributes,['a=b','c=d'])",
        ),
    ] {
        assert_eq!(compile(on(field, op, value)).as_deref(), Ok(sql));
    }
}

/// The shapes `docs/mcp-tools.md` says are answered from indexes alone: a
/// plan without a refine step reads no row. An `or` needs every side indexed.
#[tokio::test]
async fn an_or_is_answered_from_indexes_only_when_every_side_is_indexed() {
    let (dataset, root) = fixture("plan-or", false).await;
    let (_, admission_predicate, _) = subjects().remove(0);
    let on = |field: &str, op: &str, value: serde_json::Value| json!({"field":field,"op":op,"value":value});
    for (name, tree, from_indexes) in [
        (
            "or of indexed fields",
            json!({"or": [on("author", "eq", json!("a1")), on("rank", "gt", json!(3))]}),
            true,
        ),
        (
            "or with an unindexed field",
            json!({"or": [on("author", "eq", json!("a1")), on("root_id", "eq", json!("root-005"))]}),
            false,
        ),
        (
            "and with an unindexed field",
            json!({"and": [on("author", "eq", json!("a1")), on("root_id", "eq", json!("root-005"))]}),
            false,
        ),
        (
            "not of an indexed field",
            json!({"not": on("author", "eq", json!("a1"))}),
            true,
        ),
        (
            "and of indexed fields",
            json!({"and": [on("author", "ne", json!("a1")), on("flag", "eq", json!(true))]}),
            true,
        ),
    ] {
        let predicate = format!("({admission_predicate}) AND ({})", compiled(tree));
        let plan = PreparedReader::filter_scan(&dataset, &predicate)
            .unwrap()
            .explain_plan(true)
            .await
            .unwrap();
        assert_eq!(
            plan.contains("refine_filter=--"),
            from_indexes,
            "{name}: {plan}"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}
