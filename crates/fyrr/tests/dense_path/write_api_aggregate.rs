//! Reads of the attributes of documents written through the write API:
//! listings, filters, aggregates, recall and the attribute schema of a live
//! namespace.
use super::tools::{Mcp, body, error};
use super::write_api::{
    ADMIN_KEY, READ_KEY, RECALL_KEY, WRITE_KEY, Writer, configure, embed, hits, run_when_free,
    secret, stored, vector,
};
use super::*;
use std::time::Instant;

/// A direction in one of 4 clusters with a little noise, scaled to `length`.
fn metric_vector(cluster: usize, noise: usize, length: f32) -> Vec<f32> {
    (0..1024)
        .map(|d| {
            length
                * ((((cluster + 1) * (d + 3)) as f32 * 0.37).sin()
                    + 0.15 * (((noise + 1) * (d + 11)) as f32 * 0.73).sin())
        })
        .collect()
}

/// A row: 50 rows per cluster, of lengths from 0.2 to 5.1, so that within a
/// cluster the length decides the dot product, the direction the cosine and
/// both the squared distance to a query: the three metrics rank the rows of a
/// cluster differently.
fn metric_row(n: usize) -> Vec<f32> {
    metric_vector(n % 4, n, 0.2 + (n / 4) as f32 * 0.1)
}

/// How near `row` is to `query` under `metric`: higher is nearer.
fn nearness(metric: &str, query: &[f32], row: &[f32]) -> f32 {
    let dot: f32 = query.iter().zip(row).map(|(q, r)| q * r).sum();
    let norm = |v: &[f32]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
    match metric {
        "cosine" => dot / (norm(query) * norm(row)),
        "squared_l2" => -query
            .iter()
            .zip(row)
            .map(|(q, r)| (q - r).powi(2))
            .sum::<f32>(),
        "dot" => dot,
        _ => unreachable!("{metric}"),
    }
}

/// A namespace created with a metric and an element type records them, reports
/// them and answers with the recall of that metric: the ten nearest rows by the
/// metric are among the ten hits for at least nine in ten. Index builds run on
/// two threads (`configure`).
async fn metric_recall(metric: &str, element: &str) {
    const ROWS: usize = 200;
    const QUERIES: usize = 12;
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    let text = std::fs::read_to_string(&config).unwrap();
    // Every partition of the index is probed: the recall is that of the metric.
    std::fs::write(&config, text.replace("nprobes = 1", "nprobes = 16")).unwrap();
    let writer = Writer::start(&config, &root, address).await;
    let namespace = "dense-fixture";
    let rows: Vec<Value> = (0..ROWS)
        .map(|n| json!({"id": format!("r{n}"), "text": format!("alpha{n}"), "vector": metric_row(n)}))
        .collect();
    let settings =
        json!({"dense_metric": metric, "dense_element": element, "dense_partition_rows": 32});
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": settings, "upserts": rows})
            )
            .await,
        (200, json!({"upserted": ROWS, "deleted": 0}))
    );
    // The manifest records them unless they are the defaults, which leave no entry.
    let (_, manifest) = super::coordination::publication(&config).await;
    assert_eq!(
        serde_json::to_value(&manifest).unwrap()["settings"]["dense_metric"]
            .as_str()
            .unwrap_or("cosine"),
        metric
    );
    assert_eq!(
        serde_json::to_value(&manifest).unwrap()["settings"]["dense_element"]
            .as_str()
            .unwrap_or("f16"),
        element
    );
    assert_eq!(manifest.schema_version.major, 6);
    let vectors = stored(&config, "source_id = '_api'").await;
    let column = vectors.column_by_name("vector").unwrap();
    let stored_type = format!("{:?}", column.data_type());
    assert!(
        stored_type.contains(if element == "f32" {
            "Float32"
        } else {
            "Float16"
        }),
        "{stored_type}"
    );
    // A name the pinned index has no distance or element type for is refused,
    // and another valid one is another namespace: the settings are fixed.
    for (key, other) in [("dense_metric", "hamming"), ("dense_element", "f64")] {
        assert_eq!(
            writer
                .post(
                    WRITE_KEY,
                    namespace,
                    &json!({"settings": {key: other}, "upserts": [{"id": "x", "text": "x"}]})
                )
                .await,
            (400, json!({"error": "settings_invalid"})),
            "{key}"
        );
    }
    // A number that could overflow a distance is refused by either type, and one
    // beyond 16 bits by `f16` alone: the 32-bit namespaces test the bound itself.
    let mut limits = vec![(2e6, "beyond the magnitude limit")];
    if element == "f16" {
        limits.push((70_000.0, "beyond 16 bits"));
    }
    for (number, why) in limits {
        let mut big = metric_row(0);
        big[0] = number;
        assert_eq!(
            writer
                .post(
                    WRITE_KEY,
                    namespace,
                    &json!({"upserts": [{"id": "x", "text": "x", "vector": big}]})
                )
                .await,
            (400, json!({"error": "vector_invalid"})),
            "{why}"
        );
    }
    let changed = if metric == "dot" { "cosine" } else { "dot" };
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": {"dense_metric": changed}, "upserts": [{"id": "x", "text": "x"}]})
            )
            .await,
        (409, json!({"error": "namespace_settings_fixed"}))
    );

    // A patch with a text and a vector writes the vector in the element type.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"patches": [{"id": "r0", "text": "alphapatched", "vector": metric_row(0)}]})
            )
            .await,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 1, "skipped": 0})
        )
    );
    assert_eq!(
        stored(&config, "searchable_text = 'alphapatched'")
            .await
            .num_rows(),
        1
    );

    let mut recall = 0.0;
    let mut other = std::collections::BTreeMap::new();
    for k in 0..QUERIES {
        let query = metric_vector(k % 4, 10_000 + k, 2.0);
        let text = format!("zzprobe{k}");
        root.embedding.insert_both(&text, query.clone());
        let truth = |metric: &str| {
            let mut ranked: Vec<usize> = (0..ROWS).collect();
            ranked.sort_by(|&a, &b| {
                nearness(metric, &query, &metric_row(b)).total_cmp(&nearness(
                    metric,
                    &query,
                    &metric_row(a),
                ))
            });
            ranked.truncate(10);
            ranked
        };
        let expected = truth(metric);
        // What the other metrics would answer, to show that the fixture tells
        // the metrics apart: a test that any of them passes proves nothing.
        for name in ["cosine", "squared_l2", "dot"] {
            if name != metric {
                *other.entry(name).or_insert(0.0) += truth(name)
                    .iter()
                    .filter(|row| expected.contains(row))
                    .count() as f64
                    / 10.0;
            }
        }
        let found = hits(&config, &text);
        recall += found
            .iter()
            .filter(|id| expected.iter().any(|row| **id == format!("r{row}")))
            .count() as f64
            / 10.0;
    }
    for (name, overlap) in other {
        let overlap = overlap / QUERIES as f64;
        assert!(
            overlap < 0.90,
            "{name} would hold {overlap} of {metric}'s top ten: the fixture does not separate them"
        );
    }
    let recall = recall / QUERIES as f64;
    assert!(recall >= 0.90, "{metric} {element}: recall@10 {recall}");
    drop(writer);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_holds_recall_under_cosine_on_storage() {
    metric_recall("cosine", "f16").await;
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_holds_recall_under_squared_l2_on_storage() {
    metric_recall("squared_l2", "f16").await;
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_holds_recall_under_dot_on_storage() {
    metric_recall("dot", "f16").await;
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_holds_recall_under_squared_l2_on_32_bit_vectors_on_storage() {
    metric_recall("squared_l2", "f32").await;
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_holds_recall_under_dot_on_32_bit_vectors_on_storage() {
    metric_recall("dot", "f32").await;
}

/// A listing orders the documents that pass its filter by typed attributes,
/// nulls first ascending and last descending, ties by the document key, and
/// never lists a document the caller may not read.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_orders_a_listing_by_attributes_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let texts = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;
    let row = |id: &str, text: &str, price: Option<f64>, stock: Option<i64>, readers: &str| {
        let mut row = json!({"id": id, "text": text, "readers": [readers]});
        row["price"] = json!(price);
        row["stock"] = json!(stock);
        row
    };
    let allow = "grp:allow";
    let upserts = [
        row("a", texts[0], Some(3.0), Some(2), allow),
        row("b", texts[1], Some(1.0), Some(2), allow),
        row("c", texts[2], Some(2.0), None, allow),
        row("d", texts[3], None, Some(5), allow),
        row("e", texts[4], Some(1.0), Some(1), allow),
        // The cheapest of all, but nobody may read it.
        row("hidden", texts[5], Some(0.5), Some(9), "usr:nobody"),
    ];
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": {"attributes": {"price": "float", "stock": "int"}}, "upserts": upserts})
            )
            .await,
        (200, json!({"upserted": 6, "deleted": 0}))
    );
    let mut client = Mcp::open(&config);
    let mut listed = |arguments: Value| -> Vec<String> {
        let mut arguments = arguments;
        arguments["output"] = json!("metadata");
        body(client.call("search", arguments))["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
            .collect()
    };
    let key = |field: &str, direction: &str| json!({"field": field, "direction": direction});
    assert_eq!(
        listed(json!({"order_by": [key("price", "asc")]})),
        ["d", "b", "e", "c", "a"],
        "nulls first, the tie on the document key"
    );
    assert_eq!(
        listed(json!({"order_by": [key("price", "desc")]})),
        ["a", "c", "b", "e", "d"],
        "nulls last, the tie on the document key"
    );
    assert_eq!(
        listed(json!({"order_by": [key("stock", "desc"), key("price", "asc")]})),
        ["d", "b", "a", "e", "c"]
    );
    assert_eq!(
        listed(json!({"order_by": [key("root_id", "desc")], "limit": 2})),
        ["e", "d"]
    );
    assert_eq!(
        listed(json!({
            "order_by": [key("price", "desc")],
            "filter": {"field": "stock", "op": "gte", "value": 2},
        })),
        ["a", "b", "d"],
        "only the documents that pass the filter"
    );
    for arguments in [
        json!({"order_by": [key("nope", "asc")]}),
        json!({"order_by": [key("attributes", "asc")]}),
        json!({"order_by": []}),
        json!({"order_by": vec![key("price", "asc"); 5]}),
        json!({"order_by": [{"field": "price"}]}),
        json!({"order_by": [key("price", "asc")], "query": "alpha"}),
        json!({"order_by": [key("price", "asc")], "order": "updated_at"}),
    ] {
        error(client.call("search", arguments.clone()), "invalid_argument");
    }
}

/// The JSON `aggregate` route counts, sums and groups the documents a caller
/// may read: a document it may not read changes no count, minimum or group,
/// and a selection over the row bound is `busy`.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_aggregates_the_documents_a_caller_reads_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let texts = ["alpha", "bravo", "charlie", "delta", "echo"];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;
    let upserts = [
        json!({"id": "a", "text": texts[0], "readers": ["usr:x"],
               "price": 3.0, "stock": 2, "tag": "red", "seen": "2026-01-02T03:04:05Z"}),
        json!({"id": "b", "text": texts[1], "readers": ["usr:x"],
               "price": 1.0, "stock": 2, "tag": "red"}),
        json!({"id": "c", "text": texts[2], "readers": ["usr:x"], "price": 2.0, "tag": "blue"}),
        json!({"id": "d", "text": texts[3], "readers": ["usr:x"], "stock": 5}),
        // The cheapest, the largest and in a group of its own, but nobody may read it.
        json!({"id": "hidden", "text": texts[4], "readers": ["usr:nobody"],
               "price": 0.5, "stock": 9, "tag": "secret", "seen": "2025-01-01T00:00:00Z"}),
    ];
    let settings = json!({"attributes": {
        "price": "float", "stock": "int", "tag": "string", "seen": "datetime"}});
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": settings, "upserts": upserts})
            )
            .await,
        (200, json!({"upserted": 5, "deleted": 0}))
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let ask = async |reader: &super::http_mcp::Reader, body: Value| -> (u16, Value) {
        let response = reader
            .client
            .post(format!(
                "{}/v1/namespaces/dense-fixture/aggregate",
                reader.base
            ))
            .bearer_auth(secret(READ_KEY))
            .json(&body)
            .send()
            .await
            .unwrap();
        (
            response.status().as_u16(),
            response.json::<Value>().await.unwrap(),
        )
    };
    let groups = |answer: (u16, Value)| -> Value {
        assert_eq!(answer.0, 200, "{}", answer.1);
        answer.1["groups"].clone()
    };
    let op = |op: &str, field: &str| json!({"op": op, "field": field});
    let count = json!({"op": "count"});

    // One group over every readable document: nulls are skipped, and the
    // hidden document is in no count, sum, minimum or maximum.
    assert_eq!(
        groups(
            ask(
                &reader,
                json!({"aggregations": [
                    count, op("count", "price"), op("sum", "price"), op("avg", "price"),
                    op("min", "price"), op("max", "price"), op("sum", "stock"),
                    op("min", "tag"), op("max", "seen"),
                ]})
            )
            .await
        ),
        json!([{"key": [], "values": [
            4, 3, 6.0, 2.0, 1.0, 3.0, 9, "blue", "2026-01-02T03:04:05Z"
        ]}])
    );

    // Groups come in key order, by a string or by a number; a document with no
    // value of a key is in no group.
    assert_eq!(
        groups(
            ask(
                &reader,
                json!({"group_by": ["tag"], "aggregations": [count, op("sum", "price")]})
            )
            .await
        ),
        json!([
            {"key": ["blue"], "values": [1, 2.0]},
            {"key": ["red"], "values": [2, 4.0]},
        ])
    );
    assert_eq!(
        groups(
            ask(
                &reader,
                json!({"group_by": ["stock", "tag"], "aggregations": [count]})
            )
            .await
        ),
        json!([{"key": [2, "red"], "values": [2]}])
    );
    assert_eq!(
        groups(
            ask(
                &reader,
                json!({"group_by": ["root_id"], "aggregations": [count]})
            )
            .await
        ),
        json!([
            {"key": ["a"], "values": [1]},
            {"key": ["b"], "values": [1]},
            {"key": ["c"], "values": [1]},
            {"key": ["d"], "values": [1]},
        ])
    );

    // The filter selects the documents to aggregate, as in `search`.
    assert_eq!(
        groups(
            ask(
                &reader,
                json!({"filter": {"field": "stock", "op": "gte", "value": 2},
                       "aggregations": [count]})
            )
            .await
        ),
        json!([{"key": [], "values": [3]}])
    );
    // A document the caller may not read is not counted, however it is asked for.
    assert_eq!(
        groups(
            ask(
                &reader,
                json!({"filter": {"field": "tag", "op": "eq", "value": "secret"},
                       "aggregations": [count, op("max", "stock")]})
            )
            .await
        ),
        json!([{"key": [], "values": [0, null]}])
    );
    assert_eq!(
        groups(
            ask(
                &reader,
                json!({"filter": {"field": "tag", "op": "eq", "value": "secret"},
                       "group_by": ["tag"], "aggregations": [count]})
            )
            .await
        ),
        json!([])
    );

    // Arguments the schema does not take.
    for body in [
        json!({"aggregations": []}),
        json!({"aggregations": vec![count.clone(); 17]}),
        json!({"aggregations": [{"op": "sum"}]}),
        json!({"aggregations": [op("sum", "tag")]}),
        json!({"aggregations": [op("avg", "seen")]}),
        json!({"aggregations": [op("max", "nope")]}),
        json!({"aggregations": [count], "group_by": []}),
        json!({"aggregations": [count], "group_by": ["attributes"]}),
        json!({"aggregations": [count], "group_by": ["nope"]}),
        json!({"aggregations": [count], "group_by": ["tag", "stock", "price", "seen", "root_id"]}),
        json!({"aggregations": [count], "unknown": true}),
    ] {
        assert_eq!(
            ask(&reader, body.clone()).await,
            (400, json!({"error": "invalid_argument"})),
            "{body}"
        );
    }

    // More readable rows than `reader.filter_count_max_rows` is `busy`.
    let text = std::fs::read_to_string(&config).unwrap();
    let bounded = root.join("bounded.toml");
    std::fs::write(
        &bounded,
        text.replace("nprobes = 1", "nprobes = 1\nfilter_count_max_rows = 1"),
    )
    .unwrap();
    let narrow = super::http_mcp::Reader::start_appended(&bounded, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    assert_eq!(
        ask(&narrow, json!({"aggregations": [count]})).await,
        (503, json!({"error": "busy"}))
    );
}

/// The JSON `recall` route measures the vector index on stored vectors of the
/// rows a key may read: only a key with the `admin` and the `read` role may ask,
/// a filter narrows the rows that are sampled, searched and counted, a deadline
/// bounds the run, and the answer holds counts and recall, never a document.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_reports_recall_to_an_admin_key_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let texts = ["alpha", "bravo", "charlie", "delta", "echo"];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;
    let upserts = [
        json!({"id": "a", "text": texts[0], "readers": ["usr:x"], "tag": "red"}),
        json!({"id": "b", "text": texts[1], "readers": ["usr:x"], "tag": "red"}),
        json!({"id": "c", "text": texts[2], "readers": ["usr:x"], "tag": "blue"}),
        json!({"id": "d", "text": texts[3], "readers": ["usr:x"], "tag": "blue"}),
        // Nobody the keys act for may read it: it is no row of any sample or count.
        json!({"id": "hidden", "text": texts[4], "readers": ["usr:nobody"], "tag": "secret"}),
    ];
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": {"attributes": {"tag": "string"}}, "upserts": upserts})
            )
            .await,
        (200, json!({"upserted": 5, "deleted": 0}))
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let ask = async |key: usize, body: Value| -> (u16, Value) {
        let response = reader
            .client
            .post(format!(
                "{}/v1/namespaces/dense-fixture/recall",
                reader.base
            ))
            .bearer_auth(secret(key))
            .json(&body)
            .send()
            .await
            .unwrap();
        (
            response.status().as_u16(),
            response.json::<Value>().await.unwrap(),
        )
    };

    // Only a key with both the `admin` and the `read` role may ask: one that
    // only reads, only writes, or administers without reading is refused.
    for key in [READ_KEY, WRITE_KEY, ADMIN_KEY] {
        assert_eq!(
            ask(key, json!({"count": 3})).await,
            (403, json!({"error": "forbidden"}))
        );
    }

    // Every row of a namespace this small is found. The answer holds numbers
    // alone: the hidden row is in no count, and no key, text or tag shows.
    let (status, answer) = ask(RECALL_KEY, json!({"count": 3})).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["samples"], 3);
    assert_eq!(answer["rows"], 4);
    assert_eq!(answer["recall_at_10"], 1.0);
    assert_eq!(answer["recall_at_75"], 1.0);
    assert!(answer["approximate"].is_boolean(), "{answer}");
    assert_eq!(answer.as_object().unwrap().len(), 5, "{answer}");
    // Fewer rows than `count`: every readable row is measured once.
    let (status, answer) = ask(RECALL_KEY, json!({"count": 100})).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["samples"], 4);
    assert_eq!(answer["rows"], 4);

    // The filter narrows both the rows that are sampled and the rows that are
    // searched; one that selects only what the key may not read selects nothing.
    let tag = |value: &str| json!({"field": "tag", "op": "eq", "value": value});
    let (status, answer) = ask(RECALL_KEY, json!({"count": 100, "filter": tag("red")})).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["samples"], 2);
    // Two of the four readable rows, not the four.
    assert_eq!(answer["rows"], 2);
    // Two rows are cheaper to scan than to probe: the index answered none, and
    // the answer says so rather than report a recall of two exact scans.
    assert_eq!(answer["approximate"], false);
    assert_eq!(
        ask(RECALL_KEY, json!({"count": 5, "filter": tag("secret")})).await,
        (
            200,
            json!({"samples": 0, "rows": 0, "approximate": false,
                   "recall_at_10": null, "recall_at_75": null})
        )
    );

    // A deadline that the run cannot meet is `deadline_exceeded`, with no partial answer.
    assert_eq!(
        ask(RECALL_KEY, json!({"count": 100, "deadline_ms": 1})).await,
        (408, json!({"error": "deadline_exceeded"}))
    );

    // Arguments the schema does not take.
    for body in [
        json!({}),
        json!({"count": 0}),
        json!({"count": 101}),
        json!({"count": 1, "deadline_ms": 0}),
        json!({"count": 1, "unknown": true}),
        json!({"count": 1, "filter": {"field": "nope", "op": "eq", "value": 1}}),
    ] {
        assert_eq!(
            ask(RECALL_KEY, body.clone()).await,
            (400, json!({"error": "invalid_argument"})),
            "{body}"
        );
    }
}

/// A `string` is filtered by prefix, substring and regular expression, and by
/// their negations: case-sensitively, a missing value matches none of them, a
/// document the caller may not read matches none however it is asked for, and
/// a pattern the engine cannot bound is `invalid_argument`.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_filters_strings_by_prefix_substring_and_expression_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let texts = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;
    // The values hold what a pattern must not take for syntax: `%`, `_`, a
    // quote and a backslash. Row e is unreadable and row d has no value.
    let notes = [
        json!("Alpha_one"),
        json!("alpha%two'"),
        json!("back\\slash"),
        Value::Null,
        json!("Alpha secret"),
        json!("release-2026-11-22"),
    ];
    let upserts: Vec<Value> = ["a", "b", "c", "d", "e", "f"]
        .iter()
        .enumerate()
        .map(|(n, id)| {
            let readers = if *id == "e" {
                json!(["usr:nobody"])
            } else {
                json!(["grp:allow", "usr:x"])
            };
            json!({"id": id, "text": texts[n], "readers": readers, "note": notes[n], "price": 1.5})
        })
        .collect();
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({
                    "settings": {
                        "attributes": {"note": "string", "price": "float"},
                        "attribute_indexes": {"note": {"ngram": true}},
                    },
                    "upserts": upserts,
                })
            )
            .await,
        (200, json!({"upserted": 6, "deleted": 0}))
    );
    let on = |op: &str, value: Value| json!({"field": "note", "op": op, "value": value});
    for (op, value, expected) in [
        ("starts_with", json!("Alpha"), vec!["a"]),
        ("starts_with", json!("alpha%"), vec!["b"]),
        ("starts_with", json!("alpha_"), vec![]),
        ("starts_with", json!("back\\"), vec!["c"]),
        ("matches", json!(".*"), vec!["a", "b", "c", "f"]),
        ("not_matches", json!(".*"), vec![]),
        ("not_starts_with", json!("Alpha"), vec!["b", "c", "f"]),
        ("contains", json!("lpha"), vec!["a", "b"]),
        ("contains", json!("'"), vec!["b"]),
        ("contains", json!("\\"), vec!["c"]),
        ("contains", json!("ALPHA"), vec![]),
        ("contains", json!("secret"), vec![]),
        ("not_contains", json!("lpha"), vec!["c", "f"]),
        ("matches", json!("^Alpha_\\w+$"), vec!["a"]),
        ("matches", json!("\\\\"), vec!["c"]),
        ("matches", json!("\\d{4}-\\d{2}-\\d{2}$"), vec!["f"]),
        ("matches", json!("e"), vec!["a", "f"]),
        ("not_matches", json!("\\d"), vec!["a", "b", "c"]),
    ] {
        assert_eq!(
            listing(&config, on(op, value.clone())),
            expected,
            "{op} {value}"
        );
    }
    // `not` of a test keeps a missing value out, as `not_*` does.
    assert_eq!(
        listing(&config, json!({"not": on("starts_with", json!("Alpha"))})),
        ["b", "c", "f"]
    );
    assert_eq!(
        listing(
            &config,
            json!({"and": [on("contains", json!("lpha")), on("not_matches", json!("^Alpha"))]})
        ),
        ["b"]
    );

    let mut client = Mcp::open(&config);
    let on_field = |field: &str, op: &str, value: Value| json!({"filter": {"field": field, "op": op, "value": value}});
    for arguments in [
        // An expression over the bound, one that does not parse, and one whose
        // compiled form is too large.
        on_field("note", "matches", json!("a".repeat(513))),
        on_field("note", "matches", json!("(")),
        on_field("note", "matches", json!("((a{100}){100}){100}")),
        // Case-insensitive matching is not offered.
        on_field("note", "matches", json!("(?i)ALPHA")),
        // Text tests take text and a `string` only.
        on_field("note", "starts_with", json!(1)),
        on_field("note", "contains", Value::Null),
        on_field("note", "matches", json!(["a"])),
        on_field("note", "starts_with", json!("")),
        on_field("price", "starts_with", json!("1")),
        on_field("price", "matches", json!("1")),
        on_field("price", "contains", json!("1")),
    ] {
        error(client.call("search", arguments.clone()), "invalid_argument");
    }
}

/// Unsigned, UUID, bytes and array attributes are written over HTTP and stored
/// in their own columns. A listing filters and orders by the ones that can be,
/// and refuses `bytes` and the arrays where they cannot be.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_stores_filters_and_orders_the_other_attribute_types_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let texts = ["alpha", "bravo", "charlie", "delta", "echo"];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;
    let uuid = |n: u64| json!(format!("00000000-0000-0000-0000-{n:012x}"));
    // The two values of each array type as stored, and as a filter writes them.
    let arrays = [
        (
            "tags",
            json!("string"),
            [json!("red"), json!("blue")],
            [json!("red"), json!("blue")],
        ),
        (
            "nums",
            json!("int"),
            [json!(-5), json!(7)],
            [json!(-5), json!(7)],
        ),
        (
            "ratios",
            json!("float"),
            [json!(0.5), json!(1.5)],
            [json!(0.5), json!(1.5)],
        ),
        (
            "flags",
            json!("bool"),
            [json!(true), json!(false)],
            [json!(true), json!(false)],
        ),
        (
            "stamps",
            json!("datetime"),
            [
                json!("2026-01-02T03:04:05Z"),
                json!("2026-02-03T04:05:06+01:00"),
            ],
            [
                json!("2026-01-02T04:04:05+01:00"),
                json!("2026-02-03T03:05:06Z"),
            ],
        ),
        (
            "counts",
            json!("uint"),
            [json!(1), json!(u64::MAX)],
            [json!(1), json!(u64::MAX)],
        ),
        (
            "owners",
            json!("uuid"),
            [uuid(7), uuid(8)],
            [json!(uuid(7).as_str().unwrap().to_uppercase()), uuid(8)],
        ),
    ];
    let mut attributes = json!({
        "big": "uint", "owner": "uuid", "blob": "bytes",
    });
    for (name, element, _, _) in &arrays {
        attributes[name] = json!({"array": element});
    }
    // Row a holds both values of each array, b the first, c none, d is missing
    // and e holds the second.
    let big = [
        json!(1),
        json!(u64::MAX),
        json!(1_u64 << 63),
        Value::Null,
        json!(5),
    ];
    let owner = [uuid(3), uuid(1), uuid(2), Value::Null, uuid(5)];
    let blob = [
        json!("AAEC"),
        json!("/w=="),
        Value::Null,
        Value::Null,
        json!(""),
    ];
    let upserts: Vec<Value> = ["a", "b", "c", "d", "e"]
        .iter()
        .enumerate()
        .map(|(n, id)| {
            let mut row = json!({
                "id": id, "text": texts[n], "readers": ["grp:allow", "usr:x"],
                "big": big[n], "owner": owner[n], "blob": blob[n],
            });
            for (name, _, [x, y], _) in &arrays {
                row[name] = match n {
                    0 => json!([x, y]),
                    1 => json!([x]),
                    2 => json!([]),
                    3 => Value::Null,
                    _ => json!([y]),
                };
            }
            row
        })
        .collect();
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": {"attributes": attributes}, "upserts": upserts})
            )
            .await,
        (200, json!({"upserted": 5, "deleted": 0}))
    );

    // The columns are of the types the schema names, `bytes` holds its bytes
    // and has no index, and every other attribute has the index of its kind.
    let loaded = fyrr_core::config::load(&config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    for (column, arrow) in [
        ("big", "UInt64"),
        ("owner", "FixedSizeBinary(16)"),
        ("blob", "Binary"),
    ] {
        let field = dataset.schema().field(column).unwrap();
        assert_eq!(field.data_type().to_string(), arrow, "{column}");
    }
    assert_eq!(
        dataset
            .count_rows(Some("blob = X'000102'".into()))
            .await
            .unwrap(),
        1
    );
    let (_, manifest) = super::coordination::publication(&config).await;
    let kinds: std::collections::BTreeMap<_, _> = manifest
        .indexes
        .iter()
        .map(|index| (index.column.as_str(), index.kind.as_str()))
        .collect();
    assert!(!kinds.contains_key("blob"), "bytes is not indexed");
    assert_eq!(kinds["big"], "btree");
    assert_eq!(kinds["owner"], "btree");
    for (name, ..) in &arrays {
        assert_eq!(kinds[*name], "label_list", "{name}");
    }

    let mut client = Mcp::open(&config);
    let mut listed = |arguments: Value| -> Vec<String> {
        let mut arguments = arguments;
        arguments["output"] = json!("metadata");
        body(client.call("search", arguments))["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
            .collect()
    };
    let sorted = |mut found: Vec<String>| {
        found.sort();
        found
    };
    let on = |field: &str, op: &str, value: Value| json!({"order": "updated_at", "filter": {"field": field, "op": op, "value": value}});
    // Every array type answers the five tests, a missing array none of them.
    for (name, _, _, [x, y]) in &arrays {
        for (op, value, expected) in [
            ("contains", json!(x), vec!["a", "b"]),
            ("not_contains", json!(x), vec!["c", "e"]),
            ("contains_any", json!([x, y]), vec!["a", "b", "e"]),
            ("contains_all", json!([x, y]), vec!["a"]),
            ("contains_none", json!([x]), vec!["c", "e"]),
        ] {
            assert_eq!(sorted(listed(on(name, op, value))), expected, "{name} {op}");
        }
    }
    assert_eq!(sorted(listed(on("tags", "eq", Value::Null))), ["d"]);
    assert_eq!(
        sorted(listed(on("tags", "ne", Value::Null))),
        ["a", "b", "c", "e"]
    );
    // The scalar types compare as their type does, the whole range of `uint` included.
    for (field, op, value, expected) in [
        ("big", "gt", json!(i64::MAX), vec!["b", "c"]),
        ("big", "eq", json!(u64::MAX), vec!["b"]),
        ("big", "in", json!([1, 5]), vec!["a", "e"]),
        ("big", "lt", json!(5), vec!["a"]),
        (
            "owner",
            "eq",
            json!(uuid(3).as_str().unwrap().to_uppercase()),
            vec!["a"],
        ),
        ("owner", "in", json!([uuid(1), uuid(5)]), vec!["b", "e"]),
        ("owner", "gt", uuid(2), vec!["a", "e"]),
    ] {
        assert_eq!(
            sorted(listed(on(field, op, value))),
            expected,
            "{field} {op}"
        );
    }

    // Ordering by `uint` and UUID: nulls first ascending, last descending.
    let key = |field: &str, direction: &str| json!({"field": field, "direction": direction});
    for (field, direction, expected) in [
        ("big", "asc", ["d", "a", "e", "c", "b"]),
        ("big", "desc", ["b", "c", "e", "a", "d"]),
        ("owner", "asc", ["d", "b", "c", "a", "e"]),
        ("owner", "desc", ["e", "a", "c", "b", "d"]),
    ] {
        assert_eq!(
            listed(json!({"order_by": [key(field, direction)]})),
            expected,
            "{field} {direction}"
        );
    }

    // What cannot be ordered or compared is refused, never guessed at.
    for arguments in [
        json!({"order_by": [key("blob", "asc")]}),
        json!({"order_by": [key("tags", "asc")]}),
        json!({"order_by": [key("counts", "desc")]}),
        on("blob", "eq", json!("AAEC")),
        on("blob", "eq", Value::Null),
        on("blob", "contains", json!("AAEC")),
        on("tags", "eq", json!("red")),
        on("tags", "in", json!(["red"])),
        on("tags", "lt", json!("red")),
        on("big", "contains", json!(1)),
        on("big", "contains_any", json!([1])),
        on("tags", "contains", Value::Null),
        on("tags", "contains", json!(1)),
        on("tags", "contains_any", json!("red")),
        on("tags", "contains_any", json!([])),
        on("nums", "contains_all", json!([1, "2"])),
        on("owners", "contains", json!("not a uuid")),
        on("counts", "contains", json!(-1)),
    ] {
        error(client.call("search", arguments.clone()), "invalid_argument");
    }

    // A value that does not fit its type, and a type that is not offered,
    // refuse the whole request.
    for (row, code) in [
        (json!({"blob": "not base64!"}), "attribute_value_invalid"),
        (
            json!({"owner": "00000000000000000000000000000001"}),
            "attribute_value_invalid",
        ),
        (json!({"big": -1}), "attribute_value_invalid"),
        (json!({"big": 1.5}), "attribute_type_mismatch"),
        (json!({"tags": ["red", 1]}), "attribute_type_mismatch"),
        (json!({"tags": "red"}), "attribute_type_mismatch"),
        (json!({"nums": [1, null]}), "attribute_type_mismatch"),
    ] {
        let mut row = row;
        row["id"] = json!("x");
        row["text"] = json!("alpha");
        assert_eq!(
            writer
                .post(WRITE_KEY, "dense-fixture", &json!({"upserts": [row]}))
                .await,
            (400, json!({"error": code})),
            "{code}"
        );
    }
    let (status, _) = writer
        .post(
            WRITE_KEY,
            "other-fixture",
            &json!({
                "settings": {"attributes": {"x": {"array": "bytes"}}},
                "upserts": [{"id": "x", "text": "alpha"}],
            }),
        )
        .await;
    assert_eq!(status, 400, "an array of bytes is not a type");

    // The HTTP query returns every type as its JSON form: `uint` a number,
    // a UUID its digits, `bytes` base64 and an array an array.
    let mut reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let response = reader
        .client
        .post(format!("{}/v1/namespaces/dense-fixture/query", reader.base))
        .bearer_auth(secret(READ_KEY))
        .json(&json!({
            "query": "alpha", "output": "metadata", "limit": 20,
            "fields": {"exclude": []},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let answer: Value = response.json().await.unwrap();
    let attributes = |id: &str| {
        answer["hits"]
            .as_array()
            .unwrap()
            .iter()
            .find(|hit| hit["root_id"] == id)
            .unwrap()["attributes"]
            .clone()
    };
    let expected = |row: usize| {
        let mut object = json!({"big": big[row], "owner": owner[row], "blob": blob[row]});
        for (name, _, [x, y], _) in &arrays {
            object[name] = match row {
                0 => json!([x, y]),
                1 => json!([x]),
                2 => json!([]),
                3 => Value::Null,
                _ => json!([y]),
            };
        }
        object
    };
    for (row, id) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        let mut want = expected(row);
        // A datetime comes back in UTC.
        want["stamps"] = match row {
            0 => json!(["2026-01-02T03:04:05Z", "2026-02-03T03:05:06Z"]),
            1 => json!(["2026-01-02T03:04:05Z"]),
            2 => json!([]),
            3 => Value::Null,
            _ => json!(["2026-02-03T03:05:06Z"]),
        };
        assert_eq!(attributes(id), want, "{id}");
    }
    assert_eq!(attributes("a")["counts"], json!([1, u64::MAX]));
    assert_eq!(attributes("b")["blob"], "/w==");
    reader.stop().await;
}

/// A JSON query chooses the attributes, the vector and the scores a hit
/// returns, reads only those columns, and returns nothing of a row the caller
/// may not read.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_returns_the_attributes_vectors_and_scores_a_query_asks_for_on_storage()
{
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let texts = ["alpha", "bravo", "charlie", "golf"];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;
    let upserts = [
        json!({"id": "a", "text": texts[0], "vector": vector(0, 1024), "readers": ["usr:x"],
               "price": 3.0, "stock": 2, "tag": "red", "ok": true, "seen": "2026-01-02T03:04:05Z"}),
        json!({"id": "b", "text": texts[1], "vector": vector(1, 1024), "readers": ["usr:x"],
               "price": 1.0, "ok": false}),
        json!({"id": "c", "text": texts[2], "vector": vector(2, 1024), "readers": ["usr:x"],
               "price": 2.0, "stock": 7, "tag": "blue"}),
        // As close to the query as a row can be, and the cheapest, but nobody may read it.
        json!({"id": "hidden", "text": texts[3], "vector": vector(0, 1024), "readers": ["usr:nobody"],
               "price": 0.5, "stock": 9, "tag": "secret"}),
    ];
    let settings = json!({"attributes": {
        "price": "float", "stock": "int", "tag": "string", "ok": "bool", "seen": "datetime"}});
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": settings, "upserts": upserts})
            )
            .await,
        (200, json!({"upserted": 4, "deleted": 0}))
    );
    let mut reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let ask = async |body: Value| -> (u16, Value) {
        let response = reader
            .client
            .post(format!("{}/v1/namespaces/dense-fixture/query", reader.base))
            .bearer_auth(secret(READ_KEY))
            .json(&body)
            .send()
            .await
            .unwrap();
        (
            response.status().as_u16(),
            response.json::<Value>().await.unwrap(),
        )
    };
    let search = |fields: Value| json!({"query": "alpha", "vector": vector(0, 1024), "output": "metadata", "fields": fields});
    let hits = |answer: (u16, Value)| -> Vec<Value> {
        assert_eq!(answer.0, 200, "{}", answer.1);
        answer.1["hits"].as_array().unwrap().clone()
    };
    let hit = |hits: &[Value], id: &str| {
        hits.iter()
            .find(|hit| hit["root_id"] == id)
            .unwrap()
            .clone()
    };

    // Without `fields` a hit carries neither attributes nor the extra scores.
    let mut plain = search(json!({}));
    plain.as_object_mut().unwrap().remove("fields");
    let plain = hits(ask(plain).await);
    assert_eq!(plain.len(), 3, "the hidden row is not a hit");
    for hit in &plain {
        for key in ["attributes", "lexical_score", "distance"] {
            assert!(hit.get(key).is_none(), "{key}");
        }
    }

    // Only the attributes in the include list; a value that is not set is null.
    let included = hits(ask(search(json!({"include": ["price", "tag"]}))).await);
    assert_eq!(
        hit(&included, "a")["attributes"],
        json!({"price": 3.0, "tag": "red"})
    );
    assert_eq!(
        hit(&included, "b")["attributes"],
        json!({"price": 1.0, "tag": null})
    );
    // An empty exclude list returns every attribute but the vector, typed.
    let all = hits(ask(search(json!({"exclude": []}))).await);
    assert_eq!(
        hit(&all, "a")["attributes"],
        json!({"price": 3.0, "stock": 2, "tag": "red", "ok": true, "seen": "2026-01-02T03:04:05Z"})
    );
    assert_eq!(
        hit(&all, "b")["attributes"],
        json!({"price": 1.0, "stock": null, "tag": null, "ok": false, "seen": null})
    );
    // A name the namespace does not declare is ignored in the exclude list.
    let rest = hits(ask(search(json!({"exclude": ["tag", "stock", "nope"]}))).await);
    assert_eq!(
        hit(&rest, "c")["attributes"],
        json!({"price": 2.0, "ok": null, "seen": null})
    );
    // The vector is a number array, named in the include list only.
    let vectors = hits(ask(search(json!({"include": ["vector"]}))).await);
    let stored = hit(&vectors, "a")["attributes"]["vector"]
        .as_array()
        .unwrap()
        .clone();
    let sent = vector(0, 1024);
    assert_eq!(stored.len(), sent.len());
    for (got, want) in stored.iter().zip(&sent) {
        // Stored as 16-bit floats.
        assert!((got.as_f64().unwrap() - f64::from(*want)).abs() < 2e-3);
    }
    // The scores: the lexical lane finds one row, the dense lane all of them.
    let scored = hits(ask(search(json!({"scores": true}))).await);
    let (a, b) = (hit(&scored, "a"), hit(&scored, "b"));
    assert!(a["lexical_score"].as_f64().unwrap() > 0.0);
    assert!(a["distance"].as_f64().unwrap() < 0.01);
    assert!(b.get("lexical_score").is_none());
    assert!(b["distance"].as_f64().unwrap() > a["distance"].as_f64().unwrap());
    assert!(a.get("attributes").is_none());
    // A listing returns attributes too, and still only of rows the caller may read.
    let listing = hits(
        ask(
            json!({"order_by": [{"field": "price", "direction": "asc"}], "output": "metadata",
                   "fields": {"include": ["price", "tag"]}}),
        )
        .await,
    );
    let prices: Vec<_> = listing
        .iter()
        .map(|hit| hit["attributes"]["price"].clone())
        .collect();
    assert_eq!(prices, [json!(1.0), json!(2.0), json!(3.0)]);

    // An included name the namespace does not declare is invalid; so is a
    // malformed `fields`, and the fetch route has no such member.
    for fields in [
        json!({"include": ["nope"]}),
        json!({"include": []}),
        json!({"include": ["price"], "exclude": []}),
        json!({"scores": "yes"}),
        json!({"other": true}),
        json!("all"),
    ] {
        assert_eq!(
            ask(search(fields.clone())).await,
            (400, json!({"error": "invalid_argument"})),
            "{fields}"
        );
    }
    // Also when no row matches, so nothing is hydrated.
    let mut nothing = search(json!({"include": ["nope"]}));
    nothing["filter"] = json!({"field": "root_id", "op": "eq", "value": "none"});
    assert_eq!(
        ask(nothing).await,
        (400, json!({"error": "invalid_argument"}))
    );
    let fetch = reader
        .client
        .post(format!("{}/v1/namespaces/dense-fixture/fetch", reader.base))
        .bearer_auth(secret(READ_KEY))
        .json(&json!({"source_id": "_api", "root_id": "a", "fields": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(fetch.status(), 400);
    // MCP tools keep their arguments and their output.
    let mut client = Mcp::open(&config);
    error(
        client.call("search", search(json!({"include": ["price"]}))),
        "invalid_argument",
    );
    reader.stop().await;
}

/// A listing takes an offset, and a scan by the document key visits every
/// readable document of a publication once, on twice the largest page plus one
/// documents with unreadable ones between them.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
#[allow(clippy::print_stderr)]
async fn identity_write_api_pages_a_listing_and_scans_by_the_document_key_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["alpha"]);
    let writer = Writer::start(&config, &root, address).await;
    let window = fyrr_core::limits::mcp::MAX_LISTING_WINDOW as usize;
    let readable: Vec<String> = (0..2 * window + 1).map(|n| format!("d{n:05}")).collect();
    let upserts: Vec<Value> = readable
        .iter()
        .map(|id| json!({"id": id, "text": "alpha", "readers": ["grp:allow"]}))
        // Nobody may read these, whatever the order.
        .chain((0..readable.len()).step_by(333).map(
            |n| json!({"id": format!("d{n:05}x"), "text": "alpha", "readers": ["usr:nobody"]}),
        ))
        .collect();
    assert_eq!(
        writer
            .post(WRITE_KEY, "dense-fixture", &json!({"upserts": upserts}))
            .await
            .0,
        200
    );
    let mut client = Mcp::open(&config);
    let key = |field: &str, direction: &str| json!({"field": field, "direction": direction});
    let by_key = json!([key("source_id", "asc"), key("root_id", "asc")]);
    let page = |client: &mut Mcp, arguments: Value| -> Vec<(String, String)> {
        let mut arguments = arguments;
        arguments["output"] = json!("metadata");
        body(client.call("search", arguments))["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| {
                (
                    hit["source_id"].as_str().unwrap().to_owned(),
                    hit["root_id"].as_str().unwrap().to_owned(),
                )
            })
            .collect()
    };

    // The offset counts documents.
    let offset_page = page(
        &mut client,
        json!({"order_by": by_key, "offset": 5, "limit": 3}),
    );
    assert_eq!(
        offset_page.iter().map(|hit| &hit.1).collect::<Vec<_>>(),
        readable[5..8].iter().collect::<Vec<_>>()
    );
    // A page ends within the window.
    for arguments in [
        json!({"order_by": by_key, "offset": 1, "limit": window}),
        json!({"order_by": by_key, "offset": window}),
        json!({"query": "alpha", "offset": 1}),
    ] {
        error(client.call("search", arguments), "invalid_argument");
    }

    // The scan: the key after the last one seen, until a page is empty.
    let mut seen = Vec::new();
    let mut last: Option<(String, String)> = None;
    loop {
        let mut arguments = json!({"order_by": by_key, "limit": window});
        if let Some((source, root)) = &last {
            arguments["filter"] = json!({"or": [
                {"field": "source_id", "op": "gt", "value": source},
                {"and": [
                    {"field": "source_id", "op": "eq", "value": source},
                    {"field": "root_id", "op": "gt", "value": root},
                ]},
            ]});
        }
        let hits = page(&mut client, arguments);
        let Some(end) = hits.last().cloned() else {
            break;
        };
        seen.extend(hits.iter().map(|hit| hit.1.clone()));
        last = Some(end);
    }
    assert_eq!(seen, readable);
    // The measurement behind the window: the response size and the latency of
    // a full page of metadata.
    let start = Instant::now();
    let full = client.call(
        "search",
        json!({"order_by": by_key, "limit": window, "output": "metadata"}),
    );
    eprintln!(
        "page of {window} documents: {} bytes in {:?}",
        full.to_string().len(),
        start.elapsed()
    );

    // The response budget truncates a page from its end, and the scan goes on
    // from the last document it kept.
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("[reader]\n", "[reader]\nmax_response_bytes=4096\n");
    std::fs::write(&config, text).unwrap();
    let mut bounded = Mcp::open(&config);
    let result = body(bounded.call(
        "search",
        json!({"order_by": by_key, "limit": window, "output": "metadata"}),
    ));
    assert!(result["truncated"]["hits_dropped"].as_u64().unwrap() > 0);
    let kept: Vec<_> = result["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
        .collect();
    assert!(kept.len() < window);
    assert_eq!(kept, readable[..kept.len()]);
}

/// A request that carries only a schema adds attributes to a namespace that
/// serves and sets the indexes of its attributes. It is one publication; an
/// attribute without an index is filtered by a scan, an index that is switched
/// on is built by the next maintenance, and what would change what stored
/// values mean is refused.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_changes_the_schema_of_a_live_namespace_on_storage() {
    use std::collections::BTreeSet;
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    // The maintenance of this test is the one it runs (`configure` builds on 2 threads).
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!("{text}\n[writer.serve]\nmaintenance = false\n"),
    )
    .unwrap();
    embed(&root, &["alpha", "bravo", "charlie"]);
    let writer = Writer::start(&config, &root, address).await;
    let post = async |body: Value| writer.post(WRITE_KEY, "dense-fixture", &body).await;
    let names = async || -> BTreeSet<String> {
        let (_, manifest) = super::coordination::publication(&config).await;
        manifest
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .filter(|name| name.starts_with("attr"))
            .filter(|name| name != "attributes_idx")
            .collect()
    };
    let listed = |filter: Value| listing(&config, filter);
    let set =
        |names: &[&str]| -> BTreeSet<String> { names.iter().map(|n| (*n).to_owned()).collect() };

    // A namespace is created with an index of its own kind per attribute.
    let rows = |ids: &[(&str, &str)]| -> Vec<Value> {
        ids.iter()
            .map(|(id, text)| json!({"id": id, "text": text, "readers": ["grp:allow"], "price": 2.5}))
            .collect()
    };
    assert_eq!(
        post(json!({
            "settings": {
                "attributes": {"price": "float", "note": "string"},
                "attribute_indexes": {"note": {"index": false, "ngram": true}},
            },
            "upserts": rows(&[("a", "alpha"), ("b", "bravo")]),
        }))
        .await,
        (200, json!({"upserted": 2, "deleted": 0}))
    );
    assert_eq!(
        names().await,
        set(&["attr_price_idx", "attrngram_note_idx"])
    );
    let (head, manifest) = super::coordination::publication(&config).await;
    assert_eq!(
        serde_json::to_value(&manifest.settings.namespace.attribute_indexes).unwrap(),
        json!({"note": {"index": false, "full_text": false, "ngram": true}})
    );
    assert_eq!(manifest.schema_version.to_string(), "6.0");
    let sequence = head.content.as_ref().unwrap().sequence;

    // A schema alone: a new attribute, the price index off and a text index on.
    let schema = json!({"settings": {
        "attributes": {"stock": "int", "title": "string"},
        "attribute_indexes": {"price": {"index": false}, "title": {"full_text": true}},
    }});
    assert_eq!(
        post(schema.clone()).await,
        (200, json!({"upserted": 0, "deleted": 0}))
    );
    let (head, manifest) = super::coordination::publication(&config).await;
    assert_eq!(
        head.content.as_ref().unwrap().sequence,
        sequence + 1,
        "one publication"
    );
    assert_eq!(
        manifest
            .settings
            .namespace
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["note", "price", "stock", "title"]
    );
    // The price index went with the publication. The ones that are switched on
    // are not built yet: the manifest holds what is built.
    assert_eq!(names().await, set(&["attrngram_note_idx"]));
    assert_eq!(
        serde_json::to_value(&manifest.settings.namespace.attribute_indexes).unwrap(),
        json!({
            "note": {"index": false, "full_text": false, "ngram": true},
            "price": {"index": false, "full_text": false, "ngram": false},
            "stock": {"index": false, "full_text": false, "ngram": false},
            "title": {"index": false, "full_text": false, "ngram": false},
        })
    );
    // The rows written before read null for the new attribute, and a filter on
    // an attribute with no index is a scan that answers.
    assert_eq!(
        listed(json!({"field": "stock", "op": "eq", "value": null})),
        ["a", "b"]
    );
    assert_eq!(
        listed(json!({"field": "stock", "op": "gt", "value": 0})),
        Vec::<String>::new()
    );
    assert_eq!(
        listed(json!({"field": "price", "op": "gte", "value": 2.0})),
        ["a", "b"]
    );

    // The same schema again changes nothing.
    assert_eq!(post(schema).await.0, 200);
    let (head, _) = super::coordination::publication(&config).await;
    assert_eq!(head.content.as_ref().unwrap().sequence, sequence + 1);

    // Rows with the new attribute are written and filtered by a scan until
    // maintenance builds the index.
    assert_eq!(
        post(json!({"upserts": [
            {"id": "c", "text": "charlie", "readers": ["grp:allow"], "price": 4.0, "stock": 5, "title": "Blue shirt"},
        ]}))
        .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    assert_eq!(
        listed(json!({"field": "stock", "op": "gt", "value": 2})),
        ["c"]
    );
    assert_eq!(
        listed(json!({"field": "stock", "op": "eq", "value": null})),
        ["a", "b"]
    );
    run_when_free(&config, &["writer", "maintain"]).await;
    assert_eq!(
        names().await,
        set(&[
            "attr_stock_idx",
            "attr_title_idx",
            "attrfts_title_idx",
            "attrngram_note_idx"
        ])
    );
    let (_, manifest) = super::coordination::publication(&config).await;
    assert_eq!(
        serde_json::to_value(&manifest.settings.namespace.attribute_indexes).unwrap(),
        json!({
            "note": {"index": false, "full_text": false, "ngram": true},
            "price": {"index": false, "full_text": false, "ngram": false},
            "title": {"index": true, "full_text": true, "ngram": false},
        })
    );
    assert_eq!(
        listed(json!({"field": "stock", "op": "gt", "value": 2})),
        ["c"]
    );
    assert_eq!(
        listed(json!({"field": "stock", "op": "eq", "value": null})),
        ["a", "b"]
    );
    assert_eq!(success(&config, &["writer", "verify"])["verified"], true);

    // What would change the meaning of a stored value is refused with its name,
    // and so is a request that changes anything else; nothing is published.
    let (head, _) = super::coordination::publication(&config).await;
    for (settings, status, error) in [
        (
            json!({"attributes": {"price": "int"}}),
            409,
            "attribute_type_change_refused",
        ),
        (
            json!({"attributes": {"price": {"array": "float"}}}),
            409,
            "attribute_type_change_refused",
        ),
        (
            json!({"attributes": {"price": null}}),
            409,
            "attribute_removal_refused",
        ),
        (json!({"dense_rq_bits": 3}), 409, "namespace_settings_fixed"),
        (
            json!({"attribute_indexes": {"nope": {"index": false}}}),
            400,
            "settings_invalid",
        ),
        (
            json!({"attribute_indexes": {"stock": {"ngram": true}}}),
            400,
            "settings_invalid",
        ),
        (
            json!({"attribute_indexes": {"stock": {"fulltext": true}}}),
            400,
            "settings_invalid",
        ),
        (
            json!({"attributes": {"id": "int"}}),
            400,
            "settings_invalid",
        ),
    ] {
        assert_eq!(
            post(json!({"settings": settings.clone()})).await,
            (status, json!({"error": error})),
            "{settings}"
        );
    }
    let (after, _) = super::coordination::publication(&config).await;
    assert_eq!(after.content, head.content);
    assert_eq!(after.policy, head.policy);

    // An index switched off is dropped with the publication, and a filter on its
    // attribute is a scan again.
    assert_eq!(
        post(json!({"settings": {"attribute_indexes": {"stock": {"index": false}}}})).await,
        (200, json!({"upserted": 0, "deleted": 0}))
    );
    assert_eq!(
        names().await,
        set(&["attr_title_idx", "attrfts_title_idx", "attrngram_note_idx"])
    );
    assert_eq!(
        listed(json!({"field": "stock", "op": "gt", "value": 2})),
        ["c"]
    );
}

/// A change of the analyzer of a full-text index is a schema change: it is
/// published at once, and the index it replaces keeps serving with its old
/// analyzer until maintenance has built the new one and published it. Nothing
/// is built again once the index is current.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_rebuilds_a_full_text_index_for_a_changed_analyzer_on_storage() {
    use lance::index::DatasetIndexExt;
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    // The maintenance of this test is the one it runs (`configure` builds on 2 threads).
    let text = std::fs::read_to_string(&config).unwrap().replacen(
        "[namespace]\n",
        "[namespace]\nanalyzer = { ascii_folding = true }\n",
        1,
    );
    std::fs::write(
        &config,
        format!("{text}\n[writer.serve]\nmaintenance = false\n"),
    )
    .unwrap();
    embed(&root, &["alpha", "bravo"]);
    let writer = Writer::start(&config, &root, address).await;
    let post = async |body: Value| writer.post(WRITE_KEY, "dense-fixture", &body).await;
    // The sequence of the served publication, the segments of the index and the
    // options its manifest records, and the parameters the index was built with.
    let served = async || {
        let (head, manifest) = super::coordination::publication(&config).await;
        let mut segments: Vec<_> = manifest
            .indexes
            .iter()
            .filter(|index| index.name == "attrfts_note_idx")
            .map(|index| index.uuid.clone())
            .collect();
        segments.sort();
        let loaded = fyrr_core::config::load(&config).unwrap();
        let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
            .await
            .unwrap();
        let params = dataset
            .index_statistics("attrfts_note_idx")
            .await
            .unwrap()
            .replace(' ', "");
        (
            head.content.as_ref().unwrap().sequence,
            segments,
            serde_json::to_value(&manifest.settings.namespace.attribute_indexes).unwrap(),
            params,
        )
    };

    assert_eq!(
        post(json!({
            "settings": {
                "attributes": {"note": "string"},
                "attribute_indexes": {"note": {"index": false, "full_text": true}},
            },
            "upserts": [
                {"id": "a", "text": "alpha", "readers": ["grp:allow"], "note": "Caf\u{e9}"},
                {"id": "b", "text": "bravo", "readers": ["grp:allow"], "note": "na\u{ef}ve"},
            ],
        }))
        .await,
        (200, json!({"upserted": 2, "deleted": 0}))
    );
    let (sequence, old, recorded, params) = served().await;
    assert_eq!(old.len(), 1);
    assert_eq!(
        recorded,
        json!({"note": {"index": false, "full_text": true, "ngram": false}})
    );
    assert!(params.contains("\"base_tokenizer\":\"simple\""), "{params}");
    // An index without an analyzer of its own takes the namespace's settings.
    assert!(params.contains("\"ascii_folding\":true"), "{params}");

    // The change is one publication that leaves the index as it was built.
    let change = json!({"settings": {"attribute_indexes": {"note": {
        "analyzer": {"tokenizer": "unicode"}}}}});
    assert_eq!(
        post(change.clone()).await,
        (200, json!({"upserted": 0, "deleted": 0}))
    );
    let (changed, segments, recorded, params) = served().await;
    assert_eq!(changed, sequence + 1);
    assert_eq!(segments, old);
    let changed_id = super::coordination::publication(&config)
        .await
        .1
        .publication_id;
    assert_eq!(
        recorded,
        json!({"note": {"index": false, "full_text": true, "ngram": false}})
    );
    assert!(params.contains("\"base_tokenizer\":\"simple\""), "{params}");
    assert!(params.contains("\"ascii_folding\":true"), "{params}");
    // The reader serves that publication, and what is stored verifies.
    assert_eq!(
        listing(
            &config,
            json!({"field": "note", "op": "eq", "value": "Caf\u{e9}"})
        ),
        ["a"]
    );
    assert_eq!(success(&config, &["writer", "verify"])["verified"], true);
    // The same request again changes nothing.
    assert_eq!(post(change).await.0, 200);
    assert_eq!(served().await.0, changed);

    // Maintenance builds the index again and publishes it with its analyzer.
    run_when_free(&config, &["writer", "maintain"]).await;
    let (rebuilt, segments, recorded, params) = served().await;
    assert!(rebuilt > changed);
    assert_eq!(segments.len(), 1);
    assert_ne!(segments, old);
    assert_eq!(
        recorded,
        json!({"note": {"index": false, "full_text": true, "ngram": false, "analyzer": {
            "tokenizer": "unicode", "ascii_folding": false, "max_token_length": 40,
            "ngram_min_length": 3, "ngram_max_length": 3, "prefix_only": false}}})
    );
    assert!(params.contains("\"base_tokenizer\":\"icu\""), "{params}");
    assert!(params.contains("\"ascii_folding\":false"), "{params}");
    assert_eq!(
        listing(
            &config,
            json!({"field": "note", "op": "eq", "value": "na\u{ef}ve"})
        ),
        ["b"]
    );
    assert_eq!(success(&config, &["writer", "verify"])["verified"], true);

    // A current index is not built again.
    run_when_free(&config, &["writer", "maintain"]).await;
    assert_eq!(served().await.1, segments);

    // A restore of the publication before the rebuild brings the old index back
    // and records the analyzer it was built with, so maintenance builds the new
    // one again.
    run_when_free(&config, &["writer", "restore", &changed_id]).await;
    let (restored, segments, recorded, params) = served().await;
    assert!(restored > rebuilt);
    assert_eq!(
        recorded,
        json!({"note": {"index": false, "full_text": true, "ngram": false}})
    );
    assert!(params.contains("\"base_tokenizer\":\"simple\""), "{params}");
    run_when_free(&config, &["writer", "maintain"]).await;
    let (_, again, recorded, params) = served().await;
    assert_ne!(again, segments);
    assert_eq!(recorded["note"]["analyzer"]["tokenizer"], "unicode");
    assert!(params.contains("\"base_tokenizer\":\"icu\""), "{params}");
}

/// The ids a listing of the namespace returns under `filter`, in order.
fn listing(config: &Path, filter: Value) -> Vec<String> {
    let mut client = Mcp::open(config);
    body(client.call(
        "search",
        json!({"output": "metadata", "filter": filter,
               "order_by": [{"field": "root_id", "direction": "asc"}]}),
    ))["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
        .collect()
}

/// A column added to a table that already has columns takes the next field id,
/// so a table's columns are not in name order: values written after the schema
/// grew still land in the attribute they were written for, whether they are
/// upserted or patched.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_keeps_the_columns_of_added_attributes_in_table_order_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["alpha", "bravo", "charlie"]);
    let writer = Writer::start(&config, &root, address).await;
    let post = async |body: Value| writer.post(WRITE_KEY, "dense-fixture", &body).await;
    let row = |id: &str, text: &str, more: Value| {
        let mut row = json!({"id": id, "text": text, "readers": ["grp:allow"]});
        row.as_object_mut()
            .unwrap()
            .extend(more.as_object().unwrap().clone());
        row
    };
    // Created with its schema alone: until rows are published the schema stays,
    // as the table is created from the settings its rows were prepared with.
    assert_eq!(
        post(json!({"settings": {"attributes": {"b": "float", "z": "int"}}}))
            .await
            .0,
        200
    );
    assert_eq!(
        post(json!({"settings": {"attributes": {"a": "string"}}})).await,
        (409, json!({"error": "namespace_not_published"}))
    );
    assert_eq!(
        post(json!({"upserts": [row("r1", "alpha", json!({"z": 7}))]}))
            .await
            .0,
        200
    );
    // Added after `z`, and sorted before both.
    assert_eq!(
        post(json!({"settings": {"attributes": {"a": "string"}}}))
            .await
            .0,
        200
    );
    let (head, _) = super::coordination::publication(&config).await;
    // A schema change carries nothing else, and a refused one changes nothing.
    assert_eq!(
        post(json!({"settings": {"attributes": {"c": "int"}},
                    "upserts": [row("r9", "alpha", json!({}))]}))
        .await,
        (400, json!({"error": "schema_change_with_rows"}))
    );
    let (after, manifest) = super::coordination::publication(&config).await;
    assert_eq!(after.content, head.content);
    assert_eq!(manifest.settings.namespace.attributes.len(), 3);
    assert_eq!(
        post(json!({"upserts": [
            row("r2", "bravo", json!({"z": 1, "a": "x", "b": 2.5})),
            row("r3", "charlie", json!({"z": 2, "a": "y", "b": 0.5})),
        ]}))
        .await,
        (200, json!({"upserted": 2, "deleted": 0}))
    );
    let eq = |field: &str, value: Value| json!({"field": field, "op": "eq", "value": value});
    assert_eq!(listing(&config, eq("z", json!(7))), ["r1"]);
    assert_eq!(listing(&config, eq("z", json!(1))), ["r2"]);
    assert_eq!(listing(&config, eq("a", json!("y"))), ["r3"]);
    assert_eq!(
        listing(&config, json!({"field": "b", "op": "gt", "value": 1.0})),
        ["r2"]
    );
    assert_eq!(listing(&config, eq("a", Value::Null)), ["r1"]);
    assert_eq!(
        post(json!({"patches": [{"id": "r2", "a": "w", "z": 3}]}))
            .await
            .0,
        200
    );
    assert_eq!(listing(&config, eq("a", json!("w"))), ["r2"]);
    assert_eq!(listing(&config, eq("z", json!(3))), ["r2"]);
    assert_eq!(listing(&config, eq("b", json!(2.5))), ["r2"]);
    assert_eq!(success(&config, &["writer", "verify"])["verified"], true);
}

/// An attribute that the schema does not declare is inferred from its first
/// value and added to the schema of a namespace that serves, in upserts and in
/// patches; it reads null for the rows written before it. The first type
/// committed under a name stays, and a namespace without a publication keeps
/// its schema.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_infers_undeclared_attributes_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["alpha", "bravo", "charlie", "delta", "echo"]);
    let writer = Writer::start(&config, &root, address).await;
    let post = async |body: Value| writer.post(WRITE_KEY, "dense-fixture", &body).await;
    let row = |id: &str, text: &str, more: Value| {
        let mut row = json!({"id": id, "text": text, "readers": ["grp:allow"]});
        row.as_object_mut()
            .unwrap()
            .extend(more.as_object().unwrap().clone());
        row
    };
    let eq = |field: &str, value: Value| json!({"field": field, "op": "eq", "value": value});
    let schema = async || {
        let (_, manifest) = super::coordination::publication(&config).await;
        serde_json::to_value(&manifest.settings.namespace.attributes).unwrap()
    };

    // No publication yet: the schema is what the settings say.
    assert_eq!(
        post(json!({"settings": {"attributes": {"price": "float"}}}))
            .await
            .0,
        200
    );
    assert_eq!(
        post(json!({"upserts": [row("r0", "alpha", json!({"colour": "red"}))]})).await,
        (400, json!({"error": "attribute_undeclared"}))
    );
    assert_eq!(
        post(json!({"upserts": [row("r1", "alpha", json!({"price": 1.5}))]})).await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    assert_eq!(schema().await, json!({"price": "float"}));

    // Published: the types come from the first values.
    assert_eq!(
        post(json!({"upserts": [row(
            "r2",
            "bravo",
            json!({"colour": "red", "count": 3, "ratio": 0.5, "flag": true, "tags": ["a", "b"]}),
        )]}))
        .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    assert_eq!(
        schema().await,
        json!({"colour": "string", "count": "int", "flag": "bool", "price": "float",
               "ratio": "float", "tags": {"array": "string"}})
    );
    // The new attributes filter as the declared ones do, and read null for the older row.
    assert_eq!(listing(&config, eq("colour", json!("red"))), ["r2"]);
    assert_eq!(
        listing(&config, json!({"field": "count", "op": "gt", "value": 2})),
        ["r2"]
    );
    assert_eq!(listing(&config, eq("flag", json!(true))), ["r2"]);
    assert_eq!(listing(&config, eq("colour", Value::Null)), ["r1"]);

    // A patch infers too, and the attribute is the same one for every document.
    assert_eq!(
        post(json!({"patches": [{"id": "r1", "grade": 4}]})).await,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 1, "skipped": 0})
        )
    );
    assert_eq!(listing(&config, eq("grade", json!(4))), ["r1"]);
    // A write by filter infers nothing: the committer may refuse it after the
    // schema would be published.
    assert_eq!(
        writer
            .post(
                ADMIN_KEY,
                "dense-fixture",
                &json!({"patch_where": {"filter": eq("colour", json!("red")), "set": {"size": "l"}}})
            )
            .await,
        (400, json!({"error": "attribute_undeclared"}))
    );
    assert!(schema().await.get("size").is_none());

    // A name that is declared now keeps its type, and a value that has none to
    // infer from names an attribute that is still not declared.
    for (more, code) in [
        (json!({"colour": 5}), "attribute_type_mismatch"),
        (json!({"count": "many"}), "attribute_type_mismatch"),
        (json!({"unknown": null}), "attribute_undeclared"),
        (json!({"unknown": []}), "attribute_undeclared"),
        (json!({"_hidden": 1}), "attribute_name_reserved"),
        (json!({"Colour": 1}), "attribute_name_invalid"),
        (
            json!({"big": "x".repeat(4097)}),
            "attribute_value_too_large",
        ),
    ] {
        assert_eq!(
            post(json!({"upserts": [row("r9", "charlie", more)]})).await,
            (400, json!({"error": code}))
        );
    }
    // Two rows that infer different types for one new name: the request is
    // refused whole and the name is not declared.
    assert_eq!(
        post(json!({"upserts": [
            row("r3", "charlie", json!({"shape": "square"})),
            row("r4", "delta", json!({"shape": 4})),
        ]}))
        .await,
        (400, json!({"error": "attribute_type_mismatch"}))
    );
    assert!(schema().await.get("shape").is_none());
    // A request that the envelope would refuse declares nothing either: the
    // empty id of a patch is checked before the schema is changed.
    assert_eq!(
        post(json!({"patches": [{"id": "", "hue": "x"}]})).await,
        (400, json!({"error": "document_invalid"}))
    );
    assert!(schema().await.get("hue").is_none());

    // Two requests at once that infer different types: the first committed
    // wins, the other is refused, and no row has the other type.
    let (one, two) = tokio::join!(
        post(json!({"upserts": [row("r5", "delta", json!({"weight": 7}))]})),
        post(json!({"upserts": [row("r6", "echo", json!({"weight": "heavy"}))]})),
    );
    let mut answers = [one, two];
    answers.sort_by_key(|(status, _)| *status);
    assert_eq!(answers[0], (200, json!({"upserted": 1, "deleted": 0})));
    assert_eq!(
        answers[1],
        (400, json!({"error": "attribute_type_mismatch"}))
    );
    let (winner, value) = match schema().await["weight"].as_str() {
        Some("int") => ("r5", json!(7)),
        Some("string") => ("r6", json!("heavy")),
        other => panic!("weight is {other:?}"),
    };
    assert_eq!(listing(&config, eq("weight", value)), [winner]);
    assert_eq!(
        listing(&config, json!({"not": eq("weight", Value::Null)})),
        [winner]
    );
    assert_eq!(success(&config, &["writer", "verify"])["verified"], true);
}

/// The inferred attributes count toward the bound on the schema: one more than
/// the bound is refused and the schema stays as it was.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_bounds_the_inferred_attributes_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    // Indexes of the attributes are not built here: that is maintenance's work.
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!("{text}\n[writer.serve]\nmaintenance = false\n"),
    )
    .unwrap();
    embed(&root, &["alpha", "bravo", "charlie"]);
    let writer = Writer::start(&config, &root, address).await;
    let post = async |body: Value| writer.post(WRITE_KEY, "dense-fixture", &body).await;
    let row = |id: &str, text: &str, more: Value| {
        let mut row = json!({"id": id, "text": text, "readers": ["grp:allow"]});
        row.as_object_mut()
            .unwrap()
            .extend(more.as_object().unwrap().clone());
        row
    };
    let declared = async || {
        let (_, manifest) = super::coordination::publication(&config).await;
        manifest.settings.namespace.attributes.len()
    };
    assert_eq!(
        post(json!({"settings": {"attributes": {"a000": "int"}},
                    "upserts": [row("r1", "alpha", json!({"a000": 1}))]}))
        .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    // 126 more declared, leaving room for one inferred attribute.
    let more: serde_json::Map<String, Value> = (1..127)
        .map(|n| (format!("a{n:03}"), json!("int")))
        .collect();
    assert_eq!(post(json!({"settings": {"attributes": more}})).await.0, 200);
    assert_eq!(declared().await, 127);
    assert_eq!(
        post(json!({"upserts": [row("r2", "bravo", json!({"last": 1}))]})).await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    assert_eq!(declared().await, fyrr_core::limits::schema::MAX_ATTRIBUTES);
    assert_eq!(
        post(json!({"upserts": [row("r3", "charlie", json!({"over": 1}))]})).await,
        (400, json!({"error": "attribute_limit_exceeded"}))
    );
    assert_eq!(declared().await, fyrr_core::limits::schema::MAX_ATTRIBUTES);
    // A row that names only attributes it has is not refused.
    assert_eq!(
        post(json!({"upserts": [row("r3", "charlie", json!({"last": 2}))]})).await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
}

/// A restore to a version from before an attribute was added gives the table the
/// column the policy declares, null for its rows, and publishes a table the
/// reader opens.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_restores_a_version_from_before_an_attribute_was_added_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["alpha"]);
    let writer = Writer::start(&config, &root, address).await;
    let post = async |body: Value| writer.post(WRITE_KEY, "dense-fixture", &body).await;
    assert_eq!(
        post(json!({"settings": {"attributes": {"price": "float"}},
                    "upserts": [{"id": "a", "text": "alpha", "readers": ["grp:allow"], "price": 2.5}]}))
        .await
        .0,
        200
    );
    assert_eq!(
        post(json!({"settings": {"attributes": {"stock": "int"}}}))
            .await
            .0,
        200
    );
    let (_, added) = super::coordination::publication(&config).await;
    assert!(added.settings.namespace.attributes.contains_key("stock"));
    let restored = run_when_free(&config, &["writer", "rollback"]).await;
    assert_eq!(restored["restored"], true);
    // The publication has the column the namespace declares, and a reader opens it.
    let (_, manifest) = super::coordination::publication(&config).await;
    assert_eq!(
        manifest.settings.namespace.attributes,
        added.settings.namespace.attributes
    );
    let eq = |field: &str, value: Value| json!({"field": field, "op": "eq", "value": value});
    assert_eq!(listing(&config, eq("stock", Value::Null)), ["a"]);
    assert_eq!(listing(&config, eq("price", json!(2.5))), ["a"]);
    assert_eq!(success(&config, &["writer", "verify"])["verified"], true);
    // The restored namespace takes rows with the attribute.
    assert_eq!(
        post(
            json!({"upserts": [{"id": "b", "text": "alpha", "readers": ["grp:allow"], "stock": 4}]})
        )
        .await
        .0,
        200
    );
    assert_eq!(
        listing(&config, json!({"field": "stock", "op": "gt", "value": 1})),
        ["b"]
    );
}
