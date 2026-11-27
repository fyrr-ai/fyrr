//! Queries of documents written through the write API: dense, full-text and
//! hybrid ranking, sub-queries, boosts, caps, reranking, excerpts and named
//! and multi-vector attributes.
use super::tools::{Mcp, body, error};
use super::write_api::{READ_KEY, WRITE_KEY, Writer, configure, embed, secret, vector};
use super::*;
use std::time::Duration;

/// A reader that routes by RaBitQ, so that its dense lane takes the
/// partition-map path, with `settings` added to its `[reader]` table. It
/// answers once it has mirrored the namespace.
async fn rabitq_reader(
    config: &Path,
    root: &Path,
    n: usize,
    settings: &str,
) -> super::http_mcp::Reader {
    let text = std::fs::read_to_string(config).unwrap();
    assert!(text.contains("nprobes = 1\n") && text.contains("[embedding]\n"));
    let variant = root.join(format!("rabitq-{n}.toml"));
    std::fs::write(
        &variant,
        text.replace("[embedding]\n", "[embedding]\ndimensions = 16\n")
            .replace(
                "nprobes = 1\n",
                &format!("routing = 'rabitq'\n{settings}\n"),
            ),
    )
    .unwrap();
    let reader = super::http_mcp::Reader::start_appended(&variant, root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    for _ in 0..600 {
        let ready: Value = reader
            .client
            .get(format!("{}/readyz", reader.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if ready["warm_ready"] == true {
            return reader;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the reader never warmed: {}", reader.stderr_text());
}

/// The document keys and the telemetry of the answer to `vector`.
async fn indexed_answer(
    reader: &super::http_mcp::Reader,
    vector: &[f32],
    limit: usize,
) -> (Vec<String>, Value) {
    let answer = ask(
        reader,
        json!({"vector": vector, "limit": limit, "output": "metadata"}),
    )
    .await;
    let telemetry = answer.1["metadata"]["telemetry"].clone();
    (roots(&answer), telemetry)
}

/// Rows at one distance rank by document key, whichever rows the subject may
/// read and however the reader takes the vectors of the candidates: the
/// deferred key read keeps every row of a tie at the cutoff.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_orders_equal_dense_scores_by_document_key_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    // The dense lane answers with exactly the 10 rows asked for, so a cut
    // before the key order would keep an arbitrary 10 of the tie.
    configure(
        &config,
        &address,
        "[namespace.retrieval]\ndense_candidates = 10",
    );
    let writer = Writer::start(&config, &root, address).await;
    // 3,000 rows written in reverse key order; the reader may read every third
    // of them, more than the first rescore pool. The first 60 share the query's
    // vector, so 20 readable rows tie at the cutoff of the top 10; the others
    // lie farther away.
    let rows: Vec<Value> = (0..3000)
        .rev()
        .map(|n| {
            let reader = if n % 3 == 0 { "usr:x" } else { "usr:other" };
            let vector = if n < 60 { vec![0.5; 16] } else { vector(n, 16) };
            json!({"id": format!("r{n:04}"), "text": format!("row {n}"), "vector": vector, "readers": [reader]})
        })
        .collect();
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": {"embedding": {"dimensions": 16}}, "upserts": rows}),
            )
            .await,
        (200, json!({"upserted": 3000, "deleted": 0}))
    );
    let expected: Vec<String> = (0..10).map(|n| format!("r{:04}", n * 3)).collect();
    for (n, sparse_read) in ["masked_scan", "take_rows"].into_iter().enumerate() {
        let reader = rabitq_reader(
            &config,
            &root,
            n,
            &format!("nprobes = 65536\nsparse_read = '{sparse_read}'"),
        )
        .await;
        let (found, telemetry) = indexed_answer(&reader, &[0.5; 16], 10).await;
        assert_eq!(found, expected, "{sparse_read}: {telemetry}");
        // The refinement ranked the first pool, 40 of the 1,000 rows the reader
        // may read, and the tie of 20 fit in it.
        assert_eq!(telemetry["dense_candidates_fetched"], 40, "{telemetry}");
        assert!(
            telemetry.get("dense_tie_bound_hits").is_none(),
            "{telemetry}"
        );
    }
}

/// The rescore pool grows with the square root of the rows the probes
/// inspect: every row when every partition is probed, a share of them when
/// few are.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_sizes_the_rescore_pool_by_the_rows_the_probes_inspect_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(
        &config,
        &address,
        "[namespace.retrieval]\ndense_candidates = 2",
    );
    let writer = Writer::start(&config, &root, address).await;
    let rows: Vec<Value> = (0..4000)
        .map(|n| json!({"id": format!("d{n}"), "text": format!("row {n}"), "vector": vector(n, 16), "readers": ["usr:x"]}))
        .collect();
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": {"embedding": {"dimensions": 16}}, "upserts": rows}),
            )
            .await,
        (200, json!({"upserted": 4000, "deleted": 0}))
    );
    // At two candidates the pool starts at 8 and ends at 32: the square root
    // of 4000 rows is 63, that of one partition's rows is below 32.
    let mut fetched = Vec::new();
    for (n, nprobes) in [65536, 1].into_iter().enumerate() {
        let reader = rabitq_reader(&config, &root, n, &format!("nprobes = {nprobes}")).await;
        let (found, telemetry) = indexed_answer(&reader, &vector(7, 16), 2).await;
        assert_eq!(found.len(), 2, "{telemetry}");
        fetched.push(telemetry["dense_candidates_fetched"].as_u64().unwrap());
    }
    assert_eq!(fetched[0], 32, "every partition probed");
    assert!(
        (8..32).contains(&fetched[1]),
        "one partition probed: {fetched:?}"
    );
}

/// A JSON query with `fields.highlights` reports where the tokens that match
/// the query lie in an excerpt, as the namespace's analyzer cuts and reduces
/// them, in codepoints of the record; without it the answer is what it was.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_highlights_the_tokens_an_excerpt_matches_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let text = std::fs::read_to_string(&config).unwrap().replacen(
        "[namespace]\n",
        "[namespace]\nanalyzer = { language = \"english\", ascii_folding = true }\n",
        1,
    );
    std::fs::write(&config, text).unwrap();
    let record = "\u{1f600} Alphas sold at the caf\u{e9}, then ALPHA again";
    embed(&root, &[record, "alpha"]);
    let writer = Writer::start(&config, &root, address).await;
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"upserts": [{"id": "a", "text": record, "vector": vector(0, 1024),
                                     "readers": ["usr:x"]}]})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    let mut reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let post = async |body: Value| -> (u16, Value) {
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
    let ask = async |query: &str, fields: Value| {
        let mut body = json!({"query": query, "vector": vector(0, 1024)});
        if !fields.is_null() {
            body["fields"] = fields;
        }
        post(body).await
    };
    let excerpt = |answer: (u16, Value)| {
        assert_eq!(answer.0, 200, "{}", answer.1);
        answer.1["hits"][0]["excerpt"].clone()
    };

    // The stemmed `Alphas` and the upper-case `ALPHA` match `alpha`, and the
    // offsets count the emoji before them as one codepoint.
    let marked = excerpt(ask("alpha", json!({"highlights": true})).await);
    assert_eq!(marked["highlights"], json!([[2, 8], [32, 37]]));
    let chars: Vec<char> = record.chars().collect();
    let words: Vec<String> = marked["highlights"]
        .as_array()
        .unwrap()
        .iter()
        .map(|range| {
            chars[range[0].as_u64().unwrap() as usize..range[1].as_u64().unwrap() as usize]
                .iter()
                .collect()
        })
        .collect();
    assert_eq!(words, ["Alphas", "ALPHA"]);
    // The analyzer folds the accent of `caf\u{e9}` and cuts it from the comma.
    assert_eq!(
        excerpt(ask("cafe", json!({"highlights": true})).await)["highlights"],
        json!([[21, 25]])
    );
    assert_eq!(
        excerpt(ask("zulu", json!({"highlights": true})).await)["highlights"],
        json!([])
    );
    // Without the option the excerpt has no `highlights` and is otherwise the same.
    let plain = excerpt(ask("alpha", Value::Null).await);
    assert!(plain.get("highlights").is_none(), "{plain}");
    let mut unmarked = marked.clone();
    unmarked.as_object_mut().unwrap().remove("highlights");
    assert_eq!(plain, unmarked);
    assert_eq!(
        excerpt(ask("alpha", json!({"highlights": false})).await),
        plain
    );
    // A hit without an excerpt has no text to mark and is no error.
    let metadata = post(json!({"query": "alpha", "vector": vector(0, 1024),
                               "output": "metadata", "fields": {"highlights": true}}))
    .await;
    assert_eq!(metadata.0, 200, "{}", metadata.1);
    assert_eq!(metadata.1["hits"][0]["excerpt"], Value::Null);
    assert_eq!(
        ask("alpha", json!({"highlights": "yes"})).await,
        (400, json!({"error": "invalid_argument"}))
    );
    reader.stop().await;
}

/// The `analyzer` of `[namespace]` cuts and reduces the text of `searchable_text`
/// as an attribute's analyzer does: with German and the `unicode` tokenizer a
/// stemmed query matches in the text lane, and a highlight marks the stemmed
/// match.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_text_follows_the_namespace_analyzer_on_storage() {
    use lance::index::DatasetIndexExt;
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!("{text}\n[namespace.analyzer]\ntokenizer = \"unicode\"\nlanguage = \"german\"\n"),
    )
    .unwrap();
    let record = "Die Katzen schlafen";
    embed(&root, &[record, "andere", "Katze"]);
    let writer = Writer::start(&config, &root, address).await;
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"upserts": [
                    {"id": "a", "text": record, "vector": vector(0, 1024),
                     "readers": ["usr:x"]},
                    {"id": "b", "text": "andere", "vector": vector(1, 1024),
                     "readers": ["usr:x"]},
                ]})
            )
            .await,
        (200, json!({"upserted": 2, "deleted": 0}))
    );
    // The index of the text is built with the analyzer.
    let (head, _) = super::coordination::publication(&config).await;
    let loaded = fyrr_core::config::load(&config).unwrap();
    let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    let params = dataset
        .index_statistics("fts_idx")
        .await
        .unwrap()
        .replace(' ', "");
    assert!(params.contains("\"base_tokenizer\":\"icu\""), "{params}");
    assert!(params.contains("\"language\":\"German\""), "{params}");

    let mut reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let post = async |body: Value| -> Value {
        let response = reader
            .client
            .post(format!("{}/v1/namespaces/dense-fixture/query", reader.base))
            .bearer_auth(secret(READ_KEY))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        response.json::<Value>().await.unwrap()
    };
    // The roots the text lane found, and the highlights of the first hit.
    let ask = async |query: &str, vector_seed: usize| {
        post(json!({"query": query, "vector": vector(vector_seed, 1024),
                    "output": "metadata", "fields": {"scores": true}}))
        .await["hits"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|hit| hit.get("lexical_score").is_some_and(|s| !s.is_null()))
            .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    // `Katze` and `Katzen` have one stem in German.
    assert_eq!(ask("Katze", 1).await, ["a"]);
    let marked = post(json!({"query": "Katze", "vector": vector(0, 1024),
                             "fields": {"highlights": true}}))
    .await;
    assert_eq!(marked["hits"][0]["excerpt"]["highlights"], json!([[4, 10]]));
    reader.stop().await;
}

/// A search takes its query vector from the request instead of the embedding
/// provider, scans exactly when asked, fetches as many candidates as its limit
/// asks for, and stays inside the filter and the caller's permissions.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_searches_by_a_query_vector_exact_or_approximate_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    // Two dense candidates: only the request limit can fetch more.
    configure(
        &config,
        &address,
        "[namespace.retrieval]\ndense_candidates = 2",
    );
    let writer = Writer::start(&config, &root, address).await;
    let texts = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
    let mut upserts: Vec<Value> = texts
        .iter()
        .enumerate()
        .map(|(n, text)| {
            let id = ["a", "b", "c", "d", "e", "f"][n];
            json!({"id": id, "text": text, "vector": vector(n, 1024), "readers": ["grp:allow"]})
        })
        .collect();
    // As close to the query as a row can be, but nobody may read it.
    upserts.push(json!({"id": "hidden", "text": "golf", "vector": vector(1, 1024), "readers": ["usr:nobody"]}));
    assert_eq!(
        writer
            .post(WRITE_KEY, "dense-fixture", &json!({"upserts": upserts}))
            .await,
        (200, json!({"upserted": 7, "deleted": 0}))
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
    let query = vector(1, 1024);
    let calls = root.embedding.calls();

    // A vector without text searches the dense lane alone, and the limit, not
    // the two configured candidates, sets how deep it reads.
    let approximate = listed(json!({"vector": query, "limit": 5}));
    assert_eq!(approximate.len(), 5);
    assert_eq!(approximate[0], "b");
    assert!(!approximate.contains(&"hidden".to_owned()));
    // An exact search finds the same rows in the same order.
    assert_eq!(
        listed(json!({"vector": query, "limit": 5, "exact": true})),
        approximate
    );
    // With text, the vector stands in for the embedding of the text.
    let hybrid = listed(json!({"query": "charlie", "vector": query, "limit": 3}));
    let mut top = hybrid[..2].to_vec();
    top.sort();
    assert_eq!(top, ["b", "c"]);
    // The filter and the caller's permissions still bound an exact scan.
    let mut filtered = listed(json!({
        "vector": query,
        "exact": true,
        "filter": {"field": "root_id", "op": "in", "value": ["a", "c", "hidden"]},
    }));
    filtered.sort();
    assert_eq!(filtered, ["a", "c"]);
    assert_eq!(root.embedding.calls(), calls, "no embedding call");

    // An exact search whose text cannot be embedded fails instead of
    // answering from the lexical lane alone, as the same search without exact does.
    root.embedding.reject("alpha");
    assert_eq!(
        listed(json!({"query": "alpha", "limit": 3}))[0],
        "a",
        "the lexical lane answers"
    );
    error(
        client.call("search", json!({"query": "alpha", "exact": true})),
        "storage_unavailable",
    );

    let mut short = query.clone();
    short.pop();
    for arguments in [
        json!({"vector": short}),
        json!({"vector": vec![0.0; 1024]}),
        // Zero once stored as 16-bit floats.
        json!({"vector": vec![1e-10; 1024]}),
        json!({"vector": [], "query": "alpha"}),
        json!({"vector": query, "queries": ["alpha", "bravo"]}),
        json!({"exact": true, "order": "updated_at"}),
    ] {
        error(client.call("search", arguments), "invalid_argument");
    }
}

/// `POST /v1/namespaces/dense-fixture/query` as the read key.
async fn ask(reader: &super::http_mcp::Reader, body: Value) -> (u16, Value) {
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
}

fn roots(answer: &(u16, Value)) -> Vec<String> {
    assert_eq!(answer.0, 200, "{}", answer.1);
    answer.1["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_fuses_the_sub_queries_of_a_query_as_it_asks_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    // At most three documents in an answer to `queries`, whatever the limit.
    configure(
        &config,
        &address,
        "[namespace.retrieval]\nmax_documents = 3",
    );
    let texts = ["alpha", "bravo", "charlie", "delta"];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;
    let upserts: Vec<Value> = texts
        .iter()
        .map(|text| json!({"id": &text[..1], "text": text, "readers": ["usr:x"]}))
        .collect();
    assert_eq!(
        writer
            .post(WRITE_KEY, "dense-fixture", &json!({"upserts": upserts}))
            .await
            .0,
        200
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let search = |fusion: Value| {
        let mut body = json!({"queries": ["alpha", "bravo"], "limit": 4, "output": "metadata"});
        if !fusion.is_null() {
            body["fusion"] = fusion;
        }
        body
    };

    // Without `fusion` each sub-query keeps its own hits, merged round-robin:
    // `limit` counts the hits of each sub-query, `max_documents` the documents.
    let merged = ask(&reader, search(Value::Null)).await;
    assert_eq!(roots(&merged).len(), 3);
    assert_eq!(merged.1["hits"][0]["query_index"], 0);
    assert_eq!(merged.1["hits"][1]["query_index"], 1);

    // A member that is `null` is the default, as the schema says: the same answer.
    let mut nulls = search(Value::Null);
    for member in [
        "consistency",
        "fields",
        "fusion",
        "rerank",
        "sub_queries",
        "query_match",
        "query_attributes",
        "phrase",
        "contains",
        "boosts",
        "group_documents",
        "caps",
    ] {
        nulls[member] = Value::Null;
    }
    let defaulted = ask(&reader, nulls).await;
    assert_eq!(defaulted.0, 200, "{}", defaulted.1);
    assert_eq!(defaulted.1["hits"], merged.1["hits"]);

    // With it, one ranking of at most `max_documents` documents, though the
    // limit is 4. The first hit is the
    // one the heavier sub-query ranks first, and it scores its weight over the
    // rank constant plus one, plus what the other sub-query gives it.
    for (weights, first, query_index) in [([3.0, 1.0], "a", 0), ([1.0, 3.0], "b", 1)] {
        let fused = ask(
            &reader,
            search(json!({"method": "rrf", "weights": weights})),
        )
        .await;
        assert_eq!(roots(&fused).len(), 3, "{}", fused.1);
        let hit = &fused.1["hits"][0];
        assert_eq!(hit["root_id"], first);
        assert_eq!(hit["query_index"], query_index);
        let score = hit["score"].as_f64().unwrap();
        assert!(
            score > 3.0 / 61.0 && score < 3.0 / 61.0 + 1.0 / 61.0,
            "{score}"
        );
        let scores: Vec<f64> = fused.1["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["score"].as_f64().unwrap())
            .collect();
        assert!(
            scores.windows(2).all(|pair| pair[0] >= pair[1]),
            "{scores:?}"
        );
    }
    // A large rank constant flattens the parts of the ranks: the weights decide.
    let flat = ask(
        &reader,
        search(json!({"method": "rrf", "rank_constant": 1000000, "weights": [1.0, 2.0]})),
    )
    .await;
    assert_eq!(roots(&flat)[0], "b");

    // Fusion is for `queries`, one weight each, no negative ones, a positive
    // rank constant and the one method.
    for (body, fusion) in [
        (
            json!({"query": "alpha", "output": "metadata"}),
            json!({"method": "rrf"}),
        ),
        (
            search(Value::Null),
            json!({"method": "rrf", "weights": [1.0]}),
        ),
        (
            search(Value::Null),
            json!({"method": "rrf", "weights": [1.0, -1.0]}),
        ),
        (
            search(Value::Null),
            json!({"method": "rrf", "weights": [1.0, 1.0000001e6]}),
        ),
        (
            search(Value::Null),
            json!({"method": "rrf", "rank_constant": 0}),
        ),
        (search(Value::Null), json!({"method": "borda"})),
        (search(Value::Null), json!({"weights": [1.0, 1.0]})),
        (search(Value::Null), json!({"method": "rrf", "k": 60})),
    ] {
        let mut body = body;
        body["fusion"] = fusion.clone();
        assert_eq!(
            ask(&reader, body).await,
            (400, json!({"error": "invalid_argument"})),
            "{fusion}"
        );
    }
    // The namespace has no reranker: a request cannot switch one on, or name
    // what it scores, but it may switch it off.
    for rerank in [
        json!({}),
        json!({"query": "alpha"}),
        json!({"enabled": true}),
    ] {
        let mut body = search(Value::Null);
        body["rerank"] = rerank.clone();
        assert_eq!(
            ask(&reader, body).await,
            (400, json!({"error": "invalid_argument"})),
            "{rerank}"
        );
    }
    let mut off = search(Value::Null);
    off["rerank"] = json!({"enabled": false});
    assert_eq!(roots(&ask(&reader, off).await).len(), 3);
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_answers_sub_queries_with_their_own_filters_limits_and_ranking_on_storage()
 {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    // At most four documents in an answer that is not fused.
    configure(
        &config,
        &address,
        "[namespace.retrieval]\nmax_documents = 4",
    );
    let texts = ["alpha", "bravo", "charlie", "delta"];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;
    // The read key reads `usr:x`: it may not read delta.
    let tags = ["red", "blue", "red", "blue"];
    let upserts: Vec<Value> = texts
        .iter()
        .zip(tags)
        .enumerate()
        .map(|(n, (text, tag))| {
            let reader = if n == 3 { "usr:y" } else { "usr:x" };
            json!({"id": &text[..1], "text": text, "tag": tag, "readers": [reader]})
        })
        .collect();
    let settings = json!({"attributes": {"tag": "string"}});
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": settings, "upserts": upserts})
            )
            .await
            .0,
        200
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let tag = |value: &str| json!({"field": "tag", "op": "eq", "value": value});
    let sub = |query: &str, filter: Value, limit: u32| {
        let mut sub = json!({"query": query, "limit": limit});
        if !filter.is_null() {
            sub["filter"] = filter;
        }
        sub
    };
    let search = |subs: Vec<Value>| json!({"sub_queries": subs, "output": "metadata"});
    let indexes = |answer: &(u16, Value)| -> Vec<u64> {
        answer.1["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["query_index"].as_u64().unwrap())
            .collect()
    };

    // Each sub-query has its own filter and limit, and its hits come one after
    // the other in request order: two red documents, then the one blue
    // document the key may read, though delta is blue too.
    let answer = ask(
        &reader,
        search(vec![
            sub("alpha", tag("red"), 2),
            sub("bravo", tag("blue"), 1),
        ]),
    )
    .await;
    assert_eq!(roots(&answer), ["a", "c", "b"], "{}", answer.1);
    assert_eq!(indexes(&answer), [0, 0, 1]);
    // A document two sub-queries find is in both answers.
    let answer = ask(
        &reader,
        search(vec![
            sub("alpha", Value::Null, 1),
            sub("alpha", tag("red"), 1),
        ]),
    )
    .await;
    assert_eq!(roots(&answer), ["a", "a"], "{}", answer.1);
    assert_eq!(indexes(&answer), [0, 1]);
    // The limit cuts a sub-query's hits.
    let answer = ask(&reader, search(vec![sub("bravo", Value::Null, 2)])).await;
    assert_eq!(roots(&answer).len(), 2, "{}", answer.1);
    assert_eq!(roots(&answer)[0], "b");
    // A sub-query ranks by vector alone, with the vector the fixture gives bravo.
    let by_vector: Vec<f32> = (0..1024)
        .map(|d| (((1 + 2000) * (d + 7)) as f32).sin())
        .collect();
    let answer = ask(
        &reader,
        search(vec![
            sub("alpha", tag("red"), 1),
            json!({"vector": by_vector, "filter": tag("blue"), "limit": 1}),
        ]),
    )
    .await;
    assert_eq!(roots(&answer), ["a", "b"], "{}", answer.1);
    // The request's own filters bind every sub-query.
    let mut body = search(vec![
        sub("alpha", Value::Null, 2),
        sub("bravo", tag("blue"), 2),
    ]);
    body["filter"] = tag("red");
    let answer = ask(&reader, body).await;
    assert_eq!(roots(&answer), ["a", "c"], "{}", answer.1);
    assert_eq!(indexes(&answer), [0, 0]);

    // A filter that selects only what the key may not read gives no hits and
    // no error, next to a sub-query that gives some.
    let denied = json!({"field": "root_id", "op": "eq", "value": "d"});
    let answer = ask(
        &reader,
        search(vec![
            sub("delta", denied.clone(), 2),
            sub("delta", tag("blue"), 2),
        ]),
    )
    .await;
    assert_eq!(roots(&answer), ["b"], "{}", answer.1);
    assert_eq!(indexes(&answer), [1]);
    let answer = ask(&reader, search(vec![sub("delta", denied, 2)])).await;
    assert!(roots(&answer).is_empty());

    // With `fusion` one ranking of the documents of all sub-queries, each read
    // through its own filter: the first hit is the one the heavier sub-query
    // ranks first, and `limit` sizes the fused answer, not the sub-queries.
    for (weights, first, query_index) in [([3.0, 1.0], "a", 0), ([1.0, 3.0], "b", 1)] {
        let mut body = search(vec![
            sub("alpha", tag("red"), 4),
            sub("bravo", tag("blue"), 4),
        ]);
        body["fusion"] = json!({"method": "rrf", "weights": weights});
        let fused = ask(&reader, body.clone()).await;
        assert_eq!(roots(&fused).len(), 3, "{}", fused.1);
        assert_eq!(fused.1["hits"][0]["root_id"], first);
        assert_eq!(fused.1["hits"][0]["query_index"], query_index);
        let score = fused.1["hits"][0]["score"].as_f64().unwrap();
        assert!((score - 3.0 / 61.0).abs() < 1e-9, "{score}");
        body["limit"] = json!(2);
        assert_eq!(roots(&ask(&reader, body).await).len(), 2);
    }
    // Their limits may add up to more than `max_documents` once fused.
    let mut body = search(vec![
        sub("alpha", Value::Null, 3),
        sub("bravo", Value::Null, 3),
    ]);
    body["fusion"] = json!({"method": "rrf"});
    assert_eq!(roots(&ask(&reader, body).await).len(), 3);

    // Refused: no sub-query, more than 16, a sub-query without a ranking, a
    // limit out of range or missing, an unknown member or field, a limit sum
    // above `max_documents` without fusion, and what sub-queries replace or
    // do not take.
    let one = || sub("alpha", Value::Null, 1);
    let many: Vec<Value> = (0..17).map(|_| one()).collect();
    for body in [
        search(vec![]),
        search(many),
        search(vec![json!({"limit": 1})]),
        search(vec![json!({"query": "alpha", "limit": 0})]),
        search(vec![json!({"query": "alpha", "limit": 76})]),
        search(vec![json!({"query": "alpha"})]),
        search(vec![json!({"query": "alpha", "limit": 1, "exact": true})]),
        search(vec![json!({"query": " ", "limit": 1})]),
        search(vec![json!({"vector": [1.0], "limit": 1})]),
        search(vec![sub(
            "alpha",
            json!({"field": "nope", "op": "eq", "value": 1}),
            1,
        )]),
        search(vec![
            sub("alpha", Value::Null, 3),
            sub("bravo", Value::Null, 2),
        ]),
        {
            let mut body = search(vec![one()]);
            body["query"] = json!("alpha");
            body
        },
        {
            let mut body = search(vec![one()]);
            body["queries"] = json!(["alpha", "bravo"]);
            body
        },
        {
            let mut body = search(vec![one()]);
            body["exact"] = json!(true);
            body
        },
        {
            let mut body = search(vec![one()]);
            body["limit"] = json!(2);
            body
        },
        {
            let mut body = search(vec![one(), one()]);
            body["fusion"] = json!({"method": "rrf", "weights": [1.0]});
            body
        },
    ] {
        assert_eq!(
            ask(&reader, body.clone()).await,
            (400, json!({"error": "invalid_argument"})),
            "{body}"
        );
    }
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_narrows_a_query_by_the_tokens_of_a_full_text_attribute_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["alpha", "bravo", "alpha beta"]);
    let writer = Writer::start(&config, &root, address).await;
    // `d` meets every condition below but the caller may not read it.
    let rows = [
        ("a", "alpha", "red shoes", "red running shoes", "usr:x"),
        ("b", "alpha", "red hat", "blue hat", "usr:x"),
        ("c", "bravo", "blue shoes", "red shoes", "usr:x"),
        ("d", "alpha", "red shoes", "red shoes", "usr:other"),
        ("e", "alpha beta", "green hat", "green hat", "usr:x"),
    ];
    let upserts: Vec<Value> = rows
        .iter()
        .map(|(id, text, title, note, reader)| {
            json!({"id": id, "text": text, "title": title, "note": note, "code": title,
                   "readers": [reader]})
        })
        .collect();
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({
                    "settings": {
                        "attributes": {"title": "string", "note": "string", "code": "string",
                                       "price": "float"},
                        "attribute_indexes": {
                            "title": {"full_text": true},
                            "note": {"full_text": true},
                            "code": {"full_text": true, "analyzer": {
                                "tokenizer": "ngram", "ngram_min_length": 1,
                                "ngram_max_length": 8, "prefix_only": true}},
                        },
                    },
                    "upserts": upserts,
                })
            )
            .await
            .0,
        200
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let with = |mut body: Value, contains: Value| {
        body["output"] = json!("metadata");
        body["contains"] = contains;
        body
    };
    let found = async |body: Value, contains: Value| {
        let mut found = roots(&ask(&reader, with(body, contains)).await);
        found.sort();
        found
    };
    let refused = async |body: Value, contains: Value| {
        assert_eq!(
            ask(&reader, with(body, contains)).await,
            (400, json!({"error": "invalid_argument"})),
        );
    };
    let text = json!({"query": "alpha"});
    let by_vector = json!({"vector": vector(7, 1024)});

    // Both rankings return the rows the caller may read that meet the
    // conditions, and no others: not `b`, and not `d`, which is unreadable.
    for ranking in [&text, &by_vector] {
        let all = json!([{"attribute": "note", "all": "red shoes"}]);
        assert_eq!(found(ranking.clone(), all).await, ["a", "c"]);
        let any = json!([{"attribute": "note", "any": "hat shoes"}]);
        assert_eq!(found(ranking.clone(), any).await, ["a", "b", "c", "e"]);
        let none = json!([{"attribute": "note", "all": "hat shoes"}]);
        assert!(found(ranking.clone(), none).await.is_empty());
        // Conditions are joined by AND.
        let both = json!([
            {"attribute": "note", "any": "red"},
            {"attribute": "title", "all": "red shoes"},
        ]);
        assert_eq!(found(ranking.clone(), both).await, ["a"]);
    }
    // A condition also narrows what a filter selects.
    let filtered = json!({"query": "alpha",
                          "filter": {"field": "title", "op": "starts_with", "value": "red"}});
    assert_eq!(
        found(filtered, json!([{"attribute": "note", "any": "shoes"}])).await,
        ["a"]
    );

    // A text ranking can require all terms: only `e` holds both of them in its
    // lexical lane, which is the one `query_match` narrows.
    let lexical = async |query_match: Value| {
        let mut body = json!({"query": "alpha beta", "output": "metadata",
                              "fields": {"scores": true}});
        if !query_match.is_null() {
            body["query_match"] = query_match;
        }
        let answer = ask(&reader, body).await;
        let mut lexical: Vec<String> = answer.1["hits"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|hit| hit.get("lexical_score").is_some_and(|s| !s.is_null()))
            .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
            .collect();
        lexical.sort();
        lexical
    };
    assert_eq!(lexical(json!("all")).await, ["e"]);
    assert_eq!(lexical(json!("any")).await, ["a", "b", "e"]);
    assert_eq!(lexical(Value::Null).await, ["a", "b", "e"]);

    // An n-gram index holds the n-grams of a value as alternatives, so it does
    // not answer a condition on tokens; there is no `prefix` member. The
    // attribute needs a full-text index, and a condition has one text.
    for contains in [
        json!([{"attribute": "code", "all": "red sh"}]),
        json!([{"attribute": "code", "any": "red sh"}]),
        json!([{"attribute": "note", "all": "red sh", "prefix": true}]),
        json!([{"attribute": "price", "all": "red"}]),
        json!([{"attribute": "missing", "all": "red"}]),
        json!([{"attribute": "note"}]),
        json!([{"attribute": "note", "all": "red", "any": "blue"}]),
        json!([{"attribute": "note", "all": " "}]),
        json!([]),
    ] {
        refused(text.clone(), contains).await;
    }
    // A listing has no ranking to narrow, `query_match` needs a text query, and
    // a sub-query has a ranking of its own.
    refused(
        json!({"order_by": [{"field": "root_id", "direction": "asc"}]}),
        json!([{"attribute": "note", "all": "red"}]),
    )
    .await;
    refused(
        json!({"sub_queries": [{"query": "alpha", "limit": 3}]}),
        json!([{"attribute": "note", "all": "red"}]),
    )
    .await;
    for body in [
        json!({"vector": vector(7, 1024), "output": "metadata", "query_match": "all"}),
        json!({"sub_queries": [{"query": "alpha", "limit": 3}], "query_match": "all"}),
    ] {
        assert_eq!(
            ask(&reader, body).await,
            (400, json!({"error": "invalid_argument"}))
        );
    }
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_ranks_a_text_query_by_several_full_text_attributes_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    // Two lexical candidates: the top two documents of a ranking, whatever the
    // number of elements of an array that match.
    configure(
        &config,
        &address,
        "[namespace.retrieval]\nlexical_candidates = 2",
    );
    embed(&root, &["zeta", "alpha", "alpha beta"]);
    let writer = Writer::start(&config, &root, address).await;
    // `d` matches every ranking best but the caller may not read it. No text
    // holds the query: only the attributes do.
    let rows = [
        ("a", "alpha", "beta", json!(vec!["alpha"; 30]), "usr:x"),
        (
            "b",
            "beta",
            "alpha",
            json!(["alpha beta", "alpha delta", "alpha x y"]),
            "usr:x",
        ),
        (
            "c",
            "gamma",
            "alpha alpha",
            json!(["beta", "alpha x y"]),
            "usr:x",
        ),
        (
            "d",
            "alpha",
            "alpha",
            json!(["alpha beta", "alpha beta"]),
            "usr:other",
        ),
        ("e", "delta", "delta", json!(["delta"]), "usr:x"),
    ];
    let upserts: Vec<Value> = rows
        .iter()
        .map(|(id, subject, body, tags, reader)| {
            json!({"id": id, "text": "zeta", "subject": subject, "body": body,
                   "tags": tags, "code": subject, "plain": subject, "readers": [reader]})
        })
        .collect();
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({
                    "settings": {
                        "attributes": {"subject": "string", "body": "string",
                                       "tags": {"array": "string"}, "code": "string",
                                       "plain": "string", "price": "float"},
                        "attribute_indexes": {
                            "subject": {"full_text": true},
                            "body": {"full_text": true},
                            "tags": {"full_text": true},
                            "code": {"full_text": true, "analyzer": {
                                "tokenizer": "ngram", "ngram_min_length": 1,
                                "ngram_max_length": 8, "prefix_only": true}},
                        },
                    },
                    "upserts": upserts,
                })
            )
            .await
            .0,
        200
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    // The documents the text lane scored, best first.
    let lexical = async |query: &str, attributes: Value, query_match: &str| {
        let mut body = json!({"query": query, "output": "metadata", "fields": {"scores": true},
                              "query_match": query_match, "consistency": "strong"});
        if !attributes.is_null() {
            body["query_attributes"] = attributes;
        }
        let answer = ask(&reader, body).await;
        assert_eq!(answer.0, 200, "{}", answer.1);
        let mut scored: Vec<(f64, String)> = answer.1["hits"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|hit| {
                Some((
                    hit.get("lexical_score")?.as_f64()?,
                    hit["root_id"].as_str()?.to_owned(),
                ))
            })
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        scored.into_iter().map(|(_, root)| root).collect::<Vec<_>>()
    };
    let by = |attribute: &str, weight: f64| json!({"attribute": attribute, "weight": weight});
    let sorted = |mut roots: Vec<String>| {
        roots.sort();
        roots
    };

    // One attribute ranks by its own text: `a` alone has "alpha" in its
    // subject, `b` and `c` in their body (`d` is not readable).
    assert_eq!(
        lexical("alpha", json!([by("subject", 1.0)]), "any").await,
        ["a"]
    );
    assert_eq!(
        sorted(lexical("alpha", json!([by("body", 1.0)]), "any").await),
        ["b", "c"]
    );
    // Several attributes score the sum of their weighted scores, and the weights
    // decide the top two of the three documents that match: the subject
    // outweighs the body, or the body the subject.
    let subject_first = lexical(
        "alpha",
        json!([by("subject", 10.0), by("body", 1.0)]),
        "any",
    )
    .await;
    assert_eq!(subject_first[0], "a");
    assert_eq!(subject_first.len(), 2);
    let body_first = lexical(
        "alpha",
        json!([by("subject", 1.0), by("body", 10.0)]),
        "any",
    )
    .await;
    assert_eq!(sorted(body_first), ["b", "c"]);
    // `searchable_text` is one of the attributes, and the weight defaults to 1.
    let with_text = json!([{"attribute": "searchable_text"}, by("subject", 2.0)]);
    assert_eq!(lexical("alpha", with_text, "any").await, ["a"]);

    // A string array has one document per element. The query holds "alpha" and
    // "beta": no element of `a` or `c` holds both, `b` has one that does.
    let tags = json!([{"attribute": "tags"}]);
    assert_eq!(lexical("alpha beta", tags.clone(), "all").await, ["b"]);
    // A document with several matching elements is one candidate. `a` has 30
    // elements that outscore every one of `b` and `c`: more than a first fetch
    // for two candidates holds, and within the bound of the fetch (41), so the
    // refill finds the two best documents, `a` and then `b`, whose best element
    // outscores that of `c`.
    assert_eq!(lexical("alpha", tags.clone(), "any").await, ["a", "b"]);
    // `contains` takes an array the same way.
    let contained = ask(
        &reader,
        json!({"query": "alpha", "output": "metadata",
               "contains": [{"attribute": "tags", "all": "alpha beta"}]}),
    )
    .await;
    assert_eq!(roots(&contained), ["b"]);

    // A later write is indexed by the commit as a segment of its own. `f` is
    // readable and `g` is not, and `g` matches as well as `f` does: it is
    // never a candidate, by an array or by several attributes.
    let later = [
        ("f", "alpha", "gamma", json!(["alpha"]), "usr:x"),
        ("g", "alpha", "alpha", json!(vec!["alpha"; 5]), "usr:other"),
    ];
    let upserts: Vec<Value> = later
        .iter()
        .map(|(id, subject, body, tags, reader)| {
            json!({"id": id, "text": "zeta", "subject": subject, "body": body,
                   "tags": tags, "code": subject, "plain": subject, "readers": [reader]})
        })
        .collect();
    assert_eq!(
        writer
            .post(WRITE_KEY, "dense-fixture", &json!({"upserts": upserts}))
            .await
            .0,
        200
    );
    assert_eq!(lexical("alpha", tags, "any").await, ["a", "f"]);
    let weighted = json!([by("subject", 3.0), by("body", 1.0)]);
    let by_several = lexical("alpha", weighted, "any").await;
    assert!(by_several.contains(&"f".to_owned()), "{by_several:?}");
    assert!(!by_several.contains(&"g".to_owned()), "{by_several:?}");

    // Refused: an attribute with no full-text index, one of n-grams, one that is
    // not text, an unknown one, an array next to another attribute, a repeat, a
    // weight out of range, no attribute, and more than eight.
    let nine: Vec<Value> = (0..9)
        .map(|n| json!({"attribute": format!("a{n}")}))
        .collect();
    for attributes in [
        json!([{"attribute": "plain"}]),
        json!([{"attribute": "code"}]),
        json!([{"attribute": "price"}]),
        json!([{"attribute": "missing"}]),
        json!([{"attribute": "tags"}, {"attribute": "subject"}]),
        json!([{"attribute": "subject"}, {"attribute": "subject"}]),
        json!([by("subject", 0.0)]),
        json!([by("subject", -1.0)]),
        json!([by("subject", 1e-100)]),
        json!([by("subject", 1e-7)]),
        json!([by("subject", 100.5)]),
        json!([]),
        json!(nine),
    ] {
        assert_eq!(
            ask(
                &reader,
                json!({"query": "alpha", "output": "metadata", "query_attributes": attributes})
            )
            .await,
            (400, json!({"error": "invalid_argument"})),
            "{attributes}"
        );
    }
    // A ranking by attributes needs a text query, and a sub-query has its own.
    for body in [
        json!({"vector": vector(7, 1024), "output": "metadata",
               "query_attributes": [{"attribute": "subject"}]}),
        json!({"order_by": [{"field": "root_id", "direction": "asc"}],
               "query_attributes": [{"attribute": "subject"}]}),
        json!({"sub_queries": [{"query": "alpha", "limit": 3}],
               "query_attributes": [{"attribute": "subject"}]}),
    ] {
        assert_eq!(
            ask(&reader, body).await,
            (400, json!({"error": "invalid_argument"}))
        );
    }
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_boosts_a_ranking_by_an_attribute_and_by_recency_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["alpha"]);
    let writer = Writer::start(&config, &root, address).await;
    // The vector `embed` gives the text "alpha": the documents hold it, or its
    // opposite.
    let alpha: Vec<f32> = (0..1024)
        .map(|d: usize| ((2000 * (d + 7)) as f32).sin())
        .collect();
    let towards = |sign: f32| alpha.iter().map(|x| x * sign).collect::<Vec<f32>>();
    // `a`, `b` and `c` match the query best, by text and by vector, and tie.
    // `f` matches it worst and is the most viewed and the most recent. `g`
    // matches it best and has more of both, but the caller may not read it.
    let long = "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda";
    let rows = [
        ("a", "alpha", 1.0, 1.0, "2020-01-01T00:00:00Z", "usr:x"),
        ("b", "alpha", 1.0, 2.0, "2020-01-02T00:00:00Z", "usr:x"),
        ("c", "alpha", 1.0, -3.0, "2020-01-03T00:00:00Z", "usr:x"),
        ("f", long, -1.0, 500.0, "2026-11-20T00:00:00Z", "usr:x"),
        ("g", "alpha", 1.0, 1e9, "2026-11-22T00:00:00Z", "usr:other"),
    ];
    let upserts: Vec<Value> = rows
        .iter()
        .map(|(id, text, sign, views, seen, reader)| {
            json!({"id": id, "text": text, "vector": towards(*sign), "views": views,
                   "seen": seen, "title": id, "readers": [reader]})
        })
        .collect();
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({
                    "settings": {"attributes": {"views": "float", "seen": "datetime",
                                                "title": "string"}},
                    "upserts": upserts,
                })
            )
            .await
            .0,
        200
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let ask_with = async |mut body: Value, boosts: Value| {
        body["output"] = json!("metadata");
        if !boosts.is_null() {
            body["boosts"] = boosts;
        }
        ask(&reader, body).await
    };
    let views = json!({"function": "attribute", "attribute": "views", "weight": 2.0,
                       "midpoint": 100.0});
    let recent = json!({"function": "recency", "attribute": "seen", "weight": 2.0,
                        "midpoint": "7d", "origin": "2026-11-23T00:00:00Z"});
    // A text query with no vector of the request's own: both lanes rank it.
    let text = json!({"query": "alpha", "limit": 2});

    // By relevance alone `f` comes last of the four, outside the top 2.
    assert_eq!(
        roots(&ask_with(json!({"query": "alpha", "limit": 4}), Value::Null).await),
        ["a", "b", "c", "f"]
    );
    assert_eq!(
        roots(&ask_with(text.clone(), Value::Null).await),
        ["a", "b"]
    );
    // A boost lifts it to the first rank, and the other hit is the one the
    // boost favours among the rest: the more viewed `b`, the more recent `c`.
    // A negative value counts as 0, and a weight of 0 changes nothing.
    for (boosts, expected) in [
        (json!([views]), ["f", "b"]),
        (json!([recent]), ["f", "c"]),
        (json!([views, recent]), ["f", "b"]),
        (
            json!([{"function": "attribute", "attribute": "views", "weight": 0.0,
                    "midpoint": 1.0}]),
            ["a", "b"],
        ),
    ] {
        assert_eq!(
            roots(&ask_with(text.clone(), boosts.clone()).await),
            expected,
            "{boosts}"
        );
    }
    // The same for a ranking by vector alone, and for the caller's rows only:
    // `g` never shows, whatever its values.
    let by_vector = json!({"vector": alpha, "limit": 2});
    assert_eq!(
        roots(&ask_with(by_vector.clone(), Value::Null).await),
        ["a", "b"]
    );
    assert_eq!(
        roots(&ask_with(by_vector, json!([views])).await),
        ["f", "b"]
    );
    // A hybrid request with its own vector is boosted the same way.
    let hybrid = json!({"query": "alpha", "vector": alpha, "limit": 2});
    assert_eq!(roots(&ask_with(hybrid, json!([views])).await), ["f", "b"]);
    // `updated_at` takes recency as well.
    let updated = json!([{"function": "recency", "attribute": "updated_at", "weight": 1.0,
                          "midpoint": "1d", "origin": "2026-11-23T00:00:00Z"}]);
    assert_eq!(roots(&ask_with(text.clone(), updated).await).len(), 2);

    // A boost needs a declared attribute of the right type, a weight from 0 to
    // 1000000, a midpoint above 0, an origin, and at most 8 of them. It does
    // not go with a listing, which has no ranking, or with sub-queries or a
    // fusion, which rank by their own rules.
    let refused = [
        json!([{"function": "recency", "attribute": "views", "weight": 1.0,
                "midpoint": "7d", "origin": "2026-11-23T00:00:00Z"}]),
        json!([{"function": "attribute", "attribute": "seen", "weight": 1.0, "midpoint": 1.0}]),
        json!([{"function": "attribute", "attribute": "title", "weight": 1.0, "midpoint": 1.0}]),
        json!([{"function": "attribute", "attribute": "missing", "weight": 1.0, "midpoint": 1.0}]),
        json!([{"function": "attribute", "attribute": "views", "weight": -1.0, "midpoint": 1.0}]),
        json!([{"function": "attribute", "attribute": "views", "weight": 1000001.0,
                "midpoint": 1.0}]),
        json!([{"function": "attribute", "attribute": "views", "weight": 1.0, "midpoint": 0.0}]),
        json!([{"function": "attribute", "attribute": "views", "weight": 1.0, "midpoint": -1.0}]),
        json!([{"function": "attribute", "attribute": "views", "weight": 1.0}]),
        json!([{"function": "recency", "attribute": "seen", "weight": 1.0, "midpoint": "0s",
                "origin": "2026-11-23T00:00:00Z"}]),
        json!([{"function": "recency", "attribute": "seen", "weight": 1.0, "midpoint": "7",
                "origin": "2026-11-23T00:00:00Z"}]),
        json!([{"function": "recency", "attribute": "seen", "weight": 1.0, "midpoint": "7d"}]),
        json!([{"function": "recency", "attribute": "seen", "weight": 1.0, "midpoint": "7d",
                "origin": "yesterday"}]),
        json!([{"function": "log", "attribute": "views", "weight": 1.0, "midpoint": 1.0}]),
        json!([{"function": "attribute", "attribute": "views", "weight": 1.0, "midpoint": 1.0,
                "extra": true}]),
        json!([]),
        Value::Array(vec![views.clone(); 9]),
    ];
    for boosts in refused {
        assert_eq!(
            ask_with(text.clone(), boosts.clone()).await,
            (400, json!({"error": "invalid_argument"})),
            "{boosts}"
        );
    }
    for body in [
        json!({"order": "updated_at"}),
        json!({"sub_queries": [{"query": "alpha", "limit": 3}]}),
        json!({"queries": ["alpha", "bravo"], "fusion": {"method": "rrf"}}),
    ] {
        assert_eq!(
            ask_with(body, json!([views])).await,
            (400, json!({"error": "invalid_argument"}))
        );
    }
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_caps_the_hits_per_attribute_value_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["alpha"]);
    let writer = Writer::start(&config, &root, address).await;
    let alpha: Vec<f32> = (0..1024)
        .map(|d: usize| ((2000 * (d + 7)) as f32).sin())
        .collect();
    // `a` to `e` match the query equally, by text and by vector, so they rank
    // by their ids. `0h` would rank first, but the caller may not read it.
    let rows = [
        ("0h", "red", "en", "usr:other"),
        ("a", "red", "en", "usr:x"),
        ("b", "red", "en", "usr:x"),
        ("c", "red", "de", "usr:x"),
        ("d", "blue", "en", "usr:x"),
        ("e", "blue", "de", "usr:x"),
    ];
    let upserts: Vec<Value> = rows
        .iter()
        .map(|(id, kind, lang, reader)| {
            json!({"id": id, "text": "alpha", "vector": alpha, "kind": kind, "lang": lang,
                   "readers": [reader]})
        })
        .collect();
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({
                    "settings": {"attributes": {"kind": "string", "lang": "string",
                                                "views": "float"}},
                    "upserts": upserts,
                })
            )
            .await
            .0,
        200
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let ask_with = async |mut body: Value, members: Value| {
        body["output"] = json!("metadata");
        for (name, member) in members.as_object().into_iter().flatten() {
            body[name] = member.clone();
        }
        ask(&reader, body).await
    };
    let by_kind = json!([{"attribute": "kind", "max": 2}]);
    let by_lang = json!([{"attribute": "lang", "max": 2}]);
    let both = json!([{"attribute": "kind", "max": 2}, {"attribute": "lang", "max": 2}]);
    // Text and vector alone, and the two fused.
    let rankings = [
        json!({"query": "alpha", "limit": 5}),
        json!({"vector": alpha, "limit": 5}),
        json!({"query": "alpha", "vector": alpha, "limit": 5}),
    ];
    for ranking in rankings {
        let plain = ask_with(ranking.clone(), json!({})).await;
        assert_eq!(roots(&plain), ["a", "b", "c", "d", "e"], "{ranking}");
        // Without the new members, or with `group_documents` on, the answer is the same.
        let grouped = ask_with(ranking.clone(), json!({"group_documents": true})).await;
        assert_eq!(grouped.1["hits"], plain.1["hits"], "{ranking}");
        // A cap is hard: `c` is over the two reds, and no one takes its place
        // but the next candidates.
        for (caps, expected) in [
            (&by_kind, vec!["a", "b", "d", "e"]),
            (&by_lang, vec!["a", "b", "c", "e"]),
            (&both, vec!["a", "b", "e"]),
            // Every row is `api`; no row has an author.
            (
                &json!([{"attribute": "source_type", "max": 3}]),
                vec!["a", "b", "c"],
            ),
            (
                &json!([{"attribute": "author", "max": 1}]),
                vec!["a", "b", "c", "d", "e"],
            ),
        ] {
            assert_eq!(
                roots(&ask_with(ranking.clone(), json!({"caps": caps})).await),
                expected,
                "{ranking} {caps}"
            );
        }
        // The limit still applies after the cap.
        let mut limited = ranking.clone();
        limited["limit"] = json!(3);
        assert_eq!(
            roots(&ask_with(limited, json!({"caps": by_kind})).await),
            ["a", "b", "d"],
            "{ranking}"
        );
    }

    // A cap names a declared `string` attribute or `author`, `project` or
    // `source_type`, 1 to 4 times, once each, and at most 1 to 75 hits; the
    // members go with neither a listing nor `queries` nor `sub_queries`.
    let text = json!({"query": "alpha", "limit": 5});
    let cap = |attribute: &str, max: Value| json!([{"attribute": attribute, "max": max}]);
    let refused = [
        cap("views", json!(1)),
        cap("missing", json!(1)),
        cap("updated_at", json!(1)),
        cap("kind", json!(0)),
        cap("kind", json!(76)),
        cap("kind", json!(-1)),
        cap("kind", json!("2")),
        json!([{"attribute": "kind"}]),
        json!([{"attribute": "kind", "max": 1, "extra": true}]),
        json!([{"attribute": "kind", "max": 1}, {"attribute": "kind", "max": 2}]),
        json!([]),
        json!([{"attribute": "kind", "max": 1}, {"attribute": "lang", "max": 1},
               {"attribute": "author", "max": 1}, {"attribute": "project", "max": 1},
               {"attribute": "source_type", "max": 1}]),
    ];
    for caps in refused {
        assert_eq!(
            ask_with(text.clone(), json!({"caps": caps})).await,
            (400, json!({"error": "invalid_argument"})),
            "{caps}"
        );
    }
    for members in [
        json!({"group_documents": "no"}),
        json!({"group_documents": 1}),
    ] {
        assert_eq!(
            ask_with(text.clone(), members.clone()).await,
            (400, json!({"error": "invalid_argument"})),
            "{members}"
        );
    }
    // Grouping on is the default, so saying it is no refusal.
    assert_eq!(
        ask_with(
            json!({"queries": ["alpha", "bravo"]}),
            json!({"group_documents": true})
        )
        .await
        .0,
        200
    );
    for body in [
        json!({"order": "updated_at"}),
        json!({"queries": ["alpha", "bravo"]}),
        json!({"queries": ["alpha", "bravo"], "fusion": {"method": "rrf"}}),
        json!({"sub_queries": [{"query": "alpha", "limit": 3}]}),
    ] {
        for members in [json!({"caps": by_kind}), json!({"group_documents": false})] {
            assert_eq!(
                ask_with(body.clone(), members.clone()).await,
                (400, json!({"error": "invalid_argument"})),
                "{body} {members}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_reranks_as_a_query_asks_within_the_spend_limit_on_storage() {
    let provider = super::rerank::Provider::open().await;
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    // Room for five provider calls of at most 10 millionths of a dollar.
    configure(
        &config,
        &address,
        &format!(
            "[namespace.rerank]\nendpoint='{}'\nmodel='fixture-rerank'\napi_key={{env='FYRR_TEST_SECRET'}}\n\
             candidates=10\nweight=1\ndeadline='5s'\nspend_limit_microusd=55\nmax_call_cost_microusd=10\n",
            provider.endpoint
        ),
    );
    let texts = ["alpha", "bravo", "charlie", "delta"];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;
    let tags = [Some("red"), Some("blue"), None, Some("green")];
    let upserts: Vec<Value> = texts
        .iter()
        .zip(tags)
        .map(|(text, tag)| {
            let mut row = json!({"id": &text[..1], "text": text, "readers": ["usr:x"]});
            if let Some(tag) = tag {
                row["tag"] = json!(tag);
            }
            row
        })
        .collect();
    let settings = json!({"attributes": {"tag": "string", "price": "float"}});
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": settings, "upserts": upserts})
            )
            .await
            .0,
        200
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let search = |rerank: Value| {
        let mut body = json!({"query": "alpha", "limit": 4, "output": "metadata"});
        if !rerank.is_null() {
            body["rerank"] = rerank;
        }
        body
    };
    let last_request = || {
        provider
            .state
            .requests
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .clone()
    };
    let documents = |request: &Value| {
        let mut documents: Vec<String> = request["documents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|text| text.as_str().unwrap().to_owned())
            .collect();
        documents.sort();
        documents
    };

    // Switched off, the request makes no call and keeps the fusion order.
    let off = ask(&reader, search(json!({"enabled": false}))).await;
    let fused = roots(&off);
    assert_eq!(fused.len(), 4);
    assert_eq!(provider.calls(), 0);
    let reversed: Vec<String> = fused.iter().rev().cloned().collect();

    // Without the member the namespace's policy applies: the fixture provider
    // puts the last candidate first, and scores the text of the record.
    let policy = ask(&reader, search(Value::Null)).await;
    assert_eq!(roots(&policy), reversed);
    assert_eq!(provider.calls(), 1);
    let request = last_request();
    assert_eq!(request["query"], "alpha");
    assert_eq!(documents(&request), texts);

    // The request names the text the candidates are scored against.
    let named = ask(&reader, search(json!({"query": "something else"}))).await;
    assert_eq!(roots(&named), reversed);
    assert_eq!(provider.calls(), 2);
    assert_eq!(last_request()["query"], "something else");

    // And the attribute: a string one, with "" for a row that sets none, or
    // the namespace's text attribute.
    assert_eq!(
        roots(&ask(&reader, search(json!({"attribute": "tag"}))).await),
        reversed
    );
    assert_eq!(provider.calls(), 3);
    assert_eq!(documents(&last_request()), ["", "blue", "green", "red"]);
    assert_eq!(
        roots(&ask(&reader, search(json!({"attribute": "text"}))).await),
        reversed
    );
    assert_eq!(provider.calls(), 4);
    assert_eq!(documents(&last_request()), texts);

    // Anything else is refused, before any call.
    for rerank in [
        json!({"attribute": "price"}),
        json!({"attribute": "nope"}),
        json!({"query": ""}),
        json!({"enabled": "yes"}),
        json!({"other": 1}),
    ] {
        assert_eq!(
            ask(&reader, search(rerank.clone())).await,
            (400, json!({"error": "invalid_argument"})),
            "{rerank}"
        );
    }
    assert_eq!(provider.calls(), 4);

    // A search by vector alone has no text to score against, unless the
    // request supplies it.
    let by_vector = |rerank: Value| {
        let mut body = json!({"vector": vector(0, 1024), "limit": 4, "output": "metadata"});
        if !rerank.is_null() {
            body["rerank"] = rerank;
        }
        body
    };
    assert_eq!(roots(&ask(&reader, by_vector(Value::Null)).await).len(), 4);
    assert_eq!(provider.calls(), 4);
    assert_eq!(
        roots(&ask(&reader, by_vector(json!({"query": "alpha"}))).await).len(),
        4
    );
    assert_eq!(provider.calls(), 5);

    // The spend limit is the namespace's, whoever asks: the next call is not
    // made and the answer says so.
    let spent = ask(&reader, search(json!({"query": "again"}))).await;
    assert_eq!(roots(&spent), fused);
    assert_eq!(spent.1["metadata"]["degraded"], json!(["rerank"]));
    assert_eq!(provider.calls(), 5);
}

/// A namespace declares named vector attributes beside the built-in `vector`,
/// each with its own dimensions and index: a query names the one it searches,
/// text is embedded only for an attribute that names a `string` attribute it
/// is embedded from, a row may leave one null, and a row nobody may read is
/// not returned though it is the nearest one.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_searches_named_vector_attributes_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    // Two dense candidates: only the request limit can fetch more.
    configure(
        &config,
        &address,
        "[namespace.retrieval]\ndense_candidates = 2",
    );
    let writer = Writer::start(&config, &root, address).await;
    let ids = ["a", "b", "c", "d", "e", "f"];
    let mut upserts: Vec<Value> = Vec::new();
    for (n, id) in ids.iter().enumerate() {
        let mut row = json!({
            "id": id,
            "text": format!("row{id}"),
            "vector": vector(n, 1024),
            "readers": ["grp:allow"],
        });
        // Row e has no `image` and row f no caption: their vectors are null.
        if *id != "e" {
            row["image"] = json!(vector(10 + n, 1024));
        }
        if *id != "f" {
            // The provider embeds the caption: the vector is not in the row.
            let caption = format!("caption {id}");
            root.embedding.insert_both(&caption, vector(20 + n, 1024));
            row["caption"] = json!(caption);
        }
        if n < 3 {
            row["small"] = json!(vector(30 + n, 16));
            row["order"] = json!(vector(40 + n, 8));
        }
        upserts.push(row);
    }
    // As near to every query as a row can be, but nobody may read it.
    upserts.push(json!({
        "id": "hidden",
        "text": "rowhidden",
        "vector": vector(1, 1024),
        "image": vector(11, 1024),
        "caption": "caption b",
        "readers": ["usr:nobody"],
    }));
    let settings = json!({
        "attributes": {"caption": "string"},
        "vectors": {
            "image": {"dimensions": 1024},
            "caption_vec": {"dimensions": 1024, "from": "caption"},
            "small": {"dimensions": 16},
            // Names that are words of SQL: a predicate on them needs quoting.
            "order": {"dimensions": 8},
            "null": {"dimensions": 1024, "from": "caption"},
        },
    });
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": settings, "upserts": upserts})
            )
            .await,
        (200, json!({"upserted": 7, "deleted": 0}))
    );

    // The manifest records them, with an index of each.
    let (_, manifest) = super::coordination::publication(&config).await;
    assert_eq!(manifest.schema_version.major, 6);
    assert_eq!(
        manifest
            .settings
            .namespace
            .vectors
            .keys()
            .collect::<Vec<_>>(),
        ["caption_vec", "image", "null", "order", "small"]
    );
    let indexes: Vec<_> = manifest.indexes.iter().map(|i| i.name.as_str()).collect();
    for name in [
        "vector_idx",
        "vec_image_idx",
        "vec_caption_vec_idx",
        "vec_small_idx",
        "vec_order_idx",
        "vec_null_idx",
    ] {
        assert!(indexes.contains(&name), "{name} is not among {indexes:?}");
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
    let sorted = |mut ids: Vec<String>| {
        ids.sort();
        ids
    };

    // The default attribute is the built-in vector, as before.
    let built_in = listed(json!({"vector": vector(1, 1024), "limit": 10}));
    assert_eq!(built_in.len(), 6);
    assert_eq!(built_in[0], "b");
    assert!(!built_in.contains(&"hidden".to_owned()));

    // A query that names an attribute searches its column: the nearest row is
    // the one whose `image` is the query, never the hidden one that ties with
    // it, and row e, which has none, is not returned at all.
    let image =
        listed(json!({"vector": vector(11, 1024), "vector_attribute": "image", "limit": 10}));
    assert_eq!(image[0], "b");
    assert_eq!(sorted(image.clone()), ["a", "b", "c", "d", "f"]);
    // An exact search finds the same rows in the same order.
    assert_eq!(
        listed(
            json!({"vector": vector(11, 1024), "vector_attribute": "image", "limit": 10, "exact": true})
        ),
        image
    );
    // The filter bounds it, and the permissions still bound the scan.
    assert_eq!(
        sorted(listed(json!({
            "vector": vector(11, 1024),
            "vector_attribute": "image",
            "exact": true,
            "filter": {"field": "root_id", "op": "in", "value": ["a", "c", "e", "hidden"]},
        }))),
        ["a", "c"]
    );

    // The dimensions are the attribute's own.
    let small = listed(json!({"vector": vector(31, 16), "vector_attribute": "small", "limit": 10}));
    assert_eq!(small[0], "b");
    assert_eq!(sorted(small), ["a", "b", "c"]);

    let order = listed(json!({"vector": vector(41, 8), "vector_attribute": "order", "limit": 10}));
    assert_eq!(order[0], "b");
    assert_eq!(sorted(order), ["a", "b", "c"]);

    // A caption's embedding is made by the writer; a query text is embedded by
    // the namespace's model for an attribute that is embedded from text, and
    // the row without a caption is not returned.
    let caption =
        listed(json!({"query": "caption c", "vector_attribute": "caption_vec", "limit": 10}));
    assert_eq!(caption[0], "c");
    assert_eq!(sorted(caption), ["a", "b", "c", "d", "e"]);
    let tie = listed(json!({"query": "caption b", "vector_attribute": "caption_vec", "limit": 10}));
    assert_eq!(tie[0], "b");
    assert!(!tie.contains(&"hidden".to_owned()));
    assert_eq!(
        sorted(listed(
            json!({"vector": vector(22, 1024), "vector_attribute": "caption_vec", "limit": 10})
        )),
        ["a", "b", "c", "d", "e"]
    );
    // Row f has no caption and so no vector, but its text is what "rowf" says: a
    // text query of an attribute selects only the rows that have a vector, so the
    // lexical lane does not return it either, exact or not, whatever the
    // attribute is called.
    root.embedding.insert_both("rowf", vector(25, 1024));
    for attribute in ["caption_vec", "null"] {
        for exact in [false, true] {
            let found = listed(json!({
                "query": "rowf",
                "vector_attribute": attribute,
                "limit": 10,
                "exact": exact,
            }));
            assert!(
                !found.contains(&"f".to_owned()),
                "{attribute} {exact}: {found:?}"
            );
            assert!(!found.contains(&"hidden".to_owned()), "{attribute} {exact}");
            assert_eq!(found.len(), 5, "{attribute} {exact}: {found:?}");
        }
    }

    // A name the namespace does not declare, a text query of an attribute that
    // is not embedded from text, and a vector of another dimension are refused.
    for arguments in [
        json!({"vector": vector(11, 1024), "vector_attribute": "nope"}),
        json!({"query": "rowa", "vector_attribute": "image"}),
        json!({"vector": vector(11, 1024), "vector_attribute": "small"}),
        json!({"vector": vector(31, 16), "vector_attribute": "image"}),
        json!({"vector": vector(31, 16)}),
        json!({"query": "rowa", "vector_attribute": "vector"}),
    ] {
        error(client.call("search", arguments), "invalid_argument");
    }

    // A patch keeps the stored vectors: it brings none, and it cannot change
    // the attribute one was embedded from.
    for (patch, code) in [
        (
            json!({"id": "a", "image": vector(10, 1024)}),
            "vector_not_patchable",
        ),
        (
            json!({"id": "a", "caption": "caption z"}),
            "text_not_patchable",
        ),
    ] {
        assert_eq!(
            writer
                .post(WRITE_KEY, "dense-fixture", &json!({"patches": [patch]}))
                .await,
            (400, json!({"error": code}))
        );
    }
    // A vector of another dimension is refused, and the vector attributes are
    // fixed for the life of the namespace.
    let row = json!({"id": "x", "text": "rowx", "image": vector(10, 16)});
    assert_eq!(
        writer
            .post(WRITE_KEY, "dense-fixture", &json!({"upserts": [row]}))
            .await,
        (400, json!({"error": "vector_invalid"}))
    );
    let more = json!({"settings": {"vectors": {"extra": {"dimensions": 8}}}, "upserts": [{"id": "x", "text": "rowx"}]});
    assert_eq!(
        writer.post(WRITE_KEY, "dense-fixture", &more).await,
        (409, json!({"error": "namespace_settings_fixed"}))
    );
}

/// A multi-vector attribute holds a list of vectors in each row, and a query of
/// several vectors is one query that Lance scores by the best similarity of each
/// of them: the nearest row is the one that holds all of them, though a row
/// nobody may read does too. A row with no vectors is no result. The same holds
/// under both element types and below the number of vectors an index is trained
/// from, where the search scans, and above it, where the index answers.
async fn multi_vector_case(element: &str, background: usize) {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    // Not an open namespace: its readers are enforced.
    configure(&config, &address, "");
    let writer = Writer::start(&config, &root, address).await;
    let axis = |pairs: &[(usize, f32)]| {
        let mut vector = vec![0.0_f32; 16];
        for (at, value) in pairs {
            vector[*at] = *value;
        }
        vector
    };
    // The query is the axes 0 and 1. A holds both, B holds the second at
    // cosine 0.9, C at 0.5 and D holds each at 0.6 and 0.5; E and F have none.
    let planted = [
        ("a", vec![axis(&[(0, 1.0)]), axis(&[(1, 1.0)])]),
        ("b", vec![axis(&[(0, 1.0)]), axis(&[(1, 0.9), (2, 0.436)])]),
        ("c", vec![axis(&[(0, 1.0)]), axis(&[(1, 0.5), (2, 0.866)])]),
        (
            "d",
            vec![axis(&[(0, 0.6), (3, 0.8)]), axis(&[(1, 0.5), (2, 0.866)])],
        ),
    ];
    let mut upserts: Vec<Value> = Vec::new();
    for (n, (id, tokens)) in planted.iter().enumerate() {
        upserts.push(json!({
            "id": id, "text": format!("row{id}"), "vector": vector(n, 1024),
            "readers": ["grp:allow", "usr:x"], "tokens": tokens,
        }));
    }
    upserts.push(json!({
        "id": "e", "text": "rowe", "vector": vector(4, 1024), "readers": ["grp:allow", "usr:x"],
    }));
    upserts.push(json!({
        "id": "f", "text": "rowf", "vector": vector(5, 1024), "readers": ["grp:allow", "usr:x"],
        "tokens": [],
    }));
    // As near to the query as a row can be, but nobody may read it.
    upserts.push(json!({
        "id": "hidden", "text": "rowhidden", "vector": vector(6, 1024),
        "readers": ["usr:nobody"], "tokens": [axis(&[(0, 1.0)]), axis(&[(1, 1.0)])],
    }));
    // Rows of eight vectors near the axes 8 to 15, away from the query.
    for n in 0..background {
        let tokens: Vec<_> = (0..8)
            .map(|j| {
                (0..16)
                    .map(|d| {
                        let noise = ((n * 31 + j * 17 + d * 7) % 11) as f32 / 100.0;
                        noise + f32::from(d == 8 + (n + j) % 8)
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        upserts.push(json!({
            "id": format!("bg{n}"), "text": format!("rowbg{n}"), "vector": vector(10 + n, 1024),
            "readers": ["grp:allow", "usr:x"], "tokens": tokens,
        }));
    }
    let count = upserts.len();
    let settings = json!({
        "dense_element": element,
        "vectors": {
            "tokens": {"dimensions": 16, "multi": true},
            "single": {"dimensions": 16},
        },
    });
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": settings, "upserts": upserts})
            )
            .await,
        (200, json!({"upserted": count, "deleted": 0})),
        "{element} {background}"
    );
    let (_, manifest) = super::coordination::publication(&config).await;
    assert!(manifest.settings.namespace.vectors["tokens"].multi);
    assert!(
        manifest
            .indexes
            .iter()
            .any(|index| index.name == "vec_tokens_idx"),
        "{element} {background}"
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
    let both = json!([axis(&[(0, 1.0)]), axis(&[(1, 1.0)])]);
    let found = listed(json!({"vectors": both, "vector_attribute": "tokens", "limit": 10}));
    let context = format!("{element} {background}: {found:?}");
    assert_eq!(found[..4], ["a", "b", "c", "d"], "{context}");
    // Rows without vectors and the row nobody may read are not returned, though
    // the hidden one is as near as the best.
    for denied in ["e", "f", "hidden"] {
        assert!(!found.contains(&denied.to_owned()), "{context}");
    }
    if background == 0 {
        assert_eq!(found.len(), 4, "{context}");
    }
    // An exact search finds the same rows in the same order, and a filter
    // bounds it under the permissions.
    let exact =
        listed(json!({"vectors": both, "vector_attribute": "tokens", "limit": 10, "exact": true}));
    assert_eq!(exact[..4], ["a", "b", "c", "d"], "{context}");
    assert_eq!(
        listed(json!({
            "vectors": both,
            "vector_attribute": "tokens",
            "exact": true,
            "filter": {"field": "root_id", "op": "in", "value": ["a", "c", "e", "hidden"]},
        })),
        ["a", "c"],
        "{context}"
    );
    // One vector is a query of one.
    let single =
        listed(json!({"vectors": [axis(&[(1, 1.0)])], "vector_attribute": "tokens", "limit": 10}));
    assert_eq!(single[..2], ["a", "b"], "{context}");
    // Another dimension, `vectors` of an attribute that holds one vector in a
    // row, a `vector` of the one that holds a list, a count of vectors out of
    // bounds and a text query of the attribute are refused.
    for arguments in [
        json!({"vectors": [axis(&[(0, 1.0)])], "vector_attribute": "single"}),
        json!({"vectors": [vec![1.0; 8]], "vector_attribute": "tokens"}),
        // Vectors of half the dimensions, or of twice, are not vectors of the
        // attribute though they make as many numbers as its dimensions are.
        json!({"vectors": [vec![1.0; 8], vec![1.0; 8]], "vector_attribute": "tokens"}),
        json!({"vectors": [vec![1.0; 32]], "vector_attribute": "tokens"}),
        json!({"vectors": vec![vec![1.0; 32]; 32], "vector_attribute": "tokens"}),
        json!({"vector": axis(&[(0, 1.0)]), "vector_attribute": "tokens"}),
        json!({"vectors": both}),
        json!({"vectors": both, "vector": axis(&[(0, 1.0)]), "vector_attribute": "tokens"}),
        json!({"vectors": [axis(&[(0, 1.0)]), vec![1.0; 8]], "vector_attribute": "tokens"}),
        json!({"vectors": [], "vector_attribute": "tokens"}),
        json!({"vectors": vec![axis(&[(0, 1.0)]); 33], "vector_attribute": "tokens"}),
        json!({"query": "rowa", "vector_attribute": "tokens"}),
    ] {
        error(client.call("search", arguments.clone()), "invalid_argument");
    }
    // A sub-query of the JSON query route takes them as `search` does, and
    // refuses the same.
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let sub = |vectors: Value| {
        json!({
            "sub_queries": [{"vectors": vectors, "vector_attribute": "tokens", "limit": 10}],
            "output": "metadata",
        })
    };
    assert_eq!(
        roots(&ask(&reader, sub(both.clone())).await)[..4],
        ["a", "b", "c", "d"],
        "{context}"
    );
    for bad in [
        json!([vec![1.0; 8], vec![1.0; 8]]),
        json!([vec![1.0; 32]]),
        json!(vec![vec![1.0; 32]; 32]),
    ] {
        assert_eq!(
            ask(&reader, sub(bad.clone())).await.0,
            400,
            "{context} {bad}"
        );
    }
    // A row brings its vectors as a list of vectors of the attribute's
    // dimensions, and a patch keeps what is stored.
    for (row, code) in [
        (
            json!({"id": "x", "text": "rowx", "tokens": axis(&[(0, 1.0)])}),
            "vector_invalid",
        ),
        (
            json!({"id": "x", "text": "rowx", "tokens": [vec![1.0; 8]]}),
            "vector_invalid",
        ),
        (
            json!({"id": "x", "text": "rowx", "tokens": "text"}),
            "vector_invalid",
        ),
    ] {
        assert_eq!(
            writer
                .post(WRITE_KEY, "dense-fixture", &json!({"upserts": [row]}))
                .await,
            (400, json!({"error": code}))
        );
    }
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"patches": [{"id": "a", "tokens": [axis(&[(0, 1.0)])]}]})
            )
            .await,
        (400, json!({"error": "vector_not_patchable"}))
    );
}

/// The metric of the namespace is cosine for a multi-vector attribute, and a
/// namespace of another metric is refused one.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_searches_multi_vector_attributes_on_storage() {
    for (element, background) in [("f16", 0), ("f32", 0), ("f16", 100)] {
        multi_vector_case(element, background).await;
    }
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    let writer = Writer::start(&config, &root, address).await;
    for metric in ["dot", "squared_l2"] {
        let settings = json!({"dense_metric": metric, "vectors": {"tokens": {"dimensions": 16, "multi": true}}});
        assert_eq!(
            writer
                .post(
                    WRITE_KEY,
                    "dense-fixture",
                    &json!({"settings": settings, "upserts": [{"id": "x", "text": "rowx"}]})
                )
                .await,
            (400, json!({"error": "settings_invalid"})),
            "{metric}"
        );
    }
}

/// A `phrase` member keeps the hits whose text holds its tokens next to one
/// another in this order, for a text and a vector ranking alike; a `slop`
/// above 0 is refused, because Lance 14.0.3 loses matches for it; it is joined to the filter by AND and refused where
/// there is no ranking to narrow.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_requires_a_phrase_of_the_text_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let text = std::fs::read_to_string(&config).unwrap().replacen(
        "[namespace]\n",
        "[namespace]\nfts_positions = true\n",
        1,
    );
    std::fs::write(&config, text).unwrap();
    embed(&root, &["alpha"]);
    let writer = Writer::start(&config, &root, address).await;
    // `d` holds the phrase but the caller may not read it; `b` holds both
    // tokens in the other order.
    let rows = [
        ("a", "alpha beta gamma", "usr:x"),
        ("b", "beta alpha gamma", "usr:x"),
        ("c", "alpha gamma beta", "usr:x"),
        ("d", "alpha beta", "usr:other"),
        ("e", "alpha delta epsilon beta", "usr:x"),
    ];
    let upserts: Vec<Value> = rows
        .iter()
        .enumerate()
        .map(|(n, (id, text, reader))| {
            json!({"id": id, "text": text, "vector": vector(n, 1024), "readers": [reader]})
        })
        .collect();
    assert_eq!(
        writer
            .post(WRITE_KEY, "dense-fixture", &json!({"upserts": upserts}))
            .await,
        (200, json!({"upserted": 5, "deleted": 0}))
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let with = |mut body: Value, phrase: Value| {
        body["output"] = json!("metadata");
        body["phrase"] = phrase;
        body
    };
    let found = async |body: Value, phrase: Value| {
        let mut found = roots(&ask(&reader, with(body, phrase)).await);
        found.sort();
        found
    };
    let refused = async |body: Value, phrase: Value| {
        assert_eq!(
            ask(&reader, with(body, phrase)).await,
            (400, json!({"error": "invalid_argument"})),
        );
    };
    let text = json!({"query": "alpha"});
    let by_vector = json!({"vector": vector(7, 1024)});
    for ranking in [&text, &by_vector] {
        let phrase = |text: &str, slop: u32| json!({"text": text, "slop": slop});
        assert_eq!(
            found(ranking.clone(), json!({"text": "alpha beta"})).await,
            ["a"]
        );
        assert_eq!(found(ranking.clone(), phrase("alpha beta", 0)).await, ["a"]);
        assert_eq!(found(ranking.clone(), phrase("beta alpha", 0)).await, ["b"]);
        assert!(
            found(ranking.clone(), phrase("beta gamma alpha", 0))
                .await
                .is_empty()
        );
    }
    // The phrase and the filter are joined by AND.
    let filtered = json!({"query": "alpha",
                          "filter": {"field": "root_id", "op": "ne", "value": "b"}});
    assert_eq!(found(filtered, json!({"text": "alpha gamma"})).await, ["c"]);
    // A phrase has a text of at most 4096 bytes and a slop of 0, and it names
    // nothing else; there is no ranking in a listing to narrow, and a
    // sub-query has its own.
    for phrase in [
        json!({"text": ""}),
        json!({"text": " "}),
        json!({"text": "alpha beta", "slop": 1}),
        json!({"text": "alpha beta", "slop": 17}),
        json!({"text": "alpha beta", "slop": -1}),
        json!({"text": "alpha beta", "attribute": "title"}),
        json!({"slop": 1}),
    ] {
        refused(text.clone(), phrase).await;
    }
    refused(
        json!({"order_by": [{"field": "root_id", "direction": "asc"}]}),
        json!({"text": "alpha beta"}),
    )
    .await;
    refused(
        json!({"sub_queries": [{"query": "alpha", "limit": 3}]}),
        json!({"text": "alpha beta"}),
    )
    .await;
}

/// A namespace stores no token positions unless `fts_positions` asks for them,
/// and a phrase on one is refused, not answered by a scan.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_refuses_a_phrase_on_a_namespace_without_positions_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["alpha"]);
    let writer = Writer::start(&config, &root, address).await;
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"upserts": [{"id": "a", "text": "alpha beta", "vector": vector(0, 1024),
                                     "readers": ["usr:x"]}]})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    let reader = super::http_mcp::Reader::start_appended(&config, &root, |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
    })
    .await;
    let body = json!({"query": "alpha", "output": "metadata"});
    assert_eq!(roots(&ask(&reader, body.clone()).await), ["a"]);
    for ranking in [
        body,
        json!({"vector": vector(0, 1024), "output": "metadata"}),
    ] {
        let mut phrase = ranking.clone();
        phrase["phrase"] = json!({"text": "alpha beta"});
        assert_eq!(
            ask(&reader, phrase).await,
            (400, json!({"error": "invalid_argument"}))
        );
    }
}
