//! The document write API of the real writer executable: documents written
//! over HTTP are published whole, searched, fetched and deleted.
use super::coordination::{host_command, worker_command};
use super::tools::{Mcp, body, error};
use super::*;
use reqwest::Client;
use std::time::{Duration, Instant};

const OPERATOR: &str = "owned-fixture-operator-token-only";

fn secret(n: usize) -> String {
    format!("fyrr_key_{n:064}")
}
const WRITE_KEY: usize = 41;
const READ_KEY: usize = 42;

/// A configuration without sources that writes through the API, with a key
/// that may write and one that may only read.
fn configure(config: &Path, address: &std::net::SocketAddr, extra: &str) {
    let text = std::fs::read_to_string(config).unwrap();
    let base = &text[..text.find("[[sources]]").unwrap()];
    std::fs::write(
        config,
        format!(
            "{base}\n[namespace.write_api]\n{extra}\n\
             [[identity.api_keys]]\nname='writer'\nkey={{env='FYRR_TEST_WRITE_KEY'}}\nroles=['write']\n\
             [[identity.api_keys]]\nname='reader'\nkey={{env='FYRR_TEST_READ_KEY'}}\nreaders=['usr:x']\n\
             [writer.commit]\ncoalescing_timer='10m'\n\
             [writer.serve.admin]\nlisten='{address}'\noperator_token={{env='FYRR_TEST_OPERATOR'}}\n"
        ),
    )
    .unwrap();
}

struct Writer {
    child: std::process::Child,
    base: String,
    client: Client,
}
impl Drop for Writer {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let until = Instant::now() + Duration::from_secs(15);
        while Instant::now() < until {
            if self.child.try_wait().unwrap().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Writer {
    /// Publishes the keys, then serves.
    async fn start(config: &Path, root: &Path, address: std::net::SocketAddr) -> Self {
        let keys = host_command(config)
            .args(["writer", "keys"])
            .env("FYRR_TEST_WRITE_KEY", secret(WRITE_KEY))
            .env("FYRR_TEST_READ_KEY", secret(READ_KEY))
            .output()
            .unwrap();
        assert!(keys.status.success(), "{}", report(&keys));
        let child = worker_command(config)
            .args(["writer", "serve", "--jobs", "1"])
            .env("FYRR_TEST_WRITE_KEY", secret(WRITE_KEY))
            .env("FYRR_TEST_READ_KEY", secret(READ_KEY))
            .env("FYRR_TEST_OPERATOR", OPERATOR)
            .stdout(std::fs::File::create(root.join("service.stdout")).unwrap())
            .stderr(std::fs::File::create(root.join("service.stderr")).unwrap())
            .spawn()
            .unwrap();
        let mut writer = Self {
            child,
            base: format!("http://{address}"),
            client: Client::builder()
                .timeout(Duration::from_secs(1200))
                .build()
                .unwrap(),
        };
        for _ in 0..400 {
            assert!(
                writer.child.try_wait().unwrap().is_none(),
                "the writer exited: {}",
                std::fs::read_to_string(root.join("service.stderr")).unwrap()
            );
            // Up when it answers, even a refusal: the namespace may not exist yet.
            let probe = writer
                .client
                .post(format!(
                    "{}/v1/namespaces/dense-fixture/documents",
                    writer.base
                ))
                .send()
                .await;
            if probe.is_ok_and(|response| response.status() == 401) {
                return writer;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("writer HTTP startup deadline");
    }

    async fn post(&self, key: usize, namespace: &str, body: &Value) -> (u16, Value) {
        let response = self
            .client
            .post(format!("{}/v1/namespaces/{namespace}/documents", self.base))
            .bearer_auth(secret(key))
            .json(body)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }
}

/// Embeddings of the texts the fixture will be asked for.
fn embed(root: &FixtureRoot, texts: &[&str]) {
    for (n, text) in texts.iter().enumerate() {
        let vector: Vec<f32> = (0..1024)
            .map(|d| (((n + 2000) * (d + 7)) as f32).sin())
            .collect();
        root.embedding.insert_both(text, vector);
    }
}

fn hits(config: &Path, query: &str) -> Vec<String> {
    success(config, &["reader", "search", query, "--limit", "10"])["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_upserts_searches_fetches_deletes_and_refuses_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["blue shirt", "red hat", "green coat"]);
    let writer = Writer::start(&config, &root, address).await;
    let namespace = "dense-fixture";
    let create = json!({
        "settings": {"attributes": {"price": "float", "stock": "int"}},
        "upserts": [
            {"id": "a1", "text": "blue shirt", "price": 12.5, "stock": 3, "readers": ["grp:allow"]},
            {"id": "a2", "text": "red hat", "price": 4.0, "readers": ["usr:nobody"]},
            {"id": "a3", "text": "green coat"},
        ],
    });

    // Authentication and the shape of a request come before anything is written.
    let unkeyed = writer
        .client
        .post(format!(
            "{}/v1/namespaces/{namespace}/documents",
            writer.base
        ))
        .json(&create)
        .send()
        .await
        .unwrap();
    assert_eq!(unkeyed.status(), 401);
    assert_eq!(writer.post(READ_KEY, namespace, &create).await.0, 403);
    assert_eq!(
        writer.post(WRITE_KEY, "other", &create).await.0,
        404,
        "one namespace is served"
    );
    let gzip = writer
        .client
        .post(format!(
            "{}/v1/namespaces/{namespace}/documents",
            writer.base
        ))
        .bearer_auth(secret(WRITE_KEY))
        .header("Content-Encoding", "gzip")
        .json(&create)
        .send()
        .await
        .unwrap();
    assert_eq!(gzip.status(), 415);
    let duplicate = json!({"upserts": [{"id": "d", "text": "red hat"}], "deletes": ["d"]});
    assert_eq!(
        writer.post(WRITE_KEY, namespace, &duplicate).await,
        (400, json!({"error": "duplicate_id"}))
    );
    // A document that is refused after the settings are read (a NUL in its
    // text) must not leave the namespace created, with the mode it asked for.
    let invalid = json!({
        "settings": {"write_api": {"open": true}},
        "upserts": [{"id": "d", "text": "red\0hat"}],
    });
    assert_eq!(
        writer.post(WRITE_KEY, namespace, &invalid).await,
        (400, json!({"error": "document_invalid"}))
    );
    let loaded = fyrr_core::config::load(&config).unwrap();
    assert!(
        fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .is_none(),
        "a refused request creates nothing"
    );

    // The first request creates the namespace with its settings and answers
    // when the head serves the rows.
    assert_eq!(
        writer.post(WRITE_KEY, namespace, &create).await,
        (200, json!({"upserted": 3, "deleted": 0}))
    );
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let (store, path) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let manifest: ContentManifest = fyrr_reader::storage::read_manifest(
        store.as_ref(),
        &path,
        &head.content.as_ref().unwrap().manifest,
    )
    .await
    .unwrap();
    assert_eq!(manifest.attributes.len(), 2);
    assert!(
        manifest
            .envelope_ids
            .iter()
            .all(|id| !head.rejected_envelope_ids.contains(id))
    );
    let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    assert_eq!(
        dataset
            .count_rows(Some("price = 12.5 AND stock = 3".into()))
            .await
            .unwrap(),
        1,
        "values are in their typed columns"
    );

    // A row names its readers; a row that names none is read by nobody.
    assert_eq!(hits(&config, "blue shirt"), ["a1"]);
    assert!(!hits(&config, "red hat").contains(&"a2".to_owned()));
    assert!(!hits(&config, "green coat").contains(&"a3".to_owned()));
    let fetched =
        body(Mcp::open(&config).call("fetch", json!({"source_id":"_api","root_id":"a1"})));
    assert!(fetched.to_string().contains("blue shirt"));

    // The namespace's settings and schema are fixed; a row refers to what it declared.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": {"dense_rq_bits": 3}})
            )
            .await,
        (409, json!({"error": "namespace_settings_fixed"}))
    );
    for (row, code) in [
        (
            json!({"id": "x", "text": "red hat", "colour": "red"}),
            "attribute_undeclared",
        ),
        (
            json!({"id": "x", "text": "red hat", "price": "cheap"}),
            "attribute_type_mismatch",
        ),
        (json!({"id": "x"}), "text_missing"),
        (json!({"text": "red hat"}), "id_missing"),
    ] {
        assert_eq!(
            writer
                .post(WRITE_KEY, namespace, &json!({"upserts": [row]}))
                .await,
            (400, json!({"error": code}))
        );
    }
    let oversized = json!({"upserts": [{"id": "x", "text": "a".repeat(16385)}]});
    assert_eq!(writer.post(WRITE_KEY, namespace, &oversized).await.0, 413);

    // An upsert replaces readers and text; a delete removes the document.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"upserts": [{"id": "a2", "text": "red hat", "readers": ["grp:allow"]}], "deletes": ["a1"]})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 1}))
    );
    assert_eq!(hits(&config, "red hat"), ["a2"]);
    assert!(!hits(&config, "blue shirt").contains(&"a1".to_owned()));
    let events = std::fs::read_to_string(root.join("service.stderr")).unwrap();
    assert!(!events.contains("blue shirt"), "no text in the log");
}

/// A vector of `dimensions` numbers that no other `n` shares.
fn vector(n: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|d| (((n + 3000) * (d + 5)) as f32).sin())
        .collect()
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_stores_the_vectors_a_caller_supplies_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    let writer = Writer::start(&config, &root, address).await;
    let namespace = "dense-fixture";
    let rows = json!({"upserts": [
        {"id": "a", "text": "alpha", "vector": vector(0, 1024)},
        {"id": "b", "text": "bravo", "vector": vector(1, 1024)},
        {"id": "c", "text": "charlie", "vector": vector(2, 1024)},
    ]});
    // A row that carries its vector makes no embedding call, though a provider is configured.
    assert_eq!(
        writer.post(WRITE_KEY, namespace, &rows).await,
        (200, json!({"upserted": 3, "deleted": 0}))
    );
    assert_eq!(root.embedding.calls(), 0);

    // The stored vector is the caller's: a query that embeds to it finds its row first.
    root.embedding.insert_both("vector query", vector(1, 1024));
    assert_eq!(hits(&config, "vector query")[0], "b");

    // A vector of another dimension, a number that is not a finite float32, a
    // zero vector and a value that is no list are refused with a named error.
    let mut short = vector(3, 1024);
    short.pop();
    for bad in [
        json!(short),
        json!(vec![0.0; 1024]),
        json!(["x"]),
        json!("x"),
        json!(
            std::iter::once(1e39)
                .chain(std::iter::repeat_n(1.0, 1023))
                .collect::<Vec<_>>()
        ),
    ] {
        assert_eq!(
            writer
                .post(
                    WRITE_KEY,
                    namespace,
                    &json!({"upserts": [{"id": "x", "text": "alpha", "vector": bad}]})
                )
                .await,
            (400, json!({"error": "vector_invalid"}))
        );
    }
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_without_embedding_settings_takes_only_vectors_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let text = std::fs::read_to_string(&config).unwrap();
    let (from, to) = (
        text.find("[embedding]").unwrap(),
        text.find("[identity]").unwrap(),
    );
    std::fs::write(&config, format!("{}{}", &text[..from], &text[to..])).unwrap();
    let address = free_address();
    configure(&config, &address, "open = true");
    let writer = Writer::start(&config, &root, address).await;
    let namespace = "dense-fixture";

    // The creating request declares the dimension of the namespace's vectors. A
    // fingerprint it sends must be that of what it declares.
    let row = json!({"id": "a", "text": "alpha", "vector": vector(0, 16)});
    let wrong = format!("sha256:{}", "0".repeat(64));
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": {"embedding": {"dimensions": 16, "fingerprint": wrong}}, "upserts": [row]})
            )
            .await,
        (400, json!({"error": "settings_invalid"}))
    );
    let fingerprint = fyrr_core::envelope::EmbeddingSettings {
        api_style: fyrr_core::envelope::EmbeddingApiStyle::OpenaiCompatible,
        prefix: String::new(),
        roles: Default::default(),
        send_dimensions: false,
    }
    .fingerprint("BAAI/bge-m3", 16);
    let create = json!({
        "settings": {"embedding": {"dimensions": 16, "fingerprint": fingerprint}},
        "upserts": [
            {"id": "a", "text": "alpha", "vector": vector(0, 16)},
            {"id": "b", "text": "bravo", "vector": vector(1, 16)},
        ],
    });
    assert_eq!(
        writer.post(WRITE_KEY, namespace, &create).await,
        (200, json!({"upserted": 2, "deleted": 0}))
    );
    let fetched = body(Mcp::open(&config).call("fetch", json!({"source_id":"_api","root_id":"b"})));
    assert!(fetched.to_string().contains("bravo"));
    // The settings are fixed, a fingerprint that differs from the namespace's included.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": {"embedding": {"fingerprint": wrong}}, "upserts": [row]})
            )
            .await,
        (409, json!({"error": "namespace_settings_fixed"}))
    );

    // Nothing embeds the text of a row without a vector, and the dimension is the namespace's.
    for (row, code) in [
        (
            json!({"id": "x", "text": "alpha"}),
            "embedding_not_configured",
        ),
        (
            json!({"id": "x", "text": "alpha", "vector": vector(2, 1024)}),
            "vector_invalid",
        ),
    ] {
        assert_eq!(
            writer
                .post(WRITE_KEY, namespace, &json!({"upserts": [row]}))
                .await,
            (400, json!({"error": code}))
        );
    }
}

#[tokio::test]
#[ignore = "real writer executable on local storage; scripts/ci"]
async fn identity_write_api_refuses_whole_and_keeps_connector_namespaces_apart_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    // A namespace fed by connectors takes no API write, whichever process asks.
    success(&config, &["writer", "namespace", "create"]);
    let combined = std::fs::read_to_string(&config).unwrap()
        + "\n[namespace.write_api]\n[[identity.api_keys]]\nname='k'\nkey={env='FYRR_TEST_WRITE_KEY'}\nroles=['write']\n";
    let both = root.join("both.toml");
    std::fs::write(&both, combined).unwrap();
    let check = run(&both, &["config", "check", "--no-env"]);
    assert!(!check.status.success(), "{}", report(&check));
    assert!(report(&check).contains("namespace.write_api"));

    let address = free_address();
    configure(&config, &address, "");
    let writer = Writer::start(&config, &root, address).await;
    embed(&root, &["blue shirt"]);
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"upserts": [{"id": "a", "text": "blue shirt"}]})
            )
            .await,
        (409, json!({"error": "namespace_not_writable"}))
    );
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_open_namespace_is_read_by_every_subject_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    embed(&root, &["blue shirt", "red hat"]);
    let writer = Writer::start(&config, &root, address).await;
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"upserts": [{"id": "a", "text": "blue shirt"}, {"id": "b", "text": "red hat"}]})
            )
            .await,
        (200, json!({"upserted": 2, "deleted": 0}))
    );
    let mut found = hits(&config, "blue shirt");
    found.sort();
    assert_eq!(found, ["a", "b"]);
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
            json!({"id": ["a", "b", "c", "d", "e", "f"][n], "text": text, "vector": vector(n, 1024), "readers": ["grp:allow"]})
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

/// The peak resident memory of a process, in bytes.
fn peak_memory(pid: u32) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|rest| rest.trim().strip_suffix("kB"))
        .map(|kb| kb.trim().parse::<u64>().unwrap() * 1024)
        .expect("VmHWM")
}

#[tokio::test]
#[ignore = "real writer executable on local storage, 240 MiB body; scripts/ci"]
#[allow(clippy::print_stderr)]
async fn identity_write_api_largest_body_stays_under_the_worker_memory_limit_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["same text", "small text"]);
    let writer = Writer::start(&config, &root, address).await;
    let loaded = fyrr_core::config::load(&config).unwrap();
    let limit = loaded.config.writer.worker_memory_max_bytes.as_u64();

    // The most rows a request may carry, each short with 100 attributes: a body
    // of 240 MiB, under `envelope_max_bytes`.
    let rows = loaded.config.writer.envelope_max_rows;
    let attributes: serde_json::Map<String, Value> = (0..100)
        .map(|n| (format!("a{n:03}"), json!("string")))
        .collect();
    let value = "x".repeat(40);
    // The namespace exists, with its schema and its indexes, before the large
    // body: the indexes of a first publication are built apart from the body.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": {"attributes": attributes}, "upserts": [{"id": "first", "text": "small text"}]})
            )
            .await
            .0,
        200
    );
    let mut body = String::from("{\"upserts\":[");
    for n in 0..rows {
        if n > 0 {
            body.push(',');
        }
        body.push_str(&format!("{{\"id\":\"r{n}\",\"text\":\"same text\""));
        for a in 0..100 {
            body.push_str(&format!(",\"a{a:03}\":\"{value}\""));
        }
        body.push('}');
    }
    body.push_str("]}");
    assert!(body.len() as u64 <= loaded.config.writer.envelope_max_bytes.as_u64());
    eprintln!("body {} MiB", body.len() >> 20);

    // The peak from the first byte of the body to its envelope being durable:
    // the part of a request that the body's size decides. The publication that
    // follows (indexes of the rows) is the committer's, as for any connector.
    let envelopes =
        std::path::PathBuf::from(loaded.config.storage.uri.strip_prefix("file://").unwrap())
            .join("namespaces/dense-fixture/envelopes");
    let count = move || std::fs::read_dir(&envelopes).map_or(0, Iterator::count);
    let before = count();
    let pid = writer.child.id();
    let prepared = tokio::spawn(async move {
        while count() <= before {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        peak_memory(pid)
    });
    let client = writer.client.clone();
    let url = format!("{}/v1/namespaces/dense-fixture/documents", writer.base);
    let large = tokio::spawn({
        let (client, url) = (client.clone(), url.clone());
        async move {
            client
                .post(url)
                .bearer_auth(secret(WRITE_KEY))
                .header("Content-Type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap()
        }
    });
    // Requests that arrive meanwhile wait for their turn and share a commit.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let small = writer
        .client
        .post(&url)
        .bearer_auth(secret(WRITE_KEY))
        .json(&json!({"upserts": [{"id": "s1", "text": "small text"}]}))
        .send()
        .await
        .unwrap();
    let large = large.await.unwrap();
    assert_eq!(large.status(), 200, "{}", large.text().await.unwrap());
    assert_eq!(small.status(), 200);
    let received = prepared.await.unwrap();
    let published = peak_memory(writer.child.id());
    eprintln!(
        "peak {} MiB until the envelope, {} MiB with its publication, of {} MiB",
        received >> 20,
        published >> 20,
        limit >> 20
    );
    // Kept with the fixture (`FYRR_CI_KEEP_FIXTURES=1`) for the record.
    std::fs::write(
        root.join("peak.txt"),
        format!("{received} bytes until the envelope, {published} with its publication\n"),
    )
    .unwrap();
    assert!(received < limit, "peak {received} bytes");
}
