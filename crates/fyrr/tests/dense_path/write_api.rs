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
pub(super) const WRITE_KEY: usize = 41;
const READ_KEY: usize = 42;
/// May write the namespaces whose names start with `shop-`.
const SCOPED_KEY: usize = 43;

/// A configuration without sources that writes through the API, with a key
/// that may write and one that may only read.
pub(super) fn configure(config: &Path, address: &std::net::SocketAddr, extra: &str) {
    let text = std::fs::read_to_string(config).unwrap();
    let base = &text[..text.find("[[sources]]").unwrap()];
    std::fs::write(
        config,
        format!(
            "{base}\n[namespace.write_api]\n{extra}\n\
             [[identity.api_keys]]\nname='writer'\nkey={{env='FYRR_TEST_WRITE_KEY'}}\nroles=['write']\n\
             [[identity.api_keys]]\nname='reader'\nkey={{env='FYRR_TEST_READ_KEY'}}\nreaders=['usr:x']\n\
             [[identity.api_keys]]\nname='scoped'\nkey={{env='FYRR_TEST_SCOPED_KEY'}}\nroles=['write']\nnamespace_prefix='shop-'\n\
             [writer.commit]\ncoalescing_timer='10m'\n\
             [writer.serve.admin]\nlisten='{address}'\noperator_token={{env='FYRR_TEST_OPERATOR'}}\n"
        ),
    )
    .unwrap();
}

pub(super) struct Writer {
    pub(super) child: std::process::Child,
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
    pub(super) async fn start(config: &Path, root: &Path, address: std::net::SocketAddr) -> Self {
        let keys = host_command(config)
            .args(["writer", "keys"])
            .env("FYRR_TEST_WRITE_KEY", secret(WRITE_KEY))
            .env("FYRR_TEST_READ_KEY", secret(READ_KEY))
            .env("FYRR_TEST_SCOPED_KEY", secret(SCOPED_KEY))
            .output()
            .unwrap();
        assert!(keys.status.success(), "{}", report(&keys));
        let child = worker_command(config)
            .args(["writer", "serve", "--jobs", "1"])
            .env("FYRR_TEST_WRITE_KEY", secret(WRITE_KEY))
            .env("FYRR_TEST_READ_KEY", secret(READ_KEY))
            .env("FYRR_TEST_SCOPED_KEY", secret(SCOPED_KEY))
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

    pub(super) async fn post(&self, key: usize, namespace: &str, body: &Value) -> (u16, Value) {
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
pub(super) fn embed(root: &FixtureRoot, texts: &[&str]) {
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
        writer.post(WRITE_KEY, "Not_a_name", &create).await.0,
        404,
        "a name that is no namespace name"
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

/// One process writes to many namespaces at once: each is created by its
/// first write, holds only its own documents, and is written only by a key that
/// may write it.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_writes_to_many_namespaces_from_one_process_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    let names = ["dense-fixture", "shop-a", "shop-b", "shop-c", "shop-d"];
    let texts = [
        "blue shirt",
        "red hat",
        "green coat",
        "black scarf",
        "white sock",
    ];
    embed(&root, &texts);
    let writer = Writer::start(&config, &root, address).await;

    // Requests for different namespaces wait for their publications together.
    let api = &writer;
    let replies = futures::future::join_all(names.iter().zip(texts).map(|(name, text)| {
        let id = format!("doc-{name}");
        let body = json!({"upserts": [{"id": id, "text": text}]});
        async move { api.post(WRITE_KEY, name, &body).await }
    }))
    .await;
    for reply in replies {
        assert_eq!(reply, (200, json!({"upserted": 1, "deleted": 0})));
    }

    // Every namespace serves its own document, and only that.
    let text = std::fs::read_to_string(&config).unwrap();
    for (name, text_of) in names.iter().zip(texts) {
        let own = root.join(format!("{name}.toml"));
        std::fs::write(
            &own,
            text.replace("name = \"dense-fixture\"", &format!("name = \"{name}\"")),
        )
        .unwrap();
        assert_eq!(hits(&own, text_of), [format!("doc-{name}")], "{name}");
        let head =
            fyrr_reader::storage::optional_head(&fyrr_core::config::load(&own).unwrap().config)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(head.namespace, *name);
        assert!(
            head.lease
                .is_none_or(|lease| lease.expires_at <= fyrr_core::clock::now()),
            "{name} holds no lease at rest"
        );
    }

    // A key limited to a prefix writes the namespaces under it, and a name
    // that is no namespace name is not one.
    let row = json!({"upserts": [{"id": "x", "text": "red hat"}]});
    assert_eq!(writer.post(SCOPED_KEY, "shop-e", &row).await.0, 200);
    assert_eq!(writer.post(SCOPED_KEY, "dense-fixture", &row).await.0, 403);
    assert_eq!(writer.post(WRITE_KEY, "Not_a_name", &row).await.0, 404);
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
                "id": id, "text": texts[n], "readers": ["grp:allow"],
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
