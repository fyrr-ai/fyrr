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
/// May write and, with `admin`, set conditions or write by filter.
const ADMIN_KEY: usize = 44;
/// May write the namespaces whose names start with `shop-`.
const SCOPED_KEY: usize = 43;
/// May read the rows of `usr:x` and, with `admin`, measure recall.
const RECALL_KEY: usize = 45;

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
             [[identity.api_keys]]\nname='admin-writer'\nkey={{env='FYRR_TEST_ADMIN_KEY'}}\nroles=['write','admin']\n\
             [[identity.api_keys]]\nname='recall-admin'\nkey={{env='FYRR_TEST_RECALL_KEY'}}\nroles=['read','admin']\nreaders=['usr:x']\n\
             [[identity.api_keys]]\nname='scoped'\nkey={{env='FYRR_TEST_SCOPED_KEY'}}\nroles=['write']\nnamespace_prefix='shop-'\n\
             [writer]\nthreads=2\n[writer.commit]\ncoalescing_timer='10m'\n\
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
            .env("FYRR_TEST_ADMIN_KEY", secret(ADMIN_KEY))
            .env("FYRR_TEST_RECALL_KEY", secret(RECALL_KEY))
            .output()
            .unwrap();
        assert!(keys.status.success(), "{}", report(&keys));
        let child = worker_command(config)
            .args(["writer", "serve", "--jobs", "1"])
            .env("FYRR_TEST_WRITE_KEY", secret(WRITE_KEY))
            .env("FYRR_TEST_READ_KEY", secret(READ_KEY))
            .env("FYRR_TEST_SCOPED_KEY", secret(SCOPED_KEY))
            .env("FYRR_TEST_ADMIN_KEY", secret(ADMIN_KEY))
            .env("FYRR_TEST_RECALL_KEY", secret(RECALL_KEY))
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
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
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

    /// `PATCH /v1/namespaces/{namespace}` with `read_only`, repeated while the
    /// committer holds the namespace lease.
    async fn freeze(&self, key: usize, namespace: &str, read_only: bool) -> (u16, Value) {
        for _ in 0..100 {
            let response = self
                .client
                .patch(format!("{}/v1/namespaces/{namespace}", self.base))
                .bearer_auth(secret(key))
                .json(&json!({ "read_only": read_only }))
                .send()
                .await
                .unwrap();
            let status = response.status().as_u16();
            if status != 503 {
                return (status, response.json().await.unwrap_or(Value::Null));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the namespace lease stayed taken");
    }

    /// `DELETE /v1/namespaces/{namespace}` with the name repeated in the body,
    /// repeated while the committer holds the namespace lease.
    async fn delete(&self, key: usize, namespace: &str) -> (u16, Value) {
        for _ in 0..100 {
            let response = self
                .client
                .delete(format!("{}/v1/namespaces/{namespace}", self.base))
                .bearer_auth(secret(key))
                .json(&json!({ "namespace": namespace }))
                .send()
                .await
                .unwrap();
            let status = response.status().as_u16();
            if status != 503 {
                return (status, response.json().await.unwrap_or(Value::Null));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the namespace lease stayed taken");
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
    assert_eq!(manifest.settings.namespace.attributes.len(), 2);
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

    // The namespace's settings are fixed; a row that names an attribute that
    // is not declared and has no value to infer a type from (`null`) is refused.
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
            json!({"id": "x", "text": "red hat", "colour": null}),
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

/// The stored rows of the documents that pass `filter`, from the dataset the
/// head serves.
async fn stored(config: &Path, filter: &str) -> arrow_array::RecordBatch {
    let loaded = fyrr_core::config::load(config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    dataset
        .scan()
        .filter(&format!("source_id = '_api' AND ({filter})"))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap()
}

/// A patch writes the attributes it names and leaves the document as stored:
/// the text, the vector (nothing is embedded) and the readers. It shares its
/// request with upserts and deletes, and never creates a document.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_patches_a_document_and_keeps_what_it_does_not_name_on_storage() {
    use arrow_array::Array;
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["blue shirt", "red hat", "green coat"]);
    let writer = Writer::start(&config, &root, address).await;
    let post = async |body: Value| writer.post(WRITE_KEY, "dense-fixture", &body).await;
    assert_eq!(
        post(json!({
            "settings": {"attributes": {"price": "float", "stock": "int", "count": "uint",
                "uid": "uuid", "blob": "bytes", "tags": {"array": "string"}}},
            "upserts": [
                {"id": "a1", "text": "blue shirt", "price": 12.5, "stock": 3, "readers": ["grp:allow"]},
                {"id": "a2", "text": "red hat", "price": 4.0, "readers": ["usr:nobody"]},
            ],
        }))
        .await,
        (200, json!({"upserted": 2, "deleted": 0}))
    );
    let vector_before = stored(&config, "root_id = 'a1'").await["vector"].to_data();
    let calls = root.embedding.calls();

    // A named attribute changes; the other attribute, the text, the vector and
    // the readers stay. A patch of a document that is not there is skipped.
    assert_eq!(
        post(json!({"patches": [{"id": "a1", "price": 9.0}, {"id": "zz", "price": 1.0}]})).await,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 1, "skipped": 1})
        )
    );
    assert_eq!(root.embedding.calls(), calls, "a patch embeds nothing");
    let one = stored(&config, "root_id = 'a1'").await;
    assert_eq!(one.num_rows(), 1);
    assert_eq!(one["vector"].to_data(), vector_before);
    assert_eq!(
        stored(
            &config,
            "root_id = 'a1' AND price = 9.0 AND stock = 3 AND searchable_text = 'blue shirt'"
        )
        .await
        .num_rows(),
        1
    );
    assert_eq!(stored(&config, "root_id = 'zz'").await.num_rows(), 0);
    assert_eq!(hits(&config, "blue shirt"), ["a1"], "the readers stayed");

    // The attributes of every type are written, with the value of its column's type.
    assert_eq!(
        post(
            json!({"patches": [{"id": "a1", "count": 7, "blob": "aGk=", "tags": ["x", "y"],
            "uid": "123e4567-e89b-12d3-a456-426614174000"}]})
        )
        .await
        .0,
        200
    );
    assert_eq!(
        stored(
            &config,
            "root_id = 'a1' AND count = 7 AND price = 9.0 AND blob IS NOT NULL \
             AND uid IS NOT NULL AND array_length(tags) = 2"
        )
        .await
        .num_rows(),
        1
    );

    // `null` clears an attribute; readers named by a patch replace the stored ones.
    assert_eq!(
        post(json!({"patches": [{"id": "a1", "stock": null, "readers": ["usr:nobody"]}]})).await,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 1, "skipped": 0})
        )
    );
    assert_eq!(
        stored(&config, "root_id = 'a1' AND price = 9.0 AND stock IS NULL")
            .await
            .num_rows(),
        1
    );
    assert!(!hits(&config, "blue shirt").contains(&"a1".to_owned()));

    // One request holds upserts, patches and deletes, all or none.
    assert_eq!(
        post(json!({
            "upserts": [{"id": "a3", "text": "green coat", "readers": ["grp:allow"]}],
            "patches": [{"id": "a1", "price": 1.0}, {"id": "a2", "price": 5.5}],
            "deletes": ["a2"],
        }))
        .await,
        (400, json!({"error": "duplicate_id"}))
    );
    assert_eq!(
        post(json!({
            "upserts": [{"id": "a3", "text": "green coat", "readers": ["grp:allow"]}],
            "patches": [{"id": "a1", "price": 1.0}],
            "deletes": ["a2"],
        }))
        .await,
        (
            200,
            json!({"upserted": 1, "deleted": 1, "patched": 1, "skipped": 0})
        )
    );
    assert_eq!(
        stored(&config, "root_id = 'a1' AND price = 1.0")
            .await
            .num_rows(),
        1
    );
    assert_eq!(stored(&config, "root_id = 'a3'").await.num_rows(), 1);
    assert_eq!(stored(&config, "root_id = 'a2'").await.num_rows(), 0);
    assert_eq!(
        post(json!({"upserts": [{"id": "x"}], "patches": [{"id": "a1", "price": 7.0}]})).await,
        (400, json!({"error": "text_missing"})),
        "a refused request patches nothing"
    );
    assert_eq!(
        stored(&config, "root_id = 'a1' AND price = 1.0")
            .await
            .num_rows(),
        1
    );

    // A deleted document is not created again by a patch.
    assert_eq!(
        post(json!({"patches": [{"id": "a2", "price": 2.0}]})).await,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 0, "skipped": 1})
        )
    );
    assert_eq!(stored(&config, "root_id = 'a2'").await.num_rows(), 0);

    // Patches of a namespace that holds no document are answered at once, all
    // skipped, and leave nothing waiting for the committer.
    let empty = tokio::time::timeout(
        Duration::from_secs(60),
        writer.post(
            WRITE_KEY,
            "shop-empty",
            &json!({"settings": {"attributes": {"price": "float"}}, "patches": [{"id": "x", "price": 1.0}]}),
        ),
    )
    .await
    .expect("answered without waiting for a publication");
    assert_eq!(
        empty,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 0, "skipped": 1})
        )
    );

    // The vector is never patched, nor the text that a provider embeds.
    for (patch, code) in [
        (
            json!({"id": "a1", "vector": vector(5, 1024)}),
            "vector_not_patchable",
        ),
        (json!({"id": "a1", "text": "red hat"}), "text_not_patchable"),
        (json!({"id": "a1", "colour": null}), "attribute_undeclared"),
        (
            json!({"id": "a1", "price": "cheap"}),
            "attribute_type_mismatch",
        ),
        (json!({"id": "a1", "readers": "grp:x"}), "readers_invalid"),
        (json!({"id": "a1", "readers": ["bob"]}), "document_invalid"),
        (
            json!({"id": "a1", "text": "red\0hat", "vector": vector(5, 1024)}),
            "document_invalid",
        ),
        (json!({"price": 1.0}), "id_missing"),
    ] {
        assert_eq!(
            post(json!({"patches": [patch]})).await,
            (400, json!({"error": code}))
        );
    }
}

/// A request deletes and patches every document that matches a filter, in the
/// namespace it writes only and whatever the readers of the documents. The
/// deletes come first, so one request replaces the documents of a team, and the
/// documents the request names are written as it says.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_deletes_and_patches_the_documents_that_match_a_filter_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(
        &root,
        &[
            "blue shirt",
            "red hat",
            "green coat",
            "black scarf",
            "white sock",
            "grey vest",
        ],
    );
    let writer = Writer::start(&config, &root, address).await;
    let post = async |body: Value| writer.post(ADMIN_KEY, "dense-fixture", &body).await;
    let team = |name: &str| json!({"field": "team", "op": "eq", "value": name});
    assert_eq!(
        post(json!({
            "settings": {"attributes": {"team": "string", "price": "float"}},
            "upserts": [
                {"id": "a1", "text": "blue shirt", "team": "t1", "price": 1.0, "readers": ["grp:allow"]},
                {"id": "a2", "text": "red hat", "team": "t1", "price": 2.0, "readers": ["usr:nobody"]},
                {"id": "a3", "text": "green coat", "team": "t1", "price": 3.0, "readers": ["grp:allow"]},
                {"id": "b1", "text": "black scarf", "team": "t2", "price": 4.0, "readers": ["grp:allow"]},
            ],
        }))
        .await,
        (200, json!({"upserted": 4, "deleted": 0}))
    );
    // Another namespace holds a document of the same team.
    let other = "shop-other";
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                other,
                &json!({"settings": {"attributes": {"team": "string"}},
                    "upserts": [{"id": "a2", "text": "red hat", "team": "t1", "readers": ["grp:allow"]}]})
            )
            .await
            .0,
        200
    );

    // The team is replaced: its documents go, whoever may read them, and the
    // rows of the request are written after the deletes, a1 as it says.
    assert_eq!(
        post(json!({
            "delete_where": team("t1"),
            "upserts": [
                {"id": "a1", "text": "white sock", "team": "t1", "price": 1.5, "readers": ["grp:allow"]},
                {"id": "a4", "text": "grey vest", "team": "t1", "price": 6.0, "readers": ["grp:allow"]},
            ],
        }))
        .await,
        (200, json!({"upserted": 2, "deleted": 0}))
    );
    assert_eq!(
        stored(&config, "root_id IN ('a2','a3')").await.num_rows(),
        0
    );
    assert_eq!(
        stored(
            &config,
            "root_id = 'a1' AND searchable_text = 'white sock' AND price = 1.5"
        )
        .await
        .num_rows(),
        1
    );
    assert_eq!(
        stored(&config, "root_id IN ('a4','b1')").await.num_rows(),
        2
    );
    let own = root.join(format!("{other}.toml"));
    std::fs::write(
        &own,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("name = \"dense-fixture\"", &format!("name = \"{other}\"")),
    )
    .unwrap();
    assert_eq!(
        stored(&own, "root_id = 'a2'").await.num_rows(),
        1,
        "another namespace is not matched"
    );

    // A patch by filter writes the attributes and keeps the rest as stored,
    // the vector included, and embeds nothing.
    let vector_before = stored(&config, "root_id = 'b1'").await["vector"].to_data();
    let calls = root.embedding.calls();
    assert_eq!(
        post(json!({"patch_where": {"filter": team("t2"), "set": {"price": 9.5, "readers": ["usr:nobody"]}}}))
            .await,
        (200, json!({"upserted": 0, "deleted": 0}))
    );
    assert_eq!(root.embedding.calls(), calls);
    let b1 = stored(
        &config,
        "root_id = 'b1' AND price = 9.5 AND searchable_text = 'black scarf'",
    )
    .await;
    assert_eq!(b1.num_rows(), 1);
    assert_eq!(b1["vector"].to_data(), vector_before);
    assert!(
        !hits(&config, "black scarf").contains(&"b1".to_owned()),
        "its readers changed"
    );
    assert_eq!(
        stored(&config, "root_id = 'a1' AND price = 1.5")
            .await
            .num_rows(),
        1,
        "a document outside the filter stayed"
    );

    // Deletes by filter come first: a patch by id of a document they delete
    // finds it deleted. A patch by id over a patch by filter wins.
    assert_eq!(
        post(json!({
            "delete_where": team("t1"),
            "patch_where": {"filter": team("t2"), "set": {"price": 7.0}},
            "patches": [{"id": "a1", "price": 5.0}, {"id": "b1", "price": 8.0}],
        }))
        .await,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 1, "skipped": 1})
        )
    );
    assert_eq!(
        stored(&config, "root_id IN ('a1','a4')").await.num_rows(),
        0
    );
    assert_eq!(
        stored(&config, "root_id = 'b1' AND price = 8.0")
            .await
            .num_rows(),
        1
    );

    // A change of the request whose condition is not met writes nothing and
    // does not hold the filters back: the filter patch writes its readers, and
    // the filter delete deletes a document that a conditional upsert would replace.
    let never = json!({"field": "price", "op": "gt", "value": 100});
    assert_eq!(
        post(
            json!({"upserts": [{"id": "c1", "text": "grey vest", "team": "t3", "price": 1.0,
            "readers": ["grp:allow"]}]})
        )
        .await
        .0,
        200
    );
    assert!(!hits(&config, "black scarf").contains(&"b1".to_owned()));
    assert_eq!(
        post(json!({
            "patch_where": {"filter": team("t2"), "set": {"readers": ["grp:allow"]}},
            "patches": [{"id": "b1", "price": 3.0}],
            "delete_where": team("t3"),
            "upserts": [{"id": "c1", "text": "grey vest", "team": "t3", "price": 2.0,
                "readers": ["grp:allow"]}],
            "conditions": {"b1": never, "c1": never},
        }))
        .await
        .0,
        200
    );
    assert!(
        hits(&config, "black scarf").contains(&"b1".to_owned()),
        "its readers changed"
    );
    assert_eq!(
        stored(&config, "root_id = 'b1' AND price = 8.0")
            .await
            .num_rows(),
        1,
        "the patch of its price was skipped"
    );
    assert_eq!(stored(&config, "root_id = 'c1'").await.num_rows(), 0);

    // A key without the `admin` role learns nothing from a filter: it is refused
    // whatever the filter matches, and nothing changes.
    for body in [
        json!({"delete_where": team("t2")}),
        json!({"patch_where": {"filter": team("t2"), "set": {"price": 1.0}}}),
    ] {
        assert_eq!(
            writer.post(WRITE_KEY, "dense-fixture", &body).await,
            (403, json!({"error": "forbidden"}))
        );
    }
    assert_eq!(
        stored(&config, "root_id = 'b1' AND price = 8.0")
            .await
            .num_rows(),
        1
    );

    // A filter that matches nothing changes nothing.
    assert_eq!(post(json!({"delete_where": team("none")})).await.0, 200);
    assert_eq!(stored(&config, "root_id = 'b1'").await.num_rows(), 1);

    // A filter that is not valid for the namespace, a patch of a text, or of
    // an attribute it does not declare, is refused whole.
    for (body, code) in [
        (
            json!({"delete_where": {"field": "colour", "op": "eq", "value": "red"}}),
            "filter_invalid",
        ),
        (
            json!({"delete_where": {"field": "team", "op": "eq", "value": 5}}),
            "filter_invalid",
        ),
        (
            json!({"patch_where": {"filter": team("t2"), "set": {"text": "red hat"}}}),
            "text_not_patchable",
        ),
        (
            json!({"patch_where": {"filter": team("t2"), "set": {"colour": "red"}}}),
            "attribute_undeclared",
        ),
    ] {
        assert_eq!(post(body).await, (400, json!({"error": code})));
    }
    assert_eq!(post(json!({"delete_where": {"and": []}})).await.0, 400);
    assert_eq!(stored(&config, "root_id = 'b1'").await.num_rows(), 1);

    // A namespace that holds nothing answers at once.
    let empty = tokio::time::timeout(
        Duration::from_secs(60),
        writer.post(
            ADMIN_KEY,
            "shop-empty",
            &json!({"settings": {"attributes": {"team": "string"}}, "delete_where": team("t1")}),
        ),
    )
    .await
    .expect("answered without waiting for a publication");
    assert_eq!(empty, (200, json!({"upserted": 0, "deleted": 0})));
}

/// A filter that matches more documents than `MAX_FILTER_WRITE_DOCUMENTS`
/// fails its request and changes nothing; one that matches exactly that many
/// is applied.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_refuses_a_filter_that_matches_too_many_documents_on_storage() {
    use fyrr_core::limits::write::MAX_FILTER_WRITE_DOCUMENTS as MAX;
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    let writer = Writer::start(&config, &root, address).await;
    let namespace = "dense-fixture";
    // Vectors of 16 numbers keep the thousands of rows small. The namespace is
    // created by a small request; the many documents come after its indexes.
    let row = |n: usize, team: &str| {
        json!({"id": format!("d{n}"), "text": format!("text {n}"), "team": team,
            "vector": vector(0, 16)})
    };
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": {"embedding": {"dimensions": 16}, "attributes": {"team": "string"}},
                    "upserts": [row(0, "keep")]})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    let mut rows: Vec<_> = (1..=MAX).map(|n| row(n, "big")).collect();
    rows.push(row(MAX + 1, "extra"));
    assert_eq!(
        writer
            .post(WRITE_KEY, namespace, &json!({"upserts": rows}))
            .await,
        (200, json!({"upserted": MAX + 1, "deleted": 0}))
    );
    let both = json!({"field": "team", "op": "in", "value": ["big", "extra"]});
    assert_eq!(
        writer
            .post(ADMIN_KEY, namespace, &json!({"delete_where": both}))
            .await,
        (409, json!({"error": "rejected"}))
    );
    assert_eq!(
        writer
            .post(
                ADMIN_KEY,
                namespace,
                &json!({"patch_where": {"filter": both, "set": {"team": "moved"}}})
            )
            .await,
        (409, json!({"error": "rejected"}))
    );
    assert_eq!(
        stored(&config, "team IN ('big', 'extra')").await.num_rows(),
        MAX + 1,
        "nothing changed"
    );
    let big = json!({"field": "team", "op": "eq", "value": "big"});
    assert_eq!(
        writer
            .post(ADMIN_KEY, namespace, &json!({"delete_where": big}))
            .await,
        (200, json!({"upserted": 0, "deleted": 0}))
    );
    assert_eq!(
        stored(&config, "team IN ('keep', 'extra')")
            .await
            .num_rows(),
        2
    );
    assert_eq!(stored(&config, "team = 'big'").await.num_rows(), 0);
}

/// A change with a condition applies only to a stored document that meets it,
/// and is skipped and counted otherwise; the other changes of its request
/// apply. A document that is not there meets the condition of an upsert only.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_applies_a_change_only_when_its_condition_holds_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["blue shirt", "red hat", "green coat"]);
    let writer = Writer::start(&config, &root, address).await;
    let post = async |body: Value| writer.post(ADMIN_KEY, "dense-fixture", &body).await;
    let newer = json!({"field": "modified", "op": "lt", "value": {"row": "modified"}});
    assert_eq!(
        post(json!({
            "settings": {"attributes": {"version": "int", "owner": "string", "modified": "datetime"}},
            "upserts": [
                {"id": "a1", "text": "blue shirt", "version": 1, "modified": "2026-01-01T00:00:00Z"},
                {"id": "a2", "text": "red hat", "version": 5},
            ],
        }))
        .await,
        (200, json!({"upserted": 2, "deleted": 0}))
    );

    // An upsert that writes only if the stored `modified` is older than its own.
    let upsert = |version: u32, modified: &str| {
        json!({
            "upserts": [{"id": "a1", "text": "blue shirt", "version": version, "modified": modified}],
            "conditions": {"a1": newer},
        })
    };
    assert_eq!(
        post(upsert(2, "2026-02-01T00:00:00Z")).await,
        (200, json!({"upserted": 1, "deleted": 0, "skipped": 0}))
    );
    assert_eq!(
        post(upsert(3, "2026-01-15T00:00:00Z")).await,
        (200, json!({"upserted": 0, "deleted": 0, "skipped": 1})),
        "an older write is skipped and counted"
    );
    assert_eq!(
        stored(&config, "root_id = 'a1' AND version = 2")
            .await
            .num_rows(),
        1
    );

    // No stored document: an upsert applies, a patch or a delete is skipped.
    assert_eq!(
        post(json!({
            "upserts": [{"id": "a3", "text": "green coat", "version": 1}],
            "patches": [{"id": "zz", "version": 9}],
            "deletes": ["yy"],
            "conditions": {
                "a3": {"field": "version", "op": "lt", "value": {"row": "version"}},
                "zz": {"field": "version", "op": "eq", "value": 1},
                "yy": {"field": "version", "op": "eq", "value": 1},
            },
        }))
        .await,
        (
            200,
            json!({"upserted": 1, "deleted": 0, "patched": 0, "skipped": 2})
        )
    );
    assert_eq!(stored(&config, "root_id = 'a3'").await.num_rows(), 1);
    assert_eq!(
        stored(&config, "root_id IN ('zz', 'yy')").await.num_rows(),
        0
    );

    // A patch that sets an owner only while there is none; the request's other
    // changes apply whatever happens to it.
    let claim = |owner: &str| {
        json!({
            "patches": [{"id": "a2", "owner": owner}],
            "upserts": [{"id": "a1", "text": "blue shirt", "version": 4, "modified": "2026-01-01T00:00:00Z"}],
            "conditions": {
                "a2": {"field": "owner", "op": "eq", "value": null},
                "a1": newer,
            },
        })
    };
    assert_eq!(
        post(claim("ann")).await,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 1, "skipped": 1})
        )
    );
    assert_eq!(
        post(claim("bob")).await,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 0, "skipped": 2})
        )
    );
    assert_eq!(
        stored(&config, "root_id = 'a2' AND owner = 'ann' AND version = 5")
            .await
            .num_rows(),
        1
    );

    // A delete that waits for the version it saw.
    let delete = |version: u32| {
        json!({
            "deletes": ["a2"],
            "conditions": {"a2": {"field": "version", "op": "eq", "value": version}},
        })
    };
    assert_eq!(
        post(delete(4)).await,
        (200, json!({"upserted": 0, "deleted": 0, "skipped": 1}))
    );
    assert_eq!(stored(&config, "root_id = 'a2'").await.num_rows(), 1);
    assert_eq!(
        post(delete(5)).await,
        (200, json!({"upserted": 0, "deleted": 1, "skipped": 0}))
    );
    assert_eq!(stored(&config, "root_id = 'a2'").await.num_rows(), 0);
    assert_eq!(
        post(delete(5)).await,
        (200, json!({"upserted": 0, "deleted": 0, "skipped": 1})),
        "a deleted document is no document"
    );

    // A condition is checked as a filter is, and names a change of its request.
    for (body, code) in [
        (
            json!({"patches": [{"id": "a1", "version": 1}], "conditions": {"b9": newer}}),
            "condition_invalid",
        ),
        (
            json!({"patches": [{"id": "a1", "version": 1}], "conditions": {"a1": {"field": "colour", "op": "eq", "value": "red"}}}),
            "condition_invalid",
        ),
        (
            json!({"patches": [{"id": "a1", "version": 1}], "conditions": {"a1": {"field": "version", "op": "eq", "value": "one"}}}),
            "condition_invalid",
        ),
        (
            json!({"patches": [{"id": "a1", "version": 1}], "conditions": {"a1": {"field": "version", "op": "eq", "value": {"row": "colour"}}}}),
            "condition_invalid",
        ),
        (
            json!({"deletes": ["a1"], "conditions": {"a1": {"field": "version", "op": "eq", "value": {"row": "version"}}}}),
            "condition_invalid",
        ),
        (
            json!({"patches": [{"id": "a1", "version": 1}], "conditions": {"a1": {"all": []}}}),
            "condition_invalid",
        ),
    ] {
        assert_eq!(post(body).await, (400, json!({"error": code})));
    }

    // A condition tells whether a document holds a value, so a key that only
    // writes is refused: it may not read it.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"deletes": ["a1"], "conditions": {"a1": {"field": "version", "op": "eq", "value": 1}}})
            )
            .await,
        (403, json!({"error": "forbidden"}))
    );

    // Conditional deletes alone on a namespace that holds nothing are answered
    // at once, skipped.
    let empty = tokio::time::timeout(
        Duration::from_secs(60),
        writer.post(
            ADMIN_KEY,
            "shop-empty",
            &json!({
                "settings": {"attributes": {"version": "int"}},
                "deletes": ["x"],
                "conditions": {"x": {"field": "version", "op": "eq", "value": 1}},
            }),
        ),
    )
    .await
    .expect("answered without waiting for a publication");
    assert_eq!(
        empty,
        (200, json!({"upserted": 0, "deleted": 0, "skipped": 1}))
    );
}

/// Requests that wait unpublished, all prepared against the same stored
/// document, cannot all apply a change that only one of them may: the committer
/// tests each condition against what the requests before it left.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_lets_one_of_many_unpublished_requests_win_its_condition_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    embed(&root, &["blue shirt"]);
    let writer = Writer::start(&config, &root, address).await;
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({
                    "settings": {"attributes": {"owner": "string"}},
                    "upserts": [{"id": "d1", "text": "blue shirt"}],
                })
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    let loaded = fyrr_core::config::load(&config).unwrap();
    // The committer finds the lease taken and publishes nothing until it is
    // released, so the requests below are all prepared against the same document.
    let lease = loop {
        match fyrr_writer::lease::LeaseGuard::acquire(&loaded.config).await {
            Ok(lease) => break lease,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    };
    let owners = ["ann", "bob", "cy"];
    let release = async {
        loop {
            let head = fyrr_reader::storage::optional_head(&loaded.config)
                .await
                .unwrap()
                .unwrap();
            if fyrr_writer::commit::pending(&loaded.config, &head)
                .await
                .unwrap()
                .len()
                == owners.len()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // All three are durable and none is published.
        lease.release().await.unwrap();
    };
    let (replies, ()) = tokio::join!(
        futures::future::join_all(owners.iter().map(|owner| {
            let request = json!({
                "patches": [{"id": "d1", "owner": owner}],
                "conditions": {"d1": {"field": "owner", "op": "eq", "value": null}},
            });
            let writer = &writer;
            async move { writer.post(ADMIN_KEY, "dense-fixture", &request).await }
        })),
        release,
    );
    let applied: Vec<_> = owners
        .iter()
        .zip(&replies)
        .filter(|(_, reply)| {
            assert_eq!(reply.0, 200, "{replies:?}");
            reply.1["patched"] == 1
        })
        .map(|(owner, _)| *owner)
        .collect();
    assert_eq!(applied.len(), 1, "exactly one applies: {replies:?}");
    for reply in &replies {
        let patched = reply.1["patched"].as_u64().unwrap();
        assert_eq!(
            reply.1,
            json!({"upserted": 0, "deleted": 0, "patched": patched, "skipped": 1 - patched})
        );
    }
    assert_eq!(
        stored(
            &config,
            &format!("root_id = 'd1' AND owner = '{}'", applied[0])
        )
        .await
        .num_rows(),
        1
    );
}

/// A frozen namespace refuses writes and stays readable. A request that was
/// accepted before the switch, with its envelope durable but not published, is
/// committed after it; a request that comes after it is refused.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_freezes_a_namespace_and_commits_what_it_accepted_before_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    embed(&root, &["blue shirt", "red hat", "green coat"]);
    let writer = Writer::start(&config, &root, address).await;
    let namespace = "dense-fixture";
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"upserts": [{"id": "d1", "text": "blue shirt"}]})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    // Only a key with the `admin` role switches it, and only for a namespace it names.
    let forbidden = (403, json!({"error": "forbidden"}));
    let not_found = (404, json!({"error": "not_found"}));
    assert_eq!(writer.freeze(WRITE_KEY, namespace, true).await, forbidden);
    assert_eq!(writer.freeze(READ_KEY, namespace, true).await, forbidden);
    assert_eq!(
        writer.freeze(ADMIN_KEY, "Not_a_name", true).await,
        not_found
    );
    assert_eq!(
        writer.freeze(ADMIN_KEY, "no-such-one", true).await,
        not_found
    );

    // The committer finds the lease taken and publishes nothing until it is
    // released: the request below is accepted, its envelope durable, and the
    // switch is published under the lease before the envelope is.
    let loaded = fyrr_core::config::load(&config).unwrap();
    let lease = loop {
        match fyrr_writer::lease::LeaseGuard::acquire(&loaded.config).await {
            Ok(lease) => break lease,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    };
    let switch = async {
        loop {
            let head = fyrr_reader::storage::optional_head(&loaded.config)
                .await
                .unwrap()
                .unwrap();
            if !fyrr_writer::commit::pending(&loaded.config, &head)
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            fyrr_writer::commit::set_read_only_held(&loaded.config, &lease, true)
                .await
                .unwrap()
        );
        lease.release().await.unwrap();
    };
    let request = json!({"upserts": [{"id": "d2", "text": "red hat"}]});
    let (accepted, ()) = tokio::join!(writer.post(WRITE_KEY, namespace, &request), switch);
    assert_eq!(accepted, (200, json!({"upserted": 1, "deleted": 0})));
    assert_eq!(stored(&config, "root_id = 'd2'").await.num_rows(), 1);

    // From now on the namespace is frozen: a write, with or without settings,
    // is refused with one error, and nothing is written for it.
    let refused = (409, json!({"error": "namespace_read_only"}));
    for request in [
        json!({"upserts": [{"id": "d3", "text": "green coat"}]}),
        json!({"deletes": ["d1"]}),
        json!({"patches": [{"id": "d1", "price": 1}]}),
        json!({"settings": {}}),
        json!({"settings": {"attributes": {"price": "float"}}}),
    ] {
        assert_eq!(writer.post(WRITE_KEY, namespace, &request).await, refused);
    }
    assert_eq!(stored(&config, "root_id = 'd3'").await.num_rows(), 0);
    assert_eq!(stored(&config, "root_id = 'd1'").await.num_rows(), 1);
    // It stays readable.
    assert_eq!(hits(&config, "red hat")[0], "d2");
    let policy = {
        let (store, namespace_root) =
            fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
        let head = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        fyrr_reader::storage::artifact::<fyrr_core::publication::PolicyArtifact>(
            store.as_ref(),
            &namespace_root,
            &head.policy,
        )
        .await
        .unwrap()
    };
    assert!(policy.read_only);
    assert_eq!(policy.schema_version.to_string(), "5.0");

    // The switch is repeatable and reversible, by the key with `admin`.
    assert_eq!(
        writer.freeze(ADMIN_KEY, namespace, true).await,
        (
            200,
            json!({"namespace": namespace, "read_only": true, "changed": false})
        )
    );
    assert_eq!(
        writer.freeze(ADMIN_KEY, namespace, false).await,
        (
            200,
            json!({"namespace": namespace, "read_only": false, "changed": true})
        )
    );
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"upserts": [{"id": "d3", "text": "green coat"}]})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    assert_eq!(stored(&config, "root_id = 'd3'").await.num_rows(), 1);
}

/// A namespace written through the API is deleted by the name of its path,
/// and only that one: the others, the configured one included, keep their data.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_deletes_the_namespace_of_the_path_only_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    embed(&root, &["blue shirt", "red hat"]);
    let writer = Writer::start(&config, &root, address).await;
    let (kept, doomed) = ("dense-fixture", "shop-doomed");
    for (name, text) in [(kept, "blue shirt"), (doomed, "red hat")] {
        assert_eq!(
            writer
                .post(
                    WRITE_KEY,
                    name,
                    &json!({"upserts": [{"id": "d1", "text": text}]})
                )
                .await,
            (200, json!({"upserted": 1, "deleted": 0}))
        );
    }
    let own = root.join(format!("{doomed}.toml"));
    std::fs::write(
        &own,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("name = \"dense-fixture\"", &format!("name = \"{doomed}\"")),
    )
    .unwrap();
    let published = async |config: &Path| {
        let loaded = fyrr_core::config::load(config).unwrap();
        fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .is_some()
    };
    assert!(published(&own).await && published(&config).await);

    let forbidden = (403, json!({"error": "forbidden"}));
    let not_found = (404, json!({"error": "not_found"}));
    assert_eq!(writer.delete(WRITE_KEY, doomed).await, forbidden);
    assert_eq!(writer.delete(ADMIN_KEY, "Not_a_name").await, not_found);
    assert!(published(&own).await && published(&config).await);

    assert_eq!(
        writer.delete(ADMIN_KEY, doomed).await,
        (200, json!({"deleted": true}))
    );
    assert!(!published(&own).await, "the named namespace is gone");
    assert!(published(&config).await, "the other one is not touched");
    assert_eq!(stored(&config, "root_id = 'd1'").await.num_rows(), 1);

    // The name is free again: a write creates a new namespace, with no trace of the old.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                doomed,
                &json!({"upserts": [{"id": "d2", "text": "blue shirt"}]})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    assert!(published(&own).await);
    assert_eq!(stored(&own, "root_id = 'd2'").await.num_rows(), 1);
    assert_eq!(stored(&own, "root_id = 'd1'").await.num_rows(), 0);
}

/// A patch that changes the text brings the vector of the new text, and a
/// process without an embedding provider cannot embed it for the patch.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_patches_the_text_with_its_vector_on_storage() {
    use arrow_array::Array;
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
    let fingerprint = fyrr_core::envelope::EmbeddingSettings {
        api_style: fyrr_core::envelope::EmbeddingApiStyle::OpenaiCompatible,
        prefix: String::new(),
        roles: Default::default(),
        send_dimensions: false,
    }
    .fingerprint("BAAI/bge-m3", 16);
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({
                    "settings": {"embedding": {"dimensions": 16, "fingerprint": fingerprint}},
                    "upserts": [{"id": "a", "text": "alpha", "vector": vector(0, 16)}],
                })
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    let vector_before = stored(&config, "root_id = 'a'").await["vector"].to_data();
    // A text without its vector would keep the embedding of the old text.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"patches": [{"id": "a", "text": "alpha two"}]})
            )
            .await,
        (400, json!({"error": "embedding_not_configured"}))
    );
    assert_eq!(
        stored(&config, "root_id = 'a' AND searchable_text = 'alpha'")
            .await
            .num_rows(),
        1
    );
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"patches": [{"id": "a", "text": "alpha two", "vector": vector(1, 16)}]})
            )
            .await,
        (
            200,
            json!({"upserted": 0, "deleted": 0, "patched": 1, "skipped": 0})
        )
    );
    let one = stored(&config, "root_id = 'a' AND searchable_text = 'alpha two'").await;
    assert_eq!(one.num_rows(), 1);
    assert_ne!(
        one["vector"].to_data(),
        vector_before,
        "the vector went with the text"
    );
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
    // A dimension the dense index cannot be built over is refused before the
    // namespace exists; the 16-dimension creation below is accepted.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": {"embedding": {"dimensions": 4}}, "upserts": [{"id": "a", "text": "alpha", "vector": vector(0, 4)}]})
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

/// A first publication of more rows than one index partition holds, all with the
/// same vector, publishes: index training does not fail on identical vectors.
#[tokio::test]
#[ignore = "real writer executable on local storage; scripts/ci"]
async fn identity_write_api_ends_a_first_publication_of_identical_vectors_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    let writer = Writer::start(&config, &root, address).await;
    let rows: Vec<Value> = (0..600)
        .map(
            |n| json!({"id": format!("r{n}"), "text": format!("row {n}"), "vector": vec![0.5; 16]}),
        )
        .collect();
    let answer = tokio::time::timeout(
        Duration::from_secs(180),
        writer.post(
            WRITE_KEY,
            "dense-fixture",
            &json!({"settings": {"embedding": {"dimensions": 16}}, "upserts": rows}),
        ),
    )
    .await;
    let stderr = std::fs::read_to_string(root.join("service.stderr")).unwrap();
    assert!(
        answer.is_ok(),
        "the publication did not end in 180s; writer stderr: {stderr}"
    );
    assert_eq!(
        answer.unwrap(),
        (200, json!({"upserted": 600, "deleted": 0}))
    );
}

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

/// Runs a writer command that needs the namespace's lease, which the service
/// holds while it publishes: tries again until it is free.
async fn run_when_free(config: &Path, args: &[&str]) -> Value {
    for _ in 0..40 {
        let output = run(config, args);
        if output.status.success() {
            return serde_json::from_slice(&output.stdout).expect("JSON output");
        }
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("committer_busy"),
            "{}",
            report(&output)
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("the namespace lease never came free");
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

/// A vector attribute that most rows leave null keeps the model of its first
/// build: Lance sizes a new model by all the rows of the table, and two writes
/// of 1,100 rows ask for 4 partitions, which 6 vectors cannot train. The
/// built-in vector, which every row has, gets its model of 4 partitions, and
/// `writer maintain` succeeds.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_maintains_a_sparse_vector_attribute_on_storage() {
    const ROWS: usize = 1_100;
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let writer = Writer::start(&config, &root, address).await;
    for first in [0, ROWS] {
        let mut upserts = Vec::new();
        for n in first..first + ROWS {
            let mut row = json!({
                "id": format!("r{n}"),
                "text": format!("row{n}"),
                "vector": vector(n, 1024),
                "readers": ["grp:allow"],
            });
            if n < first + 3 {
                row["small"] = json!(vector(30 + n, 16));
            }
            upserts.push(row);
        }
        let mut body = json!({"upserts": upserts});
        if first == 0 {
            body["settings"] = json!({"vectors": {"small": {"dimensions": 16}}});
        }
        assert_eq!(writer.post(WRITE_KEY, "dense-fixture", &body).await.0, 200);
    }
    let maintained = run_when_free(&config, &["writer", "maintain"]).await;
    let (_, manifest) = super::coordination::publication(&config).await;
    let partitions = |name: &str| -> Vec<_> {
        manifest
            .indexes
            .iter()
            .filter(|index| index.name == name)
            .map(|index| index.partitions)
            .collect()
    };
    assert_eq!(partitions("vector_idx"), [Some(4)], "{maintained}");
    let small = partitions(&fyrr_core::attributes::vector_index_name("small"));
    assert!(
        !small.is_empty() && small.iter().all(|p| *p == Some(1)),
        "{small:?}"
    );
    drop(writer);
    std::fs::remove_dir_all(root).unwrap();
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

/// A vector attribute that no row has a vector for has no index, and a search
/// of it finds nothing; the commit that brings the first vector builds the
/// index, and the rows after it are indexed with the others.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_builds_a_vector_index_when_the_first_vector_arrives_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "open = true");
    let writer = Writer::start(&config, &root, address).await;
    let settings = json!({"vectors": {"image": {"dimensions": 16}}});
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                "dense-fixture",
                &json!({"settings": settings, "upserts": [
                    {"id": "a", "text": "alpha", "vector": vector(0, 1024)},
                ]})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    let index_names = async || -> Vec<String> {
        let (_, manifest) = super::coordination::publication(&config).await;
        assert_eq!(manifest.schema_version.major, 6);
        manifest.indexes.iter().map(|i| i.name.clone()).collect()
    };
    assert!(!index_names().await.contains(&"vec_image_idx".to_owned()));
    // A new reader for each question: it serves the publication that is the latest.
    let query = |n: usize| {
        body(Mcp::open(&config).call(
            "search",
            json!({"vector": vector(n, 16), "vector_attribute": "image", "output": "metadata"}),
        ))["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert!(query(40).is_empty());
    let rows: Vec<Value> = (0..3)
        .map(|n| json!({"id": format!("r{n}"), "text": format!("row{n}"), "vector": vector(n + 1, 1024), "image": vector(40 + n, 16)}))
        .collect();
    assert_eq!(
        writer
            .post(WRITE_KEY, "dense-fixture", &json!({"upserts": rows}))
            .await,
        (200, json!({"upserted": 3, "deleted": 0}))
    );
    assert!(index_names().await.contains(&"vec_image_idx".to_owned()));
    assert_eq!(query(41)[0], "r1");
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

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_takes_current_settings_and_refuses_unknown_ones_on_storage() {
    let (config, root) = setup_on(Backend::Local);
    let address = free_address();
    configure(&config, &address, "");
    let writer = Writer::start(&config, &root, address).await;
    let namespace = "dense-fixture";
    let upsert = json!([{"id": "a", "text": "alpha beta", "vector": vector(0, 1024)}]);
    // The shape of an earlier build and a misspelled key are refused, never
    // dropped: nothing is created.
    for settings in [
        json!({"fts": {"with_position": true}}),
        json!({"fts_position": true}),
        // Inside a setting too, at every depth.
        json!({"write_api": {"text_atribute": "body"}}),
        json!({"retrieval": {"dense_weigth": 0.3}}),
        json!({"analyzer": {"max_token_lenght": 20}}),
        json!({"embedding": {"settings": {"roles": {"query": {"prefx": "q: "}}}}}),
        // An array is not a struct, though serde would read one as it.
        json!({"embedding": {"settings": {"roles": [{}, {"prefx": "q: "}]}}}),
        // A key that no setting has, whatever its value.
        json!({"typo": {}}),
        json!({"typo": null}),
    ] {
        assert_eq!(
            writer
                .post(
                    WRITE_KEY,
                    namespace,
                    &json!({"settings": settings, "upserts": upsert})
                )
                .await,
            (400, json!({"error": "settings_invalid"})),
            "{settings}"
        );
    }
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": {"fts_positions": true, "vectors": {"image": {"dimensions": 16, "multi": false}}}, "upserts": upsert})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    let (_, manifest) = super::coordination::publication(&config).await;
    assert!(manifest.settings.namespace.fts_positions);
    // An existing namespace refuses an unknown key as it refuses a changed setting.
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": {"fts": {"with_position": false}}, "upserts": upsert})
            )
            .await,
        (400, json!({"error": "settings_invalid"}))
    );
}
