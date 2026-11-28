//! The document write API of the real writer executable: documents written
//! over HTTP are published whole, searched, fetched and deleted.
use super::coordination::{host_command, worker_command};
use super::tools::{Mcp, body};
use super::*;
use reqwest::Client;
use std::time::{Duration, Instant};

const OPERATOR: &str = "owned-fixture-operator-token-only";

pub(super) fn secret(n: usize) -> String {
    format!("fyrr_key_{n:064}")
}
pub(super) const WRITE_KEY: usize = 41;
pub(super) const READ_KEY: usize = 42;
/// May write and, with `admin`, set conditions or write by filter.
pub(super) const ADMIN_KEY: usize = 44;
/// May write the namespaces whose names start with `shop-`.
const SCOPED_KEY: usize = 43;
/// May read the rows of `usr:x` and, with `admin`, measure recall.
pub(super) const RECALL_KEY: usize = 45;

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

pub(super) fn hits(config: &Path, query: &str) -> Vec<String> {
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
pub(super) async fn stored(config: &Path, filter: &str) -> arrow_array::RecordBatch {
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

/// Runs a writer command that needs the namespace's lease, which the service
/// holds while it publishes: tries again until it is free.
pub(super) async fn run_when_free(config: &Path, args: &[&str]) -> Value {
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

/// A patch writes the attributes it names and leaves the document as stored:
/// the text, the vector (nothing is embedded) and the readers. It shares its
/// request with upserts and deletes, and never creates a document.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_patches_a_document_and_keeps_what_it_does_not_name_on_storage() {
    use arrow_array::Array;
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
pub(super) fn vector(n: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|d| (((n + 3000) * (d + 5)) as f32).sin())
        .collect()
}

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_stores_the_vectors_a_caller_supplies_on_storage() {
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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

/// A first publication of more rows than one index partition holds, all with the
/// same vector, publishes: index training does not fail on identical vectors.
#[tokio::test]
#[ignore = "real writer executable on local storage; scripts/ci"]
async fn identity_write_api_ends_a_first_publication_of_identical_vectors_on_storage() {
    let (config, root) = setup_sized(Backend::Local, 0);
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

#[tokio::test]
#[ignore = "real writer executable on local storage; scripts/ci"]
async fn identity_write_api_refuses_whole_and_keeps_connector_namespaces_apart_on_storage() {
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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
    let (config, root) = setup_sized(Backend::Local, 0);
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

/// A vector attribute that most rows leave null keeps the model of its first
/// build: Lance sizes a new model by all the rows of the table, and two writes
/// of 1,100 rows ask for 4 partitions, which 6 vectors cannot train. The
/// built-in vector, which every row has, gets its model of 4 partitions, and
/// `writer maintain` succeeds.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_maintains_a_sparse_vector_attribute_on_storage() {
    const ROWS: usize = 1_100;
    let (config, root) = setup_sized(Backend::Local, 0);
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

/// A vector attribute that no row has a vector for has no index, and a search
/// of it finds nothing; the commit that brings the first vector builds the
/// index, and the rows after it are indexed with the others.
#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_builds_a_vector_index_when_the_first_vector_arrives_on_storage() {
    let (config, root) = setup_sized(Backend::Local, 0);
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

#[tokio::test]
#[ignore = "real writer and reader executables on local storage; scripts/ci"]
async fn identity_write_api_takes_current_settings_and_refuses_unknown_ones_on_storage() {
    let (config, root) = setup_sized(Backend::Local, 0);
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
        // An array is no object, nor a stand-in for a `null` member.
        json!({"write_api": []}),
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

/// A `delete_where` over more documents than one write may delete is answered
/// 409: its envelope is listed as applied and as rejected, so it is rejected
/// and its receipt is unpublished, while its manifest is retained and after
/// maintenance deleted it. An envelope applied only keeps its receipt.
#[tokio::test]
#[ignore = "real writer on local storage; scripts/ci"]
async fn identity_write_api_a_rejected_write_has_no_receipt_across_retention_on_storage() {
    use super::coordination::publication;
    use fyrr_core::limits::write::MAX_FILTER_WRITE_DOCUMENTS as MAX;
    let (config, root) = setup_sized(Backend::Local, 0);
    let address = free_address();
    configure(&config, &address, "open = true");
    let writer = Writer::start(&config, &root, address).await;
    let namespace = "dense-fixture";
    let row = |n: usize, team: &str| {
        json!({"id": format!("d{n}"), "text": format!("text {n}"), "team": team,
            "vector": vector(0, 16)})
    };
    let settings = json!({"embedding": {"dimensions": 16}, "attributes": {"team": "string"}});
    assert_eq!(
        writer
            .post(
                WRITE_KEY,
                namespace,
                &json!({"settings": settings, "upserts": [row(0, "keep")]})
            )
            .await,
        (200, json!({"upserted": 1, "deleted": 0}))
    );
    let mut rows: Vec<_> = (1..=MAX).map(|n| row(n, "big")).collect();
    rows.push(row(MAX + 1, "extra"));
    let upserted = writer
        .post(WRITE_KEY, namespace, &json!({"upserts": rows}))
        .await;
    assert_eq!(upserted.0, 200);
    let (_, plain) = publication(&config).await;
    let plain_id = plain.envelope_ids[0].clone();
    let both = json!({"field": "team", "op": "in", "value": ["big", "extra"]});
    assert_eq!(
        writer
            .post(ADMIN_KEY, namespace, &json!({"delete_where": both}))
            .await,
        (409, json!({"error": "rejected"}))
    );
    let (head, manifest) = publication(&config).await;
    let rejected_id = head
        .rejected_envelope_ids
        .first()
        .or(manifest.rejected_envelope_ids.first())
        .cloned()
        .expect("a rejected envelope");
    // One more publication, so that the manifest above is not the served one.
    let kept = writer
        .post(WRITE_KEY, namespace, &json!({"upserts": [row(0, "kept")]}))
        .await;
    assert_eq!(kept.0, 200);
    drop(writer);
    let receipt = |id: &str| {
        let output = host_command(&config)
            .args(["writer", "commit", "--receipt", id])
            .env("FYRR_TEST_WRITE_KEY", secret(WRITE_KEY))
            .env("FYRR_TEST_READ_KEY", secret(READ_KEY))
            .env("FYRR_TEST_SCOPED_KEY", secret(SCOPED_KEY))
            .env("FYRR_TEST_ADMIN_KEY", secret(ADMIN_KEY))
            .env("FYRR_TEST_RECALL_KEY", secret(RECALL_KEY))
            .env("FYRR_TEST_OPERATOR", OPERATOR)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        match (
            output.status.success(),
            stderr.contains("receipt_unpublished"),
        ) {
            (true, _) => "published",
            (false, true) => "unpublished",
            (false, false) => panic!("writer commit --receipt {id}: {stderr}"),
        }
    };
    assert_eq!(
        (receipt(&rejected_id), receipt(&plain_id)),
        ("unpublished", "published"),
        "while the manifests are retained"
    );
    fyrr_core::clock::advance(Duration::from_secs(25 * 3600)).unwrap();
    for _ in 0..2 {
        let maintain = host_command(&config)
            .args(["writer", "maintain"])
            .env("FYRR_TEST_WRITE_KEY", secret(WRITE_KEY))
            .env("FYRR_TEST_READ_KEY", secret(READ_KEY))
            .env("FYRR_TEST_SCOPED_KEY", secret(SCOPED_KEY))
            .env("FYRR_TEST_ADMIN_KEY", secret(ADMIN_KEY))
            .env("FYRR_TEST_RECALL_KEY", secret(RECALL_KEY))
            .env("FYRR_TEST_OPERATOR", OPERATOR)
            .output()
            .unwrap();
        assert!(maintain.status.success());
    }
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    for id in [&manifest.publication_id, &plain.publication_id] {
        let path = ns.clone().join("publications").join(format!("{id}.json"));
        assert!(
            matches!(
                object_store::ObjectStoreExt::head(store.as_ref(), &path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "maintenance deleted {id}"
        );
    }
    assert_eq!(
        (receipt(&rejected_id), receipt(&plain_id)),
        ("unpublished", "published"),
        "after maintenance deleted the manifests"
    );
    std::fs::remove_dir_all(root).unwrap();
}
