//! API keys through the real writer and reader executables: the identity the
//! writer publishes, and what the HTTP reader admits with it, with and
//! without OIDC.
use super::coordination::{host_command, worker_command};
use super::http_mcp::{Issuer, Reader, fixture};
use super::*;
use fyrr_core::config::ApiKeyRole;
use fyrr_reader::storage::{api_key_principal, api_key_subject};
use std::time::Duration;

/// A secret that is distinct per `n` and has the shape `openssl rand -hex 32`
/// makes: `fyrr_key_` and 64 characters.
fn secret(n: usize) -> String {
    let secret = format!("fyrr_key_{n:064}");
    assert!(fyrr_core::config::is_api_key_secret(&secret), "{secret}");
    secret
}

struct Key {
    name: &'static str,
    variable: &'static str,
    secret: String,
    readers: &'static str,
    /// More lines of the key's table, such as `roles`.
    extra: String,
}
impl Key {
    fn new(name: &'static str, variable: &'static str, n: usize, readers: &'static str) -> Self {
        Self {
            name,
            variable,
            secret: secret(n),
            readers,
            extra: String::new(),
        }
    }
    fn table(&self) -> String {
        let readers = if self.readers.is_empty() {
            String::new()
        } else {
            format!("readers=['{}']\n", self.readers)
        };
        format!(
            "\n[[identity.api_keys]]\nname='{}'\nkey={{env='{}'}}\n{readers}{}\n",
            self.name, self.variable, self.extra
        )
    }
}

/// Only the writer holds the secrets.
fn writer(config: &Path, keys: &[&Key]) -> Command {
    let mut command = host_command(config);
    for key in keys {
        command.env(key.variable, &key.secret);
    }
    command
}

/// The identity generation the serving head names, and the identity.
async fn identity(config: &Path) -> (u64, fyrr_core::identity::IdentityArtifact) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let head = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .expect("namespace head");
    let (store, root) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let identity = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{root}/{}", head.identity.object)),
    )
    .await
    .unwrap();
    (head.identity.generation, identity)
}

/// What a caller of `tools/list` is told with `token`: status, body, challenge.
async fn refusal(reader: &Reader, token: &str) -> (u16, String, Option<String>) {
    let response = reader.rpc(token, 1, "tools/list", json!({})).await;
    let status = response.status().as_u16();
    let challenge = response
        .headers()
        .get("www-authenticate")
        .map(|v| v.to_str().unwrap().to_owned());
    (status, response.text().await.unwrap(), challenge)
}

/// What an operator does after publishing: `POST /admin/refresh` makes the
/// reader read the serving head and apply the identity in it. The route allows
/// one refresh every five seconds.
async fn refresh(reader: &Reader) {
    for _ in 0..30 {
        let response = reader
            .client
            .post(format!("{}/admin/refresh", reader.base))
            .bearer_auth("owned-http-operator-canary")
            .send()
            .await
            .unwrap();
        match response.status().as_u16() {
            200 => return,
            429 => tokio::time::sleep(Duration::from_secs(1)).await,
            status => panic!("refresh answered {status}"),
        }
    }
    panic!("refresh was rate limited for 30 s");
}

/// The reader's HTTP table for a deployment without OIDC.
fn key_http(address: &std::net::SocketAddr, base: &str) -> String {
    format!(
        "\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\noperator_credential={{env='FYRR_HTTP_OPERATOR'}}\nuser_requests_per_minute=1000\ngroup_requests_per_minute=1000\n"
    )
}

fn roots(result: &Value) -> Vec<String> {
    assert_eq!(result["isError"], false, "{result}");
    result["structuredContent"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
        .collect()
}

/// A published namespace whose documents `http-0`..`http-3` are readable by
/// `usr:u0`..`usr:u3`, and an OIDC issuer for the subjects of those users.
struct Deployment {
    issuer: Issuer,
    _issuer_root: FixtureRoot,
    worker: PathBuf,
    commit: PathBuf,
    root: FixtureRoot,
    namespace: String,
    base_worker: String,
    base_commit: String,
}
impl Deployment {
    async fn new() -> Self {
        let (_, issuer_root) = setup();
        let issuer = Issuer::new(&issuer_root).await;
        let (worker, commit, root, _) = fixture(&issuer).await;
        let namespace = fyrr_core::config::load(&worker)
            .unwrap()
            .config
            .namespace
            .name;
        Self {
            _issuer_root: issuer_root,
            base_worker: std::fs::read_to_string(&worker).unwrap(),
            base_commit: std::fs::read_to_string(&commit).unwrap(),
            issuer,
            worker,
            commit,
            root,
            namespace,
        }
    }

    /// Declares exactly `keys` in both configurations, plus `extra` in the
    /// writer's, and commits the identity with the secrets of `present`. When
    /// every key has a usable secret the deployment's keys are published too, as
    /// a reader that declares keys needs them.
    fn publish(&self, keys: &[&Key], present: &[&Key], extra: &str) -> Output {
        let tables: String = keys.iter().map(|key| key.table()).collect();
        std::fs::write(&self.worker, format!("{}{tables}", self.base_worker)).unwrap();
        std::fs::write(&self.commit, format!("{}{tables}{extra}", self.base_commit)).unwrap();
        let committed = writer(&self.commit, present)
            .args(["writer", "commit"])
            .output()
            .unwrap();
        let usable = keys.iter().all(|key| {
            present.iter().any(|set| set.variable == key.variable)
                && fyrr_core::config::is_api_key_secret(&key.secret)
        });
        if usable && committed.status.success() {
            let published = writer(&self.commit, present)
                .args(["writer", "keys"])
                .output()
                .unwrap();
            assert!(published.status.success(), "{}", report(&published));
        }
        committed
    }

    /// Publishes exactly `keys` as the deployment's keys.
    fn keys(&self, keys: &[&Key]) -> Output {
        let tables: String = keys.iter().map(|key| key.table()).collect();
        std::fs::write(&self.commit, format!("{}{tables}", self.base_commit)).unwrap();
        writer(&self.commit, keys)
            .args(["writer", "keys"])
            .output()
            .unwrap()
    }

    /// Removes the `[identity.directory]` the fixture adds, so that the
    /// configuration has no identity provider (`provider = "none"`).
    fn without_directory(&mut self) {
        for text in [&mut self.base_worker, &mut self.base_commit] {
            let start = text.find("\n[identity.directory]\n").unwrap();
            let end = start
                + text[start..].find("sync_interval='5m'\n").unwrap()
                + "sync_interval='5m'\n".len();
            text.replace_range(start..end, "");
            assert!(!text.contains("identity.directory"));
        }
    }

    /// Publishes the namespace `<namespace>-<suffix>` in the same storage: the
    /// documents of the fixture, readable by the same users, with exactly
    /// `keys` declared. Returns its name.
    fn sibling(&self, suffix: &str, keys: &[&Key]) -> String {
        let name = format!("{}-{suffix}", self.namespace);
        let tables: String = keys.iter().map(|key| key.table()).collect();
        let config = self.root.join(format!("{suffix}.toml"));
        let text = self.base_worker.replace(
            &format!("name = \"{}\"", self.namespace),
            &format!("name = \"{name}\""),
        );
        assert_ne!(text, self.base_worker);
        std::fs::write(&config, format!("{text}{tables}")).unwrap();
        for args in [
            &["writer", "namespace", "create"][..],
            &[
                "writer",
                "sync",
                "incremental",
                "--kind",
                "content",
                "--kind",
                "permissions",
            ],
            &["writer", "commit"],
        ] {
            let mut command = worker_command(&config);
            for key in keys {
                command.env(key.variable, &key.secret);
            }
            let output = command.args(args).output().unwrap();
            assert!(output.status.success(), "{args:?}: {}", report(&output));
        }
        name
    }

    fn doc(&self, i: usize) -> String {
        format!("{}-http-{i}", self.namespace)
    }

    /// The reader's file as a shipped reader reads it: the suites ask for a head
    /// read at every authorization check; a reader as it ships checks the head
    /// of its last version check.
    fn shipped(&self) -> PathBuf {
        let text = std::fs::read_to_string(&self.worker).unwrap();
        assert!(text.contains("# fyrr-test-head-check = every"));
        let path = self.root.join("shipped.toml");
        std::fs::write(&path, text.replace("# fyrr-test-head-check = every", "")).unwrap();
        path
    }
}

fn search() -> Value {
    json!({"query":"fixture query","sources":["incremental"],"limit":5})
}

#[tokio::test]
#[ignore = "real HTTP MCP executable and local storage; scripts/ci"]
async fn roles_are_isolated_without_oidc() {
    let mut deployment = Deployment::new().await;
    // Keys and no identity provider at all.
    deployment.without_directory();
    // A document larger than one fetch page (64 KiB), readable by usr:u1. The
    // fixture caps documents at 64 bytes and knows no vector for new text.
    for text in [&mut deployment.base_worker, &mut deployment.base_commit] {
        *text = text.replace("document_max_bytes='64B'", "document_max_bytes='1MiB'");
    }
    std::fs::write(&deployment.worker, &deployment.base_worker).unwrap();
    std::fs::write(&deployment.commit, &deployment.base_commit).unwrap();
    let big = format!("{}-big", deployment.namespace);
    let texts: Vec<String> = (0..6)
        .map(|n| {
            let text = format!("{n} {}", "page ".repeat(2400));
            let vector: Vec<f32> = (0..1024)
                .map(|d| (((n + 900) * (d + 7)) as f32).sin())
                .collect();
            deployment.root.embedding.insert_both(&text, vector);
            text
        })
        .collect();
    super::incremental::input(
        &deployment.root,
        json!({"epoch":2,"content":[[{"id":big,"revision":"r1","texts":texts,"readers":["usr:u1"],"section_readers":vec![vec!["usr:u1"]; 6]}]],"permissions":[[{"id":big,"revision":"p1","readers":["usr:u1"]}]]}),
    );
    super::coordination::worker_sync(
        &deployment.worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    let one = Key::new("nm-one-q7x", "FYRR_TEST_API_KEY_ONE", 11, "usr:u1");
    let two = Key::new("nm-two-q7x", "FYRR_TEST_API_KEY_TWO", 12, "usr:u2");
    let keys = [&one, &two];
    let published = deployment.publish(&keys, &keys, "");
    assert!(published.status.success(), "{}", report(&published));

    // A deployment with API keys and no OIDC starts and serves. The reader has
    // none of the secrets in its environment.
    let mut reader = Reader::start_appended(&deployment.worker, &deployment.root, key_http).await;
    let first = roots(&reader.call(&one.secret, 1, "search", search()).await);
    assert!(first.contains(&deployment.doc(1)) && !first.contains(&deployment.doc(2)));
    assert_eq!(
        roots(&reader.call(&two.secret, 2, "search", search()).await),
        [deployment.doc(2)]
    );

    // A document outside a key's grants answers like one that does not exist.
    let fetch = |id: String| json!({"source_id":"incremental","root_id":id});
    let outside = reader
        .call(&two.secret, 3, "fetch", fetch(deployment.doc(1)))
        .await;
    let missing = reader
        .call(&two.secret, 4, "fetch", fetch(deployment.doc(9)))
        .await;
    // Everything but the per-request telemetry.
    let normal = |value: &Value, id: &str| {
        let content = &value["structuredContent"];
        json!([
            value["isError"],
            content["documents"],
            content["next_cursor"],
            content["window"],
            content["document_changed"]
        ])
        .to_string()
        .replace(id, "ID")
    };
    assert_eq!(
        normal(&outside, &deployment.doc(1)),
        normal(&missing, &deployment.doc(9))
    );
    let own = reader
        .call(&one.secret, 5, "fetch", fetch(deployment.doc(1)))
        .await;
    assert_ne!(
        normal(&own, &deployment.doc(1)),
        normal(&missing, &deployment.doc(9))
    );

    // A fetch cursor is bound to the key it was issued to.
    let page = reader.call(&one.secret, 6, "fetch", fetch(big)).await;
    let cursor = page["structuredContent"]["next_cursor"]
        .as_str()
        .unwrap_or_else(|| panic!("a second page: {page}"))
        .to_owned();
    let next = reader
        .call(&one.secret, 7, "fetch", json!({"cursor":cursor}))
        .await;
    assert_eq!(next["isError"], false, "{next}");
    let stolen = reader
        .call(&two.secret, 8, "fetch", json!({"cursor":cursor}))
        .await;
    assert_eq!(stolen["isError"], true, "{stolen}");
    assert_eq!(stolen["structuredContent"]["code"], "stale_cursor");

    // Unknown and malformed keys are one and the same 401, with a challenge that
    // names no authorization server.
    let wrong = refusal(&reader, &secret(99)).await;
    assert_eq!(wrong.0, 401);
    assert_eq!(wrong.2.as_deref(), Some("Bearer"));
    for malformed in [
        "fyrr_key_short".to_owned(),
        format!("{}!", secret(11)),
        "fyrr_key_".to_owned(),
        "other-token".to_owned(),
    ] {
        assert_eq!(refusal(&reader, &malformed).await, wrong, "{malformed}");
    }
    let anonymous = reader
        .client
        .post(format!("{}/mcp", reader.base))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 401);
    assert_eq!(anonymous.headers()["www-authenticate"], "Bearer");
    // A cancellation with a key nobody bound is refused as well.
    let cancel = |token: String| {
        reader
            .client
            .post(format!("{}/mcp", reader.base))
            .bearer_auth(token)
            .header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}))
            .send()
    };
    assert_eq!(cancel(secret(99)).await.unwrap().status(), 401);
    assert_eq!(cancel(one.secret.clone()).await.unwrap().status(), 202);
    // No authorization server exists, so none is advertised.
    for path in [
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/mcp",
    ] {
        let response = reader
            .client
            .get(format!("{}{path}", reader.base))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404, "{path}");
    }

    // The published identity and the reader's log hold the keys' hashes, where
    // those are the contract, and nothing else of them.
    let (_, artifact) = identity(&deployment.commit).await;
    let cfg = fyrr_core::config::load(&deployment.worker).unwrap().config;
    let log = reader.stderr_text();
    let published = String::from_utf8(write_contract(&artifact)).unwrap();
    for key in keys {
        let subject = api_key_subject(&cfg, &key.secret).unwrap();
        let principal = api_key_principal(&cfg, key.name).unwrap();
        let binding = artifact.subjects.iter().find(|b| b.subject == subject);
        assert_eq!(binding.map(|b| &b.principal), Some(&principal));
        for text in [
            key.secret.as_str(),
            &key.secret["fyrr_key_".len()..],
            key.name,
            key.variable,
        ] {
            assert!(!published.contains(text), "the identity holds {text}");
            assert!(!log.contains(text), "the reader's log holds {text}");
        }
        for text in [&subject, &principal] {
            assert!(
                !log.contains(text.as_str()),
                "the reader's log holds {text}"
            );
            assert!(!log.contains(&text["key:".len()..]));
        }
    }
    reader.stop().await;
}

#[tokio::test]
#[ignore = "real writer, HTTP MCP executable and local storage; scripts/ci"]
async fn last_key_revocation_reaches_a_running_reader() {
    let key = Key::new("nm-last-q7x", "FYRR_TEST_API_KEY_LAST", 31, "usr:u1");
    let deployment = Deployment::new().await;
    let published = deployment.publish(&[&key], &[&key], "");
    assert!(published.status.success(), "{}", report(&published));
    // A reader as it ships: it applies a publication at its next version
    // check, or when an operator refreshes it.
    let mut reader =
        Reader::start_appended(&deployment.shipped(), &deployment.root, key_http).await;
    assert_eq!(
        roots(&reader.call(&key.secret, 1, "search", search()).await),
        [deployment.doc(1)]
    );

    // The key is removed from the file the writer shares with an HTTP reader
    // that now has no way to authenticate callers. The writer does not need
    // one: it publishes.
    let stub = "\n[reader.http]\nlisten='127.0.0.1:1'\npublic_url='http://127.0.0.1:1/mcp'\n";
    let revoked = deployment.publish(&[], &[], stub);
    assert!(revoked.status.success(), "{}", report(&revoked));
    let cfg = fyrr_core::config::load(&deployment.commit).unwrap().config;
    let bound = api_key_subject(&cfg, &key.secret).unwrap();
    let (_, after) = identity(&deployment.commit).await;
    assert!(after.subjects.iter().all(|b| b.subject != bound));
    // The running reader refuses the key once an operator has refreshed it.
    refresh(&reader).await;
    assert_eq!(refusal(&reader, &key.secret).await.0, 401);
    // `config check` warns, and a fresh reader does not start.
    let check = worker_command(&deployment.commit)
        .args(["config", "check", "--no-env"])
        .output()
        .unwrap();
    assert!(check.status.success(), "{}", report(&check));
    assert!(
        String::from_utf8_lossy(&check.stderr).contains("reader.http"),
        "{}",
        report(&check)
    );
    let start = host_command(&deployment.commit)
        .args(["reader", "serve"])
        .output()
        .unwrap();
    assert_eq!(start.status.code(), Some(78), "{}", report(&start));
    reader.stop().await;
}

#[tokio::test]
#[ignore = "real writer, HTTP MCP executable and local storage; scripts/ci"]
async fn bad_secret_does_not_block_revocation() {
    let good = Key::new("nm-good-q7x", "FYRR_TEST_API_KEY_GOOD", 71, "usr:u1");
    let other = Key::new("nm-other-q7x", "FYRR_TEST_API_KEY_OTHER", 72, "usr:u2");
    let malformed = Key {
        secret: "fyrr_key_too-short".into(),
        ..Key::new("nm-bad-q7x", "FYRR_TEST_API_KEY_BAD", 0, "usr:u3")
    };
    let unset = Key::new("nm-unset-q7x", "FYRR_TEST_API_KEY_UNSET", 73, "usr:u3");
    let deployment = Deployment::new().await;
    let all = [&good, &other, &malformed, &unset];
    // The writer has every secret but the unset one's, and one is malformed.
    let present = [&good, &other, &malformed];
    let published = deployment.publish(&all, &present, "");
    assert!(published.status.success(), "{}", report(&published));
    let (_, before) = identity(&deployment.commit).await;
    let cfg = fyrr_core::config::load(&deployment.commit).unwrap().config;
    let bound = |key: &Key| {
        let subject = api_key_subject(&cfg, &key.secret).unwrap();
        before.subjects.iter().any(|b| b.subject == subject)
    };
    assert!(bound(&good) && bound(&other), "the usable keys are bound");
    assert!(!bound(&malformed) && !bound(&unset), "the others are not");
    // `config check` names the missing variable and the malformed secret by path.
    let mut check = worker_command(&deployment.commit);
    for key in present {
        check.env(key.variable, &key.secret);
    }
    let check = check.args(["config", "check"]).output().unwrap();
    assert_eq!(check.status.code(), Some(78), "{}", report(&check));
    let stderr = String::from_utf8_lossy(&check.stderr);
    assert!(
        stderr.contains("identity.api_keys[2].key") && stderr.contains("identity.api_keys[3].key")
    );
    assert!(!report(&check).contains("fyrr_key_too-short"));
    // The deployment's keys are all or nothing: an unset or malformed secret
    // would otherwise revoke its key everywhere without a word.
    let refused = writer(&deployment.commit, &present)
        .args(["writer", "keys"])
        .output()
        .unwrap();
    assert!(!refused.status.success(), "{}", report(&refused));
    let keyed = deployment.keys(&[&good, &other]);
    assert!(keyed.status.success(), "{}", report(&keyed));

    let mut reader =
        Reader::start_appended(&deployment.shipped(), &deployment.root, key_http).await;
    assert_eq!(refusal(&reader, &good.secret).await.0, 200);
    assert_eq!(refusal(&reader, &other.secret).await.0, 200);
    let wrong = refusal(&reader, &secret(999)).await;
    for key in [&malformed, &unset] {
        assert_eq!(refusal(&reader, &key.secret).await, wrong, "{}", key.name);
    }

    // The bad variables do not stop the revocation of another key.
    let revoked = deployment.publish(&[&good, &malformed, &unset], &present, "");
    assert!(revoked.status.success(), "{}", report(&revoked));
    let keyed = deployment.keys(&[&good]);
    assert!(keyed.status.success(), "{}", report(&keyed));
    refresh(&reader).await;
    assert_eq!(refusal(&reader, &other.secret).await, wrong, "revoked");
    assert_eq!(refusal(&reader, &good.secret).await.0, 200);
    reader.stop().await;

    // Two keys with one secret stop the commit; the identity stays as it was.
    let (generation, _) = identity(&deployment.commit).await;
    let twin = Key {
        secret: good.secret.clone(),
        ..Key::new("nm-twin-q7x", "FYRR_TEST_API_KEY_TWIN", 0, "usr:u2")
    };
    let refused = deployment.publish(&[&good, &twin], &[&good, &twin], "");
    assert!(!refused.status.success(), "{}", report(&refused));
    assert_eq!(identity(&deployment.commit).await.0, generation);
}

#[tokio::test]
#[ignore = "real HTTP MCP executable, pinned JWKS and local storage; scripts/ci"]
async fn oidc_and_keys_coexist() {
    let key = Key::new("nm-both-q7x", "FYRR_TEST_API_KEY_BOTH", 51, "usr:u1");
    let deployment = Deployment::new().await;
    let published = deployment.publish(&[&key], &[&key], "");
    assert!(published.status.success(), "{}", report(&published));
    let mut reader = Reader::start(
        &deployment.worker,
        &deployment.root,
        &deployment.issuer,
        1000,
        1000,
    )
    .await;
    let token = deployment.issuer.token("subject-2", json!({}), json!({}));
    assert_eq!(
        roots(&reader.call(&token, 1, "search", search()).await),
        [deployment.doc(2)]
    );
    assert_eq!(
        roots(&reader.call(&key.secret, 2, "search", search()).await),
        [deployment.doc(1)]
    );
    // An unknown key and a garbage token are refused, and the challenge and the
    // OIDC metadata stay.
    for bearer in [secret(99), "not-a-token".to_owned()] {
        let refused = refusal(&reader, &bearer).await;
        assert_eq!(refused.0, 401);
        assert!(refused.2.unwrap().contains("resource_metadata"));
    }
    let metadata = reader
        .client
        .get(format!(
            "{}/.well-known/oauth-protected-resource",
            reader.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(metadata.status(), 200);
    reader.stop().await;

    // A reader whose own configuration declares no key takes a published key
    // for an OIDC token and refuses it with the OIDC challenge.
    std::fs::write(&deployment.worker, &deployment.base_worker).unwrap();
    let mut oidc_only = Reader::start(
        &deployment.worker,
        &deployment.root,
        &deployment.issuer,
        1000,
        1000,
    )
    .await;
    assert_eq!(
        roots(&oidc_only.call(&token, 3, "search", search()).await),
        [deployment.doc(2)]
    );
    let refused = refusal(&oidc_only, &key.secret).await;
    assert_eq!(refused.0, 401);
    assert!(refused.2.unwrap().contains("resource_metadata"));
    oidc_only.stop().await;
}

#[tokio::test]
#[ignore = "real writer, HTTP MCP executable and local storage; scripts/ci"]
async fn deployment_keys_carry_roles_and_a_namespace_prefix() {
    let deployment = Deployment::new().await;
    let (matching, other) = (
        deployment.namespace[..2].to_owned(),
        format!("{}-other", deployment.namespace),
    );
    let reads = Key::new("nm-reads-q7x", "FYRR_TEST_API_KEY_READS", 81, "usr:u1");
    let writes = Key {
        extra: "roles=['write']".into(),
        ..Key::new("nm-writes-q7x", "FYRR_TEST_API_KEY_WRITES", 82, "")
    };
    let both = Key {
        extra: format!("roles=['read','write']\nnamespace_prefix='{matching}'"),
        ..Key::new("nm-both-q7x", "FYRR_TEST_API_KEY_BOTH", 83, "usr:u2")
    };
    let elsewhere = Key {
        extra: format!("namespace_prefix='{other}'"),
        ..Key::new("nm-else-q7x", "FYRR_TEST_API_KEY_ELSE", 84, "usr:u3")
    };
    let all = [&reads, &writes, &both, &elsewhere];
    let published = deployment.publish(&all, &all, "");
    assert!(published.status.success(), "{}", report(&published));
    let keys = |present: &[&Key]| {
        writer(&deployment.commit, present)
            .args(["writer", "keys"])
            .output()
            .unwrap()
    };

    // A reader that checks its version once a second.
    let fast = deployment.root.join("fast.toml");
    std::fs::write(
        &fast,
        std::fs::read_to_string(deployment.shipped())
            .unwrap()
            .replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
    )
    .unwrap();
    let mut reader = Reader::start_appended(&fast, &deployment.root, key_http).await;
    let output = keys(&all);
    assert!(output.status.success(), "{}", report(&output));
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("\"changed\":false"),
        "a current artifact is not written again: {}",
        report(&output)
    );
    // A key that does not hold `read`, or acts in other namespaces only, is
    // refused with one code on every route; within two version checks.
    let forbidden = |status: u16, body: &str| {
        assert_eq!(status, 403);
        assert_eq!(body, "{\"error\":\"forbidden\"}");
    };
    for key in [&writes, &elsewhere] {
        let (status, body, _) = refusal(&reader, &key.secret).await;
        forbidden(status, &body);
        let response = reader
            .client
            .post(format!(
                "{}/v1/namespaces/{}/query",
                reader.base, deployment.namespace
            ))
            .bearer_auth(&key.secret)
            .json(&json!({"query":"fixture query"}))
            .send()
            .await
            .unwrap();
        forbidden(response.status().as_u16(), &response.text().await.unwrap());
    }
    assert_eq!(refusal(&reader, &reads.secret).await.0, 200);
    assert_eq!(refusal(&reader, &both.secret).await.0, 200);
    assert_eq!(refusal(&reader, &secret(99)).await.0, 401);

    // The artifact holds hashes with roles and prefixes, nothing else of a key.
    let cfg = fyrr_core::config::load(&deployment.worker).unwrap().config;
    let (store, root) = fyrr_reader::storage::namespace_store(&cfg).unwrap();
    let artifact: fyrr_core::identity::ApiKeysArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &fyrr_reader::storage::deployment_root(&root).join(fyrr_reader::storage::API_KEYS_FILE),
    )
    .await
    .unwrap();
    let grant = artifact
        .grant(&fyrr_reader::storage::api_key_deployment_hash(&cfg, &both.secret).unwrap())
        .expect("a grant per key");
    assert_eq!(grant.roles, [ApiKeyRole::Read, ApiKeyRole::Write]);
    assert_eq!(grant.namespace_prefix.as_deref(), Some(matching.as_str()));
    let text = String::from_utf8(write_contract(&artifact)).unwrap();
    let log = reader.stderr_text();
    for key in all {
        for secret in [
            key.secret.as_str(),
            &key.secret["fyrr_key_".len()..],
            key.name,
        ] {
            assert!(!text.contains(secret), "the artifact holds {secret}");
            assert!(!log.contains(secret), "the reader's log holds {secret}");
        }
    }

    // Dropping a key from the artifact alone refuses it at the readers within
    // two version checks; the identity of the namespace is not republished.
    let kept = [&writes, &both, &elsewhere];
    let tables: String = kept.iter().map(|key| key.table()).collect();
    std::fs::write(
        &deployment.commit,
        format!("{}{tables}", deployment.base_commit),
    )
    .unwrap();
    let output = keys(&kept);
    assert!(output.status.success(), "{}", report(&output));
    let started = std::time::Instant::now();
    while refusal(&reader, &reads.secret).await.0 != 401 {
        assert!(
            started.elapsed() < Duration::from_secs(6),
            "a removed key was still served"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(refusal(&reader, &both.secret).await.0, 200);
    reader.stop().await;
}

/// What a caller is told with `token` on the JSON route of `namespace`.
async fn path_query(reader: &Reader, token: &str, namespace: &str) -> (u16, Value) {
    let response = reader
        .client
        .post(format!("{}/v1/namespaces/{namespace}/query", reader.base))
        .bearer_auth(token)
        .json(&search())
        .send()
        .await
        .unwrap();
    (
        response.status().as_u16(),
        response.json().await.unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "real writer, HTTP reader executable and local storage; scripts/ci"]
async fn a_reader_serves_the_namespace_its_path_names() {
    let mut deployment = Deployment::new().await;
    deployment.without_directory();
    let both = Key::new("nm-both-p4z", "FYRR_TEST_API_KEY_BOTH", 61, "usr:u1");
    let only = Key::new("nm-only-p4z", "FYRR_TEST_API_KEY_ONLY", 62, "usr:u2");
    let elsewhere = Key {
        extra: "namespace_prefix='zz'".into(),
        ..Key::new("nm-else-p4z", "FYRR_TEST_API_KEY_ELSE", 63, "usr:u1")
    };
    let all = [&both, &only, &elsewhere];
    let published = deployment.publish(&all, &all, "");
    assert!(published.status.success(), "{}", report(&published));
    // `only` is declared for the first namespace alone.
    let second = deployment.sibling("two", &[&both, &elsewhere]);
    let mut reader = Reader::start_appended(&deployment.worker, &deployment.root, key_http).await;
    let roots = |body: &Value| -> Vec<String> {
        body["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
            .collect()
    };

    // The namespace the path names is opened on its first request, and the
    // key reads there what its readers say; the configured one still serves.
    let (status, body) = path_query(&reader, &both.secret, &second).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(roots(&body), [deployment.doc(1)]);
    let (status, body) = path_query(&reader, &both.secret, &deployment.namespace).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(roots(&body), [deployment.doc(1)]);

    // A key declared for the first namespace alone does not read the other.
    let unbound = path_query(&reader, &only.secret, &second).await;
    assert_eq!(unbound, (401, json!({"error":"unauthorized"})));
    // A namespace that does not exist answers a caller that may read there as
    // one that is not bound, and a caller that may not read it as it does
    // for one that exists, whatever the name looks like.
    let missing = format!("{}-missing", deployment.namespace);
    assert_eq!(path_query(&reader, &only.secret, &missing).await, unbound);
    assert_eq!(path_query(&reader, &both.secret, &missing).await, unbound);
    let forbidden = (403, json!({"error":"forbidden"}));
    for name in [second.as_str(), missing.as_str(), "Bad_Name", "a%2Fb", ""] {
        assert_eq!(
            path_query(&reader, &elsewhere.secret, name).await,
            forbidden,
            "{name:?}"
        );
    }
    for name in ["Bad_Name", "a%2Fb", ""] {
        assert_eq!(
            path_query(&reader, &both.secret, name).await,
            (404, json!({"error":"invalid_argument"})),
            "{name:?}"
        );
    }

    // MCP keeps serving the configured namespace only.
    assert_eq!(
        roots(&reader.call(&both.secret, 1, "search", search()).await["structuredContent"]),
        [deployment.doc(1)]
    );
    reader.stop().await;
}

/// What a caller is told with `token` on the report route of `namespace`.
async fn report_of(reader: &Reader, token: &str, namespace: &str) -> (u16, Value) {
    let response = reader
        .client
        .get(format!("{}/v1/namespaces/{namespace}", reader.base))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    (
        response.status().as_u16(),
        response.json().await.unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "real writer, HTTP reader executable and local storage; scripts/ci"]
async fn a_namespace_report_needs_the_admin_or_list_role() {
    let mut deployment = Deployment::new().await;
    deployment.without_directory();
    let reads = Key::new("nm-reads-r5k", "FYRR_TEST_API_KEY_READS", 71, "usr:u1");
    let lists = Key {
        extra: "roles=['list']".into(),
        ..Key::new("nm-lists-r5k", "FYRR_TEST_API_KEY_LISTS", 72, "")
    };
    let admins = Key {
        extra: "roles=['admin']".into(),
        ..Key::new("nm-admin-r5k", "FYRR_TEST_API_KEY_ADMIN", 73, "")
    };
    let elsewhere = Key {
        extra: "roles=['admin']\nnamespace_prefix='zz'".into(),
        ..Key::new("nm-else-r5k", "FYRR_TEST_API_KEY_ELSE", 74, "")
    };
    let all = [&reads, &lists, &admins, &elsewhere];
    let published = deployment.publish(&all, &all, "");
    assert!(published.status.success(), "{}", report(&published));
    let second = deployment.sibling("report", &all);
    let mut reader = Reader::start_appended(&deployment.worker, &deployment.root, key_http).await;

    // A key with `list` or `admin` reads the report of a namespace it may act
    // in: the configured one and one its path names.
    for key in [&lists, &admins] {
        for namespace in [&deployment.namespace, &second] {
            let (status, body) = report_of(&reader, &key.secret, namespace).await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(body["namespace"], *namespace);
            assert!(body["rows"].as_u64().unwrap() >= 4, "{body}");
            assert!(body["logical_bytes"].as_u64().unwrap() > 0, "{body}");
            assert!(body["created_at"].is_string(), "{body}");
            assert!(body["last_write_at"].is_string(), "{body}");
            assert_eq!(body["schema"], json!({}));
            assert!(body["settings"]["embedding"]["model"].is_string(), "{body}");
            assert_eq!(body["unindexed"], json!({"rows":0,"logical_bytes":0}));
        }
    }

    // A key that reads, or acts in other namespaces only, is refused; so is a
    // secret nobody published, a caller without one, and a write to the route.
    let forbidden = (403, json!({"error":"forbidden"}));
    assert_eq!(
        report_of(&reader, &reads.secret, &deployment.namespace).await,
        forbidden
    );
    assert_eq!(
        report_of(&reader, &elsewhere.secret, &deployment.namespace).await,
        forbidden
    );
    assert_eq!(
        report_of(&reader, &secret(99), &deployment.namespace).await,
        (401, json!({"error":"unauthorized"}))
    );
    let anonymous = reader
        .client
        .get(format!(
            "{}/v1/namespaces/{}",
            reader.base, deployment.namespace
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 401);
    let post = reader
        .client
        .post(format!(
            "{}/v1/namespaces/{}",
            reader.base, deployment.namespace
        ))
        .bearer_auth(&admins.secret)
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), 405);
    // A namespace that does not exist is one the key is not bound in.
    let missing = format!("{}-missing", deployment.namespace);
    assert_eq!(
        report_of(&reader, &admins.secret, &missing).await,
        (401, json!({"error":"unauthorized"}))
    );
    reader.stop().await;
}

/// The root IDs a JSON route answered with.
fn hits_of(body: &Value) -> Vec<String> {
    body["hits"]
        .as_array()
        .map(|hits| {
            hits.iter()
                .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
#[ignore = "real writer, HTTP reader executable and local storage; scripts/ci"]
async fn an_idle_namespace_answers_from_a_head_read_after_its_request() {
    let mut deployment = Deployment::new().await;
    deployment.without_directory();
    let key = Key::new("nm-idle-p4z", "FYRR_TEST_API_KEY_IDLE", 65, "usr:u1");
    let published = deployment.publish(&[&key], &[&key], "");
    assert!(published.status.success(), "{}", report(&published));
    let second = deployment.sibling("idle", &[&key]);
    // A reader as it ships, with checks every second.
    let config = deployment.root.join("idle-reader.toml");
    let text = std::fs::read_to_string(deployment.shipped()).unwrap();
    std::fs::write(
        &config,
        text.replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
    )
    .unwrap();
    let mut reader = Reader::start_appended(&config, &deployment.root, key_http).await;
    let (status, body) = path_query(&reader, &key.secret, &second).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(hits_of(&body), [deployment.doc(1)]);

    // No request for several intervals, then its source is retired. The next
    // request must not be answered from the view the namespace held.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let sibling = deployment.root.join("idle.toml");
    for args in [
        &["writer", "source", "retire", "incremental"][..],
        &["writer", "commit"],
    ] {
        let output = worker_command(&sibling)
            .env(key.variable, &key.secret)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}: {}", report(&output));
    }
    let (status, body) = path_query(&reader, &key.secret, &second).await;
    assert!(
        status != 200 || hits_of(&body).is_empty(),
        "answered {status} {body} after the source was retired"
    );
    reader.stop().await;
}

/// A field of the last line the reader reported about its memory envelope.
fn accounting(log: &str, field: &str) -> u64 {
    log.lines()
        .rev()
        .find(|line| line.contains("generation_preparation=accounting"))
        .and_then(|line| {
            line.split_whitespace()
                .find_map(|word| word.strip_prefix(&format!("{field}=")))?
                .parse()
                .ok()
        })
        .unwrap_or_else(|| panic!("no accounting of {field} in the reader's log:\n{log}"))
}

#[tokio::test]
#[ignore = "real writer, HTTP reader executable and local storage; scripts/ci"]
async fn namespaces_opened_by_path_stay_inside_the_memory_budget() {
    const MIB: u64 = 1024 * 1024;
    let mut deployment = Deployment::new().await;
    deployment.without_directory();
    let key = Key::new("nm-mem-p4z", "FYRR_TEST_API_KEY_MEM", 66, "usr:u1");
    let published = deployment.publish(&[&key], &[&key], "");
    assert!(published.status.success(), "{}", report(&published));
    // Two more than fit: the configured namespace and two others fill the
    // budget set below, and four are opened.
    let names: Vec<String> = (0..4)
        .map(|n| deployment.sibling(&format!("mem{n}"), &[&key]))
        .collect();

    // What the configured namespace holds, and what the process held before
    // the envelope was taken from the budget: both are read from a reader
    // with the default budget.
    let text = std::fs::read_to_string(&deployment.worker).unwrap();
    let mut probe = Reader::start_appended(&deployment.worker, &deployment.root, key_http).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let log = probe.stderr_text();
    probe.stop().await;
    let held = accounting(&log, "reserved_bytes");
    // The pool is the budget less the headroom (128 MiB by default), the
    // subject cache (128 MiB), the caches of the namespaces opened by path
    // (128 MiB) and the resident memory at start.
    let resident =
        accounting(&log, "budget_bytes") - accounting(&log, "capacity_bytes") - 384 * MIB;
    let budget = (resident + 384 * MIB + held * 7 / 2).div_ceil(MIB);
    let tight = deployment.root.join("tight-reader.toml");
    std::fs::write(
        &tight,
        text.replace(
            "[reader]",
            &format!("[reader]\nmemory_max_bytes = '{budget}MiB'"),
        ),
    )
    .unwrap();

    let mut reader = Reader::start_appended(&tight, &deployment.root, key_http).await;
    for name in names.iter().chain(&names[..1]) {
        let (status, body) = path_query(&reader, &key.secret, name).await;
        assert_eq!(status, 200, "{name}: {body}");
        assert_eq!(hits_of(&body), [deployment.doc(1)], "{name}");
    }
    let log = reader.stderr_text();
    assert!(
        log.contains("telemetry namespace=closed reason=memory_budget"),
        "no namespace was closed for memory:\n{log}"
    );
    reader.stop().await;
}
