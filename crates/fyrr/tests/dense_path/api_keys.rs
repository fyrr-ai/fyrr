//! API keys through the real writer and reader executables: the identity the
//! writer publishes, and what the HTTP reader admits with it, with and
//! without OIDC.
use super::coordination::{host_command, worker_command};
use super::http_mcp::{Issuer, Reader, fixture};
use super::*;
use fyrr_reader::storage::{api_key_principal, api_key_subject};
use std::time::Duration;

/// A realistically shaped secret that is distinct per `n`: `fyrr_key_` and 64
/// hexadecimal characters, as `openssl rand -hex 32` makes them.
fn secret(n: usize) -> String {
    let mut state = (n as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut hex = String::new();
    while hex.len() < 64 {
        state ^= state >> 29;
        state = state.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        state ^= state >> 32;
        hex.push_str(&format!("{state:016x}"));
    }
    let secret = format!("fyrr_key_{}", &hex[..64]);
    assert!(fyrr_core::config::is_api_key_secret(&secret), "{secret}");
    secret
}

struct Key {
    name: &'static str,
    variable: &'static str,
    secret: String,
    readers: &'static str,
}
impl Key {
    fn new(name: &'static str, variable: &'static str, n: usize, readers: &'static str) -> Self {
        Self {
            name,
            variable,
            secret: secret(n),
            readers,
        }
    }
    fn table(&self) -> String {
        format!(
            "\n[[identity.api_keys]]\nname='{}'\nkey={{env='{}'}}\nreaders=['{}']\n",
            self.name, self.variable, self.readers
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

/// The 401 every unauthenticated key gets, as the caller sees it.
async fn refusal(reader: &Reader, token: &str) -> (u16, String, Option<String>) {
    let response = reader.rpc(token, 1, "tools/list", json!({})).await;
    let status = response.status().as_u16();
    let challenge = response
        .headers()
        .get("www-authenticate")
        .map(|v| v.to_str().unwrap().to_owned());
    (status, response.text().await.unwrap(), challenge)
}

async fn status(reader: &Reader, token: &str) -> u16 {
    reader
        .rpc(token, 2, "tools/list", json!({}))
        .await
        .status()
        .as_u16()
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
    /// writer's, and commits the identity with the secrets of `present`.
    fn publish(&self, keys: &[&Key], present: &[&Key], extra: &str) -> Output {
        let tables: String = keys.iter().map(|key| key.table()).collect();
        std::fs::write(&self.worker, format!("{}{tables}", self.base_worker)).unwrap();
        std::fs::write(&self.commit, format!("{}{tables}{extra}", self.base_commit)).unwrap();
        writer(&self.commit, present)
            .args(["writer", "commit"])
            .output()
            .unwrap()
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
    let mut reader =
        Reader::start_appended(&deployment.worker, &deployment.root, &[], key_http).await;
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
        Reader::start_appended(&deployment.shipped(), &deployment.root, &[], key_http).await;
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

    let mut reader =
        Reader::start_appended(&deployment.shipped(), &deployment.root, &[], key_http).await;
    assert_eq!(status(&reader, &good.secret).await, 200);
    assert_eq!(status(&reader, &other.secret).await, 200);
    let wrong = refusal(&reader, &secret(999)).await;
    for key in [&malformed, &unset] {
        assert_eq!(refusal(&reader, &key.secret).await, wrong, "{}", key.name);
    }

    // The bad variables do not stop the revocation of another key.
    let revoked = deployment.publish(&[&good, &malformed, &unset], &present, "");
    assert!(revoked.status.success(), "{}", report(&revoked));
    refresh(&reader).await;
    assert_eq!(refusal(&reader, &other.secret).await, wrong, "revoked");
    assert_eq!(status(&reader, &good.secret).await, 200);
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
}
