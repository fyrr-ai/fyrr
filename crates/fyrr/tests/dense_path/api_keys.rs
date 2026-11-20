//! API keys through the real writer and reader executables: the identity the
//! writer publishes, and what the HTTP reader admits with it, with and
//! without OIDC.
use super::coordination::{host_command, worker_command};
use super::corrections::{BoundaryHook, ProxyGuard, RequestGate, ResponseTransform};
use super::http_mcp::{Issuer, Reader, fixture};
use super::identity::{snapshot, user};
use super::*;
use fyrr_core::directory::DirectorySnapshot;
use fyrr_reader::storage::{api_key_principal, api_key_subject};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    grant: &'static str,
}
impl Key {
    fn table(&self) -> String {
        format!(
            "\n[[identity.api_keys]]\nname='{}'\nkey={{env='{}'}}\n{}\n",
            self.name, self.variable, self.grant
        )
    }
}

/// Only the writer holds the secrets.
fn writer(config: &Path, keys: &[Key]) -> Command {
    keyed(host_command(config), keys)
}

fn keyed(mut command: Command, keys: &[Key]) -> Command {
    for key in keys {
        command.env(key.variable, &key.secret);
    }
    command
}

fn tokens_of(
    identity: &fyrr_core::identity::IdentityArtifact,
    principal: &str,
) -> BTreeSet<String> {
    let principal = identity
        .principals
        .iter()
        .find(|p| p.key == principal)
        .expect("principal");
    principal
        .tokens
        .iter()
        .map(|i| identity.tokens[*i as usize].clone())
        .collect()
}

/// The serving head and identity the writer published; no content is needed.
async fn artifact(config: &Path) -> (ServingHead, fyrr_core::identity::IdentityArtifact) {
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
    (head, identity)
}

#[tokio::test]
#[ignore = "real writer and storage; scripts/ci"]
async fn api_key_identity_tokens_users_collisions_and_removal_on_storage() {
    let (config, root) = setup();
    success(&config, &["writer", "namespace", "create"]);
    let base = std::fs::read_to_string(&config).unwrap();
    let keys = [
        Key {
            name: "twin",
            variable: "FYRR_TEST_API_KEY_TWIN",
            secret: secret(1),
            grant: "readers=['grp:allow']",
        },
        Key {
            name: "wide",
            variable: "FYRR_TEST_API_KEY_WIDE",
            secret: secret(2),
            grant: "readers=['grp:allow','grp:deny']\nrequests_per_minute=7",
        },
        Key {
            name: "bo",
            variable: "FYRR_TEST_API_KEY_BO",
            secret: secret(3),
            grant: "user='Bo@Example.test'\nexpires=2999-01-01T00:00:00Z",
        },
        Key {
            name: "gone",
            variable: "FYRR_TEST_API_KEY_GONE",
            secret: secret(4),
            grant: "user='gone@example.test'",
        },
        Key {
            name: "absent",
            variable: "FYRR_TEST_API_KEY_ABSENT",
            secret: secret(5),
            grant: "user='absent@example.test'",
        },
    ];
    let write_config = |keys: &[&Key]| {
        let tables: String = keys.iter().map(|key| key.table()).collect();
        std::fs::write(&config, format!("{base}{tables}")).unwrap();
    };
    let all: Vec<&Key> = keys.iter().collect();
    write_config(&all);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let cfg = &loaded.config;

    // The directory: Bo is active, Gone is not, Absent is missing.
    let mut directory: DirectorySnapshot = snapshot(cfg, "api-key-test");
    // The stdio subject takes part in a configured directory too.
    directory.users.push(user(cfg, "fixture-subject"));
    directory.users.push(user(cfg, "bo@example.test"));
    let mut gone = user(cfg, "gone@example.test");
    gone.active = false;
    directory.users.push(gone);
    let path = root.join("directory.json");
    std::fs::write(&path, write_contract(&directory)).unwrap();
    let imported = writer(&config, &keys)
        .args(["writer", "directory", "--snapshot"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(imported.status.success(), "{}", report(&imported));

    let (_, identity) = artifact(&config).await;
    let subject = |key: &Key| api_key_subject(cfg, &key.secret).unwrap();
    let binding = |key: &Key| identity.subjects.iter().find(|b| b.subject == subject(key));
    // `readers` keys have a principal of their own; its tokens are the stdio
    // subject's for the same readers, and a wider list adds to them.
    let stdio = principal_hash(cfg, "key", "fixture-subject");
    let twin = binding(&keys[0]).expect("twin bound");
    assert_eq!(twin.principal, api_key_principal(cfg, "twin").unwrap());
    assert_eq!(
        binding(&keys[1]).unwrap().principal,
        api_key_principal(cfg, "wide").unwrap()
    );
    // The stdio subject holds its readers and, as a directory user, its own user token.
    let mut stdio_tokens = tokens_of(&identity, &stdio);
    assert!(stdio_tokens.remove(&principal_hash(cfg, "tok", "usr:fixture-subject")));
    assert!(!stdio_tokens.is_empty());
    assert_eq!(tokens_of(&identity, &twin.principal), stdio_tokens);
    let wide = tokens_of(&identity, &binding(&keys[1]).unwrap().principal);
    assert!(wide.is_superset(&stdio_tokens) && wide.len() == stdio_tokens.len() + 1);
    // A `user` key binds to the directory user's own principal, with the expiry.
    let bo = binding(&keys[2]).expect("bo bound");
    assert_eq!(bo.principal, principal_hash(cfg, "key", "bo@example.test"));
    assert_eq!(
        bo.expires.unwrap().to_rfc3339(),
        "2999-01-01T00:00:00+00:00"
    );
    // An inactive and an absent user get no binding.
    assert!(binding(&keys[3]).is_none() && binding(&keys[4]).is_none());
    assert_eq!(identity.subjects.len(), 3);
    // No two kinds of principal share a key.
    let principals: BTreeSet<_> = identity.principals.iter().map(|p| &p.key).collect();
    assert_eq!(principals.len(), identity.principals.len());
    assert!(!principals.contains(&principal_hash(cfg, "key", "twin")));
    // Neither a secret nor its variable name reaches an artifact or `writer status`.
    let published = write_contract(&identity);
    let status = writer(&config, &keys)
        .args(["writer", "status"])
        .output()
        .unwrap();
    assert!(status.status.success(), "{}", report(&status));
    let warnings: Value =
        serde_json::from_slice::<Value>(&status.stdout).unwrap()["warnings"].clone();
    assert_eq!(warnings, json!(["api_key_unbound"]));
    for key in &keys {
        for text in [&key.secret, &key.variable.to_owned()] {
            assert!(!String::from_utf8_lossy(&published).contains(text.as_str()));
            assert!(!String::from_utf8_lossy(&status.stdout).contains(text.as_str()));
        }
    }
    // Nor does `writer status` print a binding's subject or principal hash, or a key's name.
    let printed = report(&status);
    for binding in &identity.subjects {
        for hash in [&binding.subject, &binding.principal] {
            assert!(
                !printed.contains(hash.as_str()),
                "a hash in `writer status`"
            );
            assert!(!printed.contains(&hash[hash.find(':').unwrap() + 1..]));
        }
    }
    for key in &keys {
        assert!(
            !printed.contains(&format!("'{}'", key.name))
                && !printed.contains(&format!("\"{}\"", key.name))
        );
    }

    // A secret shared by two keys fails the commit and publishes nothing.
    let generation = artifact(&config).await.0.identity.generation;
    let leaks = |text: &str| {
        for key in &keys {
            assert!(!text.contains(&key.secret), "a secret is echoed");
        }
    };
    let mut shared = writer(&config, &keys);
    shared.env("FYRR_TEST_API_KEY_GONE", secret(1));
    let output = shared.args(["writer", "commit"]).output().unwrap();
    assert!(!output.status.success(), "shared: {}", report(&output));
    leaks(&report(&output));
    assert_eq!(artifact(&config).await.0.identity.generation, generation);
    // A missing or malformed secret fails closed for its own key only: the
    // commit still publishes every other key, and `writer status` says why.
    let unusable = |label: &str, change: &dyn Fn(&mut Command)| {
        let mut command = writer(&config, &keys);
        change(&mut command);
        let output = command.args(["writer", "commit"]).output().unwrap();
        assert!(output.status.success(), "{label}: {}", report(&output));
        leaks(&report(&output));
        let mut status = writer(&config, &keys);
        change(&mut status);
        let status = status.args(["writer", "status"]).output().unwrap();
        assert!(status.status.success(), "{label}: {}", report(&status));
        leaks(&report(&status));
        let warnings: Vec<String> =
            serde_json::from_slice::<Value>(&status.stdout).unwrap()["warnings"]
                .as_array()
                .unwrap()
                .iter()
                .map(|w| w.as_str().unwrap().to_owned())
                .collect();
        assert!(
            warnings.contains(&"api_key_secret_unusable".to_owned()),
            "{label}: {warnings:?}"
        );
    };
    for (label, change) in [
        (
            "malformed",
            &(|c: &mut Command| {
                c.env("FYRR_TEST_API_KEY_TWIN", "fyrr_key_too-short");
            }) as &dyn Fn(&mut Command),
        ),
        ("low entropy", &|c: &mut Command| {
            c.env(
                "FYRR_TEST_API_KEY_TWIN",
                format!("fyrr_key_{}", "a".repeat(64)),
            );
        }),
        ("missing", &|c: &mut Command| {
            c.env_remove("FYRR_TEST_API_KEY_TWIN");
        }),
    ] {
        unusable(label, change);
        let (_, skipped) = artifact(&config).await;
        assert!(
            skipped
                .subjects
                .iter()
                .all(|b| b.subject != subject(&keys[0])),
            "{label}: the unusable key is not bound"
        );
        assert!(
            skipped
                .subjects
                .iter()
                .any(|b| b.subject == subject(&keys[1]))
                && skipped
                    .subjects
                    .iter()
                    .any(|b| b.subject == subject(&keys[2])),
            "{label}: the other keys stay bound"
        );
        // With the secret back, the next commit binds the key again.
        let restored = writer(&config, &keys)
            .args(["writer", "commit"])
            .output()
            .unwrap();
        assert!(restored.status.success(), "{}", report(&restored));
        assert!(
            artifact(&config)
                .await
                .1
                .subjects
                .iter()
                .any(|b| b.subject == subject(&keys[0])),
            "{label}: the key is bound again"
        );
    }
    let generation = artifact(&config).await.0.identity.generation;
    // `config check` reports the same mistakes by path, without the values.
    let check = |extra: &[(&str, String)]| {
        // `config check` also asks the providers, which needs the worker's Python.
        let mut command = keyed(worker_command(&config), &keys);
        for (name, value) in extra {
            command.env(name, value);
        }
        command.args(["config", "check"]).output().unwrap()
    };
    let shared = check(&[("FYRR_TEST_API_KEY_GONE", secret(1))]);
    assert_eq!(shared.status.code(), Some(78), "{}", report(&shared));
    assert!(String::from_utf8_lossy(&shared.stderr).contains("identity.api_keys[3].key"));
    assert!(!report(&shared).contains(&secret(1)));
    let duplicate_name = std::fs::read_to_string(&config)
        .unwrap()
        .replace("name='gone'", "name='twin'");
    let aside = root.join("duplicate-name.toml");
    std::fs::write(&aside, duplicate_name).unwrap();
    let named = keyed(worker_command(&aside), &keys)
        .args(["config", "check"])
        .output()
        .unwrap();
    assert_eq!(named.status.code(), Some(78), "{}", report(&named));
    assert!(String::from_utf8_lossy(&named.stderr).contains("identity.api_keys[3].name"));

    // Removing a key from the file removes its binding at the next commit, even
    // with nothing else to publish.
    write_config(&[&keys[1], &keys[2], &keys[3], &keys[4]]);
    let commit = writer(&config, &keys)
        .args(["writer", "commit"])
        .output()
        .unwrap();
    assert!(commit.status.success(), "{}", report(&commit));
    let (head, after) = artifact(&config).await;
    assert!(head.identity.generation > generation);
    assert!(
        after
            .subjects
            .iter()
            .all(|b| b.subject != subject(&keys[0])),
        "the removed key is no longer bound"
    );
    assert!(
        after
            .subjects
            .iter()
            .any(|b| b.subject == subject(&keys[1]))
    );
    assert!(!after.principals.iter().any(|p| p.key == twin.principal));
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

/// The suites ask a reader for a head read at every authorization check, so
/// that a request right after a publication sees it. A reader as it ships does
/// not: it checks the head its last version check read. This is `config` for
/// that reader.
fn shipped(config: &Path, root: &Path) -> PathBuf {
    let text = std::fs::read_to_string(config).unwrap();
    assert!(text.contains("# fyrr-test-head-check = every"));
    let name = format!("shipped-{}", config.file_name().unwrap().to_string_lossy());
    let path = root.join(name);
    std::fs::write(&path, text.replace("# fyrr-test-head-check = every", "")).unwrap();
    path
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

#[tokio::test]
#[ignore = "real HTTP MCP executable and local storage; scripts/ci"]
async fn api_key_http_isolation_expiry_revocation_limits_and_oidc_coexistence_on_storage() {
    let (_, issuer_root) = setup();
    let issuer = Issuer::new(&issuer_root).await;
    let (worker, commit, root, _) = fixture(&issuer).await;
    let namespace = fyrr_core::config::load(&worker)
        .unwrap()
        .config
        .namespace
        .name;
    // The fixture's documents are readable by usr:u0..usr:u3 respectively.
    let expiry = fyrr_core::clock::now() + chrono::Duration::seconds(120);
    let expiry_text = expiry.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let expiring_grant: &'static str =
        Box::leak(format!("readers=['usr:u0']\nexpires={expiry_text}").into_boxed_str());
    let keys = [
        Key {
            name: "nm-one-q7x",
            variable: "FYRR_TEST_API_KEY_ONE",
            secret: secret(11),
            grant: "readers=['usr:u1']",
        },
        Key {
            name: "nm-two-q7x",
            variable: "FYRR_TEST_API_KEY_TWO",
            secret: secret(12),
            grant: "readers=['usr:u2']",
        },
        Key {
            name: "nm-limited-q7x",
            variable: "FYRR_TEST_API_KEY_LIMITED",
            secret: secret(13),
            grant: "readers=['usr:u3']\nrequests_per_minute=3",
        },
        Key {
            name: "nm-short-q7x",
            variable: "FYRR_TEST_API_KEY_SHORT",
            secret: secret(14),
            grant: expiring_grant,
        },
        Key {
            name: "nm-user-q7x",
            variable: "FYRR_TEST_API_KEY_USER",
            secret: secret(15),
            grant: "user='http-1@example.test'",
        },
    ];
    let tables: String = keys.iter().map(Key::table).collect();
    let base_worker = std::fs::read_to_string(&worker).unwrap();
    let base_commit = std::fs::read_to_string(&commit).unwrap();
    for (file, base) in [(&worker, &base_worker), (&commit, &base_commit)] {
        std::fs::write(file, format!("{base}{tables}")).unwrap();
    }
    let published = writer(&commit, &keys)
        .args(["writer", "commit"])
        .output()
        .unwrap();
    assert!(published.status.success(), "{}", report(&published));

    // A deployment with API keys and no OIDC starts and serves. The reader has
    // none of the secrets in its environment.
    let shipped_config = shipped(&worker, &root);
    let mut reader = Reader::start_appended(&shipped_config, &root, &[], |address, base| {
        format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\noperator_credential={{env='FYRR_HTTP_OPERATOR'}}\nuser_requests_per_minute=1000\ngroup_requests_per_minute=1000\n")
    })
    .await;
    let hits = |result: &Value| -> Vec<String> {
        assert_eq!(result["isError"], false, "{result}");
        result["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
            .collect()
    };
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    let one = reader
        .call(&keys[0].secret, 1, "search", search.clone())
        .await;
    assert_eq!(hits(&one), [format!("{namespace}-http-1")]);
    let two = reader
        .call(&keys[1].secret, 2, "search", search.clone())
        .await;
    assert_eq!(hits(&two), [format!("{namespace}-http-2")]);
    // The `user` key acts as that directory user: it reads their document.
    let user = reader
        .call(&keys[4].secret, 3, "search", search.clone())
        .await;
    assert_eq!(hits(&user), [format!("{namespace}-http-1")]);
    // A document outside a key's grants answers like one that does not exist.
    let fetch = |id: String| json!({"source_id":"incremental","root_id":id});
    let outside = reader
        .call(
            &keys[0].secret,
            4,
            "fetch",
            fetch(format!("{namespace}-http-0")),
        )
        .await;
    let missing = reader
        .call(
            &keys[0].secret,
            5,
            "fetch",
            fetch(format!("{namespace}-http-9")),
        )
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
        normal(&outside, &format!("{namespace}-http-0")),
        normal(&missing, &format!("{namespace}-http-9"))
    );
    let own = reader
        .call(
            &keys[0].secret,
            6,
            "fetch",
            fetch(format!("{namespace}-http-1")),
        )
        .await;
    assert_ne!(
        normal(&own, &format!("{namespace}-http-1")),
        normal(&missing, &format!("{namespace}-http-9"))
    );

    // Wrong, malformed, revoked and expired keys are one and the same 401, with a
    // challenge that names no authorization server.
    let wrong = refusal(&reader, &secret(99)).await;
    assert_eq!(wrong.0, 401);
    assert_eq!(wrong.2.as_deref(), Some("Bearer"));
    for malformed in [
        "fyrr_key_short".to_owned(),
        format!("{}!", secret(11)),
        "fyrr_key_".to_owned(),
    ] {
        assert_eq!(refusal(&reader, &malformed).await, wrong, "{malformed}");
    }
    assert_eq!(refusal(&reader, "other-token").await, wrong);
    let anonymous = reader
        .client
        .post(format!("{}/mcp", reader.base))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 401);
    assert_eq!(anonymous.headers()["www-authenticate"], "Bearer");
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
    // A cancellation with a key nobody bound is refused as well.
    let cancel = reader
        .client
        .post(format!("{}/mcp", reader.base))
        .bearer_auth(secret(99))
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}))
        .send()
        .await
        .unwrap();
    assert_eq!(cancel.status(), 401);

    // Each key has its own allowance: `limited` allows three requests a minute.
    for id in 0..3 {
        assert_eq!(
            reader
                .rpc(&keys[2].secret, 20 + id, "tools/list", json!({}))
                .await
                .status(),
            200
        );
    }
    assert_eq!(
        reader
            .rpc(&keys[2].secret, 24, "tools/list", json!({}))
            .await
            .status(),
        429
    );
    assert_eq!(
        reader
            .rpc(&keys[0].secret, 25, "tools/list", json!({}))
            .await
            .status(),
        200
    );

    // Before its expiry the short-lived key is served; the expiry itself is
    // checked at the end, because moving the test clock also moves the OIDC
    // tokens' validity window out of the issuer's reach.
    assert_eq!(
        reader
            .rpc(&keys[3].secret, 30, "tools/list", json!({}))
            .await
            .status(),
        200
    );

    // Removing a key and republishing the identity refuses it once the reader
    // has read the new head: an operator refreshes the reader to make that now.
    assert_eq!(
        reader
            .rpc(&keys[1].secret, 40, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    let remaining: String = keys
        .iter()
        .filter(|k| k.name != "nm-two-q7x")
        .map(Key::table)
        .collect();
    std::fs::write(&commit, format!("{base_commit}{remaining}")).unwrap();
    let revoked = writer(&commit, &keys)
        .args(["writer", "commit"])
        .output()
        .unwrap();
    assert!(revoked.status.success(), "{}", report(&revoked));
    refresh(&reader).await;
    assert_eq!(refusal(&reader, &keys[1].secret).await, wrong, "revoked");
    assert_eq!(
        reader
            .rpc(&keys[0].secret, 41, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    // Nothing the reader wrote to stderr holds a secret, a key's name, or the
    // hash of either: those are the subject and the principal of its requests.
    let log = reader.stderr_text();
    let cfg = fyrr_core::config::load(&worker).unwrap().config;
    for key in &keys {
        let subject = api_key_subject(&cfg, &key.secret).unwrap();
        let principal = api_key_principal(&cfg, key.name).unwrap();
        for text in [
            key.secret.as_str(),
            &key.secret["fyrr_key_".len()..],
            key.name,
            &subject,
            &subject["key:".len()..],
            &principal,
            &principal["key:".len()..],
        ] {
            assert!(!log.contains(text), "the reader's log holds {text}");
        }
    }
    reader.stop().await;

    // With OIDC as well, an access token is still accepted next to a key, and
    // the OIDC metadata and challenge stay.
    let mut both = Reader::start(&worker, &root, &issuer, 1000, 1000).await;
    let token = issuer.token("subject-1", json!({}), json!({}));
    assert_eq!(
        both.rpc(&token, 50, "tools/list", json!({})).await.status(),
        200
    );
    assert_eq!(
        both.rpc(&keys[0].secret, 51, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    let refused = refusal(&both, &secret(99)).await;
    assert_eq!(refused.0, 401);
    assert!(refused.2.unwrap().contains("resource_metadata"));
    let metadata = both
        .client
        .get(format!(
            "{}/.well-known/oauth-protected-resource",
            both.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(metadata.status(), 200);
    // Expiry is enforced by the reader at request time, with nothing
    // republished. The test clock moves every process of this fixture at once.
    fyrr_core::clock::advance(Duration::from_secs(121)).unwrap();
    let expired = refusal(&both, &keys[3].secret).await;
    assert_eq!((expired.0, &expired.1), (wrong.0, &wrong.1), "expired");
    // The other keys carry no expiry.
    assert_eq!(
        both.rpc(&keys[0].secret, 52, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    both.stop().await;
}

/// The reader's HTTP table for a deployment without OIDC.
fn key_http(address: &std::net::SocketAddr, base: &str) -> String {
    format!(
        "\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\noperator_credential={{env='FYRR_HTTP_OPERATOR'}}\nuser_requests_per_minute=1000\ngroup_requests_per_minute=1000\n"
    )
}

/// `grant` text that expires `seconds` from the test clock's now.
fn expiring(readers: &str, seconds: i64) -> &'static str {
    let at = fyrr_core::clock::now() + chrono::Duration::seconds(seconds);
    let at = at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    Box::leak(format!("readers=['{readers}']\nexpires={at}").into_boxed_str())
}

/// A published fixture: documents readable by `usr:u0`..`usr:u3`, a snapshot
/// directory that has completed a pull (so the head carries a freshness
/// timestamp), and an OIDC issuer for the subjects `subject-0`..`subject-3`.
struct Deployment {
    issuer: Issuer,
    _issuer_root: FixtureRoot,
    worker: PathBuf,
    commit: PathBuf,
    root: FixtureRoot,
    namespace: String,
    snapshot: DirectorySnapshot,
    base_worker: String,
    base_commit: String,
}
impl Deployment {
    async fn new() -> Self {
        let (_, issuer_root) = setup();
        let issuer = Issuer::new(&issuer_root).await;
        let (worker, commit, root, snapshot) = fixture(&issuer).await;
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
            snapshot,
        }
    }

    /// Declares exactly `keys` in both configurations and commits the identity.
    fn publish(&self, keys: &[Key]) -> Output {
        let tables: String = keys.iter().map(Key::table).collect();
        for (file, base) in [
            (&self.worker, &self.base_worker),
            (&self.commit, &self.base_commit),
        ] {
            std::fs::write(file, format!("{base}{tables}")).unwrap();
        }
        writer(&self.commit, keys)
            .args(["writer", "commit"])
            .output()
            .unwrap()
    }

    fn doc(&self, i: usize) -> String {
        format!("{}-http-{i}", self.namespace)
    }

    /// The reader's file with no `[identity.directory]`: the Lambda shape.
    fn without_directory(&self, path: &Path) -> PathBuf {
        let text = std::fs::read_to_string(path).unwrap();
        let start = text
            .find("\n[identity.directory]\nprovider='snapshot'")
            .expect("the fixture's directory table");
        let end = start
            + text[start..]
                .find("sync_interval='5m'\n")
                .expect("the table's last key")
            + "sync_interval='5m'\n".len();
        let stripped = self.root.join("reader-without-directory.toml");
        std::fs::write(&stripped, format!("{}{}", &text[..start], &text[end..])).unwrap();
        stripped
    }
}

/// A storage boundary between a reader and its store: it counts reads, can
/// make the published directory read look two hours old, and can hold one read.
struct Boundary {
    config: PathBuf,
    reads: Arc<AtomicUsize>,
    stale: Arc<AtomicBool>,
    held: Arc<std::sync::Mutex<Option<BoundaryHook>>>,
    _proxy: ProxyGuard,
}
impl Boundary {
    fn new(config: &Path, root: &Path) -> Self {
        Self::build(config, root, false)
    }

    /// A boundary in front of a reader as it ships: no head read at every
    /// authorization check.
    fn shipped(config: &Path, root: &Path) -> Self {
        Self::build(config, root, true)
    }

    fn build(config: &Path, root: &Path, shipped: bool) -> Self {
        let loaded = fyrr_core::config::load(config).unwrap();
        let target = loaded
            .config
            .storage
            .endpoint
            .clone()
            .unwrap_or_else(|| "local-storage".into());
        let reads = Arc::new(AtomicUsize::new(0));
        let stale = Arc::new(AtomicBool::new(false));
        let held: Arc<std::sync::Mutex<Option<BoundaryHook>>> = Arc::default();
        let hook: BoundaryHook = {
            let (reads, held) = (reads.clone(), held.clone());
            Arc::new(move |request: &str, body: &[u8], after: bool| {
                if !after && (request.starts_with("GET ") || request.starts_with("HEAD ")) {
                    reads.fetch_add(1, Ordering::SeqCst);
                }
                let gate = held.lock().unwrap().clone();
                gate.is_some_and(|gate| gate(request, body, after))
            })
        };
        let transform: ResponseTransform = {
            let stale = stale.clone();
            Arc::new(move |request, response| {
                if stale.load(Ordering::SeqCst)
                    && request.starts_with("GET ")
                    && request.contains("/head.json ")
                {
                    super::corrections::rewrite_ok_body(response, |body| {
                        let mut head: ServingHead = serde_json::from_slice(body).unwrap();
                        head.freshness.directory =
                            Some(fyrr_core::clock::now() - chrono::Duration::hours(2));
                        *body = serde_json::to_vec(&head).unwrap();
                        true
                    });
                }
            })
        };
        let (endpoint, proxy) = super::corrections::proxy_transform(
            target.clone(),
            Arc::new(AtomicBool::new(false)),
            transform,
            Some(hook),
        );
        // No mirror: every search reads the store, so a request can be held there.
        let mut text = std::fs::read_to_string(config).unwrap();
        if shipped {
            text = text.replace("# fyrr-test-head-check = every", "");
        }
        let text = text
            .replace(&target, &endpoint)
            .replace(
                "[reader]",
                "[reader]\nconcurrency=1\nversion_check_interval='10m'",
            )
            .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 0");
        let observed = root.join("boundary.toml");
        std::fs::write(&observed, text).unwrap();
        Self {
            config: observed,
            reads,
            stale,
            held,
            _proxy: proxy,
        }
    }

    /// Holds the next response at the point where the reader hands it to the
    /// transport, after it is fully built and its last expiry check is the only
    /// one left, until the gate is released.
    fn hold_reply(&self) -> RequestGate {
        self.hold(|request| request.starts_with("REPLY ") && request.contains("/handoff "))
    }

    /// Holds the next request that reaches its reranker body authorization.
    fn hold_rerank_body(&self) -> RequestGate {
        self.hold(|request| {
            request.starts_with("RERANK ") && request.contains("/body-authorization ")
        })
    }

    fn hold(&self, matches: impl Fn(&str) -> bool + Send + Sync + 'static) -> RequestGate {
        let (gate, hook) = RequestGate::new(move |request, _, after| !after && matches(request));
        *self.held.lock().unwrap() = Some(hook);
        gate
    }

    /// Waits until the reader has stopped reading on its own.
    async fn quiet(&self) {
        let mut last = self.reads.load(Ordering::SeqCst);
        let mut still = 0;
        for _ in 0..600 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = self.reads.load(Ordering::SeqCst);
            still = if now == last { still + 1 } else { 0 };
            last = now;
            if still >= 8 {
                return;
            }
        }
        panic!("the reader never stopped reading");
    }
}

/// One tool call on its own task, so a test can act while the reader holds it.
fn call_task(
    reader: &Reader,
    token: &str,
    id: u64,
    name: &str,
    args: Value,
) -> tokio::task::JoinHandle<Value> {
    let client = reader.client.clone();
    let url = format!("{}/mcp", reader.base);
    let (token, name) = (token.to_owned(), name.to_owned());
    tokio::spawn(async move {
        let response = client
            .post(url)
            .bearer_auth(token)
            .timeout(Duration::from_secs(60))
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", "2025-11-25")
            .json(&json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":args}}))
            .send()
            .await
            .unwrap();
        if response.status() != 200 {
            let status = response.status().as_u16();
            return json!({"isError":true,"http_status":status,"body":response.text().await.unwrap()});
        }
        response.json::<Value>().await.unwrap()["result"].clone()
    })
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

// A gate blocks its thread while a request is held, so the task that sends the
// request needs a thread of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real HTTP MCP executable and local storage; scripts/ci"]
async fn api_key_expiry_applies_to_a_request_admitted_just_before_it_on_storage() {
    let keys = [
        Key {
            name: "nm-search-q7x",
            variable: "FYRR_TEST_API_KEY_SEARCH",
            secret: secret(21),
            grant: expiring("usr:u1", 1200),
        },
        Key {
            name: "nm-fetch-q7x",
            variable: "FYRR_TEST_API_KEY_FETCH",
            secret: secret(22),
            grant: expiring("usr:u1", 1500),
        },
    ];
    let deployment = Deployment::new().await;
    let published = deployment.publish(&keys);
    assert!(published.status.success(), "{}", report(&published));
    let boundary = Boundary::new(&deployment.worker, &deployment.root);
    let reader = Reader::start_appended(&boundary.config, &deployment.root, &[], key_http).await;
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    let fetch = json!({"source_id":"incremental","root_id":deployment.doc(1)});
    let hits = [deployment.doc(1)];
    assert_eq!(
        roots(
            &reader
                .call(&keys[0].secret, 1, "search", search.clone())
                .await
        ),
        hits
    );
    boundary.quiet().await;

    // Held where the finished response is handed to the transport: released
    // unchanged, the request is served.
    let gate = boundary.hold_reply();
    let held = call_task(&reader, &keys[0].secret, 2, "search", search.clone());
    gate.entered();
    gate.release();
    assert_eq!(roots(&held.await.unwrap()), hits);
    boundary.quiet().await;

    // The same request, admitted before `expires` and with its response built,
    // is held while the clock passes it: nothing leaves, and the next request
    // does not get past admission.
    let gate = boundary.hold_reply();
    let held = call_task(&reader, &keys[0].secret, 3, "search", search.clone());
    gate.entered();
    fyrr_core::clock::advance(Duration::from_secs(1201)).unwrap();
    gate.release();
    let refused = held.await;
    let refused = refused.unwrap();
    assert_eq!(refused["isError"], true, "{refused}");
    assert_eq!(
        refused["structuredContent"]["code"],
        "authorization_unavailable"
    );
    assert!(refused["structuredContent"]["hits"].is_null(), "{refused}");
    assert_eq!(
        refusal(&reader, &keys[0].secret).await.0,
        401,
        "expired before admission"
    );
    boundary.quiet().await;

    // The same for a fetch with the second key, which is still valid.
    let own = reader
        .call(&keys[1].secret, 4, "fetch", fetch.clone())
        .await;
    assert_eq!(own["isError"], false, "{own}");
    let gate = boundary.hold_reply();
    let held = call_task(&reader, &keys[1].secret, 5, "fetch", fetch.clone());
    gate.entered();
    fyrr_core::clock::advance(Duration::from_secs(300)).unwrap();
    gate.release();
    let refused = held.await.unwrap();
    assert_eq!(refused["isError"], true, "{refused}");
    assert_eq!(
        refused["structuredContent"]["code"],
        "authorization_unavailable"
    );
    assert!(
        refused["structuredContent"]["documents"].is_null(),
        "{refused}"
    );
    assert_eq!(refusal(&reader, &keys[1].secret).await.0, 401);
    drop(gate);
    let mut reader = reader;
    reader.stop().await;
}

/// A reranker endpoint that records every request it receives in full. The
/// connection of the request numbered `parked` (counting from 1) is accepted
/// and then left unread until `read` has a permit, as a slow connection setup.
struct Reranker {
    endpoint: String,
    bodies: Arc<std::sync::Mutex<Vec<String>>>,
    accepted: Arc<tokio::sync::Notify>,
    read: Arc<tokio::sync::Semaphore>,
    task: tokio::task::JoinHandle<()>,
}
impl Reranker {
    async fn start(parked: usize) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/rerank", socket.local_addr().unwrap());
        let bodies: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let accepted = Arc::new(tokio::sync::Notify::new());
        let read = Arc::new(tokio::sync::Semaphore::new(0));
        let task = tokio::spawn({
            let (bodies, accepted, read) = (bodies.clone(), accepted.clone(), read.clone());
            async move {
                for number in 1.. {
                    let (mut connection, _) = socket.accept().await.unwrap();
                    if number == parked {
                        accepted.notify_one();
                        read.acquire().await.unwrap().forget();
                    }
                    let mut received = Vec::new();
                    // The request body is chunked: read to its terminating chunk,
                    // or until the reader gives the connection up.
                    let _ = tokio::time::timeout(Duration::from_secs(20), async {
                        loop {
                            let mut chunk = [0; 4096];
                            let count = connection.read(&mut chunk).await.unwrap_or(0);
                            if count == 0 {
                                break;
                            }
                            received.extend_from_slice(&chunk[..count]);
                            if received.ends_with(b"\r\n0\r\n\r\n") {
                                break;
                            }
                        }
                    })
                    .await;
                    let text = String::from_utf8_lossy(&received).into_owned();
                    let documents = text.matches("fixture document").count();
                    bodies.lock().unwrap().push(text);
                    let results: Vec<_> = (0..documents.max(1))
                        .map(|index| json!({"index":index,"relevance_score":index as f64}))
                        .collect();
                    let body = json!({"data": results}).to_string();
                    let _ = connection
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await;
                }
            }
        });
        Self {
            endpoint,
            bodies,
            accepted,
            read,
            task,
        }
    }
    fn texts(&self) -> usize {
        self.bodies
            .lock()
            .unwrap()
            .iter()
            .map(|body| body.matches("fixture document").count())
            .sum()
    }
}
impl Drop for Reranker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real HTTP MCP executable, local reranker and local storage; scripts/ci"]
async fn api_key_expiry_while_the_reranker_connection_is_set_up_sends_no_text_on_storage() {
    // Document text is sent to the reranker for the first request and, for the
    // second, only its connection is set up: the reader never gets to the body.
    let reranker = Reranker::start(2).await;
    let keys = [Key {
        name: "nm-rerank-q7x",
        variable: "FYRR_TEST_API_KEY_RERANK",
        secret: secret(41),
        grant: expiring("usr:u1", 1200),
    }];
    let mut deployment = Deployment::new().await;
    // Retrieval settings are publication policy, so both files carry them.
    let rerank = format!(
        "\n[namespace.rerank]\nendpoint='{}'\nmodel='fixture-rerank'\napi_key={{env='FYRR_TEST_SECRET'}}\ncandidates=10\nweight=1.0\ndeadline='300s'\nspend_limit_microusd=1000\nmax_call_cost_microusd=10\n",
        reranker.endpoint
    );
    deployment.base_worker.push_str(&rerank);
    deployment.base_commit.push_str(&rerank);
    let published = deployment.publish(&keys);
    assert!(published.status.success(), "{}", report(&published));
    let boundary = Boundary::new(&deployment.worker, &deployment.root);
    let reader = Reader::start_appended(&boundary.config, &deployment.root, &[], key_http).await;
    let search = json!({"query":"fixture document","sources":["incremental"],"limit":5,"deadline_ms":fyrr_core::limits::mcp::MAX_DEADLINE_MS});
    let served = reader
        .call(&keys[0].secret, 1, "search", search.clone())
        .await;
    assert_eq!(served["isError"], false, "{served}");
    assert!(reranker.texts() > 0, "the reranker received candidate text");
    let before = reranker.texts();
    boundary.quiet().await;

    // Held at the reranker's body authorization with the connection accepted
    // and unread, as the clock passes the key's expiry.
    let gate = boundary.hold_rerank_body();
    let held = call_task(&reader, &keys[0].secret, 2, "search", search.clone());
    gate.entered();
    tokio::time::timeout(Duration::from_secs(60), reranker.accepted.notified())
        .await
        .unwrap();
    fyrr_core::clock::advance(Duration::from_secs(1201)).unwrap();
    gate.release();
    reranker.read.add_permits(1);
    let refused = held.await.unwrap();
    assert_eq!(refused["isError"], true, "{refused}");
    assert_eq!(
        refused["structuredContent"]["code"],
        "authorization_unavailable"
    );
    assert!(refused["structuredContent"]["hits"].is_null(), "{refused}");
    assert_eq!(reranker.texts(), before, "no text after the key expired");
    let mut reader = reader;
    reader.stop().await;
}

#[tokio::test]
#[ignore = "real writer, HTTP MCP executable and local storage; scripts/ci"]
async fn removing_the_last_api_key_publishes_no_binding_and_refuses_the_key_on_storage() {
    let keys = [Key {
        name: "nm-last-q7x",
        variable: "FYRR_TEST_API_KEY_LAST",
        secret: secret(31),
        grant: "readers=['usr:u1']",
    }];
    let deployment = Deployment::new().await;
    let shared = |keys: &[&Key]| {
        // One file for the writer and the reader: API keys and no OIDC.
        let tables: String = keys.iter().map(|key| key.table()).collect();
        std::fs::write(
            &deployment.commit,
            format!(
                "{}{tables}\n[reader.http]\nlisten='127.0.0.1:1'\npublic_url='http://127.0.0.1:1/mcp'\n",
                deployment.base_commit
            ),
        )
        .unwrap();
    };
    shared(&[&keys[0]]);
    let committed = writer(&deployment.commit, &keys)
        .args(["writer", "commit"])
        .output()
        .unwrap();
    assert!(committed.status.success(), "{}", report(&committed));
    // The reader runs on the old file: it keeps the key declared.
    let tables: String = keys.iter().map(Key::table).collect();
    std::fs::write(
        &deployment.worker,
        format!("{}{tables}", deployment.base_worker),
    )
    .unwrap();
    let reader = Reader::start_appended(
        &shipped(&deployment.worker, &deployment.root),
        &deployment.root,
        &[],
        key_http,
    )
    .await;
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    assert_eq!(
        roots(
            &reader
                .call(&keys[0].secret, 1, "search", search.clone())
                .await
        ),
        [deployment.doc(1)]
    );

    // The key is removed from the shared file, which then has no way to
    // authenticate an HTTP reader. The writer does not need one: it commits.
    shared(&[]);
    let (cfg, before) = {
        let (_, before) = artifact(&deployment.commit).await;
        (
            fyrr_core::config::load(&deployment.commit).unwrap().config,
            before,
        )
    };
    let bound = api_key_subject(&cfg, &keys[0].secret).unwrap();
    assert!(before.subjects.iter().any(|b| b.subject == bound));
    let revoked = writer(&deployment.commit, &[])
        .args(["writer", "commit"])
        .output()
        .unwrap();
    assert!(revoked.status.success(), "{}", report(&revoked));
    let (_, after) = artifact(&deployment.commit).await;
    assert!(
        after.subjects.iter().all(|b| b.subject != bound),
        "the last key is no longer bound"
    );
    let status = writer(&deployment.commit, &[])
        .args(["writer", "status"])
        .output()
        .unwrap();
    assert!(status.status.success(), "{}", report(&status));
    // The running reader refuses the key once an operator has refreshed it.
    refresh(&reader).await;
    assert_eq!(refusal(&reader, &keys[0].secret).await.0, 401);
    // `config check` says what a restarted reader would do, and the reader does it.
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
    let mut reader = reader;
    reader.stop().await;
}

/// The rule without an operator: a reader that is not refreshed applies a
/// published removal by itself, within two version-check intervals.
#[tokio::test]
#[ignore = "real writer, HTTP MCP executable and local storage; scripts/ci"]
async fn a_removed_api_key_is_refused_within_two_version_check_intervals_without_a_refresh_on_storage()
 {
    let keys = [
        Key {
            name: "nm-stays-q7x",
            variable: "FYRR_TEST_API_KEY_STAYS",
            secret: secret(81),
            grant: "readers=['usr:u1']",
        },
        Key {
            name: "nm-goes-q7x",
            variable: "FYRR_TEST_API_KEY_GOES",
            secret: secret(82),
            grant: "readers=['usr:u2']",
        },
    ];
    let deployment = Deployment::new().await;
    let published = deployment.publish(&keys);
    assert!(published.status.success(), "{}", report(&published));
    let config = shipped(&deployment.worker, &deployment.root);
    std::fs::write(
        &config,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("[reader]", "[reader]\nversion_check_interval='1s'"),
    )
    .unwrap();
    let mut reader = Reader::start_appended(&config, &deployment.root, &[], key_http).await;
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    for (key, doc) in [(&keys[0], 1), (&keys[1], 2)] {
        assert_eq!(
            roots(&reader.call(&key.secret, 1, "search", search.clone()).await),
            [deployment.doc(doc)]
        );
    }
    let published = deployment.publish(&keys[..1]);
    assert!(published.status.success(), "{}", report(&published));
    let removed = std::time::Instant::now();
    // Two intervals are two seconds; the rest is the scheduler's and the
    // machine's slack.
    let wrong = refusal(&reader, &secret(999)).await;
    while refusal(&reader, &keys[1].secret).await != wrong {
        assert!(
            removed.elapsed() < Duration::from_secs(20),
            "the removed key was still served {:?} after its removal was published",
            removed.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(
        roots(
            &reader
                .call(&keys[0].secret, 2, "search", search.clone())
                .await
        ),
        [deployment.doc(1)]
    );
    reader.stop().await;
}

#[tokio::test]
#[ignore = "real HTTP MCP executable and local storage; scripts/ci"]
async fn garbage_api_keys_cost_no_storage_reads_and_hide_a_stale_directory_on_storage() {
    let keys = [
        Key {
            name: "nm-first-q7x",
            variable: "FYRR_TEST_API_KEY_FIRST",
            secret: secret(41),
            grant: "readers=['usr:u1']",
        },
        Key {
            name: "nm-second-q7x",
            variable: "FYRR_TEST_API_KEY_SECOND",
            secret: secret(42),
            grant: "readers=['usr:u2']",
        },
    ];
    let deployment = Deployment::new().await;
    let published = deployment.publish(&keys[..1]);
    assert!(published.status.success(), "{}", report(&published));
    let boundary = Boundary::shipped(&deployment.worker, &deployment.root);
    let reader = Reader::start_appended(&boundary.config, &deployment.root, &[], key_http).await;
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    assert_eq!(
        roots(
            &reader
                .call(&keys[0].secret, 1, "search", search.clone())
                .await
        ),
        [deployment.doc(1)]
    );
    boundary.quiet().await;

    // A flood of well-shaped keys nobody bound: each is the same 401 and none
    // reads storage. The reader's own version check, once per 30 s at most, is
    // the only read a quiet reader makes.
    let wrong = refusal(&reader, &secret(900)).await;
    assert_eq!(wrong.0, 401);
    boundary.quiet().await;
    let before = boundary.reads.load(Ordering::SeqCst);
    let started = std::time::Instant::now();
    for n in 0..60 {
        assert_eq!(refusal(&reader, &secret(1000 + n)).await, wrong);
    }
    let checks = 1 + started.elapsed().as_secs() as usize / 30;
    let reads = boundary.reads.load(Ordering::SeqCst) - before;
    assert!(
        reads <= 3 * checks,
        "{reads} storage reads for 60 unknown keys in {:?}",
        started.elapsed()
    );
    // A key the identity binds is checked against the held head, so it reads
    // nothing either.
    boundary.quiet().await;
    let before = boundary.reads.load(Ordering::SeqCst);
    let started = std::time::Instant::now();
    for id in 0..5 {
        assert_eq!(
            reader
                .rpc(&keys[0].secret, 100 + id, "tools/list", json!({}))
                .await
                .status(),
            200
        );
    }
    let checks = 1 + started.elapsed().as_secs() as usize / 30;
    let reads = boundary.reads.load(Ordering::SeqCst) - before;
    assert!(
        reads <= 3 * checks,
        "{reads} storage reads for 5 requests of a bound key"
    );

    // A key published while the reader runs is unknown to it until its next
    // version check; an operator makes that now.
    let published = deployment.publish(&keys);
    assert!(published.status.success(), "{}", report(&published));
    refresh(&reader).await;
    assert_eq!(
        reader
            .rpc(&keys[1].secret, 200, "tools/list", json!({}))
            .await
            .status(),
        200
    );

    // The directory read the head attests expires: a bound key learns that
    // (503), a caller with a random key learns nothing (the same 401).
    let (head, _) = artifact(&deployment.commit).await;
    assert!(head.freshness.directory.is_some());
    boundary.stale.store(true, Ordering::SeqCst);
    refresh(&reader).await;
    assert_eq!(refusal(&reader, &secret(2000)).await, wrong);
    assert_eq!(refusal(&reader, "fyrr_key_short").await, wrong);
    let learned = refusal(&reader, &keys[0].secret).await;
    assert_eq!(learned.0, 503, "{learned:?}");
    assert!(
        learned.1.contains("authorization_unavailable"),
        "{learned:?}"
    );
    boundary.stale.store(false, Ordering::SeqCst);
    refresh(&reader).await;
    assert_eq!(
        reader
            .rpc(&keys[0].secret, 300, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    let mut reader = reader;
    reader.stop().await;
}

#[tokio::test]
#[ignore = "real HTTP MCP executable and local storage; scripts/ci"]
async fn a_reader_without_a_directory_provider_still_refuses_a_stale_published_directory_on_storage()
 {
    let keys = [
        Key {
            name: "nm-user-q7x",
            variable: "FYRR_TEST_API_KEY_USER_STALE",
            secret: secret(51),
            grant: "user='http-1@example.test'",
        },
        Key {
            name: "nm-readers-q7x",
            variable: "FYRR_TEST_API_KEY_READERS_STALE",
            secret: secret(52),
            grant: "readers=['usr:u2']",
        },
    ];
    let deployment = Deployment::new().await;
    let published = deployment.publish(&keys);
    assert!(published.status.success(), "{}", report(&published));
    // The publication attests a directory read ...
    let (head, _) = artifact(&deployment.commit).await;
    assert!(head.freshness.directory.is_some());
    // ... and this reader's own file names no directory provider.
    let own = deployment.without_directory(&deployment.worker);
    let boundary = Boundary::new(&own, &deployment.root);
    assert!(
        !std::fs::read_to_string(&boundary.config)
            .unwrap()
            .contains("provider='snapshot'")
    );
    let reader = Reader::start(
        &boundary.config,
        &deployment.root,
        &deployment.issuer,
        1000,
        1000,
    )
    .await;
    let oidc = deployment.issuer.token("subject-1", json!({}), json!({}));
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    for (label, token) in [("user key", &keys[0].secret), ("OIDC", &oidc)] {
        assert_eq!(
            roots(&reader.call(token, 1, "search", search.clone()).await),
            [deployment.doc(1)],
            "{label}"
        );
    }
    boundary.stale.store(true, Ordering::SeqCst);
    for (label, token) in [("user key", &keys[0].secret), ("OIDC", &oidc)] {
        let response = reader.rpc(token, 2, "tools/list", json!({})).await;
        assert_eq!(
            response.status(),
            503,
            "{label} was served from an expired directory"
        );
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("authorization_unavailable")
        );
    }
    // A `readers` key is no directory subject, but the namespace is refused as a whole.
    assert_eq!(refusal(&reader, &keys[1].secret).await.0, 503);
    boundary.stale.store(false, Ordering::SeqCst);
    assert_eq!(
        reader
            .rpc(&keys[0].secret, 3, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    let mut reader = reader;
    reader.stop().await;
}

#[tokio::test]
#[ignore = "real writer, HTTP MCP executable and local storage; scripts/ci"]
async fn a_manual_directory_import_without_a_provider_withdraws_the_published_freshness_on_storage()
{
    let keys = [Key {
        name: "nm-manual-q7x",
        variable: "FYRR_TEST_API_KEY_MANUAL",
        secret: secret(61),
        grant: "user='http-1@example.test'",
    }];
    let deployment = Deployment::new().await;
    let published = deployment.publish(&keys);
    assert!(published.status.success(), "{}", report(&published));
    // The deployment ran a provider: its head carries a directory timestamp.
    let (head, _) = artifact(&deployment.commit).await;
    assert!(head.freshness.directory.is_some());
    let import = |config: &Path| {
        let mut snapshot = deployment.snapshot.clone();
        snapshot.built_at = fyrr_core::clock::now();
        let path = deployment.root.join("manual-directory.json");
        std::fs::write(&path, write_contract(&snapshot)).unwrap();
        let imported = keyed(host_command(config), &keys)
            .args(["writer", "directory", "--snapshot"])
            .arg(path)
            .output()
            .unwrap();
        assert!(imported.status.success(), "{}", report(&imported));
    };
    // A provider is configured: the import is no proof and keeps what the
    // provider attested.
    import(&deployment.commit);
    let (head, _) = artifact(&deployment.commit).await;
    assert!(head.freshness.directory.is_some());
    // The deployment moved to manual imports: its import attests nothing, and
    // the old timestamp does not outlive it.
    let manual = deployment.without_directory(&deployment.commit);
    import(&manual);
    let (head, _) = artifact(&manual).await;
    assert_eq!(head.freshness.directory, None);
    // A reader without a provider serves the namespace, however long ago the
    // provider last pulled: the head no longer carries a directory clock.
    let own = deployment.without_directory(&deployment.worker);
    let boundary = Boundary::new(&own, &deployment.root);
    let reader = Reader::start(
        &boundary.config,
        &deployment.root,
        &deployment.issuer,
        1000,
        1000,
    )
    .await;
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    assert_eq!(
        roots(&reader.call(&keys[0].secret, 1, "search", search).await),
        [deployment.doc(1)]
    );
    let mut reader = reader;
    reader.stop().await;
}

#[tokio::test]
#[ignore = "real writer, HTTP MCP executable and local storage; scripts/ci"]
async fn a_user_key_is_refused_once_a_directory_sync_deactivates_its_user_on_storage() {
    let keys = [
        Key {
            name: "nm-ada-q7x",
            variable: "FYRR_TEST_API_KEY_ADA",
            secret: secret(61),
            grant: "user='http-1@example.test'",
        },
        Key {
            name: "nm-other-q7x",
            variable: "FYRR_TEST_API_KEY_OTHER",
            secret: secret(62),
            grant: "user='http-2@example.test'",
        },
    ];
    let mut deployment = Deployment::new().await;
    let published = deployment.publish(&keys);
    assert!(published.status.success(), "{}", report(&published));
    let reader = Reader::start_appended(
        &shipped(&deployment.worker, &deployment.root),
        &deployment.root,
        &[],
        key_http,
    )
    .await;
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    for (key, doc) in [(&keys[0], 1), (&keys[1], 2)] {
        assert_eq!(
            roots(&reader.call(&key.secret, 1, "search", search.clone()).await),
            [deployment.doc(doc)]
        );
    }
    deployment.snapshot.users[1].active = false;
    deployment.snapshot.built_at = fyrr_core::clock::now();
    std::fs::write(
        deployment.root.join("http-directory.json"),
        write_contract(&deployment.snapshot),
    )
    .unwrap();
    let synced = writer(&deployment.commit, &keys)
        .args(["writer", "sync", "--directory"])
        .output()
        .unwrap();
    assert!(synced.status.success(), "{}", report(&synced));
    refresh(&reader).await;
    // The deactivated user's key is the same 401 as any unbound one; the other works.
    let wrong = refusal(&reader, &secret(999)).await;
    assert_eq!(refusal(&reader, &keys[0].secret).await, wrong);
    assert_eq!(
        roots(
            &reader
                .call(&keys[1].secret, 2, "search", search.clone())
                .await
        ),
        [deployment.doc(2)]
    );
    let mut reader = reader;
    reader.stop().await;
}

#[tokio::test]
#[ignore = "real writer, HTTP MCP executable and local storage; scripts/ci"]
async fn an_unusable_key_secret_skips_only_its_key_and_a_fetch_cursor_stays_with_its_key_on_storage()
 {
    let keys = [
        Key {
            name: "nm-good-q7x",
            variable: "FYRR_TEST_API_KEY_GOOD",
            secret: secret(71),
            grant: "readers=['usr:u1']",
        },
        Key {
            name: "nm-bad-q7x",
            variable: "FYRR_TEST_API_KEY_BAD",
            secret: "fyrr_key_too-short".into(),
            grant: "readers=['usr:u2']",
        },
        Key {
            name: "nm-unset-q7x",
            variable: "FYRR_TEST_API_KEY_UNSET",
            secret: secret(73),
            grant: "readers=['usr:u3']",
        },
        Key {
            name: "nm-twin-q7x",
            variable: "FYRR_TEST_API_KEY_TWIN2",
            secret: secret(74),
            grant: "readers=['usr:u1']",
        },
    ];
    let mut deployment = Deployment::new().await;
    // A document larger than one fetch page (64 KiB), readable by usr:u1. The
    // fixture caps documents at 64 bytes and knows no vector for new text.
    deployment.base_worker = deployment
        .base_worker
        .replace("document_max_bytes='64B'", "document_max_bytes='1MiB'");
    deployment.base_commit = deployment
        .base_commit
        .replace("document_max_bytes='64B'", "document_max_bytes='1MiB'");
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
    // The writer has every secret but the unset one's, and one is malformed.
    let tables: String = keys.iter().map(Key::table).collect();
    for (file, base) in [
        (&deployment.worker, &deployment.base_worker),
        (&deployment.commit, &deployment.base_commit),
    ] {
        std::fs::write(file, format!("{base}{tables}")).unwrap();
    }
    let present = [&keys[0], &keys[1], &keys[3]];
    let mut commit = host_command(&deployment.commit);
    for key in present {
        commit.env(key.variable, &key.secret);
    }
    let committed = commit.args(["writer", "commit"]).output().unwrap();
    assert!(committed.status.success(), "{}", report(&committed));
    let mut status = host_command(&deployment.commit);
    for key in present {
        status.env(key.variable, &key.secret);
    }
    let status = status.args(["writer", "status"]).output().unwrap();
    assert!(status.status.success(), "{}", report(&status));
    assert!(
        serde_json::from_slice::<Value>(&status.stdout).unwrap()["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w == "api_key_secret_unusable"),
        "{}",
        report(&status)
    );
    // `config check` reports the missing variable and the malformed secret by path.
    let mut check = worker_command(&deployment.commit);
    for key in present {
        check.env(key.variable, &key.secret);
    }
    let check = check.args(["config", "check"]).output().unwrap();
    assert_eq!(check.status.code(), Some(78), "{}", report(&check));
    let stderr = String::from_utf8_lossy(&check.stderr);
    assert!(
        stderr.contains("identity.api_keys[1].key") && stderr.contains("identity.api_keys[2].key")
    );
    assert!(!report(&check).contains("fyrr_key_too-short"));

    // The reader serves the good keys and an OIDC subject; the bad ones get the 401.
    let mut reader = Reader::start(
        &deployment.worker,
        &deployment.root,
        &deployment.issuer,
        1000,
        1000,
    )
    .await;
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    let good = roots(
        &reader
            .call(&keys[0].secret, 1, "search", search.clone())
            .await,
    );
    assert!(good.contains(&deployment.doc(1)) && !good.contains(&deployment.doc(2)));
    let oidc = deployment.issuer.token("subject-2", json!({}), json!({}));
    assert_eq!(
        roots(&reader.call(&oidc, 2, "search", search.clone()).await),
        [deployment.doc(2)]
    );
    let wrong = refusal(&reader, &secret(999)).await;
    for key in [&keys[1], &keys[2]] {
        assert_eq!(refusal(&reader, &key.secret).await, wrong, "{}", key.name);
    }

    // A fetch cursor is bound to the principal it was issued to: another key
    // with the same grants cannot continue it, and nobody can once its key is removed.
    let first = reader
        .call(
            &keys[0].secret,
            3,
            "fetch",
            json!({"source_id":"incremental","root_id":big}),
        )
        .await;
    let cursor = first["structuredContent"]["next_cursor"]
        .as_str()
        .unwrap_or_else(|| panic!("a second page: {first}"))
        .to_owned();
    let next = reader
        .call(&keys[0].secret, 4, "fetch", json!({"cursor":cursor}))
        .await;
    assert_eq!(next["isError"], false, "{next}");
    let other = reader
        .call(&keys[3].secret, 5, "fetch", json!({"cursor":cursor}))
        .await;
    assert_eq!(other["isError"], true, "{other}");
    assert_eq!(other["structuredContent"]["code"], "stale_cursor");
    let remaining: Vec<&Key> = vec![&keys[1], &keys[3]];
    let tables: String = remaining.iter().map(|key| key.table()).collect();
    std::fs::write(
        &deployment.commit,
        format!("{}{tables}", deployment.base_commit),
    )
    .unwrap();
    let mut revoke = host_command(&deployment.commit);
    for key in present {
        revoke.env(key.variable, &key.secret);
    }
    let revoked = revoke.args(["writer", "commit"]).output().unwrap();
    assert!(revoked.status.success(), "{}", report(&revoked));
    let response = reader
        .rpc(
            &keys[0].secret,
            6,
            "tools/call",
            json!({"name":"fetch","arguments":{"cursor":cursor}}),
        )
        .await;
    assert_eq!(
        response.status(),
        401,
        "a removed key's cursor is no credential"
    );
    reader.stop().await;
}
