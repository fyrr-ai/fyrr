//! API keys through the real writer and reader executables: the identity the
//! writer publishes, and what the HTTP reader admits with it, with and
//! without OIDC.
use super::coordination::{host_command, worker_command};
use super::http_mcp::{Issuer, Reader, fixture};
use super::identity::{snapshot, user};
use super::*;
use fyrr_core::directory::DirectorySnapshot;
use fyrr_reader::storage::{api_key_principal, api_key_subject};
use std::collections::BTreeSet;
use std::time::Duration;

/// A well-formed secret that is distinct per `n`.
fn secret(n: usize) -> String {
    format!("fyrr_key_{:A<43}", format!("test{n}"))
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

    // A secret that is shared, malformed or missing fails the commit; nothing is published.
    let generation = artifact(&config).await.0.identity.generation;
    let broken = |label: &str, change: &dyn Fn(&mut Command)| {
        let mut command = writer(&config, &keys);
        change(&mut command);
        let output = command.args(["writer", "commit"]).output().unwrap();
        assert!(!output.status.success(), "{label}: {}", report(&output));
        let text = report(&output);
        for key in &keys {
            assert!(!text.contains(&key.secret), "{label} echoes a secret");
        }
    };
    broken("shared", &|c| {
        c.env("FYRR_TEST_API_KEY_GONE", secret(1));
    });
    broken("malformed", &|c| {
        c.env("FYRR_TEST_API_KEY_GONE", "fyrr_key_too-short");
    });
    broken("missing", &|c| {
        c.env_remove("FYRR_TEST_API_KEY_GONE");
    });
    assert_eq!(artifact(&config).await.0.identity.generation, generation);
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
    let expiry = fyrr_core::clock::now() + chrono::Duration::seconds(75);
    let expiry_text = expiry.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let expiring_grant: &'static str =
        Box::leak(format!("readers=['usr:u0']\nexpires={expiry_text}").into_boxed_str());
    let keys = [
        Key {
            name: "reader-one",
            variable: "FYRR_TEST_API_KEY_ONE",
            secret: secret(11),
            grant: "readers=['usr:u1']",
        },
        Key {
            name: "reader-two",
            variable: "FYRR_TEST_API_KEY_TWO",
            secret: secret(12),
            grant: "readers=['usr:u2']",
        },
        Key {
            name: "limited",
            variable: "FYRR_TEST_API_KEY_LIMITED",
            secret: secret(13),
            grant: "readers=['usr:u3']\nrequests_per_minute=3",
        },
        Key {
            name: "short-lived",
            variable: "FYRR_TEST_API_KEY_SHORT",
            secret: secret(14),
            grant: expiring_grant,
        },
        Key {
            name: "http-user",
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
    let published_at = std::time::Instant::now();

    // A deployment with API keys and no OIDC starts and serves. The reader has
    // none of the secrets in its environment.
    let mut reader = Reader::start_appended(&worker, &root, &[], |address, base| {
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

    // Expiry is enforced by the reader at request time, with nothing republished.
    assert!(
        published_at.elapsed() < Duration::from_secs(60),
        "the fixture was too slow for the expiry window"
    );
    assert_eq!(
        reader
            .rpc(&keys[3].secret, 30, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    let wait = (expiry - fyrr_core::clock::now())
        .to_std()
        .unwrap_or_default();
    tokio::time::sleep(wait + Duration::from_secs(2)).await;
    assert_eq!(refusal(&reader, &keys[3].secret).await, wrong, "expired");

    // Removing a key and republishing the identity refuses it on the next request.
    assert_eq!(
        reader
            .rpc(&keys[1].secret, 40, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    let remaining: String = keys
        .iter()
        .filter(|k| k.name != "reader-two")
        .map(Key::table)
        .collect();
    std::fs::write(&commit, format!("{base_commit}{remaining}")).unwrap();
    let revoked = writer(&commit, &keys)
        .args(["writer", "commit"])
        .output()
        .unwrap();
    assert!(revoked.status.success(), "{}", report(&revoked));
    assert_eq!(refusal(&reader, &keys[1].secret).await, wrong, "revoked");
    assert_eq!(
        reader
            .rpc(&keys[0].secret, 41, "tools/list", json!({}))
            .await
            .status(),
        200
    );
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
    both.stop().await;
}
