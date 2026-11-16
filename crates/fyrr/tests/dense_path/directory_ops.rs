//! Directory operations through the CLI, HTTP, MCP, Keycloak and RustFS.
use super::coordination::{host_command, publication};
use super::incremental::{fixture, publish, publish_initial};
use super::tools::Mcp;
use super::*;
use reqwest::{Client, Method};
use std::time::{Duration, Instant};
const USER: &str = "urn:ietf:params:scim:schemas:core:2.0:User";
const GROUP: &str = "urn:ietf:params:scim:schemas:core:2.0:Group";
const EXT: &str = "urn:fyrr:params:scim:schemas:extension:identity:2.0:User";
const SCIM_TOKEN: &str = "owned-fixture-scim-token-only";
const OP_TOKEN: &str = "owned-fixture-operator-token-only";

struct Service {
    child: std::process::Child,
    endpoint: String,
    client: Client,
}
impl Drop for Service {
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
impl Service {
    async fn start(config: &Path, root: &Path) -> Self {
        let address = free_address();
        let text = std::fs::read_to_string(config).unwrap();
        std::fs::write(config, format!("{text}\n[identity.directory]\nprovider='scim'\nissuer='https://issuer.one'\nverified_domains=['example.test']\ncredentials={{env='SCIM_TOKEN'}}\nsync_interval='5m'\n[writer.commit]\ncoalescing_timer='10m'\n[writer.serve.admin]\nlisten='{address}'\noperator_token={{env='OP_TOKEN'}}\n")).unwrap();
        let child = super::coordination::worker_command(config)
            .args(["writer", "serve", "--jobs", "1"])
            .env("SCIM_TOKEN", SCIM_TOKEN)
            .env("OP_TOKEN", OP_TOKEN)
            .stdout(std::fs::File::create(root.join("service.stdout")).unwrap())
            .stderr(std::fs::File::create(root.join("service.stderr")).unwrap())
            .spawn()
            .unwrap();
        let mut service = Self {
            child,
            endpoint: format!("http://{address}"),
            client: Client::builder()
                .timeout(Duration::from_secs(40))
                .build()
                .unwrap(),
        };
        for _ in 0..100 {
            if ready_request(
                &mut service.child,
                &root.join("service.stderr"),
                service
                    .client
                    .get(format!(
                        "{}/scim/v2/ServiceProviderConfig",
                        service.endpoint
                    ))
                    .bearer_auth(SCIM_TOKEN),
            )
            .await
            .is_some_and(|status| status.is_success())
            {
                return service;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("writer HTTP startup deadline");
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Value,
        version: Option<&str>,
        status: u16,
    ) -> Value {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.endpoint))
            .bearer_auth(SCIM_TOKEN);
        if let Some(version) = version {
            request = request.header("If-Match", version);
        }
        if !body.is_null() {
            request = request.json(&body);
        }
        let response = request.send().await.unwrap();
        let actual = response.status().as_u16();
        let bytes = response.bytes().await.unwrap();
        assert_eq!(actual, status, "{}", String::from_utf8_lossy(&bytes));
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        }
    }
}
fn patch(ops: Value) -> Value {
    json!({"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":ops})
}
fn user(email: &str) -> Value {
    json!({"schemas":[USER],"userName":email,"active":true,"emails":[{"value":email,"primary":true,"type":"work"}]})
}
fn path(resource: &Value, users: bool) -> String {
    format!(
        "/scim/v2/{}/{}",
        if users { "Users" } else { "Groups" },
        resource["id"].as_str().unwrap()
    )
}
fn version(resource: &Value) -> &str {
    resource["meta"]["version"].as_str().unwrap()
}
fn hit_count(client: &mut Mcp) -> usize {
    fixture_hits(client)["structuredContent"]["hits"]
        .as_array()
        .unwrap()
        .len()
}
async fn state(config: &Path) -> ServingHead {
    publication(config).await.0
}

#[tokio::test]
#[ignore = "owned Keycloak, writer HTTP/MCP and RustFS; scripts/ci"]
async fn identity_directory_scim_crud_patch_replay_collision_and_revocation_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let service = Service::start(&commit, &root).await;
    // Readers use the same directory policy, with no operator/provider credentials required.
    let text = std::fs::read_to_string(&worker).unwrap();
    std::fs::write(
        &worker,
        format!("{text}\n[identity.directory]\nprovider='scim'\nissuer='https://issuer.one'\nverified_domains=['example.test']\n"),
    )
    .unwrap();
    let wrong = service
        .client
        .get(format!("{}/scim/v2/Users", service.endpoint))
        .bearer_auth(OP_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 401);
    let response = service
        .client
        .get(format!("{}/admin/status", service.endpoint))
        .bearer_auth(OP_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let status: Value = response.json().await.unwrap();
    assert!(status["directory"].is_object());
    let schemas = service
        .request(Method::GET, "/scim/v2/Schemas", Value::Null, None, 200)
        .await;
    assert_eq!(schemas["totalResults"], 3);
    // Astra's attack: personal canonical address, a corporate alias and an
    // issuer-scoped subject must not capture the verified source's grants.
    let mut attacker = user("personal@outside.test");
    attacker["emails"]
        .as_array_mut()
        .unwrap()
        .push(json!({"value":"fixture@example.test"}));
    attacker["schemas"].as_array_mut().unwrap().push(json!(EXT));
    attacker[EXT] = json!({"subjects":[{"issuer":"https://issuer.one","subject":"attacker"}]});
    let personal = service
        .request(Method::POST, "/scim/v2/Users", attacker.clone(), None, 201)
        .await;
    assert!(
        super::identity::artifact(&commit)
            .await
            .1
            .principals
            .is_empty()
    );
    assert!(
        super::identity::artifact(&commit)
            .await
            .1
            .subjects
            .is_empty()
    );
    let before = state(&commit).await;
    let original = attacker.clone();
    attacker["userName"] = json!("another@outside.test");
    attacker[EXT]["subjects"][0]["issuer"] = json!("https://issuer.two");
    let refused = service
        .request(Method::POST, "/scim/v2/Users", attacker, None, 400)
        .await;
    assert_eq!(refused["scimType"], "invalidValue");
    assert_eq!(state(&commit).await.identity, before.identity);
    assert_eq!(state(&commit).await.freshness, before.freshness);
    service
        .request(
            Method::DELETE,
            &path(&personal, true),
            Value::Null,
            Some(version(&personal)),
            204,
        )
        .await;
    let recreated = service
        .request(Method::POST, "/scim/v2/Users", original, None, 201)
        .await;
    assert_ne!(recreated["id"], personal["id"]);
    service
        .request(
            Method::DELETE,
            &path(&recreated, true),
            Value::Null,
            Some(version(&recreated)),
            204,
        )
        .await;
    let mut body = user("fixture@example.test");
    body[EXT] = json!({"subjects":[{"issuer":"https://issuer.one","subject":"same-subject"}]});
    body["schemas"].as_array_mut().unwrap().push(json!(EXT));
    let mut u = service
        .request(Method::POST, "/scim/v2/Users", body.clone(), None, 201)
        .await;
    let before = state(&commit).await;
    assert!(before.freshness.directory.is_some());
    let original_version = version(&u).to_owned();
    let unordered: Value = Value::Object(
        body.as_object()
            .unwrap()
            .iter()
            .rev()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    );
    // A semantically identical POST with different object ordering is still a replay.
    let replay = service
        .request(Method::POST, "/scim/v2/Users", unordered, None, 200)
        .await;
    assert_eq!(replay["id"], u["id"]);
    assert!(state(&commit).await.freshness.directory > before.freshness.directory);
    let child=service.request(Method::POST,"/scim/v2/Groups",json!({"schemas":[GROUP],"displayName":"child","members":[{"value":u["id"],"type":"User"}]}),None,201).await;
    let mut parent=service.request(Method::POST,"/scim/v2/Groups",json!({"schemas":[GROUP],"displayName":"parent","members":[{"value":child["id"],"type":"Group"}]}),None,201).await;
    let a = super::identity::artifact(&commit).await.1;
    assert_eq!(a.subjects.len(), 1);
    assert!(a.principals.iter().any(|p| p.groups.len() == 4));
    let mut client = Mcp::open(&worker);
    assert_eq!(hit_count(&mut client), 2);
    parent=service.request(Method::PUT,&path(&parent,false),json!({"schemas":[GROUP],"displayName":"renamed-parent","members":[{"value":child["id"],"type":"Group"}]}),Some(version(&parent)),200).await;
    parent = service
        .request(
            Method::PATCH,
            &path(&parent, false),
            patch(json!([{"op":"add","path":"members","value":[{"value":u["id"],"type":"User"}]}])),
            Some(version(&parent)),
            200,
        )
        .await;
    let before = state(&commit).await;
    let invalid=service.request(Method::PATCH,&path(&u,true),patch(json!([{"op":"replace","path":"active","value":false},{"op":"replace","path":"notAnAttribute","value":"identity-canary"}])),Some(version(&u)),400).await;
    assert_eq!(invalid["scimType"], "invalidPath");
    assert!(!invalid.to_string().contains("identity-canary"));
    assert_eq!(state(&commit).await.freshness, before.freshness);
    assert_eq!(hit_count(&mut client), 2);
    let noop = service
        .request(
            Method::PATCH,
            &path(&u, true),
            patch(json!([{"op":"replace","path":"active","value":true}])),
            Some(version(&u)),
            200,
        )
        .await;
    assert_eq!(version(&noop), version(&u));
    assert_eq!(state(&commit).await.identity, before.identity);
    assert!(state(&commit).await.freshness.directory > before.freshness.directory);
    let before = state(&commit).await;
    service
        .request(
            Method::PATCH,
            &path(&u, true),
            patch(json!([{"op":"replace","path":"active","value":false}])),
            Some("W/\"stale\""),
            412,
        )
        .await;
    assert_eq!(state(&commit).await.freshness, before.freshness);
    u = service
        .request(
            Method::PATCH,
            &path(&u, true),
            patch(json!([{"op":"replace","path":"active","value":false}])),
            Some(version(&u)),
            200,
        )
        .await;
    publish(&commit);
    assert_eq!(
        fixture_hits(&mut client)["structuredContent"]["code"],
        "authorization_unavailable"
    );
    u = service
        .request(
            Method::PATCH,
            &path(&u, true),
            patch(json!([{"op":"replace","path":"active","value":true}])),
            Some(version(&u)),
            200,
        )
        .await;
    assert_eq!(hit_count(&mut client), 2);
    assert_ne!(
        version(&u),
        original_version,
        "restoring a resource cannot restore an old ETag"
    );
    let before_replay = state(&commit).await;
    service
        .request(
            Method::PATCH,
            &path(&u, true),
            patch(json!([{"op":"replace","path":"active","value":false}])),
            Some(&original_version),
            412,
        )
        .await;
    assert_eq!(state(&commit).await.freshness, before_replay.freshness);
    let mut other = user("other@example.test");
    other["emails"]
        .as_array_mut()
        .unwrap()
        .push(json!({"value":"fixture@example.test"}));
    let mut other = service
        .request(Method::POST, "/scim/v2/Users", other, None, 201)
        .await;
    assert_eq!(hit_count(&mut client), 0);
    other = service
        .request(
            Method::PATCH,
            &path(&other, true),
            patch(json!([{"op":"remove","path":"emails[value eq \"fixture@example.test\"]"}])),
            Some(version(&other)),
            200,
        )
        .await;
    assert_eq!(hit_count(&mut client), 2);
    // Four compressed 15-minute reconciliation cycles during a real quiet hour:
    // GET/list only, no resource mutations and no new identity generations.
    let clock = DirectoryClock::new(&worker, &root);
    let mut compressed = Mcp::open(&clock.config);
    let identity = state(&commit).await.identity;
    let rejected = state(&commit).await;
    service
        .request(
            Method::GET,
            "/scim/v2/Users?count=1001",
            Value::Null,
            None,
            400,
        )
        .await;
    assert_eq!(state(&commit).await.freshness, rejected.freshness);
    for _ in 0..4 {
        let previous = state(&commit).await;
        service
            .request(Method::GET, "/scim/v2/Users", Value::Null, None, 200)
            .await;
        let next = state(&commit).await;
        assert!(next.freshness.directory > previous.freshness.directory);
        assert_eq!(next.identity, identity);
        assert_eq!(hit_count(&mut client), 2);
        clock.age(14);
        assert_eq!(hit_count(&mut compressed), 2);
    }
    // No IdP requests: freshness ages to warning, then the deadline.
    clock.age(16);
    assert_eq!(clock.warning(), "identity_expiring");
    clock.age(31);
    assert_eq!(
        fixture_hits(&mut compressed)["structuredContent"]["code"],
        "authorization_unavailable"
    );
    drop(compressed);
    drop(clock);
    let listed = service
        .request(
            Method::GET,
            "/scim/v2/Users?startIndex=1&count=1",
            Value::Null,
            None,
            200,
        )
        .await;
    assert_eq!(listed["totalResults"], 2);
    assert_eq!(listed["itemsPerPage"], 1);
    let too_large = service
        .client
        .post(format!("{}/scim/v2/Users", service.endpoint))
        .bearer_auth(SCIM_TOKEN)
        .header("Content-Type", "application/scim+json")
        .body(" ".repeat(1024 * 1024 + 1))
        .send()
        .await
        .unwrap();
    assert_eq!(too_large.status(), 413);
    service
        .request(
            Method::DELETE,
            &path(&u, true),
            Value::Null,
            Some(version(&u)),
            204,
        )
        .await;
    publish(&commit);
    assert_eq!(
        fixture_hits(&mut client)["structuredContent"]["code"],
        "authorization_unavailable"
    );
    let after = state(&commit).await;
    service
        .request(
            Method::DELETE,
            &path(&u, true),
            Value::Null,
            Some(version(&u)),
            404,
        )
        .await;
    assert_eq!(state(&commit).await.freshness, after.freshness);
    service
        .request(
            Method::DELETE,
            &path(&other, true),
            Value::Null,
            Some(version(&other)),
            204,
        )
        .await;
    // Deleting a member changes group ETags, so obtain the current versions first.
    parent = service
        .request(Method::GET, &path(&parent, false), Value::Null, None, 200)
        .await;
    service
        .request(
            Method::DELETE,
            &path(&parent, false),
            Value::Null,
            Some(version(&parent)),
            204,
        )
        .await;
    let child = service
        .request(Method::GET, &path(&child, false), Value::Null, None, 200)
        .await;
    service
        .request(
            Method::DELETE,
            &path(&child, false),
            Value::Null,
            Some(version(&child)),
            204,
        )
        .await;
    drop(client);
    drop(service);
    let logs = std::fs::read_to_string(root.join("service.stderr")).unwrap();
    for marker in [
        SCIM_TOKEN,
        OP_TOKEN,
        "fixture@example.test",
        "same-subject",
        "issuer.one",
        "identity-canary",
    ] {
        assert!(!logs.contains(marker));
    }
}

async fn keycloak_token(client: &Client, endpoint: &str, username: &str, password: &str) -> String {
    let response = client
        .post(format!(
            "{endpoint}/realms/master/protocol/openid-connect/token"
        ))
        .form(&[
            ("client_id", "admin-cli"),
            ("grant_type", "password"),
            ("username", username),
            ("password", password),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    body["access_token"].as_str().unwrap().to_owned()
}

fn directory_command(config: &Path, token: Option<&str>) -> Output {
    let mut command = host_command(config);
    command.args(["writer", "sync", "--directory"]);
    if let Some(token) = token {
        command.env("KEYCLOAK_TOKEN", token);
    }
    command.output().unwrap()
}
#[tokio::test]
#[ignore = "owned Keycloak admin API, CLI/MCP and local storage; scripts/ci"]
async fn identity_directory_keycloak_pull_nested_deactivation_and_quiet_hour_on_storage() {
    let endpoint = std::env::var("FYRR_TEST_KEYCLOAK_ENDPOINT").unwrap();
    let username = std::env::var("FYRR_TEST_KEYCLOAK_USERNAME").unwrap();
    let password = std::env::var("FYRR_TEST_KEYCLOAK_PASSWORD").unwrap();
    let client = Client::new();
    let token_value = keycloak_token(&client, &endpoint, &username, &password).await;
    let token = token_value.as_str();
    let realm = format!(
        "fyrr-directory-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_micros()
    );
    let base = format!("{endpoint}/admin/realms/{realm}");
    assert_eq!(
        client
            .post(format!("{endpoint}/admin/realms"))
            .bearer_auth(token)
            .json(&json!({"realm":realm,"enabled":true,"sslRequired":"none"}))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    for (email, verified) in [
        ("fixture@example.test", true),
        ("unverified@example.test", false),
        ("personal@example.org", true),
    ] {
        assert_eq!(
            client
                .post(format!("{base}/users"))
                .bearer_auth(token)
                .json(
                    &json!({"username":email,"email":email,"emailVerified":verified,"enabled":true})
                )
                .send()
                .await
                .unwrap()
                .status(),
            201
        );
    }
    let users: Value = client
        .get(format!("{base}/users"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let uid = users
        .as_array()
        .unwrap()
        .iter()
        .find(|u| u["email"] == "fixture@example.test")
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let response = client
        .post(format!("{base}/groups"))
        .bearer_auth(token)
        .json(&json!({"name":"parent"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let parent = response.headers()["location"]
        .to_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    let response = client
        .post(format!("{base}/groups/{parent}/children"))
        .bearer_auth(token)
        .json(&json!({"name":"child"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let child = response.headers()["location"]
        .to_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    assert_eq!(
        client
            .put(format!("{base}/users/{uid}/groups/{child}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let directory = format!(
        "\n[identity.directory]\nprovider='keycloak'\nendpoint='{endpoint}'\nrealm='{realm}'\nverified_domains=['example.test']\ncredentials={{env='KEYCLOAK_TOKEN'}}\nsync_interval='15m'\n"
    );
    for config in [&worker, &commit] {
        std::fs::write(
            config,
            format!("{}{directory}", std::fs::read_to_string(config).unwrap()),
        )
        .unwrap();
    }
    let output = directory_command(&commit, Some(token));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let a = super::identity::artifact(&commit).await.1;
    assert_eq!(a.principals.len(), 1);
    assert_eq!(a.subjects.len(), 1);
    assert!(a.principals.iter().any(|p| p.groups.len() == 4));
    let mut mcp = Mcp::open(&worker);
    assert_eq!(hit_count(&mut mcp), 2);
    // Four unchanged successful pulls model the scheduled pulls during one quiet hour.
    let clock = DirectoryClock::new(&worker, &root);
    let mut compressed = Mcp::open(&clock.config);
    for _ in 0..4 {
        let before = state(&commit).await;
        let result = directory_command(&commit, Some(token));
        assert!(result.status.success());
        let next = state(&commit).await;
        assert!(next.freshness.directory > before.freshness.directory);
        assert_eq!(next.identity, before.identity);
        assert_eq!(hit_count(&mut mcp), 2);
        clock.age(14);
        assert_eq!(hit_count(&mut compressed), 2);
    }
    clock.age(16);
    assert_eq!(clock.warning(), "identity_expiring");
    assert_eq!(hit_count(&mut compressed), 2);
    clock.age(31);
    assert_eq!(
        fixture_hits(&mut compressed)["structuredContent"]["code"],
        "authorization_unavailable"
    );
    drop(compressed);
    drop(clock);
    // A valid HTTP short page is not completion: retain real Keycloak counts,
    // but truncate a directory page. No publication or proof may result.
    for truncated in ["users", "groups", "memberships"] {
        use std::sync::{Arc, atomic::AtomicBool};
        let select = truncated.to_owned();
        let transform: super::corrections::ResponseTransform =
            Arc::new(move |request, response| {
                let route = request.split_whitespace().nth(1).unwrap_or_default();
                let path = route.split('?').next().unwrap_or_default();
                let truncate = match select.as_str() {
                    "users" => path.ends_with("/users"),
                    "groups" => path.ends_with("/groups") && !path.contains("/users/"),
                    _ => path.ends_with("/groups") && path.contains("/users/"),
                };
                if request.starts_with("GET ") && truncate && response.starts_with(b"HTTP/1.1 200")
                {
                    super::corrections::rewrite_ok_body(response, |body| {
                        *body = b"[]".to_vec();
                        true
                    });
                }
            });
        let stop = Arc::new(AtomicBool::new(false));
        let (proxy, thread) =
            super::corrections::proxy_transform(endpoint.to_owned(), stop.clone(), transform, None);
        let mut _guard = thread;
        _guard.resume = Arc::new(AtomicBool::new(true));
        let truncated_config = root.join(format!("truncated-{truncated}.toml"));
        let real_issuer = format!("{endpoint}/realms/{realm}");
        let config_text = std::fs::read_to_string(&commit)
            .unwrap()
            .replace(
                &format!("endpoint='{endpoint}'"),
                &format!("endpoint='{proxy}'"),
            )
            .replace(
                "provider='keycloak'",
                &format!("provider='keycloak'\nissuer='{real_issuer}'"),
            );
        std::fs::write(&truncated_config, config_text).unwrap();
        let before = state(&commit).await;
        assert!(
            !directory_command(&truncated_config, Some(token))
                .status
                .success()
        );
        assert_eq!(state(&commit).await.identity, before.identity);
        assert_eq!(state(&commit).await.freshness, before.freshness);
    }
    let before = state(&commit).await;
    assert!(
        !directory_command(&commit, Some("invalid-bearer-canary"))
            .status
            .success()
    );
    assert_eq!(state(&commit).await.freshness, before.freshness);
    assert_eq!(state(&commit).await.identity, before.identity);
    assert_eq!(
        client
            .put(format!("{base}/users/{uid}"))
            .bearer_auth(token)
            .json(&json!({"enabled":false}))
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert!(directory_command(&commit, Some(token)).status.success());
    publish(&commit);
    assert_eq!(
        fixture_hits(&mut mcp)["structuredContent"]["code"],
        "authorization_unavailable"
    );
    drop(mcp);
    let token_value = keycloak_token(&client, &endpoint, &username, &password).await;
    let token = token_value.as_str();
    // A valid realm may require more than ten thousand membership requests.
    let users: Vec<_> = (0..3400)
        .map(|index| {
            let email = format!("bulk-{index}@example.test");
            json!({"username":email,"email":email,"emailVerified":true,"enabled":true})
        })
        .collect();
    assert_eq!(
        client
            .post(format!("{base}/partialImport"))
            .bearer_auth(token)
            .json(&json!({"ifResourceExists":"FAIL","users":users}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let token_value = keycloak_token(&client, &endpoint, &username, &password).await;
    let token = token_value.as_str();
    let before = state(&commit).await.freshness.directory;
    let output = directory_command(&commit, Some(token));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        super::identity::artifact(&commit).await.1.principals.len(),
        3400
    );
    assert!(state(&commit).await.freshness.directory > before);
    let token_value = keycloak_token(&client, &endpoint, &username, &password).await;
    let token = token_value.as_str();
    assert_eq!(
        client
            .delete(base)
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
}

/// Time compression changes only observed head freshness; stored payloads and
/// their hashes stay untouched. The real CLI and MCP still enforce the deadline.
struct DirectoryClock {
    age: std::sync::Arc<std::sync::atomic::AtomicI64>,
    config: PathBuf,
    _proxy: super::corrections::ProxyGuard,
}
impl DirectoryClock {
    fn new(config: &Path, root: &Path) -> Self {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicI64, Ordering},
        };
        let age = Arc::new(AtomicI64::new(14));
        let observed = age.clone();
        let transform: super::corrections::ResponseTransform =
            Arc::new(move |request, response| {
                if !request.starts_with("GET ") || !request.contains("/head.json") {
                    return;
                }
                super::corrections::rewrite_ok_body(response, |body| {
                    let mut value: Value = serde_json::from_slice(body).unwrap();
                    assert!(value["freshness"]["directory"].is_string());
                    value["freshness"]["directory"] = json!(
                        (chrono::Utc::now()
                            - chrono::Duration::minutes(observed.load(Ordering::Relaxed)))
                        .to_rfc3339()
                    );
                    *body = serde_json::to_vec(&value).unwrap();
                    true
                });
            });
        let stop = Arc::new(AtomicBool::new(false));
        let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
        let (proxy, thread) =
            super::corrections::proxy_transform(endpoint.clone(), stop.clone(), transform, None);
        let target = root.join("directory-clock.toml");
        let text = std::fs::read_to_string(config)
            .unwrap()
            .replace(&endpoint, &proxy)
            .replace("[identity]", "[identity]\nvalidity_deadline='30m'\n");
        std::fs::write(&target, text).unwrap();
        Self {
            age,
            config: target,
            _proxy: thread,
        }
    }
    fn age(&self, minutes: i64) {
        self.age
            .store(minutes, std::sync::atomic::Ordering::Relaxed);
    }
    fn warning(&self) -> Value {
        success(&self.config, &["writer", "status"])["directory"]["warning"].clone()
    }
}
