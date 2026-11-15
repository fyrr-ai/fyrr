//! Jira through the actual worker/reader and owned RustFS, using the one HTTP fake.
//! Synced proofs cover published content, section audiences and revocation.
use super::coordination::{committer_host, host_command, publication, worker_command, worker_sync};
use super::incremental::{publish, roots};
use super::*;

struct Fake {
    child: std::process::Child,
    endpoint: String,
}
impl Fake {
    fn start() -> Self {
        let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut child = Command::new(checkout.join("connectors/.venv/bin/python"))
            .arg(checkout.join("connectors/tests/jira/fake_jira.py"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let result: Value = serde_json::from_str(&line).unwrap();
        Self {
            child,
            endpoint: result["endpoint"].as_str().unwrap().into(),
        }
    }
    fn call(&self, operation: &str, body: Value) -> Value {
        let output = Command::new("curl")
            .args([
                "-fsS",
                "-X",
                "POST",
                "-H",
                "Content-Type: application/json",
                "-d",
            ])
            .arg(body.to_string())
            .arg(format!("{}/fake/{operation}", self.endpoint))
            .output()
            .unwrap();
        assert!(output.status.success(), "synthetic Jira control failed");
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"].clone()
    }
}
impl Drop for Fake {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn fixture(fake: &Fake, mode: &str) -> (PathBuf, PathBuf, FixtureRoot) {
    let (worker, root) = setup();
    let text = std::fs::read_to_string(&worker).unwrap();
    let readers = if mode == "fixed" {
        "readers=['grp:allow']\n"
    } else {
        ""
    };
    let intervals = if mode == "sync" {
        "[sources.intervals]\npermissions='15m'\nmembers='15m'\n"
    } else {
        ""
    };
    std::fs::write(&worker, format!("{text}\n[[sources]]\nid='jira'\ntype='jira'\npermissions='{mode}'\n{readers}document_max_bytes='64B'\n[sources.settings]\nsite_url='https://synthetic.atlassian.net'\ncloud_id='11111111-1111-4111-8111-111111111111'\napi_token={{ env='FYRR_TEST_JIRA_TOKEN' }}\napi_endpoint='{}'\n{intervals}[sources.scope]\nprojects=[1]\n", fake.endpoint)).unwrap();
    assert!(
        host_command(&worker)
            .args(["writer", "namespace", "create"])
            .output()
            .unwrap()
            .status
            .success()
    );
    worker_sync(&worker, &["allowed", "denied", "--full"]);
    let commit = committer_host(&worker, &root);
    publish(&commit);
    for path in [&worker, &commit] {
        let text = std::fs::read_to_string(path).unwrap();
        std::fs::write(
            path,
            format!("{text}\n[writer]\nenvelope_max_rows=2\n[writer.commit]\ngroup_max_rows=2\n"),
        )
        .unwrap();
    }
    (worker, commit, root)
}
fn sync(worker: &Path, args: &[&str]) -> Output {
    worker_command(worker)
        .env("FYRR_TEST_JIRA_TOKEN", "jira-synthetic-only")
        .args(["writer", "sync", "jira"])
        .args(args)
        .output()
        .unwrap()
}
#[track_caller]
fn synced(worker: &Path, args: &[&str]) -> Value {
    let output = sync(worker, args);
    assert!(
        output.status.success(),
        "Jira worker failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
async fn documents(worker: &Path) -> Vec<String> {
    let loaded = fyrr_core::config::load(worker).unwrap();
    let (head, _) = publication(worker).await;
    let dataset = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        loaded
            .config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        head.content.unwrap().lance_version,
        None,
    )
    .await
    .unwrap();
    let batch = dataset
        .scan()
        .filter("source_id = 'jira'")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let mut ids = roots(&batch);
    ids.sort();
    ids.dedup();
    ids
}

#[tokio::test]
#[ignore = "actual worker/reader and owned RustFS; coordinator Cargo slot"]
async fn jira_fixed_issues_comments_quarantine_and_reconcile_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "fixed");
    for id in 101..104 {
        fake.call("issue", json!({"identity":id,"summary":format!("fixture document {}", id-101),"description":null,"status":null,"issuetype":{"id":"1"},"reporter":null}));
    }
    let report = synced(&worker, &["--kind", "content"]);
    assert_eq!(
        report["paid_calls"], 2,
        "three novel texts occupy two bounded envelopes"
    );
    assert_eq!(report["stats"]["upserts"], 3);
    assert_eq!(report["stats"]["quarantined"], 0);
    publish(&commit);
    assert_eq!(documents(&worker).await.len(), 3);
    fake.call(
        "comment",
        json!({"issue":101,"identity":201,"body":"fixture document 3"}),
    );
    synced(&worker, &["--kind", "content"]);
    publish(&commit);
    assert_eq!(documents(&worker).await.len(), 3);
    fake.call("delete_comment", json!({"issue":101,"identity":201}));
    fake.call("advance", json!({"seconds":43201}));
    synced(&worker, &["--kind", "content"]);
    publish(&commit);
    fake.call(
        "edit",
        json!({"identity":101,"description":"oversized synthetic text ".repeat(8)}),
    );
    fake.call("issue",json!({"identity":104,"summary":"fixture document 4","description":null,"status":null,"issuetype":{"id":"1"},"reporter":null}));
    let report = synced(&worker, &["--kind", "content"]);
    assert_eq!(report["stats"]["quarantined"], 1);
    publish(&commit);
    assert_eq!(documents(&worker).await.len(), 4);
    synced(&worker, &["--kind", "content", "--full"]);
    publish(&commit);
    assert_eq!(documents(&worker).await.len(), 4);
    fake.call("delete", json!({"identity":102}));
    fake.call("fail", json!({"path":"/search/jql","times":6}));
    assert!(!sync(&worker, &["--kind", "reconcile"]).status.success());
    publish(&commit);
    assert_eq!(documents(&worker).await.len(), 4);
    synced(&worker, &["--kind", "reconcile"]);
    publish(&commit);
    assert_eq!(documents(&worker).await.len(), 3);
    fake.call("project", json!({"identity":1,"archived":true}));
    synced(&worker, &["--kind", "reconcile"]);
    publish(&commit);
    assert!(documents(&worker).await.is_empty());
}

async fn link_account(worker: &Path, user: &str, email: Option<&str>) {
    use fyrr_core::directory::{AccountLink, DirectorySnapshot, DirectoryUser};
    let loaded = fyrr_core::config::load(worker).unwrap();
    let (head, _) = publication(worker).await;
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let identity: fyrr_core::identity::IdentityArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{ns}/{}", head.identity.object)),
    )
    .await
    .unwrap();
    let key =
        fyrr_reader::storage::hash_principal(&loaded.config, "key", "fixture-subject").unwrap();
    let snapshot = DirectorySnapshot {
        schema_version: DirectorySnapshot::VERSION,
        namespace: loaded.config.namespace.name.clone(),
        provider: "jira-test-directory".into(),
        built_at: chrono::Utc::now(),
        hashing: identity.hashing,
        users: vec![DirectoryUser {
            key: key.clone(),
            token: fyrr_reader::storage::hash_principal(
                &loaded.config,
                "tok",
                "usr:fixture-subject",
            )
            .unwrap(),
            active: true,
            aliases: email
                .map(|address| {
                    vec![
                        fyrr_reader::storage::hash_principal(
                            &loaded.config,
                            "key",
                            &address.to_lowercase(),
                        )
                        .unwrap(),
                    ]
                })
                .unwrap_or_default(),
            subjects: vec![],
        }],
        groups: vec![],
        state: None,
        links: if email.is_none() {
            vec![AccountLink {
                source: "jira".into(),
                user: fyrr_reader::storage::hash_principal(
                    &loaded.config,
                    "key",
                    &format!("atlassian.net:11111111-1111-4111-8111-111111111111:{user}"),
                )
                .unwrap(),
                principal: key,
            }]
        } else {
            vec![]
        },
    };
    fyrr_writer::directory::publish(&loaded.config, &snapshot)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn jira_sync_silent_content_restricted_sections_and_revoke_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "sync");
    fake.call("issue",json!({"identity":101,"summary":"fixture document 0","description":null,"status":null,"issuetype":{"id":"1"},"reporter":null}));
    fake.call("comment",json!({"issue":101,"identity":201,"body":"fixture document 1","visibility":{"type":"group","identifier":"g1"}}));
    link_account(&worker, "U001", None).await;
    synced(
        &worker,
        &[
            "--kind",
            "permissions",
            "--kind",
            "members",
            "--kind",
            "content",
        ],
    );
    publish(&commit);
    let root = "jira:11111111-1111-4111-8111-111111111111:issue:101";
    let mut reader = super::tools::Mcp::open(&worker);
    for query in ["fixture query", "fixture document 1"] {
        let result = reader.call(
            "search",
            json!({"sources":["jira"],"query":query,"limit":10}),
        );
        assert!(
            !result["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    let result = reader.call("fetch", json!({"source_id":"jira","root_id":root}));
    assert!(
        !result["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    fake.call("group", json!({"identity":"g1","members":[]}));
    synced(&worker, &["--kind", "permissions", "--kind", "members"]);
    publish(&commit);
    reader.rebind(&worker, "jira", root);
    for (tool, request, key) in [
        (
            "search",
            json!({"sources":["jira"],"query":"fixture query","limit":10}),
            "hits",
        ),
        (
            "search",
            json!({"sources":["jira"],"query":"fixture document 1","limit":10}),
            "hits",
        ),
        (
            "fetch",
            json!({"source_id":"jira","root_id":root}),
            "documents",
        ),
    ] {
        let result = reader.call(tool, request);
        assert!(!result["isError"].as_bool().unwrap_or(false));
        assert!(
            result["structuredContent"][key]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn jira_sync_comment_role_under_share_and_membership_refresh_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "sync");
    fake.call("user", json!({"identity":"U002"}));
    fake.call("group", json!({"identity":"g1","members":["U001","U002"]}));
    fake.call("issue",json!({"identity":101,"summary":"fixture document 0","description":null,"status":null,"issuetype":{"id":"1"},"reporter":null}));
    fake.call("comment",json!({"issue":101,"identity":201,"body":"fixture document 1","visibility":{"type":"role","identifier":"10"}}));
    link_account(&worker, "U002", None).await;
    synced(
        &worker,
        &[
            "--kind",
            "permissions",
            "--kind",
            "members",
            "--kind",
            "content",
        ],
    );
    publish(&commit);
    let mut reader = super::tools::Mcp::open(&worker);
    let result = reader.call(
        "fetch",
        json!({"source_id":"jira","root_id":"jira:11111111-1111-4111-8111-111111111111:issue:101"}),
    );
    assert!(
        !result["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(!result.to_string().contains("fixture document 1"));
    fake.call("project", json!({"identity":1,"archived":true}));
    synced(&worker, &["--kind", "permissions", "--kind", "members"]);
    publish(&commit);
    reader.rebind(
        &worker,
        "jira",
        "jira:11111111-1111-4111-8111-111111111111:issue:101",
    );
    let result = reader.call(
        "fetch",
        json!({"source_id":"jira","root_id":"jira:11111111-1111-4111-8111-111111111111:issue:101"}),
    );
    assert!(
        result["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn jira_sync_acl_failure_default_deadline_hides_only_source_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "sync");
    fake.call("issue",json!({"identity":101,"summary":"fixture document 0","description":null,"status":null,"issuetype":{"id":"1"},"reporter":null}));
    let root_id = "jira:11111111-1111-4111-8111-111111111111:issue:101";
    link_account(&worker, "U001", None).await;
    synced(
        &worker,
        &[
            "--kind",
            "permissions",
            "--kind",
            "members",
            "--kind",
            "content",
        ],
    );
    publish(&commit);
    let (before, _) = publication(&worker).await;
    let prior = before
        .freshness
        .sources
        .iter()
        .find(|f| f.source_id == "jira")
        .unwrap()
        .clone();
    fake.call(
        "fail",
        json!({"path":"/project/1/permissionscheme","status":403}),
    );
    assert!(!sync(&worker, &["--kind", "permissions"]).status.success());
    publish(&commit);
    let (mut head, _) = publication(&worker).await;
    assert_eq!(
        head.freshness
            .sources
            .iter()
            .find(|f| f.source_id == "jira")
            .unwrap(),
        &prior
    );
    let mut reader = super::tools::Mcp::open(&worker);
    let visible = reader.call("fetch", json!({"source_id":"jira","root_id":&root_id}));
    assert!(
        !visible["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let deadline =
        chrono::Duration::from_std(loaded.config.identity.validity_deadline.as_duration()).unwrap();
    head.freshness
        .sources
        .iter_mut()
        .find(|f| f.source_id == "jira")
        .unwrap()
        .permissions_read_at = chrono::Utc::now() - deadline - chrono::Duration::seconds(1);
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    store
        .put(
            &ns.join("head.json"),
            fyrr_core::write_contract(&head).into(),
        )
        .await
        .unwrap();
    let hidden = reader.call("fetch", json!({"source_id":"jira","root_id":&root_id}));
    assert!(
        hidden["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        hidden["structuredContent"]["metadata"]["hidden_sources"],
        json!(["jira"])
    );
    let unaffected = reader.call(
        "search",
        json!({"sources":["allowed"],"order":"updated_at","limit":2}),
    );
    assert_eq!(
        unaffected["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

/// A durable but unpublished replacement cannot refresh permissions of the old plain row.
#[tokio::test]
#[ignore = "real CLI and RustFS; focused Jira risk correction"]
async fn jira_visibility_change_waits_for_published_content_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "sync");
    fake.call("issue", json!({"identity":101,"summary":"fixture document 0","description":null,"status":null,"issuetype":{"id":"1"},"reporter":null}));
    fake.call(
        "comment",
        json!({"issue":101,"identity":201,"body":"fixture document 1"}),
    );
    synced(
        &worker,
        &[
            "--kind",
            "permissions",
            "--kind",
            "members",
            "--kind",
            "content",
        ],
    );
    publish(&commit);
    fake.call("group", json!({"identity":"g2","members":[]}));
    fake.call(
        "edit_comment",
        json!({"issue":101,"identity":201,"visibility":{"type":"group","identifier":"g2"}}),
    );
    synced(&worker, &["--kind", "content"]);
    let before = super::incremental::envelopes(&worker).await.len();
    for kind in ["permissions", "members"] {
        assert!(
            !sync(&worker, &["--kind", kind]).status.success(),
            "{kind} refreshed a stale plain comment using an unpublished revision"
        );
        assert_eq!(super::incremental::envelopes(&worker).await.len(), before);
    }
    publish(&commit);
    synced(&worker, &["--kind", "permissions", "--kind", "members"]);
    publish(&commit);
    super::coordination::command(&commit, &["writer", "verify"]);
}
