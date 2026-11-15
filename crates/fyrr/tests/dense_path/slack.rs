//! Slack through the actual worker/reader and owned RustFS, using the one HTTP fake.
//! Synced permission tests await the coordinator's writer-lane merge.
use super::coordination::{committer_host, host_command, publication, worker_command, worker_sync};
use super::incremental::{publish, source_roots};
use super::*;

struct Fake {
    child: std::process::Child,
    endpoint: String,
}
impl Fake {
    fn start() -> Self {
        let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut child = Command::new(checkout.join("connectors/.venv/bin/python"))
            .arg(checkout.join("connectors/tests/slack/fake_slack.py"))
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
        assert!(output.status.success(), "synthetic Slack control failed");
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
    std::fs::write(&worker, format!("{text}\n[[sources]]\nid='slack'\ntype='slack'\npermissions='{mode}'\n{readers}document_max_bytes='64B'\n[sources.settings]\nworkspace_id='T001'\ntoken={{ env='FYRR_TEST_SLACK_TOKEN' }}\napi_endpoint='{}'\n{intervals}[sources.scope]\ninclude=['*']\n", fake.endpoint)).unwrap();
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
        .env("FYRR_TEST_SLACK_TOKEN", "xoxp-synthetic-only")
        .args(["writer", "sync", "slack"])
        .args(args)
        .output()
        .unwrap()
}
fn synced(worker: &Path, args: &[&str]) -> Value {
    let output = sync(worker, args);
    assert!(
        output.status.success(),
        "Slack worker failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
#[ignore = "actual worker/reader and owned RustFS; coordinator Cargo slot"]
async fn slack_fixed_windows_threads_quarantine_and_reconcile_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "fixed");
    let first = fake.call("post", json!({"text":"fixture document 0","seconds":400}));
    fake.call("post", json!({"text":"fixture document 1","seconds":400}));
    fake.call("post", json!({"text":"fixture document 2","seconds":400}));
    let report = synced(&worker, &["--kind", "content"]);
    assert_eq!(
        report["paid_calls"], 2,
        "three novel texts occupy two bounded envelopes"
    );
    publish(&commit);
    assert_eq!(source_roots(&worker, "slack").await.len(), 3);
    fake.call("advance", json!({"seconds":172800}));
    fake.call(
        "post",
        json!({"text":"fixture document 3","thread":first["ts"]}),
    );
    synced(&worker, &["--kind", "content"]);
    publish(&commit);
    assert_eq!(source_roots(&worker, "slack").await.len(), 3);
    fake.call(
        "edit",
        json!({"channel":"C001","ts":first["ts"],"text":"fixture document 4"}),
    );
    fake.call(
        "post",
        json!({"text":"oversized synthetic text ".repeat(8),"seconds":400}),
    );
    let report = synced(&worker, &["--kind", "content"]);
    assert_eq!(report["stats"]["quarantined"], 1);
    publish(&commit);
    assert_eq!(source_roots(&worker, "slack").await.len(), 3);
    synced(&worker, &["--kind", "content", "--full"]);
    publish(&commit);
    assert_eq!(source_roots(&worker, "slack").await.len(), 3);
    fake.call("remove", json!({"channel":"C001","ts":first["ts"]}));
    fake.call(
        "fail",
        json!({"method":"conversations.history","code":"internal_error"}),
    );
    assert!(!sync(&worker, &["--kind", "reconcile"]).status.success());
    publish(&commit);
    assert_eq!(source_roots(&worker, "slack").await.len(), 3);
    synced(&worker, &["--kind", "reconcile"]);
    publish(&commit);
    assert_eq!(source_roots(&worker, "slack").await.len(), 2);
    // Authoritative channel loss is scope exit; no permission API is needed.
    fake.call("delete_channel", json!({"channel":"C001"}));
    synced(&worker, &["--kind", "reconcile"]);
    publish(&commit);
    assert!(source_roots(&worker, "slack").await.is_empty());
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
        provider: "slack-test-directory".into(),
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
                source: "slack".into(),
                user: fyrr_reader::storage::hash_principal(
                    &loaded.config,
                    "key",
                    &format!("slack.com:T001:{user}"),
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
async fn slack_sync_silent_content_and_revoke_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "sync");
    fake.call(
        "channel",
        json!({"identity":"C001","name":"private-room","private":true}),
    );
    let message = fake.call("post", json!({"text":"fixture document 0"}));
    let root_id = format!(
        "slack:T001:C001:window:{}",
        message["ts"].as_str().unwrap().replace('.', "")
    );
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
    let mut reader = super::tools::Mcp::open(&worker);
    for request in [
        json!({"sources":["slack"],"query":"fixture query","limit":10}),
        json!({"sources":["slack"],"query":"fixture document 0","limit":10}),
    ] {
        let result = reader.call("search", request);
        assert!(!result["isError"].as_bool().unwrap_or(false));
        assert!(
            !result["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    let fetched = reader.call("fetch", json!({"source_id":"slack","root_id":&root_id}));
    assert!(
        !fetched["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    fake.call("set_members", json!({"channel":"C001","members":[]}));
    synced(&worker, &["--kind", "permissions", "--kind", "members"]);
    publish(&commit);
    reader.rebind(&worker, "slack", &root_id);
    for (tool, request) in [
        (
            "search",
            json!({"sources":["slack"],"query":"fixture query","limit":10}),
        ),
        (
            "search",
            json!({"sources":["slack"],"query":"fixture document 0","limit":10}),
        ),
        ("fetch", json!({"source_id":"slack","root_id":&root_id})),
    ] {
        let result = reader.call(tool, request);
        assert!(!result["isError"].as_bool().unwrap_or(false));
        let field = if tool == "search" {
            "hits"
        } else {
            "documents"
        };
        assert!(
            result["structuredContent"][field]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn slack_sync_public_floor_guest_membership_and_deactivation_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "sync");
    let message = fake.call("post", json!({"text":"fixture document 0"}));
    let root_id = format!(
        "slack:T001:C001:window:{}",
        message["ts"].as_str().unwrap().replace('.', "")
    );
    fake.call("set_members", json!({"channel":"C001","members":[]}));
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
    let mut reader = super::tools::Mcp::open(&worker);
    let full = reader.call("fetch", json!({"source_id":"slack","root_id":&root_id}));
    assert!(
        !full["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    // A full user outside the channel inherits public access. A guest does not.
    fake.call("user", json!({"identity":"U001","is_restricted":true}));
    synced(&worker, &["--kind", "permissions", "--kind", "members"]);
    publish(&commit);
    reader.rebind(&worker, "slack", &root_id);
    let guest = reader.call("fetch", json!({"source_id":"slack","root_id":&root_id}));
    assert!(
        guest["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    fake.call("set_members", json!({"channel":"C001","members":["U001"]}));
    synced(&worker, &["--kind", "permissions", "--kind", "members"]);
    publish(&commit);
    reader.rebind(&worker, "slack", &root_id);
    let member = reader.call("fetch", json!({"source_id":"slack","root_id":&root_id}));
    assert!(
        !member["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    fake.call("user", json!({"identity":"U001","deleted":true}));
    synced(&worker, &["--kind", "permissions", "--kind", "members"]);
    publish(&commit);
    reader.rebind(&worker, "slack", &root_id);
    let inactive = reader.call("fetch", json!({"source_id":"slack","root_id":&root_id}));
    assert!(
        inactive["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn slack_sync_corporate_email_link_and_changed_external_email_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "sync");
    for path in [&worker, &commit] {
        let text = std::fs::read_to_string(path).unwrap().replace(
            "workspace_id='T001'",
            "workspace_id='T001'\ncorporate_domains=['corp.test']",
        );
        std::fs::write(path, text).unwrap();
    }
    let message = fake.call("post", json!({"text":"fixture document 0"}));
    let root_id = format!(
        "slack:T001:C001:window:{}",
        message["ts"].as_str().unwrap().replace('.', "")
    );
    // Profile e-mail is used only as the member batch's approved-domain evidence.
    fake.call(
        "user",
        json!({"identity":"U001","profile":{"email":"person@corp.test"}}),
    );
    link_account(&worker, "U001", Some("person@corp.test")).await;
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
    let linked = reader.call("fetch", json!({"source_id":"slack","root_id":&root_id}));
    assert!(
        !linked["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    fake.call(
        "user",
        json!({"identity":"U001","profile":{"email":"person@external.test"}}),
    );
    synced(&worker, &["--kind", "permissions", "--kind", "members"]);
    publish(&commit);
    reader.rebind(&worker, "slack", &root_id);
    let unlinked = reader.call("fetch", json!({"source_id":"slack","root_id":&root_id}));
    assert!(
        unlinked["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}
