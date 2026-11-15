//! Jira through the actual worker/reader and owned RustFS, using the one HTTP fake.
//! Synced proofs cover published content, section audiences and revocation.
use super::coordination::{committer_host, worker_command, worker_sync};
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
    super::coordination::command(&worker, &["writer", "namespace", "create"]);
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
    assert_eq!(source_roots(&worker, "jira").await.len(), 3);
    fake.call(
        "comment",
        json!({"issue":101,"identity":201,"body":"fixture document 3"}),
    );
    synced(&worker, &["--kind", "content"]);
    publish(&commit);
    assert_eq!(source_roots(&worker, "jira").await.len(), 3);
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
    assert_eq!(source_roots(&worker, "jira").await.len(), 4);
    synced(&worker, &["--kind", "content", "--full"]);
    publish(&commit);
    assert_eq!(source_roots(&worker, "jira").await.len(), 4);
    fake.call("delete", json!({"identity":102}));
    fake.call("fail", json!({"path":"/search/jql","times":6}));
    assert!(!sync(&worker, &["--kind", "reconcile"]).status.success());
    publish(&commit);
    assert_eq!(source_roots(&worker, "jira").await.len(), 4);
    synced(&worker, &["--kind", "reconcile"]);
    publish(&commit);
    assert_eq!(source_roots(&worker, "jira").await.len(), 3);
    fake.call("project", json!({"identity":1,"archived":true}));
    synced(&worker, &["--kind", "reconcile"]);
    publish(&commit);
    assert!(source_roots(&worker, "jira").await.is_empty());
}

#[tokio::test]
#[ignore = "real CLI and RustFS; scripts/ci"]
async fn jira_sync_silent_content_restricted_sections_and_revoke_on_real_s3() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake, "sync");
    fake.call("issue",json!({"identity":101,"summary":"fixture document 0","description":null,"status":null,"issuetype":{"id":"1"},"reporter":null}));
    fake.call("comment",json!({"issue":101,"identity":201,"body":"fixture document 1","visibility":{"type":"group","identifier":"g1"}}));
    super::identity::link_account(
        &worker,
        "jira",
        "atlassian.net:11111111-1111-4111-8111-111111111111:U001",
    )
    .await;
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
