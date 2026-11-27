//! Slack through the actual worker/reader and owned RustFS, using the one HTTP fake.
use super::coordination::{committer_host, worker_command, worker_sync};
use super::incremental::{publish, source_roots};
use super::*;

use super::synthetic::Server as Fake;

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
#[ignore = "actual worker/reader and local storage; coordinator Cargo slot"]
async fn slack_fixed_windows_threads_quarantine_and_reconcile_on_storage() {
    let fake = Fake::start("slack/fake_slack.py");
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

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn slack_sync_silent_content_and_revoke_on_storage() {
    let fake = Fake::start("slack/fake_slack.py");
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
    super::identity::link_account(&worker, "slack", "slack.com:T001:U001").await;
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
    // A running session reads the head at every request here and answers
    // under the published identity: the revoke reaches it without a new session.
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
