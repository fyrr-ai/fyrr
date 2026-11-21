//! GitHub through the real worker and owned RustFS, using the one synthetic HTTP fake.
use super::coordination::{committer_host, worker_command, worker_sync};
use super::incremental::{publish, source_roots};
use super::*;

struct Fake {
    server: super::synthetic::Server,
    key: String,
}
impl Fake {
    fn start() -> Self {
        let server = super::synthetic::Server::start("github/fake_github.py");
        let key = server.connection["private_key"].as_str().unwrap().into();
        Self { server, key }
    }
    /// An issue whose indexed text is its title above `text`; the embedding
    /// service knows that text by the vector it has for `text` itself.
    fn topic(&self, root: &FixtureRoot, number: usize, text: &str) {
        self.server
            .call("topic", json!({"repo_id":1,"number":number,"body":text}));
        let indexed = format!("Synthetic issue {number}\n\n{text}");
        root.embedding
            .insert_both(&indexed, root.embedding.vector(text));
    }
}
fn fixture(fake: &Fake, mode: &str) -> (PathBuf, PathBuf, FixtureRoot) {
    let (worker, root) = setup();
    let text = std::fs::read_to_string(&worker).unwrap();
    let authorization = if mode == "fixed" {
        "readers=['grp:allow']\n"
    } else {
        ""
    };
    let intervals = if mode == "sync" {
        "[sources.intervals]\npermissions='15m'\nmembers='15m'\n"
    } else {
        ""
    };
    std::fs::write(&worker, format!(
        "{text}\n[[sources]]\nid='github'\ntype='github'\npermissions='{mode}'\n\
         {authorization}document_max_bytes='64B'\n[sources.settings]\n\
         app_id=10\ninstallation_id=20\naccount_id=30\nprivate_key={{ env='FYRR_TEST_GITHUB_KEY' }}\n\
         api_endpoint='{}'\n{intervals}[[sources.scope.repositories]]\nid=1\nbranch='main'\n",
        fake.server.endpoint)).unwrap();
    super::coordination::command(&worker, &["writer", "namespace", "create"]);
    // Bootstrap with normal batches; one-row batches are needed only for the connector replay test.
    worker_sync(&worker, &["allowed", "denied", "--full"]);
    let commit = committer_host(&worker, &root);
    publish(&commit);
    for path in [&worker, &commit] {
        let text = std::fs::read_to_string(path).unwrap();
        std::fs::write(
            path,
            format!("{text}\n[writer]\nenvelope_max_rows=1\n[writer.commit]\ngroup_max_rows=1\n"),
        )
        .unwrap();
    }
    (worker, commit, root)
}
fn sync(worker: &Path, fake: &Fake, kinds: &[&str]) -> Output {
    worker_command(worker)
        .env("FYRR_TEST_GITHUB_KEY", &fake.key)
        .args(["writer", "sync", "github"])
        .args(kinds)
        .output()
        .unwrap()
}
fn synced(worker: &Path, fake: &Fake, kinds: &[&str]) -> Value {
    let output = sync(worker, fake, kinds);
    assert!(
        output.status.success(),
        "GitHub worker failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
#[ignore = "real worker and local storage; coordinator Cargo slot"]
async fn github_fixed_replay_quarantine_scope_and_reconcile_on_storage() {
    let fake = Fake::start();
    let (worker, commit, root) = fixture(&fake, "fixed");
    for number in 1..=3 {
        fake.topic(&root, number, &format!("fixture document {}", number - 1));
    }
    fake.server.call("fail", json!({"path":"/repos/synthetic/repo-1/issues/3/comments","status":503,"times":6,"headers":{"Retry-After":"0"}}));
    assert!(
        !sync(&worker, &fake, &["--kind", "content"])
            .status
            .success()
    );
    publish(&commit);
    assert_eq!(
        source_roots(&worker, "github").await,
        vec!["github:repo:1:issue:101:1"]
    );
    let report = synced(&worker, &fake, &["--kind", "content"]);
    assert_eq!(
        report["paid_calls"], 2,
        "only the two novel document inputs are embedded"
    );
    publish(&commit);
    assert_eq!(source_roots(&worker, "github").await.len(), 3);
    fake.topic(&root, 1, "fixture document 4");
    fake.server.call(
        "topic",
        json!({"repo_id":1,"number":4,"body":"fixture document 6 ".repeat(8)}),
    );
    let report = synced(&worker, &fake, &["--kind", "content"]);
    assert_eq!(report["stats"]["quarantined"], 1);
    publish(&commit);
    assert_eq!(source_roots(&worker, "github").await.len(), 3);
    synced(&worker, &fake, &["--kind", "content", "--full"]);
    publish(&commit);
    assert_eq!(source_roots(&worker, "github").await.len(), 3);
    fake.server.call(
        "remove_topic",
        json!({"repo_id":1,"kind":"issue","number":2}),
    );
    fake.server.call("fail", json!({"path":"/repos/synthetic/repo-1/issues","status":503,"times":6,"headers":{"Retry-After":"0"}}));
    assert!(
        !sync(&worker, &fake, &["--kind", "reconcile"])
            .status
            .success()
    );
    publish(&commit);
    assert_eq!(source_roots(&worker, "github").await.len(), 3);
    synced(&worker, &fake, &["--kind", "reconcile"]);
    publish(&commit);
    assert_eq!(source_roots(&worker, "github").await.len(), 2);
    let paths = fake.server.call("requests", json!({}));
    assert!(
        paths
            .as_array()
            .unwrap()
            .iter()
            .all(|path| !path.as_str().unwrap().contains("collaborators"))
    );
}

/// A word only in a pull request's title finds the pull request by full-text
/// search alone: the query has no vector, so the dense lane is degraded.
#[tokio::test]
#[ignore = "real worker and local storage; coordinator Cargo slot"]
async fn github_title_only_word_is_found_by_lexical_search_on_storage() {
    let fake = Fake::start();
    let (worker, commit, root) = fixture(&fake, "fixed");
    fake.server.call(
        "topic",
        json!({"repo_id":1,"kind":"pr","number":5,"body":"fixture document 9",
               "title":"Rename the quokka marshaller"}),
    );
    root.embedding.insert_both(
        "Rename the quokka marshaller\n\nfixture document 9",
        root.embedding.vector("fixture document 9"),
    );
    synced(&worker, &fake, &["--kind", "content"]);
    publish(&commit);
    let result = super::tools::Mcp::open(&worker).call(
        "search",
        json!({"sources":["github"],"query":"quokka","limit":10,"output":"metadata"}),
    );
    assert_eq!(result["isError"], false, "{result}");
    let body = &result["structuredContent"];
    assert_eq!(body["metadata"]["degraded"], json!(["dense"]));
    let roots: Vec<_> = body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["root_id"].as_str().unwrap())
        .collect();
    assert_eq!(roots, ["github:repo:1:pr:1005:5"]);
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn github_sync_silent_content_and_revoke_on_storage() {
    let fake = Fake::start();
    let (worker, commit, root) = fixture(&fake, "sync");
    fake.topic(&root, 1, "fixture document 0");
    fake.server
        .call("collaborator", json!({"repo_id":1,"user_id":7}));
    super::identity::link_account(&worker, "github", "github.com:7").await;
    synced(
        &worker,
        &fake,
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
        json!({"sources":["github"],"query":"fixture query","limit":10}),
        json!({"sources":["github"],"query":"fixture document 0","limit":10}),
    ] {
        let result = reader.call("search", request);
        assert!(
            !result["isError"].as_bool().unwrap_or(false),
            "reader error code: {}",
            result["structuredContent"]["code"]
        );
        assert!(
            !result["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    let fetched = reader.call(
        "fetch",
        json!({"source_id":"github","root_id":"github:repo:1:issue:101:1"}),
    );
    assert!(
        !fetched["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    fake.server.call("clear_readers", json!({"repo_id":1}));
    synced(
        &worker,
        &fake,
        &["--kind", "permissions", "--kind", "members"],
    );
    publish(&commit);
    // Admission pins an identity generation. The old session must fail closed
    // when membership changes; a new session then proves the current rights.
    let rebound = reader.call(
        "search",
        json!({"sources":["github"],"query":"fixture query","limit":10}),
    );
    assert_eq!(
        rebound["structuredContent"]["code"],
        "authorization_unavailable"
    );
    assert_eq!(rebound["structuredContent"]["retryable"], true);
    drop(reader);
    let mut reader = super::tools::Mcp::open(&worker);
    for (tool, request) in [
        (
            "search",
            json!({"sources":["github"],"query":"fixture query","limit":10}),
        ),
        (
            "search",
            json!({"sources":["github"],"query":"fixture document 0","limit":10}),
        ),
        (
            "fetch",
            json!({"source_id":"github","root_id":"github:repo:1:issue:101:1"}),
        ),
    ] {
        let result = reader.call(tool, request);
        assert!(
            !result["isError"].as_bool().unwrap_or(false),
            "reader error code: {}",
            result["structuredContent"]["code"]
        );
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
