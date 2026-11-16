//! The bundled Notion provider through `fyrr writer sync` on the local storage,
//! against the synthetic Notion of `connectors/tests/notion/fake_notion.py`
//! served over HTTP.
//!
//! Page texts are `fixture document N`, inputs registered with the fixture's embedding service
//! embeddings; the source's `document_max_bytes` is 64 bytes.
use super::coordination::{committer_host, worker_command, worker_sync};
use super::incremental::{publish, source_roots};
use super::*;

/// The fake Notion as a process; it ends when this is dropped.
struct Fake {
    child: std::process::Child,
    endpoint: String,
    token: String,
}

impl Fake {
    fn start() -> Self {
        let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut child = Command::new(checkout.join("connectors/.venv/bin/python"))
            .arg(checkout.join("connectors/tests/notion/fake_notion.py"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("fake Notion");
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let connection: Value = serde_json::from_str(&line).expect("fake Notion connection");
        Self {
            child,
            endpoint: connection["endpoint"].as_str().unwrap().to_owned(),
            token: connection["token"].as_str().unwrap().to_owned(),
        }
    }

    /// `POST /fake/<name>`: change the world, inject a fault, read the request log.
    fn call(&self, name: &str, arguments: Value) -> Value {
        let output = Command::new("curl")
            .args([
                "-sS",
                "-X",
                "POST",
                "-H",
                "Content-Type: application/json",
                "-d",
            ])
            .arg(arguments.to_string())
            .arg(format!("{}/fake/{name}", self.endpoint))
            .output()
            .expect("curl");
        let answer: Value = serde_json::from_slice(&output.stdout).unwrap_or(Value::Null);
        assert!(
            answer.get("result").is_some(),
            "fake {name}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        answer["result"].clone()
    }

    /// A page with one paragraph; its ID.
    fn page(&self, name: &str, parent: &str, text: &str) -> String {
        let blocks = json!([{"kind": "paragraph", "text": text}]);
        self.call(
            "page",
            json!({"name": name, "parent": parent, "blocks": blocks}),
        )
        .as_str()
        .unwrap()
        .to_owned()
    }

    /// How often the page's own block list was asked for.
    fn reads(&self, page: &str) -> usize {
        let path = format!("/blocks/{page}/children$");
        self.call("requests", json!({"match": {"path": path}}))
            .as_array()
            .unwrap()
            .len()
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The dense fixture plus a Notion source rooted at the fake's `root` page,
/// one document per envelope; the worker's config and the committer's.
fn fixture(fake: &Fake) -> (PathBuf, PathBuf, FixtureRoot) {
    let (config, root) = setup();
    let root_page = fake.call("page", json!({"name": "root", "title": "Handbook"}));
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!(
            "{text}\n[[sources]]\nid='notion'\ntype='notion'\npermissions='fixed'\n\
             readers=['grp:allow']\ndocument_max_bytes='64B'\n[sources.settings]\n\
             token={{ env='FYRR_TEST_NOTION_TOKEN' }}\napi_endpoint='{}'\n[sources.scope]\n\
             root_pages=['{}']\n",
            fake.endpoint,
            root_page.as_str().unwrap()
        ),
    )
    .unwrap();
    root.embedding.insert_both(
        "Name: fixture document 5",
        root.embedding.vector("fixture document 5"),
    );
    super::coordination::command(&config, &["writer", "namespace", "create"]);
    worker_sync(&config, &["allowed", "denied", "--full"]);
    let commit = committer_host(&config, &root);
    publish(&commit);
    // The resume proof needs one Notion document per envelope. Its unrelated
    // dense baseline uses normal batching, then both hosts adopt these limits.
    for path in [&config, &commit] {
        let text = std::fs::read_to_string(path).unwrap();
        std::fs::write(
            path,
            format!("{text}\n[writer]\nenvelope_max_rows=1\n[writer.commit]\ngroup_max_rows=1\n"),
        )
        .unwrap();
    }
    (config, commit, root)
}

fn sync(config: &Path, fake: &Fake, args: &[&str]) -> Output {
    worker_command(config)
        .env("FYRR_TEST_NOTION_TOKEN", &fake.token)
        .args(["writer", "sync", "notion"])
        .args(args)
        .output()
        .unwrap()
}

fn synced(config: &Path, fake: &Fake, args: &[&str]) -> Value {
    let output = sync(config, fake, args);
    assert!(
        output.status.success(),
        "notion sync {args:?}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("single JSON result")
}

fn sorted(mut ids: Vec<String>) -> Vec<String> {
    ids.sort();
    ids
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn notion_content_changes_quarantine_and_reconcile_on_storage() {
    let fake = Fake::start();
    let (worker, commit, _root) = fixture(&fake);
    let early = fake.page("early", "root", "fixture document 0");
    let plan = fake.page("plan", "root", "fixture document 1");
    let notes = fake.page("notes", "plan", "fixture document 2");
    let late = fake.page("late", "root", "fixture document 3");

    // The first sync fails at the last page, with `notes` read but not yet
    // handed over. What was acknowledged stays, and the next run continues
    // behind it: the cursor follows the envelopes.
    fake.call(
        "fail",
        json!({"match": {"path": format!("/blocks/{late}/children$")},
               "status": 503, "times": 6, "retry_after": "0"}),
    );
    assert!(
        !sync(&worker, &fake, &["--kind", "content"])
            .status
            .success()
    );
    publish(&commit);
    assert_eq!(
        source_roots(&worker, "notion").await,
        sorted(vec![early.clone(), plan.clone()])
    );
    assert_eq!(fake.reads(&early), 1);
    let report = synced(&worker, &fake, &["--kind", "content"]);
    assert_eq!(
        report["paid_calls"], 2,
        "only the two novel document inputs are embedded"
    );
    publish(&commit);
    assert_eq!(
        source_roots(&worker, "notion").await,
        sorted(vec![
            early.clone(),
            plan.clone(),
            notes.clone(),
            late.clone()
        ])
    );
    assert_eq!(
        fake.reads(&early),
        1,
        "a page before the position is not read again"
    );

    // Edit, a database row, trash; an oversized page is quarantined and the sync goes on.
    fake.page("plan", "root", "fixture document 4");
    fake.call("database", json!({"name": "tasks", "parent": "root"}));
    let task = fake
        .call(
            "row",
            json!({"name": "task", "database": "tasks", "title": "fixture document 5"}),
        )
        .as_str()
        .unwrap()
        .to_owned();
    fake.page("huge", "root", &"fixture document 6 ".repeat(8));
    fake.call("trash", json!({"name": "early"}));
    let report = synced(&worker, &fake, &["--kind", "content"]);
    assert_eq!(report["stats"]["quarantined"], 1);
    assert_eq!(
        report["paid_calls"], 2,
        "only the two novel document inputs are embedded"
    );
    publish(&commit);
    let current = sorted(vec![
        plan.clone(),
        notes.clone(),
        late.clone(),
        task.clone(),
    ]);
    assert_eq!(source_roots(&worker, "notion").await, current);

    // A reset cursor walks everything again and publishes nothing twice.
    let reads = fake.reads(&late);
    synced(&worker, &fake, &["--kind", "content", "--full"]);
    assert_eq!(fake.reads(&late), reads + 1);
    publish(&commit);
    assert_eq!(source_roots(&worker, "notion").await, current);

    // A page that left the roots has no change entry. A failed listing
    // deletes nothing; the next complete one removes it.
    fake.call("move", json!({"name": "notes", "parent": null}));
    synced(&worker, &fake, &["--kind", "content"]);
    fake.call(
        "fail",
        json!({"match": {"path": format!("/blocks/{late}/children$")},
               "status": 503, "times": 6, "retry_after": "0"}),
    );
    assert!(
        !sync(&worker, &fake, &["--kind", "reconcile"])
            .status
            .success()
    );
    publish(&commit);
    assert_eq!(source_roots(&worker, "notion").await, current);
    synced(&worker, &fake, &["--kind", "reconcile"]);
    publish(&commit);
    assert_eq!(
        source_roots(&worker, "notion").await,
        sorted(vec![plan, late, task])
    );
}
