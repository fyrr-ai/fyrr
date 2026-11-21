//! The bundled Notion provider through `fyrr writer sync` on the local storage,
//! against the synthetic Notion of `connectors/tests/notion/fake_notion.py`
//! served over HTTP.
//!
//! Page texts are `fixture document N`. A standalone page is indexed as its
//! title above that text, which the fixture's embedding service knows by the
//! vector of the text itself. The source's `document_max_bytes` is 64 bytes.
use super::coordination::{committer_host, earlier_release, worker_command, worker_sync};
use super::incremental::{publish, source_roots};
use super::*;

/// The fake Notion as a process; it ends when this is dropped.
struct Fake {
    server: super::synthetic::Server,
    token: String,
}

impl Fake {
    fn start() -> Self {
        let server = super::synthetic::Server::start("notion/fake_notion.py");
        let token = server.connection["token"].as_str().unwrap().into();
        Self { server, token }
    }

    /// A page with one paragraph, indexed as its name above `text`; its ID.
    fn page(&self, root: &FixtureRoot, name: &str, parent: &str, text: &str) -> String {
        root.embedding
            .insert_both(&format!("{name}\n\n{text}"), root.embedding.vector(text));
        self.unindexed_page(name, parent, text)
    }

    /// A page the worker never embeds: its text is over the source's limit.
    fn unindexed_page(&self, name: &str, parent: &str, text: &str) -> String {
        let blocks = json!([{"kind": "paragraph", "text": text}]);
        self.server
            .call(
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
        self.server
            .call("requests", json!({"match": {"path": path}}))
            .as_array()
            .unwrap()
            .len()
    }
}

/// The dense fixture plus a Notion source rooted at the fake's `root` page,
/// one document per envelope; the worker's config and the committer's.
fn fixture(fake: &Fake) -> (PathBuf, PathBuf, FixtureRoot) {
    let (config, root) = setup();
    let root_page = fake
        .server
        .call("page", json!({"name": "root", "title": "Handbook"}));
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!(
            "{text}\n[[sources]]\nid='notion'\ntype='notion'\npermissions='fixed'\n\
             readers=['grp:allow']\ndocument_max_bytes='64B'\n[sources.settings]\n\
             token={{ env='FYRR_TEST_NOTION_TOKEN' }}\napi_endpoint='{}'\n[sources.scope]\n\
             root_pages=['{}']\n",
            fake.server.endpoint,
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

/// A word only in a standalone page's title finds the page by full-text
/// search alone (the query has no vector, so the dense lane is degraded), also
/// when the page was published before titles were indexed: a full sync
/// republishes it with its title although the page did not change.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn notion_full_sync_indexes_the_title_of_a_page_published_without_it_on_storage() {
    let fake = Fake::start();
    let (worker, commit, root) = fixture(&fake);
    let blocks = json!([{"kind": "paragraph", "text": "fixture document 9"}]);
    let page = fake.server.call(
        "page",
        json!({"name": "quokka", "parent": "root", "title": "Rename the quokka marshaller",
               "blocks": blocks}),
    );
    let vector = root.embedding.vector("fixture document 9");
    root.embedding
        .insert_both("fixture document 9", vector.clone());
    root.embedding
        .insert_both("Rename the quokka marshaller\n\nfixture document 9", vector);
    let search = || {
        let result = super::tools::Mcp::open(&worker).call(
            "search",
            json!({"sources": ["notion"], "query": "quokka", "limit": 10, "output": "metadata"}),
        );
        assert_eq!(result["isError"], false, "{result}");
        let body = &result["structuredContent"];
        assert_eq!(body["metadata"]["degraded"], json!(["dense"]));
        body["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["root_id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };

    // The release before: its render format, and no title in the text.
    let before_titles = earlier_release(
        &worker,
        r#"from fyrr_connectors.notion import connector
connector.RENDER_FORMAT = "2"
text = connector.Text
connector.Text = lambda limit, title=None: text(limit)
"#,
    );
    let mut before = worker_command(&worker);
    before
        .env("FYRR_TEST_NOTION_TOKEN", &fake.token)
        .env("PYTHONPATH", before_titles)
        .args(["writer", "sync", "notion", "--kind", "content"]);
    assert!(before.output().unwrap().status.success());
    publish(&commit);
    assert!(search().is_empty(), "the title was not indexed yet");

    synced(&worker, &fake, &["--kind", "content", "--full"]);
    publish(&commit);
    assert_eq!(fake.reads(page.as_str().unwrap()), 2);
    assert_eq!(search(), [page.as_str().unwrap()]);
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn notion_content_changes_quarantine_and_reconcile_on_storage() {
    let fake = Fake::start();
    let (worker, commit, root) = fixture(&fake);
    let early = fake.page(&root, "early", "root", "fixture document 0");
    let plan = fake.page(&root, "plan", "root", "fixture document 1");
    let notes = fake.page(&root, "notes", "plan", "fixture document 2");
    let late = fake.page(&root, "late", "root", "fixture document 3");

    // The first sync fails at the last page, with `notes` read but not yet
    // handed over. What was acknowledged stays, and the next run continues
    // behind it: the cursor follows the envelopes.
    fake.server.call(
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
    fake.page(&root, "plan", "root", "fixture document 4");
    fake.server
        .call("database", json!({"name": "tasks", "parent": "root"}));
    let task = fake
        .server
        .call(
            "row",
            json!({"name": "task", "database": "tasks", "title": "fixture document 5"}),
        )
        .as_str()
        .unwrap()
        .to_owned();
    fake.unindexed_page("huge", "root", &"fixture document 6 ".repeat(8));
    fake.server.call("trash", json!({"name": "early"}));
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
    fake.server
        .call("move", json!({"name": "notes", "parent": null}));
    synced(&worker, &fake, &["--kind", "content"]);
    fake.server.call(
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
