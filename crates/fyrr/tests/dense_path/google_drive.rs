//! The Google Drive connector through `fyrr writer sync`: the real worker, its
//! real delegated sign-in and the owned RustFS, against the fake Drive served
//! over HTTP (`connectors/tests/google_drive/fake_drive.py`). Path S of
//! docs/connectors/google-drive.md §6.
use super::coordination::{command, committer_host, earlier_release, worker_command, worker_sync};
use super::incremental::{envelopes, publish, roots, row_readers, source_rows};
use super::tools::Mcp;
use super::*;
use fyrr_core::connector::SyncKind;
use fyrr_core::envelope::EnvelopePayload;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

const OWNER: &str = "owner@example.test";
/// The stdio subject of the reader; the Directory snapshot links it by e-mail.
const READER: &str = "fixture@example.test";
const SCOPE: &str = "owners=['owner@example.test']";
const FIXED: &str = "permissions='fixed'\nreaders=['grp:allow']";
const SYNC: &str = "permissions='sync'\nintervals={ permissions='5m', members='5m' }";

/// The fake Drive as a process. It ends with the test: on drop, or when its
/// standard input closes.
struct Drive {
    server: super::synthetic::Server,
    key: String,
    /// `(file, text)` of the files put since the embedding service last learned them.
    texts: std::sync::Mutex<Vec<(String, String)>>,
}
impl Drive {
    fn start() -> Self {
        let server = super::synthetic::Server::start("google_drive/fake_drive.py");
        let key = server.connection["service_account"]
            .as_str()
            .unwrap()
            .into();
        Self {
            server,
            key,
            texts: Default::default(),
        }
    }
    /// A Doc that is indexed as its name (the file ID unless `place` names it)
    /// above `text`; call `embed` before the next sync.
    async fn put(&self, file: &str, text: &str, place: Value) {
        let name = place["name"].as_str().unwrap_or(file).to_owned();
        self.texts.lock().unwrap().push((name, text.to_owned()));
        self.put_unindexed(file, text, place).await;
    }
    /// The embedding service learns the files put so far, by the vector of their text.
    fn embed(&self, root: &FixtureRoot) {
        for (file, text) in self.texts.lock().unwrap().drain(..) {
            root.embedding
                .insert_both(&format!("{file}\n\n{text}"), root.embedding.vector(&text));
        }
    }
    /// A file the worker never embeds: its text is over the source's limit.
    async fn put_unindexed(&self, file: &str, text: &str, place: Value) {
        let mut arguments = json!({"file_id":file,"content":text});
        for (key, value) in place.as_object().unwrap() {
            arguments[key] = value.clone();
        }
        self.server.call_async("put", arguments).await;
    }
    /// How often the connector exported `file`.
    async fn exports(&self, file: &str) -> usize {
        self.server
            .call_async(
                "requests",
                json!({"match":{"path":format!("/files/{file}/export$")}}),
            )
            .await
            .as_array()
            .unwrap()
            .len()
    }
    /// `fyrr writer sync drive` as a worker that holds the fake's key.
    fn sync_command(&self, config: &Path) -> Command {
        let mut command = worker_command(config);
        command
            .env("FYRR_TEST_GDRIVE_KEY", &self.key)
            .env("NO_PROXY", "127.0.0.1")
            .args(["writer", "sync", "drive"]);
        command
    }
    fn sync(&self, config: &Path, args: &[&str]) -> Output {
        self.sync_command(config).args(args).output().unwrap()
    }
    fn synced(&self, config: &Path, args: &[&str]) {
        let output = self.sync(config, args);
        assert!(
            output.status.success(),
            "drive sync {args:?}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    fn refused(&self, config: &Path, args: &[&str]) {
        let output = self.sync(config, args);
        assert!(!output.status.success(), "drive sync {args:?} succeeded");
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("\"failed\":true"),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}
/// A published namespace (the local-file sources) whose `drive` source reads
/// the fake. `keys` are the source's own keys: permissions mode, readers,
/// intervals, limits.
fn fixture(drive: &Drive, keys: &str) -> (PathBuf, PathBuf, FixtureRoot) {
    let (config, root) = setup();
    let text = std::fs::read_to_string(&config).unwrap().replace(
        "stdio_user = \"fixture-subject\"",
        &format!("stdio_user = \"{READER}\""),
    );
    std::fs::write(
        &config,
        format!(
            "{text}\n[[sources]]\nid='drive'\ntype='google-drive'\n{keys}\n[sources.settings]\nservice_account={{ env='FYRR_TEST_GDRIVE_KEY' }}\nadmin_email='admin@example.com'\napi_endpoint='{}'\n[sources.scope]\n{SCOPE}\n",
            drive.server.endpoint
        ),
    )
    .unwrap();
    drive.embed(&root);
    super::coordination::command(&config, &["writer", "namespace", "create"]);
    worker_sync(&config, &["allowed", "denied", "--full"]);
    let commit = committer_host(&config, &root);
    publish(&commit);
    (config, commit, root)
}

/// The published Drive documents, one entry per row.
async fn published(config: &Path) -> Vec<String> {
    let mut found = roots(&source_rows(config, "drive").await);
    found.sort();
    found
}

/// What the reader's subject gets for a Drive document. The reader is opened
/// for the call: an open one refuses its first request after the published
/// identity moved.
fn fetch(config: &Path, root: &str) -> Value {
    Mcp::open(config).call("fetch", json!({"source_id":"drive","root_id":root}))
}

fn readable(config: &Path, root: &str) -> bool {
    let result = fetch(config, root);
    !result["isError"].as_bool().unwrap_or(false)
        && result["structuredContent"]["documents"]
            .as_array()
            .is_some_and(|documents| !documents.is_empty())
}

/// Public searches isolate dense and lexical retrieval before checking their composition.
pub(super) fn lanes(
    config: &Path,
    source: &str,
    has_readable_documents: bool,
) -> BTreeMap<&'static str, BTreeSet<String>> {
    let mut client = Mcp::open(config);
    [
        ("dense", "dense-only query"),
        ("lexical", "document"),
        ("composition", "fixture query"),
    ]
    .into_iter()
    .map(|(lane, query)| {
        let result = client.call(
            "search",
            json!({"query":query,"sources":[source],"limit":75,"output":"metadata"}),
        );
        assert_eq!(result["isError"], false, "{result}");
        let body = &result["structuredContent"];
        assert_eq!(
            body["metadata"]["degraded"],
            if lane == "lexical" || !has_readable_documents {
                json!(["dense"])
            } else {
                json!([])
            }
        );
        for field in ["embedding_memory_hits", "embedding_provider_calls"] {
            assert!(
                body["metadata"]["telemetry"].get(field).is_none(),
                "shared embedding cache activity must remain private"
            );
        }
        assert!(
            body["metadata"]["telemetry"]["stage_us"]
                .get("embedding_provider")
                .is_none(),
            "provider-stage presence must not reveal shared cache activity"
        );
        let found = body["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| {
                assert_eq!(hit["source_id"], source);
                hit["root_id"].as_str().unwrap().to_owned()
            })
            .collect();
        (lane, found)
    })
    .collect()
}

fn in_every_lane(config: &Path, expected: &[&str]) {
    let expected: BTreeSet<String> = expected.iter().map(|root| (*root).to_owned()).collect();
    for (lane, found) in lanes(config, "drive", !expected.is_empty()) {
        assert_eq!(found, expected, "{lane} lane");
    }
}

/// Under `sync` the permission stream alone decides who reads a document:
/// direct, group and shared-drive grants admit the reader, and each revoke
/// removes the document from every retrieval lane.
#[tokio::test]
#[ignore = "real CLI, fake Drive and local storage; scripts/ci"]
async fn google_drive_sync_source_grants_and_revokes_in_every_lane_on_storage() {
    let drive = Drive::start();
    let staff = "staff@example.test";
    let grant = format!("p-{READER}");
    drive
        .server
        .call_async("user", json!({"email":OWNER}))
        .await;
    drive
        .server
        .call_async("user", json!({"email":READER}))
        .await;
    drive
        .server
        .call_async("group", json!({"email":staff,"members":[[READER,"USER"]]}))
        .await;
    drive
        .server
        .call_async(
            "drive",
            json!({"drive_id":"drive-eng","name":"Engineering","members":[OWNER,READER]}),
        )
        .await;
    // Every text equals the fixture query's vector, so the dense lane would
    // return each of them if its rows admitted the reader.
    let text = "fixture document 0";
    drive
        .put(
            "direct",
            text,
            json!({"owner":OWNER,"shared_with":[READER]}),
        )
        .await;
    drive.put("team", text, json!({"owner":OWNER})).await;
    drive
        .server
        .call_async(
            "share",
            json!({"file_id":"team","type":"group","emailAddress":staff}),
        )
        .await;
    drive.put("private", text, json!({"owner":OWNER})).await;
    drive
        .put("shared", text, json!({"drive":"drive-eng"}))
        .await;
    let (worker, commit, root) = fixture(&drive, SYNC);

    let every_kind = [
        "--kind",
        "content",
        "--kind",
        "permissions",
        "--kind",
        "members",
    ];
    drive.synced(&worker, &every_kind);
    publish(&commit);
    assert_eq!(
        published(&worker).await,
        ["direct", "private", "shared", "team"]
    );
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let token = |principal: &str| {
        fyrr_writer::apply::reader_token(&loaded.config, "drive", &format!("usr:{principal}"))
            .unwrap()
    };
    let readers = |batch: &arrow_array::RecordBatch, root: &str| {
        row_readers(batch, root)
            .into_iter()
            .collect::<BTreeSet<_>>()
    };
    let batch = source_rows(&worker, "drive").await;
    assert_eq!(readers(&batch, "private"), BTreeSet::from([token(OWNER)]));
    let granted = BTreeSet::from([token(OWNER), token(READER)]);
    assert_eq!(readers(&batch, "direct"), granted);
    for root in ["direct", "team", "shared"] {
        assert!(readable(&worker, root), "{root}");
    }
    assert!(!readable(&worker, "private"));
    in_every_lane(&worker, &["direct", "shared", "team"]);

    // A content edit carries no readers: the applied permissions stay.
    drive.put("direct", text, json!({})).await;
    drive.embed(&root);
    drive.synced(&worker, &["--kind", "content"]);
    publish(&commit);
    assert_eq!(drive.exports("direct").await, 2);
    assert_eq!(
        readers(&source_rows(&worker, "drive").await, "direct"),
        granted
    );
    assert!(readable(&worker, "direct"));

    // The direct grant is revoked.
    drive
        .server
        .call_async("unshare", json!({"file_id":"direct","permission_id":grant}))
        .await;
    drive.synced(&worker, &["--kind", "permissions"]);
    publish(&commit);
    assert!(!readable(&worker, "direct"));
    in_every_lane(&worker, &["shared", "team"]);

    // The reader leaves the group.
    drive
        .server
        .call_async("group", json!({"email":staff,"members":[]}))
        .await;
    drive.synced(&worker, &["--kind", "members"]);
    publish(&commit);
    assert!(!readable(&worker, "team"));
    in_every_lane(&worker, &["shared"]);

    // The reader leaves the shared drive: no file reports a change.
    drive
        .server
        .call_async(
            "leave",
            json!({"drive_id":"drive-eng","permission_id":grant}),
        )
        .await;
    drive.synced(&worker, &["--kind", "permissions"]);
    publish(&commit);
    assert!(!readable(&worker, "shared"));
    in_every_lane(&worker, &[]);
    assert_eq!(
        drive.exports("shared").await,
        1,
        "no revoke rewrote content"
    );
}

/// A word only in a file's name finds the file by full-text search alone (the
/// query has no vector, so the dense lane is degraded), also when the file was
/// published before names were indexed: a full sync republishes it with its
/// name although the file did not change.
#[tokio::test]
#[ignore = "real CLI, fake Drive and local storage; scripts/ci"]
async fn google_drive_full_sync_indexes_the_name_of_a_file_published_without_it_on_storage() {
    let drive = Drive::start();
    drive
        .server
        .call_async("user", json!({"email":OWNER}))
        .await;
    drive
        .put(
            "plan",
            "fixture document 9",
            json!({"owner":OWNER,"name":"Rename the quokka marshaller"}),
        )
        .await;
    let (worker, commit, root) = fixture(&drive, FIXED);
    root.embedding.insert_both(
        "fixture document 9",
        root.embedding.vector("fixture document 9"),
    );
    let search = || {
        let result = Mcp::open(&worker).call(
            "search",
            json!({"sources":["drive"],"query":"quokka","limit":10,"output":"metadata"}),
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

    // The release before: its export format and order, and no name in the text.
    let before_names = earlier_release(
        &worker,
        r#"from fyrr_connectors.google_drive import connector
connector.EXPORT_FORMAT = 1
connector._order = lambda file: int(file["version"])
for kind, (export, parse, source, path) in list(connector.FORMATS.items()):
    connector.FORMATS[kind] = (export, lambda data, limit, name, parse=parse: parse(data, limit, None), source, path)
"#,
    );
    let mut before = drive.sync_command(&worker);
    before
        .env("PYTHONPATH", before_names)
        .args(["--kind", "content"]);
    assert!(before.output().unwrap().status.success());
    publish(&commit);
    assert_eq!(published(&worker).await, ["plan"]);
    assert!(search().is_empty(), "the name was not indexed yet");

    drive.synced(&worker, &["--kind", "content", "--full"]);
    publish(&commit);
    assert_eq!(drive.exports("plan").await, 2);
    assert_eq!(search(), ["plan"]);
}

/// The cursor moves only past acknowledged batches: a worker killed between
/// batches resumes without exporting what was delivered, and a reset cursor
/// re-scans without exporting anything published and without losing a delete.
#[tokio::test]
#[ignore = "real CLI, fake Drive and local storage; scripts/ci"]
async fn google_drive_killed_worker_resumes_and_a_full_rescan_keeps_deletes_on_storage() {
    let drive = Drive::start();
    drive
        .server
        .call_async("user", json!({"email":OWNER}))
        .await;
    for row in 1..=7 {
        drive
            .put(
                &format!("doc-{row}"),
                &format!("fixture document {row}"),
                json!({"owner":OWNER}),
            )
            .await;
    }
    let (worker, commit, root) = fixture(&drive, FIXED);
    // Two documents per batch, as the fake's pages. Only the worker that is
    // killed gets a short cursor lease: a live sync must not lose its lease
    // to a busy host.
    let text = format!(
        "{}\n[writer]\nenvelope_max_rows = 2\n[writer.commit]\ngroup_max_rows = 4\n",
        std::fs::read_to_string(&worker).unwrap()
    );
    std::fs::write(&worker, &text).unwrap();
    let victim = worker.with_file_name("victim.toml");
    std::fs::write(&victim, format!("{text}lease_ttl = '6s'\n")).unwrap();

    // The third page of the first listing never answers; by then the first
    // batch is durable and the second is exported but not handed over.
    drive
        .server
        .call_async(
            "hold",
            json!({"match":{"path":"/files$","params":{"pageToken":"^4$"}}}),
        )
        .await;
    let mut sync = drive
        .sync_command(&victim)
        .args(["--kind", "content"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    while drive.server.call_async("parked", json!({})).await != 1 {
        assert!(
            sync.try_wait().unwrap().is_none(),
            "the sync ended before its third page"
        );
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let killed = Command::new("pkill")
        .args(["-KILL", "-P", &sync.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success(), "the worker process of the sync");
    assert!(!sync.wait_with_output().unwrap().status.success());
    drive.server.call_async("release", json!({})).await;
    let delivered: Vec<_> = envelopes(&worker)
        .await
        .into_iter()
        .filter(|(_, envelope)| envelope.source_id == "drive" && envelope.kind == SyncKind::Content)
        .collect();
    assert_eq!(delivered.len(), 1);
    let (_, first) = &delivered[0];
    assert!(first.cursor.start.is_none() && first.advances_cursor);
    let EnvelopePayload::Content(payload) = &first.payload else {
        panic!("a content envelope")
    };
    let handed: Vec<_> = payload
        .documents
        .iter()
        .map(|d| d.root_id.as_str())
        .collect();
    assert_eq!(handed, ["doc-1", "doc-2"]);

    // The killed worker cannot renew its cursor lease; expire it before resume.
    fyrr_core::clock::advance(Duration::from_secs(7)).unwrap();
    drive.synced(&worker, &["--kind", "content"]);
    publish(&commit);
    let all: Vec<_> = (1..=7).map(|row| format!("doc-{row}")).collect();
    assert_eq!(published(&worker).await, all);
    for file in ["doc-1", "doc-2"] {
        assert_eq!(drive.exports(file).await, 1, "{file} was already durable");
    }
    let resumed = envelopes(&worker)
        .await
        .into_iter()
        .filter(|(_, envelope)| envelope.source_id == "drive" && envelope.kind == SyncKind::Content)
        .filter(|(_, envelope)| envelope.cursor.start.as_ref() == Some(&first.cursor.end))
        .count();
    assert_eq!(
        resumed, 1,
        "the next batch starts at the acknowledged cursor"
    );

    // A reset cursor: the trashed file's change entry is behind the new start
    // token and the deleted file has none, so both leave at the listing.
    drive
        .server
        .call_async("touch", json!({"file_id":"doc-1","trashed":true}))
        .await;
    drive
        .server
        .call_async("remove", json!({"file_id":"doc-2"}))
        .await;
    drive
        .put("doc-8", "fixture document 8", json!({"owner":OWNER}))
        .await;
    drive.embed(&root);
    drive.synced(
        &worker,
        &["--kind", "content", "--kind", "reconcile", "--full"],
    );
    publish(&commit);
    let kept: Vec<_> = (3..=8).map(|row| format!("doc-{row}")).collect();
    assert_eq!(published(&worker).await, kept);
    // Only the batch the killed worker had not handed over was exported twice.
    for row in 3..=8 {
        let file = format!("doc-{row}");
        let expected = if row == 3 || row == 4 { 2 } else { 1 };
        assert_eq!(drive.exports(&file).await, expected, "{file}");
    }
}

/// A file that cannot be exported is quarantined while the sync goes on, and
/// a failed permission read, also one that fails after its first batch, leaves
/// the last rights in place.
#[tokio::test]
#[ignore = "real CLI, fake Drive and local storage; scripts/ci"]
async fn google_drive_quarantine_and_permission_read_failure_keeps_rights_on_storage() {
    let drive = Drive::start();
    let place = json!({"drive":"drive-eng"});
    drive
        .server
        .call_async("user", json!({"email":OWNER}))
        .await;
    drive
        .server
        .call_async("user", json!({"email":READER}))
        .await;
    drive
        .server
        .call_async(
            "drive",
            json!({"drive_id":"drive-eng","name":"Engineering","members":[OWNER,READER]}),
        )
        .await;
    drive.put("good", "fixture document 0", place.clone()).await;
    let oversized = "fixture document 1 ".repeat(4);
    drive.put_unindexed("big", &oversized, place.clone()).await;
    drive.put("refused", "fixture document 2", place).await;
    drive.server
        .call_async(
            "fail",
            json!({"match":{"path":"/files/refused/export$"},"status":403,"reason":"exportSizeLimitExceeded"}),
        )
        .await;
    let (worker, commit, _root) = fixture(&drive, &format!("{SYNC}\ndocument_max_bytes='64B'"));

    drive.synced(
        &worker,
        &[
            "--kind",
            "content",
            "--kind",
            "permissions",
            "--kind",
            "members",
        ],
    );
    publish(&commit);
    assert_eq!(published(&worker).await, ["good"]);
    assert_eq!(
        command(&commit, &["writer", "status"])["quarantine"]["document_invalid"],
        2
    );
    assert!(readable(&worker, "good"));
    // A quarantined edit keeps the last good revision.
    drive.put_unindexed("good", &oversized, json!({})).await;
    drive.synced(&worker, &["--kind", "content"]);
    publish(&commit);
    let kept = fetch(&worker, "good")["structuredContent"]["documents"][0].clone();
    assert_eq!(kept["records"][0]["text"], "good\n\nfixture document 0");
    assert_eq!(
        command(&commit, &["writer", "status"])["quarantine"]["document_invalid"],
        3
    );

    // The shared drive's members cannot be read: nothing is derived from the
    // failed read, and the last rights stand.
    drive.server
        .call_async(
            "fail",
            json!({"match":{"path":"/files/drive-eng/permissions$"},"status":403,"reason":"insufficientFilePermissions"}),
        )
        .await;
    drive.refused(&worker, &["--kind", "permissions"]);
    publish(&commit);
    assert!(readable(&worker, "good"));

    // A later successful read changes nothing visible, and the writer still
    // reports when each source's permissions were last read.
    drive.synced(&worker, &["--kind", "permissions"]);
    publish(&commit);
    assert!(readable(&worker, "good"));
    let status = command(&commit, &["writer", "status"]);
    assert!(
        status["source_reads"]
            .as_array()
            .unwrap()
            .iter()
            .any(|read| read["source_id"] == "drive" && read["kind"] == "permissions"),
        "{status}"
    );

    // The reader leaves the shared drive and a later page of the same read
    // fails, after the batch that carries the drive's grant was read: the
    // revoke is not published alone, and the next read publishes it whole.
    drive
        .server
        .call_async(
            "leave",
            json!({"drive_id":"drive-eng","permission_id":format!("p-{READER}")}),
        )
        .await;
    drive.server
        .call_async(
            "fail",
            json!({"match":{"path":"/files$","params":{"pageToken":"^2$"}},"status":403,"reason":"insufficientFilePermissions"}),
        )
        .await;
    drive.refused(&worker, &["--kind", "permissions", "--full"]);
    publish(&commit);
    assert!(readable(&worker, "good"), "a failed read publishes nothing");
    drive.synced(&worker, &["--kind", "permissions", "--full"]);
    publish(&commit);
    assert!(
        !readable(&worker, "good"),
        "the next read publishes the revoke"
    );
}
