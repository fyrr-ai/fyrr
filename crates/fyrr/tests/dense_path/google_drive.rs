//! The Google Drive connector through `fyrr writer sync`: the real worker, its
//! real delegated sign-in and the owned RustFS, against the fake Drive served
//! over HTTP (`connectors/tests/google_drive/fake_drive.py`). Path S of
//! docs/connectors/google-drive.md §6.
use super::coordination::{command, committer_host, publication, worker_command, worker_sync};
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
    child: std::process::Child,
    endpoint: String,
    key: String,
    http: reqwest::Client,
}
impl Drive {
    fn start() -> Self {
        let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut child = Command::new(checkout.join("connectors/.venv/bin/python"))
            .arg(checkout.join("connectors/tests/google_drive/fake_drive.py"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("the fake Drive; run scripts/doctor");
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let connection: Value = serde_json::from_str(&line).expect("the fake's connection line");
        Self {
            child,
            endpoint: connection["endpoint"].as_str().unwrap().to_owned(),
            key: connection["service_account"].as_str().unwrap().to_owned(),
            http: reqwest::Client::builder().no_proxy().build().unwrap(),
        }
    }
    /// `POST /fake/<name>`: change the world, inject a fault, read the request log.
    async fn call(&self, name: &str, arguments: Value) -> Value {
        let response = self
            .http
            .post(format!("{}/fake/{name}", self.endpoint))
            .json(&arguments)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert!(status.is_success(), "fake {name}: {body}");
        body["result"].clone()
    }
    async fn put(&self, file: &str, text: &str, place: Value) {
        let mut arguments = json!({"file_id":file,"content":text});
        for (key, value) in place.as_object().unwrap() {
            arguments[key] = value.clone();
        }
        self.call("put", arguments).await;
    }
    /// How often the connector exported `file`.
    async fn exports(&self, file: &str) -> usize {
        self.call(
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
impl Drop for Drive {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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
            drive.endpoint
        ),
    )
    .unwrap();
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
        if !has_readable_documents {
            assert_eq!(
                body["metadata"]["telemetry"]["embedding_provider_calls"]
                    .as_u64()
                    .unwrap_or(0),
                0,
                "empty admission makes no embedding request"
            );
        }
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
    drive.call("user", json!({"email":OWNER})).await;
    drive.call("user", json!({"email":READER})).await;
    drive
        .call("group", json!({"email":staff,"members":[[READER,"USER"]]}))
        .await;
    drive
        .call(
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
        .call(
            "share",
            json!({"file_id":"team","type":"group","emailAddress":staff}),
        )
        .await;
    drive.put("private", text, json!({"owner":OWNER})).await;
    drive
        .put("shared", text, json!({"drive":"drive-eng"}))
        .await;
    let (worker, commit, _root) = fixture(&drive, SYNC);

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
        .call("unshare", json!({"file_id":"direct","permission_id":grant}))
        .await;
    drive.synced(&worker, &["--kind", "permissions"]);
    publish(&commit);
    assert!(!readable(&worker, "direct"));
    in_every_lane(&worker, &["shared", "team"]);

    // The reader leaves the group.
    drive
        .call("group", json!({"email":staff,"members":[]}))
        .await;
    drive.synced(&worker, &["--kind", "members"]);
    publish(&commit);
    assert!(!readable(&worker, "team"));
    in_every_lane(&worker, &["shared"]);

    // The reader leaves the shared drive: no file reports a change.
    drive
        .call(
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

/// The cursor moves only past acknowledged batches: a worker killed between
/// batches resumes without exporting what was delivered, and a reset cursor
/// re-scans without exporting anything published and without losing a delete.
#[tokio::test]
#[ignore = "real CLI, fake Drive and local storage; scripts/ci"]
async fn google_drive_killed_worker_resumes_and_a_full_rescan_keeps_deletes_on_storage() {
    let drive = Drive::start();
    drive.call("user", json!({"email":OWNER})).await;
    for row in 1..=7 {
        drive
            .put(
                &format!("doc-{row}"),
                &format!("fixture document {row}"),
                json!({"owner":OWNER}),
            )
            .await;
    }
    let (worker, commit, _root) = fixture(&drive, FIXED);
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
        .call(
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
    while drive.call("parked", json!({})).await != 1 {
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
    drive.call("release", json!({})).await;
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
        .call("touch", json!({"file_id":"doc-1","trashed":true}))
        .await;
    drive.call("remove", json!({"file_id":"doc-2"})).await;
    drive
        .put("doc-8", "fixture document 8", json!({"owner":OWNER}))
        .await;
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
/// a failed permission read keeps the last rights until the default validity
/// deadline, then hides only this source until a read succeeds.
#[tokio::test]
#[ignore = "real CLI, fake Drive and local storage; scripts/ci"]
async fn google_drive_quarantine_and_permission_read_failure_deadline_on_storage() {
    let drive = Drive::start();
    let place = json!({"drive":"drive-eng"});
    drive.call("user", json!({"email":OWNER})).await;
    drive.call("user", json!({"email":READER})).await;
    drive
        .call(
            "drive",
            json!({"drive_id":"drive-eng","name":"Engineering","members":[OWNER,READER]}),
        )
        .await;
    drive.put("good", "fixture document 0", place.clone()).await;
    let oversized = "fixture document 1 ".repeat(4);
    drive.put("big", &oversized, place.clone()).await;
    drive.put("refused", "fixture document 2", place).await;
    drive
        .call(
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
    drive.put("good", &oversized, json!({})).await;
    drive.synced(&worker, &["--kind", "content"]);
    publish(&commit);
    let kept = fetch(&worker, "good")["structuredContent"]["documents"][0].clone();
    assert_eq!(kept["records"][0]["text"], "fixture document 0");
    assert_eq!(
        command(&commit, &["writer", "status"])["quarantine"]["document_invalid"],
        3
    );

    // The shared drive's members cannot be read: nothing is derived from the
    // failed read, and the source's permission clock stands still.
    let read_at = |head: ServingHead| {
        head.freshness
            .sources
            .into_iter()
            .find(|fresh| fresh.source_id == "drive")
            .unwrap()
            .permissions_read_at
    };
    let before = read_at(publication(&worker).await.0);
    drive
        .call(
            "fail",
            json!({"match":{"path":"/files/drive-eng/permissions$"},"status":403,"reason":"insufficientFilePermissions"}),
        )
        .await;
    drive.refused(&worker, &["--kind", "permissions"]);
    publish(&commit);
    assert_eq!(read_at(publication(&worker).await.0), before);
    assert!(readable(&worker, "good"));

    let loaded = fyrr_core::config::load(&worker).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let search = || {
        command(
            &worker,
            &["reader", "search", "fixture query", "--limit", "2"],
        )
    };
    let sources = |search: &Value| {
        search["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["source_id"].as_str().unwrap().to_owned())
            .collect::<BTreeSet<_>>()
    };
    let drive_hits = || {
        let result = Mcp::open(&worker).call(
            "search",
            json!({"query":"fixture query","sources":["drive"],"limit":5}),
        );
        assert!(!result["isError"].as_bool().unwrap_or(false), "{result}");
        result["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len()
    };
    // The deadline is the default one hour: no `validity_deadline` is configured.
    for (minutes, hidden) in [(59, false), (61, true)] {
        let mut head = publication(&worker).await.0;
        head.freshness
            .sources
            .iter_mut()
            .find(|fresh| fresh.source_id == "drive")
            .unwrap()
            .permissions_read_at = fyrr_core::clock::now() - chrono::Duration::minutes(minutes);
        write_head(store.as_ref(), &namespace, &head).await;
        let found = search();
        let expected = if hidden { json!(["drive"]) } else { json!([]) };
        assert_eq!(
            found["metadata"]["hidden_sources"], expected,
            "{minutes} min"
        );
        assert!(
            sources(&found).contains("allowed"),
            "the namespace keeps serving"
        );
        assert!(
            !hidden || !sources(&found).contains("drive"),
            "{minutes} min"
        );
        assert_eq!(drive_hits(), usize::from(!hidden), "{minutes} min");
        assert_eq!(readable(&worker, "good"), !hidden, "{minutes} min");
    }

    // A successful read restarts the clock.
    drive.synced(&worker, &["--kind", "permissions"]);
    publish(&commit);
    assert!(read_at(publication(&worker).await.0) > before);
    assert_eq!(search()["metadata"]["hidden_sources"], json!([]));
    assert!(readable(&worker, "good"));
}
