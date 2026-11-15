//! All tools through independent stdio requests over the writer's real S3 publication.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::process::Child;

pub(super) struct Mcp {
    child: Child,
    input: std::process::ChildStdin,
    output: std::sync::mpsc::Receiver<std::io::Result<String>>,
    next: u64,
    stderr_log: Option<PathBuf>,
}
impl Mcp {
    pub(super) fn open(config: &Path) -> Self {
        Self::open_with_env(config, &[])
    }
    pub(super) fn open_logged(config: &Path, log: &Path) -> Self {
        Self::open_options(config, &[], Some(log))
    }
    fn open_with_env(config: &Path, env: &[(&str, &Path)]) -> Self {
        Self::open_options(config, env, None)
    }
    fn open_options(config: &Path, env: &[(&str, &Path)], log: Option<&Path>) -> Self {
        let binary = std::env::var_os("FYRR_GATE_BINARY")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_fyrr").into());
        Self::open_binary(config, &binary, env, log)
    }
    fn open_binary(
        config: &Path,
        binary: &std::ffi::OsStr,
        env: &[(&str, &Path)],
        log: Option<&Path>,
    ) -> Self {
        let mut child = Command::new(binary)
            .arg("--config")
            .arg(config)
            .args(["reader", "serve"])
            .envs(env.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(log.map_or_else(Stdio::piped, |path| {
                Stdio::from(std::fs::File::create(path).unwrap())
            }))
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let (lines, output) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            loop {
                let mut line = String::new();
                let result = stdout.read_line(&mut line).map(|_| line);
                let done = !matches!(&result, Ok(line) if !line.is_empty());
                if lines.send(result).is_err() || done {
                    break;
                }
            }
        });
        let mut client = Self {
            child,
            input,
            output,
            next: 0,
            stderr_log: log.map(Path::to_owned),
        };
        client.send(json!({"jsonrpc":"2.0","id":"ready","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"fyrr-tests","version":"1"}}}));
        assert_eq!(client.read()["id"], "ready");
        client.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        client
    }
    pub(super) fn send(&mut self, value: Value) {
        writeln!(self.input, "{value}").unwrap();
        self.input.flush().unwrap();
    }
    pub(super) fn read(&mut self) -> Value {
        let line = self
            .output
            .recv_timeout(std::time::Duration::from_secs(120))
            .expect("MCP response watchdog")
            .expect("MCP response read");
        if line.is_empty() {
            use std::io::Read;
            let _ = self.child.kill();
            let _ = self.child.wait();
            let mut diagnostics = self
                .stderr_log
                .as_ref()
                .map(|p| std::fs::read_to_string(p).unwrap())
                .unwrap_or_default();
            if let Some(mut stderr) = self.child.stderr.take() {
                stderr.read_to_string(&mut diagnostics).unwrap();
            }
            panic!("MCP ended: {diagnostics}");
        }
        serde_json::from_str(&line).unwrap()
    }
    pub(super) fn pid(&self) -> u32 {
        self.child.id()
    }
    /// An admitted subject pins its identity generation. Prove the old
    /// session fails closed, then bind a new one to the published identity.
    pub(super) fn rebind(&mut self, config: &Path, source: &str, root: &str) {
        let denied = self.call("fetch", json!({"source_id":source,"root_id":root}));
        assert_eq!(denied["isError"], true);
        assert_eq!(
            denied["structuredContent"]["code"],
            "authorization_unavailable"
        );
        assert_eq!(denied["structuredContent"]["retryable"], true);
        *self = Self::open(config);
    }

    pub(super) fn call(&mut self, name: &str, args: Value) -> Value {
        self.next += 1;
        let id = self.next;
        self.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":args}}));
        let response = self.read();
        assert_eq!(response["id"], id);
        response["result"].clone()
    }
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_hidden_section_insertion_removal_preserves_record_windows_on_real_s3() {
    let (config, root, _) = fixture();
    let mut input: Value =
        serde_json::from_slice(&std::fs::read(root.join("documents.json")).unwrap()).unwrap();
    let document = input["documents"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|d| d["id"] == "restricted")
        .unwrap();
    // Force pagination within the visible section, after an unreadable section.
    document["note"] = json!("restricted metadata");
    document["text"] = json!("Résumé 👩🏽‍💻 exactneedle authorized passage.".repeat(4000));
    let visible = json!({"text":document["text"],"readers":["grp:allow"],"location":{"kind":"heading","label":"visible-section"}});
    let hidden = json!({"text":"HIDDEN_LENGTH_AND_TEXT","readers":["grp:deny"],"location":{"kind":"heading","label":"restricted"}});
    let inserted = json!({"text":"HIDDEN_LENGTH_AND_TEXT","readers":["grp:deny"],"location":{"kind":"heading","label":"another-hidden-section"}});
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    for chunk in fyrr_writer::chunk::chunks(visible["text"].as_str().unwrap()) {
        root.embedding.insert_both(chunk, vector.clone());
    }
    let mut previous: Option<(Value, Value)> = None;
    for (round, sections) in [
        vec![hidden.clone(), visible.clone()],
        vec![inserted.clone(), hidden.clone(), visible.clone()],
        vec![inserted, visible.clone()],
        vec![visible],
    ]
    .into_iter()
    .enumerate()
    {
        let document = input["documents"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|d| d["id"] == "restricted")
            .unwrap();
        document["sections"] = json!(sections);
        document["revision"] = json!(format!("hidden-edit-{round}"));
        input["cursor"] = json!(format!("hidden-edit-{round}"));
        std::fs::write(
            root.join("documents.json"),
            serde_json::to_vec(&input).unwrap(),
        )
        .unwrap();
        sync_fixture(&config);
        success(&config, &["writer", "commit"]);
        let mut client = Mcp::open(&config);
        let page = body(client.call(
            "fetch",
            json!({"source_id":"allowed","root_id":"restricted"}),
        ));
        let record = page["documents"][0]["records"][0].clone();
        let id = record["record_id"].as_str().unwrap();
        let (section, chunk) = id.rsplit_once(':').unwrap();
        assert!(
            section.starts_with("sha256:"),
            "section identity must be opaque: {id}"
        );
        assert_eq!(section.len(), 71);
        assert_eq!(chunk, "0");
        let cursor = page["next_cursor"].as_str().unwrap();
        let decoded: Value = serde_json::from_str(cursor.rsplit_once('.').unwrap().0).unwrap();
        assert!(
            decoded.get("position").is_none(),
            "decoded cursor exposes a section position: {decoded}"
        );
        assert_eq!(
            decoded["after"][2],
            page["documents"][0]["records"]
                .as_array()
                .unwrap()
                .last()
                .unwrap()["record_id"]
        );
        assert!(!cursor.contains("HIDDEN") && !cursor.contains("hidden-section"));
        let window = previous.as_ref().map(|(_, w)| w.clone()).unwrap_or_else(|| json!({"record_id":record["record_id"],"revision":record["revision"],"start":7,"end":30}));
        let fetched = body(client.call(
            "fetch",
            json!({"source_id":"allowed","root_id":"restricted","window":window}),
        ));
        assert_eq!(fetched["document_changed"], false);
        if let Some((before, _)) = &previous {
            assert_eq!(
                &record, before,
                "hidden insertion/removal changed visible identity, revision or text"
            );
        }
        assert_eq!(
            fetched["documents"][0]["records"][0]["revision"],
            record["revision"]
        );
        assert_eq!(
            fetched["documents"][0]["records"][0]["text"],
            record["text"]
                .as_str()
                .unwrap()
                .chars()
                .skip(7)
                .take(23)
                .collect::<String>()
        );
        assert_eq!(
            fetched["window"],
            json!({"record_id":record["record_id"],"start":7,"end":30})
        );
        previous = Some((record, window));
    }
    std::fs::remove_dir_all(root).unwrap();
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
pub(super) fn body(result: Value) -> Value {
    assert_eq!(result["isError"], false, "{result}");
    result["structuredContent"].clone()
}
fn error(result: Value, code: &str) {
    assert_eq!(result["isError"], true, "{result}");
    assert_eq!(result["structuredContent"]["code"], code);
    assert!(result["structuredContent"]["retryable"].is_boolean());
}
pub(super) fn fixture() -> (PathBuf, FixtureRoot, String) {
    let (config, root) = setup();
    let prefix = std::fs::read_to_string(&config)
        .unwrap()
        .split("[[sources]]")
        .next()
        .unwrap()
        .to_owned();
    let text = format!(
        "{prefix}\n[[sources]]\nid='allowed'\ntype='sentinel-docs'\npermissions='sync'\nintervals={{ permissions='30m' }}\n[sources.scope]\nfixture='{}'\n",
        root.join("documents.json").display()
    );
    std::fs::write(&config, &text).unwrap();
    let mut documents = Vec::new();
    for row in 0..8 {
        documents.push(json!({"id":format!("r{row}"),"text":format!("Résumé 👩🏽‍💻 exactneedle fixture document {row}. Final sentence."),"readers":["grp:allow"],"title":format!("label{row}"),"author":if row%2==0 {"Ada"} else {"Lin"},"project":"public-project","updated_at":format!("2026-09-{:02}T12:00:00.512000+00:00",20+row),"uri":"test://shared"}));
    }
    documents.push(json!({"id":"restricted","body_location":{"kind":"heading","label":"visible-section"},"first_text":"HIDDEN_LENGTH_AND_TEXT","text":"Résumé 👩🏽‍💻 exactneedle authorized passage.","readers":["grp:allow"],"first_readers":["grp:deny"],"title":"restricted","author":"Ada","updated_at":null}));
    documents.push(json!({"id":"denied","text":"HIDDEN_LENGTH_AND_TEXT","readers":["grp:deny"],"title":"hidden-label","author":"hidden-author","project":"hidden-project"}));
    documents.push(json!({"id":"split","note":"split metadata","text":"αβγδ exactneedle ".repeat(10000),"readers":["grp:allow"],"title":"split","author":"Ada","updated_at":null}));
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    for document in &documents {
        for text in [
            document["text"].as_str().unwrap(),
            document
                .get("first_text")
                .and_then(Value::as_str)
                .unwrap_or(document["text"].as_str().unwrap()),
        ] {
            for chunk in fyrr_writer::chunk::chunks(text) {
                root.embedding.insert_both(chunk, vector.clone());
            }
        }
    }
    std::fs::write(
        root.join("documents.json"),
        serde_json::to_vec(&json!({"documents":documents,"cursor":"one"})).unwrap(),
    )
    .unwrap();
    success(&config, &["writer", "namespace", "create"]);
    sync_fixture(&config);
    success(&config, &["writer", "commit"]);
    (config, root, text)
}
fn sync_fixture(config: &Path) {
    let result = super::coordination::worker_command(config)
        .args(["writer", "sync", "--full"])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        format!(
            "{} {}",
            String::from_utf8_lossy(&result.stderr),
            String::from_utf8_lossy(&result.stdout)
        )
    );
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
#[allow(clippy::print_stderr)]
async fn tools_fetch_filters_windows_and_cursors_on_real_s3() {
    let (config, root, text) = fixture();
    let mut client = Mcp::open(&config);
    let counts = body(client.call("list_filters", json!({})));
    assert_eq!(
        counts["projects"]["values"],
        json!([{"value":"public-project","count":8}])
    );
    assert_eq!(
        counts["source_types"]["values"],
        json!([{"value":"test.doc","count":10}])
    );
    assert!(!counts.to_string().contains("hidden-"));
    assert!(
        counts["metadata"]["telemetry"]["filter_rows_scanned"]
            .as_u64()
            .unwrap()
            > 10
    );
    assert!(counts["metadata"]["telemetry"]["stage_us"]["filter_counts"].is_number());
    eprintln!(
        "list_filters debug real-path cost: {}",
        counts["metadata"]["telemetry"]
    );
    let listing = body(client.call(
        "search",
        json!({"order":"updated_at","limit":75,"output":"metadata"}),
    ));
    let keys: Vec<_> = listing["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["root_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        keys,
        vec![
            "r7",
            "r6",
            "r5",
            "r4",
            "r3",
            "r2",
            "r1",
            "r0",
            "restricted",
            "split"
        ]
    );
    assert_eq!(
        listing["hits"][0]["updated_at"],
        "2026-09-27T12:00:00.512000Z"
    );
    let filtered=body(client.call("search",json!({"query":"fixture query","authors":["Ada"],"updated_after":"2026-09-23T12:00:00Z","updated_before":"2026-09-27T00:00:00Z","attributes":[{"key":"test.label","values":["label4","label6"]}],"limit":75})));
    assert_eq!(filtered["hits"].as_array().unwrap().len(), 2);
    assert_eq!(
        filtered["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["root_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["r4", "r6"])
    );
    for filters in [
        json!({"order":"updated_at","projects":["hidden-project"]}),
        json!({"order":"updated_at","authors":["unknown"]}),
        json!({"order":"updated_at","source_types":["unknown"]}),
        json!({"order":"updated_at","sources":["unknown"]}),
    ] {
        assert!(
            body(client.call("search", filters))["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    error(
        client.call("search", json!({"query":"x","authors":["é".repeat(257)]})),
        "invalid_argument",
    );
    error(
        client.call("fetch", json!({"source_id":"allowed"})),
        "invalid_argument",
    );
    error(client.call("unknown", json!({})), "invalid_argument");
    assert!(
        body(client.call("fetch", json!({"source_id":"allowed","root_id":"denied"})))["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let hit=&body(client.call("search",json!({"query":"exactneedle","sources":["allowed"],"projects":["public-project"],"exclude":[{"source_id":"allowed","root_id":"split"}]})))["hits"][0];
    let window = json!({"record_id":hit["record_id"],"revision":hit["revision"],"start":hit["excerpt"]["start"],"end":hit["excerpt"]["end"]});
    let fetched = body(client.call(
        "fetch",
        json!({"source_id":hit["source_id"],"root_id":hit["root_id"],"window":window}),
    ));
    assert_eq!(
        fetched["documents"][0]["records"][0]["text"],
        hit["excerpt"]["text"]
    );
    assert_eq!(fetched["documents"][0]["updated_at"], hit["updated_at"]);
    let restricted = body(client.call(
        "fetch",
        json!({"source_id":"allowed","root_id":"restricted"}),
    ));
    assert_eq!(
        restricted["documents"][0]["records"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        restricted["documents"][0]["records"][0]["record_id"],
        format!(
            "{}:0",
            digest(
                serde_json::json!(["restricted", {"kind":"heading","label":"visible-section"}])
                    .to_string()
                    .as_bytes()
            )
        )
    );
    let hidden=body(client.call("fetch",json!({"source_id":"allowed","root_id":"restricted","window":{"record_id":"restricted:0","revision":"unknown","start":400000,"end":400001}})));
    assert!(hidden["documents"].as_array().unwrap().is_empty());
    assert_eq!(hidden["document_changed"], false);
    assert!(hidden["window"].is_null());
    let page = body(client.call("fetch", json!({"uri":"  test://shared  "})));
    assert_eq!(page["documents"].as_array().unwrap().len(), 5);
    let cursor = page["next_cursor"].as_str().unwrap().to_owned();
    let mut tampered = cursor.clone();
    tampered.replace_range(0..1, if &cursor[0..1] == "0" { "1" } else { "0" });
    error(
        client.call("fetch", json!({"cursor":tampered})),
        "stale_cursor",
    );
    // Identity-only update retains the content pin and accepts the next authorized page.
    let renewed = root.join("renewed.toml");
    std::fs::write(
        &renewed,
        text.replace(
            "stdio_readers = [\"grp:allow\"]",
            "stdio_readers = [\"grp:allow\",\"grp:extra\"]",
        ),
    )
    .unwrap();
    success(&renewed, &["writer", "commit"]);
    let second = body(client.call("fetch", json!({"cursor":cursor})));
    assert_eq!(second["documents"].as_array().unwrap().len(), 3);
    let revoked_page = root.join("revoked-page.toml");
    std::fs::write(
        &revoked_page,
        text.replace(
            "stdio_readers = [\"grp:allow\"]",
            "stdio_readers = [\"grp:deny\"]",
        ),
    )
    .unwrap();
    success(&revoked_page, &["writer", "commit"]);
    assert!(
        body(client.call("fetch", json!({"cursor":cursor})))["documents"]
            .as_array()
            .unwrap()
            .is_empty(),
        "each cursor page uses the new grants"
    );
    success(&renewed, &["writer", "commit"]);
    let split = body(client.call("fetch", json!({"source_id":"allowed","root_id":"split"})));
    let bytes: usize = split["documents"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|d| d["records"].as_array().unwrap())
        .map(|r| r["text"].as_str().unwrap().len())
        .sum();
    assert!(bytes <= 65536);
    assert!(split["next_cursor"].is_string());
    assert!(split["metadata"]["omitted_bytes"].as_u64().unwrap() > 0);
    let next = body(client.call("fetch", json!({"cursor":split["next_cursor"]})));
    assert!(next["documents"][0]["records"].as_array().unwrap().len() <= 32);
    let mut joined = String::new();
    for page in [&split, &next] {
        for doc in page["documents"].as_array().unwrap() {
            for record in doc["records"].as_array().unwrap() {
                joined.push_str(record["text"].as_str().unwrap());
            }
        }
    }
    let mut next_cursor = next["next_cursor"].clone();
    let mut pages = 2;
    while next_cursor.is_string() {
        let page = body(client.call("fetch", json!({"cursor":next_cursor})));
        pages += 1;
        assert!(pages <= 16);
        for doc in page["documents"].as_array().unwrap() {
            for record in doc["records"].as_array().unwrap() {
                joined.push_str(record["text"].as_str().unwrap());
            }
        }
        next_cursor = page["next_cursor"].clone();
    }
    assert_eq!(joined, "αβγδ exactneedle ".repeat(20000));
    let bound = root.join("bounded.toml");
    std::fs::write(
        &bound,
        text.replace("nprobes = 1", "nprobes = 1\nfilter_count_max_rows = 1"),
    )
    .unwrap();
    error(Mcp::open(&bound).call("list_filters", json!({})), "busy");
    let changed=body(client.call("fetch",json!({"source_id":hit["source_id"],"root_id":hit["root_id"],"window":{"record_id":hit["record_id"],"revision":"different","start":400000,"end":400001}})));
    assert_eq!(changed["document_changed"], true);
    assert!(changed["documents"].as_array().unwrap().is_empty());
    // Expiry is fixed across pages, and authentication rejects a forged payload.
    let payload = cursor.rsplit_once('.').unwrap().0;
    let mut expired: Value = serde_json::from_str(payload).unwrap();
    expired["expires"] = json!(0);
    let payload = expired.to_string();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let signature = fyrr_reader::storage::hash_principal(
        &loaded.config,
        "cursor",
        &format!("fyrr-fetch-1:{payload}"),
    )
    .unwrap();
    error(
        client.call(
            "fetch",
            json!({"cursor":format!("{payload}.{}",&signature[7..])}),
        ),
        "stale_cursor",
    );
    // Republish changed text, restart, then compare the old record revision and cursor.
    let mut documents: Value =
        serde_json::from_slice(&std::fs::read(root.join("documents.json")).unwrap()).unwrap();
    let changed_root = hit["root_id"].as_str().unwrap();
    let changed_text = "Changed Résumé exactneedle passage.";
    let document = documents["documents"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|d| d["id"] == changed_root)
        .unwrap();
    document["text"] = json!(changed_text);
    document["revision"] = json!("r2");
    std::fs::write(
        root.join("documents.json"),
        serde_json::to_vec(&documents).unwrap(),
    )
    .unwrap();
    root.embedding
        .insert_both(changed_text, root.embedding.vector("fixture query"));
    let hidden_text = "HIDDEN_LENGTH_AND_TEXT ".repeat(1800);
    let restricted_doc = documents["documents"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|d| d["id"] == "restricted")
        .unwrap();
    restricted_doc["first_text"] = json!(hidden_text);
    restricted_doc["revision"] = json!("hidden-r2");
    std::fs::write(
        root.join("documents.json"),
        serde_json::to_vec(&documents).unwrap(),
    )
    .unwrap();
    let vector = root.embedding.vector("fixture query");
    for chunk in fyrr_writer::chunk::chunks(&hidden_text) {
        root.embedding.insert_both(chunk, vector.clone());
    }
    sync_fixture(&config);
    success(&config, &["writer", "commit"]);
    let mut fresh = Mcp::open(&config);
    error(
        fresh.call("fetch", json!({"cursor":cursor})),
        "stale_cursor",
    );
    let changed = body(fresh.call(
        "fetch",
        json!({"source_id":hit["source_id"],"root_id":hit["root_id"],"window":window}),
    ));
    assert_eq!(changed["document_changed"], true);
    assert!(changed["documents"].as_array().unwrap().is_empty());
    let visible_record = &restricted["documents"][0]["records"][0];
    let visible_window=body(fresh.call("fetch",json!({"source_id":"allowed","root_id":"restricted","window":{"record_id":visible_record["record_id"],"revision":visible_record["revision"],"start":0,"end":visible_record["text"].as_str().unwrap().chars().count()}})));
    assert_eq!(visible_window["document_changed"], false);
    assert_eq!(
        visible_window["documents"][0]["records"][0], *visible_record,
        "hidden section length/change leaves the visible identity, revision and window unchanged"
    );
    // Another admitted subject cannot reuse the authenticated cursor.
    let fresh_split = body(fresh.call("fetch", json!({"source_id":"allowed","root_id":"split"})));
    let other = root.join("other.toml");
    std::fs::write(&other, text.replace("fixture-subject", "other-subject")).unwrap();
    let identity_only = success(&other, &["writer", "commit"]);
    assert_eq!(
        identity_only["lance_version"],
        fresh_split["metadata"]["content_version"]
    );
    error(
        Mcp::open(&other).call("fetch", json!({"cursor":fresh_split["next_cursor"]})),
        "stale_cursor",
    );
    drop(fresh);
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_concurrency_deadlines_cancellation_and_cost_on_real_s3() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    let (config, root, text) = fixture();
    let heads = Arc::new(super::corrections::HeadReads::default());
    let stop = Arc::new(AtomicBool::new(false));
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, thread) = super::corrections::proxy(
        endpoint.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
        config.clone(),
        stop.clone(),
        None,
        Some(heads.clone()),
    );
    let proxied = root.join("proxy.toml");
    // This proxy counts request admission and rechecks, not the independent
    // background warm-up's final head fence (covered by maintenance tests).
    std::fs::write(
        &proxied,
        text.replace(&endpoint, &proxy)
            .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 0")
            .replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
    )
    .unwrap();
    let mut client = Mcp::open(&proxied);
    body(client.call("list_filters", json!({})));
    body(client.call("list_filters", json!({})));
    // Hold the idle checker for the entire proof so it cannot consume a request gate.
    let background = heads.hold_next(1);
    background[0].entered();
    let base = heads.started();
    // Invalid requests do not read storage or terminate the SDK session.
    client.send(json!({"jsonrpc":"1.0","id":99,"method":"tools/call","params":{"name":"list_filters","arguments":{}}}));
    assert_eq!(client.read()["error"]["code"], -32600);
    error(
        client.call("search", json!({"query":"x","limit":76})),
        "invalid_argument",
    );
    assert_eq!(heads.started(), base);
    let admitted = heads.hold_next(2);
    for id in [101, 102] {
        client.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"list_filters","arguments":{}}}));
    }
    for gate in &admitted {
        gate.entered();
    }
    for (id, args) in [
        (103, json!({})),
        (104, json!({"deadline_ms":10})),
        (105, json!({})),
    ] {
        client.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"list_filters","arguments":args}}));
    }
    client.send(
        json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":103}}),
    );
    client.send(json!({"jsonrpc":"2.0","id":106,"method":"ping"}));
    let mut replies = BTreeMap::new();
    for _ in 0..4 {
        let value = client.read();
        replies.insert(value["id"].as_u64().unwrap(), value["result"].clone());
    }
    assert_eq!(
        replies.keys().copied().collect::<Vec<_>>(),
        [103, 104, 105, 106]
    );
    error(replies.remove(&103).unwrap(), "cancelled");
    error(replies.remove(&104).unwrap(), "deadline_exceeded");
    error(replies.remove(&105).unwrap(), "busy");
    assert_eq!(replies[&106], json!({}));
    assert_eq!(
        heads.started(),
        base + 2,
        "queued, expired and cancelled requests do no head work"
    );
    drop(admitted);
    for _ in 0..2 {
        let response = client.read();
        assert!([101, 102].contains(&response["id"].as_u64().unwrap()));
        let result = body(response["result"].clone());
        assert_eq!(
            result["metadata"]["telemetry"]["s3_requests"], 2,
            "each concurrent call owns exactly its two head GETs"
        );
    }
    assert_eq!(heads.started(), base + 4);
    // An executing timeout and cancellation still perform their final authorization read.
    let timed_out = heads.hold_next(1);
    let started = std::time::Instant::now();
    error(
        client.call("list_filters", json!({"deadline_ms":1000})),
        "deadline_exceeded",
    );
    assert!(started.elapsed() >= std::time::Duration::from_secs(1));
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    timed_out[0].entered();
    drop(timed_out);
    let base = heads.started();
    let cancelled = heads.hold_next(1);
    client.send(json!({"jsonrpc":"2.0","id":107,"method":"tools/call","params":{"name":"fetch","arguments":{"source_id":"allowed","root_id":"r0"}}}));
    cancelled[0].entered();
    client.send(
        json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":107}}),
    );
    error(client.read()["result"].clone(), "cancelled");
    assert_eq!(heads.started(), base + 2);
    drop(cancelled);
    assert!(
        !body(client.call("fetch", json!({"source_id":"allowed","root_id":"r0"})))["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    drop(client);
    drop(background);
    stop.store(true, Ordering::Relaxed);
    drop(thread);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_filter_none_too_many_and_readable_counts_on_real_s3() {
    let (_config, root, text) = fixture();
    let uri = format!("test://{}", "\"".repeat(4089));
    assert_eq!(uri.len(), 4096);
    let documents:Vec<Value>=(0..65).map(|i|json!({"id":format!("d{i}"),"text":"fixture document 0","readers":["grp:allow"],"title":"label","author":format!("author{i}"),"updated_at":null,"uri":uri})).collect();
    std::fs::write(
        root.join("documents.json"),
        serde_json::to_vec(&json!({"documents":documents,"cursor":"two"})).unwrap(),
    )
    .unwrap();
    let fresh = root.join("cardinality.toml");
    std::fs::write(&fresh, text.replace("dense-fixture", "cardinality-fixture")).unwrap();
    success(&fresh, &["writer", "namespace", "create"]);
    sync_fixture(&fresh);
    success(&fresh, &["writer", "commit"]);
    let result = body(Mcp::open(&fresh).call("list_filters", json!({})));
    assert_eq!(result["authors"], json!({"status":"too_many","values":[]}));
    assert_eq!(result["projects"], json!({"status":"values","values":[]}));
    assert_eq!(
        result["source_types"]["values"],
        json!([{"value":"test.doc","count":65}])
    );
    assert_eq!(result["metadata"]["telemetry"]["filter_rows_scanned"], 130);
    let mut client = Mcp::open(&fresh);
    let page = body(client.call("fetch", json!({"uri":uri})));
    assert_eq!(page["documents"].as_array().unwrap().len(), 5);
    assert!(page["next_cursor"].as_str().unwrap().len() <= 16384);
    assert_eq!(
        body(client.call("fetch", json!({"cursor":page["next_cursor"]})))["documents"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    drop(client);

    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_every_admitted_result_and_error_rechecks_identity_on_real_s3() {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let (config, root, text) = fixture();
    let revoked = root.join("revoked.toml");
    std::fs::write(
        &revoked,
        text.replace(
            "stdio_readers = [\"grp:allow\"]",
            "stdio_readers = [\"grp:deny\"]",
        ),
    )
    .unwrap();
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    for (name, args) in [
        ("fetch", json!({"source_id":"allowed","root_id":"r0"})),
        ("fetch", json!({"cursor":"invalid"})),
        ("list_filters", json!({})),
        ("search", json!({"order":"updated_at"})),
    ] {
        success(&config, &["writer", "commit"]);
        let hold_background = Arc::new(AtomicBool::new(false));
        let (gate, background_gate) = super::corrections::RequestGate::new(|_, _, _| true);
        let background_thread = Arc::new(Mutex::new(None));
        let stop = Arc::new(AtomicBool::new(false));
        let armed = Arc::new(AtomicBool::new(false));
        let heads = Arc::new(AtomicUsize::new(0));
        let events = Arc::new(Mutex::new(Vec::new()));
        let hook: super::corrections::BoundaryHook = Arc::new({
            let (hold_background, background_thread) =
                (hold_background.clone(), background_thread.clone());
            let (armed, heads, events, revoked) = (
                armed.clone(),
                heads.clone(),
                events.clone(),
                revoked.clone(),
            );
            move |request, bytes, after| {
                if !request.starts_with("GET ") || !request.contains("/head.json ") {
                    return false;
                }
                let thread = std::thread::current().id();
                if !after && hold_background.swap(false, Ordering::SeqCst) {
                    *background_thread.lock().unwrap() = Some(thread);
                }
                if *background_thread.lock().unwrap() == Some(thread) {
                    if after {
                        background_gate(request, bytes, after);
                    }
                } else if armed.load(Ordering::SeqCst) {
                    if !after {
                        let phase = if heads.fetch_add(1, Ordering::SeqCst) == 0 {
                            "admission_start"
                        } else {
                            "output_start"
                        };
                        events.lock().unwrap().push((phase, None));
                    } else {
                        let split = bytes.windows(4).position(|v| v == b"\r\n\r\n").unwrap();
                        let head: Value = serde_json::from_slice(&bytes[split + 4..]).unwrap();
                        let admission = heads.load(Ordering::SeqCst) == 1;
                        let phase = if admission {
                            "admission_read"
                        } else {
                            "output_read"
                        };
                        events
                            .lock()
                            .unwrap()
                            .push((phase, Some(head["identity"].clone())));
                        if admission {
                            // Preserve the old admission response, but publish the
                            // real identity change before any output recheck starts.
                            success(&revoked, &["writer", "commit"]);
                            events.lock().unwrap().push(("revocation_committed", None));
                        }
                    }
                }
                false
            }
        });
        let (proxy, guard) = super::corrections::proxy_at_boundary(endpoint.clone(), stop, hook);
        let gate = gate;
        let proxied = root.join("proxy.toml");
        std::fs::write(
            &proxied,
            text.replace(&endpoint, &proxy)
                .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 0")
                .replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
        )
        .unwrap();
        let mut client = Mcp::open(&proxied);
        // A global head-read countdown also counts warming/interval work.
        // Hold that checker separately so it cannot steal the request's fence.
        hold_background.store(true, Ordering::SeqCst);
        gate.entered();
        armed.store(true, Ordering::SeqCst);
        let response = client.call(name, args);
        let observed = events.lock().unwrap().clone();
        let position = |phase| {
            observed
                .iter()
                .position(|(event, _)| *event == phase)
                .unwrap()
        };
        assert_eq!(
            heads.load(Ordering::SeqCst),
            2,
            "admission and output must each recheck"
        );
        for pair in [
            ("admission_start", "admission_read"),
            ("admission_read", "revocation_committed"),
            ("revocation_committed", "output_start"),
            ("output_start", "output_read"),
        ] {
            assert!(
                position(pair.0) < position(pair.1),
                "identity phase order for {name}"
            );
        }
        let old = &observed[position("admission_read")].1;
        let new = &observed[position("output_read")].1;
        assert!(old.as_ref().is_some_and(Value::is_object));
        assert_ne!(
            old, new,
            "output must observe the published identity change"
        );
        #[allow(clippy::print_stderr)]
        {
            eprintln!(
                "identity_response_order tool={name} phases={:?} changed=true error={}",
                observed.iter().map(|(phase, _)| *phase).collect::<Vec<_>>(),
                response["isError"]
            );
        }
        error(response, "authorization_unavailable");
        drop(client);
        drop(gate);
        drop(guard);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_retrieval_failures_recheck_identity_on_real_s3() {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let (config, root, text) = fixture();
    let revoked = root.join("revoked.toml");
    std::fs::write(
        &revoked,
        text.replace(
            "stdio_readers = [\"grp:allow\"]",
            "stdio_readers = [\"grp:deny\"]",
        ),
    )
    .unwrap();
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    for (name, args) in [
        ("fetch", json!({"source_id":"allowed","root_id":"r0"})),
        ("list_filters", json!({})),
        ("search", json!({"order":"updated_at"})),
    ] {
        success(&config, &["writer", "commit"]);
        let hold_background = Arc::new(AtomicBool::new(false));
        let (gate, background_gate) = super::corrections::RequestGate::new(|_, _, _| true);
        let stop = Arc::new(AtomicBool::new(false));
        let background_thread = Arc::new(Mutex::new(None));
        let armed = Arc::new(AtomicBool::new(false));
        let failures = Arc::new(AtomicUsize::new(0));
        let fault = Arc::new(Mutex::new(()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let hook: super::corrections::BoundaryHook = Arc::new({
            let (hold_background, background_thread) =
                (hold_background.clone(), background_thread.clone());
            let (armed, failures, fault, events, revoked) = (
                armed.clone(),
                failures.clone(),
                fault.clone(),
                events.clone(),
                revoked.clone(),
            );
            move |request, bytes, after| {
                if request.starts_with("GET ") && request.contains("/head.json ") {
                    let thread = std::thread::current().id();
                    if !after && hold_background.swap(false, Ordering::SeqCst) {
                        assert!(!armed.load(Ordering::SeqCst));
                        *background_thread.lock().unwrap() = Some(thread);
                    }
                    if *background_thread.lock().unwrap() == Some(thread) {
                        if after {
                            events.lock().unwrap().push(("background_held", None));
                            background_gate(request, bytes, after);
                            events.lock().unwrap().push(("background_released", None));
                        }
                    } else if armed.load(Ordering::SeqCst) {
                        let final_refresh = events
                            .lock()
                            .unwrap()
                            .iter()
                            .any(|(name, _)| *name == "revocation_committed");
                        let event = match (final_refresh, after) {
                            (false, false) => "admission_start",
                            (false, true) => "admission_complete",
                            (true, false) => "final_refresh_start",
                            (true, true) => "final_refresh_complete",
                        };
                        let identity = after.then(|| {
                            let split = bytes.windows(4).position(|v| v == b"\r\n\r\n").unwrap();
                            let head: Value = serde_json::from_slice(&bytes[split + 4..]).unwrap();
                            head["identity"].clone()
                        });
                        events.lock().unwrap().push((event, identity));
                    }
                } else if !after
                    && armed.load(Ordering::SeqCst)
                    && (request.starts_with("GET ") || request.starts_with("HEAD "))
                    && request.contains("/dataset.lance/")
                {
                    // Serialize native failures so every 403 follows the real
                    // identity commit, including concurrent Lance reads.
                    let _fault = fault.lock().unwrap();
                    assert!(
                        events
                            .lock()
                            .unwrap()
                            .iter()
                            .any(|(name, _)| *name == "admission_complete"),
                        "retrieval must be admitted"
                    );
                    if failures.load(Ordering::SeqCst) == 0 {
                        events.lock().unwrap().push(("native_fault", None));
                        success(&revoked, &["writer", "commit"]);
                        events.lock().unwrap().push(("revocation_committed", None));
                    }
                    failures.fetch_add(1, Ordering::SeqCst);
                    events
                        .lock()
                        .unwrap()
                        .push(("native_failure_returned", None));
                    return true;
                }
                false
            }
        });
        let (proxy, guard) = super::corrections::proxy_at_boundary(endpoint.clone(), stop, hook);
        let gate = gate;
        let proxied = root.join("proxy-fail.toml");
        std::fs::write(
            &proxied,
            text.replace(&endpoint, &proxy)
                .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 0")
                .replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
        )
        .unwrap();
        let mut client = Mcp::open(&proxied);
        // Capture a genuine idle checker response before issuing a request.
        // It stays held through admission, native failure and final refresh;
        // its head read therefore cannot impersonate request authorization.
        hold_background.store(true, Ordering::SeqCst);
        gate.entered();
        armed.store(true, Ordering::SeqCst);
        let response = client.call(name, args);
        error(response.clone(), "authorization_unavailable");
        assert!(!response.to_string().contains("storage_unavailable"));
        events
            .lock()
            .unwrap()
            .push(("authorization_response", None));
        assert!(
            failures.load(Ordering::SeqCst) > 0,
            "native read really failed"
        );
        let observed = events.lock().unwrap().clone();
        let position = |event| {
            observed
                .iter()
                .position(|(name, _)| *name == event)
                .unwrap()
        };
        for pair in [
            ("background_held", "admission_start"),
            ("admission_start", "admission_complete"),
            ("admission_complete", "native_fault"),
            ("native_fault", "revocation_committed"),
            ("revocation_committed", "native_failure_returned"),
            ("native_failure_returned", "final_refresh_start"),
            ("final_refresh_start", "final_refresh_complete"),
            ("final_refresh_complete", "authorization_response"),
        ] {
            assert!(position(pair.0) < position(pair.1), "{observed:?}");
        }
        for event in [
            "admission_start",
            "admission_complete",
            "final_refresh_start",
            "final_refresh_complete",
        ] {
            assert_eq!(
                observed.iter().filter(|(name, _)| *name == event).count(),
                1,
                "exact request phase while background is held: {observed:?}"
            );
        }
        let old = &observed[position("admission_complete")].1;
        let new = &observed[position("final_refresh_complete")].1;
        assert!(old.as_ref().is_some_and(|v| v.is_object()));
        assert!(new.as_ref().is_some_and(|v| v.is_object()));
        assert_ne!(old, new, "final refresh must read the committed identity");
        assert!(
            !observed
                .iter()
                .any(|(name, _)| *name == "background_released")
        );
        drop(client);
        drop(gate);
        drop(guard);
        let observed = events.lock().unwrap();
        assert_eq!(observed.last().unwrap().0, "background_released");
        #[allow(clippy::print_stderr)]
        {
            eprintln!(
                "retrieval_identity_order tool={name} phases={:?} native_failures={}",
                observed.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
                failures.load(Ordering::SeqCst)
            );
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_default_deadline_on_real_s3() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::{Duration, Instant};
    let (config, root, text) = fixture();
    let heads = Arc::new(super::corrections::HeadReads::default());
    let stop = Arc::new(AtomicBool::new(false));
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, thread) = super::corrections::proxy(
        endpoint.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
        config.clone(),
        stop.clone(),
        None,
        Some(heads.clone()),
    );
    let proxied = root.join("default-deadline.toml");
    // Count only the request's admission and final recheck, independently of
    // the supported background mirror-warming path.
    std::fs::write(
        &proxied,
        text.replace(&endpoint, &proxy)
            .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 0")
            .replace("[reader]", "[reader]\nversion_check_interval = '1s'"),
    )
    .unwrap();
    let mut client = Mcp::open(&proxied);
    assert_eq!(heads.started(), 1, "one opening head read");
    let background = heads.hold_next(1);
    background[0].entered();
    let base = heads.started();
    let admission = heads.hold_next(1);
    let start = Instant::now();
    error(client.call("list_filters", json!({})), "deadline_exceeded");
    assert!(start.elapsed() >= Duration::from_secs(10));
    assert!(start.elapsed() < Duration::from_secs(11));
    admission[0].entered();
    drop(admission);
    assert_eq!(
        heads.started(),
        base + 2,
        "the interrupted request's output head is checked"
    );
    body(client.call("list_filters", json!({}))); // a timeout leaves the session usable
    drop(client);
    drop(background);
    stop.store(true, Ordering::Relaxed);
    drop(thread);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_agent_multiquery_text_and_fetch_list_on_real_s3() {
    let (config, root, text) = fixture();
    let mut client = Mcp::open(&config);
    let queries = ["fixture", "document 7", "authorized passage"];
    let individual: Vec<_> = queries
        .iter()
        .map(|query| {
            body(client.call(
                "search",
                json!({"query":query,"limit":4,"text_hits":1,"excerpt_chars":20}),
            ))
        })
        .collect();
    let mut expected = Vec::new();
    let mut seen = BTreeSet::new();
    for rank in 0..4 {
        for (query_index, result) in individual.iter().enumerate() {
            if let Some(hit) = result["hits"].get(rank) {
                let key = (hit["source_id"].clone(), hit["root_id"].clone());
                // JSON values are not ordered, use their canonical serialized keys.
                if seen.insert(key.0.to_string() + &key.1.to_string()) {
                    let mut hit = hit.clone();
                    hit["query_index"] = json!(query_index);
                    expected.push(hit);
                }
            }
        }
    }
    let batch = body(client.call(
        "search",
        json!({"queries":queries,"limit":4,"text_hits":1,"excerpt_chars":20}),
    ));
    assert_eq!(batch["hits"], json!(expected));
    assert_eq!(batch["metadata"]["degraded"], json!(["dense"]));
    let recent = body(client.call(
        "search",
        json!({"query":"fixture","order":"updated_at","limit":4,"text_hits":1}),
    ));
    for (rank, hit) in recent["hits"].as_array().unwrap().iter().enumerate() {
        assert_eq!(hit["excerpt"].is_object(), rank == 0);
    }
    let hit = &individual[0]["hits"][0];
    let excerpt = &hit["excerpt"];
    assert!(excerpt["text"].as_str().unwrap().chars().count() <= 20);
    let list = body(client.call("fetch",json!({"documents":[
        {"source_id":"allowed","root_id":"r7"},
        {"source_id":hit["source_id"],"root_id":hit["root_id"],"window":{
            "record_id":hit["record_id"],"revision":hit["revision"],"start":excerpt["start"],"end":excerpt["end"]}},
        {"source_id":"allowed","root_id":"r6"}
    ]})));
    assert_eq!(list["documents"][0]["root_id"], "r7");
    assert_eq!(list["documents"][1]["records"][0]["text"], excerpt["text"]);
    assert_eq!(list["documents"][1]["window"]["start"], excerpt["start"]);
    assert_eq!(list["documents"][2]["root_id"], "r6");
    assert_eq!(list["documents"][0]["document_changed"], false);
    let changed = body(client.call("fetch",json!({"documents":[
        {"source_id":"allowed","root_id":"r0","window":{"record_id":"r0:0","revision":"different","start":400000,"end":400001}},
        {"source_id":"allowed","root_id":"denied"},
        {"source_id":"allowed","root_id":"missing"},
        {"source_id":"allowed","root_id":"r1"}
    ]})));
    assert_eq!(changed["documents"].as_array().unwrap().len(), 2);
    assert_eq!(changed["documents"][0]["document_changed"], true);
    assert_eq!(changed["documents"][0]["records"], json!([]));
    assert_eq!(changed["documents"][1]["root_id"], "r1");
    assert!(!changed.to_string().contains("HIDDEN_LENGTH_AND_TEXT"));
    let mut denied = body(client.call(
        "fetch",
        json!({"documents":[
            {"source_id":"allowed","root_id":"denied"}
        ]}),
    ));
    let mut unknown = body(client.call(
        "fetch",
        json!({"documents":[
            {"source_id":"allowed","root_id":"missing"}
        ]}),
    ));
    denied["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("telemetry");
    unknown["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("telemetry");
    assert_eq!(
        denied, unknown,
        "same fields and absence for denied and unknown documents"
    );
    let stale = root.join("stale-batch.toml");
    std::fs::write(
        &stale,
        text.replace(
            "name = \"dense-fixture\"",
            "name = \"dense-fixture\"\nretrieval = { dense_weight = 0.5 }",
        ),
    )
    .unwrap();
    let mut rejected = Command::new(env!("CARGO_BIN_EXE_fyrr"))
        .arg("--config")
        .arg(&stale)
        .args(["reader", "serve"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let requests = [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"stale-batch","version":"1"}}}),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search","arguments":{"queries":queries,"limit":4}}}),
    ].iter().map(|request| format!("{request}\n")).collect::<String>();
    let mut input = rejected.stdin.take().unwrap();
    // Startup may close stdin before the queued batch is consumed.
    let _ = input.write_all(requests.as_bytes());
    drop(input);
    let refusal = rejected.wait_with_output().unwrap();
    assert!(!refusal.status.success());
    assert!(String::from_utf8_lossy(&refusal.stderr).contains("incompatible_index"));
    assert!(
        refusal.stdout.is_empty(),
        "stale policy cannot serve a batched request"
    );
    for args in [
        json!({"query":"x","queries":["a","b"]}),
        json!({"queries":["a","b"],"limit":21}),
        json!({"query":"x","output":"metadata","text_hits":1}),
    ] {
        error(client.call("search", args), "invalid_argument");
    }
    error(client.call("fetch",json!({"documents":[{"source_id":"allowed","root_id":"r1"},{"source_id":"allowed","root_id":"r1"}]})),"invalid_argument");
    let page = body(client.call(
        "fetch",
        json!({"documents":[
            {"source_id":"allowed","root_id":"split"},{"source_id":"allowed","root_id":"r1"}
        ]}),
    ));
    let cursor = page["next_cursor"].as_str().unwrap().to_owned();
    let initial: Value = serde_json::from_str(cursor.rsplit_once('.').unwrap().0).unwrap();
    let mut pages = vec![page];
    let mut next = json!(cursor);
    while next.is_string() {
        let page = body(client.call("fetch", json!({"cursor":next})));
        assert!(page["documents"].as_array().unwrap().len() <= 5);
        assert!(
            page["documents"]
                .as_array()
                .unwrap()
                .iter()
                .map(|d| d["records"].as_array().unwrap().len())
                .sum::<usize>()
                <= 32
        );
        assert!(
            page["documents"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|d| d["records"].as_array().unwrap())
                .map(|r| r["text"].as_str().unwrap().len())
                .sum::<usize>()
                <= 65536
        );
        next = page["next_cursor"].clone();
        if let Some(cursor) = next.as_str() {
            let decoded: Value = serde_json::from_str(cursor.rsplit_once('.').unwrap().0).unwrap();
            for key in ["namespace", "subject", "content_version", "expires"] {
                assert_eq!(decoded[key], initial[key]);
            }
        }
        pages.push(page);
    }
    let whole = body(client.call("fetch", json!({"source_id":"allowed","root_id":"r1"})));
    assert_eq!(
        pages.last().unwrap()["documents"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["records"],
        whole["documents"][0]["records"]
    );
    let restored_split = pages
        .iter()
        .flat_map(|p| p["documents"].as_array().unwrap())
        .filter(|d| d["root_id"] == "split")
        .flat_map(|d| d["records"].as_array().unwrap())
        .map(|r| r["text"].as_str().unwrap())
        .collect::<String>();
    // Sentinel publishes a heading and a body section, both readable here.
    let expected_split = "αβγδ exactneedle ".repeat(20000);
    assert_eq!(restored_split.len(), expected_split.len());
    assert_eq!(
        digest(restored_split.as_bytes()),
        digest(expected_split.as_bytes())
    );
    let window_list = body(client.call(
        "fetch",
        json!({"documents":[
            {"source_id":"allowed","root_id":"split"},
            {"source_id":hit["source_id"],"root_id":hit["root_id"],"window":{
                "record_id":hit["record_id"],"revision":hit["revision"],
                "start":excerpt["start"],"end":excerpt["end"]}}
        ]}),
    ));
    assert!(window_list["next_cursor"].is_string());
    let mut next = window_list["next_cursor"].clone();
    let mut found_window = false;
    while next.is_string() {
        let continued = body(client.call("fetch", json!({"cursor":next})));
        for document in continued["documents"].as_array().unwrap() {
            if document["root_id"] == hit["root_id"] {
                assert_eq!(document["records"][0]["text"], excerpt["text"]);
                assert_eq!(document["window"]["start"], excerpt["start"]);
                assert_eq!(document["window"]["end"], excerpt["end"]);
                found_window = true;
            }
        }
        next = continued["next_cursor"].clone();
    }
    assert!(found_window, "continuation retains the per-document window");
    let other = root.join("other-list-subject.toml");
    std::fs::write(&other, text.replace("fixture-subject", "other-subject")).unwrap();
    let identity_only = success(&other, &["writer", "commit"]);
    assert_eq!(
        identity_only["lance_version"],
        pages[0]["metadata"]["content_version"]
    );
    error(
        Mcp::open(&other).call("fetch", json!({"cursor":cursor})),
        "stale_cursor",
    );
    success(&config, &["writer", "commit"]);
    // Identity-only publications leave the content version and cursor valid,
    // but every remaining document is tested against the new grants.
    let revoked = root.join("list-revoked.toml");
    std::fs::write(
        &revoked,
        text.replace(
            "stdio_readers = [\"grp:allow\"]",
            "stdio_readers = [\"grp:deny\"]",
        ),
    )
    .unwrap();
    success(&revoked, &["writer", "commit"]);
    assert_eq!(
        body(client.call("fetch", json!({"cursor":cursor})))["documents"],
        json!([])
    );
    success(&config, &["writer", "commit"]);
    let mut expired = initial;
    expired["expires"] = json!(0);
    let payload = expired.to_string();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let signature = fyrr_reader::storage::hash_principal(
        &loaded.config,
        "cursor",
        &format!("fyrr-fetch-1:{payload}"),
    )
    .unwrap();
    error(
        client.call(
            "fetch",
            json!({"cursor":format!("{payload}.{}",&signature[7..])}),
        ),
        "stale_cursor",
    );
    let mut tampered = cursor;
    tampered.replace_range(0..1, "!");
    error(
        client.call("fetch", json!({"cursor":tampered})),
        "stale_cursor",
    );
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_multiquery_configured_document_caps_on_real_s3() {
    let (config, root, text) = fixture();
    let mut input: Value =
        serde_json::from_slice(&std::fs::read(root.join("documents.json")).unwrap()).unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    let documents = input["documents"].as_array_mut().unwrap();
    documents.clear();
    let queries: Vec<_> = (0..16).map(|group| format!("group{group}")).collect();
    for (group, query) in queries.iter().enumerate() {
        for rank in 0..20 {
            let id = format!("g{group:02}r{rank:02}");
            let text = format!("{query} document {id}. Final sentence.");
            root.embedding.insert_both(&text, vector.clone());
            documents.push(
                json!({"id":id,"text":text,"readers":["grp:allow"],"title":id,"author":"Ada"}),
            );
        }
    }
    let all_documents = std::mem::take(documents);
    // The provider contract caps a batch at 256 items. Preserve prior pages
    // by selecting content and permissions, without a partial reconcile listing.
    for (page, documents) in all_documents.chunks(160).enumerate() {
        input["documents"] = json!(documents);
        input["cursor"] = json!(format!("groups-{page}"));
        std::fs::write(
            root.join("documents.json"),
            serde_json::to_vec(&input).unwrap(),
        )
        .unwrap();
        super::coordination::worker_sync(
            &config,
            &[
                "allowed",
                "--kind",
                "content",
                "--kind",
                "permissions",
                "--full",
            ],
        );
    }
    success(&config, &["writer", "commit"]);
    for (cap, expected) in [(75, 75), (160, 160), (320, 320)] {
        let configured = root.join(format!("cap-{cap}.toml"));
        std::fs::write(&configured,format!("{text}\n[namespace.retrieval]\ndense_weight=0.5\nlexical_candidates=200\nsource_cap=0\nmax_documents={cap}\n")).unwrap();
        success(&configured, &["writer", "commit"]);
        let mut client = Mcp::open(&configured);
        let result = body(client.call(
            "search",
            json!({"queries":queries,"text_hits":4,"excerpt_chars":12}),
        ));
        let hits = result["hits"].as_array().unwrap();
        assert_eq!(hits.len(), expected);
        let roots: BTreeSet<_> = hits
            .iter()
            .map(|hit| hit["root_id"].as_str().unwrap())
            .collect();
        assert_eq!(roots.len(), hits.len());
        for (position, hit) in hits.iter().enumerate() {
            assert_eq!(hit["query_index"], position % 16);
            assert_eq!(hit["excerpt"].is_object(), position / 16 < 4);
            assert!(
                hit["root_id"]
                    .as_str()
                    .unwrap()
                    .starts_with(&format!("g{:02}", position % 16))
            );
        }
        let single = body(client.call(
            "search",
            json!({"query":"group0","limit":75,"output":"metadata"}),
        ));
        assert_eq!(single["hits"].as_array().unwrap().len(), 20);
        assert!(
            single["hits"]
                .as_array()
                .unwrap()
                .iter()
                .all(|hit| hit.get("query_index").is_none())
        );
        drop(client);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real executable and RustFS fixture; scripts/ci"]
async fn tools_hydration_projection_reads_only_text_bearing_rows_on_real_s3() {
    let (config, root, _) = fixture();
    let binary =
        std::env::var_os("FYRR_ROUTING_TEST_BINARY").expect("reader libtest built by scripts/ci");
    let output = Command::new(&binary)
        .args([
            "search::hydration_tests::hydration_projection_reads_only_text_bearing_rows_on_real_s3",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("FYRR_TEST_HYDRATION_CONFIG", &config)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real provider, MCP and RustFS; scripts/ci"]
async fn tools_metadata_preserves_author_null_time_text_and_revision_on_real_s3() {
    use super::coordination::worker_sync;
    use super::incremental::{doc, fixture_host_at, initial, input, publish, rows};
    let setup = setup();
    for row in 8..768 {
        std::fs::remove_file(setup.1.join(format!("sources/{row}.txt"))).unwrap();
    }
    let (config, commit, root) = fixture_host_at(8, true, "metadata-state", setup);
    initial(&root);
    let mut document = doc("a", "r1", "fixture document 2");
    document["author"] = json!("Document owner");
    let mut initial: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    initial["content"] = json!([[document.clone()]]);
    input(&root, initial);
    worker_sync(
        &config,
        &[
            "incremental",
            "--kind",
            "content",
            "--kind",
            "permissions",
            "--kind",
            "members",
        ],
    );
    let first = publish(&commit);
    for (epoch, revision, author) in [(1, "r1", "Document owner"), (2, "r2", "Changed owner")] {
        if epoch == 2 {
            document["revision"] = json!(revision);
            document["expected_prior_revision"] = json!("r1");
            document["author"] = json!(author);
            input(&root, json!({"epoch":epoch,"content":[[document.clone()]]}));
            worker_sync(&config, &["incremental", "--kind", "content"]);
            assert_ne!(publish(&commit)["lance_version"], first["lance_version"]);
        }
        let result = Mcp::open(&config).call("search", json!({"query":"fixture query","authors":[author],"sources":["incremental"],"limit":1,"output":"content"}));
        let result = body(result);
        let hits = result["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["author"], author);
        assert!(hits[0]["updated_at"].is_null());
        assert_eq!(hits[0]["content"], "fixture document 2");
        let batch = rows(&config).await;
        let row = fyrr_writer::apply::strings(&batch, "root_id")
            .unwrap()
            .iter()
            .position(|id| id == Some("a"))
            .unwrap();
        assert_eq!(
            fyrr_writer::apply::strings(&batch, "source_revision")
                .unwrap()
                .value(row),
            revision
        );
        let current = publish(&commit);
        worker_sync(&config, &["incremental", "--kind", "content"]);
        assert_eq!(publish(&commit)["lance_version"], current["lance_version"]);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "real S3 locator index failure through MCP; scripts/ci"]
async fn tools_fetch_refuses_unreadable_locator_index_on_real_s3() {
    use lance::index::DatasetIndexExt;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let (config, root, text) = fixture();
    let control =
        body(Mcp::open(&config).call("fetch", json!({"source_id":"allowed","root_id":"r0"})));
    assert_eq!(control["documents"].as_array().unwrap().len(), 1);
    assert!(control.to_string().contains("fixture document 0"));
    let loaded = fyrr_core::config::load(&config).unwrap();
    let head = super::coordination::publication(&config).await.0;
    let dataset = fyrr_reader::storage::pinned(&loaded.config, &head)
        .await
        .unwrap();
    let indexes = dataset.load_indices().await.unwrap();
    let (_, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let bucket = std::env::var("FYRR_TEST_S3_BUCKET").unwrap();
    let encoded_ns = ns.as_ref().replace('(', "%28").replace(')', "%29");
    let prefixes: Vec<_> = indexes
        .iter()
        .filter(|index| index.name == "root_id_idx")
        .map(|index| {
            format!(
                "GET /{bucket}/{encoded_ns}/dataset.lance/_indices/{}/",
                index.uuid
            )
        })
        .collect();
    assert!(
        !prefixes.is_empty(),
        "fixture must have the root locator index"
    );
    let armed = Arc::new(AtomicBool::new(false));
    let enabled = armed.clone();
    let hits = Arc::new(AtomicUsize::new(0));
    let observed = hits.clone();
    let hook = Arc::new(move |request: &str, _: &[u8], after: bool| {
        if !after
            && enabled.load(Ordering::SeqCst)
            && prefixes.iter().any(|prefix| request.starts_with(prefix))
        {
            observed.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        false
    });
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, _guard) = super::corrections::proxy_at_boundary(
        endpoint.clone(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let proxied = root.join("unreadable-locator.toml");
    std::fs::write(
        &proxied,
        text.replace(&endpoint, &proxy).replace(
            &root.join("mirror").display().to_string(),
            &root.join("locator-mirror").display().to_string(),
        ),
    )
    .unwrap();
    let mut client = Mcp::open(&proxied);
    armed.store(true, Ordering::SeqCst);
    let result = client.call("fetch", json!({"source_id":"allowed","root_id":"r0"}));
    // Assert the public outcome first. A skipped lookup must fail here, not at
    // the baseline-only qualification that the intended index was actually read.
    error(result.clone(), "storage_unavailable");
    assert!(!result.to_string().contains("fixture document 0"));
    assert!(result["structuredContent"].get("documents").is_none());
    assert!(
        hits.load(Ordering::SeqCst) > 0,
        "baseline qualification: fetch reached the root locator index"
    );
}
