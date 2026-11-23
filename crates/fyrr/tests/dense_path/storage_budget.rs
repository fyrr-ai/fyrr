//! What a running reader asks of storage. Every storage request costs money
//! on an object store, so each phase of a reader's life has a budget here: a
//! change that adds requests fails this test until the budget is raised on
//! purpose.
use super::corrections::{BoundaryHook, proxy_at_boundary};
use super::mirror_identity::{EXTRA, KEPT, served, sync_texts};
use super::*;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Every storage operation the reader has started, in order.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<String>>>);
impl Seen {
    fn hook(&self) -> BoundaryHook {
        let seen = self.0.clone();
        Arc::new(move |request, _, after| {
            if !after {
                // `GET /local/<absolute path> HTTP/1.1`, shown from the namespace on.
                let line = request.lines().next().unwrap_or_default();
                // Request boundaries of the reader are labels, not storage requests.
                if ["AUTHORIZE ", "RERANK "]
                    .iter()
                    .any(|label| line.starts_with(label))
                {
                    return false;
                }
                let line = line.trim_end_matches(" HTTP/1.1");
                let shown = match line.split_once("/namespaces/") {
                    Some((method, path)) => {
                        // The observer renders a streamed listing as an S3 list request.
                        let method = if method.contains("?list-type=2") {
                            "LIST"
                        } else {
                            method.split(' ').next().unwrap_or_default()
                        };
                        format!("{method} {}", path.split_once('/').map_or(path, |(_, p)| p))
                    }
                    None => line.to_owned(),
                };
                seen.lock().unwrap().push(shown);
            }
            false
        })
    }
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    /// Every operation since `from`.
    fn all(&self, from: usize) -> Vec<String> {
        self.0.lock().unwrap()[from..].to_vec()
    }
    /// The operations since `from`: reads of the serving head, and all others.
    fn since(&self, from: usize) -> (usize, Vec<String>) {
        let seen = self.0.lock().unwrap();
        let (heads, others): (Vec<_>, Vec<_>) = seen[from..]
            .iter()
            .cloned()
            .partition(|request| *request == "GET head.json");
        (heads.len(), others)
    }
    /// Waits until only the version check still reads, for two quiet
    /// windows in a row: background warming has ended.
    fn settle(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        let mut quiet = 0;
        while quiet < 2 {
            let mark = self.len();
            std::thread::sleep(Duration::from_secs(3));
            quiet = if self.since(mark).1.is_empty() {
                quiet + 1
            } else {
                0
            };
            assert!(
                std::time::Instant::now() < deadline,
                "storage reads never settled"
            );
        }
    }
}

/// Asserts one phase against its budget, printing what it requested.
#[allow(clippy::print_stderr)]
fn within(phase: &str, (heads, others): (usize, Vec<String>), budget: usize, heads_allowed: usize) {
    let report = format!(
        "budget {phase}: {} requests (budget {budget}), {heads} head reads (allowed {heads_allowed})\n{}\n",
        others.len(),
        others.join("\n")
    );
    eprint!("{report}");
    // CI keeps the report of a passing run too.
    if let Some(directory) = std::env::var_os("FYRR_CI_LOG_DIR") {
        use std::io::Write;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(Path::new(&directory).join("storage-budget.txt"))
            .and_then(|mut file| file.write_all(report.as_bytes()))
            .unwrap();
    }
    assert!(
        others.len() <= budget,
        "{phase} made {} storage requests, budget {budget}",
        others.len()
    );
    assert!(
        heads <= heads_allowed,
        "{phase} read the head {heads} times"
    );
}

/// The version check reads the head once per interval, whatever else happens.
/// Preparing a new generation reads it three more times: when the check
/// finds the publication, when the generation opens and before it serves.
fn head_reads_allowed(elapsed: Duration, switches: usize) -> usize {
    usize::try_from(elapsed.as_millis() / INTERVAL.as_millis()).unwrap() + 5 + 3 * switches
}

const INTERVAL: Duration = Duration::from_secs(1);
/// How long after a switch the reader warms the new generation
/// (`serve/scheduler.rs`, `next_warm`).
const WARM_DELAY_SECS: u64 = 30;
/// Storage requests, other than the version check's, of a reader that opens a
/// one-document namespace on an empty mirror, answers its first search and
/// warms the generation: each control artifact, index file and data file
/// once. They grow with the files of a publication. Raise a budget only on
/// purpose: every request is paid for on an object store. The five attribute
/// columns of a connector namespace (title, team, status, labels, created_at)
/// each have an index, and the title has a full-text index besides: 37
/// requests, 10 more than without them (the full-text index of the title
/// costs one).
const OPEN_BUDGET: usize = 37;
/// The same for switching to a publication that adds one document, warming
/// of the new generation included: 10 more than without the connector columns
/// (the full-text index of the title costs one).
const SWITCH_BUDGET: usize = 35;

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn a_running_reader_stays_within_its_storage_request_budget() {
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "")
        .replace("[reader]", "[reader]\nversion_check_interval = '1s'");
    std::fs::write(&config, &text).unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    success(&config, &["writer", "namespace", "create"]);
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    success(&config, &["writer", "commit"]);

    // Only the reader runs behind the observer; the writer keeps `config`.
    let seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), seen.hook());
    let reader = root.join("observed.toml");
    std::fs::write(&reader, text.replace(&endpoint, &observed)).unwrap();

    // Open, the first search and warming.
    let started = std::time::Instant::now();
    let mut client = super::tools::Mcp::open(&reader);
    let first = served(&mut client).unwrap().0;
    seen.settle();
    let allowed = head_reads_allowed(started.elapsed(), 1);
    // The budgets are upper bounds: a detached observer would pass them.
    let opened = seen.since(0);
    assert!(
        opened.0 >= 1 && !opened.1.is_empty(),
        "the observer saw {} head reads and {} other requests while the reader opened",
        opened.0,
        opened.1.len()
    );
    within("open", opened, OPEN_BUDGET, allowed);

    // A warm reader: searches cost nothing, an idle reader only checks.
    let (mark, started) = (seen.len(), std::time::Instant::now());
    for _ in 0..20 {
        assert_eq!(served(&mut client).unwrap().0, first);
    }
    std::thread::sleep(Duration::from_secs(3));
    let allowed = head_reads_allowed(started.elapsed(), 0);
    within("warm", seen.since(mark), 0, allowed);

    // A new publication: one switch and its warming, then nothing again.
    // The writer is not observed, so the mark can precede its commit.
    let (mark, started) = (seen.len(), std::time::Instant::now());
    sync_texts(&config, &root, &[("kept.txt", KEPT), ("extra.txt", EXTRA)]);
    success(&config, &["writer", "commit"]);
    let second = super::coordination::publication(&config)
        .await
        .1
        .publication_id;
    assert_ne!(second, first);
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while served(&mut client).map(|(publication, _)| publication) != Ok(second.clone()) {
        assert!(std::time::Instant::now() < deadline, "never switched");
        std::thread::sleep(Duration::from_millis(250));
    }
    // A switched generation serves at once and is warmed 30 s later.
    std::thread::sleep(Duration::from_secs(WARM_DELAY_SECS + 2));
    seen.settle();
    let allowed = head_reads_allowed(started.elapsed(), 1);
    within("switch", seen.since(mark), SWITCH_BUDGET, allowed);

    let (mark, started) = (seen.len(), std::time::Instant::now());
    for _ in 0..20 {
        served(&mut client).unwrap();
    }
    std::thread::sleep(Duration::from_secs(3));
    let allowed = head_reads_allowed(started.elapsed(), 0);
    within("warm after the switch", seen.since(mark), 0, allowed);
    // Mark teardown before the reader is killed mid-observation.
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
}

/// Attributes of the namespace that measures the cost of one more.
const ATTRIBUTES: usize = 64;
/// Storage requests one declared attribute adds to opening a reader on an
/// empty mirror and warming it: reads of its index files (1.8 measured with
/// 64 attributes). Raise it only on purpose.
const ATTRIBUTE_OPEN_BUDGET: usize = 2;

/// The storage requests, other than the version check's, of a reader that
/// opens a one-document namespace declaring `attributes` attributes, answers
/// its first search and warms the generation.
async fn open_requests(attributes: usize) -> usize {
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let kinds = ["string", "int", "float", "bool", "datetime"];
    let declared: Vec<_> = (0..attributes)
        .map(|n| (format!("attribute_{n}"), kinds[n % kinds.len()]))
        .collect();
    let declared: Vec<_> = declared.iter().map(|(n, k)| (n.as_str(), *k)).collect();
    let text = super::index_errors::declare(&config, &declared)
        .replace("# fyrr-test-head-check = every", "")
        .replace("[reader]", "[reader]\nversion_check_interval = '1s'");
    std::fs::write(&config, &text).unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    success(&config, &["writer", "namespace", "create"]);
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    success(&config, &["writer", "commit"]);
    let seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), seen.hook());
    let reader = root.join("observed.toml");
    std::fs::write(&reader, text.replace(&endpoint, &observed)).unwrap();
    let started = std::time::Instant::now();
    let mut client = super::tools::Mcp::open(&reader);
    served(&mut client).unwrap();
    seen.settle();
    let allowed = head_reads_allowed(started.elapsed(), 1);
    let opened = seen.since(0);
    let requests = opened.1.len();
    within(
        &format!("open with {attributes} attributes"),
        opened,
        usize::MAX,
        allowed,
    );
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
    requests
}

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
#[allow(clippy::print_stderr)]
async fn an_attribute_costs_a_reader_a_bounded_number_of_storage_requests() {
    let without = open_requests(0).await;
    let with = open_requests(ATTRIBUTES).await;
    let added = with.saturating_sub(without);
    eprintln!(
        "budget attributes: {without} requests without, {with} with {ATTRIBUTES}: {added} added, {:.2} per attribute",
        added as f64 / ATTRIBUTES as f64
    );
    assert!(
        added <= ATTRIBUTES * ATTRIBUTE_OPEN_BUDGET,
        "{ATTRIBUTES} attributes added {added} storage requests, budget {} each",
        ATTRIBUTE_OPEN_BUDGET
    );
}

/// Storage requests one declared vector attribute adds to a reader that opens
/// a namespace on an empty mirror, warms it and answers a first search of each
/// attribute: Lance opens an index when it is first searched, so the open
/// itself costs none, and the first search reads the files of the index and of
/// the column. Raise it only on purpose.
const VECTOR_OPEN_BUDGET: usize = 2;
// The most vector attributes a namespace declares cost a reader no more to open
// than a whole namespace without any: `limits::schema::MAX_VECTOR_ATTRIBUTES` is
// held to the measured cost.
const _: () =
    assert!(fyrr_core::limits::schema::MAX_VECTOR_ATTRIBUTES * VECTOR_OPEN_BUDGET <= OPEN_BUDGET);

/// The storage requests, other than the version check's, of a reader that
/// opens a one-document namespace written through the API with `vectors` named
/// vector attributes, each with a vector and so an index, answers its first
/// search, warms the generation, and answers a first search of each attribute.
async fn vector_open_requests(vectors: usize) -> usize {
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let address = free_address();
    super::write_api::configure(&config, &address, "open = true");
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "")
        .replace("[reader]", "[reader]\nversion_check_interval = '1s'");
    std::fs::write(&config, &text).unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector.clone());
    let writer = super::write_api::Writer::start(&config, &root, address).await;
    let mut row = json!({"id": "kept", "text": "ledger", "vector": vector});
    let mut declared = serde_json::Map::new();
    for n in 0..vectors {
        declared.insert(format!("v{n}"), json!({"dimensions": 8}));
        let value: Vec<f32> = (0..8).map(|d| (((n + 1) * (d + 3)) as f32).sin()).collect();
        row[format!("v{n}")] = json!(value);
    }
    let answer = writer
        .post(
            super::write_api::WRITE_KEY,
            "dense-fixture",
            &json!({"settings": {"vectors": declared}, "upserts": [row]}),
        )
        .await;
    assert_eq!(answer, (200, json!({"upserted": 1, "deleted": 0})));
    drop(writer);
    let seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), seen.hook());
    let reader = root.join("observed.toml");
    std::fs::write(&reader, text.replace(&endpoint, &observed)).unwrap();
    let started = std::time::Instant::now();
    let mut client = super::tools::Mcp::open(&reader);
    served(&mut client).unwrap();
    for n in 0..vectors {
        let value: Vec<f32> = (0..8).map(|d| (((n + 1) * (d + 3)) as f32).sin()).collect();
        let hits = super::tools::body(client.call(
            "search",
            json!({"vector": value, "vector_attribute": format!("v{n}"), "output": "metadata"}),
        ));
        assert_eq!(hits["hits"].as_array().unwrap().len(), 1);
    }
    seen.settle();
    let allowed = head_reads_allowed(started.elapsed(), 1);
    let opened = seen.since(0);
    let requests = opened.1.len();
    within(
        &format!("open and search with {vectors} vector attributes"),
        opened,
        usize::MAX,
        allowed,
    );
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
    requests
}

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
#[allow(clippy::print_stderr)]
async fn a_vector_attribute_costs_a_reader_a_bounded_number_of_storage_requests() {
    const VECTORS: usize = fyrr_core::limits::schema::MAX_VECTOR_ATTRIBUTES;
    let without = vector_open_requests(0).await;
    let with = vector_open_requests(VECTORS).await;
    let added = with.saturating_sub(without);
    eprintln!(
        "budget vectors: {without} requests without, {with} with {VECTORS}: {added} added, {:.2} per vector attribute",
        added as f64 / VECTORS as f64
    );
    assert!(
        added <= VECTORS * VECTOR_OPEN_BUDGET,
        "{VECTORS} vector attributes added {added} storage requests ({without} without, {with} with), budget {VECTOR_OPEN_BUDGET} each"
    );
}

/// What a reader that declares API keys adds to the budgets above: one `GET`
/// of the published keys when it starts and one per version check, and nothing
/// per request.
#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn an_http_reader_with_api_keys_reads_them_once_per_version_check() {
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "")
        .replace("[reader]", "[reader]\nversion_check_interval = '1s'");
    let text = format!(
        "{text}\n[[identity.api_keys]]\nname='budget'\nkey={{env='FYRR_BUDGET_KEY'}}\nreaders=['usr:budget']\n\
         [[identity.api_keys]]\nname='outside'\nkey={{env='FYRR_BUDGET_OUTSIDE'}}\nreaders=['usr:out']\nnamespace_prefix='zz'\n"
    );
    std::fs::write(&config, &text).unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    success(&config, &["writer", "namespace", "create"]);
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    success(&config, &["writer", "commit"]);
    let keys = super::coordination::host_command(&config)
        .args(["writer", "keys"])
        .env("FYRR_BUDGET_KEY", format!("fyrr_key_{:064}", 6))
        .env("FYRR_BUDGET_OUTSIDE", format!("fyrr_key_{:064}", 8))
        .output()
        .unwrap();
    assert!(keys.status.success(), "{}", report(&keys));
    // A second namespace of the deployment, which a request path can name.
    let second = root.join("second.toml");
    std::fs::write(
        &second,
        text.replace("name = \"dense-fixture\"", "name = \"dense-fixture-two\""),
    )
    .unwrap();
    success(&second, &["writer", "namespace", "create"]);
    sync_texts(&second, &root, &[("kept.txt", KEPT)]);
    let committed = super::coordination::host_command(&second)
        .args(["writer", "commit"])
        .env("FYRR_BUDGET_KEY", format!("fyrr_key_{:064}", 6))
        .output()
        .unwrap();
    assert!(committed.status.success(), "{}", report(&committed));

    let seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), seen.hook());
    let observed_config = root.join("observed.toml");
    std::fs::write(&observed_config, text.replace(&endpoint, &observed)).unwrap();
    let started = std::time::Instant::now();
    let mut reader =
        super::http_mcp::Reader::start_appended(&observed_config, &root, |address, base| {
            format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
        })
        .await;
    // Requests that carry a key nobody published read no keys.
    let secret = format!("fyrr_key_{:064}", 7);
    for _ in 0..20 {
        let response = reader
            .client
            .post(format!("{}/mcp", reader.base))
            .bearer_auth(&secret)
            .header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 401);
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    let keys = seen
        .all(0)
        .iter()
        .filter(|request| request.ends_with("/api-keys.json"))
        .count();
    let allowed =
        usize::try_from(started.elapsed().as_millis() / INTERVAL.as_millis()).unwrap() + 2;
    assert!(
        (2..=allowed).contains(&keys),
        "the keys were read {keys} times, allowed {allowed}"
    );

    // A namespace that does not exist costs one read of its head and nothing
    // else; one that does opens within the budget of any namespace. Reads of
    // the published keys belong to the version checks, so they are set apart.
    let post_as = |key: usize, namespace: String| {
        reader
            .client
            .post(format!("{}/v1/namespaces/{namespace}/query", reader.base))
            .bearer_auth(format!("fyrr_key_{key:064}"))
            .json(&json!({"query":"ledger"}))
            .send()
    };
    let phase = |from: usize| {
        let reads = seen.all(from);
        let heads = reads.iter().filter(|read| *read == "GET head.json").count();
        let others: Vec<_> = reads
            .into_iter()
            .filter(|read| read != "GET head.json" && !read.ends_with("/api-keys.json"))
            .collect();
        (heads, others)
    };
    let post = |namespace: String| post_as(6, namespace);
    // A key outside its prefix reads no head: nothing of a namespace is read
    // before the published keys admit the caller.
    let (mark, started) = (seen.len(), std::time::Instant::now());
    for i in 0..20 {
        let response = post_as(8, format!("missing-{i}")).await.unwrap();
        assert_eq!(response.status(), 403);
    }
    let intervals = usize::try_from(started.elapsed().as_millis() / INTERVAL.as_millis()).unwrap();
    within("a key outside its prefix", phase(mark), 0, intervals + 2);
    let (mark, started) = (seen.len(), std::time::Instant::now());
    for i in 0..20 {
        let response = post(format!("missing-{i}")).await.unwrap();
        assert_eq!(response.status(), 401);
    }
    let (heads, others) = phase(mark);
    assert!(heads >= 20, "the observer saw {heads} head reads");
    // Two readers check their version: allow both their reads.
    let intervals = usize::try_from(started.elapsed().as_millis() / INTERVAL.as_millis()).unwrap();
    within(
        "a namespace that does not exist",
        (heads, others),
        0,
        20 + 2 * intervals + 2,
    );
    let (mark, started) = (seen.len(), std::time::Instant::now());
    let response = post("dense-fixture-two".to_owned()).await.unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let mut quiet = 0;
    while quiet < 2 {
        let before = seen.len();
        tokio::time::sleep(Duration::from_secs(3)).await;
        quiet = if phase(before).1.is_empty() {
            quiet + 1
        } else {
            0
        };
        assert!(
            std::time::Instant::now() < deadline,
            "storage reads never settled"
        );
    }
    let (heads, others) = phase(mark);
    let intervals = usize::try_from(started.elapsed().as_millis() / INTERVAL.as_millis()).unwrap();
    within(
        "a namespace opened by its path",
        (heads, others),
        OPEN_BUDGET,
        2 * intervals + 10,
    );

    // Once it has served nothing for an interval it asks for nothing, so
    // only the configured namespace checks its head; the next request
    // is still answered.
    let (mark, started) = (seen.len(), std::time::Instant::now());
    tokio::time::sleep(Duration::from_secs(8)).await;
    let intervals = usize::try_from(started.elapsed().as_millis() / INTERVAL.as_millis()).unwrap();
    within(
        "an idle namespace opened by its path",
        phase(mark),
        0,
        intervals + 2,
    );
    let response = post("dense-fixture-two".to_owned()).await.unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    reader.stop().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// A namespace report is built from the head and one manifest at most.
const REPORT_READS: usize = 2;

/// `GET /v1/namespaces/{namespace}` answers from what the reader holds: it
/// reads at most the head and one manifest, here nothing, and keeps the
/// published keys to the version checks.
#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn a_namespace_report_reads_at_most_the_head_and_one_manifest() {
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let key = format!("fyrr_key_{:064}", 6);
    // The longest interval of a reader: no check of its own during the test.
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "")
        .replace("[reader]", "[reader]\nversion_check_interval = '30s'");
    let text = format!(
        "{text}\n[[identity.api_keys]]\nname='budget'\nkey={{env='FYRR_BUDGET_KEY'}}\nroles=['list']\n"
    );
    std::fs::write(&config, &text).unwrap();
    let namespace = fyrr_core::config::load(&config)
        .unwrap()
        .config
        .namespace
        .name;
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    success(&config, &["writer", "namespace", "create"]);
    let writer = |args: &[&str]| {
        let output = super::coordination::host_command(&config)
            .args(args)
            .env("FYRR_BUDGET_KEY", &key)
            .output()
            .unwrap();
        assert!(output.status.success(), "{}", report(&output));
    };
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    writer(&["writer", "commit"]);
    writer(&["writer", "keys"]);

    let seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), seen.hook());
    let observed_config = root.join("observed.toml");
    std::fs::write(&observed_config, text.replace(&endpoint, &observed)).unwrap();
    let mut reader =
        super::http_mcp::Reader::start_appended(&observed_config, &root, |address, base| {
            format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
        })
        .await;
    let ask = || {
        reader
            .client
            .get(format!("{}/v1/namespaces/{namespace}", reader.base))
            .bearer_auth(&key)
            .send()
    };
    // The first request may wait for the reader's first version check.
    let response = ask().await.unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    seen.settle();
    let mark = seen.len();
    for _ in 0..5 {
        let response = ask().await.unwrap();
        assert_eq!(response.status(), 200);
        let body: Value = response.json().await.unwrap();
        assert!(body["rows"].as_u64().unwrap() > 0, "{body}");
        assert_eq!(body["unindexed"]["rows"], 0, "{body}");
    }
    let (heads, others) = seen.since(mark);
    let others: Vec<_> = others
        .into_iter()
        .filter(|read| !read.ends_with("/api-keys.json"))
        .collect();
    assert!(
        heads + others.len() <= REPORT_READS,
        "5 reports read {heads} heads and {others:?}"
    );
    within(
        "a namespace report",
        (heads, others),
        REPORT_READS,
        REPORT_READS,
    );
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    reader.stop().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// `GET /v1/namespaces` is one delimited listing of `namespaces/`, 1 LIST
/// request per 1,000 names, and nothing else: no head, no manifest.
const LIST_REQUESTS: usize = 1;

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn listing_namespaces_is_one_delimited_listing() {
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let key = format!("fyrr_key_{:064}", 7);
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "")
        .replace("[reader]", "[reader]\nversion_check_interval = '30s'");
    let text = format!(
        "{text}\n[[identity.api_keys]]\nname='budget'\nkey={{env='FYRR_BUDGET_KEY'}}\nroles=['list']\n"
    );
    std::fs::write(&config, &text).unwrap();
    let namespace = fyrr_core::config::load(&config)
        .unwrap()
        .config
        .namespace
        .name;
    success(&config, &["writer", "namespace", "create"]);
    let writer = |args: &[&str]| {
        let output = super::coordination::host_command(&config)
            .args(args)
            .env("FYRR_BUDGET_KEY", &key)
            .output()
            .unwrap();
        assert!(output.status.success(), "{}", report(&output));
    };
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    writer(&["writer", "commit"]);
    writer(&["writer", "keys"]);

    let seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), seen.hook());
    let observed_config = root.join("observed.toml");
    std::fs::write(&observed_config, text.replace(&endpoint, &observed)).unwrap();
    let mut reader =
        super::http_mcp::Reader::start_appended(&observed_config, &root, |address, base| {
            format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
        })
        .await;
    let ask = || {
        reader
            .client
            .get(format!("{}/v1/namespaces", reader.base))
            .bearer_auth(&key)
            .send()
    };
    // The first request may wait for the reader's first version check.
    let response = ask().await.unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    seen.settle();
    let mark = seen.len();
    for _ in 0..5 {
        let response = ask().await.unwrap();
        assert_eq!(response.status(), 200);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["namespaces"], json!([namespace]), "{body}");
    }
    let (heads, others) = seen.since(mark);
    let others: Vec<_> = others
        .into_iter()
        .filter(|read| !read.ends_with("/api-keys.json"))
        .collect();
    assert!(
        others.iter().all(|request| request.contains("LISTDIR")),
        "5 listings made {heads} head reads and {others:?}"
    );
    within(
        "5 namespace listings",
        (heads, others),
        5 * LIST_REQUESTS,
        0,
    );
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    reader.stop().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// What a strong read adds to the budgets above: one `GET` of the head when it
/// has not moved. A cached read adds none.
const STRONG_HEAD_READS: usize = 1;

/// A `strong` read of the HTTP JSON route reads the head after it arrived: one
/// request when the head has not moved, the new publication when it has, and
/// a retryable refusal, never the held view, when the head cannot be read.
#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn a_strong_read_checks_the_head_and_never_answers_from_a_held_view() {
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let key = format!("fyrr_key_{:064}", 6);
    // The longest interval of a reader: no check of its own during the test.
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "")
        .replace("[reader]", "[reader]\nversion_check_interval = '30s'");
    let text = format!(
        "{text}\n[[identity.api_keys]]\nname='budget'\nkey={{env='FYRR_BUDGET_KEY'}}\nreaders=['usr:budget']\n"
    );
    std::fs::write(&config, &text).unwrap();
    let namespace = fyrr_core::config::load(&config)
        .unwrap()
        .config
        .namespace
        .name;
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    success(&config, &["writer", "namespace", "create"]);
    // The commit binds the declared key to the namespace, so it reads the key.
    let writer = |args: &[&str]| {
        let output = super::coordination::host_command(&config)
            .args(args)
            .env("FYRR_BUDGET_KEY", &key)
            .output()
            .unwrap();
        assert!(output.status.success(), "{}", report(&output));
    };
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    writer(&["writer", "commit"]);
    writer(&["writer", "keys"]);

    let seen = Seen::default();
    let unreadable = Arc::new(AtomicBool::new(false));
    let hook: BoundaryHook = {
        let (observe, unreadable) = (seen.hook(), unreadable.clone());
        Arc::new(move |request, body, after| {
            let head = request
                .lines()
                .next()
                .is_some_and(|line| line.starts_with("GET ") && line.contains("/head.json "));
            observe(request, body, after)
                || (!after && head && unreadable.load(std::sync::atomic::Ordering::SeqCst))
        })
    };
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
    let observed_config = root.join("observed.toml");
    std::fs::write(&observed_config, text.replace(&endpoint, &observed)).unwrap();
    let mut reader =
        super::http_mcp::Reader::start_appended(&observed_config, &root, |address, base| {
            format!("\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n")
        })
        .await;
    let ask = |consistency: Option<&str>| {
        let mut body = json!({"query":"ledger"});
        if let Some(level) = consistency {
            body["consistency"] = json!(level);
        }
        let request = reader
            .client
            .post(format!("{}/v1/namespaces/{namespace}/query", reader.base))
            .bearer_auth(&key)
            .json(&body);
        async move {
            let response = request.send().await.unwrap();
            let retry = response.headers().get("retry-after").cloned();
            (
                response.status().as_u16(),
                response.json::<Value>().await.unwrap(),
                retry,
            )
        }
    };
    let publication = |body: &Value| {
        body["metadata"]["publication_id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    // The requests of a phase other than reads of the published keys.
    let phase = |from: usize| {
        let (heads, others) = seen.since(from);
        let others: Vec<_> = others
            .into_iter()
            .filter(|read| !read.ends_with("/api-keys.json"))
            .collect();
        (heads, others)
    };

    let (status, body, _) = ask(None).await;
    assert_eq!(status, 200, "{body}");
    let first = publication(&body);
    // A value that is no level is refused before anything is read.
    let (status, body, _) = ask(Some("eventual")).await;
    assert_eq!((status, body), (400, json!({"error":"invalid_argument"})));
    seen.settle();
    // The first strong read starts the check that the next ones follow.
    let (status, body, _) = ask(Some("strong")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(publication(&body), first);

    // Nothing moved: a cached read asks storage nothing, a strong one reads the head once.
    let mark = seen.len();
    let (status, ..) = ask(Some("cached")).await;
    assert_eq!(status, 200);
    let (status, ..) = ask(None).await;
    assert_eq!(status, 200);
    within("a cached read", phase(mark), 0, 0);
    let mark = seen.len();
    let (status, body, _) = ask(Some("strong")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(publication(&body), first);
    let (heads, others) = phase(mark);
    assert_eq!(
        heads, STRONG_HEAD_READS,
        "a strong read reads the head once"
    );
    within("a strong read", (heads, others), 0, STRONG_HEAD_READS);

    // A write acknowledged before the request: the held view lacks it, a strong read has it.
    sync_texts(&config, &root, &[("kept.txt", KEPT), ("extra.txt", EXTRA)]);
    writer(&["writer", "commit"]);
    let second = super::coordination::publication(&config)
        .await
        .1
        .publication_id;
    assert_ne!(second, first);
    let (status, body, _) = ask(Some("cached")).await;
    assert_eq!(status, 200);
    assert_eq!(publication(&body), first, "the held view is not refreshed");
    let (status, body, _) = ask(Some("strong")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(publication(&body), second);

    // A head that cannot be read: a named, retryable refusal, not the held view.
    unreadable.store(true, std::sync::atomic::Ordering::SeqCst);
    let (status, body, retry) = ask(Some("strong")).await;
    assert_eq!(
        (status, body),
        (503, json!({"error":"storage_unavailable"}))
    );
    assert_eq!(retry.unwrap(), "1");
    unreadable.store(false, std::sync::atomic::Ordering::SeqCst);
    let (status, body, _) = ask(Some("strong")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(publication(&body), second);

    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    reader.stop().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// A running writer service, stopped when dropped, also when a check fails.
struct Service(std::process::Child, Arc<AtomicBool>);
impl Drop for Service {
    fn drop(&mut self) {
        // Mark teardown before the service is stopped mid-observation.
        self.1.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status();
        let _ = self.0.wait();
    }
}

/// Publications made before the writer service starts. Work that grows with
/// the history shows as storage requests that grow with this number.
/// They, the namespace's first publication and the one `writer maintain`
/// makes form a history of six.
const PUBLICATIONS: usize = 4;
/// Seconds after the service settled that it is watched for sync workers:
/// past the minute after the last worker ended, when one used to be started
/// again whatever its intervals.
const RESPAWN_WINDOW_SECS: u64 = 70;
/// Whether a request changes storage: an idle service takes no lease and
/// writes nothing, not even the head.
fn writes_storage(request: &str) -> bool {
    !["GET ", "HEAD ", "LIST "]
        .iter()
        .any(|read| request.starts_with(read))
}

/// Ticks of the committer measured once the service has settled.
const TICKS: u64 = 8;
/// Storage requests of one idle committer tick, other than reads of
/// publication manifests: the head, the policy twice, the envelope listing,
/// the identity and the dataset open (12 measured), and the head once more
/// for the service's own watch for a change of the head (13). None of them
/// writes.
const IDLE_TICK_BUDGET: usize = 13;
/// Reads of publication manifests by one idle tick: the served manifest,
/// once for the receipt. A walk of the history would read `PUBLICATIONS` or
/// more.
const IDLE_TICK_MANIFESTS: usize = 1;

/// Watches a running writer service: waits until it has written nothing for
/// two seconds in a row, after its first seconds, then follows `TICKS` seconds.
/// Returns when it settled, where the window starts in `seen`, the window's
/// requests, and the committer's idle and published ticks in it.
fn watch_idle(
    child: &mut std::process::Child,
    seen: &Seen,
    log: &Path,
) -> (std::time::Instant, usize, Vec<String>, usize, usize) {
    // Startup runs a sync worker per source, which finds nothing due. The
    // service is idle once, after its first seconds, two seconds in a row
    // write nothing.
    let started = std::time::Instant::now();
    let deadline = started + Duration::from_secs(120);
    let mut quiet = 0;
    while quiet < 2 || started.elapsed() < Duration::from_secs(5) {
        let mark = seen.len();
        std::thread::sleep(Duration::from_secs(1));
        let writes = seen
            .all(mark)
            .iter()
            .filter(|request| writes_storage(request))
            .count();
        quiet = if writes == 0 && seen.len() > mark {
            quiet + 1
        } else {
            0
        };
        assert!(
            child.try_wait().unwrap().is_none(),
            "the writer service exited"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the service never settled"
        );
    }
    let events = |event: &str| {
        std::fs::read_to_string(log)
            .unwrap()
            .matches(&format!("writer_service event={event}"))
            .count()
    };
    let (settled, mark, idle_before, published_before) = (
        std::time::Instant::now(),
        seen.len(),
        events("idle"),
        events("published"),
    );
    std::thread::sleep(Duration::from_secs(TICKS));
    let requests = seen.all(mark);
    let (idle, published) = (
        events("idle") - idle_before,
        events("published") - published_before,
    );
    assert!(
        child.try_wait().unwrap().is_none(),
        "the writer service exited"
    );
    (settled, mark, requests, idle, published)
}

/// The budget of an idle window: no write, the ticks' reads, and one manifest
/// read per tick.
fn assert_idle_window(requests: Vec<String>, idle: usize, published: usize) {
    let (manifests, rest): (Vec<_>, Vec<_>) = requests
        .into_iter()
        .partition(|request| request.starts_with("GET publications/"));
    // A window of `TICKS` seconds holds at most one tick more; a loaded host
    // may run fewer. Only the service's own events tell an idle tick from a
    // tick that had nothing to look at: the committer ticked and took no lease.
    let ticks = usize::try_from(TICKS).unwrap() + 1;
    assert!(
        idle >= usize::try_from(TICKS / 2).unwrap(),
        "only {idle} idle ticks: the committer did not tick"
    );
    assert_eq!(published, 0, "an idle service committed");
    let writes: Vec<_> = rest
        .iter()
        .filter(|request| writes_storage(request))
        .collect();
    assert!(writes.is_empty(), "an idle service wrote {writes:?}");
    within(
        "idle writer, manifest reads",
        (0, manifests),
        IDLE_TICK_MANIFESTS * ticks,
        0,
    );
    within(
        "idle writer, other requests",
        (0, rest),
        IDLE_TICK_BUDGET * ticks,
        0,
    );
}

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn an_idle_writer_service_stays_within_its_storage_request_budget() {
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "")
        .replacen(
            "[[sources]]",
            "[writer.commit]\ncoalescing_timer = \"1s\"\n[[sources]]",
            1,
        );
    std::fs::write(&config, &text).unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    success(&config, &["writer", "namespace", "create"]);
    let mut texts = Vec::new();
    for i in 0..PUBLICATIONS {
        texts.push((format!("{i}.txt"), format!("ledger entry {i}")));
        let shown: Vec<_> = texts
            .iter()
            .map(|(n, t)| (n.as_str(), t.as_str()))
            .collect();
        sync_texts(&config, &root, &shown);
        success(&config, &["writer", "commit"]);
    }
    // The service maintains a namespace that was never maintained, which is not
    // idle; it is measured once that is done.
    success(&config, &["writer", "maintain"]);

    // Only the service runs behind the observer.
    let seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), seen.hook());
    let service = root.join("service.toml");
    std::fs::write(&service, text.replace(&endpoint, &observed)).unwrap();
    let log = root.join("service.log");
    let mut service = Service(
        Command::new(crate::common::binary())
            .arg("--config")
            .arg(&service)
            .args(["writer", "serve", "--jobs", "1"])
            .env(
                "VIRTUAL_ENV",
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../../connectors/.venv"),
            )
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap(),
        stop.clone(),
    );
    let child = &mut service.0;
    let (settled, mark, requests, idle, published) = watch_idle(child, &seen, &log);
    // Past the minute after which a sync worker used to be started again
    // whatever its intervals: a source with nothing due starts none, and the
    // service writes nothing. A worker reads its cursors first.
    std::thread::sleep(
        (settled + Duration::from_secs(RESPAWN_WINDOW_SECS))
            .saturating_duration_since(std::time::Instant::now()),
    );
    let all = seen.all(mark);
    assert!(
        child.try_wait().unwrap().is_none(),
        "the writer service exited"
    );
    drop(service);
    let workers: Vec<_> = all
        .iter()
        .filter(|request| request.contains(" cursors/"))
        .cloned()
        .collect();
    within(
        "idle writer, sync workers past the first minute",
        (0, workers),
        0,
        0,
    );
    let writes: Vec<_> = all
        .iter()
        .filter(|request| writes_storage(request))
        .cloned()
        .collect();
    within(
        "idle writer, writes over the whole window",
        (0, writes),
        0,
        0,
    );

    assert_idle_window(requests, idle, published);
    std::fs::remove_dir_all(root).unwrap();
}

/// A writer that wrote to many namespaces costs, idle, what one namespace costs:
/// only the configured one is looked at, and nothing is written.
#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn an_idle_writer_service_that_wrote_to_many_namespaces_stays_within_the_budget_of_one() {
    use super::write_api::{WRITE_KEY, Writer, configure, embed};
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let address = free_address();
    configure(&config, &address, "open = true");
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "")
        .replace("coalescing_timer='10m'", "coalescing_timer='1s'");
    let seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), seen.hook());
    let service = root.join("service.toml");
    std::fs::write(&service, text.replace(&endpoint, &observed)).unwrap();
    let mut writer = Writer::start(&service, &root, address).await;

    let names = ["dense-fixture", "shop-a", "shop-b", "shop-c", "shop-d"];
    let texts = [
        "blue shirt",
        "red hat",
        "green coat",
        "black scarf",
        "white sock",
    ];
    embed(&root, &texts);
    let api = &writer;
    let replies = futures::future::join_all(names.iter().zip(texts).map(|(name, text)| {
        let body = json!({"upserts": [{"id": "a", "text": text}]});
        async move { api.post(WRITE_KEY, name, &body).await }
    }))
    .await;
    for reply in replies {
        assert_eq!(reply, (200, json!({"upserted": 1, "deleted": 0})));
    }

    let log = root.join("service.stderr");
    let (_, _, requests, idle, published) = watch_idle(&mut writer.child, &seen, &log);
    assert_idle_window(requests, idle, published);
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    drop(writer);
    std::fs::remove_dir_all(root).unwrap();
}

/// Index segments `writer maintain` allows an index before it merges them.
const SEGMENT_CAP: usize = 64;

/// A service that only serves keeps its namespace bounded: it compacts a
/// fragment with deletions once the grace has passed, and two more commits than the cap leave at most
/// the cap of segments per index and few fragments, with no `writer maintain`
/// run after the service started.
#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn a_writer_service_maintains_the_namespace_it_serves() {
    let (config, root) = setup_on(Backend::Local);
    assert_eq!(
        std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap(),
        "local-storage",
        "owned by the local CI row"
    );
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "")
        .replacen(
            "[[sources]]",
            "[writer.commit]\ncoalescing_timer = \"1s\"\n[[sources]]",
            1,
        );
    std::fs::write(&config, &text).unwrap();
    success(&config, &["writer", "namespace", "create"]);
    // Two documents in one fragment: replacing one leaves a deletion in it.
    let mut texts = vec![
        ("0.txt".to_owned(), "ledger entry 0".to_owned()),
        ("1.txt".to_owned(), "ledger entry 1".to_owned()),
    ];
    let sync = |texts: &[(String, String)]| {
        let borrowed: Vec<_> = texts
            .iter()
            .map(|(n, t)| (n.as_str(), t.as_str()))
            .collect();
        sync_texts(&config, &root, &borrowed);
    };
    sync(&texts);
    success(&config, &["writer", "commit"]);
    // The command keeps working, and the service starts from a maintained namespace.
    success(&config, &["writer", "maintain"]);

    let log = root.join("service.log");
    let service = Service(
        Command::new(crate::common::binary())
            .arg("--config")
            .arg(&config)
            .args(["writer", "serve", "--jobs", "1"])
            .env(
                "VIRTUAL_ENV",
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../../connectors/.venv"),
            )
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap(),
        Arc::new(AtomicBool::new(false)),
    );
    let events = |event: &str| {
        std::fs::read_to_string(&log)
            .unwrap()
            .matches(&format!("writer_service event={event}"))
            .count()
    };
    let wait = |what: &str, done: &dyn Fn() -> bool| {
        let deadline = std::time::Instant::now() + Duration::from_secs(180);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "timed out: {what}");
            std::thread::sleep(Duration::from_millis(250));
        }
    };
    // A replaced document leaves a deletion in its fragment: the service compacts it.
    texts[0].1 = "ledger entry zero, revised".to_owned();
    sync(&texts);
    // Deletions wait for `writer.version_grace` after the last maintenance, so
    // that steady upserts do not run it on every publication.
    fyrr_core::clock::advance(Duration::from_secs(2 * 3600)).unwrap();
    wait("maintenance of a deletion", &|| events("maintained") >= 1);
    let (_, manifest) = super::coordination::publication(&config).await;
    assert!(
        manifest
            .files
            .iter()
            .all(|file| !file.path.starts_with("_deletions/")),
        "{:?}",
        manifest.files
    );
    for i in 2..SEGMENT_CAP + 4 {
        let before = events("published");
        texts.push((format!("{i}.txt"), format!("ledger entry {i}")));
        sync(&texts);
        wait("a publication", &|| events("published") > before);
    }
    wait("maintenance of the segments", &|| events("maintained") >= 2);
    drop(service);
    assert_eq!(events("maintenance_failed"), 0);

    let (_, manifest) = super::coordination::publication(&config).await;
    let mut segments = std::collections::BTreeMap::<String, usize>::new();
    for index in &manifest.indexes {
        *segments.entry(index.name.clone()).or_default() += 1;
    }
    assert!(
        segments.values().all(|count| *count <= SEGMENT_CAP),
        "{segments:?}"
    );
    let loaded = fyrr_core::config::load(&config).unwrap();
    let dataset = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        loaded
            .config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        manifest.lance_version,
        None,
    )
    .await
    .unwrap();
    let fragments = dataset.manifest().fragments.len();
    // Each commit adds a fragment per source; unmaintained they would all remain.
    let commits = SEGMENT_CAP + 2;
    assert!(
        fragments < 2 * commits,
        "{fragments} fragments after {commits} commits"
    );
    success(&config, &["writer", "verify"]);
    std::fs::remove_dir_all(root).unwrap();
}

/// Publications a namespace has when a sync worker is first measured, and
/// when it is measured again after its history has grown.
const HISTORY: [usize; 2] = [4, 12];
/// Reads of publication manifests by one `writer sync` run (4 measured): the
/// newest manifest, again and again. A walk of the history would read one
/// more for every publication.
const SYNC_MANIFESTS: usize = 4;

#[tokio::test]
#[ignore = "real executable and local storage; scripts/ci"]
async fn a_sync_worker_reads_no_more_manifests_as_the_history_grows() {
    let (config, root) = setup_on(Backend::Local);
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    assert_eq!(endpoint, "local-storage", "owned by the local CI row");
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("# fyrr-test-head-check = every", "");
    std::fs::write(&config, &text).unwrap();
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    root.embedding.insert_both("ledger", vector);
    success(&config, &["writer", "namespace", "create"]);

    // Only the sync workers run behind the observer.
    let seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (observed, _observer) = proxy_at_boundary(endpoint.clone(), stop.clone(), seen.hook());
    let worker = root.join("worker.toml");
    std::fs::write(&worker, text.replace(&endpoint, &observed)).unwrap();

    let mut texts = Vec::new();
    for history in HISTORY {
        while texts.len() < history {
            texts.push((
                format!("{}.txt", texts.len()),
                format!("ledger entry {}", texts.len()),
            ));
            let shown: Vec<_> = texts
                .iter()
                .map(|(n, t)| (n.as_str(), t.as_str()))
                .collect();
            sync_texts(&config, &root, &shown);
            success(&config, &["writer", "commit"]);
        }
        // New worker processes: nothing they know survives from the last run.
        let mark = seen.len();
        let sync = run(&worker, &["writer", "sync"]);
        assert!(sync.status.success(), "{}", report(&sync));
        let manifests: Vec<_> = seen
            .all(mark)
            .into_iter()
            .filter(|request| request.starts_with("GET publications/"))
            .collect();
        within(
            &format!("sync workers, {history} publications"),
            (0, manifests),
            SYNC_MANIFESTS,
            0,
        );
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);

    // Maintenance deletes the checkpoints no retained manifest names, and only
    // those: the publication that is served keeps its own.
    use futures::TryStreamExt;
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let evidence = namespace.clone().join("evidence");
    let named: Vec<_> = store.list(Some(&evidence)).try_collect().await.unwrap();
    assert_eq!(named.len(), 1, "one checkpoint for a history this short");
    // More than the 64 deletes between two revocation boundaries.
    let strays: Vec<_> = (0..130)
        .map(|i| {
            evidence
                .clone()
                .join(format!("{i}-{}.json", "0".repeat(64)))
        })
        .collect();
    for stray in &strays {
        store.put(stray, "{}".into()).await.unwrap();
    }
    success(&config, &["writer", "maintain"]);
    for stray in &strays {
        assert!(matches!(
            store.head(stray).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
    let kept: Vec<_> = store.list(Some(&evidence)).try_collect().await.unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].location, named[0].location);
    std::fs::remove_dir_all(root).unwrap();
}
