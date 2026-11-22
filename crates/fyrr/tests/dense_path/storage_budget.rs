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
/// purpose: every request is paid for on an object store.
const OPEN_BUDGET: usize = 27;
/// The same for switching to a publication that adds one document, warming
/// of the new generation included.
const SWITCH_BUDGET: usize = 25;

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
/// They and the namespace's first publication form a history of five.
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
        std::fs::read_to_string(&log)
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
