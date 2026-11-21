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
    /// The operations since `from`: reads of the serving head, and all others.
    fn since(&self, from: usize) -> (usize, Vec<String>) {
        let seen = self.0.lock().unwrap();
        let (heads, others): (Vec<_>, Vec<_>) = seen[from..]
            .iter()
            .cloned()
            .partition(|request| request.ends_with(" head.json"));
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
// shortcut: a data file is read twice or three times when searches reach it
// in pages before warming downloads it whole (open 28 to 30 on a loaded host,
// switch 26 to 28); lower both budgets once small data files are downloaded
// whole on their first read.
const OPEN_BUDGET: usize = 30;
/// The same for switching to a publication that adds one document, warming
/// of the new generation included.
const SWITCH_BUDGET: usize = 28;

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
    within("open", seen.since(0), OPEN_BUDGET, allowed);

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
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while served(&mut client).is_ok_and(|(publication, _)| publication == first) {
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
