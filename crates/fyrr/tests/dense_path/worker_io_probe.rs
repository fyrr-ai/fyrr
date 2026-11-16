//! An isolated provider copy measures GIL ownership without changing provider behavior.
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

fn copy_tree(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            if entry.file_name() != "__pycache__" {
                copy_tree(&entry.path(), &target.join(entry.file_name()));
            }
        } else {
            std::fs::copy(entry.path(), target.join(entry.file_name())).unwrap();
        }
    }
}

#[allow(clippy::print_stdout)]
#[tokio::test]
#[ignore = "diagnostic: threaded provider around a real revision callback"]
async fn revision_callback_thread_progress_measurement_on_local_storage() {
    let setup = setup_on(Backend::Local);
    for n in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{n}.txt"))).unwrap();
    }
    let (worker, commit, root) = incremental::fixture_host_at(8, true, "dense-fixture", setup);
    incremental::publish_initial(&worker, &commit, &root);
    let config = fyrr_core::config::load(&worker).unwrap().config;
    let (_, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
    let template = incremental::envelopes(&worker)
        .await
        .into_iter()
        .find(|(_, envelope)| {
            envelope.source_id == "incremental"
                && envelope.kind == fyrr_core::connector::SyncKind::Content
        })
        .unwrap()
        .1;
    let mut seed = Vec::new();
    for n in 0..64 {
        let mut envelope = template.clone();
        envelope.sequence = n + 100;
        envelope.envelope_id = fyrr_core::envelope::envelope_id(
            &envelope.namespace,
            &envelope.source_id,
            envelope.kind,
            envelope.sequence,
            envelope.rejection_generation,
            envelope.cursor.start.as_deref(),
        );
        seed.push((
            namespace
                .clone()
                .join("envelopes")
                .join(format!("{}.json", envelope.envelope_id)),
            write_contract(&envelope),
        ));
    }
    fyrr_reader::storage::test_store::seed_local_fixture(&config, seed).unwrap();
    let providers = root.join("measurement-providers");
    let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    copy_tree(&checkout.join("crates/fyrr/tests/providers"), &providers);
    let module = providers.join("fyrr_test_providers/incremental.py");
    let mut code = std::fs::read_to_string(&module).unwrap();
    code.push_str(r#"

# Diagnostic only: this module is an owned copy in the individual test fixture.
def _io_probe(self, data):
    if not data.get("_io_probe"):
        return _original_revision_probe(self, data)
    import threading
    ticks = []
    stop = threading.Event()
    def heartbeat():
        while not stop.is_set():
            ticks.append(time.perf_counter_ns())
            time.sleep(0.001)
    thread = threading.Thread(target=heartbeat)
    thread.start()
    time.sleep(0.02)
    control_start = time.perf_counter_ns()
    time.sleep(0.2)
    control_end = time.perf_counter_ns()
    marker = Path(data["_io_probe"])
    marker.with_suffix(".entered").touch()
    start = time.perf_counter_ns()
    self.context.lookup_revisions(["a"])
    end = time.perf_counter_ns()
    time.sleep(0.02)
    stop.set()
    thread.join()
    gaps = [b-a for a,b in zip(ticks,ticks[1:])]
    marker.write_text(json.dumps({"callback_ms":(end-start)/1e6,"max_python_thread_gap_ms":max(gaps,default=0)/1e6,"control_ticks":sum(control_start<=t<=control_end for t in ticks),"callback_ticks":sum(start<=t<=end for t in ticks)}))
_original_revision_probe = Incremental.catch_revision_failure
Incremental.catch_revision_failure = _io_probe
"#);
    std::fs::write(module, code).unwrap();
    let metrics = root.join("thread-progress.json");
    let entered = metrics.with_extension("entered");
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if !after
            && request.starts_with("GET ")
            && request.contains("/envelopes/")
            && entered.exists()
        {
            observed.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        false
    });
    let (endpoint, proxy) = corrections::proxy_at_boundary(
        "local-storage".into(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let proxied = corrections::proxied(&worker, &endpoint);
    incremental::input(
        &root,
        json!({"epoch":2,"permissions":[[]],"_io_probe":metrics}),
    );
    let output = coordination::worker_command(&proxied)
        .env("PYTHONPATH", &providers)
        .args([
            "writer",
            "sync",
            "incremental",
            "--kind",
            "permissions",
            "--full",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&std::fs::read(metrics).unwrap()).unwrap();
    println!(
        "worker-io-probe: metrics={value}, delayed_envelope_gets={}",
        reads.load(Ordering::SeqCst)
    );
    assert!(reads.load(Ordering::SeqCst) >= 64);
    assert!(value["control_ticks"].as_u64().unwrap() > 0);
    drop(proxy);
}
