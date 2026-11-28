//! Provider-boundary compatibility through the real worker executable and local storage.
#![cfg(feature = "writer")]

mod common;

use common::{Sandbox, embedding::Provider};
use serde_json::json;
use std::path::{Path, PathBuf};

struct Fixture {
    sandbox: Sandbox,
    config: PathBuf,
    _embedding: Provider,
}

impl Fixture {
    fn new(provider: &str, mode: &str) -> Self {
        let sandbox = Sandbox::new("writer-provider");
        let embedding = Provider::start("BAAI/bge-m3", false);
        let documents = sandbox.input(
            "documents.json",
            &json!({"documents":[{"text":"fixture"}]}).to_string(),
        );
        let config = sandbox.input("fyrr.toml", &format!(
            "[namespace]\nname='driver-fixture'\n[storage]\nuri='file://{}'\n[embedding]\nendpoint='{}'\napi_key={{env='FYRR_TEST_SECRET'}}\nlocal_cache='{}'\nspend={{limit_microusd=1000,max_call_microusd=1}}\n[identity]\nstdio_user='fixture'\nhashing_key={{env='FYRR_TEST_SECRET'}}\nencryption_key={{env='FYRR_TEST_ENCRYPTION_KEY'}}\n[writer]\nenvelope_max_bytes='16MiB'\n[[sources]]\nid='fixture'\ntype='{provider}'\npermissions='fixed'\nreaders=['grp:fixture']\n[sources.settings]\nmode='{mode}'\n[sources.scope]\nfixture='{}'\n",
            sandbox.output().join("storage").display(), embedding.endpoint,
            sandbox.output().join("embeddings").display(), documents.display()
        ));
        let fixture = Self {
            sandbox,
            config,
            _embedding: embedding,
        };
        let output = fixture.run(&["writer", "namespace", "create"]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        fixture
    }

    fn run(&self, args: &[&str]) -> std::process::Output {
        self.sandbox.configured(&self.config, args, &[])
    }
}

#[test]
#[ignore = "real worker and RustFS; scripts/ci"]
fn prepared_errors_preserve_batch_invalid_and_provider_failure_on_storage() {
    for (provider, mode, code) in [
        ("sentinel-review", "text-huge", "batch_invalid"),
        ("sentinel-failing", "raise", "provider_failure"),
    ] {
        let fixture = Fixture::new(provider, mode);
        let output = fixture.run(&["writer", "sync", "--kind", "content"]);
        assert_eq!(
            output.status.code(),
            Some(1),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let expected = format!(
            "{{\"kind\":\"content\",\"error\":\"{code}\",\"paid_calls\":0,\"provider_calls\":1,\"reserved_microusd\":1,\"source_id\":\"fixture\",\"failed\":true,\"kinds\":[{{\"kind\":\"content\",\"error\":\"{code}\",\"paid_calls\":0,\"provider_calls\":1,\"reserved_microusd\":1}}]}}\n"
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            expected,
            "{provider}/{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
#[ignore = "real committer lease and RustFS; scripts/ci"]
async fn committer_busy_remains_closed_on_storage() {
    let fixture = Fixture::new("sentinel-failing", "raise");
    let loaded = fyrr_core::config::load(Path::new(&fixture.config)).unwrap();
    let lease = fyrr_writer::lease::LeaseGuard::acquire(&loaded.config)
        .await
        .unwrap();
    let output = fixture.run(&["writer", "commit"]);
    lease.release().await.unwrap();
    assert_eq!(output.status.code(), Some(75));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).ends_with("error: committer_busy\n"));
}

#[tokio::test]
#[ignore = "real worker timeout, durable envelopes and cached vectors; scripts/ci"]
async fn prepared_timeout_names_flag_and_rerun_preserves_durable_work_on_storage() {
    use futures::TryStreamExt;
    use fyrr_core::connector::CursorState;
    use fyrr_reader::storage::{namespace_store, read_contract};
    use std::time::Duration;

    let fixture = Fixture::new("sentinel-review", "timeout-resume");
    let output = fixture.run(&[
        "writer",
        "sync",
        "--full",
        "--kind",
        "content",
        "--timeout",
        // The provider yields one batch and then sleeps for 60 s. The batch
        // must be durable before the timeout fires; a worker start on a
        // loaded machine has taken more than 5 s.
        "30s",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("timeout after 30000ms; increase --timeout"),
        "{stderr}"
    );
    assert!(output.stdout.is_empty());
    let config = fyrr_core::config::load(&fixture.config).unwrap().config;
    let (store, root) = namespace_store(&config).unwrap();
    let cursor: CursorState = read_contract(
        store.as_ref(),
        &root.clone().join("cursors").join("fixture-content.json"),
    )
    .await
    .unwrap();
    assert_eq!(cursor.cursor.as_deref(), Some("done"));
    let before: Vec<_> = store
        .list(Some(&root.clone().join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    assert!(
        !before.is_empty(),
        "the timed-out worker left durable prepared work"
    );
    let requests = fixture._embedding.state.requests.lock().unwrap().len();
    assert!(requests > 0);
    let remaining = (cursor.lease.unwrap().expires_at - fyrr_core::clock::now())
        .to_std()
        .unwrap_or_default();
    fyrr_core::clock::advance(remaining + Duration::from_millis(50)).unwrap();
    let resumed = fixture.run(&["writer", "sync", "--kind", "content"]);
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert_eq!(
        fixture._embedding.state.requests.lock().unwrap().len(),
        requests
    );
    let after: Vec<_> = store
        .list(Some(&root.clone().join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "resuming an advanced cursor does not duplicate envelopes"
    );
    // Force a replay from the beginning: existing vectors remain reusable.
    let text = std::fs::read_to_string(&fixture.config)
        .unwrap()
        .replace("mode='timeout-resume'", "mode='resume'");
    std::fs::write(&fixture.config, text).unwrap();
    let replayed = fixture.run(&["writer", "sync", "--full", "--kind", "content"]);
    assert!(
        replayed.status.success(),
        "{}",
        String::from_utf8_lossy(&replayed.stderr)
    );
    assert_eq!(
        fixture._embedding.state.requests.lock().unwrap().len(),
        requests
    );
    let committed = fixture.run(&["writer", "commit"]);
    assert!(
        committed.status.success(),
        "{}",
        String::from_utf8_lossy(&committed.stderr)
    );
    let verified = fixture.run(&["writer", "verify"]);
    assert!(
        verified.status.success(),
        "{}",
        String::from_utf8_lossy(&verified.stderr)
    );
}

/// The service's workers that failed so far.
fn failed_workers(log: &Path) -> usize {
    std::fs::read_to_string(log)
        .unwrap()
        .matches("writer_service event=worker_failed")
        .count()
}

/// Waits until `count` workers have failed, as the log shows it, and returns
/// the test clock when the test saw it. The service schedules the retry from
/// its own reading of the clock, taken before it logged the failure, so the
/// retry is due at most its delay after the returned time.
fn wait_for_failures(log: &Path, count: usize) -> chrono::DateTime<chrono::Utc> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while failed_workers(log) < count {
        assert!(
            std::time::Instant::now() < deadline,
            "worker {count} never failed"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    fyrr_core::clock::now()
}

/// Moves the test clock to `by` seconds after `since`. The clock is real time
/// plus the test's offset, so a step relative to an observed event does not
/// count the real time the test spent since then.
fn advance_to(since: chrono::DateTime<chrono::Utc>, by: i64) {
    let target = since + chrono::TimeDelta::seconds(by);
    let step = (target - fyrr_core::clock::now())
        .to_std()
        .unwrap_or_else(|_| {
            panic!(
                "real time alone passed {by} s after the observed event; the check would not hold"
            )
        });
    fyrr_core::clock::advance(step).unwrap();
}

/// Moves the test clock to `by` seconds after `since`, then gives the service
/// real time to act on it: only a check that nothing happened needs that.
fn advance_to_and_settle(since: chrono::DateTime<chrono::Utc>, by: i64) {
    advance_to(since, by);
    std::thread::sleep(std::time::Duration::from_secs(3));
}

/// The cursor's position in the namespace's storage, once a worker has saved one.
fn saved_cursor(storage: &Path) -> Option<String> {
    let mut stack = vec![storage.to_owned()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.ends_with("cursors/fixture-content.json") {
                let state: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                return state["cursor"].as_str().map(str::to_owned);
            }
        }
    }
    None
}

/// A running writer service, stopped when dropped, also when a check fails.
struct Service(std::process::Child);
impl Drop for Service {
    fn drop(&mut self) {
        // SIGTERM lets the service stop its workers.
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status();
        let _ = self.0.wait();
    }
}

/// The service's commits that published so far.
fn publications(log: &Path) -> usize {
    std::fs::read_to_string(log)
        .unwrap()
        .matches("writer_service event=published")
        .count()
}

#[test]
#[ignore = "real service, workers and local storage; scripts/ci"]
fn a_failing_source_is_retried_with_growing_delays_and_a_success_resets_them() {
    let fixture = Fixture::new("sentinel-failing", "raise");
    // A healthy source of the namespace, whose changes publish a new head.
    let docs = |revision: &str| {
        fixture.sandbox.input(
            "docs.json",
            &json!({"cursor": revision, "documents": [{
                "id": "doc", "title": "doc", "author": "author", "revision": revision,
                "text": format!("text {revision}"), "readers": ["grp:fixture"],
            }]})
            .to_string(),
        )
    };
    let docs_path = docs("r1");
    let text = std::fs::read_to_string(&fixture.config)
        .unwrap()
        .replace(
            "[sources.settings]",
            "[sources.intervals]\ncontent='10m'\nreconcile='10m'\n[sources.settings]",
        )
        .replacen(
            "[[sources]]",
            "[writer.commit]\ncoalescing_timer='1s'\n[[sources]]",
            1,
        )
        + &format!(
            "[[sources]]\nid='other'\ntype='sentinel-docs'\npermissions='fixed'\nreaders=['grp:fixture']\n[sources.settings]\n[sources.scope]\nfixture='{}'\n",
            docs_path.display()
        );
    std::fs::write(&fixture.config, &text).unwrap();
    // Publish and maintain the namespace first, so the service finds nothing
    // to publish at its start.
    for args in [
        &["writer", "sync", "other"][..],
        &["writer", "commit"][..],
        &["writer", "maintain"][..],
    ] {
        let output = fixture.run(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let log = fixture.sandbox.output().join("service.log");
    let _service = Service(
        fixture
            .sandbox
            .configured_command(&fixture.config, ["writer", "serve", "--jobs", "1"])
            .stdout(std::process::Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap(),
    );
    // The first worker starts at once and fails. The next waits 60 s of the
    // test clock, the one after 120 s, the one after that 240 s. Each step is
    // measured from the failure the test saw.
    let failed = wait_for_failures(&log, 1);
    advance_to_and_settle(failed, 40);
    assert_eq!(failed_workers(&log), 1, "retried before 60 s");
    advance_to(failed, 80);
    let failed = wait_for_failures(&log, 2);
    // Another source publishes a new head while the failing one waits: that
    // does not make it due.
    let published = publications(&log);
    docs("r2");
    let output = fixture.run(&["writer", "sync", "other", "--full"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while publications(&log) == published {
        assert!(
            std::time::Instant::now() < deadline,
            "the other source never published"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    std::thread::sleep(std::time::Duration::from_secs(3));
    assert_eq!(failed_workers(&log), 2, "a new head made the source due");
    advance_to_and_settle(failed, 80);
    assert_eq!(failed_workers(&log), 2, "retried before 120 s");
    advance_to(failed, 130);
    let failed = wait_for_failures(&log, 3);
    advance_to_and_settle(failed, 200);
    assert_eq!(failed_workers(&log), 3, "retried before 240 s");
    // The source recovers, and runs when its backoff has passed.
    let healed = text.replace("mode='raise'", "mode='print'");
    std::fs::write(&fixture.config, healed).unwrap();
    advance_to(failed, 260);
    let storage = fixture.sandbox.output().join("storage");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while saved_cursor(&storage).as_deref() != Some("printed") {
        assert!(
            std::time::Instant::now() < deadline,
            "the recovered source never ran"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    assert_eq!(failed_workers(&log), 3, "a recovered source failed");
    // The success reset the delay: when the source fails again after its
    // interval, the retry waits 60 s, not 480 s.
    std::fs::write(&fixture.config, &text).unwrap();
    fyrr_core::clock::advance(std::time::Duration::from_secs(11 * 60)).unwrap();
    let failed = wait_for_failures(&log, 4);
    advance_to_and_settle(failed, 40);
    assert_eq!(failed_workers(&log), 4, "retried before 60 s");
    advance_to(failed, 80);
    wait_for_failures(&log, 5);
}
