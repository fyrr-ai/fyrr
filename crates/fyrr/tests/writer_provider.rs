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

/// A failed worker as the service logged it: the service's clock at the
/// failure, and when the service retries the source.
struct Failure {
    at: chrono::DateTime<chrono::Utc>,
    retry_at: chrono::DateTime<chrono::Utc>,
}

/// The service's failed workers so far, in the order it logged them.
fn failures(log: &Path) -> Vec<Failure> {
    let time = |line: &str, key: &str| {
        let value = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix(key))
            .unwrap_or_else(|| panic!("no {key} in {line:?}"));
        chrono::DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&chrono::Utc)
    };
    std::fs::read_to_string(log)
        .unwrap()
        .lines()
        .filter(|line| line.contains("writer_service event=worker_failed "))
        .map(|line| Failure {
            at: time(line, "at="),
            retry_at: time(line, "retry_at="),
        })
        .collect()
}

/// Waits up to 120 s of real time until `count` workers have failed, and
/// returns the last of them.
fn wait_for_failures(log: &Path, count: usize) -> Failure {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        if let Some(failure) = failures(log).into_iter().nth(count - 1) {
            return failure;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "worker {count} never failed; service log:\n{}",
            std::fs::read_to_string(log).unwrap()
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

/// Moves the test clock to `time`, unless real time already passed it.
fn advance_to(time: chrono::DateTime<chrono::Utc>) {
    let step = (time - fyrr_core::clock::now())
        .to_std()
        .unwrap_or_default();
    fyrr_core::clock::advance(step).unwrap();
}

/// Checks that failed worker `count` schedules its retry exactly `delay`
/// seconds later, moves the test clock there, and returns the retry's failure,
/// which must not come before it.
fn retried_after(log: &Path, count: usize, failure: &Failure, delay: i64) -> Failure {
    assert_eq!(
        failure.retry_at - failure.at,
        chrono::TimeDelta::seconds(delay),
        "worker {count} retries after {delay} s"
    );
    advance_to(failure.retry_at);
    let retry = wait_for_failures(log, count + 1);
    assert!(
        retry.at >= failure.retry_at,
        "worker {} failed at {}, before its retry at {}",
        count + 1,
        retry.at,
        failure.retry_at
    );
    retry
}

/// The content cursor in the namespace's storage, once a worker has saved one.
fn saved_cursor(storage: &Path) -> Option<serde_json::Value> {
    let mut stack = vec![storage.to_owned()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.ends_with("cursors/fixture-content.json") {
                return Some(serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap());
            }
        }
    }
    None
}

/// Waits up to 120 s of real time until `done`, else fails naming `what`.
fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
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
    let fixture = Fixture::new("sentinel-failing", "heal-once");
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
    std::fs::write(&fixture.config, text).unwrap();
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
    // The first worker starts at once and fails. The service logs when it
    // retries each failure: 60 s later, then 120 s, then 240 s.
    let failed = wait_for_failures(&log, 1);
    let failed = retried_after(&log, 1, &failed, 60);
    // Another source publishes a new head while the failing one waits: that
    // does not make it due, so no worker fails before the retry the service
    // logged, which `retried_after` checks.
    let published = publications(&log);
    docs("r2");
    let output = fixture.run(&["writer", "sync", "other", "--full"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    wait_until("the other source never published", || {
        publications(&log) > published
    });
    std::thread::sleep(std::time::Duration::from_secs(3));
    assert_eq!(failures(&log).len(), 2, "a new head made the source due");
    let failed = retried_after(&log, 2, &failed, 120);
    assert_eq!(
        failed.retry_at - failed.at,
        chrono::TimeDelta::seconds(240),
        "worker 3 retries after 240 s"
    );
    // The source recovers for one worker only: the provider succeeds in the
    // run that removes this file, and fails in every other.
    fixture.sandbox.input("heal", "");
    advance_to(failed.retry_at);
    let storage = fixture.sandbox.output().join("storage");
    let cursor = || saved_cursor(&storage).unwrap_or_default();
    wait_until("the recovered source never ran", || {
        cursor()["cursor"] == "printed"
    });
    // The cursor moves with each page, before the worker records the read as
    // complete. A clock step past the lease's expiry in between would lose
    // the lease and fail the read, so the clock moves only after the release.
    wait_until("the recovered read never ended", || {
        let expires: chrono::DateTime<chrono::Utc> =
            serde_json::from_value(cursor()["lease"]["expires_at"].clone()).unwrap();
        expires <= fyrr_core::clock::now()
    });
    assert_eq!(failures(&log).len(), 3, "a recovered source failed");
    // The success reset the delay: when the source fails again after its
    // interval, the retry waits 60 s, not 480 s, and grows from there.
    fyrr_core::clock::advance(std::time::Duration::from_secs(11 * 60)).unwrap();
    let failed = wait_for_failures(&log, 4);
    let failed = retried_after(&log, 4, &failed, 60);
    assert_eq!(
        failed.retry_at - failed.at,
        chrono::TimeDelta::seconds(120),
        "worker 5 retries after 120 s"
    );
}
