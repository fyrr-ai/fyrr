//! Real CLI crash boundaries against the local storage, using the existing proxy.
use super::*;
use crate::common::Sentinels;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

// Test failures must not leave a stopped holder or a blocked proxy behind.
pub(super) struct CommitProcess {
    child: Option<std::process::Child>,
    resume: Arc<AtomicBool>,
}
impl CommitProcess {
    fn spawn(config: &Path, resume: Arc<AtomicBool>) -> Self {
        Self::spawn_command(config, &["writer", "commit"], resume)
    }
    pub(super) fn spawn_command(config: &Path, args: &[&str], resume: Arc<AtomicBool>) -> Self {
        let mut command = host_command(config);
        command.args(args);
        Self::from_command(command, resume)
    }
    pub(super) fn from_command(mut command: Command, resume: Arc<AtomicBool>) -> Self {
        Self {
            child: Some(
                command
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap(),
            ),
            resume,
        }
    }
    pub(super) fn child(&mut self) -> &mut std::process::Child {
        self.child.as_mut().unwrap()
    }
    pub(super) fn finish(mut self) -> Output {
        self.child.take().unwrap().wait_with_output().unwrap()
    }
    pub(super) fn finish_bounded(mut self) -> Output {
        let child = self.child.take().unwrap();
        let pid = child.id().to_string();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let waiter = std::thread::spawn(move || {
            let _ = tx.send(child.wait_with_output());
        });
        let result = rx.recv_timeout(Duration::from_secs(60));
        if result.is_err() {
            let _ = Command::new("kill").args(["-KILL", &pid]).status();
        }
        waiter.join().unwrap();
        result.expect("command completion watchdog").unwrap()
    }
}
impl Drop for CommitProcess {
    fn drop(&mut self) {
        self.resume.store(true, Ordering::SeqCst);
        if let Some(child) = &mut self.child {
            let _ = Command::new("kill")
                .args(["-CONT", &child.id().to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
pub(super) fn host_command(config: &Path) -> Command {
    let mut command = Command::new(crate::common::binary());
    command
        .arg("--config")
        .arg(config)
        .current_dir(config.parent().unwrap());
    command
}

fn host_commit(config: &Path) -> Output {
    host_command(config)
        .args(["writer", "commit"])
        .output()
        .unwrap()
}

pub(super) fn worker_command(config: &Path) -> Command {
    let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut command = host_command(config);
    command
        .env("VIRTUAL_ENV", checkout.join("connectors/.venv"))
        .env("PYTHONPATH", checkout.join("crates/fyrr/tests/providers"));
    command
}
/// A `PYTHONPATH` for a worker that runs an earlier release of a connector:
/// `patch` is Python that undoes what changed, run against the installed
/// package. The embedded interpreter reads `sitecustomize` before it puts the
/// virtual environment on its path, so this does it first.
pub(super) fn earlier_release(config: &Path, patch: &str) -> String {
    let dir = config.parent().unwrap().join("earlier-release");
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(
        dir.join("sitecustomize.py"),
        format!(
            "import os, site, sysconfig\n\
             venv = os.environ['VIRTUAL_ENV']\n\
             site.addsitedir(sysconfig.get_path('purelib', vars={{'base': venv, 'platbase': venv}}))\n\
             {patch}"
        ),
    )
    .unwrap();
    let providers = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/providers");
    format!("{}:{}", dir.display(), providers.display())
}
pub(super) fn command(config: &Path, args: &[&str]) -> Value {
    let output = host_command(config).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("single JSON result")
}
pub(super) fn worker_sync(config: &Path, args: &[&str]) {
    let output = worker_command(config)
        .args(["writer", "sync"])
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "worker sync {args:?}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(super) fn committer_host(config: &Path, root: &Path) -> PathBuf {
    let host = root.join("committer-host");
    std::fs::create_dir(&host).unwrap();
    let separate = host.join("fyrr.toml");
    let text = std::fs::read_to_string(config)
        .unwrap()
        .replace(
            &root.join("sources").display().to_string(),
            &host.join("no-worker-files").display().to_string(),
        )
        .replace(
            &root.join("mirror").display().to_string(),
            &host.join("mirror").display().to_string(),
        );
    std::fs::write(&separate, text).unwrap();
    // Recovery uses the product default regardless of TOML whitespace or quotes.
    set_lease_ttl(&separate, None);
    separate
}

fn set_lease_ttl(config: &Path, ttl: Option<&str>) {
    #[derive(serde::Deserialize)]
    struct Document {
        writer: Option<Writer>,
    }
    #[derive(serde::Deserialize)]
    struct Writer {
        commit: Option<Commit>,
    }
    #[derive(serde::Deserialize)]
    struct Commit {
        lease_ttl: Option<toml::Spanned<String>>,
    }

    let mut text = std::fs::read_to_string(config).unwrap();
    let document: Document = toml::from_str(&text).unwrap();
    let span = document
        .writer
        .and_then(|writer| writer.commit)
        .and_then(|commit| commit.lease_ttl);
    if let Some(span) = span {
        let default = fyrr_core::config::CommitConfig::default()
            .lease_ttl
            .to_string();
        // Edit only the parsed value span: other fixtures deliberately replace
        // source settings in this file and depend on their original formatting.
        text.replace_range(span.span(), &format!("{:?}", ttl.unwrap_or(&default)));
        std::fs::write(config, text).unwrap();
    } else {
        assert!(
            ttl.is_none(),
            "a short-lease fixture already sets lease_ttl"
        );
    }
}

#[test]
fn recovery_lease_ttl_ignores_toml_formatting_and_preserves_source_settings() {
    let path = std::env::temp_dir().join(format!("fyrr-ttl-{}.toml", std::process::id()));
    for setting in ["lease_ttl = '3s'", "lease_ttl='3s'", "lease_ttl = \"3s\""] {
        let text = format!(
            "[writer.commit]\n{setting} # keep comment\n[[sources]]\ntype='incremental-test'\n"
        );
        std::fs::write(&path, text).unwrap();
        set_lease_ttl(&path, None);
        let changed = std::fs::read_to_string(&path).unwrap();
        assert!(changed.contains("# keep comment"));
        assert!(changed.contains("type='incremental-test'"));
        let document: toml::Table = changed.parse().unwrap();
        assert_eq!(
            document["writer"]["commit"]["lease_ttl"]
                .as_str()
                .unwrap()
                .parse::<fyrr_core::units::ConfigDuration>()
                .unwrap(),
            fyrr_core::config::CommitConfig::default().lease_ttl
        );
        set_lease_ttl(&path, Some("3s"));
        let document: toml::Table = std::fs::read_to_string(&path).unwrap().parse().unwrap();
        assert_eq!(
            document["writer"]["commit"]["lease_ttl"].as_str(),
            Some("3s")
        );
    }
    std::fs::remove_file(path).unwrap();
}

pub(super) async fn publication(config: &Path) -> (ServingHead, ContentManifest) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, root) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let head: ServingHead =
        fyrr_reader::storage::read_contract(store.as_ref(), &root.clone().join("head.json"))
            .await
            .unwrap();
    let manifest = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!(
            "{root}/{}",
            head.content.as_ref().unwrap().manifest
        )),
    )
    .await
    .unwrap();
    (head, manifest)
}

pub(super) async fn pinned_count(config: &Path) -> usize {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (head, _) = publication(config).await;
    fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        loaded
            .config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        head.content.unwrap().lance_version,
        None,
    )
    .await
    .unwrap()
    .count_rows(Some("source_id IN ('allowed', 'denied')".into()))
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "real CLI and owned RustFS; scripts/ci"]
async fn coordination_crash_boundaries_on_storage() {
    // Distinct storage boundaries, not a cross-product. Lost successful responses
    // are replaced with a terminal transport failure to avoid SDK retry timing.
    let cases = [
        ("lance-not-sent", "/_versions/", false, false),
        ("lance-applied-without-response", "/_versions/", true, false),
        ("manifest-not-sent", "/publications/", false, false),
        ("head-not-sent", "/head.json", false, false),
        ("policy-head-not-sent", "/head.json", false, false),
        ("head-response-lost", "/head.json", true, true),
    ];
    for (name, boundary, after, published) in cases {
        let (worker, root) = setup();
        success(&worker, &["writer", "namespace", "create"]);
        worker_sync(&worker, &["--full"]);
        let committer = committer_host(&worker, &root);
        let bulk = host_commit(&committer);
        assert!(
            bulk.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&bulk.stderr)
        );
        let (_, initial) = publication(&committer).await;
        assert_eq!(pinned_count(&committer).await, 1536);
        let policy_only = name == "policy-head-not-sent";
        if policy_only {
            for config in [&worker, &committer] {
                // A source's fixed readers are policy; the namespace's settings are not.
                let text = std::fs::read_to_string(config).unwrap().replace(
                    "permissions = \"fixed\"\nreaders = [\"grp:allow\"]",
                    "permissions = \"fixed\"\nreaders = [\"grp:allow\", \"grp:extra\"]",
                );
                std::fs::write(config, text).unwrap();
            }
        } else {
            std::fs::remove_file(root.join("sources/0.txt")).unwrap();
            worker_sync(&worker, &["allowed"]);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let (gate, hook) = corrections::RequestGate::rejecting(move |request, body, response| {
            let publication_put = boundary != "/head.json"
                || serde_json::from_slice::<Value>(body)
                    .ok()
                    .is_some_and(|head| head["content"]["sequence"] == 2);
            request.starts_with("PUT ")
                && request.contains(boundary)
                && publication_put
                && response == after
        });
        let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
        let (proxy, proxy_guard) =
            corrections::proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
        let gate = gate; // Release a blocked request before joining the proxy on unwind.
        let proxied = committer.parent().unwrap().join("fault.toml");
        std::fs::write(
            &proxied,
            std::fs::read_to_string(&committer)
                .unwrap()
                .replace(&endpoint, &proxy),
        )
        .unwrap();
        let mut process = CommitProcess::spawn(&proxied, Arc::new(AtomicBool::new(false)));
        gate.entered();
        process.child().kill().unwrap();
        let failed = process.finish_bounded();
        gate.release();
        assert!(!failed.status.success(), "boundary {name} must fail");
        stop.store(true, Ordering::SeqCst);
        drop(proxy_guard);
        let (_, at_failure) = publication(&committer).await;
        assert_eq!(at_failure.sequence, if published { 2 } else { 1 }, "{name}");
        assert_eq!(
            pinned_count(&committer).await,
            if published && !policy_only {
                1535
            } else {
                1536
            },
            "{name}"
        );
        // A killed process cannot release its lease. Advance the shared wall
        // clock past its persisted deadline; never rewrite the head or lease.
        let (held, _) = publication(&committer).await;
        let remaining = (held.lease.unwrap().expires_at - fyrr_core::clock::now())
            .to_std()
            .unwrap_or_default();
        // The crashed holder is gone; advance its persisted lease deadline.
        fyrr_core::clock::advance(remaining + Duration::from_millis(50)).unwrap();
        let retry = host_commit(&committer);
        assert!(
            retry.status.success(),
            "retry {name}: {}",
            String::from_utf8_lossy(&retry.stderr)
        );
        let receipt: Value = serde_json::from_slice(&retry.stdout).unwrap();
        let (_, final_manifest) = publication(&committer).await;
        assert_eq!(final_manifest.sequence, 2, "{name}: one publication only");
        assert_eq!(receipt["publication_id"], final_manifest.publication_id);
        assert_eq!(receipt["envelope_ids"], json!(final_manifest.envelope_ids));
        assert_eq!(
            pinned_count(&committer).await,
            if policy_only { 1536 } else { 1535 }
        );
        if policy_only {
            assert_eq!(final_manifest.lance_version, initial.lance_version);
            assert!(final_manifest.envelope_ids.is_empty());
        }
        let lineage = final_manifest.lineage;
        assert_eq!(lineage.base_version, initial.lance_version);
        if name == "manifest-not-sent"
            || name == "lance-applied-without-response"
            || name == "head-not-sent"
        {
            assert!(
                lineage.recovery_version.is_some(),
                "{name}: recovery used Restore"
            );
        }
        let repeated = host_commit(&committer);
        assert!(repeated.status.success(), "{}", report(&repeated));
        assert_eq!(
            serde_json::from_slice::<Value>(&repeated.stdout).unwrap(),
            receipt
        );
        if published {
            let id = receipt["envelope_ids"][0].as_str().unwrap();
            std::fs::remove_file(root.join("sources/1.txt")).unwrap();
            worker_sync(&worker, &["allowed"]);
            assert!(host_commit(&committer).status.success());
            let recovered = command(&committer, &["writer", "commit", "--receipt", id]);
            assert_eq!(recovered, receipt);
        }
        success(
            &worker,
            &["reader", "search", "fixture query", "--limit", "2"],
        );
    }
}

#[tokio::test]
#[ignore = "real CLI and owned RustFS; scripts/ci"]
async fn coordination_busy_and_expired_holder_on_storage() {
    for pause_after in [false, true] {
        let (worker, root) = setup();
        let text = std::fs::read_to_string(&worker).unwrap();
        std::fs::write(
            &worker,
            format!("{text}\n[writer.commit]\nlease_ttl = '3s'\n"),
        )
        .unwrap();
        // Prepare data with the normal lease; only the stalled holder uses 3s.
        set_lease_ttl(&worker, None);
        success(&worker, &["writer", "namespace", "create"]);
        worker_sync(&worker, &["--full"]);
        let committer = committer_host(&worker, &root);
        assert!(host_commit(&committer).status.success());
        std::fs::remove_file(root.join("sources/0.txt")).unwrap();
        worker_sync(&worker, &["allowed"]);
        let stop = Arc::new(AtomicBool::new(false));
        let (gate, hook) = corrections::RequestGate::new(move |request, _, after| {
            after == pause_after && request.starts_with("PUT ") && request.contains("/_versions/")
        });
        let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
        let (proxy, proxy_guard) =
            corrections::proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
        let gate = gate;
        let slow = committer.parent().unwrap().join("slow.toml");
        std::fs::write(
            &slow,
            std::fs::read_to_string(&committer)
                .unwrap()
                .replace(&endpoint, &proxy),
        )
        .unwrap();
        set_lease_ttl(&slow, Some("3s"));
        let mut old = CommitProcess::spawn(&slow, Arc::new(AtomicBool::new(false)));
        gate.entered();
        let pid = old.child().id().to_string();
        assert!(
            Command::new("kill")
                .args(["-STOP", &pid])
                .status()
                .unwrap()
                .success()
        );
        let busy_started = Instant::now();
        let busy = host_commit(&committer);
        assert_eq!(busy.status.code(), Some(75));
        // Half the holder's 3 s lease: a committer that waits on the live
        // lease before it gives up exceeds it, and process start has 1.5 s.
        assert!(busy_started.elapsed() < Duration::from_millis(1500));
        assert!(String::from_utf8_lossy(&busy.stderr).contains("writer_busy holder=holder-"));
        let (held, _) = publication(&committer).await;
        let remaining = (held.lease.unwrap().expires_at - fyrr_core::clock::now())
            .to_std()
            .unwrap_or_default();
        // The stopped holder cannot renew; advance its persisted lease deadline.
        fyrr_core::clock::advance(remaining + Duration::from_millis(50)).unwrap();
        let takeover = host_commit(&committer);
        assert!(takeover.status.success(), "{}", report(&takeover));
        let (new_head, manifest) = publication(&committer).await;
        assert_eq!(manifest.lineage.recovery_version.is_some(), pause_after);
        gate.release();
        assert!(
            Command::new("kill")
                .args(["-CONT", &pid])
                .status()
                .unwrap()
                .success()
        );
        let late = old.finish_bounded();
        assert!(!late.status.success(), "expired holder must be fenced");
        let error = String::from_utf8_lossy(&late.stderr);
        assert!(
            error.contains("lease_lost") || error.contains("foreign_lineage"),
            "expired holder failure: {error}"
        );
        let (final_head, final_manifest) = publication(&committer).await;
        assert_eq!(final_head.content, new_head.content);
        assert_eq!(final_manifest, manifest);
        assert_eq!(pinned_count(&committer).await, 1535);
        // A native commit that finishes after takeover may leave another orphan,
        // but never changes the head. The next holder must recover rather than block.
        let retry = host_commit(&committer);
        assert!(retry.status.success(), "{}", report(&retry));
        let (recovered_head, recovered_manifest) = publication(&committer).await;
        assert_eq!(pinned_count(&committer).await, 1535);
        if recovered_head.content != new_head.content {
            assert_eq!(recovered_manifest.sequence, manifest.sequence + 1);
            assert!(recovered_manifest.envelope_ids.is_empty());
            assert!(recovered_manifest.lineage.recovery_version.is_some());
        }
        assert!(host_commit(&committer).status.success());
        assert_eq!(
            publication(&committer).await.0.content,
            recovered_head.content
        );
        stop.store(true, Ordering::SeqCst);
        drop(proxy_guard);
    }
}

#[tokio::test]
#[ignore = "real S3 conditional publication; scripts/ci"]
async fn coordination_stale_publish_preserves_authoritative_head_on_real_s3() {
    coordination_stale_publish_preserves_authoritative_head(Backend::S3).await;
}

#[tokio::test]
#[ignore = "real local conditional publication; scripts/ci"]
async fn coordination_stale_publish_preserves_authoritative_head_on_local_storage() {
    coordination_stale_publish_preserves_authoritative_head(Backend::Local).await;
}

async fn coordination_stale_publish_preserves_authoritative_head(backend: Backend) {
    use fyrr_writer::lease::LeaseGuard;
    let (config, root) = setup_on(backend);
    success(&config, &["writer", "namespace", "create"]);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns.join("head.json");
    // On S3 a proxy also records the conditional write's HTTP status.
    let statuses = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (guard_config, _proxy_guard) = match backend {
        Backend::Local => (loaded.config.clone(), None),
        Backend::S3 => {
            let target = format!(
                "PUT /{}/{} HTTP/1.1\r\n",
                std::env::var("FYRR_TEST_S3_BUCKET").unwrap(),
                path.as_ref().replace('(', "%28").replace(')', "%29")
            );
            let observed = statuses.clone();
            let transform = Arc::new(move |request: &str, response: &mut Vec<u8>| {
                if request == target {
                    observed.lock().unwrap().push(
                        String::from_utf8_lossy(response)
                            .lines()
                            .next()
                            .unwrap()
                            .to_owned(),
                    );
                }
            });
            let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
            let (proxy, proxy_guard) = corrections::proxy_transform(
                endpoint.clone(),
                Arc::new(AtomicBool::new(false)),
                transform,
                None,
            );
            let proxied = root.join("stale-publish.toml");
            std::fs::write(
                &proxied,
                std::fs::read_to_string(&config)
                    .unwrap()
                    .replace(&endpoint, &proxy),
            )
            .unwrap();
            (
                fyrr_core::config::load(&proxied).unwrap().config,
                Some(proxy_guard),
            )
        }
    };
    let guard = LeaseGuard::acquire(&guard_config).await.unwrap();
    let saved = guard.head().await.unwrap();
    let original = store.head(&path).await.unwrap().e_tag;
    let mut successor = saved.clone();
    let lease = successor.lease.as_mut().unwrap();
    lease.holder = "holder-00000000000000000000000000000000".into();
    lease.epoch += 1;
    let bytes = write_contract(&successor);
    parse_contract::<ServingHead>(&bytes).unwrap();
    store.put(&path, bytes.clone().into()).await.unwrap();
    let etag = store.head(&path).await.unwrap().e_tag;
    assert_ne!(etag, original);
    statuses.lock().unwrap().clear();
    let result = tokio::time::timeout(Duration::from_secs(20), guard.publish(saved))
        .await
        .unwrap();
    let authoritative = store.get(&path).await.unwrap();
    assert_eq!(
        result.map_err(fyrr_writer::WriterError::as_str),
        Err("head_cas_failed")
    );
    assert_eq!(authoritative.meta.e_tag, etag);
    assert_eq!(
        authoritative.bytes().await.unwrap().as_ref(),
        bytes.as_slice()
    );
    assert_eq!(
        guard
            .head()
            .await
            .err()
            .map(fyrr_writer::WriterError::as_str),
        Some("lease_lost")
    );
    if backend == Backend::S3 {
        assert_eq!(
            *statuses.lock().unwrap(),
            ["HTTP/1.1 412 Precondition Failed"]
        );
    }
}

#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn coordination_live_holder_is_busy_then_fenced_after_expiry_on_local_storage() {
    coordination_live_holder_is_busy_then_fenced_after_expiry(Backend::Local).await;
}

/// A second committer exits busy at once while a holder's lease is live, takes
/// over once it expires, and the expired holder can neither publish nor
/// release over the new holder's head.
async fn coordination_live_holder_is_busy_then_fenced_after_expiry(backend: Backend) {
    use fyrr_writer::lease::LeaseGuard;
    let (config, _root) = setup_on(backend);
    success(&config, &["writer", "namespace", "create"]);
    worker_sync(&config, &["allowed"]);
    // Shorten only the contention phase: fixture preparation retains the
    // normal sync lease even on a busy build host.
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!("{text}\n[writer.commit]\nlease_ttl = '15s'\n"),
    )
    .unwrap();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let path = ns.join("head.json");
    // Held without keepalive, so it expires after its TTL.
    let holder = LeaseGuard::acquire(&loaded.config).await.unwrap();
    let saved = holder.head().await.unwrap();
    let started = Instant::now();
    let busy = host_commit(&config);
    assert_eq!(busy.status.code(), Some(75));
    // A third of the 15 s lease: busy at once, not after waiting on it.
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(String::from_utf8_lossy(&busy.stderr).contains("writer_busy holder=holder-"));
    let held = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(parse_contract::<ServingHead>(&held).unwrap(), saved);

    let remaining = (saved.lease.as_ref().unwrap().expires_at - fyrr_core::clock::now())
        .to_std()
        .unwrap_or_default();
    // This holder has no keepalive; advance its persisted lease deadline.
    fyrr_core::clock::advance(remaining + Duration::from_millis(50)).unwrap();
    let takeover = host_commit(&config);
    assert!(takeover.status.success(), "{}", report(&takeover));
    let published = store.get(&path).await.unwrap();
    let (tag, bytes) = (
        published.meta.e_tag.clone(),
        published.bytes().await.unwrap(),
    );
    let head: ServingHead = parse_contract(&bytes).unwrap();
    assert!(head.content.is_some());
    assert!(head.lease.unwrap().epoch > saved.lease.as_ref().unwrap().epoch);

    assert_eq!(
        holder
            .publish(saved)
            .await
            .map_err(fyrr_writer::WriterError::as_str),
        Err("lease_lost")
    );
    assert_eq!(
        holder
            .release()
            .await
            .map_err(fyrr_writer::WriterError::as_str),
        Err("lease_lost")
    );
    let after = store.get(&path).await.unwrap();
    assert_eq!(after.meta.e_tag, tag);
    assert_eq!(after.bytes().await.unwrap(), bytes);
    assert_eq!(pinned_count(&config).await, 768);
}

#[tokio::test]
#[ignore = "real native commit and S3 publication fence; scripts/ci"]
async fn coordination_foreign_native_commit_cannot_publish_head_on_real_s3() {
    let (worker, root) = setup();
    success(&worker, &["writer", "namespace", "create"]);
    worker_sync(&worker, &["--full"]);
    let committer = committer_host(&worker, &root);
    command(&committer, &["writer", "commit"]);
    let before = publication(&committer).await.0;
    std::fs::remove_file(root.join("sources/0.txt")).unwrap();
    worker_sync(&worker, &["allowed"]);
    let loaded = fyrr_core::config::load(&committer).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let bucket = std::env::var("FYRR_TEST_S3_BUCKET").unwrap();
    let encoded_ns = ns.as_ref().replace('(', "%28").replace(')', "%29");
    let prefix = format!("PUT /{bucket}/{encoded_ns}/publications/");
    let head_request = format!("PUT /{bucket}/{encoded_ns}/head.json HTTP/1.1\r\n");
    let (gate, pause) = corrections::RequestGate::new(move |request, _, after| {
        after && request.starts_with(&prefix)
    });
    let heads = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = heads.clone();
    let hook = Arc::new(move |request: &str, body: &[u8], after: bool| {
        if after && request == head_request {
            observed
                .lock()
                .unwrap()
                .push(parse_contract::<ServingHead>(body).unwrap().content);
        }
        pause(request, body, after)
    });
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, _proxy_guard) =
        corrections::proxy_at_boundary(endpoint.clone(), Arc::new(AtomicBool::new(false)), hook);
    let gate = gate;
    let proxied = root.join("foreign-lineage.toml");
    std::fs::write(
        &proxied,
        std::fs::read_to_string(&committer)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let process = CommitProcess::spawn(&proxied, Arc::new(AtomicBool::new(false)));
    let pending: ContentManifest = parse_contract(&gate.entered()).unwrap();
    let stored: ContentManifest = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &ns.clone()
            .join("publications")
            .join(format!("{}.json", pending.publication_id)),
    )
    .await
    .unwrap();
    assert_eq!(stored, pending);
    let held = publication(&committer).await.0;
    assert_eq!(held.content, before.content);
    assert!(held.lease.as_ref().unwrap().expires_at > fyrr_core::clock::now());
    let mut native = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        loaded
            .config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        pending.lance_version,
        None,
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(15), native.restore())
        .await
        .unwrap()
        .unwrap();
    assert!(native.latest_version_id().await.unwrap() > pending.lance_version);
    let still_held = publication(&committer).await.0;
    assert_eq!(still_held.content, before.content);
    let lease = still_held.lease.as_ref().unwrap();
    assert_eq!(lease.holder, held.lease.as_ref().unwrap().holder);
    assert_eq!(lease.epoch, held.lease.as_ref().unwrap().epoch);
    assert!(lease.expires_at > fyrr_core::clock::now());
    gate.release();
    let output = process.finish_bounded();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "foreign native commit must prevent publication"
    );
    assert!(stderr.contains("foreign_lineage"), "{stderr}");
    assert!(!stderr.contains("lease_lost"), "{stderr}");
    assert_eq!(publication(&committer).await.0.content, before.content);
    assert!(
        heads
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .all(|head| head.publication_id != pending.publication_id)
    );
    let fetched = super::tools::body(
        super::tools::Mcp::open(&committer)
            .call("fetch", json!({"source_id":"allowed","root_id":"0.txt"})),
    );
    assert_eq!(fetched["documents"].as_array().unwrap().len(), 1);
    assert!(fetched.to_string().contains("fixture document 0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 conditional writes, pagination and visibility; scripts/ci"]
async fn s3_conditional_writes_and_paginated_listing_are_visible() {
    use futures::{StreamExt, TryStreamExt};
    use object_store::{PutMode, PutOptions, UpdateVersion};
    let (config, _root) = setup_on(Backend::S3);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let key = namespace.clone().join("cas/head.json");
    let create = |body: &'static [u8]| {
        store.put_opts(
            &key,
            body.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
    };
    let (first, second) = futures::future::join(create(b"first"), create(b"second")).await;
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let loser = if first.is_ok() { &second } else { &first };
    assert!(matches!(
        loser,
        Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. })
    ));
    let current = store.head(&key).await.unwrap();
    let version = UpdateVersion {
        e_tag: current.e_tag,
        version: current.version,
    };
    let update = |body: &'static [u8]| {
        store.put_opts(
            &key,
            body.into(),
            PutOptions {
                mode: PutMode::Update(version.clone()),
                ..Default::default()
            },
        )
    };
    let (first, second) = futures::future::join(update(b"new-first"), update(b"new-second")).await;
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let loser = if first.is_ok() { &second } else { &first };
    assert!(matches!(
        loser,
        Err(object_store::Error::Precondition { .. })
    ));
    let expected: &[u8] = if first.is_ok() {
        b"new-first"
    } else {
        b"new-second"
    };
    assert_eq!(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        expected
    );

    let prefix = namespace.join("listing");
    futures::stream::iter(0..1005)
        .map(|index| {
            let store = store.clone();
            let path = prefix.clone().join(format!("item-{index:04}"));
            async move { store.put(&path, vec![(index % 256) as u8].into()).await }
        })
        .buffer_unordered(16)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let pages = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = pages.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if !after
            && request.starts_with("GET ")
            && request.contains("list-type=2")
            && request.contains("listing")
        {
            observed
                .lock()
                .unwrap()
                .push(request.contains("continuation-token="));
        }
        false
    });
    let endpoint = loaded.config.storage.endpoint.clone().unwrap();
    let (proxy, guard) =
        corrections::proxy_at_boundary(endpoint.clone(), Arc::new(AtomicBool::new(false)), hook);
    let observed_config = config.with_file_name("listing-observed.toml");
    std::fs::write(
        &observed_config,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let observed_config = fyrr_core::config::load(&observed_config).unwrap();
    let (observed, _) = fyrr_reader::storage::namespace_store(&observed_config.config).unwrap();
    let objects: Vec<_> = observed.list(Some(&prefix)).try_collect().await.unwrap();
    assert_eq!(
        objects.len(),
        1005,
        "a fresh listing must see every completed PUT"
    );
    let keys: std::collections::BTreeSet<_> = objects
        .iter()
        .map(|object| object.location.to_string())
        .collect();
    assert_eq!(
        keys.len(),
        1005,
        "continuation pages must neither repeat nor lose objects"
    );
    assert!(objects.iter().all(|object| object.e_tag.is_some()));
    assert_eq!(
        *pages.lock().unwrap(),
        [false, true],
        "actual S3 continuation requests"
    );
    for index in [0, 1004] {
        let path = prefix.clone().join(format!("item-{index:04}"));
        assert_eq!(
            store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            &[(index % 256) as u8]
        );
    }
    drop(guard);
}

/// Every object a create, sync and commit write is in the object-store
/// diagnostics log, dataset files and control objects alike. With
/// `FYRR_TEST_S3_KMS_KEY_ID` (a key ARN) set, each also answers a HEAD with that
/// key. The RustFS fixture has no KMS, so it runs the coverage half only; the
/// encryption half runs on real S3 under #108.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3; the key assertions need FYRR_TEST_S3_KMS_KEY_ID; scripts/ci"]
#[allow(clippy::print_stderr)]
async fn s3_every_written_object_is_logged_and_carries_the_kms_key() {
    use futures::TryStreamExt;
    use object_store::{aws::AmazonS3Builder, signer::Signer};
    let key = std::env::var("FYRR_TEST_S3_KMS_KEY_ID").ok();
    let (config, root) = setup_on(Backend::S3);
    if let Some(key) = &key {
        let text = std::fs::read_to_string(&config).unwrap();
        let text = text.replacen(
            "\n[embedding]",
            &format!("\nkms_key_id = \"{key}\"\n[embedding]"),
            1,
        );
        std::fs::write(&config, text).unwrap();
    }
    let log = root.join("object-store.jsonl");
    let steps: [&[&str]; 3] = [
        &["writer", "namespace", "create"],
        &["writer", "sync"],
        &["writer", "commit"],
    ];
    for args in steps {
        let output = worker_command(&config)
            .env("FYRR_OBJECT_STORE_DIAGNOSTICS", &log)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}: {}", report(&output));
    }
    let written: std::collections::HashSet<String> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|record| {
            record["error_code"] == "ok"
                && ["PUT", "MULTIPART_CREATE", "COPY", "RENAME"]
                    .contains(&record["method"].as_str().unwrap())
        })
        .map(|record| record["key_sha256"].as_str().unwrap().to_owned())
        .collect();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let objects: Vec<_> = store.list(Some(&namespace)).try_collect().await.unwrap();
    for suffix in ["/head.json", ".lance", ".manifest"] {
        assert!(
            objects
                .iter()
                .any(|object| object.location.as_ref().ends_with(suffix)),
            "no {suffix} object: the run did not publish"
        );
    }
    for object in &objects {
        let hash = fyrr_core::limits::digest(object.location.as_ref().as_bytes());
        assert!(
            written.contains(hash.trim_start_matches("sha256:")),
            "{} is in no write record of the diagnostics log",
            object.location
        );
    }
    let Some(key) = key else {
        eprintln!("FYRR_TEST_S3_KMS_KEY_ID is not set: coverage checked, key not asserted");
        return;
    };
    let Some(fyrr_core::config::StorageBackend::S3 { bucket, .. }) =
        loaded.config.storage.backend()
    else {
        unreachable!("the fixture is S3");
    };
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    for (name, value) in loaded
        .config
        .storage
        .object_store_options(&fyrr_core::config::ProcessEnvironment)
        .unwrap()
        .into_map()
    {
        builder = builder.with_config(name.parse().unwrap(), value);
    }
    let s3 = builder.build().unwrap();
    let client = reqwest::Client::new();
    for object in &objects {
        let url = s3
            .signed_url(
                reqwest::Method::HEAD,
                &object.location,
                Duration::from_secs(300),
            )
            .await
            .unwrap();
        let response = client.head(url).send().await.unwrap();
        assert!(response.status().is_success(), "HEAD {}", object.location);
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .map(|value| value.to_str().unwrap().to_owned())
        };
        assert_eq!(
            header("x-amz-server-side-encryption").as_deref(),
            Some("aws:kms"),
            "{}",
            object.location
        );
        assert_eq!(
            header("x-amz-server-side-encryption-aws-kms-key-id").as_deref(),
            Some(key.as_str()),
            "{}",
            object.location
        );
    }
}

#[test]
#[ignore = "real CLI and RustFS; scripts/ci"]
fn coordination_status_bounded_backlog_on_storage() {
    let (worker, _, root) = super::incremental::fixture();
    let mut sentinels = Sentinels::new();
    let marker = sentinels.mark("status-private-cursor");
    let result_path = root.join("status-counts.json");
    let output = Command::new(
        std::env::var_os("FYRR_WRITER_TEST_BINARY").expect("writer libtest built by scripts/ci"),
    )
    .args([
        "envelopes::tests::status_bounded_backlog_on_storage",
        "--exact",
        "--ignored",
        "--nocapture",
    ])
    .env("FYRR_TEST_STATUS_CONFIG", &worker)
    .env("FYRR_TEST_STATUS_PRIVATE_CURSOR", &marker)
    .env("FYRR_TEST_STATUS_RESULT", &result_path)
    .output()
    .unwrap();
    for log in [&output.stdout, &output.stderr] {
        assert!(
            sentinels.found_in(&String::from_utf8_lossy(log)).is_empty(),
            "status proof leaked a private cursor"
        );
    }
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    let expected_bytes = std::fs::read(&result_path).unwrap();
    assert!(
        sentinels
            .found_in(&String::from_utf8_lossy(&expected_bytes))
            .is_empty()
    );
    let expected: Value = serde_json::from_slice(&expected_bytes).unwrap();
    let output = host_command(&worker)
        .args(["writer", "status"])
        .output()
        .unwrap();
    for log in [&output.stdout, &output.stderr] {
        assert!(
            sentinels.found_in(&String::from_utf8_lossy(log)).is_empty(),
            "writer status leaked a private cursor"
        );
    }
    assert!(
        output.status.success(),
        "{}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
    for field in [
        "content_sequence",
        "envelopes",
        "pending_envelopes",
        "quarantine",
    ] {
        assert_eq!(actual[field], expected[field], "status field {field}");
    }
}
