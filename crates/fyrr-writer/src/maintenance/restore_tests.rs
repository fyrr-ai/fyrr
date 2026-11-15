use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct HookGuard;
impl Drop for HookGuard {
    fn drop(&mut self) {
        *RESTORE_TEST_GROUP.lock().unwrap() = None;
        *RESTORE_TEST_PUBLICATION.lock().unwrap() = None;
        *RESTORE_TEST_DRAIN.lock().unwrap() = None;
        *RESTORE_TEST_SCAN.lock().unwrap() = None;
    }
}

#[test]
fn restore_step_deadline_elapsed_arithmetic() {
    let config: Config = toml_config();
    let start = std::time::Instant::now()
        .checked_sub(config.writer.maintenance.step_max_duration.as_duration() * 2)
        .unwrap();
    assert_eq!(
        restore_deadline(&config, start),
        Err(WriterError::MaintenanceStepDeadline)
    );
    assert_eq!(restore_deadline(&config, std::time::Instant::now()), Ok(()));
    assert_eq!(
        RestoreCadence::new(&config).interval,
        std::time::Duration::from_secs(30)
    );
    let mut short = config;
    short.writer.maintenance.step_max_duration = "40s".parse().unwrap();
    assert_eq!(
        RestoreCadence::new(&short).interval,
        std::time::Duration::from_secs(20)
    );
}

fn toml_config() -> Config {
    // Use the repository's existing configuration loader, with no runtime I/O.
    fyrr_core::config::parse(
            "[namespace]\nname='restore-test'\n[storage]\nuri='s3://restore-test'\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n",
        )
        .unwrap()
        .config
}

#[test]
fn restore_future_can_move_between_runtime_threads() {
    fn requires_send(_: impl Send) {}
    let config = toml_config();
    requires_send(restore(&config, None));
}

#[tokio::test]
async fn restore_body_bound_precedes_native_materialization() {
    let batch = apply::marker_row(
        toml_config().embedding.dimensions,
        "source",
        "root",
        &apply::DocumentState::default(),
    )
    .unwrap();
    let schema = batch.schema();
    let dataset = Dataset::write(
        arrow_array::RecordBatchIterator::new([Ok(batch)], schema),
        &format!(
            "memory://restore-body-{}",
            Utc::now().timestamp_nanos_opt().unwrap()
        ),
        None,
    )
    .await
    .unwrap();
    let mut config = toml_config();
    config.writer.commit.group_max_bytes = "80KiB".parse().unwrap();
    let mut reads = 0;
    let result = restore_bodies(&config, &dataset, &[0; 40], &mut reads, None).await;
    assert_eq!(result.unwrap_err(), WriterError::EnvelopeBoundsInvalid);
    assert_eq!(reads, 0, "an oversized root must fail before any body take");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real CLI and RustFS fixture supplied by dense_path"]
async fn restore_drains_restarts_and_caps_attempts_on_real_s3() {
    let worker = std::path::PathBuf::from(std::env::var_os("FYRR_TEST_RESTORE_WORKER").unwrap());
    let config_path =
        std::path::PathBuf::from(std::env::var_os("FYRR_TEST_RESTORE_CONFIG").unwrap());
    let input = std::path::PathBuf::from(std::env::var_os("FYRR_TEST_RESTORE_INPUT").unwrap());
    let target = std::env::var("FYRR_TEST_RESTORE_TARGET").unwrap();
    let binary = std::env::var_os("FYRR_TEST_RESTORE_BINARY").unwrap();
    let config = fyrr_core::config::load(&config_path).unwrap().config;
    let attempts = Arc::new(AtomicUsize::new(0));
    let remaining = Arc::new(AtomicUsize::new(1));
    let observed = attempts.clone();
    let queue = remaining.clone();
    let config_probe = config_path.clone();
    let hook_input = input.clone();
    let hook_worker = worker.clone();
    let hook_binary = binary.clone();
    let hook: RestoreTestHook = Arc::new(move || {
        let input = &hook_input;
        let worker = &hook_worker;
        let binary = &hook_binary;
        if queue
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_err()
        {
            return;
        }
        let turn = observed.fetch_add(1, Ordering::SeqCst) + 1;
        let permission = serde_json::json!({"epoch":100+turn,"permissions":[[{"id":"restore-0","revision":format!("drain-{turn}"),"readers":[]}]]});
        std::fs::write(input, serde_json::to_vec(&permission).unwrap()).unwrap();
        // Observe the real serving head while the native suffix exists.
        let before = std::process::Command::new(binary)
            .arg("--config")
            .arg(&config_probe)
            .args(["writer", "status"])
            .output()
            .unwrap();
        assert!(
            before.status.success(),
            "{}",
            String::from_utf8_lossy(&before.stderr)
        );
        let status: serde_json::Value = serde_json::from_slice(&before.stdout).unwrap();
        let output = std::process::Command::new(binary)
            .arg("--config")
            .arg(worker)
            .args(["writer", "sync", "incremental", "--kind", "permissions"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let after = std::process::Command::new(binary)
            .arg("--config")
            .arg(&config_probe)
            .args(["writer", "status"])
            .output()
            .unwrap();
        assert!(after.status.success());
        let after: serde_json::Value = serde_json::from_slice(&after.stdout).unwrap();
        assert_eq!(
            status["content_sequence"], after["content_sequence"],
            "queuing authorization must not publish a partial restore"
        );
    });
    let _guard = HookGuard;
    grant_restore_root(&input, &worker, &config_path, &binary).await;
    let granted = pinned(&config, &lease_head(&config).await).await.unwrap();
    let granted_rows = restore_read(
        &config,
        &granted,
        "incremental",
        &["restore-0".into()],
        false,
    )
    .await
    .unwrap();
    assert!(
        granted_rows
            .iter()
            .any(|b| (0..b.num_rows()).any(|r| !apply::tokens(b, r).unwrap().is_empty())),
        "revoke a currently granted root"
    );
    match std::env::var("FYRR_TEST_RESTORE_REVIEW_CASE").as_deref() {
        Ok("cadence") => {
            let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed_drain = drained.clone();
            *RESTORE_TEST_DRAIN.lock().unwrap() = Some(Arc::new(move || {
                observed_drain.store(true, Ordering::SeqCst);
            }));
            let batches = Arc::new(AtomicUsize::new(0));
            let observed_batches = batches.clone();
            let observed_drain = drained.clone();
            *RESTORE_TEST_SCAN.lock().unwrap() = Some(Arc::new(move |cadence| {
                if observed_batches.fetch_add(1, Ordering::SeqCst) == 0 {
                    hook(); // Queue a real permission revoke inside the first projected scan.
                    // Advance the existing test boundary without depending on provider latency.
                    cadence.last = std::time::Instant::now()
                        .checked_sub(cadence.interval)
                        .unwrap();
                } else {
                    assert!(
                        observed_drain.load(Ordering::SeqCst),
                        "elapsed cadence must discover the revoke before processing another scan batch"
                    );
                }
            }));
            restore(&config, Some(&target)).await.unwrap();
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert!(batches.load(Ordering::SeqCst) > 1);
            let current = pinned(&config, &lease_head(&config).await).await.unwrap();
            let rows = restore_read(
                &config,
                &current,
                "incremental",
                &["restore-0".into()],
                false,
            )
            .await
            .unwrap();
            assert!(
                rows.iter()
                    .all(|b| (0..b.num_rows()).all(|r| apply::tokens(b, r).unwrap().is_empty()))
            );
            return;
        }
        Ok("a1") => {
            *RESTORE_TEST_PUBLICATION.lock().unwrap() = Some(hook);
            restore(&config, Some(&target)).await.unwrap();
            let head = lease_head(&config).await;
            let current = pinned(&config, &head).await.unwrap();
            let rows = restore_read(
                &config,
                &current,
                "incremental",
                &["restore-0".into()],
                false,
            )
            .await
            .unwrap();
            assert!(
                rows.iter()
                    .all(|b| (0..b.num_rows()).all(|r| apply::tokens(b, r).unwrap().is_empty())),
                "authorization arriving during publication must be applied before restore returns"
            );
            assert!(
                commit::pending(&config, &head)
                    .await
                    .unwrap()
                    .iter()
                    .all(|e| !e.kind.is_authorization())
            );
            return;
        }
        Ok("a2") => {
            remaining.store(8, Ordering::SeqCst);
            hook(); // Seed the first snapshot; every committed drain adds another envelope.
            *RESTORE_TEST_DRAIN.lock().unwrap() = Some(hook);
            let lease = LeaseGuard::acquire(&config).await.unwrap();
            let keepalive = lease.keepalive();
            let drained = restore_boundary(&config, &lease).await;
            keepalive.abort();
            let _ = keepalive.await;
            lease.release().await.unwrap();
            assert_eq!(drained, Err(WriterError::RestoreAuthorizationChanged));
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                2,
                "a drain must service its initial snapshot only"
            );
            assert_eq!(
                restore(&config, Some(&target)).await,
                Err(WriterError::RestoreAuthorizationChanged)
            );
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                8,
                "entry, retry boundaries and three attempts each service one snapshot"
            );
            return;
        }
        _ => {}
    }
    *RESTORE_TEST_GROUP.lock().unwrap() = Some(hook);
    let before = lease_head(&config).await;
    let result = restore(&config, Some(&target)).await.unwrap();
    assert_eq!(result["from_publication"], target);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    let after = lease_head(&config).await;
    assert_eq!(
        after.content.as_ref().unwrap().sequence,
        before.content.as_ref().unwrap().sequence + 2,
        "one authorization drain, one final restore publication"
    );
    let current = pinned(&config, &after).await.unwrap();
    let (store, root) = namespace_store(&config).unwrap();
    let final_manifest: ContentManifest = read_contract(
        store.as_ref(),
        &Path::from(format!(
            "{root}/{}",
            after.content.as_ref().unwrap().manifest
        )),
    )
    .await
    .unwrap();
    let previous: ContentManifest = read_contract(
        store.as_ref(),
        &Path::from(format!(
            "{root}/publications/{}.json",
            final_manifest.previous_publication_id.unwrap()
        )),
    )
    .await
    .unwrap();
    assert_eq!(
        previous.sequence,
        before.content.as_ref().unwrap().sequence + 1,
        "revocation publication precedes restored content"
    );
    let revoked = current
        .checkout_version(previous.lance_version)
        .await
        .unwrap();
    let revoked_rows = restore_read(
        &config,
        &revoked,
        "incremental",
        &["restore-0".into()],
        false,
    )
    .await
    .unwrap();
    assert!(
        revoked_rows
            .iter()
            .all(|b| (0..b.num_rows()).all(|r| apply::tokens(b, r).unwrap().is_empty())),
        "the publication before restore already denies the root"
    );
    let rows = restore_read(
        &config,
        &current,
        "incremental",
        &["restore-0".into()],
        false,
    )
    .await
    .unwrap();
    assert!(rows.iter().all(|batch| {
        (0..batch.num_rows()).all(|row| apply::tokens(batch, row).unwrap().is_empty())
    }));
    let permission = serde_json::json!({"epoch":200,"permissions":[[{"id":"restore-0","revision":"before-rollback","readers":[]}]]});
    std::fs::write(&input, serde_json::to_vec(&permission).unwrap()).unwrap();
    for (path, args) in [
        (
            &worker,
            vec!["writer", "sync", "incremental", "--kind", "permissions"],
        ),
        (&config_path, vec!["writer", "commit"]),
    ] {
        let output = std::process::Command::new(&binary)
            .arg("--config")
            .arg(path)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    remaining.store(3, Ordering::SeqCst);
    // This rollback target contains older permission revisions. Every first
    // group gets a new real permission envelope, invalidating the attempt.
    let old_sequence = lease_head(&config).await.content.unwrap().sequence;
    assert_eq!(
        restore(&config, None).await,
        Err(WriterError::RestoreAuthorizationChanged)
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        4,
        "exactly three further attempts"
    );
    let final_head = lease_head(&config).await;
    assert_eq!(
        final_head.content.as_ref().unwrap().sequence,
        old_sequence + 3,
        "only the three authorization publications, no partial rollback"
    );
    let current = pinned(&config, &final_head).await.unwrap();
    assert_eq!(
        current.latest_version_id().await.unwrap(),
        current.version().version,
        "authorization drains recovered every private suffix"
    );
    let mut tiny = config.clone();
    tiny.writer.commit.group_max_rows = 1;
    assert_eq!(
        restore_read(&tiny, &current, "incremental", &["restore-0".into()], false)
            .await
            .unwrap_err(),
        WriterError::EnvelopeBoundsInvalid,
        "a complete two-row root cannot fit a one-row group"
    );
}

async fn lease_head(config: &Config) -> ServingHead {
    fyrr_reader::storage::optional_head(config)
        .await
        .unwrap()
        .unwrap()
}

async fn grant_restore_root(
    input: &std::path::Path,
    worker: &std::path::Path,
    config: &std::path::Path,
    binary: &std::ffi::OsStr,
) {
    // Since 908112e, a document permission cannot override an explicit
    // section deny. Seed a new section statement before proving a revoke.
    // Restore also rewinds membership: refresh both authorization streams
    // before content, or commit_group correctly fences the pending upsert.
    let loaded = fyrr_core::config::load(config).unwrap().config;
    let head = lease_head(&loaded).await;
    let current = pinned(&loaded, &head).await.unwrap();
    let rows = restore_read(
        &loaded,
        &current,
        "incremental",
        &["restore-0".into()],
        false,
    )
    .await
    .unwrap();
    let prior = apply::state(&rows[0], 0).unwrap().revision;
    let epoch = format!("grant-{}", head.content.unwrap().sequence);
    let members: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::env::var_os("FYRR_TEST_RESTORE_MEMBERS").unwrap()).unwrap(),
    )
    .unwrap();
    let value = serde_json::json!({"epoch":epoch,"content":[[{"id":"restore-0","revision":epoch,"expected_prior_revision":prior,"texts":["fixture document 0","fixture document 1"],"section_readers":[["grp:group"],["grp:group"]]}]],"permissions":[[{"id":"restore-0","revision":"granted-before-retry","readers":["grp:group"]}]],"members":members});
    std::fs::write(input, serde_json::to_vec(&value).unwrap()).unwrap();
    for (path, args) in [
        (
            worker,
            vec![
                "writer",
                "sync",
                "incremental",
                "--kind",
                "permissions",
                "--kind",
                "members",
            ],
        ),
        (config, vec!["writer", "commit"]),
        (
            worker,
            vec!["writer", "sync", "incremental", "--kind", "content"],
        ),
        (config, vec!["writer", "commit"]),
    ] {
        let output = std::process::Command::new(binary)
            .arg("--config")
            .arg(path)
            .args(&args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "args={args:?} status={} stdout={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if path == worker && args.contains(&"content") {
            let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(
                report["stats"]["upserts"],
                serde_json::json!(1),
                "seed one new section statement"
            );
            assert_eq!(
                report["stats"]["quarantined"],
                serde_json::json!(0),
                "grant fixture must not be quarantined"
            );
        }
    }
}
