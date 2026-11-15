//! Optional scheduling; all durable state remains in S3 and the published head.
use crate::worker::{Job, Running};
use crate::{Result, WriterError};
use fyrr_core::config::{Config, DirectoryProvider, LoadedConfig};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

/// Only fully processed, authenticated IdP requests supply endpoint health proof.
pub(crate) async fn attest_directory(
    config: &Config,
    applied_sync: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<bool> {
    let Some(applied_at) = applied_sync else {
        return Ok(false);
    };
    let now = chrono::Utc::now();
    if config.identity.directory.provider != DirectoryProvider::Scim
        || applied_at > now
        || (now - applied_at)
            .to_std()
            .map_err(|_| WriterError::DirectoryHealthInvalid)?
            > config.identity.directory.sync_interval.as_duration()
    {
        return Err(WriterError::DirectoryHealthInvalid);
    }
    crate::lease::LeaseGuard::with(config, |lease| async move {
        async {
            let mut head = lease.head().await?;
            if head.freshness.directory.is_some_and(|v| v >= applied_at) {
                return Ok(false);
            }
            head.updated_at = chrono::Utc::now();
            if (head.updated_at - applied_at)
                .to_std()
                .map_err(|_| WriterError::DirectoryHealthInvalid)?
                > config.identity.directory.sync_interval.as_duration()
            {
                return Err(WriterError::DirectoryHealthInvalid);
            }
            head.freshness.directory = Some(applied_at);
            lease.publish(head).await?;
            Ok(true)
        }
        .await
    })
    .await
}

/// Workers have their existing independent process deadlines even during commits.
#[allow(clippy::print_stderr)]
pub async fn run(
    loaded: &LoadedConfig,
    path: &Path,
    jobs: usize,
    timeout: Duration,
) -> Result<serde_json::Value> {
    if jobs == 0 || timeout.is_zero() {
        return Err(WriterError::ConfigInvalid);
    }
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| WriterError::SignalUnavailable)?;
    let stop = Arc::new(AtomicBool::new(false));
    let requested = stop.clone();
    let mut shutdown = tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
        requested.store(true, Ordering::Release);
    });
    let admin = crate::admin::start(&loaded.config, stop.clone()).await?;
    let stopped = stop.clone();
    let config = loaded.config.clone();
    let path = path.to_owned();
    let supervisor = tokio::task::spawn_blocking(move || {
        let mut due = vec![Instant::now(); config.sources.len()];
        let mut running = Vec::<(usize, Running)>::new();
        while !stopped.load(Ordering::Relaxed) {
            let mut waiting = Vec::new();
            for (i, mut worker) in running {
                match worker.poll() {
                    Ok(None) => waiting.push((i, worker)),
                    result => {
                        if !matches!(result, Ok(Some(ref outcome)) if outcome.failure().is_none()) {
                            eprintln!("writer_service event=worker_failed");
                        }
                        let intervals = &config.sources[i].intervals;
                        let interval = [
                            Some(intervals.content),
                            intervals.permissions,
                            intervals.members,
                            Some(intervals.reconcile),
                        ]
                        .into_iter()
                        .flatten()
                        .map(|v| v.as_duration())
                        .min()
                        .unwrap_or(Duration::from_secs(60));
                        // Cursor timestamps, not this retry clock, decide whether a kind is due.
                        due[i] = Instant::now() + interval.min(Duration::from_secs(60));
                    }
                }
            }
            running = waiting;
            for (i, source) in config.sources.iter().enumerate() {
                if running.len() >= jobs {
                    break;
                }
                if due[i] > Instant::now() || running.iter().any(|(n, _)| *n == i) {
                    continue;
                }
                let job = Job::Sync {
                    config: path.clone(),
                    source: source.id.clone(),
                    kinds: Vec::new(),
                    prepare: true,
                    embedding_cap: config
                        .embedding
                        .spend
                        .as_ref()
                        .map(|spend| spend.limit_microusd),
                    full: false,
                    due: true,
                };
                match Running::spawn_with_memory(
                    &job,
                    timeout,
                    config.writer.worker_memory_max_bytes.as_u64(),
                ) {
                    Ok(worker) => running.push((i, worker)),
                    Err(_) => {
                        due[i] = Instant::now() + Duration::from_secs(1);
                        eprintln!("writer_service event=worker_failed");
                    }
                }
            }
            std::thread::sleep(crate::worker::POLL);
        }
        // Running::drop kills and reaps only supervisor-owned worker groups.
        drop(running);
    });
    let mut timer =
        tokio::time::interval(loaded.config.writer.commit.coalescing_timer.as_duration());
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer.tick().await; // First publication waits for the configured coalescing period.
    let mut directory_timer =
        tokio::time::interval(loaded.config.identity.directory.sync_interval.as_duration());
    directory_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = directory_timer.tick(), if matches!(loaded.config.identity.directory.provider, DirectoryProvider::Keycloak|DirectoryProvider::Snapshot) => {
                if crate::directory_pull::pull(&loaded.config, true, timeout).await.is_err() {
                    eprintln!("writer_service event=directory_pull_failed");
                }
            },
            _ = timer.tick(), if loaded.config.writer.serve.committer => {
                let proof = if let Some((_, health)) = &admin { *health.lock().await } else { None };
                let proof = proof.filter(|at| chrono::Utc::now().signed_duration_since(*at).to_std().is_ok_and(|age|age <= loaded.config.identity.directory.sync_interval.as_duration()));
                if attest_directory(&loaded.config, proof).await.is_err() {
                    eprintln!("writer_service event=directory_attestation_failed");
                }
                match crate::commit::commit_until_stopped(&loaded.config, false, Some(&stop)).await {
                    Ok(_) => eprintln!("writer_service event=published"),
                    Err(WriterError::CommitterBusy) => eprintln!("writer_service event=committer_busy"),
                    Err(_) => eprintln!("writer_service event=commit_failed"),
                }
                if stop.load(Ordering::Acquire) {
                    break;
                }
            }
        }
    }
    stop.store(true, Ordering::Release);
    if !shutdown.is_finished() {
        shutdown.abort();
        let _ = shutdown.await;
    }
    supervisor.await.map_err(|_| WriterError::WorkerFailure)?;
    if let Some((task, _)) = admin {
        task.await.map_err(|_| WriterError::AdminServiceFailed)??;
    }
    Ok(serde_json::json!({"stopped":true}))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Invoked by the real CLI/RustFS acceptance harness. Never linked into fyrr.
    #[tokio::test]
    #[ignore = "test-only SCIM health source; real-S3 publication lifecycle harness supplies config"]
    async fn scim_verified_applied_sync_hook() {
        let path = std::env::var_os("FYRR_SCIM_HEALTH_TEST_CONFIG").unwrap();
        let loaded = fyrr_core::config::load(Path::new(&path)).unwrap();
        let config = &loaded.config;
        let before = fyrr_reader::storage::optional_head(config)
            .await
            .unwrap()
            .unwrap();
        assert!(!attest_directory(config, None).await.unwrap());
        assert_eq!(
            fyrr_reader::storage::optional_head(config)
                .await
                .unwrap()
                .unwrap(),
            before
        );
        assert_eq!(
            attest_directory(
                config,
                Some(chrono::Utc::now() - chrono::Duration::hours(2))
            )
            .await,
            Err(WriterError::DirectoryHealthInvalid)
        );
        assert_eq!(
            attest_directory(
                config,
                Some(chrono::Utc::now() + chrono::Duration::hours(2))
            )
            .await,
            Err(WriterError::DirectoryHealthInvalid)
        );
        // Only this test endpoint supplies a successful applied push.
        let test_health_source = || Some(chrono::Utc::now());
        let lease = crate::lease::LeaseGuard::acquire(config).await.unwrap();
        assert_eq!(
            attest_directory(config, test_health_source()).await,
            Err(WriterError::CommitterBusy)
        );
        lease.release().await.unwrap();
        let applied = test_health_source().unwrap();
        assert!(attest_directory(config, Some(applied)).await.unwrap());
        let after = fyrr_reader::storage::optional_head(config)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.freshness.directory, Some(applied));
        assert_eq!(after.identity, before.identity);
        assert_eq!(after.content, before.content);
        assert_eq!(after.freshness.sources, before.freshness.sources);
        assert!(!attest_directory(config, Some(applied)).await.unwrap());
        assert_eq!(
            fyrr_reader::storage::optional_head(config)
                .await
                .unwrap()
                .unwrap()
                .freshness
                .directory,
            Some(applied)
        );
    }
}
