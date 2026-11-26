//! Optional scheduling; all durable state remains in S3 and the published head.
use crate::worker::{Job, Running};
use crate::{Result, WriterError};
use fyrr_core::config::{DirectoryProvider, LoadedConfig};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

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
    // A change of the served head, which carries rejections and rewinds, makes
    // every source due: a task of its own counts them, so that a long commit
    // does not delay them; the supervisor reads the count.
    let heads = Arc::new(AtomicU64::new(0));
    let watch = {
        let config = loaded.config.clone();
        let heads = heads.clone();
        let period = config.writer.commit.coalescing_timer.as_duration();
        let mut head = served(&config).await;
        tokio::spawn(async move {
            let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                let now = served(&config).await;
                if now.is_some() && now != head {
                    heads.fetch_add(1, Ordering::Release);
                    head = now;
                }
            }
        })
    };
    let metrics = match &loaded.config.writer.serve.metrics_listen {
        Some(address) => Some(
            fyrr_reader::metrics::listen(address)
                .await
                .map_err(|_| WriterError::MetricsListenUnavailable)?,
        ),
        None => None,
    };
    let commits = Arc::new(crate::write_api::Commits::new());
    let admin = crate::admin::start(&loaded.config, stop.clone(), commits.clone()).await?;
    let stopped = stop.clone();
    let config = loaded.config.clone();
    let path = path.to_owned();
    let observed = heads.clone();
    let supervisor = tokio::task::spawn_blocking(move || {
        let mut due = vec![fyrr_core::clock::now(); config.sources.len()];
        // Where a pass starts, so that a source is not always the first to be served.
        let mut first = 0;
        // The head count each source's last worker started under.
        let mut started = vec![0; config.sources.len()];
        let mut running = Vec::<(usize, Running)>::new();
        let mut last = fyrr_core::clock::now();
        // Clock steps back so far, and the count each source's last worker started under.
        let (mut steps, mut started_steps) = (0, vec![0; config.sources.len()]);
        while !stopped.load(Ordering::Relaxed) {
            let heads = observed.load(Ordering::Acquire);
            let now = fyrr_core::clock::now();
            // A clock stepped back leaves the cursors' times in the future,
            // which the worker treats as due; wake every source to let it.
            if now < last {
                due.fill(now);
                steps += 1;
            }
            last = now;
            let mut waiting = Vec::new();
            for (i, mut worker) in running {
                match worker.poll() {
                    Ok(None) => waiting.push((i, worker)),
                    result => {
                        if !matches!(result, Ok(Some(ref outcome)) if outcome.failure().is_none()) {
                            eprintln!("writer_service event=worker_failed");
                        }
                        let intervals = &config.sources[i].intervals;
                        let retry = [
                            Some(intervals.content),
                            intervals.permissions,
                            intervals.members,
                            Some(intervals.reconcile),
                        ]
                        .into_iter()
                        .flatten()
                        .map(|v| v.as_duration())
                        .min()
                        .unwrap_or(Duration::from_secs(60))
                        .min(Duration::from_secs(60));
                        // The cursors' timestamps decide what is due; a worker
                        // that failed, or reported no schedule, is retried sooner.
                        let wait = match &result {
                            Ok(Some(outcome)) if outcome.failure().is_none() => next_due(outcome),
                            _ => None,
                        };
                        due[i] = wait.unwrap_or_else(|| {
                            now + chrono::TimeDelta::from_std(retry).unwrap_or_default()
                        });
                        // A schedule computed before a clock step back is stale.
                        if started_steps[i] != steps {
                            due[i] = now;
                        }
                    }
                }
            }
            running = waiting;
            let start = first;
            for k in 0..config.sources.len() {
                let i = (start + k) % config.sources.len();
                let source = &config.sources[i];
                if running.len() >= jobs {
                    break;
                }
                // A head that changed since the source's worker started makes it due.
                if (due[i] > now && started[i] == heads) || running.iter().any(|(n, _)| *n == i) {
                    continue;
                }
                started[i] = heads;
                started_steps[i] = steps;
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
                        due[i] = now + chrono::TimeDelta::seconds(1);
                        eprintln!("writer_service event=worker_failed");
                    }
                }
                first = (i + 1) % config.sources.len();
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
    let mut maintaining: Option<tokio::task::JoinHandle<()>> = None;
    'serving: loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = directory_timer.tick(), if matches!(loaded.config.identity.directory.provider, DirectoryProvider::Keycloak|DirectoryProvider::Snapshot) => {
                if crate::directory_pull::pull(&loaded.config, timeout).await.is_err() {
                    eprintln!("writer_service event=directory_pull_failed");
                }
            },
            // The timer, or a request of the write API that wants its envelope published now.
            () = round(&mut timer, &commits), if loaded.config.writer.serve.committer => {
                // The configured namespace, then those with a request that waits.
                let waiting = commits.waiting();
                let namespaces = std::iter::once(loaded.config.clone()).chain(
                    waiting
                        .iter()
                        .filter(|name| **name != loaded.config.namespace.name)
                        .map(|name| crate::write_api::named(&loaded.config, name)),
                );
                for config in namespaces {
                    let mut maintenance = None;
                    let began = std::time::Instant::now();
                    let outcome = match crate::commit::commit_until_stopped(&config, false, Some(&stop)).await {
                        Ok((_, tick)) => {
                            let outcome = if tick.published { "published" } else { "idle" };
                            eprintln!("writer_service event={outcome}");
                            maintenance = tick.maintenance;
                            if tick.published {
                                fyrr_reader::metrics::observe(&crate::telemetry::COMMIT_DURATION, &[], began.elapsed());
                            }
                            outcome
                        }
                        Err(WriterError::CommitterBusy) => {
                            eprintln!("writer_service event=committer_busy");
                            "busy"
                        }
                        Err(error) => {
                            eprintln!("writer_service event=commit_failed code={}", error.as_str());
                            "failed"
                        }
                    };
                    fyrr_reader::metrics::add(&crate::telemetry::COMMIT_ROUNDS, &[("outcome", outcome)], 1);
                    // A round that found the lease taken published nothing to recheck: a request
                    // that waits would wake at once, ask for a round again and spin.
                    if outcome != "busy" {
                        commits.done.send_modify(|rounds| *rounds += 1);
                    }
                    if stop.load(Ordering::Acquire) {
                        break 'serving;
                    }
                    // The tick judged this from the manifest it read anyway. It runs on a
                    // task of its own, so that the next round is not behind it, and one
                    // at a time.
                    if let Some(config) = maintenance
                        .filter(|_| loaded.config.writer.serve.maintenance)
                        .filter(|_| maintaining.as_ref().is_none_or(tokio::task::JoinHandle::is_finished))
                    {
                        maintaining = Some(tokio::spawn(maintain(config, commits.clone())));
                    }
                }
            }
        }
    }
    stop.store(true, Ordering::Release);
    // A signal waits for the maintenance step in progress, as it does for a commit group.
    if let Some(task) = maintaining {
        let _ = task.await;
    }
    if let Some(metrics) = metrics {
        metrics.abort();
    }
    watch.abort();
    let _ = watch.await;
    if !shutdown.is_finished() {
        shutdown.abort();
        let _ = shutdown.await;
    }
    supervisor.await.map_err(|_| WriterError::WorkerFailure)?;
    if let Some(task) = admin {
        task.await.map_err(|_| WriterError::AdminServiceFailed)??;
    }
    Ok(serde_json::json!({"stopped":true}))
}

/// One maintenance run of a namespace. A write that waits for the namespace's lease is
/// committed between its steps, under the lease the run holds.
#[allow(clippy::print_stderr)]
async fn maintain(config: fyrr_core::config::Config, commits: Arc<crate::write_api::Commits>) {
    eprintln!("writer_service event=maintenance_started");
    match crate::maintenance::maintain_serving(&config, false, Some(&commits)).await {
        Ok(done) => {
            let yielded = done["yielded"].as_bool().unwrap_or(false);
            eprintln!(
                "writer_service event={}",
                if yielded {
                    "maintenance_yielded"
                } else {
                    "maintained"
                }
            );
            for (step, key) in [
                ("compaction", "compaction_steps"),
                ("index_merge", "index_merge_steps"),
            ] {
                let steps = done[key].as_i64().unwrap_or(0);
                fyrr_reader::metrics::add(
                    &crate::telemetry::MAINTENANCE_STEPS,
                    &[("step", step)],
                    steps,
                );
            }
        }
        Err(WriterError::CommitterBusy) => eprintln!("writer_service event=maintenance_busy"),
        Err(error) => eprintln!(
            "writer_service event=maintenance_failed code={}",
            error.as_str()
        ),
    }
}

/// The next commit round: at the coalescing timer, or when a write request asks.
async fn round(timer: &mut tokio::time::Interval, commits: &crate::write_api::Commits) {
    tokio::select! {
        _ = timer.tick() => {},
        () = commits.requested.notified() => {},
    }
}

/// The served head, as far as it matters to a worker; `None` when unreadable.
async fn served(config: &fyrr_core::config::Config) -> Option<fyrr_core::publication::ServingHead> {
    let head = fyrr_reader::storage::optional_head(config).await.ok()??;
    Some(crate::python::served_identity(&head))
}

/// When the source's kinds are next due, as its worker reported it.
fn next_due(outcome: &crate::worker::Outcome) -> Option<chrono::DateTime<chrono::Utc>> {
    let report: serde_json::Value = serde_json::from_slice(outcome.result_line()?).ok()?;
    serde_json::from_value(report["next_due"].clone()).ok()
}
