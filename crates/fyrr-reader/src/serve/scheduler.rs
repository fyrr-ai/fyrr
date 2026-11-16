use super::*;

fn blocking_thread_limit(worker_count: usize, concurrency: usize) -> usize {
    worker_count
        .saturating_add(concurrency.max(2))
        .saturating_add(3)
}

pub(crate) async fn start(reader: PreparedReader) -> Result<McpService> {
    #[cfg(test)]
    let (sample_events, _) = tokio::sync::broadcast::channel(16);
    #[cfg(test)]
    let observed_samples = sample_events.clone();
    let http = reader.config.reader.http.is_some();
    let cap = reader.config.reader.concurrency;
    let worker_count = crate::generation::PreparationReservation::worker_count(&reader.config);
    let native_slots = Arc::new(tokio::sync::Semaphore::new(cap));
    #[cfg(test)]
    let observed_native_slots = native_slots.clone();
    let template = reader;
    let config = template.config.clone();
    let mut idle = VecDeque::new();
    for _ in 0..worker_count {
        idle.push_back(template.worker().await?);
    }
    let (sender, input) = mpsc::channel::<Job>(cap * 2);
    let publication = Arc::new(tokio::sync::RwLock::new(template.readiness_state()));
    let served = publication.clone();
    let context = PreparationContext {
        policy_cache: template.policy_cache.clone(),
        identity_cache: template.identity_cache.clone(),
        control_backend: template.control_backend.clone(),
        control_root: template.control_root.clone(),
        reranker: template.reranker.clone(),
        #[cfg(test)]
        test_hooks: template.test_hooks.clone(),
        config: config.clone(),
        mirror: template.mirror.clone(),
        query_embeddings: template.query_embeddings.clone(),
        cache: template.admission_cache.clone(),
        budget: template.preparation_budget.clone(),
    };
    let template = Some(template);
    let (reload, control) = mpsc::channel::<oneshot::Sender<Result<Value>>>(1);
    std::thread::spawn(move || {
        // Native scans include CPU decoding on Tokio workers. Cap the pool on
        // large hosts; request admission and generation budgets remain separate.
        let worker_threads =
            std::thread::available_parallelism().map_or(2, |cpus| cpus.get().min(8));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            // Every driver may park on native I/O. Two checksum verifiers and
            // one centroid build can also block; reserve those independently
            // plus at least two I/O workers (one per admitted request at scale).
            .max_blocking_threads(blocking_thread_limit(worker_count, cap))
            .enable_all()
            .build()
            .unwrap_or_else(|error| panic!("reader runtime: {error:?}"));
        runtime.block_on(async move {
            let interval = config
                .reader
                .version_check_interval
                .as_duration()
                .min(Duration::from_secs(30));
            let next_check = Instant::now() + interval;
            let warm_cancel = CancellationToken::new();
            let next_warm = Instant::now();
            let warm_loss = if template
                .as_ref()
                .is_some_and(|r| !r.preparation_reservation.warm())
            {
                Some("memory_budget")
            } else {
                None
            };
            Scheduler {
                context,
                config,
                interval,
                next_check,
                warm_cancel,
                next_warm,
                warm_loss,
                preparation: None,
                warming: None,
                warm_delay: Duration::from_secs(30),
                handover: false,
                reported_reserved: None,
                running: FuturesUnordered::new(),
                sample_cancel: CancellationToken::new(),
                successful_dense: 0,
                selected_samples: 0,
                completed_samples: 0,
                pending: VecDeque::new(),
                hup: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                    .unwrap_or_else(|error| panic!("signal: {error:?}")),
                tick: tokio::time::interval(Duration::from_millis(10)),
                eof: false,
                reload_reply: None,
                reload_force: false,
                native_slots,
                idle,
                template,
                input,
                control,
                served,
                worker_count,
                cap,
                #[cfg(test)]
                sample_events: observed_samples,
            }
            .run()
            .await;
        });
    });
    Ok(McpService {
        #[cfg(test)]
        sample_events,
        #[cfg(test)]
        native_slots: observed_native_slots,
        http,
        sender,
        cancellations: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        slots: Arc::new(tokio::sync::Semaphore::new(cap * 2)),
        reload,
        publication,
    })
}

type ExecutionResult = (
    PreparedReader,
    Option<(crate::search::DenseCompletion, Option<i64>)>,
);
type PreparationCompletion = <VersionPreparation as std::future::Future>::Output;

struct Scheduler {
    context: PreparationContext,
    config: fyrr_core::config::Config,
    interval: Duration,
    next_check: Instant,
    preparation: Option<VersionPreparation>,
    warming: Option<(Arc<()>, VersionPreparation)>,
    warm_cancel: CancellationToken,
    next_warm: Instant,
    warm_delay: Duration,
    warm_loss: Option<&'static str>,
    handover: bool,
    reported_reserved: Option<u64>,
    running: FuturesUnordered<tokio::task::JoinHandle<ExecutionResult>>,
    sample_cancel: CancellationToken,
    successful_dense: u64,
    selected_samples: u64,
    completed_samples: u64,
    pending: VecDeque<Job>,
    hup: tokio::signal::unix::Signal,
    tick: tokio::time::Interval,
    eof: bool,
    reload_reply: Option<oneshot::Sender<Result<Value>>>,
    reload_force: bool,
    native_slots: Arc<tokio::sync::Semaphore>,
    idle: VecDeque<PreparedReader>,
    template: Option<PreparedReader>,
    input: mpsc::Receiver<Job>,
    control: mpsc::Receiver<oneshot::Sender<Result<Value>>>,
    served: Arc<tokio::sync::RwLock<crate::readiness::Readiness>>,
    worker_count: usize,
    cap: usize,
    #[cfg(test)]
    sample_events: tokio::sync::broadcast::Sender<Value>,
}

impl Scheduler {
    async fn run(mut self) {
        // Only one recall can run at a time. Keep its inferred future type in
        // this scope so sampling needs no additional box or spawned task.
        let mut sampling = FuturesUnordered::new();
        loop {
            let now = Instant::now();
            let sampling_idle = sampling.is_empty();
            let reserved = self.context.budget.cap.saturating_sub(
                self.context.budget.permits.available_permits() as u64
                    * crate::generation::PAGE_BYTES,
            );
            if self.reported_reserved != Some(reserved) {
                emit(format_args!(
                    "telemetry generation_preparation=accounting reserved_bytes={reserved} \
                        capacity_bytes={} budget_bytes={} rss_bytes={}",
                    self.context.budget.cap,
                    self.config.reader.memory_max_bytes.as_u64(),
                    crate::generation::PreparationReservation::rss().unwrap_or(0)
                ));
                self.reported_reserved = Some(reserved);
            }
            if let Some(reader) = &mut self.template {
                reader.expire_pin();
                if reader.release_unreserved_roots() {
                    *self.served.write().await = reader.readiness_state();
                    self.warm_loss = Some("memory_budget");
                }
            }
            if self.handover
                && self.running.is_empty()
                && sampling_idle
                && self.warming.is_none()
                && self.preparation.is_none()
            {
                if self.template.take().is_some() {
                    self.idle.clear();
                    crate::sync::lock(&self.context.cache).retain_content(u64::MAX);
                    self.next_check = now;
                }
                if now >= self.next_check {
                    self.prepare(Trigger::Reload);
                    self.next_check = now + self.interval;
                }
            } else if !self.handover && self.preparation.is_none() && now >= self.next_check {
                self.prepare(Trigger::Collect);
                self.next_check = now + self.interval;
            }
            if !self.handover
                && self.preparation.is_none()
                && self.warming.is_none()
                && now >= self.next_warm
                && let Some(reader) = &self.template
                && reader.generation_state != crate::generation::GenerationState::WarmReady
            {
                self.warm_cancel = CancellationToken::new();
                self.warming = Some((
                    reader.generation_id.clone(),
                    prepare_warm(reader, self.warm_cancel.clone()),
                ));
            }
            if self.eof
                || !self.pending.is_empty()
                || !self.input.is_empty()
                || self.handover
                || self.preparation.is_some()
                || self.warming.is_some()
            {
                self.sample_cancel.cancel();
            }
            let mut retained = VecDeque::new();
            while let Some(job) = self.pending.pop_front() {
                let code = if job.timing.cancelled.is_cancelled() {
                    Some(Cancelled)
                } else if now >= job.timing.deadline {
                    Some(if self.handover {
                        Busy
                    } else {
                        DeadlineExceeded
                    })
                } else {
                    None
                };
                if let Some(code) = code {
                    let _ = job.reply.send(tool_error(code));
                } else {
                    retained.push_back(job);
                }
            }
            self.pending = retained;
            for reader in &mut self.idle {
                reader.release_unreserved_roots();
            }
            self.dispatch(now);
            if self.eof && self.pending.is_empty() && self.running.is_empty() && sampling.is_empty()
            {
                break;
            }
            tokio::select! {
                _ = self.tick.tick() => {},
                _ = self.hup.recv() => self.on_hup(),
                request = self.control.recv(), if self.reload_reply.is_none() => {
                    self.on_control(request);
                },
                Some(prepared) = futures::future::OptionFuture::from(self.preparation.as_mut()),
                    if self.preparation.is_some() => self.on_prepared(prepared).await,
                Some(warmed) = futures::future::OptionFuture::from(
                    self.warming.as_mut().map(|(_, task)| task)),
                    if self.warming.is_some() => self.on_warmed(warmed).await,
                Some(result) = self.running.next(), if !self.running.is_empty() => {
                    let Ok(result) = result else {
                        // Publish failure before dropping readers and native sessions:
                        // their cleanup can delay destruction of the input receiver.
                        self.input.close();
                        self.served.write().await.ready = false;
                        #[cfg(test)]
                        self.context.test_hooks.scheduler_failed.notify_one();
                        emit(format_args!(
                            "telemetry reader_scheduler=unavailable reason=request_task_failed"
                        ));
                        break;
                    };
                    if let Some(work) = self.on_finished(result, sampling.is_empty()) {
                        sampling.push(work.run());
                    }
                },
                Some(result) = sampling.next(), if !sampling.is_empty() => {
                    self.on_sample(result);
                },
                job = self.input.recv(), if !self.eof && self.pending.len() < self.cap => {
                    self.on_job(job);
                },
            }
        }
    }
    fn prepare(&mut self, trigger: Trigger) {
        self.preparation = Some(prepare_version(
            &self.context,
            self.template.as_ref(),
            trigger,
            self.warm_cancel.clone(),
        ));
    }
    fn dispatch(&mut self, now: Instant) {
        while !self.handover
            && !self.idle.is_empty()
            && !self.pending.is_empty()
            && !(self.preparation.is_some()
                && self
                    .pending
                    .front()
                    .is_some_and(|j| now < j.timing.version_wait_until))
        {
            let Ok(permit) = self.native_slots.clone().try_acquire_owned() else {
                break;
            };
            let Some(mut job) = self.pending.pop_front() else {
                unreachable!("dispatch requires a pending job");
            };
            #[allow(clippy::cast_possible_truncation)]
            // Keep the sampling counter's wrapping policy.
            let query_index = match &job.request {
                ToolCall::Search(args) => {
                    self.selected_samples as usize % args.queries.as_ref().map_or(1, Vec::len)
                }
                _ => 0,
            };
            job.recall_query_index = query_index;
            let Some(mut reader) = self.idle.pop_front() else {
                unreachable!("dispatch requires an idle worker");
            };
            reader.execution_slot = Some(ExecutionSlot::new(self.native_slots.clone(), permit));
            let mut timing = RequestTiming {
                arrived: job.timing.arrived,
                deadline: job.timing.deadline,
                cancelled: job.timing.cancelled.clone(),
                version_wait_until: job.timing.version_wait_until,
                authorization_build_us: 0,
            };
            job.timing.cancelled = CancellationToken::new();
            let pin = reader.mirror_pin.request();
            // Keep CPU work off the scheduler's poll loop. Each admitted
            // reader stays on one blocking worker, including its SQLite
            // handle and Lance futures. The existing permit and pin move
            // with it; completion still returns the reader here.
            self.running.push(tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let _pin = pin;
                    if let Some(subject) = &job.subject {
                        let bound = bounded(&timing, reader.bind_subject(subject)).await;
                        if let Err(code) = bound {
                            let Err(code) = settle::<()>(&mut reader, Err(code)).await else {
                                unreachable!("a refused binding cannot settle successfully");
                            };
                            reader.execution_slot.take();
                            let _ = job.reply.send(tool_error(code));
                            return (reader, None);
                        }
                    }
                    timing.authorization_build_us = reader.admission_build_us;
                    execute(reader, job, timing).await
                })
            }));
        }
    }
    fn on_hup(&mut self) {
        self.sample_cancel.cancel();
        self.reload_force = true;
        if self.preparation.is_none() && !self.handover {
            self.prepare(Trigger::Reload);
            self.reload_force = false;
        }
    }
    fn on_control(&mut self, request: Option<oneshot::Sender<Result<Value>>>) {
        self.sample_cancel.cancel();
        if let Some(request) = request {
            self.reload_reply = Some(request);
            self.reload_force = true;
            if self.preparation.is_none() && !self.handover {
                self.prepare(Trigger::Reload);
                self.reload_force = false;
            }
        }
    }
    async fn on_prepared(&mut self, prepared: PreparationCompletion) {
        self.preparation = None;
        match prepared {
            Ok(Ok(Some((new, workers)))) => {
                // Serving readiness retains the last active state
                // across a drain; abandoned preparations never log it.
                new.report_cache_degradation(self.served.read().await.cache_degraded);
                self.warm_cancel.cancel();
                self.idle = workers;
                let reader = self.template.insert(new);
                self.handover = false;
                *self.served.write().await = reader.readiness_state();
                log_refresh("ready", reader.manifest.sequence);
                self.next_warm = Instant::now() + Duration::from_secs(30);
                self.warm_delay = Duration::from_secs(30);
                self.warm_loss =
                    if reader.generation_state == crate::generation::GenerationState::Cold {
                        Some("memory_budget")
                    } else {
                        None
                    };
            }
            Ok(Ok(None)) => {}
            Ok(Err(PreparationBudgetExhausted)) if self.template.is_some() => {
                // Pause before draining, including mandatory ACL/restore switches.
                // Running requests own their old pins and claims independently.
                self.handover = true;
                self.warm_cancel.cancel();
                self.served.write().await.ready = false;
                emit(format_args!(
                    "telemetry generation_preparation=degraded reason=sequential_handover"
                ));
            }
            _ => {
                log_refresh(
                    "retry",
                    self.template.as_ref().map_or(0, |r| r.manifest.sequence),
                );
                self.next_check = Instant::now() + Duration::from_secs(1);
                if let Some(reply) = self.reload_reply.take() {
                    let _ = reply.send(Err(StorageUnavailable));
                }
            }
        }
        if !self.handover {
            if self.reload_force {
                self.prepare(Trigger::Reload);
                self.reload_force = false;
            } else if let Some(reply) = self.reload_reply.take() {
                let _ = reply.send(Ok(self.served.read().await.clone().into_wire()));
            }
        }
    }
    async fn on_warmed(&mut self, warmed: PreparationCompletion) {
        let Some((source_id, _)) = self.warming.take() else {
            unreachable!("a warming completion owns its source generation");
        };
        if !self.handover && !self.warm_cancel.is_cancelled() {
            match warmed {
                Ok(Ok(Some((new, workers))))
                    if self.template.as_ref().is_some_and(|r| {
                        Arc::ptr_eq(&r.generation_id, &source_id)
                            && r.manifest.sequence == new.manifest.sequence
                    }) =>
                {
                    new.report_cache_degradation(self.served.read().await.cache_degraded);
                    self.idle = workers;
                    let reader = self.template.insert(new);
                    *self.served.write().await = reader.readiness_state();
                    self.warm_loss = None;
                    log_refresh("ready", reader.manifest.sequence);
                }
                Ok(Err(reason)) => {
                    let reason = if reason == MirrorCapacityExhausted {
                        "mirror_capacity"
                    } else if reason == PreparationBudgetExhausted {
                        "memory_budget"
                    } else {
                        "warming_failed"
                    };
                    if self.warm_loss != Some(reason) {
                        emit(format_args!(
                            "telemetry generation_preparation=degraded reason={reason}"
                        ));
                        self.warm_loss = Some(reason);
                    }
                }
                _ => {}
            }
        }
        self.next_warm = Instant::now() + self.warm_delay;
        self.warm_delay = (self.warm_delay * 2).min(Duration::from_secs(300));
    }
    fn on_finished(&mut self, result: ExecutionResult, sampling_idle: bool) -> Option<SampleWork> {
        let (mut reader, dense) = result;
        if let Some((completion, expires)) = dense {
            self.successful_dense += 1;
            if self.successful_dense.is_multiple_of(100) {
                self.selected_samples += 1;
                let skip = if !sampling_idle {
                    Some(SampleSkip::InFlight)
                } else if !self.pending.is_empty()
                    || !self.input.is_empty()
                    || (self.config.embedding.reader_live && !self.running.is_empty())
                {
                    Some(SampleSkip::ForegroundQueued)
                } else if self.handover
                    || self.preparation.is_some()
                    || self.warming.is_some()
                    || self.template.as_ref().is_none_or(|current| {
                        !Arc::ptr_eq(&current.generation_id, &reader.generation_id)
                    })
                {
                    Some(SampleSkip::Preparation)
                } else {
                    None
                };
                let native = self.native_slots.clone().try_acquire_owned().ok();
                let skip =
                    skip.or_else(|| native.is_none().then_some(SampleSkip::ForegroundQueued));
                if let Some(reason) = skip {
                    self.report_sample(Err(reason));
                } else {
                    let sample = reader.recall_sample(completion, expires);
                    let pin = reader.mirror_pin.request();
                    self.sample_cancel = CancellationToken::new();
                    let cancelled = self.sample_cancel.clone();
                    self.report_sample(Ok(json!({"recall_started":1,"recall_in_flight":1})));
                    return Some(SampleWork {
                        reader,
                        sample,
                        pin,
                        native,
                        cancelled,
                        expires,
                    });
                }
            }
        }
        reader.release_unreserved_roots();
        if self.can_idle_worker(&reader) {
            self.idle.push_back(reader);
        }
        #[cfg(test)]
        self.context.test_hooks.request_finished.notify_one();

        None
    }
    fn on_sample(&mut self, sample: SampleResult) {
        let (mut reader, result, expires) = sample;

        // Foreground arrival or activation may race a ready reference.
        // Only the scheduler records measurements, after these checks.
        let result = if self.sample_cancel.is_cancelled()
            || self.eof
            || !self.pending.is_empty()
            || !self.input.is_empty()
            || self.handover
            || self.preparation.is_some()
            || self.warming.is_some()
            || self
                .template
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(&current.generation_id, &reader.generation_id))
        {
            Err(SampleSkip::Cancelled)
        } else if expires.is_some_and(|expiry| expiry <= fyrr_core::clock::now().timestamp()) {
            Err(SampleSkip::AuthorizationChanged)
        } else {
            result
        };
        if result.is_ok() {
            self.completed_samples += 1;
        }
        self.report_sample(result);
        reader.release_unreserved_roots();
        if self.can_idle_worker(&reader) {
            self.idle.push_back(reader);
        }
    }
    fn can_idle_worker(&self, reader: &PreparedReader) -> bool {
        !self.handover
            && self.idle.len() < self.worker_count
            && self
                .template
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(&current.generation_id, &reader.generation_id))
    }
    fn on_job(&mut self, job: Option<Job>) {
        self.sample_cancel.cancel();
        let Some(mut job) = job else {
            self.eof = true;
            return;
        };
        if self.config.reader.version_check == fyrr_core::config::VersionCheck::PerRequest {
            if self.preparation.is_none() && !self.handover {
                self.prepare(Trigger::Check);
            }
            job.timing.version_wait_until = job
                .timing
                .deadline
                .min(job.timing.arrived + self.config.reader.per_request_wait.as_duration());
        }
        self.pending.push_back(job);
    }

    fn report_sample(&self, result: Result<Value, SampleSkip>) {
        let event = recall_telemetry(
            self.successful_dense,
            self.selected_samples,
            self.completed_samples,
            result,
        );
        #[cfg(test)]
        let _ = self.sample_events.send(event);
        #[cfg(not(test))]
        drop(event);
    }
}

#[cfg(test)]
mod blocking_tests {
    #[test]
    fn reads_progress_while_drivers_and_hashes_are_blocked() {
        fn probe(limit: usize) -> bool {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(limit)
                .build()
                .unwrap();
            let gate =
                std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
            let (started, ready) = std::sync::mpsc::channel();
            for _ in 0..6 {
                let gate = gate.clone();
                let started = started.clone();
                runtime.spawn_blocking(move || {
                    let (lock, condition) = &*gate;
                    let mut released = lock.lock().unwrap();
                    started.send(()).unwrap();
                    while !*released {
                        released = condition.wait(released).unwrap();
                    }
                });
            }
            for _ in 0..6 {
                ready
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
            let (done, completion) = std::sync::mpsc::channel();
            runtime.spawn_blocking(move || {
                let _ = done.send(());
            });
            let progressed = completion
                .recv_timeout(std::time::Duration::from_millis(500))
                .is_ok();
            let (lock, condition) = &*gate;
            *lock.lock().unwrap() = true;
            condition.notify_all();
            drop(runtime);
            progressed
        }
        assert!(!probe(4 + 2), "baseline exhausts I/O workers");
        assert!(
            probe(super::blocking_thread_limit(4, 2)),
            "reserved I/O workers progress"
        );
    }
}
