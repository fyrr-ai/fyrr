use super::*;
pub(super) fn log_refresh(status: &str, sequence: u64) {
    emit(format_args!(
        "reader_refresh status={status} content_sequence={sequence}"
    ));
}

/// One preparer per stdio host; workers retain their generation-owned native session.
pub(super) fn prepare_warm(
    reader: &PreparedReader,
    cancel: CancellationToken,
) -> VersionPreparation {
    // Only capture owned, nonblocking resources here. Every operation, including
    // the worker open, is inside the spawned task's cancellation/timeout boundary.
    let seed = reader.worker();
    #[cfg(test)]
    let seed_hook = reader.test_hooks.clone();
    let reservation = reader.preparation_reservation.clone();
    let budget = reader.preparation_budget.clone();
    let config = reader.config.clone();
    tokio::spawn(async move {
        let preparation = async {
            #[cfg(test)]
            seed_hook.cache_warming.wait().await;
            let upgraded = reservation.upgrade(budget.permits)?;
            let mut prepared = seed.await?;
            prepared.preparation_reservation = upgraded;
            prepared.generation_id = Arc::new(());
            if !reservation.warm() {
                prepared.vector_roots = Arc::default();
                prepared.centroid_routers = Arc::default();
            }
            prepared.generation_state = crate::generation::GenerationState::Serving;
            if config.reader.http.is_none() {
                prepared.rebuild_admission().await?;
            }
            prepared.prepare_routing().await?;
            prepared.prewarm_files().await?;
            if prepared.generation_state != crate::generation::GenerationState::WarmReady {
                return Ok(None);
            }
            let mut workers = VecDeque::new();
            for _ in 0..crate::generation::PreparationReservation::worker_count(&config) {
                workers.push_back(prepared.worker().await?);
            }
            let latest = prepared
                .head_cache
                .read_fresh(prepared.control_store.as_ref(), &prepared.control_root)
                .await?;
            if latest
                .content
                .as_ref()
                .is_none_or(|p| p.publication_id != prepared.manifest.publication_id)
            {
                return Err(PreparationSuperseded);
            }
            Ok(Some((prepared, workers)))
        };
        tokio::select! { biased;
            () = cancel.cancelled() =>Err(PreparationSuperseded),
            result=tokio::time::timeout(config.reader.max_preparation.as_duration(),
                preparation)=>result.map_err(|_|PreparationDeadline)?,
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Trigger {
    Reload,
    Collect,
}

impl PreparationContext {
    /// Later preparations reuse what the serving reader created: a changed
    /// embedding profile or a reranker the namespace gained keeps one spend
    /// counter instead of starting a new one at each publication.
    pub(super) fn adopt(&mut self, reader: &PreparedReader) {
        self.query_embeddings = reader.query_embeddings.clone();
        if let Some(reranker) = &reader.reranker {
            self.reranker = Some(reranker.clone());
        }
    }
}

pub(super) fn prepare_version(
    context: &PreparationContext,
    reader: Option<&PreparedReader>,
    trigger: Trigger,
    cancel_warm: CancellationToken,
) -> VersionPreparation {
    let context = context.clone();
    let current = reader.map(|r| {
        (
            r.manifest.sequence,
            r.manifest.lance_version,
            r.manifest.committed_at,
            r.mirror_pin.clone(),
            (
                r.manifest.publication_id.clone(),
                r.manifest.incarnation.clone(),
            ),
        )
    });
    let session = reader
        .map(|r| Arc::downgrade(&r.native_session))
        .unwrap_or_default();
    tokio::spawn(async move {
        let config = &context.config;
        let preparation = async {
            if let Some((_, version, _, _, _)) = &current {
                context
                    .cache
                    .lock()
                    .map_err(|_| AuthorizationUnavailable)?
                    .retain_content(*version);
            }
            let store = context.control_backend.clone();
            let root = context.control_root.clone();
            let store = context.mirror.wrap("control", store);
            // A version check, a refresh and SIGHUP read the head here; the
            // authorization checks of requests see what they saw.
            let head = context.head_cache.read_fresh(store.as_ref(), &root).await?;
            let pointer = head.content.as_ref().ok_or(PublicationUnavailable)?;
            let mut replaced = false;
            if let Some((sequence, _, committed, pin, (publication_id, incarnation))) = &current {
                replaced =
                    !PreparedReader::same_lineage(&head, publication_id, *sequence, incarnation);
                if !replaced && pointer.sequence <= *sequence {
                    // Collection must not queue the head check behind a large
                    // cache fill in this or another reader process.
                    if trigger == Trigger::Collect
                        && let Ok(result) = tokio::time::timeout(
                            Duration::from_millis(10),
                            context.mirror.evict_unpinned(),
                        )
                        .await
                    {
                        result.map_err(|_| StorageUnavailable)?;
                    }
                    return Ok(None);
                }
                cancel_warm.cancel();
                pin.retire();
                let age = (fyrr_core::clock::now() - *committed)
                    .to_std()
                    .unwrap_or_default();
                pin.expire(age, config.writer.purge_window.as_duration());
                #[cfg(feature = "test-store")]
                if trigger != Trigger::Reload
                    && crate::storage::test_store::version_check()
                        == Some(crate::storage::test_store::VersionCheck::Held)
                {
                    let policy: fyrr_core::publication::PolicyArtifact =
                        crate::storage::artifact(store.as_ref(), &root, &head.policy).await?;
                    let horizon = config
                        .writer
                        .purge_window
                        .as_duration()
                        .saturating_sub(config.writer.version_grace.as_duration());
                    // Sequences of a replaced namespace say nothing about this one.
                    if !replaced
                        && policy.row_acl_sequence.is_none_or(|s| s <= *sequence)
                        && age < horizon
                    {
                        return Ok(None);
                    }
                }
            }
            // Admission entries name Lance versions, which a replaced
            // namespace reuses for other rows.
            context
                .cache
                .lock()
                .map_err(|_| AuthorizationUnavailable)?
                .retain_content(if replaced {
                    u64::MAX
                } else {
                    pointer.lance_version
                });
            let mut prepared = PreparedReader::open_shared(
                config,
                Some((
                    // So do the native session's caches, which key the index
                    // metadata of a dataset by its URI and Lance version.
                    if replaced {
                        std::sync::Weak::new()
                    } else {
                        session
                    },
                    context.mirror.clone(),
                    context.cache.clone(),
                    context.query_embeddings.clone(),
                    context.policy_cache.clone(),
                    context.identity_cache.clone(),
                    context.head_cache.clone(),
                )),
                context.budget.clone(),
                None,
            )
            .await?;
            // The process-lifetime spend counter, unless the namespace gained a reranker.
            prepared.reranker = context.reranker.clone().or(prepared.reranker.take());
            #[cfg(test)]
            let prepared = {
                let mut prepared = prepared;
                prepared.test_hooks = context.test_hooks.clone();
                prepared
            };
            // New generations become serving immediately; physical warming is
            // independent, so its downloads cannot delay version activation.
            let mut workers = VecDeque::new();
            for _ in 0..crate::generation::PreparationReservation::worker_count(config) {
                workers.push_back(prepared.worker().await?);
            }
            let latest = context.head_cache.read_fresh(store.as_ref(), &root).await?;
            if latest
                .content
                .as_ref()
                .is_none_or(|p| p.publication_id != prepared.manifest.publication_id)
            {
                return Err(PreparationSuperseded);
            }
            Ok(Some((prepared, workers)))
        };
        tokio::time::timeout(config.reader.max_preparation.as_duration(), preparation)
            .await
            .map_err(|_| PreparationDeadline)?
    })
}
