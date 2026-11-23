use super::*;

pub async fn restore(config: &Config, publication: Option<&str>) -> Result<serde_json::Value> {
    if publication.is_some_and(|id| !fyrr_core::limits::prefixed_hex(id, "pub-", 32)) {
        return Err(WriterError::PublicationIdInvalid);
    }
    crate::lease::LeaseGuard::with(config, |lease| async move {
        // Keep the native publication future off the caller's stack, including
        // embedders that invoke restore on a runtime worker thread.
        Box::pin(restore_held(config, &lease, publication)).await
    })
    .await
}

pub(super) const RESTORE_ROOTS: usize = 256;
const RESTORE_ATTEMPTS: usize = 3;

#[derive(Debug)]
pub(super) struct RestoreAttempt<'a> {
    pub(super) head: ServingHead,
    pub(super) dataset: Dataset,
    pub(super) work: CommitLineage,
    pub(super) history: &'a mut commit::RestoreHistory,
    pub(super) cadence: RestoreCadence,
}

#[derive(Debug)]
pub(super) struct RestoreCadence {
    pub(super) last: std::time::Instant,
    pub(super) interval: std::time::Duration,
}
impl RestoreCadence {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            last: std::time::Instant::now(),
            interval: std::time::Duration::from_secs(30)
                .min(config.writer.maintenance.step_max_duration.as_duration() / 2),
        }
    }
    pub(super) async fn maybe(
        &mut self,
        config: &Config,
        lease: &LeaseGuard,
        history: &mut commit::RestoreHistory,
    ) -> Result<()> {
        // Keep the in-memory lease liveness check even when discovery is not due.
        lease.head().await?;
        if self.last.elapsed() >= self.interval {
            self.drain(config, lease, history).await?;
        }
        Ok(())
    }
    pub(super) async fn drain(
        &mut self,
        config: &Config,
        lease: &LeaseGuard,
        history: &mut commit::RestoreHistory,
    ) -> Result<()> {
        restore_boundary_cached(config, lease, history).await?;
        self.last = std::time::Instant::now();
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(super) struct RestoreAuthority {
    pub(super) states: BTreeMap<String, apply::DocumentState>,
    pub(super) readers: BTreeMap<(String, String), Vec<String>>,
    pub(super) own_readers: BTreeMap<(String, String), Option<Vec<String>>>,
}

pub(super) async fn restore_held(
    config: &Config,
    lease: &LeaseGuard,
    publication: Option<&str>,
) -> Result<serde_json::Value> {
    restore_index::Index::scratch(config)?;
    let mut history = commit::RestoreHistory::new(config)?;
    crate::envelopes::cleanup_held(config, lease).await?;
    let head = command_start(config, lease, &mut history, true).await?;
    let retained = retained(config, &head).await?;
    // Pin rollback's target once too: an authorization publication must not
    // change what the next attempt means by "previous publication".
    let target = if let Some(id) = publication {
        retained.iter().find(|m| m.publication_id == id)
    } else {
        retained.get(1)
    }
    .filter(|m| {
        (fyrr_core::clock::now() - m.committed_at)
            .to_std()
            .is_ok_and(|age| age < config.writer.purge_window.as_duration())
    })
    .ok_or(WriterError::RestoreHorizonExpired)?;
    let mut head = head;
    for attempt in 0..RESTORE_ATTEMPTS {
        if attempt != 0 {
            head = command_start(config, lease, &mut history, true).await?;
        }
        match Box::pin(restore_attempt(
            config,
            lease,
            head.clone(),
            target,
            &mut history,
        ))
        .await
        {
            Err(WriterError::RestoreAuthorizationChanged) => continue,
            result => {
                if result
                    .as_ref()
                    .is_err_and(|e| *e == WriterError::MaintenanceStepDeadline)
                {
                    // The completed private suffix is recovered before the lease
                    // is released, and queued authorization is serviced.
                    command_start(config, lease, &mut history, true).await?;
                }
                return result;
            }
        }
    }
    Err(WriterError::RestoreAuthorizationChanged)
}

#[cfg(test)]
pub(super) async fn restore_boundary(config: &Config, lease: &LeaseGuard) -> Result<()> {
    let mut history = commit::RestoreHistory::new(config)?;
    history.refresh(config, &lease.head().await?).await?;
    restore_boundary_cached(config, lease, &mut history).await
}

// A fresh complete listing is mandatory, even when a cursor advance failed.
// Quiet boundaries stop here, before history, markers, grouping or publication.
pub(super) async fn cached_drain_authorization(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
    snapshot: bool,
) -> Result<usize> {
    let pending = history.pending(config, true).await?;
    if pending.is_empty() {
        return Ok(0);
    }
    commit::drain_cached_authorization(config, lease, history, pending, snapshot).await
}

pub(super) async fn restore_boundary_cached(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
) -> Result<()> {
    if cached_drain_authorization(config, lease, history, true).await? != 0 {
        return Err(WriterError::RestoreAuthorizationChanged);
    }
    lease.head().await?;
    Ok(())
}

pub(super) async fn command_start(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
    snapshot: bool,
) -> Result<ServingHead> {
    history.refresh(config, &lease.head().await?).await?;
    command_recover(config, lease, history, snapshot).await
}

// A failed maintenance step must yield to fresh authorization discovery before
// reading new publication history or recovering its unpublished native suffix.
pub(super) async fn command_recover(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut commit::RestoreHistory,
    snapshot: bool,
) -> Result<ServingHead> {
    cached_drain_authorization(config, lease, history, snapshot).await?;
    commit::commit_one(config, false, lease, Vec::new(), Some(history)).await?;
    lease.head().await
}

pub(super) fn restore_deadline(config: &Config, start: std::time::Instant) -> Result<()> {
    // Cooperative step deadline: bounds when another group may start, not the
    // wall time of a native mutation or storage call. Lance has no public join;
    // we await every mutation before checking the deadline/recovering its suffix.
    // Authorization waits during that one native call; this is not a hard
    // interruption bound on index optimization or any other native operation.
    if start.elapsed() >= config.writer.maintenance.step_max_duration.as_duration() {
        return Err(WriterError::MaintenanceStepDeadline);
    }
    Ok(())
}

#[cfg(test)]
pub(super) async fn restore_read(
    config: &Config,
    dataset: &Dataset,
    source: &str,
    roots: &[String],
    authority: bool,
) -> Result<Vec<arrow_array::RecordBatch>> {
    let mut scan = dataset.scan();
    scan.filter(&apply::keys_filter(source, roots))
        .map_err(|_| WriterError::RestoreVerificationFailed)?;
    if authority {
        scan.project(&[
            "root_id",
            "metadata_json",
            "source_revision",
            "record_id",
            "readers",
        ])
        .map_err(|_| WriterError::RestoreVerificationFailed)?;
    }
    scan.batch_size(RESTORE_ROOTS)
        .batch_readahead(1)
        .fragment_readahead(1)
        .target_parallelism(1);
    let start = std::time::Instant::now();
    let mut stream = scan
        .try_into_stream()
        .await
        .map_err(|_| WriterError::RestoreVerificationFailed)?;
    restore_deadline(config, start)?;
    let mut rows = Vec::new();
    let mut count = 0u64;
    let mut bytes = apply::ROW_OVERHEAD_BYTES;
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|_| WriterError::RestoreVerificationFailed)?
    {
        restore_deadline(config, start)?;
        if batch.num_rows() == 0 {
            continue;
        }
        let (batch_count, batch_bytes) = apply::row_size(std::slice::from_ref(&batch))?;
        count = count
            .checked_add(batch_count)
            .ok_or(WriterError::EnvelopeBoundsInvalid)?;
        bytes = bytes
            .checked_add(batch_bytes - apply::ROW_OVERHEAD_BYTES)
            .ok_or(WriterError::EnvelopeBoundsInvalid)?;
        if count > config.writer.commit.group_max_rows
            || bytes > config.writer.commit.group_max_bytes.as_u64()
        {
            return Err(WriterError::EnvelopeBoundsInvalid);
        }
        restore_deadline(config, start)?;
        rows.push(batch);
    }
    Ok(rows)
}

/// Rewrites one complete root within a bounded page using current reader statements.
pub(super) fn restore_rows(
    config: &Config,
    source: &str,
    root: &str,
    rows: &[arrow_array::RecordBatch],
    authority: &RestoreAuthority,
    historical: bool,
) -> Result<Option<Vec<arrow_array::RecordBatch>>> {
    let Some(current) = authority.states.get(root) else {
        // Historical-only roots never inherit historical grants.
        return Ok(historical.then(Vec::new));
    };
    let mut output = Vec::new();
    let mut has_content = false;
    for batch in rows {
        for row in 0..batch.num_rows() {
            let state = apply::state(batch, row)?;
            has_content |= apply::strings(batch, "source_id")?.value(row) == source
                && !state.deleted
                && !state.content_purged;
        }
    }
    if current.deleted || current.content_purged || !has_content {
        let mut state = current.clone();
        if !state.deleted {
            state.content_purged = true;
        }
        output.push(apply::marker_row(
            config.embedding.dimensions,
            config.namespace.dense_element,
            source,
            root,
            &state,
        )?);
    } else {
        for batch in rows {
            for row in 0..batch.num_rows() {
                let mut state = apply::state(batch, row)?;
                state.permission.clone_from(&current.permission);
                let record = apply::strings(batch, "record_id")?.value(row);
                let mut tokens = authority
                    .readers
                    .get(&(root.to_owned(), record.to_owned()))
                    .cloned()
                    .unwrap_or_default();
                tokens.retain(|t| {
                    current
                        .permission
                        .as_ref()
                        .is_some_and(|p| p.readers.contains(t))
                });
                // Historical-only records must not regain historical section grants.
                let own = authority
                    .own_readers
                    .get(&(root.to_owned(), record.to_owned()))
                    .cloned()
                    .unwrap_or(Some(Vec::new()));
                let same = state == apply::state(batch, row)?
                    && tokens == apply::tokens(batch, row)?
                    && own == apply::own_readers(batch, row)?;
                output.push(if same {
                    batch.slice(row, 1)
                } else {
                    let restored = apply::with_own_readers(&batch.slice(row, 1), own.as_deref())?;
                    apply::with_state(&restored, &state, &tokens)?
                });
            }
        }
    }
    Ok(Some(output))
}

pub(super) fn restore_verify(
    source: &str,
    rows: &[arrow_array::RecordBatch],
    authority: &RestoreAuthority,
) -> Result<()> {
    for batch in rows {
        for row in 0..batch.num_rows() {
            let root = apply::strings(batch, "root_id")?.value(row);
            let current = authority
                .states
                .get(root)
                .ok_or(WriterError::RestoreVerificationFailed)?;
            let tokens = apply::tokens(batch, row)?;
            if apply::strings(batch, "source_id")?.value(row) == apply::state_source(source) {
                if !tokens.is_empty() {
                    return Err(WriterError::RestoreVerificationFailed);
                }
            } else {
                let own_readers = apply::own_readers(batch, row)?;
                let record = apply::strings(batch, "record_id")?.value(row);
                if own_readers
                    != authority
                        .own_readers
                        .get(&(root.to_owned(), record.to_owned()))
                        .cloned()
                        .unwrap_or(Some(Vec::new()))
                    || current.deleted
                    || current.content_purged
                    || tokens.iter().any(|t| {
                        current
                            .permission
                            .as_ref()
                            .is_none_or(|p| !p.readers.contains(t))
                            || authority
                                .readers
                                .get(&(root.to_owned(), record.to_owned()))
                                .is_none_or(|allowed| !allowed.contains(t))
                    })
                {
                    return Err(WriterError::RestoreVerificationFailed);
                }
            }
        }
    }
    Ok(())
}

pub(super) async fn restore_bodies(
    config: &Config,
    historical: &Dataset,
    addresses: &[u64],
    body_reads: &mut u64,
    mut boundary: Option<(&LeaseGuard, &mut RestoreAttempt<'_>)>,
) -> Result<Vec<arrow_array::RecordBatch>> {
    // The pinned prepared schema has 1024 Float16 vector values per row.
    // This lower bound rejects certainly oversized roots without any body read.
    let minimum = (addresses.len() as u64)
        .checked_mul(2048)
        .and_then(|n| n.checked_add(apply::ROW_OVERHEAD_BYTES))
        .ok_or(WriterError::EnvelopeBoundsInvalid)?;
    if addresses.len() as u64 > config.writer.commit.group_max_rows
        || minimum > config.writer.commit.group_max_bytes.as_u64()
    {
        return Err(WriterError::EnvelopeBoundsInvalid);
    }
    let dataset = std::sync::Arc::new(historical.clone());
    let projection = lance::dataset::ProjectionRequest::from(historical.schema().clone())
        .into_projection_plan(dataset.clone())
        .map_err(|_| WriterError::RestoreVerificationFailed)?;
    let projection = std::sync::Arc::new(projection);
    let mut rows = Vec::new();
    let start = std::time::Instant::now();
    for address in addresses {
        if let Some((lease, attempt)) = boundary.as_mut() {
            attempt
                .cadence
                .maybe(config, lease, attempt.history)
                .await?;
        }
        let batch = lance::dataset::TakeBuilder::try_new_from_addresses(
            dataset.clone(),
            vec![*address],
            projection.clone(),
        )
        .map_err(|_| WriterError::RestoreVerificationFailed)?
        .execute()
        .await
        .map_err(|_| WriterError::RestoreVerificationFailed)?;
        *body_reads += 1;
        if batch.num_rows() != 1 {
            return Err(WriterError::RestoreVerificationFailed);
        }
        rows.push(batch);
        apply::check_row_bounds(config, &rows)?;
        restore_deadline(config, start)?;
    }
    Ok(rows)
}

pub(super) async fn restore_pass(
    config: &Config,
    lease: &LeaseGuard,
    attempt: &mut RestoreAttempt<'_>,
    historical: &Dataset,
    index: &mut restore_index::Index,
) -> Result<(u64, u64)> {
    let mut after = (String::new(), String::new());
    let mut root_count = 0;
    let mut body_reads = 0;
    loop {
        attempt
            .cadence
            .maybe(config, lease, attempt.history)
            .await?;
        let roots = index.roots(&after)?;
        let Some(last) = roots.last() else {
            break;
        };
        after = last.clone();
        root_count += roots.len() as u64;
        let mut sources = BTreeMap::<String, Vec<String>>::new();
        for (source, root) in roots {
            sources.entry(source).or_default().push(root);
        }
        for (source, roots) in sources {
            let mut pages = vec![roots];
            while let Some(mut roots) = pages.pop() {
                attempt
                    .cadence
                    .maybe(config, lease, attempt.history)
                    .await?;
                let start = std::time::Instant::now();
                let page = {
                    let index = &mut *index;
                    let roots = &roots;
                    let source = &source;
                    let body_reads = &mut body_reads;
                    let attempt = &mut *attempt;
                    async move {
                        let mut keys = Vec::new();
                        let mut output = Vec::new();
                        for root in roots {
                            attempt
                                .cadence
                                .maybe(config, lease, attempt.history)
                                .await?;
                            if index.retired.contains(source) {
                                keys.push(root.clone());
                                continue;
                            }
                            let authority = index.authority(config, source, root)?;
                            let metadata = index.rows(config, 1, source, root)?;
                            if restore_index::unchanged(source, root, &metadata, &authority) {
                                continue;
                            }
                            let mut rows = Vec::new();
                            // Addresses belong to the immutable target, never a mutated suffix.
                            if !metadata.is_empty()
                                && metadata.iter().all(|r| {
                                    r.source == *source
                                        && !r.state.deleted
                                        && !r.state.content_purged
                                })
                                && authority
                                    .states
                                    .get(root)
                                    .is_some_and(|s| !s.deleted && !s.content_purged)
                            {
                                rows = restore_bodies(
                                    config,
                                    historical,
                                    &metadata.iter().map(|r| r.address).collect::<Vec<_>>(),
                                    body_reads,
                                    Some((lease, &mut *attempt)),
                                )
                                .await?;
                            }
                            if let Some(rows) = restore_rows(
                                config,
                                source,
                                root,
                                &rows,
                                &authority,
                                !metadata.is_empty(),
                            )? {
                                keys.push(root.clone());
                                output.extend(rows);
                                apply::check_row_bounds(config, &output)?;
                            }
                        }
                        Ok::<_, WriterError>((keys, output))
                    }
                    .await
                };
                restore_deadline(config, start)?;
                let (keys, rows) = match page {
                    Err(WriterError::EnvelopeBoundsInvalid) if roots.len() > 1 => {
                        let second = roots.split_off(roots.len() / 2);
                        pages.push(second);
                        pages.push(roots);
                        continue;
                    }
                    result => result?,
                };
                if !keys.is_empty() {
                    let fragments = apply::write_rows(config, rows).await?;
                    // Body preparation may await storage. Discover revokes again
                    // immediately before every changed-root native commit.
                    attempt
                        .cadence
                        .drain(config, lease, attempt.history)
                        .await?;
                    commit::apply_staged(
                        config,
                        &mut attempt.dataset,
                        lease,
                        &mut attempt.work,
                        vec![apply::keys_filter(&source, &keys)],
                        fragments,
                    )
                    .await?;
                    #[cfg(test)]
                    restore_test_group();
                }
                restore_deadline(config, start)?;
            }
        }
    }
    Ok((root_count, body_reads))
}

pub(super) async fn restore_attempt(
    config: &Config,
    lease: &LeaseGuard,
    head: ServingHead,
    target: &ContentManifest,
    history: &mut commit::RestoreHistory,
) -> Result<serde_json::Value> {
    let current = pinned(config, &head).await?;
    let (store, root) = namespace_store(config)?;
    let mut latest = BTreeMap::new();
    let mut sources: BTreeSet<_> = config.sources.iter().map(|s| s.id.clone()).collect();
    let policy: fyrr_core::publication::PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    sources.extend(policy.retired_sources.keys().cloned());
    for envelope in history.summaries() {
        sources.insert(envelope.source_id.clone());
        let key = (envelope.source_id.clone(), envelope.kind.as_str());
        if latest
            .get(&key)
            .is_none_or(|previous: &RejectionMarker| previous.sequence < envelope.sequence)
        {
            latest.insert(
                key,
                RejectionMarker {
                    schema_version: fyrr_core::envelope::ENVELOPE_CONTRACT_VERSION,
                    envelope_id: envelope.envelope_id.clone(),
                    source_id: envelope.source_id.clone(),
                    kind: envelope.kind,
                    sequence: envelope.sequence,
                    rejection_generation: envelope.rejection_generation,
                    restore_sequence: head.content.as_ref().map_or(1, |p| p.sequence + 1),
                    start_cursor: None,
                    reason: "restore_rewind".into(),
                },
            );
        }
    }
    restore_boundary_cached(config, lease, history).await?;
    let cadence = RestoreCadence::new(config);
    let start = std::time::Instant::now();
    let historical = current
        .checkout_version(target.lance_version)
        .await
        .map_err(|_| WriterError::RestoreHorizonExpired)?;
    let mut fragments = restore_fragments::Difference::new(&current, &historical);
    if !policy.retired_sources.is_empty() {
        // Retirement can delete rows from otherwise shared files. Index their
        // metadata too: deleting a retired row can change a mixed fragment that
        // verification must still check against the active rows' authority.
        // Bodies remain lazy and changed roots still use bounded groups.
        fragments.current = current.manifest().fragments.iter().map(|f| f.id).collect();
        fragments.historical = historical
            .manifest()
            .fragments
            .iter()
            .map(|f| f.id)
            .collect();
        fragments.fallback = Some("retired_sources");
    }
    let mut index =
        restore_index::Index::new(config, &sources, fragments.rows(&current, &historical)?)?;
    index.retired = policy.retired_sources.keys().cloned().collect();
    let mut dataset = historical.clone();
    let mut work = lineage(lease, &head);
    commit::native_step(lease, dataset.restore(), WriterError::RestoreCommitFailed).await?;
    commit::record_transaction(&dataset, &mut work, current.version().version).await?;
    fragments.restored(&current, &historical, &dataset);
    index.reserve(fragments.rows(&current, &historical)?)?;
    restore_deadline(config, start)?;
    let mut attempt = RestoreAttempt {
        head,
        dataset,
        work,
        history,
        cadence,
    };
    index
        .scan(config, lease, &mut attempt, &current, 0, &fragments.current)
        .await?;
    index
        .scan(
            config,
            lease,
            &mut attempt,
            &historical,
            1,
            &fragments.historical,
        )
        .await?;

    let (roots, body_reads) = Box::pin(restore_pass(
        config,
        lease,
        &mut attempt,
        &historical,
        &mut index,
    ))
    .await?;
    let verify_fragments = fragments.verification(&attempt.dataset);
    index
        .verify(config, lease, &mut attempt, &verify_fragments)
        .await?;
    attempt
        .cadence
        .drain(config, lease, attempt.history)
        .await?;
    let start = std::time::Instant::now();

    // The settings hold the attributes of now: a version from before one was
    // added has no column for it, and gets one that reads null.
    Box::pin(commit::add_attribute_columns(
        config,
        lease,
        &mut attempt.dataset,
        &mut attempt.work,
    ))
    .await?;
    let before = attempt.dataset.version().version;
    commit::native_step(
        lease,
        attempt.dataset.optimize_indices(&OptimizeOptions::append()),
        WriterError::IndexBuildFailed,
    )
    .await?;
    commit::record_transaction(&attempt.dataset, &mut attempt.work, before).await?;
    restore_deadline(config, start)?;
    attempt
        .cadence
        .drain(config, lease, attempt.history)
        .await?;
    let (version, cadence) = publish_restored(
        config,
        lease,
        store.as_ref(),
        &root,
        attempt,
        &latest,
        target,
    )
    .await?;
    Ok(
        serde_json::json!({"restored":true,"lance_version":version,"from_publication":target.publication_id,
            "restore_scans":{"authority":index.scans[0],"historical":index.scans[1],"verification":index.scans[2]},
            "restore_scan_reads":{"authority":index.reads[0],"historical":index.reads[1],"verification":index.reads[2]},
            "restore_roots":roots,"restore_body_reads":body_reads,
            "restore_fragments":{"authority":fragments.current.len(),"historical":fragments.historical.len(),"verification":verify_fragments.len(),"full_fallback":fragments.fallback},
            "restore_working_bytes":index.bytes()?,"restore_working_cap_bytes":index.cap,
            "restore_cadence_ms":cadence.as_millis()}),
    )
}

async fn publish_restored(
    config: &Config,
    lease: &LeaseGuard,
    store: &dyn object_store::ObjectStore,
    root: &Path,
    mut attempt: RestoreAttempt<'_>,
    latest: &BTreeMap<(String, &'static str), RejectionMarker>,
    target: &ContentManifest,
) -> Result<(u64, std::time::Duration)> {
    let start = std::time::Instant::now();

    // Authorization inputs arriving here are drained at the final boundary;
    // they must never be rejected as stale content and hide a queued revoke.
    for envelope in attempt.history.pending(config, false).await? {
        if envelope.kind.is_authorization() {
            continue;
        }
        if !apply::rejected(config, &envelope.envelope_id).await? {
            apply::reject(config, &envelope, "restore_refetch").await?;
        }
        attempt
            .head
            .rejected_envelope_ids
            .push(envelope.envelope_id);
    }
    attempt.head.rejected_envelope_ids.sort();
    attempt.head.rejected_envelope_ids.dedup();
    let mut rewinds = Vec::new();
    for marker in latest.values() {
        let hash = fyrr_reader::embedding::digest(&fyrr_core::write_contract(marker));
        let path = format!("rejections/restore-{}.json", &hash[7..]);
        prepare::put_immutable(store, &Path::from(format!("{root}/{path}")), marker).await?;
        rewinds.push(path);
    }
    restore_deadline(config, start)?;
    attempt
        .cadence
        .drain(config, lease, attempt.history)
        .await?;
    restore_deadline(config, start)?;
    let version = commit::publish(
        config,
        lease,
        &attempt.dataset,
        attempt.head,
        attempt.work,
        Vec::new(),
        true,
        rewinds,
        false,
        // The indexes stand as the restored publication built them.
        Some(&target.attribute_indexes),
    )
    .await?;
    // Service authorization queued during final publication under this same lease.
    // The restore is already visible, so this drain must not restart the attempt.
    cached_drain_authorization(config, lease, attempt.history, true).await?;
    Ok((version, attempt.cadence.interval))
}
