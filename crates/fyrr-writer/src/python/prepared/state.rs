//! Pending envelope discovery, cursor recovery and preparation gates.
use super::*;
use futures::TryStreamExt;
use fyrr_core::envelope::RejectionMarker;

#[derive(Default)]
pub(crate) struct CommandEvidence {
    snapshot: Option<Snapshot>,
}

struct Snapshot {
    identity: ServingHead,
    published: crate::commit::PublishedEvidence,
    markers: Vec<RejectionMarker>,
    index: crate::commit::history::index::Index,
}

/// The head without what does not change publication evidence: the lease and
/// its renewal timestamps.
pub(crate) fn served_identity(head: &ServingHead) -> ServingHead {
    let mut identity = head.clone();
    identity.lease = None;
    identity.updated_at = chrono::DateTime::UNIX_EPOCH;
    identity
}

impl CommandEvidence {
    fn refresh(
        &mut self,
        config: &fyrr_core::config::Config,
        rt: &tokio::runtime::Runtime,
        head: &ServingHead,
    ) -> Result<()> {
        let identity = served_identity(head);
        if self
            .snapshot
            .as_ref()
            .is_some_and(|old| old.identity == identity && old.index.is_valid())
        {
            return Ok(());
        }
        self.snapshot = None;
        let published = rt.block_on(crate::commit::published_evidence(config, head))?;
        let markers = rt.block_on(crate::commit::markers_from_evidence(config, &published))?;
        let index = crate::commit::history::index::Index::command(config)?;
        self.snapshot = Some(Snapshot {
            identity,
            published,
            markers,
            index,
        });
        Ok(())
    }

    /// The rejection markers and cursor rewinds the head's publications record.
    fn markers(
        &mut self,
        config: &fyrr_core::config::Config,
        rt: &tokio::runtime::Runtime,
        head: &ServingHead,
    ) -> Result<&[RejectionMarker]> {
        self.refresh(config, rt, head)?;
        Ok(&self
            .snapshot
            .as_ref()
            .ok_or(WriterError::PublicationInvalid)?
            .markers)
    }
}

fn interval(
    source: &fyrr_core::config::SourceConfig,
    kind: SyncKind,
) -> Option<std::time::Duration> {
    match kind {
        SyncKind::Content => Some(source.intervals.content),
        SyncKind::Permissions => source.intervals.permissions,
        SyncKind::Members => source.intervals.members,
        SyncKind::Reconcile => Some(source.intervals.reconcile),
    }
    .map(|i| i.as_duration())
}

/// The markers a read of `kind` has not yet consumed.
fn unconsumed<'a>(
    markers: &'a [RejectionMarker],
    source: &str,
    kind: SyncKind,
    state: &CursorState,
) -> Vec<&'a RejectionMarker> {
    markers
        .iter()
        .filter(|m| {
            m.source_id == source
                && m.kind == kind
                && (m.rejection_generation >= state.rejection_generation
                    || m.restore_sequence > state.restore_sequence)
        })
        .collect()
}

/// Whether the last completed read is younger than the kind's interval.
fn within_interval(state: &CursorState, interval: Option<std::time::Duration>) -> bool {
    interval.is_some_and(|i| {
        (fyrr_core::clock::now() - state.updated_at)
            .to_std()
            .is_ok_and(|elapsed| elapsed < i)
    })
}

/// When the schedule makes `kind` due, from its stored cursor.
pub(super) fn next_due(
    source: &fyrr_core::config::SourceConfig,
    kind: SyncKind,
    state: &CursorState,
) -> Option<chrono::DateTime<chrono::Utc>> {
    state
        .updated_at
        .checked_add_signed(chrono::TimeDelta::from_std(interval(source, kind)?).ok()?)
}

/// When `kind` of `source` is next due, if the head and the cursor alone show
/// that it is not due now. Reads only: no lease, no listing of envelopes.
/// `None` when it may be due; the cursor lease then decides.
pub(super) fn idle_until(
    config: &fyrr_core::config::Config,
    source: &fyrr_core::config::SourceConfig,
    kind: SyncKind,
    rt: &tokio::runtime::Runtime,
    evidence: &mut CommandEvidence,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let Some(head) = rt.block_on(fyrr_reader::storage::optional_head(config))? else {
        return Ok(None);
    };
    let Some(state) = rt.block_on(crate::sync_lease::CursorLease::peek(
        config, &source.id, kind,
    ))?
    else {
        return Ok(None);
    };
    // shortcut: whether a permission or member recovery is unfinished is known
    // from the envelopes, which are not listed here; such a kind after any
    // rejection takes the lease to decide. Upgrade: record the completed
    // generation in the cursor.
    if matches!(kind, SyncKind::Permissions | SyncKind::Members) && state.rejection_generation > 0 {
        return Ok(None);
    }
    // A cursor behind its envelopes (a worker that died after publishing one)
    // has the lower generation: it can only find more markers, never fewer.
    if !unconsumed(
        evidence.markers(config, rt, &head)?,
        &source.id,
        kind,
        &state,
    )
    .is_empty()
        || !within_interval(&state, interval(source, kind))
    {
        return Ok(None);
    }
    Ok(next_due(source, kind, &state))
}

pub(super) struct Pending {
    pub(super) paths: Vec<Path>,
    /// Whether an envelope of the source, of any kind, is in the log and no
    /// publication applied or rejected it.
    pub(super) source_pending: bool,
    pub(super) content_paths: Vec<Path>,
    pub(super) cursors: Vec<(u64, Option<String>)>,
    pub(super) generation: u32,
    pub(super) restore_sequence: u64,
    /// The newest rejection generation that a complete-read envelope ended.
    pub(super) completed_generation: u32,
    pub(super) members_tail: Option<(u64, bool)>,
    pub(super) store: Arc<dyn ObjectStore>,
    content_count: u64,
    content_bytes: u64,
    markers: Vec<RejectionMarker>,
}

impl Preparation<'_> {
    pub(super) fn scan(
        &self,
        evidence: &mut CommandEvidence,
        head: &ServingHead,
    ) -> Result<Pending> {
        let mut paths = Vec::new();
        let mut content_paths = Vec::new();
        let mut durable_cursors = Vec::new();
        let mut durable_generation = 0;
        let mut durable_restore_sequence = 0;
        let mut members_tail = None;
        let mut completed_generation = 0;
        let mut source_pending = false;
        evidence.refresh(self.config, self.rt, head)?;
        let Snapshot {
            published,
            markers,
            index,
            ..
        } = evidence
            .snapshot
            .as_ref()
            .ok_or(WriterError::PublicationInvalid)?;
        // The worker walks no chain: a permissions envelope that no publication
        // resolved is unconfirmed to it.
        let (retired, chains) = (Default::default(), Default::default());
        let facts = crate::commit::Facts {
            applied: &published.applied,
            rejected: &published.rejected,
            retired: &retired,
            chains: &chains,
        };
        let (store, root) = fyrr_reader::storage::namespace_store(self.config)?;
        // Always discover new envelopes; only immutable, fingerprinted bodies are reused.
        let objects: Vec<_> = self
            .rt
            .block_on(store.list(Some(&root.clone().join("envelopes"))).try_collect())?;
        let mut content_count = 0;
        let mut content_bytes = 0;
        // The content of this source, decided once the chain its cursor
        // confirmed is known: (path, ID, previous, bytes, resolved).
        let mut own_content = Vec::new();
        for object in objects {
            let path = object.location;
            let stamp = serde_json::to_string(&(
                object.e_tag.as_ref(),
                object.version.as_ref(),
                object.size,
                object.last_modified,
            ))
            .map_err(|_| WriterError::PublicationInvalid)?;
            let cached: Option<(String, PreparedEnvelope)> = index.get(
                crate::commit::history::index::Evidence::Preparation,
                path.as_ref(),
            )?;
            let envelope = if let Some((old, envelope)) = cached {
                if old != stamp {
                    return Err(WriterError::RestoreHistoryIdentityChanged);
                }
                envelope
            } else {
                let envelope: PreparedEnvelope = self
                    .rt
                    .block_on(fyrr_reader::storage::read_contract(store.as_ref(), &path))?;
                if object.e_tag.is_some() || object.version.is_some() {
                    index.put(
                        crate::commit::history::index::Evidence::Preparation,
                        path.as_ref(),
                        &(stamp, &envelope),
                        false,
                    )?;
                }
                envelope
            };
            let resolved = matches!(
                crate::commit::classify(&envelope, &facts),
                crate::commit::State::Applied | crate::commit::State::Rejected
            );
            if envelope.namespace != self.config.namespace.name {
                return Err(WriterError::EnvelopeNamespaceInvalid);
            }
            let bytes = match &envelope.payload {
                EnvelopePayload::Content(payload) => payload
                    .fragments
                    .iter()
                    .flat_map(|f| &f.files)
                    .map(|f| f.size_bytes)
                    .sum::<u64>(),
                _ => 0,
            };
            if envelope.source_id == self.source.id && envelope.kind == SyncKind::Content {
                own_content.push((
                    path,
                    envelope.envelope_id,
                    envelope.previous,
                    bytes,
                    resolved,
                ));
                continue;
            }
            if !resolved && envelope.kind == SyncKind::Content {
                content_count += 1;
                content_bytes += bytes;
            }
            if envelope.source_id == self.source.id && !resolved {
                source_pending = true;
            }
            // A chained envelope counts only once the cursor confirmed it: the
            // cursor holds what a read needs of them, and the committer reads
            // their chain.
            if envelope.source_id == self.source.id
                && envelope.kind == self.kind
                && !crate::commit::chained(envelope.kind)
            {
                durable_generation = durable_generation.max(envelope.rejection_generation);
                durable_restore_sequence = durable_restore_sequence.max(envelope.restore_sequence);
                if matches!(
                    envelope.payload,
                    EnvelopePayload::Permissions(_) | EnvelopePayload::Members(_)
                ) && crate::apply::complete_read(&envelope)
                {
                    completed_generation = completed_generation.max(envelope.rejection_generation);
                }
                if let EnvelopePayload::Members(part) = &envelope.payload
                    && !part.complete_read
                    && members_tail.is_none_or(|(sequence, _)| envelope.sequence > sequence)
                {
                    members_tail = Some((envelope.sequence, part.last));
                }
                durable_cursors.push((
                    envelope.sequence,
                    if envelope.advances_cursor {
                        Some(envelope.cursor.end)
                    } else {
                        envelope.cursor.start
                    },
                ));
                if !resolved {
                    paths.push(path);
                }
            }
        }
        // Content of this source counts only on the chain from its cursor: an
        // envelope that no cursor write confirmed never publishes.
        if own_content.iter().any(|(.., resolved)| !resolved) {
            let state = if self.kind == SyncKind::Content {
                Some(self.rt.block_on(self.lease.state())?)
            } else {
                self.rt.block_on(crate::sync_lease::CursorLease::peek(
                    self.config,
                    &self.source.id,
                    SyncKind::Content,
                ))?
            };
            let links: BTreeMap<_, _> = own_content
                .iter()
                .map(|(_, id, previous, ..)| (id.clone(), previous.clone()))
                .collect();
            let mut on_chain = std::collections::BTreeSet::new();
            let mut next = state.and_then(|state| state.last).map(|last| last.id);
            while let Some(id) = next.take() {
                if !on_chain.insert(id.clone()) {
                    break;
                }
                if let Some(previous) = links.get(&id) {
                    next.clone_from(previous);
                    continue;
                }
                // A link the cursor confirmed after the listing is read by ID,
                // as the committer reads it.
                let path = root.clone().join("envelopes").join(format!("{id}.json"));
                let bytes = match self.rt.block_on(async {
                    object_store::ObjectStoreExt::get(store.as_ref(), &path)
                        .await?
                        .bytes()
                        .await
                }) {
                    Ok(bytes) => bytes,
                    Err(object_store::Error::NotFound { .. }) => break,
                    Err(_) => return Err(WriterError::StorageUnavailable),
                };
                let envelope: PreparedEnvelope = fyrr_core::parse_contract(&bytes)
                    .map_err(|_| WriterError::PublicationInvalid)?;
                if envelope.envelope_id != id
                    || matches!(
                        crate::commit::classify(&envelope, &facts),
                        crate::commit::State::Applied | crate::commit::State::Rejected
                    )
                {
                    break;
                }
                let EnvelopePayload::Content(payload) = &envelope.payload else {
                    break;
                };
                own_content.push((
                    path,
                    id,
                    envelope.previous.clone(),
                    payload
                        .fragments
                        .iter()
                        .flat_map(|f| &f.files)
                        .map(|f| f.size_bytes)
                        .sum::<u64>(),
                    false,
                ));
                next = envelope.previous;
            }
            for (path, id, _, bytes, resolved) in own_content {
                if resolved || !on_chain.contains(&id) {
                    continue;
                }
                source_pending = true;
                content_count += 1;
                content_bytes += bytes;
                content_paths.push(path.clone());
                if self.kind == SyncKind::Content {
                    paths.push(path);
                }
            }
        }
        Ok(Pending {
            paths,
            source_pending,
            content_paths,
            cursors: durable_cursors,
            generation: durable_generation,
            restore_sequence: durable_restore_sequence,
            completed_generation,
            members_tail,
            store,
            content_count,
            content_bytes,
            markers: markers.clone(),
        })
    }

    /// The cursor state, whether the kind is not due, and whether this read
    /// replaces a rejected generation that no complete read has ended.
    pub(super) fn cursor_state(
        &self,
        durable: &Pending,
        due: bool,
    ) -> Result<(CursorState, bool, bool)> {
        let mut state = self.rt.block_on(self.lease.state())?;
        state.rejection_generation = state.rejection_generation.max(durable.generation);
        state.restore_sequence = state.restore_sequence.max(durable.restore_sequence);
        // Repair the cache from every durable envelope before checking due time.
        self.rt.block_on(crate::prepare::recover_cursor(
            self.lease,
            &mut state,
            &durable.cursors,
        ))?;
        let interval = interval(self.source, self.kind);
        let markers = unconsumed(&durable.markers, &self.source.id, self.kind, &state);
        if let Some(oldest) = markers.iter().min_by_key(|m| m.sequence) {
            state.cursor = if markers
                .iter()
                .any(|m| m.restore_sequence > state.restore_sequence)
            {
                None
            } else {
                oldest.start_cursor.clone()
            };
            state.restore_sequence = markers
                .iter()
                .map(|m| m.restore_sequence)
                .max()
                .unwrap_or(0)
                .max(state.restore_sequence);
            state.rejection_generation = markers
                .iter()
                .map(|m| m.rejection_generation)
                .fold(oldest.rejection_generation, std::cmp::max)
                .max(state.rejection_generation)
                .checked_add(1)
                .ok_or(WriterError::RejectionGenerationOverflow)?;
            self.rt.block_on(self.lease.write(state.clone()))?;
        }
        // A generation no complete-read envelope has ended is a replacement of a
        // rejection, also after an earlier attempt failed; the schedule never
        // delays an unfinished permission or member recovery.
        let completed = if self.kind == SyncKind::Permissions {
            state.completed_generation
        } else {
            durable.completed_generation
        };
        let replaces = state.rejection_generation > completed;
        let recovering = replaces && matches!(self.kind, SyncKind::Permissions | SyncKind::Members);
        if due && markers.is_empty() && !recovering && within_interval(&state, interval) {
            return Ok((state, true, false));
        }
        Ok((state, false, replaces))
    }

    pub(super) fn backpressure(&self, pending: &Pending, full: bool) -> Result<()> {
        if self.kind == SyncKind::Content && !full {
            let count = pending.content_count;
            let bytes = pending.content_bytes;
            if count
                >= self
                    .config
                    .writer
                    .backpressure
                    .max_pending_content_envelopes
                || bytes
                    >= self
                        .config
                        .writer
                        .backpressure
                        .max_pending_content_bytes
                        .as_u64()
            {
                return Err(WriterError::ContentBackpressure);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::history::index::Evidence;

    #[test]
    fn command_evidence_discards_bodies_when_head_identity_changes() {
        let (config, root) = crate::lease::tests::local_config("preparation-evidence");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut head: ServingHead = serde_json::from_str(include_str!(
            "../../../../../fixtures/contracts/serving-head/head.json"
        ))
        .unwrap();
        head.content = None;
        head.rejected_envelope_ids.clear();
        let mut evidence = CommandEvidence::default();
        evidence.refresh(&config, &rt, &head).unwrap();
        evidence
            .snapshot
            .as_ref()
            .unwrap()
            .index
            .put(Evidence::Preparation, "body", &1, false)
            .unwrap();
        head.updated_at += chrono::TimeDelta::seconds(1);
        evidence.refresh(&config, &rt, &head).unwrap();
        assert_eq!(
            evidence
                .snapshot
                .as_ref()
                .unwrap()
                .index
                .get::<u64>(Evidence::Preparation, "body")
                .unwrap(),
            Some(1)
        );
        evidence.snapshot.as_ref().unwrap().index.invalidate();
        evidence.refresh(&config, &rt, &head).unwrap();
        assert!(evidence.snapshot.as_ref().unwrap().index.is_valid());
        assert_eq!(
            evidence
                .snapshot
                .as_ref()
                .unwrap()
                .index
                .get::<u64>(Evidence::Preparation, "body")
                .unwrap(),
            None
        );
        head.incarnation.push('x');
        evidence.refresh(&config, &rt, &head).unwrap();
        assert_eq!(
            evidence
                .snapshot
                .as_ref()
                .unwrap()
                .index
                .get::<u64>(Evidence::Preparation, "body")
                .unwrap(),
            None
        );
        drop(evidence);
        let _ = std::fs::remove_dir_all(root);
    }
}
