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
        let applied: std::collections::BTreeSet<_> = published
            .applied
            .union(&published.rejected)
            .cloned()
            .collect();
        let (store, root) = fyrr_reader::storage::namespace_store(self.config)?;
        // Always discover new envelopes; only immutable, fingerprinted bodies are reused.
        let objects: Vec<_> = self
            .rt
            .block_on(store.list(Some(&root.join("envelopes"))).try_collect())?;
        let mut content_count = 0;
        let mut content_bytes = 0;
        for object in objects {
            let envelope = cached_envelope(self.rt, store.as_ref(), index, &object)?;
            let path = object.location;
            if !path
                .filename()
                .and_then(|name| name.strip_suffix(".json"))
                .is_some_and(|id| applied.contains(id))
                && let EnvelopePayload::Content(payload) = &envelope.payload
            {
                content_count += 1;
                content_bytes += payload
                    .fragments
                    .iter()
                    .flat_map(|f| &f.files)
                    .map(|f| f.size_bytes)
                    .sum::<u64>();
            }
            if envelope.namespace != self.config.namespace.name {
                return Err(WriterError::EnvelopeNamespaceInvalid);
            }
            if envelope.source_id == self.source.id
                && envelope.kind == SyncKind::Content
                && !applied.contains(&envelope.envelope_id)
            {
                content_paths.push(path.clone());
            }
            // A permissions envelope counts only once the cursor confirmed it: the
            // cursor holds what a read needs of them, and the committer reads
            // their chain.
            if envelope.source_id == self.source.id
                && envelope.kind == self.kind
                && envelope.kind != SyncKind::Permissions
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
                if !applied.contains(&envelope.envelope_id) {
                    paths.push(path);
                }
            }
        }
        Ok(Pending {
            paths,
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

/// The envelope `object` holds, read once a command: a body whose object has an
/// etag or a version is kept in the command's evidence, and read from there.
fn cached_envelope(
    rt: &tokio::runtime::Runtime,
    store: &dyn object_store::ObjectStore,
    index: &crate::commit::history::index::Index,
    object: &object_store::ObjectMeta,
) -> Result<PreparedEnvelope> {
    let stamp = serde_json::to_string(&(
        object.e_tag.as_ref(),
        object.version.as_ref(),
        object.size,
        object.last_modified,
    ))
    .map_err(|_| WriterError::PublicationInvalid)?;
    let cached: Option<(String, PreparedEnvelope)> = index.get(
        crate::commit::history::index::Evidence::Preparation,
        object.location.as_ref(),
    )?;
    if let Some((old, envelope)) = cached {
        if old != stamp {
            return Err(WriterError::RestoreHistoryIdentityChanged);
        }
        return Ok(envelope);
    }
    let envelope: PreparedEnvelope =
        rt.block_on(fyrr_reader::storage::read_contract(store, &object.location))?;
    if object.e_tag.is_some() || object.version.is_some() {
        index.put(
            crate::commit::history::index::Evidence::Preparation,
            object.location.as_ref(),
            &(stamp, &envelope),
            false,
        )?;
    }
    Ok(envelope)
}

/// The permissions cursor of `source` as the log allows a worker to take it,
/// checked before anything is written: one that is not there is lost when a
/// chain is left that it confirmed (a publication applied some of it, or a
/// chained envelope of it waits), and one that is there is older than its log
/// when an envelope of a later lease than its own is in it. Either refuses the
/// read with `permission_cursor_lost`, unless the cursor moved meanwhile, as
/// another worker's read ends: that is `sync_busy`. A new cursor takes a lease
/// generation above every one in the log. The envelopes are read through the
/// command's evidence, as the preparation reads them.
pub(super) fn checked_cursor(
    config: &fyrr_core::config::Config,
    source: &str,
    rt: &tokio::runtime::Runtime,
    evidence: &mut CommandEvidence,
) -> Result<crate::sync_lease::Checked> {
    let peek = || {
        rt.block_on(crate::sync_lease::CursorLease::peek(
            config,
            source,
            SyncKind::Permissions,
        ))
    };
    let cursor = peek()?;
    let head = rt
        .block_on(fyrr_reader::storage::optional_head(config))?
        .ok_or(WriterError::NamespaceUnavailable)?;
    evidence.refresh(config, rt, &head)?;
    let Snapshot {
        published, index, ..
    } = evidence
        .snapshot
        .as_ref()
        .ok_or(WriterError::PublicationInvalid)?;
    let (store, root) = fyrr_reader::storage::namespace_store(config)?;
    let mut lost = cursor.is_none()
        && rt
            .block_on(crate::commit::previous_manifest(
                store.as_ref(),
                &root,
                &head,
            ))?
            .is_some_and(|m| m.authorization_applied.contains_key(source));
    let epoch = cursor
        .as_ref()
        .and_then(|c| c.lease.as_ref())
        .map_or(0, |lease| lease.epoch);
    let mut floor = 0;
    if !lost {
        let objects: Vec<_> =
            rt.block_on(store.list(Some(&root.join("envelopes"))).try_collect())?;
        for object in objects {
            let envelope = cached_envelope(rt, store.as_ref(), index, &object)?;
            if envelope.namespace != config.namespace.name {
                return Err(WriterError::EnvelopeNamespaceInvalid);
            }
            if envelope.source_id != source
                || envelope.kind != SyncKind::Permissions
                || envelope.lease_generation == 0
            {
                continue;
            }
            floor = floor.max(envelope.lease_generation);
            lost |= match &cursor {
                None => {
                    !published.applied.contains(&envelope.envelope_id)
                        && !published.rejected.contains(&envelope.envelope_id)
                }
                Some(_) => envelope.lease_generation > epoch,
            };
            // The first proof is enough; the cursor is read again at once to
            // tell a lost one from one that a read moved meanwhile.
            if lost {
                break;
            }
        }
    }
    if lost {
        return Err(if peek()? == cursor {
            WriterError::PermissionCursorLost
        } else {
            WriterError::SyncBusy
        });
    }
    Ok(crate::sync_lease::Checked {
        state: cursor,
        floor,
    })
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
