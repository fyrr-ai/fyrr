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

impl CommandEvidence {
    fn refresh(
        &mut self,
        config: &fyrr_core::config::Config,
        rt: &tokio::runtime::Runtime,
        head: &ServingHead,
    ) -> Result<()> {
        let mut identity = head.clone();
        identity.lease = None;
        // Lease renewal timestamps do not change publication evidence.
        identity.updated_at = chrono::DateTime::UNIX_EPOCH;
        if self
            .snapshot
            .as_ref()
            .is_some_and(|old| old.identity == identity)
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
}

pub(super) struct Pending {
    pub(super) paths: Vec<Path>,
    pub(super) content_paths: Vec<Path>,
    pub(super) cursors: Vec<(u64, Option<String>)>,
    pub(super) generation: u32,
    pub(super) restore_sequence: u64,
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
            if envelope.source_id == self.source.id && envelope.kind == self.kind {
                durable_generation = durable_generation.max(envelope.rejection_generation);
                durable_restore_sequence = durable_restore_sequence.max(envelope.restore_sequence);
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
            members_tail,
            store,
            content_count,
            content_bytes,
            markers: markers.clone(),
        })
    }

    pub(super) fn cursor_state(&self, durable: &Pending, due: bool) -> Result<(CursorState, bool)> {
        let mut state = self.rt.block_on(self.lease.state())?;
        state.rejection_generation = state.rejection_generation.max(durable.generation);
        state.restore_sequence = state.restore_sequence.max(durable.restore_sequence);
        // Repair the cache from every durable envelope before checking due time.
        self.rt.block_on(crate::prepare::recover_cursor(
            self.lease,
            &mut state,
            &durable.cursors,
        ))?;
        let interval = match self.kind {
            SyncKind::Content => Some(self.source.intervals.content),
            SyncKind::Permissions => self.source.intervals.permissions,
            SyncKind::Members => self.source.intervals.members,
            SyncKind::Reconcile => Some(self.source.intervals.reconcile),
        };
        let markers = durable
            .markers
            .iter()
            .filter(|m| {
                m.source_id == self.source.id
                    && m.kind == self.kind
                    && (m.rejection_generation >= state.rejection_generation
                        || m.restore_sequence > state.restore_sequence)
            })
            .collect::<Vec<_>>();
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
        if due
            && markers.is_empty()
            && state.sequence > 0
            && interval.is_some_and(|i| {
                (fyrr_core::clock::now() - state.updated_at)
                    .to_std()
                    .is_ok_and(|elapsed| elapsed < i.as_duration())
            })
        {
            return Ok((state, true));
        }
        Ok((state, false))
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
