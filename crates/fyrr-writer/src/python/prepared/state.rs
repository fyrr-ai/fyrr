//! Pending envelope discovery, cursor recovery and preparation gates.
use super::*;

pub(super) struct Pending {
    pub(super) paths: Vec<Path>,
    pub(super) content_paths: Vec<Path>,
    pub(super) cursors: Vec<(u64, Option<String>)>,
    pub(super) generation: u32,
    pub(super) restore_sequence: u64,
    pub(super) members_tail: Option<(u64, bool)>,
    pub(super) store: Arc<dyn ObjectStore>,
}

impl Preparation<'_> {
    pub(super) fn scan(&self, head: &ServingHead) -> Result<Pending> {
        let mut paths = Vec::new();
        let mut content_paths = Vec::new();
        let mut durable_cursors = Vec::new();
        let mut durable_generation = 0;
        let mut durable_restore_sequence = 0;
        let mut members_tail = None;
        let (envelope_paths, applied) = self
            .rt
            .block_on(crate::commit::envelope_paths(self.config, head))?;
        let (store, _) = fyrr_reader::storage::namespace_store(self.config)?;
        for path in envelope_paths {
            let envelope: PreparedEnvelope = self
                .rt
                .block_on(fyrr_reader::storage::read_contract(store.as_ref(), &path))?;
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
        })
    }

    pub(super) fn cursor_state(
        &self,
        head: &ServingHead,
        durable: &Pending,
        due: bool,
    ) -> Result<(CursorState, bool)> {
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
        let markers = self
            .rt
            .block_on(crate::commit::rejection_markers(self.config, head))?
            .into_iter()
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
                (chrono::Utc::now() - state.updated_at)
                    .to_std()
                    .is_ok_and(|elapsed| elapsed < i.as_duration())
            })
        {
            return Ok((state, true));
        }
        Ok((state, false))
    }

    pub(super) fn backpressure(&self, head: &ServingHead, full: bool) -> Result<()> {
        if self.kind == SyncKind::Content && !full {
            let pending = self
                .rt
                .block_on(crate::commit::pending(self.config, head))?;
            let count = pending
                .iter()
                .filter(|e| e.kind == SyncKind::Content)
                .count() as u64;
            let bytes = pending
                .iter()
                .filter_map(|e| match &e.payload {
                    EnvelopePayload::Content(p) => Some(
                        p.fragments
                            .iter()
                            .flat_map(|f| &f.files)
                            .map(|f| f.size_bytes)
                            .sum::<u64>(),
                    ),
                    _ => None,
                })
                .sum::<u64>();
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
