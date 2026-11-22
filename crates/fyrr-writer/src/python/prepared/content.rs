//! Content preparation, pending row overlays and per-batch staging.
use super::*;

pub(super) struct RowBudget {
    limited: bool,
    source_rows: u64,
    known_counts: BTreeMap<String, u64>,
}

impl Preparation<'_> {
    pub(super) fn row_budget(
        &self,
        dataset: &Option<Arc<lance::Dataset>>,
        pending: &state::Pending,
    ) -> Result<RowBudget> {
        let paths = &pending.paths;
        let store = &pending.store;
        let limited = self.source.row_warning.is_some() || self.source.row_error.is_some();
        let mut source_rows = if limited {
            if let Some(ds) = dataset.as_deref() {
                self.rt.block_on(ds.count_rows(Some(format!(
                    "source_id={}",
                    fyrr_reader::storage::sql(&self.source.id)
                ))))? as u64
            } else {
                0
            }
        } else {
            0
        };
        let mut known_counts = std::collections::BTreeMap::new();
        if limited {
            let mut overlay = std::collections::BTreeMap::new();
            for path in paths.iter() {
                let envelope: PreparedEnvelope = self
                    .rt
                    .block_on(fyrr_reader::storage::read_contract(store.as_ref(), path))?;
                if let EnvelopePayload::Content(payload) = envelope.payload {
                    for doc in payload.documents {
                        // A same-revision zero-record intent is only presence;
                        // the committer preserves its published records.
                        if doc.intent == DocumentIntent::Replace
                            && doc.records == 0
                            && doc.source_revision.is_some()
                            && doc.source_revision == doc.expected_prior_revision
                        {
                            continue;
                        }
                        let item = overlay.entry(doc.root_id).or_insert((0, 0));
                        if envelope.sequence > item.0 {
                            *item = (envelope.sequence, doc.records as u64);
                        }
                    }
                }
            }
            let roots: Vec<_> = overlay.keys().cloned().collect();
            known_counts =
                self.rt
                    .block_on(source_counts(dataset.as_deref(), &self.source.id, &roots))?;
            for (root, (_, rows)) in overlay {
                let previous = known_counts.insert(root, rows).unwrap_or(0);
                source_rows = source_rows
                    .checked_sub(previous)
                    .and_then(|n| n.checked_add(rows))
                    .ok_or(WriterError::SourceRowLimit)?;
            }
        }
        Ok(RowBudget {
            limited,
            source_rows,
            known_counts,
        })
    }
}

impl Preparation<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn prepare_batch(
        &self,
        batch: &ConnectorBatch,
        rows: &RecordBatch,
        stats: &mut KindStats,
        state: &mut CursorState,
        base: &Option<BaseHead>,
        dataset: &Option<Arc<lance::Dataset>>,
        revisions: &PendingRevisions,
        cache: &mut WriterEmbedding,
        permission_stream: bool,
        budget: &mut RowBudget,
    ) -> Result<()> {
        let config = self.config;
        let source = self.source;
        let rt = self.rt;
        let lease = self.lease;
        let mut changes =
            crate::batch::document_changes(rows).map_err(WriterError::from_provider)?;
        let roots = changes
            .iter()
            .map(|c| c.root_id.clone())
            .collect::<Vec<_>>();
        let (published, known) = rt.block_on(revision_views(
            config,
            dataset.as_deref(),
            revisions,
            &roots,
        ))?;
        for change in &mut changes {
            if change.op == fyrr_core::connector::ChangeOp::Upsert
                && source.retention_days.is_some_and(|days| {
                    change
                        .metadata
                        .as_ref()
                        .and_then(|m| m.updated_at)
                        .is_some_and(|updated| {
                            updated < fyrr_core::clock::now() - chrono::Duration::days(days as i64)
                        })
                })
            {
                change.op = fyrr_core::connector::ChangeOp::Delete;
                change.sections.clear();
                change.metadata = None;
                change.readers.clear();
                change.containers.clear();
                change.attributes.clear();
            }
        }
        let mut prepared_changes = Vec::new();
        changes.retain(|change| {
            let unchanged = change.op == fyrr_core::connector::ChangeOp::Upsert
                && change.source_revision.as_ref().is_some_and(|revision| {
                    published
                        .get(&change.root_id)
                        .is_some_and(|(old, _)| old == revision)
                        // A pending replacement or delete can change the
                        // published revision before this intent applies.
                        // Restoring it requires rows, not just presence.
                        && known
                            .get(&change.root_id)
                            .is_some_and(|(effective, _)| effective == revision)
                });
            if unchanged {
                prepared_changes.push(crate::prepare::ContentChange::Present(EnvelopeDocument {
                    root_id: change.root_id.clone(),
                    intent: DocumentIntent::Replace,
                    source_revision: change.source_revision.clone(),
                    revision_order: change.revision_order,
                    expected_prior_revision: change.source_revision.clone(),
                    permission_revision: change.permission_revision.clone(),
                    records: 0,
                    patch: None,
                }));
            }
            !unchanged
        });
        if budget.limited {
            let unknown: Vec<_> = roots
                .iter()
                .filter(|root| !budget.known_counts.contains_key(*root))
                .cloned()
                .collect();
            let counts = rt.block_on(source_counts(dataset.as_deref(), &source.id, &unknown))?;
            for root in unknown {
                budget
                    .known_counts
                    .insert(root.clone(), counts.get(&root).copied().unwrap_or(0));
            }
        }
        for change in &mut changes {
            if change.expected_prior_revision.is_none() {
                change.expected_prior_revision = known.get(&change.root_id).map(|(r, _)| r.clone());
            }
            if budget.limited && crate::prepare::quarantine_reason(config, source, change).is_none()
            {
                let rows = change
                    .sections
                    .iter()
                    .map(|s| crate::chunk::chunks(&s.text).len() as u64)
                    .sum::<u64>();
                let previous = budget
                    .known_counts
                    .get(&change.root_id)
                    .copied()
                    .unwrap_or(0);
                let projected = budget
                    .source_rows
                    .checked_sub(previous)
                    .and_then(|n| n.checked_add(rows))
                    .ok_or(WriterError::SourceRowLimit)?;
                if source.row_error.is_some_and(|max| projected > max) {
                    return Err(WriterError::SourceRowLimit);
                }
                stats.row_warning |= source.row_warning.is_some_and(|max| projected > max);
                budget.known_counts.insert(change.root_id.clone(), rows);
                budget.source_rows = projected;
            }
        }
        prepared_changes.extend(
            changes
                .into_iter()
                .map(|change| crate::prepare::ContentChange::Write(Box::new(change))),
        );
        stats.quarantined += rt.block_on(crate::prepare::content(
            config,
            source,
            batch,
            prepared_changes,
            state,
            lease,
            base.clone(),
            cache,
            permission_stream,
        ))?;
        Ok(())
    }
}
