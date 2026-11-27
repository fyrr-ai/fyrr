//! Logical-key apply. State is carried by the published Lance rows, never an S3 mutable map.
use crate::{Result, WriterError};
use futures::TryStreamExt;
mod content_apply;
mod row_state;

pub use content_apply::content;
pub use row_state::{
    DocumentState, PermissionState, container_token, keys_filter, permission_readers, reader_token,
    rows_filter, state, state_source, states, strings, tokens,
};
pub(crate) use row_state::{
    OWN_READERS, effective_readers, marker_row, own_readers, with_own_readers, with_patch,
    with_state, with_typed,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, ListArray, RecordBatch, RecordBatchIterator, StringArray,
};
use fyrr_core::config::{Config, DenseElement};
use fyrr_core::envelope::*;
use fyrr_reader::embedding::digest;
use fyrr_reader::storage::{namespace_store, sql};
use lance::Dataset;
use lance::dataset::WriteParams;
use lance::dataset::fragment::write::FragmentCreateBuilder;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Rejects `envelope` for `reason`: writes its marker and adds its ID to
/// `rejected`, the IDs the commit in progress publishes as rejected. An
/// envelope already in `rejected` is left as it is.
pub async fn reject(
    config: &Config,
    envelope: &PreparedEnvelope,
    reason: &str,
    rejected: &mut Vec<String>,
) -> Result<()> {
    if rejected.contains(&envelope.envelope_id) {
        return Ok(());
    }
    let (store, root) = namespace_store(config)?;
    let marker = RejectionMarker {
        schema_version: ENVELOPE_CONTRACT_VERSION,
        envelope_id: envelope.envelope_id.clone(),
        source_id: envelope.source_id.clone(),
        kind: envelope.kind,
        sequence: envelope.sequence,
        rejection_generation: envelope.rejection_generation,
        restore_sequence: envelope.restore_sequence,
        start_cursor: envelope.cursor.start.clone(),
        reason: reason.into(),
    };
    // The marker outlives the envelope it rejects only if the envelope's
    // name was never flushed: its worker may still be flushing, or be dead.
    // A re-prepared envelope has the same ID and would then count as
    // rejected. Flush what was observed before the marker exists.
    fyrr_reader::storage::observed_durability(config)?
        .persist()
        .await?;
    let path = root
        .join("rejections")
        .join(format!("{}.json", envelope.envelope_id));
    match crate::prepare::put_immutable(store.as_ref(), &path, &marker).await {
        Ok(()) => fyrr_reader::metrics::add(&crate::telemetry::REJECTED_ENVELOPES, &[], 1),
        // A marker written before, by a commit that did not live to publish it,
        // stands with its own reason: this one is a retry, not a second rejection.
        Err(WriterError::ImmutableObjectConflict) => {
            let written: RejectionMarker =
                fyrr_reader::storage::read_contract(store.as_ref(), &path).await?;
            if written.envelope_id != marker.envelope_id {
                return Err(WriterError::ImmutableObjectConflict);
            }
        }
        Err(error) => return Err(error),
    }
    rejected.push(envelope.envelope_id.clone());
    Ok(())
}
pub async fn verify_files(config: &Config, payload: &ContentPayload) -> Result<()> {
    let (store, root) = namespace_store(config)?;
    for fragment in &payload.fragments {
        if fragment
            .files
            .iter()
            .any(|file| !file.path.starts_with("data/") || file.path.contains(".."))
        {
            return Err(WriterError::ChangedRowsInvalid);
        }
        let paths: Vec<_> = fragment
            .files
            .iter()
            .map(|file| file.path.clone())
            .collect();
        let (files, binding) = crate::prepare::bind_files(store.as_ref(), &root, &paths)
            .await
            .map_err(|_| WriterError::ChangedRowsInvalid)?;
        if files != fragment.files || fragment.digest != binding {
            return Err(WriterError::ChangedRowsInvalid);
        }
    }
    Ok(())
}
/// Whether this build commits `payload`: fragments of its table major and,
/// once the table exists, a table that holds connector maps in their rows.
/// Anything else was written before table 2.0, and its namespace is
/// recreated, never upgraded.
pub fn check_table(payload: &ContentPayload, dataset: Option<&Dataset>) -> Result<()> {
    if payload.table_schema_version.major != TABLE_SCHEMA_VERSION.major
        || dataset.is_some_and(|dataset| dataset.schema().field("connector_map").is_none())
    {
        return Err(WriterError::NamespaceLayoutOld);
    }
    Ok(())
}
async fn fragment_rows(
    config: &Config,
    dataset: &Dataset,
    payload: &ContentPayload,
) -> Result<RecordBatch> {
    verify_files(config, payload).await?;
    let fragments = payload
        .fragments
        .iter()
        .map(|f| {
            serde_json::from_value(f.lance_fragment.clone())
                .map_err(|_| WriterError::ChangedRowsInvalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if fragments.is_empty() {
        return Ok(RecordBatch::new_empty(crate::prepare::schema(
            config.embedding.dimensions,
            config.namespace.settings.dense_element,
        )));
    }
    dataset
        .scan()
        .with_fragments(fragments)
        .try_into_batch()
        .await
        .map_err(|_| WriterError::ChangedRowsInvalid)
}
/// Writes `rows` in [`Steps`]: the caller stages them as one change of the
/// dataset, so what it rewrites is never refused for its size.
pub(crate) async fn write_rows(
    config: &Config,
    rows: Vec<RecordBatch>,
) -> Result<Vec<serde_json::Value>> {
    let mut steps = Steps::default();
    for row in rows {
        steps.push(config, row).await?;
    }
    steps.finish(config).await
}

/// Writes `rows` as data files, unbounded: the caller bounds what it holds.
async fn write_fragments(
    config: &Config,
    rows: Vec<RecordBatch>,
) -> Result<Vec<serde_json::Value>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let params = WriteParams {
        store_params: Some(crate::prepare::store_params(config)?),
        ..Default::default()
    };
    // Rows with attribute columns and rows without (a document's marker) are
    // written apart: a fragment has one schema.
    let dimensions = config.embedding.dimensions;
    let element = config.namespace.settings.dense_element;
    let built_in = crate::prepare::schema(dimensions, element).fields().len();
    let mut groups: Vec<(arrow_schema::SchemaRef, Vec<RecordBatch>)> = Vec::new();
    for row in rows {
        let extra: Vec<_> = row
            .schema()
            .fields()
            .iter()
            .skip(built_in)
            .map(|f| arrow_schema::Field::new(f.name(), f.data_type().clone(), true))
            .collect();
        let schema = crate::prepare::schema_with(dimensions, element, extra);
        match groups.iter_mut().find(|(known, _)| *known == schema) {
            Some((_, batches)) => batches.push(row),
            None => groups.push((schema, vec![row])),
        }
    }
    let mut fragments = Vec::new();
    for (schema, batches) in groups {
        fragments.extend(
            FragmentCreateBuilder::new(&fyrr_reader::storage::dataset_uri(config))
                .write_params(&params)
                .write_fragments(RecordBatchIterator::new(
                    batches.into_iter().map(Ok),
                    schema,
                ))
                .await
                .map_err(|_| WriterError::FragmentWriteFailed)?,
        );
    }
    fragments
        .into_iter()
        .map(|f| serde_json::to_value(f).map_err(|_| WriterError::PreparedFragmentInvalid))
        .collect()
}

pub(crate) const ROW_OVERHEAD_BYTES: u64 = 16384;

/// Adds the rows of `batch`, loaded from storage, to the `(rows, bytes)` held so
/// far, the lists of a multi-vector attribute included, and checks the total
/// against the bounds of a group: a patch holds the stored rows it loads, so
/// those that are too many are refused as they are loaded.
pub(crate) fn hold_rows(config: &Config, held: &mut (u64, u64), batch: &RecordBatch) -> Result<()> {
    let (count, bytes) = row_size(std::slice::from_ref(batch))?;
    held.0 = held.0.saturating_add(count);
    held.1 = held
        .1
        .saturating_add(bytes.saturating_sub(ROW_OVERHEAD_BYTES));
    if held.0 > config.writer.commit.group_max_rows
        || held.1.saturating_add(ROW_OVERHEAD_BYTES) > config.writer.commit.group_max_bytes.as_u64()
    {
        return Err(WriterError::EnvelopeBoundsInvalid);
    }
    Ok(())
}

pub(crate) fn row_size(rows: &[RecordBatch]) -> Result<(u64, u64)> {
    let count = rows.iter().map(|b| b.num_rows() as u64).sum::<u64>();
    let bytes = rows
        .iter()
        .flat_map(|b| b.columns())
        .try_fold(ROW_OVERHEAD_BYTES, |sum, a| {
            let bytes = if let Some(list) = a.as_any().downcast_ref::<ListArray>() {
                let mut bytes = (list.len() as u64 + 1) * 4 + list.len().div_ceil(8) as u64;
                for row in 0..list.len() {
                    if !list.is_null(row) {
                        bytes += list
                            .value(row)
                            .to_data()
                            .get_slice_memory_size()
                            .map_err(|_| WriterError::ChangedRowsInvalid)?
                            as u64;
                    }
                }
                bytes
            } else {
                a.to_data()
                    .get_slice_memory_size()
                    .map_err(|_| WriterError::ChangedRowsInvalid)? as u64
            };
            sum.checked_add(bytes)
                .ok_or(WriterError::EnvelopeBoundsInvalid)
        })?;
    Ok((count, bytes))
}
/// Genesis deletes need the same published tombstone authority as later Updates.
pub async fn initial_deletes(
    config: &Config,
    envelope: &PreparedEnvelope,
    sequence: u64,
) -> Result<Vec<serde_json::Value>> {
    let EnvelopePayload::Content(payload) = &envelope.payload else {
        return Err(WriterError::EnvelopeInvalid);
    };
    let rows = payload
        .documents
        .iter()
        .filter(|d| d.intent == DocumentIntent::Delete)
        .map(|d| {
            marker_row(
                config.embedding.dimensions,
                config.namespace.settings.dense_element,
                &envelope.source_id,
                &d.root_id,
                &DocumentState {
                    revision: d.source_revision.clone(),
                    revision_order: d.revision_order,
                    deleted: true,
                    content_purged: false,
                    delete_sequence: Some(envelope.sequence),
                    publication_sequence: sequence,
                    permission: None,
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    write_rows(config, rows).await
}

/// The exact prepared fragments, including undeclared rows, are checked on both publication paths.
pub async fn verified_staged(
    config: &Config,
    dataset: &Dataset,
    envelope: &PreparedEnvelope,
) -> Result<RecordBatch> {
    let EnvelopePayload::Content(payload) = &envelope.payload else {
        return Err(WriterError::EnvelopeInvalid);
    };
    let staged = fragment_rows(config, dataset, payload).await?;
    let documents: BTreeMap<_, _> = payload
        .documents
        .iter()
        .map(|d| (d.root_id.as_str(), d))
        .collect();
    if documents.len() != payload.documents.len() {
        return Err(WriterError::DuplicateDocumentChange);
    }
    let mut counts = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for row in 0..staged.num_rows() {
        let root = strings(&staged, "root_id")?.value(row);
        let Some(document) = documents.get(root) else {
            return Err(WriterError::ChangedRowsInvalid);
        };
        let values = staged
            .column_by_name("vector")
            .and_then(|a| a.as_any().downcast_ref::<FixedSizeListArray>())
            .ok_or(WriterError::ChangedRowsInvalid)?
            .value(row);
        if strings(&staged, "source_id")?.value(row) != envelope.source_id
            || Some(strings(&staged, "source_revision")?.value(row))
                != document.source_revision.as_deref()
            || document.intent != DocumentIntent::Replace
            || !keys.insert((root, strings(&staged, "record_id")?.value(row)))
            || !valid_vector(values.as_ref(), config.embedding.dimensions)
            || strings(&staged, "searchable_text")?.value(row).is_empty()
        {
            return Err(WriterError::ChangedRowsInvalid);
        }
        *counts.entry(root).or_insert(0u32) += 1;
    }
    if payload
        .documents
        .iter()
        .any(|d| counts.get(d.root_id.as_str()).copied().unwrap_or(0) != d.records)
    {
        return Err(WriterError::ChangedRowsInvalid);
    }
    Ok(staged)
}

pub fn complete_read(envelope: &PreparedEnvelope) -> bool {
    match &envelope.payload {
        EnvelopePayload::Permissions(p) => p.complete_read,
        EnvelopePayload::Members(p) => p.complete_read,
        _ => envelope.advances_cursor,
    }
}

/// Per-row permissions are reapplied from their separate revision before content.
/// A root's rows are read [`STEP_ROWS`] at a time and written in steps of at
/// most a group's rows and bytes, a row alone being a step, so that an
/// envelope is never refused for what it rewrites: the steps are staged as one
/// change of the dataset.
pub async fn permissions(
    config: &Config,
    dataset: &Dataset,
    envelope: &PreparedEnvelope,
) -> Result<(Vec<String>, Vec<serde_json::Value>)> {
    let EnvelopePayload::Permissions(payload) = &envelope.payload else {
        return Err(WriterError::EnvelopeInvalid);
    };
    let roots = payload
        .documents
        .iter()
        .map(|d| d.root_id.clone())
        .collect::<Vec<_>>();
    let previous = states(dataset, &envelope.source_id, &roots).await?;
    let mut changed = Vec::new();
    let mut steps = Steps::default();
    for document in &payload.documents {
        let mut state = previous.get(&document.root_id).cloned().unwrap_or_default();
        if state
            .permission
            .as_ref()
            .is_some_and(|p| p.revision.as_ref() == Some(&document.permission_revision))
        {
            continue;
        }
        let permission = PermissionState {
            revision: Some(document.permission_revision.clone()),
            readers: {
                let mut readers = document.readers.clone();
                for id in &document.containers {
                    readers.push(container_token(config, &envelope.source_id, id)?);
                }
                readers.sort();
                readers.dedup();
                readers
            },
            containers: document.containers.clone(),
        };
        let filter = keys_filter(&envelope.source_id, std::slice::from_ref(&document.root_id));
        state.permission = Some(permission.clone());
        let mut scan = dataset.scan();
        scan.filter(&filter)
            .map_err(|_| WriterError::ChangedRowsInvalid)?
            .batch_size(STEP_ROWS)
            .batch_readahead(1)
            .fragment_readahead(1);
        let mut stream = scan
            .try_into_stream()
            .await
            .map_err(|_| WriterError::ChangedRowScanFailed)?;
        let mut found = false;
        while let Some(rows) = stream
            .try_next()
            .await
            .map_err(|_| WriterError::ChangedRowScanFailed)?
        {
            for row in 0..rows.num_rows() {
                found = true;
                let tokens = if strings(&rows, "source_id")?.value(row)
                    == state_source(&envelope.source_id)
                {
                    Vec::new()
                } else {
                    effective_readers(own_readers(&rows, row)?.as_deref(), &permission.readers)
                };
                steps
                    .push(config, with_state(&rows.slice(row, 1), &state, &tokens)?)
                    .await?;
            }
        }
        if !found {
            let marker = marker_row(
                config.embedding.dimensions,
                config.namespace.settings.dense_element,
                &envelope.source_id,
                &document.root_id,
                &state,
            )?;
            steps.push(config, marker).await?;
        }
        changed.push(filter);
    }
    Ok((changed, steps.finish(config).await?))
}

/// The rows of a root a permission replacement reads at a time.
pub(crate) const STEP_ROWS: usize = 256;

/// Rows written in steps of at most a group's rows and bytes, or of one row
/// that is over them alone: the rows of a step not yet written, and the
/// fragments of those written.
#[derive(Default)]
pub(crate) struct Steps {
    rows: Vec<RecordBatch>,
    held: (u64, u64),
    fragments: Vec<serde_json::Value>,
}

impl Steps {
    /// Holds the rows of `batch`, and writes the step held first when a row
    /// would put it over. The rows of `batch` in one step are held as one
    /// slice: a step of one-row batches costs its write per row.
    pub(crate) async fn push(&mut self, config: &Config, batch: RecordBatch) -> Result<()> {
        let mut start = 0;
        for row in 0..batch.num_rows() {
            let (count, bytes) = row_size(&[batch.slice(row, 1)])?;
            let bytes = bytes - ROW_OVERHEAD_BYTES;
            if (!self.rows.is_empty() || row > start)
                && (self.held.0 + count > config.writer.commit.group_max_rows
                    || ROW_OVERHEAD_BYTES
                        .saturating_add(self.held.1)
                        .saturating_add(bytes)
                        > config.writer.commit.group_max_bytes.as_u64())
            {
                if row > start {
                    self.rows.push(batch.slice(start, row - start));
                }
                start = row;
                self.held = (0, 0);
                let full = std::mem::take(&mut self.rows);
                self.fragments.extend(write_fragments(config, full).await?);
            }
            self.held = (self.held.0 + count, self.held.1.saturating_add(bytes));
        }
        if batch.num_rows() > start {
            self.rows.push(batch.slice(start, batch.num_rows() - start));
        }
        Ok(())
    }

    /// Writes the step held and gives the fragments of every step.
    pub(crate) async fn finish(mut self, config: &Config) -> Result<Vec<serde_json::Value>> {
        self.fragments
            .extend(write_fragments(config, self.rows).await?);
        Ok(self.fragments)
    }
}

/// A complete listing stages at most one bounded delete group per publication.
/// Receipting the final listing waits until every eligible root has been removed.
pub async fn reconcile(
    config: &Config,
    dataset: &Dataset,
    envelope: &PreparedEnvelope,
    sequence: u64,
    head: &fyrr_core::publication::ServingHead,
) -> Result<(Vec<String>, Vec<serde_json::Value>, bool)> {
    let EnvelopePayload::Reconcile(listing) = &envelope.payload else {
        return Err(WriterError::EnvelopeInvalid);
    };
    if !listing.last || !listing.complete {
        return Ok((Vec::new(), Vec::new(), true));
    }
    let (store, _) = namespace_store(config)?;
    let (paths, _) = crate::commit::envelope_paths(config, head).await?;
    let mut parts = BTreeMap::new();
    let mut pending = BTreeSet::new();
    for item in crate::commit::pending(config, head).await? {
        if item.source_id == envelope.source_id
            && let EnvelopePayload::Content(p) = item.payload
        {
            pending.extend(p.documents.into_iter().map(|d| d.root_id));
        }
    }
    for path in paths {
        let item: PreparedEnvelope =
            fyrr_reader::storage::read_contract(store.as_ref(), &path).await?;
        if item.source_id != envelope.source_id
            || item.rejection_generation != envelope.rejection_generation
        {
            continue;
        }
        let EnvelopePayload::Reconcile(p) = item.payload else {
            continue;
        };
        if item.sequence < envelope.sequence.saturating_sub(listing.part as u64)
            || item.sequence > envelope.sequence
            || p.listing_id != listing.listing_id
        {
            continue;
        }
        if p.watermark != listing.watermark || parts.insert(p.part, p).is_some() {
            return Err(WriterError::ListingInvalid);
        }
    }
    if parts.len() != listing.part as usize + 1
        || (0..=listing.part).any(|p| !parts.contains_key(&p))
    {
        return Ok((Vec::new(), Vec::new(), true));
    }
    let present: BTreeSet<_> = parts.into_values().flat_map(|p| p.root_ids).collect();
    let mut scan = dataset.scan();
    scan.project(&["root_id", "source_revision", "metadata_json", "readers"])
        .map_err(|_| WriterError::ListingInvalid)?
        .filter(&format!("source_id = {}", sql(&envelope.source_id)))
        .map_err(|_| WriterError::ListingInvalid)?;
    // Listing necessarily visits all IDs, but projects no text/vector and bounds the delete output.
    let mut stream = scan
        .try_into_stream()
        .await
        .map_err(|_| WriterError::ChangedRowScanFailed)?;
    let mut selected = BTreeMap::new();
    let mut bytes = 16384u64;
    let mut full = false;
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|_| WriterError::ChangedRowScanFailed)?
    {
        for row in 0..batch.num_rows() {
            let root = strings(&batch, "root_id")?.value(row);
            let current = state(&batch, row)?;
            if !current.deleted
                && current.publication_sequence <= listing.watermark
                && !present.contains(root)
                && !pending.contains(root)
            {
                if selected.contains_key(root) {
                    continue;
                }
                let estimate = marker_row(
                    config.embedding.dimensions,
                    config.namespace.settings.dense_element,
                    &envelope.source_id,
                    root,
                    &current,
                )?
                .get_array_memory_size() as u64
                    + 4096;
                if bytes.saturating_add(estimate) > config.writer.commit.group_max_bytes.as_u64() {
                    if selected.is_empty() {
                        return Err(WriterError::EnvelopeBoundsInvalid);
                    }
                    full = true;
                    break;
                }
                bytes += estimate;
                selected.insert(root.to_owned(), current);
                if selected.len() as u64 >= config.writer.commit.group_max_rows {
                    break;
                }
            }
        }
        if full || selected.len() as u64 >= config.writer.commit.group_max_rows {
            break;
        }
    }
    if selected.is_empty() {
        return Ok((Vec::new(), Vec::new(), true));
    }
    let mut changed = Vec::new();
    let mut rows = Vec::new();
    for (root, mut current) in selected {
        current.deleted = true;
        current.delete_sequence = None;
        current.publication_sequence = sequence;
        changed.push(keys_filter(
            &envelope.source_id,
            std::slice::from_ref(&root),
        ));
        rows.push(marker_row(
            config.embedding.dimensions,
            config.namespace.settings.dense_element,
            &envelope.source_id,
            &root,
            &current,
        )?);
    }
    Ok((changed, write_rows(config, rows).await?, false))
}

/// Before visibility, independently reread declared changed roots and check logical keys/state.
pub async fn verify_publication(
    config: &Config,
    dataset: &Dataset,
    envelopes: &[PreparedEnvelope],
    sequence: u64,
    rejected: &[String],
) -> Result<()> {
    for envelope in envelopes {
        let roots = match &envelope.payload {
            EnvelopePayload::Content(p) => p
                .documents
                .iter()
                .map(|d| d.root_id.clone())
                .collect::<Vec<_>>(),
            EnvelopePayload::Permissions(p) => {
                p.documents.iter().map(|d| d.root_id.clone()).collect()
            }
            _ => Vec::new(),
        };
        for page in roots.chunks(256) {
            // The rows of a page are read in batches: a root of many rows is
            // never held whole.
            let mut scan = dataset.scan();
            scan.filter(&keys_filter(&envelope.source_id, page))
                .map_err(|_| WriterError::ChangedRowsInvalid)?
                .batch_size(STEP_ROWS)
                .batch_readahead(1)
                .fragment_readahead(1);
            let mut stream = scan
                .try_into_stream()
                .await
                .map_err(|_| WriterError::ChangedRowsInvalid)?;
            let mut keys = BTreeSet::new();
            let mut states: BTreeMap<String, DocumentState> = BTreeMap::new();
            while let Some(batch) = stream
                .try_next()
                .await
                .map_err(|_| WriterError::ChangedRowsInvalid)?
            {
                for row in 0..batch.num_rows() {
                    let root = strings(&batch, "root_id")?.value(row);
                    let record = strings(&batch, "record_id")?.value(row);
                    let source = strings(&batch, "source_id")?.value(row);
                    if !keys.insert((source.to_owned(), root.to_owned(), record.to_owned())) {
                        return Err(WriterError::DuplicateChangedRows);
                    }
                    let vector = batch
                        .column_by_name("vector")
                        .and_then(|a| a.as_any().downcast_ref::<FixedSizeListArray>())
                        .ok_or(WriterError::ChangedRowsInvalid)?
                        .value(row);
                    if !valid_vector(vector.as_ref(), config.embedding.dimensions) {
                        return Err(WriterError::ChangedRowsInvalid);
                    }
                    let current = state(&batch, row)?;
                    if states.get(root).is_some_and(|old| old != &current) {
                        return Err(WriterError::DocumentStateInvalid);
                    }
                    states.insert(root.to_owned(), current.clone());
                    if source == state_source(&envelope.source_id) {
                        if !tokens(&batch, row)?.is_empty()
                            || !strings(&batch, "searchable_text")?.value(row).is_empty()
                        {
                            return Err(WriterError::TombstoneInvalid);
                        }
                    } else if current.deleted
                        || strings(&batch, "searchable_text")?.value(row).is_empty()
                        || current.revision.as_deref()
                            != Some(strings(&batch, "source_revision")?.value(row))
                    {
                        return Err(WriterError::ChangedRowsInvalid);
                    }
                }
            }
            if let EnvelopePayload::Content(payload) = &envelope.payload {
                for document in payload
                    .documents
                    .iter()
                    .filter(|d| page.contains(&d.root_id) && d.intent == DocumentIntent::Replace)
                {
                    match states.get(document.root_id.as_str()) {
                        Some(state)
                            if state.publication_sequence == sequence
                                && state.revision == document.source_revision
                                && !state.deleted
                                // A purged document stays its marker, whatever
                                // the stale content says: it has no rows to count.
                                && !state.content_purged =>
                        {
                            let actual = keys
                                .iter()
                                .filter(|(source, root, _)| {
                                    *source == envelope.source_id && *root == document.root_id
                                })
                                .count();
                            if actual != document.records as usize {
                                return Err(WriterError::ChangedRowsInvalid);
                            }
                        }
                        None if !rejected.contains(&envelope.envelope_id) => {
                            return Err(WriterError::ChangedRowsInvalid);
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(())
}

/// Whether a stored vector has the namespace's dimension, only finite
/// numbers (of one of the two element types) and a nonzero one.
pub(crate) fn valid_vector(values: &dyn Array, dimensions: u32) -> bool {
    fyrr_reader::embedding::stored_values(values).is_some_and(|values| {
        values.len() == dimensions as usize
            && values.iter().all(|v| v.is_finite())
            && values.iter().any(|v| *v != 0.0)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder};

    #[test]
    fn stored_rows_are_held_against_the_bounds_of_an_envelope_with_their_multi_vector_lists() {
        let mut config = fyrr_core::config::parse(
            "[namespace]\nname='held'\n[storage]\nuri='file:///x'\n\
             [embedding]\nendpoint='http://127.0.0.1:9/v1'\n",
        )
        .unwrap()
        .config;
        // One row holds a list of 64 vectors of 128 numbers: 32 KiB of numbers.
        let mut lists = ListBuilder::new(FixedSizeListBuilder::new(Float32Builder::new(), 128));
        for _ in 0..64 {
            lists.values().values().append_slice(&[0.5; 128]);
            lists.values().append(true);
        }
        lists.append(true);
        let batch =
            RecordBatch::try_from_iter([("tokens", Arc::new(lists.finish()) as ArrayRef)]).unwrap();
        let (_, bytes) = row_size(std::slice::from_ref(&batch)).unwrap();
        let row = bytes - ROW_OVERHEAD_BYTES;
        assert!(row >= 64 * 128 * 4, "the list is counted: {row}");
        // The bound admits three such rows and not a fourth, though the rows
        // are few and the table's other columns are small.
        config.writer.commit.group_max_bytes =
            fyrr_core::units::ByteSize::bytes(ROW_OVERHEAD_BYTES + 3 * row + 1);
        let mut held = (0, 0);
        for _ in 0..3 {
            hold_rows(&config, &mut held, &batch).unwrap();
        }
        assert!(matches!(
            hold_rows(&config, &mut held, &batch),
            Err(WriterError::EnvelopeBoundsInvalid)
        ));
    }

    #[tokio::test]
    async fn a_batch_of_many_rows_is_written_in_steps_within_the_bounds_of_a_group() {
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target")
            .canonicalize()
            .unwrap()
            .join(format!("apply-steps-{}", std::process::id()));
        let mut config = fyrr_core::config::parse(&format!(
            "[namespace]\nname='steps'\n[storage]\nuri='file://{}'\n\
             [embedding]\nendpoint='http://127.0.0.1:9/v1'\n",
            directory.display()
        ))
        .unwrap()
        .config;
        let row = |root: &str| {
            marker_row(
                config.embedding.dimensions,
                config.namespace.settings.dense_element,
                "source",
                root,
                &DocumentState::default(),
            )
            .unwrap()
        };
        let rows: Vec<_> = (0..5).map(|n| row(&format!("root-{n}"))).collect();
        let (_, one) = row_size(&rows[..1]).unwrap();
        let one = one - ROW_OVERHEAD_BYTES;
        // Two rows fit a group, three do not; the five come as one batch, as
        // the rows of an API envelope do.
        config.writer.commit.group_max_bytes =
            fyrr_core::units::ByteSize::bytes(ROW_OVERHEAD_BYTES + 2 * one + one / 2);
        let batch = arrow_select::concat::concat_batches(&rows[0].schema(), &rows).unwrap();
        let mut steps = Steps::default();
        steps.push(&config, batch).await.unwrap();
        let fragments = steps.finish(&config).await.unwrap();
        let sizes: Vec<_> = fragments
            .iter()
            .map(|f| f["physical_rows"].as_u64().unwrap())
            .collect();
        std::fs::remove_dir_all(&directory).ok();
        assert_eq!(
            sizes,
            [2, 2, 1],
            "each step is within the bounds of a group"
        );
    }
}
