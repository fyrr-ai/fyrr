//! Rust preparation inside a source's worker; envelope durability precedes cursor advancement.

use crate::{Result, WriterError};
mod page;
use page::content_page;

use crate::embedding::WriterEmbedding;
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::types::Float16Type;
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float64Array,
    Int64Array, ListArray, RecordBatch, RecordBatchIterator, StringArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use fyrr_core::config::{Config, PermissionsMode, ProcessEnvironment};
use fyrr_core::connector::{
    CONNECTOR_CONTRACT_VERSION, ChangeOp, ConnectorBatch, CursorState, DocumentChange, SyncKind,
};
use fyrr_core::envelope::*;
use fyrr_core::{Contract, SchemaVersion, write_contract};
use fyrr_reader::embedding::{digest, identity};
use fyrr_reader::storage::{dataset_uri, namespace_store};
use lance::dataset::WriteParams;
use lance::dataset::fragment::write::FragmentCreateBuilder;
use lance::io::{ObjectStoreParams, StorageOptionsAccessor};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use std::sync::Arc;

pub fn schema(dimensions: u32) -> Arc<Schema> {
    schema_with(dimensions, Vec::new())
}

/// The built-in columns and then `extra`: a namespace's attribute columns.
pub fn schema_with(dimensions: u32, extra: Vec<Field>) -> Arc<Schema> {
    let strings = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    let mut fields = vec![
        Field::new("source_id", DataType::Utf8, false),
        Field::new("root_id", DataType::Utf8, false),
        Field::new("record_id", DataType::Utf8, false),
        Field::new("source_revision", DataType::Utf8, false),
        Field::new("readers", strings.clone(), false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float16, true)),
                dimensions as i32,
            ),
            false,
        ),
        Field::new("searchable_text", DataType::Utf8, false),
        Field::new("metadata_json", DataType::Utf8, false),
        Field::new("record_order", DataType::UInt64, true),
        Field::new("connector_map_ref", DataType::Utf8, true),
        Field::new("source_uri", DataType::Utf8, true),
        Field::new("source_type", DataType::Utf8, true),
        Field::new("project", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("attributes", strings, true),
    ];
    fields.extend(extra);
    Arc::new(Schema::new(fields))
}

pub fn store_params(config: &Config) -> Result<ObjectStoreParams> {
    Ok(ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            config
                .storage
                .object_store_options(&ProcessEnvironment)
                .map_err(|_| WriterError::StorageConfigurationInvalid)?
                .into_map()
                .into_iter()
                .collect(),
        ))),
        ..fyrr_reader::storage::native_store_params(None)
    })
}

pub fn chunk_identity() -> ChunkProfileIdentity {
    ChunkProfileIdentity {
        name: "whole-unit-v5".into(),
        max_unit_bytes: 16384,
        overlap_bytes: 0,
        digest: digest(b"whole-unit-v5:utf8:16384:no-overlap:structural-boundaries:stable-section-location-record-identities"),
    }
}

pub async fn put_immutable<T: Contract>(
    store: &dyn ObjectStore,
    path: &Path,
    value: &T,
) -> Result<()> {
    value
        .validate()
        .map_err(|_| WriterError::PublicationInvalid)?;
    let bytes = write_contract(value);
    match store
        .put_opts(
            path,
            bytes.clone().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. })
        | Err(object_store::Error::Precondition { .. }) => {
            let existing = store.get(path).await?.bytes().await?;
            if existing.as_ref() == bytes {
                Ok(())
            } else {
                Err(WriterError::ImmutableObjectConflict)
            }
        }
        Err(_) => Err(WriterError::StorageUnavailable),
    }
}

/// The durable envelope log is authoritative; repair its cached cursor before acquisition.
pub async fn recover_cursor(
    lease: &crate::sync_lease::CursorLease,
    state: &mut CursorState,
    durable_cursors: &[(u64, Option<String>)],
) -> Result<()> {
    let mut sequences = std::collections::BTreeSet::new();
    if durable_cursors
        .iter()
        .any(|(sequence, _)| !sequences.insert(*sequence))
    {
        return Err(WriterError::ImmutableObjectConflict);
    }
    loop {
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or(WriterError::CursorSequenceOverflow)?;
        let Some((sequence, end)) = durable_cursors
            .iter()
            .filter(|(s, _)| *s >= sequence)
            .min_by_key(|(s, _)| s)
        else {
            return Ok(());
        };
        state.sequence = *sequence;
        // A durable --full envelope starts at None even if S3 retained the
        // preceding nonempty cursor. Sequence, rather than start, is authoritative.
        state.cursor.clone_from(end);
        lease.write(state.clone()).await?;
    }
}

/// One document-level decision shared by quota admission and fragment preparation.
pub fn quarantine_reason(
    config: &Config,
    source: &fyrr_core::config::SourceConfig,
    change: &DocumentChange,
) -> Option<&'static str> {
    let text_bytes = change
        .sections
        .iter()
        .map(|s| s.text.len() as u64)
        .sum::<u64>();
    let Ok((work, estimate)) = weigh_change(config, change) else {
        return Some(WriterError::DocumentInvalid.as_str());
    };
    if work > config.writer.commit.group_max_rows
        || estimate.saturating_add(16384) > config.writer.commit.group_max_bytes.as_u64()
        || text_bytes
            > source
                .document_max_bytes
                .unwrap_or(config.writer.document_max_bytes)
                .as_u64()
    {
        return Some("document_oversized");
    }
    let sections = change
        .sections
        .iter()
        .map(|s| {
            s.location.as_ref().map_or_else(
                || change.root_id.clone(),
                |location| serde_json::to_string(location).unwrap_or_default(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    if change.validate().is_err()
        || sections.len() != change.sections.len()
        || change.sections.iter().any(|s| {
            s.text.is_empty()
                || s.text.contains('\0')
                // A vector is the record of one chunk, of the namespace's dimension.
                || s.vector.as_ref().is_some_and(|vector| {
                    crate::chunk::chunks(&s.text).len() != 1
                        || fyrr_reader::embedding::validate(vector, config.embedding.dimensions)
                            .is_err()
                })
        })
    {
        return Some(WriterError::DocumentInvalid.as_str());
    }
    None
}

/// An unchanged observation carries no text or fragments, but keeps the same
/// pending presence of a replacement without rewriting the same revision.
pub enum ContentChange {
    /// A replacement or a delete, with the condition the stored document must meet.
    Write(Box<DocumentChange>, Option<fyrr_core::mcp::Filter>),
    Present(EnvelopeDocument),
    /// Deletes and patches of the documents that match a filter, which the
    /// committer finds when it applies the envelope.
    Where(Box<fyrr_core::envelope::FilterWrites>),
}
impl std::fmt::Debug for ContentChange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Write(..) => "ContentChange::Write",
            Self::Present(_) => "ContentChange::Present",
            Self::Where(_) => "ContentChange::Where",
        })
    }
}

/// The records one change writes and its estimated size, the unit of an envelope's bounds.
fn weigh(config: &Config, change: &ContentChange) -> Result<(u64, u64)> {
    match change {
        ContentChange::Write(change, condition) => {
            let (work, estimate) = weigh_change(config, change)?;
            let condition = match condition {
                Some(condition) => serde_json::to_vec(condition)
                    .map_err(|_| WriterError::DocumentInvalid)?
                    .len() as u64,
                None => 0,
            };
            Ok((work, estimate + condition))
        }
        // The documents it matches are not known yet: only the committer's
        // check of the records it really writes can refuse them.
        ContentChange::Where(writes) => {
            let encoded = serde_json::to_vec(writes).map_err(|_| WriterError::DocumentInvalid)?;
            Ok((1, encoded.len() as u64))
        }
        ContentChange::Present(document) => {
            let encoded = serde_json::to_vec(document).map_err(|_| WriterError::DocumentInvalid)?;
            // The records a patch writes are weighed once for all patches of
            // an envelope, in `check_atomic`.
            let row = if document.intent == DocumentIntent::Patch {
                0
            } else {
                record_estimate(config)
            };
            Ok((1, encoded.len() as u64 + row))
        }
    }
}

fn weigh_change(config: &Config, change: &DocumentChange) -> Result<(u64, u64)> {
    let work = if change.op == ChangeOp::Delete {
        1
    } else {
        change
            .sections
            .iter()
            .map(|s| crate::chunk::chunks(&s.text).len() as u64)
            .sum::<u64>()
    };
    let encoded = serde_json::to_vec(change).map_err(|_| WriterError::DocumentInvalid)?;
    Ok((
        work,
        encoded.len() as u64 + work.saturating_mul(record_estimate(config)),
    ))
}

/// What a record adds to the size of an envelope besides its change.
fn record_estimate(config: &Config) -> u64 {
    2048 + u64::from(config.embedding.dimensions) * 2
}

/// What an envelope weighs before its first change.
const BASE_BYTES: u64 = 16384;

/// What the committer may write for a patched document: its record with the
/// text and every attribute at the largest its type holds, and what any record
/// takes besides. The stored record is what the patch keeps, so this is an upper
/// bound for all but a record with very many readers, which the committer
/// refuses when it is written.
pub(crate) fn patch_bound(config: &Config) -> u64 {
    patch_bytes(&config.namespace.attributes, record_estimate(config))
}
/// The part of `bound` that an envelope of `taken` bytes may weigh within
/// `max`. The estimate only groups envelopes: a patch is never refused for it
/// alone, and the committer refuses the rows that really overrun the limit.
pub(crate) fn within(bound: u64, max: u64, taken: u64) -> u64 {
    bound.min(max.saturating_sub(taken))
}
fn patch_bytes(attributes: &fyrr_core::attributes::AttributeSchema, record: u64) -> u64 {
    let attributes: u64 = attributes.values().map(|kind| value_bytes(*kind)).sum();
    crate::chunk::MAX_BYTES as u64 + attributes + record
}
/// The most a value of the type `kind` takes in its column.
fn value_bytes(kind: fyrr_core::attributes::AttributeType) -> u64 {
    use fyrr_core::attributes::{AttributeType as T, ElementType as E};
    match kind {
        T::String | T::Bytes => fyrr_core::limits::schema::MAX_VALUE_BYTES as u64,
        T::Int | T::Float | T::Datetime | T::Uint => 8,
        T::Uuid => 16,
        T::Bool => 1,
        // An element takes at least 2 bytes of a row, so a row holds at most
        // `ROW_MAX_BYTES / 2` of them. Strings share the row's bytes besides.
        T::Array(element) => {
            let count = crate::write_api::ROW_MAX_BYTES as u64 / 2;
            match element {
                E::String => crate::write_api::ROW_MAX_BYTES as u64 + 4 * count,
                element => count * value_bytes(element.scalar()),
            }
        }
    }
}

/// Whether `changes` fit the bounds of a single page, which one envelope must.
pub fn check_atomic(config: &Config, changes: &[ContentChange]) -> Result<()> {
    let (mut rows, mut bytes) = (0u64, BASE_BYTES);
    for change in changes {
        let (work, estimate) = weigh(config, change)?;
        rows = rows.saturating_add(work);
        bytes = bytes.saturating_add(estimate);
    }
    let patches = changes
        .iter()
        .filter(|change| {
            matches!(change, ContentChange::Present(d) if d.intent == DocumentIntent::Patch)
        })
        .count() as u64;
    bytes = bytes.saturating_add(within(
        patches.saturating_mul(patch_bound(config)),
        config.writer.commit.group_max_bytes.as_u64(),
        bytes,
    ));
    if rows > config.writer.commit.group_max_rows
        || bytes > config.writer.commit.group_max_bytes.as_u64()
    {
        return Err(WriterError::EnvelopeBoundsInvalid);
    }
    Ok(())
}

/// One envelope for all of `changes`, which must fit the bounds of a single
/// page: a request that does not is refused, never split, so that it stays
/// atomic. The cursor does not advance; the source has none.
#[allow(clippy::too_many_arguments)]
pub async fn atomic(
    config: &Config,
    source: &fyrr_core::config::SourceConfig,
    batch: &ConnectorBatch,
    changes: Vec<ContentChange>,
    state: &mut CursorState,
    lease: &crate::sync_lease::CursorLease,
    base_head: Option<BaseHead>,
    cache: &mut WriterEmbedding,
    attributes: Vec<ArrayRef>,
) -> Result<()> {
    check_atomic(config, &changes)?;
    content_page(
        config,
        source,
        batch,
        changes,
        state,
        lease,
        base_head,
        cache,
        false,
        false,
        &attributes,
    )
    .await
    .map(|_| ())
}

/// Split provider batches by actual inserted-record/delete-intent work, without
/// advancing the opaque provider cursor until the final durable sub-envelope.
#[allow(clippy::too_many_arguments)]
pub async fn content(
    config: &Config,
    source: &fyrr_core::config::SourceConfig,
    batch: &ConnectorBatch,
    changes: Vec<ContentChange>,
    state: &mut CursorState,
    lease: &crate::sync_lease::CursorLease,
    base_head: Option<BaseHead>,
    cache: &mut WriterEmbedding,
    permission_stream: bool,
) -> Result<u64> {
    let mut pages = Vec::new();
    let mut page = Vec::new();
    let mut rows = 0u64;
    let mut bytes = 16384u64;
    for change in changes {
        let (work, estimate) = weigh(config, &change)?;
        if !page.is_empty()
            && (rows.saturating_add(work) > config.writer.commit.group_max_rows
                || bytes.saturating_add(estimate) > config.writer.commit.group_max_bytes.as_u64())
        {
            pages.push(std::mem::take(&mut page));
            rows = 0;
            bytes = 16384;
        }
        rows = rows.saturating_add(work);
        bytes = bytes.saturating_add(estimate);
        page.push(change);
    }
    if !page.is_empty() || pages.is_empty() {
        pages.push(page);
    }
    let count = pages.len();
    let mut quarantined = 0;
    for (index, page) in pages.into_iter().enumerate() {
        let last = index + 1 == count;
        let mut part = batch.clone();
        part.cursor.start = state.cursor.clone();
        if !last {
            part.cursor.end = state.cursor.clone().unwrap_or_default();
        }
        quarantined += content_page(
            config,
            source,
            &part,
            page,
            state,
            lease,
            base_head.clone(),
            cache,
            last,
            permission_stream,
            &[],
        )
        .await?;
    }
    Ok(quarantined)
}

pub fn initial_cursor(config: &Config, source: &str, kind: SyncKind) -> CursorState {
    CursorState {
        schema_version: CONNECTOR_CONTRACT_VERSION,
        namespace: config.namespace.name.clone(),
        source_id: source.into(),
        kind,
        cursor: None,
        sequence: 0,
        rejection_generation: 0,
        restore_sequence: 0,
        // No read has completed yet: the kind is due at once.
        updated_at: chrono::DateTime::UNIX_EPOCH,
        lease: None,
    }
}

pub async fn non_content(
    config: &Config,
    source: &str,
    batch: &ConnectorBatch,
    state: &mut CursorState,
    lease: &crate::sync_lease::CursorLease,
    base: Option<BaseHead>,
    complete_read: bool,
) -> Result<()> {
    use fyrr_core::connector::BatchBody;
    // A permission read that changes nothing publishes nothing: only the cursor
    // moves. Replaying its window after a lost cursor is harmless.
    if !complete_read
        && let BatchBody::PermissionChanges {
            documents,
            container_grants,
        } = &batch.body
        && documents.is_empty()
        && container_grants.is_empty()
    {
        state.cursor = Some(batch.cursor.end.clone());
        return lease.write(state.clone()).await;
    }
    // A permission intent replaces every chunk of its root. Keep roots whole, and
    // let the committer charge their current published row counts before grouping.
    if let BatchBody::PermissionChanges {
        documents,
        container_grants,
    } = &batch.body
        && documents.len() + container_grants.len() > 1
    {
        let mut parts = documents
            .iter()
            .map(|document| BatchBody::PermissionChanges {
                documents: vec![document.clone()],
                container_grants: Vec::new(),
            })
            .chain(
                container_grants
                    .iter()
                    .map(|grant| BatchBody::PermissionChanges {
                        documents: Vec::new(),
                        container_grants: vec![grant.clone()],
                    }),
            )
            .peekable();
        while let Some(body) = parts.next() {
            let mut part = batch.clone();
            part.body = body;
            part.cursor.start = state.cursor.clone();
            let advances = parts.peek().is_none();
            if !advances {
                part.cursor.end = state.cursor.clone().unwrap_or_default();
            }
            non_content_page(
                config,
                source,
                &part,
                state,
                lease,
                base.clone(),
                complete_read,
                advances && !complete_read,
            )
            .await?;
        }
        return Ok(());
    }
    non_content_page(
        config,
        source,
        batch,
        state,
        lease,
        base,
        complete_read,
        !complete_read,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn non_content_page(
    config: &Config,
    source: &str,
    batch: &ConnectorBatch,
    state: &mut CursorState,
    lease: &crate::sync_lease::CursorLease,
    base: Option<BaseHead>,
    complete_read: bool,
    advances_cursor: bool,
) -> Result<()> {
    use fyrr_core::connector::BatchBody;
    let (store, root) = namespace_store(config)?;
    let sequence = state
        .sequence
        .checked_add(1)
        .ok_or(WriterError::CursorSequenceOverflow)?;
    let id = envelope_id(
        &config.namespace.name,
        source,
        batch.kind,
        sequence,
        state.rejection_generation,
        state.cursor.as_deref(),
    );
    let payload = match &batch.body {
        BatchBody::PermissionChanges {
            documents,
            container_grants,
        } => {
            let mut documents = documents.clone();
            let mut container_grants = container_grants.clone();
            for readers in documents
                .iter_mut()
                .map(|d| &mut d.readers)
                .chain(container_grants.iter_mut().map(|g| &mut g.readers))
            {
                *readers = readers
                    .iter()
                    .map(|t| crate::apply::reader_token(config, source, t))
                    .collect::<Result<_, _>>()?;
            }
            EnvelopePayload::Permissions(PermissionsPayload {
                complete_read,
                documents,
                container_grants,
            })
        }
        BatchBody::PrincipalSnapshot(part) => {
            let bytes = write_contract(batch);
            let hash = digest(&bytes);
            let object = format!("members/{}.json", &hash[7..]);
            crate::secrets::put(
                config,
                store.as_ref(),
                &object_store::path::Path::from(format!("{root}/{object}")),
                batch,
            )
            .await?;
            EnvelopePayload::Members(MembersPayload {
                complete_read,
                snapshot_id: part.snapshot_id.clone(),
                snapshot_object: object,
                snapshot_digest: hash,
                part: part.part,
                last: part.last,
                principals: (part.users.len() + part.groups.len()) as u64,
            })
        }
        BatchBody::Listing(part) => EnvelopePayload::Reconcile(ReconcilePayload {
            listing_id: part.listing_id.clone(),
            part: part.part,
            last: part.last,
            complete: part.complete,
            watermark: base.as_ref().map_or(0, |b| b.content_sequence),
            root_ids: part.root_ids.clone(),
        }),
        _ => return Err(WriterError::EnvelopeInvalid),
    };
    let envelope = PreparedEnvelope {
        schema_version: ENVELOPE_CONTRACT_VERSION,
        envelope_id: id.clone(),
        namespace: config.namespace.name.clone(),
        source_id: source.into(),
        kind: batch.kind,
        sequence,
        rejection_generation: state.rejection_generation,
        restore_sequence: state.restore_sequence,
        cursor: batch.cursor.clone(),
        advances_cursor,
        base_head: base,
        created_at: fyrr_core::clock::now(),
        payload,
    };
    lease.state().await?;
    lease.persist_observed().await?;
    put_immutable(
        store.as_ref(),
        &root.join("envelopes").join(format!("{id}.json")),
        &envelope,
    )
    .await?;
    state.sequence = sequence;
    if advances_cursor {
        state.cursor = Some(batch.cursor.end.clone());
    }
    lease.write(state.clone()).await
}

/// The key, relative to the namespace root, and the digest of a generation.
pub(crate) fn generation_object<T: Contract>(
    kind: &str,
    generation: u64,
    value: &T,
) -> (String, String) {
    let digest = digest(&write_contract(value));
    (format!("{kind}/{generation}-{}.json", &digest[7..]), digest)
}

pub(crate) async fn put_generation<T: Contract>(
    store: &dyn ObjectStore,
    root: &Path,
    kind: &str,
    generation: u64,
    value: &T,
) -> Result<fyrr_core::publication::GenerationPointer> {
    let (object, digest) = generation_object(kind, generation, value);
    put_immutable(store, &Path::from(format!("{root}/{object}")), value).await?;
    Ok(fyrr_core::publication::GenerationPointer {
        generation,
        object,
        digest,
    })
}

pub(crate) fn string_lists<'a>(
    lists: impl IntoIterator<Item = &'a [String]>,
) -> arrow_array::ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(Field::new(
        "item",
        DataType::Utf8,
        false,
    ));
    for values in lists {
        for value in values {
            builder.values().append_value(value);
        }
        builder.append(true);
    }
    builder.finish()
}

pub(crate) async fn bind_files(
    store: &dyn ObjectStore,
    root: &Path,
    paths: &[String],
) -> Result<(Vec<FileRef>, String)> {
    let mut files = Vec::new();
    let mut bound = Vec::new();
    for path in paths {
        let bytes = store
            .get(&Path::from(format!("{root}/dataset.lance/{path}")))
            .await?
            .bytes()
            .await?;
        bound.extend_from_slice(digest(&bytes).as_bytes());
        files.push(FileRef {
            path: path.clone(),
            size_bytes: bytes.len() as u64,
        });
    }
    Ok((files, digest(&bound)))
}

/// Typed attribute columns, built row by row from the values of a request.
pub(crate) struct AttributeColumns {
    schema: fyrr_core::attributes::AttributeSchema,
    columns: Vec<Vec<fyrr_core::attributes::AttributeValue>>,
}
impl AttributeColumns {
    pub(crate) fn new(schema: &fyrr_core::attributes::AttributeSchema) -> Self {
        Self {
            schema: schema.clone(),
            columns: vec![Vec::new(); schema.len()],
        }
    }

    /// One row: each attribute of the schema takes its value from `values`, or
    /// is null. A value that does not fit its attribute is an error and the
    /// columns are not to be used.
    pub(crate) fn push(
        &mut self,
        values: &serde_json::Map<String, serde_json::Value>,
    ) -> std::result::Result<(), fyrr_core::attributes::AttributeError> {
        for (name, column) in self.schema.clone().keys().zip(&mut self.columns) {
            column.push(fyrr_core::attributes::resolve(
                &mut self.schema,
                name,
                values.get(name).unwrap_or(&serde_json::Value::Null),
            )?);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<ArrayRef> {
        self.schema
            .values()
            .zip(self.columns)
            .map(|(kind, values)| attribute_array(*kind, values))
            .collect()
    }
}

/// The column of an attribute of type `kind`: each of `values` is of that type,
/// or null.
fn attribute_array(
    kind: fyrr_core::attributes::AttributeType,
    values: Vec<fyrr_core::attributes::AttributeValue>,
) -> ArrayRef {
    use fyrr_core::attributes::{AttributeType as T, AttributeValue as V};
    match kind {
        T::String => Arc::new(StringArray::from_iter(values.iter().map(|v| match v {
            V::String(text) => Some(text.as_str()),
            _ => None,
        }))),
        T::Int => Arc::new(Int64Array::from_iter(values.iter().map(|v| match v {
            V::Int(number) => Some(*number),
            _ => None,
        }))),
        T::Uint => Arc::new(UInt64Array::from_iter(values.iter().map(|v| match v {
            V::Uint(number) => Some(*number),
            _ => None,
        }))),
        T::Float => Arc::new(Float64Array::from_iter(values.iter().map(|v| match v {
            V::Float(number) => Some(*number),
            _ => None,
        }))),
        T::Bool => Arc::new(BooleanArray::from_iter(values.iter().map(|v| match v {
            V::Bool(flag) => Some(*flag),
            _ => None,
        }))),
        T::Datetime => Arc::new(
            TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                V::Datetime(micros) => Some(*micros),
                _ => None,
            }))
            .with_timezone("UTC"),
        ),
        T::Uuid => Arc::new(FixedSizeBinaryArray::new(
            16,
            Buffer::from_iter(values.iter().flat_map(|v| match v {
                V::Uuid(bytes) => *bytes,
                _ => [0; 16],
            })),
            Some(NullBuffer::from_iter(
                values.iter().map(|v| matches!(v, V::Uuid(_))),
            )),
        )),
        T::Bytes => Arc::new(BinaryArray::from_iter(values.iter().map(|v| match v {
            V::Bytes(bytes) => Some(bytes.as_slice()),
            _ => None,
        }))),
        T::Array(element) => {
            let lengths: Vec<usize> = values
                .iter()
                .map(|v| match v {
                    V::Array(items) => items.len(),
                    _ => 0,
                })
                .collect();
            let present = NullBuffer::from_iter(values.iter().map(|v| matches!(v, V::Array(_))));
            // The elements move out of the values: they are not copied.
            let items: Vec<V> = values
                .into_iter()
                .flat_map(|v| match v {
                    V::Array(items) => items,
                    _ => Vec::new(),
                })
                .collect();
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", element.scalar().arrow_type(), false)),
                OffsetBuffer::from_lengths(lengths),
                attribute_array(element.scalar(), items),
                Some(present),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_patch_of_a_number_is_weighed_by_the_width_of_the_numbers_of_its_namespace() {
        use fyrr_core::attributes::{AttributeSchema, AttributeType, ElementType};
        let floats: AttributeSchema = (0..64)
            .map(|n| (format!("f{n}"), AttributeType::Float))
            .collect();
        // 16 KiB of text, 64 numbers of 8 bytes and the part of any record.
        let record = 2048 + 1024 * 2;
        assert_eq!(patch_bytes(&floats, record), 16384 + 64 * 8 + record);
        assert!(patch_bytes(&floats, record) < 256 * 1024);
        // A namespace of arrays weighs 4 MiB a patch, which is not held against
        // a request in a group of 256 KiB: a patch of a number still fits.
        let arrays: AttributeSchema = (0..64)
            .map(|n| (format!("a{n}"), AttributeType::Array(ElementType::Uint)))
            .collect();
        let bound = patch_bytes(&arrays, record);
        assert!(bound > 256 * 1024);
        let taken = BASE_BYTES + 100;
        assert!(taken + within(bound, 256 * 1024, taken) <= 256 * 1024);
        assert_eq!(within(5, 256 * 1024, taken), 5, "a small bound is kept");
        // The patches of an envelope share one clamp, so a delete beside them
        // still fits: 100 bytes of patch and 3 KiB of delete.
        let taken = taken + 3 * 1024;
        assert!(taken + within(2 * bound, 256 * 1024, taken) <= 256 * 1024);
        // Each other type takes the most its column can hold of it.
        let weigh = |kind| {
            patch_bytes(&AttributeSchema::from([("a".into(), kind)]), record) - (16384 + record)
        };
        assert_eq!(weigh(AttributeType::Uint), 8);
        assert_eq!(weigh(AttributeType::Uuid), 16);
        assert_eq!(weigh(AttributeType::Bool), 1);
        assert_eq!(weigh(AttributeType::String), 4096);
        assert_eq!(weigh(AttributeType::Bytes), 4096);
        assert_eq!(
            weigh(AttributeType::Array(ElementType::Float)),
            (1 << 19) * 8
        );
        assert_eq!(
            weigh(AttributeType::Array(ElementType::String)),
            (1 << 20) + 4 * (1 << 19)
        );
    }
    #[test]
    fn the_table_holds_exactly_the_reserved_built_in_columns() {
        let schema = schema(8);
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, fyrr_core::attributes::BUILT_IN_COLUMNS);
    }
    #[tokio::test]
    async fn generation_pointer_reads_the_immutable_object_and_checks_its_digest() {
        let store = object_store::memory::InMemory::new();
        let root = Path::from("namespace");
        let policy: fyrr_core::publication::PolicyArtifact = fyrr_core::parse_contract(
            include_bytes!("../../../fixtures/contracts/policy-artifact/valid.json"),
        )
        .unwrap();
        let pointer = put_generation(&store, &root, "policy", policy.generation, &policy)
            .await
            .unwrap();
        let read: fyrr_core::publication::PolicyArtifact =
            fyrr_reader::storage::artifact(&store, &root, &pointer)
                .await
                .unwrap();
        assert_eq!(read, policy);
        store
            .put(
                &Path::from(format!("{root}/{}", pointer.object)),
                b"{}".to_vec().into(),
            )
            .await
            .unwrap();
        assert_eq!(
            fyrr_reader::storage::artifact::<fyrr_core::publication::PolicyArtifact>(
                &store, &root, &pointer
            )
            .await
            .unwrap_err(),
            fyrr_reader::storage::StorageError::AuthorizationUnavailable
        );
    }
}
