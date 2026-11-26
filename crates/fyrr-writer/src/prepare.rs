//! Rust preparation inside a source's worker; envelope durability precedes cursor advancement.

use crate::{Result, WriterError};
mod page;
use page::content_page;

use crate::embedding::WriterEmbedding;
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::types::{Float16Type, Float32Type};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float64Array,
    Int64Array, ListArray, RecordBatch, RecordBatchIterator, StringArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use fyrr_core::config::{Config, DenseElement, PermissionsMode, ProcessEnvironment};
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

/// The `vector` column of `rows`, in the namespace's element type.
pub(crate) fn vector_column<'a>(
    element: DenseElement,
    dimensions: u32,
    rows: impl Iterator<Item = &'a [f32]>,
) -> ArrayRef {
    nullable_vector_column(element, dimensions, rows.map(Some))
}

/// The column of a named vector attribute: a row that is `None` is null.
pub(crate) fn nullable_vector_column<'a>(
    element: DenseElement,
    dimensions: u32,
    rows: impl Iterator<Item = Option<&'a [f32]>>,
) -> ArrayRef {
    // The dimension is at most 8192.
    let dimensions = dimensions as i32;
    match element {
        DenseElement::F16 => Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float16Type, _, _>(
                rows.map(|v| v.map(|v| v.iter().map(|x| Some(half::f16::from_f32(*x))))),
                dimensions,
            ),
        ),
        DenseElement::F32 => Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                rows.map(|v| v.map(|v| v.iter().copied().map(Some))),
                dimensions,
            ),
        ),
    }
}

/// The type of a vector column: fixed-size lists of the element type.
pub(crate) fn vector_type(element: DenseElement, dimensions: u32) -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new(
            "item",
            match element {
                DenseElement::F16 => DataType::Float16,
                DenseElement::F32 => DataType::Float32,
            },
            true,
        )),
        dimensions as i32,
    )
}

/// The type of the column of a named vector attribute: a list of vectors when
/// it is multi-vector.
pub(crate) fn named_vector_type(
    element: DenseElement,
    spec: &fyrr_core::attributes::VectorAttribute,
) -> DataType {
    let vector = vector_type(element, spec.dimensions);
    if spec.multi {
        DataType::List(Arc::new(Field::new("item", vector, true)))
    } else {
        vector
    }
}

/// The column of a named vector attribute: a row that is `None` is null. A row
/// of a multi-vector attribute is its vectors one after the other, and a row
/// with none is null too.
pub(crate) fn named_vector_column<'a>(
    element: DenseElement,
    spec: &fyrr_core::attributes::VectorAttribute,
    rows: impl Iterator<Item = Option<&'a [f32]>>,
) -> ArrayRef {
    if !spec.multi {
        return nullable_vector_column(element, spec.dimensions, rows);
    }
    let dimensions = spec.dimensions as usize;
    let mut offsets = vec![0_i32];
    let mut valid = Vec::new();
    let mut vectors = Vec::new();
    for row in rows {
        let row = row.filter(|row| !row.is_empty());
        valid.push(row.is_some());
        vectors.extend(row.into_iter().flat_map(|row| row.chunks_exact(dimensions)));
        // A row holds at most as many vectors as its request's body has bytes.
        offsets.push(i32::try_from(vectors.len()).unwrap_or(i32::MAX));
    }
    let values = nullable_vector_column(element, spec.dimensions, vectors.into_iter().map(Some));
    Arc::new(ListArray::new(
        Arc::new(Field::new(
            "item",
            vector_type(element, spec.dimensions),
            true,
        )),
        OffsetBuffer::new(offsets.into()),
        values,
        Some(NullBuffer::from(valid)),
    ))
}

pub fn schema(dimensions: u32, element: DenseElement) -> Arc<Schema> {
    schema_with(dimensions, element, Vec::new())
}

/// The built-in columns and then `extra`: a namespace's attribute columns.
pub fn schema_with(dimensions: u32, element: DenseElement, extra: Vec<Field>) -> Arc<Schema> {
    let strings = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    let mut fields = vec![
        Field::new("source_id", DataType::Utf8, false),
        Field::new("root_id", DataType::Utf8, false),
        Field::new("record_id", DataType::Utf8, false),
        Field::new("source_revision", DataType::Utf8, false),
        Field::new("readers", strings.clone(), false),
        Field::new("vector", vector_type(element, dimensions), false),
        Field::new("searchable_text", DataType::Utf8, false),
        Field::new("metadata_json", DataType::Utf8, false),
        Field::new("record_order", DataType::UInt64, true),
        Field::new("connector_map", DataType::Binary, true),
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
                        || fyrr_reader::embedding::validate(
                            vector,
                            config.embedding.dimensions,
                            config.namespace.dense_element,
                        )
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

/// What a record adds to the size of an envelope besides its change. A
/// multi-vector attribute adds none: a record holds as many vectors as it
/// carries, which [`listed_bytes`] counts.
fn record_estimate(config: &Config) -> u64 {
    let dimensions = u64::from(config.embedding.dimensions)
        + config
            .namespace
            .vectors
            .values()
            .filter(|vector| !vector.multi)
            .map(|vector| u64::from(vector.dimensions))
            .sum::<u64>();
    2048 + dimensions * config.namespace.dense_element.bytes()
}

/// What the lists of the multi-vector attributes in `attributes` hold: the
/// vectors the rows carry, not the room a null row would take.
fn listed_bytes(config: &Config, attributes: &[(String, ArrayRef)]) -> u64 {
    config
        .namespace
        .vectors
        .iter()
        .filter(|(_, spec)| spec.multi)
        .filter_map(|(name, spec)| {
            let (_, column) = attributes.iter().find(|(named, _)| named == name)?;
            let lists = column.as_any().downcast_ref::<ListArray>()?;
            Some(lists.values().len() as u64 * u64::from(spec.dimensions))
        })
        .sum::<u64>()
        .saturating_mul(config.namespace.dense_element.bytes())
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
pub fn check_atomic(
    config: &Config,
    changes: &[ContentChange],
    attributes: &[(String, ArrayRef)],
) -> Result<()> {
    let (mut rows, mut bytes) = (
        0u64,
        BASE_BYTES.saturating_add(listed_bytes(config, attributes)),
    );
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
    attributes: Vec<(String, ArrayRef)>,
) -> Result<()> {
    check_atomic(config, &changes, &attributes)?;
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
        last: None,
        completed_generation: 0,
    }
}

/// What a permission read is compared with, to leave out what repeats the
/// published rights: a root whose permission revision is the one its rows hold,
/// which the committer would skip, and a container grant equal to the
/// published one. What the read keeps replaces them as it goes, so a later
/// batch is compared with the state the earlier ones make. It is used only
/// when nothing of the source waits to be published.
pub(crate) struct PermissionRepeats {
    dataset: Arc<lance::Dataset>,
    revisions: std::collections::BTreeMap<String, Option<String>>,
    grants: std::collections::BTreeMap<String, fyrr_core::connector::ContainerGrant>,
}

impl PermissionRepeats {
    pub(crate) async fn new(
        config: &Config,
        source: &str,
        dataset: Arc<lance::Dataset>,
    ) -> Result<Self> {
        Ok(Self {
            dataset,
            revisions: Default::default(),
            grants: crate::membership::published_container_grants(config, source).await?,
        })
    }

    /// Leaves out of `batch` the roots and grants that change nothing.
    pub(crate) async fn retain_changes(
        &mut self,
        config: &Config,
        source: &str,
        batch: &mut ConnectorBatch,
    ) -> Result<()> {
        let fyrr_core::connector::BatchBody::PermissionChanges {
            documents,
            container_grants,
        } = &mut batch.body
        else {
            return Err(WriterError::BatchInvalid);
        };
        let mut unknown: Vec<String> = documents
            .iter()
            .filter(|d| !self.revisions.contains_key(&d.root_id))
            .map(|d| d.root_id.clone())
            .collect();
        unknown.sort();
        unknown.dedup();
        if !unknown.is_empty() {
            let states = crate::apply::states(&self.dataset, source, &unknown).await?;
            for root in unknown {
                let revision = states
                    .get(&root)
                    .and_then(|s| s.permission.as_ref())
                    .and_then(|p| p.revision.clone());
                self.revisions.insert(root, revision);
            }
        }
        documents.retain(|d| {
            let repeat = self
                .revisions
                .get(&d.root_id)
                .is_some_and(|r| r.as_ref() == Some(&d.permission_revision));
            if !repeat {
                self.revisions
                    .insert(d.root_id.clone(), Some(d.permission_revision.clone()));
            }
            !repeat
        });
        let mut kept = Vec::new();
        for grant in std::mem::take(container_grants) {
            let mut tokens = grant.clone();
            tokens.readers = grant
                .readers
                .iter()
                .map(|t| crate::apply::reader_token(config, source, t))
                .collect::<Result<_>>()?;
            // Readers are a set: their order and repeats change no right.
            tokens.readers.sort();
            tokens.readers.dedup();
            if self.grants.get(&grant.container_id) != Some(&tokens) {
                self.grants.insert(grant.container_id.clone(), tokens);
                kept.push(grant);
            }
        }
        *container_grants = kept;
        Ok(())
    }
}

/// The permission batches of one read, held until they fill a part. A root or
/// container named twice keeps its last state, as the later batch observed it.
/// The roots go only into parts, which carry no end cursor, so a part is
/// measured exactly as it is put; the envelope that ends the read carries no
/// root, only its cursors, and the configuration makes room for it. What the
/// committer rewrites for the roots is its own to bound, in steps.
#[derive(Debug)]
pub struct HeldPermissions {
    /// Each root with what it adds to an envelope, as estimated: the estimate
    /// only proposes a part, which is measured as it encodes.
    documents: std::collections::BTreeMap<String, (fyrr_core::connector::DocumentPermission, u64)>,
    container_grants:
        std::collections::BTreeMap<String, (fyrr_core::connector::ContainerGrant, u64)>,
    /// The bounds of an envelope, as of a connector batch.
    limits: crate::batch::Limits,
    /// The connector cursor after the last batch of the read.
    end: Option<String>,
    /// Whether a part of the read was put.
    parted: bool,
}

type PermissionRoots = (
    Vec<fyrr_core::connector::DocumentPermission>,
    Vec<fyrr_core::connector::ContainerGrant>,
);

/// What `item` adds to an envelope as `write_contract` lays it out: its lines,
/// each indented by the three levels above it, and the separator after it.
fn item_bytes<T: serde::Serialize>(item: &T) -> u64 {
    let pretty = serde_json::to_vec_pretty(item)
        .unwrap_or_else(|error| unreachable!("permission roots serialize to JSON: {error}"));
    let lines = pretty.iter().filter(|b| **b == b'\n').count() as u64 + 1;
    pretty.len() as u64 + lines * 6 + 2
}

impl HeldPermissions {
    pub fn new(limits: crate::batch::Limits) -> Self {
        Self {
            documents: Default::default(),
            container_grants: Default::default(),
            limits,
            end: None,
            parted: false,
        }
    }

    /// Holds `batch`, its readers as tokens. A root that does not fit a part
    /// alone, as the next part would be put, refuses the read.
    pub fn push(
        &mut self,
        config: &Config,
        source: &str,
        batch: ConnectorBatch,
        state: &CursorState,
        base: &Option<BaseHead>,
    ) -> Result<()> {
        let fyrr_core::connector::BatchBody::PermissionChanges {
            documents,
            container_grants,
        } = batch.body
        else {
            return Err(WriterError::BatchInvalid);
        };
        let tokens = |readers: &[String]| -> Result<Vec<String>> {
            readers
                .iter()
                .map(|t| crate::apply::reader_token(config, source, t))
                .collect()
        };
        for mut document in documents {
            document.readers = tokens(&document.readers)?;
            self.alone(
                config,
                source,
                (vec![document.clone()], Vec::new()),
                state,
                base,
            )?;
            let bytes = item_bytes(&document);
            self.documents
                .insert(document.root_id.clone(), (document, bytes));
        }
        for mut grant in container_grants {
            grant.readers = tokens(&grant.readers)?;
            self.alone(
                config,
                source,
                (Vec::new(), vec![grant.clone()]),
                state,
                base,
            )?;
            let bytes = item_bytes(&grant);
            self.container_grants
                .insert(grant.container_id.clone(), (grant, bytes));
        }
        self.end = Some(batch.cursor.end);
        Ok(())
    }

    /// Refuses `root` when it does not fit an otherwise empty part with the
    /// largest header a part of this read can have: a `previous` and a
    /// sequence of the most digits, its other fields as they stay through the
    /// read. A root that passes fits any part it is put in.
    fn alone(
        &self,
        config: &Config,
        source: &str,
        root: PermissionRoots,
        state: &CursorState,
        base: &Option<BaseHead>,
    ) -> Result<()> {
        let mut largest = state.clone();
        largest.sequence = u64::MAX - 1;
        largest.last = Some(fyrr_core::connector::LastEnvelope {
            sequence: u64::MAX - 1,
            id: fyrr_core::envelope::envelope_id(
                &config.namespace.name,
                source,
                SyncKind::Permissions,
                0,
                0,
                None,
            ),
        });
        let part = permission_envelope(config, source, root, None, false, &largest, base)?;
        if !self.fits(&part, 1) {
            return Err(WriterError::PermissionRootOversized);
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.documents.len() + self.container_grants.len()
    }

    /// The first `count` roots, in order: the documents, then the containers.
    fn first(&self, count: usize) -> PermissionRoots {
        let documents: Vec<_> = self
            .documents
            .values()
            .take(count)
            .map(|(d, _)| d.clone())
            .collect();
        let grants = self
            .container_grants
            .values()
            .take(count - documents.len())
            .map(|(g, _)| g.clone())
            .collect();
        (documents, grants)
    }

    fn drop_first(&mut self, mut count: usize) {
        while count > 0 && self.documents.pop_first().is_some() {
            count -= 1;
        }
        while count > 0 && self.container_grants.pop_first().is_some() {
            count -= 1;
        }
        self.parted = true;
    }

    /// Whether `envelope`, of `roots` roots, fits an envelope, its bytes as
    /// they encode: what the committer checks, inclusive.
    fn fits(&self, envelope: &PreparedEnvelope, roots: usize) -> bool {
        roots as u64 <= self.limits.max_rows
            && write_contract(envelope).len() as u64 <= self.limits.max_bytes
    }

    /// The part of the first roots that fits an envelope, the most that do,
    /// with how many it holds: proposed by the estimates, then grown or shrunk
    /// by a root on the envelope as it encodes. None when not even the first
    /// root fits.
    fn largest_part(
        &self,
        config: &Config,
        source: &str,
        state: &CursorState,
        base: &Option<BaseHead>,
    ) -> Result<Option<(PreparedEnvelope, usize)>> {
        let build = |count: usize| {
            permission_envelope(config, source, self.first(count), None, false, state, base)
        };
        let mut total = write_contract(&build(0)?).len() as u64;
        let mut count = 0;
        for bytes in self
            .documents
            .values()
            .map(|(_, b)| *b)
            .chain(self.container_grants.values().map(|(_, b)| *b))
        {
            if count as u64 >= self.limits.max_rows
                || total.saturating_add(bytes) > self.limits.max_bytes
            {
                break;
            }
            total += bytes;
            count += 1;
        }
        let mut count = count.clamp(1, self.len());
        let mut fit = None;
        while count > 0 {
            let envelope = build(count)?;
            if self.fits(&envelope, count) {
                fit = Some(envelope);
                break;
            }
            count -= 1;
        }
        let Some(mut fit) = fit else {
            return Ok(None);
        };
        while count < self.len() {
            let envelope = build(count + 1)?;
            if !self.fits(&envelope, count + 1) {
                break;
            }
            fit = envelope;
            count += 1;
        }
        Ok(Some((fit, count)))
    }

    /// Puts parts while what is held does not fit one, or, with `all`, until
    /// nothing is held.
    async fn put_parts(
        &mut self,
        config: &Config,
        source: &str,
        state: &mut CursorState,
        lease: &crate::sync_lease::CursorLease,
        base: &Option<BaseHead>,
        all: bool,
    ) -> Result<()> {
        while self.len() > 0 {
            let whole = permission_envelope(
                config,
                source,
                self.first(self.len()),
                None,
                false,
                state,
                base,
            )?;
            if self.fits(&whole, self.len()) {
                if !all {
                    return Ok(());
                }
                put_envelope(config, whole, false, state, lease).await?;
                self.drop_first(self.len());
                return Ok(());
            }
            let (part, count) = self
                .largest_part(config, source, state, base)?
                .ok_or(WriterError::PermissionRootOversized)?;
            put_envelope(config, part, false, state, lease).await?;
            self.drop_first(count);
        }
        Ok(())
    }
}

/// Puts what a permission read holds over the bounds of an envelope as parts
/// of the read, each within them. A part is not confirmed: the cursor is written
/// only by the envelope that ends the read, which chains after the parts, so
/// the committer publishes the read whole or not at all. A read that fails
/// leaves its parts inert.
pub async fn permission_parts(
    config: &Config,
    source: &str,
    held: &mut HeldPermissions,
    state: &mut CursorState,
    lease: &crate::sync_lease::CursorLease,
    base: &Option<BaseHead>,
) -> Result<()> {
    held.put_parts(config, source, state, lease, base, false)
        .await
}

/// Puts what a permission read still holds as parts, then the envelope that
/// ends the read, which holds no root and moves the cursor. A read that
/// replaces a rejection, or that put parts, ends with that envelope; a
/// replacement keeps the connector cursor when the read produced no batch. Any
/// other read that holds no changes moves only the cursor.
pub async fn permission_end(
    config: &Config,
    source: &str,
    held: &mut HeldPermissions,
    state: &mut CursorState,
    lease: &crate::sync_lease::CursorLease,
    base: Option<BaseHead>,
    replaces_rejection: bool,
) -> Result<()> {
    if held.len() == 0 && !replaces_rejection && !held.parted {
        if let Some(end) = held.end.take() {
            state.cursor = Some(end);
            return lease.write(state.clone()).await;
        }
        return Ok(());
    }
    held.put_parts(config, source, state, lease, &base, true)
        .await?;
    let last = permission_envelope(
        config,
        source,
        (Vec::new(), Vec::new()),
        held.end.clone(),
        true,
        state,
        &base,
    )?;
    put_envelope(config, last, true, state, lease).await
}

/// The permissions envelope of `roots`, as it would be put now: a part, or the
/// envelope that ends the read, which moves the cursor to `end` when there is
/// one.
fn permission_envelope(
    config: &Config,
    source: &str,
    (documents, container_grants): PermissionRoots,
    end: Option<String>,
    complete: bool,
    state: &CursorState,
    base: &Option<BaseHead>,
) -> Result<PreparedEnvelope> {
    let advances = complete && end.is_some();
    let cursor = fyrr_core::connector::CursorStep {
        start: state.cursor.clone(),
        // A part leaves the cursor and carries no end of its own. An end that
        // leaves the cursor keeps the one it has.
        end: if complete {
            end.unwrap_or_else(|| state.cursor.clone().unwrap_or_else(|| "complete".into()))
        } else {
            String::new()
        },
    };
    let payload = EnvelopePayload::Permissions(PermissionsPayload {
        complete_read: complete,
        documents,
        container_grants,
    });
    build_envelope(
        config,
        source,
        SyncKind::Permissions,
        cursor,
        payload,
        advances,
        state,
        base.clone(),
    )
}

/// A members part or a listing part, as one envelope that moves the cursor,
/// unless it ends a read.
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
    let payload = match &batch.body {
        BatchBody::PrincipalSnapshot(part) => {
            let (store, root) = namespace_store(config)?;
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
    let envelope = build_envelope(
        config,
        source,
        batch.kind,
        batch.cursor.clone(),
        payload,
        !complete_read,
        state,
        base,
    )?;
    put_envelope(config, envelope, true, state, lease).await
}

/// The envelope that is not content that `state` puts next: a permissions
/// envelope follows the one before it in the chain, and carries the
/// generation of the lease it is written under.
#[allow(clippy::too_many_arguments)]
fn build_envelope(
    config: &Config,
    source: &str,
    kind: SyncKind,
    cursor: fyrr_core::connector::CursorStep,
    payload: EnvelopePayload,
    advances_cursor: bool,
    state: &CursorState,
    base: Option<BaseHead>,
) -> Result<PreparedEnvelope> {
    let sequence = state
        .sequence
        .checked_add(1)
        .ok_or(WriterError::CursorSequenceOverflow)?;
    let chained = kind == SyncKind::Permissions;
    let lease_generation = state
        .lease
        .as_ref()
        .filter(|_| chained)
        .map_or(0, |lease| lease.epoch);
    let previous = state
        .last
        .as_ref()
        .filter(|_| chained)
        .map(|last| last.id.clone());
    Ok(PreparedEnvelope {
        schema_version: ENVELOPE_CONTRACT_VERSION,
        envelope_id: fyrr_core::envelope::leased_envelope_id(
            &config.namespace.name,
            source,
            kind,
            sequence,
            state.rejection_generation,
            state.cursor.as_deref(),
            lease_generation,
        ),
        namespace: config.namespace.name.clone(),
        source_id: source.into(),
        kind,
        sequence,
        rejection_generation: state.rejection_generation,
        restore_sequence: state.restore_sequence,
        cursor,
        advances_cursor,
        base_head: base,
        created_at: fyrr_core::clock::now(),
        lease_generation,
        previous,
        payload,
    })
}

/// Puts `envelope` and records it in `state`. `confirmed` writes the cursor
/// after it; a part of a permission read is put without, and the envelope that
/// ends the read confirms it.
async fn put_envelope(
    config: &Config,
    envelope: PreparedEnvelope,
    confirmed: bool,
    state: &mut CursorState,
    lease: &crate::sync_lease::CursorLease,
) -> Result<()> {
    let (store, root) = namespace_store(config)?;
    lease.state().await?;
    lease.persist_observed().await?;
    crate::commit::ensure_writable(config).await?;
    put_immutable(
        store.as_ref(),
        &root
            .join("envelopes")
            .join(format!("{}.json", envelope.envelope_id)),
        &envelope,
    )
    .await?;
    state.sequence = envelope.sequence;
    if envelope.advances_cursor {
        state.cursor = Some(envelope.cursor.end.clone());
    }
    if envelope.kind == SyncKind::Permissions {
        state.last = Some(fyrr_core::connector::LastEnvelope {
            sequence: envelope.sequence,
            id: envelope.envelope_id.clone(),
        });
        if crate::apply::complete_read(&envelope) {
            state.completed_generation = state.completed_generation.max(state.rejection_generation);
        }
    }
    if !confirmed {
        return Ok(());
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

/// Typed attribute columns, built row by row from the values of a request, in
/// the order of `order`.
pub(crate) struct AttributeColumns {
    order: Vec<String>,
    schema: fyrr_core::attributes::AttributeSchema,
    columns: Vec<Vec<fyrr_core::attributes::AttributeValue>>,
}
impl AttributeColumns {
    pub(crate) fn new(schema: &fyrr_core::attributes::AttributeSchema, order: Vec<String>) -> Self {
        Self {
            columns: vec![Vec::new(); order.len()],
            order,
            schema: schema.clone(),
        }
    }

    /// One row: each attribute of the schema takes its value from `values`, or
    /// is null. A value that does not fit its attribute is an error and the
    /// columns are not to be used.
    pub(crate) fn push(
        &mut self,
        values: &serde_json::Map<String, serde_json::Value>,
    ) -> std::result::Result<(), fyrr_core::attributes::AttributeError> {
        for (name, column) in self.order.iter().zip(&mut self.columns) {
            column.push(fyrr_core::attributes::resolve(
                &mut self.schema,
                name,
                values.get(name).unwrap_or(&serde_json::Value::Null),
            )?);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<(String, ArrayRef)> {
        let schema = self.schema;
        self.order
            .into_iter()
            .zip(self.columns)
            .map(|(name, values)| {
                let column = attribute_array(schema[&name], values);
                (name, column)
            })
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
    fn a_multi_vector_attribute_weighs_the_vectors_its_rows_carry_not_the_room_of_one() {
        let head = "[namespace]\nname='multi'\n[storage]\nuri='file:///x'\n\
                    [embedding]\nendpoint='http://127.0.0.1:9/v1'\n[namespace.write_api]\n";
        let declared: String = (0..16)
            .map(|n| format!("[namespace.vectors.m{n}]\ndimensions=8192\nmulti=true\n"))
            .collect();
        let multi = fyrr_core::config::parse(&format!("{head}{declared}"))
            .unwrap()
            .config;
        let plain = fyrr_core::config::parse(head).unwrap().config;
        // A record with no list takes no more room than one without the attributes.
        assert_eq!(record_estimate(&multi), record_estimate(&plain));
        // Two rows carry vectors, two carry none, one of them an empty list.
        let spec = &multi.namespace.vectors["m0"];
        let flat = vec![0.5_f32; 2 * 8192];
        let column = named_vector_column(
            multi.namespace.dense_element,
            spec,
            [
                None,
                Some(flat.as_slice()),
                Some(&[][..]),
                Some(&flat[..8192]),
            ]
            .into_iter(),
        );
        let attributes = [("m0".to_owned(), column)];
        assert_eq!(
            listed_bytes(&multi, &attributes),
            3 * 8192 * multi.namespace.dense_element.bytes()
        );
        assert_eq!(listed_bytes(&multi, &[]), 0);
        assert_eq!(listed_bytes(&plain, &attributes), 0);
    }
    #[test]
    fn the_table_holds_exactly_the_reserved_built_in_columns() {
        let schema = schema(8, DenseElement::F16);
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
