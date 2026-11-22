//! Validates what a provider hands over, at the boundary:
//! the batch header as a versioned contract document and content rows as
//! Arrow, checked against this build's schema and then row by row with the
//! same semantic rules as the JSON contract. Messages name fields, never
//! values.

use crate::{Result, WriterError};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Int64Type, TimestampMicrosecondType, UInt32Type};
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray};
use arrow_schema::DataType;
use chrono::{DateTime, Utc};
use fyrr_core::connector::{
    BatchBody, ChangeOp, ConnectorBatch, DocumentChange, DocumentMetadata, Location, Section,
    SyncKind, check_document_changes_schema,
};
use fyrr_core::parse_contract;
use fyrr_core::version::Contract;
use serde::Serialize;

/// Bounds of one content batch, from the configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Limits {
    /// `writer.envelope_max_rows`
    pub max_rows: u64,
    /// `writer.envelope_max_bytes`, counted across every Arrow buffer.
    pub max_bytes: u64,
    /// `document_max_bytes`: larger documents would be quarantined.
    pub document_max_bytes: u64,
    pub row_warning: Option<u64>,
    pub row_error: Option<u64>,
    pub retention_days: Option<u32>,
}

impl Limits {
    pub fn for_source(
        config: &fyrr_core::config::Config,
        source: &fyrr_core::config::SourceConfig,
    ) -> Self {
        Self {
            max_rows: config
                .writer
                .envelope_max_rows
                .min(config.writer.commit.group_max_rows)
                .min(256),
            max_bytes: config.writer.envelope_max_bytes.as_u64(),
            document_max_bytes: source
                .document_max_bytes
                .unwrap_or(config.writer.document_max_bytes)
                .as_u64(),
            row_warning: source.row_warning,
            row_error: source.row_error,
            retention_days: source.retention_days,
        }
    }
}

/// What one sync kind produced; counts only, never values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KindStats {
    pub batches: u64,
    pub upserts: u64,
    pub deletes: u64,
    pub sections: u64,
    pub text_bytes: u64,
    /// Documents above `document_max_bytes` (quarantined in a real sync).
    pub oversized: u64,
    #[serde(default)]
    pub quarantined: u64,
    #[serde(default)]
    pub row_warning: bool,
    pub permission_documents: u64,
    pub container_grants: u64,
    pub users: u64,
    pub groups: u64,
    #[serde(default)]
    pub members_complete: bool,
    pub listed_root_ids: u64,
    /// Whether the last listing part was final and complete.
    pub listing_complete: bool,
    /// Whether the last batch moved the cursor.
    pub cursor_advanced: bool,
}

/// Checks batches of one sync kind of one source, in order.
#[derive(Debug)]
pub struct BatchChecker {
    source_id: String,
    kind: SyncKind,
    pub(crate) limits: Limits,
    cursor: Option<String>,
    pub stats: KindStats,
}

impl BatchChecker {
    pub fn new(source_id: &str, kind: SyncKind, limits: Limits, cursor: Option<String>) -> Self {
        Self {
            source_id: source_id.to_owned(),
            kind,
            limits,
            cursor,
            stats: KindStats::default(),
        }
    }

    /// Checks one batch: `header` is the contract JSON (content rows
    /// excluded), `rows` the Arrow rows of a content batch.
    pub fn check(&mut self, header: &str, rows: Option<&RecordBatch>) -> Result<(), String> {
        self.check_inner(header, rows, false)
    }

    pub fn check_prepared(
        &mut self,
        header: &str,
        rows: Option<&RecordBatch>,
    ) -> Result<(), String> {
        self.check_inner(header, rows, true)
    }

    fn check_inner(
        &mut self,
        header: &str,
        rows: Option<&RecordBatch>,
        quarantine: bool,
    ) -> Result<(), String> {
        if header.len() as u64 > self.limits.max_bytes {
            return Err(WriterError::BatchHeaderExceedsLimit.into());
        }
        let mut batch: ConnectorBatch =
            parse_contract(header.as_bytes()).map_err(|error| error.to_string())?;
        if batch.source_id != self.source_id {
            return Err("source_id differs from the source being synced".into());
        }
        if batch.kind != self.kind {
            return Err(format!(
                "kind differs from the sync being run ({})",
                self.kind.as_str()
            ));
        }
        if batch.cursor.start != self.cursor {
            return Err("cursor.start does not continue from the previous cursor".into());
        }
        match (&mut batch.body, rows) {
            (BatchBody::DocumentChanges { changes }, Some(rows)) => {
                if !changes.is_empty() {
                    return Err("content rows travel as Arrow, not in the header".into());
                }
                if rows.num_rows() as u64 > self.limits.max_rows {
                    return Err("a content batch exceeds writer.envelope_max_rows".into());
                }
                // Includes nullable extensions and nested buffers, before any
                // row or text is copied into Rust objects.
                if rows_bytes(rows).is_none_or(|bytes| bytes > self.limits.max_bytes) {
                    return Err(
                        "a content batch exceeds writer.envelope_max_bytes (Arrow buffers)".into(),
                    );
                }
                *changes = document_changes(rows)?;
                // The header passed alone; now the whole batch with its rows.
                if !quarantine {
                    batch.validate()?;
                } else {
                    let mut roots = std::collections::BTreeSet::new();
                    if changes.iter().any(|c| !roots.insert(&c.root_id)) {
                        return Err(WriterError::DuplicateDocumentChange.into());
                    }
                }
            }
            (BatchBody::DocumentChanges { .. }, None) => {
                return Err("a content batch needs Arrow rows".into());
            }
            (_, Some(_)) => return Err("only content batches carry Arrow rows".into()),
            (_, None) => {}
        }
        let items = match &batch.body {
            BatchBody::DocumentChanges { changes } => changes.len(),
            BatchBody::PermissionChanges {
                documents,
                container_grants,
            } => documents.len() + container_grants.len(),
            BatchBody::PrincipalSnapshot(p) => p.users.len() + p.groups.len(),
            BatchBody::Listing(p) => p.root_ids.len(),
        };
        if items as u64 > self.limits.max_rows {
            return Err(WriterError::BatchRowsExceedLimit.into());
        }
        match &batch.body {
            BatchBody::DocumentChanges { changes } => self.count_content(changes)?,
            BatchBody::PermissionChanges {
                documents,
                container_grants,
            } => {
                self.stats.permission_documents += documents.len() as u64;
                self.stats.container_grants += container_grants.len() as u64;
            }
            BatchBody::PrincipalSnapshot(snapshot) => {
                self.stats.users += snapshot.users.len() as u64;
                self.stats.groups += snapshot.groups.len() as u64;
                self.stats.members_complete = snapshot.last;
            }
            BatchBody::Listing(listing) => {
                self.stats.listed_root_ids += listing.root_ids.len() as u64;
                self.stats.listing_complete = listing.last && listing.complete;
            }
        }
        self.stats.batches += 1;
        self.stats.cursor_advanced = batch.cursor.start.as_deref() != Some(&batch.cursor.end);
        self.cursor = Some(batch.cursor.end);
        Ok(())
    }

    fn count_content(&mut self, changes: &[DocumentChange]) -> Result<(), String> {
        if changes.len() as u64 > self.limits.max_rows {
            return Err(format!(
                "a content batch has {} rows, more than writer.envelope_max_rows ({})",
                changes.len(),
                self.limits.max_rows
            ));
        }
        let mut batch_bytes = 0u64;
        for change in changes {
            match change.op {
                ChangeOp::Upsert => self.stats.upserts += 1,
                ChangeOp::Delete => self.stats.deletes += 1,
            }
            let bytes: u64 = change
                .sections
                .iter()
                .map(|section| section.text.len() as u64)
                .sum();
            if bytes > self.limits.document_max_bytes {
                self.stats.oversized += 1;
            }
            self.stats.sections += change.sections.len() as u64;
            batch_bytes += bytes;
        }
        if batch_bytes > self.limits.max_bytes {
            return Err(format!(
                "a content batch holds {batch_bytes} text bytes, more than writer.envelope_max_bytes ({})",
                self.limits.max_bytes
            ));
        }
        self.stats.text_bytes += batch_bytes;
        Ok(())
    }
}

/// The bytes held by the buffers of `rows` (data, offsets and validity, nested
/// ones included); `None` when the sum overflows.
pub(crate) fn rows_bytes(rows: &RecordBatch) -> Option<u64> {
    fn data_bytes(data: &arrow_data::ArrayData) -> Option<u64> {
        let own = data
            .buffers()
            .iter()
            .map(|buffer| buffer.len() as u64)
            .chain(data.nulls().map(|nulls| nulls.buffer().len() as u64));
        let mut total = 0u64;
        for bytes in own {
            total = total.checked_add(bytes)?;
        }
        for child in data.child_data() {
            total = total.checked_add(data_bytes(child)?)?;
        }
        Some(total)
    }
    rows.columns().iter().try_fold(0u64, |total, column| {
        total.checked_add(data_bytes(&column.to_data())?)
    })
}

/// Reads document-change rows after checking their schema (contract name,
/// known major, every field's type and nullability).
pub fn document_changes(rows: &RecordBatch) -> Result<Vec<DocumentChange>, String> {
    check_document_changes_schema(rows.schema().as_ref()).map_err(|error| error.to_string())?;
    let columns = Columns::new(rows)?;
    (0..rows.num_rows())
        .map(|row| {
            columns
                .change(row)
                .map_err(|error| format!("row {row}: {error}"))
        })
        .collect()
}

fn column<'a>(rows: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    rows.column_by_name(name)
        .ok_or_else(|| format!("missing field {name}"))
}

fn strings<'a>(array: &'a dyn Array, field: &str) -> Result<&'a StringArray, String> {
    array
        .as_string_opt::<i32>()
        .ok_or_else(|| format!("field {field} is not utf8"))
}

fn lists<'a>(array: &'a dyn Array, field: &str) -> Result<&'a ListArray, String> {
    array
        .as_list_opt::<i32>()
        .ok_or_else(|| format!("field {field} is not a list"))
}

/// A required string: a null in a non-nullable field is a contract violation
/// that pyarrow does not prevent.
fn required(array: &StringArray, row: usize, field: &str) -> Result<String, String> {
    if array.is_null(row) {
        return Err(format!("field {field} is not nullable but holds a null"));
    }
    Ok(array.value(row).to_owned())
}

fn optional(array: &StringArray, row: usize) -> Option<String> {
    array.is_valid(row).then(|| array.value(row).to_owned())
}

/// The strings of a list cell; `nullable_cell` decides whether a null list is
/// allowed (then `None`). List items are never nullable in the contract.
fn string_list(
    array: &ListArray,
    row: usize,
    field: &str,
    nullable_cell: bool,
) -> Result<Option<Vec<String>>, String> {
    if array.is_null(row) {
        if nullable_cell {
            return Ok(None);
        }
        return Err(format!("field {field} is not nullable but holds a null"));
    }
    let values = array.value(row);
    let items = strings(values.as_ref(), field)?;
    (0..items.len())
        .map(|index| required(items, index, &format!("{field}[]")))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// The numbers of a nullable list cell of float32, whose items are never null.
fn float_list(array: &ListArray, row: usize, field: &str) -> Result<Option<Vec<f32>>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    let values = array.value(row);
    let items = values
        .as_primitive_opt::<Float32Type>()
        .ok_or_else(|| format!("field {field} is not float32"))?;
    if items.null_count() > 0 {
        return Err(format!("field {field}[] is not nullable but holds a null"));
    }
    Ok(Some(items.values().to_vec()))
}

struct Columns<'a> {
    op: &'a StringArray,
    root_id: &'a StringArray,
    source_revision: &'a StringArray,
    revision_order: &'a dyn Array,
    expected_prior_revision: &'a StringArray,
    permission_revision: &'a StringArray,
    readers: &'a ListArray,
    containers: &'a ListArray,
    source_uri: &'a StringArray,
    source_type: &'a StringArray,
    texts: [(&'static str, &'a StringArray); 5],
    labels: &'a ListArray,
    created_at: &'a dyn Array,
    updated_at: &'a dyn Array,
    connector_map: &'a StringArray,
    attributes: &'a ListArray,
    sections: &'a ListArray,
}

impl<'a> Columns<'a> {
    fn new(rows: &'a RecordBatch) -> Result<Self, String> {
        let text = |name: &'static str| -> Result<&'a StringArray, String> {
            strings(column(rows, name)?.as_ref(), name)
        };
        let list = |name: &'static str| -> Result<&'a ListArray, String> {
            lists(column(rows, name)?.as_ref(), name)
        };
        Ok(Self {
            op: text("op")?,
            root_id: text("root_id")?,
            source_revision: text("source_revision")?,
            revision_order: column(rows, "revision_order")?.as_ref(),
            expected_prior_revision: text("expected_prior_revision")?,
            permission_revision: text("permission_revision")?,
            readers: list("readers")?,
            containers: list("containers")?,
            source_uri: text("source_uri")?,
            source_type: text("source_type")?,
            texts: [
                ("title", text("title")?),
                ("author", text("author")?),
                ("team", text("team")?),
                ("project", text("project")?),
                ("status", text("status")?),
            ],
            labels: list("labels")?,
            created_at: column(rows, "created_at")?.as_ref(),
            updated_at: column(rows, "updated_at")?.as_ref(),
            connector_map: text("connector_map")?,
            attributes: list("attributes")?,
            sections: list("sections")?,
        })
    }

    fn change(&self, row: usize) -> Result<DocumentChange, String> {
        let op = match required(self.op, row, "op")?.as_str() {
            "upsert" => ChangeOp::Upsert,
            "delete" => ChangeOp::Delete,
            _ => return Err("op must be upsert or delete".into()),
        };
        let revision_order = self.revision_order.as_primitive::<Int64Type>();
        let list = |array: &ListArray, field: &str| {
            string_list(array, row, field, false).map(Option::unwrap_or_default)
        };
        let source_uri = optional(self.source_uri, row);
        let source_type = optional(self.source_type, row);
        let [title, author, team, project, status] =
            self.texts.map(|(_, array)| optional(array, row));
        let labels = list(self.labels, "labels")?;
        let created_at = instant(self.created_at, row, "created_at")?;
        let updated_at = instant(self.updated_at, row, "updated_at")?;
        let connector_map = match optional(self.connector_map, row) {
            None => None,
            Some(text) => Some(
                serde_json::from_str(&text)
                    .map_err(|_| "field connector_map is not a JSON object".to_owned())?,
            ),
        };
        let has_metadata = source_uri.is_some()
            || source_type.is_some()
            || [&title, &author, &team, &project, &status]
                .iter()
                .any(|value| value.is_some())
            || !labels.is_empty()
            || created_at.is_some()
            || updated_at.is_some()
            || connector_map.is_some();
        let metadata = if has_metadata {
            Some(DocumentMetadata {
                source_uri: source_uri.ok_or("field source_uri is null on a row with metadata")?,
                source_type: source_type
                    .ok_or("field source_type is null on a row with metadata")?,
                title,
                author,
                team,
                project,
                status,
                labels,
                created_at,
                updated_at,
                connector_map,
            })
        } else {
            None
        };
        Ok(DocumentChange {
            op,
            root_id: required(self.root_id, row, "root_id")?,
            source_revision: optional(self.source_revision, row),
            revision_order: revision_order
                .is_valid(row)
                .then(|| revision_order.value(row)),
            expected_prior_revision: optional(self.expected_prior_revision, row),
            permission_revision: optional(self.permission_revision, row),
            readers: list(self.readers, "readers")?,
            containers: list(self.containers, "containers")?,
            metadata,
            attributes: list(self.attributes, "attributes")?,
            sections: self.sections(row)?,
        })
    }

    fn sections(&self, row: usize) -> Result<Vec<Section>, String> {
        if self.sections.is_null(row) {
            return Err("field sections is not nullable but holds a null".into());
        }
        let cell = self.sections.value(row);
        let items: &StructArray = cell
            .as_struct_opt()
            .ok_or("field sections[] is not a struct")?;
        if items.null_count() > 0 {
            return Err("field sections[] is not nullable but holds a null".into());
        }
        let field = |name: &str| {
            items
                .column_by_name(name)
                .ok_or_else(|| format!("missing field sections[].{name}"))
        };
        let text = strings(field("text")?.as_ref(), "sections[].text")?;
        let kind = strings(field("location_kind")?.as_ref(), "sections[].location_kind")?;
        let label = strings(
            field("location_label")?.as_ref(),
            "sections[].location_label",
        )?;
        let index = field("location_index")?.as_primitive::<UInt32Type>();
        let readers = lists(field("readers")?.as_ref(), "sections[].readers")?;
        // Absent in a batch of contract minor 1.3.
        let vectors = items
            .column_by_name("vector")
            .map(|column| lists(column.as_ref(), "sections[].vector"))
            .transpose()?;
        (0..items.len())
            .map(|at| {
                let location = match optional(kind, at) {
                    Some(kind) => Some(Location {
                        kind,
                        label: optional(label, at),
                        index: index.is_valid(at).then(|| index.value(at)),
                    }),
                    None if label.is_valid(at) || index.is_valid(at) => {
                        return Err(
                            "sections[]: a location label or index needs location_kind".to_owned()
                        );
                    }
                    None => None,
                };
                Ok(Section {
                    text: required(text, at, "sections[].text")?,
                    location,
                    readers: string_list(readers, at, "sections[].readers", true)?,
                    vector: vectors
                        .map(|vectors| float_list(vectors, at, "sections[].vector"))
                        .transpose()?
                        .flatten(),
                })
            })
            .collect()
    }
}

fn instant(array: &dyn Array, row: usize, field: &str) -> Result<Option<DateTime<Utc>>, String> {
    if !matches!(array.data_type(), DataType::Timestamp(..)) {
        return Err(format!("field {field} is not a timestamp"));
    }
    let values = array.as_primitive::<TimestampMicrosecondType>();
    if values.is_null(row) {
        return Ok(None);
    }
    DateTime::from_timestamp_micros(values.value(row))
        .map(Some)
        .ok_or_else(|| format!("field {field} is out of range"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{Int64Array, TimestampMicrosecondArray, UInt32Array};
    use arrow_schema::Schema;
    use arrow_schema::{Field, Fields};
    use fyrr_core::connector::{ARROW_VERSION_KEY, document_changes_arrow_schema};
    use serde_json::json;
    use std::sync::Arc;

    /// An empty, contract-exact batch.
    fn empty_document_changes() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(document_changes_arrow_schema()))
    }

    const SENTINEL: &str = "SENTINEL-batch-8e21";

    fn limits() -> Limits {
        Limits {
            max_rows: 10,
            max_bytes: 64 << 10,
            document_max_bytes: 20,
            row_warning: None,
            row_error: None,
            retention_days: None,
        }
    }

    /// One row; nulls in non-nullable fields cannot be built safely in Rust
    /// and are covered end to end with pyarrow producers.
    struct Row {
        op: &'static str,
        root_id: String,
        readers: Vec<String>,
        title: Option<String>,
        sections: Vec<String>,
        attributes: Vec<String>,
        connector_map: Option<String>,
    }

    impl Default for Row {
        fn default() -> Self {
            Self {
                op: "upsert",
                root_id: "doc-1".into(),
                readers: vec!["grp:eng".into()],
                title: Some("Title".into()),
                sections: vec!["Body text.".into()],
                attributes: vec!["k=v".into()],
                connector_map: Some("{\"a\":1}".into()),
            }
        }
    }

    fn item() -> Arc<Field> {
        Arc::new(Field::new("item", DataType::Utf8, false))
    }

    fn string_lists(values: Vec<Vec<String>>) -> ArrayRef {
        let mut builder = ListBuilder::new(StringBuilder::new()).with_field(item());
        for value in values {
            for entry in value {
                builder.values().append_value(entry);
            }
            builder.append(true);
        }
        Arc::new(builder.finish())
    }

    fn sections(rows: &[Row]) -> ArrayRef {
        let schema = document_changes_arrow_schema();
        let DataType::List(section_item) = schema
            .field_with_name("sections")
            .expect("sections")
            .data_type()
            .clone()
        else {
            unreachable!("sections is a list")
        };
        let DataType::Struct(fields) = section_item.data_type().clone() else {
            unreachable!("sections items are structs")
        };
        let texts: Vec<String> = rows.iter().flat_map(|row| row.sections.clone()).collect();
        let count = texts.len();
        let children: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(texts)),
            Arc::new(StringArray::from(vec![Some("heading"); count])),
            Arc::new(StringArray::new_null(count)),
            Arc::new(UInt32Array::new_null(count)),
            Arc::new(ListArray::new_null(item(), count)),
            Arc::new(ListArray::new_null(
                Arc::new(Field::new("item", DataType::Float32, false)),
                count,
            )),
        ];
        let structs = StructArray::try_new(fields, children, None).expect("sections");
        Arc::new(ListArray::new(
            section_item,
            arrow_buffer::OffsetBuffer::from_lengths(rows.iter().map(|row| row.sections.len())),
            Arc::new(structs),
            None,
        ))
    }

    fn build(rows: &[Row]) -> RecordBatch {
        let count = rows.len();
        let text =
            |values: Vec<Option<String>>| -> ArrayRef { Arc::new(StringArray::from(values)) };
        let nulls = || -> ArrayRef { Arc::new(StringArray::new_null(count)) };
        let empty = || string_lists(vec![Vec::new(); count]);
        let columns: Vec<ArrayRef> = vec![
            text(rows.iter().map(|row| Some(row.op.to_owned())).collect()),
            text(rows.iter().map(|row| Some(row.root_id.clone())).collect()),
            text(vec![Some("r1".to_owned()); count]),
            Arc::new(Int64Array::new_null(count)),
            nulls(),
            nulls(),
            string_lists(rows.iter().map(|row| row.readers.clone()).collect()),
            empty(),
            text(vec![Some("x://doc".to_owned()); count]),
            text(vec![Some("test.doc".to_owned()); count]),
            text(rows.iter().map(|row| row.title.clone()).collect()),
            nulls(),
            nulls(),
            nulls(),
            nulls(),
            empty(),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_759_225_272_512_000); count])
                    .with_timezone("UTC"),
            ),
            Arc::new(TimestampMicrosecondArray::new_null(count).with_timezone("UTC")),
            text(rows.iter().map(|row| row.connector_map.clone()).collect()),
            string_lists(rows.iter().map(|row| row.attributes.clone()).collect()),
            sections(rows),
        ];
        RecordBatch::try_new(Arc::new(document_changes_arrow_schema()), columns)
            .expect("contract batch")
    }

    fn header(start: Option<&str>, end: &str) -> String {
        json!({
            "schema_version": "1.0",
            "source_id": "src",
            "kind": "content",
            "cursor": { "start": start, "end": end },
            "body": { "type": "document_changes", "changes": [] }
        })
        .to_string()
    }

    fn checker() -> BatchChecker {
        BatchChecker::new("src", SyncKind::Content, limits(), None)
    }

    #[test]
    fn contract_rows_convert_and_count() {
        let rows = build(&[
            Row::default(),
            Row {
                root_id: "doc-2".into(),
                sections: vec!["x".repeat(30)],
                ..Row::default()
            },
        ]);
        let changes = document_changes(&rows).expect("valid rows");
        let metadata = changes[0].metadata.as_ref().expect("metadata");
        assert_eq!(metadata.title.as_deref(), Some("Title"));
        assert_eq!(
            metadata.created_at.map(|instant| instant.to_rfc3339()),
            Some("2025-09-30T09:41:12.512+00:00".to_owned())
        );
        let mut checker = checker();
        checker
            .check(&header(None, "c1"), Some(&rows))
            .expect("first batch");
        checker
            .check(&header(Some("c1"), "c2"), Some(&build(&[Row::default()])))
            .expect("second batch");
        assert_eq!(checker.stats.batches, 2);
        assert_eq!(checker.stats.upserts, 3);
        assert_eq!(
            checker.stats.oversized, 1,
            "30 bytes exceed document_max_bytes 20"
        );
        assert!(checker.stats.cursor_advanced);
    }

    /// `batch` as a producer of contract minor `version` without the section
    /// vector column writes it.
    fn without_section_vector(batch: &RecordBatch, version: &str) -> RecordBatch {
        let schema = batch.schema();
        let at = schema.index_of("sections").expect("sections");
        let list = batch.column(at).as_list::<i32>();
        let (fields, mut children, _) = list.values().as_struct().clone().into_parts();
        let kept = fields.len() - 1;
        children.truncate(kept);
        let fields: Fields = fields.iter().take(kept).cloned().collect();
        let structs = StructArray::try_new(fields.clone(), children, None).expect("sections");
        let item = Arc::new(Field::new("item", DataType::Struct(fields), false));
        let sections: ArrayRef = Arc::new(ListArray::new(
            item.clone(),
            list.offsets().clone(),
            Arc::new(structs),
            None,
        ));
        let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
        fields[at] = Arc::new(Field::new("sections", DataType::List(item), false));
        let mut metadata = schema.metadata().clone();
        metadata.insert(ARROW_VERSION_KEY.into(), version.into());
        let mut columns = batch.columns().to_vec();
        columns[at] = sections;
        RecordBatch::try_new(Arc::new(Schema::new(fields).with_metadata(metadata)), columns)
            .expect("batch without the vector column")
    }

    #[test]
    fn a_contract_1_3_batch_has_no_vector_column() {
        let rows = build(&[Row::default()]);
        let changes = document_changes(&without_section_vector(&rows, "1.3")).expect("1.3 batch");
        assert_eq!(changes[0].sections[0].vector, None);
        // From 1.4 the column is part of the contract.
        let error = document_changes(&without_section_vector(&rows, "1.4")).expect_err("1.4");
        assert!(error.contains("sections[].vector"), "{error}");
    }

    #[test]
    fn violations_are_refused_without_values() {
        for (row, expected) in [
            (
                Row {
                    readers: vec![format!("bad:{SENTINEL}")],
                    ..Row::default()
                },
                "usr:<id> or grp:<id>",
            ),
            (
                Row {
                    attributes: vec![SENTINEL.to_owned()],
                    ..Row::default()
                },
                "key=value",
            ),
            (
                Row {
                    sections: vec![],
                    ..Row::default()
                },
                "at least one section",
            ),
            (
                Row {
                    connector_map: Some(format!("not json {SENTINEL}")),
                    ..Row::default()
                },
                "connector_map is not a JSON object",
            ),
            (
                Row {
                    op: "upsertx",
                    ..Row::default()
                },
                "op must be upsert or delete",
            ),
            (
                Row {
                    title: Some(format!(" {SENTINEL}")),
                    ..Row::default()
                },
                "surrounding whitespace",
            ),
            (
                Row {
                    root_id: format!("{SENTINEL}\n"),
                    ..Row::default()
                },
                "surrounding whitespace",
            ),
        ] {
            let error = checker()
                .check(&header(None, "c1"), Some(&build(&[row])))
                .expect_err(expected);
            assert!(error.contains(expected), "{error}");
            assert!(!error.contains(SENTINEL), "{error}");
        }
        let error = checker()
            .check(
                &header(None, "c1"),
                Some(&build(&[Row::default(), Row::default()])),
            )
            .expect_err("duplicate");
        assert!(error.contains("appears twice"), "{error}");
    }

    #[test]
    fn cursors_chain_and_bounds_hold() {
        let mut checker = checker();
        let error = checker
            .check(
                &header(Some("elsewhere"), "c1"),
                Some(&build(&[Row::default()])),
            )
            .expect_err("broken chain");
        assert!(error.contains("cursor.start"), "{error}");
        let many: Vec<Row> = (0..11)
            .map(|index| Row {
                root_id: format!("d{index}"),
                ..Row::default()
            })
            .collect();
        let error = checker
            .check(&header(None, "c1"), Some(&build(&many)))
            .expect_err("too many rows");
        assert!(error.contains("envelope_max_rows"), "{error}");
        let error = checker
            .check(&header(None, "c1"), None)
            .expect_err("no rows");
        assert!(error.contains("needs Arrow rows"), "{error}");
    }

    #[test]
    fn producer_schemas_must_match_the_contract() {
        let rows = build(&[Row::default()]);
        let loose: Fields = rows
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect();
        let loose = RecordBatch::try_new(Arc::new(Schema::new(loose)), rows.columns().to_vec())
            .expect("batch");
        let error = document_changes(&loose).expect_err("pyarrow defaults");
        assert!(error.contains("fyrr.contract"), "{error}");
        assert_eq!(
            document_changes(&empty_document_changes()).expect("empty"),
            Vec::new()
        );
    }

    #[test]
    fn other_kinds_carry_no_rows() {
        let listing = json!({
            "schema_version": "1.0", "source_id": "src", "kind": "reconcile",
            "cursor": { "start": null, "end": "l1" },
            "body": { "type": "listing", "listing_id": "l", "part": 0, "last": true, "complete": true, "root_ids": ["a", "b"] }
        })
        .to_string();
        let mut checker = BatchChecker::new("src", SyncKind::Reconcile, limits(), None);
        checker.check(&listing, None).expect("listing");
        assert_eq!(checker.stats.listed_root_ids, 2);
        assert!(checker.stats.listing_complete);
        let mut checker = BatchChecker::new("src", SyncKind::Reconcile, limits(), None);
        let error = checker
            .check(&listing, Some(&empty_document_changes()))
            .expect_err("rows");
        assert!(error.contains("only content batches"), "{error}");
        let mut checker = BatchChecker::new("other", SyncKind::Reconcile, limits(), None);
        assert!(checker.check(&listing, None).is_err());
    }
}
