//! The connector contract: what a connector provider hands to the
//! Rust worker in the same process, and the cursor state the worker persists.
//!
//! One [`ConnectorBatch`] belongs to one source and one sync kind and covers one
//! cursor step. Its body depends on the kind:
//! - `content` -> [`BatchBody::DocumentChanges`]: complete upserts (with the
//!   source revision, source metadata, filterable attributes, readers and
//!   sections with their location) or deletes. On the in-process path the rows
//!   travel as Arrow in [`document_changes_arrow_schema`]; the JSON form is the
//!   same data for fixtures and tests.
//! - `permissions` -> [`BatchBody::PermissionChanges`]: per-document readers
//!   with their permission revision, and container grants.
//! - `members` -> [`BatchBody::PrincipalSnapshot`]: one part of a full snapshot
//!   of source users and groups with members.
//! - `reconcile` -> [`BatchBody::Listing`]: one part of a full-ID listing; only
//!   a final part marked complete can ever derive deletes.
//!
//! Cursor protocol: the worker gives the connector the last durable cursor
//! (`null` for a first or reset sync). Each batch's `cursor.start` must equal
//! the previous batch's `cursor.end`; the worker advances [`CursorState`] with a
//! conditional write only after the batch's envelope is durable.
//! Cursors are opaque to fyrr.

use std::collections::BTreeSet;
use std::sync::Arc;

/// The source ID of the documents of a namespace written through the HTTP API.
/// A configured source ID starts with a letter or digit, so none can be this.
pub const API_SOURCE_ID: &str = "_api";

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::limits::{
    MAX_ATTRIBUTE_BYTES, MAX_ATTRIBUTE_KEY_BYTES, MAX_ATTRIBUTES, MAX_CONNECTOR_MAP_BYTES,
    MAX_CURSOR_BYTES, MAX_ID_BYTES, MAX_LABELS, MAX_METADATA_VALUE_BYTES, MAX_READER_REF_BYTES,
    MAX_READERS, MAX_REVISION_BYTES, MAX_URI_BYTES, check_count, check_id, check_text,
};
use crate::version::{Contract, ContractError, SchemaVersion, check_compatible};

/// The connector contract version written by this build.
pub const CONNECTOR_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(1, 4);
/// The version of the cursor state, which only the writer reads and writes.
pub const CURSOR_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(2, 0);

/// Arrow schema metadata key naming the contract part.
pub const ARROW_CONTRACT_KEY: &str = "fyrr.contract";
/// Arrow schema metadata key holding the `major.minor` version.
pub const ARROW_VERSION_KEY: &str = "fyrr.schema_version";
/// Value of [`ARROW_CONTRACT_KEY`] for document-change rows.
pub const DOCUMENT_CHANGES_CONTRACT: &str = "connector.document_changes";

/// Which of a source's independent syncs produced a batch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SyncKind {
    Content,
    Permissions,
    Members,
    Reconcile,
}

impl SyncKind {
    pub fn is_authorization(self) -> bool {
        matches!(self, Self::Permissions | Self::Members)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::Permissions => "permissions",
            Self::Members => "members",
            Self::Reconcile => "reconcile",
        }
    }
}

/// One cursor step: the cursor the batch started from and the cursor after it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CursorStep {
    /// Cursor before this batch; `null` for a first or reset sync.
    pub start: Option<String>,
    /// Opaque cursor after this batch, at most 16 KiB.
    pub end: String,
}

/// A hand-off from a connector to the worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorBatch {
    pub schema_version: SchemaVersion,
    /// Configured source ID (`[[sources]] id`).
    pub source_id: String,
    pub kind: SyncKind,
    pub cursor: CursorStep,
    pub body: BatchBody,
}

/// The kind-specific batch content, tagged by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchBody {
    DocumentChanges {
        changes: Vec<DocumentChange>,
    },
    PermissionChanges {
        #[serde(default)]
        documents: Vec<DocumentPermission>,
        #[serde(default)]
        container_grants: Vec<ContainerGrant>,
    },
    PrincipalSnapshot(PrincipalSnapshotPart),
    Listing(ListingPart),
}

/// Upsert replaces all of a document's records; delete removes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOp {
    Upsert,
    Delete,
}

/// A complete document change. A document never spans batches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DocumentChange {
    pub op: ChangeOp,
    /// Document identity inside the source; with `source_id` it is the logical
    /// key of every record of the document.
    pub root_id: String,
    /// Source revision of this change; required for upserts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    /// Connector-defined total order of revisions, when the source has one.
    /// Without it the committer relies on `expected_prior_revision`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_order: Option<i64>,
    /// The revision this change replaces, for sources without revision order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_prior_revision: Option<String>,
    /// Revision of the permission state carried in `readers`, when the source
    /// versions permissions separately from content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_revision: Option<String>,
    /// Source-scoped reader references (`usr:<id>` or `grp:<id>`).
    #[serde(default)]
    pub readers: Vec<String>,
    /// Containers (folder, channel, project) whose grants also apply.
    #[serde(default)]
    pub containers: Vec<String>,
    /// Source metadata; required for upserts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<DocumentMetadata>,
    /// Filterable `key=value` attributes; untrusted, never grant access.
    #[serde(default)]
    pub attributes: Vec<String>,
    /// Text sections in document order; required (non-empty) for upserts.
    #[serde(default)]
    pub sections: Vec<Section>,
}

/// Source metadata. Unknown values stay null, never inferred.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DocumentMetadata {
    /// Canonical source URI (exact-match `fetch` key).
    pub source_uri: String,
    pub source_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default)]
    pub labels: Vec<String>,
    /// Source instant (RFC 3339), never the ingest time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
    /// Bounded connector-owned map (at most 16 KiB of JSON), read only by
    /// `fetch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector_map: Option<serde_json::Map<String, serde_json::Value>>,
}

/// A section of document text with its location (heading, sheet, slide, page).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Section {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    /// Narrower readers for a restricted part (e.g. a Jira comment under a
    /// security level); `null` means the document's readers. Sections with
    /// different readers never share a record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readers: Option<Vec<String>>,
    /// The section's own vector, stored as its record in place of an embedding
    /// of the text. It has the namespace's dimension, finite elements and a
    /// nonzero length, and the text is one chunk (at most 16 KiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
}

/// Where a section sits in its source document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Location {
    /// `heading`, `sheet`, `slide`, `page`, `message` or another
    /// connector-defined kind.
    pub kind: String,
    /// Human label: heading text, sheet name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Ordinal: slide or page number, first sheet row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
}

/// The readers of one document from a permission sync.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DocumentPermission {
    pub root_id: String,
    pub permission_revision: String,
    pub readers: Vec<String>,
    #[serde(default)]
    pub containers: Vec<String>,
}

/// Readers granted on a container, resolved like a source grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ContainerGrant {
    pub container_id: String,
    /// `folder`, `channel`, `project`, `connection` or another container kind.
    pub container_kind: String,
    pub permission_revision: String,
    pub readers: Vec<String>,
}

/// One part of a full principal snapshot of the source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PrincipalSnapshotPart {
    /// Same ID on every part of one snapshot.
    pub snapshot_id: String,
    /// Part number, starting at 0.
    pub part: u32,
    /// True on the final part; a snapshot without it is never applied.
    pub last: bool,
    #[serde(default)]
    pub users: Vec<SourceUser>,
    #[serde(default)]
    pub groups: Vec<SourceGroup>,
}

/// Source evidence for an address; personal addresses never auto-link.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EmailVerification {
    #[default]
    Unverified,
    /// The source verified an organization-managed address.
    VerifiedCorporate,
    /// Even verified personal addresses require an explicit account link.
    VerifiedPersonal,
}

impl EmailVerification {
    fn is_unverified(&self) -> bool {
        *self == Self::Unverified
    }
}

/// A source user, linked to a canonical user by the writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SourceUser {
    pub id: String,
    /// E-mail when the source exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "EmailVerification::is_unverified")]
    pub email_verification: EmailVerification,
    pub active: bool,
}

/// A source group with its direct members (`usr:<id>` or `grp:<id>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SourceGroup {
    pub id: String,
    pub members: Vec<String>,
}

/// One part of a full-ID listing (reconcile).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ListingPart {
    /// Same ID on every part of one listing.
    pub listing_id: String,
    /// Part number, starting at 0.
    pub part: u32,
    /// True on the final part.
    pub last: bool,
    /// True only on a final part when the listing saw every document in scope.
    /// An incomplete listing never deletes anything.
    pub complete: bool,
    pub root_ids: Vec<String>,
}

/// The durable per-source, per-kind cursor the worker keeps on S3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CursorState {
    pub schema_version: SchemaVersion,
    pub namespace: String,
    pub source_id: String,
    pub kind: SyncKind,
    /// Opaque connector cursor; `null` restarts with a full scan.
    pub cursor: Option<String>,
    /// Sequence number of the last durable envelope of this source and kind.
    pub sequence: u64,
    /// Incremented when a rejected envelope rewinds the cursor.
    pub rejection_generation: u32,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<crate::publication::Lease>,
    /// The newest content or permissions envelope this cursor confirmed. Only
    /// the envelopes that chain back from it, through their `previous`, are
    /// committable: the conditional write of this state is what makes such an
    /// envelope count, so a worker that lost its lease cannot publish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<LastEnvelope>,
    /// The newest rejection generation that a complete permissions read ended.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub completed_generation: u32,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// A permissions envelope a cursor confirmed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LastEnvelope {
    pub sequence: u64,
    pub id: String,
}

impl Contract for ConnectorBatch {
    const NAME: &'static str = "connector-batch";
    const VERSION: SchemaVersion = CONNECTOR_CONTRACT_VERSION;

    fn validate(&self) -> Result<(), String> {
        check_id("source_id", &self.source_id)?;
        if let Some(start) = &self.cursor.start {
            check_cursor("cursor.start", start)?;
        }
        check_cursor("cursor.end", &self.cursor.end)?;
        let expected = match &self.body {
            BatchBody::DocumentChanges { changes } => {
                let mut roots = BTreeSet::new();
                for (index, change) in changes.iter().enumerate() {
                    change
                        .validate()
                        .map_err(|error| format!("changes[{index}]: {error}"))?;
                    if !roots.insert(change.root_id.as_str()) {
                        return Err(format!(
                            "changes[{index}]: a document appears twice in one batch"
                        ));
                    }
                }
                SyncKind::Content
            }
            BatchBody::PermissionChanges {
                documents,
                container_grants,
            } => {
                for (index, document) in documents.iter().enumerate() {
                    let at = |error| format!("documents[{index}]: {error}");
                    check_id("root_id", &document.root_id).map_err(at)?;
                    check_revision("permission_revision", &document.permission_revision)
                        .map_err(at)?;
                    check_readers("readers", &document.readers).map_err(at)?;
                    check_ids("containers", &document.containers).map_err(at)?;
                }
                for (index, grant) in container_grants.iter().enumerate() {
                    let at = |error| format!("container_grants[{index}]: {error}");
                    check_id("container_id", &grant.container_id).map_err(at)?;
                    check_id("container_kind", &grant.container_kind).map_err(at)?;
                    check_revision("permission_revision", &grant.permission_revision)
                        .map_err(at)?;
                    check_readers("readers", &grant.readers).map_err(at)?;
                }
                SyncKind::Permissions
            }
            BatchBody::PrincipalSnapshot(snapshot) => {
                check_id("snapshot_id", &snapshot.snapshot_id)?;
                for (index, user) in snapshot.users.iter().enumerate() {
                    let at = |error| format!("users[{index}]: {error}");
                    check_id("id", &user.id).map_err(at)?;
                    if let Some(email) = &user.email {
                        check_text("email", email, MAX_METADATA_VALUE_BYTES).map_err(at)?;
                    }
                }
                for (index, group) in snapshot.groups.iter().enumerate() {
                    let at = |error| format!("groups[{index}]: {error}");
                    check_id("id", &group.id).map_err(at)?;
                    check_readers("members", &group.members).map_err(at)?;
                }
                SyncKind::Members
            }
            BatchBody::Listing(listing) => {
                check_id("listing_id", &listing.listing_id)?;
                if listing.complete && !listing.last {
                    return Err("listing: only the final part can be complete".into());
                }
                check_ids("root_ids", &listing.root_ids)?;
                SyncKind::Reconcile
            }
        };
        if self.kind != expected {
            return Err(format!(
                "body type does not match kind {}",
                self.kind.as_str()
            ));
        }
        Ok(())
    }
}

impl DocumentChange {
    /// Checks one change; messages name fields, never values.
    pub fn validate(&self) -> Result<(), String> {
        check_id("root_id", &self.root_id)?;
        for (field, value) in [
            ("source_revision", &self.source_revision),
            ("expected_prior_revision", &self.expected_prior_revision),
            ("permission_revision", &self.permission_revision),
        ] {
            if let Some(value) = value {
                check_revision(field, value)?;
            }
        }
        check_readers("readers", &self.readers)?;
        check_ids("containers", &self.containers)?;
        match self.op {
            ChangeOp::Delete => {
                if self.metadata.is_some()
                    || !self.sections.is_empty()
                    || !self.attributes.is_empty()
                {
                    return Err("a delete carries no metadata, attributes or sections".into());
                }
            }
            ChangeOp::Upsert => {
                if self.source_revision.is_none() {
                    return Err("an upsert needs source_revision".into());
                }
                let metadata = self.metadata.as_ref().ok_or("an upsert needs metadata")?;
                metadata.validate()?;
                if self.sections.is_empty() {
                    return Err("an upsert needs at least one section".into());
                }
                for (index, section) in self.sections.iter().enumerate() {
                    let at = |error| format!("sections[{index}]: {error}");
                    if let Some(readers) = &section.readers {
                        check_readers("readers", readers).map_err(at)?;
                    }
                    if let Some(location) = &section.location {
                        check_id("location.kind", &location.kind).map_err(at)?;
                        if let Some(label) = &location.label {
                            check_text("location.label", label, MAX_METADATA_VALUE_BYTES)
                                .map_err(at)?;
                        }
                    }
                }
                check_attributes(&self.attributes)?;
            }
        }
        Ok(())
    }
}

impl DocumentMetadata {
    fn validate(&self) -> Result<(), String> {
        check_text("metadata.source_uri", &self.source_uri, MAX_URI_BYTES)?;
        check_id("metadata.source_type", &self.source_type)?;
        for (field, value) in [
            ("metadata.title", &self.title),
            ("metadata.author", &self.author),
            ("metadata.team", &self.team),
            ("metadata.project", &self.project),
            ("metadata.status", &self.status),
        ] {
            if let Some(value) = value {
                check_text(field, value, MAX_METADATA_VALUE_BYTES)?;
            }
        }
        check_count("metadata.labels", self.labels.len(), MAX_LABELS)?;
        for label in &self.labels {
            check_text("metadata.labels[]", label, MAX_METADATA_VALUE_BYTES)?;
        }
        if let Some(map) = &self.connector_map {
            let bytes = serde_json::to_vec(map).map_or(usize::MAX, |bytes| bytes.len());
            if bytes > MAX_CONNECTOR_MAP_BYTES {
                return Err(format!(
                    "metadata.connector_map exceeds {MAX_CONNECTOR_MAP_BYTES} bytes"
                ));
            }
        }
        Ok(())
    }
}

impl Contract for CursorState {
    const NAME: &'static str = "connector-cursor";
    const VERSION: SchemaVersion = CURSOR_CONTRACT_VERSION;

    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        check_id("source_id", &self.source_id)?;
        if let Some(cursor) = &self.cursor {
            check_cursor("cursor", cursor)?;
        }
        Ok(())
    }
}

fn check_cursor(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_CURSOR_BYTES {
        return Err(format!("{field} must be 1..={MAX_CURSOR_BYTES} bytes"));
    }
    Ok(())
}

fn check_revision(field: &str, value: &str) -> Result<(), String> {
    check_text(field, value, MAX_REVISION_BYTES)
}

fn check_ids(field: &str, values: &[String]) -> Result<(), String> {
    check_count(field, values.len(), MAX_READERS)?;
    values
        .iter()
        .try_for_each(|value| check_text(field, value, MAX_ID_BYTES))
}

/// Reader and member references are `usr:<id>` or `grp:<id>`.
pub fn check_readers(field: &str, values: &[String]) -> Result<(), String> {
    check_count(field, values.len(), MAX_READERS)?;
    for value in values {
        check_text(field, value, MAX_READER_REF_BYTES)?;
        let id = value
            .strip_prefix("usr:")
            .or_else(|| value.strip_prefix("grp:"));
        if id.is_none_or(str::is_empty) {
            return Err(format!("{field} entries must be usr:<id> or grp:<id>"));
        }
    }
    Ok(())
}

/// Filterable attributes: `key=value`, key of lowercase letters, digits, `.`, `_`
/// or `-`, value non-empty.
fn check_attributes(values: &[String]) -> Result<(), String> {
    check_count("attributes", values.len(), MAX_ATTRIBUTES)?;
    for value in values {
        check_text("attributes", value, MAX_ATTRIBUTE_BYTES)?;
        let (key, rest) = value
            .split_once('=')
            .ok_or("attributes entries must be key=value")?;
        let key_ok = !key.is_empty()
            && key.len() <= MAX_ATTRIBUTE_KEY_BYTES
            && key.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
            });
        if !key_ok || rest.is_empty() {
            return Err("attributes entries must be key=value with a [a-z0-9._-] key".into());
        }
    }
    Ok(())
}

fn utf8_list(nullable: bool) -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, nullable)))
}

/// The Arrow schema of document-change rows (one row per [`DocumentChange`],
/// metadata flattened). Connectors build it with pyarrow; the worker checks it
/// with [`check_document_changes_schema`].
pub fn document_changes_arrow_schema() -> Schema {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let section = DataType::Struct(Fields::from(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("location_kind", DataType::Utf8, true),
        Field::new("location_label", DataType::Utf8, true),
        Field::new("location_index", DataType::UInt32, true),
        Field::new("readers", utf8_list(false), true),
        Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, false))),
            true,
        ),
    ]));
    let fields = vec![
        Field::new("op", DataType::Utf8, false),
        Field::new("root_id", DataType::Utf8, false),
        Field::new("source_revision", DataType::Utf8, true),
        Field::new("revision_order", DataType::Int64, true),
        Field::new("expected_prior_revision", DataType::Utf8, true),
        Field::new("permission_revision", DataType::Utf8, true),
        Field::new("readers", utf8_list(false), false),
        Field::new("containers", utf8_list(false), false),
        Field::new("source_uri", DataType::Utf8, true),
        Field::new("source_type", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
        Field::new("team", DataType::Utf8, true),
        Field::new("project", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("labels", utf8_list(false), false),
        Field::new("created_at", timestamp.clone(), true),
        Field::new("updated_at", timestamp, true),
        Field::new("connector_map", DataType::Utf8, true),
        Field::new("attributes", utf8_list(false), false),
        Field::new(
            "sections",
            DataType::List(Arc::new(Field::new("item", section, false))),
            false,
        ),
    ];
    Schema::new(fields).with_metadata(
        [
            (
                ARROW_CONTRACT_KEY.to_owned(),
                DOCUMENT_CHANGES_CONTRACT.to_owned(),
            ),
            (
                ARROW_VERSION_KEY.to_owned(),
                CONNECTOR_CONTRACT_VERSION.to_string(),
            ),
        ]
        .into(),
    )
}

/// Checks a producer's document-change schema before reading any row: the
/// contract name and a known major version, then every field of this build's
/// schema, recursively through structs and lists, with the same type and no
/// weaker nullability. A newer minor version may add fields at any nesting
/// level; added fields must be nullable and are ignored. Field order
/// and list item names do not matter.
///
/// What a producer (part B's pyarrow SDK) must emit: the field names and types
/// of [`document_changes_arrow_schema`] (`pa.string()`, not `large_string`;
/// `pa.timestamp("us", tz="UTC")`; `pa.uint32()`/`pa.int64()` as listed);
/// `nullable=False` exactly where this schema has it, including list items
/// (`pa.list_(pa.field("item", pa.string(), nullable=False))`) and the
/// `sections` struct children; and the schema metadata `fyrr.contract` and
/// `fyrr.schema_version`.
pub fn check_document_changes_schema(schema: &Schema) -> Result<SchemaVersion, ContractError> {
    const NAME: &str = "connector-document-changes";
    let invalid = |message: String| ContractError::Invalid {
        contract: NAME,
        message,
    };
    if schema
        .metadata()
        .get(ARROW_CONTRACT_KEY)
        .map(String::as_str)
        != Some(DOCUMENT_CHANGES_CONTRACT)
    {
        return Err(invalid(format!(
            "Arrow schema metadata {ARROW_CONTRACT_KEY} must be {DOCUMENT_CHANGES_CONTRACT}"
        )));
    }
    let version: SchemaVersion = schema
        .metadata()
        .get(ARROW_VERSION_KEY)
        .ok_or(ContractError::MissingVersion { contract: NAME })?
        .parse()
        .map_err(|_| ContractError::MalformedVersion { contract: NAME })?;
    check_compatible(NAME, CONNECTOR_CONTRACT_VERSION, version)?;
    check_fields(
        document_changes_arrow_schema().fields(),
        schema.fields(),
        "",
    )
    .map_err(invalid)?;
    Ok(version)
}

fn check_fields(expected: &Fields, found: &Fields, prefix: &str) -> Result<(), String> {
    let path = |name: &str| {
        if prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{prefix}.{name}")
        }
    };
    for wanted in expected {
        let Some((_, present)) = found.find(wanted.name()) else {
            return Err(format!("missing field {}", path(wanted.name())));
        };
        check_field(wanted, present, &path(wanted.name()))?;
    }
    for (ordinal, present) in found.iter().enumerate() {
        if expected.find(present.name()).is_none() && !present.is_nullable() {
            return Err(format!(
                "added field at {prefix}[{ordinal}] must be nullable (a non-nullable addition is a major change)"
            ));
        }
    }
    Ok(())
}

fn check_field(expected: &Field, found: &Field, path: &str) -> Result<(), String> {
    if found.is_nullable() && !expected.is_nullable() {
        return Err(format!("field {path} must not be nullable"));
    }
    match (expected.data_type(), found.data_type()) {
        (DataType::Struct(wanted), DataType::Struct(present)) => {
            check_fields(wanted, present, path)
        }
        (DataType::List(wanted), DataType::List(present)) => {
            check_field(wanted, present, &format!("{path}[]"))
        }
        (wanted, present) if wanted == present => Ok(()),
        (_, _) => Err(format!("field {path} has an incompatible type")),
    }
}
