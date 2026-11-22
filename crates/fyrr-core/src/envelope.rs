//! The prepared envelope: durable, recoverable input written by
//! a sync worker and published only by the namespace's committer. Envelopes
//! live outside the Lance dataset directory. Lance fragment metadata is carried
//! opaquely: Lance's private fragment format is never part of this contract.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::connector::{ContainerGrant, CursorStep, DocumentPermission, SyncKind};
use crate::limits::{MAX_REVISION_BYTES, check_digest, check_id, check_text};
use crate::version::{Contract, SchemaVersion};

/// The envelope contract version written by this build.
pub const ENVELOPE_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(5, 0);
/// The version of an envelope with a document condition. A committer that
/// does not know conditions would apply the change without it, so it must
/// refuse the envelope: it reads only major 5, and this build reads both.
pub const CONDITIONAL_ENVELOPE_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(6, 0);

/// A prepared envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PreparedEnvelope {
    pub schema_version: SchemaVersion,
    /// Deterministic ID from [`envelope_id`]; a repeated sync is a no-op.
    pub envelope_id: String,
    pub namespace: String,
    pub source_id: String,
    pub kind: SyncKind,
    /// Per-source, per-kind sequence number.
    pub sequence: u64,
    /// Rejection generation of this source and kind; part of the ID so
    /// re-fetched changes after a rejection form a new envelope.
    pub rejection_generation: u32,
    /// Latest manifest-authorized restore reset consumed by this log/cache entry.
    pub restore_sequence: u64,
    /// Connector cursors before and after the envelope's changes.
    pub cursor: CursorStep,
    /// False when the envelope leaves the cursor where it was, absent or opaque.
    pub advances_cursor: bool,
    /// The serving head the worker prepared against; `null` on a namespace
    /// without a publication.
    pub base_head: Option<BaseHead>,
    pub created_at: DateTime<Utc>,
    pub payload: EnvelopePayload,
}

/// The head an envelope was prepared against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BaseHead {
    pub content_publication_id: String,
    pub content_sequence: u64,
    pub lance_version: u64,
}

/// Kind-specific envelope content, tagged by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EnvelopePayload {
    Content(ContentPayload),
    Permissions(PermissionsPayload),
    Members(MembersPayload),
    Reconcile(ReconcilePayload),
}

/// Prepared content: per-document intent and the unreferenced Lance fragments
/// that hold the new records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ContentPayload {
    /// Embedding model identity used for the fragments.
    pub embedding: EmbeddingIdentity,
    /// Chunk profile identity used for the fragments.
    pub chunk_profile: ChunkProfileIdentity,
    /// Version of the namespace table schema the fragments were written in.
    pub table_schema_version: SchemaVersion,
    pub documents: Vec<EnvelopeDocument>,
    pub fragments: Vec<PreparedFragment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quarantined: Vec<QuarantineEntry>,
}

/// Replacement or delete intent for one document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EnvelopeDocument {
    pub root_id: String,
    pub intent: DocumentIntent,
    pub source_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_order: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_prior_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_revision: Option<String>,
    /// Records written for this document (0 for a delete or a patch).
    pub records: u32,
    /// What a `Patch` writes; present for that intent only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patch: Option<Box<DocumentPatch>>,
    /// A condition the stored document must meet when the committer applies
    /// this intent, else it is skipped and writes nothing. A document that is
    /// not there skips a `Patch` or a `Delete` and meets it for a `Replace`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<crate::mcp::Filter>,
}

/// The values a patch writes into the stored records of a document. Every
/// column it does not name stays as stored, the vector unless the patch brings
/// one with the new text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DocumentPatch {
    /// Hashed readers that replace the document's; `null` keeps them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readers: Option<Vec<String>>,
    /// The new text of the record; `null` keeps it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// The vector of the new text, which replaces the stored one; present
    /// with `text` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    /// Attribute values by name; a `null` clears the attribute.
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

/// What the committer does with a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DocumentIntent {
    /// Atomically replace every record of the document.
    Replace,
    /// Remove every record of the document.
    Delete,
    /// Write the values of `patch` into the records of an existing document;
    /// a document that does not exist is skipped, never created.
    Patch,
}

/// A Lance fragment written by the worker but not yet committed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PreparedFragment {
    pub rows: u64,
    /// Data files, relative to the namespace dataset root.
    pub files: Vec<FileRef>,
    /// Lance's own fragment description, opaque to fyrr.
    pub lance_fragment: serde_json::Value,
    /// SHA-256 of the ordered file bytes.
    pub digest: String,
}

/// A file with its size, for accounting and the mirror.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FileRef {
    pub path: String,
    pub size_bytes: u64,
}

/// The embedding identity bound into envelopes and publications.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EmbeddingIdentity {
    pub model: String,
    pub dimensions: u32,
    /// Digest of the model settings and text transformation (prefix,
    /// normalization) that keys the embedding cache.
    pub fingerprint: String,
    pub settings: Box<EmbeddingSettings>,
}

/// Vector-affecting settings recorded with a publication's model/dimensions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EmbeddingSettings {
    pub api_style: EmbeddingApiStyle,
    #[schemars(length(max = 4096), extend("x-maxUtf8Bytes" = 4096))]
    pub prefix: String,
    pub roles: EmbeddingRoles,
    pub send_dimensions: bool,
}

/// Fixed protocol identity recorded in persisted embedding settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum EmbeddingApiStyle {
    OpenaiCompatible,
}

/// Persisted role transformations accept future minor-version additions.
/// Configuration uses strict types at its TOML boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EmbeddingRoles {
    pub document: EmbeddingRole,
    pub query: EmbeddingRole,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EmbeddingRole {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 4096), extend("x-maxUtf8Bytes" = 4096))]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 128), extend("x-maxUtf8Bytes" = 128))]
    pub input_type: Option<String>,
}

impl EmbeddingSettings {
    /// A tuple avoids JSON object-order differences between feature graphs.
    pub fn fingerprint(&self, model: &str, dimensions: u32) -> String {
        crate::limits::digest(
            serde_json::json!([
                "embedding-profile-v1",
                model,
                dimensions,
                self.api_style,
                self.prefix,
                self.roles.document.prefix,
                self.roles.document.input_type,
                self.roles.query.prefix,
                self.roles.query.input_type,
                self.send_dimensions,
                "exact-utf8"
            ])
            .to_string()
            .as_bytes(),
        )
    }
}

impl EmbeddingIdentity {
    pub fn validate_profile(&self) -> Result<(), String> {
        check_digest("embedding.fingerprint", &self.fingerprint)?;
        if self.model.is_empty()
            || self.model.len() > 512
            || self.dimensions == 0
            || u64::from(self.dimensions) > crate::config::DIMENSIONS_MAX
        {
            return Err("embedding profile invalid".into());
        }
        let settings = &self.settings;
        if settings.prefix.len() > 4096
            || [&settings.roles.document, &settings.roles.query]
                .iter()
                .any(|role| {
                    role.prefix
                        .as_ref()
                        .is_some_and(|prefix| prefix.len() > 4096)
                        || role.input_type.as_ref().is_some_and(|kind| {
                            kind.is_empty()
                                || kind.len() > 128
                                || kind.chars().any(char::is_control)
                        })
                })
        {
            return Err("embedding role invalid".into());
        }
        let expected = settings.fingerprint(&self.model, self.dimensions);
        if self.fingerprint != expected {
            return Err("embedding profile fingerprint invalid".into());
        }
        Ok(())
    }
}

/// The chunk profile identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChunkProfileIdentity {
    pub name: String,
    pub max_unit_bytes: u32,
    pub overlap_bytes: u32,
    pub digest: String,
}

/// A permission sync's per-document readers and container grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PermissionsPayload {
    /// True only on the empty envelope that ends a read replacing a rejected
    /// generation; it alone consumes the rejection markers.
    pub complete_read: bool,
    pub documents: Vec<DocumentPermission>,
    pub container_grants: Vec<ContainerGrant>,
}

/// A membership sync's immutable principal snapshot on S3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MembersPayload {
    /// True only on the empty envelope that ends a read replacing a rejected
    /// generation; it alone consumes the rejection markers.
    pub complete_read: bool,
    pub snapshot_id: String,
    /// Object key of the snapshot, relative to the namespace root.
    pub snapshot_object: String,
    pub snapshot_digest: String,
    pub part: u32,
    pub last: bool,
    pub principals: u64,
}

/// One part of a full-ID listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReconcilePayload {
    pub listing_id: String,
    pub part: u32,
    pub last: bool,
    pub complete: bool,
    /// The head's content publication sequence when the listing started;
    /// derived deletes cover only documents published at or before it.
    pub watermark: u64,
    pub root_ids: Vec<String>,
}

impl Contract for PreparedEnvelope {
    const NAME: &'static str = "prepared-envelope";
    const VERSION: SchemaVersion = ENVELOPE_CONTRACT_VERSION;
    const ALSO_READS: &'static [u16] = &[CONDITIONAL_ENVELOPE_CONTRACT_VERSION.major];

    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        check_id("source_id", &self.source_id)?;
        let expected = envelope_id(
            &self.namespace,
            &self.source_id,
            self.kind,
            self.sequence,
            self.rejection_generation,
            self.cursor.start.as_deref(),
        );
        if self.envelope_id != expected {
            return Err("envelope_id does not match its derivation".into());
        }
        let kind = match &self.payload {
            EnvelopePayload::Content(content) => {
                content.embedding.validate_profile()?;
                check_digest(
                    "payload.embedding.fingerprint",
                    &content.embedding.fingerprint,
                )?;
                check_digest(
                    "payload.chunk_profile.digest",
                    &content.chunk_profile.digest,
                )?;
                for fragment in &content.fragments {
                    check_digest("fragments[].digest", &fragment.digest)?;
                }
                for entry in &content.quarantined {
                    check_digest("quarantined[].root_digest", &entry.root_digest)?;
                    check_digest("quarantined[].revision_digest", &entry.revision_digest)?;
                    if entry.reason != "document_invalid" && entry.reason != "document_oversized" {
                        return Err("quarantine reason invalid".into());
                    }
                }
                for document in &content.documents {
                    check_id("documents[].root_id", &document.root_id)?;
                    match (document.intent, &document.source_revision) {
                        (DocumentIntent::Replace | DocumentIntent::Patch, None) => {
                            return Err("a replace or a patch needs source_revision".into());
                        }
                        (_, Some(revision)) => {
                            check_text(
                                "documents[].source_revision",
                                revision,
                                MAX_REVISION_BYTES,
                            )?;
                        }
                        (DocumentIntent::Delete, None) => {}
                    }
                    if document.condition.is_some()
                        && self.schema_version.major != CONDITIONAL_ENVELOPE_CONTRACT_VERSION.major
                    {
                        return Err("a condition needs the conditional envelope version".into());
                    }
                    if document.intent != DocumentIntent::Replace && document.records != 0 {
                        return Err("only a replace writes records".into());
                    }
                    if (document.intent == DocumentIntent::Patch) != document.patch.is_some() {
                        return Err("a patch, and only a patch, carries patch values".into());
                    }
                    if document
                        .patch
                        .as_ref()
                        .is_some_and(|patch| patch.vector.is_some() != patch.text.is_some())
                    {
                        return Err("a patch carries a vector with its text, and only so".into());
                    }
                }
                SyncKind::Content
            }
            EnvelopePayload::Permissions(p) => {
                for token in p
                    .documents
                    .iter()
                    .flat_map(|d| &d.readers)
                    .chain(p.container_grants.iter().flat_map(|g| &g.readers))
                {
                    if !crate::identity::is_hashed(token, crate::identity::TOKEN_PREFIX) {
                        return Err("permission reader token must be hashed".into());
                    }
                }
                SyncKind::Permissions
            }
            EnvelopePayload::Members(members) => {
                check_digest("payload.snapshot_digest", &members.snapshot_digest)?;
                SyncKind::Members
            }
            EnvelopePayload::Reconcile(listing) => {
                if listing.complete && !listing.last {
                    return Err("only the final listing part can be complete".into());
                }
                SyncKind::Reconcile
            }
        };
        if kind != self.kind {
            return Err("payload type does not match kind".into());
        }
        Ok(())
    }
}

/// Derives an envelope ID from the fields that identify one sync step:
/// namespace, source, kind, sequence, rejection generation and start cursor.
/// Every field is length-prefixed so different splits never collide.
pub fn envelope_id(
    namespace: &str,
    source_id: &str,
    kind: SyncKind,
    sequence: u64,
    rejection_generation: u32,
    start_cursor: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    let mut part = |bytes: &[u8]| {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    };
    part(b"fyrr.envelope.v1");
    part(namespace.as_bytes());
    part(source_id.as_bytes());
    part(kind.as_str().as_bytes());
    part(&sequence.to_le_bytes());
    part(&rejection_generation.to_le_bytes());
    match start_cursor {
        None => part(b"-"),
        Some(cursor) => {
            part(b"+");
            part(cursor.as_bytes());
        }
    }
    let digest = hasher.finalize();
    format!("env-{}", hex::encode(&digest[..16]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_ids_are_deterministic_and_distinct() {
        let id = envelope_id("acme", "docs", SyncKind::Content, 7, 0, Some("c6"));
        assert_eq!(
            id,
            envelope_id("acme", "docs", SyncKind::Content, 7, 0, Some("c6"))
        );
        assert_eq!(id.len(), 4 + 32);
        let variants = [
            envelope_id("acme", "docs", SyncKind::Content, 7, 1, Some("c6")),
            envelope_id("acme", "docs", SyncKind::Content, 8, 0, Some("c6")),
            envelope_id("acme", "docs", SyncKind::Permissions, 7, 0, Some("c6")),
            envelope_id("acme", "docs", SyncKind::Content, 7, 0, None),
            envelope_id("acme", "docs", SyncKind::Content, 7, 0, Some("")),
            envelope_id("acmed", "ocs", SyncKind::Content, 7, 0, Some("c6")),
        ];
        for other in variants {
            assert_ne!(id, other);
        }
    }
}

/// Immutable envelope-log rejection; a later generation consumes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RejectionMarker {
    pub schema_version: SchemaVersion,
    pub envelope_id: String,
    pub source_id: String,
    pub kind: SyncKind,
    pub sequence: u64,
    pub rejection_generation: u32,
    /// Latest manifest-authorized restore reset consumed by this log/cache entry.
    pub restore_sequence: u64,
    pub start_cursor: Option<String>,
    pub reason: String,
}
impl Contract for RejectionMarker {
    const NAME: &'static str = "envelope-rejection";
    const VERSION: SchemaVersion = ENVELOPE_CONTRACT_VERSION;
    fn validate(&self) -> Result<(), String> {
        check_id("envelope_id", &self.envelope_id)?;
        check_id("source_id", &self.source_id)?;
        check_id("reason", &self.reason)
    }
}
/// Value-free diagnostic carried by an immutable envelope, never mutable state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QuarantineEntry {
    pub root_digest: String,
    pub revision_digest: String,
    pub reason: String,
}
