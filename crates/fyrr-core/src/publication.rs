//! The content publication manifest and the serving head.
//!
//! The manifest binds one published content version: namespace, exact Lance
//! version (FTS included), the namespace settings it was built with, chunking,
//! index and file identities, and the envelope IDs it
//! published (the commit point of their receipts). The serving head points at
//! the content publication and, separately, at the policy and identity
//! generations, so those can move without a new content publication. Head CAS
//! is the only visibility point; readers compare publication IDs and
//! generations, never ETags.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::{PersistedSettings, RerankSettings};
use crate::envelope::{ChunkProfileIdentity, EmbeddingIdentity, FileRef};
use crate::limits::{check_digest, check_id};
use crate::version::{Contract, SchemaVersion};

/// The version of the serving head, the envelope checkpoint and the history cut.
pub const PUBLICATION_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(4, 1);
/// The version of the content manifest.
pub const MANIFEST_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(6, 0);
/// The version of the policy artifact.
pub const POLICY_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(5, 0);

/// The index and retrieval settings a namespace is created with and keeps for
/// its life. The process configuration only supplies them when the namespace is
/// created; every reader and writer afterwards takes them from the namespace's
/// policy artifact, which each content manifest pins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(transform = stored_settings_required)]
pub struct NamespaceSettings {
    pub embedding: EmbeddingIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rerank: Option<RerankSettings>,
    #[serde(flatten)]
    pub namespace: PersistedSettings,
}

/// The settings whose members a stored document names too: a file may leave
/// one out for its default, a stored document may not.
const COMPLETE_MEMBERS: [&str; 3] = ["retrieval", "composition", "analyzer"];

/// A stored document names every setting that has a default in a file, and
/// every member of the settings in [`COMPLETE_MEMBERS`].
fn stored_settings_required(schema: &mut schemars::Schema) {
    let defaults = PersistedSettings::defaults();
    let mut required = vec!["embedding".to_owned()];
    required.extend(defaults.keys().cloned());
    schema.insert("required".into(), required.into());
    for key in COMPLETE_MEMBERS {
        let members: Vec<String> = defaults[key]
            .as_object()
            .map(|members| members.keys().cloned().collect())
            .unwrap_or_default();
        if let Some(property) = schema
            .get_mut("properties")
            .and_then(|properties| properties.get_mut(key))
            .and_then(|property| property.as_object_mut())
        {
            property.insert("required".into(), members.into());
        }
    }
}

/// Whether `request` fits `schema` as far as keys go: an object names only the
/// properties of an object schema (and, for a map, fits its value schema); a
/// value that is not an object does not stand where a struct is declared.
fn names_only(
    request: &serde_json::Value,
    schema: &serde_json::Value,
    root: &serde_json::Value,
) -> bool {
    use serde_json::Value;
    let Value::Object(schema) = schema else {
        return true;
    };
    if let Some(Value::String(reference)) = schema.get("$ref") {
        return reference
            .strip_prefix("#/")
            .and_then(|path| root.pointer(&format!("/{path}")))
            .is_some_and(|target| names_only(request, target, root));
    }
    for union in ["anyOf", "oneOf"] {
        if let Some(Value::Array(branches)) = schema.get(union) {
            return branches
                .iter()
                .any(|branch| names_only(request, branch, root));
        }
    }
    // A branch of a union fits the value only if its type is the value's.
    let kind = match request {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    };
    let fits = |declared: &serde_json::Value| {
        declared == kind || (kind == "number" && declared == "integer")
    };
    if let Some(declared) = schema.get("type")
        && !declared
            .as_array()
            .map_or_else(|| fits(declared), |types| types.iter().any(fits))
    {
        return false;
    }
    let declared = schema.get("properties").and_then(Value::as_object);
    let values = schema.get("additionalProperties").filter(|v| v.is_object());
    match request {
        Value::Object(members) => match (declared, values) {
            (Some(declared), _) => members.iter().all(|(key, member)| {
                declared
                    .get(key)
                    .is_some_and(|schema| names_only(member, schema, root))
            }),
            (None, Some(values)) => members
                .values()
                .all(|member| names_only(member, values, root)),
            // An enum or a constant is no object.
            (None, None) => !schema.contains_key("enum") && !schema.contains_key("const"),
        },
        // A struct does not stand in for an array: serde would read one.
        _ => declared.is_none(),
    }
}

impl NamespaceSettings {
    /// Whether a request's `settings` names only settings, at every depth. A key
    /// this build does not know would otherwise be dropped without a word.
    /// Persisted types may not refuse it (a newer minor version adds fields),
    /// so the request is checked against the keys the schema declares.
    pub fn names_only_settings(request: &serde_json::Value) -> bool {
        let schema = serde_json::Value::from(schemars::schema_for!(NamespaceSettings));
        names_only(request, &schema, &schema)
    }

    /// Refuses a stored document that leaves a setting out, which this build
    /// would fill with its own default, and a `retrieval`, `composition` or
    /// `analyzer` that leaves out one of its members.
    fn check_complete(document: &serde_json::Value) -> Result<(), String> {
        let stored = document.get("settings").and_then(|s| s.as_object());
        let defaults = PersistedSettings::defaults();
        let missing =
            |stored: &serde_json::Map<String, serde_json::Value>,
             defaults: &serde_json::Map<String, serde_json::Value>| {
                defaults
                    .keys()
                    .find(|key| !stored.contains_key(*key))
                    .cloned()
            };
        let Some(stored) = stored else {
            return Err("settings are missing".into());
        };
        if let Some(key) = missing(stored, &defaults) {
            return Err(format!("settings.{key} is missing"));
        }
        for key in COMPLETE_MEMBERS {
            if let (Some(stored), Some(defaults)) = (
                stored.get(key).and_then(|v| v.as_object()),
                defaults.get(key).and_then(|v| v.as_object()),
            ) && let Some(member) = missing(stored, defaults)
            {
                return Err(format!("settings.{key}.{member} is missing"));
            }
        }
        Ok(())
    }

    /// The bounds a configuration file is held to, so that no stored settings
    /// can fail a build or a search later.
    pub fn validate(&self) -> Result<(), String> {
        self.embedding.validate_profile()?;
        let mut issues = Vec::new();
        let mut issue = |path: &str, message: String| issues.push(format!("{path}: {message}"));
        crate::config::check_namespace(&self.namespace, self.embedding.dimensions, &mut issue);
        if let Some(rerank) = &self.rerank {
            crate::config::check_rerank(rerank, &mut issue);
        }
        issues.into_iter().next().map_or(Ok(()), Err)
    }
}

/// Immutable policy generation used by the reader's admission path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PolicyArtifact {
    pub schema_version: SchemaVersion,
    pub namespace: String,
    pub generation: u64,
    /// Fixed for the life of the namespace but its attributes and their
    /// indexes, which only grow and change by a schema request.
    pub settings: NamespaceSettings,
    /// Source-wide fixed grants: the hashed reader tokens of every source with
    /// `permissions = "fixed"`. Rows of other sources carry their own readers.
    pub fixed_grants: BTreeMap<String, Vec<String>>,
    /// Old pins predating a row-ACL change cannot authorize with their old rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_acl_sequence: Option<u64>,
    /// Retired sources: readers deny them at once and the committer
    /// rejects their envelopes. An entry is never removed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub retired_sources: BTreeMap<String, RetiredSource>,
    /// The namespace is frozen: API writes and connector syncs are refused
    /// while it stays readable. An envelope that was prepared before the
    /// switch is still committed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
}

/// At most this many sources of a namespace can ever be retired.
pub const MAX_RETIRED_SOURCES: usize = 1024;

/// When a source was retired and, once its data was removed, purged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RetiredSource {
    pub retired_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purged_at: Option<DateTime<Utc>>,
}

impl Contract for PolicyArtifact {
    const NAME: &'static str = "policy-artifact";
    const VERSION: SchemaVersion = POLICY_CONTRACT_VERSION;
    fn check_document(document: &serde_json::Value) -> Result<(), String> {
        NamespaceSettings::check_complete(document)
    }
    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        if self.settings.namespace.write_api.is_none()
            && crate::attributes::CONNECTOR_COLUMNS
                .iter()
                .any(|(name, kind)| self.settings.namespace.attributes.get(*name) != Some(kind))
        {
            return Err("a connector namespace needs its typed connector columns".into());
        }
        if self.retired_sources.len() > MAX_RETIRED_SOURCES {
            return Err("retired_sources holds too many sources".into());
        }
        for (source, retired) in &self.retired_sources {
            check_id("retired_sources.source", source)?;
            if retired.purged_at.is_some_and(|at| at < retired.retired_at) {
                return Err("retired_sources: purged before retired".into());
            }
        }
        self.settings.validate()?;
        for (source, tokens) in &self.fixed_grants {
            check_id("fixed_grants.source", source)?;
            if tokens
                .iter()
                .any(|t| !crate::limits::prefixed_hex(t, "tok:", 64))
            {
                return Err("fixed_grants contains an unhashed token".into());
            }
        }
        if self.row_acl_sequence == Some(0) {
            return Err("row_acl_sequence must be positive".into());
        }
        Ok(())
    }
}

/// A content publication manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ContentManifest {
    pub schema_version: SchemaVersion,
    pub namespace: String,
    pub publication_id: String,
    /// Monotonic content publication sequence (listing watermarks use it).
    pub sequence: u64,
    pub previous_publication_id: Option<String>,
    /// Exact Lance version, including its FTS index.
    pub lance_version: u64,
    pub committed_at: DateTime<Utc>,
    /// Last completed maintenance, inherited by ordinary publications.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance_completed_at: Option<DateTime<Utc>>,
    /// The namespace's settings as this version was built with them. The reader
    /// refuses a version that differs from the policy's, apart from
    /// `attribute_indexes`, which here holds the indexes that are built.
    pub settings: NamespaceSettings,
    pub chunk_profile: ChunkProfileIdentity,
    pub indexes: Vec<IndexIdentity>,
    /// Of each vector index with more live segments than `writer serve` leaves
    /// alone (64), the smallest sum of the rows of two neighboring segments, in
    /// fragment order. `writer serve` compares it with the rows one merge step
    /// may rewrite, to know whether a merge has anything to do.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub merge_pair_rows: BTreeMap<String, u64>,
    /// Immutable files of this version, relative to the dataset root.
    pub files: Vec<FileRef>,
    /// Envelopes this publication committed.
    pub envelope_ids: Vec<String>,
    /// Exact Lance transactions made by the leased holder.
    pub lineage: CommitLineage,
    /// Pins immutable membership/container inputs through the identity artifact.
    pub identity: GenerationPointer,
    pub policy: GenerationPointer,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejected_envelope_ids: Vec<String>,
    /// The incarnation of the namespace that published this version, copied
    /// from the serving head.
    pub incarnation: String,
    /// Which envelopes this publication and every older one committed or
    /// rejected, so that the newest manifest is enough to tell.
    pub envelope_evidence: EnvelopeEvidence,
    /// What the version holds.
    pub stats: PublicationStats,
    /// The newest chained envelope of each source and kind (content and
    /// permissions) that this publication and every older one resolved. The
    /// committer walks the chain of the cursor back to it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub chain_applied: BTreeMap<String, BTreeMap<crate::connector::SyncKind, String>>,
}

/// The size of a published version and when its namespace was first published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PublicationStats {
    /// Live rows, deleted ones not counted.
    pub rows: u64,
    /// Bytes of the data files of those rows, indexes and deletion files not counted.
    pub logical_bytes: u64,
    /// When the namespace's first content publication was committed, carried
    /// forward by every later one.
    pub first_published_at: DateTime<Utc>,
}

/// The envelope outcomes of a publication chain up to one manifest: an
/// immutable checkpoint of the older ones, and the few since.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EnvelopeEvidence {
    /// The [`EnvelopeCheckpoint`] of an older publication.
    pub checkpoint: GenerationPointer,
    /// Envelopes committed after the checkpoint, this publication's included.
    pub applied_since: Vec<String>,
    /// Envelopes rejected after the checkpoint, this publication's included.
    pub rejected_since: Vec<String>,
}

/// Every envelope a publication chain committed or rejected, up to one
/// publication. The committer rewrites it now and then, so the manifests
/// never carry more than a few ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EnvelopeCheckpoint {
    pub schema_version: SchemaVersion,
    pub namespace: String,
    pub applied_envelope_ids: Vec<String>,
    pub rejected_envelope_ids: Vec<String>,
}

impl Contract for EnvelopeCheckpoint {
    const NAME: &'static str = "envelope-checkpoint";
    const VERSION: SchemaVersion = PUBLICATION_CONTRACT_VERSION;
    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        check_unique("applied_envelope_ids", &self.applied_envelope_ids)?;
        check_unique("rejected_envelope_ids", &self.rejected_envelope_ids)
    }
}

/// Where `writer maintain` cut the publication chain: the manifests before
/// `publication_id` are deleted, and a walk of the chain ends at that
/// manifest. A manifest missing anywhere else was lost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HistoryCut {
    pub schema_version: SchemaVersion,
    pub namespace: String,
    /// The oldest publication the namespace keeps.
    pub publication_id: String,
    /// Its sequence: the cut only moves to a higher one.
    pub sequence: u64,
}

/// The object that holds the [`HistoryCut`], in the namespace root.
pub const HISTORY_CUT_OBJECT: &str = "history-cut.json";

impl Contract for HistoryCut {
    const NAME: &'static str = "history-cut";
    const VERSION: SchemaVersion = PUBLICATION_CONTRACT_VERSION;
    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        check_id("publication_id", &self.publication_id)?;
        if self.sequence == 0 {
            return Err("sequence must be positive".into());
        }
        Ok(())
    }
}

fn check_unique(field: &str, ids: &[String]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    if ids.iter().all(|id| seen.insert(id)) {
        Ok(())
    } else {
        Err(format!("{field} has duplicates"))
    }
}

impl ContentManifest {
    /// Whether this publication was built with the settings its namespace
    /// records. The built indexes are the manifest's own record.
    pub fn built_with(&self, settings: &NamespaceSettings) -> bool {
        let mut built = settings.clone();
        built
            .namespace
            .attribute_indexes
            .clone_from(&self.settings.namespace.attribute_indexes);
        self.settings == built
    }
}

/// Publication-local fencing evidence, immutable with the content manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CommitLineage {
    pub holder: String,
    pub epoch: u64,
    pub base_version: u64,
    /// A recovery Restore supersedes unpublished history before this boundary.
    pub recovery_version: Option<u64>,
    pub transactions: Vec<LineageTransaction>,
}

/// Identity of one actual Lance transaction, never an inferred version range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LineageTransaction {
    pub version: u64,
    pub transaction_id: String,
}

/// One index of the published version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IndexIdentity {
    pub name: String,
    /// `ivf_rq`, `fts`, `btree`, `bitmap` or `label_list`.
    pub kind: String,
    pub column: String,
    pub uuid: String,
    /// The IVF partitions of the model of a vector index segment
    /// (`ivf_rq`); absent for every other kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<u32>,
}

/// The serving head: the single mutable object per namespace (CAS-updated).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ServingHead {
    pub schema_version: SchemaVersion,
    pub namespace: String,
    /// `null` until the first content publication.
    pub content: Option<ContentPointer>,
    pub policy: GenerationPointer,
    pub identity: GenerationPointer,
    /// The single committer's lease; renewals change the ETag, not what is served.
    pub lease: Option<Lease>,
    pub updated_at: DateTime<Utc>,
    /// Rejection references before the first content publication exists.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejected_envelope_ids: Vec<String>,
    /// A random identifier written once by `namespace create`. A namespace
    /// deleted and created again at the same storage URI restarts its
    /// sequence; the incarnation tells its publications apart from the old
    /// ones.
    pub incarnation: String,
}

/// Points at a content publication manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ContentPointer {
    pub publication_id: String,
    pub sequence: u64,
    pub lance_version: u64,
    /// Manifest object key, relative to the namespace root.
    pub manifest: String,
    pub committed_at: DateTime<Utc>,
}

/// Points at one immutable generation of a policy or identity artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GenerationPointer {
    pub generation: u64,
    /// Object key, relative to the namespace root.
    pub object: String,
    pub digest: String,
}

/// The namespace committer lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Lease {
    /// Opaque holder identity (host and process), never a user identity.
    pub holder: String,
    /// Incremented on every takeover; fences a former holder.
    pub epoch: u64,
    pub expires_at: DateTime<Utc>,
}

impl Contract for ContentManifest {
    const NAME: &'static str = "content-manifest";
    const VERSION: SchemaVersion = MANIFEST_CONTRACT_VERSION;

    fn check_document(document: &serde_json::Value) -> Result<(), String> {
        NamespaceSettings::check_complete(document)
    }

    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        check_id("publication_id", &self.publication_id)?;
        check_id("incarnation", &self.incarnation)?;
        check_digest(
            "embedding.fingerprint",
            &self.settings.embedding.fingerprint,
        )?;
        check_digest("chunk_profile.digest", &self.chunk_profile.digest)?;
        self.settings.validate()?;
        if !self.indexes.iter().any(|index| index.column == "readers") {
            // A publication lacking the readers index is refused.
            return Err("the readers index is missing".into());
        }
        let mut seen = BTreeSet::new();
        if !self.envelope_ids.iter().all(|id| seen.insert(id)) {
            return Err("envelope_ids has duplicates".into());
        }
        check_unique(
            "envelope_evidence.applied_since",
            &self.envelope_evidence.applied_since,
        )?;
        check_unique(
            "envelope_evidence.rejected_since",
            &self.envelope_evidence.rejected_since,
        )?;
        let lineage = &self.lineage;
        check_id("lineage.holder", &lineage.holder)?;
        if lineage.epoch == 0 {
            return Err("lineage epoch must be positive".into());
        }
        let mut previous = lineage.base_version;
        for (position, transaction) in lineage.transactions.iter().enumerate() {
            check_id("lineage.transaction_id", &transaction.transaction_id)?;
            let recovery = position == 0 && lineage.recovery_version == Some(transaction.version);
            if transaction.version <= previous
                || (!recovery && previous.checked_add(1) != Some(transaction.version))
            {
                return Err("lineage versions are not contiguous".into());
            }
            previous = transaction.version;
        }
        if previous != self.lance_version
            || lineage
                .recovery_version
                .is_some_and(|v| lineage.transactions.first().is_none_or(|t| t.version != v))
        {
            return Err("lineage does not end at the publication".into());
        }
        Ok(())
    }
}

impl Contract for ServingHead {
    const NAME: &'static str = "serving-head";
    const VERSION: SchemaVersion = PUBLICATION_CONTRACT_VERSION;

    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        check_digest("policy.digest", &self.policy.digest)?;
        check_digest("identity.digest", &self.identity.digest)?;
        check_id("incarnation", &self.incarnation)?;
        Ok(())
    }
}
