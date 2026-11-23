//! The content publication manifest and the serving head.
//!
//! The manifest binds one published content version: namespace, exact Lance
//! version (FTS included), embedding, chunking and FTS settings, the
//! retrieval-policy digest, index and file identities, and the envelope IDs it
//! published (the commit point of their receipts). The serving head points at
//! the content publication and, separately, at the policy and identity
//! generations, so those can move without a new content publication. Head CAS
//! is the only visibility point; readers compare publication IDs and
//! generations, never ETags.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::attributes::{
    AttributeIndexes, AttributeSchema, check_indexes, check_schema, has_connector_columns,
};
use crate::config::{
    CompositionSettings, DenseElement, DenseMetric, NamespaceConfig, RerankSettings,
    RetrievalSettings,
};
use crate::envelope::{ChunkProfileIdentity, EmbeddingIdentity, FileRef};
use crate::limits::{check_digest, check_id};
use crate::version::{Contract, SchemaVersion};

/// The publication contract version (manifest and head) written by this build.
pub const PUBLICATION_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(4, 1);
/// The version of a manifest that records something an older build would
/// ignore, and of a policy artifact that does: attribute index options, the
/// typed columns of the metadata fields of connector documents (a build without
/// them would read nulls), a vector metric or element type other than cosine
/// and 16-bit floats (a build without them would search and write the wrong
/// ones), or, for a policy, the `read_only` switch (a build without it would
/// write). An older build refuses them: it reads only its own
/// major, and this build reads both. A document with none of them keeps its
/// version.
pub const INDEXED_PUBLICATION_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(5, 0);
/// The policy artifact's counterpart of [`INDEXED_PUBLICATION_CONTRACT_VERSION`].
pub const INDEXED_POLICY_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(4, 0);

/// The index and retrieval settings a namespace is created with and keeps for
/// its life. The process configuration only supplies them when the namespace is
/// created; every reader and writer afterwards takes them from the namespace's
/// policy artifact, which each content manifest pins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NamespaceSettings {
    pub embedding: EmbeddingIdentity,
    pub fts: FtsSettings,
    /// RaBitQ bits per dimension of the vector index.
    pub dense_rq_bits: u8,
    /// Target rows per partition of the vector index.
    pub dense_partition_rows: u32,
    /// The distance of the vector index and the rescoring; absent: cosine.
    #[serde(default, skip_serializing_if = "DenseMetric::is_default")]
    pub dense_metric: DenseMetric,
    /// The element type of the stored vectors; absent: 16-bit floats.
    #[serde(default, skip_serializing_if = "DenseElement::is_default")]
    pub dense_element: DenseElement,
    pub retrieval: RetrievalSettings,
    pub composition: CompositionSettings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rerank: Option<RerankSettings>,
    /// The typed attributes the namespace declares. Each is a nullable column;
    /// a request that changes the schema adds attributes and never changes or
    /// removes one.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: AttributeSchema,
    /// The indexes the attributes are to have where they are not the default.
    /// An index switched on is built by the next maintenance; the manifest
    /// records the ones that are built.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attribute_indexes: AttributeIndexes,
    /// Present: the namespace is written through the HTTP API and takes no
    /// connector source. Fixed with the rest of the settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_api: Option<crate::config::WriteApiSettings>,
}

impl NamespaceSettings {
    /// Whether an older build would misread a namespace with these settings:
    /// it ignores index options, reads the connector columns as null and
    /// searches and writes cosine and 16-bit floats.
    pub fn needs_newer_build(&self) -> bool {
        !self.attribute_indexes.is_empty()
            || (self.write_api.is_none() && has_connector_columns(&self.attributes))
            || !self.dense_metric.is_default()
            || !self.dense_element.is_default()
    }

    /// The bounds a configuration file is held to, so that no stored settings
    /// can fail a build or a search later.
    pub fn validate(&self) -> Result<(), String> {
        self.embedding.validate_profile()?;
        let namespace = NamespaceConfig {
            rerank: None,
            retrieval: (&self.retrieval).into(),
            composition: (&self.composition).into(),
            name: String::new(),
            fts_max_token_length: self.fts.max_token_length,
            fts_stem: self.fts.stem,
            fts_remove_stop_words: self.fts.remove_stop_words,
            fts_ascii_folding: self.fts.ascii_folding,
            dense_rq_bits: self.dense_rq_bits,
            dense_partition_rows: self.dense_partition_rows,
            dense_metric: self.dense_metric,
            dense_element: self.dense_element,
            attributes: self.attributes.clone(),
            attribute_indexes: self.attribute_indexes.clone(),
            write_api: self.write_api.clone(),
        };
        let mut issues = Vec::new();
        let mut issue = |path: &str, message: String| issues.push(format!("{path}: {message}"));
        crate::config::check_namespace(&namespace, &mut issue);
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
    /// Retired sources: readers deny them at once, the committer
    /// rejects their envelopes, and a restore cannot bring them back. An
    /// entry is never removed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub retired_sources: BTreeMap<String, RetiredSource>,
    /// The namespace is frozen: API writes and connector syncs are refused
    /// while it stays readable. An envelope that was prepared before the
    /// switch is still committed. A policy that sets it is written as version
    /// 4.0, which a build that does not know the switch refuses instead of
    /// writing.
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

impl PolicyArtifact {
    /// Whether an older build would misread this policy.
    fn needs_newer_build(&self) -> bool {
        self.settings.needs_newer_build() || self.read_only
    }

    /// The version to write: the one an older build refuses when the policy
    /// holds index options, connector columns or the read-only switch, which it
    /// would ignore.
    pub fn current_version(&self) -> SchemaVersion {
        if !self.needs_newer_build() {
            <Self as Contract>::VERSION
        } else {
            INDEXED_POLICY_CONTRACT_VERSION
        }
    }
}

impl Contract for PolicyArtifact {
    const NAME: &'static str = "policy-artifact";
    const VERSION: SchemaVersion = SchemaVersion::new(3, 1);
    const ALSO_READS: &'static [u16] = &[INDEXED_POLICY_CONTRACT_VERSION.major];
    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        if self.needs_newer_build()
            && self.schema_version.major != INDEXED_POLICY_CONTRACT_VERSION.major
        {
            return Err(
                "attribute index options, connector columns, a vector metric or element type and read_only need the indexed policy version"
                    .into(),
            );
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
    pub embedding: EmbeddingIdentity,
    pub chunk_profile: ChunkProfileIdentity,
    pub fts: FtsSettings,
    /// RaBitQ bits per dimension of the vector index.
    pub dense_rq_bits: u8,
    /// Target rows per partition of the vector index.
    pub dense_partition_rows: u32,
    /// The distance of the vector index and the rescoring; absent: cosine.
    #[serde(default, skip_serializing_if = "DenseMetric::is_default")]
    pub dense_metric: DenseMetric,
    /// The element type of the stored vectors; absent: 16-bit floats.
    #[serde(default, skip_serializing_if = "DenseElement::is_default")]
    pub dense_element: DenseElement,
    /// Digest of the retrieval policy, including the result composition.
    pub retrieval_policy_digest: String,
    pub indexes: Vec<IndexIdentity>,
    /// The typed attributes of this version: each is a nullable column.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: AttributeSchema,
    /// The attributes whose built indexes are not the default: each attribute
    /// has the scalar index of its type unless it says `index: false`, and the
    /// text indexes it asks for, among `indexes`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attribute_indexes: AttributeIndexes,
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
    /// Immutable restore rewind marker paths; visible only through this publication.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cursor_rewinds: Vec<String>,
    /// The incarnation of the namespace that published this version, copied
    /// from the serving head.
    pub incarnation: String,
    /// Which envelopes this publication and every older one committed or
    /// rejected, so that the newest manifest is enough to tell. Absent from
    /// manifests written before it existed: a reader then walks the chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope_evidence: Option<EnvelopeEvidence>,
    /// What the version holds. Absent from manifests written before 4.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<PublicationStats>,
}

/// The size of a published version and when its namespace was first published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PublicationStats {
    /// Live rows, deleted ones not counted.
    pub rows: u64,
    /// Bytes of the data files of those rows, indexes and deletion files not counted.
    pub logical_bytes: u64,
    /// When the namespace's first content publication was committed, carried
    /// forward by every later one. Absent when the namespace was published
    /// before 4.1: that time is not known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_published_at: Option<DateTime<Utc>>,
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
    /// records: the embedding profile, the vector index and the attributes.
    pub fn built_with(&self, settings: &NamespaceSettings) -> bool {
        self.embedding == settings.embedding
            && self.dense_rq_bits == settings.dense_rq_bits
            && self.dense_partition_rows == settings.dense_partition_rows
            && self.dense_metric == settings.dense_metric
            && self.dense_element == settings.dense_element
            && self.attributes == settings.attributes
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

/// FTS analyzer settings, part of the publication identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FtsSettings {
    pub tokenizer: String,
    pub max_token_length: u32,
    pub lowercase: bool,
    pub stem: bool,
    pub remove_stop_words: bool,
    pub ascii_folding: bool,
    /// Positional postings; false while there is no phrase scoring.
    pub with_position: bool,
}

/// One index of the published version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IndexIdentity {
    pub name: String,
    /// `ivf_rq`, `fts`, `btree`, `bitmap` or `label_list`.
    pub kind: String,
    pub column: String,
    pub uuid: String,
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
    const VERSION: SchemaVersion = PUBLICATION_CONTRACT_VERSION;
    const ALSO_READS: &'static [u16] = &[INDEXED_PUBLICATION_CONTRACT_VERSION.major];

    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        check_id("publication_id", &self.publication_id)?;
        check_id("incarnation", &self.incarnation)?;
        check_digest("embedding.fingerprint", &self.embedding.fingerprint)?;
        self.embedding.validate_profile()?;
        check_digest("chunk_profile.digest", &self.chunk_profile.digest)?;
        check_digest("retrieval_policy_digest", &self.retrieval_policy_digest)?;
        check_schema(&self.attributes).map_err(|error| format!("attributes: {error}"))?;
        check_indexes(&self.attributes, &self.attribute_indexes)
            .map_err(|error| format!("attribute_indexes: {error}"))?;
        if (!self.attribute_indexes.is_empty()
            || !self.dense_metric.is_default()
            || !self.dense_element.is_default())
            && self.schema_version.major != INDEXED_PUBLICATION_CONTRACT_VERSION.major
        {
            return Err(
                "attribute index options and a vector metric or element type need the indexed manifest version"
                    .into(),
            );
        }
        if !self.indexes.iter().any(|index| index.column == "readers") {
            // A publication lacking the readers index is refused.
            return Err("the readers index is missing".into());
        }
        let mut seen = BTreeSet::new();
        if !self.envelope_ids.iter().all(|id| seen.insert(id)) {
            return Err("envelope_ids has duplicates".into());
        }
        if let Some(evidence) = &self.envelope_evidence {
            check_unique("envelope_evidence.applied_since", &evidence.applied_since)?;
            check_unique("envelope_evidence.rejected_since", &evidence.rejected_since)?;
        }
        let mut rewind_paths = BTreeSet::new();
        for path in &self.cursor_rewinds {
            let hash = path
                .strip_prefix("rejections/restore-")
                .and_then(|p| p.strip_suffix(".json"))
                .ok_or("cursor rewind path invalid")?;
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                || !rewind_paths.insert(path)
            {
                return Err("cursor rewind path invalid".into());
            }
        }
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
