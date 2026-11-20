//! The deployment configuration: one TOML file read by
//! both images, parsed into these types with unknown fields rejected, checked
//! across fields, and published as a JSON Schema generated from the types.
//!
//! Every constraint the published schema states (patterns, ranges) is also
//! enforced by [`LoadedConfig::validate`], from the same constants; a test
//! walks the generated schema and proves each constraint fails `config check`.
//!
//! Secrets are never in the file: a secret field holds `{ env = "VAR" }` and is
//! resolved from the environment only where it is used. No error, `Debug`
//! output or `config show` line contains a secret value, and errors never
//! quote values from the file.

mod storage;
mod validation;

pub use storage::StorageBackend;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use schemars::JsonSchema;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::connector::check_readers;
use crate::units::{ByteSize, ConfigDuration};

/// Configuration schema revision: adding a storage backend changes the URI
/// contract beyond an optional field, so it starts a new major revision.
pub const CONFIG_SCHEMA_VERSION: crate::SchemaVersion = crate::SchemaVersion::new(5, 1);

/// Namespace IDs: lowercase letters, digits and `-`, at most 63 bytes.
pub const NAMESPACE_PATTERN: &str = r"^[a-z0-9][a-z0-9-]{0,62}$";
/// Storage is S3 (`s3://bucket[/prefix]`) or a local directory
/// (`file:///absolute/path`).
pub const STORAGE_URI_PATTERN: &str =
    r"^(s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](/.*)?|file:///[^?#]+)$";
/// Source IDs: lowercase letters, digits, `_` and `-`, at most 63 bytes.
pub const SOURCE_ID_PATTERN: &str = r"^[a-z0-9][a-z0-9_-]{0,62}$";
/// Connector provider names.
pub const PROVIDER_PATTERN: &str = r"^[a-z0-9][a-z0-9-]{0,62}$";
pub const FTS_MAX_TOKEN_LENGTH_MAX: u64 = 1024;
/// Bit widths Lance accepts for an IVF_RQ index.
pub const DENSE_RQ_BITS_MAX: u64 = 9;
/// Smallest and largest target rows per IVF_RQ partition. The floor keeps
/// Lance's partition-join threshold (a quarter of the target) above a few
/// rows; the ceiling is Lance's own default target for IVF_RQ.
pub const DENSE_PARTITION_ROWS_MIN: u64 = 32;
pub const DENSE_PARTITION_ROWS_MAX: u64 = 4096;
pub const DIMENSIONS_MAX: u64 = 8192;
pub const BATCH_SIZE_MAX: u64 = 2048;
pub const MAX_RETRIES_MAX: u64 = 10;
/// Composed result slots and dense candidates: one MCP search page.
const RESULT_SLOTS_MAX: u64 = crate::limits::mcp::MAX_SEARCH_LIMIT as u64;
const LEXICAL_CANDIDATES_MAX: u64 = 200;
const MAX_DOCUMENTS_MAX: u64 = 320;
const RERANK_TEXT_CHARS_MIN: u64 = 200;
const RERANK_TEXT_CHARS_MAX: u64 = 32768;
const QUERY_CACHE_MAX_BYTES: u64 = 128 * 1024 * 1024;
const MODEL_MAX_BYTES: u64 = 512;
const PREFIX_MAX_BYTES: u64 = 4096;
const INPUT_TYPE_MAX_BYTES: u64 = 128;
const URL_MAX_BYTES: u64 = 2048;
const AUTHORIZATION_CACHE_SUBJECTS_MAX: u64 = 1024;
const CONCURRENCY_MAX: u64 = 16;
const FILTER_COUNT_MAX_ROWS: u64 = 10_000_000;
const NPROBES_MAX: u64 = 65536;
const FTS_SEARCH_CHUNK_MAX: u64 = 1024;
const FTS_PARTITION_MEMORY_MB_MAX: u64 = 1_048_576;
const INSTRUCTIONS_MAX_CHARS: u64 = 4096;
const RESPONSE_BYTES_MIN: u64 = 4096;
const RESPONSE_BYTES_MAX: u64 = 16 * 1024 * 1024;

fn compiled(pattern: &str) -> regex::Regex {
    regex::Regex::new(pattern).unwrap_or_else(|error| unreachable!("valid pattern: {error}"))
}

static NAMESPACE: LazyLock<regex::Regex> = LazyLock::new(|| compiled(NAMESPACE_PATTERN));
static STORAGE_URI: LazyLock<regex::Regex> = LazyLock::new(|| compiled(STORAGE_URI_PATTERN));
static SOURCE_ID: LazyLock<regex::Regex> = LazyLock::new(|| compiled(SOURCE_ID_PATTERN));
pub static PROVIDER: LazyLock<regex::Regex> = LazyLock::new(|| compiled(PROVIDER_PATTERN));

/// Lance keeps unreferenced fragment files for 7 days; envelopes must be
/// committed or rejected well before that.
pub const LANCE_UNREFERENCED_RETENTION: ConfigDuration = ConfigDuration::from_days(7);
/// Margin below [`LANCE_UNREFERENCED_RETENTION`] for the maximum envelope age.
pub const ENVELOPE_AGE_MARGIN: ConfigDuration = ConfigDuration::from_days(1);
/// The maximum MCP request deadline; the version grace must exceed it.
pub const MAX_REQUEST_DEADLINE: ConfigDuration = ConfigDuration::from_secs(300);
/// Margin the identity validity deadline keeps beyond the visible sync,
/// commit and version-check intervals.
pub const IDENTITY_DEADLINE_MARGIN: ConfigDuration = ConfigDuration::from_mins(5);
/// Providers whose sources expose no permissions to read, so their
/// connections always use fixed readers.
pub const FIXED_ONLY_PROVIDERS: [&str; 2] = ["local-files", "notion"];
/// Providers whose `settings.corporate_domains` defaults to
/// `identity.directory.verified_domains`: the same operator assertion.
pub const DIRECTORY_DOMAIN_PROVIDERS: [&str; 2] = ["slack", "jira"];

/// The whole configuration file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "fyrr configuration")]
pub struct Config {
    pub namespace: NamespaceConfig,
    pub storage: StorageConfig,
    pub embedding: EmbeddingConfig,
    #[serde(default)]
    pub identity: IdentityConfig,
    #[serde(default)]
    pub reader: ReaderConfig,
    #[serde(default)]
    pub writer: WriterConfig,
    /// Connector sources (`[[sources]]`).
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

/// The namespace this deployment serves and writes: one Lance dataset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfig {
    /// Optional hosted reranker. Its calls occupy a reader concurrency slot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rerank: Option<RerankConfig>,
    /// Candidate depths, fusion weight and batched output bounds.
    #[serde(default)]
    pub retrieval: RetrievalConfig,
    /// Document slots: fusion first, then further unique dense and lexical hits.
    #[serde(default)]
    pub composition: Composition,
    /// Namespace ID: lowercase letters, digits and `-`, at most 63 bytes.
    #[schemars(regex(pattern = NAMESPACE_PATTERN))]
    pub name: String,
    /// Native FTS `max_token_length`; part of the publication identity.
    #[serde(default = "default_fts_max_token_length")]
    #[schemars(range(min = 1, max = FTS_MAX_TOKEN_LENGTH_MAX))]
    pub fts_max_token_length: u32,
    /// Publication-level English stemming; changing it rebuilds the FTS index.
    #[serde(default)]
    pub fts_stem: bool,
    /// Publication-level stop-word removal; changing it rebuilds the FTS index.
    #[serde(default)]
    pub fts_remove_stop_words: bool,
    /// Publication-level ASCII folding; changing it rebuilds the FTS index.
    #[serde(default)]
    pub fts_ascii_folding: bool,
    /// RaBitQ bits per dimension of the dense vector index. Fixed when the
    /// namespace is first published; a writer with another value refuses to
    /// extend the index.
    #[serde(default = "default_dense_rq_bits")]
    #[schemars(range(min = 1, max = DENSE_RQ_BITS_MAX))]
    pub dense_rq_bits: u8,
    /// Target rows per partition of the dense vector index. Fixed when the
    /// namespace is first published; a writer with another value refuses to
    /// extend the index.
    #[serde(default = "default_dense_partition_rows")]
    #[schemars(range(min = DENSE_PARTITION_ROWS_MIN, max = DENSE_PARTITION_ROWS_MAX))]
    pub dense_partition_rows: u32,
}

/// Optional reranking after fusion; absent configuration makes no provider calls.
/// Batch calls run at most four at once and occupy one reader concurrency slot
/// until all finish or the shared deadline expires. No retries or redirects.
/// Authorization is checked after connecting, immediately before emitting the
/// body. A revocation between that check and the provider receiving the bytes
/// remains possible, as with any read that returns data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RerankConfig {
    /// Complete HTTPS endpoint (loopback HTTP is allowed for a local provider).
    pub endpoint: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<SecretRef>,
    /// Authorized composed documents to rerank before the request limit.
    #[schemars(range(min = 1, max = RESULT_SLOTS_MAX))]
    pub candidates: usize,
    /// Logistic population-z score blend; 1 uses raw reranker order, 0 fusion order.
    #[schemars(range(min = 0, max = 1))]
    pub weight: f64,
    /// One deadline for preparation and every provider call in a search batch.
    pub deadline: ConfigDuration,
    /// Shared process-lifetime namespace cap, in millionths of a US dollar.
    /// Reservations are not refunded after dispatch, even on failure.
    pub spend_limit_microusd: u64,
    /// Operator-attested upper cost bound for any request up to 1 MiB of text.
    /// Must cover the chosen model's billing, including repeated query tokens.
    /// This amount is reserved before each dispatch; no provider invoice is trusted.
    #[schemars(range(min = 1))]
    pub max_call_cost_microusd: u64,
    /// Optional per-candidate text bound in characters: send the window with the
    /// most distinct query terms instead of the whole record. Absent sends the
    /// whole record (and keeps the policy digest unchanged).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = RERANK_TEXT_CHARS_MIN, max = RERANK_TEXT_CHARS_MAX))]
    pub text_chars: Option<usize>,
}

/// Retrieval policy bound into each publication's ranking identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RetrievalConfig {
    /// Float16 candidate refinement; legacy keeps the original fourfold depth.
    #[serde(default, skip_serializing_if = "RescoreDepth::is_legacy")]
    pub rescore_depth: RescoreDepth,
    #[schemars(range(min = 0, max = 1))]
    pub dense_weight: f64,
    #[schemars(range(min = 1, max = RESULT_SLOTS_MAX))]
    pub dense_candidates: usize,
    #[schemars(range(min = 1, max = LEXICAL_CANDIDATES_MAX))]
    pub lexical_candidates: usize,
    /// Soft cap per composition bucket; zero disables it.
    #[schemars(range(max = RESULT_SLOTS_MAX))]
    pub source_cap: usize,
    /// Total unique documents in a multi-query response.
    #[schemars(range(min = 1, max = MAX_DOCUMENTS_MAX))]
    pub max_documents: usize,
}
impl Default for RetrievalConfig {
    fn default() -> Self {
        Self {
            rescore_depth: RescoreDepth::Legacy,
            dense_weight: 0.5,
            dense_candidates: 75,
            lexical_candidates: 200,
            source_cap: 2,
            max_documents: 75,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum RescoreDepth {
    #[default]
    Legacy,
    Fixed {
        candidates: usize,
    },
    /// Expand when the exact cosine gap at the retained boundary is small.
    Adaptive {
        min_candidates: usize,
        max_candidates: usize,
        min_gap: f64,
    },
}
impl RescoreDepth {
    fn is_legacy(&self) -> bool {
        *self == Self::Legacy
    }
    pub fn initial(&self, k: usize) -> usize {
        match self {
            Self::Legacy => k * 4,
            Self::Fixed { candidates } => (*candidates).max(k + 1),
            Self::Adaptive { min_candidates, .. } => (*min_candidates).max(k + 1),
        }
    }
    pub fn maximum(&self, k: usize) -> usize {
        let configured = match self {
            Self::Legacy => 0,
            Self::Fixed { candidates } => *candidates,
            Self::Adaptive { max_candidates, .. } => *max_candidates,
        };
        configured.max(k * 16)
    }
    pub fn expand(&self, depth: usize, k: usize, gap: f64) -> Option<usize> {
        match self {
            Self::Adaptive {
                max_candidates,
                min_gap,
                ..
            } if gap < *min_gap && depth < (*max_candidates).max(k) => {
                Some((depth * 2).min((*max_candidates).max(k)))
            }
            _ => None,
        }
    }
}

/// Operator composition; its values are bound into the retrieval policy identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Composition {
    #[schemars(range(min = 1, max = RESULT_SLOTS_MAX))]
    pub fusion: usize,
    #[schemars(range(max = RESULT_SLOTS_MAX - 1))]
    pub dense: usize,
    #[schemars(range(max = RESULT_SLOTS_MAX - 1))]
    pub lexical: usize,
}
impl Default for Composition {
    fn default() -> Self {
        Self {
            fusion: 50,
            dense: 0,
            lexical: 0,
        }
    }
}
impl NamespaceConfig {
    /// Canonical analyzer settings shared by publication and admission.
    pub fn fts_settings(&self) -> crate::publication::FtsSettings {
        crate::publication::FtsSettings {
            tokenizer: "simple".into(),
            max_token_length: self.fts_max_token_length,
            lowercase: true,
            stem: self.fts_stem,
            remove_stop_words: self.fts_remove_stop_words,
            ascii_folding: self.fts_ascii_folding,
            with_position: false,
        }
    }
    /// Includes fixed lane weights, candidate bounds, source cap, P1 and composition.
    pub fn retrieval_digest(&self) -> String {
        let mut policy = vec![
            serde_json::json!(
                "slice2:cosine:ivf-rq1:f16:550:training256:logistic-z:population:lex.75:dense.25:source2-soft-per-bucket:lex50:dense75:p1:1200:phrase0"
            ),
            serde_json::json!(self.fts_settings()),
            serde_json::json!(self.composition),
        ];
        // The historical policy omitted these fields. Keep its identity tied
        // to the original values rather than to defaults that can change.
        let legacy = RetrievalConfig {
            rescore_depth: RescoreDepth::Legacy,
            dense_weight: 0.25,
            dense_candidates: 75,
            lexical_candidates: 50,
            source_cap: 2,
            max_documents: 75,
        };
        if self.retrieval != legacy {
            policy.push(serde_json::json!({"retrieval_v1": self.retrieval}));
        }
        if let Some(rerank) = &self.rerank {
            policy.push(serde_json::json!({"rerank_v1": rerank}));
        }
        crate::limits::digest(serde_json::Value::Array(policy).to_string().as_bytes())
    }
}

/// The durable truth: S3 (or S3-compatible) storage, or a local directory
/// for single-host use. The other keys configure S3 access only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Root URI, `s3://bucket[/prefix]` or `file:///absolute/path`.
    #[schemars(regex(pattern = STORAGE_URI_PATTERN))]
    pub uri: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// Endpoint of an S3-compatible service; absent for AWS S3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Allow a plain-HTTP endpoint (development only).
    #[serde(default)]
    pub allow_http: bool,
    /// Use virtual-hosted-style requests instead of path-style.
    #[serde(default)]
    pub virtual_hosted_style: bool,
    /// Static credentials; absent means the default AWS credential chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<SecretRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<SecretRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<SecretRef>,
}

/// Embedding identity, role transformations and bounded provider access.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingConfig {
    /// Base URL of the OpenAI-compatible embeddings API.
    pub endpoint: String,
    #[serde(default = "default_model")]
    #[schemars(length(min = 1, max = MODEL_MAX_BYTES), extend("x-maxUtf8Bytes" = MODEL_MAX_BYTES))]
    pub model: String,
    #[serde(default = "default_dimensions")]
    #[schemars(range(min = 1, max = DIMENSIONS_MAX))]
    pub dimensions: u32,
    /// Prefix applied to both documents and queries (same rules for both).
    #[serde(default)]
    #[schemars(length(max = PREFIX_MAX_BYTES), extend("x-maxUtf8Bytes" = PREFIX_MAX_BYTES))]
    pub prefix: String,
    /// Optional document/query transformations; exact UTF-8 is preserved.
    #[serde(default)]
    pub roles: EmbeddingRoles,
    /// Send the configured dimension as the standard API `dimensions` field.
    #[serde(default)]
    pub send_dimensions: bool,
    /// Permit live query embeddings after reader authorization (off by default).
    #[serde(default)]
    pub reader_live: bool,
    /// Writer document-vector cache directory; default
    /// `$XDG_CACHE_HOME/fyrr/writer-embeddings`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_cache: Option<String>,
    /// Maximum query-cache bytes, including keys and vectors, per reader process.
    #[serde(default = "default_query_cache_bytes")]
    #[schemars(range(min = 0, max = QUERY_CACHE_MAX_BYTES))]
    pub query_cache_max_bytes: u64,
    /// Required for live calls. Scope: one reader process or writer sync
    /// invocation, including retries; this is not an account-wide spending cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spend: Option<SpendConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<SecretRef>,
    #[serde(default = "default_embedding_batch")]
    #[schemars(range(min = 1, max = BATCH_SIZE_MAX))]
    pub batch_size: u32,
    #[serde(default = "default_embedding_timeout")]
    pub timeout: ConfigDuration,
    #[serde(default = "default_embedding_retries")]
    #[schemars(range(max = MAX_RETRIES_MAX))]
    pub max_retries: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingRole {
    /// Added after the shared prefix, before the unchanged input text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = PREFIX_MAX_BYTES), extend("x-maxUtf8Bytes" = PREFIX_MAX_BYTES))]
    pub prefix: Option<String>,
    /// Sent as `input_type` only when configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(min = 1, max = INPUT_TYPE_MAX_BYTES),
        extend("x-maxUtf8Bytes" = INPUT_TYPE_MAX_BYTES)
    )]
    pub input_type: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingRoles {
    #[serde(default)]
    pub document: EmbeddingRole,
    #[serde(default)]
    pub query: EmbeddingRole,
}

/// Conservative reservation before every attempt, including identity probes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpendConfig {
    #[schemars(range(min = 1))]
    pub limit_microusd: u64,
    /// Upper bound on one call's cost; reserved even for an ambiguous timeout.
    #[schemars(range(min = 1))]
    pub max_call_microusd: u64,
}

fn default_query_cache_bytes() -> u64 {
    16 * 1024 * 1024
}

/// Identity mapping and the identity artifact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    /// 32-byte hex AEAD key, supplied only through an environment reference.
    /// Required with a `permissions = "sync"` source, a `snapshot` or
    /// `keycloak` directory, or writer administration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption_key: Option<SecretRef>,

    /// Tokens assigned to the configured stdio subject.
    #[serde(default)]
    pub stdio_readers: Vec<String>,
    /// Namespace HMAC key, required; only an environment reference is accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hashing_key: Option<SecretRef>,
    /// Identity artifact validity, counted from the directory freshness instant.
    #[serde(default = "default_validity_deadline")]
    pub validity_deadline: ConfigDuration,
    /// A subject with more reader tokens gets `authorization_unavailable`.
    #[serde(default = "default_max_tokens_per_user")]
    #[schemars(range(min = 1))]
    pub max_tokens_per_user: u32,
    /// The subject of the stdio transport (one process identity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdio_user: Option<String>,
    #[serde(default)]
    pub directory: DirectoryConfig,
    /// OIDC login for HTTP MCP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oidc: Option<OidcConfig>,
}

/// Where canonical users and groups come from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DirectoryConfig {
    #[serde(default)]
    pub provider: DirectoryProvider,
    /// Pull interval, or the attestation interval of SCIM push.
    #[serde(default = "default_directory_interval")]
    pub sync_interval: ConfigDuration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<SecretRef>,
    /// Keycloak base URL; HTTPS except explicitly allowed loopback fixtures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    /// Optional Keycloak service-account client; credentials then reference its secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// Corporate domains accepted for Keycloak canonical e-mail keys.
    #[serde(default)]
    pub verified_domains: Vec<String>,
    /// A complete generic DirectorySnapshot file, read at the writer boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_file: Option<PathBuf>,
}

/// Directory provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum DirectoryProvider {
    Scim,
    Keycloak,
    Snapshot,
    #[default]
    None,
}

/// OIDC resource-server settings for HTTP MCP.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OidcConfig {
    /// Operator-pinned key endpoint; token headers never choose a URL. With
    /// `keycloak = true` it defaults to `<issuer>/protocol/openid-connect/certs`.
    #[serde(default)]
    pub jwks_url: String,
    #[serde(default = "default_jwks_validity")]
    pub jwks_validity: ConfigDuration,
    #[serde(default = "default_jwks_refresh")]
    pub jwks_refresh_interval: ConfigDuration,
    #[serde(default = "default_mcp_scope")]
    pub scope: String,
    /// Accept Keycloak's `JWT`/`Bearer` access-token profile. Defaults to
    /// true when the issuer is taken from a Keycloak directory.
    #[serde(default)]
    pub keycloak: bool,
    /// Exact issuer. With a Keycloak directory it defaults to the directory's
    /// issuer; with a Keycloak or SCIM directory it must equal it.
    #[serde(default)]
    pub issuer: String,
    /// Required audience: the token's `aud`, a string or an array, contains it.
    pub audience: String,
}

fn default_jwks_validity() -> ConfigDuration {
    ConfigDuration::from_secs(300)
}
fn default_jwks_refresh() -> ConfigDuration {
    ConfigDuration::from_secs(5)
}
fn default_mcp_scope() -> String {
    "fyrr:read".into()
}
fn default_cache_subjects() -> usize {
    64
}
fn default_requests_per_minute() -> u32 {
    120
}

/// Operator-controlled HTTP boundary and bounded authorized-state retention.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    pub listen: String,
    #[schemars(length(min = 1, max = URL_MAX_BYTES))]
    pub public_url: String,
    /// Bearer credential of `POST /admin/refresh`; absent, the route is not served.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_credential: Option<SecretRef>,
    #[serde(default = "default_cache_subjects")]
    #[schemars(range(min = 1, max = AUTHORIZATION_CACHE_SUBJECTS_MAX))]
    pub authorization_cache_subjects: usize,
    #[serde(default = "default_requests_per_minute")]
    #[schemars(range(min = 1))]
    pub user_requests_per_minute: u32,
    #[serde(default = "default_requests_per_minute")]
    #[schemars(range(min = 1))]
    pub group_requests_per_minute: u32,
}

/// Reader settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReaderConfig {
    /// Float16 refinement read backend; both paths read only admitted rows.
    #[serde(default)]
    pub sparse_read: SparseRead,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpConfig>,
    /// Independent requests executing at once; the waiting queue has the same bound.
    #[serde(default = "default_concurrency")]
    #[schemars(range(min = 1, max = CONCURRENCY_MAX))]
    pub concurrency: usize,
    /// Maximum authorized rows inspected by one list_filters call; no partial counts.
    #[serde(default = "default_filter_rows")]
    #[schemars(range(min = 1, max = FILTER_COUNT_MAX_ROWS))]
    pub filter_count_max_rows: usize,
    /// Nearest permission-bearing IVF partitions searched per index segment.
    #[serde(default = "default_nprobes")]
    #[schemars(range(min = 1, max = NPROBES_MAX))]
    pub nprobes: usize,
    /// IVF centroid routing; exact stays the default until scale calibration.
    #[serde(default)]
    pub routing: RoutingStrategy,
    /// Full-text partitions scored by one task of a query. With 1 every
    /// partition of an index segment is its own task, so one query scores
    /// several partitions at once on spare compute threads. Absent keeps
    /// Lance's default of 16, which scores up to 16 partitions one after
    /// another. Scores do not depend on it. It takes effect only for indexes
    /// of several partitions (`writer.fts_partition_memory_mb`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = FTS_SEARCH_CHUNK_MAX))]
    pub fts_search_chunk: Option<usize>,
    /// Local immutable-file mirror directory; default
    /// `$XDG_CACHE_HOME/fyrr/<namespace>`, else `$HOME/.cache/fyrr/<namespace>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror_path: Option<String>,
    /// Disk limit for the read-through mirror.
    #[serde(default = "default_mirror_bytes")]
    pub mirror_max_bytes: u64,
    /// Reader memory envelope used for native caches and generation reservations.
    #[serde(default = "default_reader_memory")]
    pub memory_max_bytes: ByteSize,
    /// Space left unused by generation preparation for requests and allocator overhead.
    #[serde(default = "default_reader_headroom")]
    pub memory_headroom_bytes: ByteSize,
    /// Serving-head version check mode.
    #[serde(default)]
    pub version_check: VersionCheck,
    #[serde(default = "default_version_check_interval")]
    pub version_check_interval: ConfigDuration,
    /// `per-request` mode: wait this long for a newer version, then serve flagged.
    #[serde(default = "default_per_request_wait")]
    pub per_request_wait: ConfigDuration,
    /// Upper bound on preparing a new generation (mirror sync, maps, prewarm);
    /// `writer.version_grace` must exceed it plus the maximum request deadline.
    #[serde(default = "default_max_preparation")]
    pub max_preparation: ConfigDuration,
    /// Optional search response budget, in bytes of the serialized MCP tool result
    /// including its structured JSON and text copies, for MCP
    /// clients that refuse large tool results. Over budget, whole hits are dropped
    /// from the end of the rank order (each query keeps at least 4 while another
    /// still has tail hits); only then do the lowest-ranked hits lose their excerpt
    /// or content. `truncated` reports both counts and the removed bytes count as
    /// `omitted_bytes`. Absent returns every selected hit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = RESPONSE_BYTES_MIN, max = RESPONSE_BYTES_MAX))]
    pub max_response_bytes: Option<usize>,
    /// Text sent to MCP clients as `instructions` in the `initialize` result.
    /// Absent sends the built-in text; an empty string sends none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = INSTRUCTIONS_MAX_CHARS))]
    pub instructions: Option<String>,
}

impl ReaderConfig {
    /// The `instructions` the reader serves: the configured text, the built-in
    /// text when absent, nothing when empty.
    pub fn served_instructions(&self) -> Option<&str> {
        match self.instructions.as_deref() {
            None => Some(crate::mcp::SERVER_INSTRUCTIONS),
            Some("") => None,
            Some(text) => Some(text),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SparseRead {
    #[default]
    MaskedScan,
    TakeRows,
}

/// Partition routing; auto is not a measured crossover.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RoutingStrategy {
    #[default]
    Exact,
    Rabitq,
    Auto,
}

/// Version check modes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum VersionCheck {
    #[default]
    Interval,
    Off,
    PerRequest,
}

/// Writer settings: envelopes, commit, maintenance, retention.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriterConfig {
    /// Environment references resolved from encrypted credential records by workers.
    #[serde(default)]
    pub credentials: BTreeMap<String, String>,
    /// Per-worker committed-memory ceiling (RLIMIT_DATA); virtual address reservations are unrestricted.
    #[serde(default = "default_worker_memory_max_bytes")]
    pub worker_memory_max_bytes: ByteSize,
    /// CPU threads of each writer process: its async runtime, Lance's compute
    /// runtime, the parallel-iterator pool and the full-text build workers.
    /// Absent uses every CPU of the host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub threads: Option<usize>,
    /// Build target for one full-text index partition, in MiB of builder
    /// memory per full-text worker (Lance's partition size, 2048 by default).
    /// A smaller target makes a full-text index of several partitions, which a
    /// reader can search concurrently (`reader.fts_search_chunk`); it also
    /// keeps that size when index segments are merged. Scores do not depend on
    /// it. The count is roughly total builder memory divided by the target.
    /// Absent keeps Lance's default, usually one partition. Applies to the
    /// next full-text build and merge, not to an index already published.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = FTS_PARTITION_MEMORY_MB_MAX))]
    pub fts_partition_memory_mb: Option<u32>,
    /// Deleted or revoked content leaves old versions and the mirror within it.
    #[serde(default = "default_purge_window")]
    pub purge_window: ConfigDuration,
    /// Minimum time a version is kept after it stopped serving.
    #[serde(default = "default_version_grace")]
    pub version_grace: ConfigDuration,
    /// Older pending envelopes are rejected (forcing a re-scan).
    #[serde(default = "default_max_envelope_age")]
    pub max_envelope_age: ConfigDuration,
    /// Default per-document size limit; larger documents are quarantined.
    #[serde(default = "default_document_max_bytes")]
    pub document_max_bytes: ByteSize,
    #[serde(default = "default_envelope_max_rows")]
    #[schemars(range(min = 1))]
    pub envelope_max_rows: u64,
    #[serde(default = "default_envelope_max_bytes")]
    pub envelope_max_bytes: ByteSize,
    #[serde(default)]
    pub commit: CommitConfig,
    #[serde(default)]
    pub maintenance: MaintenanceConfig,
    #[serde(default)]
    pub backpressure: BackpressureConfig,
    #[serde(default)]
    pub serve: WriterServeConfig,
}

/// Commit groups and the namespace lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommitConfig {
    /// Merge lexical deltas before publication; off preserves incremental cost.
    #[serde(default)]
    pub merge_fts_segments: bool,
    #[serde(default = "default_group_max_rows")]
    #[schemars(range(min = 1))]
    pub group_max_rows: u64,
    #[serde(default = "default_group_max_bytes")]
    pub group_max_bytes: ByteSize,
    /// `fyrr writer serve` closes a group after this long.
    #[serde(default = "default_coalescing_timer")]
    pub coalescing_timer: ConfigDuration,
    #[serde(default = "default_lease_ttl")]
    pub lease_ttl: ConfigDuration,
}

/// Maintenance in bounded steps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceConfig {
    #[serde(default = "default_step_max_duration")]
    pub step_max_duration: ConfigDuration,
}

/// Content backpressure; permission, member and freshness work always passes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BackpressureConfig {
    #[serde(default = "default_max_pending_envelopes")]
    #[schemars(range(min = 1))]
    pub max_pending_content_envelopes: u64,
    #[serde(default = "default_max_pending_bytes")]
    pub max_pending_content_bytes: ByteSize,
}

/// The optional standing writer service.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriterServeConfig {
    /// Run the committer role; switch off to keep batch `commit` jobs.
    #[serde(default = "default_true")]
    pub committer: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin: Option<WriterAdminConfig>,
}

/// Optional writer administration; production uses a TLS-terminating proxy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriterAdminConfig {
    pub listen: String,
    pub operator_token: SecretRef,
    /// Required for non-loopback listeners, which must sit behind a TLS proxy.
    #[serde(default)]
    pub tls_terminated: bool,
}

/// One connector source (`[[sources]]`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    /// Source ID, stable for the life of the source.
    #[schemars(regex(pattern = SOURCE_ID_PATTERN))]
    pub id: String,
    /// Connector provider name, e.g. `local-files`.
    #[serde(rename = "type")]
    #[schemars(regex(pattern = PROVIDER_PATTERN))]
    pub provider: String,
    /// `sync` maps the source's permissions; `fixed` uses `readers`.
    #[serde(default)]
    pub permissions: PermissionsMode,
    /// Reader tokens (`usr:<id>`, `grp:<id>`) of a fixed connection.
    #[serde(default)]
    pub readers: Vec<String>,
    #[serde(default)]
    pub intervals: SourceIntervals,
    /// Overrides `writer.document_max_bytes`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_max_bytes: Option<ByteSize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_warning: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_error: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub retention_days: Option<u32>,
    /// Sync scope; validated by the provider's schema.
    #[serde(default)]
    pub scope: Map<String, Value>,
    /// Provider connection settings; secrets as `{ env = "VAR" }`.
    #[serde(default)]
    pub settings: Map<String, Value>,
}

/// Permissions mode of a connection.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum PermissionsMode {
    #[default]
    Sync,
    Fixed,
}

/// When each of a source's syncs is due.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceIntervals {
    #[serde(default = "default_content_interval")]
    pub content: ConfigDuration,
    /// Required for `sync` permissions, absent for `fixed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<ConfigDuration>,
    /// Membership snapshot interval; absent for `fixed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub members: Option<ConfigDuration>,
    /// Full-ID listing interval.
    #[serde(default = "default_reconcile_interval")]
    pub reconcile: ConfigDuration,
}

/// A reference to a secret in the environment: `{ env = "VAR_NAME" }`.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct SecretRef {
    pub env: String,
}

/// A resolved secret. Its value is never formatted.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretRef(env:{})", self.env)
    }
}

const SECRET_IN_FILE: &str =
    "secrets are never stored in the config file; write { env = \"VAR_NAME\" } instead";

/// Environment variable names a secret reference may use.
const ENV_NAME: &str = "[A-Z_][A-Z0-9_]*";

/// Whether `name` is an environment variable name a secret reference may use.
pub fn valid_env_name(name: &str) -> bool {
    !name.is_empty()
        && !name.as_bytes()[0].is_ascii_digit()
        && name
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// The longest credential variable name the provider host takes.
pub const HOST_ENV_NAME_MAX: usize = 256;

/// Why the provider host refuses a stored credential under `name`, or `None`.
/// `PATH`, `VIRTUAL_ENV`, `PYTHON*` and `LD_*` would change which interpreter,
/// modules or libraries the host runs, and a longer name is over its limit.
pub fn host_env_refusal(name: &str) -> Option<&'static str> {
    if name.len() > HOST_ENV_NAME_MAX {
        Some("a name delivered to the provider host is at most 256 bytes")
    } else if matches!(name, "PATH" | "VIRTUAL_ENV")
        || name.starts_with("PYTHON")
        || name.starts_with("LD_")
    {
        Some(
            "PATH, VIRTUAL_ENV and names starting with PYTHON or LD_ cannot be delivered to \
             the provider host: they would change what it runs",
        )
    } else {
        None
    }
}

impl<'de> Deserialize<'de> for SecretRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SecretVisitor;

        // Every non-table form is refused with the same message, so the
        // offending value is never echoed.
        macro_rules! refuse {
            ($($name:ident: $type:ty),*) => {$(
                fn $name<E: de::Error>(self, _: $type) -> Result<SecretRef, E> {
                    Err(E::custom(SECRET_IN_FILE))
                }
            )*};
        }

        impl<'de> Visitor<'de> for SecretVisitor {
            type Value = SecretRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("{ env = \"VAR_NAME\" }")
            }

            refuse!(visit_str: &str, visit_i64: i64, visit_u64: u64, visit_f64: f64, visit_bool: bool);

            fn visit_seq<A: de::SeqAccess<'de>>(self, _: A) -> Result<SecretRef, A::Error> {
                Err(de::Error::custom(SECRET_IN_FILE))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<SecretRef, A::Error> {
                let mut env = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key != "env" || env.is_some() {
                        return Err(de::Error::custom(
                            "a secret reference has exactly one key: env",
                        ));
                    }
                    // Deserialize the value as raw data so an unexpected type
                    // is refused without its value in the message.
                    let value: Value = map.next_value()?;
                    match value {
                        Value::String(name) if valid_env_name(&name) => env = Some(name),
                        _ => {
                            return Err(de::Error::custom(format!(
                                "env must name an environment variable ({ENV_NAME})"
                            )));
                        }
                    }
                }
                env.map(|env| SecretRef { env })
                    .ok_or_else(|| de::Error::custom("a secret reference needs env"))
            }
        }

        deserializer.deserialize_any(SecretVisitor)
    }
}

impl JsonSchema for SecretRef {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SecretRef".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "A secret taken from the environment variable `env`; never the secret itself.",
            "type": "object",
            "properties": { "env": { "type": "string", "pattern": format!("^{ENV_NAME}$") } },
            "required": ["env"],
            "additionalProperties": false
        })
    }
}

/// Reads environment variables; tests pass a fake.
pub trait Environment {
    fn var(&self, name: &str) -> Option<String>;
}

/// The process environment.
#[derive(Debug, Clone, Copy)]
pub struct ProcessEnvironment;

impl Environment for ProcessEnvironment {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

impl<F: Fn(&str) -> Option<String>> Environment for F {
    fn var(&self, name: &str) -> Option<String> {
        self(name)
    }
}

impl SecretRef {
    /// Resolves the secret; a missing or empty variable is an error naming the
    /// variable only.
    pub fn resolve(&self, env: &dyn Environment) -> Result<Secret, Issue> {
        match env.var(&self.env) {
            Some(value) if !value.is_empty() => Ok(Secret(value)),
            _ => Err(Issue::error(
                format!("env:{}", self.env),
                format!("environment variable {} is not set", self.env),
            )),
        }
    }
}

/// Object-store options for Lance, with secrets resolved. `Debug` redacts every
/// value.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageOptions(BTreeMap<String, String>);

impl StorageOptions {
    pub fn into_map(self) -> BTreeMap<String, String> {
        self.0
    }
}

impl fmt::Debug for StorageOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.keys().map(|key| (key, "<redacted>")))
            .finish()
    }
}

impl StorageConfig {
    /// Builds the object-store options (object_store `AmazonS3ConfigKey`
    /// names) Lance needs to reach this storage.
    /// A local directory needs none.
    pub fn object_store_options(&self, env: &dyn Environment) -> Result<StorageOptions, Issue> {
        let mut options = BTreeMap::new();
        if matches!(self.backend(), Some(StorageBackend::Local { .. })) {
            return Ok(StorageOptions(options));
        }
        options.insert("aws_region".to_owned(), self.region.clone());
        if let Some(endpoint) = &self.endpoint {
            options.insert("aws_endpoint".to_owned(), endpoint.clone());
        }
        options.insert("allow_http".to_owned(), self.allow_http.to_string());
        options.insert(
            "aws_virtual_hosted_style_request".to_owned(),
            self.virtual_hosted_style.to_string(),
        );
        for (key, secret) in [
            ("aws_access_key_id", &self.access_key_id),
            ("aws_secret_access_key", &self.secret_access_key),
            ("aws_session_token", &self.session_token),
        ] {
            if let Some(secret) = secret {
                options.insert(key.to_owned(), secret.resolve(env)?.0);
            }
        }
        Ok(StorageOptions(options))
    }
}

/// One configuration problem, located by its dotted key path or file position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub severity: Severity,
    pub path: String,
    pub message: String,
}

/// Errors fail `fyrr config check`; warnings are printed only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

impl Issue {
    pub fn error(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            path: path.into(),
            message: message.into(),
        }
    }

    pub fn warning(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            ..Self::error(path, message)
        }
    }
}

impl fmt::Display for Issue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let severity = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        write!(f, "{severity}: {}: {}", self.path, self.message)
    }
}

/// A parsed configuration with the raw file table, for value origins.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config: Config,
    raw: toml::Table,
}

/// Where an effective value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Default,
    File,
    Env,
}

impl Origin {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::File => "file",
            Self::Env => "env",
        }
    }
}

/// One effective configuration value for `fyrr config show`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveValue {
    pub path: String,
    /// TOML rendering; a secret shows only `env:NAME (set|unset)`.
    pub value: String,
    pub origin: Origin,
}

/// Reads and parses a configuration file. The error names the file and a
/// position, never quoting file content.
pub fn load(path: &Path) -> Result<LoadedConfig, Issue> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        Issue::error(
            path.display().to_string(),
            format!("cannot read: {}", error.kind()),
        )
    })?;
    parse(&text).map_err(|issue| Issue {
        path: format!("{}:{}", path.display(), issue.path),
        ..issue
    })
}

/// Parses configuration text (no cross-field validation; see
/// [`LoadedConfig::validate`]).
pub fn parse(text: &str) -> Result<LoadedConfig, Issue> {
    let raw: toml::Table = toml::from_str(text).map_err(|error| toml_issue(text, &error))?;
    let mut config: Config = toml::from_str(text).map_err(|error| toml_issue(text, &error))?;
    derive_defaults(&mut config, &raw);
    Ok(LoadedConfig { config, raw })
}

/// Fills defaults that follow from other keys of the same file; explicit
/// values always win. Every derived URL comes from operator configuration.
fn derive_defaults(config: &mut Config, raw: &toml::Table) {
    let identity = &mut config.identity;
    if let Some(oidc) = &mut identity.oidc {
        if oidc.issuer.is_empty()
            && identity.directory.provider == DirectoryProvider::Keycloak
            && let Some(issuer) = identity.directory.subject_issuer()
        {
            oidc.issuer = issuer;
            let keycloak_set = raw
                .get("identity")
                .and_then(|identity| identity.get("oidc"))
                .is_some_and(|oidc| oidc.get("keycloak").is_some());
            if !keycloak_set {
                oidc.keycloak = true;
            }
        }
        if oidc.keycloak && oidc.jwks_url.is_empty() && !oidc.issuer.is_empty() {
            oidc.jwks_url = format!(
                "{}/protocol/openid-connect/certs",
                oidc.issuer.trim_end_matches('/')
            );
        }
    }
    let domains = &identity.directory.verified_domains;
    if !domains.is_empty() {
        for source in &mut config.sources {
            if DIRECTORY_DOMAIN_PROVIDERS.contains(&source.provider.as_str()) {
                source
                    .settings
                    .entry("corporate_domains")
                    .or_insert_with(|| Value::from(domains.clone()));
            }
        }
    }
}

impl DirectoryConfig {
    /// The issuer under which the directory binds OIDC subjects: `issuer`,
    /// or for Keycloak without it `<endpoint>/realms/<realm>`. `None` for
    /// directories that bind no single issuer.
    pub fn subject_issuer(&self) -> Option<String> {
        match self.provider {
            DirectoryProvider::Keycloak => self.issuer.clone().or_else(|| {
                let mut url = url::Url::parse(self.endpoint.as_deref()?).ok()?;
                let realm = self.realm.as_deref().filter(|realm| !realm.is_empty())?;
                url.path_segments_mut()
                    .ok()?
                    .pop_if_empty()
                    .extend(["realms", realm]);
                Some(url.to_string())
            }),
            DirectoryProvider::Scim => self.issuer.clone(),
            DirectoryProvider::Snapshot | DirectoryProvider::None => None,
        }
    }
}

impl Config {
    /// The reader's mirror directory: `reader.mirror_path`, or
    /// `$XDG_CACHE_HOME/fyrr/<namespace>`, else `$HOME/.cache/fyrr/<namespace>`.
    /// `None` when it is not configured and neither variable is an absolute path.
    pub fn mirror_directory(&self, env: &dyn Environment) -> Option<PathBuf> {
        if let Some(path) = &self.reader.mirror_path {
            return Some(PathBuf::from(path));
        }
        let absolute = |name: &str| {
            env.var(name)
                .map(PathBuf::from)
                .filter(|path| path.is_absolute())
        };
        let cache = absolute("XDG_CACHE_HOME")
            .or_else(|| absolute("HOME").map(|home| home.join(".cache")))?;
        Some(cache.join("fyrr").join(&self.namespace.name))
    }
}

fn toml_issue(text: &str, error: &toml::de::Error) -> Issue {
    let position = error.span().map_or_else(
        || "1:1".to_owned(),
        |span| {
            let before = &text[..span.start.min(text.len())];
            let line = before.matches('\n').count() + 1;
            let column = before.len() - before.rfind('\n').map_or(0, |at| at + 1) + 1;
            format!("{line}:{column}")
        },
    );
    // `message()` excludes toml's source snippet, which could quote a secret.
    Issue::error(position, without_values(error.message().trim()))
}

/// Removes file values from a deserialization message. Serde and toml
/// messages read `<what was found>, expected <what the schema wants>`: the
/// first part can quote the value (`invalid type: string "…"`, `unknown
/// variant `…``), the expectation comes from the types. A value may itself
/// contain quotes, backticks or `, expected `, so the message is split at the
/// last `, expected ` and the found part is cut at its first quote character;
/// nothing after an opening quote is kept. Key names (`unknown field `x``)
/// and fyrr's own fixed messages are kept whole.
fn without_values(message: &str) -> String {
    let keeps_key = [
        "unknown field",
        "missing field",
        "duplicate field",
        "duplicate key",
    ]
    .iter()
    .any(|prefix| message.starts_with(prefix));
    if keeps_key || message == SECRET_IN_FILE {
        return message.to_owned();
    }
    let (found, expected) = match message.rsplit_once(", expected ") {
        Some((found, expected)) => (found, Some(expected)),
        None => (message, None),
    };
    let found = match found.find(['"', '`', '\'']) {
        Some(quote) => format!("{}…", &found[..quote]),
        None => found.to_owned(),
    };
    match expected {
        Some(expected) => format!("{found}, expected {expected}"),
        None => found,
    }
}

/// The schema's `minimum`/`maximum` message for an integer outside `min..=max`.
fn range_issue(value: u64, min: u64, max: u64) -> Option<String> {
    if (min..=max).contains(&value) {
        None
    } else if max == u64::MAX {
        Some(format!("must be at least {min}"))
    } else {
        Some(format!("must be between {min} and {max}"))
    }
}

impl LoadedConfig {
    /// Every secret reference in the file whose variable is unset.
    pub fn missing_secrets(&self, env: &dyn Environment) -> Vec<Issue> {
        let mut found = Vec::new();
        collect_secret_refs("", &toml::Value::Table(self.effective_table()), &mut found);
        let mut issues = Vec::new();
        for (path, secret) in found {
            if self.config.writer.credentials.contains_key(&secret.env)
                && path != "identity.encryption_key"
            {
                continue;
            }
            if let Err(issue) = secret.resolve(env) {
                issues.push(Issue::error(path, issue.message));
            }
        }
        issues
    }

    fn effective_table(&self) -> toml::Table {
        toml::Table::try_from(&self.config)
            .unwrap_or_else(|error| unreachable!("config types serialize to TOML: {error}"))
    }

    /// Every effective value with its origin, in file order of sections.
    pub fn effective_values(&self, env: &dyn Environment) -> Vec<EffectiveValue> {
        let mut values = Vec::new();
        let raw = toml::Value::Table(self.raw.clone());
        walk_effective(
            "",
            &toml::Value::Table(self.effective_table()),
            Some(&raw),
            Context::default(),
            env,
            &mut values,
        );
        for value in &mut values {
            if value.path == "identity.stdio_user"
                || value.path.starts_with("identity.stdio_readers")
            {
                value.value = "<redacted: identity>".into();
            }
        }
        values
    }
}

const SECRET_KEY_MARKERS: [&str; 8] = [
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "credential",
];

fn secret_like(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    SECRET_KEY_MARKERS
        .iter()
        .any(|marker| lowered.contains(marker))
}

/// Opaque provider tables must not hold secrets inline: a string or number
/// under a secret-like key, or anywhere inside a table under one, is refused
/// (its value is never echoed). `config show` redacts the same paths.
fn find_inline_secrets(
    path: &str,
    value: &Value,
    inherited: bool,
    found: &mut Vec<(String, String)>,
) {
    match value {
        Value::Object(object) if is_secret_ref_object(object) => {}
        Value::Object(object) => {
            for (key, value) in object {
                find_inline_secrets(
                    &format!("{path}.{key}"),
                    value,
                    inherited || secret_like(key),
                    found,
                );
            }
        }
        Value::Array(items) => {
            for (index, value) in items.iter().enumerate() {
                find_inline_secrets(&format!("{path}[{index}]"), value, inherited, found);
            }
        }
        Value::String(_) | Value::Number(_) if inherited => {
            found.push((path.into(), SECRET_IN_FILE.into()));
        }
        _ => {}
    }
}

pub fn is_secret_ref_object(object: &Map<String, Value>) -> bool {
    object.len() == 1
        && object
            .get("env")
            .and_then(Value::as_str)
            .is_some_and(valid_env_name)
}

fn as_secret_ref(value: &toml::Value) -> Option<SecretRef> {
    let table = value.as_table()?;
    let name = table.get("env")?.as_str()?;
    (table.len() == 1 && valid_env_name(name)).then(|| SecretRef {
        env: name.to_owned(),
    })
}

fn join(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_owned()
    } else {
        format!("{prefix}.{key}")
    }
}

fn collect_secret_refs(path: &str, value: &toml::Value, found: &mut Vec<(String, SecretRef)>) {
    if let Some(secret) = as_secret_ref(value) {
        found.push((path.to_owned(), secret));
        return;
    }
    match value {
        toml::Value::Table(table) => {
            for (key, inner) in table {
                collect_secret_refs(&join(path, key), inner, found);
            }
        }
        toml::Value::Array(items) => {
            for (index, inner) in items.iter().enumerate() {
                collect_secret_refs(&format!("{path}[{index}]"), inner, found);
            }
        }
        _ => {}
    }
}

/// Where a value sits: inside a source's opaque `scope`/`settings`, and
/// under a secret-like key there.
#[derive(Debug, Clone, Copy, Default)]
struct Context {
    opaque: bool,
    secret: bool,
}

/// Rendering of a value the inline-secret check refuses.
pub const REDACTED: &str = "<redacted: inline secret>";

fn walk_effective(
    path: &str,
    value: &toml::Value,
    raw: Option<&toml::Value>,
    context: Context,
    env: &dyn Environment,
    out: &mut Vec<EffectiveValue>,
) {
    if let Some(secret) = as_secret_ref(value) {
        let state = if env.var(&secret.env).is_some_and(|value| !value.is_empty()) {
            "set"
        } else {
            "unset"
        };
        out.push(EffectiveValue {
            path: path.to_owned(),
            value: format!("env:{} ({state})", secret.env),
            origin: Origin::Env,
        });
        return;
    }
    let source_entry = path.starts_with("sources[") && path.ends_with(']') && !path.contains('.');
    match value {
        toml::Value::Table(table) if !table.is_empty() => {
            for (key, inner) in table {
                let opaque =
                    context.opaque || (source_entry && (key == "scope" || key == "settings"));
                let child = Context {
                    opaque,
                    secret: context.secret || (context.opaque && secret_like(key)),
                };
                let raw_inner = raw.and_then(|raw| raw.get(key));
                walk_effective(&join(path, key), inner, raw_inner, child, env, out);
            }
        }
        // An array holding a table anywhere (also mixed with scalars or nested
        // in inner arrays) is walked element by element, so a secret-like key
        // inside it is redacted like any other.
        toml::Value::Array(items) if items.iter().any(holds_table) => {
            for (index, inner) in items.iter().enumerate() {
                let raw_inner = raw.and_then(|raw| raw.get(index));
                walk_effective(
                    &format!("{path}[{index}]"),
                    inner,
                    raw_inner,
                    context,
                    env,
                    out,
                );
            }
        }
        leaf => out.push(EffectiveValue {
            path: path.to_owned(),
            value: if context.secret && !matches!(leaf, toml::Value::Boolean(_)) {
                REDACTED.to_owned()
            } else {
                leaf.to_string()
            },
            origin: if raw.is_some() {
                Origin::File
            } else {
                Origin::Default
            },
        }),
    }
}

fn holds_table(value: &toml::Value) -> bool {
    match value {
        toml::Value::Table(_) => true,
        toml::Value::Array(items) => items.iter().any(holds_table),
        _ => false,
    }
}

fn default_fts_max_token_length() -> u32 {
    40
}
fn default_dense_rq_bits() -> u8 {
    crate::publication::LEGACY_DENSE_RQ_BITS
}
fn default_dense_partition_rows() -> u32 {
    crate::publication::LEGACY_DENSE_PARTITION_ROWS
}
fn default_region() -> String {
    "us-east-1".to_owned()
}
fn default_model() -> String {
    "BAAI/bge-m3".to_owned()
}
fn default_dimensions() -> u32 {
    1024
}
fn default_concurrency() -> usize {
    8
}
fn default_filter_rows() -> usize {
    1_000_000
}
fn default_nprobes() -> usize {
    256
}
fn default_reader_memory() -> ByteSize {
    ByteSize::mib(2304)
}
fn default_reader_headroom() -> ByteSize {
    ByteSize::mib(128)
}
fn default_mirror_bytes() -> u64 {
    8 * 1024 * 1024 * 1024
}
fn default_embedding_batch() -> u32 {
    32
}
fn default_embedding_timeout() -> ConfigDuration {
    ConfigDuration::from_secs(30)
}
fn default_embedding_retries() -> u32 {
    3
}
fn default_validity_deadline() -> ConfigDuration {
    ConfigDuration::from_hours(1)
}
fn default_max_tokens_per_user() -> u32 {
    5000
}
fn default_directory_interval() -> ConfigDuration {
    ConfigDuration::from_mins(15)
}
fn default_version_check_interval() -> ConfigDuration {
    ConfigDuration::from_secs(30)
}
fn default_per_request_wait() -> ConfigDuration {
    ConfigDuration::from_secs(2)
}
fn default_max_preparation() -> ConfigDuration {
    ConfigDuration::from_mins(15)
}
fn default_purge_window() -> ConfigDuration {
    ConfigDuration::from_hours(24)
}
fn default_version_grace() -> ConfigDuration {
    ConfigDuration::from_hours(1)
}
fn default_max_envelope_age() -> ConfigDuration {
    ConfigDuration::from_days(3)
}
fn default_worker_memory_max_bytes() -> ByteSize {
    ByteSize::gib(2)
}
fn default_document_max_bytes() -> ByteSize {
    ByteSize::mib(8)
}
fn default_envelope_max_rows() -> u64 {
    50_000
}
fn default_envelope_max_bytes() -> ByteSize {
    ByteSize::mib(256)
}
fn default_group_max_rows() -> u64 {
    1_000_000
}
fn default_group_max_bytes() -> ByteSize {
    ByteSize::gib(4)
}
fn default_coalescing_timer() -> ConfigDuration {
    ConfigDuration::from_secs(60)
}
fn default_lease_ttl() -> ConfigDuration {
    ConfigDuration::from_secs(60)
}
fn default_step_max_duration() -> ConfigDuration {
    ConfigDuration::from_mins(5)
}
fn default_max_pending_envelopes() -> u64 {
    1000
}
fn default_max_pending_bytes() -> ByteSize {
    ByteSize::gib(16)
}
fn default_true() -> bool {
    true
}
fn default_content_interval() -> ConfigDuration {
    ConfigDuration::from_mins(15)
}
fn default_reconcile_interval() -> ConfigDuration {
    ConfigDuration::from_hours(24)
}

macro_rules! default_from_serde {
    ($($type:ty),*) => {$(
        impl Default for $type {
            fn default() -> Self {
                toml::from_str("").unwrap_or_else(|error| {
                    unreachable!("{} has a default for every field: {error}", stringify!($type))
                })
            }
        }
    )*};
}

default_from_serde!(
    IdentityConfig,
    DirectoryConfig,
    ReaderConfig,
    WriterConfig,
    CommitConfig,
    MaintenanceConfig,
    BackpressureConfig,
    WriterServeConfig,
    SourceIntervals
);

/// Operator-pinned service URLs share one transport boundary rule.
pub fn secure_url(value: &str) -> Option<url::Url> {
    let url = url::Url::parse(value).ok()?;
    let loopback = match url.host()? {
        url::Host::Ipv4(ip) => ip.is_loopback(),
        url::Host::Ipv6(ip) => ip.is_loopback(),
        url::Host::Domain(_) => false,
    };
    (url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && (url.scheme() == "https" || (url.scheme() == "http" && loopback)))
        .then_some(url)
}
