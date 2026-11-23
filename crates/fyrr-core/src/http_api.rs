//! Request and response bodies of the HTTP API that no other contract types:
//! the writer's document and namespace routes, the reader's namespace report
//! and list, and the error body. The routes' handlers use these types, and the
//! generated OpenAPI description (`schemas/openapi.json`) is built from them.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::attributes::AttributeSchema;
use crate::mcp::{
    AggregateInput, Contains, FetchInput, Filter, Fusion, RerankChoice, ReturnFields, SearchInput,
    SubQuery, TermMatch,
};
use crate::publication::NamespaceSettings;

/// How current the version is that a `query` or `fetch` answer is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Consistency {
    /// The version the reader holds; a write acknowledged a moment ago may not show yet.
    Cached,
    /// After a version check that started once the request arrived.
    Strong,
}

/// The body of `POST /v1/namespaces/{namespace}/query`: the arguments of the
/// `search` tool and seven members that are no tool arguments.
#[derive(Debug, Clone, PartialEq, JsonSchema)]
pub struct QueryRequest {
    #[schemars(flatten)]
    pub search: SearchInput,
    /// Defaults to `cached`.
    pub consistency: Option<Consistency>,
    /// What a hit returns besides its document fields.
    pub fields: Option<ReturnFields>,
    /// With `queries`: one ranking of all sub-queries instead of their round-robin merge.
    pub fusion: Option<Fusion>,
    /// Switches the configured reranker off, or names what it scores.
    pub rerank: Option<RerankChoice>,
    /// 1-16 searches of their own, each with its filter, ranking and limit, instead of `query`, `queries` and `vector`.
    pub sub_queries: Option<Vec<SubQuery>>,
    /// With `query`: `all` ranks only the documents that hold every term; default `any`.
    pub query_match: Option<TermMatch>,
    /// Conditions on full-text attributes, each also required of a hit, like `filter`. Not with a listing.
    #[schemars(length(min = 1, max = 16))]
    pub contains: Option<Vec<Contains>>,
}

/// The body of `POST /v1/namespaces/{namespace}/fetch`: the arguments of the
/// `fetch` tool and `consistency`, which is no tool argument.
#[derive(Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct FetchRequest {
    #[schemars(flatten)]
    pub fetch: FetchInput,
    /// Defaults to `cached`.
    pub consistency: Option<Consistency>,
}

/// The body of `POST /v1/namespaces/{namespace}/aggregate`: the aggregate
/// arguments and `consistency`.
#[derive(Debug, Clone, PartialEq, JsonSchema)]
pub struct AggregateRequest {
    #[schemars(flatten)]
    pub aggregate: AggregateInput,
    /// Defaults to `cached`.
    pub consistency: Option<Consistency>,
}

/// What `GET /v1/namespaces/{namespace}` answers: what the reader holds of the
/// namespace's published version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NamespaceReport {
    pub namespace: String,
    pub publication_id: String,
    /// The typed attributes of the version, name to type.
    pub schema: AttributeSchema,
    /// The namespace's settings, as its policy records them.
    pub settings: NamespaceSettings,
    /// Whether the namespace is frozen: writes are refused while it stays readable.
    pub read_only: bool,
    /// Approximate live rows; `null` for a version published before contract 4.1.
    pub rows: Option<u64>,
    /// Approximate bytes of the data files of those rows, without indexes; `null` as `rows`.
    pub logical_bytes: Option<u64>,
    /// When the namespace's first content publication was committed; `null` when that is not known.
    pub first_published_at: Option<DateTime<Utc>>,
    /// The data that an index of the version does not cover yet.
    pub unindexed: Unindexed,
    /// When the version was published.
    pub last_write_at: DateTime<Utc>,
}

/// `rows` and `logical_bytes` of the data that no index covers yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Unindexed {
    pub rows: u64,
    pub logical_bytes: u64,
}

/// What `GET /v1/namespaces` answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NamespaceList {
    /// Names in name order.
    pub namespaces: Vec<String>,
}

/// One row of `upserts` or `patches`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WriteRow {
    /// The row's key in the namespace.
    pub id: String,
    /// `usr:<id>` or `grp:<id>`; ignored by an open namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readers: Option<Vec<String>>,
    /// The vector of the row's text, of the namespace's dimension.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    /// The text under the key `text_attribute`, and a value for any declared attribute.
    #[serde(flatten)]
    pub values: Map<String, Value>,
}

/// The body of `POST /v1/namespaces/{namespace}/documents`.
// shortcut: `WriteRequest` and `WriteRow` mirror the writer's streaming
// `Payload`, and its test checks one direction only (what these types write,
// `Payload` parses); share the types if `Payload` stops needing its row limit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    /// Part of the settings of a namespace that does not exist yet, laid over the
    /// process defaults. For a namespace that is published, a request with
    /// `settings` alone and only `attributes` and `attribute_indexes` in them
    /// changes its schema: attributes are added and indexes set; another type
    /// or a removed attribute is `attribute_type_change_refused` or
    /// `attribute_removal_refused`, rows with it `schema_change_with_rows`, and a
    /// namespace with no publication yet `namespace_not_published`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<Map<String, Value>>,
    /// Rows that replace the documents with their ids.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upserts: Vec<WriteRow>,
    /// IDs to delete.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deletes: Vec<String>,
    /// Rows that write some attributes of a document that exists.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub patches: Vec<WriteRow>,
    /// From an id of the request to the filter that the stored document with
    /// that id must meet for the change to apply. Needs the `admin` role.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub conditions: BTreeMap<String, Filter>,
    /// Deletes every document that matches the filter, before the rest of the
    /// request is applied. Needs the `admin` role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_where: Option<Filter>,
    /// Patches every document that matches a filter, after `delete_where` and
    /// before the rest of the request. Needs the `admin` role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patch_where: Option<PatchWhere>,
}

/// The `patch_where` member of a write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchWhere {
    pub filter: Filter,
    /// A patch row without its `id`: attributes and `readers`, never a text.
    pub set: Map<String, Value>,
}

/// What a write answers: the changes that applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WriteResponse {
    pub upserted: u64,
    pub deleted: u64,
    /// Present when the request has patches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patched: Option<u64>,
    /// The patches and conditional changes that did not apply; present when the request has any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<u64>,
}

/// The body of `DELETE /v1/namespaces/{namespace}`: the name, repeated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteNamespaceRequest {
    pub namespace: String,
}

/// The body of `PATCH /v1/namespaces/{namespace}`: freezes the namespace or thaws it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchNamespaceRequest {
    /// `true` refuses writes and connector syncs; `false` allows them again.
    pub read_only: bool,
}

/// What a namespace switch answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PatchNamespaceResponse {
    pub namespace: String,
    pub read_only: bool,
    /// `false` when the namespace was already so.
    pub changed: bool,
}

/// What a namespace deletion answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DeleteNamespaceResponse {
    pub deleted: bool,
}

/// The body of an error answer of the reader and of the write route. The
/// namespace deletion answers a SCIM error instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ErrorBody {
    /// A code that [`docs/http-api.md`](../docs/http-api.md) lists.
    pub error: String,
}
