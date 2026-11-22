//! `POST /v1/namespaces/{namespace}/documents`: upserts, patches and deletes of one
//! request become one prepared envelope, which the committer publishes at once;
//! the answer is sent when the served head holds it or has refused it.
use crate::embedding::WriterEmbedding;
use crate::prepare::ContentChange;
use crate::sync_lease::CursorLease;
use crate::{Result, WriterError};
use arrow_array::ArrayRef;
use axum::{
    Json,
    body::Body,
    extract::Request,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures::StreamExt;
use fyrr_core::config::{
    ApiKeyRole, Config, PermissionsMode, SourceConfig, SourceIntervals, is_api_key_secret,
    is_namespace_name, prefix_allows,
};
use fyrr_core::connector::{
    API_SOURCE_ID, BatchBody, CONNECTOR_CONTRACT_VERSION, ChangeOp, ConnectorBatch, CursorStep,
    DocumentChange, DocumentMetadata, Section, SyncKind,
};
use fyrr_core::envelope::{BaseHead, DocumentIntent, DocumentPatch, EnvelopeDocument, envelope_id};
use fyrr_core::identity::ApiKeysArtifact;
use fyrr_core::mcp::Filter;
use fyrr_core::publication::{NamespaceSettings, PolicyArtifact};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, Semaphore, watch};

/// How long a request waits for its publication before the outcome is unknown.
const ANSWER_DEADLINE: Duration = Duration::from_secs(600);
/// How long the body may stay silent while it is received.
const BODY_IDLE: Duration = Duration::from_secs(30);
/// The most a row, or the settings of a request, may take of the body before it
/// is read as a tree: a row holds a text of 16 KiB and attributes of 4 KiB each.
pub(crate) const ROW_MAX_BYTES: usize = 1 << 20;
const SETTINGS_MAX_BYTES: usize = 64 << 10;
/// Requests the write API holds at once, waiting for their turn or their answer.
const IN_FLIGHT: usize = 8;
/// How often a waiting request looks at the published history again, and asks
/// the committer for a round when its envelope is not in it.
const RECHECK: Duration = Duration::from_millis(500);

/// The committer's side of the API: a request wakes a round, and each round's
/// end is counted. The namespaces whose requests wait for their publication
/// are the ones a round commits besides the configured one; each has a request
/// in flight, so they are never more than `IN_FLIGHT`, and a namespace with
/// nothing to publish is not looked at.
// shortcut: a restart forgets which namespaces had an envelope waiting, and an
// envelope whose request gave up waits for the next request of its namespace;
// list the envelopes of the namespaces at startup if that must not be.
#[derive(Debug)]
pub(crate) struct Commits {
    pub(crate) requested: Notify,
    pub(crate) done: watch::Sender<u64>,
    waiting: std::sync::Mutex<BTreeMap<String, usize>>,
}
impl Commits {
    pub(crate) fn new() -> Self {
        Self {
            requested: Notify::new(),
            done: watch::channel(0).0,
            waiting: std::sync::Mutex::default(),
        }
    }
    /// The namespaces with a request that waits for its publication.
    pub(crate) fn waiting(&self) -> Vec<String> {
        self.waiting
            .lock()
            .map(|waiting| waiting.keys().cloned().collect())
            .unwrap_or_default()
    }
    fn wait(&self, name: &str) -> Waiting<'_> {
        if let Ok(mut waiting) = self.waiting.lock() {
            *waiting.entry(name.to_owned()).or_default() += 1;
        }
        Waiting(self, name.to_owned())
    }
}
/// A request that waits for its publication, until it is answered or gives up.
struct Waiting<'a>(&'a Commits, String);
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if let Ok(mut waiting) = self.0.waiting.lock()
            && let Some(count) = waiting.get_mut(&self.1)
        {
            *count -= 1;
            if *count == 0 {
                waiting.remove(&self.1);
            }
        }
    }
}

/// The configuration of the namespace `name`: the process's, which holds the
/// defaults of a namespace that is created.
pub(crate) fn named(config: &Config, name: &str) -> Config {
    let mut config = config.clone();
    name.clone_into(&mut config.namespace.name);
    config
}

#[derive(Debug)]
pub(crate) struct WriteApi {
    pub(crate) commits: Arc<Commits>,
    /// One request receives its body and prepares its envelope at a time, so
    /// that one body is in memory and the source's sequence is single.
    slot: Arc<Mutex<()>>,
    /// Its own capacity: a request that waits for its answer for minutes does
    /// not take the listener's, which SCIM and administration share.
    capacity: Arc<Semaphore>,
}
impl WriteApi {
    pub(crate) fn new(commits: Arc<Commits>) -> Self {
        Self {
            commits,
            slot: Arc::new(Mutex::new(())),
            capacity: Arc::new(Semaphore::new(IN_FLIGHT)),
        }
    }
}

/// How long a schema change waits for the lease: tries, and the pause between them.
const LEASE_WAIT_TRIES: u32 = 50;
const LEASE_WAIT: Duration = Duration::from_millis(100);

struct ApiError(u16, &'static str);
impl ApiError {
    fn response(self) -> Response {
        let mut reply = (
            StatusCode::from_u16(self.0).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(json!({ "error": self.1 })),
        )
            .into_response();
        if self.0 == 503 {
            reply
                .headers_mut()
                .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
        }
        reply
    }
}
impl From<WriterError> for ApiError {
    fn from(error: WriterError) -> Self {
        match error {
            WriterError::EnvelopeBoundsInvalid => Self(413, "request_too_large"),
            WriterError::NamespaceSettingsFixed => Self(409, "namespace_settings_fixed"),
            WriterError::AttributeTypeChange => Self(409, "attribute_type_change_refused"),
            WriterError::AttributeRemoval => Self(409, "attribute_removal_refused"),
            WriterError::NamespaceChanged => Self(409, "namespace_changed"),
            WriterError::CommitterBusy => Self(503, "busy"),
            WriterError::PublicationInvalid => Self(400, "settings_invalid"),
            WriterError::DocumentInvalid => Self(400, "document_invalid"),
            WriterError::SyncBusy => Self(503, "busy"),
            _ => Self(503, "unavailable"),
        }
    }
}
impl From<fyrr_reader::storage::StorageError> for ApiError {
    fn from(_: fyrr_reader::storage::StorageError) -> Self {
        Self(503, "unavailable")
    }
}

thread_local! {
    /// Rows the body may still hold while it is parsed, and whether it held more.
    static ROWS_LEFT: Cell<u64> = const { Cell::new(0) };
    static ROWS_EXCEEDED: Cell<bool> = const { Cell::new(false) };
}
/// A list that stops being read at the row limit, so an oversized list is
/// never built.
struct Rows<T>(Vec<T>);
impl<T> Default for Rows<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Rows<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
            type Value = Rows<T>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a list")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut rows = Vec::new();
                while let Some(row) = seq.next_element()? {
                    let left = ROWS_LEFT.get();
                    if left == 0 {
                        ROWS_EXCEEDED.set(true);
                        return Err(serde::de::Error::custom("too many rows"));
                    }
                    ROWS_LEFT.set(left - 1);
                    rows.push(row);
                }
                Ok(Rows(rows))
            }
        }
        deserializer.deserialize_seq(Visitor(std::marker::PhantomData))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    /// Settings of a new namespace, laid over the process defaults.
    #[serde(default)]
    settings: Option<Box<serde_json::value::RawValue>>,
    #[serde(default)]
    upserts: Rows<Box<serde_json::value::RawValue>>,
    #[serde(default)]
    deletes: Rows<String>,
    /// Rows that write some attributes of a document that exists.
    #[serde(default)]
    patches: Rows<Box<serde_json::value::RawValue>>,
    /// A filter by the `id` of an upsert, a patch or a delete of the request,
    /// which the stored document with that id must meet for the change to apply.
    #[serde(default)]
    conditions: BTreeMap<String, Value>,
}

/// The conditions of a request: as given, and as bound to the row of their id.
struct Conditions {
    given: BTreeMap<String, Value>,
    bound: BTreeMap<String, Filter>,
}
impl Conditions {
    /// Binds the condition of the document `id`, if it has one, to its incoming
    /// `row` (a delete has none): a comparison's value `{"row": "<attribute>"}`
    /// is what the row holds of that attribute. The filter is checked as the
    /// reader checks one.
    fn bind(
        &mut self,
        id: &str,
        row: Option<&Map<String, Value>>,
        attributes: &fyrr_core::attributes::AttributeSchema,
    ) -> Result<(), ApiError> {
        let Some(mut condition) = self.given.remove(id) else {
            return Ok(());
        };
        let invalid = || ApiError(400, "condition_invalid");
        bind_row(&mut condition, row, attributes).ok_or_else(invalid)?;
        let filter: Filter = serde_json::from_value(condition).map_err(|_| invalid())?;
        fyrr_reader::filter_sql(&filter, attributes).ok_or_else(invalid)?;
        self.bound.insert(id.to_owned(), filter);
        Ok(())
    }
}

/// Puts the value of the incoming row where a comparison names its attribute.
/// `None` for a node that is no object, or a reference to an attribute the
/// namespace does not declare, or to a row there is none of.
fn bind_row(
    node: &mut Value,
    row: Option<&Map<String, Value>>,
    attributes: &fyrr_core::attributes::AttributeSchema,
) -> Option<()> {
    let node = node.as_object_mut()?;
    for key in ["and", "or"] {
        if let Some(nodes) = node.get_mut(key) {
            return nodes
                .as_array_mut()?
                .iter_mut()
                .try_for_each(|node| bind_row(node, row, attributes));
        }
    }
    if let Some(inner) = node.get_mut("not") {
        return bind_row(inner, row, attributes);
    }
    if let Some(value) = node.get_mut("value")
        && let Some(reference) = value.as_object()
    {
        let name = reference
            .get("row")
            .and_then(Value::as_str)
            .filter(|name| reference.len() == 1 && attributes.contains_key(*name))?;
        *value = row?.get(name).cloned().unwrap_or(Value::Null);
    }
    Some(())
}

/// What each patched document is given, by its id.
type Patches = Vec<(String, DocumentPatch)>;

/// A row that passed its checks.
struct Upsert {
    id: String,
    readers: Vec<String>,
    text: String,
    vector: Option<Vec<f32>>,
}

/// Serves the route.
pub(crate) async fn handle(api: &WriteApi, config: &Config, request: Request) -> Response {
    match accept(api, config, request).await {
        Ok(reply) => reply,
        Err(error) => error.response(),
    }
}

async fn accept(api: &WriteApi, config: &Config, request: Request) -> Result<Response, ApiError> {
    let namespace = request
        .uri()
        .path()
        .strip_prefix("/v1/namespaces/")
        .and_then(|rest| rest.strip_suffix("/documents"))
        .filter(|name| is_namespace_name(name));
    let Some(namespace) = namespace.filter(|_| config.namespace.write_api.is_some()) else {
        return Err(ApiError(404, "not_found"));
    };
    if request.method() != axum::http::Method::POST {
        return Err(ApiError(405, "method_not_allowed"));
    }
    // The key is checked as the request arrives; a key revoked afterwards does
    // not cancel an envelope that was already accepted.
    let admin = authorize(config, namespace, request.headers()).await?;
    let permit = api
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(503, "busy"))?;
    let config = &named(config, namespace);
    let headers = request.headers();
    if !headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|t| t.trim() == "application/json")
        })
        || headers.contains_key(header::CONTENT_ENCODING)
    {
        return Err(ApiError(415, "unsupported_media_type"));
    }
    let max = config.writer.envelope_max_bytes.as_u64();
    let length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if length.is_some_and(|length| length > max) {
        return Err(ApiError(413, "body_too_large"));
    }
    let slot = api.slot.clone().lock_owned().await;
    let bytes = read_body(request.into_body(), length, max).await?;
    let rows = config.writer.envelope_max_rows;
    // Parsed off the runtime; the body's bytes are gone once its rows exist.
    // The slot goes with the parse and comes back with it: a caller that goes
    // away does not let the next body in while this one is still in memory.
    let (parsed, exceeded, slot) = tokio::task::spawn_blocking(move || {
        ROWS_LEFT.set(rows);
        ROWS_EXCEEDED.set(false);
        let parsed = serde_json::from_slice::<Payload>(&bytes);
        (parsed, ROWS_EXCEEDED.get(), slot)
    })
    .await
    .map_err(|_| ApiError(503, "unavailable"))?;
    let body = match parsed {
        Ok(body) => body,
        Err(_) if exceeded => return Err(ApiError(413, "too_many_rows")),
        Err(_) => return Err(ApiError(400, "body_invalid")),
    };
    // Whether a condition holds tells about a document the key may not read.
    if !admin && !body.conditions.is_empty() {
        return Err(ApiError(403, "forbidden"));
    }
    // The work outlives a caller that goes away: an envelope that was begun is
    // finished, and the capacity is held until then.
    let (config, commits) = (config.clone(), api.commits.clone());
    let done = tokio::spawn(async move {
        let _permit = permit;
        execute(&config, &commits, body, slot).await
    })
    .await
    .map_err(|_| ApiError(503, "unavailable"))??;
    Ok(Json(done).into_response())
}

/// Checks the key for writing in `namespace`; whether it also holds `admin`.
async fn authorize(
    config: &Config,
    namespace: &str,
    headers: &HeaderMap,
) -> Result<bool, ApiError> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|token| is_api_key_secret(token))
        .ok_or(ApiError(401, "unauthorized"))?;
    let hash = fyrr_reader::storage::api_key_deployment_hash(config, token)?;
    let (store, root) = fyrr_reader::storage::namespace_store(config)?;
    // Read at each request: a revoked key stops at the next one. Without the
    // published keys no key is known.
    let keys: ApiKeysArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &fyrr_reader::storage::deployment_root(&root).join(fyrr_reader::storage::API_KEYS_FILE),
    )
    .await?;
    let grant = keys.grant(&hash).ok_or(ApiError(401, "unauthorized"))?;
    if grant.roles.contains(&ApiKeyRole::Write)
        && prefix_allows(grant.namespace_prefix.as_deref(), namespace)
    {
        Ok(grant.roles.contains(&ApiKeyRole::Admin))
    } else {
        Err(ApiError(403, "forbidden"))
    }
}

/// The body, counted as it is received: past `max` it is refused at once.
async fn read_body(body: Body, length: Option<u64>, max: u64) -> Result<Vec<u8>, ApiError> {
    let mut bytes = Vec::with_capacity(crate::narrow_usize(length.unwrap_or(0).min(max)));
    let mut stream = body.into_data_stream();
    while let Some(chunk) = tokio::time::timeout(BODY_IDLE, stream.next())
        .await
        .map_err(|_| ApiError(408, "request_timeout"))?
    {
        let chunk = chunk.map_err(|_| ApiError(400, "body_invalid"))?;
        if bytes.len() as u64 + chunk.len() as u64 > max {
            return Err(ApiError(413, "body_too_large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn merge(into: &mut Value, patch: &Value) {
    match (into, patch) {
        (Value::Object(into), Value::Object(patch)) => {
            for (key, value) in patch {
                merge(into.entry(key.clone()).or_insert(Value::Null), value);
            }
        }
        (into, patch) => *into = patch.clone(),
    }
}

/// `base` with the request's settings laid over it.
fn laid_over(
    base: &NamespaceSettings,
    request: Option<&Value>,
) -> Result<NamespaceSettings, WriterError> {
    let invalid = WriterError::PublicationInvalid;
    let mut value = serde_json::to_value(base).map_err(|_| invalid)?;
    if let Some(request) = request {
        merge(&mut value, request);
    }
    let mut settings: NamespaceSettings = serde_json::from_value(value).map_err(|_| invalid)?;
    // A request declares the model and dimension of its vectors. The profile
    // that binds them is derived unless the request sends it: a wrong one is
    // refused by the settings' validation, or differs from an existing namespace's.
    if request.is_none_or(|request| request.pointer("/embedding/fingerprint").is_none()) {
        let embedding = &mut settings.embedding;
        embedding.fingerprint = embedding
            .settings
            .fingerprint(&embedding.model, embedding.dimensions);
    }
    // A request cannot make a namespace that the API does not write.
    if settings.write_api.is_none() {
        return Err(invalid);
    }
    // An option that is the default is not an entry: a schema has one spelling.
    settings
        .attribute_indexes
        .retain(|_, options| *options != fyrr_core::attributes::AttributeIndex::default());
    Ok(settings)
}

/// The settings of an existing namespace with the request's `settings` laid
/// over them. They may add attributes and set the indexes of attributes, and
/// nothing else: a removed attribute, a changed type or any other setting is
/// refused with its own error.
fn changed_schema(
    current: &NamespaceSettings,
    request: Option<&Value>,
) -> Result<NamespaceSettings, WriterError> {
    // `null` is how a request says that an attribute goes.
    if request
        .and_then(|request| request.get("attributes")?.as_object())
        .is_some_and(|attributes| attributes.values().any(Value::is_null))
    {
        return Err(WriterError::AttributeRemoval);
    }
    let next = laid_over(current, request)?;
    crate::commit::check_schema_change(current, &next)?;
    if next != *current && (next.validate().is_err() || next.attributes.contains_key("id")) {
        return Err(WriterError::PublicationInvalid);
    }
    Ok(next)
}

/// The attributes in the order of the table's columns, which is the order they
/// were added in. A fragment names its columns by position, so the columns of
/// the rows follow it, whatever the order of the names. A table that does not
/// exist yet is created with them in name order.
async fn attribute_order(
    config: &Config,
    head: Option<&fyrr_core::publication::ServingHead>,
) -> Result<Vec<String>, ApiError> {
    let attributes = &config.namespace.attributes;
    let mut order = Vec::new();
    if let Some(head) = head.filter(|head| head.content.is_some() && !attributes.is_empty()) {
        let dataset = fyrr_reader::storage::pinned(config, head).await?;
        order.extend(
            dataset
                .schema()
                .fields
                .iter()
                .filter(|field| attributes.contains_key(&field.name))
                .map(|field| field.name.clone()),
        );
    }
    let missing: Vec<_> = attributes
        .keys()
        .filter(|name| !order.contains(name))
        .cloned()
        .collect();
    order.extend(missing);
    Ok(order)
}

fn api_source() -> SourceConfig {
    SourceConfig {
        id: API_SOURCE_ID.into(),
        provider: "api".into(),
        permissions: PermissionsMode::Sync,
        readers: Vec::new(),
        intervals: SourceIntervals::default(),
        document_max_bytes: None,
        row_warning: None,
        row_error: None,
        retention_days: None,
        scope: Map::new(),
        settings: Map::new(),
    }
}

/// The vector of a row, of the namespace's dimension.
// A vector is stored as float32: the numbers of a JSON body narrow to it.
#[allow(clippy::cast_possible_truncation)]
fn vector_of(value: Value, config: &Config) -> Result<Vec<f32>, ApiError> {
    match value {
        Value::Array(list) => list
            .iter()
            .map(|n| n.as_f64().map(|n| n as f32))
            .collect::<Option<Vec<_>>>()
            .filter(|vector| {
                fyrr_reader::embedding::validate(vector, config.embedding.dimensions).is_ok()
            })
            .ok_or(ApiError(400, "vector_invalid")),
        _ => Err(ApiError(400, "vector_invalid")),
    }
}

/// The readers of a row: a list of strings.
fn reader_list(value: Value) -> Result<Vec<String>, ApiError> {
    match value {
        Value::Array(list) => list
            .into_iter()
            .map(|v| match v {
                Value::String(reader) => Some(reader),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(ApiError(400, "readers_invalid")),
        _ => Err(ApiError(400, "readers_invalid")),
    }
}

/// The checked patches of the request: what each row writes, by the id of its
/// document. A patch keeps the stored vector. It may change the text only
/// with the vector of that text, which it then replaces, so that the two never
/// disagree. The ids join the ones `ids` holds.
fn checked_patches(
    body: &mut Payload,
    config: &Config,
    text_key: &str,
    ids: &mut std::collections::HashSet<String>,
    conditions: &mut Conditions,
) -> Result<Patches, ApiError> {
    let open = config
        .namespace
        .write_api
        .as_ref()
        .is_some_and(|api| api.open);
    let mut patches = Vec::with_capacity(body.patches.0.len());
    for raw in std::mem::take(&mut body.patches.0) {
        if raw.get().len() > ROW_MAX_BYTES {
            return Err(ApiError(413, "row_too_large"));
        }
        let Ok(mut row) = serde_json::from_str::<Map<String, Value>>(raw.get()) else {
            return Err(ApiError(400, "body_invalid"));
        };
        drop(raw);
        let Some(Value::String(id)) = row.remove("id") else {
            return Err(ApiError(400, "id_missing"));
        };
        if !ids.insert(id.clone()) {
            return Err(ApiError(400, "duplicate_id"));
        }
        // An open namespace ignores readers, as an upsert does.
        let readers = row
            .remove("readers")
            .map(reader_list)
            .transpose()?
            .filter(|_| !open)
            .map(|raw| -> Result<_, ApiError> {
                // The check an upsert's readers get in its change.
                fyrr_core::connector::check_readers("readers", &raw)
                    .map_err(|_| ApiError(400, "document_invalid"))?;
                Ok(crate::apply::permission_readers(
                    config,
                    API_SOURCE_ID,
                    &raw,
                    &[],
                )?)
            })
            .transpose()?;
        let vector = row
            .remove("vector")
            .map(|value| vector_of(value, config))
            .transpose()?;
        let text = match row.remove(text_key) {
            None => None,
            Some(Value::String(text)) if !text.is_empty() => Some(text),
            Some(_) => return Err(ApiError(400, "text_missing")),
        };
        if text.as_ref().is_some_and(|t| t.contains('\0')) {
            return Err(ApiError(400, "document_invalid"));
        }
        if text
            .as_ref()
            .is_some_and(|t| t.len() > crate::chunk::MAX_BYTES)
        {
            return Err(ApiError(413, "text_too_large"));
        }
        // The vector goes with the text it stands for; a text without one would
        // need embedding, which a patch does not do.
        match (&text, &vector) {
            (None, Some(_)) => return Err(ApiError(400, "vector_not_patchable")),
            (Some(_), None) if config.embedding.endpoint.is_empty() => {
                return Err(ApiError(400, "embedding_not_configured"));
            }
            (Some(_), None) => return Err(ApiError(400, "text_not_patchable")),
            _ => {}
        }
        let mut schema = config.namespace.attributes.clone();
        for (name, value) in &row {
            if !schema.contains_key(name) {
                return Err(ApiError(400, "attribute_undeclared"));
            }
            fyrr_core::attributes::resolve(&mut schema, name, value)
                .map_err(|error| ApiError(400, error.code()))?;
        }
        conditions.bind(&id, Some(&row), &config.namespace.attributes)?;
        patches.push((
            id,
            DocumentPatch {
                readers,
                text,
                vector,
                attributes: row,
            },
        ));
    }
    Ok(patches)
}

/// How many of the documents `ids` the committer applied a change to: their
/// rows of `source` carry the revision of the envelope. A patch or a condition
/// the stored document did not meet leaves them as they were; a delete leaves
/// the marker of the document in the source of its state.
async fn committed(
    dataset: &lance::Dataset,
    source: &str,
    ids: &[String],
    revision: &str,
) -> Result<u64, ApiError> {
    let mut found = 0;
    for page in ids.chunks(256) {
        found += dataset
            .count_rows(Some(format!(
                "{} AND source_revision = {}",
                crate::apply::rows_filter(source, page),
                fyrr_reader::storage::sql(revision)
            )))
            .await
            .map_err(|_| ApiError(503, "unavailable"))? as u64;
    }
    Ok(found)
}

/// The checked rows of the request: its upserts, their attribute columns, its
/// patches and the filters of its conditions by the id they test.
type Checked = (
    Vec<Upsert>,
    Vec<(String, ArrayRef)>,
    Patches,
    BTreeMap<String, Filter>,
);

/// The checked rows of the request, against the schema, the text key and the
/// vector dimension the namespace was created with. The rows are taken from
/// `body`, not copied.
fn checked(
    body: &mut Payload,
    config: &Config,
    text_key: &str,
    order: Vec<String>,
) -> Result<Checked, ApiError> {
    let attributes = &config.namespace.attributes;
    let mut conditions = Conditions {
        given: std::mem::take(&mut body.conditions),
        bound: BTreeMap::new(),
    };
    let mut ids = std::collections::HashSet::new();
    for id in &body.deletes.0 {
        if !ids.insert(id.clone()) {
            return Err(ApiError(400, "duplicate_id"));
        }
    }
    let mut rows = Vec::with_capacity(body.upserts.0.len());
    let mut columns = crate::prepare::AttributeColumns::new(attributes, order);
    for raw in std::mem::take(&mut body.upserts.0) {
        // A row is read one at a time, as a tree only when it is of a size a row can be.
        if raw.get().len() > ROW_MAX_BYTES {
            return Err(ApiError(413, "row_too_large"));
        }
        let Ok(mut row) = serde_json::from_str::<Map<String, Value>>(raw.get()) else {
            return Err(ApiError(400, "body_invalid"));
        };
        drop(raw);
        let Some(Value::String(id)) = row.remove("id") else {
            return Err(ApiError(400, "id_missing"));
        };
        if !ids.insert(id.clone()) {
            return Err(ApiError(400, "duplicate_id"));
        }
        let readers = row
            .remove("readers")
            .map(reader_list)
            .transpose()?
            .unwrap_or_default();
        let text = match row.remove(text_key) {
            Some(Value::String(text)) if !text.is_empty() => text,
            _ => return Err(ApiError(400, "text_missing")),
        };
        if text.len() > crate::chunk::MAX_BYTES {
            // A row is one record: no chunking.
            return Err(ApiError(413, "text_too_large"));
        }
        // The row's own vector, or the provider's embedding of its text.
        let vector = row
            .remove("vector")
            .map(|value| vector_of(value, config))
            .transpose()?;
        if vector.is_none() && config.embedding.endpoint.is_empty() {
            return Err(ApiError(400, "embedding_not_configured"));
        }
        if row.keys().any(|name| !attributes.contains_key(name)) {
            return Err(ApiError(400, "attribute_undeclared"));
        }
        conditions.bind(&id, Some(&row), attributes)?;
        columns
            .push(&row)
            .map_err(|error| ApiError(400, error.code()))?;
        rows.push(Upsert {
            id,
            readers,
            text,
            vector,
        });
    }
    let patches = checked_patches(body, config, text_key, &mut ids, &mut conditions)?;
    // A delete has no row to refer to, and a condition names an id of the request.
    for id in &body.deletes.0 {
        conditions.bind(id, None, attributes)?;
    }
    if !conditions.given.is_empty() {
        return Err(ApiError(400, "condition_invalid"));
    }
    Ok((rows, columns.finish(), patches, conditions.bound))
}

/// The request's documents as the changes of one envelope, checked as the
/// connector path checks what it quarantines: a request is refused whole.
fn changes(
    config: &Config,
    rows: Vec<Upsert>,
    deletes: Vec<String>,
    patches: Patches,
    mut conditions: BTreeMap<String, Filter>,
) -> Result<Vec<ContentChange>, ApiError> {
    let open = config
        .namespace
        .write_api
        .as_ref()
        .is_some_and(|api| api.open);
    let change = |op, root_id: String| DocumentChange {
        op,
        root_id,
        // The revision is set with the sequence of the envelope; until then the
        // longest it can be, so that the bounds checked now are those of the envelope.
        source_revision: (op == ChangeOp::Upsert).then(|| format!("rev-{}", u64::MAX)),
        revision_order: Some(i64::MAX),
        expected_prior_revision: None,
        permission_revision: None,
        readers: Vec::new(),
        containers: Vec::new(),
        metadata: None,
        attributes: Vec::new(),
        sections: Vec::new(),
    };
    let source = api_source();
    let mut changes = Vec::with_capacity(rows.len() + deletes.len());
    for row in rows {
        let mut change = change(ChangeOp::Upsert, row.id.clone());
        if !open {
            change.readers = row.readers;
        }
        change.metadata = Some(DocumentMetadata {
            source_uri: row.id,
            source_type: "api".into(),
            title: None,
            author: None,
            team: None,
            project: None,
            status: None,
            labels: Vec::new(),
            created_at: None,
            updated_at: None,
            connector_map: None,
        });
        change.sections = vec![Section {
            text: row.text,
            location: None,
            readers: None,
            vector: row.vector,
        }];
        changes.push(change);
    }
    changes.extend(deletes.into_iter().map(|id| change(ChangeOp::Delete, id)));
    for change in &changes {
        match crate::prepare::quarantine_reason(config, &source, change) {
            None => {}
            Some("document_oversized") => return Err(ApiError(413, "document_too_large")),
            Some(_) => return Err(ApiError(400, "document_invalid")),
        }
    }
    let mut changes: Vec<_> = changes
        .into_iter()
        .map(|change| {
            let condition = conditions.remove(&change.root_id);
            ContentChange::Write(Box::new(change), condition)
        })
        .collect();
    changes.extend(patches.into_iter().map(|(root_id, patch)| {
        let condition = conditions.remove(&root_id);
        ContentChange::Present(EnvelopeDocument {
            root_id,
            intent: DocumentIntent::Patch,
            // Set with the sequence of the envelope, as an upsert's are.
            source_revision: Some(format!("rev-{}", u64::MAX)),
            revision_order: Some(i64::MAX),
            expected_prior_revision: None,
            permission_revision: None,
            records: 0,
            patch: Some(Box::new(patch)),
            condition,
        })
    }));
    crate::prepare::check_atomic(config, &changes)?;
    Ok(changes)
}

/// The namespace is not to be created over what a deleted one left: its
/// envelopes would be published into the new one.
async fn leftovers(
    store: &dyn object_store::ObjectStore,
    root: &object_store::path::Path,
) -> Result<(), ApiError> {
    match store
        .list(Some(&root.clone().join("envelopes")))
        .next()
        .await
    {
        None => Ok(()),
        Some(Ok(_)) => Err(ApiError(409, "namespace_not_empty")),
        Some(Err(_)) => Err(ApiError(503, "unavailable")),
    }
}

/// Every success answer follows what was written being durable.
async fn durable(config: &Config) -> Result<(), ApiError> {
    fyrr_reader::storage::observed_durability(config)?
        .persist()
        .await
        .map_err(|_| ApiError(503, "unavailable"))
}

/// Creates or confirms the namespace, prepares the request's envelope and
/// waits for the head to serve it. Returns the answer: the rows upserted and
/// deleted, and with patches the ones patched and the ones skipped because
/// their document is not there.
async fn execute(
    config: &Config,
    commits: &Commits,
    mut body: Payload,
    slot: OwnedMutexGuard<()>,
) -> Result<Value, ApiError> {
    let requested = match &body.settings {
        None => None,
        Some(raw) if raw.get().len() > SETTINGS_MAX_BYTES => {
            return Err(ApiError(413, "settings_too_large"));
        }
        Some(raw) => Some(
            serde_json::from_str::<Value>(raw.get())
                .map_err(|_| ApiError(400, "settings_invalid"))?,
        ),
    };
    let (store, root) = fyrr_reader::storage::namespace_store(config)?;
    let existing = match fyrr_reader::storage::optional_head(config).await? {
        Some(head) => {
            let policy: PolicyArtifact =
                fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
            Some((head, policy))
        }
        None => None,
    };
    // The settings the request is checked against: the namespace's with the
    // schema the request asks for, or those the request would create it with.
    // Nothing is created or changed by a refused request.
    let settings = match &existing {
        Some((_, policy)) => {
            if policy.settings.write_api.is_none() {
                return Err(ApiError(409, "namespace_not_writable"));
            }
            changed_schema(&policy.settings, requested.as_ref())?
        }
        None => {
            let settings = laid_over(
                &crate::commit::namespace_settings(config),
                requested.as_ref(),
            )?;
            // The settings of a new namespace are held to the bounds of a
            // configuration before anything is built from them; `id` is the key
            // of a row in the request.
            if settings.validate().is_err()
                || settings.attributes.contains_key("id")
                || settings
                    .write_api
                    .as_ref()
                    .is_some_and(|api| api.text_attribute == "id")
            {
                return Err(ApiError(400, "settings_invalid"));
            }
            settings
        }
    };
    if settings.write_api.is_none() {
        return Err(ApiError(409, "namespace_not_writable"));
    }
    if let Some((head, policy)) = &existing
        && settings != policy.settings
    {
        // A schema change carries settings only: the rows of a request that is
        // refused or lost cannot leave a changed schema behind.
        if !(body.upserts.0.is_empty()
            && body.patches.0.is_empty()
            && body.deletes.0.is_empty()
            && body.conditions.is_empty())
        {
            return Err(ApiError(400, "schema_change_with_rows"));
        }
        // The change is made again under the lease, to the settings it finds
        // there. The committer holds the lease while it publishes, and a request
        // it answered may not have released it yet: wait for it a moment.
        let mut waited = 0;
        loop {
            let changed = crate::commit::change_schema(config, |held, current| {
                if held.incarnation != head.incarnation {
                    return Err(WriterError::NamespaceChanged);
                }
                changed_schema(current, requested.as_ref())
            })
            .await;
            match changed {
                Err(WriterError::CommitterBusy) if waited < LEASE_WAIT_TRIES => {
                    waited += 1;
                    tokio::time::sleep(LEASE_WAIT).await;
                }
                other => break other?,
            }
        }
        return Ok(json!({"upserted": 0, "deleted": 0}));
    }
    let config = config
        .with_settings(&settings)
        .ok_or(ApiError(503, "unavailable"))?;
    let text_key = config
        .namespace
        .write_api
        .as_ref()
        .map_or("text", |api| api.text_attribute.as_str());
    let order = attribute_order(&config, existing.as_ref().map(|(head, _)| head)).await?;
    let (rows, attributes, patches, conditions) = checked(&mut body, &config, text_key, order)?;
    let (upserted, deleted) = (rows.len() as u64, body.deletes.0.len() as u64);
    if upserted + deleted + patches.len() as u64 == 0 && requested.is_none() {
        return Err(ApiError(400, "empty_request"));
    }
    // Every patch is in the envelope: the committer applies it, in the order
    // of publication, to a document that exists then, and skips it otherwise.
    let patch_ids: Vec<_> = patches.iter().map(|(id, _)| id.clone()).collect();
    // So is every change with a condition, which the committer tests against the
    // document as it is then.
    let ids_with_condition = |ids: Vec<&String>| -> Vec<String> {
        ids.into_iter()
            .filter(|id| conditions.contains_key(*id))
            .cloned()
            .collect()
    };
    let guarded_upserts = ids_with_condition(rows.iter().map(|row| &row.id).collect());
    let guarded_deletes = ids_with_condition(body.deletes.0.iter().collect());
    let guarded = patch_ids.len() + guarded_upserts.len() + guarded_deletes.len();
    let changes = changes(&config, rows, body.deletes.0, patches, conditions)?;
    let mut answer = json!({"upserted": upserted, "deleted": deleted});
    let (head, served) = match existing {
        Some(existing) => existing,
        None => {
            leftovers(store.as_ref(), &root).await?;
            crate::commit::create_with(&config, settings.clone()).await?;
            let head = fyrr_reader::storage::optional_head(&config)
                .await?
                .ok_or(ApiError(503, "unavailable"))?;
            // The policy that is served decides, also when another creation won.
            let policy: PolicyArtifact =
                fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
            (head, policy)
        }
    };
    if served.settings.write_api.is_none() {
        return Err(ApiError(409, "namespace_not_writable"));
    }
    if served.settings != settings {
        return Err(ApiError(409, "namespace_settings_fixed"));
    }
    // Patches and deletes with a condition alone, with no document published
    // and no envelope waiting that could create one, change nothing: there is
    // nothing to publish.
    let only_skipped = guarded > 0 && patch_ids.len() + guarded_deletes.len() == changes.len();
    if changes.is_empty()
        || (only_skipped
            && head.content.is_none()
            && crate::commit::pending(&config, &head).await?.is_empty())
    {
        durable(&config).await?;
        if only_skipped {
            answer["upserted"] = 0.into();
            answer["deleted"] = 0.into();
            if !patch_ids.is_empty() {
                answer["patched"] = 0.into();
            }
            answer["skipped"] = guarded.into();
        }
        return Ok(answer);
    }
    let mut done = commits.done.subscribe();
    let (id, revision) = prepare(&config, &head, changes, attributes, slot).await?;
    let _waiting = commits.wait(&config.namespace.name);
    commits.requested.notify_one();
    let deadline = tokio::time::Instant::now() + ANSWER_DEADLINE;
    loop {
        // The published history decides, the refusal first. A refusal is
        // published only with the head that carries it.
        let current = fyrr_reader::storage::optional_head(&config).await;
        if let Ok(Some(current)) = current
            && let Ok((applied, rejected)) = crate::commit::published_ids(&config, &current).await
        {
            if rejected.contains(&id) {
                return Err(ApiError(409, "rejected"));
            }
            if applied.contains(&id) {
                durable(&config).await?;
                if guarded > 0 {
                    // What the committer did, counted in what it published.
                    let dataset = fyrr_reader::storage::pinned(&config, &current).await?;
                    let patched = committed(&dataset, API_SOURCE_ID, &patch_ids, &revision).await?;
                    let upserts =
                        committed(&dataset, API_SOURCE_ID, &guarded_upserts, &revision).await?;
                    let state = crate::apply::state_source(API_SOURCE_ID);
                    let deletes = committed(&dataset, &state, &guarded_deletes, &revision).await?;
                    answer["upserted"] = (upserted - guarded_upserts.len() as u64 + upserts).into();
                    answer["deleted"] = (deleted - guarded_deletes.len() as u64 + deletes).into();
                    if !patch_ids.is_empty() {
                        answer["patched"] = patched.into();
                    }
                    answer["skipped"] = (guarded as u64 - patched - upserts - deletes).into();
                }
                return Ok(answer);
            }
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            // The envelope is durable: the committer may still publish it.
            return Err(ApiError(503, "outcome_unknown"));
        }
        commits.requested.notify_one();
        let _ = tokio::time::timeout(RECHECK.min(deadline - now), done.changed()).await;
    }
}

/// Writes the request's envelope under the source's cursor lease and returns
/// its ID and the revision of its documents. The slot is released when the
/// envelope is durable.
async fn prepare(
    config: &Config,
    head: &fyrr_core::publication::ServingHead,
    changes: Vec<ContentChange>,
    attributes: Vec<(String, ArrayRef)>,
    slot: OwnedMutexGuard<()>,
) -> Result<(String, String), ApiError> {
    let lease = CursorLease::acquire(config, API_SOURCE_ID, SyncKind::Content).await?;
    let renewal = lease.keepalive();
    let result = write_envelope(config, head, &lease, changes, attributes).await;
    let stopped = renewal.stop().await;
    let released = lease.release().await;
    drop(slot);
    let written = result?;
    stopped?;
    released?;
    Ok(written)
}

async fn write_envelope(
    config: &Config,
    head: &fyrr_core::publication::ServingHead,
    lease: &CursorLease,
    mut changes: Vec<ContentChange>,
    attributes: Vec<(String, ArrayRef)>,
) -> Result<(String, String), ApiError> {
    let (store, root) = fyrr_reader::storage::namespace_store(config)?;
    let mut state = lease.state().await?;
    // The namespace the request was checked against is the one it is written
    // into: another may have taken its place since.
    if fyrr_reader::storage::optional_head(config)
        .await?
        .is_none_or(|current| current.incarnation != head.incarnation)
    {
        return Err(ApiError(409, "namespace_changed"));
    }
    let (generation, cursor) = (state.rejection_generation, state.cursor.clone());
    let id_of = |sequence: u64| {
        envelope_id(
            &config.namespace.name,
            API_SOURCE_ID,
            SyncKind::Content,
            sequence,
            generation,
            cursor.as_deref(),
        )
    };
    // A crash between an envelope and the cursor that counts it leaves the
    // envelope named by the next sequence: the cursor catches up first.
    let mut sequence = state.sequence;
    loop {
        let next = sequence
            .checked_add(1)
            .ok_or(WriterError::CursorSequenceOverflow)?;
        let path = root
            .clone()
            .join("envelopes")
            .join(format!("{}.json", id_of(next)));
        match object_store::ObjectStoreExt::head(store.as_ref(), &path).await {
            Ok(_) => sequence = next,
            Err(object_store::Error::NotFound { .. }) => break,
            Err(_) => return Err(ApiError(503, "unavailable")),
        }
    }
    // A restore moves the boundary that an envelope of the source must have been
    // prepared after, as it does for a connector's cursor.
    let boundary = crate::commit::rejection_markers(config, head)
        .await?
        .iter()
        .filter(|m| m.source_id == API_SOURCE_ID && m.kind == SyncKind::Content)
        .map(|m| m.restore_sequence)
        .max()
        .unwrap_or(0);
    if sequence != state.sequence || boundary > state.restore_sequence {
        state.sequence = sequence;
        state.restore_sequence = state.restore_sequence.max(boundary);
        lease.write(state.clone()).await?;
    }
    let sequence = sequence
        .checked_add(1)
        .ok_or(WriterError::CursorSequenceOverflow)?;
    let id = id_of(sequence);
    // Every revision of the source is newer than the one before it.
    let revision = i64::try_from(sequence).map_err(|_| WriterError::CursorSequenceOverflow)?;
    for change in &mut changes {
        match change {
            ContentChange::Write(change, _) => {
                change.revision_order = Some(revision);
                // A delete states it too: its marker tells that this request wrote it.
                change.source_revision = Some(format!("rev-{sequence}"));
            }
            // The only unchanged observations of this source are its patches.
            ContentChange::Present(document) => {
                document.revision_order = Some(revision);
                document.source_revision = Some(format!("rev-{sequence}"));
            }
        }
    }
    let batch = ConnectorBatch {
        schema_version: CONNECTOR_CONTRACT_VERSION,
        source_id: API_SOURCE_ID.into(),
        kind: SyncKind::Content,
        cursor: CursorStep {
            start: None,
            end: "api".into(),
        },
        body: BatchBody::DocumentChanges {
            changes: Vec::new(),
        },
    };
    let base = head.content.as_ref().map(|p| BaseHead {
        content_publication_id: p.publication_id.clone(),
        content_sequence: p.sequence,
        lance_version: p.lance_version,
    });
    let mut embedding = WriterEmbedding::open(config, API_SOURCE_ID).await?;
    crate::prepare::atomic(
        config,
        &api_source(),
        &batch,
        changes,
        &mut state,
        lease,
        base,
        &mut embedding,
        attributes,
    )
    .await?;
    Ok((id, format!("rev-{sequence}")))
}
