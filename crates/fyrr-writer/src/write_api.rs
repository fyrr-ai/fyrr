//! `POST /v1/namespaces/{namespace}/documents`: upserts and deletes of one
//! request become one prepared envelope, which the committer publishes at once;
//! the answer is sent when the served head holds it or has refused it.
use crate::embedding::WriterEmbedding;
use crate::prepare::ContentChange;
use crate::sync_lease::CursorLease;
use crate::{Result, WriterError};
use axum::{
    Json,
    body::Body,
    extract::Request,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures::StreamExt;
use fyrr_core::attributes::{AttributeSchema, resolve};
use fyrr_core::config::{
    ApiKeyRole, Config, PermissionsMode, SourceConfig, SourceIntervals, is_api_key_secret,
    prefix_allows,
};
use fyrr_core::connector::{
    API_SOURCE_ID, BatchBody, CONNECTOR_CONTRACT_VERSION, ChangeOp, ConnectorBatch, CursorStep,
    DocumentChange, DocumentMetadata, Section, SyncKind,
};
use fyrr_core::envelope::{BaseHead, envelope_id};
use fyrr_core::identity::ApiKeysArtifact;
use fyrr_core::publication::{NamespaceSettings, PolicyArtifact};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, OwnedSemaphorePermit, watch};

/// How long a request waits for its publication before the outcome is unknown.
const ANSWER_DEADLINE: Duration = Duration::from_secs(120);
/// How long the body may stay silent while it is received.
const BODY_IDLE: Duration = Duration::from_secs(30);
/// How often a waiting request looks at the published history again, and asks
/// the committer for a round when its envelope is not in it.
const RECHECK: Duration = Duration::from_millis(500);

/// The committer's side of the API: a request wakes a round, and each round's
/// end is counted.
#[derive(Debug)]
pub(crate) struct Commits {
    pub(crate) requested: Notify,
    pub(crate) done: watch::Sender<u64>,
}
impl Commits {
    pub(crate) fn new() -> Self {
        Self {
            requested: Notify::new(),
            done: watch::channel(0).0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct WriteApi {
    pub(crate) commits: Arc<Commits>,
    /// One request receives its body and prepares its envelope at a time, so
    /// that one body is in memory and the source's sequence is single.
    slot: Arc<Mutex<()>>,
}
impl WriteApi {
    pub(crate) fn new(commits: Arc<Commits>) -> Self {
        Self {
            commits,
            slot: Arc::new(Mutex::new(())),
        }
    }
}

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
            WriterError::ApiNamespaceHasSources => Self(409, "api_namespace_has_sources"),
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
    settings: Option<Value>,
    #[serde(default)]
    upserts: Rows<Map<String, Value>>,
    #[serde(default)]
    deletes: Rows<String>,
}

/// A row that passed its checks.
struct Upsert {
    id: String,
    readers: Vec<String>,
    text: String,
    typed: Map<String, Value>,
}

/// Serves the route; `permit` is the listener's capacity, held until the answer.
pub(crate) async fn handle(
    api: &WriteApi,
    config: &Config,
    request: Request,
    permit: OwnedSemaphorePermit,
) -> Response {
    match accept(api, config, request, permit).await {
        Ok(reply) => reply,
        Err(error) => error.response(),
    }
}

async fn accept(
    api: &WriteApi,
    config: &Config,
    request: Request,
    permit: OwnedSemaphorePermit,
) -> Result<Response, ApiError> {
    let namespace = request
        .uri()
        .path()
        .strip_prefix("/v1/namespaces/")
        .and_then(|rest| rest.strip_suffix("/documents"))
        .filter(|name| !name.contains('/'));
    if namespace != Some(config.namespace.name.as_str()) || config.namespace.write_api.is_none() {
        return Err(ApiError(404, "not_found"));
    }
    if request.method() != axum::http::Method::POST {
        return Err(ApiError(405, "method_not_allowed"));
    }
    // The key is checked as the request arrives; a key revoked afterwards does
    // not cancel an envelope that was already accepted.
    authorize(config, request.headers()).await?;
    let headers = request.headers();
    if !headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(';').next().is_some_and(|t| t.trim() == "application/json"))
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
    let (parsed, exceeded) = tokio::task::spawn_blocking(move || {
        ROWS_LEFT.set(rows);
        ROWS_EXCEEDED.set(false);
        let parsed = serde_json::from_slice::<Payload>(&bytes);
        (parsed, ROWS_EXCEEDED.get())
    })
    .await
    .map_err(|_| ApiError(503, "unavailable"))?;
    let body = match parsed {
        Ok(body) => body,
        Err(_) if exceeded => return Err(ApiError(413, "too_many_rows")),
        Err(_) => return Err(ApiError(400, "body_invalid")),
    };
    // The work outlives a caller that goes away: an envelope that was begun is
    // finished, and the capacity is held until then.
    let (config, commits) = (config.clone(), api.commits.clone());
    let done = tokio::spawn(async move {
        let _permit = permit;
        execute(&config, &commits, body, slot).await
    })
    .await
    .map_err(|_| ApiError(503, "unavailable"))??;
    Ok(Json(json!({"upserted": done.0, "deleted": done.1})).into_response())
}

async fn authorize(config: &Config, headers: &HeaderMap) -> Result<(), ApiError> {
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
        && prefix_allows(grant.namespace_prefix.as_deref(), &config.namespace.name)
    {
        Ok(())
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
fn laid_over(base: &NamespaceSettings, request: Option<&Value>) -> Result<NamespaceSettings, ApiError> {
    let mut value = serde_json::to_value(base).map_err(|_| ApiError(400, "settings_invalid"))?;
    if let Some(request) = request {
        merge(&mut value, request);
    }
    let settings: NamespaceSettings =
        serde_json::from_value(value).map_err(|_| ApiError(400, "settings_invalid"))?;
    // A request cannot make a namespace that the API does not write.
    if settings.write_api.is_none() {
        return Err(ApiError(400, "settings_invalid"));
    }
    Ok(settings)
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

/// The checked rows of the request, against the schema and the text key the
/// namespace was created with.
fn checked(
    body: &Payload,
    attributes: &AttributeSchema,
    text_key: &str,
) -> Result<Vec<Upsert>, ApiError> {
    let mut ids = std::collections::BTreeSet::new();
    for id in &body.deletes.0 {
        if !ids.insert(id.as_str()) {
            return Err(ApiError(400, "duplicate_id"));
        }
    }
    let mut rows = Vec::with_capacity(body.upserts.0.len());
    let mut working = attributes.clone();
    for row in &body.upserts.0 {
        let id = row
            .get("id")
            .and_then(Value::as_str)
            .ok_or(ApiError(400, "id_missing"))?;
        if !ids.insert(id) {
            return Err(ApiError(400, "duplicate_id"));
        }
        let readers = match row.get("readers") {
            None => Vec::new(),
            Some(Value::Array(list)) => list
                .iter()
                .map(|v| v.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
                .ok_or(ApiError(400, "readers_invalid"))?,
            Some(_) => return Err(ApiError(400, "readers_invalid")),
        };
        let text = match row.get(text_key) {
            Some(Value::String(text)) if !text.is_empty() => text.clone(),
            _ => return Err(ApiError(400, "text_missing")),
        };
        if text.len() > crate::chunk::MAX_BYTES {
            // A row is one record: no chunking.
            return Err(ApiError(413, "text_too_large"));
        }
        let mut typed = Map::new();
        for (name, value) in row {
            if matches!(name.as_str(), "id" | "readers") || name == text_key {
                continue;
            }
            if !attributes.contains_key(name) {
                return Err(ApiError(400, "attribute_undeclared"));
            }
            resolve(&mut working, name, value)
                .map_err(|error| ApiError(400, error.code()))?;
            typed.insert(name.clone(), value.clone());
        }
        rows.push(Upsert {
            id: id.to_owned(),
            readers,
            text,
            typed,
        });
    }
    Ok(rows)
}

/// Creates or confirms the namespace, prepares the request's envelope and
/// waits for the head to serve it. Returns the rows upserted and deleted.
async fn execute(
    config: &Config,
    commits: &Commits,
    body: Payload,
    slot: OwnedMutexGuard<()>,
) -> Result<(u64, u64), ApiError> {
    let head = match fyrr_reader::storage::optional_head(config).await? {
        Some(head) => head,
        None => {
            let settings = laid_over(&crate::commit::namespace_settings(config), body.settings.as_ref())?;
            crate::commit::create_with(config, settings).await?;
            fyrr_reader::storage::optional_head(config)
                .await?
                .ok_or(ApiError(503, "unavailable"))?
        }
    };
    let (store, root) = fyrr_reader::storage::namespace_store(config)?;
    // The policy that is served decides, also when another creation won.
    let policy: PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    if policy.settings.write_api.is_none() {
        return Err(ApiError(409, "namespace_not_writable"));
    }
    if body.settings.is_some()
        && laid_over(&policy.settings, body.settings.as_ref())? != policy.settings
    {
        return Err(ApiError(409, "namespace_settings_fixed"));
    }
    let config = config
        .with_settings(&policy.settings)
        .ok_or(ApiError(503, "unavailable"))?;
    if !config.sources.is_empty() {
        return Err(ApiError(409, "api_namespace_has_sources"));
    }
    let text_key = config
        .namespace
        .write_api
        .as_ref()
        .map_or("text", |api| api.text_attribute.as_str());
    let rows = checked(&body, &config.namespace.attributes, text_key)?;
    let counts = (rows.len() as u64, body.deletes.0.len() as u64);
    if counts == (0, 0) {
        if body.settings.is_none() {
            return Err(ApiError(400, "empty_request"));
        }
        return Ok(counts);
    }
    let mut done = commits.done.subscribe();
    let id = prepare(&config, &head, rows, body.deletes.0, slot).await?;
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
                return Err(refusal(&config, &id).await);
            }
            if applied.contains(&id) {
                return Ok(counts);
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

/// The named reason a refused envelope carries.
async fn refusal(config: &Config, id: &str) -> ApiError {
    let reason = async {
        let (store, root) = fyrr_reader::storage::namespace_store(config).ok()?;
        let marker: fyrr_core::envelope::RejectionMarker = fyrr_reader::storage::read_contract(
            store.as_ref(),
            &root.join("rejections").join(format!("{id}.json")),
        )
        .await
        .ok()?;
        Some(marker.reason)
    }
    .await;
    match reason.as_deref() {
        Some("namespace_takes_connectors") => ApiError(409, "namespace_not_writable"),
        Some("envelope_expired") => ApiError(409, "envelope_expired"),
        Some("envelope_bounds_exceeded") => ApiError(413, "request_too_large"),
        _ => ApiError(409, "rejected"),
    }
}

/// Writes the request's envelope under the source's cursor lease and returns
/// its ID. The slot is released when the envelope is durable.
async fn prepare(
    config: &Config,
    head: &fyrr_core::publication::ServingHead,
    rows: Vec<Upsert>,
    deletes: Vec<String>,
    slot: OwnedMutexGuard<()>,
) -> Result<String, ApiError> {
    let lease = CursorLease::acquire(config, API_SOURCE_ID, SyncKind::Content).await?;
    let renewal = lease.keepalive();
    let result = write_envelope(config, head, &lease, rows, deletes).await;
    let stopped = renewal.stop().await;
    let released = lease.release().await;
    drop(slot);
    let id = result?;
    stopped?;
    released?;
    Ok(id)
}

async fn write_envelope(
    config: &Config,
    head: &fyrr_core::publication::ServingHead,
    lease: &CursorLease,
    rows: Vec<Upsert>,
    deletes: Vec<String>,
) -> Result<String, ApiError> {
    let (store, root) = fyrr_reader::storage::namespace_store(config)?;
    let mut state = lease.state().await?;
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
        let path = root.join("envelopes").join(format!("{}.json", id_of(next)));
        match object_store::ObjectStoreExt::head(store.as_ref(), &path).await {
            Ok(_) => sequence = next,
            Err(object_store::Error::NotFound { .. }) => break,
            Err(_) => return Err(ApiError(503, "unavailable")),
        }
    }
    if sequence != state.sequence {
        state.sequence = sequence;
        lease.write(state.clone()).await?;
    }
    let sequence = sequence
        .checked_add(1)
        .ok_or(WriterError::CursorSequenceOverflow)?;
    let id = id_of(sequence);
    // Every revision of the source is newer than the one before it.
    let revision = i64::try_from(sequence).map_err(|_| WriterError::CursorSequenceOverflow)?;
    let change = |op, root_id: String| DocumentChange {
        op,
        root_id,
        source_revision: (op == ChangeOp::Upsert).then(|| format!("rev-{sequence}")),
        revision_order: Some(revision),
        expected_prior_revision: None,
        permission_revision: None,
        readers: Vec::new(),
        containers: Vec::new(),
        metadata: None,
        attributes: Vec::new(),
        sections: Vec::new(),
        typed_attributes: Map::new(),
    };
    let source = api_source();
    let mut changes = Vec::new();
    for row in rows {
        let mut change = change(ChangeOp::Upsert, row.id.clone());
        change.readers = if config.namespace.write_api.as_ref().is_some_and(|api| api.open) {
            Vec::new()
        } else {
            row.readers
        };
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
        }];
        change.typed_attributes = row.typed;
        changes.push(change);
    }
    changes.extend(deletes.into_iter().map(|id| change(ChangeOp::Delete, id)));
    // The connector path quarantines what it refuses; a request is refused whole.
    for change in &changes {
        match crate::prepare::quarantine_reason(config, &source, change) {
            None => {}
            Some("document_oversized") => return Err(ApiError(413, "document_too_large")),
            Some(_) => return Err(ApiError(400, "document_invalid")),
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
        &source,
        &batch,
        changes
            .into_iter()
            .map(|change| ContentChange::Write(Box::new(change)))
            .collect(),
        &mut state,
        lease,
        base,
        &mut embedding,
    )
    .await?;
    Ok(id)
}
