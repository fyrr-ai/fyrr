//! Authenticated bounded writer administration; HTTP diagnostics contain no request values.
use crate::scim::{Mutation, ScimError};
use crate::write_api::ApiError;
use crate::{Result, WriterError};
use axum::{
    Json, Router,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use fyrr_core::config::{
    ApiKeyRole, Config, DirectoryProvider, is_api_key_secret, is_namespace_name, prefix_allows,
};
use fyrr_core::identity::ApiKeysArtifact;
use fyrr_reader::storage::{
    API_KEYS_FILE, api_key_deployment_hash, deployment_root, namespace_store,
};
use ring::hmac;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Semaphore;

struct AdminState {
    config: Config,
    operator: hmac::Tag,
    scim: Option<hmac::Tag>,
    capacity: Arc<Semaphore>,
    /// Present when the namespace is written through the API.
    write: Option<crate::write_api::WriteApi>,
}
impl std::fmt::Debug for AdminState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AdminState(<redacted>)")
    }
}
fn token_key() -> hmac::Key {
    hmac::Key::new(hmac::HMAC_SHA256, b"fyrr-writer-admin-token-comparison-v1")
}
fn token_tag(value: &str) -> Result<hmac::Tag> {
    if value.len() < 16 || value.len() > 8192 {
        return Err(WriterError::AdminCredentialInvalid);
    }
    Ok(hmac::sign(&token_key(), value.as_bytes()))
}
fn error(error: ScimError) -> Response {
    let mut body = json!({"schemas":["urn:ietf:params:scim:api:messages:2.0:Error"],"status":error.status.to_string(),"detail":"Request could not be applied"});
    if matches!(
        error.kind,
        "invalidFilter"
            | "tooMany"
            | "uniqueness"
            | "mutability"
            | "invalidSyntax"
            | "invalidPath"
            | "noTarget"
            | "invalidValue"
            | "invalidVers"
            | "sensitive"
    ) {
        body["scimType"] = json!(error.kind);
    }
    let mut reply = (
        StatusCode::from_u16(error.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(body),
    )
        .into_response();
    reply.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/scim+json"),
    );
    if error.status == 401 {
        reply.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            header::HeaderValue::from_static("Bearer"),
        );
    }
    reply
}
fn reply(status: u16, value: Value) -> Response {
    if status == 204 {
        return StatusCode::NO_CONTENT.into_response();
    }
    let version = value["meta"]["version"].as_str().map(str::to_owned);
    let location = value["meta"]["location"].as_str().map(str::to_owned);
    let mut reply = (
        StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
        Json(value),
    )
        .into_response();
    reply.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/scim+json"),
    );
    for (header, value) in [(header::ETAG, version), (header::LOCATION, location)] {
        if let Some(value) = value.and_then(|s| s.parse().ok()) {
            reply.headers_mut().insert(header, value);
        }
    }
    reply
}
/// Admits an API key of the deployment with the `admin` role in this namespace.
/// The keys are read from storage at each request, so a key withdrawn by
/// `writer keys` is refused at once; an unreadable set refuses every key.
async fn admin_key(config: &Config, secret: &str) -> std::result::Result<(), ApiError> {
    if !is_api_key_secret(secret) {
        return Err(ApiError(401, "unauthorized"));
    }
    let (store, root) = namespace_store(config)?;
    let keys: ApiKeysArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &deployment_root(&root).join(API_KEYS_FILE),
    )
    .await?;
    let grant = keys
        .grant(&api_key_deployment_hash(config, secret)?)
        .ok_or(ApiError(401, "unauthorized"))?;
    if grant.roles.contains(&ApiKeyRole::Admin)
        && prefix_allows(grant.namespace_prefix.as_deref(), &config.namespace.name)
    {
        Ok(())
    } else {
        Err(ApiError(403, "forbidden"))
    }
}

/// The configuration of the namespace a route names. A namespace written
/// through the API is named by its path, as for its documents; any other is
/// the one this process is configured for.
fn namespace_config(state: &AdminState, name: &str) -> std::result::Result<Config, ApiError> {
    if state.write.is_none() {
        return Ok(state.config.clone());
    }
    if !is_namespace_name(name) {
        return Err(ApiError(404, "not_found"));
    }
    Ok(crate::write_api::named(&state.config, name))
}

/// Admits the request of a namespace route: an API key with the `admin` role
/// in `config`'s namespace, and the body, bounded. The permit is the request's
/// capacity, held until its work is done.
async fn admitted(
    state: &AdminState,
    request: Request,
    config: &Config,
) -> std::result::Result<(tokio::sync::OwnedSemaphorePermit, axum::body::Bytes), ApiError> {
    let secret = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .filter(|s| s.len() <= 8192)
        .map(str::to_owned);
    let Some(secret) = secret else {
        return Err(ApiError(401, "unauthorized"));
    };
    let Ok(permit) = state.capacity.clone().try_acquire_owned() else {
        return Err(ApiError(503, "busy"));
    };
    admin_key(config, &secret).await?;
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        axum::body::to_bytes(request.into_body(), 4096),
    )
    .await
    {
        Ok(Ok(bytes)) => Ok((permit, bytes)),
        Ok(Err(_)) => Err(ApiError(413, "body_too_large")),
        Err(_) => Err(ApiError(408, "request_timeout")),
    }
}

/// `DELETE /v1/namespaces/{namespace}`: deletes the namespace and its data. It
/// needs an API key with the `admin` role and the name repeated in the body.
async fn delete_namespace(
    state: Arc<AdminState>,
    request: Request,
    name: String,
) -> std::result::Result<Response, ApiError> {
    let config = namespace_config(&state, &name)?;
    let (permit, bytes) = admitted(&state, request, &config).await?;
    let body = serde_json::from_slice::<fyrr_core::http_api::DeleteNamespaceRequest>(&bytes)
        .map_err(|_| ApiError(400, "body_invalid"))?;
    if name != config.namespace.name {
        return Err(ApiError(404, "not_found"));
    }
    if body.namespace != name {
        return Err(ApiError(400, "body_invalid"));
    }
    // Keep the lease lifecycle, and the request's capacity, alive even if the
    // client closes its connection.
    let result = tokio::spawn(async move {
        let _permit = permit;
        crate::commit::delete(&config, None, Some(&name)).await
    })
    .await
    .map_err(|_| ApiError(503, "unavailable"))?;
    Ok((StatusCode::OK, Json(result?)).into_response())
}

/// `PATCH /v1/namespaces/{namespace}` with `read_only`: freezes the namespace
/// or thaws it. It needs an API key with the `admin` role. It answers `503`
/// while the committer holds the namespace lease; the request is then repeated.
async fn patch_namespace(
    state: Arc<AdminState>,
    request: Request,
    name: String,
) -> std::result::Result<Response, ApiError> {
    let config = namespace_config(&state, &name)?;
    let (permit, bytes) = admitted(&state, request, &config).await?;
    let body = serde_json::from_slice::<fyrr_core::http_api::PatchNamespaceRequest>(&bytes)
        .map_err(|_| ApiError(400, "body_invalid"))?;
    if name != config.namespace.name {
        return Err(ApiError(404, "not_found"));
    }
    let result = tokio::spawn(async move {
        let _permit = permit;
        crate::commit::set_read_only(&config, body.read_only).await
    })
    .await
    .map_err(|_| ApiError(503, "unavailable"))?;
    let changed = match result {
        Ok(changed) => changed,
        Err(WriterError::NamespaceUnavailable) => return Err(ApiError(404, "not_found")),
        Err(e) => return Err(e.into()),
    };
    Ok((
        StatusCode::OK,
        Json(fyrr_core::http_api::PatchNamespaceResponse {
            namespace: name,
            read_only: body.read_only,
            changed,
        }),
    )
        .into_response())
}

async fn handle(State(state): State<Arc<AdminState>>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    // `/v1/namespaces/{namespace}` only: `/documents` below it is the write API.
    if let Some(name) = path
        .strip_prefix("/v1/namespaces/")
        .filter(|name| !name.contains('/'))
    {
        let answer = match *request.method() {
            axum::http::Method::PATCH => patch_namespace(state, request, name.to_owned()).await,
            axum::http::Method::DELETE => delete_namespace(state, request, name.to_owned()).await,
            _ => Err(ApiError(405, "method_not_allowed")),
        };
        return answer.unwrap_or_else(ApiError::response);
    }
    if path.starts_with("/v1/") {
        // Its own authentication (an API key with the `write` role) and its own capacity.
        return match &state.write {
            Some(api) => crate::write_api::handle(api, &state.config, request).await,
            None => ApiError(404, "not_found").response(),
        };
    }
    let expected = if path.starts_with("/scim/v2/") {
        state.scim.as_ref()
    } else if path.starts_with("/admin/") {
        Some(&state.operator)
    } else {
        return error(ScimError::new(404, "notFound"));
    };
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if !matches!((expected,supplied),(Some(tag),Some(value)) if value.len()<=8192 && hmac::verify(&token_key(),value.as_bytes(),tag.as_ref()).is_ok())
    {
        return error(ScimError::new(401, "unauthorized"));
    }
    let Ok(permit) = state.capacity.clone().try_acquire_owned() else {
        return error(ScimError::new(503, "temporarilyUnavailable"));
    };
    process_request(State(state), request, permit).await
}
async fn process_request(
    State(state): State<Arc<AdminState>>,
    request: Request,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Response {
    let path = request.uri().path().to_owned();
    let method = request.method().as_str().to_owned();
    let uri = request.uri().clone();
    let version = request
        .headers()
        .get(header::IF_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let idempotency = request
        .headers()
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if path.len() > 4096
        || version.as_ref().is_some_and(|v| v.len() > 256)
        || idempotency.as_ref().is_some_and(|v| v.len() > 256)
    {
        return error(ScimError::new(400, "invalidSyntax"));
    }
    if method == "GET" && path == "/admin/status" {
        return match crate::envelopes::status(&state.config).await {
            Ok(v) => reply(200, v),
            Err(e) => error(e.into()),
        };
    }
    if method == "GET"
        && let Some(response) = discovery(&path)
    {
        return response;
    }
    let Some(tail) = path.strip_prefix("/scim/v2/") else {
        return error(ScimError::new(404, "notFound"));
    };
    let mut parts = tail.split('/');
    let users = match parts.next() {
        Some("Users") => true,
        Some("Groups") => false,
        _ => return error(ScimError::new(404, "notFound")),
    };
    let id = parts.next().map(str::to_owned);
    if parts.next().is_some() || id.as_ref().is_some_and(|s| s.is_empty() || s.len() > 256) {
        return error(ScimError::new(404, "notFound"));
    }
    if method == "GET" {
        return read_resources(&state.config, users, id, uri).await;
    }
    if !state.config.writer.serve.committer {
        return error(ScimError::new(503, "temporarilyUnavailable"));
    }
    if method == "POST" && id.is_some() || method != "POST" && id.is_none() {
        return error(ScimError::new(405, "invalidSyntax"));
    }
    if method != "POST" && version.is_none() {
        return error(ScimError::new(428, "preconditionRequired"));
    }
    if !matches!(method.as_str(), "POST" | "PUT" | "PATCH" | "DELETE") {
        return error(ScimError::new(405, "invalidSyntax"));
    }
    if method != "DELETE"
        && !request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| {
                s.split(';').next().is_some_and(|v| {
                    matches!(v.trim(), "application/json" | "application/scim+json")
                })
            })
    {
        return error(ScimError::new(415, "invalidSyntax"));
    }
    let bytes = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        axum::body::to_bytes(request.into_body(), 1024 * 1024),
    )
    .await
    {
        Ok(Ok(b)) => b,
        Ok(Err(_)) => return error(ScimError::new(413, "tooLarge")),
        Err(_) => return error(ScimError::new(408, "invalidSyntax")),
    };
    let body = if method == "DELETE" {
        Value::Null
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) => return error(ScimError::new(400, "invalidSyntax")),
        }
    };
    // Keep the lease lifecycle, and the request's capacity, alive even if the
    // client closes its connection.
    let config = state.config.clone();
    let result = tokio::spawn(async move {
        let _permit = permit;
        crate::scim::mutate(
            &config,
            Mutation {
                users,
                method,
                id,
                version,
                idempotency,
                body,
            },
        )
        .await
    })
    .await;
    match result {
        Ok(Ok((status, body))) => reply(status, body),
        Ok(Err(e)) => error(e),
        Err(_) => error(ScimError::new(503, "temporarilyUnavailable")),
    }
}

pub(crate) async fn start(
    config: &Config,
    stop: Arc<AtomicBool>,
    commits: Arc<crate::write_api::Commits>,
) -> Result<Option<tokio::task::JoinHandle<Result<(), WriterError>>>> {
    if config.namespace.write_api.is_some() && !config.writer.serve.committer {
        return Err(WriterError::WriteApiNeedsCommitter);
    }
    let Some(admin) = &config.writer.serve.admin else {
        return Ok(None);
    };
    let address: std::net::SocketAddr = admin
        .listen
        .parse()
        .map_err(|_| WriterError::AdminListenInvalid)?;
    if !address.ip().is_loopback() && !admin.tls_terminated {
        return Err(WriterError::AdminTlsRequired);
    }
    let _ = crate::secrets::seal(config, &Path::from("startup-validation"), b"")?;
    let operator = token_tag(&crate::secrets::resolve(config, &admin.operator_token).await?)?;
    let scim = if config.identity.directory.provider == DirectoryProvider::Scim {
        Some(token_tag(
            &crate::secrets::resolve(
                config,
                config
                    .identity
                    .directory
                    .credentials
                    .as_ref()
                    .ok_or(WriterError::CredentialUnavailable)?,
            )
            .await?,
        )?)
    } else {
        None
    };
    if scim
        .as_ref()
        .is_some_and(|tag| tag.as_ref() == operator.as_ref())
    {
        return Err(WriterError::AdminCredentialsMustDiffer);
    }
    let state = Arc::new(AdminState {
        config: config.clone(),
        operator,
        scim,
        capacity: Arc::new(Semaphore::new(8)),
        write: config
            .namespace
            .write_api
            .is_some()
            .then(|| crate::write_api::WriteApi::new(commits)),
    });
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|_| WriterError::AdminListenUnavailable)?;
    let task = tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(handle).with_state(state))
            .with_graceful_shutdown(async move {
                while !stop.load(Ordering::Acquire) {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .map_err(|_| WriterError::AdminServiceFailed)
    });
    Ok(Some(task))
}
use object_store::path::Path;

fn discovery(path: &str) -> Option<Response> {
    if path == "/scim/v2/ServiceProviderConfig" {
        return Some(reply(
            200,
            json!({"schemas":["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],"patch":{"supported":true},"bulk":{"supported":false,"maxOperations":0,"maxPayloadSize":0},"filter":{"supported":true,"maxResults":1000},"changePassword":{"supported":false},"sort":{"supported":false},"etag":{"supported":true},"authenticationSchemes":[{"type":"oauthbearertoken","name":"Provider bearer token","description":"Bearer secret over TLS","primary":true}]}),
        ));
    }
    if path == "/scim/v2/ResourceTypes" {
        return Some(reply(
            200,
            json!({"schemas":["urn:ietf:params:scim:api:messages:2.0:ListResponse"],"totalResults":2,"startIndex":1,"itemsPerPage":2,"Resources":[{"id":"User","name":"User","endpoint":"/Users","schema":crate::scim::USER_SCHEMA,"schemaExtensions":[{"schema":crate::scim::IDENTITY_EXTENSION,"required":false}]},{"id":"Group","name":"Group","endpoint":"/Groups","schema":crate::scim::GROUP_SCHEMA}]}),
        ));
    }
    if path.starts_with("/scim/v2/Schemas") {
        let schemas = crate::scim::schemas();
        if path == "/scim/v2/Schemas" {
            return Some(reply(
                200,
                json!({"schemas":["urn:ietf:params:scim:api:messages:2.0:ListResponse"],"totalResults":schemas.len(),"startIndex":1,"itemsPerPage":schemas.len(),"Resources":schemas}),
            ));
        }
        if let Some(id) = path.strip_prefix("/scim/v2/Schemas/") {
            return Some(
                schemas
                    .into_iter()
                    .find(|s| s["id"].as_str() == Some(id))
                    .map_or_else(|| error(ScimError::new(404, "notFound")), |s| reply(200, s)),
            );
        }
    }
    None
}
async fn read_resources(
    config: &Config,
    users: bool,
    id: Option<String>,
    uri: axum::http::Uri,
) -> Response {
    let mut resources = match crate::scim::resources(config, users).await {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    if let Some(id) = id {
        return resources
            .into_iter()
            .find(|v| v["id"].as_str() == Some(&id))
            .map_or_else(|| error(ScimError::new(404, "notFound")), |v| reply(200, v));
    }
    let mut start = 1usize;
    let mut count = 100usize;
    let Ok(query) = reqwest::Url::parse(&format!("http://localhost{uri}")) else {
        return error(ScimError::new(400, "invalidSyntax"));
    };
    for (key, value) in query.query_pairs() {
        match key.as_ref() {
            "startIndex" => match value.parse::<usize>() {
                Ok(v) if v > 0 => start = v,
                _ => return error(ScimError::new(400, "invalidValue")),
            },
            "count" => match value.parse::<usize>() {
                Ok(v) if v <= 1000 => count = v,
                _ => return error(ScimError::new(400, "tooMany")),
            },
            "filter" => {
                let Some((field, expected)) = value
                    .split_once(" eq \"")
                    .and_then(|(f, v)| v.strip_suffix('"').map(|v| (f, v)))
                else {
                    return error(ScimError::new(400, "invalidFilter"));
                };
                let field = match field.to_ascii_lowercase().as_str() {
                    "username" if users => "userName",
                    "displayname" => "displayName",
                    "id" => "id",
                    "externalid" => "externalId",
                    _ => return error(ScimError::new(400, "invalidFilter")),
                };
                resources.retain(|v| {
                    v[field]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case(expected))
                });
            }
            _ => return error(ScimError::new(400, "invalidValue")),
        }
    }
    let total = resources.len();
    let page: Vec<_> = resources.into_iter().skip(start - 1).take(count).collect();
    reply(
        200,
        json!({"schemas":["urn:ietf:params:scim:api:messages:2.0:ListResponse"],"totalResults":total,"startIndex":start,"itemsPerPage":page.len(),"Resources":page}),
    )
}
