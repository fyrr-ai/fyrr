//! Bounded HTTP resource boundary; MCP messages and sessions belong to rmcp.
use crate::error::*;
use axum::{
    Router,
    extract::{Path, Request, State},
    http::{Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use fyrr_core::config::{Config, ProcessEnvironment};
use fyrr_core::mcp::ErrorCode;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

/// Largest accepted HTTP request body, checked before and while reading it.
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_HEADERS: usize = 64;
const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_BEARER_BYTES: usize = 16 * 1024;
const MAX_CONNECTIONS: usize = 64;
const MAX_REQUESTS_IN_FLIGHT: usize = 64;
const MAX_CONTROLS_IN_FLIGHT: usize = 8;
const MAX_CONTROL_KEYS: usize = 1024;
const MAX_CONTROL_REQUESTS_PER_MINUTE: u32 = 30;
const MAX_CONTROL_ATTEMPTS_PER_SECOND: u32 = 32;
const MAX_KEYS_PER_SUBJECT: usize = 5001;
const MAX_TRACKED_RATE_KEYS: usize = 32768;
const RATE_WINDOW: Duration = Duration::from_secs(60);
const CONTROL_ATTEMPT_WINDOW: Duration = Duration::from_secs(1);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(301);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(302);
/// The caller went away before its answer; no standard status says so.
const CLIENT_CLOSED_REQUEST: StatusCode = match StatusCode::from_u16(499) {
    Ok(status) => status,
    Err(_) => unreachable!(),
};

#[derive(Clone)]
struct HttpState {
    admission: Arc<admission::Admission>,
    service: crate::serve::McpService,
    config: Config,
    public: reqwest::Url,
    /// Absent when only API keys authenticate callers.
    auth: Option<crate::auth::Authenticator>,
    permits: Arc<Semaphore>,
    controls: Arc<Semaphore>,
    control_limits: Arc<Mutex<ControlLimits>>,
    limits: Arc<Mutex<BTreeMap<String, (Instant, u32)>>>,
    refresh: Arc<Mutex<Option<Instant>>>,
}
impl HttpState {
    fn http(&self) -> &fyrr_core::config::HttpConfig {
        let Some(http) = &self.config.reader.http else {
            unreachable!("HTTP state is created only with HTTP configuration");
        };
        http
    }
    fn oidc(&self) -> Option<&fyrr_core::config::OidcConfig> {
        self.config.identity.oidc.as_ref()
    }
}

// Verified fingerprints let exhausted subjects be refused before signature work.
struct ControlLimits {
    subjects: BTreeMap<String, (Instant, u32)>,
    tokens: BTreeMap<[u8; 32], (Instant, String)>,
    attempts: (Instant, u32),
}
impl ControlLimits {
    fn before_auth(&mut self, token: [u8; 32]) -> bool {
        self.subjects.retain(|_, (t, _)| t.elapsed() < RATE_WINDOW);
        self.tokens.retain(|_, (t, _)| t.elapsed() < RATE_WINDOW);
        if self.tokens.get(&token).is_some_and(|(_, subject)| {
            self.subjects
                .get(subject)
                .is_some_and(|(_, n)| *n >= MAX_CONTROL_REQUESTS_PER_MINUTE)
        }) {
            return false;
        }
        if self.attempts.0.elapsed() >= CONTROL_ATTEMPT_WINDOW {
            self.attempts = (Instant::now(), 0);
        }
        if self.attempts.1 >= MAX_CONTROL_ATTEMPTS_PER_SECOND {
            return false;
        }
        self.attempts.1 += 1;
        true
    }
    fn authenticated(&mut self, token: [u8; 32], subject: &str) -> bool {
        if !self.subjects.contains_key(subject) && self.subjects.len() >= MAX_CONTROL_KEYS {
            return false;
        }
        if self.tokens.len() < MAX_CONTROL_KEYS {
            self.tokens
                .insert(token, (Instant::now(), subject.to_owned()));
        }
        let (_, count) = self
            .subjects
            .entry(subject.to_owned())
            .or_insert((Instant::now(), 0));
        if *count >= MAX_CONTROL_REQUESTS_PER_MINUTE {
            return false;
        }
        *count += 1;
        true
    }
}
fn error(status: StatusCode, code: ReaderError) -> Response {
    retry_after((status, axum::Json(json!({"error":code.as_str()}))).into_response())
}
/// Names a delay on every 429 (the rate window) and 503 (a short pause).
fn retry_after(mut response: Response) -> Response {
    let seconds = match response.status() {
        StatusCode::TOO_MANY_REQUESTS => RATE_WINDOW.as_secs(),
        StatusCode::SERVICE_UNAVAILABLE => 1,
        _ => return response,
    };
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, seconds.into());
    response
}
/// The tool a `POST /v1/namespaces/{namespace}/query` or `.../fetch` runs.
fn json_tool(request: &Request) -> Option<&'static str> {
    if request.method() != Method::POST {
        return None;
    }
    match request
        .uri()
        .path()
        .strip_prefix("/v1/namespaces/")?
        .split_once('/')?
    {
        (_, "query") => Some("search"),
        (_, "fetch") => Some("fetch"),
        _ => None,
    }
}
fn bearer(request: &Request) -> Result<&str, ()> {
    let mut values = request.headers().get_all(header::AUTHORIZATION).iter();
    let value = values.next().ok_or(())?;
    if values.next().is_some() {
        return Err(());
    }
    let value = value.to_str().map_err(|_| ())?;
    let (scheme, token) = value.split_once(' ').ok_or(())?;
    if !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() || token.len() > MAX_BEARER_BYTES
    {
        return Err(());
    }
    Ok(token)
}
async fn boundary(State(state): State<HttpState>, mut request: Request, next: Next) -> Response {
    request.extensions_mut().insert(tokio::time::Instant::now());
    tokio::time::timeout(REQUEST_TIMEOUT, boundary_inner(state, request, next))
        .await
        .unwrap_or_else(|_| error(StatusCode::GATEWAY_TIMEOUT, DeadlineExceeded))
}
async fn boundary_inner(state: HttpState, mut request: Request, next: Next) -> Response {
    let h = state.http();
    let public = &state.public;
    let expected = public.authority().to_owned();
    if request.headers().len() > MAX_HEADERS
        || request
            .headers()
            .iter()
            .map(|(k, v)| k.as_str().len() + v.as_bytes().len())
            .sum::<usize>()
            > MAX_HEADER_BYTES
    {
        return error(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE, InvalidArgument);
    }
    if request.headers().contains_key(header::ORIGIN) {
        return error(StatusCode::FORBIDDEN, InvalidArgument);
    }
    let foreign_host = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        != Some(expected.as_str());
    // Probes reach /readyz under other Host names; they get only the status.
    let probe = request.uri().path() == "/readyz";
    if foreign_host && !probe {
        crate::telemetry::emit(format_args!("http refused code={}", HostMismatch));
        return error(StatusCode::FORBIDDEN, HostMismatch);
    }
    let tool = json_tool(&request);
    let _permit = if request.uri().path() != "/mcp" && tool.is_none() {
        match state.permits.clone().try_acquire_owned() {
            Ok(p) => Some(p),
            Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, Busy),
        }
    } else {
        None
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|n| n > MAX_REQUEST_BYTES as u64)
    {
        return error(StatusCode::PAYLOAD_TOO_LARGE, InvalidArgument);
    }
    if request.uri().path() == "/mcp" || tool.is_some() {
        let token = bearer(&request).map(str::to_owned);
        let (mut parts, body) = request.into_parts();
        let bytes =
            match tokio::time::timeout(IO_TIMEOUT, axum::body::to_bytes(body, MAX_REQUEST_BYTES))
                .await
            {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(_)) => {
                    return error(StatusCode::PAYLOAD_TOO_LARGE, InvalidArgument);
                }
                Err(_) => {
                    return error(StatusCode::REQUEST_TIMEOUT, DeadlineExceeded);
                }
            };
        let (message, parsed) = if let Some(name) = tool {
            let call = serde_json::from_slice(&bytes)
                .map_err(|_| InvalidArgument)
                .and_then(|arguments| crate::serve::ToolCall::parse(name, arguments));
            (None, Some(call))
        } else {
            let message = serde_json::from_slice::<rmcp::model::ClientJsonRpcMessage>(&bytes).ok();
            let parsed = match &message {
                Some(rmcp::model::ClientJsonRpcMessage::Request(rpc)) => match &rpc.request {
                    rmcp::model::ClientRequest::CallToolRequest(call) => {
                        Some(crate::serve::ToolCall::parse(
                            &call.params.name,
                            serde_json::Value::Object(
                                call.params.arguments.clone().unwrap_or_default(),
                            ),
                        ))
                    }
                    _ => None,
                },
                _ => None,
            };
            (message, parsed)
        };
        let ms = parsed
            .as_ref()
            .and_then(|result| result.as_ref().ok())
            .map_or(
                fyrr_core::limits::mcp::DEFAULT_DEADLINE_MS,
                crate::serve::ToolCall::deadline_ms,
            );
        if let Some(parsed) = parsed {
            parts.extensions.insert(parsed);
        }
        let Some(arrived) = parts.extensions.get::<tokio::time::Instant>() else {
            unreachable!("the HTTP boundary records arrival before reading the body");
        };
        let deadline = *arrived + Duration::from_millis(u64::from(ms));
        if tokio::time::Instant::now() >= deadline {
            return error(StatusCode::REQUEST_TIMEOUT, DeadlineExceeded);
        }
        let control = matches!(
            &message,
            Some(rmcp::model::ClientJsonRpcMessage::Notification(notification))
                if matches!(
                    &notification.notification,
                    rmcp::model::ClientNotification::CancelledNotification(_)
                )
        );
        let permits = if control {
            state.controls.clone()
        } else {
            state.permits.clone()
        };
        let Ok(_mcp_permit) = permits.try_acquire_owned() else {
            return error(StatusCode::SERVICE_UNAVAILABLE, Busy);
        };
        let subject = match authenticate_mcp(&state, h, token, &message, deadline, &expected).await
        {
            Ok(subject) => subject,
            Err(response) => return response,
        };
        request = Request::from_parts(parts, axum::body::Body::from(bytes));
        request.extensions_mut().insert(subject);
    }
    if foreign_host {
        return next.run(request).await.status().into_response();
    }
    let claims = Arc::new(crate::serve::ResponseClaims::default());
    request.extensions_mut().insert(claims.clone());
    let (parts, body) = next.run(request).await.into_parts();
    Response::from_parts(
        parts,
        axum::body::Body::new(AccountedBody {
            body,
            _claims: claims,
        }),
    )
}
async fn authenticate_mcp(
    state: &HttpState,
    h: &fyrr_core::config::HttpConfig,
    token: Result<String, ()>,
    message: &Option<rmcp::model::ClientJsonRpcMessage>,
    deadline: tokio::time::Instant,
    expected: &str,
) -> Result<crate::auth::AuthenticatedSubject, Response> {
    let cancellation = match message {
        Some(rmcp::model::ClientJsonRpcMessage::Notification(notification)) => {
            match &notification.notification {
                rmcp::model::ClientNotification::CancelledNotification(cancel) => Some(cancel),
                _ => None,
            }
        }
        _ => None,
    };
    let fingerprint: [u8; 32] = Sha256::digest(token.as_deref().unwrap_or("").as_bytes()).into();
    if cancellation.is_some() {
        let mut limits = timeout_at(deadline, state.control_limits.lock()).await?;
        if !limits.before_auth(fingerprint) {
            return Err(error(StatusCode::TOO_MANY_REQUESTS, RateLimited));
        }
    }
    let authenticated = timeout_at(deadline, async {
        match (token, &state.auth) {
            // Only a reader whose own configuration declares API keys takes
            // this path; any other reader treats the bearer as an OIDC token.
            (Ok(token), _)
                if !state.config.identity.api_keys.is_empty()
                    && token.starts_with(fyrr_core::config::API_KEY_PREFIX) =>
            {
                crate::auth::api_key(&state.config, &token)
            }
            (Ok(token), Some(auth)) => auth.authenticate(&token).await,
            _ => Err(Unauthorized),
        }
    })
    .await?;
    let mut subject = match authenticated {
        Ok(s) => s,
        Err(code) => return Err(authentication_failure(state, code, expected)),
    };
    // The lookup of an API key precedes everything else, a cancellation included.
    let admit = || async {
        timeout_at(deadline, state.admission.authorize(&state.config, &subject))
            .await?
            .map_err(|code| match code {
                Unauthorized => authentication_failure(state, code, expected),
                _ => error(StatusCode::SERVICE_UNAVAILABLE, AuthorizationUnavailable),
            })
    };
    // Authenticated control traffic has its own concurrency bound and never
    // spends a user's or group's request allowance. IDs remain subject-scoped.
    if let Some(cancel) = cancellation {
        if subject.api_key {
            admit().await?;
        }
        let mut limits = timeout_at(deadline, state.control_limits.lock()).await?;
        if !limits.authenticated(fingerprint, &subject.key) {
            return Err(error(StatusCode::TOO_MANY_REQUESTS, RateLimited));
        }
        if let Some(id) = &cancel.params.request_id {
            state.service.cancel(&subject.key, id);
        }
        return Err(StatusCode::ACCEPTED.into_response());
    }
    let (principal, groups, identity) = admit().await?;
    subject.identity = Some(identity);
    let mut limits = timeout_at(deadline, state.limits.lock()).await?;
    limits.retain(|_, (t, _)| t.elapsed() < RATE_WINDOW);
    let mut keys = vec![(format!("user:{principal}"), h.user_requests_per_minute)];
    keys.extend(
        groups
            .into_iter()
            .map(|g| (format!("group:{g}"), h.group_requests_per_minute)),
    );
    if keys.len() > MAX_KEYS_PER_SUBJECT
        || limits.len() + keys.iter().filter(|(k, _)| !limits.contains_key(k)).count()
            > MAX_TRACKED_RATE_KEYS
    {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            AuthorizationUnavailable,
        ));
    }
    if keys
        .iter()
        .any(|(k, cap)| limits.get(k).is_some_and(|(_, n)| n >= cap))
    {
        return Err(error(StatusCode::TOO_MANY_REQUESTS, RateLimited));
    }
    for (key, _) in keys {
        let count = limits.entry(key).or_insert((Instant::now(), 0));
        count.1 += 1;
    }
    drop(limits);
    Ok(subject)
}

/// 401 for an unauthenticated caller and 503 when authentication cannot be
/// decided. The challenge points at the OIDC resource metadata only when an
/// authorization server exists.
fn authentication_failure(state: &HttpState, code: ReaderError, expected: &str) -> Response {
    let mut response = error(
        if code == Unauthorized {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        code,
    );
    let challenge = match state.oidc() {
        Some(oidc) => format!(
            "Bearer resource_metadata=\"{}://{}/.well-known/oauth-protected-resource\", \
                scope=\"{}\"",
            state.public.scheme(),
            expected,
            oidc.scope
        ),
        None => "Bearer".to_owned(),
    };
    if let Ok(value) = challenge.parse() {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, value);
    }
    response
}

struct AccountedBody {
    body: axum::body::Body,
    _claims: Arc<crate::serve::ResponseClaims>,
}
struct AccountedBytes {
    bytes: axum::body::Bytes,
    _claims: Arc<crate::serve::ResponseClaims>,
}
impl AsRef<[u8]> for AccountedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl hyper::body::Body for AccountedBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let claims = self._claims.clone();
        std::pin::Pin::new(&mut self.body)
            .poll_frame(cx)
            .map(|frame| {
                frame.map(|frame| {
                    frame.map(|frame| {
                        frame.map_data(|bytes| {
                            axum::body::Bytes::from_owner(AccountedBytes {
                                bytes,
                                _claims: claims,
                            })
                        })
                    })
                })
            })
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

async fn metadata(State(state): State<HttpState>) -> axum::Json<serde_json::Value> {
    let h = state.http();
    let Some(oidc) = state.oidc() else {
        unreachable!("the metadata routes are registered only with an OIDC configuration");
    };
    axum::Json(json!({"resource":h.public_url,
            "authorization_servers":[oidc.issuer],
            "scopes_supported":[oidc.scope],
            "bearer_methods_supported":["header"]}))
}
async fn readiness(State(state): State<HttpState>) -> Response {
    if let Some(auth) = &state.auth
        && !auth.ready().await
    {
        return error(StatusCode::SERVICE_UNAVAILABLE, AuthorizationUnavailable);
    }
    let Ok(head) = state.admission.fresh_head().await else {
        return error(StatusCode::SERVICE_UNAVAILABLE, PublicationUnavailable);
    };
    if head.namespace != state.config.namespace.name
        || !state
            .admission
            .identity(&state.config, &head.identity)
            .await
            .is_ok_and(|identity| crate::storage::directory_published(&state.config, &identity))
    {
        return error(StatusCode::SERVICE_UNAVAILABLE, AuthorizationUnavailable);
    }
    let serde_json::Value::Object(mut readiness) = state.service.publication().await else {
        unreachable!("serving readiness is an object");
    };
    readiness.retain(|key, _| {
        matches!(
            key.as_str(),
            "ready" | "publication_id" | "lance_version" | "state" | "warm_ready"
        )
    });
    let status = if readiness["ready"] == true {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    retry_after((status, axum::Json(serde_json::Value::Object(readiness))).into_response())
}
/// Runs the parsed call the boundary authenticated through the scheduler that
/// serves MCP and answers its output as plain JSON.
async fn tool_json(
    State(state): State<HttpState>,
    Path(namespace): Path<String>,
    request: Request,
) -> Response {
    let extensions = request.extensions();
    let (Some(arrived), Some(parsed), Some(claims)) = (
        extensions.get::<tokio::time::Instant>().copied(),
        extensions.get::<Result<crate::serve::ToolCall>>().cloned(),
        extensions
            .get::<Arc<crate::serve::ResponseClaims>>()
            .cloned(),
    ) else {
        unreachable!("the HTTP boundary parses and accounts a JSON call before routing it");
    };
    let subject = extensions
        .get::<crate::auth::AuthenticatedSubject>()
        .cloned();
    if namespace != state.config.namespace.name {
        return error(StatusCode::NOT_FOUND, InvalidArgument);
    }
    let call = match parsed {
        Ok(call) => call,
        Err(code) => return error(StatusCode::BAD_REQUEST, code),
    };
    let result = state
        .service
        .call_json(call, subject, arrived, claims)
        .await;
    if result.is_error != Some(true) {
        return axum::Json(result.structured_content).into_response();
    }
    let code = result
        .structured_content
        .and_then(|value| serde_json::from_value::<fyrr_core::mcp::ToolError>(value).ok())
        .map_or(ErrorCode::StorageUnavailable, |error| error.code);
    let status = match code {
        ErrorCode::InvalidArgument => StatusCode::BAD_REQUEST,
        ErrorCode::StaleCursor => StatusCode::CONFLICT,
        ErrorCode::DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
        ErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        ErrorCode::Cancelled => CLIENT_CLOSED_REQUEST,
        ErrorCode::AuthorizationUnavailable
        | ErrorCode::StorageUnavailable
        | ErrorCode::IncompatibleIndex
        | ErrorCode::Busy => StatusCode::SERVICE_UNAVAILABLE,
    };
    retry_after((status, axum::Json(json!({"error":code.as_str()}))).into_response())
}
async fn refresh(State(state): State<HttpState>, request: Request) -> Response {
    let Some(credential) = &state.http().operator_credential else {
        unreachable!("the refresh route is registered only with an operator credential");
    };
    let Ok(expected) = credential.resolve(&ProcessEnvironment) else {
        return error(StatusCode::SERVICE_UNAVAILABLE, AuthorizationUnavailable);
    };
    if !bearer(&request).is_ok_and(|value| {
        constant_time_eq::constant_time_eq(value.as_bytes(), expected.expose().as_bytes())
    }) {
        return error(StatusCode::UNAUTHORIZED, Unauthorized);
    }
    match tokio::time::timeout(
        IO_TIMEOUT,
        axum::body::to_bytes(request.into_body(), MAX_REQUEST_BYTES),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => return error(StatusCode::PAYLOAD_TOO_LARGE, InvalidArgument),
        Err(_) => return error(StatusCode::REQUEST_TIMEOUT, DeadlineExceeded),
    }
    let mut refresh = state.refresh.lock().await;
    if refresh.is_some_and(|t| t.elapsed() < MIN_REFRESH_INTERVAL) {
        return error(StatusCode::TOO_MANY_REQUESTS, RateLimited);
    }
    *refresh = Some(Instant::now());
    match state.service.refresh().await {
        Ok(p) => axum::Json(p).into_response(),
        Err(code) => error(StatusCode::SERVICE_UNAVAILABLE, code),
    }
}
pub(crate) async fn serve(reader: crate::search::PreparedReader) -> Result<()> {
    let config = reader.config.clone();
    let h = config.reader.http.as_ref().ok_or(ConfigurationInvalid)?;
    let public: reqwest::Url = h.public_url.parse().map_err(|_| ConfigurationInvalid)?;
    let auth = config
        .identity
        .oidc
        .is_some()
        .then(|| crate::auth::Authenticator::new(&config))
        .transpose()?;
    let listener = tokio::net::TcpListener::bind(&h.listen)
        .await
        .map_err(|_| TransportUnavailable)?;
    let admission = Arc::new(admission::Admission::new(
        reader.control_store.clone(),
        reader.control_root.clone(),
        reader.identity_cache.clone(),
        reader.head_cache.clone(),
    ));
    let service = crate::serve::start(reader).await?;
    let state = HttpState {
        admission,
        service: service.clone(),
        config: config.clone(),
        public: public.clone(),
        auth,
        permits: Arc::new(Semaphore::new(MAX_REQUESTS_IN_FLIGHT)),
        controls: Arc::new(Semaphore::new(MAX_CONTROLS_IN_FLIGHT)),
        control_limits: Arc::new(Mutex::new(ControlLimits {
            subjects: BTreeMap::new(),
            tokens: BTreeMap::new(),
            attempts: (Instant::now(), 0),
        })),
        limits: Arc::new(Mutex::new(BTreeMap::new())),
        refresh: Arc::new(Mutex::new(None)),
    };

    let mut transport = StreamableHttpServerConfig::default().enforce_origin_validation();
    transport.max_request_body_bytes = MAX_REQUEST_BYTES;
    transport.legacy_session_mode = false;
    transport.json_response = true;
    transport.allowed_hosts = vec![public.authority().to_owned()];
    let mcp = StreamableHttpService::new(
        move || Ok(service.clone()),
        Arc::new(LocalSessionManager::default()),
        transport,
    );
    let mut router = Router::new()
        .route_service("/mcp", mcp)
        .route("/readyz", get(readiness));
    // Without an authorization server there is no metadata to advertise: the
    // routes are absent (404), which clients read as "no OAuth here".
    if config.identity.oidc.is_some() {
        router = router
            .route("/.well-known/oauth-protected-resource", get(metadata))
            .route("/.well-known/oauth-protected-resource/mcp", get(metadata));
    }
    router = router
        .route("/v1/namespaces/{namespace}/query", post(tool_json))
        .route("/v1/namespaces/{namespace}/fetch", post(tool_json));
    if h.operator_credential.is_some() {
        router = router.route("/admin/refresh", post(refresh));
    }
    let router = router
        .layer(middleware::from_fn_with_state(state.clone(), boundary))
        .with_state(state);
    let sockets = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let drain = tokio_util::sync::CancellationToken::new();
    let mut connections = tokio::task::JoinSet::new();
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| TransportUnavailable)?;
    loop {
        let accepted = tokio::select! {
            biased;
            _=terminate.recv()=>{drain.cancel();break;},
            Some(_)=connections.join_next(), if !connections.is_empty()=>continue,
            accepted = listener.accept() => accepted,
        };
        let (socket, _) = accepted.map_err(|_| TransportUnavailable)?;
        let Ok(permit) = sockets.clone().try_acquire_owned() else {
            drop(socket);
            continue;
        };
        connections.spawn(serve_connection(
            socket,
            router.clone(),
            drain.clone(),
            permit,
        ));
    }
    drop(listener);
    let finish = async { while connections.join_next().await.is_some() {} };
    if tokio::time::timeout(DRAIN_TIMEOUT, finish).await.is_err() {
        connections.abort_all();
    }
    Ok(())
}

async fn serve_connection(
    socket: tokio::net::TcpStream,
    router: Router,
    drain: tokio_util::sync::CancellationToken,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    let _permit = permit;
    let io = hyper_util::rt::TokioIo::new(socket);
    let service = hyper_util::service::TowerToHyperService::new(router);
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .max_buf_size(MAX_HEADER_BYTES)
        .max_headers(MAX_HEADERS)
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(IO_TIMEOUT);
    let connection = builder.serve_connection(io, service);
    tokio::pin!(connection);
    tokio::select! {
        _ = &mut connection => return,
        () = drain.cancelled() => {},
    }
    connection.as_mut().graceful_shutdown();
    let _ = connection.await;
}

async fn timeout_at<T>(
    deadline: tokio::time::Instant,
    future: impl std::future::Future<Output = T>,
) -> Result<T, Response> {
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| error(StatusCode::REQUEST_TIMEOUT, DeadlineExceeded))
}

#[cfg(test)]
#[path = "http/response_memory_tests.rs"]
mod response_memory_tests;

#[cfg(test)]
#[path = "http/scheduler_tests.rs"]
mod scheduler_tests;

#[path = "http/admission.rs"]
mod admission;
