//! Opt-in diagnostics at ObjectStore boundaries. No backend text is formatted.
use futures::{StreamExt, stream::BoxStream};
use object_store::{client::*, path::Path, *};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

#[derive(Debug)]
struct Sink(Mutex<std::fs::File>);
impl Sink {
    fn open(path: &std::path::Path) -> std::io::Result<Arc<Self>> {
        if !path.is_absolute() {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .custom_flags(
                (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
            )
            .open(path)?;
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.uid() != rustix::process::geteuid().as_raw()
            || meta.mode() & 0o077 != 0
            || meta.nlink() != 1
        {
            return Err(std::io::ErrorKind::PermissionDenied.into());
        }
        Ok(Arc::new(Self(Mutex::new(file))))
    }
    fn current() -> Option<Arc<Self>> {
        static SINK: OnceLock<Option<Arc<Sink>>> = OnceLock::new();
        SINK.get_or_init(|| {
            let path = std::env::var_os("FYRR_OBJECT_STORE_DIAGNOSTICS")?;
            match Self::open(std::path::Path::new(&path)) {
                Ok(sink) => Some(sink),
                Err(_) => {
                    crate::telemetry::emit(format_args!(
                        "telemetry object_store_diagnostics=unavailable"
                    ));
                    None
                }
            }
        })
        .clone()
    }
    fn emit(&self, record: &Record) {
        if let Ok(mut bytes) = serde_json::to_vec(record) {
            bytes.push(b'\n');
            if let Ok(mut file) = self.0.lock() {
                let _ = file.write_all(&bytes);
            }
        }
    }
}

#[derive(Debug, Default)]
struct Attempts {
    // Only hashes of HTTP request identities are retained, never URLs or headers.
    requests: BTreeMap<[u8; 32], u64>,
    overflow: bool,
    status: Option<u16>,
    transport_error: Option<&'static str>,
}
type Observations = Arc<Mutex<Attempts>>;
tokio::task_local! { static OBSERVATIONS: Observations; }

/// Collect numeric transport facts for the enclosing boundary, without logging here.
#[derive(Debug)]
struct Transport(HttpClient);
#[async_trait::async_trait]
impl HttpService for Transport {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        let observation = OBSERVATIONS.try_with(Arc::clone).ok();
        if let Some(observation) = &observation {
            let mut digest = Sha256::new();
            digest.update(request.method().as_str().as_bytes());
            digest.update([0]);
            digest.update(request.uri().to_string().as_bytes());
            if let Ok(mut facts) = observation.lock() {
                let key = digest.finalize().into();
                if let Some(count) = facts.requests.get_mut(&key) {
                    *count = count.saturating_add(1);
                } else if facts.requests.len() < 1024 {
                    facts.requests.insert(key, 1);
                } else {
                    facts.overflow = true;
                }
            }
        }
        let result = self.0.execute(request).await;
        if let Some(observation) = observation
            && let Ok(mut facts) = observation.lock()
        {
            facts.status = result.as_ref().ok().map(|r| r.status().as_u16());
            facts.transport_error = result.as_ref().err().map(|error| match error.kind() {
                HttpErrorKind::Connect => "transport_connect",
                HttpErrorKind::Request => "transport_request",
                HttpErrorKind::Timeout => "transport_timeout",
                HttpErrorKind::Interrupted => "transport_interrupted",
                HttpErrorKind::Decode => "transport_decode",
                _ => "transport_error",
            });
        }
        result
    }
}
pub(super) fn transport(client: HttpClient) -> HttpClient {
    if Sink::current().is_some() {
        HttpClient::new(Transport(client))
    } else {
        client
    }
}

#[derive(Debug, Serialize)]
struct Record {
    method: &'static str,
    key_sha256: String,
    http_status: Option<u16>,
    error_code: &'static str,
    retry_count: Option<u64>,
    latency_us: u64,
}
fn code(error: &Error) -> &'static str {
    match error {
        Error::NotFound { .. } => "not_found",
        Error::AlreadyExists { .. } => "already_exists",
        Error::Precondition { .. } => "precondition",
        Error::NotModified { .. } => "not_modified",
        Error::PermissionDenied { .. } => "permission_denied",
        Error::Unauthenticated { .. } => "unauthenticated",
        Error::NotSupported { .. } | Error::NotImplemented { .. } => "not_supported",
        Error::InvalidPath { .. } => "invalid_path",
        _ => "storage_error",
    }
}
#[derive(Debug)]
struct Operation {
    sink: Arc<Sink>,
    method: &'static str,
    key: String,
    started: Instant,
    facts: Observations,
    local: bool,
    finished: bool,
}
impl Operation {
    fn new(sink: Arc<Sink>, method: &'static str, path: &Path, local: bool) -> Self {
        Self {
            sink,
            method,
            key: hex::encode(Sha256::digest(path.as_ref().as_bytes())),
            started: Instant::now(),
            facts: Arc::default(),
            local,
            finished: false,
        }
    }
    fn record(&mut self, error_code: &'static str) {
        self.finished = true;
        let facts = self.facts.lock().ok();
        let status = facts.as_ref().and_then(|f| f.status);
        let retries = facts
            .as_ref()
            .filter(|f| !f.requests.is_empty() && !f.overflow)
            .map(|f| {
                f.requests
                    .values()
                    .map(|count| count.saturating_sub(1))
                    .sum()
            });
        self.sink.emit(&Record {
            method: self.method,
            key_sha256: self.key.clone(),
            http_status: status,
            error_code: if error_code == "storage_error"
                || (error_code == "ok" && status.is_some_and(|s| !(200..300).contains(&s)))
            {
                facts
                    .as_ref()
                    .and_then(|f| f.transport_error)
                    .unwrap_or(match status {
                        Some(304) => "not_modified",
                        Some(400) => "bad_request",
                        Some(401) => "unauthenticated",
                        Some(403) => "permission_denied",
                        Some(404) => "not_found",
                        Some(409) => "conflict",
                        Some(412) => "precondition",
                        Some(408) => "http_timeout",
                        Some(429) => "throttled",
                        Some(500) => "remote_internal",
                        Some(502) => "bad_gateway",
                        Some(503) => "service_unavailable",
                        Some(504) => "gateway_timeout",
                        Some(s) if !(200..300).contains(&s) => "http_error",
                        _ => error_code,
                    })
            } else {
                error_code
            },
            retry_count: if self.local {
                Some(0)
            } else if self.method == "DELETE_STREAM" {
                None
            } else {
                retries
            },
            latency_us: crate::telemetry::micros(self.started.elapsed()),
        });
    }
    fn finish<T>(&mut self, result: &Result<T>) {
        self.record(result.as_ref().err().map_or("ok", code));
    }
    async fn run<T>(mut self, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        let result = OBSERVATIONS.scope(self.facts.clone(), future).await;
        self.finish(&result);
        result
    }
    fn stream<T: Send + 'static>(
        self,
        mut stream: BoxStream<'static, Result<T>>,
    ) -> BoxStream<'static, Result<T>> {
        let facts = self.facts.clone();
        let mut operation = Some(self);
        futures::stream::poll_fn(move |context| {
            let next =
                OBSERVATIONS.sync_scope(facts.clone(), || stream.as_mut().poll_next(context));
            match &next {
                std::task::Poll::Ready(Some(Err(error))) => {
                    if let Some(mut op) = operation.take() {
                        op.record(code(error));
                    }
                }
                std::task::Poll::Ready(None) => {
                    if let Some(mut op) = operation.take() {
                        op.record("ok");
                    }
                }
                _ => {}
            }
            next
        })
        .boxed()
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        if !self.finished {
            self.record("cancelled");
        }
    }
}

#[derive(Debug)]
struct Store {
    inner: Arc<dyn ObjectStore>,
    sink: Arc<Sink>,
    local: bool,
    scope: Path,
}
impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("diagnostic object store")
    }
}
impl Store {
    fn operation(&self, method: &'static str, path: &Path) -> Operation {
        Operation::new(self.sink.clone(), method, path, self.local)
    }
}
pub(super) fn wrap(inner: Arc<dyn ObjectStore>, local: bool, scope: &Path) -> Arc<dyn ObjectStore> {
    match Sink::current() {
        Some(sink) => Arc::new(Store {
            inner,
            sink,
            local,
            scope: scope.clone(),
        }),
        None => inner,
    }
}
#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.operation("PUT", path)
            .run(self.inner.put_opts(path, payload, options))
            .await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = self
            .operation("MULTIPART_CREATE", path)
            .run(self.inner.put_multipart_opts(path, options))
            .await?;
        Ok(Box::new(Upload {
            inner: upload,
            sink: self.sink.clone(),
            path: path.clone(),
            local: self.local,
        }))
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        let head = options.head;
        let mut operation = self.operation(if head { "HEAD" } else { "GET" }, path);
        let result = OBSERVATIONS
            .scope(operation.facts.clone(), self.inner.get_opts(path, options))
            .await;
        match result {
            Ok(mut result) => {
                if !head && let GetResultPayload::Stream(stream) = result.payload {
                    result.payload = GetResultPayload::Stream(operation.stream(stream));
                } else {
                    operation.record("ok");
                }
                Ok(result)
            }
            Err(error) => {
                operation.record(code(&error));
                Err(error)
            }
        }
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.operation("DELETE_STREAM", &self.scope)
            .stream(self.inner.delete_stream(paths))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.operation("LIST", &prefix.cloned().unwrap_or_default())
            .stream(self.inner.list(prefix))
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.operation("LIST_OFFSET", &prefix.cloned().unwrap_or_default())
            .stream(self.inner.list_with_offset(prefix, offset))
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.operation("LIST_DELIMITER", &prefix.cloned().unwrap_or_default())
            .run(self.inner.list_with_delimiter(prefix))
            .await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.operation("COPY", to)
            .run(self.inner.copy_opts(from, to, options))
            .await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.operation("RENAME", to)
            .run(self.inner.rename_opts(from, to, options))
            .await
    }
}
#[derive(Debug)]
struct Upload {
    inner: Box<dyn MultipartUpload>,
    sink: Arc<Sink>,
    path: Path,
    local: bool,
}
#[async_trait::async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let operation = Operation::new(self.sink.clone(), "MULTIPART_PART", &self.path, self.local);
        let future = self.inner.put_part(data);
        Box::pin(operation.run(future))
    }
    async fn complete(&mut self) -> Result<PutResult> {
        Operation::new(
            self.sink.clone(),
            "MULTIPART_COMPLETE",
            &self.path,
            self.local,
        )
        .run(self.inner.complete())
        .await
    }
    async fn abort(&mut self) -> Result<()> {
        Operation::new(self.sink.clone(), "MULTIPART_ABORT", &self.path, self.local)
            .run(self.inner.abort())
            .await
    }
}
pub(super) async fn observe<T>(
    method: &'static str,
    path: &Path,
    local: bool,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    match Sink::current() {
        Some(sink) => Operation::new(sink, method, path, local).run(future).await,
        None => future.await,
    }
}

#[derive(Debug)]
pub(super) struct Native;
impl lance_io::object_store::WrappingObjectStore for Native {
    fn wrap(&self, prefix: &str, inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        // Lance owns its native HTTP transport; unavailable retry/status facts stay null.
        wrap(inner, false, &Path::from(prefix))
    }
    fn wrap_paginated(
        &self,
        _prefix: &str,
        _inner: Arc<dyn object_store::list::PaginatedListStore>,
    ) -> Option<Arc<dyn object_store::list::PaginatedListStore>> {
        None
    }
}
pub(super) fn native_wrapper(
    mirror: Option<Arc<dyn lance_io::object_store::WrappingObjectStore>>,
) -> Option<Arc<dyn lance_io::object_store::WrappingObjectStore>> {
    if Sink::current().is_none() {
        return mirror;
    }
    let mut wrappers: Vec<Arc<dyn lance_io::object_store::WrappingObjectStore>> =
        vec![Arc::new(Native)];
    wrappers.extend(mirror);
    Some(Arc::new(
        lance_io::object_store::ChainedWrappingObjectStore::new(wrappers),
    ))
}

#[cfg(test)]
#[path = "diagnostics/tests.rs"]
mod tests;
