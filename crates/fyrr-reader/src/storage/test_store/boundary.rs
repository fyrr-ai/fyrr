//! Test-only operation observations around real local storage, including CLI children.
//! The controller observes durable operations; it never implements an object store.

use base64::Engine;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::path::Path;
use object_store::*;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub location: String,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub size: u64,
    pub e_tag: Option<String>,
    pub version: Option<String>,
}
impl From<ObjectMeta> for Metadata {
    fn from(meta: ObjectMeta) -> Self {
        Self {
            location: meta.location.to_string(),
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }
    }
}
impl From<Metadata> for ObjectMeta {
    fn from(meta: Metadata) -> Self {
        Self {
            location: Path::from(meta.location),
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    pub method: String,
    pub path: String,
    pub after: bool,
    pub status: u16,
    pub body: String,
    pub objects: Option<Vec<Metadata>>,
}
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Reply {
    pub fail: bool,
    pub capture_objects: bool,
    pub body: Option<String>,
    pub objects: Option<Vec<Metadata>>,
}

#[derive(Debug, Clone)]
pub(super) struct Client(std::path::PathBuf);
impl Client {
    pub(super) fn current() -> Option<Self> {
        static CLIENT: OnceLock<Option<Client>> = OnceLock::new();
        CLIENT
            .get_or_init(|| {
                // A comment keeps test instrumentation out of the public configuration
                // schema. Every real CLI child already receives its exact --config path.
                let path = {
                    let mut arguments = std::env::args_os();
                    arguments
                        .find(|arg| arg == "--config")
                        .and_then(|_| arguments.next())?
                };
                let text = std::fs::read_to_string(path).ok()?;
                let socket = text
                    .lines()
                    .find_map(|line| line.strip_prefix("# fyrr-test-store = local-store:"))?;
                Some(Self(socket.into()))
            })
            .clone()
    }

    async fn operation(&self) -> Result<Operation> {
        tokio::net::UnixStream::connect(&self.0)
            .await
            .map(Operation)
            .map_err(|error| Error::Generic {
                store: "test operation observer",
                source: Box::new(error),
            })
    }
}

// One connection preserves before/after callback identity, just as one HTTP
// request is handled by one proxy worker. Concurrent operations stay independent.
struct Operation(tokio::net::UnixStream);
impl Operation {
    async fn observe(
        &mut self,
        method: &str,
        path: &Path,
        after: bool,
        status: u16,
        body: &[u8],
        objects: Option<Vec<Metadata>>,
    ) -> Result<Reply> {
        let action = async {
            let stream = &mut self.0;
            let event = Event {
                method: method.into(),
                path: path.to_string(),
                after,
                status,
                body: base64::engine::general_purpose::STANDARD.encode(body),
                objects,
            };
            let bytes = serde_json::to_vec(&event).map_err(std::io::Error::other)?;
            stream
                .write_u32(bytes.len().try_into().map_err(std::io::Error::other)?)
                .await?;
            stream.write_all(&bytes).await?;
            let length = stream.read_u32().await?;
            if length > 64 * 1024 * 1024 {
                return Err(std::io::Error::other("oversized test observation reply"));
            }
            let mut bytes = vec![0; length as usize];
            stream.read_exact(&mut bytes).await?;
            serde_json::from_slice::<Reply>(&bytes).map_err(std::io::Error::other)
        };
        let reply = action.await.map_err(|error| Error::Generic {
            store: "test operation observer",
            source: Box::new(error),
        })?;
        if reply.fail {
            return Err(Error::PermissionDenied {
                path: path.to_string(),
                source: Box::new(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
            });
        }
        Ok(reply)
    }
}

fn status<T>(result: &Result<T>) -> u16 {
    match result {
        Ok(_) => 200,
        Err(Error::NotFound { .. }) => 404,
        Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => 412,
        Err(Error::PermissionDenied { .. }) => 403,
        Err(_) => 500,
    }
}

#[derive(Debug)]
struct Store {
    inner: Arc<dyn ObjectStore>,
    client: Client,
}
impl std::fmt::Display for Store {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("observed local storage")
    }
}

pub(crate) fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    match Client::current() {
        Some(client) => Arc::new(Store { inner, client }),
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
        let body = if path.as_ref().ends_with(".json") {
            payload
                .iter()
                .flat_map(|chunk| chunk.iter().copied())
                .collect()
        } else {
            Vec::new()
        };
        let mut operation = self.client.operation().await?;
        operation
            .observe("PUT", path, false, 0, &body, None)
            .await?;
        let result = self.inner.put_opts(path, payload, options).await;
        // LocalStore has released its lock and finished its durable publication.
        operation
            .observe("PUT", path, true, status(&result), &body, None)
            .await?;
        result
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        let method = if options.head { "HEAD" } else { "GET" };
        let capture = !options.head && options.range.is_none() && path.as_ref().ends_with(".json");
        let mut operation = self.client.operation().await?;
        operation.observe(method, path, false, 0, &[], None).await?;
        let result = self.inner.get_opts(path, options).await;
        if !capture || result.is_err() {
            operation
                .observe(method, path, true, status(&result), &[], None)
                .await?;
            return result;
        }
        let result = result?;
        let mut meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let original = result.bytes().await?;
        let reply = operation
            .observe(method, path, true, 200, &original, None)
            .await?;
        let bytes = match reply.body {
            Some(body) => base64::engine::general_purpose::STANDARD
                .decode(body)
                .map_err(|error| Error::Generic {
                    store: "test operation observer",
                    source: Box::new(error),
                })?
                .into(),
            None => original,
        };
        meta.size = bytes.len() as u64;
        Ok(GetResult {
            range: 0..bytes.len() as u64,
            meta,
            attributes,
            extensions,
            payload: GetResultPayload::Stream(futures::stream::once(async { Ok(bytes) }).boxed()),
        })
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        let client = self.client.clone();
        paths
            .then(move |path| {
                let inner = inner.clone();
                let client = client.clone();
                async move {
                    let path = path?;
                    let mut operation = client.operation().await?;
                    operation
                        .observe("DELETE", &path, false, 0, &[], None)
                        .await?;
                    let result = inner.delete(&path).await;
                    operation
                        .observe("DELETE", &path, true, status(&result), &[], None)
                        .await?;
                    result.map(|()| path)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self.inner.clone();
        let client = self.client.clone();
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let path = prefix.clone().unwrap_or_default();
            let mut operation = client.operation().await?;
            let before = operation
                .observe("LIST", &path, false, 0, &[], None)
                .await?;
            if !before.capture_objects {
                // Counting and gates retain the underlying streaming enumeration.
                let end = futures::stream::once(async move {
                    operation
                        .observe("LIST", &path, true, 200, &[], None)
                        .await?;
                    Ok::<_, Error>(futures::stream::empty())
                })
                .try_flatten();
                return Ok::<_, Error>(inner.list(prefix.as_ref()).chain(end).boxed());
            }
            // Only explicit metadata transformations request a complete observation.
            let objects: Vec<_> = inner.list(prefix.as_ref()).try_collect().await?;
            let observed = objects.iter().cloned().map(Metadata::from).collect();
            let reply = operation
                .observe("LIST", &path, true, 200, &[], Some(observed))
                .await?;
            Ok(futures::stream::iter(match reply.objects {
                Some(objects) => objects.into_iter().map(|meta| Ok(meta.into())).collect(),
                None => objects.into_iter().map(Ok).collect::<Vec<_>>(),
            })
            .boxed())
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
pub(super) struct Native;
impl lance_io::object_store::WrappingObjectStore for Native {
    fn wrap(&self, _prefix: &str, inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        wrap(inner)
    }
    fn wrap_paginated(
        &self,
        _prefix: &str,
        _inner: Arc<dyn object_store::list::PaginatedListStore>,
    ) -> Option<Arc<dyn object_store::list::PaginatedListStore>> {
        // Route listing through the observed ObjectStore instead of bypassing it.
        None
    }
}
