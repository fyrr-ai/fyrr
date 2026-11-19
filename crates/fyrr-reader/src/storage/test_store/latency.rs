//! A read-latency object store for tests of request count, bytes and dependency depth.
//!
//! Every read waits a fixed latency on the tokio clock, so a test that pauses
//! the clock measures sequential rounds as virtual elapsed time over the latency,
//! independent of host speed. Each read is recorded with its start and end.
//! Writes pass through without delay and are not recorded. Never part of a
//! shipping build (see the `test-store` feature).
use futures::stream::BoxStream;
use lance_io::object_store::WrappingObjectStore;
use object_store::list::PaginatedListStore;
use object_store::path::Path;
use object_store::*;
use std::fmt::{Display, Formatter};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;

/// One recorded read: its location, the range it asked for (`None` for the whole
/// object or a head request), the bytes it returned and when it started and ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Read {
    pub path: String,
    pub range: Option<GetRange>,
    pub head: bool,
    pub bytes: u64,
    pub start: Instant,
    pub end: Instant,
}

type Observer = dyn Fn(&Read) + Send + Sync;

#[derive(Default)]
struct Record {
    reads: Mutex<Vec<Read>>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

/// Decrements the in-flight count even when the read future is dropped.
struct Flight<'a>(&'a Record);
impl<'a> Flight<'a> {
    fn begin(record: &'a Record) -> Self {
        let now = record.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        record.peak.fetch_max(now, Ordering::SeqCst);
        Self(record)
    }
}
impl Drop for Flight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    latency: Duration,
    record: Arc<Record>,
    observer: Option<Arc<Observer>>,
}

impl std::fmt::Debug for LatencyStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyStore")
            .field("latency", &self.latency)
            .finish_non_exhaustive()
    }
}
impl Display for LatencyStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fyrr latency store ({:?})", self.latency)
    }
}

impl LatencyStore {
    pub fn new(inner: Arc<dyn ObjectStore>, latency: Duration) -> Self {
        Self {
            inner,
            latency,
            record: Arc::default(),
            observer: None,
        }
    }
    /// Call `observer` with each read as it completes, before its caller resumes.
    #[must_use]
    pub fn observed(mut self, observer: impl Fn(&Read) + Send + Sync + 'static) -> Self {
        self.observer = Some(Arc::new(observer));
        self
    }
    pub fn latency(&self) -> Duration {
        self.latency
    }
    pub fn reads(&self) -> Vec<Read> {
        self.record.reads.lock().expect("latency record").clone()
    }
    pub fn requests(&self) -> usize {
        self.record.reads.lock().expect("latency record").len()
    }
    /// Bytes the reads returned, a head request counting none.
    pub fn bytes(&self) -> u64 {
        self.reads().iter().map(|read| read.bytes).sum()
    }
    /// The largest number of reads that were waiting or running at once.
    pub fn peak_in_flight(&self) -> usize {
        self.record.peak.load(Ordering::SeqCst)
    }
    /// Sequential rounds `elapsed` amounts to, rounded to the nearest whole round.
    pub fn rounds(&self, elapsed: Duration) -> u128 {
        let latency = self.latency.as_nanos().max(1);
        (elapsed.as_nanos() + latency / 2) / latency
    }
    async fn read<T>(
        &self,
        path: &str,
        range: Option<GetRange>,
        head: bool,
        run: impl std::future::Future<Output = Result<(T, u64)>>,
    ) -> Result<T> {
        let _flight = Flight::begin(&self.record);
        let start = Instant::now();
        tokio::time::sleep(self.latency).await;
        let outcome = run.await;
        let read = Read {
            path: path.to_owned(),
            range,
            head,
            bytes: outcome.as_ref().map_or(0, |(_, bytes)| *bytes),
            start,
            end: Instant::now(),
        };
        if let Some(observer) = &self.observer {
            observer(&read);
        }
        self.record.reads.lock().expect("latency record").push(read);
        outcome.map(|(value, _)| value)
    }
}

impl WrappingObjectStore for LatencyStore {
    fn wrap(&self, _: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(Self {
            inner: original,
            ..self.clone()
        })
    }
    fn wrap_paginated(
        &self,
        _: &str,
        _: Arc<dyn PaginatedListStore>,
    ) -> Option<Arc<dyn PaginatedListStore>> {
        None
    }
}

#[async_trait::async_trait]
impl ObjectStore for LatencyStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let (range, head) = (options.range.clone(), options.head);
        self.read(location.as_ref(), range, head, async {
            let result = self.inner.get_opts(location, options).await?;
            let bytes = if head {
                0
            } else {
                result.range.end - result.range.start
            };
            Ok((result, bytes))
        })
        .await
    }
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        o: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, o).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let name = prefix.map_or(String::new(), ToString::to_string);
        self.read(&name, None, false, async {
            Ok((self.inner.list_with_delimiter(prefix).await?, 0))
        })
        .await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
