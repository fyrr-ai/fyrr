//! Real S3 namespace calls; pagination is observed at its actual request seam.
use super::*;
use futures::{StreamExt, stream::BoxStream};
use object_store::list::{PaginatedListOptions, PaginatedListStore};
use object_store::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug, Default)]
struct Counts {
    phase: AtomicUsize,
    calls: Mutex<[[u64; 2]; 5]>,
    native: Mutex<[[u64; 2]; 5]>,
    delay: std::time::Duration,
}
static ACTIVE: Mutex<Option<Arc<Counts>>> = Mutex::new(None);
pub(super) fn phase(name: &str) {
    let index = match name {
        "restore" | "history_start" => 0,
        "authority_scan" | "historical_scan" => 1,
        "overlay" => 2,
        "verification_scan" => 3,
        "publication" | "index_optimization" => 4,
        _ => return,
    };
    if let Some(active) = ACTIVE.lock().unwrap().as_ref() {
        active.phase.store(index, Ordering::Relaxed);
    }
}
impl Counts {
    fn count(&self, kind: usize, native: bool) {
        let counters = if native { &self.native } else { &self.calls };
        counters.lock().unwrap()[self.phase.load(Ordering::Relaxed)][kind] += 1;
    }
}
type PageSlot = Arc<Mutex<Option<Arc<dyn PaginatedListStore>>>>;
struct Store {
    inner: Arc<dyn ObjectStore>,
    pages: PageSlot,
    native: bool,
    counts: Arc<Counts>,
}
impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("restore-counting-store")
    }
}
impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("restore-counting-store")
    }
}
#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.counts.count(1, self.native);
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = self
            .pages
            .lock()
            .unwrap()
            .clone()
            .expect("S3 paginated store registered before listing");
        let native = self.native;
        let prefix = prefix
            .filter(|p| !p.as_ref().is_empty())
            .map(|p| format!("{p}/"));
        let counts = self.counts.clone();
        futures::stream::try_unfold((prefix, None, false), move |(prefix, token, finished)| {
            let inner = inner.clone();
            let counts = counts.clone();
            async move {
                if finished {
                    return Ok::<_, object_store::Error>(None);
                }
                counts.count(0, native);
                if !native {
                    tokio::time::sleep(counts.delay).await;
                }
                let page = inner
                    .list_paginated(
                        prefix.as_deref(),
                        PaginatedListOptions {
                            page_token: token,
                            max_keys: Some(1000),
                            ..Default::default()
                        },
                    )
                    .await?;
                let finished = page.page_token.is_none();
                Ok(Some((
                    futures::stream::iter(page.result.objects.into_iter().map(Ok)),
                    (prefix, page.page_token, finished),
                )))
            }
        })
        .try_flatten()
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.counts.count(0, self.native);
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}
struct Native {
    counts: Arc<Counts>,
    pages: Mutex<BTreeMap<String, PageSlot>>,
}
impl std::fmt::Debug for Native {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("restore-native-counting-wrapper")
    }
}
impl Native {
    fn pages(&self, prefix: &str) -> PageSlot {
        self.pages
            .lock()
            .unwrap()
            .entry(prefix.into())
            .or_default()
            .clone()
    }
}
impl lance::io::WrappingObjectStore for Native {
    fn wrap(&self, prefix: &str, inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(Store {
            inner,
            pages: self.pages(prefix),
            native: true,
            counts: self.counts.clone(),
        })
    }
    fn wrap_paginated(
        &self,
        prefix: &str,
        inner: Arc<dyn PaginatedListStore>,
    ) -> Option<Arc<dyn PaginatedListStore>> {
        *self.pages(prefix).lock().unwrap() = Some(inner.clone());
        Some(Arc::new(NativePages {
            inner,
            counts: self.counts.clone(),
        }))
    }
}
struct NativePages {
    inner: Arc<dyn PaginatedListStore>,
    counts: Arc<Counts>,
}
#[async_trait::async_trait]
impl PaginatedListStore for NativePages {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> object_store::Result<object_store::list::PaginatedListResult> {
        self.counts.count(0, true);
        self.inner.list_paginated(prefix, opts).await
    }
}
struct Active;
impl Drop for Active {
    fn drop(&mut self) {
        *ACTIVE.lock().unwrap() = None;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real sync/commit fixture supplied by dense_path"]
async fn restore_counting_probe_on_real_s3() {
    let config = fyrr_core::config::load(std::path::Path::new(
        &std::env::var_os("FYRR_TEST_RESTORE_CONFIG").unwrap(),
    ))
    .unwrap()
    .config;
    let counts = Arc::new(Counts {
        delay: std::time::Duration::from_millis(200),
        ..Default::default()
    });
    *ACTIVE.lock().unwrap() = Some(counts.clone());
    let _active = Active;
    let observed = counts.clone();
    let _store = fyrr_reader::storage::test_store::register(
        &config,
        Arc::new(move |inner| {
            Arc::new(Store {
                inner: inner.clone(),
                pages: Arc::new(Mutex::new(Some(inner))),
                native: false,
                counts: observed.clone(),
            })
        }),
    );
    fyrr_reader::storage::test_store::register_native(
        &config,
        Arc::new(Native {
            counts: counts.clone(),
            pages: Default::default(),
        }),
    );
    let result = restore(
        &config,
        Some(&std::env::var("FYRR_TEST_RESTORE_TARGET").unwrap()),
    )
    .await
    .unwrap();
    let calls = *counts.calls.lock().unwrap();
    let phases = ["start", "scans", "pass", "verification", "publication"];
    let namespace: BTreeMap<_, _> = phases
        .into_iter()
        .zip(calls)
        .map(|(phase, [pages, gets])| (phase, serde_json::json!({"list_pages":pages,"gets":gets})))
        .collect();
    let lance: BTreeMap<_, _> = phases
        .into_iter()
        .zip(*counts.native.lock().unwrap())
        .map(|(phase, [pages, gets])| (phase, serde_json::json!({"list_pages":pages,"gets":gets})))
        .collect();
    std::fs::write(
        std::env::var_os("FYRR_TEST_RESTORE_COUNTS").unwrap(),
        serde_json::to_vec(
            &serde_json::json!({"namespace":namespace,"lance":lance,"result":result}),
        )
        .unwrap(),
    )
    .unwrap();
}
