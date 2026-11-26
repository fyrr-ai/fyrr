use super::*;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
    PutPayload, PutResult,
};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

/// The delegated PUT can finish even if its caller drops the response, as with
/// LocalStore's blocking worker or an S3 request already received by the server.
#[derive(Debug)]
struct DelayedStore {
    inner: Arc<dyn ObjectStore>,
    armed: AtomicBool,
    /// Whether the held PUT is written at once and only its answer waits.
    written_first: bool,
    entered: Arc<Notify>,
    resume: Arc<Notify>,
}

impl std::fmt::Display for DelayedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("held real-store write")
    }
}

#[async_trait::async_trait]
impl ObjectStore for DelayedStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return self.inner.put_opts(path, payload, options).await;
        }
        assert!(matches!(options.mode, PutMode::Update(_)));
        let (inner, path) = (self.inner.clone(), path.clone());
        let (entered, resume) = (self.entered.clone(), self.resume.clone());
        let written_first = self.written_first;
        tokio::spawn(async move {
            entered.notify_one();
            if written_first {
                let written = inner.put_opts(&path, payload, options).await;
                resume.notified().await;
                return written;
            }
            resume.notified().await;
            inner.put_opts(&path, payload, options).await
        })
        .await
        .unwrap()
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

pub(crate) fn delayed_store(
    inner: Arc<dyn ObjectStore>,
) -> (Arc<dyn ObjectStore>, Arc<Notify>, Arc<Notify>) {
    held_store(inner, false)
}

fn held_store(
    inner: Arc<dyn ObjectStore>,
    written_first: bool,
) -> (Arc<dyn ObjectStore>, Arc<Notify>, Arc<Notify>) {
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let store = Arc::new(DelayedStore {
        inner,
        armed: AtomicBool::new(true),
        written_first,
        entered: entered.clone(),
        resume: resume.clone(),
    });
    (store, entered, resume)
}

pub(crate) async fn finish_held_renewal(
    renewal: Renewal,
    entered: Arc<Notify>,
    resume: Arc<Notify>,
) {
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("renewal reached the conditional write");
    let stopping = renewal.stop();
    tokio::pin!(stopping);
    // The gate acknowledgement establishes an in-flight request; shutdown must
    // remain pending while that explicitly held request has no response.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut stopping)
            .await
            .is_err(),
        "stop cancelled an in-flight renewal"
    );
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(10), stopping)
        .await
        .expect("renewal completed after its gate opened")
        .unwrap();
}

type Gate = (Arc<Notify>, Arc<Notify>);
/// Armed by one test: its publication announces that the flush of observed
/// objects is done, then waits, standing in for a flush that takes long.
static FLUSH_GATE: std::sync::Mutex<Option<Gate>> = std::sync::Mutex::new(None);

pub(super) async fn flushed() {
    let gate = FLUSH_GATE.lock().unwrap().take();
    if let Some((entered, resume)) = gate {
        entered.notify_one();
        resume.notified().await;
    }
}

/// A publication flushes what it observed before it takes the lease state.
/// While that flush is slow, a renewal still gets the state and the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_observed_flush_does_not_block_renewal_on_local_storage() {
    let (config, root) = local_config("flush-renewal");
    let mut head: ServingHead = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/serving-head/head.json"
    ))
    .unwrap();
    head.namespace = config.namespace.name.clone();
    head.lease = None;
    let (store, namespace) = namespace_store(&config).unwrap();
    cas_put(
        store.as_ref(),
        &namespace.join("head.json"),
        &head,
        PutMode::Create,
    )
    .await
    .unwrap();
    let lease = LeaseGuard::acquire(&config).await.unwrap();
    let (entered, resume) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    *FLUSH_GATE.lock().unwrap() = Some((entered.clone(), resume.clone()));
    let publishing = {
        let (lease, head) = (lease.clone(), lease.head().await.unwrap());
        tokio::spawn(async move { lease.publish(head).await })
    };
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("the publication reached its flush");
    // The publication is held at its flush. One renewal runs to completion.
    let renewed = Arc::new(AtomicBool::new(false));
    let renewal = {
        let (guard, renewed) = (lease.clone(), renewed.clone());
        tokio::spawn(async move {
            let mut state = guard.state.lock().await;
            let head = state.head.clone();
            let written = guard.write(&mut state, head, HeadWriteStage::Renew).await;
            renewed.store(written.is_ok(), Ordering::SeqCst);
        })
    };
    tokio::time::timeout(Duration::from_secs(10), renewal)
        .await
        .expect("a renewal waited for the publication's flush")
        .unwrap();
    assert!(renewed.load(Ordering::SeqCst));
    resume.notify_one();
    publishing
        .await
        .unwrap()
        .expect("the publication completes");
    lease.release().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

pub(crate) fn local_config(name: &str) -> (Config, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "fyrr-{name}-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_nanos_opt().unwrap()
    ));
    let config = fyrr_core::config::parse(&format!(
        "[namespace]\nname='{name}'\n[storage]\nuri='file://{}'\n\
         [embedding]\nendpoint='http://127.0.0.1:9/v1'\n\
         [writer.commit]\nlease_ttl='3s'\n",
        root.display()
    ))
    .unwrap()
    .config;
    (config, root)
}

/// Publishes a namespace: a source takes a cursor only in one that exists.
pub(crate) async fn put_head(config: &Config) -> ServingHead {
    let mut head: ServingHead = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/serving-head/head.json"
    ))
    .unwrap();
    head.namespace = config.namespace.name.clone();
    head.lease = None;
    let (store, root) = namespace_store(config).unwrap();
    let path = root.join("head.json");
    cas_put(store.as_ref(), &path, &head, PutMode::Create)
        .await
        .unwrap();
    head
}

async fn namespace_renewal(config: &Config) {
    let head = put_head(config).await;
    let path = namespace_store(config).unwrap().1.join("head.json");
    let mut lease = LeaseGuard::acquire(config).await.unwrap();
    let (store, entered, resume) = delayed_store(lease.store.clone());
    Arc::get_mut(&mut lease).unwrap().store = store;
    finish_held_renewal(lease.keepalive(), entered, resume).await;
    lease.release().await.unwrap();
    let released: ServingHead = fyrr_reader::storage::read_contract(lease.store.as_ref(), &path)
        .await
        .unwrap();
    assert_eq!(released.content, head.content);
    assert_eq!(released.policy, head.policy);
    assert_eq!(released.identity, head.identity);
    let next = LeaseGuard::acquire(config)
        .await
        .expect("a late renewal must not resurrect a released lease");
    assert!(next.holder().epoch > lease.holder().epoch);
    next.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namespace_renewal_finishes_before_release_on_local_storage() {
    let (config, root) = local_config("namespace-renewal");
    namespace_renewal(&config).await;
    std::fs::remove_dir_all(root).unwrap();
}

/// A renewal whose write takes longer than the renewal interval, but less than
/// the TTL, as on a busy disk, keeps the lease: the next renewal starts as soon
/// as it ends, not an interval later, past the expiry it wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_renewal_within_the_ttl_keeps_the_lease_on_local_storage() {
    let (config, root) = local_config("slow-renewal");
    put_head(&config).await;
    let mut lease = LeaseGuard::acquire(&config).await.unwrap();
    // The held renewal is written at once; only its answer waits, so a slow
    // disk adds nothing to the hold unless the write outlasts it.
    let (store, entered, resume) = held_store(lease.store.clone(), true);
    Arc::get_mut(&mut lease).unwrap().store = store;
    // TTL 3 s: renewals every 1 s. The first one starts at about 1 s, writes
    // an expiry of about 4 s and is answered 2.1 s later. The next renewal
    // starts then; one an interval later would come after the expiry.
    let renewal = lease.keepalive();
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("the first renewal reached its write");
    let held = tokio::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(2100)).await;
    resume.notify_one();
    // Past the expiry the slow renewal wrote, the lease is live again once the
    // next renewal's write is answered; a lost lease never is.
    tokio::time::sleep_until(held + Duration::from_millis(3300)).await;
    let renewed = tokio::time::timeout(Duration::from_secs(10), async {
        while lease.head().await.is_err() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(renewed.is_ok(), "the lease is renewed after the slow write");
    renewal.stop().await.unwrap();
    lease.release().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 conditional writes; scripts/ci"]
async fn renewals_finish_before_release_on_real_s3() {
    let (mut config, _) = local_config("s3-renewal");
    config.storage.uri = format!(
        "s3://{}/renewal-{}-{}",
        std::env::var("FYRR_TEST_S3_BUCKET").unwrap(),
        std::process::id(),
        fyrr_core::clock::now().timestamp_nanos_opt().unwrap()
    );
    config.storage.endpoint = Some(std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap());
    config.storage.allow_http = true;
    config.storage.access_key_id = Some(fyrr_core::config::SecretRef {
        env: "FYRR_TEST_S3_ACCESS_KEY_ID".into(),
    });
    config.storage.secret_access_key = Some(fyrr_core::config::SecretRef {
        env: "FYRR_TEST_S3_SECRET_ACCESS_KEY".into(),
    });
    namespace_renewal(&config).await;
    crate::sync_lease::tests::renewal_finishes_before_release(&config).await;
}
