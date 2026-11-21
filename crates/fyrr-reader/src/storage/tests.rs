use super::*;
#[test]
fn ec2_metadata_opt_out_preserves_other_credential_sources() {
    use AmazonS3ConfigKey as Key;
    let cases: &[(&[(AmazonS3ConfigKey, &str)], bool)] = &[
        (&[], true),
        (&[(Key::AccessKeyId, "key")], false),
        (&[(Key::SecretAccessKey, "secret")], false),
        (&[(Key::WebIdentityTokenFile, "/token")], true),
        (&[(Key::RoleArn, "role")], true),
        (
            &[
                (Key::WebIdentityTokenFile, "/token"),
                (Key::RoleArn, "role"),
            ],
            false,
        ),
        (&[(Key::ContainerCredentialsRelativeUri, "/task")], false),
        (
            &[(Key::ContainerCredentialsFullUri, "http://container")],
            true,
        ),
        (&[(Key::ContainerAuthorizationTokenFile, "/token")], true),
        (
            &[
                (Key::ContainerCredentialsFullUri, "http://container"),
                (Key::ContainerAuthorizationTokenFile, "/token"),
            ],
            false,
        ),
        (&[(Key::SkipSignature, "true")], false),
        (&[(Key::SkipSignature, "false")], true),
    ];
    for (options, expected) in cases {
        let mut builder = AmazonS3Builder::new().with_bucket_name("credentials");
        for (key, value) in *options {
            builder = builder.with_config(*key, *value);
        }
        assert_eq!(
            uses_instance_credentials(&builder),
            *expected,
            "{options:?}"
        );
    }
    // A malformed explicit source must retain its configuration error instead
    // of being mistaken for a disabled instance-profile fallback.
    assert!(
        AmazonS3Builder::new()
            .with_bucket_name("credentials")
            .with_access_key_id("key")
            .build()
            .is_err()
    );
}

/// Counts the reads of a store whose head a test replaces.
#[derive(Debug)]
struct CountedReads {
    inner: object_store::memory::InMemory,
    reads: std::sync::atomic::AtomicU64,
}
impl std::fmt::Display for CountedReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("counted reads")
    }
}
#[async_trait::async_trait]
impl ObjectStore for CountedReads {
    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
struct Heads {
    store: Arc<CountedReads>,
    root: Path,
    head: fyrr_core::publication::ServingHead,
}
impl Heads {
    async fn new() -> Self {
        let head: fyrr_core::publication::ServingHead = parse_contract(
            &std::fs::read(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../fixtures/contracts/serving-head/head.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let heads = Self {
            store: Arc::new(CountedReads {
                inner: object_store::memory::InMemory::new(),
                reads: Default::default(),
            }),
            root: Path::from("namespace"),
            head,
        };
        heads.publish(0).await;
        heads
    }
    /// Publishes the head with its policy generation advanced by `step`.
    async fn publish(&self, step: u64) -> fyrr_core::publication::ServingHead {
        let mut head = self.head.clone();
        head.policy.generation += step;
        self.store
            .inner
            .put(
                &self.root.clone().join("head.json"),
                fyrr_core::write_contract(&head).into(),
            )
            .await
            .unwrap();
        head
    }
    fn reads(&self) -> u64 {
        self.store.reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Heads {
    fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }
}

#[tokio::test(start_paused = true)]
async fn authorization_checks_share_one_head_read_per_interval() {
    let heads = Heads::new().await;
    let cache = HeadCache::with_interval(std::time::Duration::from_secs(60));
    let store = heads.store();
    // Concurrent checks that find no head share one read.
    let first = futures::future::join_all((0..8).map(|_| cache.read(&store, &heads.root))).await;
    let first = first.into_iter().next().unwrap().unwrap();
    assert_eq!(heads.reads(), 1);
    let changed = heads.publish(1).await;
    assert_eq!(cache.read(&store, &heads.root).await.unwrap(), first);
    tokio::time::advance(std::time::Duration::from_secs(59)).await;
    assert_eq!(cache.read(&store, &heads.root).await.unwrap(), first);
    assert_eq!(heads.reads(), 1);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        cache.read(&store, &heads.root).await.unwrap(),
        changed,
        "the published change is seen after one interval"
    );
    assert_eq!(heads.reads(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_zero_interval_reads_the_head_for_every_check() {
    let heads = Heads::new().await;
    let cache = HeadCache::with_interval(std::time::Duration::ZERO);
    let store = heads.store();
    cache.read(&store, &heads.root).await.unwrap();
    cache.read_fresh(store.as_ref(), &heads.root).await.unwrap();
    let changed = heads.publish(1).await;
    assert_eq!(cache.read(&store, &heads.root).await.unwrap(), changed);
    assert_eq!(heads.reads(), 3);
}

#[tokio::test(start_paused = true)]
async fn a_fresh_head_read_is_what_checks_see_next() {
    let heads = Heads::new().await;
    let cache = HeadCache::with_interval(std::time::Duration::from_secs(60));
    let store = heads.store();
    cache.read(&store, &heads.root).await.unwrap();
    // A version check, a refresh or a generation being opened reads a newer
    // head: requests see it at once, never the older one they held.
    let changed = heads.publish(1).await;
    assert_eq!(
        cache.read_fresh(store.as_ref(), &heads.root).await.unwrap(),
        changed
    );
    assert_eq!(cache.read(&store, &heads.root).await.unwrap(), changed);
    assert_eq!(heads.reads(), 2);
}

#[tokio::test(start_paused = true)]
async fn an_older_head_read_never_replaces_a_newer_one() {
    let heads = Heads::new().await;
    let cache = HeadCache::with_interval(std::time::Duration::from_secs(60));
    let store = heads.store();
    // A read that started first finishes last, with the older head.
    let slow = SlowReads {
        inner: heads.store.clone(),
        delay: std::time::Duration::from_secs(5),
    };
    let older = tokio::spawn({
        let (cache, root) = (cache.clone(), heads.root.clone());
        async move { cache.read_fresh(&slow, &root).await }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    let changed = heads.publish(1).await;
    assert_eq!(
        cache.read_fresh(store.as_ref(), &heads.root).await.unwrap(),
        changed
    );
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    assert_eq!(older.await.unwrap().unwrap(), heads.head);
    assert_eq!(cache.read(&store, &heads.root).await.unwrap(), changed);
}

#[tokio::test(start_paused = true)]
async fn a_failed_head_read_is_shared_and_not_kept() {
    let heads = Heads::new().await;
    let cache = HeadCache::with_interval(std::time::Duration::from_secs(60));
    let store = heads.store();
    cache.read(&store, &heads.root).await.unwrap();
    // A fresh read that fails drops the head held before it.
    let path = heads.root.clone().join("head.json");
    heads.store.inner.delete(&path).await.unwrap();
    assert!(cache.read_fresh(store.as_ref(), &heads.root).await.is_err());
    assert_eq!(heads.reads(), 2);
    // Waiting checks share one failed read; the next check reads again.
    let failed = futures::future::join_all((0..8).map(|_| cache.read(&store, &heads.root))).await;
    assert!(failed.iter().all(Result::is_err));
    assert_eq!(heads.reads(), 3);
    heads.publish(2).await;
    assert!(cache.read(&store, &heads.root).await.is_ok());
    assert_eq!(heads.reads(), 4);
}

#[tokio::test(start_paused = true)]
async fn an_older_read_is_not_held_after_a_newer_read_failed() {
    let heads = Heads::new().await;
    let cache = HeadCache::with_interval(std::time::Duration::from_secs(60));
    let store = heads.store();
    let slow: Arc<dyn ObjectStore> = Arc::new(SlowReads {
        inner: heads.store.clone(),
        delay: std::time::Duration::from_secs(5),
    });
    // A check shares a slow read that was answered before a publication.
    let older = tokio::spawn({
        let (cache, slow, root) = (cache.clone(), slow.clone(), heads.root.clone());
        async move { cache.read(&slow, &root).await }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    // A newer read succeeds, then one after it fails and drops the head.
    heads.publish(1).await;
    cache.read_fresh(store.as_ref(), &heads.root).await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    let path = heads.root.clone().join("head.json");
    heads.store.inner.delete(&path).await.unwrap();
    assert!(cache.read_fresh(store.as_ref(), &heads.root).await.is_err());
    let changed = heads.publish(1).await;
    // The old read ends: its head is neither returned nor held.
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    assert!(older.await.unwrap().is_err());
    assert_eq!(cache.read(&store, &heads.root).await.unwrap(), changed);
}

#[tokio::test(start_paused = true)]
async fn an_abandoned_shared_read_is_never_handed_to_a_later_check() {
    let heads = Heads::new().await;
    let cache = HeadCache::with_interval(std::time::Duration::from_secs(60));
    let slow: Arc<dyn ObjectStore> = Arc::new(SlowReads {
        inner: heads.store.clone(),
        delay: std::time::Duration::from_secs(5),
    });
    // The only check waiting for a shared read is cancelled; the read ends.
    let abandoned = tokio::spawn({
        let (cache, slow, root) = (cache.clone(), slow.clone(), heads.root.clone());
        async move { cache.read(&slow, &root).await }
    });
    tokio::task::yield_now().await;
    abandoned.abort();
    tokio::time::advance(std::time::Duration::from_secs(6)).await;
    tokio::task::yield_now().await;
    assert_eq!(heads.reads(), 1);
    // Much later the held head is gone: a check reads storage again.
    let changed = heads.publish(1).await;
    tokio::time::advance(std::time::Duration::from_secs(3600)).await;
    let store = heads.store();
    assert_eq!(cache.read(&store, &heads.root).await.unwrap(), changed);
    assert_eq!(heads.reads(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_shared_read_that_outlasts_the_age_limit_is_not_served() {
    let heads = Heads::new().await;
    let cache = HeadCache::with_interval(std::time::Duration::from_secs(60));
    let slow: Arc<dyn ObjectStore> = Arc::new(SlowReads {
        inner: heads.store.clone(),
        delay: std::time::Duration::from_secs(90),
    });
    // A check starts a read that is answered with the head as it is now, but
    // only after 90 s: longer than a head may be used for.
    let first = tokio::spawn({
        let (cache, slow, root) = (cache.clone(), slow.clone(), heads.root.clone());
        async move { cache.read(&slow, &root).await }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    // A revocation is published after the read began; a second check joins it.
    let revoked = heads.publish(1).await;
    let joined = tokio::spawn({
        let (cache, slow, root) = (cache.clone(), slow.clone(), heads.root.clone());
        async move { cache.read(&slow, &root).await }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(80)).await;
    // The read ends with a head older than the limit: neither check gets it.
    for check in [first.await.unwrap(), joined.await.unwrap()] {
        assert_eq!(check, Err(StorageError::AuthorizationUnavailable));
    }
    assert_eq!(heads.reads(), 1);
    // The next check reads again and sees the revocation.
    let store = heads.store();
    assert_eq!(cache.read(&store, &heads.root).await.unwrap(), revoked);
    assert_eq!(heads.reads(), 2);
}

#[test]
fn checks_read_the_head_themselves_after_two_version_check_intervals() {
    let config = |reader: &str| {
        fyrr_core::config::parse(&format!(
            "[namespace]\nname='heads'\n[storage]\nuri='s3://heads'\n\
             [embedding]\nendpoint='https://embed.invalid'\n{reader}"
        ))
        .unwrap()
        .config
    };
    let age = |reader: &str| HeadCache::new(&config(reader)).max_age;
    assert_eq!(age(""), std::time::Duration::from_secs(60));
    assert_eq!(
        age("[reader]\nversion_check_interval='5s'"),
        std::time::Duration::from_secs(10)
    );
    // The scheduler checks at least every 30 s whatever the setting.
    assert_eq!(
        age("[reader]\nversion_check_interval='10m'"),
        std::time::Duration::from_secs(60)
    );
}

/// Answers a head read with the object as it was when the read began, late.
#[derive(Debug)]
struct SlowReads {
    inner: Arc<CountedReads>,
    delay: std::time::Duration,
}
impl std::fmt::Display for SlowReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("slow reads")
    }
}
#[async_trait::async_trait]
impl ObjectStore for SlowReads {
    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let result = self.inner.get_opts(location, options).await;
        tokio::time::sleep(self.delay).await;
        result
    }
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
