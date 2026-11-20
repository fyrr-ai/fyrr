//! HTTP principal admission shares the reader's control store.
use super::*;
use fyrr_core::identity::IdentityArtifact;
use fyrr_core::publication::{GenerationPointer, ServingHead};
use object_store::{ObjectStore, path::Path};

/// A key unknown to the cached identity makes the reader look at the serving
/// head again at most this often, one lookup at a time; every other unknown key
/// in the interval is refused from memory. It is also how long a newly
/// published key can wait before it is usable.
pub(super) const API_KEY_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// A refresh that has not finished by now is given up, so that one hung read
/// cannot stop every later refresh.
const API_KEY_REFRESH_LIMIT: std::time::Duration = std::time::Duration::from_secs(30);

/// The refresh of the cached identity for unknown API keys. It runs on a task
/// of its own, so that no caller's deadline cancels it.
#[derive(Debug, Default)]
struct KeyRefresh {
    /// When the last refresh started: the interval counts from there, whether
    /// that refresh succeeded, failed or is still running.
    started: Option<tokio::time::Instant>,
    /// Closes when the running refresh task ends, however it ends.
    running: Option<tokio::sync::watch::Receiver<()>>,
}

#[derive(Debug)]
pub(super) struct Admission {
    store: Arc<dyn ObjectStore>,
    root: Path,
    cached_identity: crate::storage::ArtifactCache<IdentityArtifact>,
    key_refresh: std::sync::Mutex<KeyRefresh>,
    key_refresh_interval: std::time::Duration,
}
impl Admission {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        root: Path,
        cached_identity: crate::storage::ArtifactCache<IdentityArtifact>,
    ) -> Self {
        Self {
            store,
            root,
            cached_identity,
            key_refresh: std::sync::Mutex::default(),
            key_refresh_interval: API_KEY_REFRESH_INTERVAL,
        }
    }
    async fn read_head(store: &dyn ObjectStore, root: &Path) -> Result<ServingHead, ReaderError> {
        crate::storage::read_contract(store, &root.clone().join("head.json"))
            .await
            .map_err(Into::into)
    }
    pub(super) async fn head(&self) -> Result<ServingHead, ReaderError> {
        Self::read_head(self.store.as_ref(), &self.root).await
    }
    /// The identity `pointer` names, if it belongs to `namespace`.
    async fn fetch_identity(
        store: &dyn ObjectStore,
        root: &Path,
        namespace: &str,
        pointer: &GenerationPointer,
    ) -> Result<Arc<IdentityArtifact>, ReaderError> {
        let identity: IdentityArtifact = crate::storage::artifact(store, root, pointer).await?;
        if identity.namespace != namespace || identity.generation != pointer.generation {
            return Err(AuthorizationUnavailable);
        }
        Ok(Arc::new(identity))
    }
    pub(super) async fn identity(
        &self,
        config: &Config,
        pointer: &GenerationPointer,
    ) -> Result<Arc<IdentityArtifact>, ReaderError> {
        let mut cached = self.cached_identity.lock().await;
        if let Some((bound, identity)) = cached.as_ref()
            && bound == pointer
        {
            if identity.namespace != config.namespace.name
                || identity.generation != pointer.generation
            {
                return Err(AuthorizationUnavailable);
            }
            return Ok(identity.clone());
        }
        let identity = Self::fetch_identity(
            self.store.as_ref(),
            &self.root,
            &config.namespace.name,
            pointer,
        )
        .await?;
        *cached = Some((pointer.clone(), identity.clone()));
        Ok(identity)
    }
    /// Whether the cached identity binds `subject` and the binding admits it at `now`.
    async fn cached_binding(&self, subject: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
        self.cached_identity
            .lock()
            .await
            .as_ref()
            .is_some_and(|(_, identity)| identity.binding_for_subject(subject, now).is_some())
    }

    /// Looks at the serving head and loads a newer identity into the cache, if
    /// the interval since the last such lookup started has passed or one is
    /// running, and waits for it until `deadline`. The lookup runs on its own
    /// task to its end, whatever becomes of the caller; it does not hold the
    /// cache during its reads, so that a slow read delays no other lookup.
    async fn refresh_for_unknown_key(&self, config: &Config, deadline: tokio::time::Instant) {
        let waiting = {
            let mut refresh = crate::sync::lock(&self.key_refresh);
            let running = refresh
                .running
                .clone()
                .filter(|running| running.has_changed().is_ok());
            if running.is_none()
                && refresh
                    .started
                    .is_some_and(|at| at.elapsed() < self.key_refresh_interval)
            {
                return;
            }
            running.unwrap_or_else(|| {
                refresh.started = Some(tokio::time::Instant::now());
                let (done, running) = tokio::sync::watch::channel(());
                let store = self.store.clone();
                let root = self.root.clone();
                let cache = self.cached_identity.clone();
                let namespace = config.namespace.name.clone();
                tokio::spawn(async move {
                    // A failed lookup counts as one: unknown keys are refused either way.
                    let _ = tokio::time::timeout(API_KEY_REFRESH_LIMIT, async {
                        let head = Self::read_head(store.as_ref(), &root).await?;
                        if cache
                            .lock()
                            .await
                            .as_ref()
                            .is_some_and(|(bound, _)| *bound == head.identity)
                        {
                            return Ok(());
                        }
                        let identity =
                            Self::fetch_identity(store.as_ref(), &root, &namespace, &head.identity)
                                .await?;
                        *cache.lock().await = Some((head.identity, identity));
                        Ok::<_, ReaderError>(())
                    })
                    .await;
                    drop(done);
                });
                refresh.running = Some(running.clone());
                running
            })
        };
        let mut waiting = waiting;
        // Closed when the task ends; a caller that runs out of time goes on
        // without the new identity and finds the key unbound.
        let _ = tokio::time::timeout_at(deadline, waiting.changed()).await;
    }

    /// Admission of an API key. The binding lookup comes before every other
    /// decision, so an unbound key is the same `Unauthorized` whatever the
    /// directory's or storage's state, and costs no storage read beyond the
    /// bounded refresh. Only a key the identity binds can be told
    /// `AuthorizationUnavailable`.
    async fn authorize_api_key(
        &self,
        config: &Config,
        subject: &mut crate::auth::AuthenticatedSubject,
        deadline: tokio::time::Instant,
    ) -> Result<Admitted, ReaderError> {
        if !self
            .cached_binding(&subject.key, fyrr_core::clock::now())
            .await
        {
            self.refresh_for_unknown_key(config, deadline).await;
            if !self
                .cached_binding(&subject.key, fyrr_core::clock::now())
                .await
            {
                return Err(Unauthorized);
            }
        }
        // A bound key is checked against the serving head on every request, so
        // a removal is seen at once. The binding lookup precedes the namespace
        // and directory checks: they cannot tell an unbound key anything.
        let head = self.head().await?;
        let identity = self.identity(config, &head.identity).await?;
        let now = fyrr_core::clock::now();
        let (binding, principal) = identity
            .binding_for_subject(&subject.key, now)
            .ok_or(Unauthorized)?;
        Self::live(config, subject, &head, now)?;
        // From here on every expiry check of the request, up to the one before
        // a response leaves, sees the binding's own expiry (whole seconds).
        subject.expires = binding.expires.map_or(i64::MAX, |at| at.timestamp());
        Self::grants(config, &identity, principal, &head)
    }

    pub(super) async fn authorize(
        &self,
        config: &Config,
        subject: &mut crate::auth::AuthenticatedSubject,
        deadline: tokio::time::Instant,
    ) -> Result<Admitted, ReaderError> {
        if subject.api_key {
            return self.authorize_api_key(config, subject, deadline).await;
        }
        let head = self.head().await?;
        let now = fyrr_core::clock::now();
        Self::live(config, subject, &head, now)?;
        let identity = self.identity(config, &head.identity).await?;
        let (binding, principal) = identity
            .binding_for_subject(&subject.key, now)
            .ok_or(AuthorizationUnavailable)?;
        // A binding with an expiry shortens the token's own, whatever the subject.
        if let Some(at) = binding.expires {
            subject.expires = subject.expires.min(at.timestamp());
        }
        Self::grants(config, &identity, principal, &head)
    }

    /// The serving head's namespace, the subject's own expiry and the
    /// directory's freshness: what must hold for any subject at `now`.
    fn live(
        config: &Config,
        subject: &crate::auth::AuthenticatedSubject,
        head: &ServingHead,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), ReaderError> {
        if head.namespace != config.namespace.name
            || subject.expires <= now.timestamp()
            || !crate::storage::directory_fresh(config, head.freshness.directory, now)
        {
            return Err(AuthorizationUnavailable);
        }
        Ok(())
    }

    /// The principal's key and its groups' tokens, if its grants are within bounds.
    fn grants(
        config: &Config,
        identity: &IdentityArtifact,
        principal: &fyrr_core::identity::Principal,
        head: &ServingHead,
    ) -> Result<Admitted, ReaderError> {
        if principal.tokens.is_empty()
            || principal.tokens.len() > config.identity.max_tokens_per_user as usize
        {
            return Err(AuthorizationUnavailable);
        }
        let groups = principal
            .groups
            .iter()
            .map(|i| {
                identity
                    .tokens
                    .get(*i as usize)
                    .cloned()
                    .ok_or(AuthorizationUnavailable)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((principal.key.clone(), groups, head.identity.clone()))
    }
}

/// The principal's key, its groups' tokens and the identity generation that bound it.
pub(super) type Admitted = (String, Vec<String>, GenerationPointer);

#[cfg(test)]
mod tests {
    use super::*;
    use fyrr_core::{Contract, parse_contract, write_contract};
    use lance_io::object_store::WrappingObjectStore;
    use object_store::ObjectStoreExt;

    fn later() -> tokio::time::Instant {
        tokio::time::Instant::now() + std::time::Duration::from_secs(60)
    }
    struct Fixture {
        config: Config,
        head: ServingHead,
        identity: IdentityArtifact,
        subject: crate::auth::AuthenticatedSubject,
        remote: Arc<object_store::memory::InMemory>,
        mirror: crate::mirror::MirrorStore,
        admission: Admission,
        directory: std::path::PathBuf,
    }
    fn fixture<T: Contract>(path: &str) -> T {
        parse_contract(
            &std::fs::read(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../fixtures/contracts")
                    .join(path),
            )
            .unwrap(),
        )
        .unwrap()
    }
    impl Fixture {
        async fn new() -> Self {
            let mut config = fyrr_core::config::parse(concat!(
                "[namespace]\nname='acme'\n",
                "[storage]\nuri='s3://admission'\n",
                "[embedding]\nendpoint='https://embed.invalid'\n",
            ))
            .unwrap()
            .config;
            config.identity.directory.provider = fyrr_core::config::DirectoryProvider::None;
            let mut head: ServingHead = fixture("serving-head/head.json");
            head.freshness.directory = None;
            let mut identity: IdentityArtifact = fixture("identity-artifact/artifact.json");
            let key = format!("key:{}", "9".repeat(64));
            identity.subjects = vec![fyrr_core::identity::SubjectBinding {
                subject: key.clone(),
                principal: identity.principals[0].key.clone(),
                expires: None,
            }];
            identity.principals[0].groups = vec![1];
            let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../../target/http-admission-{}-{}",
                std::process::id(),
                fyrr_core::clock::now().timestamp_nanos_opt().unwrap()
            ));
            let mirror = crate::mirror::MirrorStore::new(directory.clone(), 1024 * 1024).unwrap();
            let remote = Arc::new(object_store::memory::InMemory::new());
            let mut admission = Admission::new(
                mirror.wrap("control", remote.clone()),
                Path::from("namespace"),
                Arc::default(),
            );
            // Most tests publish a key and use it at once.
            admission.key_refresh_interval = std::time::Duration::ZERO;
            let mut test = Self {
                config,
                head,
                identity,
                remote,
                mirror,
                admission,
                directory,
                subject: crate::auth::AuthenticatedSubject {
                    key,
                    identity: None,
                    expires: fyrr_core::clock::now().timestamp() + 600,
                    api_key: false,
                },
            };
            test.publish_identity().await;
            test
        }
        async fn publish_head(&self) {
            self.remote
                .put(
                    &Path::from("namespace/head.json"),
                    write_contract(&self.head).into(),
                )
                .await
                .unwrap();
        }
        async fn publish_identity(&mut self) {
            let bytes = write_contract(&self.identity);
            self.head.identity = GenerationPointer {
                generation: self.identity.generation,
                object: format!("identity/generation-{}.json", self.identity.generation),
                digest: crate::embedding::digest(&bytes),
            };
            self.remote
                .put(
                    &Path::from(format!("namespace/{}", self.head.identity.object)),
                    bytes.into(),
                )
                .await
                .unwrap();
            self.publish_head().await;
        }
        async fn authorize(&self) -> Result<Admitted, ReaderError> {
            self.admission
                .authorize(&self.config, &mut self.subject.clone(), later())
                .await
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.directory).unwrap();
        }
    }

    #[tokio::test]
    async fn repeated_admission_reads_fresh_head_without_refetching_identity() {
        let test = Fixture::new().await;
        let first = test.authorize().await.unwrap();
        assert_eq!(first.1, [test.identity.tokens[1].clone()]);
        let reads = test.mirror.totals().requests;
        assert_eq!(test.authorize().await.unwrap(), first);
        assert_eq!(
            test.mirror.totals().requests - reads,
            1,
            "only the mutable head is read again"
        );
        let shared = Admission::new(
            test.admission.store.clone(),
            test.admission.root.clone(),
            test.admission.cached_identity.clone(),
        );
        let before = test.mirror.totals().requests;
        let identity = shared
            .identity(&test.config, &test.head.identity)
            .await
            .unwrap();
        let cached = test
            .admission
            .identity(&test.config, &test.head.identity)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&identity, &cached));
        assert_eq!(
            test.mirror.totals().requests,
            before,
            "shared generation adds no artifact GET"
        );
    }

    #[tokio::test]
    async fn changed_identity_binding_is_observed_on_next_request() {
        let mut test = Fixture::new().await;
        test.authorize().await.unwrap();
        test.identity.subjects.clear();
        test.identity.generation += 1;
        test.publish_identity().await;
        assert_eq!(test.authorize().await, Err(AuthorizationUnavailable));
    }

    #[tokio::test]
    async fn an_api_key_is_unauthenticated_unless_its_binding_exists_and_has_not_expired() {
        let mut test = Fixture::new().await;
        test.subject.api_key = true;
        test.authorize().await.unwrap();
        // An unknown and a removed secret are the same caller; an unbound OIDC
        // subject stays an authorization problem.
        let unknown = format!("key:{}", "8".repeat(64));
        let mut other = test.subject.clone();
        other.key = unknown;
        assert_eq!(
            test.admission
                .authorize(&test.config, &mut other, later())
                .await,
            Err(Unauthorized)
        );
        other.api_key = false;
        assert_eq!(
            test.admission
                .authorize(&test.config, &mut other, later())
                .await,
            Err(AuthorizationUnavailable)
        );
        test.identity.subjects.clear();
        test.identity.generation += 1;
        test.publish_identity().await;
        assert_eq!(test.authorize().await, Err(Unauthorized));
        // The expiry is read from the published binding on every request.
        let soon = fyrr_core::clock::now() + chrono::Duration::milliseconds(700);
        test.identity.subjects = vec![fyrr_core::identity::SubjectBinding {
            subject: test.subject.key.clone(),
            principal: test.identity.principals[0].key.clone(),
            expires: Some(soon),
        }];
        test.identity.generation += 1;
        test.publish_identity().await;
        test.authorize().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        assert_eq!(test.authorize().await, Err(Unauthorized));
    }

    fn binding(
        test: &Fixture,
        expires: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Vec<fyrr_core::identity::SubjectBinding> {
        vec![fyrr_core::identity::SubjectBinding {
            subject: test.subject.key.clone(),
            principal: test.identity.principals[0].key.clone(),
            expires,
        }]
    }

    #[tokio::test]
    async fn admission_carries_the_bindings_expiry_into_the_subject() {
        use chrono::SubsecRound;
        let mut test = Fixture::new().await;
        test.subject.api_key = true;
        let at = (fyrr_core::clock::now() + chrono::Duration::seconds(300)).trunc_subsecs(0);
        test.identity.subjects = binding(&test, Some(at));
        test.identity.generation += 1;
        test.publish_identity().await;
        let mut subject = test.subject.clone();
        assert_eq!(subject.expires, test.subject.expires);
        test.admission
            .authorize(&test.config, &mut subject, later())
            .await
            .unwrap();
        // Every later expiry check of the request now sees the key's own expiry.
        assert_eq!(subject.expires, at.timestamp());
        // A key without an expiry never expires; a token's own expiry is not replaced.
        test.identity.subjects = binding(&test, None);
        test.identity.generation += 1;
        test.publish_identity().await;
        let mut subject = test.subject.clone();
        test.admission
            .authorize(&test.config, &mut subject, later())
            .await
            .unwrap();
        assert_eq!(subject.expires, i64::MAX);
        let mut oidc = test.subject.clone();
        oidc.api_key = false;
        let own = oidc.expires;
        test.admission
            .authorize(&test.config, &mut oidc, later())
            .await
            .unwrap();
        assert_eq!(oidc.expires, own);
    }

    #[tokio::test]
    async fn an_unknown_api_key_costs_no_storage_read_beyond_one_refresh_per_interval() {
        let mut test = Fixture::new().await;
        test.admission.key_refresh_interval = std::time::Duration::from_millis(400);
        let mut unknown = test.subject.clone();
        unknown.api_key = true;
        unknown.key = format!("key:{}", "7".repeat(64));
        let started = std::time::Instant::now();
        let before = test.mirror.totals().requests;
        for _ in 0..50 {
            assert_eq!(
                test.admission
                    .authorize(&test.config, &mut unknown, later())
                    .await,
                Err(Unauthorized)
            );
        }
        let refreshes = 1 + started.elapsed().as_millis() / 400;
        let reads = test.mirror.totals().requests - before;
        // One refresh is the head and, the first time, the identity artifact.
        assert!(
            u128::from(reads) <= 2 * refreshes,
            "{reads} storage requests for 50 unknown keys in {:?}",
            started.elapsed()
        );
        // A key published now is unknown until the interval has passed ...
        test.subject.api_key = true;
        test.subject.key = format!("key:{}", "5".repeat(64));
        test.identity.subjects = binding(&test, None);
        test.identity.generation += 1;
        test.publish_identity().await;
        let before = test.mirror.totals().requests;
        let waiting = test.authorize().await;
        if started.elapsed() < std::time::Duration::from_millis(350) {
            assert_eq!(waiting, Err(Unauthorized));
            assert_eq!(
                test.mirror.totals().requests,
                before,
                "no read inside the interval"
            );
        }
        // ... and then the next request looks again and admits it.
        tokio::time::sleep(std::time::Duration::from_millis(450)).await;
        test.authorize().await.unwrap();
    }

    #[cfg(feature = "test-store")]
    /// A fixture whose storage reads take `latency` on the paused tokio clock,
    /// with a bound API key that the cached identity does not know yet.
    async fn slow(latency: std::time::Duration, interval: std::time::Duration) -> Fixture {
        let mut test = Fixture::new().await;
        test.admission.store = Arc::new(crate::storage::test_store::latency::LatencyStore::new(
            test.admission.store.clone(),
            latency,
        ));
        test.admission.key_refresh_interval = interval;
        test.subject.api_key = true;
        test
    }

    #[cfg(feature = "test-store")]
    #[tokio::test(start_paused = true)]
    async fn a_refresh_outlives_its_first_caller_and_still_updates_the_cache() {
        let second = std::time::Duration::from_secs(1);
        // Two reads (head, identity) of 5 s each; the interval is far longer.
        let test = slow(5 * second, 60 * second).await;
        let mut subject = test.subject.clone();
        let started = tokio::time::Instant::now();
        // The caller's deadline passes while the refresh reads: 401, not 408.
        let refused = tokio::time::timeout_at(
            started + 2 * second,
            test.admission
                .authorize(&test.config, &mut subject, started + second),
        )
        .await;
        assert_eq!(refused, Ok(Err(Unauthorized)));
        // The refresh went on without its caller and filled the cache, so the
        // next request inside the interval finds the key bound.
        tokio::time::sleep(15 * second).await;
        test.authorize().await.unwrap();
    }

    #[cfg(feature = "test-store")]
    #[tokio::test(start_paused = true)]
    async fn callers_share_one_refresh_and_wait_for_it() {
        let second = std::time::Duration::from_secs(1);
        let test = slow(5 * second, 60 * second).await;
        let (first, other) = tokio::join!(test.authorize(), test.authorize());
        // The first caller starts the refresh and the other waits for the same
        // one: both are admitted by it, with one head and one identity read.
        first.unwrap();
        other.unwrap();
    }

    #[cfg(feature = "test-store")]
    #[tokio::test(start_paused = true)]
    async fn a_failed_refresh_does_not_suppress_the_next_one_beyond_the_interval() {
        let second = std::time::Duration::from_secs(1);
        let test = slow(second, 10 * second).await;
        // The head cannot be read: the refresh fails and the key stays unknown.
        test.remote
            .delete(&Path::from("namespace/head.json"))
            .await
            .unwrap();
        assert_eq!(test.authorize().await, Err(Unauthorized));
        test.publish_head().await;
        // Inside the interval, no new lookup is made ...
        tokio::time::sleep(5 * second).await;
        assert_eq!(test.authorize().await, Err(Unauthorized));
        // ... after it, the next request looks again and admits the key.
        tokio::time::sleep(6 * second).await;
        test.authorize().await.unwrap();
    }

    #[tokio::test]
    async fn an_unbound_api_key_is_the_same_401_whatever_the_directory_or_storage_say() {
        let mut test = Fixture::new().await;
        test.config.identity.directory.provider = fyrr_core::config::DirectoryProvider::Snapshot;
        test.head.freshness.directory = Some(fyrr_core::clock::now());
        test.publish_head().await;
        test.subject.api_key = true;
        test.authorize().await.unwrap();
        // The directory read expires: a bound key learns it (503) ...
        test.head.freshness.directory = Some(fyrr_core::clock::now() - chrono::Duration::days(30));
        test.publish_head().await;
        assert_eq!(test.authorize().await, Err(AuthorizationUnavailable));
        // ... an unbound one, with the same stale directory, learns nothing.
        let mut unknown = test.subject.clone();
        unknown.key = format!("key:{}", "6".repeat(64));
        assert_eq!(
            test.admission
                .authorize(&test.config, &mut unknown, later())
                .await,
            Err(Unauthorized)
        );
        // Neither does a removed key whose removal the head already shows.
        test.identity.subjects.clear();
        test.identity.generation += 1;
        test.publish_identity().await;
        assert_eq!(test.authorize().await, Err(Unauthorized));
        // Nor a key when storage is down: the head cannot be read at all.
        test.remote
            .delete(&Path::from("namespace/head.json"))
            .await
            .unwrap();
        assert_eq!(
            test.admission
                .authorize(&test.config, &mut unknown, later())
                .await,
            Err(Unauthorized)
        );
    }

    #[tokio::test]
    async fn the_published_directory_clock_binds_a_reader_without_a_directory_provider() {
        let mut test = Fixture::new().await;
        assert_eq!(
            test.config.identity.directory.provider,
            fyrr_core::config::DirectoryProvider::None
        );
        for api_key in [false, true] {
            test.subject.api_key = api_key;
            // No timestamp: a snapshot imported by hand, served as before.
            test.head.freshness.directory = None;
            test.publish_head().await;
            test.authorize().await.unwrap();
            // A fresh attestation serves, a stale one refuses the namespace,
            // whatever this reader's own provider setting says.
            test.head.freshness.directory = Some(fyrr_core::clock::now());
            test.publish_head().await;
            test.authorize().await.unwrap();
            test.head.freshness.directory =
                Some(fyrr_core::clock::now() - chrono::Duration::days(30));
            test.publish_head().await;
            assert_eq!(
                test.authorize().await,
                Err(AuthorizationUnavailable),
                "{api_key}"
            );
        }
    }

    #[tokio::test]
    async fn cached_identity_never_skips_token_or_directory_expiry() {
        let mut test = Fixture::new().await;
        test.authorize().await.unwrap();
        test.subject.expires = 0;
        assert_eq!(test.authorize().await, Err(AuthorizationUnavailable));
        test.subject.expires = fyrr_core::clock::now().timestamp() + 600;
        test.config.identity.directory.provider = fyrr_core::config::DirectoryProvider::Snapshot;
        test.head.freshness.directory = Some(fyrr_core::clock::now() - chrono::Duration::days(30));
        test.publish_head().await;
        assert_eq!(test.authorize().await, Err(AuthorizationUnavailable));
        test.head.freshness.directory = Some(fyrr_core::clock::now());
        test.publish_head().await;
        test.authorize().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_identity_digest_never_reuses_cached_grants() {
        let mut test = Fixture::new().await;
        test.authorize().await.unwrap();
        test.head.identity.digest = format!("sha256:{}", "0".repeat(64));
        test.publish_head().await;
        assert!(test.authorize().await.is_err());
    }

    #[tokio::test]
    async fn identity_namespace_and_generation_must_match_the_fresh_head() {
        for namespace in [false, true] {
            let mut test = Fixture::new().await;
            test.authorize().await.unwrap();
            if namespace {
                test.identity.namespace = "other".into();
            }
            test.identity.generation += 1;
            test.publish_identity().await;
            if !namespace {
                test.head.identity.generation += 1;
                test.publish_head().await;
            }
            assert_eq!(test.authorize().await, Err(AuthorizationUnavailable));
        }
    }
}
