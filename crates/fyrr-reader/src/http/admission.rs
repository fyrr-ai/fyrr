//! HTTP principal admission shares the reader's control store.
use super::*;
use fyrr_core::identity::IdentityArtifact;
use fyrr_core::publication::{GenerationPointer, ServingHead};
use object_store::{ObjectStore, path::Path};

#[derive(Debug)]
pub(super) struct Admission {
    store: Arc<dyn ObjectStore>,
    root: Path,
    cached_identity: crate::storage::ArtifactCache<IdentityArtifact>,
    heads: crate::storage::HeadCache,
}
impl Admission {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        root: Path,
        cached_identity: crate::storage::ArtifactCache<IdentityArtifact>,
        heads: crate::storage::HeadCache,
    ) -> Self {
        Self {
            store,
            root,
            cached_identity,
            heads,
        }
    }
    pub(super) async fn head(&self) -> Result<ServingHead, ReaderError> {
        self.heads
            .read(&self.store, &self.root)
            .await
            .map_err(Into::into)
    }
    /// The head as storage holds it now, for readiness.
    pub(super) async fn fresh_head(&self) -> Result<ServingHead, ReaderError> {
        self.heads
            .read_fresh(self.store.as_ref(), &self.root)
            .await
            .map_err(Into::into)
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
        let identity: IdentityArtifact =
            crate::storage::artifact(self.store.as_ref(), &self.root, pointer).await?;
        if identity.namespace != config.namespace.name || identity.generation != pointer.generation
        {
            return Err(AuthorizationUnavailable);
        }
        let identity = Arc::new(identity);
        *cached = Some((pointer.clone(), identity.clone()));
        Ok(identity)
    }

    /// Admission of an API key. Its binding is looked up in the identity of the
    /// shared head, as an OIDC subject's is, so a key added, removed or rebound
    /// takes effect with the reader's next version check. The binding lookup
    /// comes before the namespace and directory checks, so an unbound key is
    /// the same `Unauthorized` whatever the directory's state, and costs no
    /// storage read of its own. Only a key the identity binds can be told
    /// `AuthorizationUnavailable` for those.
    async fn authorize_api_key(
        &self,
        config: &Config,
        subject: &mut crate::auth::AuthenticatedSubject,
    ) -> Result<Admitted, ReaderError> {
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
    ) -> Result<Admitted, ReaderError> {
        if subject.api_key {
            return self.authorize_api_key(config, subject).await;
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
            let admission = Admission::new(
                mirror.wrap("control", remote.clone()),
                Path::from("namespace"),
                Arc::default(),
                crate::storage::HeadCache::with_interval(std::time::Duration::ZERO),
            );
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
        /// A fixture whose checks use the head of the last version check, as a
        /// running reader's do, and whose head is read for the first time now.
        async fn held() -> Self {
            let mut test = Self::new().await;
            test.admission.heads =
                crate::storage::HeadCache::with_interval(std::time::Duration::from_secs(60));
            test
        }
        /// What a version check, `SIGHUP` and `POST /admin/refresh` do to the head.
        async fn version_check(&self) {
            self.admission
                .heads
                .read_fresh(self.admission.store.as_ref(), &self.admission.root)
                .await
                .unwrap();
        }
        async fn authorize(&self) -> Result<Admitted, ReaderError> {
            self.admission
                .authorize(&self.config, &mut self.subject.clone())
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
            test.admission.heads.clone(),
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
        let mut test = Fixture::held().await;
        test.subject.api_key = true;
        test.authorize().await.unwrap();
        // An unknown and a removed secret are the same caller; an unbound OIDC
        // subject stays an authorization problem.
        let unknown = format!("key:{}", "8".repeat(64));
        let mut other = test.subject.clone();
        other.key = unknown;
        assert_eq!(
            test.admission.authorize(&test.config, &mut other).await,
            Err(Unauthorized)
        );
        other.api_key = false;
        assert_eq!(
            test.admission.authorize(&test.config, &mut other).await,
            Err(AuthorizationUnavailable)
        );
        // A removal takes effect with the next version check, as an OIDC
        // subject's does, not with the next request.
        test.identity.subjects.clear();
        test.identity.generation += 1;
        test.publish_identity().await;
        test.authorize().await.unwrap();
        test.version_check().await;
        assert_eq!(test.authorize().await, Err(Unauthorized));
        // An expiry never waits for a version check: the reader's clock
        // decides, whatever head the request checks.
        let soon = fyrr_core::clock::now() + chrono::Duration::milliseconds(700);
        test.identity.subjects = vec![fyrr_core::identity::SubjectBinding {
            subject: test.subject.key.clone(),
            principal: test.identity.principals[0].key.clone(),
            expires: Some(soon),
        }];
        test.identity.generation += 1;
        test.publish_identity().await;
        test.version_check().await;
        test.authorize().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        assert_eq!(test.authorize().await, Err(Unauthorized));
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_version_check_a_removed_key_is_refused_within_two_intervals() {
        let mut test = Fixture::held().await;
        test.subject.api_key = true;
        test.authorize().await.unwrap();
        test.identity.subjects.clear();
        test.identity.generation += 1;
        test.publish_identity().await;
        // The held head is the one of the last version check ...
        tokio::time::advance(std::time::Duration::from_secs(59)).await;
        test.authorize().await.unwrap();
        // ... until it is two intervals old: then a check reads the head itself.
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
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
            .authorize(&test.config, &mut subject)
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
            .authorize(&test.config, &mut subject)
            .await
            .unwrap();
        assert_eq!(subject.expires, i64::MAX);
        let mut oidc = test.subject.clone();
        oidc.api_key = false;
        let own = oidc.expires;
        test.admission
            .authorize(&test.config, &mut oidc)
            .await
            .unwrap();
        assert_eq!(oidc.expires, own);
    }

    #[tokio::test]
    async fn an_unknown_api_key_costs_no_storage_read_and_a_new_key_waits_for_the_version_check() {
        let mut test = Fixture::held().await;
        test.subject.api_key = true;
        test.authorize().await.unwrap();
        let mut unknown = test.subject.clone();
        unknown.key = format!("key:{}", "7".repeat(64));
        let before = test.mirror.totals().requests;
        for _ in 0..50 {
            assert_eq!(
                test.admission.authorize(&test.config, &mut unknown).await,
                Err(Unauthorized)
            );
        }
        assert_eq!(
            test.mirror.totals().requests,
            before,
            "an unknown key reads nothing from storage"
        );
        // A key published now is unknown until the reader's next version check.
        test.identity.subjects = vec![fyrr_core::identity::SubjectBinding {
            subject: unknown.key.clone(),
            principal: test.identity.principals[0].key.clone(),
            expires: None,
        }];
        test.identity.generation += 1;
        test.publish_identity().await;
        assert_eq!(
            test.admission.authorize(&test.config, &mut unknown).await,
            Err(Unauthorized)
        );
        assert_eq!(test.mirror.totals().requests, before);
        test.version_check().await;
        test.admission
            .authorize(&test.config, &mut unknown)
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn an_unbound_api_key_is_the_same_401_whatever_the_directory_or_storage_say() {
        let mut test = Fixture::held().await;
        test.config.identity.directory.provider = fyrr_core::config::DirectoryProvider::Snapshot;
        test.head.freshness.directory = Some(fyrr_core::clock::now());
        test.publish_head().await;
        test.subject.api_key = true;
        test.authorize().await.unwrap();
        // The directory read expires: a bound key learns it (503) ...
        test.head.freshness.directory = Some(fyrr_core::clock::now() - chrono::Duration::days(30));
        test.publish_head().await;
        test.version_check().await;
        assert_eq!(test.authorize().await, Err(AuthorizationUnavailable));
        // ... an unbound one, with the same stale directory, learns nothing.
        let mut unknown = test.subject.clone();
        unknown.key = format!("key:{}", "6".repeat(64));
        assert_eq!(
            test.admission.authorize(&test.config, &mut unknown).await,
            Err(Unauthorized)
        );
        // Neither does a removed key whose removal the head already shows.
        test.identity.subjects.clear();
        test.identity.generation += 1;
        test.publish_identity().await;
        test.version_check().await;
        assert_eq!(test.authorize().await, Err(Unauthorized));
        // Storage down: while the held head is young, nothing is read and
        // an unbound key is still a 401.
        test.remote
            .delete(&Path::from("namespace/head.json"))
            .await
            .unwrap();
        assert_eq!(
            test.admission.authorize(&test.config, &mut unknown).await,
            Err(Unauthorized)
        );
        // Once the head is too old to trust and cannot be read, every key is
        // the same 503: the unbound one is told nothing the bound one is not.
        tokio::time::advance(std::time::Duration::from_secs(60)).await;
        let bound = test.authorize().await;
        let unbound = test.admission.authorize(&test.config, &mut unknown).await;
        assert!(bound.is_err() && bound != Err(Unauthorized), "{bound:?}");
        assert_eq!(bound, unbound);
    }

    #[tokio::test(start_paused = true)]
    async fn a_bound_key_is_refused_with_503_when_the_held_head_is_too_old_and_unreadable() {
        let mut test = Fixture::held().await;
        test.subject.api_key = true;
        test.authorize().await.unwrap();
        test.remote
            .delete(&Path::from("namespace/head.json"))
            .await
            .unwrap();
        // Served from the held head while it is younger than two intervals ...
        tokio::time::advance(std::time::Duration::from_secs(59)).await;
        test.authorize().await.unwrap();
        // ... refused once it is that old and a read of its own fails.
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        let refused = test.authorize().await;
        assert!(
            refused.is_err() && refused != Err(Unauthorized),
            "{refused:?}"
        );
    }

    #[tokio::test]
    async fn a_bound_api_key_is_held_to_the_published_directory_clock() {
        let mut test = Fixture::new().await;
        test.subject.api_key = true;
        // No timestamp: a snapshot imported by hand, served as before.
        test.head.freshness.directory = None;
        test.publish_head().await;
        test.authorize().await.unwrap();
        // A fresh attestation serves, a stale one refuses the namespace.
        test.head.freshness.directory = Some(fyrr_core::clock::now());
        test.publish_head().await;
        test.authorize().await.unwrap();
        test.head.freshness.directory = Some(fyrr_core::clock::now() - chrono::Duration::days(30));
        test.publish_head().await;
        assert_eq!(test.authorize().await, Err(AuthorizationUnavailable));
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
    async fn the_published_directory_clock_binds_a_reader_without_a_directory_provider() {
        let mut test = Fixture::new().await;
        assert_eq!(
            test.config.identity.directory.provider,
            fyrr_core::config::DirectoryProvider::None
        );
        // No timestamp: a snapshot imported by hand, served as before.
        test.head.freshness.directory = None;
        test.publish_head().await;
        test.authorize().await.unwrap();
        // A fresh attestation serves, a stale one refuses the namespace,
        // whatever this reader's own provider setting says.
        test.head.freshness.directory = Some(fyrr_core::clock::now());
        test.publish_head().await;
        test.authorize().await.unwrap();
        test.head.freshness.directory = Some(fyrr_core::clock::now() - chrono::Duration::days(30));
        test.publish_head().await;
        assert_eq!(test.authorize().await, Err(AuthorizationUnavailable));
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
