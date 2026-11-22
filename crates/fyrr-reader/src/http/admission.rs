//! HTTP principal admission shares the reader's control store.
use super::*;
use fyrr_core::config::{ApiKeyRole, prefix_allows};
use fyrr_core::identity::{ApiKeysArtifact, IdentityArtifact};
use fyrr_core::publication::{GenerationPointer, ServingHead};
use object_store::{ObjectStore, path::Path};

#[derive(Debug)]
pub(super) struct Admission {
    store: Arc<dyn ObjectStore>,
    root: Path,
    cached_identity: crate::storage::ArtifactCache<IdentityArtifact>,
    heads: crate::storage::HeadCache,
    /// The deployment's published API keys, held as the head is: read at each
    /// version check, by the loop that makes it, and by nothing else; a request
    /// with no copy younger than two intervals is refused without a read.
    api_keys: crate::storage::HeadCache<ApiKeysArtifact>,
    keys_root: Path,
}
impl Admission {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        root: Path,
        cached_identity: crate::storage::ArtifactCache<IdentityArtifact>,
        heads: crate::storage::HeadCache,
        api_keys: crate::storage::HeadCache<ApiKeysArtifact>,
    ) -> Self {
        let keys_root = crate::storage::deployment_root(&root);
        Self {
            store,
            root,
            cached_identity,
            heads,
            api_keys,
            keys_root,
        }
    }
    /// Reads the published API keys now.
    pub(super) async fn refresh_api_keys(&self) -> Result<(), ReaderError> {
        self.api_keys
            .read_fresh(self.store.as_ref(), &self.keys_root)
            .await?;
        Ok(())
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
    /// Whether `subject` may use a route that needs `role` in the namespace
    /// `name`, from the published keys alone, before anything of the namespace
    /// is read. An API key holds the roles of its entry in the published keys,
    /// and is unknown without one, also while the keys cannot be read; any
    /// other subject holds `read`. A key that lacks the role, or may not act in
    /// the namespace, is `Forbidden`.
    pub(super) async fn grant(
        &self,
        subject: &crate::auth::AuthenticatedSubject,
        role: ApiKeyRole,
        name: &str,
    ) -> Result<(), ReaderError> {
        let allowed = if let Some(hash) = &subject.api_key {
            let keys = self
                .api_keys
                .held(self.store.as_ref(), &self.keys_root)
                .await?;
            let grant = keys.grant(hash).ok_or(Unauthorized)?;
            grant.roles.contains(&role) && prefix_allows(grant.namespace_prefix.as_deref(), name)
        } else {
            role == ApiKeyRole::Read
        };
        if allowed { Ok(()) } else { Err(Forbidden) }
    }
    /// Admits `subject` to a route that needs `role` in the namespace this
    /// admission serves (see [`Self::grant`]).
    pub(super) async fn authorize(
        &self,
        config: &Config,
        subject: &crate::auth::AuthenticatedSubject,
        role: ApiKeyRole,
    ) -> Result<
        (
            String,
            Vec<String>,
            fyrr_core::publication::GenerationPointer,
        ),
        ReaderError,
    > {
        let head = self.head().await?;
        let now = fyrr_core::clock::now();
        self.grant(subject, role, &config.namespace.name).await?;
        let live = head.namespace == config.namespace.name && subject.expires > now.timestamp();
        // An API-key secret proves nothing until the identity binds it, so its
        // lookup comes before the namespace and expiry checks: an unbound
        // key is `Unauthorized` whatever their state.
        if !live && subject.api_key.is_none() {
            return Err(AuthorizationUnavailable);
        }
        let identity = self.identity(config, &head.identity).await?;
        let live = live && crate::storage::directory_published(config, &identity);
        let principal =
            identity
                .principal_for_subject(&subject.key)
                .ok_or(if subject.api_key.is_some() {
                    Unauthorized
                } else {
                    AuthorizationUnavailable
                })?;
        if !live {
            return Err(AuthorizationUnavailable);
        }
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
        Ok((principal.key.clone(), groups, head.identity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fyrr_core::identity::ApiKeyGrant;
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
            let config = fyrr_core::config::parse(concat!(
                "[namespace]\nname='acme'\n",
                "[storage]\nuri='s3://admission'\n",
                "[embedding]\nendpoint='https://embed.invalid'\n",
            ))
            .unwrap()
            .config;
            let head: ServingHead = fixture("serving-head/head.json");
            let mut identity: IdentityArtifact = fixture("identity-artifact/artifact.json");
            let key = format!("key:{}", "9".repeat(64));
            identity.subjects = vec![fyrr_core::identity::SubjectBinding {
                subject: key.clone(),
                principal: identity.principals[0].key.clone(),
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
                crate::storage::HeadCache::with_file(
                    std::time::Duration::from_secs(60),
                    crate::storage::API_KEYS_FILE,
                ),
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
                    api_key: None,
                    expires: fyrr_core::clock::now().timestamp() + 600,
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
        async fn authorize_role(
            &self,
            role: ApiKeyRole,
        ) -> Result<(String, Vec<String>, GenerationPointer), ReaderError> {
            self.admission
                .authorize(&self.config, &self.subject, role)
                .await
        }
        async fn authorize(&self) -> Result<(String, Vec<String>, GenerationPointer), ReaderError> {
            self.admission
                .authorize(&self.config, &self.subject, ApiKeyRole::Read)
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
            crate::storage::HeadCache::with_file(
                std::time::Duration::from_secs(60),
                crate::storage::API_KEYS_FILE,
            ),
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
    async fn published_api_keys_decide_roles_namespaces_and_age() {
        let mut test = Fixture::new().await;
        let hash = format!("key:{}", "8".repeat(64));
        test.subject.api_key = Some(hash.clone());
        let publish = |keys: Vec<ApiKeyGrant>| {
            let bytes = write_contract(&ApiKeysArtifact {
                schema_version: fyrr_core::identity::API_KEYS_CONTRACT_VERSION,
                keys,
            });
            let remote = test.remote.clone();
            async move {
                remote
                    .put(&Path::from("api-keys.json"), bytes.into())
                    .await
                    .unwrap();
            }
        };
        let grant = |roles: &[ApiKeyRole], prefix: Option<&str>| ApiKeyGrant {
            subject: hash.clone(),
            roles: roles.to_vec(),
            namespace_prefix: prefix.map(str::to_owned),
        };

        // No published keys, no key: the identity's binding decides nothing.
        assert_eq!(
            test.authorize_role(ApiKeyRole::Read).await.unwrap_err(),
            StorageUnavailable
        );
        // Published: the entry decides, and a request costs no read of the keys.
        publish(vec![grant(
            &[ApiKeyRole::Read, ApiKeyRole::Write],
            Some("ac"),
        )])
        .await;
        let before = test.mirror.totals().requests;
        test.admission.refresh_api_keys().await.unwrap();
        assert_eq!(
            test.mirror.totals().requests - before,
            1,
            "one read per refresh"
        );
        // The first request reads the identity too; later ones read the head.
        test.authorize_role(ApiKeyRole::Read).await.unwrap();
        let before = test.mirror.totals().requests;
        test.authorize_role(ApiKeyRole::Read).await.unwrap();
        test.authorize_role(ApiKeyRole::Write).await.unwrap();
        assert_eq!(test.mirror.totals().requests - before, 2, "the head only");
        assert_eq!(
            test.authorize_role(ApiKeyRole::Admin).await.unwrap_err(),
            Forbidden
        );
        publish(vec![grant(&[ApiKeyRole::Read], Some("other"))]).await;
        test.admission.refresh_api_keys().await.unwrap();
        assert_eq!(
            test.authorize_role(ApiKeyRole::Read).await.unwrap_err(),
            Forbidden
        );
        // A key the published set lacks is unknown, however the identity binds it.
        publish(Vec::new()).await;
        test.admission.refresh_api_keys().await.unwrap();
        assert_eq!(
            test.authorize_role(ApiKeyRole::Read).await.unwrap_err(),
            Unauthorized
        );

        // A subject that is no API key holds `read` only.
        test.subject.api_key = None;
        test.authorize_role(ApiKeyRole::Read).await.unwrap();
        assert_eq!(
            test.authorize_role(ApiKeyRole::Write).await.unwrap_err(),
            Forbidden
        );

        // A copy older than two intervals is never read for a request: it is
        // refused without a read of storage until the loop reads again.
        test.subject.api_key = Some(hash.clone());
        test.admission.api_keys = crate::storage::HeadCache::with_file(
            std::time::Duration::from_millis(10),
            crate::storage::API_KEYS_FILE,
        );
        publish(vec![grant(&[ApiKeyRole::Read], None)]).await;
        test.admission.refresh_api_keys().await.unwrap();
        test.authorize_role(ApiKeyRole::Read).await.unwrap();
        publish(vec![grant(&[ApiKeyRole::Write], None)]).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let before = test.mirror.totals().requests;
        assert_eq!(
            test.admission
                .grant(&test.subject, ApiKeyRole::Read, "acme")
                .await
                .unwrap_err(),
            StorageUnavailable
        );
        assert_eq!(test.mirror.totals().requests, before, "no read of storage");
        test.admission.refresh_api_keys().await.unwrap();
        assert_eq!(
            test.authorize_role(ApiKeyRole::Read).await.unwrap_err(),
            Forbidden
        );
        test.remote
            .delete(&Path::from("api-keys.json"))
            .await
            .unwrap();
        assert!(test.admission.refresh_api_keys().await.is_err());
        assert_eq!(
            test.authorize_role(ApiKeyRole::Read).await.unwrap_err(),
            StorageUnavailable
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
    async fn cached_identity_never_skips_token_expiry() {
        let mut test = Fixture::new().await;
        test.authorize().await.unwrap();
        test.subject.expires = 0;
        assert_eq!(test.authorize().await, Err(AuthorizationUnavailable));
        test.subject.expires = fyrr_core::clock::now().timestamp() + 600;
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
