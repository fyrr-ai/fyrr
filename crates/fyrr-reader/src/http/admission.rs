//! HTTP principal admission shares the reader's control store.
use super::*;
use fyrr_core::identity::IdentityArtifact;
use fyrr_core::publication::{GenerationPointer, ServingHead};
use object_store::{ObjectStore, path::Path};

#[derive(Debug)]
pub(super) struct Admission {
    store: Arc<dyn ObjectStore>,
    root: Path,
    cached_identity: Mutex<Option<(GenerationPointer, Arc<IdentityArtifact>)>>,
}
impl Admission {
    pub(super) fn new(store: Arc<dyn ObjectStore>, root: Path) -> Self {
        Self {
            store,
            root,
            cached_identity: Mutex::new(None),
        }
    }
    pub(super) async fn head(&self) -> Result<ServingHead, ReaderError> {
        crate::storage::read_contract(self.store.as_ref(), &self.root.clone().join("head.json"))
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
            if identity.namespace != config.namespace.name {
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
    pub(super) async fn authorize(
        &self,
        config: &Config,
        subject: &crate::auth::AuthenticatedSubject,
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
        if head.namespace != config.namespace.name
            || subject.expires <= now.timestamp()
            || !crate::storage::directory_fresh(config, head.freshness.directory, now)
        {
            return Err(AuthorizationUnavailable);
        }
        let identity = self.identity(config, &head.identity).await?;
        let principal = identity
            .principal_for_subject(&subject.key)
            .ok_or(AuthorizationUnavailable)?;
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
        async fn authorize(&self) -> Result<(String, Vec<String>, GenerationPointer), ReaderError> {
            self.admission.authorize(&self.config, &self.subject).await
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
