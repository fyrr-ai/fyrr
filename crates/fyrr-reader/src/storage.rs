//! Opening published Lance datasets on S3 or a local directory with options
//! from the configuration.

use crate::embedding::digest;
use fyrr_core::config::StorageOptions;
use fyrr_core::config::{Config, ProcessEnvironment, StorageBackend};
use fyrr_core::publication::GenerationPointer;
use fyrr_core::{Contract, parse_contract};
use lance::dataset::builder::DatasetBuilder;
use object_store::{
    ObjectStore, ObjectStoreExt,
    aws::{AmazonS3Builder, AmazonS3ConfigKey},
    path::Path,
};
use std::sync::Arc;

pub use lance::Dataset;
#[path = "storage/conditional.rs"]
mod conditional;
#[path = "storage/diagnostics.rs"]
mod diagnostics;

/// One digest-verified immutable artifact shared by a serving generation.
pub(crate) type ArtifactCache<T> = Arc<tokio::sync::Mutex<Option<(GenerationPointer, Arc<T>)>>>;

/// The serving head as one reader last read it, shared by the authorization
/// checks of all its requests.
///
/// Every request compares its grants with the head before it starts and
/// before it answers. Read from storage each time, that is two requests per
/// query whatever the mirror holds. The reader already reads the head every
/// `reader.version_check_interval` to find new publications, and for a
/// refresh, SIGHUP and the opening of a generation; requests check the head
/// those reads returned. A published change of grants therefore reaches a
/// reader with its next version check.
///
/// A check reads the head itself only when the held one is older than two
/// intervals: the version check did not run, or the host was suspended (its
/// age counts on the monotonic clock and on the wall clock). Such checks
/// share one read, run on its own task, and its outcome: no lock is held
/// across the request, and a failure is given to the checks that waited for
/// it and then forgotten. A read that is itself older than two intervals when
/// it ends, because storage answered late, fails the checks that waited for it
/// the same way: its head is as old as the read is. The held head is always
/// the one whose read started last, so a slower, older read never replaces a
/// newer one, and a generation opened from a fresh read never meets an older
/// head here.
///
/// The deployment's published API keys are held the same way, with the same
/// ages and reads: a removed key is refused within two intervals, a held copy
/// older than that is read again, and only a failed read refuses callers.
#[derive(Debug, Clone)]
pub(crate) struct HeadCache<T = fyrr_core::publication::ServingHead> {
    max_age: std::time::Duration,
    /// The object read under the root given to each call.
    file: &'static str,
    state: Arc<std::sync::Mutex<HeadState<T>>>,
}
#[derive(Debug)]
struct HeadState<T> {
    held: Option<HeldHead<T>>,
    /// When the latest finished read started, whether it succeeded or not:
    /// a read that started earlier is never held or handed out after it.
    latest: Option<tokio::time::Instant>,
    /// The shared read in flight and when it was started. Its task removes
    /// it when it ends, so a finished read is never handed to a later check.
    reading: Option<(tokio::time::Instant, SharedHeadRead<T>)>,
}
impl<T> Default for HeadState<T> {
    fn default() -> Self {
        Self {
            held: None,
            latest: None,
            reading: None,
        }
    }
}
#[derive(Debug, Clone)]
struct HeldHead<T> {
    started: tokio::time::Instant,
    started_at: chrono::DateTime<chrono::Utc>,
    head: T,
}
/// A head read and the instant it started.
type HeadRead<T> = (tokio::time::Instant, Result<T, StorageError>);
type SharedHeadRead<T> = futures::future::Shared<futures::future::BoxFuture<'static, HeadRead<T>>>;
impl HeadCache {
    pub(crate) fn new(config: &Config) -> Self {
        Self::for_file(config, "head.json")
    }
    /// A zero age reads the head for every check; only test suites ask for it.
    #[cfg(test)]
    pub(crate) fn with_interval(max_age: std::time::Duration) -> Self {
        Self::with_file(max_age, "head.json")
    }
}
impl<T: Contract + Clone + Send + Sync + 'static> HeadCache<T> {
    /// The scheduler checks for a new version at least every 30 s.
    const CHECK_INTERVAL_MAX: std::time::Duration = std::time::Duration::from_secs(30);

    /// A cache of `file`, aged as the head is.
    pub(crate) fn for_file(config: &Config, file: &'static str) -> Self {
        #[cfg(feature = "test-store")]
        if test_store::head_check_every() {
            return Self::with_file(std::time::Duration::ZERO, file);
        }
        let interval = config.reader.version_check_interval.as_duration();
        Self::with_file(2 * interval.min(Self::CHECK_INTERVAL_MAX), file)
    }
    pub(crate) fn with_file(max_age: std::time::Duration, file: &'static str) -> Self {
        Self {
            max_age,
            file,
            state: Arc::default(),
        }
    }
    fn young(&self, held: &HeldHead<T>) -> bool {
        let wall = fyrr_core::clock::now().signed_duration_since(held.started_at);
        held.started.elapsed() < self.max_age && wall.to_std().is_ok_and(|age| age < self.max_age)
    }
    /// The head for an authorization check: the held one while it is younger
    /// than two intervals, otherwise one read shared with concurrent checks.
    pub(crate) async fn read(
        &self,
        store: &Arc<dyn ObjectStore>,
        root: &Path,
    ) -> Result<T, StorageError> {
        if self.max_age.is_zero() {
            return read_contract(store.as_ref(), &root.clone().join(self.file)).await;
        }
        let reading = {
            let mut state = crate::sync::lock(&self.state);
            if let Some(held) = state.held.as_ref()
                && self.young(held)
            {
                return Ok(held.head.clone());
            }
            match &state.reading {
                Some((_, reading)) => reading.clone(),
                None => {
                    let started = tokio::time::Instant::now();
                    let (cache, store, root) = (self.clone(), store.clone(), root.clone());
                    // Its own task: no waiting check has to poll it, and a
                    // cancelled check does not cancel the read of the others.
                    let task = tokio::spawn(async move {
                        let outcome = cache.fresh(store.as_ref(), &root).await;
                        let mut state = crate::sync::lock(&cache.state);
                        if state.reading.as_ref().is_some_and(|(at, _)| *at == started) {
                            state.reading = None;
                        }
                        outcome
                    });
                    let reading =
                        futures::FutureExt::shared(futures::FutureExt::boxed(async move {
                            task.await
                                .unwrap_or((started, Err(StorageError::StorageUnavailable)))
                        }));
                    state.reading = Some((started, reading.clone()));
                    reading
                }
            }
        };
        let (read_started, outcome) = reading.clone().await;
        let mut state = crate::sync::lock(&self.state);
        // A task that panicked or was shut down did not remove its read.
        if state
            .reading
            .as_ref()
            .is_some_and(|(_, current)| current.ptr_eq(&reading))
        {
            state.reading = None;
        }
        // A read that started after this one has finished: its head is the
        // newer one, and if it failed this outcome is not to be trusted.
        match state.held.as_ref() {
            Some(held) if held.started >= read_started && self.young(held) => Ok(held.head.clone()),
            _ if state.latest.is_some_and(|latest| latest > read_started) => {
                Err(StorageError::StorageUnavailable)
            }
            // A read that outlasted the age limit answers with a head older
            // than a check may use, whatever its own age on arrival: its age
            // counts from when it started. The next check reads again.
            Some(held) if held.started == read_started => {
                Err(StorageError::AuthorizationUnavailable)
            }
            _ => outcome,
        }
    }
    /// Reads the head from storage and holds it for the checks of requests.
    /// A version check, a refresh, SIGHUP and the opening of a generation
    /// read here, so requests never see an older head than they did. A
    /// failed read drops a head whose read started earlier.
    pub(crate) async fn read_fresh(
        &self,
        store: &dyn ObjectStore,
        root: &Path,
    ) -> Result<T, StorageError> {
        self.fresh(store, root).await.1
    }
    /// `read_fresh` with the instant its read started.
    async fn fresh(&self, store: &dyn ObjectStore, root: &Path) -> HeadRead<T> {
        let (started, started_at) = (tokio::time::Instant::now(), fyrr_core::clock::now());
        let outcome: Result<T, _> = read_contract(store, &root.clone().join(self.file)).await;
        if self.max_age.is_zero() {
            return (started, outcome);
        }
        let mut state = crate::sync::lock(&self.state);
        if state.latest.is_none_or(|latest| latest <= started) {
            state.latest = Some(started);
            state.held = outcome.as_ref().ok().map(|head| HeldHead {
                started,
                started_at,
                head: head.clone(),
            });
        }
        (started, outcome)
    }
}

#[cfg(all(feature = "test-store", not(debug_assertions)))]
compile_error!("test-store must not be enabled in shipping builds");
#[cfg(feature = "test-store")]
#[path = "storage/test_store.rs"]
pub mod test_store;

pub fn native_store_params(
    mirror: Option<Arc<dyn lance_io::object_store::WrappingObjectStore>>,
) -> lance_io::object_store::ObjectStoreParams {
    let mirror = diagnostics::native_wrapper(mirror);
    #[cfg(feature = "test-store")]
    let mirror = test_store::native_wrapper(mirror);
    lance_io::object_store::ObjectStoreParams {
        object_store_wrapper: mirror,
        ..Default::default()
    }
}

/// Fixed diagnostics shared by reader and writer storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    StorageConfigurationInvalid,
    StorageUnavailable,
    PublicationInvalid,
    AuthorizationUnavailable,
    StaleCursor,
    PublicationUnavailable,
}
impl StorageError {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StorageConfigurationInvalid => "storage_configuration_invalid",
            Self::StorageUnavailable => "storage_unavailable",
            Self::PublicationInvalid => "publication_invalid",
            Self::AuthorizationUnavailable => "authorization_unavailable",
            Self::StaleCursor => "stale_cursor",
            Self::PublicationUnavailable => "publication_unavailable",
        }
    }
}
impl std::fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
impl std::error::Error for StorageError {}

// object_store 0.14 falls back to IMDS after these sources. Respect the AWS
// opt-out only for that fallback; let the builder validate partial credentials.
fn uses_instance_credentials(builder: &AmazonS3Builder) -> bool {
    use AmazonS3ConfigKey as Key;
    let has = |key| builder.get_config_value(&key).is_some();
    builder.get_config_value(&Key::SkipSignature).as_deref() != Some("true")
        && !has(Key::AccessKeyId)
        && !has(Key::SecretAccessKey)
        && !(has(Key::WebIdentityTokenFile) && has(Key::RoleArn))
        && !has(Key::ContainerCredentialsRelativeUri)
        && !(has(Key::ContainerCredentialsFullUri) && has(Key::ContainerAuthorizationTokenFile))
}

/// Persist immutable native files before publishing a contract that names them.
/// S3 already completes each upload before returning; local Lance rename does
/// not flush file contents or parent directory entries.
pub async fn persist_native_files(
    config: &Config,
    files: Vec<fyrr_core::envelope::FileRef>,
) -> Result<(), StorageError> {
    let Some(StorageBackend::Local { root }) = config.storage.backend() else {
        return Ok(());
    };
    if files.is_empty() {
        return Ok(());
    }
    let namespace = config.namespace.name.clone();
    tokio::task::spawn_blocking(move || {
        let store = local::LocalStore::open(&root)?;
        let dataset = store
            .root()
            .clone()
            .join("namespaces")
            .join(namespace.as_str())
            .join("dataset.lance");
        store.persist_native(&dataset, &files)
    })
    .await
    .map_err(|_| StorageError::StorageUnavailable)?
    .map_err(|_| StorageError::StorageUnavailable)
}

/// Makes durable what a writer observed before it publishes a reference to
/// it. Call `persist` after the observation and before the dependent write:
/// a head naming a manifest whose envelopes a worker wrote, a cursor advanced
/// because an envelope exists, an envelope recording the head it was based on.
///
/// A local writer acknowledges an object after flushing its directory, but
/// the name is visible earlier, so a reader can observe an object that a
/// power loss would still remove. S3 shows only durable objects.
#[derive(Debug, Clone)]
pub struct ObservedDurability(Option<(local::LocalStore, Path)>);

impl ObservedDurability {
    pub async fn persist(&self) -> Result<(), StorageError> {
        if let Some((store, namespace)) = &self.0 {
            store
                .persist_control(namespace.clone())
                .await
                .map_err(|_| StorageError::StorageUnavailable)?;
        }
        Ok(())
    }
}

pub fn observed_durability(config: &Config) -> Result<ObservedDurability, StorageError> {
    let Some(StorageBackend::Local { root }) = config.storage.backend() else {
        return Ok(ObservedDurability(None));
    };
    let store = local::LocalStore::open(&root).map_err(|_| StorageError::StorageUnavailable)?;
    let namespace = store
        .root()
        .clone()
        .join("namespaces")
        .join(config.namespace.name.as_str());
    Ok(ObservedDurability(Some((store, namespace))))
}

/// Explicit durability scope for maintenance and purge deletions.
/// Call `finish` before reporting cleanup success. Dropping an unfinished scope
/// may leave retired files after a crash; subsequent cleanup can remove them.
#[derive(Debug)]
pub struct CleanupDurability(Option<local::LocalStore>);

impl CleanupDurability {
    pub async fn finish(&self) -> Result<(), StorageError> {
        if let Some(store) = &self.0 {
            store
                .finish_cleanup()
                .await
                .map_err(|_| StorageError::StorageUnavailable)?;
        }
        Ok(())
    }
}

pub fn cleanup_namespace_store(
    config: &Config,
) -> Result<(Arc<dyn ObjectStore>, Path, CleanupDurability), StorageError> {
    if let Some(StorageBackend::Local { root }) = config.storage.backend() {
        let local = local::LocalStore::open(&root)
            .map_err(|_| StorageError::StorageUnavailable)?
            .cleanup_batch();
        let root = local
            .root()
            .clone()
            .join("namespaces")
            .join(config.namespace.name.as_str());
        let store: Arc<dyn ObjectStore> = Arc::new(local.clone());
        #[cfg(feature = "test-store")]
        let store = test_store::boundary::wrap(store);
        return Ok((
            diagnostics::wrap(store, true, &root),
            root,
            CleanupDurability(Some(local)),
        ));
    }
    let (store, root) = namespace_store(config)?;
    Ok((store, root, CleanupDurability(None)))
}

/// Namespace-scoped storage access. Only the writer calls mutating store methods.
pub fn namespace_store(config: &Config) -> Result<(Arc<dyn ObjectStore>, Path), StorageError> {
    match config.storage.backend() {
        Some(StorageBackend::Local { root }) => {
            let store =
                local::LocalStore::open(&root).map_err(|_| StorageError::StorageUnavailable)?;
            let root = store
                .root()
                .clone()
                .join("namespaces")
                .join(config.namespace.name.as_str());
            let store: Arc<dyn ObjectStore> = Arc::new(store);
            #[cfg(feature = "test-store")]
            let store = test_store::boundary::wrap(store);
            Ok((diagnostics::wrap(store, true, &root), root))
        }
        Some(StorageBackend::S3 { .. }) => {
            let (store, root, _) = s3_namespace_store(config)?;
            Ok((diagnostics::wrap(Arc::new(store), false, &root), root))
        }
        None => Err(StorageError::StorageConfigurationInvalid),
    }
}

fn s3_namespace_store(
    config: &Config,
) -> Result<
    (
        object_store::aws::AmazonS3,
        Path,
        object_store::client::HttpClient,
    ),
    StorageError,
> {
    let Some(StorageBackend::S3 { bucket, prefix }) = config.storage.backend() else {
        return Err(StorageError::StorageConfigurationInvalid);
    };
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    let options = config
        .storage
        .object_store_options(&ProcessEnvironment)
        .map_err(|_| StorageError::StorageConfigurationInvalid)?;
    for (key, value) in options.into_map() {
        builder = builder.with_config(
            key.parse()
                .map_err(|_| StorageError::StorageConfigurationInvalid)?,
            value,
        );
    }
    let instance_disabled = std::env::var("AWS_EC2_METADATA_DISABLED")
        .is_ok_and(|value| value.eq_ignore_ascii_case("true"))
        && uses_instance_credentials(&builder);
    let transport = conditional::Transport::default();
    let store = builder
        .with_http_connector(transport.clone())
        .build()
        .map_err(|_| StorageError::StorageConfigurationInvalid)?;
    if instance_disabled {
        return Err(StorageError::StorageUnavailable);
    }
    let root = Path::from(prefix)
        .join("namespaces")
        .join(config.namespace.name.as_str());
    let client = transport
        .client()
        .map_err(|_| StorageError::StorageConfigurationInvalid)?;
    Ok((store, root, client))
}

/// Delete only the current namespace head matching the holder's CAS token.
/// S3 deletes the current key without versionId so an older head cannot revive.
pub async fn delete_head_if_match(
    config: &Config,
    expected: &object_store::UpdateVersion,
) -> object_store::Result<()> {
    let failure = || object_store::Error::Generic {
        store: "fyrr namespace head",
        source: "conditional head deletion failed".into(),
    };
    let tag = expected.e_tag.as_deref().ok_or_else(failure)?;
    match config.storage.backend() {
        Some(StorageBackend::Local { root }) => {
            let store = local::LocalStore::open(&root).map_err(|_| failure())?;
            let path = store
                .root()
                .clone()
                .join("namespaces")
                .join(config.namespace.name.as_str())
                .join("head.json");
            let deletion = diagnostics::observe(
                "DELETE_CONDITIONAL",
                &path,
                true,
                store.delete_if_match(path.clone(), tag.to_owned()),
            );
            #[cfg(feature = "test-store")]
            {
                test_store::boundary::conditional_delete(&path, tag, deletion).await
            }
            #[cfg(not(feature = "test-store"))]
            {
                deletion.await
            }
        }
        Some(StorageBackend::S3 { .. }) => {
            let (store, root, client) = s3_namespace_store(config).map_err(|_| failure())?;
            conditional::delete_head(&store, &client, &root, tag).await
        }
        None => Err(failure()),
    }
}

/// The Lance URI of the namespace's dataset. A local dataset uses Lance's
/// `file-object-store` scheme: unlike `file`, it sends every read and write
/// through `object_store`, so the reader's mirror sees each request as on S3.
pub fn dataset_uri(config: &Config) -> String {
    let uri = fyrr_core::layout::NamespaceLayout::new(&config.storage.uri, &config.namespace.name)
        .dataset_uri();
    match uri.strip_prefix("file://") {
        Some(path) => format!("file-object-store://{path}"),
        None => uri,
    }
}

pub async fn read_contract<T: Contract>(
    store: &dyn ObjectStore,
    path: &Path,
) -> Result<T, StorageError> {
    let bytes = store
        .get(path)
        .await
        .map_err(|_| StorageError::StorageUnavailable)?
        .bytes()
        .await
        .map_err(|_| StorageError::StorageUnavailable)?;
    parse_contract(&bytes).map_err(|_| StorageError::PublicationInvalid)
}

/// The object that holds a deployment's API keys, under [`deployment_root`].
pub const API_KEYS_FILE: &str = "api-keys.json";

/// The storage root above `namespaces/`: what the namespaces of a deployment
/// share, so every reader and writer of it finds the same keys.
pub fn deployment_root(namespace_root: &Path) -> Path {
    let parts: Vec<_> = namespace_root.parts().collect();
    Path::from_iter(parts[..parts.len().saturating_sub(2)].iter().cloned())
}

pub async fn optional_head(
    config: &Config,
) -> Result<Option<fyrr_core::publication::ServingHead>, StorageError> {
    let (store, root) = namespace_store(config)?;
    let bytes = match store.get(&root.clone().join("head.json")).await {
        Ok(result) => result
            .bytes()
            .await
            .map_err(|_| StorageError::StorageUnavailable)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(_) => return Err(StorageError::StorageUnavailable),
    };
    parse_contract(&bytes)
        .map(Some)
        .map_err(|_| StorageError::PublicationInvalid)
}

fn namespace_mac(
    config: &Config,
    kind: &str,
    value: &str,
) -> Result<hmac::Hmac<sha2::Sha256>, StorageError> {
    keyed_mac(config, config.namespace.name.as_str(), kind, value)
}

/// The MAC of `value` in `kind` under the hashing key, scoped to `scope`: a
/// namespace name, or empty for what every namespace of a deployment shares.
fn keyed_mac(
    config: &Config,
    scope: &str,
    kind: &str,
    value: &str,
) -> Result<hmac::Hmac<sha2::Sha256>, StorageError> {
    use hmac::{KeyInit, Mac};
    let key = config
        .identity
        .hashing_key
        .as_ref()
        .ok_or(StorageError::AuthorizationUnavailable)?
        .resolve(&ProcessEnvironment)
        .map_err(|_| StorageError::AuthorizationUnavailable)?;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key.expose().as_bytes())
        .map_err(|_| StorageError::AuthorizationUnavailable)?;
    for part in ["fyrr-hmac-2", scope, kind, value] {
        mac.update(&(part.len() as u64).to_be_bytes());
        mac.update(part.as_bytes());
    }
    Ok(mac)
}

pub fn hash_principal(config: &Config, prefix: &str, value: &str) -> Result<String, StorageError> {
    hash_in_domain(config, &config.namespace.name, prefix, prefix, value)
}

/// `<prefix>:<hex>` of the HMAC of `value` in its own MAC domain and `scope`.
/// `keyed_mac` frames every part with its length, so two different domains or
/// scopes never yield the same MAC input, whatever the values: a hash in one
/// cannot equal one in another except by an HMAC-SHA256 collision.
fn hash_in_domain(
    config: &Config,
    scope: &str,
    prefix: &str,
    domain: &str,
    value: &str,
) -> Result<String, StorageError> {
    use hmac::Mac;
    Ok(format!(
        "{prefix}:{}",
        hex::encode(
            keyed_mac(config, scope, domain, value)?
                .finalize()
                .into_bytes()
        )
    ))
}

/// The subject key of an API-key secret, as a `key:` hash. Its MAC domain is
/// not the one of `hash_principal(config, "key", …)` (directory users, the
/// stdio subject, OIDC subjects), so no value of those can produce it.
pub fn api_key_subject(config: &Config, secret: &str) -> Result<String, StorageError> {
    hash_in_domain(
        config,
        &config.namespace.name,
        "key",
        "api-key-secret",
        secret,
    )
}

/// The hash of an API-key secret that names no namespace: the same in every
/// namespace that shares a hashing key, so the one published set of keys
/// (`API_KEYS_FILE`) can name a key for all of them. Only that set holds it;
/// each namespace's identity binds [`api_key_subject`].
pub fn api_key_deployment_hash(config: &Config, secret: &str) -> Result<String, StorageError> {
    hash_in_domain(config, "", "key", "api-key-secret", secret)
}

/// The principal key an API key acts as: the hash of its name in a domain of
/// its own, apart from every user and from the secret domain.
pub fn api_key_principal(config: &Config, name: &str) -> Result<String, StorageError> {
    hash_in_domain(config, &config.namespace.name, "key", "api-key-name", name)
}

pub(crate) fn verify_hash(
    config: &Config,
    kind: &str,
    value: &str,
    signature: &[u8],
) -> Result<(), StorageError> {
    use hmac::Mac;
    namespace_mac(config, kind, value)?
        .verify_slice(signature)
        .map_err(|_| StorageError::StaleCursor)
}

/// SQL string literal escaping shared by writer keys and reader predicates.
pub fn sql(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Opens the dataset at `uri` pinned to exactly `version`.
pub async fn open_version(
    uri: &str,
    options: StorageOptions,
    version: u64,
    shared: Option<(
        Arc<lance::session::Session>,
        Arc<crate::mirror::MirrorStore>,
    )>,
) -> lance::Result<Dataset> {
    let wrapper = shared
        .as_ref()
        .map(|(_, mirror)| mirror.clone() as Arc<dyn lance_io::object_store::WrappingObjectStore>);
    let mut builder = DatasetBuilder::from_uri(uri)
        .with_store_params(native_store_params(wrapper))
        .with_storage_options(options.into_map().into_iter().collect())
        .with_version(version);
    if let Some((session, _)) = &shared {
        builder = builder.with_session(session.clone());
    }
    builder.load().await
}

pub async fn artifact<T: fyrr_core::Contract>(
    store: &dyn object_store::ObjectStore,
    root: &Path,
    pointer: &GenerationPointer,
) -> Result<T, StorageError> {
    let bytes = store
        .get(&object_store::path::Path::from(format!(
            "{root}/{}",
            pointer.object
        )))
        .await
        .map_err(|_| StorageError::StorageUnavailable)?
        .bytes()
        .await
        .map_err(|_| StorageError::StorageUnavailable)?;
    if digest(&bytes) != pointer.digest {
        return Err(StorageError::AuthorizationUnavailable);
    }
    parse_contract(&bytes).map_err(|_| StorageError::AuthorizationUnavailable)
}

/// `config` with the settings its namespace was created with in place of the
/// defaults the file supplies, or the file itself while the namespace does not
/// exist. `None` when the namespace has a reranker the file cannot reach.
pub async fn with_namespace_settings(config: &Config) -> Result<Option<Config>, StorageError> {
    let Some(head) = optional_head(config).await? else {
        return Ok(Some(config.clone()));
    };
    let (store, root) = namespace_store(config)?;
    let policy: fyrr_core::publication::PolicyArtifact =
        artifact(store.as_ref(), &root, &head.policy).await?;
    Ok(config.with_settings(&policy.settings))
}

/// A reader configured with a directory provider serves only an identity that
/// holds a published directory read; none is served before the first one.
pub fn directory_published(
    config: &Config,
    identity: &fyrr_core::identity::IdentityArtifact,
) -> bool {
    config.identity.directory.provider == fyrr_core::config::DirectoryProvider::None
        || identity
            .inputs
            .iter()
            .any(|i| i.kind == fyrr_core::identity::IdentityInputKind::Directory)
}

pub async fn pinned(
    config: &Config,
    head: &fyrr_core::publication::ServingHead,
) -> Result<Dataset, StorageError> {
    open_version(
        &dataset_uri(config),
        config
            .storage
            .object_store_options(&ProcessEnvironment)
            .map_err(|_| StorageError::StorageConfigurationInvalid)?,
        head.content
            .as_ref()
            .ok_or(StorageError::PublicationUnavailable)?
            .lance_version,
        None,
    )
    .await
    .map_err(|_| StorageError::StorageUnavailable)
}

pub fn roots_filter(source: &str, roots: &[String]) -> String {
    format!(
        "source_id={} AND root_id IN ({})",
        sql(source),
        roots.iter().map(|r| sql(r)).collect::<Vec<_>>().join(",")
    )
}
pub async fn read_manifest(
    store: &dyn ObjectStore,
    root: &Path,
    relative: &str,
) -> Result<fyrr_core::publication::ContentManifest, StorageError> {
    read_contract(store, &Path::from(format!("{root}/{relative}"))).await
}
pub fn manifest_chain(
    store: Arc<dyn ObjectStore>,
    root: Path,
    first: Option<String>,
) -> futures::stream::BoxStream<
    'static,
    Result<fyrr_core::publication::ContentManifest, StorageError>,
> {
    use futures::StreamExt;
    futures::stream::try_unfold((first, None), move |(path, newer_sequence)| {
        let store = store.clone();
        let root = root.clone();
        async move {
            let Some(path) = path else {
                return Ok(None);
            };
            let manifest = read_manifest(store.as_ref(), &root, &path).await?;
            // Every publication advances sequence. A backward walk must decrease,
            // so corrupt cycles terminate without retaining paths or capping history.
            if newer_sequence.is_some_and(|sequence| manifest.sequence >= sequence) {
                return Err(StorageError::PublicationInvalid);
            }
            let sequence = manifest.sequence;
            let previous = manifest
                .previous_publication_id
                .as_ref()
                .map(|id| format!("publications/{id}.json"));
            Ok(Some((manifest, (previous, Some(sequence)))))
        }
    })
    .boxed()
}

pub fn data_paths(dataset: &Dataset) -> std::collections::BTreeSet<String> {
    let mut paths = std::collections::BTreeSet::new();
    for fragment in dataset.manifest().fragments.iter() {
        for file in &fragment.files {
            paths.insert(format!("data/{}", file.path));
        }
        if let Some(deletion) = &fragment.deletion_file {
            paths.insert(format!(
                "_deletions/{}-{}-{}.{}",
                fragment.id,
                deletion.read_version,
                deletion.id,
                deletion.file_type.suffix()
            ));
        }
    }
    paths
}
pub fn relative_path(path: &Path, root: &Path) -> String {
    path.prefix_match(root).map_or_else(
        || path.to_string(),
        |parts| {
            parts
                .map(|part| part.as_ref().to_owned())
                .collect::<Vec<_>>()
                .join("/")
        },
    )
}

pub fn coverage(
    indexes: &[lance_table::format::IndexMetadata],
    name: &str,
) -> std::collections::BTreeSet<u32> {
    indexes
        .iter()
        .filter(|i| i.name == name)
        .flat_map(|i| i.fragment_bitmap.iter().flat_map(|bitmap| bitmap.iter()))
        .collect()
}

#[path = "storage/local.rs"]
mod local;
pub(crate) use local::e_tag as local_file_e_tag;
pub(crate) use local::new_tag as random_tag;

#[cfg(test)]
#[path = "storage/tests.rs"]
mod tests;
