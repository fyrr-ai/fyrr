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

/// One digest-verified immutable artifact shared by a serving generation.
pub(crate) type ArtifactCache<T> = Arc<tokio::sync::Mutex<Option<(GenerationPointer, Arc<T>)>>>;

#[cfg(all(feature = "test-store", not(debug_assertions)))]
compile_error!("test-store must not be enabled in shipping builds");
#[cfg(feature = "test-store")]
#[path = "storage/test_store.rs"]
pub mod test_store;

pub fn native_store_params(
    mirror: Option<Arc<dyn lance_io::object_store::WrappingObjectStore>>,
) -> lance_io::object_store::ObjectStoreParams {
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
        return Ok((store, root, CleanupDurability(Some(local))));
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
            Ok((store, root))
        }
        Some(StorageBackend::S3 { .. }) => {
            let (store, root) = s3_namespace_store(config)?;
            Ok((Arc::new(store), root))
        }
        None => Err(StorageError::StorageConfigurationInvalid),
    }
}

fn s3_namespace_store(
    config: &Config,
) -> Result<(object_store::aws::AmazonS3, Path), StorageError> {
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
    let store = builder
        .build()
        .map_err(|_| StorageError::StorageConfigurationInvalid)?;
    if instance_disabled {
        return Err(StorageError::StorageUnavailable);
    }
    let root = Path::from(prefix)
        .join("namespaces")
        .join(config.namespace.name.as_str());
    Ok((store, root))
}

/// Delete only the current namespace head matching the holder's CAS token.
/// S3 deletes the current key without versionId so an older head cannot revive.
pub async fn delete_head_if_match(
    config: &Config,
    expected: &object_store::UpdateVersion,
) -> object_store::Result<()> {
    use object_store::signer::{HeaderName, HeaderValue, Method, SignedUrlOptions, Signer};
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
            let deletion = store.delete_if_match(path.clone(), tag.to_owned());
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
            let (store, root) = s3_namespace_store(config).map_err(|_| failure())?;
            let path = root.join("head.json");
            let header = HeaderValue::from_str(tag).map_err(|_| failure())?;
            let options = SignedUrlOptions::new()
                .with_signed_header(HeaderName::from_static("if-match"), header.clone());
            let url = store
                .signed_url_opts(
                    Method::DELETE,
                    &path,
                    std::time::Duration::from_secs(30),
                    &options,
                )
                .await
                .map_err(|_| failure())?;
            // Never include the signed URL or transport error in diagnostics.
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .map_err(|_| failure())?;
            let status = client
                .delete(url)
                .header(reqwest::header::IF_MATCH, header)
                .send()
                .await
                .map_err(|_| failure())?
                .status();
            if status.is_success() {
                return Ok(());
            }
            if status == reqwest::StatusCode::PRECONDITION_FAILED
                || status == reqwest::StatusCode::CONFLICT
            {
                return Err(object_store::Error::Precondition {
                    path: path.to_string(),
                    source: "namespace head changed".into(),
                });
            }
            Err(failure())
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
    for part in ["fyrr-hmac-2", config.namespace.name.as_str(), kind, value] {
        mac.update(&(part.len() as u64).to_be_bytes());
        mac.update(part.as_bytes());
    }
    Ok(mac)
}

pub fn hash_principal(config: &Config, prefix: &str, value: &str) -> Result<String, StorageError> {
    use hmac::Mac;
    Ok(format!(
        "{prefix}:{}",
        hex::encode(
            namespace_mac(config, prefix, value)?
                .finalize()
                .into_bytes()
        )
    ))
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

/// Directory proof is valid strictly before its deadline, never in the future.
/// Disabled providers use immutable operator-managed identity and need no live proof.
pub fn directory_fresh(
    config: &Config,
    freshness: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    if config.identity.directory.provider == fyrr_core::config::DirectoryProvider::None {
        return true;
    }
    freshness.is_some_and(|at| {
        at <= now
            && (now - at)
                .to_std()
                .is_ok_and(|age| age < config.identity.validity_deadline.as_duration())
    })
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

#[cfg(test)]
#[path = "storage/tests.rs"]
mod tests;
