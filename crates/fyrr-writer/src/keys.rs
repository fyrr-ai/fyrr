//! The API keys of a deployment, published as one artifact under the storage
//! root that the readers of every namespace read.
use crate::lease::{cas_put, version_of};
use crate::membership::usable_api_keys;
use crate::{Result, WriterError};
use fyrr_core::config::Config;
use fyrr_core::identity::{API_KEYS_CONTRACT_VERSION, ApiKeyGrant, ApiKeysArtifact};
use fyrr_core::write_contract;
use fyrr_reader::storage::{API_KEYS_FILE, deployment_root, namespace_store};
use object_store::{ObjectStoreExt, PutMode};

/// Publishes the configured keys with their roles and namespace prefixes as
/// keyed hashes; no secret, name or reader token leaves the writer. The
/// artifact is the deployment's whole key set, and a reader that declares keys
/// refuses every key without it: a key missing from it is refused. So a key
/// whose secret is unset or malformed fails the run, instead of being revoked
/// silently everywhere. Written by compare-and-swap, and not at all when it is
/// current, so a repeated run is harmless; a competing publisher fails the run
/// as `committer_busy`.
pub async fn publish(config: &Config) -> Result<serde_json::Value> {
    let usable = usable_api_keys(config)?;
    if usable.len() != config.identity.api_keys.len() {
        return Err(WriterError::ConfigurationInvalid);
    }
    let keys: Vec<_> = usable
        .into_iter()
        .map(|(key, _, subject)| ApiKeyGrant {
            subject,
            roles: key.roles.clone(),
            namespace_prefix: key.namespace_prefix.clone(),
        })
        .collect();
    let count = keys.len();
    let artifact = ApiKeysArtifact {
        schema_version: API_KEYS_CONTRACT_VERSION,
        keys,
    };
    let (store, root) = namespace_store(config)?;
    let path = deployment_root(&root).join(API_KEYS_FILE);
    let mode = match store.get(&path).await {
        Ok(current) => {
            let version = version_of(&current.meta);
            let held = current
                .bytes()
                .await
                .map_err(|_| WriterError::StorageUnavailable)?;
            if held == write_contract(&artifact) {
                return Ok(serde_json::json!({"keys": count, "changed": false}));
            }
            PutMode::Update(version)
        }
        Err(object_store::Error::NotFound { .. }) => PutMode::Create,
        Err(_) => return Err(WriterError::StorageUnavailable),
    };
    cas_put(store.as_ref(), &path, &artifact, mode)
        .await
        .map_err(|error| match error {
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. } => WriterError::CommitterBusy,
            _ => WriterError::StorageUnavailable,
        })?;
    Ok(serde_json::json!({"keys": count, "changed": true}))
}
