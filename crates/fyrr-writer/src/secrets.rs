//! AES-256-GCM writer storage, bound to namespace and object path. No plaintext fallback.
use crate::{Result, WriterError};
use fyrr_core::config::{Config, ProcessEnvironment, SecretRef};
use fyrr_core::secret::EncryptedSecret;
use fyrr_core::{Contract, parse_contract, write_contract};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use ring::{
    aead,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Serialize, de::DeserializeOwned};

fn key(config: &Config) -> Result<(aead::LessSafeKey, String)> {
    let secret = config
        .identity
        .encryption_key
        .as_ref()
        .ok_or(WriterError::EncryptionKeyMissing)?
        .resolve(&ProcessEnvironment)
        .map_err(|_| WriterError::EncryptionKeyMissing)?;
    let mut bytes = hex::decode(secret.expose()).map_err(|_| WriterError::EncryptionKeyInvalid)?;
    if bytes.len() != 32 {
        return Err(WriterError::EncryptionKeyInvalid);
    }
    let id = fyrr_core::limits::digest(&bytes)[7..].to_owned();
    let key = aead::UnboundKey::new(&aead::AES_256_GCM, &bytes)
        .map_err(|_| WriterError::EncryptionKeyInvalid)?;
    bytes.fill(0);
    Ok((aead::LessSafeKey::new(key), id))
}
fn aad(config: &Config, path: &Path, version: fyrr_core::SchemaVersion) -> String {
    format!(
        "fyrr-encrypted-secret:{}:{}:{}:{}",
        version,
        config.namespace.name.len(),
        config.namespace.name,
        path
    )
}
pub fn seal(config: &Config, path: &Path, plaintext: &[u8]) -> Result<EncryptedSecret> {
    if plaintext.len() > 16 * 1024 * 1024 {
        return Err(WriterError::SecretInputTooLarge);
    }
    let (key, key_id) = key(config)?;
    let mut nonce = [0; 12];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| WriterError::EncryptionUnavailable)?;
    let mut bytes = plaintext.to_vec();
    key.seal_in_place_append_tag(
        aead::Nonce::assume_unique_for_key(nonce),
        aead::Aad::from(aad(config, path, EncryptedSecret::VERSION).as_bytes()),
        &mut bytes,
    )
    .map_err(|_| WriterError::EncryptionUnavailable)?;
    Ok(EncryptedSecret {
        schema_version: EncryptedSecret::VERSION,
        algorithm: "aes-256-gcm".into(),
        key_id,
        nonce: hex::encode(nonce),
        ciphertext: hex::encode(bytes),
    })
}
pub fn open(config: &Config, path: &Path, envelope: &EncryptedSecret) -> Result<Vec<u8>> {
    envelope
        .validate()
        .map_err(|_| WriterError::EncryptedInputInvalid)?;
    let (key, id) = key(config)?;
    if envelope.key_id != id {
        return Err(WriterError::EncryptionKeyRotated);
    }
    let nonce: [u8; 12] = hex::decode(&envelope.nonce)
        .map_err(|_| WriterError::EncryptedInputInvalid)?
        .try_into()
        .map_err(|_| WriterError::EncryptedInputInvalid)?;
    let mut bytes =
        hex::decode(&envelope.ciphertext).map_err(|_| WriterError::EncryptedInputInvalid)?;
    let plaintext = key
        .open_in_place(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad(config, path, envelope.schema_version).as_bytes()),
            &mut bytes,
        )
        .map_err(|_| WriterError::EncryptedInputInvalid)?;
    Ok(plaintext.to_vec())
}
pub async fn read_bytes(config: &Config, store: &dyn ObjectStore, path: &Path) -> Result<Vec<u8>> {
    let object = store.get(path).await?;
    if object.meta.size > 34 * 1024 * 1024 {
        return Err(WriterError::EncryptedInputInvalid);
    }
    let envelope = parse_contract::<EncryptedSecret>(&object.bytes().await?)
        .map_err(|_| WriterError::EncryptedInputInvalid)?;
    open(config, path, &envelope)
}
/// Decryption does not replace the existing version and semantic contract checks.
pub async fn read<T: Contract>(config: &Config, store: &dyn ObjectStore, path: &Path) -> Result<T> {
    parse_contract(&read_bytes(config, store, path).await?)
        .map_err(|_| WriterError::EncryptedInputInvalid)
}
/// Private SCIM resource state is JSON; published inputs use the contract reader.
pub(crate) async fn read_json<T: DeserializeOwned>(
    config: &Config,
    store: &dyn ObjectStore,
    path: &Path,
) -> Result<T> {
    serde_json::from_slice(&read_bytes(config, store, path).await?)
        .map_err(|_| WriterError::EncryptedInputInvalid)
}
pub async fn put<T: Serialize>(
    config: &Config,
    store: &dyn ObjectStore,
    path: &Path,
    value: &T,
) -> Result<()> {
    let plaintext = serde_json::to_vec(value).map_err(|_| WriterError::EncryptedInputInvalid)?;
    let envelope = seal(config, path, &plaintext)?;
    match store
        .put_opts(
            path,
            write_contract(&envelope).into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(
            object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. },
        ) => {
            if read_bytes(config, store, path).await? == plaintext {
                Ok(())
            } else {
                Err(WriterError::ImmutableObjectConflict)
            }
        }
        Err(_) => Err(WriterError::StorageUnavailable),
    }
}
fn credential_path(root: &Path, name: &str) -> Path {
    Path::from(format!(
        "{root}/credentials/{}.json",
        &fyrr_reader::embedding::digest(name.as_bytes())[7..]
    ))
}
pub async fn credential_put(config: &Config, name: &str, from: &str) -> Result<serde_json::Value> {
    if name.is_empty() || name.len() > 256 || from.is_empty() || from.len() > 256 {
        return Err(WriterError::CredentialReferenceInvalid);
    }
    let secret = SecretRef { env: from.into() }
        .resolve(&ProcessEnvironment)
        .map_err(|_| WriterError::CredentialUnavailable)?;
    crate::lease::LeaseGuard::with(config, |lease| async move {
        async {
            let (store, root) = fyrr_reader::storage::namespace_store(config)?;
            let path = credential_path(&root, name);
            // Refuse old envelopes after rotation, even if a fresh source secret is supplied.
            match store.head(&path).await {
                Ok(_) => {
                    if read_bytes(config, store.as_ref(), &path).await?
                        == secret.expose().as_bytes()
                    {
                        return Ok(serde_json::json!({"stored":true,"changed":false}));
                    }
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(_) => return Err(WriterError::StorageUnavailable),
            }
            let envelope = seal(config, &path, secret.expose().as_bytes())?;
            let _ = lease.head().await?;
            store.put(&path, write_contract(&envelope).into()).await?;
            Ok(serde_json::json!({"stored":true,"changed":true}))
        }
        .await
    })
    .await
}

pub async fn resolve(config: &Config, reference: &SecretRef) -> Result<String> {
    if let Some(name) = config.writer.credentials.get(&reference.env) {
        let (store, root) = fyrr_reader::storage::namespace_store(config)?;
        String::from_utf8(read_bytes(config, store.as_ref(), &credential_path(&root, name)).await?)
            .map_err(|_| WriterError::CredentialInvalid)
    } else {
        reference
            .resolve(&ProcessEnvironment)
            .map(|s| s.expose().to_owned())
            .map_err(|_| WriterError::CredentialUnavailable)
    }
}
/// Resolved credential values, as (environment variable, value).
pub type Credentials = Vec<(String, String)>;

/// The stored credentials that `source` references, for its provider process's
/// environment. Invoked only inside the already isolated worker, before any
/// provider runs.
pub fn worker_credentials(config: &Config, source: &str) -> Result<Credentials> {
    if config.writer.credentials.is_empty() {
        return Ok(Vec::new());
    }
    let source = config
        .sources
        .iter()
        .find(|s| s.id == source)
        .ok_or(WriterError::SourceUnavailable)?;
    let mut found = Vec::new();
    for table in [&source.settings, &source.scope] {
        fyrr_core::config::env_references(
            "",
            &serde_json::Value::Object(table.clone()),
            &mut found,
        );
    }
    let referenced: std::collections::BTreeSet<_> =
        found.into_iter().map(|(_, name)| name).collect();
    let runtime = tokio::runtime::Runtime::new().map_err(|_| WriterError::CredentialUnavailable)?;
    let mut values = Vec::new();
    for env in config
        .writer
        .credentials
        .keys()
        .filter(|env| referenced.contains(*env))
    {
        values.push((
            env.clone(),
            runtime.block_on(resolve(config, &SecretRef { env: env.clone() }))?,
        ));
    }
    Ok(values)
}
