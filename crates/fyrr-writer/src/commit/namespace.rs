use super::*;

pub(crate) fn configured_identity(config: &Config, generation: u64) -> Result<IdentityArtifact> {
    // Even an empty HTTP-only namespace must have its HMAC key available.
    hash_principal(config, "key", "")?;
    let user = config.identity.stdio_user.as_deref();
    let mut tokens: Vec<_> = config
        .identity
        .stdio_readers
        .iter()
        .map(|t| hash_principal(config, "tok", t))
        .collect::<Result<_, _>>()?;
    tokens.sort();
    tokens.dedup();
    Ok(IdentityArtifact {
        schema_version: IDENTITY_CONTRACT_VERSION,
        namespace: config.namespace.name.clone(),
        generation,
        hashing: HashingScheme {
            algorithm: "hmac-sha256".into(),
            key_id: "namespace-v1".into(),
        },
        principals: user
            .map(|user| {
                Ok::<Principal, WriterError>(Principal {
                    key: hash_principal(config, "key", user)?,
                    tokens: (0..crate::narrow_u32(tokens.len() as u64)).collect(),
                    groups: Vec::new(),
                })
            })
            .transpose()?
            .into_iter()
            .collect(),
        tokens,
        inputs: Vec::new(),
        subjects: Vec::new(),
    })
}

pub async fn create(config: &Config) -> Result<()> {
    let (store, root) = namespace_store(config)?;
    if fyrr_reader::storage::optional_head(config).await?.is_some() {
        return Ok(());
    }
    let now = fyrr_core::clock::now();
    let mut incarnation = [0; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut incarnation)
        .map_err(|_| WriterError::RuntimeUnavailable)?;
    let identity = configured_identity(config, 1)?;
    let policy = PolicyArtifact {
        fixed_grants: fixed_grants(config)?,
        row_acl_sequence: None,
        retired_sources: Default::default(),
        schema_version: <PolicyArtifact as fyrr_core::Contract>::VERSION,
        namespace: config.namespace.name.clone(),
        generation: 1,
        retrieval_policy_digest: config.namespace.retrieval_digest(),
    };
    let identity_pointer =
        crate::prepare::put_generation(store.as_ref(), &root, "identity", 1, &identity).await?;
    let policy_pointer =
        crate::prepare::put_generation(store.as_ref(), &root, "policy", 1, &policy).await?;
    let head = ServingHead {
        schema_version: PUBLICATION_CONTRACT_VERSION,
        namespace: config.namespace.name.clone(),
        content: None,
        policy: policy_pointer,
        identity: identity_pointer,
        lease: None,
        rejected_envelope_ids: Vec::new(),
        updated_at: now,
        incarnation: hex::encode(incarnation),
    };
    put_immutable(store.as_ref(), &root.join("head.json"), &head).await
}

// This coordination record is not a publication or user data. Its authority is
// the matching namespace lease and incarnation, never the record's existence.
const DELETE_FENCE: &str = ".namespace-delete.json";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteFence {
    version: u8,
    namespace: String,
    incarnation: String,
    holder: String,
    epoch: u64,
}

async fn deletion_fence(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
) -> Result<Option<(DeleteFence, object_store::UpdateVersion)>> {
    let object = match store.get(&root.clone().join(DELETE_FENCE)).await {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(_) => return Err(WriterError::StorageUnavailable),
    };
    if object.meta.size > 4096 {
        return Err(WriterError::LeaseInvalid);
    }
    let version = crate::lease::version_of(&object.meta);
    let fence: DeleteFence =
        serde_json::from_slice(&object.bytes().await?).map_err(|_| WriterError::LeaseInvalid)?;
    if fence.version != 1
        || fence.namespace != config.namespace.name
        || (!fence.incarnation.is_empty() && !fyrr_core::limits::ascii_hex(&fence.incarnation, 32))
        || !fence
            .holder
            .strip_prefix("holder-")
            .is_some_and(|holder| fyrr_core::limits::ascii_hex(holder, 32))
    {
        return Err(WriterError::LeaseInvalid);
    }
    Ok(Some((fence, version)))
}

/// Cursor acquisition checks on both sides of its CAS, so a new source cannot
/// enter between the deletion fence and the active-worker inventory.
pub(crate) async fn sources_allowed(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
) -> Result<()> {
    let Some((fence, _)) = deletion_fence(config, store, root).await? else {
        return Ok(());
    };
    let head = fyrr_reader::storage::optional_head(config)
        .await?
        .ok_or(WriterError::NamespaceUnavailable)?;
    if head.namespace != config.namespace.name {
        return Err(WriterError::PublicationNamespaceInvalid);
    }
    if head.incarnation == fence.incarnation
        && head
            .lease
            .as_ref()
            .is_some_and(|lease| lease.holder == fence.holder && lease.epoch == fence.epoch)
    {
        if head
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at > fyrr_core::clock::now())
        {
            return Err(WriterError::SyncBusy);
        }
        // An expired deleter may still have a conditional request in flight.
        // Advance the namespace token before permitting a source to write.
        LeaseGuard::with(config, |_| async { Ok::<(), WriterError>(()) }).await?;
    }
    Ok(())
}

async fn fence_sources(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    lease: &LeaseGuard,
) -> Result<()> {
    let head = lease.head().await?;
    let fence = DeleteFence {
        version: 1,
        namespace: config.namespace.name.clone(),
        incarnation: head.incarnation,
        holder: lease.holder().holder.clone(),
        epoch: lease.holder().epoch,
    };
    let mode = match deletion_fence(config, store, root).await? {
        Some((_, version)) => object_store::PutMode::Update(version),
        None => object_store::PutMode::Create,
    };
    let bytes = serde_json::to_vec(&fence).map_err(|_| WriterError::LeaseInvalid)?;
    store
        .put_opts(&root.clone().join(DELETE_FENCE), bytes.into(), mode.into())
        .await?;
    let cursors: Vec<_> = store
        .list(Some(&root.clone().join("cursors")))
        .try_collect()
        .await?;
    for object in cursors {
        lease.head().await?;
        let state: fyrr_core::connector::CursorState =
            fyrr_reader::storage::read_contract(store, &object.location).await?;
        if state.namespace != config.namespace.name {
            return Err(WriterError::CursorInvalid);
        }
        if state
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at > fyrr_core::clock::now())
        {
            return Err(WriterError::SyncBusy);
        }
    }
    Ok(())
}

pub async fn delete(
    config: &Config,
    export: Option<&std::path::Path>,
) -> Result<serde_json::Value> {
    let (store, root) = namespace_store(config)?;
    if fyrr_reader::storage::optional_head(config).await?.is_none() {
        return Ok(serde_json::json!({"deleted":true}));
    }
    let Some(export) = export else {
        let objects: Vec<_> = store.list(Some(&root)).try_collect().await?;
        return Ok(
            serde_json::json!({"dry_run":true,"objects":objects.len(),"bytes":objects.iter().map(|object|object.size).sum::<u64>(),"requires_verified_export":true}),
        );
    };
    LeaseGuard::with(config, |lease| async move {
        fence_sources(config, store.as_ref(), &root, &lease).await?;
        let objects: Vec<_> = store.list(Some(&root)).try_collect().await?;
        let verified: BTreeMap<_, _> = objects
            .iter()
            .filter(|object| {
                object.location != root.clone().join(DELETE_FENCE)
                    && object.location != root.clone().join("head.json")
            })
            .map(|object| {
                (
                    object.location.clone(),
                    (
                        object.e_tag.clone(),
                        object.version.clone(),
                        object.size,
                        object.last_modified,
                    ),
                )
            })
            .collect();
        for object in objects {
            if object.location == root.clone().join(DELETE_FENCE) {
                continue;
            }
            lease.head().await?;
            let relative = fyrr_reader::storage::relative_path(&object.location, &root);
            let bytes = store.get(&object.location).await?.bytes().await?;
            let saved =
                std::fs::read(export.join(relative)).map_err(|_| WriterError::ExportInvalid)?;
            let matches = if object.location == root.clone().join("head.json") {
                // Taking/renewing the fence changes only volatile lease fields.
                let mut current: ServingHead =
                    fyrr_core::parse_contract(&bytes).map_err(|_| WriterError::ExportInvalid)?;
                let mut exported: ServingHead =
                    fyrr_core::parse_contract(&saved).map_err(|_| WriterError::ExportInvalid)?;
                current.lease = None;
                exported.lease = None;
                write_contract(&current) == write_contract(&exported)
            } else {
                digest(&saved) == digest(&bytes)
            };
            if !matches {
                return Err(WriterError::ExportInvalid);
            }
        }
        // A worker stalled past lease expiry may finish an already admitted PUT.
        // Its immutable envelope must be included in this verified export.
        let current: Vec<_> = store.list(Some(&root)).try_collect().await?;
        let current: BTreeMap<_, _> = current
            .into_iter()
            .filter(|object| {
                object.location != root.clone().join(DELETE_FENCE)
                    && object.location != root.clone().join("head.json")
            })
            .map(|object| {
                (
                    object.location,
                    (
                        object.e_tag,
                        object.version,
                        object.size,
                        object.last_modified,
                    ),
                )
            })
            .collect();
        if current != verified {
            return Err(WriterError::ExportInvalid);
        }
        lease.delete_head(config).await?;
        Ok(serde_json::json!({"deleted":true,"retained_data":true}))
    })
    .await
}
