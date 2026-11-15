use super::*;

pub(crate) fn configured_identity(
    config: &Config,
    generation: u64,
    now: chrono::DateTime<Utc>,
) -> Result<IdentityArtifact> {
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
        built_at: now,
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
    let now = Utc::now();
    let mut incarnation = [0; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut incarnation)
        .map_err(|_| WriterError::RuntimeUnavailable)?;
    let identity = configured_identity(config, 1, now)?;
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
        freshness: FreshnessPointers {
            directory: None,
            sources: Vec::new(),
        },
        lease: None,
        rejected_envelope_ids: Vec::new(),
        updated_at: now,
        incarnation: hex::encode(incarnation),
    };
    put_immutable(store.as_ref(), &root.join("head.json"), &head).await
}
