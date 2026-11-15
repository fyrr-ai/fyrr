//! Immutable membership/container inputs, referenced by the published identity generation.
use crate::{Result, WriterError};
use fyrr_core::config::Config;
use fyrr_core::connector::{BatchBody, ConnectorBatch, PrincipalSnapshotPart};
use fyrr_core::envelope::{EnvelopePayload, MembersPayload, PermissionsPayload, PreparedEnvelope};
use fyrr_core::identity::{IdentityArtifact, IdentityInput, IdentityInputKind, Principal};
use fyrr_core::publication::ServingHead;
use fyrr_core::write_contract;
use fyrr_reader::embedding::digest;
use fyrr_reader::storage::{hash_principal, namespace_store};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::collections::{BTreeMap, BTreeSet};

pub async fn build(
    config: &Config,
    current: &IdentityArtifact,
    head: &ServingHead,
    envelopes: &[PreparedEnvelope],
    mut configured: IdentityArtifact,
    member_paths: Option<&[(Path, String)]>,
) -> Result<IdentityArtifact> {
    let (store, root) = namespace_store(config)?;
    let mut inputs = current.inputs.clone();
    // A retired source's principal snapshot is no identity input.
    let policy: fyrr_core::publication::PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    inputs.retain(|input| {
        input.kind != IdentityInputKind::Source || !policy.retired_sources.contains_key(&input.id)
    });
    for envelope in envelopes {
        match &envelope.payload {
            EnvelopePayload::Members(payload) if payload.last => {
                let input = member_input(
                    config,
                    store.as_ref(),
                    &root,
                    head,
                    envelope,
                    payload,
                    member_paths,
                )
                .await?;
                inputs
                    .retain(|i| i.kind != IdentityInputKind::Source || i.id != envelope.source_id);
                inputs.push(input);
            }
            EnvelopePayload::Permissions(payload) if !payload.container_grants.is_empty() => {
                let input =
                    permission_input(store.as_ref(), &root, envelope, payload, &inputs).await?;
                inputs.retain(|i| i.id != input.id);
                inputs.push(input);
            }
            _ => {}
        }
    }
    let directory =
        read_directory(config, store.as_ref(), &root, &inputs, &configured.hashing).await?;
    let resolver = directory
        .as_ref()
        .map(crate::directory::DirectoryResolver::new);
    let mut grants: UserTokens = BTreeMap::new();
    let mut memberships = BTreeMap::new();
    if let Some(snapshot) = &directory {
        let active: BTreeSet<_> = snapshot
            .users
            .iter()
            .filter(|u| u.active)
            .map(|u| u.key.clone())
            .collect();
        let groups = snapshot
            .groups
            .iter()
            .map(|g| (g.token.clone(), g.members.clone()))
            .collect();
        memberships = crate::directory::expand(&groups, &active)?;
        for user in snapshot.users.iter().filter(|u| u.active) {
            let tokens = grants.entry(user.key.clone()).or_default();
            tokens.insert(user.token.clone());
            tokens.extend(memberships.get(&user.key).into_iter().flatten().cloned());
        }
        configured.subjects = crate::directory::bindings(snapshot);
    }
    if let Some(subject) = config.identity.stdio_user.as_deref() {
        let key = hash_principal(config, "key", subject)?;
        // The stdio subject holds the configured process grants. A configured
        // directory's activity/collision decisions take precedence.
        if directory.is_none() || grants.contains_key(&key) {
            let operator_tokens = grants.entry(key.clone()).or_default();
            operator_tokens.extend(configured.tokens.iter().cloned());
            for source in config
                .sources
                .iter()
                .filter(|s| s.permissions == fyrr_core::config::PermissionsMode::Sync)
            {
                for token in &config.identity.stdio_readers {
                    operator_tokens.insert(crate::apply::reader_token(config, &source.id, token)?);
                }
            }
        }
    }
    for input in &inputs {
        if input.kind == IdentityInputKind::Directory || input.id.starts_with("permissions:") {
            continue;
        }
        source_grants(
            config,
            store.as_ref(),
            &root,
            input,
            resolver.as_ref(),
            &mut grants,
            &mut memberships,
        )
        .await?;
    }
    container_grants(config, store.as_ref(), &root, &inputs, &mut grants).await?;
    let tokens: BTreeSet<_> = grants.values().flat_map(|v| v.iter().cloned()).collect();
    configured.tokens = tokens.into_iter().collect();
    configured.principals =
        grants
            .into_iter()
            .map(|(key, tokens)| Principal {
                key: key.clone(),
                groups: memberships
                    .get(&key)
                    .into_iter()
                    .flatten()
                    .map(|token| {
                        crate::narrow_u32(configured.tokens.binary_search(token).unwrap_or_else(
                            |_| unreachable!("membership tokens are included in the grants"),
                        ) as u64)
                    })
                    .collect(),
                tokens: tokens
                    .into_iter()
                    .map(|token| {
                        crate::narrow_u32(configured.tokens.binary_search(&token).unwrap_or_else(
                            |_| unreachable!("the token union contains every grant"),
                        ) as u64)
                    })
                    .collect(),
            })
            .collect();
    configured.inputs = inputs;
    Ok(configured)
}

async fn member_input(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    head: &ServingHead,
    envelope: &PreparedEnvelope,
    payload: &MembersPayload,
    member_paths: Option<&[(Path, String)]>,
) -> Result<IdentityInput> {
    let paths = match member_paths {
        Some(paths) => paths
            .iter()
            .map(|(p, hash)| (p.clone(), Some(hash)))
            .collect(),
        None => crate::commit::envelope_paths(config, head)
            .await?
            .0
            .into_iter()
            .map(|p| (p, None))
            .collect::<Vec<_>>(),
    };
    let mut parts = BTreeMap::new();
    for (path, expected) in paths {
        let item: PreparedEnvelope = if let Some(hash) = expected {
            let bytes = store.get(&path).await?.bytes().await?;
            if digest(&bytes) != *hash {
                return Err(WriterError::RestoreHistoryIdentityChanged);
            }
            fyrr_core::parse_contract(&bytes).map_err(|_| WriterError::PublicationInvalid)?
        } else {
            fyrr_reader::storage::read_contract(store, &path).await?
        };
        if item.source_id != envelope.source_id
            || item.rejection_generation != envelope.rejection_generation
        {
            continue;
        }
        let EnvelopePayload::Members(p) = item.payload else {
            continue;
        };
        if item.sequence < envelope.sequence.saturating_sub(payload.part as u64)
            || item.sequence > envelope.sequence
            || p.snapshot_id != payload.snapshot_id
            || p.part > payload.part
        {
            continue;
        }
        let batch: ConnectorBatch = crate::secrets::read(
            config,
            store,
            &object_store::path::Path::from(format!("{root}/{}", p.snapshot_object)),
        )
        .await?;
        if digest(&write_contract(&batch)) != p.snapshot_digest {
            return Err(WriterError::MembershipSnapshotInvalid);
        }
        let BatchBody::PrincipalSnapshot(part) = batch.body else {
            return Err(WriterError::MembershipSnapshotInvalid);
        };
        if parts.insert(part.part, part).is_some() {
            return Err(WriterError::MembershipSnapshotInvalid);
        }
    }
    if parts.len() != payload.part as usize + 1
        || (0..=payload.part).any(|p| !parts.contains_key(&p))
    {
        return Err(WriterError::MembershipSnapshotIncomplete);
    }
    let mut snapshot = PrincipalSnapshotPart {
        snapshot_id: payload.snapshot_id.clone(),
        part: 0,
        last: true,
        users: Vec::new(),
        groups: Vec::new(),
    };
    for part in parts.into_values() {
        snapshot.users.extend(part.users);
        snapshot.groups.extend(part.groups);
    }
    let batch = ConnectorBatch {
        schema_version: fyrr_core::connector::CONNECTOR_CONTRACT_VERSION,
        source_id: envelope.source_id.clone(),
        kind: envelope.kind,
        cursor: envelope.cursor.clone(),
        body: BatchBody::PrincipalSnapshot(snapshot),
    };
    let hash = digest(&write_contract(&batch));
    let object = format!("members/{}.json", &hash[7..]);
    crate::secrets::put(
        config,
        store,
        &object_store::path::Path::from(format!("{root}/{object}")),
        &batch,
    )
    .await?;
    Ok(IdentityInput {
        kind: IdentityInputKind::Source,
        id: envelope.source_id.clone(),
        object,
        digest: hash,
        read_at: payload.read_at,
    })
}

async fn permission_input(
    store: &dyn ObjectStore,
    root: &Path,
    envelope: &PreparedEnvelope,
    payload: &PermissionsPayload,
    inputs: &[IdentityInput],
) -> Result<IdentityInput> {
    let id = format!("permissions:{}", envelope.source_id);
    let mut grants = BTreeMap::new();
    if let Some(old) = inputs.iter().find(|i| i.id == id) {
        let prior: PreparedEnvelope = fyrr_reader::storage::read_contract(
            store,
            &object_store::path::Path::from(format!("{root}/{}", old.object)),
        )
        .await?;
        if let EnvelopePayload::Permissions(p) = prior.payload {
            for g in p.container_grants {
                grants.insert(g.container_id.clone(), g);
            }
        }
    }
    for grant in &payload.container_grants {
        grants.insert(grant.container_id.clone(), grant.clone());
    }
    let mut snapshot = envelope.clone();
    let EnvelopePayload::Permissions(p) = &mut snapshot.payload else {
        unreachable!()
    };
    p.documents.clear();
    p.container_grants = grants.into_values().collect();
    let hash = digest(&write_contract(&snapshot));
    let object = format!("grants/{}.json", &hash[7..]);
    crate::prepare::put_immutable(
        store,
        &object_store::path::Path::from(format!("{root}/{object}")),
        &snapshot,
    )
    .await?;
    Ok(IdentityInput {
        kind: IdentityInputKind::Source,
        id,
        object,
        digest: hash,
        read_at: payload.read_at,
    })
}

async fn read_directory(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    inputs: &[IdentityInput],
    hashing: &fyrr_core::identity::HashingScheme,
) -> Result<Option<fyrr_core::directory::DirectorySnapshot>> {
    let directory_inputs: Vec<_> = inputs
        .iter()
        .filter(|i| i.kind == IdentityInputKind::Directory)
        .collect();
    if directory_inputs.len() > 1 {
        return Err(WriterError::DirectorySnapshotInvalid);
    }
    let directory = if let Some(input) = directory_inputs.first() {
        let snapshot: fyrr_core::directory::DirectorySnapshot = crate::secrets::read(
            config,
            store,
            &object_store::path::Path::from(format!("{root}/{}", input.object)),
        )
        .await?;
        if digest(&write_contract(&snapshot)) != input.digest
            || snapshot.namespace != config.namespace.name
            || snapshot.hashing != *hashing
        {
            return Err(WriterError::DirectorySnapshotInvalid);
        }
        Some(snapshot)
    } else {
        None
    };
    Ok(directory)
}

type UserTokens = BTreeMap<String, BTreeSet<String>>;

async fn source_grants(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    input: &IdentityInput,
    resolver: Option<&crate::directory::DirectoryResolver>,
    grants: &mut UserTokens,
    memberships: &mut UserTokens,
) -> Result<()> {
    let batch: ConnectorBatch = crate::secrets::read(
        config,
        store,
        &object_store::path::Path::from(format!("{root}/{}", input.object)),
    )
    .await?;
    if digest(&write_contract(&batch)) != input.digest {
        return Err(WriterError::MembershipSnapshotInvalid);
    }
    let BatchBody::PrincipalSnapshot(snapshot) = batch.body else {
        return Err(WriterError::MembershipSnapshotInvalid);
    };
    let mut users = BTreeMap::new();
    let mut user_ids = BTreeSet::new();
    let mut group_ids = BTreeSet::new();
    if snapshot.users.iter().any(|u| !user_ids.insert(&u.id))
        || snapshot.groups.iter().any(|g| !group_ids.insert(&g.id))
    {
        return Err(WriterError::MembershipSnapshotInvalid);
    }
    let mut emails: BTreeMap<String, usize> = BTreeMap::new();
    for user in &snapshot.users {
        if let Some(email) = &user.email {
            *emails.entry(email.trim().to_lowercase()).or_default() += 1;
        }
    }
    for user in snapshot.users {
        if !user.active {
            continue;
        }
        let key = if let Some(resolver) = resolver {
            resolver.account(
                config,
                &input.id,
                &user.id,
                user.email
                    .as_deref()
                    .filter(|email| emails.get(&email.trim().to_lowercase()) == Some(&1)),
                user.email_verification,
            )?
        } else {
            // Without a directory, the same corporate verification evidence is required.
            user.email
                .filter(|_| {
                    user.email_verification
                        == fyrr_core::connector::EmailVerification::VerifiedCorporate
                })
                .filter(|email| emails.get(&email.trim().to_lowercase()) == Some(&1))
                .map(|email| hash_principal(config, "key", &email.trim().to_lowercase()))
                .transpose()?
        };
        if let Some(key) = key {
            users.insert(user.id, key);
        }
    }
    let groups: BTreeMap<_, _> = snapshot
        .groups
        .into_iter()
        .map(|g| (g.id, g.members))
        .collect();
    for (id, key) in &users {
        grants
            .entry(key.clone())
            .or_default()
            .insert(crate::apply::reader_token(
                config,
                &input.id,
                &format!("usr:{id}"),
            )?);
    }
    let groups = groups
        .into_iter()
        .map(|(id, members)| (format!("grp:{id}"), members))
        .collect();
    let user_ids = users.keys().map(|id| format!("usr:{id}")).collect();
    for (member, groups) in crate::directory::expand(&groups, &user_ids)? {
        let key = &users[member
            .strip_prefix("usr:")
            .ok_or(WriterError::MembershipSnapshotInvalid)?];
        for group in groups {
            let token = crate::apply::reader_token(config, &input.id, &group)?;
            grants.entry(key.clone()).or_default().insert(token.clone());
            memberships.entry(key.clone()).or_default().insert(token);
        }
    }
    Ok(())
}

async fn container_grants(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    inputs: &[IdentityInput],
    grants: &mut UserTokens,
) -> Result<()> {
    for input in inputs {
        if !input.id.starts_with("permissions:") {
            continue;
        }
        let snapshot: PreparedEnvelope = fyrr_reader::storage::read_contract(
            store,
            &object_store::path::Path::from(format!("{root}/{}", input.object)),
        )
        .await?;
        if digest(&write_contract(&snapshot)) != input.digest {
            return Err(WriterError::PermissionSnapshotInvalid);
        }
        let EnvelopePayload::Permissions(payload) = snapshot.payload else {
            return Err(WriterError::PermissionSnapshotInvalid);
        };
        for grant in payload.container_grants {
            let allowed = grant.readers.iter().cloned().collect::<BTreeSet<_>>();
            let token =
                crate::apply::container_token(config, &snapshot.source_id, &grant.container_id)?;
            for user in grants.values_mut() {
                if !user.is_disjoint(&allowed) {
                    user.insert(token.clone());
                }
            }
        }
    }
    Ok(())
}
