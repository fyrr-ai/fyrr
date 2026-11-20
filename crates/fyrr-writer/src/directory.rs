//! Directory input publication shares the namespace lease and head CAS.
use crate::{Result, WriterError};
use fyrr_core::config::Config;
use fyrr_core::directory::DirectorySnapshot;
use fyrr_core::identity::{IdentityArtifact, IdentityInput, IdentityInputKind, SubjectBinding};
use fyrr_core::{Contract, write_contract};
use fyrr_reader::embedding::digest;
use fyrr_reader::storage::{hash_principal, namespace_store};
use object_store::path::Path;
use std::collections::{BTreeMap, BTreeSet};

/// The issuer is part of the identity; equal subjects from different issuers differ.
pub fn subject_key(config: &Config, issuer: &str, subject: &str) -> Result<String> {
    hash_principal(
        config,
        "key",
        &format!("oidc:{}:{issuer}{subject}", issuer.len()),
    )
    .map_err(WriterError::from)
}

/// Hashed, unambiguous directory lookups built once per snapshot.
pub(crate) struct DirectoryResolver {
    active: BTreeSet<String>,
    aliases: BTreeMap<String, BTreeSet<String>>,
    links: BTreeMap<(String, String), BTreeSet<String>>,
}
impl DirectoryResolver {
    pub(crate) fn new(snapshot: &DirectorySnapshot) -> Self {
        let mut aliases: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for user in &snapshot.users {
            for alias in std::iter::once(&user.key).chain(&user.aliases) {
                // Inactive accounts participate in collision detection too.
                aliases
                    .entry(alias.clone())
                    .or_default()
                    .insert(user.key.clone());
            }
        }
        let mut links: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
        for link in &snapshot.links {
            links
                .entry((link.source.clone(), link.user.clone()))
                .or_default()
                .insert(link.principal.clone());
        }
        Self {
            active: snapshot
                .users
                .iter()
                .filter(|u| u.active)
                .map(|u| u.key.clone())
                .collect(),
            aliases,
            links,
        }
    }
    fn unique(&self, keys: &BTreeSet<String>) -> Option<String> {
        if keys.len() != 1 {
            return None;
        }
        keys.first()
            .filter(|key| self.active.contains(*key))
            .cloned()
    }
    pub(crate) fn account(
        &self,
        config: &Config,
        source: &str,
        id: &str,
        email: Option<&str>,
        verification: fyrr_core::connector::EmailVerification,
    ) -> Result<Option<String>> {
        let user = hash_principal(config, "key", id)?;
        if let Some(keys) = self.links.get(&(source.to_owned(), user)) {
            return Ok(self.unique(keys));
        }
        let Some(email) = email
            .filter(|_| verification == fyrr_core::connector::EmailVerification::VerifiedCorporate)
        else {
            return Ok(None);
        };
        // Personal GitHub e-mails require an explicit operator/SSO link. The
        // directory's corporate key can link by exact unambiguous equality.
        let alias = hash_principal(config, "key", &email.trim().to_lowercase())?;
        Ok(self.aliases.get(&alias).and_then(|keys| self.unique(keys)))
    }
}

/// Expand nested memberships. Dangling/inactive members grant nothing; cycles terminate.
pub(crate) fn expand(
    groups: &BTreeMap<String, Vec<String>>,
    users: &BTreeSet<String>,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut work = 0usize;
    for group in groups.keys() {
        let mut pending = vec![group.clone()];
        let mut seen = BTreeSet::new();
        while let Some(member) = pending.pop() {
            work += 1;
            if work > 10_000_000 {
                return Err(WriterError::MembershipExpansionLimit);
            }
            if !seen.insert(member.clone()) {
                continue;
            }
            if users.contains(&member) {
                out.entry(member).or_default().insert(group.clone());
            } else if let Some(nested) = groups.get(&member) {
                pending.extend(nested.iter().cloned());
            }
        }
    }
    Ok(out)
}

pub(crate) fn bindings(snapshot: &DirectorySnapshot) -> Vec<SubjectBinding> {
    let users: BTreeSet<_> = snapshot
        .users
        .iter()
        .filter(|u| u.active)
        .map(|u| u.key.clone())
        .collect();
    let mut subjects: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for u in &snapshot.users {
        for subject in &u.subjects {
            subjects
                .entry(subject.clone())
                .or_default()
                .insert(u.key.clone());
        }
    }
    subjects
        .into_iter()
        .filter_map(|(subject, keys)| {
            if keys.len() != 1 {
                return None;
            }
            let principal = keys.into_iter().next()?;
            users
                .contains(&principal)
                .then_some(SubjectBinding { subject, principal })
        })
        .collect()
}

/// Operator import is not a freshness proof. Pull/SCIM adapters provide that
/// separately after successful application; this path never advances its clock.
/// With `provider = "none"` the operator's imports are the directory and attest
/// nothing, so an import also withdraws a timestamp left by an earlier provider:
/// readers hold a published timestamp to `validity_deadline` whatever their own
/// provider, and a namespace that can no longer pull would otherwise be refused
/// for good once it passed.
pub async fn publish(config: &Config, snapshot: &DirectorySnapshot) -> Result<serde_json::Value> {
    snapshot
        .validate()
        .map_err(|_| WriterError::DirectorySnapshotInvalid)?;
    if snapshot.namespace != config.namespace.name || snapshot.built_at > fyrr_core::clock::now() {
        return Err(WriterError::DirectorySnapshotInvalid);
    }
    crate::lease::LeaseGuard::with(config, |lease| async move {
        let published = publish_held(config, snapshot, &lease, None).await?;
        if config.identity.directory.provider == fyrr_core::config::DirectoryProvider::None {
            let mut head = lease.head().await?;
            if head.freshness.directory.take().is_some() {
                head.updated_at = fyrr_core::clock::now();
                lease.publish(head).await?;
            }
        }
        Ok(published)
    })
    .await
}

/// Both adapters publish through the same holder; a proof is never accepted from operator imports.
pub(crate) async fn publish_held(
    config: &Config,
    snapshot: &DirectorySnapshot,
    lease: &crate::lease::LeaseGuard,
    proof: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<serde_json::Value> {
    snapshot
        .validate()
        .map_err(|_| WriterError::DirectorySnapshotInvalid)?;
    if snapshot.namespace != config.namespace.name || snapshot.built_at > fyrr_core::clock::now() {
        return Err(WriterError::DirectorySnapshotInvalid);
    }
    if let Some(at) = proof {
        let age = fyrr_core::clock::now()
            .signed_duration_since(at)
            .to_std()
            .map_err(|_| WriterError::DirectoryHealthInvalid)?;
        if age > config.identity.directory.sync_interval.as_duration() {
            return Err(WriterError::DirectoryHealthInvalid);
        }
    }

    let (store, root) = namespace_store(config)?;
    let mut head = lease.head().await?;
    let current: IdentityArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.identity).await?;
    if snapshot.hashing != current.hashing {
        return Err(WriterError::DirectorySnapshotInvalid);
    }
    // Directory timestamps are not identity: compare the canonical resource
    // content at the prior instant before deriving a new immutable input.
    if let Some(mut prior) = crate::directory_pull::current(config, &head).await? {
        let prior_at = prior.built_at;
        prior.built_at = snapshot.built_at;
        if &prior == snapshot {
            if snapshot.built_at < prior_at {
                return Err(WriterError::DirectorySnapshotStale);
            }
            if let Some(at) = proof {
                let next = head.freshness.directory.map_or(at, |old| old.max(at));
                if head.freshness.directory != Some(next) {
                    head.freshness.directory = Some(next);
                    head.updated_at = fyrr_core::clock::now();
                    lease.publish(head.clone()).await?;
                }
            }
            return Ok(
                serde_json::json!({"identity_generation":head.identity.generation,"changed":false}),
            );
        }
    }
    let bytes = write_contract(snapshot);
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(WriterError::DirectorySnapshotInvalid);
    }
    let hash = digest(&bytes);
    if let Some(prior) = current
        .inputs
        .iter()
        .find(|i| i.kind == IdentityInputKind::Directory)
        && (snapshot.built_at < prior.read_at
            || (snapshot.built_at == prior.read_at && hash != prior.digest))
    {
        return Err(WriterError::DirectorySnapshotStale);
    }
    let object = format!("directory/{}.json", &hash[7..]);
    crate::secrets::put(
        config,
        store.as_ref(),
        &Path::from(format!("{root}/{object}")),
        snapshot,
    )
    .await?;
    let mut inputs = current.clone();
    inputs
        .inputs
        .retain(|i| i.kind != IdentityInputKind::Directory);
    inputs.inputs.push(IdentityInput {
        kind: IdentityInputKind::Directory,
        id: snapshot.provider.clone(),
        object,
        digest: hash,
        read_at: snapshot.built_at,
    });
    let generation = head
        .identity
        .generation
        .checked_add(1)
        .ok_or(WriterError::IdentityGenerationOverflow)?;
    let configured =
        crate::commit::configured_identity(config, generation, fyrr_core::clock::now())?;
    let next = crate::membership::build(config, &inputs, &head, &[], configured, None).await?;
    if next.principals == current.principals
        && next.tokens == current.tokens
        && next.subjects == current.subjects
        && next.inputs == current.inputs
    {
        return Ok(
            serde_json::json!({"identity_generation": head.identity.generation, "changed": false}),
        );
    }
    head.identity =
        crate::prepare::put_generation(store.as_ref(), &root, "identity", generation, &next)
            .await?;
    head.updated_at = fyrr_core::clock::now();
    if let Some(at) = proof {
        head.freshness.directory = Some(head.freshness.directory.map_or(at, |old| old.max(at)));
    }
    lease.publish(head).await?;
    Ok(
        serde_json::json!({"identity_generation": generation, "changed": true, "principals": next.principals.len(), "over_limit_subjects": next.principals.iter().filter(|p| p.tokens.len() > config.identity.max_tokens_per_user as usize).count(), "artifact_bytes": write_contract(&next).len()}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use fyrr_core::directory::{DirectoryGroup, DirectoryUser};

    #[test]
    fn nested_cycles_and_inactive_or_dangling_members_do_not_widen_grants() {
        let groups = BTreeMap::from([
            ("child".into(), vec!["active".into(), "parent".into()]),
            (
                "parent".into(),
                vec!["child".into(), "inactive".into(), "missing".into()],
            ),
        ]);
        let result = expand(&groups, &BTreeSet::from(["active".into()])).unwrap();
        assert_eq!(
            result,
            BTreeMap::from([(
                "active".into(),
                BTreeSet::from(["child".into(), "parent".into()])
            )])
        );
    }

    #[test]
    fn subject_collisions_include_inactive_accounts() {
        let mut snapshot = DirectorySnapshot {
            schema_version: DirectorySnapshot::VERSION,
            namespace: "test".into(),
            provider: "scim".into(),
            built_at: fyrr_core::clock::now(),
            hashing: fyrr_core::identity::HashingScheme {
                algorithm: "hmac-sha256".into(),
                key_id: "namespace-v1".into(),
            },
            users: vec![
                DirectoryUser {
                    key: "active".into(),
                    token: "token".into(),
                    active: true,
                    aliases: Vec::new(),
                    subjects: vec!["shared-subject".into(), "unique-subject".into()],
                },
                DirectoryUser {
                    key: "inactive".into(),
                    token: "token".into(),
                    active: false,
                    aliases: Vec::new(),
                    subjects: vec!["shared-subject".into()],
                },
            ],
            groups: Vec::<DirectoryGroup>::new(),
            links: Vec::new(),
            state: None,
        };
        assert_eq!(
            bindings(&snapshot),
            vec![SubjectBinding {
                subject: "unique-subject".into(),
                principal: "active".into()
            }]
        );
        snapshot.users[0].active = false;
        assert!(bindings(&snapshot).is_empty());
    }
}
