//! Complete bounded directory reads. Failed/partial reads never publish or attest.
use crate::{Result, WriterError};
use fyrr_core::{
    Contract,
    config::{Config, DirectoryProvider},
    directory::{DirectoryGroup, DirectorySnapshot, DirectoryUser},
    identity::{HashingScheme, IdentityArtifact, IdentityInputKind},
};
use fyrr_reader::storage::{hash_principal, namespace_store};
use object_store::path::Path;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) async fn current(
    config: &Config,
    head: &fyrr_core::publication::ServingHead,
) -> Result<Option<DirectorySnapshot>> {
    let (store, root) = namespace_store(config)?;
    let identity: IdentityArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.identity).await?;
    let Some(input) = identity
        .inputs
        .iter()
        .find(|i| i.kind == IdentityInputKind::Directory)
    else {
        return Ok(None);
    };
    let snapshot: DirectorySnapshot = crate::secrets::read(
        config,
        store.as_ref(),
        &Path::from(format!("{root}/{}", input.object)),
    )
    .await?;
    snapshot
        .validate()
        .map_err(|_| WriterError::DirectorySnapshotInvalid)?;
    if fyrr_reader::embedding::digest(&fyrr_core::write_contract(&snapshot)) != input.digest
        || snapshot.namespace != config.namespace.name
    {
        return Err(WriterError::DirectorySnapshotInvalid);
    }
    Ok(Some(snapshot))
}
pub(crate) fn base(config: &Config, provider: &str) -> DirectorySnapshot {
    DirectorySnapshot {
        schema_version: DirectorySnapshot::VERSION,
        namespace: config.namespace.name.clone(),
        provider: provider.into(),
        built_at: chrono::Utc::now(),
        hashing: HashingScheme {
            algorithm: "hmac-sha256".into(),
            key_id: "namespace-v1".into(),
        },
        users: Vec::new(),
        groups: Vec::new(),
        links: Vec::new(),
        state: None,
    }
}
pub(crate) fn checked_url(value: &str) -> Result<reqwest::Url> {
    fyrr_core::config::secure_url(value).ok_or(WriterError::DirectoryEndpointInvalid)
}
fn append(base: &reqwest::Url, parts: &[&str]) -> Result<reqwest::Url> {
    let mut url = base.clone();
    url.path_segments_mut()
        .map_err(|()| WriterError::DirectoryEndpointInvalid)?
        .pop_if_empty()
        .extend(parts.iter().copied());
    Ok(url)
}
async fn response_json(mut response: reqwest::Response) -> Result<Value> {
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|v| v > 2 * 1024 * 1024)
    {
        return Err(WriterError::DirectoryPullFailed);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| WriterError::DirectoryPullFailed)?
    {
        if bytes.len() + chunk.len() > 2 * 1024 * 1024 {
            return Err(WriterError::DirectoryPullFailed);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| WriterError::DirectoryPullFailed)
}
struct KeycloakRead {
    client: reqwest::Client,
    base: reqwest::Url,
    token: String,
    requests: usize,
}
impl std::fmt::Debug for KeycloakRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KeycloakRead(<redacted>)")
    }
}
impl KeycloakRead {
    async fn count(&mut self, parts: &[&str]) -> Result<usize> {
        self.requests += 1;
        if self.requests > 10_000 {
            return Err(WriterError::DirectoryPullLimit);
        }
        let value = response_json(
            self.client
                .get(append(&self.base, parts)?)
                .bearer_auth(&self.token)
                .query(&[("top", "false")])
                .send()
                .await
                .map_err(|_| WriterError::DirectoryPullFailed)?,
        )
        .await?;
        value
            .as_u64()
            .or_else(|| value["count"].as_u64())
            .filter(|n| *n <= 100_000)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or(WriterError::DirectoryPullFailed)
    }
    async fn pages(&mut self, parts: &[&str]) -> Result<Vec<Value>> {
        let url = append(&self.base, parts)?;
        let mut result = Vec::new();
        loop {
            self.requests += 1;
            if self.requests > 10_000 || result.len() >= 100_000 {
                return Err(WriterError::DirectoryPullLimit);
            }
            let first = result.len().to_string();
            let response = self
                .client
                .get(url.clone())
                .bearer_auth(&self.token)
                .query(&[
                    ("first", first.as_str()),
                    ("max", "100"),
                    ("briefRepresentation", "false"),
                ])
                .send()
                .await
                .map_err(|_| WriterError::DirectoryPullFailed)?;
            let page = response_json(response)
                .await?
                .as_array()
                .cloned()
                .ok_or(WriterError::DirectoryPullFailed)?;
            if page.len() > 100 {
                return Err(WriterError::DirectoryPullFailed);
            }
            let done = page.len() < 100;
            result.extend(page);
            if done {
                return Ok(result);
            }
        }
    }
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 4096)
        .ok_or(WriterError::DirectoryPullFailed)
}
async fn keycloak(config: &Config) -> Result<DirectorySnapshot> {
    let settings = &config.identity.directory;
    let base_url = checked_url(
        settings
            .endpoint
            .as_deref()
            .ok_or(WriterError::DirectoryEndpointInvalid)?,
    )?;
    let realm = settings
        .realm
        .as_deref()
        .filter(|s| !s.is_empty() && s.len() <= 256)
        .ok_or(WriterError::DirectoryEndpointInvalid)?;
    let issuer = settings
        .subject_issuer()
        .ok_or(WriterError::DirectoryEndpointInvalid)?;
    let _ = checked_url(&issuer)?;
    if settings.verified_domains.is_empty() {
        return Err(WriterError::DirectoryVerifiedDomainsRequired);
    }
    let credential = crate::secrets::resolve(
        config,
        settings
            .credentials
            .as_ref()
            .ok_or(WriterError::CredentialUnavailable)?,
    )
    .await?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|_| WriterError::DirectoryPullFailed)?;
    let token = if let Some(id) = &settings.client_id {
        let response = client
            .post(append(
                &base_url,
                &["realms", realm, "protocol", "openid-connect", "token"],
            )?)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", id.as_str()),
                ("client_secret", credential.as_str()),
            ])
            .send()
            .await
            .map_err(|_| WriterError::DirectoryPullFailed)?;
        text(&response_json(response).await?, "access_token")?.to_owned()
    } else {
        credential
    };
    let mut api = KeycloakRead {
        client,
        base: append(&base_url, &["admin", "realms", realm])?,
        token,
        requests: 0,
    };
    let user_count = api.count(&["users", "count"]).await?;
    let group_count = api.count(&["groups", "count"]).await?;
    let users = api.pages(&["users"]).await?;
    if users.len() != user_count {
        return Err(WriterError::DirectoryPullIncomplete);
    }
    let mut snapshot = base(config, "keycloak");
    let user_keys = keycloak_users(config, &issuer, &users, &mut snapshot)?;
    let mut pending = api.pages(&["groups"]).await?;
    let mut groups = BTreeMap::<String, DirectoryGroup>::new();
    while let Some(raw) = pending.pop() {
        let id = text(&raw, "id")?.to_owned();
        if groups.contains_key(&id) || groups.len() >= 100_000 {
            return Err(WriterError::DirectoryPullFailed);
        }
        let children = api.pages(&["groups", &id, "children"]).await?;
        let token = hash_principal(config, "tok", &format!("grp:keycloak:{id}"))?;
        let mut members = Vec::new();
        for child in &children {
            members.push(hash_principal(
                config,
                "tok",
                &format!("grp:keycloak:{}", text(child, "id")?),
            )?);
        }
        groups.insert(id, DirectoryGroup { token, members });
        pending.extend(children);
    }
    if groups.len() != group_count {
        return Err(WriterError::DirectoryPullIncomplete);
    }
    for (id, key) in user_keys {
        let count = api.count(&["users", &id, "groups", "count"]).await?;
        let memberships = api.pages(&["users", &id, "groups"]).await?;
        let ids: BTreeSet<_> = memberships
            .iter()
            .map(|g| text(g, "id"))
            .collect::<Result<_, _>>()?;
        if ids.len() != count
            || memberships.len() != count
            || api.count(&["users", &id, "groups", "count"]).await? != count
        {
            return Err(WriterError::DirectoryPullIncomplete);
        }
        for group in memberships {
            let group_id = text(&group, "id")?;
            groups
                .get_mut(group_id)
                .ok_or(WriterError::DirectoryPullFailed)?
                .members
                .push(key.clone());
        }
    }
    if api.count(&["users", "count"]).await? != user_count
        || api.count(&["groups", "count"]).await? != group_count
    {
        return Err(WriterError::DirectoryPullIncomplete);
    }
    snapshot.groups = groups.into_values().collect();
    snapshot
        .validate()
        .map_err(|_| WriterError::DirectoryPullFailed)?;
    Ok(snapshot)
}
async fn read_snapshot(config: &Config) -> Result<DirectorySnapshot> {
    use std::io::Read;
    let path = config
        .identity
        .directory
        .snapshot_file
        .as_ref()
        .ok_or(WriterError::DirectorySnapshotUnavailable)?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| WriterError::DirectorySnapshotUnavailable)?
        .take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| WriterError::DirectorySnapshotUnavailable)?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(WriterError::DirectorySnapshotInvalid);
    }
    let mut snapshot: DirectorySnapshot =
        fyrr_core::parse_contract(&bytes).map_err(|_| WriterError::DirectorySnapshotInvalid)?;
    snapshot.built_at = chrono::Utc::now();
    snapshot.state = None;
    Ok(snapshot)
}
pub async fn pull(config: &Config, due: bool, timeout: std::time::Duration) -> Result<Value> {
    let started = fyrr_reader::storage::optional_head(config)
        .await?
        .ok_or(WriterError::NamespaceUnavailable)?;
    if due {
        let head = &started;
        if head.freshness.directory.is_some_and(|at| {
            chrono::Utc::now()
                .signed_duration_since(at)
                .to_std()
                .is_ok_and(|age| age < config.identity.directory.sync_interval.as_duration())
        }) {
            return Ok(serde_json::json!({"directory_due":false}));
        }
    }
    let read = async {
        match config.identity.directory.provider {
            DirectoryProvider::Keycloak => keycloak(config).await,
            DirectoryProvider::Snapshot => read_snapshot(config).await,
            _ => Err(WriterError::DirectoryPullUnsupported),
        }
    };
    let mut snapshot = tokio::time::timeout(timeout, read)
        .await
        .map_err(|_| WriterError::DirectoryPullTimeout)??;
    crate::lease::LeaseGuard::with(config, |lease| async move {
        snapshot.built_at = chrono::Utc::now();

        async {
            // An overlapping publisher invalidates the read, even if this older read
            // completed later. Never replace a newer revocation or freshness proof.
            let live = lease.head().await?;
            if live.identity != started.identity
                || live.freshness.directory != started.freshness.directory
            {
                return Err(WriterError::DirectoryPullStale);
            }
            if config.identity.directory.provider == DirectoryProvider::Keycloak
                && let Some(prior) = current(config, &lease.head().await?).await?
            {
                snapshot.links = prior.links;
            }
            crate::directory::publish_held(config, &snapshot, &lease, Some(snapshot.built_at)).await
        }
        .await
    })
    .await
}

pub(crate) fn corporate(config: &Config, email: &str) -> bool {
    email.rsplit_once('@').is_some_and(|(_, domain)| {
        config
            .identity
            .directory
            .verified_domains
            .iter()
            .any(|v| v.eq_ignore_ascii_case(domain))
    })
}

fn keycloak_users(
    config: &Config,
    issuer: &str,
    users: &[Value],
    snapshot: &mut DirectorySnapshot,
) -> Result<BTreeMap<String, String>> {
    let mut user_keys = BTreeMap::new();
    let mut ids = BTreeSet::new();
    for raw in users {
        let id = text(raw, "id")?;
        if !ids.insert(id) {
            return Err(WriterError::DirectoryPullFailed);
        }
        let email = raw["email"]
            .as_str()
            .filter(|e| e.len() <= 4096 && e.contains('@'))
            .map_or_else(
                || format!("unmapped-keycloak:{id}"),
                |e| e.trim().to_lowercase(),
            );
        let corporate = corporate(config, &email);
        let key = hash_principal(config, "key", &email)?;
        user_keys.insert(id.to_owned(), key.clone());
        snapshot.users.push(DirectoryUser {
            key,
            token: hash_principal(config, "tok", &format!("usr:{email}"))?,
            active: raw["enabled"].as_bool() == Some(true)
                && raw["emailVerified"].as_bool() == Some(true)
                && corporate,
            aliases: Vec::new(),
            subjects: vec![crate::directory::subject_key(config, issuer, id)?],
        });
    }
    Ok(user_keys)
}
