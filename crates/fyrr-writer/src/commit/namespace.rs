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

/// The settings `config` asks for: what a new namespace is created with, and
/// what an existing one must already have for a request to repeat its creation.
pub fn namespace_settings(config: &Config) -> NamespaceSettings {
    let namespace = &config.namespace;
    NamespaceSettings {
        embedding: identity(&config.embedding),
        fts: namespace.fts_settings(),
        dense_rq_bits: namespace.dense_rq_bits,
        dense_partition_rows: namespace.dense_partition_rows,
        retrieval: namespace.retrieval.settings(),
        composition: namespace.composition.settings(),
        rerank: namespace
            .rerank
            .as_ref()
            .map(fyrr_core::config::RerankConfig::settings),
        attributes: namespace.attributes.clone(),
        attribute_indexes: namespace.attribute_indexes.clone(),
        write_api: namespace.write_api.clone(),
        hash_name: namespace.hash_name.clone(),
    }
}

/// A namespace that exists: creating it again changes nothing with the same
/// settings and is refused with other ones, which are fixed for its life.
async fn repeat_create(
    store: &dyn ObjectStore,
    root: &Path,
    head: &ServingHead,
    settings: &NamespaceSettings,
) -> Result<()> {
    let policy: PolicyArtifact = fyrr_reader::storage::artifact(store, root, &head.policy).await?;
    let mut asked = settings.clone();
    // A copied namespace records the name its hashes were made under, which no
    // file asks for.
    asked.hash_name.clone_from(&policy.settings.hash_name);
    // A namespace created before the connector columns has none of them, and
    // a file that asks for them again still repeats its creation.
    if policy.settings.write_api.is_none()
        && !fyrr_core::attributes::has_connector_columns(&policy.settings.attributes)
    {
        for (name, _) in fyrr_core::attributes::CONNECTOR_COLUMNS {
            if !policy.settings.attributes.contains_key(name) {
                asked.attributes.remove(name);
            }
        }
    }
    if policy.settings == asked {
        Ok(())
    } else {
        Err(WriterError::NamespaceSettingsFixed)
    }
}

pub async fn create(config: &Config) -> Result<()> {
    create_with(config, namespace_settings(config)).await
}

/// `create` with the settings the caller chose, which a configuration file
/// only supplies the defaults of.
pub async fn create_with(config: &Config, settings: NamespaceSettings) -> Result<()> {
    let (store, root) = namespace_store(config)?;
    if let Some(head) = fyrr_reader::storage::optional_head(config).await? {
        return repeat_create(store.as_ref(), &root, &head, &settings).await;
    }
    match create_new(config, store.as_ref(), &root, settings.clone()).await {
        // A concurrent creation won: judge its settings as a repeated one.
        Err(WriterError::ImmutableObjectConflict) => {
            let head = fyrr_reader::storage::optional_head(config)
                .await?
                .ok_or(WriterError::ImmutableObjectConflict)?;
            repeat_create(store.as_ref(), &root, &head, &settings).await
        }
        result => result,
    }
}

async fn create_new(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    settings: NamespaceSettings,
) -> Result<()> {
    // The dimension is chosen here and binds the namespace for good, and the
    // dense index cannot be built over any other. Stored settings that predate
    // this check are read as they are.
    if !u64::from(settings.embedding.dimensions)
        .is_multiple_of(fyrr_core::config::DIMENSIONS_MULTIPLE)
    {
        return Err(WriterError::PublicationInvalid);
    }
    let identity = configured_identity(config, 1)?;
    let mut policy = PolicyArtifact {
        fixed_grants: fixed_grants(config)?,
        row_acl_sequence: None,
        retired_sources: Default::default(),
        read_only: false,
        schema_version: <PolicyArtifact as fyrr_core::Contract>::VERSION,
        namespace: config.namespace.name.clone(),
        generation: 1,
        settings,
    };
    policy.schema_version = policy.current_version();
    // A deletion that removed the head may still be removing objects, and a new
    // head would be removed with them: run the deletion again first. Any other
    // object under the prefix, the fence included, refuses the creation. The
    // objects of this creation are tolerated, so that it can be repeated after
    // an interruption or run concurrently.
    let own: Vec<_> = [
        crate::prepare::generation_object("identity", 1, &identity).0,
        crate::prepare::generation_object("policy", 1, &policy).0,
        "head.json".into(),
    ]
    .into_iter()
    .map(|object| Path::from(format!("{root}/{object}")))
    .collect();
    let mut present = store.list(Some(root));
    while let Some(object) = present.try_next().await? {
        if !own.contains(&object.location) {
            return Err(WriterError::NamespaceDeleting);
        }
    }
    drop(present);
    let head = new_head(config, store, root, &identity, &policy).await?;
    put_immutable(store, &root.clone().join("head.json"), &head).await
}

/// Writes generation 1 of the identity and policy of a new namespace, and
/// returns its head with no content: the head becomes visible, and the
/// namespace exists, only when the caller writes `head.json`.
pub(super) async fn new_head(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    identity: &IdentityArtifact,
    policy: &PolicyArtifact,
) -> Result<ServingHead> {
    let mut incarnation = [0; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut incarnation)
        .map_err(|_| WriterError::RuntimeUnavailable)?;
    Ok(ServingHead {
        schema_version: PUBLICATION_CONTRACT_VERSION,
        namespace: config.namespace.name.clone(),
        content: None,
        identity: crate::prepare::put_generation(store, root, "identity", 1, identity).await?,
        policy: crate::prepare::put_generation(store, root, "policy", 1, policy).await?,
        lease: None,
        rejected_envelope_ids: Vec::new(),
        updated_at: fyrr_core::clock::now(),
        incarnation: hex::encode(incarnation),
    })
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
) -> Result<
    Option<(
        DeleteFence,
        object_store::UpdateVersion,
        chrono::DateTime<chrono::Utc>,
    )>,
> {
    let object = match store.get(&root.clone().join(DELETE_FENCE)).await {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(_) => return Err(WriterError::StorageUnavailable),
    };
    if object.meta.size > 4096 {
        return Err(WriterError::LeaseInvalid);
    }
    let version = crate::lease::version_of(&object.meta);
    let written = object.meta.last_modified;
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
    Ok(Some((fence, version, written)))
}

/// Publishes `policy` as the next generation of the head the lease holds.
pub(crate) async fn publish_policy(
    config: &Config,
    lease: &LeaseGuard,
    mut head: ServingHead,
    mut policy: PolicyArtifact,
) -> Result<()> {
    let (store, root) = namespace_store(config)?;
    policy.schema_version = policy.current_version();
    policy.generation = head
        .policy
        .generation
        .checked_add(1)
        .ok_or(WriterError::PolicyGenerationOverflow)?;
    head.policy =
        crate::prepare::put_generation(store.as_ref(), &root, "policy", policy.generation, &policy)
            .await?;
    lease.publish(head).await
}

/// Freezes the namespace or thaws it, under the namespace lease, as a new
/// policy generation. Whether it changed anything: a repeated call is a no-op.
pub async fn set_read_only(config: &Config, read_only: bool) -> Result<bool> {
    fyrr_reader::storage::optional_head(config)
        .await?
        .ok_or(WriterError::NamespaceUnavailable)?;
    LeaseGuard::with(config, |lease| async move {
        set_read_only_held(config, &lease, read_only).await
    })
    .await
}

/// `set_read_only` by a caller that holds the namespace lease already.
pub async fn set_read_only_held(
    config: &Config,
    lease: &LeaseGuard,
    read_only: bool,
) -> Result<bool> {
    let (store, root) = namespace_store(config)?;
    let head = lease.head().await?;
    let mut policy: PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    if policy.read_only == read_only {
        return Ok(false);
    }
    policy.read_only = read_only;
    publish_policy(config, lease, head, policy).await?;
    Ok(true)
}

/// Refuses a write into a read-only namespace. It reads the published head
/// each time: a writer calls it as its envelope is about to become durable,
/// under its cursor lease, so a switch published since the request was
/// accepted is honoured. An envelope that was durable before the switch is
/// still committed.
pub(crate) async fn ensure_writable(config: &Config) -> Result<()> {
    let (store, root) = namespace_store(config)?;
    let head = fyrr_reader::storage::optional_head(config)
        .await?
        .ok_or(WriterError::NamespaceUnavailable)?;
    let policy: PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    if policy.read_only {
        return Err(WriterError::NamespaceReadOnly);
    }
    Ok(())
}

/// Cursor acquisition checks on both sides of its CAS, so a new source cannot
/// enter between the deletion fence and the active-worker inventory.
pub(crate) async fn sources_allowed(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
) -> Result<()> {
    let fence = deletion_fence(config, store, root).await?;
    // A source writes only into a namespace that exists: not after its head
    // was removed, whether the deletion is still running or has finished.
    let head = fyrr_reader::storage::optional_head(config)
        .await?
        .ok_or(WriterError::NamespaceUnavailable)?;
    let Some((fence, _, _)) = fence else {
        return Ok(());
    };
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
        Some((_, version, _)) => object_store::PutMode::Update(version),
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

/// Deletes the namespace: unpublishes it, then removes every object under its
/// root and the fence last. `export` is verified against storage before
/// anything is removed; without it, `confirm` must repeat the namespace name.
/// A deletion that stopped after the head went, and one that an earlier
/// release left behind, ask for the same confirmation. Every step is
/// idempotent: an interrupted deletion is run again and finishes.
pub async fn delete(
    config: &Config,
    export: Option<&std::path::Path>,
    confirm: Option<&str>,
) -> Result<serde_json::Value> {
    if confirm.is_some_and(|name| name != config.namespace.name) {
        return Err(WriterError::DeleteConfirmationInvalid);
    }
    let (store, root) = namespace_store(config)?;
    let published = fyrr_reader::storage::optional_head(config).await?.is_some();
    let fenced = deletion_fence(config, store.as_ref(), &root)
        .await?
        .is_some();
    // Without a head, a fence marks a deletion that is still under way, and
    // objects without one are the debris of an interrupted creation.
    if !published && !fenced && store.list(Some(&root)).try_next().await?.is_none() {
        return Ok(serde_json::json!({"deleted":true}));
    }
    if export.is_none() && confirm.is_none() {
        let objects: Vec<_> = store.list(Some(&root)).try_collect().await?;
        return Ok(
            serde_json::json!({"dry_run":true,"objects":objects.len(),"bytes":objects.iter().map(|object|object.size).sum::<u64>()}),
        );
    }
    if !published && !fenced {
        fence_debris(config, store.as_ref(), &root).await?;
    }
    let verified = if published {
        unpublish(config, store.clone(), &root, export).await?
    } else if let Some(export) = export {
        Some(verify_export(store.as_ref(), &root, None, export).await?)
    } else {
        None
    };
    purge(config, store.as_ref(), &root, verified.as_ref()).await?;
    Ok(serde_json::json!({"deleted":true}))
}

/// Debris has no head to hold a lease with: its fence names no incarnation, so
/// that the deletion can be run again like any other.
async fn fence_debris(config: &Config, store: &dyn ObjectStore, root: &Path) -> Result<()> {
    // A fence that cannot be deleted conditionally would never go.
    match fyrr_reader::storage::conditional_delete_supported(config).await {
        Ok(()) => {}
        Err(object_store::Error::NotSupported { .. }) => {
            return Err(WriterError::ConditionalDeleteUnsupported);
        }
        Err(_) => return Err(WriterError::StorageUnavailable),
    }
    let mut holder = [0; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut holder)
        .map_err(|_| WriterError::RuntimeUnavailable)?;
    let fence = DeleteFence {
        version: 1,
        namespace: config.namespace.name.clone(),
        incarnation: String::new(),
        holder: format!("holder-{}", hex::encode(holder)),
        epoch: 0,
    };
    let bytes = serde_json::to_vec(&fence).map_err(|_| WriterError::LeaseInvalid)?;
    store
        .put_opts(
            &root.clone().join(DELETE_FENCE),
            bytes.into(),
            object_store::PutMode::Create.into(),
        )
        .await?;
    Ok(())
}

/// Fences the sources, checks the export when there is one and removes the
/// head. Returns the keys the export verified.
async fn unpublish(
    config: &Config,
    store: Arc<dyn ObjectStore>,
    root: &Path,
    export: Option<&std::path::Path>,
) -> Result<Option<BTreeSet<Path>>> {
    LeaseGuard::with(config, |lease| async move {
        fence_sources(config, store.as_ref(), root, &lease).await?;
        let verified = match export {
            Some(export) => Some(verify_export(store.as_ref(), root, Some(&lease), export).await?),
            None => None,
        };
        lease.delete_head(config).await?;
        Ok(verified)
    })
    .await
}

/// Checks the export against storage and returns the keys it verified.
async fn verify_export(
    store: &dyn ObjectStore,
    root: &Path,
    lease: Option<&LeaseGuard>,
    export: &std::path::Path,
) -> Result<BTreeSet<Path>> {
    let objects: Vec<_> = store.list(Some(root)).try_collect().await?;
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
        if let Some(lease) = lease {
            lease.head().await?;
        }
        let relative = fyrr_reader::storage::relative_path(&object.location, root);
        let bytes = store.get(&object.location).await?.bytes().await?;
        let saved = std::fs::read(export.join(relative)).map_err(|_| WriterError::ExportInvalid)?;
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
    let current: Vec<_> = store.list(Some(root)).try_collect().await?;
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
    Ok(verified.into_keys().collect())
}

/// Whether `held` is still the fence in storage: another run that finished
/// leaves none, or a new one, and the name may belong to a new namespace.
async fn fence_held(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    held: &object_store::UpdateVersion,
) -> Result<bool> {
    Ok(matches!(
        deletion_fence(config, store, root).await?,
        Some((_, now, _)) if now.e_tag == held.e_tag && now.version == held.version
    ))
}

/// Removes every object under the root, the fence last, so that a deletion
/// stopped part way still has its fence and is run again. It first waits out
/// the leases that may still be writing: a worker whose lease expired just
/// before the fence can finish a request admitted earlier. Writers of the
/// namespace must be stopped first: after the last pass it waits once more and
/// lists again, and what has appeared is removed, or with an export (`verified`)
/// is an error. With an export only the verified keys are removed; any other
/// key stops the deletion with nothing more removed. It also stops at once,
/// before every batch, when the fence is no longer the one it read.
async fn purge(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    verified: Option<&BTreeSet<Path>>,
) -> Result<()> {
    let Some((_, held, written)) = deletion_fence(config, store, root).await? else {
        return Ok(());
    };
    let ttl = config.writer.commit.lease_ttl.as_duration();
    let first = chrono::Duration::from_std(ttl).map_err(|_| WriterError::LeaseInvalid)?;
    if let Ok(wait) = (written + first - fyrr_core::clock::now()).to_std() {
        tokio::time::sleep(wait).await;
    }
    let fence = root.clone().join(DELETE_FENCE);
    let mut settled = false;
    loop {
        if !fence_held(config, store, root, &held).await? {
            return Ok(());
        }
        let objects: Vec<_> = store.list(Some(root)).try_collect().await?;
        let rest: Vec<_> = objects
            .into_iter()
            .map(|object| object.location)
            .filter(|location| *location != fence)
            .collect();
        if verified.is_some_and(|keys| rest.iter().any(|key| settled || !keys.contains(key))) {
            return Err(WriterError::ExportInvalid);
        }
        if rest.is_empty() {
            if settled {
                break;
            }
            settled = true;
            tokio::time::sleep(ttl).await;
            continue;
        }
        for batch in rest.chunks(crate::io::CONCURRENCY) {
            if !fence_held(config, store, root, &held).await? {
                return Ok(());
            }
            crate::io::bounded(batch, |path| async move { remove(store, path).await }).await?;
        }
    }
    match fyrr_reader::storage::delete_if_match(config, DELETE_FENCE, &held).await {
        Ok(())
        | Err(object_store::Error::NotFound { .. } | object_store::Error::Precondition { .. }) => {
            Ok(())
        }
        Err(object_store::Error::NotSupported { .. }) => {
            Err(WriterError::ConditionalDeleteUnsupported)
        }
        Err(_) => Err(WriterError::StorageUnavailable),
    }
}

pub(super) async fn remove(store: &dyn ObjectStore, path: &Path) -> Result<()> {
    match store.delete(path).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(_) => Err(WriterError::StorageUnavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(namespace: &str, embedding: &str) -> Config {
        fyrr_core::config::parse(&format!(
            "[namespace]\nname='round-trip'\n{namespace}\n[storage]\nuri='s3://round-trip'\n\
             [embedding]\nendpoint='http://127.0.0.1:9/v1'\n{embedding}\n"
        ))
        .unwrap()
        .config
    }

    /// The settings a configuration asks for, laid over another configuration
    /// and read back, are unchanged; what the other one owns stays its own.
    #[test]
    fn settings_round_trip_through_a_configuration() {
        let created = parse(
            "fts_stem = true\ndense_rq_bits = 3\ndense_partition_rows = 64\n\
             retrieval = { dense_weight = 0.3, lexical_candidates = 20, rescore_depth = { mode = \"fixed\", candidates = 100 } }\n\
             composition = { fusion = 40, dense = 5, lexical = 5 }\n\
             [namespace.rerank]\nendpoint = 'http://127.0.0.1:9/created'\nmodel = 'm'\n\
             candidates = 9\nweight = 0.7\ntext_chars = 300\ndeadline = '1s'\n\
             spend_limit_microusd = 100\nmax_call_cost_microusd = 10",
            "model = 'other-model'\ndimensions = 256\nprefix = 'p: '\nsend_dimensions = true\n\
             [embedding.roles.document]\ninput_type = 'doc'",
        );
        let settings = namespace_settings(&created);
        let process = parse(
            "[namespace.rerank]\nendpoint = 'http://127.0.0.1:9/process'\nmodel = 'x'\n\
             candidates = 1\nweight = 0.1\ndeadline = '2s'\n\
             spend_limit_microusd = 7\nmax_call_cost_microusd = 3",
            "",
        );
        let effective = process.with_settings(&settings).unwrap();
        assert_eq!(namespace_settings(&effective), settings);
        assert_eq!(
            effective.namespace.retrieval_digest(),
            created.namespace.retrieval_digest()
        );
        let rerank = effective.namespace.rerank.as_ref().unwrap();
        assert_eq!(rerank.endpoint, "http://127.0.0.1:9/process");
        assert_eq!(rerank.spend_limit_microusd, 7);
        // Without a reranker of its own, a process cannot reach the namespace's.
        assert!(parse("", "").with_settings(&settings).is_none());
    }
}
