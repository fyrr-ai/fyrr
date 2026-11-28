//! Single-writer publication. The serving-head CAS is the sole visibility point.

use crate::lease::LeaseGuard;
use crate::prepare::{chunk_identity, put_immutable, store_params};
use crate::{Result, WriterError};
use futures::TryStreamExt;
use fyrr_core::config::{Config, ProcessEnvironment};
use fyrr_core::connector::SyncKind;
use fyrr_core::envelope::*;
use fyrr_core::identity::*;
use fyrr_core::publication::*;
use fyrr_core::write_contract;
use fyrr_reader::embedding::{digest, identity};
use fyrr_reader::storage::{dataset_uri, hash_principal, namespace_store};
use lance::Dataset;
use lance::dataset::transaction::Operation;
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::optimize::OptimizeOptions;
use lance_index::vector::{bq::RQBuildParams, ivf::builder::IvfBuildParams};
use lance_index::{IndexType, scalar::ScalarIndexParams};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

mod copy;
mod group;
mod namespace;
mod publication;

pub use copy::copy;
pub(crate) use group::{
    VECTOR_SAMPLE_RATE, add_attribute_columns, build_attribute_indexes, change_schema,
    check_schema_change, commit_one,
};
pub(crate) use namespace::configured_identity;
pub(crate) use namespace::create_with;
pub use namespace::namespace_settings;
pub(crate) use namespace::sources_allowed;
pub use namespace::{create, delete, set_read_only, set_read_only_held};
pub(crate) use namespace::{ensure_writable, publish_policy};
pub(crate) use publication::{previous_manifest, publish};

pub async fn pending(config: &Config, head: &ServingHead) -> Result<Vec<PreparedEnvelope>> {
    pending_of(config, head, None).await
}

/// The envelopes the authorization drain of one `writer maintain` need not
/// read again, by path, with the metadata the listing showed: resolved ones
/// and those of a kind it does not take.
pub(crate) type Drained = BTreeMap<Path, object_store::ObjectMeta>;

/// [`pending`]; with `drained`, an object the listing shows as `drained`
/// holds it is not read, and what it reads that is resolved or not of an
/// authorization kind goes into `drained`.
async fn pending_of(
    config: &Config,
    head: &ServingHead,
    mut drained: Option<&mut Drained>,
) -> Result<Vec<PreparedEnvelope>> {
    let (store, root) = namespace_store(config)?;
    let resolved = resolved(config, head).await?;
    let listed: Vec<_> = store
        .list(Some(&root.join("envelopes")))
        .try_collect()
        .await?;
    let listed: BTreeMap<_, _> = listed
        .into_iter()
        .filter(|object| {
            !object
                .location
                .filename()
                .and_then(|n| n.strip_suffix(".json"))
                .is_some_and(|id| resolved.contains(id))
                && drained
                    .as_ref()
                    .is_none_or(|drained| drained.get(&object.location) != Some(object))
        })
        .map(|object| (object.location.clone(), object))
        .collect();
    let paths: Vec<_> = listed.keys().cloned().collect();
    let (read, unreadable) = read_envelopes(config, &paths).await?;
    let mut result = Vec::new();
    for (path, envelope) in read {
        if envelope.namespace != config.namespace.name {
            return Err(WriterError::EnvelopeNamespaceInvalid);
        }
        // One stored under another name is resolved by the ID it holds.
        let taken = !resolved.contains(&envelope.envelope_id);
        match drained.as_deref_mut() {
            Some(drained) if !taken || !envelope.kind.is_authorization() => {
                drained.insert(path.clone(), listed[&path].clone());
            }
            _ if taken => result.push(envelope),
            _ => {}
        }
    }
    let mut result = confirmed(
        config,
        head,
        result,
        &unreadable,
        &|id: &str| -> Result<bool> { Ok(resolved.contains(id)) },
    )
    .await?;
    result.sort_by(|a, b| (&a.source_id, a.sequence).cmp(&(&b.source_id, b.sequence)));
    crate::telemetry::pending_envelopes(&config.namespace.name, result.len());
    Ok(result)
}

/// Where an envelope is in its lifecycle. The committer, cleanup, `writer
/// status` and the worker match on [`classify`], and on nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    /// A publication applied it.
    Applied,
    /// A publication rejected it.
    Rejected,
    /// The committer publishes it, or rejects it, at its next commit.
    Pending,
    /// It waits until its chain is whole, and content or a listing also until
    /// the permissions chain of its source is: its source must not publish
    /// with rights that chain may have revoked, nor skip a link of its own.
    Blocked,
    /// A chained envelope that is not on the chain its cursor confirmed, of a
    /// lease generation not older than the cursor's: a cursor may still
    /// confirm it. A caller that walked no chain sees every one as this.
    Unconfirmed,
    /// A chained envelope that is not on its chain, of an older lease
    /// generation than the cursor's: nothing confirms it, ever.
    Dead,
}

/// What [`classify`] reads beside the envelope. A caller whose envelopes are
/// all unresolved passes an empty [`Resolved`].
pub(crate) struct Facts<'a> {
    pub(crate) resolved: &'a Resolved,
    pub(crate) retired: &'a BTreeMap<String, RetiredSource>,
    /// The chains [`chains`] walked, by source and kind.
    pub(crate) chains: &'a BTreeMap<(String, SyncKind), Chain>,
}

/// What the walk of the chain of a source and kind found.
pub(crate) enum Chain {
    /// It reaches what the manifest applied: every link after that, and the
    /// lease generation of the cursor.
    Whole {
        ids: BTreeSet<String>,
        generation: u64,
    },
    /// It does not reach it.
    Broken,
}

/// The state of `envelope`. A chained envelope is pending only on the chain
/// its cursor confirmed, and one of a retired source is pending until the
/// committer rejects it; members wait on nothing; content and a listing,
/// which derives deletes, wait on a broken permissions chain of their source.
pub(crate) fn classify(envelope: &impl Link, facts: &Facts) -> State {
    let (id, source, kind) = (envelope.id(), envelope.source_id(), envelope.kind());
    if facts.resolved.rejected(id) {
        return State::Rejected;
    }
    if facts.resolved.applied(id) {
        return State::Applied;
    }
    let chain = |kind| facts.chains.get(&(source.to_owned(), kind));
    let own = match chain(kind) {
        _ if !chained(kind) => State::Pending,
        _ if facts.retired.contains_key(source) => State::Pending,
        Some(Chain::Whole { ids, .. }) if ids.contains(id) => State::Pending,
        Some(Chain::Whole { generation, .. }) if envelope.lease_generation() < *generation => {
            State::Dead
        }
        Some(Chain::Broken) => State::Blocked,
        _ => State::Unconfirmed,
    };
    match (kind, chain(SyncKind::Permissions)) {
        (SyncKind::Content | SyncKind::Reconcile, Some(Chain::Broken)) if own == State::Pending => {
            State::Blocked
        }
        _ => own,
    }
}

/// Whether the cursor of `kind` confirms its envelopes, as links of a chain:
/// content and permissions.
pub(crate) fn chained(kind: SyncKind) -> bool {
    matches!(kind, SyncKind::Content | SyncKind::Permissions)
}

/// The chains the committer walks for an envelope of `kind`: its own, when
/// the kind is chained, and the permissions chain of its source for content
/// and a listing, so that they never publish while its tail is missing. The
/// configuration does not decide it: a committer that does not list the
/// source walks the chain the publication applied all the same.
pub(crate) fn chains_of(kind: SyncKind, source: &str) -> Vec<(String, SyncKind)> {
    let mut found = Vec::new();
    if chained(kind) {
        found.push((source.to_owned(), kind));
    }
    if kind != SyncKind::Members && kind != SyncKind::Permissions {
        found.push((source.to_owned(), SyncKind::Permissions));
    }
    found
}

/// Every envelope ID that the content and permissions chains of `source` name,
/// walked from its cursors whether it is retired or not: what a purge of it
/// deletes of the envelopes that do not parse.
pub(crate) async fn named_by(
    config: &Config,
    head: &ServingHead,
    source: &str,
) -> Result<BTreeSet<String>> {
    let (store, root) = namespace_store(config)?;
    let manifest = previous_manifest(store.as_ref(), &root, head).await?;
    let source = source.to_owned();
    let mut named = BTreeSet::new();
    for kind in [SyncKind::Content, SyncKind::Permissions] {
        // A cursor that does not parse names nothing: the purge deletes it.
        let last = match crate::sync_lease::CursorLease::peek(config, &source, kind).await {
            Ok(cursor) => cursor.and_then(|state| state.last).map(|last| last.id),
            Err(WriterError::CursorInvalid) => None,
            Err(error) => return Err(error),
        };
        let applied = manifest
            .as_ref()
            .and_then(|m| m.chain_applied.get(&source)?.get(&kind).cloned());
        let (_, visited) = walk::<PreparedEnvelope>(
            config,
            store.as_ref(),
            &root,
            (&source, kind),
            last,
            applied.as_ref(),
            &BTreeMap::new(),
            &|_: &str| -> Result<bool> { Ok(true) },
        )
        .await?;
        named.extend(visited);
    }
    Ok(named)
}

/// Fails unless a walked chain names every envelope of `unreadable`.
pub(crate) fn attributed(unreadable: &BTreeSet<String>, named: &BTreeSet<String>) -> Result<()> {
    if unreadable.is_subset(named) {
        Ok(())
    } else {
        Err(WriterError::PublicationInvalid)
    }
}

/// Every chain of the namespace: the content and permissions of each configured
/// source, and the content of the write API.
pub(crate) fn every_chain(config: &Config, policy: &PolicyArtifact) -> Vec<(String, SyncKind)> {
    let mut keys: Vec<_> = config
        .sources
        .iter()
        .flat_map(|source| chains_of(SyncKind::Content, &source.id))
        .collect();
    if policy.settings.namespace.write_api.is_some() {
        keys.push((
            fyrr_core::connector::API_SOURCE_ID.to_owned(),
            SyncKind::Content,
        ));
    }
    keys
}

/// The envelopes of `pending` that [`classify`] finds pending, with the links
/// of a chain that the walk read by ID and no listing showed. The chain is read
/// by ID, never from a listing. `resolved` tells an envelope that was applied
/// or rejected; every one of `pending` is unresolved. `unreadable` holds the
/// IDs of listed envelopes that do not parse: every chain is walked then, so
/// that the source whose chain names one is blocked, and only that one. One
/// that no chain names, a members or listing envelope among them, could hide a
/// revoke: the commit fails as `publication_invalid`.
pub(crate) async fn confirmed(
    config: &Config,
    head: &ServingHead,
    pending: Vec<PreparedEnvelope>,
    unreadable: &BTreeSet<String>,
    resolved: &(dyn Fn(&str) -> Result<bool> + Sync),
) -> Result<Vec<PreparedEnvelope>> {
    let mut keys: BTreeSet<_> = pending
        .iter()
        .flat_map(|e| chains_of(e.kind, &e.source_id))
        .collect();
    if keys.is_empty() && unreadable.is_empty() {
        return Ok(pending);
    }
    let (store, root) = namespace_store(config)?;
    let policy: PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    if !unreadable.is_empty() {
        keys.extend(every_chain(config, &policy));
    }
    let (listed, mut kept): (Vec<_>, Vec<_>) = pending.into_iter().partition(|e| chained(e.kind));
    let listed: BTreeMap<_, _> = listed
        .into_iter()
        .map(|e| (e.envelope_id.clone(), e))
        .collect();
    let (chains, links, named) = chains(config, head, &policy, &keys, &listed, resolved).await?;
    attributed(unreadable, &named)?;
    kept.extend(
        links
            .into_iter()
            .filter(|link| !listed.contains_key(&link.envelope_id)),
    );
    kept.extend(listed.into_values());
    let facts = Facts {
        resolved: &Resolved::default(),
        retired: &policy.retired_sources,
        chains: &chains,
    };
    kept.retain(|e| classify(e, &facts) == State::Pending);
    Ok(kept)
}

/// Walks each chain of `keys` whose source is not retired, from the newest
/// envelope its cursor confirmed back to the newest one the manifest records as
/// applied: what each chain is, its links that `resolved` does not tell, oldest
/// first, and every ID a walk named, the one it stopped at included. `listed`
/// holds chained envelopes the caller has read.
pub(crate) async fn chains<L: Link>(
    config: &Config,
    head: &ServingHead,
    policy: &PolicyArtifact,
    keys: &BTreeSet<(String, SyncKind)>,
    listed: &BTreeMap<String, L>,
    resolved: &(dyn Fn(&str) -> Result<bool> + Sync),
) -> Result<(
    BTreeMap<(String, SyncKind), Chain>,
    Vec<L>,
    BTreeSet<String>,
)> {
    let (store, root) = namespace_store(config)?;
    let manifest = previous_manifest(store.as_ref(), &root, head).await?;
    let mut found = BTreeMap::new();
    let mut unresolved = Vec::new();
    let mut named = BTreeSet::new();
    for (source, kind) in keys
        .iter()
        .filter(|(source, _)| !policy.retired_sources.contains_key(source))
    {
        let (source, kind) = (source, *kind);
        let applied = manifest
            .as_ref()
            .and_then(|m| m.chain_applied.get(source))
            .and_then(|kinds| kinds.get(&kind));
        let cursor = match crate::sync_lease::CursorLease::peek(config, source, kind).await {
            Ok(cursor) => cursor,
            // A cursor that does not parse confirms nothing: the source is
            // blocked until it is purged, and only that source.
            Err(WriterError::CursorInvalid) => {
                crate::telemetry::chain_blocked(source, kind);
                found.insert((source.clone(), kind), Chain::Broken);
                continue;
            }
            Err(error) => return Err(error),
        };
        // A missing cursor of a chain whose envelopes a worker wrote lost what
        // confirmed them: the source is blocked until it is purged.
        let mut lost = false;
        if cursor.is_none() {
            for link in listed.values().filter(|link| {
                link.source_id() == source.as_str()
                    && link.kind() == kind
                    && link.lease_generation() > 0
            }) {
                if !resolved(link.id())? {
                    lost = true;
                    break;
                }
            }
        }
        let generation = cursor
            .as_ref()
            .and_then(|state| state.lease.as_ref())
            .map_or(0, |lease| lease.epoch);
        let last = cursor.and_then(|state| state.last).map(|last| last.id);
        let walked = if lost {
            None
        } else {
            let (walked, visited) = walk(
                config,
                store.as_ref(),
                &root,
                (source, kind),
                last,
                applied,
                listed,
                resolved,
            )
            .await?;
            named.extend(visited);
            walked
        };
        let chain = match walked {
            Some(walk) => {
                unresolved.extend(walk.unresolved);
                Chain::Whole {
                    ids: walk.ids,
                    generation,
                }
            }
            None => {
                crate::telemetry::chain_blocked(source, kind);
                Chain::Broken
            }
        };
        found.insert((source.clone(), kind), chain);
    }
    Ok((found, unresolved, named))
}

/// The links of a chain after `applied`, as one walk finds them.
struct Walk<L> {
    /// Every link, resolved or not.
    ids: BTreeSet<String>,
    /// The links not yet applied or rejected, oldest first.
    unresolved: Vec<L>,
}

/// What a walk and [`classify`] read of an envelope: the whole envelope for the
/// committer, less for a count.
pub(crate) trait Link: Clone + From<PreparedEnvelope> {
    fn id(&self) -> &str;
    fn source_id(&self) -> &str;
    fn kind(&self) -> SyncKind;
    fn previous(&self) -> Option<&String>;
    fn lease_generation(&self) -> u64;
}

impl Link for PreparedEnvelope {
    fn id(&self) -> &str {
        &self.envelope_id
    }
    fn source_id(&self) -> &str {
        &self.source_id
    }
    fn kind(&self) -> SyncKind {
        self.kind
    }
    fn previous(&self) -> Option<&String> {
        self.previous.as_ref()
    }
    fn lease_generation(&self) -> u64 {
        self.lease_generation
    }
}

/// Walks the chain of `source` and `kind` by ID from `last`, the newest
/// envelope its cursor confirmed, back to `applied`: `None` when it does not
/// reach it, or when `last` is missing while an envelope was applied. The
/// source is then blocked, and nothing is skipped. A link that is not there is
/// a broken chain; a storage error is an error, to be retried.
#[allow(clippy::too_many_arguments)]
async fn walk<L: Link>(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    (source, kind): (&String, SyncKind),
    last: Option<String>,
    applied: Option<&String>,
    listed: &BTreeMap<String, L>,
    resolved: &(dyn Fn(&str) -> Result<bool> + Sync),
) -> Result<(Option<Walk<L>>, BTreeSet<String>)> {
    let mut found = Walk {
        ids: BTreeSet::new(),
        unresolved: Vec::new(),
    };
    let Some(last) = last else {
        // No cursor, or one with no confirmed envelope, is whole only when the
        // manifest has applied none: otherwise the chain it held is lost.
        return Ok((applied.is_none().then_some(found), BTreeSet::new()));
    };
    let mut next = Some(last);
    while let Some(id) = next.take() {
        if applied == Some(&id) {
            found.unresolved.reverse();
            let visited = found.ids.clone();
            return Ok((Some(found), visited));
        }
        if !found.ids.insert(id.clone()) {
            return Ok((None, found.ids));
        }
        // The listed links were read under their ID, of this namespace's kind.
        let link = match listed.get(&id) {
            Some(link) => link.clone(),
            None => {
                let path = root.clone().join("envelopes").join(format!("{id}.json"));
                let object = match store.get(&path).await {
                    Ok(object) => object,
                    Err(object_store::Error::NotFound { .. }) => return Ok((None, found.ids)),
                    Err(_) => return Err(WriterError::StorageUnavailable),
                };
                let bytes = object
                    .bytes()
                    .await
                    .map_err(|_| WriterError::StorageUnavailable)?;
                match fyrr_core::parse_contract::<PreparedEnvelope>(&bytes) {
                    Ok(envelope)
                        if envelope.envelope_id == id
                            && envelope.namespace == config.namespace.name
                            && envelope.kind == kind =>
                    {
                        L::from(envelope)
                    }
                    _ => return Ok((None, found.ids)),
                }
            }
        };
        if link.source_id() != source || link.kind() != kind || link.lease_generation() == 0 {
            return Ok((None, found.ids));
        }
        next = link.previous().cloned();
        if !resolved(&id)? {
            found.unresolved.push(link);
        }
    }
    // The chain starts here, so it is whole unless an applied envelope was expected.
    if applied.is_some() {
        return Ok((None, found.ids));
    }
    found.unresolved.reverse();
    let visited = found.ids.clone();
    Ok((Some(found), visited))
}

/// What the log holds of the chain of `source` and `kind`, for a cursor that
/// is not there.
pub(crate) struct ChainLog {
    /// Whether a chain is left that its cursor confirmed: a publication applied
    /// some of it, or a chained envelope of it waits.
    pub(crate) left: bool,
    /// The highest lease generation of its envelopes: a new cursor takes one
    /// above it, so that its envelopes are never those of an older worker.
    pub(crate) generation: u64,
    /// The newest envelope a publication resolved, as the manifest names it.
    pub(crate) applied: Option<fyrr_core::connector::LastEnvelope>,
    /// The highest sequence and rejection generation of its envelopes, which
    /// the next envelope must not go below.
    pub(crate) sequence: u64,
    pub(crate) rejection_generation: u32,
}

pub(crate) async fn chain_log(config: &Config, source: &str, kind: SyncKind) -> Result<ChainLog> {
    let mut log = ChainLog {
        left: false,
        generation: 0,
        applied: None,
        sequence: 0,
        rejection_generation: 0,
    };
    let Some(head) = fyrr_reader::storage::optional_head(config).await? else {
        return Ok(log);
    };
    let (store, root) = namespace_store(config)?;
    let applied = previous_manifest(store.as_ref(), &root, &head)
        .await?
        .and_then(|m| m.chain_applied.get(source)?.get(&kind).cloned());
    log.left = applied.is_some();
    let (paths, resolved) = envelope_paths(config, &head).await?;
    let (read, unreadable) = read_envelopes(config, &paths).await?;
    // An envelope that does not parse may be one of this chain's, of a
    // sequence or generation a new cursor must exceed.
    if !unreadable.is_empty() {
        return Err(WriterError::PublicationInvalid);
    }
    for (path, envelope) in read {
        if envelope.source_id != source || envelope.kind != kind {
            continue;
        }
        log.generation = log.generation.max(envelope.lease_generation);
        log.sequence = log.sequence.max(envelope.sequence);
        log.rejection_generation = log.rejection_generation.max(envelope.rejection_generation);
        if applied.as_ref() == Some(&envelope.envelope_id) {
            log.applied = Some(fyrr_core::connector::LastEnvelope {
                sequence: envelope.sequence,
                id: envelope.envelope_id.clone(),
            });
        }
        log.left |= envelope.lease_generation > 0
            && !path
                .filename()
                .and_then(|n| n.strip_suffix(".json"))
                .is_some_and(|id| resolved.contains(id));
    }
    if log.applied.is_none()
        && let Some(id) = applied
    {
        log.applied = Some(fyrr_core::connector::LastEnvelope {
            sequence: log.sequence,
            id,
        });
    }
    Ok(log)
}

pub async fn envelope_paths(config: &Config, head: &ServingHead) -> Result<(Vec<Path>, Resolved)> {
    let (store, root) = namespace_store(config)?;
    let resolved = resolved(config, head).await?;
    let paths: Vec<_> = store
        .list(Some(&root.clone().join("envelopes")))
        .try_collect()
        .await?;
    Ok((paths.into_iter().map(|p| p.location).collect(), resolved))
}

/// The envelopes the served head resolved, as [`resolved`] reads them.
#[derive(Debug, Default)]
pub struct Resolved {
    /// What the evidence of the served manifest records.
    published: Arc<Published>,
    /// What the head rejected after that publication.
    head_rejected: BTreeSet<String>,
}

/// What the evidence of one manifest records of it and every older one: the
/// checkpoint it names, which the manifests that name it share, and the IDs
/// since.
#[derive(Debug, Default)]
struct Published {
    checkpoint: Arc<Ids>,
    since: Ids,
}

#[derive(Debug, Default)]
struct Ids {
    applied: BTreeSet<String>,
    rejected: BTreeSet<String>,
}

impl Published {
    fn applied(&self, id: &str) -> bool {
        self.checkpoint.applied.contains(id) || self.since.applied.contains(id)
    }

    fn rejected(&self, id: &str) -> bool {
        self.checkpoint.rejected.contains(id) || self.since.rejected.contains(id)
    }
}

impl Resolved {
    /// Applied by a publication and rejected by none.
    pub fn applied(&self, id: &str) -> bool {
        self.published.applied(id) && !self.rejected(id)
    }

    /// Rejected by a publication, or by the head before its next one.
    pub fn rejected(&self, id: &str) -> bool {
        self.head_rejected.contains(id) || self.published.rejected(id)
    }

    /// Applied or rejected: no commit takes it again.
    pub fn contains(&self, id: &str) -> bool {
        self.published.applied(id) || self.rejected(id)
    }

    pub fn applied_ids(&self) -> impl Iterator<Item = &String> {
        let published = &self.published;
        published
            .checkpoint
            .applied
            .union(&published.since.applied)
            .filter(|id| !self.rejected(id))
    }

    pub fn rejected_ids(&self) -> impl Iterator<Item = &String> {
        let published = &self.published;
        let rejected: BTreeSet<_> = self
            .head_rejected
            .iter()
            .chain(&published.checkpoint.rejected)
            .chain(&published.since.rejected)
            .collect();
        rejected.into_iter()
    }
}

/// The evidence this process read last of each namespace, by storage and
/// incarnation, least recently used first: the manifest it was read from, the
/// checkpoint that manifest names, and what they record. A manifest is
/// immutable and names its checkpoint by digest, so an idle tick reads
/// neither again, and a new publication reads only its manifest while the
/// checkpoint stays. A process that serves many namespaces keeps the newest
/// `PUBLISHED_MAX` of them.
struct Held {
    namespace: (String, String),
    manifest: String,
    checkpoint: GenerationPointer,
    published: Arc<Published>,
}
static PUBLISHED: std::sync::Mutex<Vec<Held>> = std::sync::Mutex::new(Vec::new());
const PUBLISHED_MAX: usize = 16;

/// What the served head resolved: the evidence of its manifest, which covers
/// that publication and every older one (the checkpoint it names and the IDs
/// since), and what the head rejected after it. The only reader of envelope
/// evidence: no older manifest is read.
pub async fn resolved(config: &Config, head: &ServingHead) -> Result<Resolved> {
    let head_rejected = head.rejected_envelope_ids.iter().cloned().collect();
    let Some(pointer) = &head.content else {
        return Ok(Resolved {
            published: Arc::default(),
            head_rejected,
        });
    };
    let namespace = (dataset_uri(config), head.incarnation.clone());
    let held = {
        let mut cache = PUBLISHED
            .lock()
            .map_err(|_| WriterError::RuntimeUnavailable)?;
        let at = cache.iter().position(|held| held.namespace == namespace);
        at.map(|at| {
            let held = cache.remove(at);
            let found = (
                held.manifest.clone(),
                held.checkpoint.clone(),
                held.published.clone(),
            );
            cache.push(held);
            found
        })
    };
    if let Some((manifest, _, published)) = &held
        && *manifest == pointer.manifest
    {
        return Ok(Resolved {
            published: published.clone(),
            head_rejected,
        });
    }
    let (store, root) = namespace_store(config)?;
    let manifest =
        fyrr_reader::storage::read_manifest(store.as_ref(), &root, &pointer.manifest).await?;
    let evidence = manifest.envelope_evidence;
    let checkpoint = match held {
        Some((_, held, published)) if held == evidence.checkpoint => published.checkpoint.clone(),
        // Maintenance keeps the checkpoint of every retained manifest.
        _ => {
            let checkpoint = read_checkpoint(store.as_ref(), &root, &evidence.checkpoint)
                .await?
                .ok_or(WriterError::PublicationInvalid)?;
            Arc::new(Ids {
                applied: checkpoint.applied_envelope_ids.into_iter().collect(),
                rejected: checkpoint.rejected_envelope_ids.into_iter().collect(),
            })
        }
    };
    let published = Arc::new(Published {
        checkpoint,
        since: Ids {
            applied: evidence.applied_since.into_iter().collect(),
            rejected: evidence.rejected_since.into_iter().collect(),
        },
    });
    let mut cache = PUBLISHED
        .lock()
        .map_err(|_| WriterError::RuntimeUnavailable)?;
    cache.retain(|held| held.namespace != namespace);
    if cache.len() >= PUBLISHED_MAX {
        cache.remove(0);
    }
    cache.push(Held {
        namespace,
        manifest: pointer.manifest.clone(),
        checkpoint: evidence.checkpoint,
        published: published.clone(),
    });
    Ok(Resolved {
        published,
        head_rejected,
    })
}

/// The checkpoint a manifest names. `None` when maintenance removed it as
/// superseded. A checkpoint that is not the one named is an error, never skipped.
pub(crate) async fn read_checkpoint(
    store: &dyn ObjectStore,
    root: &Path,
    pointer: &GenerationPointer,
) -> Result<Option<EnvelopeCheckpoint>> {
    if !pointer.object.starts_with("evidence/") || pointer.object.contains("..") {
        return Err(WriterError::PublicationInvalid);
    }
    let bytes = match store
        .get(&Path::from(format!("{root}/{}", pointer.object)))
        .await
    {
        Ok(object) => object
            .bytes()
            .await
            .map_err(|_| WriterError::StorageUnavailable)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(_) => return Err(WriterError::StorageUnavailable),
    };
    if digest(&bytes) != pointer.digest {
        return Err(WriterError::PublicationInvalid);
    }
    Ok(Some(fyrr_reader::storage::parse_stored(
        &bytes,
        fyrr_reader::storage::StorageError::PublicationInvalid,
    )?))
}

pub(crate) async fn rejection_markers(
    config: &Config,
    head: &ServingHead,
) -> Result<Vec<RejectionMarker>> {
    markers(config, &resolved(config, head).await?).await
}

/// The markers of what `resolved` rejected.
pub(crate) async fn markers(config: &Config, resolved: &Resolved) -> Result<Vec<RejectionMarker>> {
    let (store, root) = namespace_store(config)?;
    let mut markers = Vec::new();
    for id in resolved.rejected_ids() {
        markers.push(
            fyrr_reader::storage::read_contract(
                store.as_ref(),
                &root.clone().join("rejections").join(format!("{id}.json")),
            )
            .await?,
        );
    }
    Ok(markers)
}

/// Whether a read among `reads` replaces the one `marker` rejected: a complete
/// read of its source and kind, of a newer rejection generation. A published
/// read consumes a marker, never a cursor.
pub(crate) fn consumed(marker: &RejectionMarker, reads: &[&PreparedEnvelope]) -> bool {
    reads.iter().any(|e| {
        crate::apply::complete_read(e)
            && replacement_matches(marker, &e.source_id, e.kind, e.rejection_generation)
    })
}

fn replacement_matches(
    rejected: &RejectionMarker,
    source_id: &str,
    kind: SyncKind,
    rejection_generation: u32,
) -> bool {
    source_id == rejected.source_id
        && kind == rejected.kind
        && rejection_generation > rejected.rejection_generation
}

/// The rejections that neither `applying` nor an applied envelope still in the
/// log consumed. Cleanup keeps the latest applied complete read, so this proof
/// survives it.
pub async fn unconsumed_rejections(
    config: &Config,
    head: &ServingHead,
    applying: &[PreparedEnvelope],
) -> Result<Vec<RejectionMarker>> {
    let resolved = resolved(config, head).await?;
    // Without a marker nothing is unconsumed, and the log need not be listed.
    if resolved.rejected_ids().next().is_none() {
        return Ok(Vec::new());
    }
    let (store, root) = namespace_store(config)?;
    let paths: Vec<_> = store
        .list(Some(&root.join("envelopes")))
        .try_collect()
        .await?;
    let mut reads = applying.to_vec();
    for path in paths.into_iter().map(|object| object.location) {
        if path
            .filename()
            .and_then(|n| n.strip_suffix(".json"))
            .is_some_and(|id| resolved.applied(id))
        {
            // One that does not parse is attributed by its chain, not here.
            match fyrr_reader::storage::read_contract(store.as_ref(), &path).await {
                Ok(envelope) => reads.push(envelope),
                Err(fyrr_reader::storage::StorageError::PublicationInvalid) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    let reads: Vec<_> = reads.iter().collect();
    Ok(markers(config, &resolved)
        .await?
        .into_iter()
        .filter(|marker| !consumed(marker, &reads))
        .collect())
}

/// One operation-scoped discovery for envelope retention and native-file protection.
pub(crate) struct CleanupEvidence {
    pub(crate) resolved: Resolved,
    /// The rejected envelopes whose markers no applied read consumed.
    pub(crate) unresolved: BTreeSet<String>,
    pub(crate) envelopes: Vec<(Path, PreparedEnvelope)>,
}

pub(crate) async fn cleanup_evidence(
    config: &Config,
    head: &ServingHead,
) -> Result<CleanupEvidence> {
    let (paths, resolved) = envelope_paths(config, head).await?;
    let (envelopes, _) = read_envelopes(config, &paths).await?;
    let reads: Vec<_> = envelopes
        .iter()
        .filter(|(path, _)| {
            path.filename()
                .and_then(|n| n.strip_suffix(".json"))
                .is_some_and(|id| resolved.applied(id))
        })
        .map(|(_, envelope)| envelope)
        .collect();
    let unresolved = markers(config, &resolved)
        .await?
        .into_iter()
        .filter(|marker| !consumed(marker, &reads))
        .map(|marker| marker.envelope_id)
        .collect();
    Ok(CleanupEvidence {
        resolved,
        unresolved,
        envelopes,
    })
}

/// A receipt is a projection of a published manifest, never a stored artifact.
#[derive(Debug, serde::Serialize)]
pub struct CommitReceipt {
    pub lance_version: u64,
    pub publication_id: Option<String>,
    pub envelope_ids: Vec<String>,
}

/// Recover a lost receipt even after a subsequent publication moved the head.
pub async fn receipt(config: &Config, envelope_id: &str) -> Result<CommitReceipt> {
    if !fyrr_core::limits::prefixed_hex(envelope_id, "env-", 32) {
        return Err(WriterError::EnvelopeIdInvalid);
    }
    let (store, root) = namespace_store(config)?;
    let head: ServingHead =
        fyrr_reader::storage::read_contract(store.as_ref(), &root.clone().join("head.json"))
            .await?;
    if head.namespace != config.namespace.name {
        return Err(WriterError::PublicationNamespaceInvalid);
    }
    // Applied means rejected by none, also by the publication that lists it
    // as applied: a write refused during apply has no receipt.
    if !resolved(config, &head).await?.applied(envelope_id) {
        return Err(WriterError::ReceiptUnpublished);
    }
    // The retained publications name the one that applied it.
    let mut chain = fyrr_reader::storage::manifest_chain(
        store,
        root,
        head.content.as_ref().map(|p| p.manifest.clone()),
    );
    while let Some(manifest) = chain.try_next().await? {
        if manifest.namespace != config.namespace.name {
            return Err(WriterError::PublicationNamespaceInvalid);
        }
        if manifest.envelope_ids.iter().any(|id| id == envelope_id) {
            return Ok(CommitReceipt {
                lance_version: manifest.lance_version,
                publication_id: Some(manifest.publication_id),
                envelope_ids: manifest.envelope_ids,
            });
        }
    }
    // Maintenance deleted the publications outside the purge window; the
    // evidence still tells that the envelope was applied, not by which one.
    Ok(CommitReceipt {
        lance_version: 0,
        publication_id: None,
        envelope_ids: vec![envelope_id.to_owned()],
    })
}

/// What one committer tick did.
#[derive(Debug)]
pub struct Tick {
    /// It took the lease and published, rather than finding nothing to commit.
    pub published: bool,
    /// Set when the served version has work for `writer maintain`: the
    /// configuration carrying the namespace's settings, as this tick judged
    /// commits by, since maintenance fails on a configuration that differs.
    pub maintenance: Option<Config>,
}

/// A service stops only between fully published groups, then releases its lease.
/// The lease costs two writes of the head, so a namespace with nothing to
/// commit is looked at by reads alone: the answer says whether the lease was
/// taken.
pub async fn commit_until_stopped(
    config: &Config,
    stage_timings: bool,
    stop: Option<&std::sync::atomic::AtomicBool>,
) -> Result<(CommitReceipt, Tick)> {
    let head = crate::lease::LeaseGuard::peek(config).await?;
    // Every tick takes the namespace's current settings: a long-running service
    // must not judge batches by the settings of a namespace it served before.
    let (store, root) = namespace_store(config)?;
    let policy: PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    let config = &config
        .with_settings(&policy.settings)
        .ok_or(WriterError::ConfigurationInvalid)?;
    if config.namespace.settings.write_api.is_some() && !config.sources.is_empty() {
        return Err(WriterError::ApiNamespaceHasSources);
    }
    if group::idle(config, &head, &policy).await? {
        let (receipt, maintenance_due) = served_receipt(config, &head).await?;
        let tick = Tick {
            published: false,
            maintenance: maintenance_due.then(|| config.clone()),
        };
        return Ok((receipt, tick));
    }
    let (receipt, maintenance_due) = crate::lease::LeaseGuard::with(config, |lease| async move {
        drain(config, stage_timings, &lease, stop).await
    })
    .await?;
    let tick = Tick {
        published: true,
        maintenance: maintenance_due.then(|| config.clone()),
    };
    Ok((receipt, tick))
}

/// The receipt of the served publication, and whether that manifest, which
/// the receipt reads anyway, shows maintenance due. It fails as a commit does
/// when the configuration no longer matches the namespace that publication
/// built.
async fn served_receipt(config: &Config, head: &ServingHead) -> Result<(CommitReceipt, bool)> {
    let (store, root) = namespace_store(config)?;
    let Some(manifest) = publication::previous_manifest(store.as_ref(), &root, head).await? else {
        let receipt = CommitReceipt {
            lance_version: 0,
            publication_id: None,
            envelope_ids: Vec::new(),
        };
        return Ok((receipt, false));
    };
    let due = crate::maintenance::due(config, &manifest);
    let receipt = CommitReceipt {
        lance_version: manifest.lance_version,
        publication_id: Some(manifest.publication_id),
        envelope_ids: manifest.envelope_ids,
    };
    Ok((receipt, due))
}

/// Maintenance services authorization at every boundary without draining
/// content backlog, until no authorization is pending.
pub(crate) async fn drain_authorization(
    config: &Config,
    lease: &LeaseGuard,
    drained: &mut Drained,
) -> Result<usize> {
    let mut groups = 0;
    loop {
        let remaining: Vec<_> = pending_of(config, &lease.head().await?, Some(&mut *drained))
            .await?
            .into_iter()
            .filter(|e| e.kind.is_authorization())
            .collect();
        if remaining.is_empty() {
            return Ok(groups);
        }
        crate::envelopes::discard_purged(config, lease).await?;
        let head = lease.head().await?;
        if head.content.is_none() {
            return Ok(groups);
        }
        // Authorization only, so no content waits on what `remaining` replaces.
        let unresolved = unconsumed_rejections(config, &head, &remaining).await?;
        let group = commit_group_with_rejections(config, remaining, &head, unresolved).await?;
        if group.is_empty() {
            return Ok(groups);
        }
        Box::pin(commit_one(config, false, lease, group)).await?;
        groups += 1;
    }
}

async fn drain(
    config: &Config,
    stage_timings: bool,
    lease: &LeaseGuard,
    stop: Option<&std::sync::atomic::AtomicBool>,
) -> Result<(CommitReceipt, bool)> {
    let mut next = None;
    loop {
        crate::envelopes::discard_purged(config, lease).await?;
        let head = lease.head().await?;
        let stopped = stop.is_some_and(|s| s.load(std::sync::atomic::Ordering::Acquire));
        let group = if stopped {
            Vec::new()
        } else {
            let envelopes = match next.take() {
                Some(envelopes) => envelopes,
                None => pending(config, &head).await?,
            };
            commit_group(config, envelopes, &head).await?
        };
        let empty = group.is_empty();
        if !stopped {
            commit_one(config, stage_timings, lease, group).await?;
        }
        if stopped
            || stop.is_some_and(|s| s.load(std::sync::atomic::Ordering::Acquire))
            || empty
            || lease.head().await?.content.is_none()
            || {
                next = Some(pending(config, &lease.head().await?).await?);
                next.as_ref().is_some_and(Vec::is_empty)
            }
        {
            return served_receipt(config, &lease.head().await?).await;
        }
    }
}

pub(crate) async fn commit_group(
    config: &Config,
    pending: Vec<PreparedEnvelope>,
    head: &ServingHead,
) -> Result<Vec<PreparedEnvelope>> {
    let published = unconsumed_rejections(config, head, &[]).await?;
    commit_group_with_rejections(config, pending, head, published).await
}
/// `published` are the rejections that no published read replaces.
async fn commit_group_with_rejections(
    config: &Config,
    mut pending: Vec<PreparedEnvelope>,
    head: &ServingHead,
    published: Vec<RejectionMarker>,
) -> Result<Vec<PreparedEnvelope>> {
    // A replacement that is pending consumes a rejection for the order of its key.
    let reads: Vec<_> = pending.iter().collect();
    let unresolved: Vec<_> = published
        .iter()
        .filter(|m| !consumed(m, &reads))
        .cloned()
        .collect();
    // Content waits for a publication that applies the replacement of a rejected
    // authorization read, as `commit_one` holds it back. A group holds one kind,
    // except before the first content, when the permissions wait in every group:
    // the replacement is committed with the content, or rejected and it waits again.
    let blocked = blocked_sources(if head.content.is_none() {
        &unresolved
    } else {
        &published
    });
    pending.retain(|e| {
        e.kind != fyrr_core::connector::SyncKind::Content || !blocked.contains(&e.source_id)
    });
    let first = head.content.is_none()
        && pending
            .iter()
            .any(|e| matches!(&e.payload, EnvelopePayload::Content(p) if !p.fragments.is_empty()));
    let rotation = head.content.as_ref().map_or(0, |p| p.sequence);
    // Rotate source order using the published sequence; no separate fairness state.
    let order: std::collections::BTreeMap<_, _> = pending
        .iter()
        .map(|e| e.source_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .enumerate()
        .map(|(i, s)| (s, i))
        .collect();
    let count = order.len().max(1);
    // Reject over-age authorization inputs first so their rejections become visible
    // before later reads of either kind. Valid authorization reads still precede content.
    // Source fairness applies after priority; valid sequences stay ordered within a kind.
    // Keep age priorities fixed while the sorter compares envelopes. Each source
    // and kind takes the priority of its oldest envelope, so sequence orders it,
    // and an older envelope keeps its place among the other keys.
    let now = fyrr_core::clock::now();
    let mut oldest = std::collections::BTreeMap::new();
    for e in &pending {
        let priority = if e.kind.is_authorization()
            && (now - e.created_at)
                .to_std()
                .is_ok_and(|age| age > config.writer.max_envelope_age.as_duration())
        {
            0
        } else if unresolved.iter().any(|m| {
            m.source_id == e.source_id
                && m.kind == e.kind
                && e.rejection_generation > m.rejection_generation
        }) {
            1
        } else {
            match e.kind {
                fyrr_core::connector::SyncKind::Permissions => 2,
                fyrr_core::connector::SyncKind::Members => 3,
                fyrr_core::connector::SyncKind::Reconcile => 4,
                fyrr_core::connector::SyncKind::Content => 5,
            }
        };
        let first = oldest
            .entry((e.source_id.clone(), e.kind))
            .or_insert((e.sequence, priority));
        if e.sequence < first.0 {
            *first = (e.sequence, priority);
        }
    }
    pending.sort_by_key(|e| {
        (
            oldest[&(e.source_id.clone(), e.kind)].1,
            (order[&e.source_id] + count - (crate::narrow_usize(rotation) % count)) % count,
            e.sequence,
        )
    });
    if first {
        return Ok(pending);
    } // Only the empty namespace's first IVF training.
    // Before the first content the permissions envelopes only wait, or are
    // rejected: they never keep another kind from being chosen, and they are
    // judged with what is, so that a rejection is recorded at once and a
    // commit with nothing else to do writes nothing.
    let waiting: Vec<_> = if head.content.is_none() {
        let (waiting, rest) = std::mem::take(&mut pending)
            .into_iter()
            .partition(|e| e.kind == SyncKind::Permissions);
        pending = rest;
        waiting
    } else {
        Vec::new()
    };
    let mut group: Vec<PreparedEnvelope> = Vec::new();
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut sources = BTreeSet::new();
    // Select at most one envelope per source per round. A document stays whole.
    // A round takes only from the leading run of one kind: never an envelope
    // past one of another kind that goes before it.
    while !pending.is_empty() {
        let kind = pending[0].kind;
        let position = pending
            .iter()
            .take_while(|e| e.kind == kind)
            .position(|e| !sources.contains(&e.source_id));
        let Some(position) = position else {
            sources.clear();
            continue;
        };
        if !group.is_empty() && kind != group[0].kind {
            break;
        }
        // A permission read is published whole: its oldest unpublished read,
        // the parts and the envelope that ends it, is one unit of the group.
        let unit = if kind == SyncKind::Permissions {
            read_of(&pending, &pending[position].source_id)
        } else {
            vec![pending[position].envelope_id.clone()]
        };
        let (mut next_rows, mut next_bytes, mut over) = (0u64, 0u64, false);
        for envelope in pending.iter().filter(|e| unit.contains(&e.envelope_id)) {
            let (work, size) = envelope_work(config, envelope)?;
            over |= work > config.writer.commit.group_max_rows
                || size > config.writer.commit.group_max_bytes.as_u64();
            next_rows = next_rows.saturating_add(work);
            next_bytes = next_bytes.saturating_add(size);
        }
        if over {
            // An envelope prepared under larger configured bounds is committed alone and
            // rejected (envelope_bounds_exceeded), with the rest of its read; its source
            // re-fetches it through the current bounded preparer. The group applies no
            // content; CAS publishes the rejection.
            if group.is_empty() {
                group = take(&mut pending, &unit);
            }
            break;
        }
        let fits = rows
            .checked_add(next_rows)
            .is_some_and(|n| n <= config.writer.commit.group_max_rows)
            && bytes
                .checked_add(next_bytes)
                .is_some_and(|n| n <= config.writer.commit.group_max_bytes.as_u64());
        // A read over the bounds of a group, each of its envelopes within them,
        // is the group alone: each envelope is applied as its own step.
        if !fits && !group.is_empty() {
            break;
        }
        rows = rows.saturating_add(next_rows);
        bytes = bytes.saturating_add(next_bytes);
        sources.insert(pending[position].source_id.clone());
        group.extend(take(&mut pending, &unit));
        // A final listing can derive a full bounded group of deletes.
        if !fits || kind == SyncKind::Reconcile {
            break;
        }
    }
    group.extend(waiting);
    Ok(group)
}

/// The IDs of the oldest permission read of `source` in `pending`: its links
/// up to the first that ends a read.
fn read_of(pending: &[PreparedEnvelope], source: &str) -> Vec<String> {
    let mut links: Vec<_> = pending
        .iter()
        .filter(|e| e.kind == SyncKind::Permissions && e.source_id == source)
        .collect();
    links.sort_by_key(|e| e.sequence);
    let end = links
        .iter()
        .position(|e| crate::apply::complete_read(e))
        .map_or(links.len(), |i| i + 1);
    links[..end].iter().map(|e| e.envelope_id.clone()).collect()
}

/// Takes the envelopes of `ids` out of `pending`, in sequence order.
fn take(pending: &mut Vec<PreparedEnvelope>, ids: &[String]) -> Vec<PreparedEnvelope> {
    let (mut taken, rest): (Vec<_>, Vec<_>) = std::mem::take(pending)
        .into_iter()
        .partition(|e| ids.contains(&e.envelope_id));
    *pending = rest;
    taken.sort_by_key(|e| e.sequence);
    taken
}

pub(crate) async fn apply_staged(
    config: &Config,
    dataset: &mut Dataset,
    lease: &LeaseGuard,
    lineage: &mut CommitLineage,
    keys: Vec<String>,
    fragments: Vec<serde_json::Value>,
) -> Result<()> {
    if keys.is_empty() && fragments.is_empty() {
        return Ok(());
    }
    lease.head().await?;
    let before = dataset.version().version;
    let (updated_fragments, removed_fragment_ids) = if keys.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        let staged =
            lance::dataset::DeleteBuilder::new(Arc::new(dataset.clone()), keys.join(" OR "))
                .execute_uncommitted()
                .await
                .map_err(|_| WriterError::DeleteStageFailed)?;
        let Operation::Delete {
            updated_fragments,
            deleted_fragment_ids,
            ..
        } = staged.transaction.operation
        else {
            return Err(WriterError::TransactionInvalid);
        };
        (updated_fragments, deleted_fragment_ids)
    };
    let new_fragments = fragments
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| WriterError::PreparedFragmentInvalid)?;
    // Vertical Update: existing physical fields are unchanged; only delete vectors move.
    // New row values live in new_fragments, whose indexes are built as deltas.
    let fields_modified = Vec::new();
    let operation = Operation::Update {
        removed_fragment_ids,
        updated_fragments,
        new_fragments,
        fields_modified,
        compacted_sstables: Vec::new(),
        fields_for_preserving_frag_bitmap: Vec::new(),
        update_mode: None,
        inserted_rows_filter: None,
        updated_fragment_offsets: None,
    };
    *dataset = native_step(
        lease,
        Dataset::commit(
            fyrr_reader::storage::dataset_uri(config).as_str(),
            operation,
            Some(before),
            Some(store_params(config)?),
            None,
            Arc::new(lance::session::Session::default()),
            false,
        ),
        WriterError::NativeUpdateFailed,
    )
    .await?;
    record_transaction(dataset, lineage, before).await
}

pub(crate) async fn record_transaction(
    dataset: &Dataset,
    lineage: &mut CommitLineage,
    before: u64,
) -> Result<()> {
    let version = dataset.version().version;
    if version == before {
        return Ok(());
    }
    if version != before.checked_add(1).ok_or(WriterError::LineageInvalid)? {
        return Err(WriterError::ForeignLineage);
    }
    let tx = dataset
        .read_version_transaction(version)
        .await?
        .transaction
        .ok_or(WriterError::LineageInvalid)?;
    if tx.read_version != before {
        return Err(WriterError::ForeignLineage);
    }
    lineage.transactions.push(LineageTransaction {
        version,
        transaction_id: tx.uuid,
    });
    Ok(())
}

async fn verify_lineage(dataset: &Dataset, lineage: &CommitLineage) -> Result<()> {
    if dataset.latest_version_id().await? != dataset.version().version {
        return Err(WriterError::ForeignLineage);
    }
    let mut previous = lineage
        .recovery_version
        .map_or(lineage.base_version, |v| v - 1);
    for (position, recorded) in lineage.transactions.iter().enumerate() {
        let tx = dataset
            .read_version_transaction(recorded.version)
            .await?
            .transaction
            .ok_or(WriterError::LineageInvalid)?;
        if tx.uuid != recorded.transaction_id || tx.read_version != previous {
            return Err(WriterError::ForeignLineage);
        }
        if position == 0
            && lineage.recovery_version.is_some()
            && !matches!(tx.operation, Operation::Restore { version } if version == lineage.base_version)
        {
            return Err(WriterError::ForeignLineage);
        }
        previous = recorded.version;
    }
    Ok(())
}

fn fixed_grants(config: &Config) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    config
        .sources
        .iter()
        .filter(|s| s.permissions == fyrr_core::config::PermissionsMode::Fixed)
        .map(|s| {
            Ok((
                s.id.clone(),
                s.readers
                    .iter()
                    .map(|r| hash_principal(config, "tok", r))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        })
        .collect()
}

fn envelope_work(config: &Config, envelope: &PreparedEnvelope) -> Result<(u64, u64)> {
    Ok(match &envelope.payload {
        EnvelopePayload::Content(p) => {
            let rows = p.fragments.iter().try_fold(0u64, |sum, f| {
                sum.checked_add(f.rows)
                    .ok_or(WriterError::EnvelopeBoundsInvalid)
            })?;
            let bytes = p.fragments.iter().flat_map(|f| &f.files).try_fold(
                write_contract(envelope).len() as u64,
                |sum, f| {
                    sum.checked_add(f.size_bytes)
                        .ok_or(WriterError::EnvelopeBoundsInvalid)
                },
            )?;
            // A delete and a patch each touch a record that is not in a fragment,
            // and a patch is written as a whole record.
            let deletes = p
                .documents
                .iter()
                .filter(|d| d.intent != DocumentIntent::Replace)
                .count() as u64;
            let patches = p
                .documents
                .iter()
                .filter(|d| d.intent == DocumentIntent::Patch)
                .count() as u64;
            (
                rows.checked_add(deletes)
                    .ok_or(WriterError::EnvelopeBoundsInvalid)?,
                bytes.saturating_add(crate::prepare::within(
                    patches.saturating_mul(crate::prepare::patch_bound(config)),
                    config.writer.commit.group_max_bytes.as_u64(),
                    bytes,
                )),
            )
        }
        // A permission envelope is weighed by its roots: the committer rewrites
        // their rows in bounded steps, whatever they hold.
        EnvelopePayload::Permissions(p) => (
            (p.documents.len() + p.container_grants.len()) as u64,
            write_contract(envelope).len() as u64,
        ),
        EnvelopePayload::Members(p) => (p.principals, write_contract(envelope).len() as u64),
        EnvelopePayload::Reconcile(p) => (
            p.root_ids.len() as u64,
            write_contract(envelope).len() as u64,
        ),
    })
}

#[allow(clippy::print_stderr)]
fn stage(enabled: bool, name: &str, start: std::time::Instant) {
    if enabled {
        eprintln!(
            "writer_stage stage={name} elapsed_us={} end_us={}",
            start.elapsed().as_micros(),
            fyrr_core::clock::now().timestamp_micros()
        );
    }
}

pub(crate) async fn native_step<T>(
    lease: &LeaseGuard,
    work: impl std::future::Future<Output = lance::Result<T>>,
    error: WriterError,
) -> Result<T> {
    lease.head().await?;
    let result = work.await;
    lease.head().await?;
    result.map_err(|_| error)
}

const INDEXES: &[(&str, &str, IndexType, &str)] = &[
    ("vector", "vector_idx", IndexType::Vector, "ivf_rq"),
    ("readers", "readers_idx", IndexType::LabelList, "label_list"),
    ("record_id", "record_id_idx", IndexType::BTree, "btree"),
    ("root_id", "root_id_idx", IndexType::BTree, "btree"),
    ("source_id", "source_id_idx", IndexType::Bitmap, "bitmap"),
    ("source_uri", "source_uri_idx", IndexType::BTree, "btree"),
    (
        "source_type",
        "source_type_idx",
        IndexType::Bitmap,
        "bitmap",
    ),
    ("project", "project_idx", IndexType::Bitmap, "bitmap"),
    ("author", "author_idx", IndexType::Bitmap, "bitmap"),
    ("updated_at", "updated_at_idx", IndexType::BTree, "btree"),
    (
        "attributes",
        "attributes_idx",
        IndexType::LabelList,
        "label_list",
    ),
    (
        "searchable_text",
        "fts_idx",
        IndexType::Inverted,
        "inverted",
    ),
];

/// The envelopes at `paths`, and the IDs of those that do not parse: an
/// envelope is attributed to its source only by the chain that names it, and
/// the walk of that chain, which reads it by ID, blocks that source alone. A
/// caller that needs every envelope fails on one that no chain names
/// ([`attributed`]).
pub(crate) async fn read_envelopes(
    config: &Config,
    paths: &[Path],
) -> Result<(Vec<(Path, PreparedEnvelope)>, BTreeSet<String>)> {
    let (store, _) = namespace_store(config)?;
    let mut envelopes = Vec::new();
    let mut unreadable = BTreeSet::new();
    for path in paths {
        match fyrr_reader::storage::read_contract(store.as_ref(), path).await {
            Ok(envelope) => envelopes.push((path.clone(), envelope)),
            Err(fyrr_reader::storage::StorageError::PublicationInvalid) => {
                crate::telemetry::envelope_unreadable(path);
                if let Some(id) = path.filename().and_then(|n| n.strip_suffix(".json")) {
                    unreadable.insert(id.to_owned());
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok((envelopes, unreadable))
}

fn blocked_sources(rejections: &[RejectionMarker]) -> BTreeSet<String> {
    rejections
        .iter()
        .filter(|marker| marker.kind.is_authorization())
        .map(|marker| marker.source_id.clone())
        .collect()
}

#[cfg(test)]
mod classify_tests {
    use super::*;
    use State::*;

    /// Every state of every kind: what `classify` makes of an envelope of
    /// `source`, ID `on`, lease generation 1, beside the cursor's generation 2.
    #[test]
    fn every_state_of_every_kind() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/contracts/prepared-envelope/permissions.json");
        let base: PreparedEnvelope =
            fyrr_core::parse_contract(&std::fs::read(path).unwrap()).unwrap();
        let set = |ids: &[&str]| ids.iter().map(|id| id.to_string()).collect::<BTreeSet<_>>();
        let whole = |ids: &[&str], generation| Chain::Whole {
            ids: set(ids),
            generation,
        };
        let retired = BTreeMap::from([(
            "retired".to_owned(),
            RetiredSource {
                retired_at: chrono::DateTime::UNIX_EPOCH,
                purged_at: None,
            },
        )]);
        let chains = |source: &str, chain: Option<(SyncKind, Chain)>| {
            chain
                .map(|(kind, chain)| BTreeMap::from([((source.to_owned(), kind), chain)]))
                .unwrap_or_default()
        };
        use SyncKind::{Content, Members, Permissions, Reconcile};
        use fyrr_core::connector::API_SOURCE_ID as API;
        let broken = |kind| Some((kind, Chain::Broken));
        let on = |kind, ids: &[&str], generation| Some((kind, whole(ids, generation)));
        // kind, source, applied, rejected, the chain walked, state
        type Case = (
            SyncKind,
            &'static str,
            &'static [&'static str],
            &'static [&'static str],
            Option<(SyncKind, Chain)>,
            State,
        );
        let cases: Vec<Case> = vec![
            (Content, "source", &["on"], &[], None, Applied),
            (
                Reconcile,
                "source",
                &["on"],
                &[],
                broken(Permissions),
                Applied,
            ),
            (Members, "source", &["on"], &[], None, Applied),
            (
                Permissions,
                "source",
                &["on"],
                &[],
                on(Permissions, &["on"], 2),
                Applied,
            ),
            (Content, "source", &["on"], &["on"], None, Rejected),
            (Reconcile, "source", &[], &["on"], None, Rejected),
            (Members, "source", &["on"], &["on"], None, Rejected),
            (Permissions, "retired", &[], &["on"], None, Rejected),
            (
                Content,
                "source",
                &[],
                &[],
                on(Content, &["on"], 2),
                Pending,
            ),
            (Content, "retired", &[], &[], None, Pending),
            (Reconcile, "source", &[], &[], None, Pending),
            (
                Reconcile,
                "source",
                &[],
                &[],
                on(Permissions, &[], 2),
                Pending,
            ),
            (Members, "source", &[], &[], broken(Permissions), Pending),
            (
                Permissions,
                "source",
                &[],
                &[],
                on(Permissions, &["on"], 2),
                Pending,
            ),
            (Permissions, "retired", &[], &[], None, Pending),
            // Content and listings wait on a broken permissions chain.
            (Reconcile, "source", &[], &[], broken(Permissions), Blocked),
            (Content, "source", &[], &[], broken(Content), Blocked),
            (
                Permissions,
                "source",
                &[],
                &[],
                broken(Permissions),
                Blocked,
            ),
            // A chained envelope off its chain never publishes.
            (Content, "source", &[], &[], None, Unconfirmed),
            (
                Content,
                "source",
                &[],
                &[],
                broken(Permissions),
                Unconfirmed,
            ),
            (
                Content,
                "source",
                &[],
                &[],
                on(Content, &["other"], 1),
                Unconfirmed,
            ),
            (
                Content,
                "source",
                &[],
                &[],
                on(Content, &["other"], 2),
                Dead,
            ),
            (Permissions, "source", &[], &[], None, Unconfirmed),
            (
                Permissions,
                "source",
                &[],
                &[],
                on(Permissions, &["other"], 1),
                Unconfirmed,
            ),
            (
                Permissions,
                "source",
                &[],
                &[],
                on(Permissions, &["other"], 2),
                Dead,
            ),
            (Content, API, &["on"], &[], broken(Content), Applied),
            (Content, API, &[], &[], on(Content, &["on"], 2), Pending),
            (Content, API, &[], &[], broken(Content), Blocked),
            (Content, API, &[], &[], None, Unconfirmed),
            (Content, API, &[], &[], on(Content, &["other"], 2), Dead),
        ];
        for (kind, source, applied, rejected, chain, state) in cases {
            let mut envelope = base.clone();
            envelope.kind = kind;
            envelope.source_id = source.into();
            envelope.envelope_id = "on".into();
            envelope.lease_generation = 1;
            let (applied, rejected, chains) = (set(applied), set(rejected), chains(source, chain));
            let resolved = Resolved {
                published: Arc::new(Published {
                    checkpoint: Arc::new(Ids {
                        applied: applied.clone(),
                        rejected: rejected.clone(),
                    }),
                    since: Ids::default(),
                }),
                head_rejected: BTreeSet::new(),
            };
            let facts = Facts {
                resolved: &resolved,
                retired: &retired,
                chains: &chains,
            };
            assert_eq!(
                classify(&envelope, &facts),
                state,
                "{kind:?} of {source}, applied {applied:?}, rejected {rejected:?}"
            );
        }
        // Content on its chain waits on a broken permissions chain of its source.
        let mut envelope = base.clone();
        envelope.kind = Content;
        envelope.source_id = "source".into();
        envelope.envelope_id = "on".into();
        envelope.lease_generation = 1;
        let chains = BTreeMap::from([
            (("source".to_owned(), Content), whole(&["on"], 1)),
            (("source".to_owned(), Permissions), Chain::Broken),
        ]);
        let facts = Facts {
            resolved: &Resolved::default(),
            retired: &retired,
            chains: &chains,
        };
        assert_eq!(classify(&envelope, &facts), Blocked);
    }
}

#[cfg(test)]
mod evidence_tests {
    use super::*;

    fn ids(prefix: char, count: usize) -> Vec<String> {
        (0..count)
            .map(|i| format!("env-{prefix}{i:031x}"))
            .collect()
    }

    /// A namespace whose newest manifest carries evidence over a checkpoint of
    /// `old` applied ids and `since` applied ids since; the manifest it
    /// names as its predecessor is not stored.
    async fn namespace(
        old: usize,
        since: usize,
    ) -> (Config, ServingHead, ContentManifest, Vec<String>) {
        let (config, _root) = crate::lease::tests::local_config("acme");
        let mut manifest: ContentManifest = serde_json::from_str(include_str!(
            "../../../fixtures/contracts/content-manifest/manifest.json"
        ))
        .unwrap();
        let head: ServingHead = serde_json::from_str(include_str!(
            "../../../fixtures/contracts/serving-head/head.json"
        ))
        .unwrap();
        let (store, root) = namespace_store(&config).unwrap();
        let applied = ids('a', old);
        let checkpoint = EnvelopeCheckpoint {
            schema_version: <EnvelopeCheckpoint as fyrr_core::Contract>::VERSION,
            namespace: "acme".into(),
            applied_envelope_ids: applied.clone(),
            rejected_envelope_ids: ids('b', 1),
        };
        manifest.envelope_evidence = EnvelopeEvidence {
            checkpoint: crate::prepare::put_generation(
                store.as_ref(),
                &root,
                "evidence",
                1,
                &checkpoint,
            )
            .await
            .unwrap(),
            applied_since: ids('c', since),
            rejected_since: Vec::new(),
        };
        store
            .put(
                &Path::from(format!("{root}/publications/pub-000042.json")),
                write_contract(&manifest).into(),
            )
            .await
            .unwrap();
        (config, head, manifest, applied)
    }

    fn set(ids: impl IntoIterator<Item = String>) -> BTreeSet<String> {
        ids.into_iter().collect()
    }

    /// The newest manifest's evidence answers for the whole chain: the older
    /// manifest it names, which is not stored, is never read.
    #[tokio::test]
    async fn a_manifest_with_evidence_ends_the_walk_of_the_chain() {
        let (config, mut head, manifest, applied) = namespace(3, 2).await;
        assert!(manifest.previous_publication_id.is_some());
        head.rejected_envelope_ids = ids('d', 1);
        let resolved = resolved(&config, &head).await.unwrap();
        let expected = set(applied.into_iter().chain(ids('c', 2)));
        assert_eq!(set(resolved.applied_ids().cloned()), expected);
        let rejected = set(ids('b', 1).into_iter().chain(ids('d', 1)));
        assert_eq!(set(resolved.rejected_ids().cloned()), rejected);
    }

    /// "Applied" is applied and rejected by none: an envelope a publication
    /// lists as both, or that the head rejected since, is rejected.
    #[tokio::test]
    async fn an_envelope_also_rejected_is_not_applied() {
        let (config, mut head, _, applied) = namespace(3, 0).await;
        head.rejected_envelope_ids = vec![applied[0].clone()];
        let resolved = resolved(&config, &head).await.unwrap();
        assert!(resolved.rejected(&applied[0]) && !resolved.applied(&applied[0]));
        assert!(resolved.contains(&applied[0]));
        assert!(resolved.applied(&applied[1]) && !resolved.rejected(&applied[1]));
        assert!(!resolved.contains("env-00000000000000000000000000000000"));
    }

    /// Ids gather in the manifests until a new checkpoint takes them over,
    /// with every older receipt.
    #[tokio::test]
    async fn a_new_checkpoint_takes_over_the_ids_that_gathered() {
        let (config, head, manifest, applied) = namespace(3, publication::CHECKPOINT_IDS - 2).await;
        let (store, root) = namespace_store(&config).unwrap();
        let receipts = ids('e', 1);
        let carried = publication::envelope_evidence(
            &config,
            store.as_ref(),
            &root,
            &head,
            Some(&manifest),
            &receipts,
            43,
        )
        .await
        .unwrap();
        assert_eq!(carried.checkpoint, manifest.envelope_evidence.checkpoint);
        assert_eq!(carried.applied_since.len(), publication::CHECKPOINT_IDS - 1);
        let receipts = ids('e', 2);
        let rolled = publication::envelope_evidence(
            &config,
            store.as_ref(),
            &root,
            &head,
            Some(&manifest),
            &receipts,
            43,
        )
        .await
        .unwrap();
        assert!(rolled.applied_since.is_empty() && rolled.rejected_since.is_empty());
        let checkpoint: EnvelopeCheckpoint =
            fyrr_reader::storage::artifact(store.as_ref(), &root, &rolled.checkpoint)
                .await
                .unwrap();
        let expected = set(applied
            .into_iter()
            .chain(ids('c', publication::CHECKPOINT_IDS - 2))
            .chain(receipts));
        assert_eq!(set(checkpoint.applied_envelope_ids), expected);
        assert_eq!(checkpoint.rejected_envelope_ids, ids('b', 1));
    }

    /// The checkpoint of the served manifest is always kept: one that is
    /// missing, or not the one named, stops the read.
    #[tokio::test]
    async fn a_missing_or_wrong_checkpoint_is_refused() {
        let (config, head, manifest, _) = namespace(3, 2).await;
        let (store, root) = namespace_store(&config).unwrap();
        let object = Path::from(format!(
            "{root}/{}",
            manifest.envelope_evidence.checkpoint.object
        ));
        store.put(&object, "{}".into()).await.unwrap();
        assert_eq!(
            resolved(&config, &head).await.err(),
            Some(WriterError::PublicationInvalid)
        );
        store.delete(&object).await.unwrap();
        assert_eq!(
            resolved(&config, &head).await.err(),
            Some(WriterError::PublicationInvalid)
        );
    }

    /// A complete read of the rejected source and kind, of a newer rejection
    /// generation, consumes a marker; nothing else does.
    #[test]
    fn only_a_complete_newer_read_of_its_key_consumes_a_marker() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/contracts/prepared-envelope/permissions.json");
        let mut read: PreparedEnvelope =
            fyrr_core::parse_contract(&std::fs::read(path).unwrap()).unwrap();
        read.source_id = "source".into();
        if let EnvelopePayload::Permissions(payload) = &mut read.payload {
            payload.complete_read = true;
        }
        let marker = RejectionMarker {
            schema_version: ENVELOPE_CONTRACT_VERSION,
            envelope_id: "fixture".into(),
            source_id: "source".into(),
            kind: SyncKind::Permissions,
            sequence: 99,
            rejection_generation: 3,
            start_cursor: None,
            reason: "fixture".into(),
        };
        for (source, kind, generation, expected) in [
            ("other", SyncKind::Permissions, 4, false),
            ("source", SyncKind::Members, 4, false),
            ("source", SyncKind::Permissions, 2, false),
            ("source", SyncKind::Permissions, 3, false),
            ("source", SyncKind::Permissions, 4, true),
            ("source", SyncKind::Permissions, u32::MAX, true),
        ] {
            let mut e = read.clone();
            e.source_id = source.into();
            e.kind = kind;
            e.rejection_generation = generation;
            assert_eq!(
                consumed(&marker, &[&e]),
                expected,
                "{source} {kind:?} generation={generation}"
            );
        }
        let exhausted = RejectionMarker {
            rejection_generation: u32::MAX,
            ..marker.clone()
        };
        let mut newest = read.clone();
        newest.rejection_generation = u32::MAX;
        assert!(!consumed(&exhausted, &[&newest]));
        if let EnvelopePayload::Permissions(payload) = &mut read.payload {
            payload.complete_read = false;
        }
        read.rejection_generation = 4;
        assert!(!consumed(&marker, &[&read]), "an incomplete read");
    }
}

#[cfg(test)]
mod chain_tests {
    use super::*;

    /// A permissions envelope of `source` in a chain: sequence, and the one before it.
    fn link(
        config: &Config,
        sequence: u64,
        previous: Option<&PreparedEnvelope>,
    ) -> PreparedEnvelope {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/contracts/prepared-envelope/permissions.json");
        let mut envelope: PreparedEnvelope =
            fyrr_core::parse_contract(&std::fs::read(path).unwrap()).unwrap();
        envelope.namespace.clone_from(&config.namespace.name);
        envelope.source_id = "source".into();
        envelope.rejection_generation = 0;
        envelope.cursor.start = None;
        envelope.sequence = sequence;
        envelope.lease_generation = 1;
        envelope.previous = previous.map(|p| p.envelope_id.clone());
        envelope.envelope_id = fyrr_core::envelope::leased_envelope_id(
            &envelope.namespace,
            "source",
            envelope.kind,
            sequence,
            0,
            None,
            1,
        );
        envelope
    }

    /// Three links the cursor of `source` confirms, written to storage.
    async fn confirmed_chain(config: &Config) -> Vec<PreparedEnvelope> {
        crate::lease::tests::put_head(config).await;
        let (store, root) = namespace_store(config).unwrap();
        let mut links: Vec<PreparedEnvelope> = Vec::new();
        for sequence in 1..=3 {
            let envelope = link(config, sequence, links.last());
            put_immutable(
                store.as_ref(),
                &root
                    .clone()
                    .join("envelopes")
                    .join(format!("{}.json", envelope.envelope_id)),
                &envelope,
            )
            .await
            .unwrap();
            links.push(envelope);
        }
        // The cursor of the worker that chained them.
        let mut state = crate::prepare::initial_cursor(config, "source", SyncKind::Permissions);
        state.sequence = 3;
        state.last = Some(fyrr_core::connector::LastEnvelope {
            sequence: 3,
            id: links[2].envelope_id.clone(),
        });
        store
            .put(
                &root.join("cursors").join("source-permissions.json"),
                write_contract(&state).into(),
            )
            .await
            .unwrap();
        links
    }

    /// Two first reads of a source at once: the one that found no cursor finds
    /// the other's chain in the log once that read confirmed it. The cursor is
    /// there then, so the read is busy, not lost; without it, it is lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_chain_a_concurrent_first_read_confirmed_is_busy_not_lost_on_local_storage() {
        let (config, root) = crate::lease::tests::local_config("cursor-race");
        confirmed_chain(&config).await;
        let (store, ns) = namespace_store(&config).unwrap();
        // No publication yet, as on a first read.
        let head_path = ns.clone().join("head.json");
        let mut head: ServingHead = fyrr_reader::storage::read_contract(store.as_ref(), &head_path)
            .await
            .unwrap();
        head.content = None;
        head.rejected_envelope_ids.clear();
        store
            .put(&head_path, write_contract(&head).into())
            .await
            .unwrap();
        let path = ns.join("cursors").join("source-permissions.json");
        let new = || {
            crate::sync_lease::CursorLease::new_permissions_cursor(
                &config,
                store.as_ref(),
                &path,
                "source",
            )
        };
        assert_eq!(new().await.err(), Some(WriterError::SyncBusy));
        store.delete(&path).await.unwrap();
        assert_eq!(new().await.err(), Some(WriterError::PermissionCursorLost));
        std::fs::remove_dir_all(root).unwrap();
    }

    async fn walk(
        config: &Config,
        applied: Option<&String>,
        listed: &[&PreparedEnvelope],
        resolved: &[&PreparedEnvelope],
    ) -> Option<Vec<u64>> {
        let (store, root) = namespace_store(config).unwrap();
        let listed = listed
            .iter()
            .map(|e| (e.envelope_id.clone(), (*e).clone()))
            .collect();
        let resolved: BTreeSet<_> = resolved.iter().map(|e| e.envelope_id.clone()).collect();
        let source = "source".to_owned();
        let last = crate::sync_lease::CursorLease::peek(config, &source, SyncKind::Permissions)
            .await
            .unwrap()
            .and_then(|state| state.last)
            .map(|last| last.id);
        super::walk(
            config,
            store.as_ref(),
            &root,
            (&source, SyncKind::Permissions),
            last,
            applied,
            &listed,
            &|id: &str| -> Result<bool> { Ok(resolved.contains(id)) },
        )
        .await
        .unwrap()
        .0
        .map(|walk| walk.unresolved.into_iter().map(|e| e.sequence).collect())
    }

    /// The chain is read by ID from the cursor: an envelope that a listing shows
    /// late, or not yet, is still applied before the ones that follow it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_chain_is_read_by_id_whatever_a_listing_shows_on_local_storage() {
        let (config, root) = crate::lease::tests::local_config("chain-by-id");
        let links = confirmed_chain(&config).await;
        // The listing shows the newest envelope and not the two before it.
        assert_eq!(
            walk(&config, None, &[&links[2]], &[]).await,
            Some(vec![1, 2, 3])
        );
        // The manifest applied the first: the walk stops at it.
        assert_eq!(
            walk(&config, Some(&links[0].envelope_id), &[&links[2]], &[]).await,
            Some(vec![2, 3])
        );
        // A rejected link is walked and not applied.
        assert_eq!(
            walk(
                &config,
                Some(&links[0].envelope_id),
                &[&links[2]],
                &[&links[1]]
            )
            .await,
            Some(vec![3])
        );
        // Everything applied: nothing to do, and nothing read.
        assert_eq!(
            walk(&config, Some(&links[2].envelope_id), &[], &[&links[2]]).await,
            Some(vec![])
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A chain that does not reach what the manifest applied blocks its source:
    /// nothing is skipped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_chain_that_does_not_reach_the_applied_envelope_blocks_its_source_on_local_storage() {
        let (config, root) = crate::lease::tests::local_config("chain-blocked");
        let links = confirmed_chain(&config).await;
        let elsewhere = "env-00000000000000000000000000000000".to_owned();
        assert_eq!(
            walk(&config, Some(&elsewhere), &[&links[2]], &[]).await,
            None
        );
        // A link that was deleted blocks it too.
        let (store, ns) = namespace_store(&config).unwrap();
        store
            .delete(
                &ns.join("envelopes")
                    .join(format!("{}.json", links[1].envelope_id)),
            )
            .await
            .unwrap();
        assert_eq!(walk(&config, None, &[&links[2]], &[]).await, None);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A cursor that holds no envelope is whole only when the manifest applied
    /// none: otherwise the chain it held is lost, and the source is blocked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cursor_without_a_confirmed_envelope_blocks_a_source_with_a_chain_on_local_storage() {
        let (config, root) = crate::lease::tests::local_config("chain-lost");
        let links = confirmed_chain(&config).await;
        let (store, ns) = namespace_store(&config).unwrap();
        let source = "source".to_owned();
        let applied = links[0].envelope_id.clone();
        let mut found = Vec::new();
        for applied in [None, Some(&applied)] {
            found.push(
                super::walk(
                    &config,
                    store.as_ref(),
                    &ns,
                    (&source, SyncKind::Permissions),
                    None,
                    applied,
                    &BTreeMap::<String, PreparedEnvelope>::new(),
                    &|_: &str| -> Result<bool> { Ok(false) },
                )
                .await
                .unwrap()
                .0
                .is_some(),
            );
        }
        assert_eq!(found, [true, false], "nothing applied: nothing lost");
        std::fs::remove_dir_all(root).unwrap();
    }
}
