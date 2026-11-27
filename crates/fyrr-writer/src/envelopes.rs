//! Envelope diagnostics and bounded cleanup; the immutable log remains cursor authority.
use crate::commit::State;
use crate::{Result, WriterError};
use futures::{StreamExt, TryStreamExt};
use fyrr_core::config::Config;
use fyrr_core::connector::SyncKind;
use fyrr_core::envelope::{EnvelopePayload, PreparedEnvelope};
use fyrr_core::publication::ServingHead;
use fyrr_reader::storage::{namespace_store, optional_head};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const STATUS_PAGE_PATHS: usize = 128;
#[cfg(not(test))]
type StatusEnvelope = PreparedEnvelope;
#[cfg(test)]
type StatusEnvelope = tests::TrackedEnvelope;

async fn applied_page(
    store: Arc<dyn ObjectStore>,
    root: &Path,
    head: &ServingHead,
    ids: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    let mut applied: BTreeSet<_> = head
        .rejected_envelope_ids
        .iter()
        .filter(|id| ids.contains(*id))
        .cloned()
        .collect();
    // Extra manifest passes buy bounded receipt memory: retain only this page's IDs.
    let mut chain = fyrr_reader::storage::manifest_chain(
        store.clone(),
        root.clone(),
        head.content.as_ref().map(|p| p.manifest.clone()),
    );
    let mut evidence = None;
    let mut cut = false;
    while let Some(publication) = chain.try_next().await? {
        cut = publication.previous_publication_id.is_some();
        evidence.get_or_insert(publication.envelope_evidence);
        applied.extend(
            publication
                .envelope_ids
                .into_iter()
                .chain(publication.rejected_envelope_ids)
                .filter(|id| ids.contains(id)),
        );
    }
    // The chain ends where maintenance deleted it; the newest evidence tells
    // the rest.
    if cut
        && let Some(evidence) = evidence
        && let Some(checkpoint) =
            crate::commit::read_checkpoint(store.as_ref(), root, &evidence.checkpoint).await?
    {
        applied.extend(
            checkpoint
                .applied_envelope_ids
                .into_iter()
                .chain(checkpoint.rejected_envelope_ids)
                .chain(evidence.applied_since)
                .chain(evidence.rejected_since)
                .filter(|id| ids.contains(id)),
        );
    }
    Ok(applied)
}

/// When each synced source's permissions and members were last read, from the
/// cursor states that a completed read stamps (null when none did). A read that
/// changed nothing writes no envelope. The reader never uses this.
async fn source_reads(
    config: &Config,
    policy: &fyrr_core::publication::PolicyArtifact,
) -> Result<Vec<serde_json::Value>> {
    let (store, root) = namespace_store(config)?;
    let now = fyrr_core::clock::now();
    let mut reads = Vec::new();
    for source in config
        .sources
        .iter()
        .filter(|s| s.permissions == fyrr_core::config::PermissionsMode::Sync)
        .filter(|s| !policy.retired_sources.contains_key(&s.id))
    {
        for (kind, interval) in [
            (SyncKind::Permissions, source.intervals.permissions),
            (SyncKind::Members, source.intervals.members),
        ] {
            if interval.is_none() {
                continue;
            }
            let path =
                root.clone()
                    .join("cursors")
                    .join(format!("{}-{}.json", source.id, kind.as_str()));
            // A missing, damaged or never-completed cursor is a null read: a
            // connector broken from the start must show up too.
            let state = match store.get(&path).await {
                Ok(object) => fyrr_core::parse_contract::<fyrr_core::connector::CursorState>(
                    &object.bytes().await?,
                )
                .ok(),
                Err(object_store::Error::NotFound { .. }) => None,
                Err(_) => return Err(WriterError::StorageUnavailable),
            }
            .filter(|state| state.updated_at > chrono::DateTime::UNIX_EPOCH);
            reads.push(serde_json::json!({
                "source_id": source.id,
                "kind": kind,
                "last_read_at": state.as_ref().map(|s| s.updated_at),
                "age_ms": state.as_ref().map(|s| (now - s.updated_at).num_milliseconds().max(0)),
            }));
        }
    }
    Ok(reads)
}

/// What a count of the unresolved envelopes holds of each: what [`classify`]
/// and the walk of a chain read, and no body.
///
/// [`classify`]: crate::commit::classify
#[derive(Clone)]
struct StatusLink {
    id: String,
    source_id: String,
    kind: SyncKind,
    previous: Option<String>,
    lease_generation: u64,
}

impl From<&PreparedEnvelope> for StatusLink {
    fn from(envelope: &PreparedEnvelope) -> Self {
        Self {
            id: envelope.envelope_id.clone(),
            source_id: envelope.source_id.clone(),
            kind: envelope.kind,
            previous: envelope.previous.clone(),
            lease_generation: envelope.lease_generation,
        }
    }
}

impl From<PreparedEnvelope> for StatusLink {
    fn from(envelope: PreparedEnvelope) -> Self {
        Self::from(&envelope)
    }
}

impl crate::commit::Link for StatusLink {
    fn id(&self) -> &str {
        &self.id
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

/// Counts are relative to the content_sequence pinned here, not an atomic
/// backlog snapshot. Listing may see later arrivals; counts are exact when the
/// namespace is quiescent, and concurrent commits can overcount pending work.
pub async fn status(config: &Config) -> Result<serde_json::Value> {
    let head = optional_head(config)
        .await?
        .ok_or(WriterError::NamespaceUnavailable)?;
    let (store, root) = namespace_store(config)?;
    let prefix = root.clone().join("envelopes");
    let mut listing = store.list(Some(&prefix)).fuse();
    let mut envelopes = 0;
    // What no publication resolved, without its body: the committer holds all of
    // it. One of another namespace, which the committer refuses, is not pending.
    let mut chained = BTreeMap::<String, StatusLink>::new();
    let mut unreadable = BTreeSet::new();
    let mut others = Vec::<StatusLink>::new();
    let mut quarantine = BTreeMap::<String, u64>::new();
    loop {
        let mut paths = Vec::with_capacity(STATUS_PAGE_PATHS);
        while paths.len() < STATUS_PAGE_PATHS {
            let Some(object) = listing.try_next().await? else {
                break;
            };
            paths.push(object.location);
        }
        if paths.is_empty() {
            break;
        }
        #[cfg(test)]
        tests::page(paths.len());
        let ids: BTreeSet<_> = paths
            .iter()
            .filter_map(|path| path.filename())
            .map(|name| name.strip_suffix(".json").unwrap_or(name).to_owned())
            .collect();
        let applied = applied_page(store.clone(), &root, &head, &ids).await?;
        for path in paths {
            // One decoded envelope, one manifest at a time, and a 128-path page;
            // ObjectStore's listing also retains its own bounded service page.
            let envelope: StatusEnvelope =
                match fyrr_reader::storage::read_contract(store.as_ref(), &path).await {
                    Ok(envelope) => envelope,
                    // Attributed by the chain that names it, as the committer does.
                    Err(fyrr_reader::storage::StorageError::PublicationInvalid) => {
                        envelopes += 1;
                        let id = path.filename().and_then(|n| n.strip_suffix(".json"));
                        if let Some(id) = id.filter(|id| !applied.contains(*id)) {
                            unreadable.insert(id.to_owned());
                        }
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
            let resolved = if ids.contains(&envelope.envelope_id) {
                applied.contains(&envelope.envelope_id)
            } else {
                // Preserve quiescent counts for a valid envelope stored under an alias.
                applied_page(
                    store.clone(),
                    &root,
                    &head,
                    &BTreeSet::from([envelope.envelope_id.clone()]),
                )
                .await?
                .contains(&envelope.envelope_id)
            };
            envelopes += 1;
            if !resolved && envelope.namespace == config.namespace.name {
                let body: &PreparedEnvelope = &envelope;
                let link = StatusLink::from(body);
                if crate::commit::chained(link.kind) {
                    chained.insert(link.id.clone(), link);
                } else {
                    others.push(link);
                }
            }
            if let EnvelopePayload::Content(payload) = &envelope.payload {
                for entry in &payload.quarantined {
                    *quarantine.entry(entry.reason.clone()).or_default() += 1;
                }
            }
        }
    }
    let policy: fyrr_core::publication::PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    // The chains the committer walks, and those of the sources whose chained
    // envelopes a publication resolved: their chain breaks when the cursor goes,
    // whatever is pending.
    let applied_chains = crate::commit::previous_manifest(store.as_ref(), &root, &head)
        .await?
        .map(|manifest| {
            manifest
                .chain_applied
                .into_iter()
                .flat_map(|(source, kinds)| {
                    kinds.into_keys().map(move |kind| (source.clone(), kind))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let keys: BTreeSet<_> = chained
        .values()
        .chain(&others)
        .flat_map(|e| crate::commit::chains_of(e.kind, &e.source_id))
        .chain(applied_chains.into_iter().filter(|(source, kind)| {
            *kind != SyncKind::Permissions || crate::commit::synced(config, source)
        }))
        .chain(
            (!unreadable.is_empty())
                .then(|| crate::commit::every_chain(config, &policy))
                .into_iter()
                .flatten(),
        )
        .collect();
    let (chains, links, named) = crate::commit::chains(
        config,
        &head,
        &policy,
        &keys,
        &chained,
        &|_: &str| -> Result<bool> { Ok(false) },
    )
    .await?;
    // One that no chain names fails `writer status`, as it fails the commit.
    crate::commit::attributed(&unreadable, &named)?;
    let none = BTreeSet::new();
    let facts = crate::commit::Facts {
        applied: &none,
        rejected: &none,
        retired: &policy.retired_sources,
        chains: &chains,
    };
    let pending_state = |e: &StatusLink| crate::commit::classify(e, &facts) == State::Pending;
    let mut pending = chained
        .values()
        .chain(&others)
        .filter(|e| pending_state(e))
        .count();
    // A link the walk read by ID, which no listing showed, is pending when no
    // publication resolved it, as the committer applies it.
    for link in links.iter().filter(|link| !chained.contains_key(&link.id)) {
        if pending_state(link)
            && !applied_page(
                store.clone(),
                &root,
                &head,
                &BTreeSet::from([link.id.clone()]),
            )
            .await?
            .contains(&link.id)
        {
            pending += 1;
        }
    }
    // A source whose chain does not reach what the manifest applied publishes
    // nothing of its permissions, content or listings until it is whole again.
    let blocked_sources: BTreeSet<&String> = chains
        .iter()
        .filter(|(_, chain)| matches!(chain, crate::commit::Chain::Broken))
        .map(|((source, _), _)| source)
        .collect();
    let mut warnings = crate::maintenance::purge_warnings(config, &head).await?;
    // A retired source that the configuration still names: its syncs are
    // refused and its envelopes rejected until it is removed from the file.
    let retired_configured: Vec<&str> = config
        .sources
        .iter()
        .filter(|source| policy.retired_sources.contains_key(&source.id))
        .map(|source| source.id.as_str())
        .collect();
    if !retired_configured.is_empty() {
        warnings.push("retired_source_configured");
    }
    let identity: fyrr_core::identity::IdentityArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.identity).await?;
    let directory = serde_json::json!({"provider":config.identity.directory.provider,"identity_generation":head.identity.generation,"principals":identity.principals.len(),"tokens":identity.tokens.len(),"sync_interval_ms":config.identity.directory.sync_interval.as_millis(),"encrypted_inputs":config.identity.encryption_key.is_some()});
    // The shape of the served version: the data files of its rows and the
    // segments of each index, which the cost of a read and of a commit follows.
    let mut fragments = 0;
    let mut index_segments = BTreeMap::<String, u64>::new();
    if let Some(pointer) = &head.content {
        let manifest: fyrr_core::publication::ContentManifest =
            fyrr_reader::storage::read_contract(
                store.as_ref(),
                &Path::from(format!("{root}/{}", pointer.manifest)),
            )
            .await?;
        fragments = manifest
            .files
            .iter()
            .filter(|file| file.path.starts_with("data/"))
            .count();
        for index in &manifest.indexes {
            *index_segments.entry(index.name.clone()).or_default() += 1;
        }
    }
    Ok(
        serde_json::json!({"directory":directory,"warnings":warnings,"retired_sources_configured":retired_configured,"content_sequence":head.content.map(|p|p.sequence),"envelopes":envelopes,"pending_envelopes":pending,"blocked_sources":blocked_sources,"quarantine":quarantine,"fragments":fragments,"index_segments":index_segments,"source_reads":source_reads(config,&policy).await?}),
    )
}

/// The envelopes of purged sources, found by reads alone.
pub(crate) async fn purged_envelopes(
    config: &Config,
    head: &ServingHead,
    policy: &fyrr_core::publication::PolicyArtifact,
) -> Result<Vec<(Path, PreparedEnvelope)>> {
    if !policy
        .retired_sources
        .values()
        .any(|source| source.purged_at.is_some())
    {
        return Ok(Vec::new());
    }
    let (paths, _) = crate::commit::envelope_paths(config, head).await?;
    Ok(crate::commit::read_envelopes(config, &paths)
        .await?
        .0
        .into_iter()
        .filter(|(_, envelope)| {
            policy
                .retired_sources
                .get(&envelope.source_id)
                .is_some_and(|source| source.purged_at.is_some())
        })
        .collect())
}

/// Purged sources cannot leave unpublished worker payloads in the log.
/// Applied fragments remain pinned by retained publications and age out normally.
pub(crate) async fn discard_purged(
    config: &Config,
    lease: &crate::lease::LeaseGuard,
) -> Result<usize> {
    let head = lease.head().await?;
    let (store, root, durability) = fyrr_reader::storage::cleanup_namespace_store(config)?;
    let policy: fyrr_core::publication::PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    let purged = purged_envelopes(config, &head, &policy).await?;
    if purged.is_empty() {
        return Ok(0);
    }
    let (applied, _) = crate::commit::published_ids(config, &head).await?;
    let mut retired = Vec::new();
    for (path, envelope) in purged {
        if !applied.contains(&envelope.envelope_id)
            && let EnvelopePayload::Content(payload) = &envelope.payload
        {
            let files = payload
                .fragments
                .iter()
                .flat_map(|fragment| &fragment.files)
                .collect::<Vec<_>>();
            for file in &files {
                if !file.path.starts_with("data/") || file.path.contains("..") {
                    return Err(WriterError::ChangedRowsInvalid);
                }
            }
            let deletion_store = store.as_ref();
            let root = &root;
            crate::io::bounded(files, |file| async move {
                lease.head().await?;
                let fragment = Path::from(format!("{root}/dataset.lance/{}", file.path));
                match deletion_store.delete(&fragment).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(_) => Err(WriterError::StorageUnavailable),
                }
            })
            .await?;
            // The envelope is the retry index for these retired fragments.
            // Persist their disappearance before removing that index entry.
            durability.finish().await?;
        }
        retired.push(path);
    }
    let deletion_store = store.as_ref();
    let deleted = crate::io::bounded(retired, |path| async move {
        lease.head().await?;
        match deletion_store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(_) => Err(WriterError::StorageUnavailable),
        }
    })
    .await?
    .len();
    durability.finish().await?;
    Ok(deleted)
}

pub(crate) async fn cleanup_held(
    config: &Config,
    lease: &crate::lease::LeaseGuard,
) -> Result<usize> {
    crate::maintenance::verify_history(config, &lease.head().await?).await?;
    discard_purged(config, lease).await?;
    let head = lease.head().await?;
    let (store, root, durability) = fyrr_reader::storage::cleanup_namespace_store(config)?;
    let crate::commit::CleanupEvidence {
        applied,
        rejected: published_rejected,
        unresolved,
        envelopes,
    } = crate::commit::cleanup_evidence(config, &head).await?;
    let policy: fyrr_core::publication::PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    // The chains are walked as the committer walks them: what is on
    // a chain stays, and so does what the committer waits on. An envelope that
    // is dead goes after `max_envelope_age`.
    let listed: BTreeMap<_, _> = envelopes
        .iter()
        .filter(|(_, e)| crate::commit::chained(e.kind) && e.namespace == config.namespace.name)
        .map(|(_, e)| (e.envelope_id.clone(), e.clone()))
        .collect();
    let keys = listed
        .values()
        .map(|e| (e.source_id.clone(), e.kind))
        .collect();
    let (chains, _, _) = crate::commit::chains(
        config,
        &head,
        &policy,
        &keys,
        &listed,
        &|id: &str| -> Result<bool> { Ok(applied.contains(id)) },
    )
    .await?;
    let facts = crate::commit::Facts {
        applied: &applied,
        rejected: &published_rejected,
        retired: &policy.retired_sources,
        chains: &chains,
    };
    let on_chain = |envelope: &PreparedEnvelope| {
        matches!(
            chains.get(&(envelope.source_id.clone(), envelope.kind)),
            Some(crate::commit::Chain::Whole { ids, .. }) if ids.contains(&envelope.envelope_id)
        )
    };
    let envelopes: Vec<_> = envelopes
        .into_iter()
        .map(|(path, envelope)| {
            let state = crate::commit::classify(&envelope, &facts);
            (path, envelope, state)
        })
        .collect();
    let mut latest_applied = BTreeMap::new();
    let mut latest = BTreeMap::new();
    let mut keep_multipart = BTreeSet::new();
    for (_, envelope, state) in &envelopes {
        let resolved = matches!(state, State::Applied | State::Rejected);
        // The newest chained envelope that counts is one that
        // was resolved or the cursor holds: one that is off its chain does not.
        if !crate::commit::chained(envelope.kind) || resolved || on_chain(envelope) {
            latest
                .entry((envelope.source_id.clone(), envelope.kind.as_str()))
                .and_modify(|seq: &mut u64| *seq = (*seq).max(envelope.sequence))
                .or_insert(envelope.sequence);
        }
        if *state == State::Applied && crate::apply::complete_read(envelope) {
            latest_applied
                .entry((envelope.source_id.clone(), envelope.kind.as_str()))
                .and_modify(|seq: &mut u64| *seq = (*seq).max(envelope.sequence))
                .or_insert(envelope.sequence);
        }
        if !resolved {
            match &envelope.payload {
                EnvelopePayload::Reconcile(p) => {
                    keep_multipart.insert((
                        envelope.source_id.clone(),
                        envelope.kind.as_str(),
                        p.listing_id.clone(),
                    ));
                }
                EnvelopePayload::Members(p) => {
                    keep_multipart.insert((
                        envelope.source_id.clone(),
                        envelope.kind.as_str(),
                        p.snapshot_id.clone(),
                    ));
                }
                _ => {}
            }
        }
    }
    let mut retired = Vec::new();
    for (path, envelope, state) in envelopes {
        if retired.len() as u64 >= config.writer.commit.group_max_rows {
            break;
        }
        let multipart = match &envelope.payload {
            EnvelopePayload::Reconcile(p) => Some(&p.listing_id),
            EnvelopePayload::Members(p) => Some(&p.snapshot_id),
            _ => None,
        };
        let key = (envelope.source_id.clone(), envelope.kind.as_str());
        let resolved = matches!(state, State::Applied | State::Rejected);
        // A dead envelope goes whatever else says to keep it.
        let dead = state == State::Dead;
        let kept = latest.get(&key) == Some(&envelope.sequence)
            || unresolved.contains(&envelope.envelope_id)
            || latest_applied.get(&key) == Some(&envelope.sequence)
            || !resolved
            || on_chain(&envelope)
            || multipart.is_some_and(|id| {
                keep_multipart.contains(&(
                    envelope.source_id.clone(),
                    envelope.kind.as_str(),
                    id.clone(),
                ))
            });
        if (kept && !dead)
            || (fyrr_core::clock::now() - envelope.created_at)
                .to_std()
                .is_ok_and(|age| age < config.writer.max_envelope_age.as_duration())
        {
            continue;
        }
        retired.push(path);
    }
    let deletion_store = store.as_ref();
    let deleted = crate::io::bounded(retired, |path| async move {
        lease.head().await?;
        deletion_store.delete(&path).await?;
        Ok(())
    })
    .await?
    .len();
    durability.finish().await?;
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fyrr_core::{
        Contract, SchemaVersion,
        envelope::{QuarantineEntry, leased_envelope_id},
        publication::ContentPointer,
        write_contract,
    };
    use serde::{Deserialize, Deserializer, Serialize};
    use std::{cell::RefCell, ops::Deref, sync::Mutex};

    #[derive(Debug, Default, Clone, Copy)]
    struct Trace {
        live: usize,
        peak: usize,
        decoded: usize,
        pages: usize,
        max_paths: usize,
    }
    thread_local! { static TRACE: RefCell<Arc<Mutex<Trace>>> = RefCell::new(Arc::default()); }
    pub(super) fn page(paths: usize) {
        TRACE.with(|trace| {
            let trace = trace.borrow();
            let mut trace = trace.lock().unwrap();
            trace.pages += 1;
            trace.max_paths = trace.max_paths.max(paths);
        });
    }
    // Track the actual deserialized owner's lifetime, including buffered owners
    // if the status path ever regresses to collecting decoded envelopes.
    #[derive(Serialize)]
    #[serde(transparent)]
    pub(super) struct TrackedEnvelope {
        envelope: PreparedEnvelope,
        #[serde(skip)]
        trace: Arc<Mutex<Trace>>,
    }
    impl<'de> Deserialize<'de> for TrackedEnvelope {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            let envelope = PreparedEnvelope::deserialize(deserializer)?;
            let trace = TRACE.with(|trace| trace.borrow().clone());
            {
                let mut stats = trace.lock().unwrap();
                stats.live += 1;
                stats.decoded += 1;
                stats.peak = stats.peak.max(stats.live);
            }
            Ok(Self { envelope, trace })
        }
    }
    impl Deref for TrackedEnvelope {
        type Target = PreparedEnvelope;
        fn deref(&self) -> &Self::Target {
            &self.envelope
        }
    }
    impl Drop for TrackedEnvelope {
        fn drop(&mut self) {
            self.trace.lock().unwrap().live -= 1;
        }
    }
    impl Contract for TrackedEnvelope {
        const NAME: &'static str = PreparedEnvelope::NAME;
        const VERSION: SchemaVersion = PreparedEnvelope::VERSION;
        fn validate(&self) -> Result<(), String> {
            self.envelope.validate()
        }
    }

    #[tokio::test]
    #[ignore = "real publication supplied by the dense_path status fixture"]
    // Exact count assertions below use a quiescent namespace.
    async fn status_bounded_backlog_on_storage() {
        let config = fyrr_core::config::load(std::path::Path::new(
            &std::env::var("FYRR_TEST_STATUS_CONFIG").unwrap(),
        ))
        .unwrap()
        .config;
        let baseline = status(&config).await.unwrap();
        let mut head = optional_head(&config).await.unwrap().unwrap();
        let (store, root) = namespace_store(&config).unwrap();
        let prefix = root.clone().join("envelopes");
        let mut listing = store.list(Some(&prefix));
        let mut seed = None;
        while let Some(object) = listing.try_next().await.unwrap() {
            let candidate: PreparedEnvelope =
                fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
                    .await
                    .unwrap();
            if matches!(candidate.payload, EnvelopePayload::Content(_)) {
                seed = Some(candidate);
                break;
            }
        }
        drop(listing);
        let seed = seed.unwrap();
        let EnvelopePayload::Content(content) = &seed.payload else {
            unreachable!()
        };
        assert!(content.quarantined.is_empty());
        assert!(
            !content.documents.is_empty(),
            "use real payload-bearing worker envelopes"
        );
        let marker = std::env::var("FYRR_TEST_STATUS_PRIVATE_CURSOR").unwrap();
        // The backlog is a chain that the content cursor confirmed.
        let mut cursor = crate::sync_lease::CursorLease::peek(&config, &seed.source_id, seed.kind)
            .await
            .unwrap()
            .unwrap();
        let mut ids = Vec::new();
        for offset in 0..260 {
            let mut envelope = seed.clone();
            envelope.sequence += 1000 + offset;
            envelope.cursor.start = Some(marker.clone());
            envelope.lease_generation = cursor.lease.as_ref().unwrap().epoch;
            envelope.previous = cursor.last.as_ref().map(|last| last.id.clone());
            envelope.envelope_id = leased_envelope_id(
                &envelope.namespace,
                &envelope.source_id,
                envelope.kind,
                envelope.sequence,
                envelope.rejection_generation,
                envelope.cursor.start.as_deref(),
                envelope.lease_generation,
            );
            cursor.sequence = envelope.sequence;
            cursor.last = Some(fyrr_core::connector::LastEnvelope {
                sequence: envelope.sequence,
                id: envelope.envelope_id.clone(),
            });
            let EnvelopePayload::Content(content) = &mut envelope.payload else {
                unreachable!()
            };
            content.quarantined = [WriterError::DocumentInvalid.as_str(), "document_oversized"]
                .into_iter()
                .map(|reason| QuarantineEntry {
                    root_digest: format!("sha256:{}", "0".repeat(64)),
                    revision_digest: format!("sha256:{}", "1".repeat(64)),
                    reason: reason.into(),
                })
                .collect();
            envelope.validate().unwrap();
            store
                .put(
                    &prefix
                        .clone()
                        .join(format!("{}.json", envelope.envelope_id)),
                    write_contract(&envelope).into(),
                )
                .await
                .unwrap();
            ids.push(envelope.envelope_id);
        }
        store
            .put(
                &root.clone().join("cursors").join(format!(
                    "{}-{}.json",
                    seed.source_id,
                    seed.kind.as_str()
                )),
                write_contract(&cursor).into(),
            )
            .await
            .unwrap();
        // Put a valid alias in a different page from its canonical ID, proving
        // membership follows the decoded ID, not an assumption about filenames.
        let alias = if ids.iter().filter(|id| *id < &seed.envelope_id).count() >= STATUS_PAGE_PATHS
        {
            "00-alias.json"
        } else {
            "zz-alias.json"
        };
        store
            .put(&prefix.clone().join(alias), write_contract(&seed).into())
            .await
            .unwrap();
        let mut manifest: fyrr_core::publication::ContentManifest =
            fyrr_reader::storage::read_contract(
                store.as_ref(),
                &Path::from(format!(
                    "{root}/{}",
                    head.content.as_ref().unwrap().manifest
                )),
            )
            .await
            .unwrap();
        for (applied, rejected) in [(0..50, 100..130), (50..100, 130..160)] {
            manifest.previous_publication_id = Some(manifest.publication_id.clone());
            manifest.sequence += 1;
            manifest.publication_id = format!("status-proof-{}", manifest.sequence);
            manifest.committed_at = fyrr_core::clock::now();
            manifest.envelope_ids = ids[applied].to_vec();
            manifest.rejected_envelope_ids = ids[rejected].to_vec();
            let path = format!("publications/{}.json", manifest.publication_id);
            store
                .put(
                    &Path::from(format!("{root}/{path}")),
                    write_contract(&manifest).into(),
                )
                .await
                .unwrap();
            head.content = Some(ContentPointer {
                publication_id: manifest.publication_id.clone(),
                sequence: manifest.sequence,
                lance_version: manifest.lance_version,
                manifest: path,
                committed_at: manifest.committed_at,
            });
        }
        head.rejected_envelope_ids
            .extend(ids[160..170].iter().cloned());
        head.rejected_envelope_ids.push(ids[0].clone()); // Duplicate authority must not double-count.
        store
            .put(
                &root.clone().join("head.json"),
                write_contract(&head).into(),
            )
            .await
            .unwrap();
        let trace = Arc::new(Mutex::new(Trace::default()));
        TRACE.with(|slot| *slot.borrow_mut() = trace.clone());
        let result = status(&config).await.unwrap();
        assert_eq!(
            result["envelopes"].as_u64().unwrap(),
            baseline["envelopes"].as_u64().unwrap() + 261
        );
        assert_eq!(
            result["pending_envelopes"].as_u64().unwrap(),
            baseline["pending_envelopes"].as_u64().unwrap() + 90
        );
        for reason in [WriterError::DocumentInvalid.as_str(), "document_oversized"] {
            assert_eq!(
                result["quarantine"][reason].as_u64().unwrap(),
                baseline["quarantine"][reason].as_u64().unwrap_or(0) + 260
            );
        }
        assert_eq!(result["content_sequence"], manifest.sequence);
        let stats = *trace.lock().unwrap();
        assert_eq!(stats.live, 0);
        assert_eq!(stats.peak, 1);
        assert_eq!(stats.decoded as u64, result["envelopes"].as_u64().unwrap());
        assert_eq!(stats.max_paths, STATUS_PAGE_PATHS);
        assert!(stats.pages >= 3);
        std::fs::write(
            std::env::var("FYRR_TEST_STATUS_RESULT").unwrap(),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
    }
}
