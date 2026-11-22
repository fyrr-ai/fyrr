//! Single-writer publication. The serving-head CAS is the sole visibility point.

use crate::lease::LeaseGuard;
use crate::prepare::{chunk_identity, put_immutable, store_params};
use crate::{Result, WriterError};
use futures::TryStreamExt;
use fyrr_core::config::{Config, ProcessEnvironment};
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
use lance_linalg::distance::DistanceType;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

mod group;
pub(crate) mod history;
mod namespace;
mod publication;

pub(crate) use group::commit_one;
pub(crate) use history::RestoreHistory;
pub(crate) use namespace::configured_identity;
pub(crate) use namespace::namespace_settings;
pub(crate) use namespace::sources_allowed;
pub use namespace::{create, create_with, delete};
pub(crate) use publication::publish;

pub async fn pending(config: &Config, head: &ServingHead) -> Result<Vec<PreparedEnvelope>> {
    let mut result = Vec::new();
    let (paths, applied) = envelope_paths(config, head).await?;
    let paths: Vec<_> = paths
        .into_iter()
        .filter(|path| {
            !path
                .filename()
                .and_then(|n| n.strip_suffix(".json"))
                .is_some_and(|id| applied.contains(id))
        })
        .collect();
    for (_, envelope) in read_envelopes(config, &paths).await? {
        if envelope.namespace != config.namespace.name {
            return Err(WriterError::EnvelopeNamespaceInvalid);
        }
        result.push(envelope);
    }
    result.sort_by(|a, b| (&a.source_id, a.sequence).cmp(&(&b.source_id, b.sequence)));
    Ok(result)
}

pub async fn envelope_paths(
    config: &Config,
    head: &ServingHead,
) -> Result<(Vec<Path>, BTreeSet<String>)> {
    let (store, root) = namespace_store(config)?;
    let (mut applied, rejected) = published_ids(config, head).await?;
    applied.extend(rejected);
    let paths: Vec<_> = store
        .list(Some(&root.clone().join("envelopes")))
        .try_collect()
        .await?;
    Ok((paths.into_iter().map(|p| p.location).collect(), applied))
}

pub async fn published_ids(
    config: &Config,
    head: &ServingHead,
) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    let evidence = published_evidence(config, head).await?;
    Ok((evidence.applied, evidence.rejected))
}

pub(crate) struct PublishedEvidence {
    pub(crate) applied: BTreeSet<String>,
    pub(crate) rejected: BTreeSet<String>,
    rewinds: Vec<String>,
}

/// What the manifest chain ending at `manifest` records. Publications are
/// immutable, so a later walk stops at a manifest it has seen.
#[derive(Clone)]
struct ChainEvidence {
    manifest: String,
    sequence: u64,
    applied: Arc<BTreeSet<String>>,
    rejected: Arc<BTreeSet<String>>,
    rewinds: Arc<Vec<String>>,
}

/// The chain evidence of the newest head this process walked, by namespace
/// root and incarnation, least recently used first. A long-running committer
/// reads every publication once instead of walking the whole history at each
/// tick. Each entry holds every envelope id the namespace has applied, so a
/// process that serves many namespaces keeps the newest `CHAIN_EVIDENCE_MAX`
/// of them; the next tick of an evicted one walks its history again.
static CHAIN_EVIDENCE: std::sync::Mutex<Vec<((String, String), ChainEvidence)>> =
    std::sync::Mutex::new(Vec::new());
const CHAIN_EVIDENCE_MAX: usize = 16;

pub(crate) async fn published_evidence(
    config: &Config,
    head: &ServingHead,
) -> Result<PublishedEvidence> {
    let (store, root) = namespace_store(config)?;
    let mut rejected: BTreeSet<String> = head.rejected_envelope_ids.iter().cloned().collect();
    let Some(newest) = head.content.as_ref().map(|p| p.manifest.clone()) else {
        return Ok(PublishedEvidence {
            applied: BTreeSet::new(),
            rejected,
            rewinds: Vec::new(),
        });
    };
    let key = (root.to_string(), head.incarnation.clone());
    let known = {
        let mut cache = CHAIN_EVIDENCE
            .lock()
            .map_err(|_| WriterError::RuntimeUnavailable)?;
        cache.iter().position(|(held, _)| *held == key).map(|at| {
            let entry = cache.remove(at);
            let known = entry.1.clone();
            cache.push(entry);
            known
        })
    };
    // An idle tick: the head serves the manifest walked last.
    if let Some(known) = known.as_ref().filter(|known| known.manifest == newest) {
        rejected.extend(known.rejected.iter().cloned());
        return Ok(PublishedEvidence {
            applied: known.applied.as_ref().clone(),
            rejected,
            rewinds: known.rewinds.to_vec(),
        });
    }
    let mut applied = BTreeSet::new();
    let mut chain_rejected = BTreeSet::new();
    let mut rewinds = None;
    let mut newest_sequence = None;
    let mut next = Some(newest.clone());
    let mut newer_sequence: Option<u64> = None;
    while let Some(path) = next {
        if let Some(known) = known.as_ref().filter(|known| known.manifest == path) {
            // The join obeys the same order as every other step.
            if newer_sequence.is_some_and(|newer| known.sequence >= newer) {
                return Err(WriterError::PublicationInvalid);
            }
            applied.extend(known.applied.iter().cloned());
            chain_rejected.extend(known.rejected.iter().cloned());
            rewinds.get_or_insert_with(|| known.rewinds.to_vec());
            break;
        }
        let publication = fyrr_reader::storage::read_manifest(store.as_ref(), &root, &path).await?;
        // As in `manifest_chain`: every older publication has a lower sequence.
        if newer_sequence.is_some_and(|newer| publication.sequence >= newer) {
            return Err(WriterError::PublicationInvalid);
        }
        newer_sequence = Some(publication.sequence);
        newest_sequence.get_or_insert(publication.sequence);
        rewinds.get_or_insert_with(|| publication.cursor_rewinds.clone());
        applied.extend(publication.envelope_ids);
        chain_rejected.extend(publication.rejected_envelope_ids);
        next = publication
            .previous_publication_id
            .map(|id| format!("publications/{id}.json"));
        // The evidence covers this publication and every older one. A
        // predecessor this process holds is cheaper than its checkpoint.
        if let Some(evidence) = publication.envelope_evidence
            && next
                .as_ref()
                .is_none_or(|path| known.as_ref().is_none_or(|known| known.manifest != *path))
            && let Some(checkpoint) =
                read_checkpoint(store.as_ref(), &root, &evidence.checkpoint).await?
        {
            applied.extend(checkpoint.applied_envelope_ids);
            applied.extend(evidence.applied_since);
            chain_rejected.extend(checkpoint.rejected_envelope_ids);
            chain_rejected.extend(evidence.rejected_since);
            break;
        }
    }
    let rewinds = rewinds.unwrap_or_default();
    if let Some(sequence) = newest_sequence {
        let mut cache = CHAIN_EVIDENCE
            .lock()
            .map_err(|_| WriterError::RuntimeUnavailable)?;
        cache.retain(|(held, _)| *held != key);
        if cache.len() >= CHAIN_EVIDENCE_MAX {
            cache.remove(0);
        }
        cache.push((
            key,
            ChainEvidence {
                manifest: newest,
                sequence,
                applied: Arc::new(applied.clone()),
                rejected: Arc::new(chain_rejected.clone()),
                rewinds: Arc::new(rewinds.clone()),
            },
        ));
    }
    rejected.extend(chain_rejected);
    Ok(PublishedEvidence {
        applied,
        rejected,
        rewinds,
    })
}

/// The checkpoint a manifest names. `None` when maintenance removed it as
/// superseded: the older manifests then still tell the same. A checkpoint that
/// is not the one named is an error, never skipped.
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
    fyrr_core::parse_contract(&bytes)
        .map(Some)
        .map_err(|_| WriterError::PublicationInvalid)
}

pub(crate) async fn rejection_markers(
    config: &Config,
    head: &ServingHead,
) -> Result<Vec<RejectionMarker>> {
    let evidence = published_evidence(config, head).await?;
    markers_from_evidence(config, &evidence).await
}

pub(crate) async fn markers_from_evidence(
    config: &Config,
    evidence: &PublishedEvidence,
) -> Result<Vec<RejectionMarker>> {
    let (store, root) = namespace_store(config)?;
    let mut markers = Vec::new();
    for path in evidence
        .rejected
        .iter()
        .map(|id| format!("rejections/{id}.json"))
        .chain(evidence.rewinds.iter().cloned())
    {
        markers.push(
            fyrr_reader::storage::read_contract(
                store.as_ref(),
                &Path::from(format!("{root}/{path}")),
            )
            .await?,
        );
    }
    Ok(markers)
}

async fn rejection_markers_from_ids(
    config: &Config,
    head: &ServingHead,
    rejected: &BTreeSet<String>,
) -> Result<Vec<RejectionMarker>> {
    let (store, root) = namespace_store(config)?;
    let mut markers = Vec::new();
    for id in rejected {
        markers.push(
            fyrr_reader::storage::read_contract(
                store.as_ref(),
                &root.clone().join("rejections").join(format!("{id}.json")),
            )
            .await?,
        );
    }
    if let Some(pointer) = &head.content {
        let manifest: ContentManifest = fyrr_reader::storage::read_contract(
            store.as_ref(),
            &Path::from(format!("{root}/{}", pointer.manifest)),
        )
        .await?;
        for path in &manifest.cursor_rewinds {
            markers.push(
                fyrr_reader::storage::read_contract(
                    store.as_ref(),
                    &Path::from(format!("{root}/{path}")),
                )
                .await?,
            );
        }
    }
    Ok(markers)
}

/// Consumption is proved by a published replacement read, never a cursor cache update.
/// Cleanup keeps the latest applied complete read so this proof survives log cleanup.
pub async fn unconsumed_rejections(
    config: &Config,
    head: &ServingHead,
    applying: &[PreparedEnvelope],
) -> Result<Vec<RejectionMarker>> {
    let (store, root) = namespace_store(config)?;
    let evidence = published_evidence(config, head).await?;
    // Markers are the rejected envelopes and the cursor rewinds of the
    // served manifest; without either nothing is unconsumed, and the
    // retained envelopes need not be listed or read.
    if evidence.rejected.is_empty() && evidence.rewinds.is_empty() {
        return Ok(Vec::new());
    }
    let (applied, rejected) = (evidence.applied, evidence.rejected);
    let mut reads = applying.to_vec();
    let paths: Vec<_> = store
        .list(Some(&root.clone().join("envelopes")))
        .try_collect()
        .await?;
    for path in paths {
        if path
            .location
            .filename()
            .and_then(|n| n.strip_suffix(".json"))
            .is_some_and(|id| applied.contains(id) && !rejected.contains(id))
        {
            reads.push(fyrr_reader::storage::read_contract(store.as_ref(), &path.location).await?);
        }
    }
    let reads = reads.iter().collect::<Vec<_>>();
    unconsumed_rejections_from_reads(config, head, &reads, &rejected).await
}

/// Reuse an operation's envelope snapshot and receipt sets during cleanup.
pub(crate) async fn unconsumed_rejections_from_reads(
    config: &Config,
    head: &ServingHead,
    reads: &[&PreparedEnvelope],
    rejected: &BTreeSet<String>,
) -> Result<Vec<RejectionMarker>> {
    let mut unresolved = Vec::new();
    for marker in rejection_markers_from_ids(config, head, rejected).await? {
        if !reads.iter().any(|e| {
            history::replacement_matches(
                &marker,
                &e.source_id,
                e.kind,
                e.rejection_generation,
                e.restore_sequence,
            ) && crate::apply::complete_read(e)
        }) {
            unresolved.push(marker);
        }
    }
    Ok(unresolved)
}

/// One operation-scoped discovery for envelope retention and native-file protection.
pub(crate) struct CleanupEvidence {
    pub(crate) applied: BTreeSet<String>,
    pub(crate) rejected: BTreeSet<String>,
    pub(crate) unresolved: BTreeSet<String>,
    pub(crate) envelopes: Vec<(Path, PreparedEnvelope)>,
}

pub(crate) async fn cleanup_evidence(
    config: &Config,
    head: &ServingHead,
) -> Result<CleanupEvidence> {
    let evidence = published_evidence(config, head).await?;
    let (store, root) = namespace_store(config)?;
    let paths: Vec<_> = store
        .list(Some(&root.join("envelopes")))
        .try_collect()
        .await?;
    let paths: Vec<_> = paths.into_iter().map(|object| object.location).collect();
    let envelopes = read_envelopes(config, &paths).await?;
    let reads = envelopes
        .iter()
        .filter(|(path, _)| {
            path.filename()
                .and_then(|n| n.strip_suffix(".json"))
                .is_some_and(|id| evidence.applied.contains(id) && !evidence.rejected.contains(id))
        })
        .map(|(_, envelope)| envelope)
        .collect::<Vec<_>>();
    let unresolved = markers_from_evidence(config, &evidence)
        .await?
        .into_iter()
        .filter(|marker| {
            !reads.iter().any(|e| {
                history::replacement_matches(
                    marker,
                    &e.source_id,
                    e.kind,
                    e.rejection_generation,
                    e.restore_sequence,
                ) && crate::apply::complete_read(e)
            })
        })
        .map(|marker| marker.envelope_id)
        .collect();
    let mut applied = evidence.applied;
    applied.extend(evidence.rejected.iter().cloned());
    Ok(CleanupEvidence {
        applied,
        rejected: evidence.rejected,
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
    if published_evidence(config, &head)
        .await?
        .applied
        .contains(envelope_id)
    {
        return Ok(CommitReceipt {
            lance_version: 0,
            publication_id: None,
            envelope_ids: vec![envelope_id.to_owned()],
        });
    }
    Err(WriterError::ReceiptUnpublished)
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
    if config.namespace.write_api.is_some() && !config.sources.is_empty() {
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

/// Maintenance services authorization at every boundary without draining content backlog.
/// Share bounded history evidence without changing either drain policy:
/// restore snapshots initial arrivals; maintenance drains until quiet.
pub(crate) async fn drain_cached_authorization(
    config: &Config,
    lease: &LeaseGuard,
    history: &mut RestoreHistory,
    pending: Vec<PreparedEnvelope>,
    snapshot: bool,
) -> Result<usize> {
    Box::pin(drain_authorization_held(
        config,
        lease,
        snapshot,
        Some(history),
        Some(pending),
    ))
    .await
}

async fn drain_authorization_held(
    config: &Config,
    lease: &LeaseGuard,
    snapshot: bool,
    mut history: Option<&mut RestoreHistory>,
    initial: Option<Vec<PreparedEnvelope>>,
) -> Result<usize> {
    let mut groups = 0;
    crate::envelopes::discard_purged(config, lease).await?;
    let mut head = lease.head().await?;
    if head.content.is_none() {
        return Ok(groups);
    }
    let mut remaining = match initial {
        Some(pending) => pending,
        None => pending(config, &head)
            .await?
            .into_iter()
            .filter(|e| e.kind.is_authorization())
            .collect(),
    };
    let mut snapshot_ids: BTreeSet<_> = remaining.iter().map(|e| e.envelope_id.clone()).collect();
    loop {
        let group = if let Some(h) = history.as_deref_mut() {
            h.refresh(config, &head).await?;
            let unresolved = h.unconsumed_rejections(config, &[]).await?;
            commit_group_with_rejections(config, remaining, &head, unresolved).await?
        } else {
            commit_group(config, remaining, &head).await?
        };
        if group.is_empty() {
            return Ok(groups);
        }
        let selected: BTreeSet<_> = group.iter().map(|e| e.envelope_id.clone()).collect();
        commit_one(config, false, lease, group, history.as_deref_mut()).await?;
        #[cfg(test)]
        crate::maintenance::restore_test_drain();
        groups += 1;
        crate::envelopes::discard_purged(config, lease).await?;
        head = lease.head().await?;
        if let Some(h) = history.as_deref_mut() {
            h.refresh(config, &head).await?;
        }
        if snapshot {
            snapshot_ids.retain(|id| !selected.contains(id));
            if snapshot_ids.is_empty() {
                return Ok(groups);
            }
        }
        remaining = match history.as_deref_mut() {
            Some(h) => h.pending(config, true).await?,
            None => pending(config, &head).await?,
        }
        .into_iter()
        .filter(|e| {
            e.kind.is_authorization() && (!snapshot || snapshot_ids.contains(&e.envelope_id))
        })
        .collect();
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
            commit_one(config, stage_timings, lease, group, None).await?;
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
    let unresolved = unconsumed_rejections(config, head, &[]).await?;
    commit_group_with_rejections(config, pending, head, unresolved).await
}
async fn commit_group_with_rejections(
    config: &Config,
    mut pending: Vec<PreparedEnvelope>,
    head: &ServingHead,
    unresolved: Vec<RejectionMarker>,
) -> Result<Vec<PreparedEnvelope>> {
    let blocked = blocked_sources(&unresolved);
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
    // Keep age priorities fixed while the sorter compares envelopes.
    let now = fyrr_core::clock::now();
    pending.sort_by_key(|e| {
        (
            if e.kind.is_authorization()
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
            },
            (order[&e.source_id] + count - (crate::narrow_usize(rotation) % count)) % count,
            e.sequence,
        )
    });
    if first {
        return Ok(pending);
    } // Only the empty namespace's first IVF training.
    let mut group: Vec<PreparedEnvelope> = Vec::new();
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut sources = BTreeSet::new();
    // Select at most one envelope per source per round. A document stays whole.
    while !pending.is_empty() {
        let kind = pending[0].kind;
        let position = pending
            .iter()
            .position(|e| e.kind == kind && !sources.contains(&e.source_id));
        let Some(position) = position else {
            sources.clear();
            continue;
        };
        if !group.is_empty() && pending[position].kind != group[0].kind {
            break;
        }
        let envelope = &pending[position];
        let (next_rows, next_bytes) = envelope_work(config, head, envelope).await?;
        if next_rows > config.writer.commit.group_max_rows
            || next_bytes > config.writer.commit.group_max_bytes.as_u64()
        {
            // An envelope prepared under larger configured bounds is committed alone and
            // rejected (envelope_bounds_exceeded); its source re-fetches it through the
            // current bounded preparer. The group applies no content; CAS publishes the rejection.
            if group.is_empty() {
                return Ok(vec![pending.remove(position)]);
            }
            break;
        }
        if rows
            .checked_add(next_rows)
            .is_none_or(|n| n > config.writer.commit.group_max_rows)
            || bytes
                .checked_add(next_bytes)
                .is_none_or(|n| n > config.writer.commit.group_max_bytes.as_u64())
        {
            break;
        }
        rows += next_rows;
        bytes += next_bytes;
        sources.insert(envelope.source_id.clone());
        group.push(pending.remove(position));
        // A final listing can derive a full bounded group of deletes.
        if kind == fyrr_core::connector::SyncKind::Reconcile {
            break;
        }
    }
    Ok(group)
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

async fn envelope_work(
    config: &Config,
    head: &ServingHead,
    envelope: &PreparedEnvelope,
) -> Result<(u64, u64)> {
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
            // A delete and a patch each touch a record that is not in a fragment.
            let deletes = p
                .documents
                .iter()
                .filter(|d| d.intent != DocumentIntent::Replace)
                .count() as u64;
            (
                rows.checked_add(deletes)
                    .ok_or(WriterError::EnvelopeBoundsInvalid)?,
                bytes,
            )
        }
        EnvelopePayload::Permissions(p) => (
            crate::apply::permission_work(config, head, &envelope.source_id, p).await?,
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

pub(crate) async fn read_envelopes(
    config: &Config,
    paths: &[Path],
) -> Result<Vec<(Path, PreparedEnvelope)>> {
    let (store, _) = namespace_store(config)?;
    let mut envelopes = Vec::new();
    for path in paths {
        envelopes.push((
            path.clone(),
            fyrr_reader::storage::read_contract(store.as_ref(), path).await?,
        ));
    }
    Ok(envelopes)
}

fn blocked_sources(rejections: &[RejectionMarker]) -> BTreeSet<String> {
    rejections
        .iter()
        .filter(|marker| marker.kind.is_authorization())
        .map(|marker| marker.source_id.clone())
        .collect()
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
        manifest.envelope_evidence = Some(EnvelopeEvidence {
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
        });
        store
            .put(
                &Path::from(format!("{root}/publications/pub-000042.json")),
                write_contract(&manifest).into(),
            )
            .await
            .unwrap();
        (config, head, manifest, applied)
    }

    /// The newest manifest's evidence answers for the whole chain: the older
    /// manifest it names, which is not stored, is never read.
    #[tokio::test]
    async fn a_manifest_with_evidence_ends_the_walk_of_the_chain() {
        let (config, mut head, manifest, applied) = namespace(3, 2).await;
        assert!(manifest.previous_publication_id.is_some());
        head.rejected_envelope_ids = ids('d', 1);
        let evidence = published_evidence(&config, &head).await.unwrap();
        let expected: BTreeSet<_> = applied
            .into_iter()
            .chain(ids('c', 2))
            .chain(manifest.envelope_ids)
            .collect();
        assert_eq!(evidence.applied, expected);
        let rejected: BTreeSet<_> = ids('b', 1).into_iter().chain(ids('d', 1)).collect();
        assert_eq!(evidence.rejected, rejected);
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
        assert_eq!(
            carried.checkpoint,
            manifest.envelope_evidence.as_ref().unwrap().checkpoint
        );
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
        let expected: BTreeSet<_> = applied
            .into_iter()
            .chain(ids('c', publication::CHECKPOINT_IDS - 2))
            .chain(manifest.envelope_ids)
            .chain(receipts)
            .collect();
        assert_eq!(
            checkpoint
                .applied_envelope_ids
                .into_iter()
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert_eq!(checkpoint.rejected_envelope_ids, ids('b', 1));
    }
    /// A process that walked the predecessor adds only the new manifest: the
    /// new manifest's checkpoint, which is not stored here, is never read.
    #[tokio::test]
    async fn a_walked_predecessor_saves_reading_the_checkpoint() {
        let (config, head, manifest, applied) = namespace(3, 2).await;
        published_evidence(&config, &head).await.unwrap();
        let (store, root) = namespace_store(&config).unwrap();
        let mut next = manifest.clone();
        next.publication_id = "pub-000043".into();
        next.sequence = manifest.sequence + 1;
        next.previous_publication_id = Some(manifest.publication_id.clone());
        next.envelope_ids = ids('f', 1);
        let mut evidence = next.envelope_evidence.clone().unwrap();
        evidence.checkpoint.object = "evidence/missing.json".into();
        next.envelope_evidence = Some(evidence);
        store
            .put(
                &Path::from(format!("{root}/publications/pub-000043.json")),
                write_contract(&next).into(),
            )
            .await
            .unwrap();
        let mut moved = head.clone();
        let pointer = moved.content.as_mut().unwrap();
        pointer.publication_id = next.publication_id.clone();
        pointer.sequence = next.sequence;
        pointer.manifest = "publications/pub-000043.json".into();
        let evidence = published_evidence(&config, &moved).await.unwrap();
        let expected: BTreeSet<_> = applied
            .into_iter()
            .chain(ids('c', 2))
            .chain(manifest.envelope_ids)
            .chain(ids('f', 1))
            .collect();
        assert_eq!(evidence.applied, expected);
    }

    /// A checkpoint maintenance removed is replaced by the older manifests; a
    /// checkpoint that is not the one named stops the read.
    #[tokio::test]
    async fn a_missing_checkpoint_is_read_from_the_manifests_and_a_wrong_one_is_refused() {
        let (config, head, manifest, _) = namespace(3, 2).await;
        let (store, root) = namespace_store(&config).unwrap();
        let pointer = manifest
            .envelope_evidence
            .as_ref()
            .unwrap()
            .checkpoint
            .clone();
        let mut older = manifest.clone();
        older.publication_id = "pub-000041".into();
        older.sequence = manifest.sequence - 1;
        older.previous_publication_id = None;
        older.envelope_ids = ids('9', 2);
        older.envelope_evidence = None;
        store
            .put(
                &Path::from(format!("{root}/publications/pub-000041.json")),
                write_contract(&older).into(),
            )
            .await
            .unwrap();
        let object = Path::from(format!("{root}/{}", pointer.object));
        store.put(&object, "{}".into()).await.unwrap();
        assert_eq!(
            published_evidence(&config, &head).await.err(),
            Some(WriterError::PublicationInvalid)
        );
        store.delete(&object).await.unwrap();
        let evidence = published_evidence(&config, &head).await.unwrap();
        let expected: BTreeSet<_> = manifest
            .envelope_ids
            .into_iter()
            .chain(ids('9', 2))
            .collect();
        assert_eq!(evidence.applied, expected);
    }
}
