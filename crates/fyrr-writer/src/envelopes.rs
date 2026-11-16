//! Envelope diagnostics and bounded cleanup; the immutable log remains cursor authority.
use crate::{Result, WriterError};
use futures::{StreamExt, TryStreamExt};
use fyrr_core::config::Config;
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
        store,
        root.clone(),
        head.content.as_ref().map(|p| p.manifest.clone()),
    );
    while let Some(publication) = chain.try_next().await? {
        applied.extend(
            publication
                .envelope_ids
                .into_iter()
                .chain(publication.rejected_envelope_ids)
                .filter(|id| ids.contains(id)),
        );
    }
    Ok(applied)
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
    let mut pending = 0;
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
                fyrr_reader::storage::read_contract(store.as_ref(), &path).await?;
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
            pending += usize::from(!resolved);
            if let EnvelopePayload::Content(payload) = &envelope.payload {
                for entry in &payload.quarantined {
                    *quarantine.entry(entry.reason.clone()).or_default() += 1;
                }
            }
        }
    }
    let mut warnings = crate::maintenance::purge_warnings(config, &head).await?;
    // A retired source that the configuration still names: its syncs are
    // refused and its envelopes rejected until it is removed from the file.
    let policy: fyrr_core::publication::PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &namespace_store(config)?.1, &head.policy)
            .await?;
    let retired_configured: Vec<&str> = config
        .sources
        .iter()
        .filter(|source| policy.retired_sources.contains_key(&source.id))
        .map(|source| source.id.as_str())
        .collect();
    if !retired_configured.is_empty() {
        warnings.push("retired_source_configured");
    }
    let (directory_age_ms, directory_warning) = match head.freshness.directory {
        None => (None, Some("directory_freshness_missing")),
        Some(at) => {
            let age = fyrr_core::clock::now()
                .signed_duration_since(at)
                .to_std()
                .unwrap_or_default();
            let deadline = config.identity.validity_deadline.as_duration();
            (
                Some(age.as_millis()),
                if age >= deadline {
                    Some("identity_expired")
                } else if age >= deadline / 2 {
                    Some("identity_expiring")
                } else {
                    None
                },
            )
        }
    };
    let identity: fyrr_core::identity::IdentityArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.identity).await?;
    let directory = serde_json::json!({"provider":config.identity.directory.provider,"identity_generation":head.identity.generation,"principals":identity.principals.len(),"tokens":identity.tokens.len(),"sync_interval_ms":config.identity.directory.sync_interval.as_millis(),"age_ms":directory_age_ms,"warning":directory_warning,"encrypted_inputs":config.identity.encryption_key.is_some()});
    Ok(
        serde_json::json!({"directory":directory,"warnings":warnings,"retired_sources_configured":retired_configured,"content_sequence":head.content.map(|p|p.sequence),"envelopes":envelopes,"pending_envelopes":pending,"quarantine":quarantine,"freshness":head.freshness}),
    )
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
    if !policy
        .retired_sources
        .values()
        .any(|source| source.purged_at.is_some())
    {
        return Ok(0);
    }
    let (paths, _) = crate::commit::envelope_paths(config, &head).await?;
    let (applied, _) = crate::commit::published_ids(config, &head).await?;
    let mut retired = Vec::new();
    for (path, envelope) in crate::commit::read_envelopes(config, &paths).await? {
        if !policy
            .retired_sources
            .get(&envelope.source_id)
            .is_some_and(|source| source.purged_at.is_some())
        {
            continue;
        }
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
    discard_purged(config, lease).await?;
    let head = lease.head().await?;
    let (store, _, durability) = fyrr_reader::storage::cleanup_namespace_store(config)?;
    let crate::commit::CleanupEvidence {
        applied,
        rejected: published_rejected,
        unresolved,
        envelopes,
    } = crate::commit::cleanup_evidence(config, &head).await?;
    let mut latest_applied = BTreeMap::new();
    let mut latest = BTreeMap::new();
    let mut keep_multipart = BTreeSet::new();
    for (_, envelope) in &envelopes {
        latest
            .entry((envelope.source_id.clone(), envelope.kind.as_str()))
            .and_modify(|seq: &mut u64| *seq = (*seq).max(envelope.sequence))
            .or_insert(envelope.sequence);
        if applied.contains(&envelope.envelope_id)
            && crate::apply::complete_read(envelope)
            && !published_rejected.contains(&envelope.envelope_id)
        {
            latest_applied
                .entry((envelope.source_id.clone(), envelope.kind.as_str()))
                .and_modify(|seq: &mut u64| *seq = (*seq).max(envelope.sequence))
                .or_insert(envelope.sequence);
        }
        if !applied.contains(&envelope.envelope_id) {
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
    for (path, envelope) in envelopes {
        if retired.len() as u64 >= config.writer.commit.group_max_rows {
            break;
        }
        let multipart = match &envelope.payload {
            EnvelopePayload::Reconcile(p) => Some(&p.listing_id),
            EnvelopePayload::Members(p) => Some(&p.snapshot_id),
            _ => None,
        };
        if latest.get(&(envelope.source_id.clone(), envelope.kind.as_str()))
            == Some(&envelope.sequence)
            || unresolved.contains(&envelope.envelope_id)
            || latest_applied.get(&(envelope.source_id.clone(), envelope.kind.as_str()))
                == Some(&envelope.sequence)
            || !applied.contains(&envelope.envelope_id)
            || multipart.is_some_and(|id| {
                keep_multipart.contains(&(
                    envelope.source_id.clone(),
                    envelope.kind.as_str(),
                    id.clone(),
                ))
            })
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
        envelope::{QuarantineEntry, envelope_id},
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
        let mut ids = Vec::new();
        for offset in 0..260 {
            let mut envelope = seed.clone();
            envelope.sequence += 1000 + offset;
            envelope.cursor.start = Some(marker.clone());
            envelope.envelope_id = envelope_id(
                &envelope.namespace,
                &envelope.source_id,
                envelope.kind,
                envelope.sequence,
                envelope.rejection_generation,
                envelope.cursor.start.as_deref(),
            );
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
