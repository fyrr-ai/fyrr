//! Source preparation under the cursor lease.
use super::driver::{ErrMap, Request, RowPolicy, drive_provider};
use super::host::Host;
use super::*;
use crate::batch::KindStats;
use crate::embedding::WriterEmbedding;
use arrow_array::RecordBatch;
use callbacks::RevisionCallbacks;
use futures::StreamExt;
use fyrr_core::connector::{ConnectorBatch, CursorState};
use fyrr_core::envelope::{
    BaseHead, DocumentIntent, EnvelopeDocument, EnvelopePayload, PreparedEnvelope,
};
use fyrr_core::publication::ServingHead;
use object_store::{ObjectStore, path::Path};
use revisions::{
    PendingRevisions, RevisionStream, RevisionView, lookup_revisions, revision_views, source_counts,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

mod callbacks;
mod content;
mod revisions;
mod state;
pub(super) use state::CommandEvidence;

struct Preparation<'a> {
    config: &'a fyrr_core::config::Config,
    source: &'a fyrr_core::config::SourceConfig,
    kind: SyncKind,
    rt: &'a std::sync::Arc<tokio::runtime::Runtime>,
    lease: &'a std::sync::Arc<crate::sync_lease::CursorLease>,
    credentials: &'a Credentials,
}

#[allow(clippy::print_stdout, clippy::too_many_arguments)]
pub(super) fn prepared_kind(
    loaded: &fyrr_core::config::LoadedConfig,
    source_id: &str,
    kind: fyrr_core::connector::SyncKind,
    full: bool,
    due: bool,
    permission_stream: bool,
    evidence: &mut CommandEvidence,
    credentials: &Credentials,
) -> Result<serde_json::Value> {
    let config = loaded.config.clone();
    let source = config
        .sources
        .iter()
        .find(|s| s.id == source_id)
        .ok_or(WriterError::SourceInvalid)?
        .clone();
    let rt = Arc::new(tokio::runtime::Runtime::new().map_err(|_| WriterError::RuntimeUnavailable)?);
    // A retired source takes no cursor lease and prepares nothing;
    // the committer rejects what a worker that raced the retirement wrote.
    if rt.block_on(crate::source::retired(&config, &source.id))? {
        return Err(WriterError::SourceRetired);
    }
    let mut embedding = None;
    let result = crate::sync_lease::CursorLease::with(&rt, &config, &source.id, kind, |lease| {
        let preparation = Preparation {
            config: &config,
            source: &source,
            kind,
            rt: &rt,
            lease,
            credentials,
        };
        let head = rt
            .block_on(fyrr_reader::storage::optional_head(&config))?
            .ok_or(WriterError::NamespaceUnavailable)?;
        let mut pending = preparation.scan(evidence, &head)?;
        let dataset = if head.content.is_some() {
            Some(Arc::new(
                rt.block_on(fyrr_reader::storage::pinned(&config, &head))?,
            ))
        } else {
            None
        };
        if kind == SyncKind::Content
            && let Some(content) = &head.content
        {
            let (store, root) = fyrr_reader::storage::namespace_store(&config)?;
            let manifest = rt.block_on(fyrr_reader::storage::read_manifest(
                store.as_ref(),
                &root,
                &content.manifest,
            ))?;
            if manifest.embedding != fyrr_reader::embedding::identity(&config.embedding) {
                return Err(WriterError::EmbeddingProfileMismatch);
            }
            if manifest.chunk_profile != crate::prepare::chunk_identity() {
                return Err(WriterError::ChunkProfileMismatch);
            }
        }
        let base = head.content.as_ref().map(|p| BaseHead {
            content_publication_id: p.publication_id.clone(),
            content_sequence: p.sequence,
            lance_version: p.lance_version,
        });
        let (mut state, skip_not_due) = preparation.cursor_state(&pending, due)?;
        if skip_not_due {
            return Ok(default_report(ReportDefaults::NotDue(kind)));
        }
        preparation.backpressure(&pending, full)?;
        if full {
            state.cursor = None;
        }
        // Jira's visibility freshness barrier compares with the served content,
        // never a replacement that is merely staged. Content and reconcile keep
        // the effective pending view; other providers retain their existing view.
        let published_jira_acl =
            source.provider == "jira" && matches!(kind, SyncKind::Permissions | SyncKind::Members);
        let revision_paths = Arc::new(if published_jira_acl {
            Vec::new()
        } else {
            std::mem::take(&mut pending.content_paths)
        });
        let revisions = Arc::new(PendingRevisions::new(revision_paths, source.id.clone()));
        let revision_batches = preparation.revision_stream(&dataset, &revisions, &pending.store)?;
        let callback_failure = Arc::new(Mutex::new(None));
        let stats = preparation.read(
            &mut state,
            &base,
            &pending,
            RevisionView {
                dataset: &dataset,
                pending: revisions,
                revisions: revision_batches,
                failure: &callback_failure,
            },
            &mut embedding,
            permission_stream,
        )?;
        let Some(cache) = &embedding else {
            return Ok(default_report(ReportDefaults::Kind { kind, stats }));
        };
        state.updated_at = fyrr_core::clock::now();
        rt.block_on(lease.write(state.clone()))?;
        Ok(serde_json::json!({
            "kind": kind,
            "stats": stats,
            "cache_hits": cache.hits,
            "cache_misses": cache.misses,
            "paid_calls": cache.stats.embedding_calls,
            "provider_calls": cache.stats.calls,
            "reserved_microusd": cache.stats.reserved_microusd
        }))
    });
    let result = match result {
        Ok(value) => Ok(value),
        Err(crate::sync_lease::ScopeError::Operation(code)) => Err(code),
        Err(crate::sync_lease::ScopeError::Lease(code)) => return Err(code),
    };
    match (result, embedding) {
        (Err(code), Some(cache)) => Ok(serde_json::json!({"kind":kind,"error":code,
            "paid_calls":cache.stats.embedding_calls,"provider_calls":cache.stats.calls,
            "reserved_microusd":cache.stats.reserved_microusd})),
        (result, _) => result,
    }
}

impl Preparation<'_> {
    fn read(
        &self,
        state: &mut CursorState,
        base: &Option<BaseHead>,
        pending: &state::Pending,
        view: RevisionView<'_>,
        embedding: &mut Option<WriterEmbedding>,
        permission_stream: bool,
    ) -> Result<KindStats> {
        let dataset = view.dataset;
        let mut content = if self.kind == SyncKind::Content {
            let cache = embedding.insert(
                self.rt
                    .block_on(WriterEmbedding::open(self.config, &self.source.id))?,
            );
            self.rt.block_on(cache.start())?;
            Some((cache, self.row_budget(dataset, pending)?))
        } else {
            None
        };
        let (policy, errors) = if content.is_some() {
            (RowPolicy::Required, ErrMap::CONTENT)
        } else {
            (RowPolicy::Forbidden, ErrMap::NON_CONTENT)
        };
        let revisions = view.pending.clone();
        let callback_failure = view.failure;
        let outcome = (|| -> Result<KindStats> {
            let mut host =
                Host::start(self.credentials).map_err(|_| WriterError::ProviderFailure)?;
            let mut callbacks = RevisionCallbacks::new(self.config, self.rt, view);
            let limits = crate::batch::Limits::for_source(self.config, self.source);
            let source_json = crate::sync::source_json(self.source, limits);
            let mut checker =
                BatchChecker::new(&self.source.id, self.kind, limits, state.cursor.clone());
            let cursor = state.cursor.clone();
            let request = Request {
                source_json: &source_json,
                kind: self.kind,
                cursor: cursor.as_deref(),
            };
            let run = drive_provider(
                &mut host,
                &request,
                Some(&mut callbacks),
                &mut checker,
                policy,
                errors,
                |header, rows, stats| {
                    let batch: ConnectorBatch = fyrr_core::parse_contract(header.as_bytes())
                        .map_err(|_| WriterError::BatchInvalid)?;
                    if let Some((cache, budget)) = &mut content {
                        let rows = rows.ok_or(WriterError::BatchInvalid)?;
                        self.prepare_batch(
                            &batch,
                            rows,
                            stats,
                            state,
                            base,
                            dataset,
                            &revisions,
                            cache,
                            permission_stream,
                            budget,
                        )
                    } else {
                        self.rt.block_on(crate::prepare::non_content(
                            self.config,
                            &self.source.id,
                            &batch,
                            state,
                            self.lease,
                            base.clone(),
                            false,
                        ))
                    }
                },
            )?;
            // A caught callback failure still forbids a complete-read attestation.
            if let Some(code) = *callback_failure
                .lock()
                .map_err(|_| WriterError::RevisionViewInvalid)?
            {
                return Err(code);
            }
            if self.kind == SyncKind::Members
                && ((checker.stats.batches > 0 && !checker.stats.members_complete)
                    || (checker.stats.batches == 0
                        && pending.members_tail.is_some_and(|(_, last)| !last)))
            {
                return Err(WriterError::MembershipSnapshotIncomplete);
            }
            if self.kind == SyncKind::Permissions || self.kind == SyncKind::Members {
                let batch = ConnectorBatch {
                    schema_version: fyrr_core::connector::CONNECTOR_CONTRACT_VERSION,
                    source_id: self.source.id.clone(),
                    kind: self.kind,
                    cursor: fyrr_core::connector::CursorStep {
                        start: state.cursor.clone(),
                        end: state.cursor.clone().unwrap_or_else(|| "attestation".into()),
                    },
                    body: if self.kind == SyncKind::Permissions {
                        fyrr_core::connector::BatchBody::PermissionChanges {
                            documents: Vec::new(),
                            container_grants: Vec::new(),
                        }
                    } else {
                        fyrr_core::connector::BatchBody::PrincipalSnapshot(
                            fyrr_core::connector::PrincipalSnapshotPart {
                                snapshot_id: format!("attest-{}", state.sequence),
                                part: 0,
                                last: false,
                                users: Vec::new(),
                                groups: Vec::new(),
                            },
                        )
                    },
                };
                self.rt.block_on(crate::prepare::non_content(
                    self.config,
                    &self.source.id,
                    &batch,
                    state,
                    self.lease,
                    base.clone(),
                    true,
                ))?;
            }
            run.finish();
            Ok(checker.stats)
        })();
        if let Some(code) = *callback_failure
            .lock()
            .map_err(|_| WriterError::RevisionViewInvalid)?
        {
            return Err(code);
        }
        outcome
    }
}
