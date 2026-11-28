//! Per-source/per-kind cursor CAS: the lease fences cursor writes, not content publication.
use crate::lease::{cas_put, new_lease, version_of};
use crate::{Result, WriterError};
use fyrr_core::config::Config;
use fyrr_core::connector::{CursorState, SyncKind};
use fyrr_core::parse_contract;
use fyrr_reader::storage::namespace_store;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, UpdateVersion, path::Path};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Keep lease failures separate from operation errors with paid-call accounting.
#[derive(Debug, PartialEq)]
pub(crate) enum ScopeError<E> {
    Lease(WriterError),
    Operation(E),
}

#[derive(Debug)]
pub struct CursorLease {
    store: Arc<dyn ObjectStore>,
    path: Path,
    held: Mutex<(CursorState, UpdateVersion, Instant)>,
    ttl: Duration,
    observed: fyrr_reader::storage::ObservedDurability,
}
fn cursor_path(root: &Path, source: &str, kind: SyncKind) -> Path {
    root.clone()
        .join("cursors")
        .join(format!("{source}-{}.json", kind.as_str()))
}

impl CursorLease {
    pub(crate) fn with<T, E>(
        rt: &tokio::runtime::Runtime,
        config: &Config,
        source: &str,
        kind: SyncKind,
        body: impl FnOnce(&Arc<Self>) -> Result<T, E>,
    ) -> Result<T, ScopeError<E>> {
        let lease = rt
            .block_on(Self::acquire(config, source, kind))
            .map_err(ScopeError::Lease)?;
        let renewal = {
            let _entered = rt.enter();
            lease.keepalive()
        };
        let result = body(&lease);
        let release = rt.block_on(async {
            renewal.stop().await?;
            lease.release().await
        });
        if result.is_ok() {
            release.map_err(ScopeError::Lease)?;
        }
        result.map_err(ScopeError::Operation)
    }

    fn live(&self, held: &(CursorState, UpdateVersion, Instant)) -> bool {
        held.2.elapsed() < self.ttl
            && held
                .0
                .lease
                .as_ref()
                .is_some_and(|l| l.expires_at > fyrr_core::clock::now())
    }

    /// The stored cursor and its version; `None` before the first read.
    async fn read(
        config: &Config,
        store: &dyn ObjectStore,
        path: &Path,
        source: &str,
        kind: SyncKind,
    ) -> Result<Option<(CursorState, UpdateVersion)>> {
        let object = match store.get(path).await {
            Ok(object) => object,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(_) => return Err(WriterError::StorageUnavailable),
        };
        let version = version_of(&object.meta);
        let state = parse_contract::<CursorState>(&object.bytes().await?)
            .map_err(|_| WriterError::CursorInvalid)?;
        if state.namespace != config.namespace.name
            || state.source_id != source
            || state.kind != kind
        {
            return Err(WriterError::CursorInvalid);
        }
        Ok(Some((state, version)))
    }

    /// The stored cursor, read without the lease: a schedule decision that
    /// takes the lease only when it finds the kind possibly due.
    pub(crate) async fn peek(
        config: &Config,
        source: &str,
        kind: SyncKind,
    ) -> Result<Option<CursorState>> {
        let (store, root) = namespace_store(config)?;
        let path = cursor_path(&root, source, kind);
        Ok(Self::read(config, store.as_ref(), &path, source, kind)
            .await?
            .map(|(state, _)| state))
    }

    /// The new permissions cursor of `source`, whose cursor was not there. One
    /// whose chain is left is lost: a new one would confirm a chain without
    /// it, and the source stays blocked until it is purged. A chain that a
    /// concurrent first read left and confirmed meanwhile is not: its cursor is
    /// there now, and this read is `sync_busy`. A new one follows every lease
    /// generation of the log.
    pub(crate) async fn new_permissions_cursor(
        config: &Config,
        store: &dyn ObjectStore,
        path: &Path,
        source: &str,
    ) -> Result<CursorState> {
        let kind = SyncKind::Permissions;
        let crate::commit::ChainLog {
            left, generation, ..
        } = crate::commit::chain_log(config, source, kind).await?;
        if left {
            return Err(
                if Self::read(config, store, path, source, kind)
                    .await?
                    .is_some()
                {
                    WriterError::SyncBusy
                } else {
                    WriterError::PermissionCursorLost
                },
            );
        }
        let mut state = crate::prepare::initial_cursor(config, source, kind);
        state.lease = (generation > 0).then(|| fyrr_core::publication::Lease {
            holder: String::new(),
            epoch: generation,
            expires_at: chrono::DateTime::UNIX_EPOCH,
        });
        Ok(state)
    }

    /// The new content cursor of `source`, whose cursor was not there, and the
    /// namespace lease it is written under when it replaces a lost one. A
    /// source with nothing in the log starts new. Otherwise the new cursor
    /// confirms what the publication applied and nothing after it, read under
    /// the namespace lease so that no publication moves it meanwhile: the
    /// envelopes a worker left unconfirmed are dead, and the connector reads
    /// from the start again, which content may. It follows every lease
    /// generation and sequence of the log, and a rejection generation above
    /// every one of the log and its markers, so that no marker rewinds the
    /// read it starts.
    pub(crate) async fn new_content_cursor(
        config: &Config,
        source: &str,
    ) -> Result<(CursorState, Option<Arc<crate::lease::LeaseGuard>>)> {
        let kind = SyncKind::Content;
        let mut state = crate::prepare::initial_cursor(config, source, kind);
        let log = crate::commit::chain_log(config, source, kind).await?;
        if log.sequence == 0 && log.applied.is_none() {
            return Ok((state, None));
        }
        let guard = crate::lease::LeaseGuard::acquire(config).await?;
        let built = async {
            let log = crate::commit::chain_log(config, source, kind).await?;
            let markers = crate::commit::rejection_markers(config, &guard.head().await?).await?;
            let markers = markers
                .iter()
                .filter(|m| m.source_id == source && m.kind == kind);
            state.sequence = log.sequence;
            state.rejection_generation = markers
                .map(|m| m.rejection_generation)
                .fold(log.rejection_generation, u32::max)
                .checked_add(1)
                .ok_or(WriterError::RejectionGenerationOverflow)?;
            state.last = log.applied;
            state.lease = (log.generation > 0).then(|| fyrr_core::publication::Lease {
                holder: String::new(),
                epoch: log.generation,
                expires_at: chrono::DateTime::UNIX_EPOCH,
            });
            Ok(state)
        }
        .await;
        match built {
            Ok(state) => Ok((state, Some(guard))),
            Err(error) => {
                let _ = guard.release().await;
                Err(error)
            }
        }
    }

    pub async fn acquire(config: &Config, source: &str, kind: SyncKind) -> Result<Arc<Self>> {
        let (store, root) = namespace_store(config)?;
        crate::commit::sources_allowed(config, store.as_ref(), &root).await?;
        let path = cursor_path(&root, source, kind);
        let mut namespace = None;
        let (mut state, mode) =
            match Self::read(config, store.as_ref(), &path, source, kind).await? {
                Some((state, version)) => (state, PutMode::Update(version)),
                None if kind == SyncKind::Permissions => (
                    Self::new_permissions_cursor(config, store.as_ref(), &path, source).await?,
                    PutMode::Create,
                ),
                None if kind == SyncKind::Content => {
                    let (state, guard) = Self::new_content_cursor(config, source).await?;
                    namespace = guard;
                    (state, PutMode::Create)
                }
                None => (
                    crate::prepare::initial_cursor(config, source, kind),
                    PutMode::Create,
                ),
            };
        let created = matches!(mode, PutMode::Create);
        let now = fyrr_core::clock::now();
        if state.lease.as_ref().is_some_and(|l| l.expires_at > now) {
            return Err(WriterError::SyncBusy);
        }
        let ttl = config.writer.commit.lease_ttl.as_duration();
        state.lease = Some(new_lease(
            state.lease.as_ref(),
            now,
            ttl,
            format!("{source}:{}:{}:{now}", kind.as_str(), std::process::id()).as_bytes(),
        )?);
        state.schema_version = fyrr_core::connector::CURSOR_CONTRACT_VERSION;
        let result = cas_put(store.as_ref(), &path, &state, mode)
            .await
            .map_err(|e| match e {
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. } => WriterError::SyncBusy,
                _ => WriterError::StorageUnavailable,
            });
        // A replaced cursor is written before publication may move again.
        if let Some(namespace) = namespace {
            namespace.release().await?;
        }
        let result = result?;
        let lease = Arc::new(Self {
            observed: fyrr_reader::storage::observed_durability(config)?,
            store: store.clone(),
            path,
            held: Mutex::new((state, result.clone(), Instant::now())),
            ttl,
        });
        if let Err(error) = crate::commit::sources_allowed(config, store.as_ref(), &root).await {
            // A cursor created in a namespace that was deleted meanwhile would
            // outlive it and be taken for the state of a new one. Only this
            // cursor goes: a new incarnation may have written its own.
            if created && error == WriterError::NamespaceUnavailable {
                let file = fyrr_reader::storage::relative_path(&lease.path, &root);
                let _ = fyrr_reader::storage::delete_if_match(config, &file, &result).await;
            } else {
                let _ = lease.release().await;
            }
            return Err(error);
        }
        Ok(lease)
    }
    pub async fn state(&self) -> Result<CursorState> {
        let held = self.held.lock().await;
        if !self.live(&held) {
            return Err(WriterError::SyncLeaseLost);
        }
        Ok(held.0.clone())
    }
    /// Before this worker publishes an envelope: the envelope records the
    /// head it was based on and builds on pending envelopes and shared
    /// objects that other processes wrote.
    pub(crate) async fn persist_observed(&self) -> Result<()> {
        Ok(self.observed.persist().await?)
    }
    pub async fn write(&self, mut state: CursorState) -> Result<()> {
        // A cursor advances because an envelope exists, possibly one that an
        // earlier worker published without living to flush its name.
        self.persist_observed().await?;
        let mut held = self.held.lock().await;
        if !self.live(&held) {
            return Err(WriterError::SyncLeaseLost);
        }
        state.schema_version = fyrr_core::connector::CURSOR_CONTRACT_VERSION;
        state.lease = held.0.lease.clone();
        state
            .lease
            .as_mut()
            .unwrap_or_else(|| unreachable!("live cursor lease"))
            .expires_at = fyrr_core::clock::now()
            + chrono::Duration::from_std(self.ttl).map_err(|_| WriterError::LeaseInvalid)?;
        let result = cas_put(
            self.store.as_ref(),
            &self.path,
            &state,
            PutMode::Update(held.1.clone()),
        )
        .await
        .map_err(|_| WriterError::SyncLeaseLost)?;
        *held = (state, result, Instant::now());
        Ok(())
    }
    pub async fn release(&self) -> Result<()> {
        let held = self.held.lock().await;
        if held.2.elapsed() >= self.ttl {
            return Err(WriterError::SyncLeaseLost);
        }
        let mut state = held.0.clone();
        state
            .lease
            .as_mut()
            .ok_or(WriterError::SyncLeaseLost)?
            .expires_at = fyrr_core::clock::now();
        cas_put(
            self.store.as_ref(),
            &self.path,
            &state,
            PutMode::Update(held.1.clone()),
        )
        .await
        .map_err(|_| WriterError::SyncLeaseLost)?;
        Ok(())
    }
    pub(crate) fn keepalive(self: &Arc<Self>) -> crate::lease::Renewal {
        let lease = self.clone();
        crate::lease::Renewal::spawn(self.ttl / 3, move || {
            let lease = lease.clone();
            async move {
                let mut held = lease.held.lock().await;
                if !lease.live(&held) {
                    return false;
                }
                let mut state = held.0.clone();
                let Ok(ttl) = chrono::Duration::from_std(lease.ttl) else {
                    return false;
                };
                state
                    .lease
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("live cursor lease"))
                    .expires_at = fyrr_core::clock::now() + ttl;
                match cas_put(
                    lease.store.as_ref(),
                    &lease.path,
                    &state,
                    PutMode::Update(held.1.clone()),
                )
                .await
                {
                    Ok(result) => *held = (state, result, Instant::now()),
                    Err(_) => {
                        held.0
                            .lease
                            .as_mut()
                            .unwrap_or_else(|| unreachable!("live cursor lease"))
                            .expires_at = fyrr_core::clock::now();
                        return false;
                    }
                }
                true
            }
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Of members: a new content cursor reads the manifest of the head, which
    /// the fixture head does not have.
    pub(crate) async fn renewal_finishes_before_release(config: &Config) {
        let mut lease = CursorLease::acquire(config, "renewal", SyncKind::Members)
            .await
            .unwrap();
        let (store, entered, resume) = crate::lease::tests::delayed_store(lease.store.clone());
        Arc::get_mut(&mut lease).unwrap().store = store;
        let renewal = lease.keepalive();
        crate::lease::tests::finish_held_renewal(renewal, entered, resume).await;
        lease.release().await.unwrap();
        let next = CursorLease::acquire(config, "renewal", SyncKind::Members)
            .await
            .expect("the completed renewal must not leave the cursor leased");
        assert!(next.state().await.unwrap().lease.unwrap().epoch > 1);
        next.release().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cursor_renewal_finishes_before_release_on_local_storage() {
        let (config, root) = crate::lease::tests::local_config("cursor-renewal");
        crate::lease::tests::put_head(&config).await;
        renewal_finishes_before_release(&config).await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[ignore = "real cursor lease and RustFS; scripts/ci"]
    fn scoped_cursor_lease_preserves_error_priority_on_storage() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut config = fyrr_core::config::parse(
            "[namespace]\nname='cursor-scope'\n[storage]\nuri='s3://cursor-scope'\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n",
        ).unwrap().config;
        let root = std::env::temp_dir().join(format!(
            "fyrr-cursor-scope-{}-{}",
            std::process::id(),
            fyrr_core::clock::now().timestamp_nanos_opt().unwrap(),
        ));
        config.storage.uri = format!("file://{}", root.display());
        rt.block_on(crate::lease::tests::put_head(&config));
        for (index, (operation_failed, release_failed)) in
            [(false, false), (true, false), (false, true), (true, true)]
                .into_iter()
                .enumerate()
        {
            let result = CursorLease::with(
                &rt,
                &config,
                &format!("source-{index}"),
                SyncKind::Members,
                |lease| {
                    if release_failed {
                        let mut state = rt.block_on(lease.state()).unwrap();
                        state.cursor = Some("changed".into());
                        rt.block_on(
                            lease
                                .store
                                .put(&lease.path, fyrr_core::write_contract(&state).into()),
                        )
                        .unwrap();
                    }
                    if operation_failed {
                        Err(WriterError::ProviderFailure)
                    } else {
                        Ok(())
                    }
                },
            );
            let expected = if operation_failed {
                Err(ScopeError::Operation(WriterError::ProviderFailure))
            } else if release_failed {
                Err(ScopeError::Lease(WriterError::SyncLeaseLost))
            } else {
                Ok(())
            };
            assert_eq!(
                result, expected,
                "operation_failed={operation_failed}, release_failed={release_failed}"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
