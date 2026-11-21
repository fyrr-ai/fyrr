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

    pub async fn acquire(config: &Config, source: &str, kind: SyncKind) -> Result<Arc<Self>> {
        let (store, root) = namespace_store(config)?;
        crate::commit::sources_allowed(config, store.as_ref(), &root).await?;
        let path = cursor_path(&root, source, kind);
        let (mut state, mode) =
            match Self::read(config, store.as_ref(), &path, source, kind).await? {
                Some((state, version)) => (state, PutMode::Update(version)),
                None => (
                    crate::prepare::initial_cursor(config, source, kind),
                    PutMode::Create,
                ),
            };
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
        state.schema_version = fyrr_core::connector::CONNECTOR_CONTRACT_VERSION;
        let result = cas_put(store.as_ref(), &path, &state, mode)
            .await
            .map_err(|e| match e {
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. } => WriterError::SyncBusy,
                _ => WriterError::StorageUnavailable,
            })?;
        let lease = Arc::new(Self {
            observed: fyrr_reader::storage::observed_durability(config)?,
            store: store.clone(),
            path,
            held: Mutex::new((state, result, Instant::now())),
            ttl,
        });
        if let Err(error) = crate::commit::sources_allowed(config, store.as_ref(), &root).await {
            let _ = lease.release().await;
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
        state.schema_version = fyrr_core::connector::CONNECTOR_CONTRACT_VERSION;
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

    pub(crate) async fn renewal_finishes_before_release(config: &Config) {
        let mut lease = CursorLease::acquire(config, "renewal", SyncKind::Content)
            .await
            .unwrap();
        let (store, entered, resume) = crate::lease::tests::delayed_store(lease.store.clone());
        Arc::get_mut(&mut lease).unwrap().store = store;
        let renewal = lease.keepalive();
        crate::lease::tests::finish_held_renewal(renewal, entered, resume).await;
        lease.release().await.unwrap();
        let next = CursorLease::acquire(config, "renewal", SyncKind::Content)
            .await
            .expect("the completed renewal must not leave the cursor leased");
        assert!(next.state().await.unwrap().lease.unwrap().epoch > 1);
        next.release().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cursor_renewal_finishes_before_release_on_local_storage() {
        let (config, root) = crate::lease::tests::local_config("cursor-renewal");
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
        for (index, (operation_failed, release_failed)) in
            [(false, false), (true, false), (false, true), (true, true)]
                .into_iter()
                .enumerate()
        {
            let result = CursorLease::with(
                &rt,
                &config,
                &format!("source-{index}"),
                SyncKind::Content,
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
