//! Local resource ownership derived from a pinned publication, never visibility state.
use crate::error::*;
use fyrr_core::{config::Config, publication::ContentManifest};
use std::sync::Arc;

/// Memory pools count 4 KiB pages.
pub(crate) const PAGE_BYTES: u64 = 4096;

/// Pages covering `bytes`. Counts beyond `u32::MAX` saturate instead of
/// wrapping to a smaller claim; request paths independently bound their inputs.
pub(crate) fn pages(bytes: usize) -> u32 {
    u32::try_from((bytes as u64).div_ceil(PAGE_BYTES)).unwrap_or(u32::MAX)
}

/// The preparation envelope and the permits that account for its owned bytes.
#[derive(Debug, Clone)]
pub(crate) struct MemoryPool {
    pub(crate) cap: u64,
    pub(crate) permits: Arc<tokio::sync::Semaphore>,
}
impl MemoryPool {
    pub(crate) fn try_claim(&self, bytes: u64) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let pages = u32::try_from(bytes.div_ceil(PAGE_BYTES)).ok()?;
        self.permits.clone().try_acquire_many_owned(pages).ok()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum GenerationState {
    Cold,
    Serving,
    WarmReady,
}

/// Keep the reservation until all generation workers release it.
/// Reservations govern preparation allocations, not publication or authorization.
#[derive(Debug)]
pub(crate) struct PreparationReservation {
    permit: std::sync::Mutex<tokio::sync::OwnedSemaphorePermit>,
    warm: std::sync::atomic::AtomicBool,
    estimate_valid: std::sync::atomic::AtomicBool,
    cold_bytes: u64,
    scratch_bytes: u64,
    warm_bytes: u64,
    // A same-publication cold-to-warm upgrade shares the existing minimum
    // claim. Old cold requests retain it without charging that minimum twice.
    base: Option<Arc<Self>>,
}
/// One native session and the permits that pay for its cache capacity.
/// Generations/workers own this resource; preparation keeps only weak references.
/// It derives capacity from a manifest and cannot decide publication visibility.
#[derive(Debug)]
pub(crate) struct NativeSession {
    pub(crate) session: Arc<lance::session::Session>,
    pub(crate) capacity: (u64, u64),
    permit: tokio::sync::OwnedSemaphorePermit,
}
impl NativeSession {
    pub(crate) fn acquire(
        pool: MemoryPool,
        needed: (u64, u64),
        warm_bytes: u64,
        existing: Option<Arc<Self>>,
    ) -> Result<Arc<Self>> {
        if let Some(session) = &existing
            && session.capacity.0 >= needed.0
            && session.capacity.1 >= needed.1
        {
            return Ok(session.clone());
        }
        // Claims precede caches. Leave one full next-generation warm claim free;
        // settlement is never credited before its permits actually return.
        let reserve = warm_bytes.div_ceil(PAGE_BYTES).saturating_mul(PAGE_BYTES);
        let available =
            (pool.permits.available_permits() as u64 * PAGE_BYTES).saturating_sub(reserve);
        let capacity = Self::allocation(needed, available);
        if let Some(session) = existing
            && (capacity.0 < needed.0 || capacity.1 < needed.1)
        {
            let alone =
                Self::allocation(needed, pool.cap.saturating_sub(reserve.saturating_mul(2)));
            if alone.0 >= needed.0 && alone.1 >= needed.1 {
                return Err(PreparationBudgetExhausted);
            }
            // Draining cannot cure this shortfall. Keep the indexed session.
            return Ok(session);
        }
        let total = capacity.0.saturating_add(capacity.1);
        let permit = pool.try_claim(total).ok_or(PreparationBudgetExhausted)?;
        Ok(Arc::new(Self {
            session: Arc::new(lance::session::Session::new(
                usize::try_from(capacity.0).map_err(|_| PreparationBudgetExhausted)?,
                usize::try_from(capacity.1).map_err(|_| PreparationBudgetExhausted)?,
                Arc::default(),
            )),
            capacity,
            permit,
        }))
    }
    fn allocation(needed: (u64, u64), available: u64) -> (u64, u64) {
        let needed = (
            needed.0.div_ceil(PAGE_BYTES) * PAGE_BYTES,
            needed.1.div_ceil(PAGE_BYTES) * PAGE_BYTES,
        );
        let grow = |n: u64| n.saturating_mul(5).div_ceil(4).div_ceil(PAGE_BYTES) * PAGE_BYTES;
        let desired = (grow(needed.0), grow(needed.1));
        let total = desired
            .0
            .saturating_add(desired.1)
            .min(available / PAGE_BYTES * PAGE_BYTES);
        let minimum = needed.0.saturating_add(needed.1);
        let (base, weights) = if total >= minimum {
            (needed, (desired.0 - needed.0, desired.1 - needed.1))
        } else {
            ((0, 0), needed)
        };
        let extra = total - base.0 - base.1;
        let weight = weights.0.saturating_add(weights.1);
        let metadata = if weight == 0 {
            0
        } else {
            let Ok(share) =
                u64::try_from(u128::from(extra) * u128::from(weights.1) / u128::from(weight))
            else {
                unreachable!("a weighted share cannot exceed its u64 budget");
            };
            share / PAGE_BYTES * PAGE_BYTES
        };
        (base.0 + extra - metadata, base.1 + metadata)
    }
    pub(crate) fn bytes(&self) -> u64 {
        self.permit.num_permits() as u64 * PAGE_BYTES
    }
}
impl PreparationReservation {
    pub(crate) fn minimum_bytes(startup: u64, warm: u64, cache: (u64, u64)) -> u64 {
        let claim = |bytes: u64| bytes.div_ceil(PAGE_BYTES).saturating_mul(PAGE_BYTES);
        startup
            .saturating_add(claim(warm).saturating_mul(2))
            .saturating_add(claim(cache.0))
            .saturating_add(claim(cache.1))
    }
    // Measured occupancy relative to manifest index bytes: indexes = 0.857;
    // metadata = 0.490. Include 25% margin, rounded
    // upward to hundredths (1.08 and 0.62). Round capacity to 64 MiB so
    // small incremental publications can share an owned cache envelope.
    pub(crate) fn cache_bytes(manifest: &ContentManifest) -> (u64, u64) {
        let bytes = manifest
            .files
            .iter()
            .filter(|f| f.path.starts_with("_indices/"))
            .fold(0u64, |sum, f| sum.saturating_add(f.size_bytes));
        let quantum = 64 * 1024 * 1024;
        let capacity = |factor: u64| {
            bytes
                .saturating_mul(factor)
                .div_ceil(100)
                .div_ceil(quantum)
                .saturating_mul(quantum)
        };
        (capacity(108), capacity(62).max(48 * 1024 * 1024))
    }
    pub(crate) fn cold_bytes(config: &Config, rows: u64) -> u64 {
        rows.saturating_mul(64)
            .saturating_mul(Self::worker_count(config) as u64 + 1)
            .saturating_add(32 * 1024 * 1024)
    }
    /// The transport admits at most twice the native concurrency cap. Live
    /// embedding may park those workers while their native permits are free.
    pub(crate) fn worker_count(config: &Config) -> usize {
        config.reader.concurrency * (1 + usize::from(config.embedding.reader_live))
    }
    pub(crate) fn pool(config: &Config) -> Result<MemoryPool> {
        let available = config
            .reader
            .memory_max_bytes
            .as_u64()
            .saturating_sub(config.reader.memory_headroom_bytes.as_u64())
            .saturating_sub(crate::search::AUTHORIZATION_CACHE_MAX_BYTES as u64)
            .saturating_sub(Self::rss()?);
        let permits = (available / PAGE_BYTES).min(u32::MAX as u64) as usize;
        Ok(MemoryPool {
            cap: permits as u64 * PAGE_BYTES,
            permits: Arc::new(tokio::sync::Semaphore::new(permits)),
        })
    }
    pub(crate) fn rss() -> Result<u64> {
        let status =
            std::fs::read_to_string("/proc/self/status").map_err(|_| StorageUnavailable)?;
        status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .and_then(|value| value.split_whitespace().next()?.parse::<u64>().ok())
            .and_then(|kib| kib.checked_mul(1024))
            .ok_or(StorageUnavailable)
    }
    pub(crate) fn warm_estimate(
        config: &Config,
        manifest: &ContentManifest,
        rows: u64,
    ) -> (u64, u64) {
        // Admission bitmaps and worst-case unindexed row sets for the template
        // and workers. The subject cache is charged once in the shared envelope.
        let cold = Self::cold_bytes(config, rows);
        // Native caches receive a separately owned claim after generation admission.
        // Preparation additionally
        // allows decoded index input and transient partition buffers; large
        // rewrites deliberately choose cold rather than speculate about fit.
        let index_bytes = manifest
            .files
            .iter()
            .filter(|f| {
                f.path
                    .strip_prefix("_indices/")
                    .and_then(|path| path.split_once('/'))
                    .is_some_and(|(uuid, _)| {
                        manifest.indexes.iter().any(|index| {
                            index.name == fyrr_core::layout::NamespaceLayout::VECTOR_INDEX
                                && index.uuid == uuid
                        })
                    })
            })
            .fold(0u64, |sum, f| sum.saturating_add(f.size_bytes));
        // Lance's native RaBitQ root owns a scratch pool per compute thread,
        // including dimension-sized lookup buffers even for tiny delta indexes.
        // Bound variable partition scratch by the encoded index input and add
        // fixed scratch per vector UUID before opening any of those roots.
        let cpus = lance_core::utils::tokio::get_num_compute_intensive_cpus() as u64;
        let dim = u64::from(config.embedding.dimensions);
        let padded_dim = dim.div_ceil(64) * 64;
        let roots = manifest
            .indexes
            .iter()
            .filter(|i| i.name == fyrr_core::layout::NamespaceLayout::VECTOR_INDEX)
            .count() as u64;
        let scratch = padded_dim
            .saturating_mul(128)
            .saturating_mul(cpus)
            .saturating_mul(roots);
        let input_factor = 4.max(cpus.saturating_mul(48).div_ceil(dim));
        let warm = cold
            .saturating_add(index_bytes.saturating_mul(input_factor))
            .saturating_add(scratch);
        (warm, scratch)
    }
    pub(crate) fn acquire(
        config: &Config,
        manifest: &ContentManifest,
        rows: u64,
        pool: Arc<tokio::sync::Semaphore>,
    ) -> Result<Arc<Self>> {
        let cold = Self::cold_bytes(config, rows);
        let (warm_bytes, scratch) = Self::warm_estimate(config, manifest, rows);
        for (bytes, warm) in [(warm_bytes, true), (cold, false)] {
            let Ok(permits) = u32::try_from(bytes.div_ceil(PAGE_BYTES)) else {
                continue;
            };
            // The pool is fixed at startup. Allocator retention must not
            // change admission of an otherwise identical generation claim.
            if let Ok(permit) = pool.clone().try_acquire_many_owned(permits) {
                if !warm {
                    Self::degraded(bytes, config.reader.memory_max_bytes.as_u64());
                }
                return Ok(Arc::new(Self {
                    permit: std::sync::Mutex::new(permit),
                    warm: std::sync::atomic::AtomicBool::new(warm),
                    estimate_valid: std::sync::atomic::AtomicBool::new(true),
                    cold_bytes: cold,
                    scratch_bytes: scratch,
                    warm_bytes,
                    base: None,
                }));
            }
        }
        Self::degraded(cold, config.reader.memory_max_bytes.as_u64());
        Err(PreparationBudgetExhausted)
    }
    pub(crate) fn warm(&self) -> bool {
        self.warm.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(crate) fn warm_bytes(&self) -> u64 {
        self.warm_bytes
            .div_ceil(PAGE_BYTES)
            .saturating_mul(PAGE_BYTES)
    }
    pub(crate) fn bytes(&self) -> u64 {
        crate::sync::lock(&self.permit).num_permits() as u64 * PAGE_BYTES
            + self.base.as_ref().map_or(0, |base| base.bytes())
    }
    pub(crate) fn upgrade(
        self: &Arc<Self>,
        pool: Arc<tokio::sync::Semaphore>,
    ) -> Result<Arc<Self>> {
        if !self
            .estimate_valid
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(PreparationBudgetExhausted);
        }
        if self.warm() {
            return Ok(self.clone());
        }
        let additional = u32::try_from(
            self.warm_bytes
                .saturating_sub(self.bytes())
                .div_ceil(PAGE_BYTES),
        )
        .map_err(|_| PreparationBudgetExhausted)?;
        let permit = pool
            .try_acquire_many_owned(additional)
            .map_err(|_| PreparationBudgetExhausted)?;
        Ok(Arc::new(Self {
            permit: std::sync::Mutex::new(permit),
            warm: std::sync::atomic::AtomicBool::new(true),
            estimate_valid: std::sync::atomic::AtomicBool::new(true),
            cold_bytes: self.cold_bytes,
            scratch_bytes: self.scratch_bytes,
            warm_bytes: self.warm_bytes,
            base: Some(self.clone()),
        }))
    }
    /// Release decoded-input space only once publication-bound roots exist.
    /// Workers and subjects reuse those roots; their private buffers remain
    /// charged by the cold envelope and the independent subject-cache bound.
    // Bounded numeric accounting at the loss site.
    pub(crate) fn settle(&self, roots: u64) -> Result<bool> {
        let mut permit = self.permit.lock().map_err(|_| StorageUnavailable)?;
        let required = self.cold_bytes.saturating_add(roots).div_ceil(PAGE_BYTES);
        let claimed = permit.num_permits();
        let base = self
            .base
            .as_ref()
            .map_or(0, |base| base.bytes() / PAGE_BYTES);
        if required > claimed as u64 + base {
            self.estimate_valid
                .store(false, std::sync::atomic::Ordering::Release);
            if self.warm.swap(false, std::sync::atomic::Ordering::AcqRel) {
                crate::telemetry::emit(format_args!(
                    "telemetry generation_preparation=degraded reason=reservation_estimate \
                        roots_bytes={roots} cold_bytes={} reserved_bytes={}",
                    self.cold_bytes,
                    (claimed as u64 + base) * PAGE_BYTES
                ));
            }
            return Ok(false);
        }
        // Retain additional scratch growth allowance, capped by the original
        // envelope. This never borrows extra permits after allocating roots.
        let retained = self
            .cold_bytes
            .saturating_add(roots)
            .saturating_add(self.scratch_bytes)
            .div_ceil(PAGE_BYTES)
            .saturating_sub(base)
            .min(claimed as u64);
        let retained = usize::try_from(retained).map_err(|_| StorageUnavailable)?;
        drop(permit.split(claimed - retained));
        Ok(true)
    }
    // Fixed telemetry loss reason and numeric dimensions only.
    fn degraded(reserved_bytes: u64, budget_bytes: u64) {
        let rss_bytes = Self::rss().unwrap_or(0);
        crate::telemetry::emit(format_args!(
            "telemetry generation_preparation=degraded reason=memory_budget \
                reserved_bytes={reserved_bytes} budget_bytes={budget_bytes} \
                rss_bytes={rss_bytes}"
        ));
    }
}

#[cfg(test)]
#[path = "generation/tests.rs"]
mod tests;
