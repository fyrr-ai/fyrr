//! Process-owned query vectors. Entries and live request copies are charged;
//! cache-only copies use the native generation's existing request scratch.
use super::{EmbeddingClient, ProviderStats, Role, digest, identity};
use crate::error::*;
use fyrr_core::config::EmbeddingConfig;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct Entry {
    vector: Vec<f32>,
    _memory: OwnedSemaphorePermit,
    _capacity: OwnedSemaphorePermit,
}
#[derive(Default)]
struct Cache {
    entries: BTreeMap<String, Entry>,
    order: VecDeque<String>,
}

pub(crate) struct QueryEmbeddings {
    config: EmbeddingConfig,
    profile_fingerprint: String,
    client: EmbeddingClient,
    memory: Arc<Semaphore>,
    capacity: Arc<Semaphore>,
    cache: Mutex<Cache>,
}
impl std::fmt::Debug for QueryEmbeddings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryEmbeddings")
            .finish_non_exhaustive()
    }
}

pub(crate) struct QueryWork {
    pub(crate) vectors: Vec<Result<Vec<f32>>>,
    pub(crate) provider: ProviderStats,
    pub(crate) provider_us: u64,
    keys: Vec<String>,
    _copies: Option<OwnedSemaphorePermit>,
}
impl QueryWork {
    pub(crate) fn degrade(&mut self, code: ReaderError) {
        self.vectors
            .iter_mut()
            .for_each(|vector| *vector = Err(code));
    }
}

impl QueryEmbeddings {
    #[cfg(test)]
    pub(crate) fn new(config: &EmbeddingConfig, memory: Arc<Semaphore>) -> Result<Arc<Self>> {
        Self::charging(config, memory, Arc::default())
    }
    /// A cache whose live calls are charged to `spent`, shared with other caches.
    pub(crate) fn charging(
        config: &EmbeddingConfig,
        memory: Arc<Semaphore>,
        spent: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<Arc<Self>> {
        if config.reader_live && config.spend.is_none() {
            return Err(EmbeddingSpendLimitRequired);
        }
        Ok(Arc::new(Self {
            config: config.clone(),
            profile_fingerprint: identity(config).fingerprint,
            client: EmbeddingClient::new(config, Some(memory.clone()))?.charging(spent),
            memory,
            capacity: Arc::new(Semaphore::new(
                (config.query_cache_max_bytes / crate::generation::PAGE_BYTES) as usize,
            )),
            cache: Mutex::default(),
        }))
    }

    /// Whether this cache embeds with the profile of `config`.
    pub(crate) fn serves(&self, config: &EmbeddingConfig) -> bool {
        self.profile_fingerprint == identity(config).fingerprint
    }

    fn remember(&self, key: &str, vector: &[f32]) {
        if self.config.query_cache_max_bytes < crate::generation::PAGE_BYTES {
            return;
        }
        // Includes vector allocation, hash key, map/deque nodes and their capacity.
        let units = crate::generation::pages(vector.len() * 4 + 512);
        let mut cache = crate::sync::lock(&self.cache);
        if cache.entries.contains_key(key) {
            return;
        }
        loop {
            let claim = self
                .capacity
                .clone()
                .try_acquire_many_owned(units)
                .ok()
                .zip(self.memory.clone().try_acquire_many_owned(units).ok());
            if let Some((capacity, memory)) = claim {
                cache.entries.insert(
                    key.to_owned(),
                    Entry {
                        vector: vector.to_vec(),
                        _memory: memory,
                        _capacity: capacity,
                    },
                );
                cache.order.push_back(key.to_owned());
                return;
            }
            let Some(oldest) = cache.order.pop_front() else {
                return;
            };
            cache.entries.remove(&oldest);
            if cache.order.len() < cache.order.capacity() / 4 {
                cache.order.shrink_to_fit();
            }
        }
    }

    pub(crate) fn prepare(&self, queries: &[&str]) -> Result<QueryWork> {
        // Four f32 copies cover a decoded vector, provider return, alias
        // expansion and the one recall sample. Stored entries have separate
        // owned claims.
        let bytes: usize = queries
            .iter()
            .map(|query| self.config.dimensions as usize * 16 + query.len() * 2 + 512)
            .sum();
        // Cache-only decoding already belongs to the generation's bounded
        // native request scratch. Requiring another claim would disable exact
        // retrieval when optional routing consumes the remaining shared pool.
        // Live calls add provider buffers and parked requests, so their copies
        // keep a separate claim even when the process cache supplies a vector.
        let copies = if self.config.reader_live {
            Some(
                self.memory
                    .clone()
                    .try_acquire_many_owned(crate::generation::pages(bytes))
                    .map_err(|_| Busy)?,
            )
        } else {
            None
        };
        let mut work = QueryWork {
            vectors: Vec::with_capacity(queries.len()),
            provider: ProviderStats::default(),
            provider_us: 0,
            keys: Vec::with_capacity(queries.len()),
            _copies: copies,
        };
        for query in queries {
            // Process entries include the full profile and preserve exact UTF-8.
            let key = digest(
                serde_json::json!([self.profile_fingerprint, "query", query])
                    .to_string()
                    .as_bytes(),
            );
            let cached = crate::sync::lock(&self.cache)
                .entries
                .get(&key)
                .map(|entry| entry.vector.clone());
            work.keys.push(key);
            work.vectors.push(cached.ok_or(EmbeddingUnavailable));
        }
        Ok(work)
    }

    pub(crate) async fn complete(
        &self,
        work: &mut QueryWork,
        queries: &[&str],
        authorized: bool,
        deadline: std::time::Instant,
    ) {
        if work.vectors.iter().all(Result::is_ok) {
            return;
        }
        if !authorized || !self.config.reader_live {
            work.degrade(EmbeddingUnavailable);
            return;
        }
        let mut missing = BTreeMap::<String, Vec<usize>>::new();
        for (index, vector) in work.vectors.iter().enumerate() {
            if vector.is_err() {
                missing
                    .entry(work.keys[index].clone())
                    .or_default()
                    .push(index);
            }
        }
        let inputs: Vec<_> = missing
            .values()
            .map(|indices| queries[indices[0]])
            .collect();
        let start = std::time::Instant::now();
        let response = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.client.embed(&inputs, Role::Query, &mut work.provider),
        )
        .await
        .unwrap_or(Err(super::EmbeddingError::EmbeddingTimeout))
        .map_err(ReaderError::from);
        work.provider_us = crate::telemetry::micros(start.elapsed());
        match response {
            Ok(vectors) => {
                for ((key, indices), vector) in missing.into_iter().zip(vectors) {
                    self.remember(&key, &vector);
                    for index in indices {
                        work.vectors[index] = Ok(vector.clone());
                    }
                }
            }
            // One failed batch degrades every query, including cached siblings.
            Err(code) => work.degrade(code),
        }
    }
}

#[cfg(test)]
#[path = "query/tests.rs"]
mod tests;
