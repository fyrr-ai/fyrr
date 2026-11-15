//! One OpenAI-compatible protocol, with conservative per-attempt spending.
use super::validate;
use super::{EmbeddingError::*, Result};
use crate::provider::BodyError;
use fyrr_core::config::{EmbeddingConfig, EmbeddingRole, ProcessEnvironment};
use serde::Deserialize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Document,
    Query,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProviderStats {
    pub calls: u64,
    pub embedding_calls: u64,
    pub elapsed_us: u64,
    pub reserved_microusd: u64,
}

/// Shared across all workers and serving generations of one reader process.
/// Writers use one instance per source worker with an invocation-assigned cap.
pub struct EmbeddingClient {
    config: EmbeddingConfig,
    http: reqwest::Client,
    verified: tokio::sync::Mutex<bool>,
    reserved: AtomicU64,
    calls: AtomicU64,
    embedding_calls: AtomicU64,
    elapsed_us: AtomicU64,
    pool: Option<Arc<Semaphore>>,
}

impl std::fmt::Debug for EmbeddingClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddingClient")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct Model {
    id: String,
}
#[derive(Deserialize)]
struct Catalog {
    data: Vec<Model>,
}
#[derive(Deserialize)]
struct Item {
    index: usize,
    embedding: Vec<f32>,
}
#[derive(Deserialize)]
struct Response {
    model: Option<String>,
    data: Vec<Item>,
}

impl EmbeddingClient {
    pub fn new(config: &EmbeddingConfig, pool: Option<Arc<Semaphore>>) -> Result<Self> {
        if config.batch_size == 0
            || u64::from(config.batch_size) > fyrr_core::config::BATCH_SIZE_MAX
            || u64::from(config.max_retries) > fyrr_core::config::MAX_RETRIES_MAX
            || config.prefix.len() > 4096
            || config.timeout.as_duration().is_zero()
        {
            return Err(EmbeddingConfigurationInvalid);
        }
        if config.spend.as_ref().is_some_and(|spend| {
            spend.limit_microusd == 0
                || spend.max_call_microusd == 0
                || spend.max_call_microusd > spend.limit_microusd
        }) {
            return Err(EmbeddingSpendConfigurationInvalid);
        }
        super::check_profile(config)?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.timeout.as_duration())
            .build()
            .map_err(|_| EmbeddingConfigurationInvalid)?;
        Ok(Self {
            config: config.clone(),
            http,
            verified: false.into(),
            reserved: AtomicU64::new(0),
            calls: AtomicU64::new(0),
            embedding_calls: AtomicU64::new(0),
            elapsed_us: AtomicU64::new(0),
            pool,
        })
    }
    pub fn stats(&self) -> ProviderStats {
        ProviderStats {
            calls: self.calls.load(Ordering::Relaxed),
            embedding_calls: self.embedding_calls.load(Ordering::Relaxed),
            elapsed_us: self.elapsed_us.load(Ordering::Relaxed),
            reserved_microusd: self.reserved.load(Ordering::Relaxed),
        }
    }
    fn role(&self, role: Role) -> &EmbeddingRole {
        match role {
            Role::Document => &self.config.roles.document,
            Role::Query => &self.config.roles.query,
        }
    }
    fn reserve(&self, stats: &mut ProviderStats) -> Result<()> {
        let spend = self
            .config
            .spend
            .as_ref()
            .ok_or(EmbeddingSpendLimitRequired)?;
        if spend.max_call_microusd == 0 {
            return Err(EmbeddingSpendConfigurationInvalid);
        }
        if !crate::provider::reserve_spend(
            &self.reserved,
            spend.max_call_microusd,
            spend.limit_microusd,
        ) {
            return Err(EmbeddingSpendLimitExceeded);
        }
        stats.reserved_microusd += spend.max_call_microusd;
        Ok(())
    }
    fn reserve_memory(&self, bytes: usize) -> Result<Option<OwnedSemaphorePermit>> {
        self.pool
            .as_ref()
            .map(|pool| {
                pool.clone()
                    .try_acquire_many_owned(crate::generation::pages(bytes))
                    .map_err(|_| EmbeddingMemoryBudgetExhausted)
            })
            .transpose()
    }
    async fn request(
        &self,
        path: &str,
        body: Option<&serde_json::Value>,
        maximum: usize,
        stats: &mut ProviderStats,
    ) -> Result<(u16, Vec<u8>)> {
        let url = reqwest::Url::parse(&format!(
            "{}/{}",
            self.config.endpoint.trim_end_matches('/'),
            path
        ))
        .map_err(|_| EmbeddingConfigurationInvalid)?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(EmbeddingConfigurationInvalid);
        }
        let mut request = if let Some(body) = body {
            self.http.post(url).json(body)
        } else {
            self.http.get(url)
        };
        if let Some(key) = &self.config.api_key {
            let secret = key
                .resolve(&ProcessEnvironment)
                .map_err(|_| EmbeddingCredentialsUnavailable)?;
            request = request.bearer_auth(secret.expose());
        }
        self.reserve(stats)?;
        stats.calls += 1;
        if body.is_some() {
            stats.embedding_calls += 1;
            self.embedding_calls.fetch_add(1, Ordering::Relaxed);
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        let start = std::time::Instant::now();
        let result = async {
            let response = request.send().await.map_err(|error| {
                if error.is_timeout() {
                    EmbeddingTimeout
                } else {
                    EmbeddingProviderUnavailable
                }
            })?;
            let status = response.status().as_u16();
            if status != 200 {
                return Ok((status, Vec::new()));
            }
            let bytes = crate::provider::bounded_body(response, maximum)
                .await
                .map_err(|error| match error {
                    BodyError::Transport => EmbeddingProviderUnavailable,
                    BodyError::TooLarge => EmbeddingResponseInvalid,
                })?;
            Ok((status, bytes))
        }
        .await;
        let elapsed = crate::telemetry::micros(start.elapsed());
        stats.elapsed_us += elapsed;
        self.elapsed_us.fetch_add(elapsed, Ordering::Relaxed);
        result
    }
    async fn send(
        &self,
        path: &str,
        body: Option<&serde_json::Value>,
        maximum: usize,
        stats: &mut ProviderStats,
    ) -> Result<(u16, Vec<u8>)> {
        let mut last = EmbeddingProviderUnavailable;
        for attempt in 0..=self.config.max_retries {
            match self.request(path, body, maximum, stats).await {
                Ok((status, _)) if status == 429 || status >= 500 => {
                    last = EmbeddingProviderUnavailable;
                }
                Ok(response) => return Ok(response),
                Err(code @ (EmbeddingProviderUnavailable | EmbeddingTimeout)) => last = code,
                Err(code) => return Err(code),
            }
            if attempt < self.config.max_retries {
                tokio::time::sleep(std::time::Duration::from_millis(
                    50 * (1u64 << attempt.min(5)),
                ))
                .await;
            }
        }
        Err(last)
    }
    /// Verify at writer startup using the document role. Reader embedding calls
    /// verify after authorization using their requested role.
    pub async fn verify_model(&self, stats: &mut ProviderStats) -> Result<()> {
        self.verify_model_for(Role::Document, stats).await
    }
    async fn verify_model_for(&self, role: Role, stats: &mut ProviderStats) -> Result<()> {
        if self.config.spend.is_none() {
            return Err(EmbeddingSpendLimitRequired);
        }
        match tokio::time::timeout(
            self.config.timeout.as_duration(),
            self.verify_model_inner(role, stats),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(EmbeddingTimeout),
        }
    }
    async fn verify_model_inner(&self, role: Role, stats: &mut ProviderStats) -> Result<()> {
        let mut verified = self.verified.lock().await;
        if *verified {
            return Ok(());
        }
        let _memory = self.reserve_memory(4 * 1024 * 1024 * 8)?;
        let (status, bytes) = self
            .send("embeddings/models", None, 4 * 1024 * 1024, stats)
            .await?;
        let (status, bytes) = if status == 404 {
            self.send("models", None, 4 * 1024 * 1024, stats).await?
        } else {
            (status, bytes)
        };
        if status == 404 {
            // No model catalog (Voyage's own API): verify with one reserved probe
            // embedding; the response's model, dimension and vector are checked.
            // The fixed input contains no document or query text.
            drop(_memory);
            self.post_embeddings(&["fyrr model verification"], role, stats)
                .await?;
            *verified = true;
            return Ok(());
        }
        if status != 200 {
            return Err(EmbeddingProviderUnavailable);
        }

        let catalog: Catalog =
            serde_json::from_slice(&bytes).map_err(|_| EmbeddingResponseInvalid)?;
        if !catalog
            .data
            .iter()
            .any(|model| model.id == self.config.model)
        {
            return Err(EmbeddingModelMismatch);
        }
        *verified = true;
        Ok(())
    }
    /// Validate the complete response before returning any vector to a cache.
    /// One absolute timeout includes verification, retries and response decoding.
    pub async fn embed(
        &self,
        texts: &[&str],
        role: Role,
        stats: &mut ProviderStats,
    ) -> Result<Vec<Vec<f32>>> {
        let start = std::time::Instant::now();
        let before = stats.elapsed_us;
        let result = match tokio::time::timeout(
            self.config.timeout.as_duration(),
            self.embed_inner(texts, role, stats),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(EmbeddingTimeout),
        };
        let elapsed = crate::telemetry::micros(start.elapsed());
        let recorded = stats.elapsed_us.saturating_sub(before);
        self.elapsed_us
            .fetch_add(elapsed.saturating_sub(recorded), Ordering::Relaxed);
        stats.elapsed_us = before + elapsed;
        result
    }
    async fn embed_inner(
        &self,
        texts: &[&str],
        role: Role,
        stats: &mut ProviderStats,
    ) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let maximum_batch = match role {
            Role::Document => self.config.batch_size as usize,
            Role::Query => 16,
        };
        if texts.len() > maximum_batch {
            return Err(EmbeddingBatchInvalid);
        }
        let input_bytes = texts
            .iter()
            .map(|text| {
                text.len()
                    + self.config.prefix.len()
                    + self.role(role).prefix.as_ref().map_or(0, String::len)
            })
            .sum::<usize>();
        if input_bytes > 64 * 1024 * 1024 {
            return Err(EmbeddingBatchInvalid);
        }
        self.verify_model_for(role, stats).await?;
        self.post_embeddings(texts, role, stats).await
    }
    async fn post_embeddings(
        &self,
        texts: &[&str],
        role: Role,
        stats: &mut ProviderStats,
    ) -> Result<Vec<Vec<f32>>> {
        let input_bytes = texts
            .iter()
            .map(|text| {
                text.len()
                    + self.config.prefix.len()
                    + self.role(role).prefix.as_ref().map_or(0, String::len)
            })
            .sum::<usize>();
        // Profile/batch limits bound this to 512 MiB + framing at the maximum
        // writer shape; queries are independently limited to sixteen vectors.
        let maximum = texts.len() * self.config.dimensions as usize * 32 + 65536;
        let _memory = self.reserve_memory(maximum * 8 + input_bytes * 3)?;
        let mut body = serde_json::json!({"model":self.config.model,
            "input":texts.iter().map(|text| super::transform(&self.config,
                 text,
                 role)).collect::<Vec<_>>()});
        if let Some(kind) = &self.role(role).input_type {
            body["input_type"] = kind.clone().into();
        }
        if self.config.send_dimensions {
            body["dimensions"] = self.config.dimensions.into();
        }
        let (status, bytes) = self.send("embeddings", Some(&body), maximum, stats).await?;
        if status != 200 {
            return Err(EmbeddingProviderUnavailable);
        }

        let response: Response =
            serde_json::from_slice(&bytes).map_err(|_| EmbeddingResponseInvalid)?;
        if response
            .model
            .as_ref()
            .is_some_and(|model| model != &self.config.model)
        {
            return Err(EmbeddingModelMismatch);
        }
        if response.data.len() != texts.len() {
            return Err(EmbeddingResponseInvalid);
        }
        let mut vectors = vec![None; texts.len()];
        for item in response.data {
            validate(&item.embedding, self.config.dimensions)?;
            let entry = vectors
                .get_mut(item.index)
                .ok_or(EmbeddingResponseInvalid)?;
            if entry.replace(item.embedding).is_some() {
                return Err(EmbeddingResponseInvalid);
            }
        }
        vectors
            .into_iter()
            .map(|vector| vector.ok_or(EmbeddingResponseInvalid))
            .collect()
    }
}

#[cfg(test)]
#[path = "live/tests.rs"]
mod tests;
