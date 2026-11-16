//! Optional, bounded hosted reranking. Payloads and HTTP errors never reach logs.
use crate::error::*;
use crate::provider::BodyError;
use crate::retrieval::Candidate;
use crate::search::{PreparedReader, ids, strings};
use futures::{StreamExt, TryStreamExt, stream};
use fyrr_core::config::{Environment, ProcessEnvironment, RerankConfig};
use fyrr_core::mcp::Telemetry;
use serde::Deserialize;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::time::Instant;

const MAX_TEXT_BYTES: usize = 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_IN_FLIGHT: usize = 4;

#[derive(Debug)]
pub(crate) struct Reranker {
    client: reqwest::Client,
    spent: AtomicU64,
}
impl Reranker {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .http1_only()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .map_err(|_| RerankProviderFailed)?,
            spent: AtomicU64::new(0),
        })
    }

    fn reserve(&self, config: &RerankConfig) -> Result<()> {
        crate::provider::reserve_spend(
            &self.spent,
            config.max_call_cost_microusd,
            config.spend_limit_microusd,
        )
        .then_some(())
        .ok_or(RerankSpendExhausted)
    }

    fn scores<A>(
        &self,
        config: &RerankConfig,
        query: &str,
        documents: Vec<String>,
        deadline: Instant,
        authorization: A,
    ) -> impl std::future::Future<Output = Result<Vec<f64>>> + Send + 'static
    where
        A: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let count = documents.len();
        // Prepare and reserve synchronously as the stream admits each query,
        // so a limited budget always selects the same prefix, not the fastest future.
        let request = (|| {
            if count == 0 {
                return Ok(None);
            }
            if query.len() + documents.iter().map(String::len).sum::<usize>() > MAX_TEXT_BYTES {
                return Err(RerankPayloadLimit);
            }
            let payload = serde_json::to_vec(
                &serde_json::json!({"model":config.model,"query":query,"documents":documents}),
            )
            .map_err(|_| RerankPayloadLimit)?;
            // HTTP/1 polls the stream only after connecting (including TLS).
            // Emit one frame: the live authorization read is the last await
            // before any payload bytes become available to the transport.
            let body = reqwest::Body::wrap_stream(stream::once(async move {
                authorization.await.map_err(std::io::Error::other)?;
                if Instant::now() >= deadline {
                    return Err(std::io::Error::other("rerank_deadline"));
                }
                Ok(payload)
            }));
            let mut request = self
                .client
                .post(&config.endpoint)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
            if let Some(secret) = &config.api_key {
                let key = ProcessEnvironment
                    .var(&secret.env)
                    .filter(|key| !key.is_empty())
                    .ok_or(RerankProviderFailed)?;
                request = request.bearer_auth(key);
            }
            if Instant::now() >= deadline {
                return Err(RerankDeadline);
            }
            self.reserve(config)?;
            Ok(Some(request))
        })();
        async move {
            tokio::time::timeout_at(deadline, async {
                let Some(request) = request? else {
                    return Ok(Vec::new());
                };
                if Instant::now() >= deadline {
                    return Err(RerankDeadline);
                }
                let response = request.send().await.map_err(|_| RerankProviderFailed)?;
                if !response.status().is_success() {
                    return Err(RerankProviderFailed);
                }
                let bytes = crate::provider::bounded_body(response, MAX_RESPONSE_BYTES)
                    .await
                    .map_err(|error| match error {
                        BodyError::Transport => RerankProviderFailed,
                        BodyError::TooLarge => RerankResponseInvalid,
                    })?;
                parse_scores(&bytes, count)
            })
            .await
            .map_err(|_| RerankDeadline)?
        }
    }
}

/// Lowercase alphanumeric terms, excluding underscores, shorter than 40 bytes.
fn terms(text: &str) -> std::collections::BTreeSet<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|term| !term.is_empty() && term.len() < 40)
        .map(str::to_lowercase)
        .collect()
}

/// With a bound, the `chars`-character window holding the most distinct query
/// terms, stepping by a third of the bound; the earliest window wins ties.
/// Records within the bound, or without one, are sent whole.
fn window<'a>(text: &'a str, query: &str, chars: Option<usize>) -> &'a str {
    let Some(chars) = chars else {
        return text;
    };
    let bounds: Vec<usize> = text
        .char_indices()
        .map(|(offset, _)| offset)
        .chain(std::iter::once(text.len()))
        .collect();
    let count = bounds.len() - 1;
    if count <= chars {
        return text;
    }
    let wanted = terms(query);
    let mut best = (0, 0);
    for start in (0..count - chars)
        .step_by((chars / 3).max(1))
        .chain(std::iter::once(count - chars))
    {
        let found = terms(&text[bounds[start]..bounds[start + chars]])
            .intersection(&wanted)
            .count();
        if found > best.0 {
            best = (found, start);
        }
    }
    &text[bounds[best.1]..bounds[best.1 + chars]]
}

#[derive(Deserialize)]
struct ScoredDocument {
    index: usize,
    relevance_score: f64,
}

fn parse_scores(bytes: &[u8], count: usize) -> Result<Vec<f64>> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| RerankResponseInvalid)?;
    // Accept exactly one envelope: Voyage's data or Cohere's results, never both.
    let ((Some(rows), None) | (None, Some(rows))) = (value.get("data"), value.get("results"))
    else {
        return Err(RerankResponseInvalid);
    };
    let rows: Vec<ScoredDocument> =
        serde_json::from_value(rows.clone()).map_err(|_| RerankResponseInvalid)?;
    if rows.len() != count {
        return Err(RerankResponseInvalid);
    }
    let mut scores = vec![None; count];
    for row in rows {
        if !row.relevance_score.is_finite() || row.index >= count || scores[row.index].is_some() {
            return Err(RerankResponseInvalid);
        }
        scores[row.index] = Some(row.relevance_score);
    }
    scores
        .into_iter()
        .map(|score| score.ok_or(RerankResponseInvalid))
        .collect()
}

/// Logistic normalization of population z scores. Scaling
/// first keeps the arithmetic finite even for extreme valid provider scores.
fn normalized(scores: &[f64]) -> Vec<f64> {
    let scale = scores
        .iter()
        .map(|score| score.abs())
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let mean = scores.iter().map(|score| score / scale).sum::<f64>() / scores.len() as f64;
    let sd = (scores
        .iter()
        .map(|score| (score / scale - mean).powi(2))
        .sum::<f64>()
        / scores.len() as f64)
        .sqrt();
    scores
        .iter()
        .map(|score| {
            if sd == 0.0 {
                0.5
            } else {
                1.0 / (1.0 + (-((score / scale - mean) / sd).clamp(-60.0, 60.0)).exp())
            }
        })
        .collect()
}

fn blend(rows: &mut [Candidate], scores: &[f64], weight: f64) {
    if weight == 0.0 {
        return;
    }
    let blended = if weight == 1.0 {
        scores.to_vec()
    } else {
        let fusion = normalized(&rows.iter().map(|row| row.score).collect::<Vec<_>>());
        normalized(scores)
            .into_iter()
            .zip(fusion)
            .map(|(rerank, fusion)| weight * rerank + (1.0 - weight) * fusion)
            .collect()
    };
    let mut order: Vec<_> = (0..rows.len()).collect();
    order.sort_by(|a, b| {
        if blended[*a] == blended[*b] {
            a.cmp(b)
        } else {
            blended[*b].total_cmp(&blended[*a])
        }
    });
    let original = rows.to_vec();
    for (row, index) in rows.iter_mut().zip(order) {
        *row = original[index].clone();
    }
}

pub(crate) async fn rerank(
    reader: &mut PreparedReader,
    queries: &[&str],
    ranked: &mut [Vec<Candidate>],
    telemetry: &mut Telemetry,
) -> Result<bool> {
    let Some(config) = reader.config.namespace.rerank.clone() else {
        return Ok(false);
    };
    let start = Instant::now();
    let deadline = start + config.deadline.as_duration();
    let refused_admission = Arc::new(AtomicBool::new(false));
    let result = tokio::time::timeout_at(
        deadline,
        rerank_inner(
            reader,
            queries,
            ranked,
            &config,
            deadline,
            &refused_admission,
        ),
    )
    .await;
    telemetry.rerank_us = Some(crate::telemetry::micros(start.elapsed()));
    if refused_admission.load(Ordering::Acquire) {
        // A known stale admission cannot proceed to hydration, even if the
        // deadline interrupts another call. Preserve the final authorization
        // refusal rather than returning documents from the old grant snapshot.
        ranked.iter_mut().for_each(Vec::clear);
        return Err(AuthorizationUnavailable);
    }
    match result {
        Ok(result) => result,
        Err(_) => {
            crate::telemetry::emit(format_args!(
                "telemetry rerank=degraded reason=rerank_deadline"
            ));
            Ok(true)
        }
    }
}

async fn rerank_inner(
    reader: &mut PreparedReader,
    queries: &[&str],
    ranked: &mut [Vec<Candidate>],
    config: &RerankConfig,
    deadline: Instant,
    refused_admission: &Arc<AtomicBool>,
) -> Result<bool> {
    let Some(reranker) = reader.reranker.clone() else {
        return Err(StorageUnavailable);
    };
    if ranked.iter().all(Vec::is_empty) {
        return Ok(false);
    }
    // Account decoded text, escaped JSON and provider scratch through the serving pool.
    let scratch = ranked.iter().filter(|rows| !rows.is_empty()).count() * MAX_TEXT_BYTES * 8;
    let claim = reader
        .preparation_budget
        .permits
        .clone()
        .try_acquire_many_owned(crate::generation::pages(scratch));
    let Ok(_claim) = claim else {
        // Optional payload preparation cannot consume unreserved serving memory.
        crate::telemetry::emit(format_args!(
            "telemetry rerank=degraded reason=rerank_payload_limit"
        ));
        return Ok(true);
    };
    if reader.refresh_authorization().await? {
        return Err(AuthorizationUnavailable);
    }
    let documents = prepare_rerank_documents(reader, queries, ranked, config).await?;
    if reader.refresh_authorization().await? {
        return Err(AuthorizationUnavailable);
    }
    let reader = &*reader;
    let results: Vec<_> = stream::iter(documents.into_iter().enumerate())
        .map(|(index, documents)| {
            // Reserve in query order; authorization is rechecked by the body
            // stream after connection setup, not before the HTTP dispatch.
            let store = reader.control_store.clone();
            let root = reader.control_root.clone();
            let admitted = reader.head.clone();
            let config_snapshot = reader.config.clone();
            let hidden_sources = reader.hidden_sources.clone();
            let refused = refused_admission.clone();
            let refused_guard = refused.clone();
            let authorization = async move {
                let current: fyrr_core::publication::ServingHead =
                    crate::storage::read_contract(store.as_ref(), &root.join("head.json")).await?;
                // A changed grant snapshot drops the entire candidate payload:
                // no text from the old admission is sent, including revoked rows.
                if current.namespace != config_snapshot.namespace.name
                    || current.policy != admitted.policy
                    || current.identity != admitted.identity
                    || PreparedReader::expired_sources(&config_snapshot, &current)?
                        != hidden_sources
                    || !crate::storage::directory_fresh(
                        &config_snapshot,
                        current.freshness.directory,
                        fyrr_core::clock::now(),
                    )
                {
                    refused_guard.store(true, Ordering::Release);
                    return Err(AuthorizationUnavailable);
                }
                Ok(())
            };
            let work = reader
                .authorization()
                .map_err(|_| {
                    refused.store(true, Ordering::Release);
                    RerankProviderFailed
                })
                .and(documents)
                .map(|documents| {
                    reranker.scores(config, queries[index], documents, deadline, authorization)
                });
            async move {
                let scores = match work {
                    Ok(work) => work.await,
                    Err(code) => Err(code),
                };
                if refused.load(Ordering::Acquire) {
                    return Err(AuthorizationUnavailable);
                }
                Ok::<_, ReaderError>((index, scores))
            }
        })
        .buffer_unordered(MAX_IN_FLIGHT)
        .try_collect()
        .await?;
    let mut degraded = false;
    for (index, scores) in results {
        match scores {
            Ok(scores) if !scores.is_empty() => {
                let count = scores.len();
                blend(&mut ranked[index][..count], &scores, config.weight);
            }
            Ok(_) => {}
            Err(code) => {
                degraded = true;
                // Fixed reason only: never the query, document, endpoint or HTTP body.
                crate::telemetry::emit(format_args!("telemetry rerank=degraded reason={code}"));
            }
        }
    }
    Ok(degraded)
}

async fn prepare_rerank_documents(
    reader: &PreparedReader,
    queries: &[&str],
    ranked: &[Vec<Candidate>],
    config: &RerankConfig,
) -> Result<Vec<Result<Vec<String>>>> {
    let mut documents = Vec::with_capacity(ranked.len());
    for rows in ranked.iter() {
        let head = &rows[..rows.len().min(config.candidates)];
        if head.is_empty() {
            documents.push(Ok(Vec::new()));
            continue;
        }
        if head
            .iter()
            .any(|row| !reader.selected_rows().selected(row.row_id))
        {
            return Err(AuthorizationUnavailable);
        }
        let row_ids: Vec<_> = head.iter().map(|row| row.row_id).collect();
        let batch = reader
            .rows_by_id(
                &row_ids,
                &["source_id", "root_id", "record_id", "searchable_text"],
                PublicationInvalid,
            )
            .await?;
        let ids = ids(&batch)?;
        let text = strings(&batch, "searchable_text")?;
        let mut texts = Vec::new();
        let mut bytes = queries[documents.len()].len();
        let mut oversized = false;
        for row in head {
            let index = ids
                .values()
                .iter()
                .position(|id| *id == row.row_id)
                .ok_or(StorageUnavailable)?;
            if row.source != strings(&batch, "source_id")?.value(index)
                || row.root != strings(&batch, "root_id")?.value(index)
                || row.record != strings(&batch, "record_id")?.value(index)
            {
                return Err(StorageUnavailable);
            }
            let body = window(
                text.value(index),
                queries[documents.len()],
                config.text_chars,
            );
            bytes += body.len();
            if bytes > MAX_TEXT_BYTES {
                oversized = true;
                break;
            }
            texts.push(body.to_owned());
        }
        documents.push(if oversized {
            Err(RerankPayloadLimit)
        } else {
            Ok(texts)
        });
    }

    Ok(documents)
}

#[cfg(test)]
#[path = "rerank/tests.rs"]
mod tests;
