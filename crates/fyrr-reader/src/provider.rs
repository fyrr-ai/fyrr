//! Outbound provider calls (OIDC keys, embeddings, reranking): bounded
//! response bodies and fail-closed spend reservations.
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// The paid calls a reader process has reserved, whichever namespace makes them:
/// the spend limits are per process.
#[derive(Debug, Clone, Default)]
pub(crate) struct Spent {
    pub(crate) embedding: Arc<AtomicU64>,
    pub(crate) rerank: Arc<AtomicU64>,
}

/// Why an outbound client could not read a bounded response body.
pub(crate) enum BodyError {
    Transport,
    TooLarge,
}

/// Reads at most `maximum` bytes of a provider response; a larger declared or
/// streamed body fails before more is buffered.
pub(crate) async fn bounded_body(
    mut response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, BodyError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(BodyError::TooLarge);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| BodyError::Transport)? {
        if bytes.len().saturating_add(chunk.len()) > maximum {
            return Err(BodyError::TooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Reserves `cost` against a shared `limit` before a paid call; refuses when the
/// total would exceed it. Reservations are never refunded.
pub(crate) fn reserve_spend(spent: &AtomicU64, cost: u64, limit: u64) -> bool {
    spent
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |spent| {
            spent.checked_add(cost).filter(|next| *next <= limit)
        })
        .is_ok()
}
