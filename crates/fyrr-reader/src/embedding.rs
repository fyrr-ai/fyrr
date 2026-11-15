//! Profile-bound embeddings: identity, keys, validation and live resources.

use EmbeddingError::*;

type Result<T> = std::result::Result<T, EmbeddingError>;
use fyrr_core::config::EmbeddingConfig;
use fyrr_core::envelope::{EmbeddingIdentity, EmbeddingSettings};
pub use fyrr_core::limits::digest;

mod error;
pub use error::EmbeddingError;
mod live;
pub use live::{EmbeddingClient, ProviderStats, Role};
mod query;
pub(crate) use query::{QueryEmbeddings, QueryWork};

/// Profile and role bound key of `text` for the writer's local cache.
pub fn cache_key(config: &EmbeddingConfig, text: &str, role: Role) -> String {
    transformed_key(config, &transform(config, text, role), role)
}

/// The text as the provider receives it for this role.
pub(crate) fn transform(config: &EmbeddingConfig, text: &str, role: Role) -> String {
    let transform = match role {
        Role::Document => &config.roles.document,
        Role::Query => &config.roles.query,
    };
    format!(
        "{}{}{text}",
        config.prefix,
        transform.prefix.as_deref().unwrap_or("")
    )
}

/// Profile and role bound key of provider input `transformed`.
pub fn transformed_key(config: &EmbeddingConfig, transformed: &str, role: Role) -> String {
    digest(
        serde_json::json!([
            identity(config).fingerprint,
            match role {
                Role::Document => "document",
                Role::Query => "query",
            },
            transformed
        ])
        .to_string()
        .as_bytes(),
    )
}

/// Refuses an embedding profile no vector could match.
pub fn check_profile(config: &EmbeddingConfig) -> Result<()> {
    identity(config)
        .validate_profile()
        .map_err(|_| EmbeddingProfileInvalid)?;
    if config.dimensions == 0 || u64::from(config.dimensions) > fyrr_core::config::DIMENSIONS_MAX {
        return Err(EmbeddingProfileInvalid);
    }
    Ok(())
}

pub fn identity(config: &EmbeddingConfig) -> EmbeddingIdentity {
    let settings = EmbeddingSettings {
        api_style: config.profile,
        prefix: config.prefix.clone(),
        roles: config.roles.clone(),
        send_dimensions: config.send_dimensions,
    };
    let legacy = config.model == "BAAI/bge-m3"
        && config.dimensions == 1024
        && config.roles == fyrr_core::config::EmbeddingRoles::default()
        && !config.send_dimensions;
    let fingerprint = if legacy {
        digest(
            serde_json::json!([config.model, config.dimensions, config.prefix, "exact-utf8"])
                .to_string()
                .as_bytes(),
        )
    } else {
        settings.fingerprint(&config.model, config.dimensions)
    };
    EmbeddingIdentity {
        model: config.model.clone(),
        dimensions: config.dimensions,
        fingerprint,
        settings: if legacy {
            None
        } else {
            Some(Box::new(settings))
        },
    }
}

pub fn validate(vector: &[f32], dimensions: u32) -> Result<()> {
    if vector.len() != dimensions as usize || vector.iter().any(|value| !value.is_finite()) {
        return Err(EmbeddingInvalid);
    }
    let norm: f64 = vector.iter().map(|v| f64::from(*v).powi(2)).sum();
    if norm == 0.0 || !norm.is_finite() {
        return Err(EmbeddingInvalid);
    }
    let originals: Vec<_> = vector.iter().copied().map(half::f16::from_f32).collect();
    if originals.iter().any(|v| !v.is_finite()) || originals.iter().all(|v| *v == half::f16::ZERO) {
        return Err(EmbeddingInvalid);
    }
    Ok(())
}

#[cfg(test)]
#[path = "embedding/tests.rs"]
mod tests;
