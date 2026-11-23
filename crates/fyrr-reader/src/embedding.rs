//! Profile-bound embeddings: identity, keys, validation and live resources.

use EmbeddingError::*;

type Result<T> = std::result::Result<T, EmbeddingError>;
use arrow_array::{Array, ArrayRef, Float16Array, Float32Array};
use fyrr_core::config::{DenseElement, DenseMetric, EmbeddingConfig};
use fyrr_core::envelope::{
    EmbeddingApiStyle, EmbeddingIdentity, EmbeddingRole, EmbeddingRoles, EmbeddingSettings,
};
pub use fyrr_core::limits::digest;
use std::sync::Arc;

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
        api_style: EmbeddingApiStyle::OpenaiCompatible,
        prefix: config.prefix.clone(),
        roles: EmbeddingRoles {
            document: EmbeddingRole {
                prefix: config.roles.document.prefix.clone(),
                input_type: config.roles.document.input_type.clone(),
            },
            query: EmbeddingRole {
                prefix: config.roles.query.prefix.clone(),
                input_type: config.roles.query.input_type.clone(),
            },
        },
        send_dimensions: config.send_dimensions,
    };
    EmbeddingIdentity {
        model: config.model.clone(),
        dimensions: config.dimensions,
        fingerprint: settings.fingerprint(&config.model, config.dimensions),
        settings: Box::new(settings),
    }
}

/// The largest magnitude of a number in a vector. A larger one could overflow in
/// a distance of the widest vectors (8192 numbers), also inside the index, and
/// the search would fail on it instead of ranking it. Embeddings are far below.
pub const VECTOR_MAGNITUDE_MAX: f32 = 1e6;

/// Whether `vector` is a finite vector of `dimensions` numbers, none beyond
/// [`VECTOR_MAGNITUDE_MAX`], whose sum of squares is a normal positive 32-bit
/// number (a norm that underflows to zero would divide a cosine by zero), that
/// the namespace's element type stores:
/// a 16-bit one must also stay finite and nonzero once its numbers are rounded
/// to 16 bits.
pub fn validate(vector: &[f32], dimensions: u32, element: DenseElement) -> Result<()> {
    if vector.len() != dimensions as usize
        || vector
            .iter()
            .any(|value| !value.is_finite() || value.abs() > VECTOR_MAGNITUDE_MAX)
    {
        return Err(EmbeddingInvalid);
    }
    // Lance measures the norm in 32 bits.
    let norm: f32 = vector.iter().map(|v| v * v).sum();
    if !norm.is_normal() {
        return Err(EmbeddingInvalid);
    }
    if element == DenseElement::F16 {
        let originals: Vec<_> = vector.iter().copied().map(half::f16::from_f32).collect();
        if originals.iter().any(|v| !v.is_finite())
            || originals.iter().all(|v| *v == half::f16::ZERO)
        {
            return Err(EmbeddingInvalid);
        }
    }
    Ok(())
}

/// Lance's distance for a namespace's metric.
pub fn distance_type(metric: DenseMetric) -> lance_linalg::distance::DistanceType {
    use lance_linalg::distance::DistanceType;
    match metric {
        DenseMetric::Cosine => DistanceType::Cosine,
        DenseMetric::SquaredL2 => DistanceType::L2,
        DenseMetric::Dot => DistanceType::Dot,
    }
}

/// A query as an array of the element type the namespace stores its vectors in.
pub fn query_array(vector: &[f32], element: DenseElement) -> ArrayRef {
    match element {
        DenseElement::F16 => Arc::new(Float16Array::from_iter_values(
            vector.iter().copied().map(half::f16::from_f32),
        )),
        DenseElement::F32 => Arc::new(Float32Array::from(vector.to_vec())),
    }
}

/// The numbers of a stored vector. `None` when it holds a null or is of
/// another type than the two the namespace may store.
pub fn stored_values(vector: &dyn Array) -> Option<Vec<f32>> {
    if vector.null_count() > 0 {
        return None;
    }
    if let Some(vector) = vector.as_any().downcast_ref::<Float16Array>() {
        return Some(vector.values().iter().map(|v| v.to_f32()).collect());
    }
    vector
        .as_any()
        .downcast_ref::<Float32Array>()
        .map(|vector| vector.values().to_vec())
}

#[cfg(test)]
#[path = "embedding/tests.rs"]
mod tests;
