//! Loopback-only embedding provider over frozen vectors: the writer embeds
//! documents and the reader embeds queries through it, with no paid call.
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use fyrr_core::config::EmbeddingConfig;
use serde_json::{Value, json};

use crate::frozen::FrozenVectors;
use crate::{Error, Result};
use fyrr_reader::embedding::Role;

struct Service {
    config: EmbeddingConfig,
    vectors: Arc<FrozenVectors>,
}

pub(crate) async fn serve(
    config: EmbeddingConfig,
    vectors: Arc<FrozenVectors>,
    listen: std::net::SocketAddr,
) -> Result<()> {
    if !listen.ip().is_loopback() {
        return Err(Error::InvalidArgument);
    }
    let socket = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|_| Error::ListenerUnavailable)?;
    {
        println!(
            "http://{}/v1",
            socket
                .local_addr()
                .map_err(|_| Error::ListenerUnavailable)?
        );
    }
    let app = Router::new()
        .route("/v1/embeddings/models", get(models))
        .route("/v1/models", get(models))
        .route("/v1/embeddings", post(embeddings))
        .with_state(Arc::new(Service { config, vectors }));
    axum::serve(socket, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|_| Error::ListenerUnavailable)
}

async fn models(State(service): State<Arc<Service>>) -> Json<Value> {
    Json(json!({"data":[{"id":service.config.model}]}))
}

async fn embeddings(
    State(service): State<Arc<Service>>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, (StatusCode, &'static str)> {
    // Parse manually so parser errors never echo provider inputs.
    let invalid = || (StatusCode::BAD_REQUEST, "evaluation_input_invalid");
    let body: Value = serde_json::from_slice(&body).map_err(|_| invalid())?;
    if body["model"].as_str() != Some(service.config.model.as_str())
        || body
            .get("dimensions")
            .is_some_and(|d| d.as_u64() != Some(u64::from(service.config.dimensions)))
    {
        return Err(invalid());
    }
    let input_type = body
        .get("input_type")
        .map(|value| value.as_str().ok_or_else(invalid))
        .transpose()?;
    let document = service.config.roles.document.input_type.as_deref();
    let query = service.config.roles.query.input_type.as_deref();
    let roles = if input_type == document && input_type == query {
        // Equal input types do not tell the roles apart; queries come first.
        [Some(Role::Query), Some(Role::Document)]
    } else if input_type == document {
        [Some(Role::Document), None]
    } else if input_type == query {
        [Some(Role::Query), None]
    } else {
        return Err(invalid());
    };
    let inputs: Vec<String> = match &body["input"] {
        Value::String(text) => vec![text.clone()],
        value => serde_json::from_value(value.clone()).map_err(|_| invalid())?,
    };
    if inputs.is_empty() || inputs.len() > service.config.batch_size as usize {
        return Err(invalid());
    }
    let model = service.config.model.clone();
    let data = tokio::task::spawn_blocking(move || {
        inputs
            .iter()
            .enumerate()
            .map(|(index, text)| {
                roles
                    .iter()
                    .flatten()
                    .find_map(|role| service.vectors.transformed(text, *role).ok())
                    .map(|embedding| json!({"object":"embedding", "index":index,"embedding":embedding}))
                    .ok_or(())
            })
            .collect::<Result<Vec<_>, _>>()
    })
    .await
    .map_err(|_| invalid())?
    .map_err(|_| invalid())?;
    Ok(Json(json!({"object":"list","model":model,"data":data})))
}
