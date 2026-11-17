//! Conditional DELETE uses the exact transport constructed for the S3 store.
use object_store::client::{
    ClientOptions, HttpClient, HttpConnector, HttpRequestBody, ReqwestConnector,
};
use object_store::signer::{HeaderName, HeaderValue, Method, SignedUrlOptions, Signer};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, aws::AmazonS3, path::Path};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default)]
pub(super) struct Transport(Arc<Mutex<Option<HttpClient>>>);
impl HttpConnector for Transport {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let client = super::diagnostics::transport(ReqwestConnector::default().connect(options)?);
        // The builder's last connection is the store transport, after credential
        // transports. Reuse it with every configured CA, proxy and TLS option.
        *self.0.lock().map_err(|_| failure())? = Some(client.clone());
        Ok(client)
    }
}
impl Transport {
    pub(super) fn client(&self) -> object_store::Result<HttpClient> {
        self.0
            .lock()
            .map_err(|_| failure())?
            .clone()
            .ok_or_else(failure)
    }
}
fn failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "fyrr namespace head",
        source: "conditional head deletion failed".into(),
    }
}
async fn delete_unobserved(
    store: &AmazonS3,
    client: &HttpClient,
    path: &Path,
    tag: &str,
) -> object_store::Result<u16> {
    let header = HeaderValue::from_str(tag).map_err(|_| failure())?;
    let options = SignedUrlOptions::new()
        .with_signed_header(HeaderName::from_static("if-match"), header.clone());
    let url = store
        .signed_url_opts(
            Method::DELETE,
            path,
            std::time::Duration::from_secs(30),
            &options,
        )
        .await
        .map_err(|_| failure())?;
    let request = hyper::Request::builder()
        .method(Method::DELETE)
        .uri(url.as_str())
        .header("if-match", header)
        .body(HttpRequestBody::empty())
        .map_err(|_| failure())?;
    // Never expose signed URLs, headers or transport diagnostics.
    let response =
        tokio::time::timeout(std::time::Duration::from_secs(15), client.execute(request))
            .await
            .map_err(|_| failure())?
            .map_err(|_| failure())?;
    Ok(response.status().as_u16())
}

async fn delete(
    store: &AmazonS3,
    client: &HttpClient,
    path: &Path,
    tag: &str,
) -> object_store::Result<u16> {
    super::diagnostics::observe(
        "DELETE_CONDITIONAL",
        path,
        false,
        delete_unobserved(store, client, path, tag),
    )
    .await
}

pub(super) async fn delete_head(
    store: &AmazonS3,
    client: &HttpClient,
    root: &Path,
    tag: &str,
) -> object_store::Result<()> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nonce = format!(
        "{}-{}-{}",
        std::process::id(),
        fyrr_core::clock::now()
            .timestamp_nanos_opt()
            .ok_or_else(failure)?,
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let probe = root
        .clone()
        .join(format!(".conditional-delete-probe-{nonce}"));
    let boundary = super::diagnostics::wrap(Arc::new(store.clone()), false, root);
    let created = boundary
        .put_opts(
            &probe,
            b"fyrr conditional DELETE capability".to_vec().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .map_err(|_| failure())?;
    let wrong = if created.e_tag.as_deref() == Some("\"fyrr-wrong-tag\"") {
        "\"fyrr-other-wrong-tag\""
    } else {
        "\"fyrr-wrong-tag\""
    };
    // Never probe the head: a server ignoring If-Match may delete the probe.
    let capability = delete(store, client, &probe, wrong).await;
    boundary.delete(&probe).await.map_err(|_| failure())?;
    if capability? != 412 {
        return Err(object_store::Error::NotSupported {
            source: "S3 conditional DELETE capability probe must return HTTP 412".into(),
        });
    }
    let path = root.clone().join("head.json");
    match delete(store, client, &path, tag).await? {
        200..=299 => Ok(()),
        409 | 412 => Err(object_store::Error::Precondition {
            path: path.to_string(),
            source: "namespace head changed".into(),
        }),
        _ => Err(failure()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::State,
        http::{HeaderMap, Method, StatusCode},
        response::IntoResponse,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[tokio::test]
    async fn capability_probe_rejects_ignored_conditions_and_reuses_store_transport() {
        for supported in [false, true] {
            let heads = Arc::new(AtomicUsize::new(0));
            let observed = heads.clone();
            let app = Router::new()
                .fallback(
                    move |State(heads): State<Arc<AtomicUsize>>,
                          method: Method,
                          uri: axum::http::Uri,
                          headers: HeaderMap| async move {
                        assert_eq!(headers.get("x-store-transport").unwrap(), "shared");
                        if method == Method::DELETE && uri.path().ends_with("head.json") {
                            heads.fetch_add(1, Ordering::SeqCst);
                        }
                        let wrong = headers
                            .get("if-match")
                            .is_some_and(|tag| tag == "\"fyrr-wrong-tag\"");
                        let status = if wrong && supported {
                            StatusCode::PRECONDITION_FAILED
                        } else {
                            StatusCode::OK
                        };
                        (status, [("etag", "\"fixture\"")], "").into_response()
                    },
                )
                .with_state(observed);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let transport = Transport::default();
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("x-store-transport", HeaderValue::from_static("shared"));
            let store = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name("fixture")
                .with_region("us-east-1")
                .with_access_key_id("fixture")
                .with_secret_access_key("fixture")
                .with_endpoint(format!("http://{address}"))
                .with_allow_http(true)
                .with_config(
                    object_store::aws::AmazonS3ConfigKey::DisableBulkDelete,
                    "true",
                )
                .with_client_options(
                    ClientOptions::new()
                        .with_allow_http(true)
                        .with_default_headers(headers),
                )
                .with_http_connector(transport.clone())
                .build()
                .unwrap();
            let result = delete_head(
                &store,
                &transport.client().unwrap(),
                &Path::from("namespace"),
                "\"head-version\"",
            )
            .await;
            assert_eq!(result.is_ok(), supported);
            assert_eq!(heads.load(Ordering::SeqCst), usize::from(supported));
            if !supported {
                assert!(matches!(
                    result,
                    Err(object_store::Error::NotSupported { .. })
                ));
            }
            server.abort();
        }
    }
}
