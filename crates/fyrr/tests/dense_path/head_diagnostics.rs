//! Conditional-write diagnostics through the real S3 privacy boundary.
use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[test]
#[ignore = "real S3 conditional write, untrusted HTTP response and CLI privacy sandbox"]
fn conditional_head_failure_classifies_untrusted_storage_error_on_real_s3() {
    let (config, root, _) = super::tools::fixture();
    let sandbox = common::Sandbox::new("conditional-head-privacy");
    let mut sentinels = common::Sentinels::new();
    let marker = sentinels.mark("storage-error-body");
    let release = Arc::new(Mutex::new(None));
    let observed = Arc::new(AtomicBool::new(false));
    let watch = release.clone();
    let hook: super::corrections::BoundaryHook = Arc::new(move |request, body, after| {
        if !after && request.starts_with("PUT ") && request.contains("/head.json") {
            let head: fyrr_core::publication::ServingHead =
                fyrr_core::parse_contract(body).unwrap();
            if head
                .lease
                .is_some_and(|lease| lease.expires_at <= fyrr_core::clock::now())
            {
                *watch.lock().unwrap() = Some(std::thread::current().id());
            }
        }
        false
    });
    let hit = observed.clone();
    let transform: super::corrections::ResponseTransform = Arc::new(move |request, response| {
        if request.starts_with("PUT ")
            && request.contains("/head.json")
            && release
                .lock()
                .unwrap()
                .take_if(|id| *id == std::thread::current().id())
                .is_some()
        {
            assert!(
                response.starts_with(b"HTTP/1.1 200 ") || response.starts_with(b"HTTP/1.1 204 "),
                "the actual S3 release succeeded"
            );
            let body =
                format!("<Error><Code>AccessDenied</Code><Message>{marker}</Message></Error>");
            *response = format!(
                "HTTP/1.1 403 Forbidden\r\n\
                 Content-Type: application/xml\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            )
            .into_bytes();
            hit.store(true, Ordering::SeqCst);
        }
    });
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let (proxy, guard) = super::corrections::proxy_transform(
        endpoint.clone(),
        Arc::new(AtomicBool::new(false)),
        transform,
        Some(hook),
    );
    let text = std::fs::read_to_string(&config).unwrap();
    assert!(text.contains(&endpoint));
    let metered = sandbox.input("fyrr.toml", &text.replace(&endpoint, &proxy));
    let output = sandbox.run_command(
        super::coordination::host_command(&metered),
        ["writer", "commit"],
        &[],
    );
    drop(guard);
    assert!(
        observed.load(Ordering::SeqCst),
        "the release response was faulted after real S3"
    );
    assert!(
        !output.status.success(),
        "an uncertain conditional write must not report success"
    );
    common::assert_clean(
        &sentinels,
        &output,
        &sandbox,
        "conditional head-write diagnostics",
    );
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("error: head_cas_failed"), "{error}");
    assert!(
        error.contains("telemetry head_write=failed stage=release category=permission_denied"),
        "the bounded error category and write stage must survive: {error}"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3 credentials and acknowledged metadata discovery; scripts/ci"]
async fn ec2_metadata_opt_out_preserves_other_credential_sources_on_real_s3() {
    use std::sync::atomic::AtomicUsize;
    let (config, root) = setup_on(Backend::S3);
    // This storage proof needs no sources or fixed-connection warnings.
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config, text.split("[[sources]]").next().unwrap()).unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    // This is a credential endpoint, never a fake object store. A nonretryable
    // refusal proves which provider is contacted without retry timing or sleeps.
    let app = axum::Router::new().fallback(move || {
        let seen = seen.clone();
        async move {
            seen.fetch_add(1, Ordering::SeqCst);
            axum::http::StatusCode::FORBIDDEN
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let command = |path: &Path, disabled: bool| {
        let mut command = super::coordination::host_command(path);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("AWS_") {
                command.env_remove(key);
            }
        }
        command
            .env("AWS_METADATA_ENDPOINT", &endpoint)
            .env("AWS_EC2_METADATA_DISABLED", disabled.to_string());
        command
    };
    let mut create = command(&config, true);
    let output = create
        .args(["writer", "namespace", "create"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", report(&output));
    assert_eq!(
        requests.load(Ordering::SeqCst),
        0,
        "explicit credentials work against real S3 with metadata disabled"
    );

    let role = root.join("role.toml");
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .lines()
        .filter(|line| {
            !line.starts_with("access_key_id =") && !line.starts_with("secret_access_key =")
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&role, text).unwrap();
    let refused = |mut command: Command| {
        let output = command.args(["writer", "status"]).output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(output.stderr, b"error: storage_unavailable\n");
    };
    refused(command(&role, true));
    assert_eq!(
        requests.load(Ordering::SeqCst),
        0,
        "the opt-out must not contact the metadata endpoint"
    );
    refused(command(&role, false));
    assert!(
        requests.load(Ordering::SeqCst) > 0,
        "the instance provider must remain enabled without the opt-out"
    );

    requests.store(0, Ordering::SeqCst);
    let token = root.join("container-token");
    std::fs::write(&token, "fixture-container-token").unwrap();
    let mut container = command(&role, true);
    container
        .env("AWS_CONTAINER_CREDENTIALS_FULL_URI", &endpoint)
        .env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", &token);
    refused(container);
    assert!(
        requests.load(Ordering::SeqCst) > 0,
        "container credential discovery must remain enabled with IMDS disabled"
    );
    server.abort();
}
