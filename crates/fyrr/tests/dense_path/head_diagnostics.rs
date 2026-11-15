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
                .is_some_and(|lease| lease.expires_at <= chrono::Utc::now())
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
