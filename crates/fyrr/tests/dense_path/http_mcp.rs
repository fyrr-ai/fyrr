//! Pinned local issuer, real reader executable and RustFS: no model/API calls.
use super::coordination::{host_command, worker_sync};
use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use fyrr_core::directory::{DirectoryGroup, DirectorySnapshot, DirectoryUser};
use reqwest::Client;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub(super) struct Issuer {
    pub(super) url: String,
    key: PathBuf,
    jwks: Value,
    state: Arc<Mutex<(u16, Value)>>,
    requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Issuer {
    pub(super) async fn new(root: &Path) -> Self {
        let key = root.join("jwt-key.pem");
        assert!(
            Command::new("openssl")
                .args([
                    "genpkey",
                    "-algorithm",
                    "RSA",
                    "-pkeyopt",
                    "rsa_keygen_bits:2048",
                    "-out"
                ])
                .arg(&key)
                .stderr(Stdio::null())
                .status()
                .unwrap()
                .success()
        );
        let modulus = Command::new("openssl")
            .args(["rsa", "-noout", "-modulus", "-in"])
            .arg(&key)
            .output()
            .unwrap();
        let hex = String::from_utf8(modulus.stdout).unwrap();
        let hex = hex.trim().strip_prefix("Modulus=").unwrap();
        let n: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let jwks = json!({"keys":[{"kty":"RSA","kid":"good","use":"sig","key_ops":["verify"],"alg":"RS256","n":URL_SAFE_NO_PAD.encode(n),"e":"AQAB"}]});
        let state = Arc::new(Mutex::new((200, jwks.clone())));
        let requests = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let captured = state.clone();
        let counted = requests.clone();
        let router = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                let captured = captured.clone();
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    let (status, value) = captured.lock().unwrap().clone();
                    if status == 408 {
                        tokio::time::sleep(Duration::from_secs(4)).await;
                    }
                    (
                        axum::http::StatusCode::from_u16(status).unwrap(),
                        [(axum::http::header::LOCATION, "/jwks")],
                        axum::Json(value),
                    )
                }
            }),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            url,
            key,
            jwks,
            state,
            requests,
            task,
        }
    }
    pub(super) fn token(&self, subject: &str, patch: Value, header_patch: Value) -> String {
        let mut header = json!({"alg":"RS256","typ":"at+jwt","kid":"good"});
        for (k, v) in header_patch.as_object().unwrap() {
            header[k] = v.clone();
        }
        let now = chrono::Utc::now().timestamp();
        let mut claims = json!({"iss":self.url,"aud":"fyrr-http-test","scope":"fyrr:read","sub":subject,"iat":now,"nbf":now-1,"exp":now+600});
        for (k, v) in patch.as_object().unwrap() {
            claims[k] = v.clone();
        }
        let payload = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header.to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        );
        let mut signer = Command::new("openssl")
            .args(["dgst", "-sha256", "-sign"])
            .arg(&self.key)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        signer
            .stdin
            .take()
            .unwrap()
            .write_all(payload.as_bytes())
            .unwrap();
        let signed = signer.wait_with_output().unwrap();
        assert!(signed.status.success());
        format!("{payload}.{}", URL_SAFE_NO_PAD.encode(signed.stdout))
    }
}
impl Drop for Issuer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(super) struct Reader {
    child: std::process::Child,
    pub(super) base: String,
    client: Client,
    stderr: PathBuf,
}
impl Reader {
    fn search_task(&self, token: &str, id: u64, deadline: u32) -> tokio::task::JoinHandle<Value> {
        let client = self.client.clone();
        let url = format!("{}/mcp", self.base);
        let token = token.to_owned();
        tokio::spawn(async move {
            let response = client.post(url).bearer_auth(token)
                .header("Accept", "application/json, text/event-stream")
                .header("MCP-Protocol-Version", "2025-11-25")
                .json(&json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"search","arguments":{"query":"fixture query","sources":["incremental"],"deadline_ms":deadline}}}))
                .send().await.unwrap();
            assert_eq!(response.status(), 200);
            response.json::<Value>().await.unwrap()["result"].clone()
        })
    }
    async fn start(
        config: &Path,
        root: &Path,
        issuer: &Issuer,
        user_limit: u32,
        group_limit: u32,
    ) -> Self {
        Self::start_configured(
            config,
            root,
            issuer,
            user_limit,
            group_limit,
            2,
            false,
            true,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn start_configured(
        config: &Path,
        root: &Path,
        issuer: &Issuer,
        user_limit: u32,
        group_limit: u32,
        capacity: usize,
        keycloak: bool,
        operator: bool,
    ) -> Self {
        let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reserve.local_addr().unwrap();
        drop(reserve);
        let base = format!("http://{address}");
        let config_http = root.join(format!("http-{address}.toml"));
        let config_text = std::fs::read_to_string(config).unwrap();
        let operator = if operator {
            "operator_credential={env='FYRR_HTTP_OPERATOR'}\n"
        } else {
            ""
        };
        std::fs::write(&config_http,format!("{config_text}\n[identity.oidc]\nissuer='{}'\naudience='fyrr-http-test'\njwks_url='{}/jwks'\njwks_validity='1s'\njwks_refresh_interval='100ms'\nkeycloak={keycloak}\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\n{operator}authorization_cache_subjects={capacity}\nuser_requests_per_minute={user_limit}\ngroup_requests_per_minute={group_limit}\n",issuer.url,issuer.url)).unwrap();
        let stderr = root.join(format!("http-{address}.stderr"));
        let child = host_command(&config_http)
            .args(["reader", "serve"])
            .env("FYRR_HTTP_OPERATOR", "owned-http-operator-canary")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        let mut reader = Self {
            child,
            base,
            client: Client::builder()
                .timeout(Duration::from_secs(15))
                // An early 413 can close a socket still retained by the client pool.
                .pool_max_idle_per_host(0)
                .build()
                .unwrap(),
            stderr,
        };
        let startup_deadline = std::time::Instant::now() + Duration::from_secs(300);
        while std::time::Instant::now() < startup_deadline {
            if ready_request(
                &mut reader.child,
                &reader.stderr,
                reader.client.get(format!("{}/readyz", reader.base)),
            )
            .await
            .is_some_and(|status| status == 200)
            {
                let body: Value = reader
                    .client
                    .get(format!("{}/readyz", reader.base))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                let keys: std::collections::BTreeSet<_> = body
                    .as_object()
                    .unwrap()
                    .keys()
                    .map(String::as_str)
                    .collect();
                assert_eq!(
                    keys,
                    [
                        "ready",
                        "publication_id",
                        "lance_version",
                        "state",
                        "warm_ready"
                    ]
                    .into_iter()
                    .collect(),
                    "unauthenticated readiness excludes storage and memory counters"
                );
                return reader;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("HTTP reader startup");
    }
    async fn rpc(&self, token: &str, id: u64, method: &str, params: Value) -> reqwest::Response {
        self.client
            .post(format!("{}/mcp", self.base))
            .bearer_auth(token)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", "2025-11-25")
            .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .send()
            .await
            .unwrap()
    }
    pub(super) async fn call(&self, token: &str, id: u64, name: &str, args: Value) -> Value {
        let r = self
            .rpc(
                token,
                id,
                "tools/call",
                json!({"name":name,"arguments":args}),
            )
            .await;
        assert_eq!(r.status(), 200);
        r.json::<Value>().await.unwrap()["result"].clone()
    }
    pub(super) async fn stop(&mut self) {
        let pid = self.child.id().to_string();
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        for _ in 0..300 {
            if let Some(exit) = self.child.try_wait().unwrap() {
                assert!(exit.success());
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("drain timeout");
    }
}

#[tokio::test]
#[ignore = "real HTTP MCP executable, rotating signed tokens and gated RustFS"]
async fn identity_http_cancellation_flood_preserves_ordinary_requests_on_real_s3() {
    use std::sync::atomic::AtomicBool;

    // Release a held proxy worker even when an assertion unwinds.
    struct ReleaseBoundary(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for ReleaseBoundary {
        fn drop(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }
    type Boundary = (
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    );
    let (_, issuer_root) = setup();
    let issuer = Issuer::new(&issuer_root).await;
    let (worker, _, root, _) = fixture(&issuer).await;
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let boundary = Arc::new(Mutex::new(None::<Boundary>));
    let captured = boundary.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if after && request.starts_with("GET ") && request.contains("/head.json ") {
            let held = captured.lock().unwrap().take();
            if let Some((entered, release)) = held {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
        false
    });
    let (endpoint, _proxy) = corrections::proxy_at_boundary(
        loaded.config.storage.endpoint.clone().unwrap(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let config = root.join("http-fairness.toml");
    std::fs::write(
        &config,
        std::fs::read_to_string(&worker)
            .unwrap()
            .replace(
                loaded.config.storage.endpoint.as_deref().unwrap(),
                &endpoint,
            )
            .replace("[reader]", "[reader]\nversion_check='off'")
            .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 0"),
    )
    .unwrap();
    let mut reader = Reader::start(&config, &root, &issuer, 4, 8).await;
    let ordinary_tokens: Vec<_> = (0..2)
        .map(|i| issuer.token(&format!("subject-{i}"), json!({}), json!({})))
        .collect();
    let mut expected = Vec::new();
    for (i, token) in ordinary_tokens.iter().enumerate() {
        let result = reader
            .search_task(token, 10 + i as u64, 10000)
            .await
            .unwrap();
        assert_eq!(result["isError"], false);
        let hits = result["structuredContent"]["hits"].clone();
        assert!(!hits.as_array().unwrap().is_empty());
        expected.push(hits);
    }

    // All tokens authenticate as the same flooding subject, but have different
    // signed claims/fingerprints. Each lane rotates on every notification.
    let rotated: Arc<Vec<_>> = Arc::new(
        (0..64)
            .map(|i| {
                issuer.token(
                    "subject-0",
                    json!({"jti":format!("rotation-{i}")}),
                    json!({}),
                )
            })
            .collect(),
    );
    assert_eq!(
        rotated
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        64
    );
    let stop = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::new(AtomicUsize::new(0));
    let limited = Arc::new(AtomicUsize::new(0));
    let progress = Arc::new(tokio::sync::Notify::new());
    let seen = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut floods = Vec::new();
    for lane in 0..4 {
        let (stop, completed, accepted, limited, progress, rotated) = (
            stop.clone(),
            completed.clone(),
            accepted.clone(),
            limited.clone(),
            progress.clone(),
            rotated.clone(),
        );
        let seen = seen.clone();
        let client = reader.client.clone();
        let url = format!("{}/mcp", reader.base);
        floods.push(tokio::spawn(async move {
            let mut rotation = lane;
            while !stop.load(Ordering::SeqCst) {
                let response = client.post(&url).bearer_auth(&rotated[rotation])
                    .header("Accept", "application/json, text/event-stream")
                    .header("MCP-Protocol-Version", "2025-11-25")
                    // Unknown IDs exercise the authenticated control path without
                    // asking it to cancel either ordinary foreground request.
                    .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":9000}}))
                    .send().await.unwrap();
                match response.status().as_u16() {
                    202 => { accepted.fetch_add(1, Ordering::SeqCst); }
                    429 => {
                        assert_eq!(response.json::<Value>().await.unwrap()["error"], "rate_limited");
                        limited.fetch_add(1, Ordering::SeqCst);
                    }
                    status => panic!("unexpected cancellation flood HTTP status {status}"),
                }
                seen.fetch_or(1u64 << rotation, Ordering::SeqCst);
                rotation = (rotation + 4) % rotated.len();
                completed.fetch_add(1, Ordering::SeqCst);
                progress.notify_one();
            }
        }));
    }
    for (i, token) in ordinary_tokens.iter().enumerate() {
        let (entered, at_boundary) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let release = ReleaseBoundary(Some(release));
        assert!(boundary.lock().unwrap().replace((entered, held)).is_none());
        let mut ordinary = reader.search_task(token, 70 + i as u64, 10000);
        tokio::select! {
            entered = at_boundary => entered.unwrap(),
            result = &mut ordinary => { result.unwrap(); panic!("ordinary request completed before its storage boundary"); },
        }
        // Observe a fresh flood batch while this authenticated request is held.
        // The flood can stop only after BOTH ordinary results have been checked.
        let target = completed.load(Ordering::SeqCst) + 64;
        while completed.load(Ordering::SeqCst) < target || seen.load(Ordering::SeqCst) != u64::MAX {
            tokio::select! {
                _ = progress.notified() => (),
                result = &mut ordinary => { result.unwrap(); panic!("ordinary request stopped at its held storage boundary"); },
            }
        }
        assert!(floods.iter().all(|task| !task.is_finished()));
        drop(release);
        let result = ordinary.await.unwrap();
        assert_eq!(result["isError"], false);
        assert!(
            result["structuredContent"]["hits"] == expected[i],
            "flood changed the authorized result"
        );
        assert!(!stop.load(Ordering::SeqCst));
        assert!(floods.iter().all(|task| !task.is_finished()));
    }
    stop.store(true, Ordering::SeqCst);
    for flood in floods {
        flood.await.unwrap();
    }
    assert!(
        accepted.load(Ordering::SeqCst) > 0,
        "valid controls were authenticated"
    );
    assert!(
        limited.load(Ordering::SeqCst) > 0,
        "flood exceeded the control allowance"
    );
    assert!(completed.load(Ordering::SeqCst) >= 128);
    reader.stop().await;
}

#[tokio::test]
#[ignore = "real HTTP MCP and faulted real RustFS boundaries; scripts/ci"]
async fn identity_http_deadline_cancellation_busy_inflight_refusal_and_active_drain_on_real_s3() {
    use std::sync::atomic::AtomicBool;
    let (_, issuer_root) = setup();
    let issuer = Issuer::new(&issuer_root).await;
    let (worker, commit, root, mut snapshot) = fixture(&issuer).await;
    let loaded = fyrr_core::config::load(&worker).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(AtomicUsize::new(0));
    let heads = Arc::new(AtomicUsize::new(0));
    let completed_heads = Arc::new(AtomicUsize::new(0));
    let active_heads = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(AtomicBool::new(false));
    let expired_source = Arc::new(AtomicBool::new(false));
    let stall_before = Arc::new(AtomicBool::new(false));
    let hook = {
        let gate = gate.clone();
        let heads = heads.clone();
        let completed_heads = completed_heads.clone();
        let active_heads = active_heads.clone();
        let entered = entered.clone();
        let stall_before = stall_before.clone();
        Arc::new(move |request: &str, _: &[u8], after: bool| {
            if request.starts_with("GET ") && request.contains("/head.json ") {
                if !after {
                    active_heads.fetch_add(1, Ordering::SeqCst);
                    let n = heads.fetch_add(1, Ordering::SeqCst) + 1;
                    if stall_before.load(Ordering::SeqCst)
                        && gate
                            .compare_exchange(n, 0, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        entered.store(true, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_secs(2));
                    }
                } else {
                    let wanted = gate.load(Ordering::SeqCst);
                    if wanted != 0
                        && heads.load(Ordering::SeqCst) == wanted
                        && gate
                            .compare_exchange(wanted, 0, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        entered.store(true, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_secs(2));
                    }
                    completed_heads.fetch_add(1, Ordering::SeqCst);
                    active_heads.fetch_sub(1, Ordering::SeqCst);
                }
            }
            false
        }) as super::corrections::BoundaryHook
    };
    let transform: super::corrections::ResponseTransform = {
        let expired_source = expired_source.clone();
        Arc::new(move |request, response| {
            if expired_source.load(Ordering::SeqCst)
                && request.starts_with("GET ")
                && request.contains("/head.json ")
                && response.starts_with(b"HTTP/1.1 200")
            {
                super::corrections::rewrite_ok_body(response, |body| {
                    let mut head: ServingHead = serde_json::from_slice(body).unwrap();
                    for source in &mut head.freshness.sources {
                        if source.source_id == "incremental" {
                            source.permissions_read_at =
                                chrono::Utc::now() - chrono::Duration::hours(2);
                            source.members_read_at = Some(source.permissions_read_at);
                        }
                    }
                    *body = serde_json::to_vec(&head).unwrap();
                    true
                });
            }
        })
    };
    let (endpoint, proxy) = super::corrections::proxy_transform(
        loaded.config.storage.endpoint.clone().unwrap(),
        stop.clone(),
        transform,
        Some(hook),
    );
    let mut proxy_guard = proxy;
    proxy_guard.resume = Arc::new(AtomicBool::new(false));
    let config = root.join("http-faulted.toml");
    let text = std::fs::read_to_string(&worker)
        .unwrap()
        .replace(
            loaded.config.storage.endpoint.as_deref().unwrap(),
            &endpoint,
        )
        .replace(
            "[reader]",
            "[reader]\nconcurrency=1\nversion_check_interval='10s'",
        )
        // This boundary test isolates request head reads from physical warming.
        // Mirror warming is exercised by the maintenance tests.
        .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 0");
    std::fs::write(&config, text).unwrap();
    let mut reader = Reader::start(&config, &root, &issuer, 1000, 1000).await;
    let token = issuer.token("subject-0", json!({}), json!({}));
    expired_source.store(true, Ordering::SeqCst);
    let hidden = reader
        .call(&token, 20, "search", json!({"query":"fixture query"}))
        .await;
    assert_eq!(hidden["isError"], false);
    assert!(
        hidden["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        hidden["structuredContent"]["metadata"]["hidden_sources"],
        json!(["incremental"])
    );
    let allowed = issuer.token("subject-2", json!({}), json!({}));
    let visible = reader
        .call(&allowed, 21, "search", json!({"query":"fixture query"}))
        .await;
    assert_eq!(visible["isError"], false);
    assert!(
        visible["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|h| h["source_id"] == "allowed")
    );
    assert!(
        !visible["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    expired_source.store(false, Ordering::SeqCst);
    // Restore the worker's source freshness context before isolating deadlines.
    let restored = reader
        .call(
            &token,
            22,
            "search",
            json!({"query":"fixture query","sources":["incremental"]}),
        )
        .await;
    assert_eq!(restored["isError"], false);
    assert!(
        restored["structuredContent"]["metadata"]["hidden_sources"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    // Live embedding has more than one worker. A single successful request
    // refreshes only its worker; start every deadline worker from the fresh head.
    let freshness_stderr = reader.stderr.clone();
    reader.stop().await;
    reader = Reader::start(&config, &root, &issuer, 1000, 1000).await;
    async fn blocked(entered: &AtomicBool) {
        for _ in 0..300 {
            if entered.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("real storage boundary was not reached");
    }
    let arm = |n| {
        let (heads, completed, active, entered, gate) =
            (&heads, &completed_heads, &active_heads, &entered, &gate);
        async move {
            assert_eq!(gate.load(Ordering::SeqCst), 0);
            let started = std::time::Instant::now();
            // Drain a cancelled request's proxy worker before observing the next
            // idle interval check. Only one reader is live at each gated phase.
            while active.load(Ordering::SeqCst) != 0 {
                assert!(started.elapsed() < Duration::from_secs(20));
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let before = completed.load(Ordering::SeqCst);
            while completed.load(Ordering::SeqCst) == before || active.load(Ordering::SeqCst) != 0 {
                assert!(started.elapsed() < Duration::from_secs(20));
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // The completed background check leaves a clean ten-second window;
            // the two-second gate now belongs to the request's chosen head read.
            heads.store(0, Ordering::SeqCst);
            entered.store(false, Ordering::SeqCst);
            gate.store(n, Ordering::SeqCst);
        }
    };
    arm(2).await;
    let deadline = reader.search_task(&token, 1, 1000);
    blocked(&entered).await;
    assert_eq!(
        deadline.await.unwrap()["structuredContent"]["code"],
        "deadline_exceeded"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    arm(2).await;
    let cancelled = reader.search_task(&token, 2, 10000);
    blocked(&entered).await;
    let other_token = issuer.token("subject-1", json!({}), json!({}));
    let other_cancel = reader
        .client
        .post(format!("{}/mcp", reader.base))
        .bearer_auth(&other_token)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}))
        .send()
        .await
        .unwrap();
    assert_eq!(other_cancel.status(), 202);
    assert!(
        !cancelled.is_finished(),
        "another subject cannot cancel this request"
    );
    let wrong_id = reader
        .client
        .post(format!("{}/mcp", reader.base))
        .bearer_auth(&token)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .json(
            &json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"2"}}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_id.status(), 202);
    assert!(
        !cancelled.is_finished(),
        "string and numeric request IDs are distinct"
    );
    let notification = reader
        .client
        .post(format!("{}/mcp", reader.base))
        .bearer_auth(&token)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}))
        .send()
        .await
        .unwrap();
    assert_eq!(notification.status(), 202);
    assert_eq!(
        cancelled.await.unwrap()["structuredContent"]["code"],
        "cancelled"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    arm(3).await;
    let running = reader.search_task(&token, 3, 10000);
    blocked(&entered).await;
    let queued = reader.search_task(&token, 4, 10000);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let busy = reader.search_task(&token, 5, 10000).await.unwrap();
    assert_eq!(busy["structuredContent"]["code"], "busy");
    assert_eq!(running.await.unwrap()["isError"], false);
    assert_eq!(queued.await.unwrap()["isError"], false);
    arm(3).await;
    let expiring = issuer.token(
        "subject-0",
        json!({"exp":chrono::Utc::now().timestamp()+2}),
        json!({}),
    );
    let expired = reader.search_task(&expiring, 8, 10000);
    blocked(&entered).await;
    assert_eq!(
        expired.await.unwrap()["structuredContent"]["code"],
        "authorization_unavailable"
    );
    // Hold the old admitted head response, publish a revoke, then release it.
    arm(3).await;
    let inflight = reader.search_task(&token, 6, 10000);
    blocked(&entered).await;
    snapshot.users[1].active = false;
    snapshot.built_at = chrono::Utc::now();
    std::fs::write(root.join("http-directory.json"), write_contract(&snapshot)).unwrap();
    assert!(
        host_command(&commit)
            .args(["writer", "sync", "--directory"])
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(
        inflight.await.unwrap()["structuredContent"]["code"],
        "authorization_unavailable"
    );
    // Stall before the admission refresh reads G+1, after S was bound under G.
    // Rebinding and removal must refuse, without caching A under S/G+1.
    let subject = snapshot.users[0].subjects[0].clone();
    stall_before.store(true, Ordering::SeqCst);
    arm(3).await;
    let rebinding = reader.search_task(&token, 40, 10000);
    blocked(&entered).await;
    snapshot.users[0].subjects.clear();
    snapshot.users[2].subjects.push(subject.clone());
    snapshot.built_at = chrono::Utc::now();
    std::fs::write(root.join("http-directory.json"), write_contract(&snapshot)).unwrap();
    assert!(
        host_command(&commit)
            .args(["writer", "sync", "--directory"])
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(
        rebinding.await.unwrap()["structuredContent"]["code"],
        "authorization_unavailable"
    );
    let namespace = &loaded.config.namespace.name;
    let rebound = reader
        .call(
            &token,
            41,
            "fetch",
            json!({"source_id":"incremental","root_id":format!("{namespace}-http-2")}),
        )
        .await;
    assert_eq!(rebound["isError"], false);
    assert_eq!(
        rebound["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let old = reader
        .call(
            &token,
            42,
            "fetch",
            json!({"source_id":"incremental","root_id":format!("{namespace}-http-0")}),
        )
        .await;
    assert!(
        old["structuredContent"]["documents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    arm(3).await;
    let unbinding = reader.search_task(&token, 43, 10000);
    blocked(&entered).await;
    snapshot.users[2].subjects.retain(|s| s != &subject);
    snapshot.built_at = chrono::Utc::now();
    std::fs::write(root.join("http-directory.json"), write_contract(&snapshot)).unwrap();
    assert!(
        host_command(&commit)
            .args(["writer", "sync", "--directory"])
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(
        unbinding.await.unwrap()["structuredContent"]["code"],
        "authorization_unavailable"
    );
    assert_eq!(
        reader
            .rpc(&token, 44, "tools/list", json!({}))
            .await
            .status(),
        503
    );
    snapshot.users[0].subjects.push(subject);
    snapshot.users[0].subjects.push(
        fyrr_writer::directory::subject_key(&loaded.config, &issuer.url, "subject-linked").unwrap(),
    );
    snapshot.built_at = chrono::Utc::now();
    std::fs::write(root.join("http-directory.json"), write_contract(&snapshot)).unwrap();
    assert!(
        host_command(&commit)
            .args(["writer", "sync", "--directory"])
            .output()
            .unwrap()
            .status
            .success()
    );
    stall_before.store(false, Ordering::SeqCst);

    // A linked subject shares its canonical user's bucket. Authenticated
    // cancellation still works with either the user or group bucket exhausted.
    let linked = issuer.token("subject-linked", json!({}), json!({}));
    let group_peer = issuer.token("subject-2", json!({}), json!({}));
    // Keep unrelated readers' interval checks out of the shared fault gate.
    let initial_stderr = reader.stderr.clone();
    reader.stop().await;
    for (user_cap, group_cap, exhausted_token) in [(1, 1000, &linked), (1000, 1, &group_peer)] {
        let mut limited = Reader::start(&config, &root, &issuer, user_cap, group_cap).await;
        arm(3).await;
        let work = limited.search_task(&token, 60, 10000);
        blocked(&entered).await;
        assert_eq!(
            limited
                .rpc(exhausted_token, 61, "tools/list", json!({}))
                .await
                .status(),
            429
        );
        let started = std::time::Instant::now();
        let cancel = limited.client.post(format!("{}/mcp", limited.base)).bearer_auth(&token)
            .header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":60}})).send().await.unwrap();
        assert_eq!(cancel.status(), 202);
        assert_eq!(
            work.await.unwrap()["structuredContent"]["code"],
            "cancelled"
        );
        assert!(
            started.elapsed() < Duration::from_millis(1100),
            "control path stops work without waiting for the request bucket"
        );
        limited.stop().await;
    }

    // A bogus-ID flood is subject-limited before further JWT work. Another
    // subject can still cancel its real request through the bounded control path.
    let mut flood_reader = Reader::start(&config, &root, &issuer, 1000, 1000).await;
    arm(3).await;
    let legitimate = flood_reader.search_task(&group_peer, 70, 10000);
    blocked(&entered).await;
    for i in 0..50 {
        let r = flood_reader.client.post(format!("{}/mcp", flood_reader.base))
            .bearer_auth(&token).header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":9000+i}}))
            .send().await.unwrap();
        assert_eq!(r.status(), if i < 30 { 202 } else { 429 });
    }
    let r = flood_reader
        .client
        .post(format!("{}/mcp", flood_reader.base))
        .bearer_auth(&group_peer)
        .header("Accept", "application/json, text/event-stream")
        .json(
            &json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":70}}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    assert_eq!(
        legitimate.await.unwrap()["structuredContent"]["code"],
        "cancelled"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    *issuer.state.lock().unwrap() = (408, issuer.jwks.clone());
    let started = std::time::Instant::now();
    assert_eq!(flood_reader.client.post(format!("{}/mcp", flood_reader.base))
        .bearer_auth(&token).header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":9999}}))
        .send().await.unwrap().status(), 429);
    assert!(started.elapsed() < Duration::from_millis(1100));
    *issuer.state.lock().unwrap() = (200, issuer.jwks.clone());
    flood_reader.stop().await;

    // Expired trust forces a slow refresh; a second request's deadline must
    // include time queued on that same authenticator mutex, before any S3 work.
    let mut auth_reader = Reader::start(&config, &root, &issuer, 1000, 1000).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    *issuer.state.lock().unwrap() = (408, issuer.jwks.clone());
    let before = issuer.requests.load(Ordering::SeqCst);
    let client = auth_reader.client.clone();
    let url = format!("{}/mcp", auth_reader.base);
    let access = token.clone();
    let stalled = tokio::spawn(async move {
        client.post(url).bearer_auth(access).header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","id":80,"method":"tools/call","params":{"name":"search","arguments":{"query":"fixture query","deadline_ms":10000}}})).send().await.unwrap()
    });
    for _ in 0..100 {
        if issuer.requests.load(Ordering::SeqCst) > before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(issuer.requests.load(Ordering::SeqCst) > before);
    let s3_before = heads.load(Ordering::SeqCst);
    let started = std::time::Instant::now();
    let short = auth_reader
        .rpc(
            &token,
            81,
            "tools/call",
            json!({"name":"search","arguments":{"query":"fixture query","deadline_ms":100}}),
        )
        .await;
    assert_eq!(short.status(), 408);
    assert_eq!(
        short.json::<Value>().await.unwrap()["error"],
        "deadline_exceeded"
    );
    assert!(started.elapsed() < Duration::from_millis(1100));
    assert_eq!(
        heads.load(Ordering::SeqCst),
        s3_before,
        "authentication timeout did no authorization or tool work"
    );
    assert_eq!(stalled.await.unwrap().status(), 503);
    *issuer.state.lock().unwrap() = (200, issuer.jwks.clone());
    auth_reader.stop().await;

    reader = Reader::start(&config, &root, &issuer, 1000, 1000).await;
    arm(2).await;
    let draining = reader.search_task(&token, 7, 10000);
    blocked(&entered).await;
    assert!(
        Command::new("kill")
            .args(["-TERM", &reader.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(
        reader.child.try_wait().unwrap().is_none(),
        "active request must drain"
    );
    assert_eq!(draining.await.unwrap()["isError"], false);
    for _ in 0..300 {
        if reader.child.try_wait().unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(reader.child.try_wait().unwrap().is_some());
    for stderr in [freshness_stderr, initial_stderr, reader.stderr.clone()] {
        let log = std::fs::read_to_string(stderr).unwrap();
        for private in [
            &token[..],
            "subject-0",
            "http-0@example.test",
            "owned-http-operator-canary",
        ] {
            assert!(!log.contains(private));
        }
    }
    drop(proxy_guard);
}

impl Drop for Reader {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
async fn fixture(issuer: &Issuer) -> (PathBuf, PathBuf, FixtureRoot, DirectorySnapshot) {
    fixture_for_issuer(&issuer.url).await
}
pub(super) async fn fixture_for_issuer(
    issuer_url: &str,
) -> (PathBuf, PathBuf, FixtureRoot, DirectorySnapshot) {
    static NAMESPACE: AtomicUsize = AtomicUsize::new(0);
    let namespace = format!("http-fixture-{}", NAMESPACE.fetch_add(1, Ordering::SeqCst));
    let (worker, commit, root) =
        super::incremental::fixture_host_in_namespace(768, true, &namespace);
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let config = &loaded.config;
    let mut docs = Vec::new();
    let mut perms = Vec::new();
    let mut users = Vec::new();
    let mut canonical = Vec::new();
    for i in 0..4 {
        let email = format!("http-{i}@example.test");
        let id = format!("u{i}");
        let doc = format!("{}-http-{i}", config.namespace.name);
        let mut document = super::incremental::doc(&doc, "r1", &format!("fixture document {i}"));
        document["readers"] = json!([format!("usr:{id}")]);
        docs.push(document);
        perms.push(json!({"id":doc,"revision":"p1","readers":[format!("usr:{id}")]}));
        users.push(
            json!({"id":id,"email":email,"email_verification":"verified_corporate","active":true}),
        );
        canonical.push(DirectoryUser {
            key: principal_hash(config, "key", &email),
            token: principal_hash(config, "tok", &format!("usr:{email}")),
            active: true,
            aliases: vec![],
            subjects: vec![
                fyrr_writer::directory::subject_key(config, issuer_url, &format!("subject-{i}"))
                    .unwrap(),
            ],
        });
    }
    super::incremental::input(
        &root,
        json!({"epoch":1,"content":[docs],"permissions":[perms],"members":[{"users":users,"groups":[]}]}),
    );
    worker_sync(
        &worker,
        &[
            "incremental",
            "--kind",
            "content",
            "--kind",
            "permissions",
            "--kind",
            "members",
        ],
    );
    super::incremental::publish(&commit);
    let mut snapshot = DirectorySnapshot {
        users: canonical,
        ..super::identity::snapshot(config, "snapshot")
    };
    snapshot.groups.push(DirectoryGroup {
        token: principal_hash(config, "tok", "grp:team"),
        members: snapshot.users.iter().map(|u| u.key.clone()).collect(),
    });
    snapshot.groups.push(DirectoryGroup {
        token: principal_hash(config, "tok", "grp:allow"),
        members: vec![snapshot.users[2].key.clone()],
    });
    let path = root.join("http-directory.json");
    std::fs::write(&path, write_contract(&snapshot)).unwrap();
    for file in [&worker, &commit] {
        std::fs::write(file,format!("{}\n[identity.directory]\nprovider='snapshot'\nsnapshot_file='{}'\nsync_interval='5m'\n",std::fs::read_to_string(file).unwrap(),path.display())).unwrap();
    }
    let pull = host_command(&commit)
        .args(["writer", "sync", "--directory"])
        .output()
        .unwrap();
    assert!(
        pull.status.success(),
        "{}",
        String::from_utf8_lossy(&pull.stderr)
    );
    (worker, commit, root, snapshot)
}
#[tokio::test]
#[ignore = "real HTTP MCP executable, pinned JWKS and RustFS; scripts/ci"]
async fn identity_http_authentication_isolation_rate_limits_drain_and_refresh_on_real_s3() {
    let (_, issuer_root) = setup();
    let issuer = Issuer::new(&issuer_root).await;
    let (worker, commit, root, mut snapshot) = fixture(&issuer).await;
    std::fs::write(
        &worker,
        std::fs::read_to_string(&worker)
            .unwrap()
            .replace("[reader]", "[reader]\nversion_check='off'"),
    )
    .unwrap();
    let namespace = fyrr_core::config::load(&worker)
        .unwrap()
        .config
        .namespace
        .name;
    let mut reader = Reader::start(&worker, &root, &issuer, 1000, 1000).await;
    let tokens: Vec<_> = (0..4)
        .map(|i| issuer.token(&format!("subject-{i}"), json!({}), json!({})))
        .collect();
    let challenge = reader
        .client
        .post(format!("{}/mcp", reader.base))
        .send()
        .await
        .unwrap();
    assert_eq!(challenge.status(), 401);
    assert!(challenge.headers().contains_key("www-authenticate"));
    for path in ["/mcp/", "/mcp/extra", "/mcp%2Fextra"] {
        let response=reader.client.post(format!("{}{path}",reader.base)).header("Accept","application/json, text/event-stream").json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch","arguments":{"source_id":"allowed","root_id":"0.txt"}}})).send().await.unwrap();
        assert_eq!(
            response.status(),
            404,
            "only the authenticated exact route may serve MCP"
        );
    }
    let metadata: Value = reader
        .client
        .get(format!(
            "{}/.well-known/oauth-protected-resource",
            reader.base
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(metadata["resource"], format!("{}/mcp", reader.base));
    let tools = reader
        .rpc(&tokens[0], 1, "tools/list", json!({}))
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(tools["result"], fyrr_core::mcp::tools_list());
    for (claims, header) in [
        (json!({"iss":"https://other.invalid"}), json!({})),
        (json!({"aud":"other"}), json!({})),
        (json!({"aud":["other", "account"]}), json!({})),
        (json!({"aud":[]}), json!({})),
        (json!({"scope":"other"}), json!({})),
        (json!({"exp":1}), json!({})),
        (json!({"nbf":chrono::Utc::now().timestamp()+600}), json!({})),
        (json!({}), json!({"typ":"JWT"})),
        (json!({}), json!({"alg":"HS256"})),
        (json!({}), json!({"alg":"none"})),
        (json!({}), json!({"kid":"x".repeat(129)})),
        (json!({}), json!({"kid":"unknown"})),
        (json!({"sub":""}), json!({})),
        (json!({"exp":null}), json!({})),
    ] {
        let token = issuer.token("subject-0", claims, header);
        assert_eq!(
            reader
                .rpc(&token, 2, "tools/list", json!({}))
                .await
                .status(),
            401
        );
    }
    // RFC 7519 §4.1.3: `aud` is a string or an array that contains the audience.
    for audience in [
        json!("fyrr-http-test"),
        json!(["fyrr-http-test"]),
        json!(["account", "fyrr-http-test"]),
    ] {
        let token = issuer.token("subject-0", json!({ "aud": audience }), json!({}));
        assert_eq!(
            reader
                .rpc(&token, 2, "tools/list", json!({}))
                .await
                .status(),
            200,
            "aud {audience}"
        );
    }
    let mut bad_signature = tokens[0].clone();
    let start = bad_signature.rfind('.').unwrap() + 1;
    bad_signature.replace_range(
        start..start + 1,
        if &bad_signature[start..start + 1] == "A" {
            "B"
        } else {
            "A"
        },
    );
    assert_eq!(
        reader
            .rpc(&bad_signature, 2, "tools/list", json!({}))
            .await
            .status(),
        401
    );
    // An untrusted header URL never overrides the operator-pinned JWKS endpoint.
    let untrusted_url = issuer.token(
        "subject-0",
        json!({}),
        json!({"jku":"http://127.0.0.1:9/evil"}),
    );
    assert_eq!(
        reader
            .rpc(&untrusted_url, 2, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    tokio::time::sleep(Duration::from_millis(110)).await;
    let unknown = issuer.token("subject-0", json!({}), json!({"kid":"unknown"}));
    let before = issuer.requests.load(Ordering::SeqCst);
    let misses = futures::future::join_all(
        (0..10).map(|i| reader.rpc(&unknown, 800 + i, "tools/list", json!({}))),
    )
    .await;
    assert!(misses.iter().all(|r| r.status() == 401));
    assert_eq!(
        issuer.requests.load(Ordering::SeqCst) - before,
        1,
        "single-flight refresh"
    );
    // Rotation uses only the pinned endpoint, and rejects the retired kid.
    let mut rotated = issuer.jwks.clone();
    rotated["keys"][0]["kid"] = json!("rotated");
    *issuer.state.lock().unwrap() = (200, rotated);
    tokio::time::sleep(Duration::from_millis(110)).await;
    let rotated_token = issuer.token("subject-0", json!({}), json!({"kid":"rotated"}));
    assert_eq!(
        reader
            .rpc(&rotated_token, 2, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    assert_eq!(
        reader
            .rpc(&tokens[0], 2, "tools/list", json!({}))
            .await
            .status(),
        401
    );
    *issuer.state.lock().unwrap() = (200, issuer.jwks.clone());
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(
        reader
            .rpc(&tokens[0], 2, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    let key = issuer.jwks["keys"][0].clone();
    let mut enc = key.clone();
    enc["use"] = json!("enc");
    let mut alg = key.clone();
    alg["alg"] = json!("RS512");
    let mut sign = key.clone();
    sign["key_ops"] = json!(["sign"]);
    let mut short = key.clone();
    short["n"] = json!("AA");
    for (jwks, expected) in [
        (json!({"keys":[]}), 503),
        (json!({"keys":[key.clone(), key]}), 503),
        (json!({"keys":[enc]}), 401),
        (json!({"keys":[alg]}), 401),
        (json!({"keys":[sign]}), 401),
        (json!({"keys":[short]}), 401),
        (json!({"keys":[],"padding":"x".repeat(256*1024)}), 503),
    ] {
        *issuer.state.lock().unwrap() = (200, jwks);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(
            reader
                .rpc(&tokens[0], 2, "tools/list", json!({}))
                .await
                .status(),
            expected
        );
    }
    *issuer.state.lock().unwrap() = (302, issuer.jwks.clone());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let before = issuer.requests.load(Ordering::SeqCst);
    assert_eq!(
        reader
            .rpc(&tokens[0], 2, "tools/list", json!({}))
            .await
            .status(),
        503
    );
    assert_eq!(
        issuer.requests.load(Ordering::SeqCst) - before,
        1,
        "no JWKS redirects"
    );
    *issuer.state.lock().unwrap() = (408, issuer.jwks.clone());
    tokio::time::sleep(Duration::from_millis(110)).await;
    let started = std::time::Instant::now();
    assert_eq!(
        reader
            .rpc(&tokens[0], 2, "tools/list", json!({}))
            .await
            .status(),
        503
    );
    assert!(
        started.elapsed() < Duration::from_millis(3500),
        "bounded JWKS timeout"
    );
    *issuer.state.lock().unwrap() = (200, issuer.jwks.clone());
    tokio::time::sleep(Duration::from_millis(110)).await;
    let unmapped = issuer.token("unmapped", json!({}), json!({}));
    assert_eq!(
        reader
            .rpc(&unmapped, 3, "tools/list", json!({}))
            .await
            .status(),
        503
    );
    // Probes may send any Host to /readyz; they get the status code only.
    let probe = reader
        .client
        .get(format!("{}/readyz", reader.base))
        .header("Host", "10.0.0.7:8080")
        .send()
        .await
        .unwrap();
    assert_eq!(probe.status(), 200);
    assert!(
        probe.bytes().await.unwrap().is_empty(),
        "a foreign Host never sees the publication"
    );
    // Every other route keeps Host enforcement, with a distinct code.
    let wrong_host = reader
        .client
        .post(format!("{}/mcp", reader.base))
        .header("Host", "untrusted.invalid")
        .bearer_auth(&tokens[0])
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_host.status(), 403);
    assert_eq!(
        wrong_host.json::<Value>().await.unwrap()["error"],
        "host_mismatch"
    );
    for path in ["/admin/refresh", "/.well-known/oauth-protected-resource"] {
        let request = if path.starts_with("/admin") {
            reader.client.post(format!("{}{path}", reader.base))
        } else {
            reader.client.get(format!("{}{path}", reader.base))
        };
        assert_eq!(
            request
                .header("Host", "untrusted.invalid")
                .send()
                .await
                .unwrap()
                .status(),
            403,
            "{path}"
        );
    }
    assert_eq!(
        reader
            .client
            .get(format!("{}/readyz", reader.base))
            .header("Origin", "https://browser.invalid")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    for round in 0..10 {
        let replies = futures::future::join_all(tokens.iter().map(|t| {
            reader.call(
                t,
                100 + round as u64,
                "search",
                json!({"query":"fixture query","sources":["incremental"],"limit":5}),
            )
        }))
        .await;
        for (i, result) in replies.iter().enumerate() {
            assert_eq!(
                result["isError"], false,
                "bounded error: {}",
                result["structuredContent"]["code"]
            );
            let hits = result["structuredContent"]["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0]["root_id"], format!("{namespace}-http-{i}"));
        }
    }
    let bad = reader
        .call(&tokens[0], 8, "search", json!({"query":"x","limit":76}))
        .await;
    assert_eq!(bad["structuredContent"]["code"], "invalid_argument");
    let fetched = reader
        .call(
            &tokens[1],
            9,
            "fetch",
            json!({"source_id":"incremental","root_id":format!("{namespace}-http-1")}),
        )
        .await;
    assert_eq!(fetched["isError"], false);
    let record = &fetched["structuredContent"]["documents"][0]["records"][0];
    let window = reader.call(&tokens[1], 10, "fetch", json!({"source_id":"incremental","root_id":format!("{namespace}-http-1"),"window":{"record_id":record["record_id"],"revision":record["revision"],"start":0,"end":7}})).await;
    assert_eq!(
        window["structuredContent"]["documents"][0]["records"][0]["text"],
        "fixture"
    );
    let changed = reader.call(&tokens[1], 11, "fetch", json!({"source_id":"incremental","root_id":format!("{namespace}-http-1"),"window":{"record_id":record["record_id"],"revision":"stale","start":0,"end":7}})).await;
    assert_eq!(changed["structuredContent"]["document_changed"], true);
    let cursor = reader
        .call(&tokens[1], 12, "fetch", json!({"cursor":"untrusted"}))
        .await;
    assert_eq!(cursor["structuredContent"]["code"], "stale_cursor");
    let filters = reader.call(&tokens[1], 13, "list_filters", json!({})).await;
    assert_eq!(filters["isError"], false);
    assert_eq!(
        filters["structuredContent"]["source_types"]["values"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    // The Content-Length guard rejects before consuming a body. Uploading a
    // megabyte concurrently races that early response and can give the client
    // BrokenPipe instead of the 413. Headers alone exercise the same guard.
    {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let host = reader.base.strip_prefix("http://").unwrap();
        let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
        stream.write_all(format!("POST /mcp HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: 1048577\r\nConnection: close\r\n\r\n",tokens[1]).as_bytes()).await.unwrap();
        let mut status = String::new();
        tokio::time::timeout(
            Duration::from_secs(15),
            tokio::io::BufReader::new(stream).read_line(&mut status),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            status.starts_with("HTTP/1.1 413 "),
            "oversized headers must be rejected without a body"
        );
    }
    assert_eq!(
        reader
            .client
            .post(format!("{}/admin/refresh", reader.base))
            .bearer_auth(&tokens[0])
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let refresh = reader
        .client
        .post(format!("{}/admin/refresh", reader.base))
        .bearer_auth("owned-http-operator-canary")
        .send()
        .await
        .unwrap();
    assert_eq!(refresh.status(), 200);
    let publication: Value = refresh.json().await.unwrap();
    assert_eq!(
        reader
            .client
            .post(format!("{}/admin/refresh", reader.base))
            .bearer_auth("owned-http-operator-canary")
            .send()
            .await
            .unwrap()
            .status(),
        429
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    let repeated: Value = reader
        .client
        .post(format!("{}/admin/refresh", reader.base))
        .bearer_auth("owned-http-operator-canary")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(repeated, publication);
    // A normal content update stays pinned in off mode until operator refresh.
    let mut input: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    input["epoch"] = json!(2);
    input["content"][0][3]["revision"] = json!("r2");
    std::fs::write(root.join("input.json"), serde_json::to_vec(&input).unwrap()).unwrap();
    worker_sync(&worker, &["incremental", "--kind", "content"]);
    let updated = super::incremental::publish(&commit);
    let serving: Value = reader
        .client
        .get(format!("{}/readyz", reader.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(serving["publication_id"], publication["publication_id"]);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let refreshed: Value = reader
        .client
        .post(format!("{}/admin/refresh", reader.base))
        .bearer_auth("owned-http-operator-canary")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(refreshed["publication_id"], publication["publication_id"]);
    assert_eq!(refreshed["publication_id"], updated["publication_id"]);
    // Revocation is published by the real pull and observed by the running process.
    snapshot.users[0].active = false;
    snapshot.built_at = chrono::Utc::now();
    std::fs::write(root.join("http-directory.json"), write_contract(&snapshot)).unwrap();
    assert!(
        host_command(&commit)
            .args(["writer", "sync", "--directory"])
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(
        reader
            .rpc(&tokens[0], 9, "tools/list", json!({}))
            .await
            .status(),
        503
    );
    assert_eq!(
        reader
            .call(
                &tokens[1],
                10,
                "fetch",
                json!({"source_id":"incremental","root_id":format!("{namespace}-http-0")})
            )
            .await["structuredContent"]["documents"],
        json!([])
    );
    // A trust outage after the configured key lifetime refuses old keys.
    *issuer.state.lock().unwrap() = (503, json!({}));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        reader
            .rpc(&tokens[1], 11, "tools/list", json!({}))
            .await
            .status(),
        503
    );
    *issuer.state.lock().unwrap() = (200, issuer.jwks.clone());
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(
        reader
            .rpc(&tokens[1], 12, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    assert!(issuer.requests.load(Ordering::SeqCst) > 1);
    reader.stop().await;
    let logs = std::fs::read_to_string(&reader.stderr).unwrap();
    assert!(
        logs.contains("http refused code=host_mismatch"),
        "refused Host names are logged with their own code"
    );
    assert!(!logs.contains("untrusted.invalid"));
    for marker in tokens.iter().map(String::as_str).chain([
        "subject-0",
        "http-0@example.test",
        "owned-http-operator-canary",
    ]) {
        assert!(!logs.contains(marker));
    }
    let mut user_limited = Reader::start(&worker, &root, &issuer, 2, 1000).await;
    for id in 0..2 {
        assert_eq!(
            user_limited
                .rpc(&tokens[1], id, "tools/list", json!({}))
                .await
                .status(),
            200
        );
    }
    assert_eq!(
        user_limited
            .rpc(&tokens[1], 3, "tools/list", json!({}))
            .await
            .status(),
        429
    );
    user_limited.stop().await;
    let mut group_limited = Reader::start(&worker, &root, &issuer, 1000, 2).await;
    assert_eq!(
        group_limited
            .rpc(&tokens[1], 1, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    assert_eq!(
        group_limited
            .rpc(&tokens[2], 2, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    assert_eq!(
        group_limited
            .rpc(&tokens[3], 3, "tools/list", json!({}))
            .await
            .status(),
        429
    );
    group_limited.stop().await;
    // Without an operator credential the refresh route does not exist.
    let mut keycloak =
        Reader::start_configured(&worker, &root, &issuer, 1000, 1000, 2, true, false).await;
    assert_eq!(
        keycloak
            .client
            .post(format!("{}/admin/refresh", keycloak.base))
            .bearer_auth("owned-http-operator-canary")
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let access = issuer.token("subject-1", json!({"typ":"Bearer"}), json!({"typ":"JWT"}));
    assert_eq!(
        keycloak
            .rpc(&access, 1, "tools/list", json!({}))
            .await
            .status(),
        200
    );
    for claims in [json!({"typ":"ID"}), json!({"typ":null})] {
        let id_token = issuer.token("subject-1", claims, json!({"typ":"JWT"}));
        assert_eq!(
            keycloak
                .rpc(&id_token, 2, "tools/list", json!({}))
                .await
                .status(),
            401
        );
    }
    keycloak.stop().await;
    let keycloak_log = std::fs::read_to_string(&keycloak.stderr).unwrap();
    assert!(!keycloak_log.contains(&access) && !keycloak_log.contains("subject-1"));
    // Same principal, group and source tokens with one key differ across namespaces.
    let (other_worker, _, other_root, _) = fixture(&issuer).await;
    let a = fyrr_core::config::load(&worker).unwrap();
    let b = fyrr_core::config::load(&other_worker).unwrap();
    for (kind, value) in [
        ("key", "http-1@example.test"),
        ("tok", "grp:all"),
        ("tok", "incremental:usr:u1"),
    ] {
        assert_ne!(
            principal_hash(&a.config, kind, value),
            principal_hash(&b.config, kind, value)
        );
    }
    let mut other = Reader::start(&other_worker, &other_root, &issuer, 1000, 1000).await;
    let result = other
        .call(
            &tokens[1],
            1,
            "search",
            json!({"query":"fixture query","sources":["incremental"]}),
        )
        .await;
    assert_eq!(
        result["structuredContent"]["hits"][0]["root_id"],
        format!("{}-http-1", b.config.namespace.name)
    );
    other.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real CLI, local issuer and RustFS; scripts/ci"]
async fn maintenance_native_cache_resizes_for_growing_publication_on_real_s3() {
    use object_store::ObjectStoreExt;
    let (_, issuer_root) = setup();
    let issuer = Issuer::new(&issuer_root).await;
    let (worker, commit, root, _) = fixture_for_issuer(&issuer.url).await;
    let config = root.join("cache-growth.toml");
    std::fs::write(
        &config,
        std::fs::read_to_string(&worker)
            .unwrap()
            .replace(
                "[reader]",
                "[reader]\nversion_check='interval'\nversion_check_interval='100ms'\n",
            )
            .replace("mirror_max_bytes = 16777216", "mirror_max_bytes = 0"),
    )
    .unwrap();
    let mut reader = Reader::start(&config, &root, &issuer, 1000, 1000).await;
    let http_config = root.join(format!(
        "http-{}.toml",
        reader.base.strip_prefix("http://").unwrap()
    ));
    let initial = host_command(&http_config)
        .args(["reader", "check"])
        .env("FYRR_HTTP_OPERATOR", "owned-http-operator-canary")
        .output()
        .unwrap();
    assert!(
        initial.status.success(),
        "{}",
        String::from_utf8_lossy(&initial.stderr)
    );
    let initial: Value = serde_json::from_slice(&initial.stdout).unwrap();
    assert_eq!(initial["state"], "serving");
    let initial_capacity = initial["native_cache_available_bytes"].as_u64().unwrap();
    let token = issuer.token("subject-0", json!({}), json!({}));
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let old_uuids = super::coordination::publication(&commit)
        .await
        .1
        .indexes
        .into_iter()
        .map(|i| i.uuid)
        .collect::<Vec<_>>();
    // Add an unused 32-MiB immutable file beneath each NEW index UUID before the writer
    // inventories it. The real fenced commit publishes its real byte count;
    // index query bytes and authorization artifacts remain valid and unchanged.
    let padded = Arc::new(Mutex::new(std::collections::BTreeSet::<String>::new()));
    let observed = padded.clone();
    let storage_config = loaded.config.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if !after && request.starts_with("GET ") {
            let url = reqwest::Url::parse(&format!(
                "http://fixture{}",
                request.split_whitespace().nth(1).unwrap()
            ))
            .unwrap();
            if let Some((_, prefix)) = url.query_pairs().find(|(key, value)| {
                key == "prefix"
                    && value.contains("/dataset.lance/_indices/")
                    && !old_uuids
                        .iter()
                        .any(|uuid| value.trim_end_matches('/').ends_with(uuid))
            }) && observed.lock().unwrap().insert(prefix.to_string())
            {
                assert!(
                    observed.lock().unwrap().len() <= 32,
                    "bounded synthetic index padding"
                );
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(async {
                    let (store, _) =
                        fyrr_reader::storage::namespace_store(&storage_config).unwrap();
                    store
                        .put(
                            &object_store::path::Path::from(format!(
                                "{prefix}/cache-growth-fixture.bin"
                            )),
                            vec![0u8; 32 * 1024 * 1024].into(),
                        )
                        .await
                        .unwrap();
                });
            }
        }
        false
    });
    let (endpoint, guard) = corrections::proxy_at_boundary(
        loaded.config.storage.endpoint.clone().unwrap(),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
        hook,
    );
    let proxied = corrections::proxied(&commit, &endpoint);
    let id = format!("{}-http-0", loaded.config.namespace.name);
    let mut document = super::incremental::doc(&id, "r2", "fixture document 0");
    document["expected_prior_revision"] = json!("r1");
    document["readers"] = json!(["usr:u0"]);
    super::incremental::input(
        &root,
        json!({"epoch":2,"content":[[document]],"permissions":[[{"id":id,"revision":"p2","readers":["usr:u0"]}]]}),
    );
    worker_sync(
        &worker,
        &["incremental", "--kind", "content", "--kind", "permissions"],
    );
    super::incremental::publish(&proxied);
    drop(guard);
    assert!(
        !padded.lock().unwrap().is_empty(),
        "writer must inventory the synthetic growth file"
    );
    let (_, manifest) = super::coordination::publication(&commit).await;
    assert!(
        manifest
            .files
            .iter()
            .any(|f| f.path.ends_with("/cache-growth-fixture.bin")
                && f.size_bytes == 32 * 1024 * 1024)
    );
    assert!(
        manifest
            .files
            .iter()
            .filter(|f| f.path.starts_with("_indices/"))
            .map(|f| f.size_bytes)
            .sum::<u64>()
            > initial_capacity
    );
    let started = std::time::Instant::now();
    loop {
        let ready: Value = reader
            .client
            .get(format!("{}/readyz", reader.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if ready["publication_id"] == manifest.publication_id {
            assert_eq!(ready["state"], "serving", "{ready}");
            assert_eq!(ready["warm_ready"], false);
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "reader must adopt growth without restart"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut expected = Reader::start(&config, &root, &issuer, 1000, 1000).await;
    let query = json!({"query":"fixture query","limit":5});
    let want = expected.call(&token, 1, "search", query.clone()).await;
    let got = reader.call(&token, 2, "search", query).await;
    assert_eq!(got["isError"], false, "{got}");
    assert!(
        !got["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        got["structuredContent"]["hits"],
        want["structuredContent"]["hits"]
    );
    let after = host_command(&http_config)
        .args(["reader", "check"])
        .env("FYRR_HTTP_OPERATOR", "owned-http-operator-canary")
        .output()
        .unwrap();
    assert!(after.status.success());
    let after: Value = serde_json::from_slice(&after.stdout).unwrap();
    assert!(after["native_cache_available_bytes"].as_u64().unwrap() > initial_capacity);
    assert!(
        !std::fs::read_to_string(&reader.stderr)
            .unwrap()
            .contains("reason=cache_budget")
    );
    expected.stop().await;
    reader.stop().await;
    issuer.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real S3, native request failure and HTTP readiness"]
async fn tools_failed_scheduler_refuses_readiness_on_real_s3() {
    let (_, issuer_root) = setup();
    let issuer = Issuer::new(&issuer_root).await;
    let (_, config, root, _) = fixture(&issuer).await;
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config, format!(
        "{text}\n[identity.oidc]\nissuer='{}'\naudience='fyrr-http-test'\njwks_url='{}/jwks'\n[reader.http]\nlisten='127.0.0.1:0'\npublic_url='http://127.0.0.1:1/mcp'\noperator_credential={{env='FYRR_HTTP_OPERATOR'}}\n",
        issuer.url, issuer.url,
    )).unwrap();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let subject =
        fyrr_writer::directory::subject_key(&loaded.config, &issuer.url, "subject-0").unwrap();
    let output = Command::new(std::env::var_os("FYRR_ROUTING_TEST_BINARY").unwrap())
        .args([
            "http::scheduler_tests::readiness_refuses_failed_scheduler_on_real_s3",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("FYRR_TEST_EMBED_CONFIG", &config)
        .env("FYRR_TEST_SCHEDULER_SUBJECT", subject)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    std::fs::remove_dir_all(root).unwrap();
}
