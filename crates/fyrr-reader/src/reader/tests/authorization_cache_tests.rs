use super::*;
use crate::serve::{ToolCall, cache_test_enqueue};
use std::sync::atomic::Ordering;

fn key(subject: &str) -> AuthorizationCacheKey {
    AuthorizationCacheKey {
        namespace: "budget".into(),
        subject: subject.into(),
        identity: "identity".into(),
        identity_generation: 1,
        policy: "policy".into(),
        policy_generation: 1,
        content: 1,
        hidden: Vec::new(),
    }
}
#[test]
fn authorization_cache_budget_exact_boundary_and_held_claim() {
    let mut cache = AuthorizationCache::default();
    let claim = cache
        .reserve(&key("one"), AUTHORIZATION_CACHE_MAX_BYTES, 1)
        .unwrap();
    assert_eq!(claim.num_permits(), AUTHORIZATION_CACHE_MAX_BYTES);
    assert_eq!(cache.budget.available_permits(), 0);
    let held = claim.clone();
    drop(claim);
    assert!(cache.reserve(&key("two"), 1, 1).is_none());
    drop(held);
    assert_eq!(
        cache.budget.available_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES
    );
    assert!(
        cache
            .reserve(&key("two"), AUTHORIZATION_CACHE_MAX_BYTES, 1)
            .is_some()
    );
}
#[test]
fn authorization_cache_budget_disabled_and_oversized_leave_allowance() {
    let mut cache = AuthorizationCache::default();
    assert!(cache.reserve(&key("one"), 1, 0).is_none());
    assert!(
        cache
            .reserve(&key("one"), AUTHORIZATION_CACHE_MAX_BYTES + 1, 1)
            .is_none()
    );
    assert_eq!(
        cache.budget.available_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES
    );
    let claim = cache.reserve(&key("one"), 4096, 1).unwrap();
    assert_eq!(
        cache.budget.available_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES - 4096
    );
    drop(claim);
    assert_eq!(
        cache.budget.available_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES
    );
}

fn writer(config: &std::path::Path, args: &[&str]) {
    let output = std::process::Command::new(std::env::var_os("FYRR_TEST_CACHE_WRITER").unwrap())
        .arg("--config")
        .arg(config)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "writer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
async fn request(
    service: &crate::serve::McpService,
    subject: &crate::auth::AuthenticatedSubject,
    args: &SearchInput,
) -> serde_json::Value {
    let reply = cache_test_enqueue(service, ToolCall::Search(args.clone()), subject.clone())
        .await
        .await
        .unwrap();
    assert_ne!(reply.is_error, Some(true), "{:?}", reply.structured_content);
    reply.structured_content.unwrap()
}
async fn held_boundary(
    gate: &crate::serve::TestGate,
    reply: &mut tokio::sync::oneshot::Receiver<rmcp::model::CallToolResult>,
) {
    tokio::select! {
        () = gate.entered.notified() => {},
        result = reply => {
            let result = result.unwrap();
            let code = result.structured_content.as_ref()
                .and_then(|value| value.get("code"))
                .and_then(serde_json::Value::as_str).unwrap_or("none");
            panic!("request completed before held boundary: {code}");
        }
    }
}
async fn filters(
    service: &crate::serve::McpService,
    subject: &crate::auth::AuthenticatedSubject,
) -> serde_json::Value {
    let reply = cache_test_enqueue(
        service,
        ToolCall::ListFilters(fyrr_core::mcp::ListFiltersInput { deadline_ms: None }),
        subject.clone(),
    )
    .await
    .await
    .unwrap();
    assert_ne!(reply.is_error, Some(true), "{:?}", reply.structured_content);
    let mut result = reply.structured_content.unwrap();
    result.as_object_mut().unwrap().remove("metadata");
    result
}
fn cache_subjects(
    config: &fyrr_core::config::Config,
    identity: &fyrr_core::publication::GenerationPointer,
) -> Vec<crate::auth::AuthenticatedSubject> {
    (0..3)
        .map(|i| crate::auth::AuthenticatedSubject {
            key: hash_principal(config, "key", &format!("cache-subject-{i}")).unwrap(),
            identity: Some(identity.clone()),
            api_key: false,
            expires: fyrr_core::clock::now().timestamp() + 3600,
        })
        .collect()
}

struct CachePressure {
    budget: Arc<tokio::sync::Semaphore>,
    pressure: tokio::sync::OwnedSemaphorePermit,
    held: Vec<Arc<CachedAdmission>>,
    remainder: tokio::sync::OwnedSemaphorePermit,
}
async fn hold_cache_budget(
    service: &crate::serve::McpService,
    subjects: &[crate::auth::AuthenticatedSubject],
    args: &SearchInput,
    cache: &Arc<std::sync::Mutex<AuthorizationCache>>,
    exact: &[serde_json::Value],
    exact_filters: &[serde_json::Value],
) -> CachePressure {
    let (budget, largest) = {
        let mut cache = cache.lock().unwrap();
        let largest = cache
            .entries
            .iter()
            .map(|(_, entry)| entry._cache_claim.num_permits())
            .max()
            .unwrap();
        cache.entries.clear();
        (cache.budget.clone(), largest)
    };
    assert_eq!(budget.available_permits(), AUTHORIZATION_CACHE_MAX_BYTES);
    // Other paid cache entries model a dense corpus without allocating fake rows.
    // Exactly two real subject copies fit; a third must evict the LRU first.
    let pressure = budget
        .clone()
        .try_acquire_many_owned(u32::try_from(AUTHORIZATION_CACHE_MAX_BYTES - 2 * largest).unwrap())
        .unwrap();
    for i in [0, 1, 0, 2] {
        assert_eq!(request(service, &subjects[i], args).await["hits"], exact[i]);
    }
    let held = {
        let cache = cache.lock().unwrap();
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.entries[0].0.subject, subjects[0].key);
        assert_eq!(cache.entries[1].0.subject, subjects[2].key);
        cache
            .entries
            .iter()
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>()
    };
    let remainder = budget
        .clone()
        .try_acquire_many_owned(u32::try_from(budget.available_permits()).unwrap())
        .unwrap();
    assert_eq!(request(service, &subjects[1], args).await["hits"], exact[1]);
    assert_eq!(filters(service, &subjects[1]).await, exact_filters[1]);
    assert!(
        cache.lock().unwrap().entries.is_empty(),
        "held evicted entries cannot pay for another copy"
    );
    assert_eq!(budget.available_permits(), 0);
    assert_eq!(
        held.iter()
            .map(|entry| entry._cache_claim.num_permits())
            .sum::<usize>()
            + pressure.num_permits()
            + remainder.num_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES,
    );

    CachePressure {
        budget,
        pressure,
        held,
        remainder,
    }
}

#[tokio::test]
#[ignore = "real writer publication supplied by the dense_path cache fixture"]
async fn authorization_cache_budget_on_storage() {
    let config_path = std::path::PathBuf::from(std::env::var_os("FYRR_TEST_CACHE_CONFIG").unwrap());
    let config = fyrr_core::config::load(&config_path).unwrap().config;
    let mut reader = PreparedReader::open(&config).await.unwrap();
    reader.prewarm().await.unwrap();
    let cache = reader.admission_cache.clone();
    let hooks = reader.test_hooks.clone();
    hooks.cache_warming.enabled.store(true, Ordering::SeqCst);
    let mut head = reader.head.clone();
    let mut manifest = reader.manifest.clone();
    let old_publication = manifest.publication_id.clone();
    let mut subjects = cache_subjects(&config, &head.identity);
    let args: SearchInput = serde_json::from_value(serde_json::json!({
        "query":"fixture query", "sources":["allowed", "denied"], "limit":10,
        "exclude":[{"source_id":"allowed", "root_id":"cache-next.txt"}],
    }))
    .unwrap();
    // Two workers binding one subject share the cache's allocation: a hit
    // copies nothing, and nothing a worker does can change another's mask.
    {
        let (mut first, mut second) = (
            reader.worker().await.unwrap(),
            reader.worker().await.unwrap(),
        );
        first.bind_subject(&subjects[0]).await.unwrap();
        second.bind_subject(&subjects[0]).await.unwrap();
        assert!(Arc::ptr_eq(&first.admission, &second.admission));
        let cached = cache
            .lock()
            .unwrap()
            .entries
            .iter()
            .find(|(key, _)| key.subject == subjects[0].key)
            .map(|(_, entry)| entry.admission.clone())
            .unwrap();
        assert!(Arc::ptr_eq(&cached, &first.admission));
        assert!(first.maps_enabled && first.admission.maps.is_some());
        cache.lock().unwrap().entries.clear();
    }
    let service = crate::serve::start(reader).await.unwrap();
    let mut exact = Vec::new();
    let mut exact_filters = Vec::new();
    for (i, subject) in subjects.iter().enumerate() {
        let result = request(&service, subject, &args).await;
        let hits = result["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 10);
        assert!(
            hits.iter()
                .all(|hit| hit["source_id"] == if i == 1 { "denied" } else { "allowed" })
        );
        exact.push(result["hits"].clone());
        exact_filters.push(filters(&service, subject).await);
    }
    let CachePressure {
        budget,
        pressure,
        held,
        remainder,
    } = hold_cache_budget(&service, &subjects, &args, &cache, &exact, &exact_filters).await;
    // Two actual subjects execute together while all cache permits are held.
    hooks.completion.enabled.store(true, Ordering::SeqCst);
    let mut first = cache_test_enqueue(
        &service,
        ToolCall::Search(args.clone()),
        subjects[0].clone(),
    )
    .await;
    held_boundary(&hooks.completion, &mut first).await;
    let mut second = cache_test_enqueue(
        &service,
        ToolCall::Search(args.clone()),
        subjects[1].clone(),
    )
    .await;
    held_boundary(&hooks.completion, &mut second).await;
    assert_eq!(budget.available_permits(), 0);
    assert!(cache.lock().unwrap().entries.is_empty());

    // Publish an actual native increment while old requests and cache entries remain held.
    let root = config_path.parent().unwrap();
    std::fs::write(root.join("sources/cache-next.txt"), "fixture document 0").unwrap();
    writer(&config_path, &["writer", "sync", "allowed"]);
    writer(&config_path, &["writer", "commit"]);
    service.refresh().await.unwrap();
    let (store, storage_root) = namespace_store(&config).unwrap();
    head = read_contract(store.as_ref(), &storage_root.clone().join("head.json"))
        .await
        .unwrap();
    assert_ne!(
        head.content.as_ref().unwrap().lance_version,
        manifest.lance_version
    );
    manifest = read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!(
            "{storage_root}/{}",
            head.content.as_ref().unwrap().manifest
        )),
    )
    .await
    .unwrap();
    assert_eq!(
        service.publication().await["publication_id"],
        manifest.publication_id
    );
    assert_eq!(budget.available_permits(), 0);
    for subject in &mut subjects {
        subject.identity = Some(head.identity.clone());
    }
    hooks.completion.enabled.store(false, Ordering::SeqCst);
    hooks.completion.release.add_permits(2);
    for (reply, expected) in [(first, &exact[0]), (second, &exact[1])] {
        let reply = reply.await.unwrap();
        assert_ne!(reply.is_error, Some(true));
        let result = reply.structured_content.unwrap();
        assert_eq!(&result["hits"], expected);
        assert_eq!(result["metadata"]["publication_id"], old_publication);
    }
    let current = request(&service, &subjects[2], &args).await;
    assert_eq!(current["hits"], exact[2]);
    assert_eq!(
        current["metadata"]["publication_id"],
        manifest.publication_id
    );
    assert!(cache.lock().unwrap().entries.is_empty());
    drop(remainder);
    drop(held);
    drop(pressure);
    assert_eq!(budget.available_permits(), AUTHORIZATION_CACHE_MAX_BYTES);
    assert_eq!(
        request(&service, &subjects[0], &args).await["hits"],
        exact[0]
    );
    assert!(
        cache
            .lock()
            .unwrap()
            .entries
            .iter()
            .any(|(key, _)| key.subject == subjects[0].key
                && key.content == manifest.lance_version)
    );

    // Hold after real retrieval, publish a revoke, then permit the final live recheck.
    hooks
        .authorization_output
        .enabled
        .store(true, Ordering::SeqCst);
    let mut revoked =
        cache_test_enqueue(&service, ToolCall::Search(args), subjects[0].clone()).await;
    held_boundary(&hooks.authorization_output, &mut revoked).await;
    let revoke = root.join("cache-revoke.json");
    writer(
        &config_path,
        &[
            "writer",
            "directory",
            "--snapshot",
            revoke.to_str().unwrap(),
        ],
    );
    hooks.authorization_output.release.add_permits(1);
    let reply = revoked.await.unwrap();
    assert_eq!(reply.is_error, Some(true));
    assert_eq!(
        reply.structured_content.unwrap()["code"],
        "authorization_unavailable"
    );
    assert!(cache.lock().unwrap().entries.is_empty());
    assert_eq!(budget.available_permits(), AUTHORIZATION_CACHE_MAX_BYTES);
    hooks.cache_warming.enabled.store(false, Ordering::SeqCst);
    hooks.cache_warming.release.add_permits(1);
}
