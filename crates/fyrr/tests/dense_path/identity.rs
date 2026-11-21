//! Canonical identity snapshots through real writer commands, MCP and RustFS.
use super::coordination::{host_command, publication, worker_sync};
use super::incremental::{fixture, initial, input, publish, publish_initial};
use super::tools::Mcp;
use super::*;
use fyrr_core::directory::{AccountLink, DirectoryGroup, DirectorySnapshot, DirectoryUser};
use fyrr_core::identity::{IdentityArtifact, IdentityInputKind};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

pub(super) fn user(config: &fyrr_core::config::Config, email: &str) -> DirectoryUser {
    DirectoryUser {
        key: principal_hash(config, "key", email),
        token: principal_hash(config, "tok", &format!("usr:{email}")),
        active: true,
        aliases: Vec::new(),
        subjects: Vec::new(),
    }
}
pub(super) fn snapshot(config: &fyrr_core::config::Config, provider: &str) -> DirectorySnapshot {
    DirectorySnapshot {
        schema_version: <DirectorySnapshot as fyrr_core::Contract>::VERSION,
        namespace: config.namespace.name.clone(),
        provider: provider.into(),
        built_at: fyrr_core::clock::now(),
        hashing: fyrr_core::identity::HashingScheme {
            algorithm: "hmac-sha256".into(),
            key_id: "namespace-v1".into(),
        },
        users: Vec::new(),
        groups: Vec::new(),
        links: Vec::new(),
        state: None,
    }
}
pub(super) async fn link_account(config: &Path, source: &str, account: &str) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let mut directory = snapshot(&loaded.config, &format!("{source}-test-directory"));
    directory.hashing = artifact(config).await.1.hashing;
    let principal = user(&loaded.config, "fixture-subject");
    directory.links.push(AccountLink {
        source: source.into(),
        user: principal_hash(&loaded.config, "key", account),
        principal: principal.key.clone(),
    });
    directory.users.push(principal);
    fyrr_writer::directory::publish(&loaded.config, &directory)
        .await
        .unwrap();
}

fn import(config: &Path, root: &Path, snapshot: &DirectorySnapshot) -> std::process::Output {
    let path = root.join("directory-input.json");
    std::fs::write(&path, write_contract(snapshot)).unwrap();
    host_command(config)
        .args(["writer", "directory", "--snapshot"])
        .arg(path)
        .output()
        .unwrap()
}
fn applied(config: &Path, root: &Path, snapshot: &DirectorySnapshot) -> Value {
    let result = import(config, root, snapshot);
    assert!(result.status.success(), "{}", report(&result));
    serde_json::from_slice(&result.stdout).unwrap()
}

fn measured_publication(config: &Path, root: &Path, snapshot: &DirectorySnapshot) -> Value {
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let hook: super::corrections::BoundaryHook = Arc::new(move |_, _, after| {
        if !after {
            count.fetch_add(1, Ordering::Relaxed);
        }
        false
    });
    let (proxy, thread) =
        super::corrections::proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
    let proxied = root.join("directory-measurement.toml");
    std::fs::write(
        &proxied,
        std::fs::read_to_string(config)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let output = import(&proxied, root, snapshot);
    stop.store(true, Ordering::Relaxed);
    drop(thread);
    assert!(output.status.success(), "{}", report(&output));
    let mut receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    receipt["s3_requests"] = json!(requests.load(Ordering::Relaxed));
    receipt
}
pub(super) async fn artifact(config: &Path) -> (ServingHead, IdentityArtifact) {
    let (head, _) = publication(config).await;
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, root) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let identity = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{root}/{}", head.identity.object)),
    )
    .await
    .unwrap();
    (head, identity)
}

#[tokio::test]
#[ignore = "real writer and local storage; scripts/ci"]
async fn identity_scim_namespace_creation_and_import_never_attest_without_proof_on_storage() {
    for (stdio, provider) in [
        (true, "none"),
        (false, "none"),
        (true, "scim"),
        (false, "scim"),
        (true, "keycloak"),
        (false, "keycloak"),
        (true, "snapshot"),
        (false, "snapshot"),
    ] {
        let (config, root) = setup();
        let mut text = std::fs::read_to_string(&config).unwrap();
        if !stdio {
            text = text.replace("stdio_user = \"fixture-subject\"\n", "");
        }
        std::fs::write(
            &config,
            format!("{text}\n[identity.directory]\nprovider='{provider}'\nissuer='https://issuer.one'\nendpoint='http://127.0.0.1:9'\nrealm='owned-test-realm'\nverified_domains=['example.test']\ncredentials={{env='DIRECTORY_UNUSED_CREDENTIAL'}}\nsnapshot_file='unused-snapshot.json'\n"),
        )
        .unwrap();
        success(&config, &["writer", "namespace", "create"]);
        let loaded = fyrr_core::config::load(&config).unwrap();
        let before = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        assert!(before.freshness.directory.is_none());
        success(&config, &["writer", "commit"]);
        assert!(
            fyrr_reader::storage::optional_head(&loaded.config)
                .await
                .unwrap()
                .unwrap()
                .freshness
                .directory
                .is_none()
        );
        let mut directory = snapshot(&loaded.config, "scim");
        directory.provider = provider.into();
        directory
            .users
            .push(user(&loaded.config, "new@example.test"));
        applied(&config, &root, &directory);
        success(&config, &["writer", "commit"]);
        let after = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        assert!(after.freshness.directory.is_none());
        assert!(after.content.is_none());
        assert!(after.identity.generation > before.identity.generation);
    }
}

#[tokio::test]
#[ignore = "real writer/MCP and local storage; scripts/ci"]
async fn identity_alias_collision_link_revoke_and_idempotence_on_storage() {
    let (worker, commit, root) = fixture();
    initial(&root);
    let mut data: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    data["members"][0]["users"][0]["email"] = json!("ALIAS@example.test");
    input(&root, data);
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
    publish(&commit);
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let config = &loaded.config;
    let mut directory = snapshot(config, "scim");
    let mut canonical = user(config, "fixture@example.test");
    canonical
        .aliases
        .push(principal_hash(config, "key", "alias@example.test"));
    canonical.subjects.push(
        fyrr_writer::directory::subject_key(config, "https://issuer.example", "subject-canary")
            .unwrap(),
    );
    directory.users.push(canonical.clone());
    let child = principal_hash(config, "tok", "grp:operator-child");
    let parent = principal_hash(config, "tok", "grp:operator-parent");
    directory.groups = vec![
        DirectoryGroup {
            token: child.clone(),
            members: vec![canonical.key.clone(), parent.clone()],
        },
        DirectoryGroup {
            token: parent.clone(),
            members: vec![child],
        },
    ];
    let (before, _) = artifact(&commit).await;
    assert_eq!(
        config.identity.directory.provider,
        fyrr_core::config::DirectoryProvider::None
    );
    assert!(
        before.freshness.directory.is_none(),
        "this regression must not acquire directory freshness during setup"
    );
    let mut wrong_namespace = directory.clone();
    wrong_namespace.namespace = "another-namespace".into();
    assert!(!import(&commit, &root, &wrong_namespace).status.success());
    assert_eq!(artifact(&commit).await.0.identity, before.identity);
    let mut client = Mcp::open(&worker);
    assert_eq!(
        fixture_hits(&mut client)["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    let receipt = applied(&commit, &root, &directory);
    assert_eq!(receipt["changed"], true);
    let (after, identity) = artifact(&commit).await;
    assert_eq!(
        after.content, before.content,
        "identity-only publication must not rewrite content"
    );
    assert_eq!(after.policy, before.policy);
    assert_eq!(
        after.freshness, before.freshness,
        "operator imports cannot attest freshness"
    );
    assert!(
        identity
            .inputs
            .iter()
            .any(|input| input.kind == IdentityInputKind::Directory)
    );
    assert_eq!(identity.subjects.len(), 1);
    assert_eq!(
        identity.principals[0].groups.len(),
        4,
        "two directory and two source groups"
    );
    assert_eq!(
        fixture_hits(&mut client)["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(applied(&commit, &root, &directory)["changed"], false);
    let (retry, _) = artifact(&commit).await;
    assert_eq!(retry.identity, after.identity);
    publish(&commit);
    let (rebuilt, rebuilt_identity) = artifact(&commit).await;
    assert_eq!(rebuilt.identity, after.identity);
    assert_eq!(
        rebuilt.freshness, after.freshness,
        "ordinary commit cannot renew directory input"
    );
    assert_eq!(identity, rebuilt_identity);
    let original = directory.clone();
    let mut conflicting = directory.clone();
    conflicting.users[0].active = false;
    assert!(
        !import(&commit, &root, &conflicting).status.success(),
        "a conflicting replay cannot replace a published input"
    );
    // An inactive alias owner still makes the match ambiguous.
    let mut collision = user(config, "inactive@example.test");
    collision.active = false;
    collision.aliases = canonical.aliases.clone();
    directory.users.push(collision);
    directory.built_at = fyrr_core::clock::now();
    applied(&commit, &root, &directory);
    assert_eq!(
        fixture_hits(&mut client)["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    directory.links.push(AccountLink {
        source: "incremental".into(),
        user: principal_hash(config, "key", "u"),
        principal: canonical.key.clone(),
    });
    directory.built_at = fyrr_core::clock::now();
    applied(&commit, &root, &directory);
    assert_eq!(
        fixture_hits(&mut client)["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    directory.links.push(AccountLink {
        source: "incremental".into(),
        user: principal_hash(config, "key", "u"),
        principal: directory.users[1].key.clone(),
    });
    directory.built_at = fyrr_core::clock::now();
    applied(&commit, &root, &directory);
    assert_eq!(
        fixture_hits(&mut client)["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert!(
        !import(&commit, &root, &original).status.success(),
        "stale directory cannot restore grants"
    );
    directory.users[0].active = false;
    directory.built_at = fyrr_core::clock::now();
    applied(&commit, &root, &directory);
    assert_eq!(
        fixture_hits(&mut client)["structuredContent"]["code"],
        "authorization_unavailable"
    );
    let (_, revoked) = artifact(&commit).await;
    assert!(revoked.subjects.is_empty() && revoked.principals.is_empty());
    let bytes = write_contract(&revoked);
    assert!(!String::from_utf8(bytes).unwrap().contains("example.test"));
}

#[tokio::test]
#[ignore = "real writer/MCP and local storage; scripts/ci"]
async fn identity_large_grants_are_complete_and_over_limit_is_subject_local_on_storage() {
    let (worker, commit, root) = fixture();
    for path in [&worker, &commit] {
        let text = std::fs::read_to_string(path)
            .unwrap()
            .replace("stdio_readers = [\"grp:allow\"]", "stdio_readers = []");
        std::fs::write(path, text).unwrap();
    }
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let config = &loaded.config;
    let mut directory = snapshot(config, "scim");
    let large = user(config, "fixture@example.test");
    let other = user(config, "other@example.test");
    directory.users = vec![large.clone(), other.clone()];
    for size in [
        1,
        1000,
        5000,
        config.identity.max_tokens_per_user as usize + 1,
    ] {
        directory.groups = (0..size - 1)
            .map(|i| DirectoryGroup {
                token: principal_hash(config, "tok", &format!("grp:large-{i}")),
                members: vec![large.key.clone()],
            })
            .collect();
        directory.groups.push(DirectoryGroup {
            token: principal_hash(config, "tok", "grp:allow"),
            members: vec![other.key.clone()],
        });
        directory.built_at = fyrr_core::clock::now();
        let receipt = measured_publication(&commit, &root, &directory);
        let (_, identity) = artifact(&commit).await;
        let principal = identity
            .principals
            .iter()
            .find(|p| p.key == large.key)
            .unwrap();
        assert_eq!(
            principal.tokens.len(),
            size,
            "never truncate an oversized grant set"
        );
        assert_eq!(principal.groups.len(), size - 1);
        let check = host_command(&worker)
            .args(["reader", "check"])
            .output()
            .unwrap();
        assert_eq!(
            check.status.success(),
            size <= config.identity.max_tokens_per_user as usize
        );
        if size > config.identity.max_tokens_per_user as usize {
            assert_eq!(receipt["over_limit_subjects"], 1);
            assert!(String::from_utf8_lossy(&check.stderr).contains("authorization_unavailable"));
        }
    }
    let other_config = root.join("other-reader.toml");
    std::fs::write(
        &other_config,
        std::fs::read_to_string(&worker)
            .unwrap()
            .replace("fixture@example.test", "other@example.test"),
    )
    .unwrap();
    let mut client = Mcp::open(&other_config);
    let result = client.call("search", json!({"query":"fixture query","limit":5}));
    assert_eq!(result["isError"], false);
    assert!(
        !result["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let (head, identity) = artifact(&commit).await;
    assert!(
        identity
            .inputs
            .iter()
            .any(|i| i.kind == IdentityInputKind::Directory)
    );
    let lease = fyrr_writer::lease::LeaseGuard::acquire(config)
        .await
        .unwrap();
    directory.built_at = fyrr_core::clock::now();
    let busy = import(&commit, &root, &directory);
    assert_eq!(busy.status.code(), Some(75));
    lease.release().await.unwrap();
    let (unchanged, _) = artifact(&commit).await;
    assert_eq!(head.identity, unchanged.identity);
}

#[tokio::test]
#[ignore = "real writer/MCP and local storage; scripts/ci"]
async fn identity_email_evidence_and_cross_issuer_bindings_on_storage() {
    let (worker, commit, root) = fixture();
    initial(&root);
    let mut data: Value =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    data["members"][0]["users"][0]
        .as_object_mut()
        .unwrap()
        .remove("email_verification");
    input(&root, data.clone());
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
    publish(&commit);
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let config = &loaded.config;
    let mut directory = snapshot(config, "scim");
    let mut first = user(config, "fixture@example.test");
    let mut second = user(config, "second@example.test");
    let issuer_a =
        fyrr_writer::directory::subject_key(config, "https://issuer-a.example", "same-subject")
            .unwrap();
    let issuer_b =
        fyrr_writer::directory::subject_key(config, "https://issuer-b.example", "same-subject")
            .unwrap();
    assert_ne!(issuer_a, issuer_b);
    first.subjects.push(issuer_a.clone());
    second.subjects.push(issuer_b.clone());
    directory.users = vec![first.clone(), second.clone()];
    applied(&commit, &root, &directory);
    let (_, identity) = artifact(&commit).await;
    assert!(
        identity
            .subjects
            .iter()
            .any(|s| s.subject == issuer_a && s.principal == first.key)
    );
    assert!(
        identity
            .subjects
            .iter()
            .any(|s| s.subject == issuer_b && s.principal == second.key)
    );
    let mut client = Mcp::open(&worker);
    let count = |client: &mut Mcp| {
        fixture_hits(client)["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .len()
    };
    assert_eq!(
        count(&mut client),
        0,
        "missing verification evidence must deny matching email"
    );
    for (epoch, evidence, expected) in [
        (2, "verified_corporate", 2),
        (3, "unverified", 0),
        (4, "verified_personal", 0),
    ] {
        data["epoch"] = json!(epoch);
        data["members"][0]["users"][0]["email_verification"] = json!(evidence);
        input(&root, data.clone());
        worker_sync(&worker, &["incremental", "--kind", "members"]);
        publish(&commit);
        assert_eq!(count(&mut client), expected, "email evidence: {evidence}");
    }
    directory.built_at = fyrr_core::clock::now();
    directory.links.push(AccountLink {
        source: "incremental".into(),
        user: principal_hash(config, "key", "u"),
        principal: first.key.clone(),
    });
    // A collision within one issuer denies that binding; the other issuer remains independent.
    directory.users[1].subjects.push(issuer_a.clone());
    applied(&commit, &root, &directory);
    let (_, identity) = artifact(&commit).await;
    assert!(!identity.subjects.iter().any(|s| s.subject == issuer_a));
    assert!(
        identity
            .subjects
            .iter()
            .any(|s| s.subject == issuer_b && s.principal == second.key)
    );
    assert_eq!(
        count(&mut client),
        2,
        "explicit link permits personal email"
    );
    data["epoch"] = json!(5);
    data["members"][0]["users"][0]["email_verification"] = json!("unverified");
    input(&root, data);
    worker_sync(&worker, &["incremental", "--kind", "members"]);
    publish(&commit);
    assert_eq!(
        count(&mut client),
        2,
        "explicit link permits unverified email"
    );
}

#[tokio::test]
#[ignore = "slow real storage and RustFS; scripts/ci"]
async fn identity_directory_keeps_lease_alive_during_slow_storage_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let loaded = fyrr_core::config::load(&commit).unwrap();
    let mut directory = snapshot(&loaded.config, "scim");
    directory
        .users
        .push(user(&loaded.config, "fixture@example.test"));
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let delayed = Arc::new(AtomicBool::new(false));
    let delay = delayed.clone();
    let renewals = Arc::new(AtomicUsize::new(0));
    let writes = renewals.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let hook: super::corrections::BoundaryHook = Arc::new(move |request, _, after| {
        if !after
            && request.starts_with("GET ")
            && request.contains("/identity/")
            && !delay.swap(true, Ordering::Relaxed)
        {
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
        if !after && request.starts_with("PUT ") && request.contains("head.json") {
            writes.fetch_add(1, Ordering::Relaxed);
        }
        false
    });
    let (proxy, thread) =
        super::corrections::proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
    let proxied = root.join("slow-directory.toml");
    std::fs::write(
        &proxied,
        std::fs::read_to_string(&commit)
            .unwrap()
            .replace(&endpoint, &proxy)
            .replace("[reader]", "[writer.commit]\nlease_ttl='3s'\n[reader]"),
    )
    .unwrap();
    let (before, _) = artifact(&commit).await;
    let result = import(&proxied, &root, &directory);
    stop.store(true, Ordering::Relaxed);
    drop(thread);
    assert!(
        delayed.load(Ordering::Relaxed),
        "slow storage boundary exercised"
    );
    assert!(result.status.success(), "{}", report(&result));
    assert!(
        renewals.load(Ordering::Relaxed) >= 5,
        "acquire, renewal, publish and release writes"
    );
    let (after, _) = artifact(&commit).await;
    assert!(after.identity.generation > before.identity.generation);
    assert_eq!(after.content, before.content);
    assert_eq!(after.policy, before.policy);
    assert_eq!(after.freshness, before.freshness);
    assert_eq!(applied(&commit, &root, &directory)["changed"], false);
}

/// Runs one import with `hook` on every storage request; the hook sees each
/// head write's body before the store does.
fn import_observed(
    config: &Path,
    root: &Path,
    snapshot: &DirectorySnapshot,
    hook: super::corrections::BoundaryHook,
) -> std::process::Output {
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let (proxy, thread) =
        super::corrections::proxy_at_boundary(endpoint.clone(), stop.clone(), hook);
    let observed = root.join("observed-directory.toml");
    std::fs::write(
        &observed,
        std::fs::read_to_string(config)
            .unwrap()
            .replace(&endpoint, &proxy),
    )
    .unwrap();
    let output = import(&observed, root, snapshot);
    stop.store(true, Ordering::Relaxed);
    drop(thread);
    output
}

type HeadWrites = Arc<std::sync::Mutex<Vec<ServingHead>>>;

/// Records every head write that reaches the store; rejects those `reject` names.
fn head_writes(
    reject: impl Fn(&ServingHead) -> bool + Send + Sync + 'static,
) -> (HeadWrites, super::corrections::BoundaryHook) {
    let writes: HeadWrites = Arc::default();
    let seen = writes.clone();
    let hook: super::corrections::BoundaryHook = Arc::new(move |request, body, after| {
        if after || !request.starts_with("PUT ") || !request.contains("/head.json ") {
            return false;
        }
        let head: ServingHead = parse_contract(body).unwrap();
        if reject(&head) {
            return true;
        }
        seen.lock().unwrap().push(head);
        false
    });
    (writes, hook)
}

/// A manual import under `provider = "none"` withdraws the directory timestamp
/// of an earlier provider. The new identity and the withdrawal are one head
/// write: no head ever carries the new identity beside the old timestamp.
#[tokio::test]
#[ignore = "real writer, proxied head writes and local storage; scripts/ci"]
async fn identity_import_withdraws_the_directory_clock_in_the_same_head_write_on_storage() {
    let (worker, commit, root) = fixture();
    publish_initial(&worker, &commit, &root);
    let loaded = fyrr_core::config::load(&commit).unwrap();
    assert_eq!(
        loaded.config.identity.directory.provider,
        fyrr_core::config::DirectoryProvider::None
    );
    let (store, path) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let plant = |at| {
        let store = store.clone();
        let path = path.clone();
        let commit = commit.clone();
        async move {
            let mut head = publication(&commit).await.0;
            head.freshness.directory = Some(at);
            super::write_head(store.as_ref(), &path, &head).await;
            head
        }
    };
    let old = fyrr_core::clock::now() - chrono::Duration::minutes(10);
    let mut directory = snapshot(&loaded.config, "manual");
    directory
        .users
        .push(user(&loaded.config, "fixture@example.test"));

    // A write that fails leaves the previous head, timestamp included.
    let before = plant(old).await;
    let generation = before.identity.generation;
    let (writes, hook) = head_writes(move |head| head.identity.generation > generation);
    let failed = import_observed(&commit, &root, &directory, hook);
    assert!(!failed.status.success(), "{}", report(&failed));
    assert!(
        writes
            .lock()
            .unwrap()
            .iter()
            .all(|head| head.identity == before.identity)
    );
    let kept = publication(&commit).await.0;
    assert_eq!(kept.identity, before.identity);
    assert_eq!(kept.freshness.directory, Some(old));

    // A changed identity and the withdrawal arrive together.
    let (writes, hook) = head_writes(|_| false);
    directory.built_at = fyrr_core::clock::now();
    let imported = import_observed(&commit, &root, &directory, hook);
    assert!(imported.status.success(), "{}", report(&imported));
    let published: Vec<ServingHead> = writes
        .lock()
        .unwrap()
        .iter()
        .filter(|head| head.identity.generation > before.identity.generation)
        .cloned()
        .collect();
    assert!(!published.is_empty(), "the import published its identity");
    assert!(
        published
            .iter()
            .all(|head| head.freshness.directory.is_none()),
        "a head carried the new identity beside the old timestamp"
    );
    let after = publication(&commit).await.0;
    assert!(after.identity.generation > before.identity.generation);
    assert_eq!(after.freshness.directory, None);

    // An import that changes nothing in the identity is the withdrawal alone.
    let before = plant(old).await;
    let (writes, hook) = head_writes(|_| false);
    directory.built_at = fyrr_core::clock::now();
    let repeated = import_observed(&commit, &root, &directory, hook);
    assert!(repeated.status.success(), "{}", report(&repeated));
    assert_eq!(
        serde_json::from_slice::<Value>(&repeated.stdout).unwrap()["changed"],
        false
    );
    assert!(
        writes
            .lock()
            .unwrap()
            .iter()
            .all(|head| head.identity == before.identity)
    );
    let after = publication(&commit).await.0;
    assert_eq!(after.identity, before.identity);
    assert_eq!(after.freshness.directory, None);
}
