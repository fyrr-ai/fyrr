//! Command-local preparation evidence retains fresh discovery and immutable identity.
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[allow(clippy::print_stdout)] // Emit request counts for reproducible before/after runs.
#[tokio::test]
#[ignore = "real CLI multi-kind preparation on local storage"]
async fn preparation_reuses_bodies_but_discovers_new_envelopes_on_local_storage() {
    let setup = setup_on(Backend::Local);
    for row in 8..768 {
        std::fs::remove_file(setup.1.join("sources").join(format!("{row}.txt"))).unwrap();
    }
    let (worker, commit, root) = incremental::fixture_host_at(8, true, "dense-fixture", setup);
    incremental::publish_initial(&worker, &commit, &root);
    let config = fyrr_core::config::load(&worker).unwrap().config;
    let (_, namespace) = fyrr_reader::storage::namespace_store(&config).unwrap();
    let template = incremental::envelopes(&worker)
        .await
        .into_iter()
        .find(|(_, e)| e.kind == fyrr_core::connector::SyncKind::Members)
        .unwrap()
        .1;
    let mut seed = Vec::new();
    for n in 0..128 {
        let mut envelope = template.clone();
        envelope.source_id = "other-source".into();
        envelope.sequence = n + 100;
        envelope.envelope_id = fyrr_core::envelope::envelope_id(
            &envelope.namespace,
            &envelope.source_id,
            envelope.kind,
            envelope.sequence,
            envelope.rejection_generation,
            envelope.cursor.start.as_deref(),
        );
        seed.push((
            namespace
                .clone()
                .join("envelopes")
                .join(format!("{}.json", envelope.envelope_id)),
            write_contract(&envelope),
        ));
    }
    fyrr_reader::storage::test_store::seed_local_fixture(&config, seed).unwrap();
    let gets = Arc::new(AtomicUsize::new(0));
    let lists = Arc::new(AtomicUsize::new(0));
    let mut late = template.clone();
    late.source_id = "late-source".into();
    late.envelope_id = fyrr_core::envelope::envelope_id(
        &late.namespace,
        &late.source_id,
        late.kind,
        late.sequence,
        late.rejection_generation,
        late.cursor.start.as_deref(),
    );
    let late_id = late.envelope_id.clone();
    let late_path = namespace
        .clone()
        .join("envelopes")
        .join(format!("{late_id}.json"));
    let injected = Arc::new(AtomicBool::new(false));
    let seen_late = Arc::new(AtomicBool::new(false));
    let hook_injected = injected.clone();
    let hook_late = seen_late.clone();
    let seen_gets = gets.clone();
    let seen_lists = lists.clone();
    let hook: corrections::BoundaryHook = Arc::new(move |request, body, after| {
        if after
            && request.starts_with("PUT ")
            && request.contains("/cursors/incremental-content.json")
            && serde_json::from_slice::<fyrr_core::connector::CursorState>(body).is_ok_and(
                |state| {
                    state
                        .lease
                        .is_some_and(|lease| lease.expires_at <= fyrr_core::clock::now())
                },
            )
            && !hook_injected.swap(true, Ordering::SeqCst)
        {
            fyrr_reader::storage::test_store::seed_local_fixture(
                &config,
                vec![(late_path.clone(), write_contract(&late))],
            )
            .unwrap();
        }
        if !after && request.starts_with("GET ") && request.contains(&late_id) {
            hook_late.store(true, Ordering::SeqCst);
        }

        if !after && request.starts_with("GET ") {
            if request.contains("/envelopes/") {
                seen_gets.fetch_add(1, Ordering::SeqCst);
            }
            if request.contains("list-type=2") && request.contains("envelopes") {
                seen_lists.fetch_add(1, Ordering::SeqCst);
            }
        }
        false
    });
    let (endpoint, proxy) = corrections::proxy_at_boundary(
        "local-storage".into(),
        Arc::new(AtomicBool::new(false)),
        hook,
    );
    let proxied = corrections::proxied(&worker, &endpoint);
    let output = coordination::worker_command(&proxied)
        .args([
            "writer",
            "sync",
            "incremental",
            "--kind",
            "content",
            "--kind",
            "permissions",
            "--kind",
            "members",
            "--due",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let gets = gets.load(Ordering::SeqCst);
    let lists = lists.load(Ordering::SeqCst);
    println!("preparation-evidence: envelope_gets={gets}, envelope_lists={lists}");
    assert!(
        (128..150).contains(&gets),
        "envelope bodies were reread: {gets}"
    );
    assert!(
        lists >= 3,
        "each kind must discover fresh envelopes: {lists}"
    );
    drop(proxy);
}
