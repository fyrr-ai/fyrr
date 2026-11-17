//! Provider-boundary compatibility through the real worker executable and local storage.
#![cfg(feature = "writer")]

mod common;

use common::{Sandbox, embedding::Provider};
use serde_json::json;
use std::path::{Path, PathBuf};

struct Fixture {
    sandbox: Sandbox,
    config: PathBuf,
    _embedding: Provider,
}

impl Fixture {
    fn new(provider: &str, mode: &str) -> Self {
        let sandbox = Sandbox::new("writer-provider");
        let embedding = Provider::start("BAAI/bge-m3", false);
        let documents = sandbox.input(
            "documents.json",
            &json!({"documents":[{"text":"fixture"}]}).to_string(),
        );
        let config = sandbox.input("fyrr.toml", &format!(
            "[namespace]\nname='driver-fixture'\n[storage]\nuri='file://{}'\n[embedding]\nendpoint='{}'\napi_key={{env='FYRR_TEST_SECRET'}}\nlocal_cache='{}'\nspend={{limit_microusd=1000,max_call_microusd=1}}\n[identity]\nstdio_user='fixture'\nhashing_key={{env='FYRR_TEST_SECRET'}}\nencryption_key={{env='FYRR_TEST_ENCRYPTION_KEY'}}\n[writer]\nenvelope_max_bytes='16MiB'\n[[sources]]\nid='fixture'\ntype='{provider}'\npermissions='fixed'\nreaders=['grp:fixture']\n[sources.settings]\nmode='{mode}'\n[sources.scope]\nfixture='{}'\n",
            sandbox.output().join("storage").display(), embedding.endpoint,
            sandbox.output().join("embeddings").display(), documents.display()
        ));
        let fixture = Self {
            sandbox,
            config,
            _embedding: embedding,
        };
        let output = fixture.run(&["writer", "namespace", "create"]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        fixture
    }

    fn run(&self, args: &[&str]) -> std::process::Output {
        self.sandbox.configured(&self.config, args, &[])
    }
}

#[test]
#[ignore = "real worker and RustFS; scripts/ci"]
fn prepared_errors_preserve_batch_invalid_and_provider_failure_on_storage() {
    for (provider, mode, code) in [
        ("sentinel-review", "text-huge", "batch_invalid"),
        ("sentinel-failing", "raise", "provider_failure"),
    ] {
        let fixture = Fixture::new(provider, mode);
        let output = fixture.run(&["writer", "sync", "--kind", "content"]);
        assert_eq!(
            output.status.code(),
            Some(1),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let expected = format!(
            "{{\"kind\":\"content\",\"error\":\"{code}\",\"paid_calls\":0,\"provider_calls\":1,\"reserved_microusd\":1,\"source_id\":\"fixture\",\"failed\":true,\"kinds\":[{{\"kind\":\"content\",\"error\":\"{code}\",\"paid_calls\":0,\"provider_calls\":1,\"reserved_microusd\":1}}]}}\n"
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            expected,
            "{provider}/{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
#[ignore = "real committer lease and RustFS; scripts/ci"]
async fn committer_busy_remains_closed_on_storage() {
    let fixture = Fixture::new("sentinel-failing", "raise");
    let loaded = fyrr_core::config::load(Path::new(&fixture.config)).unwrap();
    let lease = fyrr_writer::lease::LeaseGuard::acquire(&loaded.config)
        .await
        .unwrap();
    let output = fixture.run(&["writer", "commit"]);
    lease.release().await.unwrap();
    assert_eq!(output.status.code(), Some(75));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).ends_with("error: committer_busy\n"));
}

#[tokio::test]
#[ignore = "real worker timeout, durable envelopes and cached vectors; scripts/ci"]
async fn prepared_timeout_names_flag_and_rerun_preserves_durable_work_on_storage() {
    use futures::TryStreamExt;
    use fyrr_core::connector::CursorState;
    use fyrr_reader::storage::{namespace_store, read_contract};
    use std::time::Duration;

    let fixture = Fixture::new("sentinel-review", "timeout-resume");
    let output = fixture.run(&[
        "writer",
        "sync",
        "--full",
        "--kind",
        "content",
        "--timeout",
        "5s",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("timeout after 5000ms; increase --timeout"),
        "{stderr}"
    );
    assert!(output.stdout.is_empty());
    let config = fyrr_core::config::load(&fixture.config).unwrap().config;
    let (store, root) = namespace_store(&config).unwrap();
    let cursor: CursorState = read_contract(
        store.as_ref(),
        &root.clone().join("cursors").join("fixture-content.json"),
    )
    .await
    .unwrap();
    assert_eq!(cursor.cursor.as_deref(), Some("done"));
    let before: Vec<_> = store
        .list(Some(&root.clone().join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    assert!(
        !before.is_empty(),
        "the timed-out worker left durable prepared work"
    );
    let requests = fixture._embedding.state.requests.lock().unwrap().len();
    assert!(requests > 0);
    let remaining = (cursor.lease.unwrap().expires_at - fyrr_core::clock::now())
        .to_std()
        .unwrap_or_default();
    fyrr_core::clock::advance(remaining + Duration::from_millis(50)).unwrap();
    let resumed = fixture.run(&["writer", "sync", "--kind", "content"]);
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert_eq!(
        fixture._embedding.state.requests.lock().unwrap().len(),
        requests
    );
    let after: Vec<_> = store
        .list(Some(&root.clone().join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "resuming an advanced cursor does not duplicate envelopes"
    );
    // Force a replay from the beginning: existing vectors remain reusable.
    let text = std::fs::read_to_string(&fixture.config)
        .unwrap()
        .replace("mode='timeout-resume'", "mode='resume'");
    std::fs::write(&fixture.config, text).unwrap();
    let replayed = fixture.run(&["writer", "sync", "--full", "--kind", "content"]);
    assert!(
        replayed.status.success(),
        "{}",
        String::from_utf8_lossy(&replayed.stderr)
    );
    assert_eq!(
        fixture._embedding.state.requests.lock().unwrap().len(),
        requests
    );
    let committed = fixture.run(&["writer", "commit"]);
    assert!(
        committed.status.success(),
        "{}",
        String::from_utf8_lossy(&committed.stderr)
    );
    let verified = fixture.run(&["writer", "verify"]);
    assert!(
        verified.status.success(),
        "{}",
        String::from_utf8_lossy(&verified.stderr)
    );
}
