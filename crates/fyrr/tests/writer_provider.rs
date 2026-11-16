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
