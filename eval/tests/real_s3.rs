//! `fyrr-eval` against real S3: the writer builds a namespace through the
//! frozen provider, and the reader then embeds queries through it.
//!
//! Needs `scripts/s3-fixture env` and the full `fyrr` binary in
//! `FYRR_BINARY` (`scripts/ci-cases run s3-evaluation` builds and sets it).
use std::io::BufRead;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::{Value, json};

fn eval(config: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fyrr-eval"));
    command.arg("--config").arg(config);
    command
}

fn fyrr(config: &Path, args: &[&str]) -> Vec<u8> {
    let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let output = Command::new(std::env::var_os("FYRR_BINARY").expect("FYRR_BINARY"))
        .arg("--config")
        .arg(config)
        .args(args)
        .env("VIRTUAL_ENV", checkout.join("connectors/.venv"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

struct Owned(std::process::Child);
impl Drop for Owned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
#[ignore = "real writer, frozen HTTP provider and RustFS; scripts/ci"]
fn frozen_vectors_build_a_namespace_and_answer_its_queries_on_real_s3() {
    let root = std::env::temp_dir().join(format!(
        "fyrr-eval-s3-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    ));
    let sources = root.join("sources");
    std::fs::create_dir_all(&sources).unwrap();
    let text = |row: usize| format!("fixture document {row}");
    let vector = |row: usize| -> Vec<f32> {
        (0..1024)
            .map(|d| (((row + 1) * (d + 7)) as f32).sin())
            .collect()
    };
    for row in 0..4 {
        std::fs::write(sources.join(format!("{row}.txt")), text(row)).unwrap();
    }
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").expect("scripts/s3-fixture env");
    let bucket = std::env::var("FYRR_TEST_S3_BUCKET").expect("scripts/s3-fixture env");
    let config = root.join("fyrr.toml");
    let write_config = |embedding: &str| {
        std::fs::write(
            &config,
            format!(
                r#"
[namespace]
name = "eval-fixture"
[storage]
uri = "s3://{bucket}/eval-{}"
endpoint = "{endpoint}"
allow_http = true
access_key_id = {{ env = "FYRR_TEST_S3_ACCESS_KEY_ID" }}
secret_access_key = {{ env = "FYRR_TEST_S3_SECRET_ACCESS_KEY" }}
[embedding]
endpoint = "{embedding}"
reader_live = true
max_retries = 0
spend = {{ limit_microusd = 1000000, max_call_microusd = 10 }}
[identity]
stdio_user = "fixture-subject"
stdio_readers = ["grp:allow"]
hashing_key = {{ env = "FYRR_TEST_S3_SECRET_ACCESS_KEY" }}
encryption_key = {{ env = "FYRR_TEST_ENCRYPTION_KEY" }}
[reader]
nprobes = 1
mirror_path = "{}"
mirror_max_bytes = 16777216
[[sources]]
id = "allowed"
type = "local-files"
permissions = "fixed"
readers = ["grp:allow"]
[sources.scope]
paths = ["{}"]
"#,
                root.file_name().unwrap().to_string_lossy(),
                root.join("mirror").display(),
                sources.display()
            ),
        )
        .unwrap();
    };
    write_config("http://127.0.0.1:9/v1");
    let loaded = fyrr_core::config::load(&config).unwrap();
    let mut vectors = serde_json::Map::new();
    for row in 0..4 {
        vectors.insert(
            fyrr_reader::embedding::digest(text(row).as_bytes()),
            json!(vector(row)),
        );
    }
    vectors.insert(
        fyrr_reader::embedding::digest(b"dense-only query"),
        json!(vector(0)),
    );
    let frozen = root.join("frozen.json");
    std::fs::write(
        &frozen,
        json!({
            "embedding": fyrr_reader::embedding::identity(&loaded.config.embedding),
            "vectors": vectors,
        })
        .to_string(),
    )
    .unwrap();
    let mut stub = Owned(
        eval(&config)
            .arg("serve-embeddings")
            .arg("--frozen-queries")
            .arg(&frozen)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut provider = String::new();
    std::io::BufReader::new(stub.0.stdout.take().unwrap())
        .read_line(&mut provider)
        .unwrap();
    assert!(provider.starts_with("http://127.0.0.1:"));
    write_config(provider.trim());

    fyrr(&config, &["writer", "namespace", "create"]);
    fyrr(&config, &["writer", "sync", "allowed"]);
    fyrr(&config, &["writer", "commit"]);
    let production: Value = serde_json::from_slice(&fyrr(
        &config,
        &[
            "reader",
            "search",
            "dense-only query",
            "--output",
            "metadata",
        ],
    ))
    .unwrap();
    assert_eq!(production["metadata"]["degraded"], json!([]));

    let requests = root.join("requests.json");
    std::fs::write(
        &requests,
        json!([{"query":"dense-only query"},{"query":"dense-only query"},{"query":"missing"}])
            .to_string(),
    )
    .unwrap();
    let stages = root.join("stages.json");
    let output = eval(&config)
        .args(["search", "--output", "metadata", "--queries"])
        .arg(&requests)
        .arg("--stages")
        .arg(&stages)
        .env("FYRR_EVAL_ROOT", &root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let results: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(results.len(), 3);
    for result in &results[..2] {
        assert_eq!(result["hits"], production["hits"]);
        assert_eq!(result["metadata"]["degraded"], json!([]));
    }
    assert_eq!(
        results[1]["metadata"]["telemetry"]["embedding_memory_hits"],
        1
    );
    // A vector the provider does not hold degrades the dense lane only.
    assert_eq!(results[2]["metadata"]["degraded"], json!(["dense"]));
    let traces: Vec<Value> = serde_json::from_slice(&std::fs::read(&stages).unwrap()).unwrap();
    assert_eq!(traces.len(), 3);
    assert!(!traces[0]["dense"].as_array().unwrap().is_empty());
    assert!(!traces[0]["composition"].as_array().unwrap().is_empty());

    for filters in [json!([]), json!("private-filter"), json!(42), json!(false)] {
        std::fs::write(
            &requests,
            json!([{"query":"dense-only query", "filters":filters}]).to_string(),
        )
        .unwrap();
        let invalid = eval(&config)
            .arg("search")
            .arg("--queries")
            .arg(&requests)
            .output()
            .unwrap();
        assert_eq!(invalid.status.code(), Some(1));
        let stderr = String::from_utf8(invalid.stderr).unwrap();
        assert_eq!(stderr, "error: evaluation_input_invalid\n");
        assert!(invalid.stdout.is_empty());
    }
    drop(stub);
    std::fs::remove_dir_all(root).unwrap();
}
