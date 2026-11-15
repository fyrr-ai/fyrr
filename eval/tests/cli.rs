//! `fyrr-eval` without storage: argument and stage-file guards, closed error
//! codes and the frozen embedding provider.
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

use fyrr_reader::embedding::{EmbeddingClient, ProviderStats, Role, cache_key, digest, identity};
use serde_json::json;

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A per-test directory: `inputs/` for planted files, `run/` for anything
/// the tool may write.
struct Sandbox(PathBuf);

impl Sandbox {
    fn new(name: &str) -> Self {
        let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        for dir in ["inputs", "run/home", "run/tmp"] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
        }
        Self(root)
    }
    fn input(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join("inputs").join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }
    fn command(&self, config: &Path) -> Command {
        let run = self.0.join("run");
        let mut command = Command::new(env!("CARGO_BIN_EXE_fyrr-eval"));
        command
            .arg("--config")
            .arg(config)
            .current_dir(&run)
            .env_remove("FYRR_CONFIG")
            .env("HOME", run.join("home"))
            .env("TMPDIR", run.join("tmp"))
            .env("AWS_EC2_METADATA_DISABLED", "true");
        command
    }
    /// Fails when a marker appears in the output or in any file written
    /// under `run/`.
    fn assert_clean(&self, markers: &[&str], output: &Output, context: &str) {
        let mut texts = vec![
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ];
        let mut stack = vec![self.0.join("run")];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    texts.push(path.display().to_string());
                    texts
                        .push(String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned());
                }
            }
        }
        for marker in markers {
            assert!(
                texts.iter().all(|text| !text.contains(marker)),
                "{context}: {marker} leaked"
            );
        }
    }
}

fn marker(category: &str) -> String {
    format!(
        "SNT{}x{}-{category}-q7",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// A valid configuration whose storage is unreachable: every search passes
/// the argument checks and then fails to open the reader.
const OFFLINE: &str = "[namespace]\nname='evaluation'\n[storage]\nuri='s3://evaluation'\nendpoint='http://127.0.0.1:9'\nallow_http=true\n[identity]\nhashing_key={env='FYRR_EVAL_HASHING_KEY'}\n[embedding]\nendpoint='https://cache.invalid'\n";

#[test]
fn stage_files_are_new_files_under_an_external_root() {
    let sandbox = Sandbox::new("stages");
    let config = sandbox.input("fyrr.toml", OFFLINE);
    let queries = sandbox.input("queries.json", "[]");
    let external = std::env::temp_dir().join(format!("fyrr-eval-stages-{}", std::process::id()));
    std::fs::create_dir_all(&external).unwrap();
    let existing = external.join("existing.json");
    std::fs::write(&existing, "[]").unwrap();
    let fresh = external.join("fresh.json");
    let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let run = |stages: &Path, root: Option<&Path>, batch: bool| {
        let mut command = sandbox.command(&config);
        command.arg("search").arg("--stages").arg(stages);
        if batch {
            command.arg("--queries").arg(&queries);
        }
        match root {
            Some(root) => command.env("FYRR_EVAL_ROOT", root),
            None => command.env_remove("FYRR_EVAL_ROOT"),
        };
        let output = command.output().unwrap();
        (
            output.status.code(),
            String::from_utf8(output.stderr).unwrap(),
        )
    };
    assert_eq!(
        run(&fresh, Some(&external), false),
        (Some(2), "error: invalid_argument\n".into())
    );
    let invalid = (Some(1), "error: evaluation_input_invalid\n".to_owned());
    assert_eq!(run(&fresh, None, true), invalid);
    assert_eq!(
        run(&checkout.join("target/stages.json"), Some(checkout), true),
        invalid
    );
    assert_eq!(run(&existing, Some(&external), true), invalid);
    // A valid path passes the guard and the reader then fails on storage.
    let (code, accepted) = run(&fresh, Some(&external), true);
    assert_eq!(code, Some(1));
    assert!(accepted.starts_with("error: ") && !accepted.contains("evaluation_input_invalid"));
    assert!(!fresh.exists(), "no stage file without a completed batch");
    std::fs::remove_dir_all(external).unwrap();
}

#[test]
fn errors_keep_query_values_and_paths_private() {
    let sandbox = Sandbox::new("privacy");
    let config = sandbox.input("fyrr.toml", OFFLINE);
    let query = marker("query");
    let path_marker = marker("path");
    let path = sandbox.input(&path_marker, "[]");
    let path = path.to_str().unwrap();
    let batch = sandbox.input("batch.json", &json!([{"query": query}]).to_string());
    let batch = batch.to_str().unwrap();
    let filters = sandbox.input(
        "filters.json",
        &json!([{"query":"q","filters":query}]).to_string(),
    );
    let filters = filters.to_str().unwrap();
    for args in [
        vec!["search", query.as_str()],
        vec!["search", "--queries", path],
        vec!["search", "--queries", batch],
        vec!["search", "--queries", filters],
        vec!["search", "--queries", path, "--stages", path],
        vec!["search", "--stages", query.as_str()],
        vec!["search", "--queries", path, "--output", query.as_str()],
        vec!["search", "--unknown", query.as_str()],
        vec!["serve-embeddings", "--frozen-queries", path],
        vec![
            "serve-embeddings",
            "--frozen-index",
            path,
            "--frozen-vectors",
            path,
        ],
        vec!["serve-embeddings", "--listen", query.as_str()],
        vec![query.as_str()],
    ] {
        let output = sandbox.command(&config).args(&args).output().unwrap();
        assert!(!output.status.success(), "{args:?}");
        sandbox.assert_clean(&[&query, &path_marker], &output, "fyrr-eval paths");
    }
}

#[tokio::test]
async fn frozen_provider_binds_files_roles_and_keeps_inputs_read_only() {
    use std::io::BufRead;
    use std::os::unix::fs::PermissionsExt;
    let sandbox = Sandbox::new("frozen-provider");
    let private = marker("frozen-input");
    let config = sandbox.input("fyrr.toml", "[namespace]\nname='frozen'\n[storage]\nuri='s3://frozen'\n[identity]\nhashing_key={env='FYRR_EVAL_HASHING_KEY'}\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\nmodel='frozen-model'\ndimensions=512\nprefix='shared:'\nspend.limit_microusd=100\nspend.max_call_microusd=1\n[embedding.roles.document]\nprefix='doc:'\ninput_type='document'\n[embedding.roles.query]\nprefix='query:'\ninput_type='query'\n");
    let mut embedding = fyrr_core::config::load(&config).unwrap().config.embedding;
    let index = sandbox.input("index.sqlite3", "");
    let vectors = sandbox.input("vectors.f32", "");
    let queries = sandbox.input("queries.json", &json!({"embedding":identity(&embedding),"vectors":{(cache_key(&embedding, &private, Role::Query)):vec![0.25f32;512]}}).to_string());
    let bytes: Vec<_> = vec![0.125f32; 512]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    std::fs::write(&vectors, &bytes).unwrap();
    let db = rusqlite::Connection::open(&index).unwrap();
    db.execute_batch("CREATE TABLE embeddings(text_sha256 TEXT PRIMARY KEY,vector_index INTEGER); CREATE TABLE embedding_profile(identity TEXT,role TEXT); CREATE TABLE embedding_vectors(dimensions INTEGER,rows INTEGER,sha256 TEXT);").unwrap();
    db.execute(
        "INSERT INTO embeddings VALUES(?1,0)",
        [&digest(format!("shared:doc:{private}").as_bytes())[7..]],
    )
    .unwrap();
    db.execute(
        "INSERT INTO embedding_profile VALUES(?1,'document')",
        [serde_json::to_string(&identity(&embedding)).unwrap()],
    )
    .unwrap();
    db.execute(
        "INSERT INTO embedding_vectors VALUES(512,1,?1)",
        [digest(&bytes)],
    )
    .unwrap();
    drop(db);
    let originals: Vec<_> = [&index, &vectors, &queries]
        .iter()
        .map(|p| std::fs::read(p).unwrap())
        .collect();
    for path in [&index, &vectors, &queries] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444)).unwrap();
    }
    struct Owned(std::process::Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let start = || {
        sandbox
            .command(&config)
            .arg("serve-embeddings")
            .arg("--frozen-index")
            .arg(&index)
            .arg("--frozen-vectors")
            .arg(&vectors)
            .arg("--frozen-queries")
            .arg(&queries)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut child = Owned(start());
    let mut endpoint = String::new();
    let mut stdout = std::io::BufReader::new(child.0.stdout.take().unwrap());
    stdout.read_line(&mut endpoint).unwrap();
    assert!(
        endpoint.starts_with("http://127.0.0.1:"),
        "stub did not start"
    );
    embedding.endpoint = endpoint.trim().into();
    let catalog: serde_json::Value = reqwest::get(format!("{}/models", embedding.endpoint))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(catalog["data"][0]["id"], embedding.model);
    let client = EmbeddingClient::new(&embedding, None).unwrap();
    let mut stats = ProviderStats::default();
    client.verify_model(&mut stats).await.unwrap();
    assert_eq!(
        client
            .embed(&[&private], Role::Document, &mut stats)
            .await
            .unwrap(),
        [vec![0.125; 512]]
    );
    assert_eq!(
        client
            .embed(&[&private], Role::Query, &mut stats)
            .await
            .unwrap(),
        [vec![0.25; 512]]
    );
    assert!(
        client
            .embed(&["missing"], Role::Document, &mut stats)
            .await
            .is_err()
    );
    // The HTTP error must not echo input text, model values or parser diagnostics.
    let response = reqwest::Client::new()
        .post(format!("{}/embeddings", embedding.endpoint))
        .body(private.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert_eq!(response.text().await.unwrap(), "evaluation_input_invalid");
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    let mut stdout_bytes = endpoint.into_bytes();
    std::io::Read::read_to_end(&mut stdout, &mut stdout_bytes).unwrap();
    let mut stderr = Vec::new();
    std::io::Read::read_to_end(&mut child.0.stderr.take().unwrap(), &mut stderr).unwrap();
    let logs = Output {
        status,
        stdout: stdout_bytes,
        stderr,
    };
    sandbox.assert_clean(&[&private], &logs, "frozen provider logs");
    drop(child);
    for (path, original) in [&index, &vectors, &queries].iter().zip(&originals) {
        assert_eq!(&std::fs::read(path).unwrap(), original);
    }
    assert!(!index.with_extension("sqlite3-wal").exists());
    assert!(!index.with_extension("sqlite3-shm").exists());
    // Equal stride and row counts cannot bind a different vector file.
    std::fs::set_permissions(&vectors, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(&vectors, vec![0u8; bytes.len()]).unwrap();
    let output = start().wait_with_output().unwrap();
    assert!(!output.status.success());
    assert_eq!(output.stderr, b"error: evaluation_input_invalid\n");
    sandbox.assert_clean(&[&private], &output, "frozen file binding");
    std::fs::write(&vectors, &bytes).unwrap();
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("model='frozen-model'", "model='different-model'");
    std::fs::write(&config, text).unwrap();
    let output = start().wait_with_output().unwrap();
    assert!(!output.status.success());
    assert_eq!(output.stderr, b"error: evaluation_input_invalid\n");
    sandbox.assert_clean(&[&private], &output, "frozen profile binding");
}
