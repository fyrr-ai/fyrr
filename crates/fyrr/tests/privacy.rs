//! Privacy tests: every CLI path that exists
//! runs with unique markers planted in secrets, document text, titles,
//! authors, reader tokens and query-like input, and no marker may appear in
//! stdout, stderr or anything the run writes. Runs in both builds; the
//! provider paths need the full build.
//!
//! Conditional head-write storage errors are also guarded on real S3 by
//! `dense_path::head_diagnostics`, using this shared sentinel harness.

mod common;

use common::{Sandbox, Sentinels, assert_clean, config_with};

/// The example's secret variables.
const SECRET_VARS: [&str; 5] = [
    "FYRR_S3_ACCESS_KEY_ID",
    "FYRR_S3_SECRET_ACCESS_KEY",
    "FYRR_EMBEDDING_API_KEY",
    "FYRR_GOOGLE_DIRECTORY_CREDENTIALS",
    "FYRR_JIRA_API_TOKEN",
];

#[test]
fn response_truncation_counts_cannot_carry_private_values() {
    use fyrr_core::mcp::Truncation;
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-response-truncation");
    let marker = sentinels.mark("truncated-private-text");
    for value in [
        serde_json::json!({"text_dropped": marker, "hits_dropped": 0}),
        serde_json::json!({"text_dropped": 0, "hits_dropped": marker}),
        serde_json::json!({"text_dropped": 4294967296u64, "hits_dropped": 0}),
        serde_json::json!({"text_dropped": 0, "hits_dropped": -1}),
    ] {
        assert!(serde_json::from_value::<Truncation>(value).is_err());
    }
    let counts = Truncation {
        text_dropped: u32::MAX,
        hits_dropped: u32::MAX,
    };
    let output = std::process::Output {
        status: std::os::unix::process::ExitStatusExt::from_raw(0),
        stdout: serde_json::to_vec(&counts).unwrap(),
        stderr: vec![],
    };
    assert_clean(&sentinels, &output, &sandbox, "response truncation counts");
}

/// The real-S3 embedding cases also inspect logs after live and failed responses.
#[test]
fn live_embedding_configuration_and_startup_keep_values_private() {
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-live-embedding");
    let secret = sentinels.mark("embedding-api-key");
    let query = sentinels.mark("embedding-query");
    let config = sandbox.input("embedding.toml", "[namespace]\nname='privacy-embedding'\n[storage]\nuri='s3://privacy-embedding'\nendpoint='http://127.0.0.1:9'\nallow_http=true\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\nmodel='fixture-model'\ndimensions=512\nsend_dimensions=true\nreader_live=true\napi_key={env='EMBEDDING_SENTINEL_KEY'}\nspend={limit_microusd=100,max_call_microusd=10}\nroles={document={prefix='document:',input_type='document'},query={prefix='query:',input_type='query'}}\n");
    for args in [
        vec!["config", "check"],
        vec!["config", "show"],
        vec!["reader", "search", &query],
        #[cfg(feature = "writer")]
        vec!["writer", "sync"],
    ] {
        let mut command = vec!["--config", config.to_str().unwrap()];
        command.extend(args);
        let output = sandbox.run(command, &[("EMBEDDING_SENTINEL_KEY", &secret)]);
        assert_clean(&sentinels, &output, &sandbox, "live embedding startup");
    }
    let uncapped = sandbox.input(
        "uncapped.toml",
        &std::fs::read_to_string(&config)
            .unwrap()
            .replace("spend={limit_microusd=100,max_call_microusd=10}", ""),
    );
    let output = sandbox.run(
        ["--config", uncapped.to_str().unwrap(), "config", "check"],
        &[("EMBEDDING_SENTINEL_KEY", &secret)],
    );
    assert!(!output.status.success());
    assert_clean(
        &sentinels,
        &output,
        &sandbox,
        "live embedding without a cap",
    );
    let output = sandbox.run(
        [
            "--config",
            uncapped.to_str().unwrap(),
            "reader",
            "search",
            &query,
        ],
        &[("EMBEDDING_SENTINEL_KEY", &secret)],
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("embedding_spend_limit_required"));
    assert_clean(&sentinels, &output, &sandbox, "named live-spend refusal");
}

/// Runtime payload privacy is also exercised by tools_rerank_* over real S3.
#[test]
fn reranker_configuration_and_startup_keep_values_private() {
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-reranker");
    let secret = sentinels.mark("rerank-secret");
    let query = sentinels.mark("rerank-query");
    let document = sentinels.mark("rerank-document");
    let config = sandbox.input("rerank.toml", "[namespace]\nname='privacy-rerank'\n[namespace.rerank]\nendpoint='http://127.0.0.1:9/rerank'\nmodel='test'\napi_key={env='RERANK_SENTINEL_KEY'}\ncandidates=5\nweight=0.5\ndeadline='100ms'\nspend_limit_microusd=10\nmax_call_cost_microusd=10\n[storage]\nuri='s3://privacy-rerank'\nendpoint='http://127.0.0.1:9'\nallow_http=true\n[embedding]\nendpoint='http://127.0.0.1:9'\n");
    for args in [
        vec!["config", "check"],
        vec!["config", "show"],
        vec!["reader", "search", &query],
    ] {
        let mut command = vec!["--config", config.to_str().unwrap()];
        command.extend(args);
        let output = sandbox.run(command, &[("RERANK_SENTINEL_KEY", &secret)]);
        assert_clean(&sentinels, &output, &sandbox, "reranker startup");
    }
    let invalid = sandbox.input(
        "invalid-rerank.toml",
        &std::fs::read_to_string(&config).unwrap().replace(
            "api_key={env='RERANK_SENTINEL_KEY'}",
            &format!("api_key='{secret}{document}'"),
        ),
    );
    let output = sandbox.run(
        ["--config", invalid.to_str().unwrap(), "config", "check"],
        &[],
    );
    assert!(!output.status.success());
    assert_clean(
        &sentinels,
        &output,
        &sandbox,
        "reranker literal credential refusal",
    );
}

/// The real HTTP/JWKS/S3 test also scans serving logs after successful requests.
/// This sentinel covers HTTP configuration and startup errors in both builds.
#[test]
fn http_reader_configuration_and_startup_keep_identity_and_credentials_private() {
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-reader-http");
    let subject = sentinels.mark("http-subject");
    let operator = sentinels.mark("http-operator");
    let config = sandbox.input("http.toml", &format!("[namespace]\nname='privacy-http'\n[storage]\nuri='s3://privacy-http'\nendpoint='http://127.0.0.1:9'\nallow_http=true\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n[identity]\nstdio_user='{subject}'\nhashing_key={{env='HTTP_SENTINEL_KEY'}}\n[identity.oidc]\nissuer='http://127.0.0.1:9'\naudience='fyrr'\njwks_url='http://127.0.0.1:9/jwks'\n[reader.http]\nlisten='127.0.0.1:0'\npublic_url='http://127.0.0.1:9/mcp'\noperator_credential={{env='HTTP_SENTINEL_OPERATOR'}}\n"));
    for args in [
        vec!["config", "show"],
        vec!["reader", "serve"],
        vec!["reader", "check"],
        vec!["reader", "prewarm"],
    ] {
        let mut command = vec!["--config", config.to_str().unwrap()];
        command.extend(args);
        let output = sandbox.run(
            command,
            &[
                ("HTTP_SENTINEL_KEY", &operator),
                ("HTTP_SENTINEL_OPERATOR", &operator),
            ],
        );
        assert_clean(&sentinels, &output, &sandbox, "HTTP reader diagnostics");
    }

    // API keys: the secret, the name and the hashes derived from them stay out of
    // every output, for a valid file, a refused one and a started reader.
    // 32 characters of the secret's random part, beyond the marker.
    const TAIL: &str = "Zk9Qm3Xw7Vb1Nc5Hd2Fg8Jt4Ls6Pr0Ay";
    let secret = |sentinels: &mut Sentinels, category: &str| {
        let secret = format!("fyrr_key_{}-{TAIL}", sentinels.mark(category));
        assert!(fyrr_core::config::is_api_key_secret(&secret), "{secret}");
        sentinels.plant(&secret, category);
        secret
    };
    let shared = secret(&mut sentinels, "api-key-secret");
    sentinels.plant(TAIL, "api-key-secret-tail");
    let wrong = format!("fyrr_key_{}", sentinels.mark("api-key-malformed"));
    // `config show` prints the operator's own file, key names included, so the
    // names are checked everywhere else.
    let mut names = Sentinels::new();
    let mut name = |category: &str| {
        let name = names.mark(category).to_lowercase();
        names.plant(&name, category);
        name
    };
    let (one, two, three) = (
        name("key-name-one"),
        name("key-name-two"),
        name("key-name-three"),
    );
    let group = sentinels.mark("api-key-group");
    // The hashing key is the test process's own PATH, so that this process can
    // derive the hashes the reader would; nothing in this test is secret.
    let config = sandbox.input("api-keys.toml", &format!("[namespace]\nname='privacy-keys'\n[storage]\nuri='s3://privacy-keys'\nendpoint='http://127.0.0.1:9'\nallow_http=true\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n[identity]\nhashing_key={{env='PATH'}}\n[[identity.api_keys]]\nname='{one}'\nkey={{env='KEYS_SENTINEL_ONE'}}\nreaders=['grp:{group}']\n[[identity.api_keys]]\nname='{two}'\nkey={{env='KEYS_SENTINEL_TWO'}}\nreaders=['grp:{group}']\n[[identity.api_keys]]\nname='{three}'\nkey={{env='KEYS_SENTINEL_THREE'}}\nreaders=['grp:{group}']\n[reader.http]\nlisten='127.0.0.1:0'\npublic_url='http://127.0.0.1:9/mcp'\n"));
    let loaded = fyrr_core::config::load(&config).expect("the configuration parses");
    for hash in [
        fyrr_reader::storage::api_key_subject(&loaded.config, &shared).unwrap(),
        fyrr_reader::storage::api_key_deployment_hash(&loaded.config, &shared).unwrap(),
        fyrr_reader::storage::api_key_principal(&loaded.config, &one).unwrap(),
    ] {
        sentinels.plant(&hash, "api-key-hash");
        sentinels.plant(&hash["key:".len()..], "api-key-hash-digits");
        names.plant(&hash, "api-key-hash");
    }
    let environment = [
        ("KEYS_SENTINEL_ONE", shared.as_str()),
        // The same secret twice and a malformed one: refused without echo.
        ("KEYS_SENTINEL_TWO", shared.as_str()),
        ("KEYS_SENTINEL_THREE", wrong.as_str()),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ];
    let commands = [
        vec!["config", "check"],
        vec!["config", "check", "--no-env"],
        vec!["config", "show"],
        vec!["reader", "serve"],
        vec!["reader", "check"],
    ];
    #[cfg(feature = "writer")]
    let commands = commands.into_iter().chain([
        vec!["writer", "status"],
        vec!["writer", "commit"],
        vec!["writer", "keys"],
    ]);
    for args in commands {
        let mut command = vec!["--config", config.to_str().expect("config path")];
        command.extend(args);
        let output = sandbox.run(command.clone(), &environment);
        assert_clean(&sentinels, &output, &sandbox, "API key diagnostics");
        if !command.contains(&"show") {
            assert_clean(&names, &output, &sandbox, "API key names");
        }
        if command == ["--config", config.to_str().unwrap(), "config", "check"] {
            assert!(
                !output.status.success(),
                "the shared and malformed secrets are refused"
            );
        }
    }
}

#[test]
fn directory_snapshot_errors_never_echo_private_input() {
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-directory");
    let marker = sentinels.mark("directory-identity");
    let snapshot = sandbox.input(
        "snapshot.json",
        &format!("{{\"schema_version\":\"1.0\",\"users\":\"{marker}\"}}"),
    );
    let config = common::example_config();
    let result = sandbox.configured(
        &config,
        [
            "writer",
            "directory",
            "--snapshot",
            snapshot.to_str().unwrap(),
        ],
        &[],
    );
    assert!(!result.status.success());
    assert_clean(
        &sentinels,
        &result,
        &sandbox,
        "directory snapshot diagnostics",
    );
}

#[test]
fn dense_reader_and_namespace_paths_keep_identity_and_query_values_private() {
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-dense");
    let user = sentinels.mark("stdio-user");
    let token = sentinels.mark("stdio-reader");
    let query = sentinels.mark("query");
    let config=sandbox.input("fyrr.toml",&format!("[namespace]\nname='privacy'\n[storage]\nuri='s3://privacy'\nendpoint='http://127.0.0.1:9'\nallow_http=true\n[embedding]\nendpoint='https://cache.invalid'\n[identity]\nstdio_user='{user}'\nstdio_readers=['grp:{token}']\nhashing_key={{env='DENSE_PRIVACY_KEY'}}\n"));
    let config = config.to_str().expect("config path");
    for args in [
        vec!["config", "show"],
        vec!["reader", "serve"],
        vec!["reader", "check"],
        vec!["reader", "prewarm"],
        vec!["reader", "search", query.as_str()],
        vec!["writer", "sync"],
        vec!["writer", "sync", "--due"],
        vec!["writer", "status"],
        vec!["writer", "serve", "--jobs", query.as_str()],
        vec!["writer", "serve", "--timeout", "0s"],
        vec!["writer", "directory", "--snapshot", query.as_str()],
        vec!["writer", "keys"],
        vec!["writer", "sync", "--directory"],
        vec![
            "writer",
            "credential",
            "--name",
            query.as_str(),
            "--from-env",
            query.as_str(),
        ],
        vec!["writer", "maintain"],
        vec!["writer", "verify"],
        vec!["writer", "restore", query.as_str()],
        vec!["writer", "rollback"],
        vec!["writer", "source", "retire", query.as_str()],
        vec!["writer", "source", "purge", query.as_str()],
        vec!["writer", "source", "purge", query.as_str(), "--confirm"],
        vec!["writer", "commit"],
        vec!["writer", "commit", "--stage-timings"],
        vec!["writer", "commit", "--receipt", query.as_str()],
        vec!["writer", "namespace", "create"],
        vec!["writer", "namespace", "delete"],
    ] {
        let mut command = vec!["--config", config];
        command.extend(args);
        let result = sandbox.run(
            &command,
            &[
                ("DENSE_PRIVACY_KEY", "test-only-key"),
                ("AWS_EC2_METADATA_DISABLED", "true"),
            ],
        );
        assert_clean(&sentinels, &result, &sandbox, "public output paths");
    }
}

#[test]
fn secrets_from_the_environment_are_never_printed() {
    let mut sentinels = Sentinels::new();
    let values: Vec<(String, String)> = SECRET_VARS
        .iter()
        .map(|name| ((*name).to_owned(), sentinels.mark("env-secret")))
        .collect();
    let env: Vec<(&str, &str)> = values
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    let sandbox = Sandbox::new("privacy-env");
    let config = common::example_config();
    let config = config.to_str().expect("path");
    for args in [
        vec!["--config", config, "config", "check"],
        vec!["--config", config, "config", "show"],
        vec!["--config", config, "config", "schema"],
        vec!["--config", config, "writer", "providers"],
        vec!["--config", config, "writer", "sync", "--dry-run"],
        vec!["--config", config, "reader", "check"],
    ] {
        let output = sandbox.run(&args, &env);
        assert_clean(&sentinels, &output, &sandbox, &args.join(" "));
    }
}

/// Values misplaced in the file: inline secrets, the two redaction gaps
/// found in review (a quote inside a value; a secret-like key inside an
/// array that mixes tables and scalars), type errors and bad reader tokens.
#[test]
fn values_in_the_config_file_never_leave_through_errors() {
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-config");
    let source = |settings: String| {
        format!(
            "\n[[sources]]\nid = \"jira-eng\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"\n[sources.settings]\n{settings}\n"
        )
    };
    let cases = [
        // (config text, whether `config show` must also keep stdout private;
        // non-secret values are intentionally displayed by that command)
        (
            config_with(&source(format!(
                "api_token = \"{}\"",
                sentinels.mark("inline-secret")
            ))),
            true,
        ),
        (
            config_with("").replace(
                "[embedding]\n",
                &format!(
                    "[embedding]\ndimensions = \"a\\\"{} tail\"\n",
                    sentinels.mark("quoted-value")
                ),
            ),
            true,
        ),
        (
            config_with("").replace(
                "[embedding]\n",
                &format!(
                    "[embedding]\ndimensions = \"x`, expected {}\"\n",
                    sentinels.mark("expected-in-value")
                ),
            ),
            true,
        ),
        (
            config_with(&source(format!(
                "list = [{{ api_token = \"{}\" }}, 1]",
                sentinels.mark("mixed-array-secret")
            ))),
            true,
        ),
        (
            config_with(&source(format!(
                "nested = [[{{ password = \"{}\" }}]]",
                sentinels.mark("nested-array-secret")
            ))),
            true,
        ),
        (
            config_with(&format!(
                "[identity.directory]\nprovider = \"{}\"\n",
                sentinels.mark("enum-value")
            )),
            true,
        ),
        (
            config_with(&format!(
                "\n[[sources]]\nid = \"docs\"\ntype = \"local-files\"\npermissions = \"fixed\"\nreaders = [\"bad-{}\"]\n",
                sentinels.mark("config-reader")
            )),
            false,
        ),
        (
            config_with("").replace(
                "name = \"acme\"",
                &format!("name = \"{}\"", sentinels.mark("namespace")),
            ),
            false,
        ),
    ];
    for (index, (text, private_show)) in cases.iter().enumerate() {
        let path = sandbox.input(&format!("case-{index}.toml"), text);
        let path = path.to_str().expect("path");
        let output = sandbox.run(["--config", path, "config", "check", "--no-env"], &[]);
        assert_eq!(output.status.code(), Some(78), "case {index} must fail");
        assert_clean(
            &sentinels,
            &output,
            &sandbox,
            &format!("config check, case {index}"),
        );
        let output = sandbox.run(["--config", path, "config", "show"], &[]);
        assert_eq!(output.status.code(), Some(78), "config show, case {index}");
        if *private_show {
            assert_clean(
                &sentinels,
                &output,
                &sandbox,
                &format!("config show, case {index}"),
            );
        } else {
            assert!(
                sentinels
                    .found_in(&String::from_utf8_lossy(&output.stderr))
                    .is_empty()
            );
        }
    }
}

#[test]
fn query_like_input_is_never_echoed() {
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-query");
    let query = format!("what did {} say", sentinels.mark("query"));
    let bare = sentinels.mark("query-token");
    let config = common::example_config();
    let config = config.to_str().expect("path");
    for args in [
        vec!["reader", "search", query.as_str()],
        vec!["reader", "search", "--", query.as_str()],
        vec!["reader", "search", "--limit", query.as_str()],
        vec!["reader", "search", "--output", query.as_str()],
        vec!["--config", config, "writer", "sync", query.as_str()],
        vec![
            "--config",
            config,
            "writer",
            "sync",
            "--dry-run",
            query.as_str(),
        ],
        vec![
            "--config",
            config,
            "writer",
            "sync",
            "--dry-run",
            bare.as_str(),
        ],
        vec![
            "--config",
            config,
            "writer",
            "sync",
            "--dry-run",
            "--kind",
            bare.as_str(),
        ],
        vec![
            "--config",
            config,
            "writer",
            "sync",
            "--dry-run",
            "--timeout",
            bare.as_str(),
        ],
        vec![query.as_str()],
        vec![bare.as_str()],
        vec!["config", "check", &format!("--{bare}")],
    ] {
        let output = sandbox.run(&args, &[]);
        assert_ne!(output.status.code(), Some(0), "{args:?}");
        assert_clean(&sentinels, &output, &sandbox, &format!("{args:?}"));
    }
}

#[cfg(feature = "writer")]
mod providers {
    use super::*;
    use common::{provider_fixture, source};

    /// The bundled Notion source refuses inline integration credentials and
    /// redacts an environment credential through the real config commands.
    #[test]
    fn notion_credentials_never_leave_config_commands() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-notion");
        let secret = sentinels.mark("notion-integration-secret");
        for (name, token, valid) in [
            ("literal", format!("\"{secret}\""), false),
            (
                "reference",
                "{ env = \"NOTION_SENTINEL_TOKEN\" }".to_owned(),
                true,
            ),
        ] {
            let config = config_with(&format!(
                "\n[[sources]]\nid='notion'\ntype='notion'\npermissions='fixed'\n\
                 readers=['grp:fixture']\n[sources.settings]\ntoken={token}\n\
                 [sources.scope]\nroot_pages=['11111111-2222-3333-4444-555555555555']\n"
            ));
            let path = sandbox.input(&format!("{name}.toml"), &config);
            let path = path.to_str().expect("path");
            for command in ["check", "show"] {
                let output = sandbox.run(
                    ["--config", path, "config", command],
                    &[("NOTION_SENTINEL_TOKEN", secret.as_str())],
                );
                assert_eq!(output.status.success(), valid, "notion config {command}");
                assert_clean(&sentinels, &output, &sandbox, "notion config");
            }
        }
    }

    /// A dry run over a provider whose documents carry markers in every
    /// field, for every sync kind.
    #[test]
    fn a_provider_dry_run_never_prints_document_values() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-dry-run");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let config = config_with(&format!(
            "\n[[sources]]\nid = \"docs\"\ntype = \"sentinel-docs\"\n[sources.intervals]\npermissions = \"30m\"\nmembers = \"30m\"\n[sources.scope]\nfixture = \"{}\"\n",
            fixture.display()
        ));
        let path = sandbox.input("fyrr.toml", &config);
        let output = sandbox.configured(&path, ["writer", "sync", "--dry-run"], &[]);
        let (stdout, stderr) = common::text(&output);
        assert!(output.status.success(), "{stdout}{stderr}");
        for kind in ["content", "permissions", "members", "reconcile"] {
            assert!(stdout.contains(&format!("  {kind}")), "{kind}: {stdout}");
        }
        assert_clean(&sentinels, &output, &sandbox, "dry run");
    }

    /// Contract violations whose offending values are markers.
    #[test]
    fn contract_validation_errors_never_echo_values() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-contract");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let variants = [
            "readers",
            "attributes",
            "root-id",
            "null-text",
            "null-readers",
            "large-string",
            "duplicate",
            "cursor",
        ];
        let mut config = String::new();
        for variant in variants {
            config.push_str(&source(
                variant,
                "sentinel-invalid",
                &fixture,
                &format!("variant = \"{variant}\""),
            ));
        }
        let path = sandbox.input("fyrr.toml", &config_with(&config));
        let output = sandbox.configured(&path, ["writer", "sync", "--dry-run"], &[]);
        let (stdout, _) = common::text(&output);
        assert_eq!(output.status.code(), Some(1), "{stdout}");
        assert!(
            stdout.contains(&format!("0 source(s) passed, {} failed", variants.len())),
            "{stdout}"
        );
        assert_clean(&sentinels, &output, &sandbox, "contract violations");
    }

    /// Providers that raise, exit or abort with markers in their messages.
    #[test]
    fn failing_providers_never_echo_their_messages() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-failing");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let mut config = String::new();
        for mode in ["raise", "exit", "abort"] {
            config.push_str(&source(
                mode,
                "sentinel-failing",
                &fixture,
                &format!("mode = \"{mode}\""),
            ));
        }
        let path = sandbox.input("fyrr.toml", &config_with(&config));
        let output = sandbox.configured(&path, ["writer", "sync", "--dry-run"], &[]);
        let (stdout, _) = common::text(&output);
        assert!(stdout.contains("0 source(s) passed, 3 failed"), "{stdout}");
        assert!(stdout.contains("the provider failed"), "{stdout}");
        assert!(
            stdout.contains("source exit (provider sentinel-failing 1.0.0): FAILED"),
            "{stdout}"
        );
        assert_clean(&sentinels, &output, &sandbox, "failing providers");
    }

    /// GitHub App keys use only environment references; config commands never reveal them.
    #[test]
    fn github_app_keys_never_leave_config_commands() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-github");
        let secret = sentinels.mark("github-app-private-key");
        for (name, key, valid) in [
            ("literal", format!("\"{secret}\""), false),
            (
                "reference",
                "{ env = \"GITHUB_SENTINEL_KEY\" }".to_owned(),
                true,
            ),
        ] {
            let config = config_with(&format!(
                "\n[[sources]]\nid='github'\ntype='github'\npermissions='fixed'\n\
                 readers=['grp:fixture']\n[sources.settings]\napp_id=10\ninstallation_id=20\naccount_id=30\n\
                 private_key={key}\n[[sources.scope.repositories]]\nid=1\nbranch='main'\n"
            ));
            let path = sandbox.input(&format!("{name}.toml"), &config);
            for command in ["check", "show"] {
                let output = sandbox.run(
                    ["--config", path.to_str().unwrap(), "config", command],
                    &[("GITHUB_SENTINEL_KEY", secret.as_str())],
                );
                assert_eq!(output.status.success(), valid, "github config {command}");
                assert_clean(&sentinels, &output, &sandbox, "github config");
            }
        }
    }

    /// Slack user tokens use only environment references; config commands never reveal them.
    #[test]
    fn slack_user_tokens_never_leave_config_commands() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-slack");
        let secret = sentinels.mark("slack-user-token");
        for (name, key, valid) in [
            ("literal", format!("\"{secret}\""), false),
            (
                "reference",
                "{ env = \"SLACK_SENTINEL_TOKEN\" }".to_owned(),
                true,
            ),
        ] {
            let config = config_with(&format!(
                "\n[[sources]]\nid='slack'\ntype='slack'\npermissions='fixed'\n\
                 readers=['grp:fixture']\n[sources.settings]\nworkspace_id='T001'\ntoken={key}\n\
                 [sources.scope]\ninclude=['C001']\n"
            ));
            let path = sandbox.input(&format!("{name}.toml"), &config);
            for command in ["check", "show"] {
                let output = sandbox.run(
                    ["--config", path.to_str().unwrap(), "config", command],
                    &[("SLACK_SENTINEL_TOKEN", secret.as_str())],
                );
                assert_eq!(output.status.success(), valid, "slack config {command}");
                assert_clean(&sentinels, &output, &sandbox, "slack config");
            }
        }
    }

    /// The version the `sentinel-versioned` distribution reports instead of one.
    const VERSION_SENTINEL: &str = "SENTINEL-VERSION";

    /// Invalid or raising provider metadata with markers in it.
    #[test]
    fn provider_listing_never_prints_metadata_values() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-metadata");
        let marker = sentinels.mark("metadata");
        let env = [("FYRR_TEST_METADATA_MARKER", marker.as_str())];
        let output = sandbox.run(["writer", "providers"], &env);
        let (stdout, _) = common::text(&output);
        assert_eq!(output.status.code(), Some(1), "{stdout}");
        assert!(stdout.contains("sentinel-bad-metadata"), "{stdout}");
        assert!(stdout.contains("its metadata callable failed"), "{stdout}");
        // A distribution whose version is not a version is listed as invalid,
        // and what it said is printed nowhere.
        assert!(stdout.contains("sentinel-versioned"), "{stdout}");
        assert!(stdout.contains("<invalid>"), "{stdout}");
        assert!(!stdout.contains(VERSION_SENTINEL), "{stdout}");
        assert_clean(&sentinels, &output, &sandbox, "providers");
        for args in [
            vec!["writer", "__worker", "discover"],
            vec![
                "writer",
                "__worker",
                "metadata",
                "--provider",
                "sentinel-bad-metadata",
            ],
        ] {
            let output = sandbox.run(&args, &env);
            assert_eq!(output.status.code(), Some(1));
            assert_clean(&sentinels, &output, &sandbox, "internal metadata worker");
        }

        let config = config_with(
            "\n[[sources]]\nid = \"bad\"\ntype = \"sentinel-bad-metadata\"\npermissions = \"fixed\"\nreaders = [\"grp:x\"]\n",
        );
        let path = sandbox.input("fyrr.toml", &config);
        let output = sandbox.configured(&path, ["config", "check", "--no-env"], &env);
        assert_eq!(output.status.code(), Some(78));
        assert_clean(
            &sentinels,
            &output,
            &sandbox,
            "config check with refused provider",
        );
        let (stdout, stderr) = common::text(&output);
        assert!(!format!("{stdout}{stderr}").contains(VERSION_SENTINEL));

        // A required property the provider's own schema names, which the
        // source leaves out: the diagnostic says that one is missing, not which.
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let config = config_with(&source("required", "sentinel-bad-required", &fixture, ""));
        let path = sandbox.input("required.toml", &config);
        let output = sandbox.configured(&path, ["config", "check", "--no-env"], &env);
        let (stdout, stderr) = common::text(&output);
        assert_eq!(output.status.code(), Some(78), "{stdout}{stderr}");
        assert!(
            stderr.contains("a field the provider requires is missing"),
            "{stderr}"
        );
        assert_clean(&sentinels, &output, &sandbox, "config check, required name");
    }

    /// A provider field marked secret (under a key fyrr's own heuristic does
    /// not recognize) refuses a literal without echoing it, and the secret
    /// the connector reads from the environment is never printed.
    #[test]
    fn provider_secret_fields_take_only_references() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-provider-secret");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let literal = config_with(&source(
            "secretive",
            "sentinel-secret",
            &fixture,
            &format!("access = \"{}\"", sentinels.mark("provider-secret")),
        ));
        let path = sandbox.input("literal.toml", &literal);
        let path = path.to_str().expect("path");
        let output = sandbox.run(["--config", path, "config", "check", "--no-env"], &[]);
        let (_, stderr) = common::text(&output);
        assert_eq!(output.status.code(), Some(78), "{stderr}");
        assert!(
            stderr.contains("sources[0].settings.access: a secret field holds"),
            "{stderr}"
        );
        assert_clean(&sentinels, &output, &sandbox, "config check");
        let output = sandbox.run(["--config", path, "config", "show"], &[]);
        assert_clean(&sentinels, &output, &sandbox, "config show");

        let reference = config_with(&source(
            "secretive",
            "sentinel-secret",
            &fixture,
            "access = { env = \"SENTINEL_ACCESS\" }",
        ));
        let path = sandbox.input("reference.toml", &reference);
        let secret = sentinels.mark("provider-env-secret");
        let env = [("SENTINEL_ACCESS", secret.as_str())];
        let path = path.to_str().expect("path");
        for args in [
            vec!["--config", path, "config", "check"],
            vec!["--config", path, "config", "show"],
            vec!["--config", path, "writer", "sync", "--dry-run"],
        ] {
            let output = sandbox.run(&args, &env);
            assert!(
                output.status.success(),
                "{args:?}: {:?}",
                common::text(&output)
            );
            assert_clean(&sentinels, &output, &sandbox, &args.join(" "));
        }
    }
    fn worker_report_case(mode: &str) {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-worker-report");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let config = sandbox.input(
            "fyrr.toml",
            &config_with(&format!(
                "[writer]\nenvelope_max_bytes = \"2MiB\"\ndocument_max_bytes = \"1MiB\"\n{}",
                source(
                    "review",
                    "sentinel-review",
                    &fixture,
                    &format!("mode = \"{mode}\"")
                )
            )),
        );
        let output = sandbox.configured(&config, ["writer", "sync", "--dry-run"], &[]);
        assert_clean(&sentinels, &output, &sandbox, mode);
        assert_eq!(
            output.status.code(),
            Some(if mode == "print" { 0 } else { 1 }),
            "{mode}: {:?}",
            common::text(&output)
        );
        // A report forged on the result channel reached the supervisor, which
        // read it instead of the worker's own: the worker's own report, were
        // it the one read, would say the provider failed (the forging provider
        // ends its host without finishing). The validator refused the forgery
        // and reports the worker's failure, which is its own diagnostic.
        if mode.starts_with("result-") {
            let (stdout, _) = common::text(&output);
            assert!(
                stdout.contains("error: the worker failed"),
                "{mode}: {stdout}"
            );
            assert!(!stdout.contains("the provider failed"), "{mode}: {stdout}");
        }
    }

    #[test]
    fn worker_report_cases() {
        for mode in [
            "print",
            "forge",
            "forge-error",
            "extra-name",
            "extra-huge",
            "text-huge",
            "allocate",
        ] {
            worker_report_case(mode);
        }
    }

    #[test]
    fn nested_secret_lists_are_redacted_and_validated() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-nested-secret");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let marker = sentinels.mark("nested-access");
        let config = sandbox.input(
            "fyrr.toml",
            &config_with(&source(
                "nested",
                "sentinel-nested",
                &fixture,
                &format!("accounts = [{{ access = \"{marker}\" }}]"),
            )),
        );
        for action in ["check", "show"] {
            let output = sandbox.configured(&config, ["config", action], &[]);
            assert_clean(&sentinels, &output, &sandbox, action);
            assert_eq!(
                output.status.code(),
                Some(78),
                "{action}: {:?}",
                common::text(&output)
            );
        }
        let config = sandbox.input(
            "valid.toml",
            &config_with(&source(
                "nested",
                "sentinel-nested",
                &fixture,
                "accounts = [{ access = { env = \"NESTED_ACCESS\" } }]",
            )),
        );
        for action in ["check", "show"] {
            let output = sandbox.configured(
                &config,
                ["config", action],
                &[("NESTED_ACCESS", marker.as_str())],
            );
            assert_clean(&sentinels, &output, &sandbox, action);
            assert!(
                output.status.success(),
                "{action}: {:?}",
                common::text(&output)
            );
        }
    }

    #[test]
    fn result_channel_rejects_empty_wrong_kind_and_free_text_reports() {
        for mode in ["result-empty", "result-kinds", "result-error"] {
            worker_report_case(mode);
        }
    }

    fn worker_descendants_case(detached: bool) {
        use std::os::unix::process::CommandExt;
        use std::time::{Duration, Instant};
        for mode in [
            "child-success",
            "child-error",
            "child-exit",
            "child-timeout",
            "child-parent-error",
        ] {
            let mut sentinels = Sentinels::new();
            let sandbox = Sandbox::new("privacy-worker-child");
            let pid_file = sandbox.output().join("child.pid");
            let mut data: serde_json::Value =
                serde_json::from_str(&provider_fixture(&mut sentinels)).unwrap();
            data["pid_file"] = pid_file.to_str().unwrap().into();
            data["detached"] = detached.into();
            let fixture = sandbox.input("docs.json", &data.to_string());
            let config = sandbox.input(
                "fyrr.toml",
                &config_with(&source(
                    "review",
                    "sentinel-review",
                    &fixture,
                    &format!("mode = \"{mode}\""),
                )),
            );
            // Watchdog also bounds the test when the old blocking reader regresses.
            let mut command = common::fyrr();
            command
                .args([
                    "--config",
                    config.to_str().unwrap(),
                    "writer",
                    "sync",
                    "--dry-run",
                    "--timeout",
                    "2s",
                ])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .process_group(0);
            let mut child = command.spawn().unwrap();
            let started = Instant::now();
            let completed = loop {
                if child.try_wait().unwrap().is_some() {
                    break true;
                }
                if started.elapsed() > Duration::from_secs(15) {
                    break false;
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            let pid = std::fs::read_to_string(&pid_file)
                .ok()
                .and_then(|p| p.parse::<u32>().ok());
            if !completed {
                let _ = std::process::Command::new("/bin/kill")
                    .args(["-KILL", "--", &format!("-{}", child.id())])
                    .status();
            }
            let output = child.wait_with_output().unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            let alive = |pid: u32| {
                std::fs::read_to_string(format!("/proc/{pid}/stat"))
                    .ok()
                    .is_some_and(|stat| {
                        stat.rsplit_once(") ")
                            .is_some_and(|(_, tail)| !tail.starts_with(['Z', 'X']))
                    })
            };
            if let Some(pid) = pid {
                while alive(pid) && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(20));
                }
                let survived = std::path::Path::new(&format!("/proc/{pid}/stat")).exists();
                if survived {
                    let _ = std::process::Command::new("/bin/kill")
                        .args(["-KILL", &pid.to_string()])
                        .status();
                }
                assert!(
                    !survived,
                    "{mode} detached={detached}: descendant survived or was not reaped"
                );
            } else {
                panic!(
                    "{mode}: provider did not create child: {:?}",
                    common::text(&output)
                );
            }
            assert!(completed, "{mode}: CLI exceeded watchdog");
            assert_eq!(
                output.status.code(),
                Some(if mode == "child-success" { 0 } else { 1 }),
                "{mode}: {:?}",
                common::text(&output)
            );
            assert_clean(&sentinels, &output, &sandbox, mode);
        }
    }

    #[test]
    fn unconfigured_aborting_metadata_does_not_disable_healthy_sources() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-isolated-metadata");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let config = sandbox.input(
            "fyrr.toml",
            &config_with(&source("healthy", "sentinel-docs", &fixture, "")),
        );
        let env = [("FYRR_TEST_METADATA_ABORT", "1")];
        let output = sandbox.run(["writer", "providers"], &env);
        assert_eq!(output.status.code(), Some(1));
        let (stdout, _) = common::text(&output);
        assert!(
            stdout
                .lines()
                .any(|line| line.starts_with("sentinel-docs ") && line.ends_with("ok")),
            "{stdout}"
        );
        assert!(stdout.contains("sentinel-raising-metadata"), "{stdout}");
        assert_clean(&sentinels, &output, &sandbox, "metadata listing");
        let output = sandbox.configured(&config, ["writer", "sync", "--dry-run"], &env);
        assert!(output.status.success(), "{:?}", common::text(&output));
        assert_clean(&sentinels, &output, &sandbox, "isolated source");
    }

    #[test]
    fn oversized_text_intake_does_not_materialize_a_second_copy() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-intake-rss");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let config = sandbox.input(
            "fyrr.toml",
            &config_with(&format!(
                "[writer]\nenvelope_max_bytes = \"2MiB\"\ndocument_max_bytes = \"1MiB\"\n{}",
                source(
                    "review",
                    "sentinel-review",
                    &fixture,
                    "mode = \"text-huge\""
                )
            )),
        );
        let rss_file = sandbox.output().join("rss-kib.txt");
        let baseline_config = sandbox.input(
            "baseline.toml",
            &std::fs::read_to_string(&config)
                .unwrap()
                .replace("text-huge", "baseline"),
        );
        let baseline_rss = sandbox.output().join("baseline-rss-kib.txt");
        let base = common::fyrr();
        let mut peaks = Vec::new();
        for (config, rss_file, exit) in [
            (&baseline_config, &baseline_rss, 0),
            (&config, &rss_file, 1),
        ] {
            let mut command = std::process::Command::new("/usr/bin/time");
            command
                .args(["-f", "%M", "-o"])
                .arg(rss_file)
                .arg(base.get_program());
            for (name, value) in base.get_envs() {
                if let Some(value) = value {
                    command.env(name, value);
                } else {
                    command.env_remove(name);
                }
            }
            let output = command
                .args([
                    "--config",
                    config.to_str().unwrap(),
                    "writer",
                    "sync",
                    "--dry-run",
                ])
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(exit),
                "{:?}",
                common::text(&output)
            );
            peaks.push(
                std::fs::read_to_string(rss_file)
                    .unwrap()
                    .lines()
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
            );
            assert_clean(&sentinels, &output, &sandbox, "oversized intake RSS");
        }
        // The producer allocates a 64 MiB Python string and a 64 MiB Arrow buffer.
        // Subtract this executable's measured startup RSS so linked Lance code cannot
        // masquerade as a document copy. A third 64 MiB text copy exceeds this bound.
        assert!(
            peaks[1].saturating_sub(peaks[0]) < 160 * 1024,
            "oversized intake copied text: baseline/oversized peak RSS {peaks:?} KiB"
        );
    }
    #[test]
    fn config_show_refuses_unset_provider_secrets() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-unset-provider-secret");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let config = sandbox.input(
            "fyrr.toml",
            &config_with(&source(
                "nested",
                "sentinel-nested",
                &fixture,
                "accounts = [{ access = { env = \"FYRR_TEST_UNSET_NESTED_ACCESS\" } }]",
            )),
        );
        for action in ["check", "show"] {
            let output = sandbox.configured(&config, ["config", action], &[]);
            assert_eq!(
                output.status.code(),
                Some(78),
                "{action}: {:?}",
                common::text(&output)
            );
            assert_clean(&sentinels, &output, &sandbox, action);
        }
    }
    #[test]
    fn worker_groups_are_cleaned_on_every_exit_path() {
        worker_descendants_case(false);
    }

    #[test]
    fn detached_worker_descendants_are_killed_and_reaped_on_every_exit_path() {
        worker_descendants_case(true);
    }

    #[test]
    fn scalar_secret_arrays_are_never_printed_as_aggregates() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("privacy-scalar-secret-array");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let marker = sentinels.mark("scalar-secret");
        let config = sandbox.input(
            "fyrr.toml",
            &config_with(&source(
                "secrets",
                "sentinel-nested",
                &fixture,
                &format!("accounts = []\naccesses = [\"{marker}\"]"),
            )),
        );
        let output = sandbox.configured(&config, ["config", "show"], &[]);
        assert_clean(&sentinels, &output, &sandbox, "scalar secret aggregate");
        assert_eq!(
            output.status.code(),
            Some(78),
            "{:?}",
            common::text(&output)
        );
        let (stdout, _) = common::text(&output);
        assert!(
            stdout.contains("sources[0].settings.accesses = <redacted: secret field>"),
            "{stdout}"
        );
    }
}

#[test]
fn mcp_validation_errors_keep_filter_cursor_and_window_values_private() {
    use fyrr_core::mcp::{ErrorCode, FetchInput, SearchInput, ToolError};
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-mcp-validation");
    let filter = sentinels.mark("filter");
    let cursor = sentinels.mark("cursor");
    let revision = sentinels.mark("window-revision");
    let search: SearchInput =
        serde_json::from_value(serde_json::json!({"query":"x","authors":[filter.repeat(512)]}))
            .unwrap();
    let fetch:FetchInput=serde_json::from_value(serde_json::json!({"cursor":cursor,"source_id":"s","root_id":"r","window":{"record_id":"r:0","revision":revision,"start":2,"end":1}})).unwrap();
    for result in [search.validate(), fetch.validate()] {
        assert_eq!(result, Err("invalid_argument"));
        let error = ToolError {
            code: ErrorCode::InvalidArgument,
            message: ErrorCode::InvalidArgument.meaning().into(),
            retryable: false,
        };
        let output = std::process::Output {
            status: std::os::unix::process::ExitStatusExt::from_raw(0),
            stdout: serde_json::to_vec(&error).unwrap(),
            stderr: vec![],
        };
        assert_clean(&sentinels, &output, &sandbox, "MCP input diagnostics");
    }
}

/// Inspect the real durable objects, not just writer output. Both the connector
/// batches and assembled membership input must be encrypted, as must raw SCIM state.
#[cfg(feature = "writer")]
#[tokio::test]
#[ignore = "owned RustFS and real writer HTTP; scripts/ci"]
async fn identity_storage_inputs_credentials_missing_key_and_rotation_are_fail_closed() {
    use futures::TryStreamExt;
    use fyrr_core::Contract;
    use object_store::ObjectStoreExt;
    use serde_json::{Value, json};
    use std::process::{Command, Stdio};
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-encrypted-storage");
    let source_id = sentinels.mark("source-account");
    let email = format!("{}@example.test", sentinels.mark("email"));
    let group_id = sentinels.mark("group-account");
    let subject = sentinels.mark("subject");
    let issuer = format!("https://{}.example.test", sentinels.mark("issuer"));
    let credential = sentinels.mark("connector-secret");
    let provider_token = sentinels.mark("scim-secret");
    let operator_token = sentinels.mark("operator-secret");
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let bucket = std::env::var("FYRR_TEST_S3_BUCKET").unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let input=sandbox.input("members.json",&json!({"epoch":1,"members":[{"last":false,"users":[{"id":source_id,"email":email,"email_verification":"verified_corporate","active":true}]},{"last":true,"groups":[{"id":group_id,"members":[format!("usr:{source_id}")]}]}]}).to_string());
    let mut inputs: Value = serde_json::from_slice(&std::fs::read(&input).unwrap()).unwrap();
    inputs["content"] = json!([[{"id":"permission-only","revision":"r1","readers":[format!("usr:{source_id}")],"texts":["storage canary document"]}]]);
    inputs["permissions"] = json!([[{"id":"permission-only","revision":"p1","readers":[format!("usr:{source_id}"),format!("grp:{group_id}")]}]]);
    inputs["container_grants"] = json!([[{"container_id":"container-only","container_kind":"folder","permission_revision":"p1","readers":[format!("usr:{source_id}")]}]]);
    std::fs::write(&input, serde_json::to_vec(&inputs).unwrap()).unwrap();
    let config=sandbox.input("fyrr.toml",&format!("[namespace]\nname='privacy-storage'\n[storage]\nuri='s3://{bucket}/privacy-storage-{}-{}'\nendpoint='{endpoint}'\nallow_http=true\naccess_key_id={{env='FYRR_TEST_S3_ACCESS_KEY_ID'}}\nsecret_access_key={{env='FYRR_TEST_S3_SECRET_ACCESS_KEY'}}\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n[identity]\nhashing_key={{env='FYRR_TEST_S3_SECRET_ACCESS_KEY'}}\nencryption_key={{env='FYRR_TEST_ENCRYPTION_KEY'}}\n[identity.directory]\nprovider='scim'\nissuer='{issuer}'\nverified_domains=['example.test']\ncredentials={{env='SCIM_TOKEN'}}\n[writer.credentials]\nSTORED_PROVIDER_SECRET='provider-secret'\n[writer.commit]\ncoalescing_timer='10m'\n[writer.serve.admin]\nlisten='{address}'\noperator_token={{env='OP_TOKEN'}}\n[[sources]]\nid='incremental'\ntype='incremental-test'\npermissions='sync'\n[sources.intervals]\npermissions='5m'\nmembers='5m'\n[sources.scope]\nfixture='{}'\n[[sources]]\nid='secret-source'\ntype='sentinel-secret'\npermissions='fixed'\nreaders=['grp:fixture']\n[sources.settings]\naccess={{env='STORED_PROVIDER_SECRET'}}\n[sources.scope]\nfixture='{}'\n",std::process::id(),fyrr_core::clock::now().timestamp_micros(),input.display(),input.display()));
    let embedding = common::embedding::Provider::open().await;
    let text = common::embedding::configure(
        &std::fs::read_to_string(&config).unwrap(),
        &format!(
            "endpoint='{}'\napi_key={{env='FYRR_TEST_S3_SECRET_ACCESS_KEY'}}\nspend.limit_microusd=1000000\nspend.max_call_microusd=1",
            embedding.endpoint
        ),
    );
    std::fs::write(&config, text).unwrap();
    let args = |rest: Vec<&'static str>| {
        let mut v = vec!["--config", config.to_str().unwrap()];
        v.extend(rest);
        v
    };
    let result = sandbox.run(args(vec!["writer", "namespace", "create"]), &[]);
    assert!(result.status.success());
    assert_clean(&sentinels, &result, &sandbox, "namespace");
    let result = sandbox.run(
        args(vec![
            "writer",
            "credential",
            "--name",
            "provider-secret",
            "--from-env",
            "CREDENTIAL_INPUT",
        ]),
        &[("CREDENTIAL_INPUT", &credential)],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_clean(&sentinels, &result, &sandbox, "encrypted credential write");
    let result = sandbox.run(
        args(vec![
            "writer",
            "credential",
            "--name",
            "provider-secret",
            "--from-env",
            "CREDENTIAL_INPUT",
        ]),
        &[("CREDENTIAL_INPUT", &credential)],
    );
    assert!(result.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&result.stdout).unwrap()["changed"],
        false
    );
    assert_clean(
        &sentinels,
        &result,
        &sandbox,
        "idempotent encrypted credential write",
    );
    // The connector reveals its Secret inside the isolated real Python worker.
    // STORED_PROVIDER_SECRET has no process-environment value.
    let result = sandbox.run(
        args(vec!["writer", "sync", "secret-source", "--kind", "content"]),
        &[],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_clean(
        &sentinels,
        &result,
        &sandbox,
        "encrypted connector credential reveal",
    );
    let result = sandbox.run(
        args(vec!["writer", "sync", "incremental", "--kind", "content"]),
        &[],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_clean(&sentinels, &result, &sandbox, "hashed content readers");
    let result = sandbox.run(
        args(vec!["writer", "sync", "incremental", "--kind", "members"]),
        &[],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_clean(&sentinels, &result, &sandbox, "multipart membership");
    let result = sandbox.run(
        args(vec![
            "writer",
            "sync",
            "incremental",
            "--kind",
            "permissions",
        ]),
        &[],
    );
    assert!(result.status.success());
    assert_clean(
        &sentinels,
        &result,
        &sandbox,
        "hashed permission and grant inputs",
    );
    let result = sandbox.run(args(vec!["writer", "commit"]), &[]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_clean(&sentinels, &result, &sandbox, "assembled membership");
    let mut process = common::fyrr()
        .args(args(vec!["writer", "serve", "--jobs", "1"]))
        .env("SCIM_TOKEN", &provider_token)
        .env("OP_TOKEN", &operator_token)
        .stdout(std::fs::File::create(sandbox.output().join("http.stdout")).unwrap())
        .stderr(std::fs::File::create(sandbox.output().join("http.stderr")).unwrap())
        .spawn()
        .unwrap();
    // Own and reap the child even if an assertion fails.
    struct Owned(std::process::Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let client = reqwest::Client::new();
    let url = format!("http://{address}/scim/v2/Users");
    for _ in 0..100 {
        if client
            .get(&url)
            .bearer_auth(&provider_token)
            .send()
            .await
            .is_ok()
        {
            break;
        }
        assert!(process.try_wait().unwrap().is_none());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let process = Owned(process);
    let response=client.post(&url).bearer_auth(&provider_token).json(&json!({"schemas":["urn:ietf:params:scim:schemas:core:2.0:User","urn:fyrr:params:scim:schemas:extension:identity:2.0:User"],"userName":email,"active":true,"emails":[{"value":email,"primary":true}],"urn:fyrr:params:scim:schemas:extension:identity:2.0:User":{"subjects":[{"issuer":issuer,"subject":subject}]}})).send().await.unwrap();
    assert_eq!(response.status(), 201);
    let _ = Command::new("kill")
        .args(["-TERM", &process.0.id().to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, root) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let before = fyrr_reader::storage::optional_head(&loaded.config)
        .await
        .unwrap()
        .unwrap();
    let objects = store
        .list(Some(&root))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut members = 0;
    let mut directory = 0;
    let mut credentials = 0;
    let mut permissions = 0;
    let mut grants = 0;
    for object in objects {
        let name = object.location.as_ref();
        if !name.contains("/members/")
            && !name.contains("/directory/")
            && !name.contains("/credentials/")
            && !name.contains("/envelopes/")
            && !name.contains("/grants/")
            && !name.contains("/identity/")
        {
            continue;
        }
        let bytes = store
            .get(&object.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            sentinels.found_in(&text).is_empty(),
            "plaintext identity in durable input"
        );
        if name.contains("/envelopes/") || name.contains("/grants/") {
            let prepared =
                fyrr_core::parse_contract::<fyrr_core::envelope::PreparedEnvelope>(&bytes).unwrap();
            if let fyrr_core::envelope::EnvelopePayload::Permissions(p) = prepared.payload {
                permissions += 1;
                if name.contains("/grants/") {
                    grants += 1;
                }
                for token in p
                    .documents
                    .iter()
                    .flat_map(|d| &d.readers)
                    .chain(p.container_grants.iter().flat_map(|g| &g.readers))
                {
                    assert!(token.starts_with("tok:") && token.len() == 68);
                }
            }
            continue;
        }
        if name.contains("/identity/") {
            continue;
        }
        let envelope =
            fyrr_core::parse_contract::<fyrr_core::secret::EncryptedSecret>(&bytes).unwrap();
        envelope.validate().unwrap();
        let mut another_version = envelope.clone();
        another_version.schema_version = fyrr_core::SchemaVersion::new(1, 1);
        assert!(
            fyrr_writer::secrets::open(&loaded.config, &object.location, &another_version).is_err()
        );
        let mut corrupted = envelope.clone();
        corrupted.ciphertext.replace_range(
            0..1,
            if corrupted.ciphertext.starts_with('0') {
                "1"
            } else {
                "0"
            },
        );
        assert!(fyrr_writer::secrets::open(&loaded.config, &object.location, &corrupted).is_err());
        let mut another_namespace = loaded.config.clone();
        another_namespace.namespace.name = "other-namespace".into();
        assert!(
            fyrr_writer::secrets::open(&another_namespace, &object.location, &envelope).is_err()
        );
        let plain =
            fyrr_writer::secrets::open(&loaded.config, &object.location, &envelope).unwrap();
        let plain = String::from_utf8_lossy(&plain);
        if name.contains("/members/") {
            members += 1;
        }
        if name.contains("/directory/") {
            directory += 1;
        }
        if name.contains("/credentials/") {
            credentials += 1;
            assert!(plain.contains(&credential));
        }
        assert!(
            fyrr_writer::secrets::open(
                &loaded.config,
                &object_store::path::Path::from(format!("{root}/wrong-object")),
                &envelope
            )
            .is_err()
        );
    }
    assert!(members >= 3, "original multipart and assembled inputs");
    assert!(
        directory >= 2,
        "hashed snapshot and raw SCIM resource state"
    );
    assert_eq!(credentials, 1);
    assert!(
        permissions >= 2 && grants >= 1,
        "permission envelope and grant snapshot scanned"
    );
    let dataset = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded.config),
        loaded
            .config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        before.content.as_ref().unwrap().lance_version,
        None,
    )
    .await
    .unwrap();
    let batch = dataset
        .scan()
        .project(&["readers"])
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let readers = batch
        .column_by_name("readers")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap();
    let values = readers
        .values()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert!(values.iter().next().is_some());
    for token in values.iter().flatten() {
        assert!(sentinels.found_in(token).is_empty());
        assert!(token.starts_with("tok:") && token.len() == 68);
    }
    // Keep major-version refusal at the encrypted contract boundary, rather than
    // trusting an otherwise authenticated envelope's inner representation.
    let source = before.identity.object.clone();
    let identity: fyrr_core::identity::IdentityArtifact = fyrr_reader::storage::read_contract(
        store.as_ref(),
        &object_store::path::Path::from(format!("{root}/{source}")),
    )
    .await
    .unwrap();
    let member = identity
        .inputs
        .iter()
        .find(|i| {
            i.kind == fyrr_core::identity::IdentityInputKind::Source
                && !i.id.starts_with("permissions:")
        })
        .unwrap();
    let path = object_store::path::Path::from(format!("{root}/{}", member.object));
    let mut future: Value = serde_json::from_slice(
        &fyrr_writer::secrets::read_bytes(&loaded.config, store.as_ref(), &path)
            .await
            .unwrap(),
    )
    .unwrap();
    future["schema_version"] = json!("2.0");
    let future_path =
        object_store::path::Path::from(format!("{root}/members/future-major-refusal.json"));
    fyrr_writer::secrets::put(&loaded.config, store.as_ref(), &future_path, &future)
        .await
        .unwrap();
    assert!(
        fyrr_writer::secrets::read::<fyrr_core::connector::ConnectorBatch>(
            &loaded.config,
            store.as_ref(),
            &future_path
        )
        .await
        .is_err()
    );

    for rotated in [
        None,
        Some("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00"),
    ] {
        let mut command = common::fyrr();
        command.args(args(vec!["writer", "commit"]));
        if let Some(key) = rotated {
            command.env("FYRR_TEST_ENCRYPTION_KEY", key);
        } else {
            command.env_remove("FYRR_TEST_ENCRYPTION_KEY");
        }
        let result = command.output().unwrap();
        assert!(!result.status.success());
        assert_clean(
            &sentinels,
            &result,
            &sandbox,
            "missing/rotated encryption key refuses membership rebuild",
        );
        let after = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.identity, before.identity);
        let mut command = common::fyrr();
        command
            .args(args(vec![
                "writer",
                "credential",
                "--name",
                "provider-secret",
                "--from-env",
                "CREDENTIAL_INPUT",
            ]))
            .env("CREDENTIAL_INPUT", &credential);
        if let Some(key) = rotated {
            command.env("FYRR_TEST_ENCRYPTION_KEY", key);
        } else {
            command.env_remove("FYRR_TEST_ENCRYPTION_KEY");
        }
        let result = command.output().unwrap();
        assert!(!result.status.success());
        assert_clean(
            &sentinels,
            &result,
            &sandbox,
            "missing/rotated key refuses credential replacement",
        );
    }
    let result = sandbox.run(args(vec!["writer", "status"]), &[]);
    assert!(result.status.success());
    let status: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(status["directory"]["encrypted_inputs"], true);
    assert_clean(&sentinels, &result, &sandbox, "status and HTTP logs");
    let target = &before.content.as_ref().unwrap().publication_id;
    let result = sandbox.run(
        [
            "--config",
            config.to_str().unwrap(),
            "writer",
            "restore",
            target,
        ],
        &[],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_clean(
        &sentinels,
        &result,
        &sandbox,
        "private restore working index",
    );
}

#[test]
fn routing_configuration_keeps_private_values_out_of_errors() {
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-routing");
    let subject = sentinels.mark("routing-subject");
    let query = sentinels.mark("routing-query");
    for route in ["exact", "rabitq", "auto"] {
        let config = sandbox.input(&format!("{route}.toml"), &format!("[namespace]\nname='privacy'\n[storage]\nuri='s3://privacy'\nendpoint='http://127.0.0.1:9'\nallow_http=true\n[embedding]\nendpoint='https://cache.invalid'\n[identity]\nstdio_user='{subject}'\nhashing_key={{env='PRIVACY_ROUTING_KEY'}}\n[reader]\nrouting='{route}'\n"));
        let result = sandbox.run(
            [
                "--config",
                config.to_str().unwrap(),
                "reader",
                "search",
                &query,
            ],
            &[
                ("PRIVACY_ROUTING_KEY", "test-only-key"),
                ("AWS_EC2_METADATA_DISABLED", "true"),
            ],
        );
        assert_clean(&sentinels, &result, &sandbox, "routing error boundary");
    }
}

/// The new local vector artifacts join the same stdout/stderr/file sentinel
/// as the CLI. Provider bodies and bearer credentials never enter the cache.
#[cfg(feature = "writer")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real CLI worker, local fake embedding provider and owned RustFS; scripts/ci"]
async fn identity_storage_writer_embedding_cache_and_failures_keep_values_private() {
    use axum::response::IntoResponse;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-writer-embedding");
    let text = sentinels.mark("writer-document");
    let secret = sentinels.mark("writer-embedding-key");
    let provider_body = sentinels.mark("writer-provider-body");
    let reader = format!("grp:{}", sentinels.mark("writer-reader"));
    let file = sandbox.input("source.txt", &text);
    let calls = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let app = axum::Router::new()
        .route(
            "/embeddings/models",
            axum::routing::get(|| async { axum::Json(json!({"data":[{"id":"BAAI/bge-m3"}]})) }),
        )
        .route(
            "/embeddings",
            axum::routing::post(
                |axum::extract::State((calls, fail, private)): axum::extract::State<(
                    Arc<AtomicUsize>,
                    Arc<AtomicBool>,
                    String,
                )>,
                 axum::Json(request): axum::Json<serde_json::Value>| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if fail.load(Ordering::SeqCst) {
                        return (axum::http::StatusCode::BAD_GATEWAY, private).into_response();
                    }
                    let count = request["input"].as_array().unwrap().len();
                    let data = (0..count)
                        .map(|index| json!({"index":index,"embedding":vec![0.125;512]}))
                        .collect::<Vec<_>>();
                    axum::Json(json!({"model":"BAAI/bge-m3","data":data})).into_response()
                },
            ),
        )
        .with_state((calls.clone(), fail.clone(), provider_body));
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_endpoint = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    let endpoint = std::env::var("FYRR_TEST_S3_ENDPOINT").unwrap();
    let bucket = std::env::var("FYRR_TEST_S3_BUCKET").unwrap();
    let config = sandbox.input("fyrr.toml", &format!("[namespace]\nname='privacy-writer'\n[storage]\nuri='s3://{bucket}/privacy-writer-{}-{}'\nendpoint='{endpoint}'\nallow_http=true\naccess_key_id={{env='FYRR_TEST_S3_ACCESS_KEY_ID'}}\nsecret_access_key={{env='FYRR_TEST_S3_SECRET_ACCESS_KEY'}}\n[embedding]\nendpoint='{provider_endpoint}'\ndimensions=512\nsend_dimensions=true\nbatch_size=2\nmax_retries=0\ntimeout='1s'\nlocal_cache='{}'\napi_key={{env='WRITER_PRIVACY_KEY'}}\nspend={{limit_microusd=100,max_call_microusd=10}}\n[identity]\nhashing_key={{env='FYRR_TEST_S3_SECRET_ACCESS_KEY'}}\nencryption_key={{env='FYRR_TEST_ENCRYPTION_KEY'}}\n[[sources]]\nid='local'\ntype='local-files'\npermissions='fixed'\nreaders=['{reader}']\n[sources.scope]\npaths=['{}']\ninclude=['source.txt']\n", std::process::id(), fyrr_core::clock::now().timestamp_micros(), sandbox.output().join("writer-cache").display(), file.parent().unwrap().display()));
    let env = [("WRITER_PRIVACY_KEY", secret.as_str())];
    let created = sandbox.configured(&config, ["writer", "namespace", "create"], &env);
    assert_clean(&sentinels, &created, &sandbox, "writer embedding namespace");
    assert!(created.status.success());
    let synced = sandbox.configured(
        &config,
        ["writer", "sync", "local", "--kind", "content"],
        &env,
    );
    assert_clean(
        &sentinels,
        &synced,
        &sandbox,
        "writer live embedding and local cache",
    );
    assert!(synced.status.success());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        sandbox
            .written_files()
            .iter()
            .any(|file| file.starts_with(sandbox.output().join("writer-cache")))
    );
    fail.store(true, Ordering::SeqCst);
    std::fs::write(&file, sentinels.mark("writer-failed-document")).unwrap();
    let failed = sandbox.configured(
        &config,
        ["writer", "sync", "local", "--kind", "content", "--full"],
        &env,
    );
    assert_clean(
        &sentinels,
        &failed,
        &sandbox,
        "writer failed embedding and retained local cache",
    );
    assert!(!failed.status.success());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(String::from_utf8_lossy(&failed.stdout).contains("embedding_provider_unavailable"));
    task.abort();
}

#[test]
fn object_store_diagnostics_hash_keys_and_keep_cli_inputs_private() {
    use std::io::{Read, Write};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;
    let mut sentinels = Sentinels::new();
    let sandbox = Sandbox::new("privacy-object-diagnostics");
    let key = sentinels.mark("object-key");
    let query = sentinels.mark("object-query");
    let secret = sentinels.mark("object-secret");
    let storage = sandbox.input("storage-path", "").with_file_name(&key);
    std::fs::create_dir(&storage).unwrap();
    std::fs::set_permissions(&storage, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = sandbox.input("fyrr.toml", &format!(
        "[namespace]\nname='private-diagnostics'\n[storage]\nuri='file://{}'\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\napi_key={{env='DIAGNOSTIC_SECRET'}}\n[identity]\nhashing_key={{env='DIAGNOSTIC_SECRET'}}\n", storage.display()
    ));
    let log = sandbox.output().join("object-store.jsonl");
    let args = ["reader", "search", &query];
    let disabled = sandbox.configured(&config, args, &[("DIAGNOSTIC_SECRET", &secret)]);
    assert_clean(&sentinels, &disabled, &sandbox, "diagnostics disabled");
    assert!(!log.exists());
    let enabled = sandbox.configured(
        &config,
        args,
        &[
            ("DIAGNOSTIC_SECRET", &secret),
            ("FYRR_OBJECT_STORE_DIAGNOSTICS", log.to_str().unwrap()),
        ],
    );
    assert_clean(&sentinels, &enabled, &sandbox, "diagnostics enabled");
    let text =
        std::fs::read_to_string(log).expect("the real ObjectStore boundary emits diagnostics");
    let records: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(!records.is_empty());
    assert!(
        records
            .iter()
            .any(|record| record["method"] == "GET" && record["error_code"] == "not_found")
    );

    // Exercise the configured S3 factory and its actual HTTP transport as well.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = stop.clone();
    let body = sentinels.mark("remote-error-body");
    let server = std::thread::spawn(move || {
        while !stopped.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0; 4096];
                    while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        let count = stream.read(&mut buffer).unwrap();
                        if count == 0 || request.len() + count > 65536 {
                            return false;
                        }
                        request.extend_from_slice(&buffer[..count]);
                    }
                    write!(stream, "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
                    return true;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return false,
            }
        }
        false
    });
    let remote_config = sandbox.input("remote.toml", &format!(
        "[namespace]\nname='private-diagnostics'\n[storage]\nuri='s3://private-diagnostics/{key}'\nendpoint='{endpoint}'\nallow_http=true\naccess_key_id={{env='DIAGNOSTIC_SECRET'}}\nsecret_access_key={{env='DIAGNOSTIC_SECRET'}}\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n[identity]\nhashing_key={{env='DIAGNOSTIC_SECRET'}}\n"
    ));
    let remote_log = sandbox.output().join("remote-object-store.jsonl");
    let remote = sandbox.configured(
        &remote_config,
        args,
        &[
            ("DIAGNOSTIC_SECRET", &secret),
            (
                "FYRR_OBJECT_STORE_DIAGNOSTICS",
                remote_log.to_str().unwrap(),
            ),
        ],
    );
    stop.store(true, Ordering::SeqCst);
    assert!(
        server.join().unwrap(),
        "the real CLI reached the loopback object store"
    );
    assert_clean(&sentinels, &remote, &sandbox, "S3 diagnostics enabled");
    let text = std::fs::read_to_string(remote_log).unwrap();
    let records: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(records.iter().any(|record| record["method"] == "GET"
        && record["error_code"] == "not_found"
        && record["http_status"] == 404
        && record["retry_count"] == 0));
}
