//! Configuration: typed TOML with unknown fields
//! rejected, cross-field validation, secrets only as environment references
//! (never echoed), the generated JSON Schema, and value origins.

mod common;

use std::collections::HashMap;

use fyrr_core::config::{Environment, LoadedConfig, Severity, parse};

const EXAMPLE: &str = "config/fyrr.example.toml";
const SENTINEL: &str = "SENTINEL-5ecr3t-9f1c";

const RERANK: &str = "\n[namespace.rerank]\nendpoint='http://127.0.0.1:1234/rerank'\nmodel='fixture-rerank'\ncandidates=50\nweight=0.5\ndeadline='1s'\nspend_limit_microusd=100\nmax_call_cost_microusd=10\n";

#[test]
fn rerank_is_optional_capped_validated_and_bound_into_policy() {
    let base = parse(MINIMAL).unwrap();
    assert!(base.config.namespace.rerank.is_none());
    let legacy = base.config.namespace.retrieval_digest();
    let configured = parse(&format!("{MINIMAL}{RERANK}")).unwrap();
    assert!(errors(&configured).is_empty());
    let digest = configured.config.namespace.retrieval_digest();
    assert_ne!(legacy, digest);
    for (from, to, field) in [
        ("127.0.0.1", "localhost", "endpoint"),
        ("fixture-rerank", "", "model"),
        ("candidates=50", "candidates=76", "candidates"),
        ("weight=0.5", "weight=nan", "weight"),
        ("weight=0.5", "weight=-0.1", "weight"),
        ("deadline='1s'", "deadline='0ms'", "deadline"),
        ("deadline='1s'", "deadline='301s'", "deadline"),
        (
            "max_call_cost_microusd=10",
            "max_call_cost_microusd=0",
            "max_call_cost_microusd",
        ),
        (
            "max_call_cost_microusd=10\n",
            "max_call_cost_microusd=10\ntext_chars=199\n",
            "text_chars",
        ),
        (
            "max_call_cost_microusd=10\n",
            "max_call_cost_microusd=10\ntext_chars=32769\n",
            "text_chars",
        ),
    ] {
        assert_error(
            &RERANK.replace(from, to),
            &format!("namespace.rerank.{field}"),
        );
    }
    assert!(
        parse(&format!(
            "{MINIMAL}{}",
            RERANK.replace("spend_limit_microusd=100\n", "")
        ))
        .is_err()
    );
    let digest_of = |from: &str, to: &str| {
        parse(&format!("{MINIMAL}{}", RERANK.replace(from, to)))
            .unwrap()
            .config
            .namespace
            .retrieval_digest()
    };
    // What decides results is bound into the policy.
    for (from, to) in [
        ("fixture-rerank", "another-model"),
        ("candidates=50", "candidates=30"),
        ("weight=0.5", "weight=1"),
        (
            "max_call_cost_microusd=10\n",
            "max_call_cost_microusd=10\ntext_chars=2000\n",
        ),
    ] {
        assert_ne!(digest, digest_of(from, to), "{from}");
    }
    // The endpoint, deadline, spend and credential are process configuration.
    for (from, to) in [
        ("1234", "1235"),
        ("deadline='1s'", "deadline='2s'"),
        ("spend_limit_microusd=100", "spend_limit_microusd=99"),
        ("max_call_cost_microusd=10", "max_call_cost_microusd=11"),
    ] {
        assert_eq!(digest, digest_of(from, to), "{from}");
    }
    let with_secret = format!("{MINIMAL}{RERANK}api_key={{env='RERANK_TEST_KEY'}}\n");
    let secret = parse(&with_secret).unwrap();
    assert_eq!(digest, secret.config.namespace.retrieval_digest());
    assert!(
        secret
            .missing_secrets(&|_: &str| None)
            .iter()
            .any(|issue| issue.path == "namespace.rerank.api_key")
    );
    let issue = parse(&with_secret.replace("{env='RERANK_TEST_KEY'}", &format!("'{SENTINEL}'")))
        .unwrap_err();
    assert!(!issue.to_string().contains(SENTINEL));
}

/// The smallest valid file; every other section takes its defaults.
const MINIMAL: &str = r#"
[namespace]
name = "acme"
[storage]
uri = "s3://fyrr-data/acme"
[identity]
hashing_key = { env = "FYRR_IDENTITY_KEY" }
[embedding]
endpoint = "https://embeddings.example/v1"
"#;

/// MINIMAL with `keys` added to its `[identity]` table.
fn with_identity(keys: &str) -> String {
    MINIMAL.replace("[identity]\n", &format!("[identity]\n{keys}\n"))
}

/// MINIMAL with an encryption key, as every `permissions = "sync"` source needs.
fn encrypted() -> String {
    with_identity("encryption_key = { env = \"FYRR_ENCRYPTION_KEY\" }")
}

fn env(pairs: &[(&str, &str)]) -> impl Environment {
    let map: HashMap<String, String> = pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();
    move |name: &str| map.get(name).cloned()
}

#[test]
fn live_embeddings_require_explicit_bounded_spending_and_private_cache() {
    let defaults = parse(MINIMAL).unwrap();
    assert!(!defaults.config.embedding.reader_live);
    assert!(defaults.config.embedding.spend.is_none());
    let loaded = parse(&format!("{MINIMAL}\nreader_live=true\n")).unwrap();
    assert!(
        loaded
            .validate()
            .iter()
            .any(|issue| issue.path == "embedding.spend"
                && issue.message == "embedding_spend_limit_required")
    );
    let valid = format!(
        "{MINIMAL}\nmodel='test/embed'\ndimensions=512\nreader_live=true\nsend_dimensions=true\nspend={{limit_microusd=100,max_call_microusd=2}}\n[embedding.roles.query]\nprefix='query: '\ninput_type='query'\n"
    );
    assert!(errors(&parse(&valid).unwrap()).is_empty());
    for invalid in [
        valid.replace("max_call_microusd=2", "max_call_microusd=0"),
        valid.replace("max_call_microusd=2", "max_call_microusd=101"),
        valid.replace("input_type='query'", "input_type=''"),
    ] {
        assert!(!errors(&parse(&invalid).unwrap()).is_empty());
    }
}

fn errors(loaded: &LoadedConfig) -> Vec<String> {
    loaded
        .validate()
        .into_iter()
        .filter(|issue| issue.severity == Severity::Error)
        .map(|issue| issue.to_string())
        .collect()
}

/// Appends TOML to the minimal file and returns the validation errors.
fn errors_with(extra: &str) -> Vec<String> {
    let loaded =
        parse(&format!("{MINIMAL}\n{extra}")).unwrap_or_else(|issue| panic!("parse: {issue}"));
    errors(&loaded)
}

fn assert_error(extra: &str, path: &str) {
    let errors = errors_with(extra);
    assert!(
        errors.iter().any(|error| error.contains(path)),
        "expected an error at {path} for:\n{extra}\ngot {errors:#?}"
    );
}

#[test]
fn service_http_urls_require_loopback_ip_literals() {
    let base = format!(
        "{MINIMAL}\n[identity.oidc]\nissuer='https://issuer.example'\naudience='fyrr'\njwks_url='https://keys.example/jwks'\n[identity.directory]\nendpoint='https://directory.example'\n[reader.http]\nlisten='127.0.0.1:8080'\npublic_url='https://reader.example/mcp'\noperator_credential={{env='FYRR_OPERATOR'}}\n"
    );
    assert!(errors(&parse(&base).unwrap()).is_empty());
    for (original, suffix, field) in [
        ("https://issuer.example", "", "identity.oidc"),
        ("https://keys.example/jwks", "/jwks", "identity.oidc"),
        (
            "https://directory.example",
            "",
            "identity.directory.endpoint",
        ),
        (
            "https://reader.example/mcp",
            "/mcp",
            "reader.http.public_url",
        ),
    ] {
        for (host, accepted) in [("localhost", false), ("127.0.0.1", true), ("[::1]", true)] {
            let text = base.replace(original, &format!("http://{host}:8080{suffix}"));
            let issues = errors(&parse(&text).unwrap());
            if accepted {
                assert!(issues.is_empty(), "{field} at {host}: {issues:?}");
            } else {
                assert!(
                    issues.iter().any(|e| e.contains(field)),
                    "{field} at {host}: {issues:?}"
                );
            }
        }
    }
}

#[test]
fn example_and_minimal_files_are_valid() {
    let example = parse(&common::read(EXAMPLE)).expect("example parses");
    assert_eq!(errors(&example), Vec::<String>::new());
    let warnings = example.validate();
    assert!(
        warnings
            .iter()
            .any(|issue| issue.path == "sources[0].permissions"),
        "fixed connections are marked: {warnings:?}"
    );
    let minimal = parse(MINIMAL).expect("minimal parses");
    assert_eq!(errors(&minimal), Vec::<String>::new());
    assert_eq!(minimal.config.writer.purge_window.to_string(), "1d");
    assert_eq!(minimal.config.reader.concurrency, 8);
    assert_eq!(minimal.config.writer.threads, None);
    let bounded = parse(&format!("{MINIMAL}\n[writer]\nthreads = 2\n")).expect("threads parse");
    assert_eq!(errors(&bounded), Vec::<String>::new());
    assert_eq!(bounded.config.writer.threads, Some(2));
    assert_eq!(minimal.config.writer.fts_partition_memory_mb, None);
    assert_eq!(minimal.config.reader.fts_search_chunk, None);
    let partitioned = parse(&format!(
        "{MINIMAL}\n[writer]\nfts_partition_memory_mb = 64\n[reader]\nfts_search_chunk = 1\n"
    ))
    .expect("full-text partition settings parse");
    assert_eq!(errors(&partitioned), Vec::<String>::new());
    assert_eq!(partitioned.config.writer.fts_partition_memory_mb, Some(64));
    assert_eq!(partitioned.config.reader.fts_search_chunk, Some(1));
    assert_eq!(minimal.config.reader.instructions, None);
    assert_eq!(
        minimal.config.reader.served_instructions(),
        Some(fyrr_core::mcp::SERVER_INSTRUCTIONS)
    );
    for (setting, served) in [("\"Search first.\"", Some("Search first.")), ("\"\"", None)] {
        let parsed = parse(&format!("{MINIMAL}\n[reader]\ninstructions = {setting}\n"))
            .expect("instructions parse");
        assert_eq!(errors(&parsed), Vec::<String>::new());
        assert_eq!(parsed.config.reader.served_instructions(), served);
    }
    let longest = format!(
        "{MINIMAL}\n[reader]\ninstructions = \"{}\"\n",
        "é".repeat(4096)
    );
    assert_eq!(
        errors(&parse(&longest).expect("parses")),
        Vec::<String>::new()
    );
    assert_eq!(
        minimal.config.reader.version_check_interval.to_string(),
        "30s"
    );
}

#[test]
fn example_matches_the_generated_json_schema() {
    for text in [common::read(EXAMPLE), MINIMAL.to_owned()] {
        let value: serde_json::Value = toml::from_str(&text).expect("TOML");
        common::assert_valid("schemas/config.schema.json", &value, "config");
    }
}

#[test]
fn unknown_fields_are_rejected() {
    for removed in [
        "profile = 'openai-compatible'",
        "roles.query.unknown = 'value'",
    ] {
        let text = MINIMAL.replace("[embedding]", &format!("[embedding]\n{removed}"));
        let issue = parse(&text).expect_err("unknown embedding setting");
        assert!(issue.message.contains("unknown field"), "{issue}");
    }

    for (extra, line) in [
        ("[bogus]\nx = 1", 11),
        ("[reader]\nversion_check_intervall = \"30s\"", 12),
        ("[writer.commit]\ntimer = \"60s\"", 12),
        (
            "[[sources]]\nid = \"a\"\ntype = \"jira\"\ncolor = \"blue\"",
            14,
        ),
        (
            "[[sources]]\nid = \"a\"\ntype = \"jira\"\n[sources.intervals]\nhourly = \"1h\"",
            15,
        ),
    ] {
        let issue = parse(&format!("{MINIMAL}\n{extra}")).expect_err(extra);
        assert!(issue.message.contains("unknown field"), "{extra}: {issue}");
        assert!(
            issue.path.starts_with(&format!("{line}:")),
            "{extra}: position {}",
            issue.path
        );
    }
    let issue = parse("[namespace]\nname = \"acme\"\nbucket = \"x\"").expect_err("nested unknown");
    assert!(issue.message.contains("unknown field `bucket`"), "{issue}");
}

#[test]
fn secrets_in_the_file_are_refused_without_echo() {
    for literal in [
        format!("\"{SENTINEL}\""),
        "12345".to_owned(),
        format!("[\"{SENTINEL}\"]"),
        format!("{{ env = \"{SENTINEL}\" }}"),
        format!("{{ env = \"FYRR_OK\", value = \"{SENTINEL}\" }}"),
        format!("{{ value = \"{SENTINEL}\" }}"),
    ] {
        let text = MINIMAL.replace(
            "uri = \"s3://fyrr-data/acme\"",
            &format!("uri = \"s3://fyrr-data/acme\"\nsecret_access_key = {literal}\naccess_key_id = {{ env = \"K\" }}"),
        );
        let issue = parse(&text).expect_err(&literal);
        let shown = issue.to_string();
        assert!(!shown.contains(SENTINEL), "secret echoed: {shown}");
        assert!(!shown.contains("12345"), "secret echoed: {shown}");
    }

    // Opaque provider tables: secret-like keys must be references too.
    for settings in [
        format!("api_token = \"{SENTINEL}\""),
        format!("[sources.settings.auth]\npassword = \"{SENTINEL}\""),
        format!("client_secrets = [\"{SENTINEL}\"]"),
    ] {
        let extra = format!(
            "[[sources]]\nid = \"j\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"\n[sources.settings]\n{settings}"
        );
        let errors = errors_with(&extra);
        assert!(!errors.is_empty(), "inline secret accepted: {settings}");
        assert!(
            errors.iter().all(|error| !error.contains(SENTINEL)),
            "{errors:?}"
        );
    }
    let referenced = "[[sources]]\nid = \"j\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"\n[sources.settings]\napi_token = { env = \"FYRR_JIRA_TOKEN\" }";
    assert_eq!(
        errors(&parse(&format!("{}\n{referenced}", encrypted())).unwrap()),
        Vec::<String>::new()
    );
}

/// A stored credential reaches the provider host's environment, which refuses
/// some names; `config check` must say so instead of the host failing to start.
#[test]
fn credential_names_the_provider_host_refuses_are_configuration_errors() {
    let long = "L".repeat(257);
    for name in [
        "PATH",
        "VIRTUAL_ENV",
        "PYTHONPATH",
        "PYTHON_X",
        "LD_PRELOAD",
        &long,
    ] {
        let extra = format!(
            "[writer.credentials]\n{name} = \"stored\"\n[[sources]]\nid = \"j\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"\n[sources.settings]\ntoken = {{ env = \"{name}\" }}\n[sources.scope]\nlists = [{{ env = \"{name}\" }}]"
        );
        let errors = errors_with(&extra);
        for path in ["sources[0].settings.token", "sources[0].scope.lists[0]"] {
            assert!(
                errors
                    .iter()
                    .any(|error| error.contains(&format!("{path}:"))),
                "{name}: expected an error at {path}, got {errors:#?}"
            );
        }
        assert!(
            errors.iter().all(|error| !error.contains("stored")),
            "{errors:?}"
        );
    }
    // Not delivered, so not refused: a reference the writer does not store
    // is read from the process environment, and similar names are ordinary.
    let ordinary = "[writer.credentials]\nPYTHON = \"s\"\nLDAP_TOKEN = \"s\"\nPATHS = \"s\"\n[[sources]]\nid = \"j\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"\n[sources.settings]\na = { env = \"LDAP_TOKEN\" }\nb = { env = \"PATHS\" }\nc = { env = \"PATH\" }";
    // `PYTHON` itself starts with PYTHON and is refused once it is referenced.
    let checked = |extra: &str| errors(&parse(&format!("{}\n{extra}", encrypted())).unwrap());
    assert_eq!(checked(ordinary), Vec::<String>::new());
    let refused = checked(&ordinary.replace("c = { env = \"PATH\" }", "c = { env = \"PYTHON\" }"));
    assert!(
        refused
            .iter()
            .any(|error| error.contains("sources[0].settings.c:")),
        "{refused:#?}"
    );
}

#[test]
fn cross_field_rules_are_enforced() {
    assert_error("[writer]\nthreads = 0", "writer.threads");
    assert_error(
        "[writer]\nfts_partition_memory_mb = 0",
        "writer.fts_partition_memory_mb",
    );
    assert_error(
        "[writer]\nfts_partition_memory_mb = 1048577",
        "writer.fts_partition_memory_mb",
    );
    assert_error(
        &format!("[reader]\ninstructions = \"{}\"", "x".repeat(4097)),
        "reader.instructions",
    );
    assert_error("[reader]\nfts_search_chunk = 0", "reader.fts_search_chunk");
    assert_error(
        "[reader]\nfts_search_chunk = 1025",
        "reader.fts_search_chunk",
    );
    assert_error(
        "[writer]\npurge_window = \"1h\"\nversion_grace = \"1h\"",
        "writer.purge_window",
    );
    assert_error("[writer]\nversion_grace = \"5m\"", "writer.version_grace");
    assert_error(
        "[writer]\nmax_envelope_age = \"7d\"",
        "writer.max_envelope_age",
    );
    assert_error(
        "[writer]\ndocument_max_bytes = \"256MiB\"",
        "writer.envelope_max_bytes",
    );
    assert_error(
        "[writer.commit]\ngroup_max_bytes = \"1MiB\"",
        "writer.commit",
    );
    assert_error(
        "[writer.commit]\nlease_ttl = \"0s\"",
        "writer.commit.lease_ttl",
    );
    assert_error(
        "[[sources]]\nid = \"a\"\ntype = \"jira\"\ndocument_max_bytes = \"1GiB\"\n[sources.intervals]\npermissions = \"1h\"",
        "sources[0].document_max_bytes",
    );
    assert_error(
        "[[sources]]\nid = \"a\"\ntype = \"local-files\"\npermissions = \"fixed\"",
        "sources[0].readers",
    );
    assert_error(
        "[[sources]]\nid = \"a\"\ntype = \"local-files\"\npermissions = \"fixed\"\nreaders = [\"engineering\"]",
        "sources[0].readers",
    );
    assert_error(
        "[[sources]]\nid = \"a\"\ntype = \"notion\"\npermissions = \"fixed\"\nreaders = [\"grp:x\"]\n[sources.intervals]\nmembers = \"1h\"",
        "sources[0].intervals",
    );
    assert_error(
        "[[sources]]\nid = \"a\"\ntype = \"local-files\"\n[sources.intervals]\npermissions = \"1h\"",
        "sources[0].permissions",
    );
    assert_error(
        "[[sources]]\nid = \"a\"\ntype = \"jira\"",
        "sources[0].intervals.permissions",
    );
    assert_error(
        "[[sources]]\nid = \"a\"\ntype = \"jira\"\nreaders = [\"grp:x\"]\n[sources.intervals]\npermissions = \"1h\"",
        "sources[0].readers",
    );
    assert_error(
        "[[sources]]\nid = \"a\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"\n[[sources]]\nid = \"a\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"",
        "sources[1].id",
    );
    assert_error(
        "[[sources]]\nid = \"a\"\ntype = \"jira\"\nrow_warning = 10\nrow_error = 10\n[sources.intervals]\npermissions = \"30m\"",
        "sources[0].row_warning",
    );
    let synced = with_identity("encryption_key = { env = \"FYRR_ENCRYPTION_KEY\" }");
    assert_eq!(
        errors(
            &parse(&format!(
                "{synced}\n[[sources]]\nid = \"a\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"1h\""
            ))
            .unwrap()
        ),
        Vec::<String>::new()
    );
}

#[test]
fn storage_rules_and_options() {
    let text = MINIMAL.replace(
        "uri = \"s3://fyrr-data/acme\"",
        "uri = \"s3://fyrr-data/acme\"\nendpoint = \"http://127.0.0.1:9000\"",
    );
    let loaded = parse(&text).expect("parse");
    assert!(
        errors(&loaded)
            .iter()
            .any(|error| error.contains("storage.endpoint"))
    );

    let text = MINIMAL.replace(
        "uri = \"s3://fyrr-data/acme\"",
        "uri = \"s3://fyrr-data/acme\"\nendpoint = \"http://127.0.0.1:9000\"\nallow_http = true\naccess_key_id = { env = \"FYRR_TEST_KEY\" }",
    );
    let loaded = parse(&text).expect("parse");
    assert!(
        errors(&loaded)
            .iter()
            .any(|error| error.contains("storage.access_key_id"))
    );

    let text = MINIMAL.replace(
        "uri = \"s3://fyrr-data/acme\"",
        "uri = \"s3://fyrr-data/acme\"\nendpoint = \"http://127.0.0.1:9000\"\nallow_http = true\naccess_key_id = { env = \"FYRR_TEST_KEY\" }\nsecret_access_key = { env = \"FYRR_TEST_SECRET\" }",
    );
    let loaded = parse(&text).expect("parse");
    assert_eq!(errors(&loaded), Vec::<String>::new());
    let missing = loaded
        .config
        .storage
        .object_store_options(&env(&[]))
        .expect_err("unset");
    assert!(missing.to_string().contains("FYRR_TEST_KEY"), "{missing}");
    let options = loaded
        .config
        .storage
        .object_store_options(&env(&[
            ("FYRR_TEST_KEY", "key-id"),
            ("FYRR_TEST_SECRET", SENTINEL),
        ]))
        .expect("resolved");
    assert!(
        !format!("{options:?}").contains(SENTINEL),
        "Debug redacts values"
    );
    let map = options.into_map();
    assert_eq!(map["aws_secret_access_key"], SENTINEL);
    assert_eq!(map["aws_endpoint"], "http://127.0.0.1:9000");
    assert_eq!(map["allow_http"], "true");
}

#[test]
fn local_storage_takes_a_canonical_file_uri_and_no_s3_keys() {
    use fyrr_core::config::StorageBackend;
    let local = |uri: &str| MINIMAL.replace("s3://fyrr-data/acme", uri);
    for (uri, root) in [
        ("file:///var/lib/fyrr", "/var/lib/fyrr"),
        ("file:///var/lib/fyrr/", "/var/lib/fyrr"),
        ("file:///srv/my%20data", "/srv/my data"),
    ] {
        let loaded = parse(&local(uri)).expect("parse");
        assert_eq!(errors(&loaded), Vec::<String>::new(), "{uri}");
        assert_eq!(
            loaded.config.storage.backend(),
            Some(StorageBackend::Local { root: root.into() })
        );
        let options = loaded.config.storage.object_store_options(&env(&[]));
        assert!(options.expect("no secrets").into_map().is_empty());
    }
    for uri in [
        "file:///",
        "file:/var/lib/fyrr",
        "file://host/var/lib/fyrr",
        "file://localhost/var/lib/fyrr",
        "file:///var/../lib",
        "file:///var/./lib",
        "file:///var//lib",
        "file:///srv/my data",
        "file:///srv/a%2Fb",
        "file:///srv/%GG",
        "file:///srv/%00",
        "file:///srv/%2E%2E/etc",
        "file:///srv/data?x=1",
        "file:///srv/data#x",
        "/var/lib/fyrr",
        "relative/path",
    ] {
        let loaded = parse(&local(uri)).expect("parse");
        let errors = errors(&loaded);
        assert!(
            errors
                .iter()
                .any(|error| error.starts_with("error: storage.uri")),
            "{uri}: {errors:?}"
        );
        assert!(loaded.config.storage.backend().is_none(), "{uri}");
    }
    assert_eq!(
        parse(MINIMAL).unwrap().config.storage.backend(),
        Some(StorageBackend::S3 {
            bucket: "fyrr-data",
            prefix: "acme"
        })
    );
    // Every S3 access key is refused for a file URI, named but not quoted,
    // also when it repeats a default.
    for (key, value) in [
        ("region", "\"eu-west-9\""),
        ("endpoint", "\"https://s3.example\""),
        ("allow_http", "false"),
        ("virtual_hosted_style", "true"),
        ("access_key_id", "{ env = \"FYRR_TEST_KEY\" }"),
        ("secret_access_key", "{ env = \"FYRR_TEST_SECRET\" }"),
        ("session_token", "{ env = \"FYRR_TEST_TOKEN\" }"),
    ] {
        let text = local("file:///var/lib/fyrr").replace(
            "uri = \"file:///var/lib/fyrr\"",
            &format!("uri = \"file:///var/lib/fyrr\"\n{key} = {value}"),
        );
        let errors = errors(&parse(&text).expect("parse"));
        assert!(
            errors
                .iter()
                .any(|error| error.starts_with(&format!("error: storage.{key}:"))),
            "{key}: {errors:?}"
        );
        assert!(errors.iter().all(|error| !error.contains("eu-west-9")));
    }
}

#[test]
fn missing_secrets_are_reported_by_variable_name() {
    let example = parse(&common::read(EXAMPLE)).expect("parse");
    let missing: Vec<String> = example
        .missing_secrets(&env(&[]))
        .iter()
        .map(ToString::to_string)
        .collect();
    for name in [
        "FYRR_S3_ACCESS_KEY_ID",
        "FYRR_S3_SECRET_ACCESS_KEY",
        "FYRR_EMBEDDING_API_KEY",
        "FYRR_JIRA_API_TOKEN",
    ] {
        assert!(
            missing.iter().any(|issue| issue.contains(name)),
            "{name} not reported: {missing:#?}"
        );
    }
    let all_set = env(&[
        ("FYRR_S3_ACCESS_KEY_ID", SENTINEL),
        ("FYRR_S3_SECRET_ACCESS_KEY", SENTINEL),
        ("FYRR_EMBEDDING_API_KEY", SENTINEL),
        ("FYRR_JIRA_API_TOKEN", SENTINEL),
        ("FYRR_IDENTITY_KEY", SENTINEL),
        ("FYRR_ENCRYPTION_KEY", SENTINEL),
    ]);
    assert!(example.missing_secrets(&all_set).is_empty());
}

const KEY_A: &str = "fyrr_key_aB3_Zk9Qm7aB3_Zk9Qm7aB3_Zk9Qm7aB3_Zk9Qm7aB3";
const KEY_B: &str = "fyrr_key_xY8-Lp2Nv5xY8-Lp2Nv5xY8-Lp2Nv5xY8-Lp2Nv5xY8";

fn api_key(name: &str, variable: &str, readers: &str) -> String {
    format!("\n[[identity.api_keys]]\nname='{name}'\nkey={{env='{variable}'}}\nreaders={readers}\n")
}

#[test]
fn api_keys_need_a_name_readers_and_a_well_formed_unique_secret() {
    let one = api_key("agents", "KEY_ONE", "['grp:engineering']");
    assert!(errors_with(&one).is_empty());
    assert_error(
        &format!("{one}{}", api_key("agents", "KEY_TWO", "['usr:7']")),
        "identity.api_keys[1].name",
    );
    assert_error(
        &api_key("Not Valid", "KEY_ONE", "['usr:7']"),
        "api_keys[0].name",
    );
    assert_error(&api_key("agents", "KEY_ONE", "[]"), "api_keys[0].readers");
    assert_error(
        &api_key("agents", "KEY_ONE", "['engineering']"),
        "api_keys[0].readers",
    );
    assert!(
        parse(&format!(
            "{MINIMAL}\n[[identity.api_keys]]\nname='agents'\nkey={{env='KEY_ONE'}}\nuser='a@b.c'\n"
        ))
        .is_err(),
        "a key states readers, nothing else"
    );

    for good in [KEY_A, KEY_B] {
        assert!(fyrr_core::config::is_api_key_secret(good), "{good}");
    }
    for bad in [
        "",
        "fyrr_key_",
        "aB3_Zk9Qm7aB3_Zk9Qm7aB3_Zk9Qm7aB3_Zk9Qm7aB3",
        &KEY_A[..KEY_A.len() - 1],
        &format!("{KEY_A}!"),
        &format!("{KEY_A} "),
        &format!("{KEY_A}\n"),
        "fyrr_token_aB3_Zk9Qm7aB3_Zk9Qm7aB3_Zk9Qm7aB3_Zk9Qm7aB3",
    ] {
        assert!(!fyrr_core::config::is_api_key_secret(bad), "{bad:?}");
    }

    let two = format!("{one}{}", api_key("tests", "KEY_TWO", "['usr:7']"));
    let loaded = parse(&format!("{MINIMAL}\n{two}")).unwrap();
    let issues = |pairs: &[(&str, &str)]| -> Vec<String> {
        loaded
            .api_key_secret_issues(&env(pairs))
            .iter()
            .map(ToString::to_string)
            .collect()
    };
    assert!(issues(&[("KEY_ONE", KEY_A), ("KEY_TWO", KEY_B)]).is_empty());
    // An unset variable is `missing_secrets`' to report, a malformed secret this check's.
    assert!(issues(&[("KEY_ONE", KEY_A)]).is_empty());
    let malformed = issues(&[("KEY_ONE", KEY_A), ("KEY_TWO", SENTINEL)]);
    assert_eq!(malformed.len(), 1);
    assert!(malformed[0].contains("identity.api_keys[1].key"));
    let duplicate = issues(&[("KEY_ONE", KEY_A), ("KEY_TWO", KEY_A)]);
    assert_eq!(duplicate.len(), 1);
    assert!(duplicate[0].contains("identity.api_keys[1].key"));
    for text in malformed.iter().chain(&duplicate) {
        assert!(!text.contains(SENTINEL) && !text.contains(KEY_A), "{text}");
    }
}

#[test]
fn http_without_a_login_method_stays_valid_for_the_writer_and_the_check_warns() {
    let http =
        "\n[reader.http]\nlisten='127.0.0.1:8080'\npublic_url='https://reader.example/mcp'\n";
    let loaded = parse(&format!("{MINIMAL}{http}")).unwrap();
    assert!(
        errors(&loaded).is_empty(),
        "the writer publishes without one"
    );
    assert!(loaded.config.http_authentication_missing());
    let keyed = parse(&format!(
        "{MINIMAL}{http}{}",
        api_key("agents", "KEY_ONE", "['grp:engineering']")
    ))
    .unwrap();
    assert!(errors(&keyed).is_empty());
    assert!(!keyed.config.http_authentication_missing());
    assert!(!parse(MINIMAL).unwrap().config.http_authentication_missing());
}

/// One step of a path into the configuration.
#[derive(Debug, Clone)]
enum Step {
    Key(String),
    Index(usize),
}

fn render(path: &[Step]) -> String {
    let mut text = String::new();
    for step in path {
        match step {
            Step::Key(key) if text.is_empty() => text.push_str(key),
            Step::Key(key) => {
                text.push('.');
                text.push_str(key);
            }
            Step::Index(index) => text.push_str(&format!("[{index}]")),
        }
    }
    text
}

/// Collects every (path, keyword, bound) constraint of the schema, following
/// `$ref`, `anyOf`/`oneOf`, object properties and array items.
fn collect_constraints(
    root: &serde_json::Value,
    node: &serde_json::Value,
    path: &mut Vec<Step>,
    found: &mut Vec<(Vec<Step>, &'static str, serde_json::Value)>,
) {
    if let Some(reference) = node.get("$ref").and_then(serde_json::Value::as_str) {
        let target = root
            .pointer(reference.trim_start_matches('#'))
            .unwrap_or_else(|| panic!("unresolved {reference}"));
        collect_constraints(root, target, path, found);
    }
    for keyword in [
        "pattern",
        "minimum",
        "maximum",
        "minLength",
        "maxLength",
        "enum",
    ] {
        if let Some(bound) = node.get(keyword) {
            found.push((path.clone(), keyword, bound.clone()));
        }
    }
    for combinator in ["anyOf", "oneOf", "allOf"] {
        for branch in node
            .get(combinator)
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            collect_constraints(root, branch, path, found);
        }
    }
    if let Some(properties) = node
        .get("properties")
        .and_then(serde_json::Value::as_object)
    {
        for (key, property) in properties {
            path.push(Step::Key(key.clone()));
            collect_constraints(root, property, path, found);
            path.pop();
        }
    }
    if let Some(items) = node.get("items") {
        path.push(Step::Index(0));
        collect_constraints(root, items, path, found);
        path.pop();
    }
}

/// An ASCII string `offset` bytes longer than a length bound.
fn text(bound: &serde_json::Value, offset: i64) -> toml::Value {
    let length = bound.as_i64().expect("integer bound") + offset;
    toml::Value::String("a".repeat(usize::try_from(length).expect("nonnegative length")))
}

/// Sets `value` at `path` in a TOML document, creating tables as needed.
fn set(table: &mut toml::Table, path: &[Step], value: toml::Value) {
    let (first, rest) = path.split_first().expect("non-empty path");
    let Step::Key(key) = first else {
        panic!("paths start with a key")
    };
    if rest.is_empty() {
        table.insert(key.clone(), value);
        return;
    }
    let slot = table
        .entry(key.clone())
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    let next = match (rest.first(), slot) {
        (Some(Step::Index(index)), toml::Value::Array(items)) => {
            let item = items
                .get_mut(*index)
                .expect("example has the indexed table");
            let toml::Value::Table(inner) = item else {
                panic!("array of tables")
            };
            return set(inner, &rest[1..], value);
        }
        (_, toml::Value::Table(inner)) => inner,
        (_, other) => panic!("cannot descend into {other}"),
    };
    set(next, rest, value)
}

/// Parse and validate errors as text, or nothing.
fn check(table: &toml::Table) -> Vec<String> {
    match parse(&toml::to_string(table).expect("TOML")) {
        Err(issue) => vec![issue.to_string()],
        Ok(loaded) => errors(&loaded),
    }
}

#[test]
fn every_schema_constraint_is_enforced() {
    let schema = common::read_json("schemas/config.schema.json");
    let mut constraints = Vec::new();
    collect_constraints(&schema, &schema, &mut Vec::new(), &mut constraints);
    let rendered: Vec<String> = constraints
        .iter()
        .map(|(path, keyword, _)| format!("{} {keyword}", render(path)))
        .collect();
    for expected in [
        "namespace.name pattern",
        "storage.uri pattern",
        "embedding.dimensions minimum",
        "embedding.dimensions maximum",
        "embedding.batch_size minimum",
        "sources[0].id pattern",
        "sources[0].type pattern",
        "sources[0].retention_days minimum",
        "storage.secret_access_key.env pattern",
        "writer.purge_window pattern",
        "identity.directory.provider enum",
        "embedding.model minLength",
        "embedding.model maxLength",
        "embedding.prefix maxLength",
        "embedding.roles.document.prefix maxLength",
        "embedding.roles.query.input_type minLength",
        "reader.http.public_url maxLength",
        "identity.api_keys[0].name pattern",
        "identity.api_keys[0].key.env pattern",
    ] {
        assert!(
            rendered.iter().any(|found| found == expected),
            "schema walk missed {expected}: {rendered:#?}"
        );
    }

    let mut base: toml::Table = toml::from_str(&common::read(EXAMPLE)).expect("example TOML");
    // The example declares no API key, whose constraints the walk must reach too.
    let key: toml::Table = toml::from_str(
        "name = \"agents\"\nkey = { env = \"FYRR_API_KEY_AGENTS\" }\nreaders = [\"grp:engineering\"]\n",
    )
    .expect("key TOML");
    base["identity"]
        .as_table_mut()
        .expect("identity table")
        .insert("api_keys".into(), toml::Value::Array(vec![key.into()]));
    const BAD: &str = "!bad value!";
    for (path, keyword, bound) in &constraints {
        let at = render(path);
        let mut outside = base.clone();
        let violation = match *keyword {
            "pattern" | "enum" => toml::Value::String(BAD.to_owned()),
            "minimum" => toml::Value::Integer(bound.as_i64().expect("integer bound") - 1),
            "maximum" => toml::Value::Integer(bound.as_i64().expect("integer bound") + 1),
            "minLength" => text(bound, -1),
            "maxLength" => text(bound, 1),
            other => unreachable!("{other}"),
        };
        set(&mut outside, path, violation.clone());
        let errors = check(&outside);
        assert!(
            !errors.is_empty(),
            "{at} {keyword} {bound}: {violation} passed config check"
        );
        for error in &errors {
            assert!(
                !error.contains(BAD),
                "{at}: the error echoes the value: {error}"
            );
        }

        let inside = match *keyword {
            "minimum" | "maximum" => {
                Some(toml::Value::Integer(bound.as_i64().expect("integer bound")))
            }
            "minLength" | "maxLength" => Some(text(bound, 0)),
            _ => None,
        };
        if let Some(inside) = inside {
            let mut boundary = base.clone();
            set(&mut boundary, path, inside);
            let errors = check(&boundary);
            assert!(
                errors
                    .iter()
                    .all(|error| !error.contains(&format!(" {at}:"))),
                "{at} = {bound} is inside the schema but failed: {errors:?}"
            );
        }
    }
    assert!(
        constraints.len() >= 40,
        "only {} constraints found",
        constraints.len()
    );
}

#[test]
fn type_errors_never_echo_values() {
    for (field, replacement) in [
        (
            "[embedding]\n",
            format!("[embedding]\ndimensions = \"{SENTINEL}\"\n"),
        ),
        ("provider = \"none\"", format!("provider = \"{SENTINEL}\"")),
        (
            "permissions = \"fixed\"",
            format!("permissions = \"{SENTINEL}\""),
        ),
        (
            "[writer]\n",
            format!("[writer]\nserve = {{ committer = \"{SENTINEL}\" }}\n"),
        ),
        (
            "[writer]\n",
            format!("[writer]\nenvelope_max_rows = [\"{SENTINEL}\"]\n"),
        ),
        // Values that contain a quote, a backtick or the `, expected `
        // separator must not escape the redaction.
        (
            "[embedding]\n",
            format!("[embedding]\ndimensions = \"a\\\"{SENTINEL} tail\"\n"),
        ),
        (
            "[embedding]\n",
            format!("[embedding]\ndimensions = \"a`{SENTINEL}` tail\"\n"),
        ),
        (
            "[embedding]\n",
            format!("[embedding]\ndimensions = \"a, expected {SENTINEL}\"\n"),
        ),
        (
            "provider = \"none\"",
            format!("provider = \"x`, expected \\\"{SENTINEL}\""),
        ),
        (
            "provider = \"none\"",
            format!("provider = \"it's {SENTINEL}\""),
        ),
    ] {
        let text = common::read(EXAMPLE).replacen(field, &replacement, 1);
        let issue = parse(&text).expect_err(&replacement);
        assert!(
            !issue.to_string().contains(SENTINEL),
            "value echoed: {issue}"
        );
        assert!(
            issue.message.contains("expected"),
            "keeps the expectation: {issue}"
        );
    }
}

#[test]
fn version_grace_covers_deadline_and_reader_preparation() {
    assert_error("[reader]\nmax_preparation = \"1h\"", "writer.version_grace");
    assert_error(
        "[reader]\nmax_preparation = \"0s\"",
        "reader.max_preparation",
    );
    assert_eq!(
        errors_with("[reader]\nmax_preparation = \"50m\""),
        Vec::<String>::new()
    );
}

#[test]
fn config_show_redacts_inline_secrets() {
    for settings in [
        format!("api_token = \"{SENTINEL}\""),
        format!("[sources.settings.oauth_token]\nvalue = \"{SENTINEL}\""),
        format!("client_secrets = [\"{SENTINEL}\"]"),
        // Arrays that mix tables and scalars, or nest tables in arrays, are
        // walked element by element.
        format!("list = [{{ api_token = \"{SENTINEL}\" }}, 1]"),
        format!("list = [[{{ api_token = \"{SENTINEL}\" }}], \"x\"]"),
        format!("list = [[[{{ nested = {{ password = \"{SENTINEL}\" }} }}]]]"),
    ] {
        let text = format!(
            "{MINIMAL}\n[[sources]]\nid = \"j\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"\n[sources.settings]\n{settings}"
        );
        let loaded = parse(&text).expect("parse");
        assert!(
            !errors(&loaded).is_empty(),
            "inline secret accepted: {settings}"
        );
        let values = loaded.effective_values(&env(&[]));
        assert!(
            values.iter().all(|value| !value.value.contains(SENTINEL)),
            "{values:#?}"
        );
        assert!(
            values
                .iter()
                .any(|value| value.value == fyrr_core::config::REDACTED),
            "{values:#?}"
        );
    }
    // Non-secret settings and secret references stay visible.
    let text = format!(
        "{MINIMAL}\n[[sources]]\nid = \"j\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"\n[sources.settings]\nbase_url = \"https://jira.example\"\napi_token = {{ env = \"FYRR_JIRA\" }}"
    );
    let values = parse(&text).expect("parse").effective_values(&env(&[]));
    let shown = |path: &str| {
        values
            .iter()
            .find(|value| value.path == path)
            .map(|value| value.value.clone())
    };
    assert_eq!(
        shown("sources[0].settings.base_url").as_deref(),
        Some("\"https://jira.example\"")
    );
    assert_eq!(
        shown("sources[0].settings.api_token").as_deref(),
        Some("env:FYRR_JIRA (unset)")
    );
}

#[test]
fn composition_is_bounded_defaulted_and_bound_into_policy_identity() {
    let base = parse(MINIMAL).unwrap();
    assert_eq!(base.config.namespace.composition.fusion, 50);
    let changed = parse(&MINIMAL.replace(
        "name = \"acme\"",
        "name = \"acme\"\ncomposition = { fusion = 50, dense = 25 }",
    ))
    .unwrap();
    assert!(errors(&changed).is_empty());
    assert_ne!(
        base.config.namespace.retrieval_digest(),
        changed.config.namespace.retrieval_digest()
    );
    for composition in [
        "{ fusion = 0 }",
        "{ fusion = 75, dense = 1 }",
        "{ fusion = 1, lexical = 75 }",
    ] {
        let input = MINIMAL.replace(
            "name = \"acme\"",
            &format!("name = \"acme\"\ncomposition = {composition}"),
        );
        assert!(
            errors(&parse(&input).unwrap())
                .iter()
                .any(|e| e.contains("namespace.composition"))
        );
    }
}

#[test]
fn retrieval_config_bounds_and_legacy_digest() {
    let base = parse(MINIMAL).unwrap();
    assert!(errors(&base).is_empty());
    let retrieval = &base.config.namespace.retrieval;
    assert_eq!(retrieval.dense_weight, 0.5);
    assert_eq!(retrieval.lexical_candidates, 200);
    assert_eq!(retrieval.dense_candidates, 75);
    assert_eq!(
        retrieval.rescore_depth,
        fyrr_core::config::RescoreDepth::Legacy
    );
    assert_eq!(
        retrieval.rescore_depth.initial(retrieval.dense_candidates),
        300
    );
    assert_eq!(
        retrieval.rescore_depth.maximum(retrieval.dense_candidates),
        1200
    );
    let legacy = serde_json::json!([
        "slice2:cosine:ivf-rq1:f16:550:training256:logistic-z:population:lex.75:dense.25:source2-soft-per-bucket:lex50:dense75:p1:1200:phrase0",
        base.config.namespace.fts_settings(),
        base.config.namespace.composition
    ]);
    let legacy_digest = fyrr_core::limits::digest(legacy.to_string().as_bytes());
    let historical = parse(&format!(
        "{MINIMAL}\n[namespace.retrieval]\ndense_weight=0.25\nlexical_candidates=50\n"
    ))
    .unwrap();
    assert!(errors(&historical).is_empty());
    assert_eq!(
        historical.config.namespace.retrieval_digest(),
        legacy_digest
    );
    assert_ne!(base.config.namespace.retrieval_digest(), legacy_digest);
    let explicit = parse(&format!("{MINIMAL}\n[namespace.retrieval]\ndense_weight=0.5\ndense_candidates=75\nlexical_candidates=200\nsource_cap=2\nmax_documents=75\n")).unwrap();
    assert!(errors(&explicit).is_empty());
    assert_eq!(
        base.config.namespace.retrieval,
        explicit.config.namespace.retrieval
    );
    assert_eq!(
        base.config.namespace.retrieval_digest(),
        explicit.config.namespace.retrieval_digest()
    );
    for (field, good) in [
        ("dense_weight", "0.25"),
        ("dense_candidates", "1"),
        ("lexical_candidates", "50"),
        ("source_cap", "0"),
        ("max_documents", "320"),
    ] {
        let extra = format!("[namespace.retrieval]\n{field}={good}");
        let config = parse(&format!("{MINIMAL}\n{extra}")).unwrap();
        assert!(errors(&config).is_empty());
        assert_ne!(
            config.config.namespace.retrieval_digest(),
            base.config.namespace.retrieval_digest()
        );
    }
    for bad in ["1.01", "-0.01", "nan"] {
        assert_error(
            &format!("[namespace.retrieval]\ndense_weight={bad}"),
            "namespace.retrieval.dense_weight",
        );
    }
    for (field, bad) in [
        ("lexical_candidates", "0"),
        ("lexical_candidates", "201"),
        ("dense_candidates", "0"),
        ("dense_candidates", "76"),
    ] {
        assert_error(
            &format!("[namespace.retrieval]\n{field}={bad}"),
            &format!("namespace.retrieval.{field}"),
        );
    }
}
#[test]
fn analyzer_flags_are_publication_settings_with_legacy_defaults() {
    let legacy = parse(MINIMAL).unwrap().config.namespace;
    let explicit = parse(&MINIMAL.replace(
        "[namespace]",
        "[namespace]\nfts_stem=false\nfts_remove_stop_words=false\nfts_ascii_folding=false",
    ))
    .unwrap()
    .config
    .namespace;
    assert_eq!(legacy.retrieval_digest(), explicit.retrieval_digest());
    assert!(!legacy.fts_settings().stem && !legacy.fts_settings().remove_stop_words);
    for flag in ["fts_stem", "fts_remove_stop_words", "fts_ascii_folding"] {
        let configured =
            parse(&MINIMAL.replace("[namespace]", &format!("[namespace]\n{flag}=true")))
                .unwrap()
                .config
                .namespace;
        assert_ne!(legacy.retrieval_digest(), configured.retrieval_digest());
        let fts = configured.fts_settings();
        assert_eq!(fts.stem, flag == "fts_stem");
        assert_eq!(fts.remove_stop_words, flag == "fts_remove_stop_words");
        assert_eq!(fts.ascii_folding, flag == "fts_ascii_folding");
    }
}
#[test]
fn dense_rq_bits_defaults_to_one_bit_and_is_bounded_without_changing_the_digest() {
    let legacy = parse(MINIMAL).unwrap().config.namespace;
    assert_eq!(legacy.dense_rq_bits, 1);
    for good in ["1", "5", "9"] {
        let configured =
            parse(&MINIMAL.replace("[namespace]", &format!("[namespace]\ndense_rq_bits={good}")))
                .unwrap();
        assert!(errors(&configured).is_empty());
        assert_eq!(configured.config.namespace.dense_rq_bits.to_string(), good);
        assert_eq!(
            configured.config.namespace.retrieval_digest(),
            legacy.retrieval_digest()
        );
    }
    for bad in ["0", "10"] {
        let rejected =
            parse(&MINIMAL.replace("[namespace]", &format!("[namespace]\ndense_rq_bits={bad}")))
                .unwrap();
        assert!(
            errors(&rejected)
                .iter()
                .any(|error| error.contains("namespace.dense_rq_bits")),
            "{bad}"
        );
    }
}
#[test]
fn dense_partition_rows_defaults_to_550_and_is_bounded_without_changing_the_digest() {
    let legacy = parse(MINIMAL).unwrap().config.namespace;
    assert_eq!(legacy.dense_partition_rows, 550);
    for good in ["32", "1000", "4096"] {
        let configured = parse(&MINIMAL.replace(
            "[namespace]",
            &format!("[namespace]\ndense_partition_rows={good}"),
        ))
        .unwrap();
        assert!(errors(&configured).is_empty());
        assert_eq!(
            configured.config.namespace.dense_partition_rows.to_string(),
            good
        );
        assert_eq!(
            configured.config.namespace.retrieval_digest(),
            legacy.retrieval_digest()
        );
    }
    for bad in ["31", "4097"] {
        let rejected = parse(&MINIMAL.replace(
            "[namespace]",
            &format!("[namespace]\ndense_partition_rows={bad}"),
        ))
        .unwrap();
        assert!(
            errors(&rejected)
                .iter()
                .any(|error| error.contains("namespace.dense_partition_rows")),
            "{bad}"
        );
    }
}
#[test]
fn namespace_attributes_are_declared_by_name_and_type() {
    use fyrr_core::attributes::AttributeType;
    let declared = parse(&format!(
        "{MINIMAL}\n[namespace.attributes]\ncreated='datetime'\npriority='int'\ndone='bool'"
    ))
    .unwrap();
    assert!(errors(&declared).is_empty());
    let attributes = &declared.config.namespace.attributes;
    assert_eq!(attributes["created"], AttributeType::Datetime);
    assert_eq!(attributes["priority"], AttributeType::Int);
    assert_eq!(attributes["done"], AttributeType::Bool);
    assert!(
        parse(MINIMAL)
            .unwrap()
            .config
            .namespace
            .attributes
            .is_empty()
    );
    for name in ["_hidden", "source_id", "Bad-Name"] {
        let rejected = parse(&format!(
            "{MINIMAL}\n[namespace.attributes]\n\"{name}\"='int'"
        ))
        .unwrap();
        assert!(
            errors(&rejected)
                .iter()
                .any(|error| error.contains("namespace.attributes")),
            "{name}"
        );
    }
    assert!(parse(&format!("{MINIMAL}\n[namespace.attributes]\na='uuid'")).is_err());
}
#[test]
fn rescore_depth_keeps_legacy_digest_and_expands_only_at_small_gaps() {
    use fyrr_core::config::RescoreDepth;
    let baseline = parse(MINIMAL).unwrap().config.namespace;
    let legacy = parse(&format!(
        "{MINIMAL}\n[namespace.retrieval]\nrescore_depth={{mode='legacy'}}"
    ))
    .unwrap()
    .config
    .namespace;
    assert_eq!(baseline.retrieval_digest(), legacy.retrieval_digest());
    assert_eq!(RescoreDepth::Legacy.initial(75), 300);
    let fixed = RescoreDepth::Fixed { candidates: 100 };
    assert_eq!(fixed.initial(75), 100);
    assert_eq!(fixed.initial(150), 151);
    assert_eq!(RescoreDepth::Fixed { candidates: 75 }.initial(75), 76);
    assert_eq!(RescoreDepth::Fixed { candidates: 1200 }.maximum(1), 1200);
    assert_eq!(RescoreDepth::Legacy.maximum(75), 1200);
    assert_eq!(fixed.expand(100, 75, 0.0), None);
    let adaptive = RescoreDepth::Adaptive {
        min_candidates: 75,
        max_candidates: 300,
        min_gap: 0.02,
    };
    assert_eq!(adaptive.expand(75, 75, 0.001), Some(150));
    assert_eq!(adaptive.expand(150, 75, 0.001), Some(300));
    assert_eq!(adaptive.expand(300, 75, 0.001), None);
    assert_eq!(adaptive.expand(75, 75, 0.03), None);
    for value in [
        "{mode='fixed',candidates=0}",
        "{mode='adaptive',min_candidates=300,max_candidates=75,min_gap=0.02}",
        "{mode='adaptive',min_candidates=75,max_candidates=300,min_gap=-1}",
    ] {
        let loaded = parse(&format!(
            "{MINIMAL}\n[namespace.retrieval]\nrescore_depth={value}"
        ))
        .unwrap();
        assert!(
            loaded
                .validate()
                .iter()
                .any(|i| i.path == "namespace.retrieval.rescore_depth")
        );
    }
}
#[test]
fn sparse_read_backend_is_explicit_and_does_not_change_policy() {
    use fyrr_core::config::SparseRead;
    let baseline = parse(MINIMAL).unwrap().config;
    assert_eq!(baseline.reader.sparse_read, SparseRead::MaskedScan);
    let take = parse(&format!("{MINIMAL}\n[reader]\nsparse_read='take_rows'"))
        .unwrap()
        .config;
    assert_eq!(take.reader.sparse_read, SparseRead::TakeRows);
    assert_eq!(
        baseline.namespace.retrieval_digest(),
        take.namespace.retrieval_digest()
    );
}

#[test]
fn config_unit_roundtrip_cases() {
    use fyrr_core::units::{ByteSize, ConfigDuration};
    for (text, expected, canonical) in [
        ("30s", ConfigDuration::from_secs(30), "30s"),
        ("250ms", ConfigDuration::from_millis(250), "250ms"),
        ("7d", ConfigDuration::from_days(7), "7d"),
        ("3600s", ConfigDuration::from_secs(3600), "1h"),
        ("90s", ConfigDuration::from_secs(90), "90s"),
        ("0ms", ConfigDuration::from_millis(0), "0ms"),
    ] {
        assert_eq!(text.parse(), Ok(expected), "{text}");
        assert_eq!(expected.to_string(), canonical, "{text}");
        assert_eq!(canonical.parse(), Ok(expected), "{canonical}");
    }
    for bad in [
        "",
        "30",
        "s",
        "1.5h",
        "-1s",
        "01s",
        "1 s",
        "1H",
        "99999999999999999999d",
    ] {
        assert!(bad.parse::<ConfigDuration>().is_err(), "{bad}");
    }
    for (text, expected, canonical) in [
        ("4096", ByteSize::bytes(4096), "4KiB"),
        ("64KiB", ByteSize::kib(64), "64KiB"),
        ("1536MiB", ByteSize::mib(1536), "1536MiB"),
        ("1000", ByteSize::bytes(1000), "1000B"),
    ] {
        assert_eq!(text.parse(), Ok(expected), "{text}");
        assert_eq!(expected.to_string(), canonical, "{text}");
        assert_eq!(canonical.parse(), Ok(expected), "{canonical}");
    }
    for bad in ["", "MiB", "1.5MiB", "1MB", "1 MiB", "-1"] {
        assert!(bad.parse::<ByteSize>().is_err(), "{bad}");
    }
}

#[test]
fn directory_defaults_to_none_and_rejects_unimplemented_providers() {
    let loaded = parse(MINIMAL).unwrap();
    assert_eq!(
        loaded.config.identity.directory.provider,
        fyrr_core::config::DirectoryProvider::None
    );
    assert!(
        loaded
            .missing_secrets(&env(&[("FYRR_IDENTITY_KEY", SENTINEL)]))
            .is_empty()
    );
    assert!(
        parse(&format!(
            "{MINIMAL}\n[identity.directory]\nprovider='google-workspace'\n"
        ))
        .is_err()
    );
    let example = parse(&common::read(EXAMPLE)).unwrap();
    assert!(example.config.identity.directory.credentials.is_none());
}

#[test]
fn unused_identity_selectors_are_rejected_and_absent_from_effective_config() {
    let issue = parse(&with_identity("user_key='email'")).unwrap_err();
    assert!(issue.to_string().contains("user_key"), "{issue}");
    for (extra, field) in [
        (
            "[identity.oidc]\nissuer='https://issuer.invalid'\naudience='fixture'\nsubject_claim='sub'\n",
            "subject_claim",
        ),
        (
            "[identity.oidc]\nissuer='https://issuer.invalid'\naudience='fixture'\nsubject_claim='email'\n",
            "subject_claim",
        ),
    ] {
        let issue = parse(&format!("{MINIMAL}\n{extra}")).unwrap_err();
        assert!(issue.to_string().contains(field), "{issue}");
    }
    let loaded = parse(&format!(
        "{MINIMAL}\n[identity.oidc]\nissuer='https://issuer.invalid'\naudience='fixture'\n"
    ))
    .unwrap();
    let values = loaded.effective_values(&env(&[]));
    assert!(!values.iter().any(|value| matches!(
        value.path.as_str(),
        "identity.user_key" | "identity.oidc.subject_claim"
    )));
}

#[test]
fn identity_keys_are_required_before_runtime() {
    let without = MINIMAL.replace("hashing_key = { env = \"FYRR_IDENTITY_KEY\" }\n", "");
    assert!(
        errors(&parse(&without).unwrap())
            .iter()
            .any(|error| error.contains("identity.hashing_key"))
    );
    let synced =
        "[[sources]]\nid = \"j\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"";
    assert!(
        errors_with(synced)
            .iter()
            .any(|error| error.contains("identity.encryption_key") && error.contains("sources[0]"))
    );
    assert_eq!(
        errors(&parse(&format!("{}\n{synced}", encrypted())).unwrap()),
        Vec::<String>::new()
    );
    let fixed = "[[sources]]\nid = \"f\"\ntype = \"local-files\"\npermissions = \"fixed\"\nreaders = [\"grp:a\"]";
    assert_eq!(errors_with(fixed), Vec::<String>::new());
}

const KEYCLOAK_DIRECTORY: &str = "[identity.directory]\nprovider = \"keycloak\"\nendpoint = \"https://sso.example\"\nrealm = \"acme\"\nclient_id = \"fyrr-directory\"\ncredentials = { env = \"FYRR_DIRECTORY_SECRET\" }\nverified_domains = [\"example.com\"]\n";

#[test]
fn keycloak_directory_derives_oidc_issuer_profile_and_keys() {
    let base = encrypted();
    let derived = parse(&format!(
        "{base}\n{KEYCLOAK_DIRECTORY}[identity.oidc]\naudience = \"fyrr\"\n"
    ))
    .unwrap();
    assert_eq!(errors(&derived), Vec::<String>::new());
    let oidc = derived.config.identity.oidc.as_ref().unwrap();
    assert_eq!(oidc.issuer, "https://sso.example/realms/acme");
    assert!(oidc.keycloak);
    assert_eq!(
        oidc.jwks_url,
        "https://sso.example/realms/acme/protocol/openid-connect/certs"
    );
    // Explicit values win, including keycloak = false and a pinned key URL.
    let explicit = parse(&format!(
        "{base}\n{KEYCLOAK_DIRECTORY}[identity.oidc]\naudience = \"fyrr\"\nkeycloak = false\njwks_url = \"https://keys.example/jwks\"\n"
    ))
    .unwrap();
    let oidc = explicit.config.identity.oidc.as_ref().unwrap();
    assert_eq!(oidc.issuer, "https://sso.example/realms/acme");
    assert!(!oidc.keycloak);
    assert_eq!(oidc.jwks_url, "https://keys.example/jwks");
    // A configured directory issuer replaces the realm URL.
    let issued = parse(&format!(
        "{base}\n{KEYCLOAK_DIRECTORY}issuer = \"https://login.example/realms/acme\"\n[identity.oidc]\naudience = \"fyrr\"\n"
    ))
    .unwrap();
    assert_eq!(
        issued.config.identity.oidc.as_ref().unwrap().issuer,
        "https://login.example/realms/acme"
    );
    // keycloak = true without a directory derives the key URL from the issuer.
    let profile = parse(&format!(
        "{MINIMAL}\n[identity.oidc]\nissuer = \"https://sso.example/realms/acme/\"\naudience = \"fyrr\"\nkeycloak = true\n"
    ))
    .unwrap();
    assert_eq!(errors(&profile), Vec::<String>::new());
    assert_eq!(
        profile.config.identity.oidc.as_ref().unwrap().jwks_url,
        "https://sso.example/realms/acme/protocol/openid-connect/certs"
    );
    // Without either, issuer and jwks_url stay required.
    let bare = errors(
        &parse(&format!(
            "{MINIMAL}\n[identity.oidc]\naudience = \"fyrr\"\n"
        ))
        .unwrap(),
    );
    for path in ["identity.oidc.issuer", "identity.oidc.jwks_url"] {
        assert!(
            bare.iter().any(|error| error.contains(path)),
            "{path}: {bare:?}"
        );
    }
}

#[test]
fn oidc_issuer_must_match_the_directory_issuer() {
    let base = encrypted();
    for directory in [
        KEYCLOAK_DIRECTORY,
        "[identity.directory]\nprovider = \"scim\"\nissuer = \"https://sso.example/realms/acme\"\nverified_domains = [\"example.com\"]\n",
    ] {
        let oidc = |issuer: &str| {
            errors(&parse(&format!(
                "{base}\n{directory}[identity.oidc]\nissuer = \"{issuer}\"\naudience = \"fyrr\"\njwks_url = \"https://sso.example/keys\"\n"
            ))
            .unwrap())
        };
        assert_eq!(
            oidc("https://sso.example/realms/acme"),
            Vec::<String>::new()
        );
        let other = oidc("https://sso.example/realms/other");
        assert!(
            other
                .iter()
                .any(|error| error.contains("identity.oidc.issuer")),
            "{other:?}"
        );
    }
}

#[test]
fn chat_sources_default_corporate_domains_to_verified_domains() {
    let text = format!(
        "{}\n{KEYCLOAK_DIRECTORY}[[sources]]\nid = \"s\"\ntype = \"slack\"\n[sources.intervals]\npermissions = \"30m\"\n[[sources]]\nid = \"j\"\ntype = \"jira\"\n[sources.intervals]\npermissions = \"30m\"\n[sources.settings]\ncorporate_domains = [\"corp.example\"]\n[[sources]]\nid = \"f\"\ntype = \"local-files\"\npermissions = \"fixed\"\nreaders = [\"grp:a\"]\n",
        encrypted()
    );
    let loaded = parse(&text).unwrap();
    let sources = &loaded.config.sources;
    assert_eq!(
        sources[0].settings["corporate_domains"],
        serde_json::json!(["example.com"])
    );
    assert_eq!(
        sources[1].settings["corporate_domains"],
        serde_json::json!(["corp.example"]),
        "an explicit value wins"
    );
    assert!(!sources[2].settings.contains_key("corporate_domains"));
    let unset = parse(&format!(
        "{}\n[[sources]]\nid = \"s\"\ntype = \"slack\"\n[sources.intervals]\npermissions = \"30m\"\n",
        encrypted()
    ))
    .unwrap();
    assert!(
        !unset.config.sources[0]
            .settings
            .contains_key("corporate_domains")
    );
}
