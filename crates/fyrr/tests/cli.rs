//! The real `fyrr` executable: config check/show/schema, secret handling,
//! connector providers and the dry-run sync (full build), the standing writer
//! service's required configuration, and the reader-only build's missing writer.

mod common;

use std::path::{Path, PathBuf};
use std::process::Output;

const SENTINEL: &str = "SENTINEL-cl1-5ecr3t-77a1";
#[cfg(not(feature = "writer"))]
const EXIT_UNAVAILABLE: i32 = 69;
const EXIT_CONFIG: i32 = 78;
const SECRETS: [&str; 6] = [
    "FYRR_S3_ACCESS_KEY_ID",
    "FYRR_S3_SECRET_ACCESS_KEY",
    "FYRR_EMBEDDING_API_KEY",
    "FYRR_JIRA_API_TOKEN",
    "FYRR_IDENTITY_KEY",
    "FYRR_ENCRYPTION_KEY",
];

fn temp_config(name: &str, text: &str) -> PathBuf {
    let path =
        Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-{}.toml", std::process::id()));
    std::fs::write(&path, text).expect("write temporary config");
    path
}

/// Runs fyrr with every example secret either set to the sentinel or unset.
fn fyrr(args: &[&str], secrets_set: bool) -> Output {
    let mut command = common::fyrr();
    command.args(args);
    for name in SECRETS {
        if secrets_set {
            command.env(name, SENTINEL);
        } else {
            command.env_remove(name);
        }
    }
    command.output().expect("run fyrr")
}

fn text(output: &Output) -> (String, String) {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        !stdout.contains(SENTINEL) && !stderr.contains(SENTINEL),
        "a secret value was printed:\n{stdout}\n{stderr}"
    );
    (stdout, stderr)
}

fn config_arg(path: &Path) -> String {
    path.display().to_string()
}

#[cfg_attr(feature = "writer", test)]
fn version_names_the_build() {
    let output = fyrr(&["--version"], false);
    let (stdout, _) = text(&output);
    assert!(output.status.success());
    let build = if cfg!(feature = "writer") {
        "(full build)"
    } else {
        "(reader-only build)"
    };
    assert!(stdout.contains(build), "{stdout}");
}

#[test]
fn config_check_passes_with_secrets_and_never_prints_them() {
    let path = config_arg(&common::example_config());
    let output = fyrr(&["--config", &path, "config", "check"], true);
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "{stdout}{stderr}");
    assert!(stdout.contains("config OK"), "{stdout}");
    assert!(
        stderr.contains("fixed connection"),
        "fixed connections are marked: {stderr}"
    );
}

#[test]
fn config_check_reports_unset_secrets_by_name() {
    let path = config_arg(&common::example_config());
    let output = fyrr(&["--config", &path, "config", "check"], false);
    let (_, stderr) = text(&output);
    assert_eq!(output.status.code(), Some(EXIT_CONFIG), "{stderr}");
    for name in SECRETS {
        assert!(stderr.contains(name), "{name} missing from: {stderr}");
    }
    let output = fyrr(&["--config", &path, "config", "check", "--no-env"], false);
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "{stdout}{stderr}");
}

#[test]
fn config_check_rejects_unknown_fields_and_inline_secrets() {
    let example = std::fs::read_to_string(common::example_config()).expect("example");
    let unknown = temp_config(
        "unknown",
        &format!("{example}\n[reader]\nmode = \"fast\"\n"),
    );
    let output = fyrr(
        &[
            "--config",
            &config_arg(&unknown),
            "config",
            "check",
            "--no-env",
        ],
        false,
    );
    let (_, stderr) = text(&output);
    assert_eq!(output.status.code(), Some(EXIT_CONFIG), "{stderr}");

    let inline = temp_config(
        "inline",
        &example.replace(
            "secret_access_key = { env = \"FYRR_S3_SECRET_ACCESS_KEY\" }",
            &format!("secret_access_key = \"{SENTINEL}\""),
        ),
    );
    let output = fyrr(
        &[
            "--config",
            &config_arg(&inline),
            "config",
            "check",
            "--no-env",
        ],
        false,
    );
    let (_, stderr) = text(&output);
    assert_eq!(output.status.code(), Some(EXIT_CONFIG), "{stderr}");
    assert!(
        stderr.contains("never stored in the config file"),
        "{stderr}"
    );

    let missing = Path::new(env!("CARGO_TARGET_TMPDIR")).join("does-not-exist.toml");
    let output = fyrr(
        &["--config", &config_arg(&missing), "config", "check"],
        false,
    );
    assert_eq!(output.status.code(), Some(EXIT_CONFIG));
}

#[test]
fn config_show_prints_values_with_origins() {
    let path = config_arg(&common::example_config());
    let output = fyrr(&["--config", &path, "config", "show"], true);
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "{stdout}{stderr}");
    let line = |prefix: &str| {
        stdout
            .lines()
            .find(|line| line.starts_with(prefix))
            .unwrap_or_else(|| panic!("{prefix} missing:\n{stdout}"))
            .to_owned()
    };
    assert!(line("namespace.name = \"acme\"").ends_with("# file"));
    assert!(line("writer.version_grace = \"1h\"").ends_with("# default"));
    assert!(
        line("storage.secret_access_key = env:FYRR_S3_SECRET_ACCESS_KEY (set)").ends_with("# env")
    );
}

#[cfg(feature = "writer")]
#[test]
fn reader_commands_require_configuration() {
    for args in [
        &["reader", "serve"][..],
        &["reader", "check"],
        &["reader", "search", "query"],
    ] {
        let output = fyrr(args, false);
        let (_, stderr) = text(&output);
        assert_eq!(
            output.status.code(),
            Some(EXIT_CONFIG),
            "{args:?}: {stderr}"
        );
        assert!(
            stderr.contains("configuration_invalid"),
            "{args:?}: {stderr}"
        );
    }
}

#[cfg(feature = "writer")]
#[test]
fn usage_errors_exit_two() {
    let output = fyrr(&["frobnicate"], false);
    assert_eq!(output.status.code(), Some(2));
}

#[cfg(feature = "writer")]
#[test]
fn writer_service_requires_configuration() {
    let output = fyrr(&["writer", "serve"], false);
    let (_, stderr) = text(&output);
    assert_eq!(output.status.code(), Some(EXIT_CONFIG), "{stderr}");
    assert!(stderr.contains("cannot read"), "{stderr}");
}

#[cfg(not(feature = "writer"))]
#[test]
fn reader_only_build_explains_the_missing_writer() {
    version_names_the_build();
    for args in [
        &["writer"][..],
        &["writer", "sync", "--full"],
        &["writer", "commit"],
        &["writer", "--help"],
    ] {
        let output = fyrr(args, false);
        let (_, stderr) = text(&output);
        assert_eq!(
            output.status.code(),
            Some(EXIT_UNAVAILABLE),
            "{args:?}: {stderr}"
        );
        assert!(stderr.contains("reader-only"), "{args:?}: {stderr}");
        assert!(stderr.contains("full build"), "{args:?}: {stderr}");
    }
}

#[test]
fn config_show_never_prints_inline_secrets() {
    let example = std::fs::read_to_string(common::example_config()).expect("example");
    let inline = temp_config(
        "show-inline",
        &example.replace(
            "api_token = { env = \"FYRR_JIRA_API_TOKEN\" }",
            &format!("api_token = \"{SENTINEL}\""),
        ),
    );
    let output = fyrr(&["--config", &config_arg(&inline), "config", "show"], true);
    let (stdout, stderr) = text(&output);
    assert_eq!(output.status.code(), Some(EXIT_CONFIG), "{stdout}{stderr}");
    let redacted = if cfg!(feature = "writer") {
        "sources[1].settings.api_token = <redacted: secret field>"
    } else {
        "sources[1].settings.api_token = <redacted: no provider schema in the reader-only build>"
    };
    assert!(stdout.contains(redacted), "{stdout}");
    assert!(stderr.contains("sources[1].settings.api_token"), "{stderr}");
}

#[test]
fn config_schema_is_the_generated_schema_plus_installed_providers() {
    let output = fyrr(&["config", "schema"], false);
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "{stderr}");
    let schema: serde_json::Value = serde_json::from_str(&stdout).expect("JSON");
    let types = &schema["$defs"]["SourceConfig"]["properties"]["type"]["enum"];
    if cfg!(feature = "writer") {
        let types: Vec<&str> = types
            .as_array()
            .expect("installed providers")
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect();
        for name in ["jira", "local-files", "sentinel-docs", "sentinel-secret"] {
            assert!(types.contains(&name), "{name} missing: {types:?}");
        }
        for refused in ["sentinel-future-sdk", "sentinel-bad-metadata"] {
            assert!(!types.contains(&refused), "{refused} is refused: {types:?}");
        }
        assert!(
            schema["$defs"]["provider-sentinel-secret-settings"]["properties"]["access"]["x-fyrr-secret"]
                == true
        );
    } else {
        assert!(types.is_null(), "the reader-only build knows no providers");
        let committed = std::fs::read_to_string(common::repo().join("schemas/config.schema.json"))
            .expect("committed schema");
        assert_eq!(
            schema,
            serde_json::from_str::<serde_json::Value>(&committed).expect("JSON")
        );
    }
}

#[cfg(not(feature = "writer"))]
#[test]
fn reader_only_config_check_says_providers_are_not_checked() {
    let path = config_arg(&common::example_config());
    let output = fyrr(&["--config", &path, "config", "check", "--no-env"], false);
    let (_, stderr) = text(&output);
    assert!(output.status.success(), "{stderr}");
    assert!(
        stderr.contains("not checked against connector providers"),
        "{stderr}"
    );
}

#[cfg(feature = "writer")]
mod providers {
    use super::*;
    use common::{Sandbox, Sentinels, config_with, provider_fixture, source};

    #[test]
    fn providers_lists_bundled_and_installed_providers_and_refuses_bad_ones() {
        let output = fyrr(&["writer", "providers"], false);
        let (stdout, stderr) = text(&output);
        assert_eq!(
            output.status.code(),
            Some(1),
            "refused providers exist: {stderr}"
        );
        let line = |name: &str| {
            stdout
                .lines()
                .find(|line| line.starts_with(&format!("{name} ")))
                .unwrap_or_else(|| panic!("{name} missing:\n{stdout}"))
                .to_owned()
        };
        assert!(
            stdout.starts_with("fyrr-connectors SDK 0.1.1 (this fyrr build loads SDK series 0.1)")
        );
        for bundled in [
            "github",
            "google-drive",
            "jira",
            "local-files",
            "notion",
            "slack",
        ] {
            assert!(line(bundled).ends_with("  ok"), "{}", line(bundled));
            assert!(
                line(bundled).contains("fyrr-connectors"),
                "{}",
                line(bundled)
            );
        }
        assert!(
            line("sentinel-docs").contains("fyrr-test-providers  1.0.0"),
            "{}",
            line("sentinel-docs")
        );
        assert!(line("sentinel-docs").contains("content,permissions,members,reconcile"));
        assert!(
            line("sentinel-future-sdk").contains(
                "refused: requires SDK series 0.2 (min_sdk_version 0.2.0); this fyrr build loads SDK series 0.1"
            ),
            "{}",
            line("sentinel-future-sdk")
        );
        assert!(line("sentinel-bad-metadata").contains("refused: invalid metadata"));
        assert!(
            line("sentinel-raising-metadata").contains("refused: its metadata callable failed")
        );
    }

    #[test]
    fn config_check_validates_sources_against_their_providers() {
        let sandbox = Sandbox::new("cli-provider-config");
        let fixture = sandbox.input("docs.json", "{}");
        for (extra, path, message) in [
            (
                "\n[[sources]]\nid = \"x\"\ntype = \"no-such-provider\"\npermissions = \"fixed\"\nreaders = [\"grp:x\"]\n".to_owned(),
                "sources[0].type",
                "no installed connector provider has this name",
            ),
            (
                "\n[[sources]]\nid = \"x\"\ntype = \"sentinel-future-sdk\"\npermissions = \"fixed\"\nreaders = [\"grp:x\"]\n".to_owned(),
                "sources[0].type",
                "its connector provider is refused: requires SDK series 0.2",
            ),
            (
                source("x", "sentinel-docs", &fixture, "unexpected = 1"),
                "sources[0].settings",
                "unknown field(s) for this provider: unexpected",
            ),
            (
                source("x", "sentinel-invalid", &fixture, "variant = \"nope\""),
                "sources[0].settings.variant",
                "does not match the provider's config schema (enum)",
            ),
            (
                "\n[[sources]]\nid = \"x\"\ntype = \"sentinel-docs\"\npermissions = \"fixed\"\nreaders = [\"grp:x\"]\n".to_owned(),
                "sources[0].scope",
                "missing field `fixture` required by the provider",
            ),
            (
                "\n[[sources]]\nid = \"x\"\ntype = \"local-files\"\npermissions = \"sync\"\n[sources.intervals]\npermissions = \"30m\"\n".to_owned(),
                "sources[0].permissions",
                "the provider supports only these connection types: fixed",
            ),
        ] {
            let path_file = sandbox.input("fyrr.toml", &config_with(&extra));
            let output = fyrr(&["--config", &config_arg(&path_file), "config", "check", "--no-env"], false);
            let (_, stderr) = text(&output);
            assert_eq!(output.status.code(), Some(EXIT_CONFIG), "{stderr}");
            assert!(stderr.contains(&format!("error: {path}: {message}")), "{path}: {stderr}");
        }
    }

    #[test]
    fn dry_run_validates_each_source_in_its_own_worker() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("cli-dry-run");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let mut config = format!(
            "\n[[sources]]\nid = \"docs\"\ntype = \"sentinel-docs\"\n[sources.intervals]\npermissions = \"30m\"\nmembers = \"30m\"\n[sources.scope]\nfixture = \"{}\"\n",
            fixture.display()
        );
        for mode in ["abort", "hang", "print"] {
            config.push_str(&source(
                mode,
                "sentinel-failing",
                &fixture,
                &format!("mode = \"{mode}\""),
            ));
        }
        let path = sandbox.input("fyrr.toml", &config_with(&config));
        let path = config_arg(&path);
        let started = std::time::Instant::now();
        let output = fyrr(
            &[
                "--config",
                &path,
                "writer",
                "sync",
                "--dry-run",
                "--timeout",
                "5s",
                "--jobs",
                "4",
            ],
            false,
        );
        let (stdout, stderr) = text(&output);
        assert_eq!(
            output.status.code(),
            Some(1),
            "partial failure: {stdout}{stderr}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "the hang was not killed"
        );
        for line in [
            "source docs (provider sentinel-docs 1.0.0): ok",
            "  content      batches=1 upserts=1 deletes=0 sections=2",
            "  permissions  batches=1 documents=1 container_grants=0",
            "  members      batches=1 users=1 groups=1",
            "  reconcile    batches=1 root_ids=1 complete=true",
            "source abort (provider sentinel-failing 1.0.0): FAILED",
            "  error: the provider failed",
            "source hang (provider sentinel-failing 1.0.0): FAILED",
            "  error: the worker timed out and its process group was killed",
            "source print (provider sentinel-failing 1.0.0): ok",
            "dry run: 2 source(s) passed, 2 failed",
        ] {
            assert!(stdout.contains(line), "{line} missing:\n{stdout}");
        }
        assert!(
            !stderr.contains("provider stdout line"),
            "provider stdout must be suppressed: {stderr}"
        );

        let output = fyrr(
            &[
                "--config",
                &path,
                "writer",
                "sync",
                "--dry-run",
                "docs",
                "--kind",
                "reconcile",
            ],
            false,
        );
        let (stdout, _) = text(&output);
        assert!(output.status.success(), "{stdout}");
        assert!(
            stdout.contains("  reconcile    batches=1") && !stdout.contains("  content "),
            "{stdout}"
        );

        let output = fyrr(
            &[
                "--config",
                &path,
                "writer",
                "sync",
                "--dry-run",
                "no-such-source",
            ],
            false,
        );
        assert_eq!(output.status.code(), Some(2));
        let output = fyrr(&["--config", &path, "writer", "sync", "docs"], false);
        let (stdout, stderr) = text(&output);
        assert_eq!(output.status.code(), Some(1), "{stdout}{stderr}");
        let report: serde_json::Value = serde_json::from_str(&stdout).expect("worker error report");
        assert_eq!(report["error"], "storage_unavailable");
        assert_eq!(report["paid_calls"], 0);
    }

    /// Whether a process still runs; a zombie that nobody has reaped yet does not.
    fn running(pid: &str) -> bool {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|stat| {
                stat.rsplit_once(')')
                    .map(|(_, rest)| rest.trim_start().starts_with(['Z', 'X']))
            })
            .is_some_and(|dead| !dead)
    }

    /// A provider that hangs, exhausts its memory, closes the connection to
    /// the worker by dying, aborts, exits or raises ends in a closed diagnostic
    /// and leaves no process behind, whichever process the failure was in.
    #[test]
    fn failing_providers_end_in_closed_diagnostics_and_leave_no_process() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("cli-isolation");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let pids = sandbox.output().join("pids");
        std::fs::create_dir_all(&pids).unwrap();
        let modes = [
            (
                "hang",
                "the worker timed out and its process group was killed",
            ),
            ("memory", "the provider failed"),
            ("die", "the provider failed"),
            ("abort", "the provider failed"),
            ("exit", "the provider failed"),
            ("raise", "the provider failed"),
        ];
        let mut config = String::new();
        for (mode, _) in modes {
            config.push_str(&source(
                mode,
                "sentinel-failing",
                &fixture,
                &format!("mode = \"{mode}\""),
            ));
        }
        let path = sandbox.input("fyrr.toml", &config_with(&config));
        let started = std::time::Instant::now();
        let output = common::fyrr()
            .env("FYRR_TEST_PID_DIR", &pids)
            .args(["--config", &config_arg(&path)])
            .args([
                "writer",
                "sync",
                "--dry-run",
                "--timeout",
                "10s",
                "--jobs",
                "6",
            ])
            .output()
            .unwrap();
        let (stdout, stderr) = text(&output);
        assert_eq!(output.status.code(), Some(1), "{stdout}{stderr}");
        assert!(started.elapsed() < std::time::Duration::from_secs(60));
        for (mode, error) in modes {
            let section = stdout
                .split("source ")
                .find(|section| section.starts_with(&format!("{mode} (")))
                .unwrap_or_else(|| panic!("{mode} missing:\n{stdout}"));
            assert!(
                section.contains("FAILED") && section.contains(&format!("error: {error}")),
                "{mode}:\n{section}"
            );
            let pid = std::fs::read_to_string(pids.join(format!("{mode}.pid")))
                .unwrap_or_else(|_| panic!("{mode}: the provider never ran"));
            assert!(
                !running(&pid),
                "{mode}: provider process {pid} outlived the command"
            );
        }
        assert!(
            pids.join("memory.limited").exists(),
            "the memory limit did not reach the provider process"
        );
        // Nothing the providers raised or printed reaches the output.
        for private in [
            "cannot parse document",
            "giving up on",
            "provider stdout line",
        ] {
            assert!(
                !stdout.contains(private) && !stderr.contains(private),
                "{private}"
            );
        }
    }

    /// Without a Python the commands that run providers stop with one message
    /// that names the fix; every other command works; an interpreter in the
    /// current directory (a relative `PATH` entry) is never run.
    #[test]
    fn commands_without_python_name_the_fix_and_the_others_run() {
        use std::os::unix::fs::PermissionsExt;
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("cli-no-python");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let with_source = sandbox.input(
            "with-source.toml",
            &config_with(&source("docs", "sentinel-docs", &fixture, "")),
        );
        let without = sandbox.input("without.toml", &config_with(""));
        let cwd = sandbox.output().join("cwd");
        let planted = cwd.join("python3");
        let marker = cwd.join("planted-python-ran");
        std::fs::write(
            &planted,
            format!("#!/bin/sh\ntouch '{}'\nexit 1\n", marker.display()),
        )
        .unwrap();
        std::fs::set_permissions(&planted, std::fs::Permissions::from_mode(0o755)).unwrap();
        let run = |config: &Path, args: &[&str], path: &str| {
            let mut command = common::fyrr();
            command
                .env_remove("VIRTUAL_ENV")
                .env("PATH", path)
                .current_dir(&cwd)
                .arg("--config")
                .arg(config)
                .args(args);
            command.output().unwrap()
        };
        for path in ["/nonexistent", ".", ":"] {
            for args in [
                &["config", "check", "--no-env"][..],
                &["config", "schema"],
                &["writer", "providers"],
                &["writer", "sync", "--dry-run"],
            ] {
                let output = run(&with_source, args, path);
                let (stdout, stderr) = text(&output);
                assert_eq!(output.status.code(), Some(1), "{args:?} {stdout}{stderr}");
                assert!(
                    stderr.contains("pip install fyrr-connectors[<type>]")
                        && stderr.contains("VIRTUAL_ENV"),
                    "{args:?}: {stderr}"
                );
            }
            // Without sources nothing needs Python.
            let output = run(&without, &["config", "check", "--no-env"], path);
            assert!(output.status.success(), "{}", text(&output).1);
            let output = run(&without, &["writer", "sync", "--dry-run"], path);
            assert!(output.status.success(), "{}", text(&output).1);
        }
        let output = run(&without, &["--version"], "/nonexistent");
        assert!(text(&output).0.contains("fyrr"), "{:?}", text(&output));
        assert!(
            !marker.exists(),
            "an interpreter from the current directory ran"
        );
        // A set but unusable VIRTUAL_ENV is named, and is not replaced by another Python.
        let mut command = common::fyrr();
        command
            .env("VIRTUAL_ENV", sandbox.output().join("no-such-env"))
            .arg("--config")
            .arg(&with_source)
            .args(["writer", "providers"]);
        let output = command.output().unwrap();
        let (_, stderr) = text(&output);
        assert_eq!(output.status.code(), Some(1), "{stderr}");
        assert!(
            stderr.contains("no-such-env") && stderr.contains("VIRTUAL_ENV"),
            "{stderr}"
        );
    }

    #[test]
    fn fixed_connections_make_no_permission_or_member_reads() {
        let mut sentinels = Sentinels::new();
        let sandbox = Sandbox::new("cli-fixed");
        let fixture = sandbox.input("docs.json", &provider_fixture(&mut sentinels));
        let path = sandbox.input(
            "fyrr.toml",
            &config_with(&source("docs", "sentinel-docs", &fixture, "")),
        );
        let output = fyrr(
            &[
                "--config",
                &config_arg(&path),
                "writer",
                "sync",
                "--dry-run",
                "--kind",
                "content",
                "--kind",
                "members",
            ],
            false,
        );
        let (stdout, _) = text(&output);
        assert!(output.status.success(), "{stdout}");
        assert!(stdout.contains("  members      skipped"), "{stdout}");
        assert!(!stdout.contains("users="), "{stdout}");
    }
}

#[test]
fn worker_memory_ceiling_is_configurable_and_validated() {
    let sandbox = common::Sandbox::new("cli-worker-memory");
    for (value, expected) in [("0B", 78), ("2GiB", 0)] {
        let config = sandbox.input(
            "fyrr.toml",
            &common::config_with(&format!("[writer]\nworker_memory_max_bytes = \"{value}\"")),
        );
        let output = sandbox.run(
            ["--config", config.to_str().unwrap(), "config", "check"],
            &[],
        );
        assert_eq!(
            output.status.code(),
            Some(expected),
            "{:?}",
            common::text(&output)
        );
    }
}

#[test]
#[cfg(feature = "writer")]
fn every_writer_command_rejects_invalid_cross_field_configuration() {
    let path = temp_config(
        "writer-cross-field",
        "[namespace]\nname='invalid'\n[storage]\nuri='s3://unused'\n[embedding]\nendpoint='https://unused.invalid'\n[writer]\npurge_window='1h'\n",
    );
    let config = config_arg(&path);
    for args in [
        vec!["commit"],
        vec!["maintain"],
        vec!["restore", "pub-00000000000000000000000000000000"],
        vec!["rollback"],
        vec!["verify"],
        vec!["status"],
        vec!["namespace", "create"],
        vec!["serve"],
    ] {
        let mut command = vec!["--config", &config, "writer"];
        command.extend(args);
        let output = fyrr(&command, false);
        assert_eq!(output.status.code(), Some(EXIT_CONFIG));
        assert!(String::from_utf8_lossy(&output.stderr).contains("writer.purge_window"));
    }
    let output = fyrr(
        &["--config", &config, "writer", "sync", "--full", "--dry-run"],
        false,
    );
    assert_eq!(output.status.code(), Some(2));
}
