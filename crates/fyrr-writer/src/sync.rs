//! `fyrr writer sync` and the worker side of a source sync. Each
//! source's provider runs in the Python host process of its own worker; its
//! batches arrive over the host's socket and are validated at the boundary.
//! `--dry-run` writes nothing. The output holds counts, never values.

use crate::{Result, WriterError};
use std::collections::VecDeque;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use std::sync::LazyLock;

use fyrr_core::config::{Config, LoadedConfig, SOURCE_ID_PATTERN, Severity, SourceConfig};
use fyrr_core::connector::SyncKind;
use fyrr_core::exit;
use serde::{Deserialize, Serialize};

use crate::batch::{BatchChecker, KindStats, Limits};
use crate::providers::{self, Discovery};
use crate::python;
use crate::secrets::Credentials;
use crate::worker::{self, Diagnostic, Job, Outcome, Running};

static SOURCE_ID: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(SOURCE_ID_PATTERN).unwrap_or_else(|error| unreachable!("{error}"))
});

/// The worker's JSON result for one source.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReport {
    pub kinds: Vec<KindReport>,
    /// A failure before any kind ran (configuration, provider loading).
    pub error: Option<Diagnostic>,
}

/// One sync kind of a source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KindReport {
    pub kind: SyncKind,
    pub stats: KindStats,
    pub error: Option<Diagnostic>,
}

pub(crate) fn source_json(source: &SourceConfig, limits: Limits) -> String {
    serde_json::json!({
        "source_id": source.id,
        "provider": source.provider,
        "permissions": providers::permissions_str(source.permissions),
        "readers": source.readers,
        "settings": source.settings,
        "scope": source.scope,
        "limits": limits,
    })
    .to_string()
}

/// The report fields shared by reads that made no embedding calls.
pub(crate) enum ReportDefaults {
    Source,
    Kind { kind: SyncKind, stats: KindStats },
    NotDue(SyncKind),
}

pub(crate) fn default_report(defaults: ReportDefaults) -> serde_json::Value {
    let (kind, skipped, stats) = match defaults {
        ReportDefaults::Source => (None, false, KindStats::default()),
        ReportDefaults::Kind { kind, stats } => (Some(kind), false, stats),
        ReportDefaults::NotDue(kind) => (Some(kind), true, KindStats::default()),
    };
    let mut report = serde_json::json!({
        "stats": stats, "cache_hits": 0, "cache_misses": 0, "paid_calls": 0
    });
    if let Some(kind) = kind {
        report["kind"] = serde_json::json!(kind);
    }
    if skipped {
        report["skipped"] = "interval_not_due".into();
    }
    report
}

impl SourceReport {
    fn failed(&self) -> bool {
        self.error.is_some() || self.kinds.iter().any(|kind| kind.error.is_some())
    }
}

/// Discovers the installed providers in a worker process.
pub fn discover(memory: u64) -> Result<Discovery, String> {
    let outcome = Running::spawn_with_memory(&Job::Discover, worker::DISCOVERY_TIMEOUT, memory)
        .and_then(Running::wait)
        .map_err(|_| "cannot start the Python worker")?;
    // A host that could not start is reported by its worker as a closed code;
    // this process, which runs no Python, prints the one message that names the fix.
    if let Some(error) = outcome
        .result_line()
        .and_then(python::host_error_from_result)
    {
        return Err(error.to_string());
    }
    if let Some(failure) = outcome.failure() {
        return Err(failure.to_string());
    }
    let mut inventory: serde_json::Value =
        serde_json::from_slice(outcome.result_line().unwrap_or_default())
            .map_err(|_| "the provider inventory is unreadable")?;
    let entries = inventory
        .get_mut("providers")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("the provider inventory is unreadable")?;
    // Sequential: a provider crash/hang affects only its metadata record.
    for entry in entries {
        let Some(name) = entry.get("entry_point").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let outcome = Running::spawn_with_memory(
            &Job::Metadata {
                provider: name.to_owned(),
            },
            worker::DISCOVERY_TIMEOUT,
            memory,
        )
        .and_then(Running::wait);
        let info = outcome
            .ok()
            .filter(|outcome| outcome.failure().is_none())
            .and_then(|outcome| {
                serde_json::from_slice::<serde_json::Value>(outcome.result_line()?).ok()
            })
            .and_then(|value| value.get("providers")?.as_array()?.first().cloned());
        if let Some(info) = info {
            if let Some(object) = entry.as_object_mut() {
                for key in ["info", "error"] {
                    if let Some(value) = info.get(key) {
                        object.insert(key.into(), value.clone());
                    }
                }
            }
        } else {
            entry["error"] = serde_json::json!({"type": "metadata_worker_failure"});
        }
    }
    providers::parse_discovery(
        &serde_json::to_vec(&inventory).map_err(|_| "invalid provider inventory")?,
    )
}

/// Source selection and worker limits shared by both sync modes.
#[derive(Debug, Clone)]
pub struct SyncOptions<'a> {
    pub config_path: &'a Path,
    pub sources: &'a [String],
    pub kinds: &'a [SyncKind],
    pub timeout: Duration,
    pub jobs: usize,
    pub memory: u64,
}

struct Plan<'a> {
    source: &'a SourceConfig,
    provider: String,
    version: String,
    kinds: Vec<SyncKind>,
    skipped: Vec<SyncKind>,
}

/// Keep configured source order and unknown argument order without extra collections.
fn resolve_sources<'a>(
    config: &'a Config,
    selected: &'a [String],
) -> Result<impl Iterator<Item = &'a SourceConfig> + 'a, impl Iterator<Item = &'a str> + 'a> {
    let mut unknown = selected
        .iter()
        .filter(|id| !config.sources.iter().any(|source| &source.id == *id))
        .map(String::as_str);
    if let Some(first) = unknown.next() {
        Err(std::iter::once(first).chain(unknown))
    } else {
        Ok(config
            .sources
            .iter()
            .filter(move |source| selected.is_empty() || selected.contains(&source.id)))
    }
}

enum JobMode {
    Validate,
    Prepare {
        full: bool,
        due: bool,
        embedding_cap: Option<u64>,
    },
}

fn sync_job(
    options: &SyncOptions<'_>,
    source: &SourceConfig,
    kinds: &[SyncKind],
    mode: JobMode,
) -> Job {
    let (prepare, embedding_cap, full, due) = match mode {
        JobMode::Validate => (false, None, false, false),
        JobMode::Prepare {
            full,
            due,
            embedding_cap,
        } => (true, embedding_cap, full, due),
    };
    Job::Sync {
        config: options.config_path.to_owned(),
        source: source.id.clone(),
        kinds: kinds.to_vec(),
        prepare,
        embedding_cap,
        full,
        due,
    }
}

/// Validates the configuration with its providers, then runs every selected
/// source in its own worker (at most `jobs` at once) and reports per source.
/// Exit: 0 all sources passed, 1 any failed, 2 unknown source, 78 invalid
/// configuration.
#[allow(clippy::print_stdout, clippy::print_stderr)]
pub fn dry_run(loaded: &LoadedConfig, options: &SyncOptions<'_>) -> ExitCode {
    let config = &loaded.config;
    let sources = match resolve_sources(config, options.sources) {
        Ok(sources) => sources,
        Err(unknown) => {
            // Arguments are echoed only when shaped like a source ID; anything
            // else may be text the operator did not mean to log.
            let shown: Vec<&str> = unknown
                .map(|id| {
                    if SOURCE_ID.is_match(id) {
                        id
                    } else {
                        "<not a source ID>"
                    }
                })
                .collect();
            eprintln!(
                "error: no configured source has the ID(s): {}",
                shown.join(", ")
            );
            return ExitCode::from(exit::USAGE);
        }
    };
    let discovery = match checked_discovery(config) {
        Ok(discovery) => discovery,
        Err(code) => return code,
    };
    let plans: Vec<Plan<'_>> = sources
        .map(|source| plan(source, &discovery, options.kinds))
        .collect();
    println!(
        "dry run: {} source(s), each in its own worker; nothing is written",
        plans.len()
    );
    let reports = run_workers(&plans, options);
    let mut failed = 0;
    for (plan, report) in plans.iter().zip(&reports) {
        failed += usize::from(report.failed());
        print_report(plan, report);
    }
    println!(
        "dry run: {} source(s) passed, {failed} failed",
        plans.len() - failed
    );
    if failed > 0 {
        ExitCode::from(exit::FAILURE)
    } else {
        ExitCode::SUCCESS
    }
}

/// Validate all configured sources before dispatch, in both sync modes.
#[allow(clippy::print_stderr)]
fn checked_discovery(config: &fyrr_core::config::Config) -> Result<Discovery, ExitCode> {
    // No sources, no providers, no Python.
    if config.sources.is_empty() {
        return Ok(Discovery {
            sdk_version: providers::NO_VALUE.to_owned(),
            providers: Vec::new(),
        });
    }
    let discovery = match discover(config.writer.worker_memory_max_bytes.as_u64()) {
        Ok(discovery) => discovery,
        Err(error) => {
            eprintln!("error: {error}");
            return Err(ExitCode::from(exit::FAILURE));
        }
    };
    let issues = providers::check_sources(config, &discovery);
    let errors: Vec<_> = issues
        .iter()
        .filter(|issue| issue.severity == Severity::Error)
        .collect();
    if !errors.is_empty() {
        for issue in &errors {
            eprintln!("{issue}");
        }
        eprintln!("sync: {} configuration error(s); nothing ran", errors.len());
        return Err(ExitCode::from(exit::CONFIG));
    }
    Ok(discovery)
}

fn plan<'a>(source: &'a SourceConfig, discovery: &Discovery, requested: &[SyncKind]) -> Plan<'a> {
    let provider = discovery
        .find(&source.provider)
        .unwrap_or_else(|| unreachable!("sources were checked against the providers"));
    let info = provider
        .info
        .as_ref()
        .unwrap_or_else(|| unreachable!("an accepted provider has metadata"));
    let defaults = providers::default_kinds(info, source);
    let (kinds, skipped) = providers::select_kinds(defaults, requested);
    Plan {
        source,
        provider: provider.display_name().to_owned(),
        version: provider
            .version
            .clone()
            .unwrap_or_else(|| providers::NO_VALUE.to_owned()),
        kinds,
        skipped,
    }
}

fn worker_pool(
    options: &SyncOptions<'_>,
    jobs: Vec<Job>,
    mut finish: impl FnMut(usize, Result<Outcome, String>),
) {
    let mut pending: VecDeque<_> = jobs.into_iter().enumerate().collect();
    let mut running = Vec::new();
    while !pending.is_empty() || !running.is_empty() {
        while running.len() < options.jobs.max(1) {
            let Some((index, job)) = pending.pop_front() else {
                break;
            };
            match Running::spawn_with_memory(&job, options.timeout, options.memory) {
                Ok(worker) => running.push((index, worker)),
                Err(_) => finish(index, Err(WriterError::WorkerFailure.into())),
            }
        }
        let mut still = Vec::with_capacity(running.len());
        for (index, mut worker) in running {
            match worker.poll() {
                Ok(None) => still.push((index, worker)),
                Ok(Some(outcome)) => finish(index, Ok(outcome)),
                Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                    finish(index, Ok(Outcome::TimedOut(options.timeout)));
                }
                Err(_) => finish(index, Err(WriterError::WorkerFailure.into())),
            }
        }
        running = still;
        if !running.is_empty() {
            std::thread::sleep(worker::POLL);
        }
    }
}
fn run_workers(plans: &[Plan<'_>], options: &SyncOptions<'_>) -> Vec<SourceReport> {
    let mut reports = vec![SourceReport::default(); plans.len()];
    let jobs = plans
        .iter()
        .map(|plan| sync_job(options, plan.source, &plan.kinds, JobMode::Validate))
        .collect();
    worker_pool(options, jobs, |index, outcome| {
        reports[index] = match outcome {
            Ok(outcome) => report_from(&outcome, &plans[index].kinds),
            Err(_) => SourceReport {
                kinds: Vec::new(),
                error: Some(Diagnostic::WorkerFailure),
            },
        };
    });
    reports
}

/// The worker's own report, or the reason it has none (crash, timeout).
fn report_from(outcome: &Outcome, expected: &[SyncKind]) -> SourceReport {
    let parsed = outcome
        .result_line()
        .and_then(|line| serde_json::from_slice::<SourceReport>(line).ok());
    if let Some(report) = parsed {
        let kinds: Vec<_> = report.kinds.iter().map(|report| report.kind).collect();
        let shape_valid = (report.error.is_some() && report.kinds.is_empty())
            || (report.error.is_none() && kinds == expected);
        let exit_valid = match outcome {
            Outcome::Exited { status, .. } => {
                status.code() == Some(if report.failed() { 1 } else { 0 })
            }
            Outcome::TimedOut(_) => false,
        };
        if shape_valid && exit_valid {
            return report;
        }
    }
    SourceReport {
        kinds: Vec::new(),
        error: Some(outcome.failure().unwrap_or(Diagnostic::InvalidReport)),
    }
}

#[allow(clippy::print_stdout)]
fn print_report(plan: &Plan<'_>, report: &SourceReport) {
    let status = if report.failed() { "FAILED" } else { "ok" };
    println!(
        "source {} (provider {} {}): {status}",
        plan.source.id, plan.provider, plan.version
    );
    if let Some(error) = &report.error {
        println!("  error: {error}");
    }
    for kind in &report.kinds {
        let stats = &kind.stats;
        let counts = match kind.kind {
            SyncKind::Content => format!(
                "batches={} upserts={} deletes={} sections={} text_bytes={} oversized={}",
                stats.batches,
                stats.upserts,
                stats.deletes,
                stats.sections,
                stats.text_bytes,
                stats.oversized
            ),
            SyncKind::Permissions => format!(
                "batches={} documents={} container_grants={}",
                stats.batches, stats.permission_documents, stats.container_grants
            ),
            SyncKind::Members => format!(
                "batches={} users={} groups={}",
                stats.batches, stats.users, stats.groups
            ),
            SyncKind::Reconcile => format!(
                "batches={} root_ids={} complete={}",
                stats.batches, stats.listed_root_ids, stats.listing_complete
            ),
        };
        println!(
            "  {:<12} {counts} cursor_advanced={}",
            kind.kind.as_str(),
            stats.cursor_advanced
        );
        if let Some(error) = &kind.error {
            println!("  {:<12} error: {error}", kind.kind.as_str());
        }
    }
    for kind in &plan.skipped {
        println!(
            "  {:<12} skipped: not implemented by the provider or not used by this connection type",
            kind.as_str()
        );
    }
}

/// The worker side: runs the given kinds of one source in Python hosts of
/// this process and sends one structured report on its result channel.
pub fn worker_sync(
    loaded: &LoadedConfig,
    source_id: &str,
    kinds: &[SyncKind],
    credentials: &Credentials,
) -> ExitCode {
    let report = sync_source(loaded, source_id, kinds, credentials);
    let sent = serde_json::to_vec(&report)
        .ok()
        .is_some_and(|bytes| worker::send(&bytes).is_ok());
    if report.failed() || !sent {
        ExitCode::from(exit::FAILURE)
    } else {
        ExitCode::SUCCESS
    }
}

fn sync_source(
    loaded: &LoadedConfig,
    source_id: &str,
    kinds: &[SyncKind],
    credentials: &Credentials,
) -> SourceReport {
    let config = &loaded.config;
    let Some((index, source)) = config
        .sources
        .iter()
        .enumerate()
        .find(|(_, source)| source.id == source_id)
    else {
        return SourceReport {
            kinds: Vec::new(),
            error: Some(Diagnostic::InvalidSource),
        };
    };
    let limits = Limits::for_source(config, source);
    // The discovery host ends with this call, before any kind starts its own:
    // at most one Python process per worker runs at a time.
    let checked = python::with_host(credentials, |host| {
        let discovery = providers::parse_discovery(
            python::discover_provider(host, &source.provider)?.as_bytes(),
        )?;
        let issues = providers::check_source(index, source, &discovery);
        if issues.iter().any(|issue| issue.severity == Severity::Error) {
            return Err("the source does not match its provider; run `fyrr config check`".into());
        }
        Ok(())
    });
    if checked.is_err() {
        return SourceReport {
            kinds: Vec::new(),
            error: Some(Diagnostic::InvalidSource),
        };
    }
    let source_json = source_json(source, limits);
    let kinds = kinds
        .iter()
        .map(|&kind| {
            let mut checker = BatchChecker::new(&source.id, kind, limits, None);
            let error = python::run_kind(credentials, &source_json, kind, &mut checker).err();
            KindReport {
                kind,
                stats: checker.stats,
                error,
            }
        })
        .collect();
    SourceReport { kinds, error: None }
}

/// Real preparation shares the existing source-isolated worker supervisor.
#[allow(clippy::print_stdout, clippy::print_stderr)]
pub fn prepared(
    loaded: &LoadedConfig,
    options: &SyncOptions<'_>,
    full: bool,
    due: bool,
) -> ExitCode {
    let Ok(selected) = resolve_sources(&loaded.config, options.sources) else {
        eprintln!("error: source_invalid");
        return ExitCode::from(exit::USAGE);
    };
    if let Err(code) = checked_discovery(&loaded.config) {
        return code;
    }
    let sources: Vec<_> = selected.collect();
    let jobs = sources
        .iter()
        .map(|source| {
            sync_job(
                options,
                source,
                options.kinds,
                JobMode::Prepare {
                    embedding_cap: loaded
                        .config
                        .embedding
                        .spend
                        .as_ref()
                        .map(|spend| spend.limit_microusd / sources.len() as u64),
                    full,
                    due,
                },
            )
        })
        .collect();
    let mut failed = false;
    worker_pool(options, jobs, |index, outcome| {
        let Ok(outcome) = outcome else {
            failed = true;
            eprintln!("error: worker_failure");
            return;
        };
        if let Outcome::TimedOut(timeout) = &outcome {
            failed = true;
            eprintln!(
                "error: worker_failure: timeout after {}ms; increase --timeout",
                timeout.as_millis()
            );
            return;
        }
        failed |= outcome.failure().is_some();
        if let Some(report) = outcome
            .result_line()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok())
            .and_then(|value| prepared_report(&sources[index].id, &value))
        {
            failed |= report["failed"] == true;
            println!("{report}");
        } else {
            failed = true;
            eprintln!("error: worker_failure");
        }
    });
    if failed {
        ExitCode::from(exit::FAILURE)
    } else {
        ExitCode::SUCCESS
    }
}

// The provider shares its worker process. Treat the result channel as untrusted:
// print only configured identifiers, closed codes and parsed numeric statistics.
fn prepared_report(source: &str, input: &serde_json::Value) -> Option<serde_json::Value> {
    if input.get("kinds").is_none() {
        let error = worker::closed_code(&input["error"])?;
        return Some(
            serde_json::json!({"source_id":source,"error":error,"failed":true,"paid_calls":0}),
        );
    }
    let mut kinds = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for item in input["kinds"].as_array()? {
        let kind: SyncKind = serde_json::from_value(item["kind"].clone()).ok()?;
        if !seen.insert(kind.as_str()) {
            return None;
        }
        let value = if item.get("error").is_some() {
            serde_json::json!({"kind":kind,"error":worker::closed_code(&item["error"])?,
                "paid_calls":item["paid_calls"].as_u64().unwrap_or(0),
                "provider_calls":item["provider_calls"].as_u64().unwrap_or(0),
                "reserved_microusd":item["reserved_microusd"].as_u64().unwrap_or(0)})
        } else {
            let stats: KindStats = serde_json::from_value(item["stats"].clone()).ok()?;
            let mut value = serde_json::json!({"kind":kind,"stats":stats,"cache_hits":item["cache_hits"].as_u64()?,"cache_misses":item["cache_misses"].as_u64()?,"paid_calls":item["paid_calls"].as_u64()?,"provider_calls":item["provider_calls"].as_u64().unwrap_or(0),"reserved_microusd":item["reserved_microusd"].as_u64().unwrap_or(0)});
            if let Some(skipped) = item.get("skipped") {
                if skipped != "interval_not_due" {
                    return None;
                }
                value["skipped"] = serde_json::json!("interval_not_due");
            }
            value
        };
        kinds.push(value);
    }
    let failed = kinds.iter().any(|k| k.get("error").is_some());
    let mut report = kinds
        .iter()
        .find(|k| k["kind"] == serde_json::json!(SyncKind::Content))
        .cloned()
        .unwrap_or_else(|| default_report(ReportDefaults::Source));
    if report.get("paid_calls").is_none() {
        report["paid_calls"] = serde_json::json!(0);
    }
    report["source_id"] = serde_json::json!(source);
    report["failed"] = serde_json::json!(failed);
    report["kinds"] = serde_json::json!(kinds);
    Some(report)
}

#[cfg(test)]
mod report_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn every_closed_code_survives_reports_with_other_kind_statistics() {
        let codes: Vec<_> = worker::codes().collect();
        assert_eq!(
            codes.len(),
            codes
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
        );
        let stats = KindStats {
            permission_documents: 17,
            ..Default::default()
        };
        for code in codes {
            assert!(WriterError::parse(code).is_some(), "{code}");
            let top = prepared_report("fixture", &json!({"error":code})).unwrap();
            assert_eq!(top["error"], code);
            let report = prepared_report("fixture", &json!({"kinds":[
                {"kind":"content","error":code,"paid_calls":2,"provider_calls":3,"reserved_microusd":4},
                {"kind":"permissions","stats":stats,"cache_hits":0,"cache_misses":0,"paid_calls":0}
            ]})).unwrap();
            assert_eq!(report["failed"], true);
            assert_eq!(report["error"], code);
            assert_eq!(report["kinds"][0]["error"], code);
            assert_eq!(report["kinds"][0]["paid_calls"], 2);
            assert_eq!(report["kinds"][0]["provider_calls"], 3);
            assert_eq!(report["kinds"][0]["reserved_microusd"], 4);
            assert_eq!(report["kinds"][1]["stats"]["permission_documents"], 17);
        }
        for value in [
            json!({"error":"private_exception"}),
            json!({"error":4}),
            json!({"kinds":[{"kind":"content","error":"private_exception"}]}),
        ] {
            assert!(prepared_report("fixture", &value).is_none());
        }
    }
}
