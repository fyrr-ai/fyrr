//! Bounded operator diagnostics; storage payloads never enter log output.
use fyrr_reader::metrics::Metric;
use object_store::Error;

pub(crate) static COMMIT_ROUNDS: Metric = Metric::counter(
    "fyrr_writer_commit_rounds_total",
    "Commit rounds by outcome: published, idle, busy or failed.",
);
pub(crate) static COMMIT_DURATION: Metric = Metric::histogram(
    "fyrr_writer_commit_duration_seconds",
    "Duration of the commit rounds that published at least one group.",
);
pub(crate) static PENDING_ENVELOPES: Metric = Metric::gauge(
    "fyrr_writer_pending_envelopes",
    "Envelopes waiting to be committed, summed over the namespaces this writer committed, each as of its last read.",
);
pub(crate) static PUBLICATIONS: Metric = Metric::counter(
    "fyrr_writer_publications_total",
    "Publications of a new head: commit groups and maintenance.",
);
pub(crate) static MAINTENANCE_STEPS: Metric = Metric::counter(
    "fyrr_writer_maintenance_steps_total",
    "Maintenance steps done, by step: compaction or index_merge.",
);
pub(crate) static REJECTED_ENVELOPES: Metric = Metric::counter(
    "fyrr_writer_rejected_envelopes_total",
    "Envelopes the committer rejected.",
);

/// Records the pending count a read of `namespace` found and exports the sum
/// over namespaces; the namespace itself is never a label.
pub(crate) fn pending_envelopes(namespace: &str, count: usize) {
    static PENDING: std::sync::Mutex<std::collections::BTreeMap<String, i64>> =
        std::sync::Mutex::new(std::collections::BTreeMap::new());
    let mut counts = PENDING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if count == 0 {
        counts.remove(namespace);
    } else {
        counts.insert(
            namespace.to_owned(),
            i64::try_from(count).unwrap_or(i64::MAX),
        );
    }
    let total = counts
        .values()
        .fold(0, |sum: i64, n| sum.saturating_add(*n));
    fyrr_reader::metrics::set(&PENDING_ENVELOPES, &[], total);
}

pub(crate) enum HeadWriteStage {
    Publish,
    Renew,
    Release,
}

#[allow(clippy::print_stderr)]
pub(crate) fn head_write_failed(stage: HeadWriteStage, error: &Error) {
    let stage = match stage {
        HeadWriteStage::Publish => "publish",
        HeadWriteStage::Renew => "renew",
        HeadWriteStage::Release => "release",
    };
    let category = match error {
        Error::Precondition { .. } => "precondition",
        Error::AlreadyExists { .. } => "already_exists",
        Error::NotFound { .. } => "not_found",
        Error::PermissionDenied { .. } => "permission_denied",
        Error::Unauthenticated { .. } => "unauthenticated",
        _ => "storage_unavailable",
    };
    eprintln!("telemetry head_write=failed stage={stage} category={category}");
}
