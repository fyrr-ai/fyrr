//! One provider boundary with explicit row and diagnostic policies.
use super::host::{Bounds, Failure, Host, Revisions, Rows, Run};
use crate::batch::{BatchChecker, KindStats};
use crate::worker::Diagnostic;
use crate::{Result, WriterError};
use arrow_array::RecordBatch;
use fyrr_core::connector::SyncKind;

#[derive(Clone, Copy)]
pub(super) enum RowPolicy {
    Optional,
    Forbidden,
    Required,
}

pub(super) struct ErrMap<E> {
    provider: E,
    tuple: E,
    batch: E,
    validation: fn(String) -> E,
}

impl<E: Copy> ErrMap<E> {
    fn of(&self, failure: Failure) -> E {
        match failure {
            Failure::Provider => self.provider,
            Failure::Tuple => self.tuple,
            Failure::Batch => self.batch,
        }
    }
}

impl ErrMap<Diagnostic> {
    pub(super) const DRY_RUN: Self = Self {
        provider: Diagnostic::ProviderFailure,
        tuple: Diagnostic::BatchRejected,
        batch: Diagnostic::BatchRejected,
        validation: |_| Diagnostic::BatchRejected,
    };
}

impl ErrMap<WriterError> {
    pub(super) const NON_CONTENT: Self = Self {
        provider: WriterError::ProviderFailure,
        tuple: WriterError::BatchInvalid,
        batch: WriterError::BatchInvalid,
        validation: WriterError::from_provider,
    };
    pub(super) const CONTENT: Self = Self {
        tuple: WriterError::ProviderFailure,
        ..Self::NON_CONTENT
    };
}

/// What to run: one sync kind of one source, from `cursor`.
pub(super) struct Request<'a> {
    pub(super) source_json: &'a str,
    pub(super) kind: SyncKind,
    pub(super) cursor: Option<&'a str>,
}

/// Runs the request in `host` and passes every checked batch to `on_batch`.
/// Returns the open run: the caller marks it finished only after it has
/// validated the run's completion, otherwise dropping it closes the
/// provider's generator.
pub(super) fn drive_provider<'h, E: Copy>(
    host: &'h mut Host,
    request: &Request<'_>,
    mut revisions: Option<&mut dyn Revisions>,
    checker: &mut BatchChecker,
    policy: RowPolicy,
    errors: ErrMap<E>,
    mut on_batch: impl FnMut(&str, Option<&RecordBatch>, &mut KindStats) -> Result<(), E>,
) -> Result<Run<'h>, E> {
    let mut run = host
        .run(
            request.source_json,
            request.kind,
            request.cursor,
            revisions.is_some(),
        )
        .map_err(|failure| errors.of(failure))?;
    // Every frame and the declared and decoded rows are bounded before they
    // are used, whatever the row policy: a header is as large as the envelope.
    let bounds = Bounds {
        max_bytes: checker.limits.max_bytes,
        rows: match policy {
            RowPolicy::Forbidden => Rows::Forbidden,
            RowPolicy::Optional => Rows::AtMost(checker.limits.max_rows),
            RowPolicy::Required => Rows::Any,
        },
    };
    while let Some(item) = run
        .next(
            bounds,
            revisions.as_deref_mut().map(|r| r as &mut dyn Revisions),
        )
        .map_err(|failure| errors.of(failure))?
    {
        let rows = item.rows;
        if matches!(policy, RowPolicy::Required) && rows.is_none() {
            return Err(errors.batch);
        }
        match policy {
            RowPolicy::Required => checker.check_prepared(&item.header, rows.as_ref()),
            RowPolicy::Optional | RowPolicy::Forbidden => {
                checker.check(&item.header, rows.as_ref())
            }
        }
        .map_err(errors.validation)?;
        on_batch(&item.header, rows.as_ref(), &mut checker.stats)?;
    }
    // The caller still has to validate completion before marking it successful.
    Ok(run)
}
