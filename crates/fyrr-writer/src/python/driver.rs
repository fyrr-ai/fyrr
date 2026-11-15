//! One provider boundary with explicit row and diagnostic policies.
use super::ProviderGenerator;
use crate::batch::{BatchChecker, KindStats};
use crate::worker::Diagnostic;
use crate::{Result, WriterError};
use arrow_array::RecordBatch;
use arrow_pyarrow::FromPyArrow;
use pyo3::call::PyCallArgs;
use pyo3::prelude::*;
use pyo3::types::PyModule;

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

pub(super) fn drive_provider<'py, E: Copy>(
    runtime: &Bound<'py, PyModule>,
    (checker, args): (&mut BatchChecker, impl PyCallArgs<'py>),
    policy: RowPolicy,
    errors: ErrMap<E>,
    mut on_batch: impl FnMut(&str, Option<&RecordBatch>, &mut KindStats) -> Result<(), E>,
) -> Result<ProviderGenerator<'py>, E> {
    let value = runtime
        .call_method1("run", args)
        .map_err(|_| errors.provider)?;
    let generator = ProviderGenerator::new(value);
    for item in generator.value.try_iter().map_err(|_| errors.provider)? {
        let (header, rows): (String, Bound<'_, PyAny>) = item
            .map_err(|_| errors.provider)?
            .extract()
            .map_err(|_| errors.tuple)?;
        let rows = match (policy, rows.is_none()) {
            (RowPolicy::Forbidden | RowPolicy::Optional, true) => None,
            (RowPolicy::Forbidden, false) => return Err(errors.batch),
            _ => {
                let limits = checker.limits;
                // Slices retain backing buffers. Measure before the C Data import.
                let over_rows = matches!(policy, RowPolicy::Optional)
                    && rows
                        .getattr("num_rows")
                        .and_then(|v| v.extract::<u64>())
                        .map_err(|_| errors.batch)?
                        > limits.max_rows;
                let bytes = rows
                    .call_method0("get_total_buffer_size")
                    .and_then(|v| v.extract::<u64>())
                    .map_err(|_| errors.batch)?;
                if over_rows || bytes > limits.max_bytes {
                    return Err(errors.batch);
                }
                Some(RecordBatch::from_pyarrow_bound(&rows).map_err(|_| errors.batch)?)
            }
        };
        match policy {
            RowPolicy::Required => checker.check_prepared(&header, rows.as_ref()),
            RowPolicy::Optional | RowPolicy::Forbidden => checker.check(&header, rows.as_ref()),
        }
        .map_err(errors.validation)?;
        on_batch(&header, rows.as_ref(), &mut checker.stats)?;
    }
    // The caller still has to validate completion before marking it successful.
    Ok(generator)
}
