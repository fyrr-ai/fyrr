use super::*;

fn generator_fixture(py: Python<'_>) -> Bound<'_, PyModule> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let name = std::ffi::CString::new(format!(
        "generator_fixture_{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
    .unwrap();
    PyModule::from_code(
        py,
        cr#"
held = []
closed = 0
cleaned = 0
class Retained:
    def __init__(self, mode):
        self.mode = mode
        self.fail = mode == "iterator-error"
        self.inner = self.stream()
        held.append(self)
    def stream(self):
        global cleaned
        try:
            if self.fail:
                raise RuntimeError("iterator_failure_value")
            if self.mode == "empty":
                return
            if self.mode == "tuple-error":
                yield 0
                return
            if self.mode == "unexpected-rows":
                yield ("not-json", object())
                return
            yield ("not-json", None)
            yield ("not-json", None)
        finally:
            cleaned += 1
    def __iter__(self):
        return self
    def __next__(self):
        return next(self.inner)
    def close(self):
        global closed
        closed += 1
        self.inner.close()
        raise RuntimeError("cleanup_failure_value")
def run(source, *args):
    return Retained(source)
"#,
        c"generator_fixture.py",
        &name,
    )
    .unwrap()
}

fn assert_generator_state(runtime: &Bound<'_, PyModule>, closed: u32, cleaned: u32) {
    assert_eq!(
        runtime.getattr("closed").unwrap().extract::<u32>().unwrap(),
        closed
    );
    assert_eq!(
        runtime
            .getattr("cleaned")
            .unwrap()
            .extract::<u32>()
            .unwrap(),
        cleaned
    );
    assert_eq!(
        runtime.getattr("held").unwrap().len().unwrap(),
        1,
        "a retained reference prevents destructor-only cleanup"
    );
}

#[test]
fn prepared_generator_closes_retained_iterators_and_preserves_errors() {
    Python::initialize();
    Python::attach(|py| {
        for error in [WriterError::LeaseInvalid, WriterError::BatchInvalid] {
            let expected = error;
            let runtime = generator_fixture(py);
            let result: Result<()> = {
                let generator =
                    ProviderGenerator::new(runtime.call_method1("run", ("fixture",)).unwrap());
                generator.value.call_method0("__next__").unwrap();
                Err(error)
            };
            assert_eq!(result.unwrap_err(), expected);
            assert_generator_state(&runtime, 1, 1);
        }
    });
}

#[test]
fn prepared_generator_success_does_not_call_close() {
    Python::initialize();
    Python::attach(|py| {
        let runtime = generator_fixture(py);
        {
            let mut generator =
                ProviderGenerator::new(runtime.call_method1("run", ("fixture",)).unwrap());
            let items = generator
                .value
                .try_iter()
                .unwrap()
                .collect::<PyResult<Vec<_>>>()
                .unwrap();
            assert_eq!(items.len(), 2);
            generator.finish();
        }
        assert_generator_state(&runtime, 0, 1);
    });
}

#[test]
fn run_kind_closes_retained_iterators_and_preserves_errors() {
    Python::initialize();
    Python::attach(|py| {
        for (mode, expected) in [
            ("invalid-batch", Diagnostic::BatchRejected),
            ("iterator-error", Diagnostic::ProviderFailure),
        ] {
            let runtime = generator_fixture(py);
            let limits = crate::batch::Limits {
                max_rows: 1,
                max_bytes: 1 << 20,
                document_max_bytes: 1 << 20,
                row_warning: None,
                row_error: None,
                retention_days: None,
            };
            let mut checker = BatchChecker::new("fixture", SyncKind::Content, limits, None);
            assert_eq!(
                run_kind(py, &runtime, mode, SyncKind::Content, &mut checker),
                Err(expected)
            );
            assert_generator_state(&runtime, 1, 1);
        }
    });
}

#[test]
fn prepared_callback_origin_keeps_host_codes_closed() {
    for code in crate::worker::codes() {
        assert!(WriterError::parse(code).is_some(), "{code}");
    }
    for (text, expected) in [
        ("sync_lease_lost", WriterError::BatchRejected),
        ("storage_unavailable", WriterError::StorageUnavailable),
        ("private-exception", WriterError::BatchRejected),
        ("host_callback_failed", WriterError::BatchRejected),
    ] {
        assert_eq!(WriterError::from_provider(text.to_owned()), expected);
    }
    assert_eq!(
        WriterError::parse("sync_lease_lost"),
        Some(WriterError::SyncLeaseLost)
    );
    assert!(WriterError::parse("private-exception").is_none());
}

#[test]
fn provider_driver_preserves_row_and_error_policies() {
    use super::driver::{ErrMap, RowPolicy, drive_provider};
    Python::initialize();
    Python::attach(|py| {
        let checker = |kind| {
            BatchChecker::new(
                "fixture",
                kind,
                crate::batch::Limits {
                    max_rows: 1,
                    max_bytes: 1 << 20,
                    document_max_bytes: 1 << 20,
                    row_warning: None,
                    row_error: None,
                    retention_days: None,
                },
                None,
            )
        };
        for (policy, errors, mode, expected) in [
            (
                RowPolicy::Forbidden,
                ErrMap::NON_CONTENT,
                "tuple-error",
                WriterError::BatchInvalid,
            ),
            (
                RowPolicy::Required,
                ErrMap::CONTENT,
                "tuple-error",
                WriterError::ProviderFailure,
            ),
            (
                RowPolicy::Forbidden,
                ErrMap::NON_CONTENT,
                "unexpected-rows",
                WriterError::BatchInvalid,
            ),
            (
                RowPolicy::Required,
                ErrMap::CONTENT,
                "unexpected-rows",
                WriterError::BatchInvalid,
            ),
            (
                RowPolicy::Forbidden,
                ErrMap::NON_CONTENT,
                "invalid-batch",
                WriterError::BatchRejected,
            ),
            (
                RowPolicy::Required,
                ErrMap::CONTENT,
                "invalid-batch",
                WriterError::BatchInvalid,
            ),
            (
                RowPolicy::Forbidden,
                ErrMap::NON_CONTENT,
                "iterator-error",
                WriterError::ProviderFailure,
            ),
            (
                RowPolicy::Required,
                ErrMap::CONTENT,
                "iterator-error",
                WriterError::ProviderFailure,
            ),
        ] {
            let runtime = generator_fixture(py);
            let kind = if matches!(policy, RowPolicy::Required) {
                SyncKind::Content
            } else {
                SyncKind::Permissions
            };
            let mut checker = checker(kind);
            let mut delivered = 0;
            let result = drive_provider(
                &runtime,
                (&mut checker, (mode, kind.as_str(), None::<String>)),
                policy,
                errors,
                |_, _, _| {
                    delivered += 1;
                    Ok(())
                },
            );
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("invalid provider input was accepted"),
            };
            assert_eq!(error, expected, "{mode}");
            assert_eq!(delivered, 0, "invalid batches must never reach staging");
            assert_generator_state(&runtime, 1, 1);
        }
        for mode in ["tuple-error", "unexpected-rows"] {
            let runtime = generator_fixture(py);
            let mut checker = checker(SyncKind::Content);
            assert_eq!(
                run_kind(py, &runtime, mode, SyncKind::Content, &mut checker),
                Err(Diagnostic::BatchRejected)
            );
            assert_generator_state(&runtime, 1, 1);
        }
        let runtime = generator_fixture(py);
        let mut checker = checker(SyncKind::Content);
        assert_eq!(
            run_kind(py, &runtime, "empty", SyncKind::Content, &mut checker),
            Ok(())
        );
        assert_generator_state(&runtime, 0, 1);
    });
}
