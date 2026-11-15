//! Revision lookup and streaming callbacks for prepared provider reads.
use super::*;

impl Preparation<'_> {
    pub(super) fn callbacks<'py>(
        &self,
        py: Python<'py>,
        view: RevisionView<'_>,
    ) -> Result<(Bound<'py, PyCFunction>, Bound<'py, PyCFunction>), WriterError> {
        let callback_failure = view.failure;
        let cfg = self.config.clone();
        let d = view.dataset.clone();
        let p = view.pending;
        let lookup_rt = self.rt.clone();
        let lookup_failure = callback_failure.clone();
        let lookup_callback = move |args: &Bound<'_, pyo3::types::PyTuple>,
                                    _: Option<&Bound<'_, pyo3::types::PyDict>>|
              -> PyResult<_> {
            let (roots,): (Vec<String>,) = args.extract()?;
            if roots.len() > 256 {
                return Err(PyRuntimeError::new_err(
                    WriterError::RevisionPageInvalid.as_str(),
                ));
            }
            lookup_rt
                .block_on(lookup_revisions(&cfg, d.as_deref(), &p, &roots))
                .map(|revisions| {
                    revisions
                        .into_iter()
                        .map(|(root, (revision, _))| (root, revision))
                        .collect::<std::collections::BTreeMap<_, _>>()
                })
                .map_err(|code| {
                    *crate::lock(&lookup_failure) = Some(code);
                    PyRuntimeError::new_err(WriterError::HostCallbackFailed.as_str())
                })
        };
        // PyO3 0.28 omits this bound (RUSTSEC-2026-0177).
        let _: &dyn Sync = &lookup_callback;
        let lookup = PyCFunction::new_closure(py, Some(c"lookup_revisions"), None, lookup_callback)
            .map_err(|_| WriterError::RevisionViewInvalid)?;
        let next_failure = callback_failure.clone();
        let next_rt = self.rt.clone();
        let revisions = view.revisions;
        let next_callback = move |_: &Bound<'_, pyo3::types::PyTuple>,
                                  _: Option<&Bound<'_, pyo3::types::PyDict>>|
              -> PyResult<Vec<(String, String)>> {
            let mut stream = revisions
                .lock()
                .map_err(|_| PyRuntimeError::new_err(WriterError::RevisionViewInvalid.as_str()))?;
            loop {
                match next_rt.block_on(stream.next()) {
                    Some(Ok(rows)) if rows.is_empty() => continue,
                    Some(rows) => {
                        return rows.map_err(|code| {
                            *crate::lock(&next_failure) = Some(code);
                            PyRuntimeError::new_err(WriterError::HostCallbackFailed.as_str())
                        });
                    }
                    None => return Ok(Vec::new()),
                }
            }
        };
        let _: &dyn Sync = &next_callback;
        let next = PyCFunction::new_closure(py, Some(c"next_revisions"), None, next_callback)
            .map_err(|_| WriterError::RevisionViewInvalid)?;

        Ok((lookup, next))
    }
}
