//! Revision lookup and streaming callbacks for prepared provider reads.
use super::*;
use crate::python::host::Revisions;

/// The most roots one `lookup_revisions` may ask for.
const MAX_ROOTS: usize = 256;

/// Answers the provider's two callbacks from the namespace's published
/// content and the pending envelopes. A storage failure, or a request the worker
/// refuses (more than [`MAX_ROOTS`] roots), fails the call and is kept: the
/// provider may catch the error and finish, and the read still cannot be
/// attested complete afterwards.
pub(super) struct RevisionCallbacks<'a> {
    config: &'a fyrr_core::config::Config,
    rt: &'a Arc<tokio::runtime::Runtime>,
    dataset: Option<Arc<lance::Dataset>>,
    pending: Arc<PendingRevisions>,
    revisions: RevisionStream,
    failure: &'a revisions::RevisionFailure,
}

impl<'a> RevisionCallbacks<'a> {
    pub(super) fn new(
        config: &'a fyrr_core::config::Config,
        rt: &'a Arc<tokio::runtime::Runtime>,
        view: RevisionView<'a>,
    ) -> Self {
        Self {
            config,
            rt,
            dataset: view.dataset.clone(),
            pending: view.pending,
            revisions: view.revisions,
            failure: view.failure,
        }
    }
}

impl Revisions for RevisionCallbacks<'_> {
    fn lookup(&mut self, roots: &[String]) -> Option<BTreeMap<String, String>> {
        if roots.len() > MAX_ROOTS {
            *crate::lock(self.failure) = Some(WriterError::HostCallbackFailed);
            return None;
        }
        match self.rt.block_on(lookup_revisions(
            self.config,
            self.dataset.as_deref(),
            &self.pending,
            roots,
        )) {
            Ok(revisions) => Some(
                revisions
                    .into_iter()
                    .map(|(root, (revision, _))| (root, revision))
                    .collect(),
            ),
            Err(code) => {
                *crate::lock(self.failure) = Some(code);
                None
            }
        }
    }

    fn next(&mut self) -> Option<Vec<(String, String)>> {
        loop {
            match self.rt.block_on(self.revisions.next()) {
                Some(Ok(rows)) if rows.is_empty() => {}
                Some(Ok(rows)) => return Some(rows),
                Some(Err(code)) => {
                    *crate::lock(self.failure) = Some(code);
                    return None;
                }
                None => return Some(Vec::new()),
            }
        }
    }
}
