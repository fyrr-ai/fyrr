//! Bounded operator diagnostics; storage payloads never enter log output.
use object_store::Error;

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
