use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SampleSkip {
    InFlight,
    ForegroundQueued,
    Preparation,
    Timeout,
    AuthorizationChanged,
    Cancelled,
    ReferenceFailed,
    EmptyReference,
}
impl SampleSkip {
    fn label(self) -> &'static str {
        match self {
            Self::InFlight => "in_flight",
            Self::ForegroundQueued => "foreground_queued",
            Self::Preparation => "preparation",
            Self::Timeout => "timeout",
            Self::AuthorizationChanged => "authorization_changed",
            Self::Cancelled => "cancelled",
            Self::ReferenceFailed => "reference_failed",
            Self::EmptyReference => "empty_reference",
        }
    }
}
pub(super) fn recall_telemetry(
    successful: u64,
    selected: u64,
    completed: u64,
    result: Result<Value, SampleSkip>,
) -> Value {
    let Value::Object(mut metrics) = json!({"recall_successful":successful,
        "recall_selected":selected, "recall_completed":completed,
        "recall_coverage":completed as f64 / successful.max(1) as f64})
    else {
        unreachable!("recall metrics are numeric objects");
    };
    let outcome = result
        .as_ref()
        .err()
        .map_or("completed", |reason| reason.label());
    crate::metrics::add(&crate::metrics::RECALL_SAMPLES, &[("outcome", outcome)], 1);
    match result {
        Ok(Value::Object(values)) => metrics.extend(values),
        Err(reason) => {
            metrics.insert(format!("recall_skip_{}", reason.label()), json!(1));
        }
        _ => unreachable!("recall metrics are numeric objects"),
    }
    let event = Value::Object(metrics);
    emit(format_args!("telemetry {event}"));
    event
}

pub(super) type SampleResult = (PreparedReader, Result<Value, SampleSkip>, Option<i64>);

/// Owned native turn and generation pin for one background recall reference.
pub(super) struct SampleWork {
    pub(super) reader: PreparedReader,
    pub(super) sample: crate::search::RecallSample,
    pub(super) pin: Result<crate::mirror::MirrorRequest>,
    pub(super) native: Option<tokio::sync::OwnedSemaphorePermit>,
    pub(super) cancelled: CancellationToken,
    pub(super) expires: Option<i64>,
}
impl SampleWork {
    pub(super) async fn run(self) -> SampleResult {
        let Self {
            mut reader,
            sample,
            pin,
            native,
            cancelled,
            expires,
        } = self;

        let _native = native;
        let result = {
            let work = async {
                let _pin = pin.map_err(|_| SampleSkip::AuthorizationChanged)?;
                reader.recall_reference(&sample).await.map(|(at_10, at_75, rows)| {
                    json!({"recall_at_10":at_10,"recall_at_75":at_75,"recall_admitted_rows":rows})
                })
            };
            tokio::select! { biased;
                () = cancelled.cancelled() =>Err(SampleSkip::Cancelled),
                result=tokio::time::timeout(Duration::from_secs(2),
                    work)=>result.unwrap_or(Err(SampleSkip::Timeout)),
            }
        }; // Drop the waiter; Lance's bounded native residue has no public join.
        (reader, result, expires)
    }
}
