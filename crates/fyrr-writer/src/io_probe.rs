//! Single-runtime-thread diagnostics; component timings are not a service-wide stall claim.
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[allow(clippy::print_stdout)]
pub(crate) async fn measure<F: Future>(label: &str, operation: F) -> F::Output {
    let stop = Arc::new(AtomicBool::new(false));
    let gap = Arc::new(AtomicU64::new(0));
    let stopping = stop.clone();
    let observed = gap.clone();
    let sampler = tokio::spawn(async move {
        let mut previous = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let now = Instant::now();
            observed.fetch_max(
                u64::try_from(now.duration_since(previous).as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            previous = now;
            if stopping.load(Ordering::Relaxed) {
                break;
            }
        }
    });
    tokio::task::yield_now().await;
    let start = Instant::now();
    let result = operation.await;
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    sampler.await.unwrap();
    println!(
        "io-probe: label={label}, elapsed_us={}, max_schedule_gap_us={}",
        elapsed.as_micros(),
        gap.load(Ordering::Relaxed)
    );
    result
}
