//! `writer.threads`: one bound for every CPU pool of a writer process.
//!
//! Each pool reads its size once from an environment variable, so the bound
//! is applied by setting those variables before the first thread exists.
//! Worker subprocesses inherit them and apply the same bound again.

/// Environment settings that bound the pools for `threads` CPU threads, and
/// the full-text build workers for the same bound.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Pools {
    pub(crate) env: [(&'static str, String); 4],
    pub(crate) fts_workers: usize,
}

/// Full-text workers tokenize and hold posting lists in memory (Lance sizes
/// each at 2 GiB by default), so they take half the threads, as Lance does.
pub(crate) fn pools(threads: usize) -> Pools {
    let threads = threads.max(1);
    let fts_workers = (threads / 2).max(1);
    Pools {
        env: [
            // The writer's Tokio runtimes (`Runtime::new`).
            ("TOKIO_WORKER_THREADS", threads.to_string()),
            // Lance's compute runtime, read once per process.
            ("LANCE_CPU_THREADS", threads.to_string()),
            // The rayon global pool (k-means, quantization), which
            // `LANCE_CPU_THREADS` does not size.
            ("RAYON_NUM_THREADS", threads.to_string()),
            // Full-text builds that take no explicit worker count, such as
            // index optimization.
            ("LANCE_FTS_NUM_SHARDS", fts_workers.to_string()),
        ],
        fts_workers,
    }
}

/// Bounds this process's pools; absent `threads` changes nothing.
///
/// Call it before the first Tokio runtime, Lance call or other thread of the
/// process.
#[allow(unsafe_code)]
pub(crate) fn apply(threads: Option<usize>) {
    let Some(threads) = threads else { return };
    for (name, value) in pools(threads).env {
        // SAFETY: the process is still single-threaded here (the callers run
        // before any runtime or worker thread starts), so no other thread
        // can read the environment concurrently.
        unsafe { std::env::set_var(name, value) };
    }
}

#[cfg(test)]
mod tests {
    use super::pools;

    #[test]
    fn threads_size_every_pool_and_the_full_text_workers() {
        let pools = pools(8);
        assert_eq!(
            pools.env,
            [
                ("TOKIO_WORKER_THREADS", "8".to_owned()),
                ("LANCE_CPU_THREADS", "8".to_owned()),
                ("RAYON_NUM_THREADS", "8".to_owned()),
                ("LANCE_FTS_NUM_SHARDS", "4".to_owned()),
            ]
        );
        assert_eq!(pools.fts_workers, 4);
        assert_eq!(super::pools(1).fts_workers, 1);
        assert_eq!(super::pools(3).fts_workers, 1);
    }
}
