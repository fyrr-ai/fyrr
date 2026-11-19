//! Writer settings that Lance reads from the environment once per process:
//! `writer.threads` bounds every CPU pool, `writer.fts_partition_memory_mb`
//! sizes full-text index partitions.
//!
//! Each is applied by setting its variables before the first thread exists
//! (the only writer of the environment is
//! [`fyrr_reader::process_env::set_at_startup`]). Worker subprocesses inherit
//! them and apply the same settings again.

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

/// Lance's full-text partition size, in MiB of builder memory per worker. A
/// build flushes a partition at this size and an index merge repacks to it, so
/// one value keeps the geometry through both; Lance's `memory_limit_mb`
/// parameter is a total split over the workers it ends up with, which cannot
/// be matched by a merge.
pub(crate) fn fts_partition_env(megabytes: u32) -> (&'static str, String) {
    ("LANCE_FTS_PARTITION_SIZE", megabytes.to_string())
}

/// Applies the process-level writer settings; absent settings change nothing.
///
/// Call it before the first Tokio runtime, Lance call or other thread of the
/// process.
pub(crate) fn apply(writer: &fyrr_core::config::WriterConfig) {
    let threads = writer
        .threads
        .into_iter()
        .flat_map(|threads| pools(threads).env);
    let partition = writer.fts_partition_memory_mb.map(fts_partition_env);
    fyrr_reader::process_env::set_at_startup(threads.chain(partition));
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

    #[test]
    fn partition_target_sets_lances_per_worker_partition_size() {
        assert_eq!(
            super::fts_partition_env(256),
            ("LANCE_FTS_PARTITION_SIZE", "256".to_owned())
        );
    }
}
