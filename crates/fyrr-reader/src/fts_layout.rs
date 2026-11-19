//! How many full-text partitions a query can score at once.
//!
//! Lance scores the partitions of one index segment in chunks of
//! `LANCE_FTS_SEARCH_CHUNK` (default 16): the partitions of a chunk are scored
//! one after another in one compute task, so a segment of several partitions
//! uses several threads only when the chunk is smaller than its partition
//! count. Lance reads the variable once per process.
//! `reader.fts_search_chunk` sets it before the first Lance call.

use fyrr_core::config::ReaderConfig;
use lance::{Dataset, index::DatasetIndexInternalExt};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::inverted::InvertedIndex;
use lance_table::format::IndexMetadata;

const CHUNK_VARIABLE: &str = "LANCE_FTS_SEARCH_CHUNK";
/// Lance's chunk when the variable is unset or invalid.
const LANCE_DEFAULT_CHUNK: usize = 16;

/// Applies `reader.fts_search_chunk` to the environment.
///
/// Absent leaves the environment, and so Lance's default or an operator's own
/// export, untouched. Call it from the start-up path only (see
/// [`crate::process_env::set_at_startup`]).
pub(crate) fn apply(reader: &ReaderConfig) {
    if let Some(chunk) = reader.fts_search_chunk {
        crate::process_env::set_at_startup([(CHUNK_VARIABLE, chunk.to_string())]);
    }
}

/// The chunk Lance uses in this process, with its own parsing rules.
pub(crate) fn effective_chunk() -> usize {
    chunk_from(std::env::var(CHUNK_VARIABLE).ok().as_deref())
}

fn chunk_from(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse().ok())
        .filter(|&value| value >= 1)
        .unwrap_or(LANCE_DEFAULT_CHUNK)
}

/// Partition counts of the full-text index segments of one generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FtsLayout {
    pub(crate) segments: usize,
    pub(crate) partitions_min: usize,
    pub(crate) partitions_max: usize,
}

impl FtsLayout {
    fn from_counts(counts: &[usize]) -> Option<Self> {
        Some(Self {
            segments: counts.len(),
            partitions_min: *counts.iter().min()?,
            partitions_max: *counts.iter().max()?,
        })
    }

    /// Compute tasks the widest segment spreads one query over.
    pub(crate) fn parallel_tasks(&self, chunk: usize, compute_threads: usize) -> usize {
        self.partitions_max
            .div_ceil(chunk)
            .min(compute_threads)
            .max(1)
    }

    /// One bounded line: counts and settings only, never content or queries.
    pub(crate) fn report(&self, chunk: usize, compute_threads: usize) -> String {
        let tasks = self.parallel_tasks(chunk, compute_threads);
        format!(
            "telemetry fts_search=layout segments={} partitions_min={} partitions_max={} \
                search_chunk={chunk} compute_threads={compute_threads} parallel_tasks={tasks} \
                concurrent={}",
            self.segments,
            self.partitions_min,
            self.partitions_max,
            if tasks > 1 { "yes" } else { "no" }
        )
    }
}

/// Reads the partition count of every full-text segment; `None` when the
/// generation has none or one cannot be opened (the line is a diagnostic and
/// never fails an open).
pub(crate) async fn read(
    dataset: &Dataset,
    indexes: &[IndexMetadata],
    index_name: &str,
    column: &str,
) -> Option<FtsLayout> {
    let mut counts = Vec::new();
    for index in indexes.iter().filter(|index| index.name == index_name) {
        let opened = dataset
            .open_generic_index(column, &index.uuid, &NoOpMetricsCollector)
            .await
            .ok()?;
        counts.push(
            opened
                .as_any()
                .downcast_ref::<InvertedIndex>()?
                .partition_count(),
        );
    }
    FtsLayout::from_counts(&counts)
}

/// Emits the layout line for a generation about to serve.
pub(crate) async fn report(dataset: &Dataset, indexes: &[IndexMetadata]) {
    let Some(layout) = read(
        dataset,
        indexes,
        fyrr_core::layout::NamespaceLayout::FTS_INDEX,
        "searchable_text",
    )
    .await
    else {
        return;
    };
    let compute_threads = lance_core::utils::tokio::get_num_compute_intensive_cpus();
    crate::telemetry::emit(format_args!(
        "{}",
        layout.report(effective_chunk(), compute_threads)
    ));
}

#[cfg(test)]
mod tests {
    use super::{FtsLayout, chunk_from};

    const APPLY_CHILD: &str = "fts_layout::tests::apply_child_process";

    /// Start-up path of `fyrr reader`: the setting reaches the environment
    /// Lance reads, and an absent setting leaves it alone.
    #[test]
    #[ignore = "child process of the apply test"]
    fn apply_child_process() {
        let mut config = fyrr_core::config::parse(
            "[namespace]\nname = \"acme\"\n[storage]\nuri = \"s3://fyrr-data/acme\"\n\
             [identity]\nhashing_key = { env = \"FYRR_IDENTITY_KEY\" }\n\
             [embedding]\nendpoint = \"https://embeddings.example/v1\"\n",
        )
        .unwrap()
        .config;
        let before = std::env::var(super::CHUNK_VARIABLE).ok();
        super::apply(&config.reader);
        assert_eq!(std::env::var(super::CHUNK_VARIABLE).ok(), before);
        config.reader.fts_search_chunk = Some(2);
        super::apply(&config.reader);
        assert_eq!(super::effective_chunk(), 2);
    }

    #[test]
    fn apply_sets_the_variable_lance_reads_and_absent_leaves_it() {
        for exported in [None, Some("7")] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command.args([APPLY_CHILD, "--exact", "--ignored", "--test-threads=1"]);
            match exported {
                Some(value) => command.env(super::CHUNK_VARIABLE, value),
                None => command.env_remove(super::CHUNK_VARIABLE),
            };
            let output = command.output().unwrap();
            let log = String::from_utf8_lossy(&output.stdout);
            assert!(output.status.success(), "{log}");
            assert!(log.contains("1 passed"), "the child test ran: {log}");
        }
    }

    #[test]
    fn chunk_follows_lance_parsing() {
        assert_eq!(chunk_from(None), 16);
        assert_eq!(chunk_from(Some("1")), 1);
        assert_eq!(chunk_from(Some("4")), 4);
        assert_eq!(chunk_from(Some("0")), 16);
        assert_eq!(chunk_from(Some("many")), 16);
    }

    #[test]
    fn layout_reports_whether_one_query_can_run_in_parallel() {
        let layout = FtsLayout::from_counts(&[4, 1]).unwrap();
        assert_eq!((layout.segments, layout.partitions_min), (2, 1));
        assert_eq!(layout.parallel_tasks(16, 6), 1);
        assert_eq!(layout.parallel_tasks(1, 6), 4);
        assert_eq!(layout.parallel_tasks(2, 6), 2);
        assert_eq!(layout.parallel_tasks(1, 1), 1);
        assert!(
            layout
                .report(1, 6)
                .ends_with("parallel_tasks=4 concurrent=yes")
        );
        assert!(
            layout
                .report(16, 6)
                .ends_with("parallel_tasks=1 concurrent=no")
        );
        assert_eq!(FtsLayout::from_counts(&[]), None);
    }
}
