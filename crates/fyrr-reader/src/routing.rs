//! Generation-owned native RaBitQ centroid scan; row admission stays in Lance.
use arrow_array::{
    Array, FixedSizeListArray, Float16Array, Float32Array, RecordBatch, UInt32Array, UInt64Array,
    types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use fyrr_core::config::{DenseMetric, RoutingStrategy};
pub(crate) use fyrr_core::mcp::RoutingFallback;
use lance_index::vector::{
    CENTROID_DIST_COLUMN, PART_ID_COLUMN,
    bq::{
        RQRotationType,
        builder::RabitQuantizer,
        storage::{RABIT_CODE_COLUMN, RabitQuantizationStorage},
        transform::{
            ADD_FACTORS_COLUMN, ERROR_FACTORS_COLUMN, RQTransformer, SCALE_FACTORS_COLUMN,
        },
    },
    graph::OrderedFloat,
    quantizer::QuantizerStorage,
    storage::{DistCalculator, VectorStore},
    transform::Transformer,
};
use lance_linalg::distance::{DistanceType, l2::l2_distance};
use std::sync::Arc;

const AUTO_CENTROIDS: usize = 8192; // Unmeasured until scale calibration picks it.
const BUILD_SCRATCH: usize = 24 * 1024 * 1024;

/// Takes `(partition, distance)` pairs in `ranked` order, skipping partitions
/// without an admitted row, until at least `nprobes` are taken and they hold
/// at least `target` admitted rows, or the ranking ends. An unrestricted
/// subject stops at `nprobes` when its partitions are of average size; a
/// sparse subject probes further, so that it inspects about as many of its
/// own rows as an unrestricted query inspects rows. Returns the taken
/// partitions, their distances and how many entries of `ranked` were consumed.
pub(crate) fn take_probes(
    ranked: impl IntoIterator<Item = (u32, f32)>,
    admitted: &[u32],
    nprobes: usize,
    target: u64,
) -> (Vec<u32>, Vec<f32>, usize) {
    let (mut ids, mut distances, mut seen, mut consumed) = (Vec::new(), Vec::new(), 0u64, 0);
    for (partition, distance) in ranked {
        if ids.len() >= nprobes && seen >= target {
            break;
        }
        consumed += 1;
        let rows = admitted.get(partition as usize).copied().unwrap_or(0);
        if rows == 0 {
            continue;
        }
        ids.push(partition);
        distances.push(distance);
        seen += u64::from(rows);
    }
    (ids, distances, consumed)
}

/// How the partitions of a query are chosen. The centroid scan ranks by
/// Euclidean distance, which is the order of the normalized query under cosine
/// and of squared L2. The order under a dot product is another one, so Lance
/// ranks those partitions itself, whatever is configured.
pub(crate) fn strategy(
    configured: RoutingStrategy,
    centroids: usize,
    metric: DenseMetric,
) -> RoutingStrategy {
    if metric == DenseMetric::Dot {
        return RoutingStrategy::Exact;
    }
    match configured {
        RoutingStrategy::Auto if centroids >= AUTO_CENTROIDS => RoutingStrategy::Rabitq,
        RoutingStrategy::Auto => RoutingStrategy::Exact,
        other => other,
    }
}

#[derive(Debug)]
pub(crate) struct CentroidRouter {
    centroids: FixedSizeListArray,
    chunks: Vec<RabitQuantizationStorage>,
    chunk_rows: usize,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl CentroidRouter {
    pub(crate) fn build(
        centroids: &FixedSizeListArray,
        pool: Arc<tokio::sync::Semaphore>,
        warm_bytes: u64,
    ) -> Result<Self, RoutingFallback> {
        let n = centroids.len();
        let dim = centroids.value_length() as usize;
        if n == 0 || dim == 0 || dim > 8192 || !dim.is_multiple_of(8) {
            return Err(RoutingFallback::BuildFailed);
        }
        // Three f32 chunk matrices cover native rotation/factor temporaries;
        // 1024 dimensions admits 2048 rows, rather than many small clones.
        let chunk_rows = (BUILD_SCRATCH / (12 * dim)).clamp(1, 2048).min(n);
        let chunks_count = n.div_ceil(chunk_rows);
        let conversion = if centroids.value_type() == DataType::Float32 {
            0
        } else {
            4 * n * dim
        };
        // Codes + three native f32 factors + row IDs, plus native metadata,
        // Arrow capacities/fields per chunk. Scratch covers code packing too.
        let residency =
            conversion + n * (dim.div_ceil(8) + 20) + chunks_count * (4096 + 4 * dim.div_ceil(8));
        let scratch = 12 * chunk_rows * dim + chunk_rows * (dim.div_ceil(8) * 2 + 64) + 8 * dim;
        let page_bytes = usize::try_from(crate::generation::PAGE_BYTES)
            .map_err(|_| RoutingFallback::MemoryBudget)?;
        let permits = (residency + scratch).div_ceil(page_bytes);
        // Like the native cache, leave one full next-generation warm claim free.
        // Acquire both claims atomically so another pool user cannot race a check;
        // return the reserve immediately, retaining only paid build allocations.
        let reserve = warm_bytes.div_ceil(crate::generation::PAGE_BYTES);
        let mut permit = pool
            .try_acquire_many_owned(
                u32::try_from((permits as u64).saturating_add(reserve))
                    .map_err(|_| RoutingFallback::MemoryBudget)?,
            )
            .map_err(|_| RoutingFallback::MemoryBudget)?;
        drop(permit.split(usize::try_from(reserve).map_err(|_| RoutingFallback::MemoryBudget)?));
        let centroids = Self::float_centroids(centroids, dim)?;
        let zero = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            i32::try_from(dim).map_err(|_| RoutingFallback::BuildFailed)?,
            Arc::new(Float32Array::from(vec![0.0; dim])),
            None,
        );
        let rq = RabitQuantizer::new_with_rotation::<Float32Type>(
            1,
            i32::try_from(dim).map_err(|_| RoutingFallback::BuildFailed)?,
            RQRotationType::Fast,
        );
        let metadata = rq.metadata_ref().clone();
        let transformer = RQTransformer::new(rq, DistanceType::L2, zero, "centroid")
            .map_err(|_| RoutingFallback::BuildFailed)?;
        let mut chunks = Vec::with_capacity(chunks_count);
        let zeros = vec![0.0; dim];
        for start in (0..n).step_by(chunk_rows) {
            let count = chunk_rows.min(n - start);
            let slice = centroids.slice(start, count);
            let matrix = slice
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or(RoutingFallback::BuildFailed)?;
            let arrays: Vec<Arc<dyn Array>> = vec![
                Arc::new(slice.clone()),
                Arc::new(UInt64Array::from_iter_values(
                    (start..start + count).map(|i| i as u64),
                )),
                Arc::new(UInt32Array::from(vec![0; count])),
                Arc::new(Float32Array::from_iter_values(
                    matrix
                        .values()
                        .chunks_exact(dim)
                        .map(|v| l2_distance(v, &zeros)),
                )),
            ];
            let schema = Arc::new(Schema::new(
                ["centroid", "_rowid", PART_ID_COLUMN, CENTROID_DIST_COLUMN]
                    .into_iter()
                    .zip(&arrays)
                    .map(|(name, a)| Field::new(name, a.data_type().clone(), false))
                    .collect::<Vec<_>>(),
            ));
            let batch = transformer
                .transform(
                    &RecordBatch::try_new(schema, arrays)
                        .map_err(|_| RoutingFallback::BuildFailed)?,
                )
                .map_err(|_| RoutingFallback::BuildFailed)?;
            let columns = [
                "_rowid",
                RABIT_CODE_COLUMN,
                ADD_FACTORS_COLUMN,
                SCALE_FACTORS_COLUMN,
                ERROR_FACTORS_COLUMN,
            ]
            .map(|name| batch.schema().index_of(name))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| RoutingFallback::BuildFailed)?;
            let batch = batch
                .project(&columns)
                .map_err(|_| RoutingFallback::BuildFailed)?;
            chunks.push(
                RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None)
                    .map_err(|_| RoutingFallback::BuildFailed)?,
            );
        }
        // Check unique storage capacities before returning build scratch permits.
        // Shared native f32 centroids are already paid for by the root claim.
        let actual = chunks
            .iter()
            .map(|chunk| {
                chunk.to_batches().map(|batches| {
                    batches
                        .map(|batch| batch.get_array_memory_size())
                        .sum::<usize>()
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| RoutingFallback::BuildFailed)?
            .into_iter()
            .sum::<usize>()
            + conversion
            + chunks_count * (2048 + 4 * dim.div_ceil(8));
        if actual > residency {
            return Err(RoutingFallback::BuildFailed);
        }
        let release = permit.num_permits() - actual.div_ceil(page_bytes);
        drop(permit.split(release));
        Ok(Self {
            centroids,
            chunks,
            chunk_rows,
            _permit: permit,
        })
    }

    fn float_centroids(
        centroids: &FixedSizeListArray,
        dim: usize,
    ) -> Result<FixedSizeListArray, RoutingFallback> {
        let values = match centroids.value_type() {
            DataType::Float32 => centroids.values().clone(),
            DataType::Float16 => Arc::new(Float32Array::from_iter_values(
                centroids
                    .values()
                    .as_any()
                    .downcast_ref::<Float16Array>()
                    .ok_or(RoutingFallback::BuildFailed)?
                    .values()
                    .iter()
                    .map(|v| v.to_f32()),
            )),
            _ => return Err(RoutingFallback::BuildFailed),
        };
        let values_f32 = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or(RoutingFallback::BuildFailed)?;
        if values_f32.values().iter().any(|v| !v.is_finite()) {
            return Err(RoutingFallback::BuildFailed);
        }
        Ok(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            i32::try_from(dim).map_err(|_| RoutingFallback::BuildFailed)?,
            values,
            None,
        ))
    }

    pub(crate) fn select(
        &self,
        query: Arc<Float32Array>,
        admitted: &[u32],
        nprobes: usize,
        target: u64,
    ) -> Result<(Vec<u32>, Vec<f32>), RoutingFallback> {
        if admitted.iter().filter(|v| **v > 0).count() < 4 * nprobes {
            return Err(RoutingFallback::EligibleShortlist);
        }
        // D <= 8192: LUT/rotation/key <= 24D bytes; the approximate ranking
        // holds 8 bytes per admitted partition. No query permits.
        let mut approximate = Vec::new();
        let norm = query.values().iter().map(|v| v * v).sum();
        for (chunk_no, chunk) in self.chunks.iter().enumerate() {
            let calculator = chunk.dist_calculator(query.clone(), norm);
            let start = chunk_no * self.chunk_rows;
            for (id, rows) in admitted
                .iter()
                .enumerate()
                .take((start + self.chunk_rows).min(admitted.len()))
                .skip(start)
            {
                if *rows == 0 {
                    continue;
                }
                let distance = calculator
                    .distance(u32::try_from(id - start).map_err(|_| RoutingFallback::BuildFailed)?);
                approximate.push((
                    OrderedFloat(distance),
                    u32::try_from(id).map_err(|_| RoutingFallback::BuildFailed)?,
                ));
            }
        }
        // Exact distances are taken for four times the partitions the
        // approximate order needs; a sparse subject needs more than nprobes.
        let mut sorted = (4 * nprobes).min(approximate.len());
        let shortlist = loop {
            approximate.select_nth_unstable(sorted - 1);
            approximate[..sorted].sort_unstable();
            let (_, _, consumed) = take_probes(
                approximate[..sorted].iter().map(|r| (r.1, r.0.0)),
                admitted,
                nprobes,
                target,
            );
            let wanted = if consumed < sorted || sorted == approximate.len() {
                (4 * consumed).min(approximate.len())
            } else {
                (2 * sorted).min(approximate.len())
            };
            if wanted <= sorted {
                break wanted;
            }
            sorted = wanted;
        };
        approximate.truncate(shortlist);
        let dim = self.centroids.value_length() as usize;
        let matrix = self
            .centroids
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or(RoutingFallback::BuildFailed)?
            .values();
        for (distance, id) in &mut approximate {
            *distance = OrderedFloat(l2_distance(
                query.values(),
                &matrix[*id as usize * dim..(*id as usize + 1) * dim],
            ));
        }
        approximate.sort_unstable();
        let (ids, distances, _) = take_probes(
            approximate.iter().map(|r| (r.1, r.0.0)),
            admitted,
            nprobes,
            target,
        );
        Ok((ids, distances))
    }
}

#[cfg(test)]
#[path = "routing/tests.rs"]
mod tests;
