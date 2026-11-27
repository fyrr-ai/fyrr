use super::*;

impl PreparedReader {
    pub(crate) fn recall_sample(
        &self,
        completion: DenseCompletion,
        expires: Option<i64>,
    ) -> RecallSample {
        RecallSample {
            completion,
            identity: self.head.identity.clone(),
            policy: self.head.policy.clone(),
            expires,
        }
    }

    pub(super) async fn recall_authorization(
        &mut self,
        sample: &RecallSample,
    ) -> Result<(), crate::serve::SampleSkip> {
        use crate::serve::SampleSkip;
        match self.refresh_authorization().await {
            Ok(false) => {}
            Err(StorageUnavailable) => return Err(SampleSkip::ReferenceFailed),
            _ => return Err(SampleSkip::AuthorizationChanged),
        }
        if self.head.identity != sample.identity
            || self.head.policy != sample.policy
            || sample.expires.is_some_and(|expiry| {
                !crate::auth::unexpired(expiry, fyrr_core::clock::now().timestamp())
            })
        {
            return Err(SampleSkip::AuthorizationChanged);
        }
        Ok(())
    }

    pub(crate) async fn recall_reference(
        &mut self,
        sample: &RecallSample,
    ) -> Result<(f64, f64, u64), crate::serve::SampleSkip> {
        use crate::serve::SampleSkip;
        self.recall_authorization(sample).await?;
        let (vector, rows, count, predicate) = &sample.completion;
        let mut scan = self.dataset.scan();
        // A one-query batch selects Lance's serial flat-KNN loop (knn.rs
        // execute_batch), instead of its CPU-count-buffered scalar path.
        // At most one distance job survives drop: <=256 KiB of vector
        // payload, with row count scaled to this publication's dimension.
        // The query uses D*4 bytes and distances use at most 256 bytes.
        // Decode fan-out and fragment readahead are also one. Encoded page/I/O
        // buffers are separate; row batching is not a total memory-byte bound.
        let batch_rows = (256 * 1024 / (self.config.embedding.dimensions as usize * 4)).min(64);
        scan.batch_size(batch_rows)
            .batch_readahead(1)
            .fragment_readahead(1)
            .target_parallelism(1);
        scan.filter(predicate)
            .map_err(|_| SampleSkip::ReferenceFailed)?
            .prefilter(true)
            .with_row_id_prefilter(self.selected_rows().clone());
        let query = arrow_array::FixedSizeListArray::new(
            Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float32,
                false,
            )),
            i32::try_from(vector.len()).map_err(|_| SampleSkip::ReferenceFailed)?,
            Arc::new(Float32Array::from(vector.clone())),
            None,
        );
        scan.nearest("vector", &query, 75)
            .map_err(|_| SampleSkip::ReferenceFailed)?
            .use_index(false)
            .distance_metric(embedding::distance_type(
                self.config.namespace.settings.dense_metric,
            ))
            .with_row_id();
        scan.project(&["_rowid", "_distance"])
            .map_err(|_| SampleSkip::ReferenceFailed)?;
        #[cfg(test)]
        if self
            .test_hooks
            .sample_plan
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            let plan = scan
                .explain_plan(true)
                .await
                .map_err(|_| SampleSkip::ReferenceFailed)?;
            assert!(
                plan.contains("KNNVectorDistance: queries=1, k=75"),
                "{plan}"
            );
        }
        let batch = scan
            .try_into_batch()
            .await
            .map_err(|_| SampleSkip::ReferenceFailed)?;
        self.recall_authorization(sample).await?;
        let reference = ids(&batch).map_err(|_| SampleSkip::ReferenceFailed)?;
        if reference
            .values()
            .iter()
            .any(|id| !self.selected_rows().selected(*id))
        {
            return Err(SampleSkip::AuthorizationChanged);
        }
        if reference.is_empty() {
            return Err(SampleSkip::EmptyReference);
        }
        let distances = column::<Float32Array>(&batch, "_distance", IndexRowsInvalid)
            .map_err(|_| SampleSkip::ReferenceFailed)?;
        let recall = |k| recall_at_k(&rows[..*count], reference, distances, k);
        Ok((
            recall(10),
            recall(75),
            self.selected_rows().max_len().unwrap_or(0),
        ))
    }

    /// The recall of the vector index on `args.count` stored vectors of the
    /// rows the caller may read and `args.filter` selects. One admission and
    /// filter selection serves the whole run; each sample is one of the
    /// background sampler: the nearest rows of the stored vector, then the
    /// exact reference over the same predicate. A change of the caller's
    /// permissions fails the reference check, never a sample.
    pub(crate) async fn recall(
        &mut self,
        args: &RecallInput,
        expires: Option<i64>,
    ) -> Result<RecallOutput> {
        use crate::serve::SampleSkip;
        args.validate().map_err(|_| InvalidArgument)?;
        let (_, _, mut telemetry, _pin) = self.request_prologue().await?;
        self.apply_filters(args.filter.as_ref(), None).await?;
        let rows = self.selected_rows().max_len().unwrap_or(0);
        let predicate = self.request_predicate()?;
        let (mut samples, mut approximate) = (0_u32, true);
        let (mut at_10, mut at_75) = (0.0, 0.0);
        for vector in self.recall_vectors(args, &predicate).await? {
            let (nearest, by_index) = self
                .dense_traced(&vector, MAX_RECALL_RANK, false, &mut telemetry)
                .await?;
            approximate &= by_index;
            let mut found = [0; MAX_RECALL_RANK];
            for (slot, candidate) in found.iter_mut().zip(&nearest) {
                *slot = candidate.row_id;
            }
            let sample = self.recall_sample(
                (
                    vector,
                    found,
                    nearest.len().min(MAX_RECALL_RANK),
                    predicate.clone(),
                ),
                expires,
            );
            match self.recall_reference(&sample).await {
                Ok((recall_10, recall_75, _)) => {
                    samples += 1;
                    at_10 += recall_10;
                    at_75 += recall_75;
                }
                // The selection lost its rows between the two searches.
                Err(SampleSkip::EmptyReference) => {}
                Err(SampleSkip::AuthorizationChanged) => return Err(AuthorizationUnavailable),
                Err(_) => return Err(StorageUnavailable),
            }
        }
        if self.interrupted().await? {
            return Err(AuthorizationUnavailable);
        }
        Ok(RecallOutput {
            samples,
            rows,
            approximate: samples > 0 && approximate,
            recall_at_10: (samples > 0).then(|| at_10 / f64::from(samples)),
            recall_at_75: (samples > 0).then(|| at_75 / f64::from(samples)),
        })
    }

    /// The stored vectors of the first `args.count` rows, in storage order,
    /// that `predicate` (the caller's admission and the filter) selects.
    // shortcut: the first rows, not a random sample of them; upgrade when the
    // order of storage correlates with the recall of the index.
    async fn recall_vectors(&self, args: &RecallInput, predicate: &str) -> Result<Vec<Vec<f32>>> {
        let mut scan = self.dataset.scan();
        scan.project(&["vector"]).map_err(|_| IncompatibleIndex)?;
        scan.filter(predicate)
            .map_err(|_| IncompatibleIndex)?
            .with_row_id();
        scan.limit(Some(i64::from(args.count)), None)
            .map_err(|_| IncompatibleIndex)?;
        let batch = scan
            .try_into_batch()
            .await
            .map_err(|_| StorageUnavailable)?;
        if ids(&batch)?
            .values()
            .iter()
            .any(|id| !self.selected_rows().selected(*id))
        {
            return Err(AuthorizationUnavailable);
        }
        let vectors =
            column::<arrow_array::FixedSizeListArray>(&batch, "vector", SearchResultInvalid)?;
        (0..vectors.len())
            .map(|row| embedding::stored_values(&*vectors.value(row)).ok_or(SearchResultInvalid))
            .collect()
    }
}

/// The ranks a recall run compares: the deepest one the sampler reports.
const MAX_RECALL_RANK: usize = fyrr_core::limits::mcp::MAX_SEARCH_LIMIT as usize;
