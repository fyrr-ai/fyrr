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
            || sample
                .expires
                .is_some_and(|expiry| expiry <= fyrr_core::clock::now().timestamp())
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
            .distance_metric(embedding::distance_type(self.config.namespace.dense_metric))
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
    /// rows the caller may read and `args.filter` selects: each is one sample of
    /// the background sampler, a dense search by that vector and the exact
    /// reference over the same predicate.
    pub(crate) async fn recall(
        &mut self,
        args: &RecallInput,
        expires: Option<i64>,
        deadline: Instant,
    ) -> Result<RecallOutput> {
        use crate::serve::SampleSkip;
        args.validate().map_err(|_| InvalidArgument)?;
        let vectors = self.recall_vectors(args).await?;
        let mut output = RecallOutput {
            samples: 0,
            rows: 0,
            recall_at_10: None,
            recall_at_75: None,
        };
        let (mut at_10, mut at_75) = (0.0, 0.0);
        for vector in vectors {
            let search: SearchInput = serde_json::from_value(serde_json::json!({
                "vector": vector, "limit": fyrr_core::limits::mcp::MAX_SEARCH_LIMIT,
                "output": "metadata", "filter": args.filter}))
            .map_err(|_| InvalidArgument)?;
            let mut completion = None;
            self.search_input(&search, Some((&mut completion, 0)), Some(deadline))
                .await?;
            let Some(completion) = completion else {
                continue;
            };
            let sample = self.recall_sample(completion, expires);
            match self.recall_reference(&sample).await {
                Ok((recall_10, recall_75, rows)) => {
                    output.samples += 1;
                    output.rows = rows;
                    at_10 += recall_10;
                    at_75 += recall_75;
                }
                // The rows of the selection are gone between the two searches.
                Err(SampleSkip::EmptyReference) => {}
                Err(SampleSkip::AuthorizationChanged) => return Err(AuthorizationUnavailable),
                Err(_) => return Err(StorageUnavailable),
            }
        }
        if output.samples > 0 {
            let samples = f64::from(output.samples);
            output.recall_at_10 = Some(at_10 / samples);
            output.recall_at_75 = Some(at_75 / samples);
        }
        Ok(output)
    }

    /// The stored vectors of the first `args.count` rows, in storage order,
    /// that the caller may read and `args.filter` selects.
    // shortcut: the first rows, not a random sample of them; upgrade when the
    // order of storage correlates with the recall of the index.
    async fn recall_vectors(&mut self, args: &RecallInput) -> Result<Vec<Vec<f32>>> {
        let (_, _, _, _pin) = self.request_prologue().await?;
        self.apply_filters(args.filter.as_ref()).await?;
        let mut scan = self.dataset.scan();
        scan.project(&["vector"]).map_err(|_| IncompatibleIndex)?;
        scan.filter(&self.request_predicate()?)
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
