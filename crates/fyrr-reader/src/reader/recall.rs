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
    ) -> Result<serde_json::Value, crate::serve::SampleSkip> {
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
        Ok(
            serde_json::json!({"recall_at_10":recall(10),"recall_at_75":recall(75),
            "recall_admitted_rows":self.selected_rows().max_len().unwrap_or(0)}),
        )
    }
}
