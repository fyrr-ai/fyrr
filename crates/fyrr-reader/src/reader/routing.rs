use super::*;

impl PreparedReader {
    pub(crate) async fn prepare_routing(&mut self) -> Result<()> {
        use lance::cache::DeepSizeOf;

        if self.config.reader.routing == RoutingStrategy::Exact
            || !self.preparation_reservation.warm()
        {
            return Ok(());
        }
        let roots = self.vector_roots.clone();
        let opened = roots.get().is_none();
        let roots = roots
            .get_or_try_init(|| async {
                let mut roots = Vec::new();
                for metadata in self
                    .indexes
                    .iter()
                    .filter(|i| i.name == Layout::VECTOR_INDEX)
                {
                    roots.push(
                        self.dataset
                            .open_vector_index("vector", &metadata.uuid, &NoOpMetricsCollector)
                            .await
                            .map_err(|_| StorageUnavailable)?,
                    );
                }
                Ok::<_, ReaderError>(roots)
            })
            .await?;

        if opened
            && !self
                .preparation_reservation
                .settle(roots.iter().map(|root| root.deep_size_of() as u64).sum())?
        {
            self.maps = None;
            self.vector_roots = Arc::default();
            self.centroid_routers = Arc::default();
            self.generation_state = GenerationState::Cold;
            return Ok(());
        }
        let routers = self.centroid_routers.clone();
        let configured = self.config.reader.routing;
        let pool = self.preparation_budget.permits.clone();
        let reservation = self.preparation_reservation.clone();
        routers
            .get_or_init(|| async {
                let centroids = roots
                    .iter()
                    .map(|root| {
                        (
                            strategy(configured, root.total_partitions()),
                            root.ivf_model().centroids_array().cloned(),
                        )
                    })
                    .collect::<Vec<_>>();
                // No source I/O occurs inside this build; storage/auth errors above
                // are never reclassified as an optional routing failure.
                tokio::task::spawn_blocking(move || {
                    // Cancellation can drop the preparer before this blocking
                    // native build ends; retain the shared centroid/root claim.
                    let _reservation = reservation;
                    centroids
                        .into_iter()
                        .map(|(selected, centroids)| {
                            if selected == RoutingStrategy::Exact {
                                return Err(RoutingFallback::Absent);
                            }
                            centroids.as_ref().ok_or(RoutingFallback::Absent).and_then(
                                |centroids| {
                                    CentroidRouter::build(
                                        centroids,
                                        pool.clone(),
                                        _reservation.warm_bytes(),
                                    )
                                },
                            )
                        })
                        .collect()
                })
                .await
                .unwrap_or_else(|_| {
                    roots
                        .iter()
                        .map(|_| Err(RoutingFallback::BuildFailed))
                        .collect()
                })
            })
            .await;
        Ok(())
    }
}
