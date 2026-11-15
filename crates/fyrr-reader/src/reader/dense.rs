use super::*;

enum DenseRefinement {
    Expand(usize),
    Rows(Vec<Candidate>),
}

impl PreparedReader {
    pub(crate) async fn dense(
        &self,
        vector: &[f32],
        k: usize,
        telemetry: &mut Telemetry,
    ) -> Result<Vec<Candidate>> {
        embedding::validate(vector, self.config.embedding.dimensions)?;
        if !(1..=75).contains(&k) {
            return Err(InvalidArgument);
        }
        self.authorization()?;
        let wanted = k.min(
            usize::try_from(self.selected_rows().max_len().unwrap_or(0))
                .map_err(|_| StorageUnavailable)?,
        );
        let query_array = Float32Array::from(vector.to_vec());
        if wanted == 0 {
            return Ok(Vec::new());
        }
        let depth = &self.config.namespace.retrieval.rescore_depth;
        let mut shortlist = depth.initial(k);
        loop {
            let before = self.mirror.counters();
            let start = Instant::now();
            if !self.preparation_reservation.warm()
                && self.config.reader.routing == RoutingStrategy::Rabitq
            {
                routing_telemetry(telemetry, Some(RoutingFallback::Absent), false);
            }
            let candidates = if self.preparation_reservation.warm()
                && self.maps.is_some()
                && self.filter_selection.is_none()
            {
                match self.candidates(vector, shortlist, telemetry).await {
                    Ok(candidates) => Some(candidates),
                    Err(PartitionMapUnavailable) => None,
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            if candidates.is_none() && telemetry.routing_strategy.is_none() {
                routing_telemetry(telemetry, None, false);
            }
            self.stage(telemetry, "dense_routing_ann", start, before);
            let refine_start = Instant::now();
            let refine_counters = self.mirror.counters();
            let mut rows = if let Some(candidates) = candidates {
                match self
                    .refine_dense_candidates(vector, candidates, wanted, k, shortlist, telemetry)
                    .await?
                {
                    DenseRefinement::Rows(rows) => rows,
                    DenseRefinement::Expand(next) => {
                        self.stage(telemetry, "dense_refinement", refine_start, refine_counters);
                        shortlist = next;
                        continue;
                    }
                }
            } else {
                self.native_dense_rows(
                    &query_array,
                    wanted,
                    k,
                    self.preparation_reservation.warm() && self.filter_selection.is_none(),
                    self.fallback_probes,
                    telemetry,
                )
                .await?
            };
            if rows.len() < wanted {
                rows = self
                    .native_dense_rows(
                        &query_array,
                        wanted,
                        k,
                        false,
                        self.fallback_probes,
                        telemetry,
                    )
                    .await?;
            }
            if rows.len() != wanted {
                return Err(StorageUnavailable);
            }
            self.stage(telemetry, "dense_refinement", refine_start, refine_counters);
            telemetry.unindexed_rows_scanned += self
                .unindexed
                .iter()
                .filter(|id| self.selected_rows().selected(**id))
                .count() as u64;
            return Ok(rows);
        }
    }

    async fn refine_dense_candidates(
        &self,
        vector: &[f32],
        candidates: BTreeSet<u64>,
        wanted: usize,
        k: usize,
        shortlist: usize,
        telemetry: &mut Telemetry,
    ) -> Result<DenseRefinement> {
        let depth = &self.config.namespace.retrieval.rescore_depth;
        let tie_bound = k * 16;

        let sparse_start = Instant::now();
        let sparse_counters = self.mirror.counters();
        // Admit the unindexed tail as well as indexed candidates before
        // either sparse path reads vectors or metadata. This finite mask
        // is authoritative; an equivalent SQL refine would reread ACLs.
        let admitted: Vec<_> = candidates
            .into_iter()
            .filter(|id| self.selected_rows().selected(*id))
            .collect();
        self.authorization()?;
        let batch = match self.config.reader.sparse_read {
            fyrr_core::config::SparseRead::MaskedScan => {
                let mut scan = self.dataset.scan();
                scan.project(&["source_id", "root_id", "record_id", "vector"])
                    .map_err(|_| PublicationInvalid)?;
                scan.with_row_id()
                    .with_row_id_prefilter(RowAddrMask::from_allowed(RowAddrTreeMap::from_iter(
                        admitted,
                    )));
                scan.try_into_batch()
                    .await
                    .map_err(|_| StorageUnavailable)?
            }
            fyrr_core::config::SparseRead::TakeRows => self
                .dataset
                .take_rows(
                    &admitted,
                    lance::dataset::ProjectionRequest::from_columns(
                        ["source_id", "root_id", "record_id", "vector", "_rowid"],
                        self.dataset.schema(),
                    ),
                )
                .await
                .map_err(|_| StorageUnavailable)?,
        };
        self.stage(
            telemetry,
            "dense_sparse_read",
            sparse_start,
            sparse_counters,
        );
        let rescore_start = Instant::now();
        let rescore_counters = self.mirror.counters();
        let key = Arc::new(arrow_array::Float16Array::from(
            vector
                .iter()
                .copied()
                .map(half::f16::from_f32)
                .collect::<Vec<_>>(),
        ));
        let batch = lance_index::vector::flat::compute_distance(
            key,
            lance_linalg::distance::DistanceType::Cosine,
            "vector",
            batch,
        )
        .await
        .map_err(|_| StorageUnavailable)?;
        self.stage(
            telemetry,
            "float16_rescore",
            rescore_start,
            rescore_counters,
        );
        let distances = column::<Float32Array>(&batch, "_distance", SearchResultInvalid)?;
        let sources = strings(&batch, "source_id")?;
        let roots = strings(&batch, "root_id")?;
        let records = strings(&batch, "record_id")?;
        let mut ranked: Vec<_> = (0..batch.num_rows()).collect();
        ranked.sort_by(|a, b| {
            distances.value(*a).total_cmp(&distances.value(*b)).then(
                (sources.value(*a), roots.value(*a), records.value(*a)).cmp(&(
                    sources.value(*b),
                    roots.value(*b),
                    records.value(*b),
                )),
            )
        });
        telemetry.dense_candidates_fetched += batch.num_rows() as u64;
        if ranked.len() > wanted
            && let Some(next) = depth.expand(
                shortlist,
                k,
                (distances.value(ranked[wanted]) - distances.value(ranked[wanted - 1])) as f64,
            )
        {
            return Ok(DenseRefinement::Expand(next));
        }
        let unresolved = ranked.len() > wanted
            && ranked.len() >= shortlist
            && (ranked.len() as u64) < self.selected_rows().max_len().unwrap_or(0)
            && distances
                .value(ranked[wanted - 1])
                .total_cmp(&distances.value(ranked[ranked.len() - 1]))
                .is_eq();
        if unresolved && shortlist < tie_bound {
            return Ok(DenseRefinement::Expand((shortlist * 2).min(tie_bound)));
        }
        telemetry.dense_tie_bound_hits += u64::from(unresolved);
        ranked.truncate(wanted);
        let batch = arrow_select::take::take_record_batch(
            &batch,
            &UInt32Array::from(
                ranked
                    .into_iter()
                    .map(|row| {
                        let Ok(row) = u32::try_from(row) else {
                            unreachable!("bounded candidate batch index fits u32");
                        };
                        row
                    })
                    .collect::<Vec<_>>(),
            ),
        )
        .map_err(|_| SearchResultInvalid)?;
        Ok(DenseRefinement::Rows(Self::lane_rows(
            &batch,
            "_distance",
            true,
            self.selected_rows(),
        )?))
    }

    pub(super) async fn native_dense_rows(
        &self,
        query: &Float32Array,
        wanted: usize,
        k: usize,
        mut use_index: bool,
        nprobes: usize,
        telemetry: &mut Telemetry,
    ) -> Result<Vec<Candidate>> {
        let bound = k * 16;
        let depth = &self.config.namespace.retrieval.rescore_depth;
        let mut pool = depth.initial(k);
        let eligible = usize::try_from(self.selected_rows().max_len().unwrap_or(0))
            .map_err(|_| StorageUnavailable)?;
        // Preserve k*refine for non-tied ANN queries. At k=75 this is
        // 100*3=300, exactly the previous 75*4=300 refinement pool.
        let next_divisor = |n: usize, pool: usize| {
            ((n + 1)..=pool)
                .find(|v| pool.is_multiple_of(*v))
                .unwrap_or(pool)
        };
        let mut requested = if use_index {
            next_divisor(wanted, pool)
        } else {
            wanted + 1
        }
        .min(eligible);
        loop {
            let mut scan = self.dataset.scan();
            scan.project(&["source_id", "root_id", "record_id"])
                .map_err(|_| PublicationInvalid)?;
            scan.filter(&self.request_predicate()?)
                .map_err(|_| AuthorizationUnavailable)?
                .prefilter(true)
                .with_row_id();
            if !self.preparation_reservation.warm() {
                scan.batch_size(256);
            }
            scan.nearest("vector", query, requested)
                .map_err(|_| EmbeddingInvalid)?
                .use_index(use_index);
            scan.distance_metric(lance_linalg::distance::DistanceType::Cosine);
            scan.nprobes(nprobes)
                .refine(u32::try_from((pool / requested).max(1)).map_err(|_| InvalidArgument)?);
            let batch = scan
                .try_into_batch()
                .await
                .map_err(|_| StorageUnavailable)?;
            let mut rows = Self::lane_rows(&batch, "_distance", true, self.selected_rows())?;
            telemetry.dense_candidates_fetched += rows.len() as u64;
            if use_index
                && rows.len() > wanted
                && let Some(next) =
                    depth.expand(pool, k, rows[wanted - 1].score - rows[wanted].score)
            {
                pool = next;
                requested = next_divisor(wanted, pool).min(eligible);
                continue;
            }
            if rows.len() < wanted && use_index {
                use_index = false;
                requested = (wanted + 1).min(eligible);
                continue;
            }
            let unresolved = rows.len() == requested
                && rows.len() > wanted
                && rows.len() < eligible
                && rows[wanted - 1]
                    .score
                    .total_cmp(&rows[rows.len() - 1].score)
                    .is_eq();
            if !unresolved || requested == bound {
                telemetry.dense_tie_bound_hits += u64::from(unresolved);
                rows.truncate(wanted);
                return Ok(rows);
            }
            requested = if use_index && requested < pool {
                next_divisor(requested, pool)
            } else {
                requested * 2
            }
            .min(bound)
            .min(eligible);
        }
    }

    pub(super) async fn candidates(
        &self,
        vector: &[f32],
        shortlist: usize,
        telemetry: &mut Telemetry,
    ) -> Result<BTreeSet<u64>> {
        let indexes = &self.indexes;
        let prefilter = Arc::new(DatasetPreFilter::new(
            self.dataset.clone(),
            indexes.as_ref(),
            Some(Box::new(RowAdmission(self.selected_rows().clone()))),
        ));
        let mut candidates = BTreeMap::<u64, f32>::new();
        for (map_no, map) in self
            .maps
            .as_ref()
            .ok_or(PartitionMapUnavailable)?
            .iter()
            .enumerate()
        {
            let key = arrow_array::Float16Array::from(
                vector
                    .iter()
                    .copied()
                    .map(half::f16::from_f32)
                    .collect::<Vec<_>>(),
            );
            let key = lance_linalg::kernels::normalize_arrow(&key)
                .map_err(|_| EmbeddingInvalid)?
                .0;
            let query = Query {
                column: "vector".into(),
                key,
                k: shortlist,
                lower_bound: None,
                upper_bound: None,
                minimum_nprobes: map.allowed.len(),
                maximum_nprobes: Some(map.allowed.len()),
                search_effort: 0.5,
                ef: None,
                refine_factor: None,
                metric_type: None,
                use_index: true,
                query_parallelism: 1,
                dist_q_c: 0.0,
                approx_mode: ApproxMode::default(),
            };
            let route = strategy(self.config.reader.routing, map.index.total_partitions());
            let routed = if route == RoutingStrategy::Rabitq {
                let outcome = self
                    .centroid_routers
                    .get()
                    .and_then(|routers| routers.get(map_no))
                    .ok_or(RoutingFallback::Absent)
                    .and_then(|router| router.as_ref().map_err(|reason| *reason))
                    .and_then(|router| {
                        let normalized = query
                            .key
                            .as_any()
                            .downcast_ref::<arrow_array::Float16Array>()
                            .ok_or(RoutingFallback::BuildFailed)?;
                        router.select(
                            Arc::new(Float32Array::from_iter_values(
                                normalized.values().iter().map(|v| v.to_f32()),
                            )),
                            &map.allowed,
                            self.config.reader.nprobes,
                        )
                    });
                routing_telemetry(telemetry, outcome.as_ref().err().copied(), outcome.is_ok());
                outcome.ok()
            } else {
                routing_telemetry(telemetry, None, false);
                None
            };
            let (selected, selected_distances) = if let Some(routed) = routed {
                routed
            } else {
                let (partitions, distances) = map
                    .index
                    .find_partitions(&query)
                    .map_err(|_| PartitionMapUnavailable)?;
                let mut selected = Vec::new();
                let mut selected_distances = Vec::new();
                for row in 0..partitions.len() {
                    let id = partitions.value(row) as usize;
                    if map.allowed[id] {
                        selected.push(u32::try_from(id).map_err(|_| PartitionMapUnavailable)?);
                        selected_distances.push(distances.value(row));
                        if selected.len() == self.config.reader.nprobes {
                            break;
                        }
                    }
                }
                (selected, selected_distances)
            };
            let count = selected.len();
            let mut stream = map
                .index
                .clone()
                .search_partitions(
                    query,
                    Arc::new(UInt32Array::from(selected)),
                    Arc::new(Float32Array::from(selected_distances)),
                    0,
                    count,
                    prefilter.clone(),
                    None,
                    Arc::new(NoOpMetricsCollector),
                )
                .await
                .map_err(|_| StorageUnavailable)?;
            while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
                let row_ids = ids(&batch)?;
                let distance = column::<Float32Array>(&batch, "_distance", IndexRowsInvalid)?;
                for row in 0..batch.num_rows() {
                    let id = row_ids.value(row);
                    if !self.selected_rows().selected(id) {
                        return Err(AuthorizationUnavailable);
                    }
                    candidates
                        .entry(id)
                        .and_modify(|d| *d = d.min(distance.value(row)))
                        .or_insert(distance.value(row));
                }
            }
        }
        let mut ranked: Vec<_> = candidates.into_iter().collect();
        ranked.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        ranked.truncate(shortlist);
        let mut result: BTreeSet<_> = ranked.into_iter().map(|(id, _)| id).collect();
        result.extend(self.unindexed.iter().copied());
        Ok(result)
    }
}
