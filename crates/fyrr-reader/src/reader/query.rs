use super::*;

type LexicalQuery = Result<(Vec<Candidate>, u64, u64)>;
type DenseQuery = Result<(Vec<Candidate>, Option<Vec<f32>>)>;
impl PreparedReader {
    /// Runs one search, as `fyrr reader search` and the MCP `search` tool do.
    pub async fn search(&mut self, args: &SearchInput) -> Result<SearchOutput> {
        self.search_input(args, None, None).await
    }
    /// Records the stage keys of the next search for `take_stages`: the
    /// dense and lexical candidates (`[source, root, record, score]`) and
    /// the composed order (`[source, root, record]`) for tests.
    #[cfg(test)]
    pub(crate) fn record_stages(&mut self) {
        self.stage_trace = Some(serde_json::Value::Null);
    }
    /// The stage keys recorded since `record_stages`; stops recording.
    #[cfg(test)]
    pub(crate) fn take_stages(&mut self) -> Option<serde_json::Value> {
        self.stage_trace.take()
    }
    #[cfg(test)]
    fn trace_stages(&mut self, trace: impl FnOnce(&mut serde_json::Value)) {
        if let Some(stages) = &mut self.stage_trace {
            trace(stages);
        }
    }
    pub(crate) fn selected_rows(&self) -> &RowAddrMask {
        self.filter_selection
            .as_ref()
            .map_or(&self.admission.eligible, |filter| &filter.eligible)
    }
    pub(crate) fn request_predicate(&self) -> Result<String> {
        let authorization = self.admission_predicate()?;
        Ok(self
            .filter_selection
            .as_ref()
            .map_or(authorization.clone(), |filter| {
                format!("({authorization}) AND ({})", filter.predicate)
            }))
    }
    pub(crate) async fn search_input(
        &mut self,
        args: &SearchInput,
        completion: Option<(&mut Option<DenseCompletion>, usize)>,
        deadline: Option<Instant>,
    ) -> Result<SearchOutput> {
        args.validate().map_err(|_| InvalidArgument)?;
        let queries: Vec<&str> = args.queries.as_ref().map_or_else(
            || vec![args.query.as_deref().unwrap_or("")],
            |queries| queries.iter().map(String::as_str).collect(),
        );
        let (total, before, telemetry, _pin) = self.request_prologue().await?;
        // An unknown included attribute is refused even when nothing matches.
        self.returned_columns(args.fields.as_ref())?;
        crate::rerank::scored_column(self, args.rerank.as_ref())?;
        let deadline = deadline.unwrap_or_else(|| {
            total
                + std::time::Duration::from_millis(u64::from(
                    args.deadline_ms
                        .unwrap_or(fyrr_core::limits::mcp::DEFAULT_DEADLINE_MS),
                ))
        });
        // Leave some request time for lexical fusion, hydration and the final
        // authorization check after a provider timeout. The request's absolute
        // deadline remains authoritative, including time spent in its queue.
        let embedding_deadline = deadline
            - std::time::Duration::from_millis(50)
                .min(deadline.saturating_duration_since(Instant::now()) / 10);
        self.response_claim = None;
        let outcome = self
            .search_admitted(args, &queries, embedding_deadline, telemetry, completion)
            .await;
        let stage = Instant::now();
        let counters = self.mirror.counters();
        #[cfg(test)]
        {
            self.test_hooks.observe_admission(self, true);
            self.test_hooks.authorization_output.wait_once().await;
        }
        #[cfg(feature = "test-store")]
        crate::storage::test_store::boundary::request_boundary("AUTHORIZE", "authorization-output")
            .await
            .map_err(|_| StorageUnavailable)?;
        if self.interrupted().await? {
            return Err(AuthorizationUnavailable);
        }
        let mut result = outcome?;
        self.stage(
            &mut result.metadata.telemetry,
            "output_authorization",
            stage,
            counters,
        );
        self.finish_metadata(&mut result.metadata, total, before);
        if self.selected_rows().max_len().unwrap_or(0) == 0 {
            // An empty selection has no retrieval work or cache-dependent telemetry.
            result.metadata.telemetry = Telemetry::default();
        }
        // Explicit search durations reveal whether shared embeddings needed a
        // provider call. Internal stage accounting remains available to execution.
        result.metadata.telemetry.stage_us.clear();
        result.metadata.telemetry.rerank_us = None;
        result.metadata.telemetry.queue_wait_ms = 0;
        // A listing already comes in the order that selected its page, and sorting
        // it again by the finer metadata time would break paging on ties.
        if args.order == Some(SearchOrder::UpdatedAt) && args.query.is_some() {
            result.hits.sort_by(|a, b| {
                b.updated_at.cmp(&a.updated_at).then(
                    (&a.source_id, &a.root_id, &a.record_id).cmp(&(
                        &b.source_id,
                        &b.root_id,
                        &b.record_id,
                    )),
                )
            });
        }
        Ok(result)
    }

    async fn search_admitted(
        &mut self,
        args: &SearchInput,
        queries: &[&str],
        embedding_deadline: Instant,
        mut telemetry: Telemetry,
        mut completion: Option<(&mut Option<DenseCompletion>, usize)>,
    ) -> Result<SearchOutput> {
        if let Some(subs) = &args.sub_queries {
            return self
                .search_sub_queries(args, subs, embedding_deadline, telemetry)
                .await;
        }
        let k = args.effective_limit() as usize;

        let stage = Instant::now();
        let filter_counters = self.mirror.counters();
        self.apply_filters(args.filter_tree().as_ref()).await?;
        self.apply_contains(args.contains.as_deref()).await?;
        let predicate = self.request_predicate()?;
        let lexical_predicate = Self::fts_requires_sql_prefilter(&self.dataset, &self.indexes)
            .then_some(predicate.as_str());
        self.stage(&mut telemetry, "predicate", stage, filter_counters);
        if !args.has_text() && args.vector.is_none() {
            return self.listing(args, telemetry).await;
        }
        // Determine participation before preparing query vectors or consulting
        // the process cache. Refused arms cannot observe shared cache membership.
        self.authorization()?;
        if let Some(vector) = &args.vector {
            embedding::validate(vector, self.config.embedding.dimensions)
                .map_err(|_| InvalidArgument)?;
        }
        if args.exact == Some(true) {
            super::dense::exact_scan_bound(self.selected_rows().max_len().unwrap_or(0))?;
        }
        if self.selected_rows().max_len().unwrap_or(0) == 0 {
            return Ok(self.nothing_admitted());
        }
        // Reserve added batch scratch from the same pool as serving generations.
        // Default requests retain their existing claims and executable path.
        self.response_claim =
            if args.queries.is_some() && self.selected_rows().max_len().unwrap_or(0) != 0 {
                Some(self.batch_claim(
                    queries.len(),
                    self.config.namespace.retrieval.dense_candidates,
                    queries.len() * k,
                )?)
            } else {
                None
            };
        let start = Instant::now();
        let runtime = self.query_embeddings.clone();
        let mut vectors = match &args.vector {
            Some(vector) => embedding::QueryWork::supplied(vector.clone()),
            None => runtime.prepare(queries)?,
        };
        telemetry
            .stage_us
            .insert("embedding".into(), micros(start.elapsed()));
        let fallback_needed = self.execution_slot.is_some()
            && self.config.embedding.reader_live
            && self.selected_rows().max_len().unwrap_or(0) != 0
            && vectors.vectors.iter().any(Result::is_err);
        let max_documents = if args.queries.is_some() {
            self.config.namespace.retrieval.max_documents
        } else {
            k
        };
        // Extra retained output and candidate copies share the serving budget.
        // Cache-only and process-cache hits keep their original path and claims.
        let _fallback_claim = if fallback_needed {
            let bytes = (max_documents.min(queries.len() * k)) as u64 * 1024 * 1024
                + queries.len() as u64
                    * self.config.namespace.retrieval.lexical_candidates as u64
                    * (std::mem::size_of::<Candidate>() + 3 * fyrr_core::limits::MAX_ID_BYTES)
                        as u64;
            Some(self.preparation_budget.try_claim(bytes).ok_or(Busy)?)
        } else {
            None
        };
        let fallback_telemetry = telemetry.clone();
        let start = Instant::now();
        let counters = self.mirror.counters();
        let ((lexical, lexical_us, lexical_arm_us, fallback), dense) = tokio::join!(
            self.lexical_batch(
                args,
                queries,
                lexical_predicate,
                fallback_needed,
                fallback_telemetry
            ),
            self.dense_batch(
                args,
                queries,
                embedding_deadline,
                &runtime,
                &mut vectors,
                &mut telemetry
            ),
        );
        let (dense, embedding_failed) = dense?;
        *telemetry.stage_us.entry("lexical".into()).or_default() += lexical_us;
        // Shared mirror counters cannot attribute overlapping reads to a lane.
        // Keep lane timers, but charge concurrent I/O exactly once to this stage.
        telemetry
            .stage_s3_requests
            .retain(|name, _| !name.starts_with("dense_") && name != "float16_rescore");
        telemetry
            .stage_s3_bytes
            .retain(|name, _| !name.starts_with("dense_") && name != "float16_rescore");
        self.stage(&mut telemetry, "retrieval_concurrent", start, counters);
        if vectors.provider_us != 0 {
            // Only subtract waiting that did not overlap native lexical
            // work. Storage counters keep their single shared charge.
            let wait_only = vectors.provider_us.saturating_sub(lexical_arm_us);
            let concurrent = telemetry
                .stage_us
                .get_mut("retrieval_concurrent")
                .ok_or(StorageUnavailable)?;
            *concurrent = concurrent.saturating_sub(wait_only);
        }
        let ranked_queries = self.compose_queries(
            args,
            &predicate,
            lexical,
            dense,
            &mut completion,
            &mut telemetry,
        )?;
        self.finish_search_batch(
            args,
            queries,
            ranked_queries,
            embedding_failed,
            fallback,
            telemetry,
        )
        .await
    }

    /// What a request answers when the caller may read nothing it selects.
    fn nothing_admitted(&self) -> SearchOutput {
        let mut metadata = self.metadata(Telemetry::default());
        metadata.degraded = vec![Lane::Dense];
        SearchOutput {
            hits: Vec::new(),
            truncated: None,
            metadata,
        }
    }

    /// The claim for a batch of `queries` retrievals that read up to
    /// `dense_candidates` dense candidates each and answer with at most
    /// `documents` documents.
    fn batch_claim(
        &self,
        queries: usize,
        dense_candidates: usize,
        documents: usize,
    ) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
        let cfg = &self.config.namespace.retrieval;
        let candidate_bytes =
            (std::mem::size_of::<Candidate>() + 3 * fyrr_core::limits::MAX_ID_BYTES) as u64;
        // Expanded tie sets, decoded vectors and distance-kernel scratch.
        let bytes = queries as u64
                * (cfg.rescore_depth.maximum(dense_candidates) as u64
                    * (candidate_bytes + u64::from(self.config.embedding.dimensions) * 8)
                    + cfg.lexical_candidates as u64 * 1024u64.div_ceil(50) * candidate_bytes)
                // Includes hydrated metadata/text, typed/JSON copies and escaped
                // structured + textual MCP output through transport buffering.
                + documents.min(cfg.max_documents) as u64 * 1024 * 1024;
        Ok(Arc::new(
            self.preparation_budget.try_claim(bytes).ok_or(Busy)?,
        ))
    }

    /// The `sub_queries` of a JSON `query` request, read from one pinned
    /// publication: each is filtered, searched and ranked on its own, then
    /// their answers follow one another in request order, or `fusion` ranks
    /// the documents of all of them. One claim, one deadline, one reranking
    /// and one hydration serve the whole request, and a sub-query's rows pass
    /// the same admission as a single query's.
    async fn search_sub_queries(
        &mut self,
        args: &SearchInput,
        subs: &[SubQuery],
        embedding_deadline: Instant,
        mut telemetry: Telemetry,
    ) -> Result<SearchOutput> {
        self.authorization()?;
        self.filter_selection = None;
        // An unknown field or a mistyped value is refused even when nothing matches.
        for sub in subs {
            if let Some(tree) = args.sub_query_filter(sub) {
                Self::filter_sql(&tree, &self.manifest.attributes)?;
            }
        }
        let cfg = &self.config.namespace.retrieval;
        let (dense_candidates, lexical_candidates, max_documents) = (
            cfg.dense_candidates,
            cfg.lexical_candidates,
            cfg.max_documents,
        );
        // The answer holds the hits of every sub-query, or the fused documents.
        let documents = if args.fusion.is_some() {
            args.effective_limit() as usize
        } else {
            subs.iter().map(|sub| sub.limit as usize).sum()
        };
        // A sum of limits above what an answer may hold is refused, not cut.
        if args.fusion.is_none() && documents > max_documents {
            return Err(InvalidArgument);
        }
        for vector in subs.iter().filter_map(|sub| sub.vector.as_ref()) {
            embedding::validate(vector, self.config.embedding.dimensions)
                .map_err(|_| InvalidArgument)?;
        }
        if self.selected_rows().max_len().unwrap_or(0) == 0 {
            return Ok(self.nothing_admitted());
        }
        let deepest = subs.iter().map(|sub| sub.limit as usize).max().unwrap_or(0);
        self.response_claim =
            Some(self.batch_claim(subs.len(), dense_candidates.max(deepest), documents)?);
        // The vectors of the sub-queries that bring none, embedded together.
        // shortcut: a missing embedding is awaited with the execution slot
        // held, where a single query releases it; release it too if parked
        // requests show up under load.
        let start = Instant::now();
        let runtime = self.query_embeddings.clone();
        let texts: Vec<&str> = subs
            .iter()
            .filter(|sub| sub.vector.is_none())
            .filter_map(|sub| sub.query.as_deref())
            .collect();
        let mut work = runtime.prepare(&texts)?;
        runtime
            .complete(&mut work, &texts, true, embedding_deadline)
            .await;
        telemetry
            .stage_us
            .insert("embedding".into(), micros(start.elapsed()));
        let mut embedded = work.vectors.iter_mut();
        let mut ranked = Vec::with_capacity(subs.len());
        let mut embedding_failed = false;
        for sub in subs {
            let vector = match &sub.vector {
                Some(vector) => Ok(vector.clone()),
                None => embedded.next().map_or(Err(EmbeddingUnavailable), |vector| {
                    std::mem::replace(vector, Err(EmbeddingUnavailable))
                }),
            };
            embedding_failed |= vector.is_err();
            let stage = Instant::now();
            let counters = self.mirror.counters();
            // The sub-query's own selection replaces the previous one.
            self.filter_selection = None;
            self.apply_filters(args.sub_query_filter(sub).as_ref())
                .await?;
            let predicate = self.request_predicate()?;
            self.stage(&mut telemetry, "predicate", stage, counters);
            let limit = sub.limit as usize;
            let (mut dense, mut lexical) = (Vec::new(), Vec::new());
            if self.selected_rows().max_len().unwrap_or(0) != 0 {
                if let Some(text) = &sub.query {
                    let start = Instant::now();
                    let (rows, fetched, bound_hits) = Self::lexical(
                        &self.dataset,
                        self.selected_rows(),
                        Self::fts_requires_sql_prefilter(&self.dataset, &self.indexes)
                            .then_some(predicate.as_str()),
                        text,
                        false,
                        lexical_candidates,
                    )
                    .await?;
                    lexical = rows;
                    telemetry.lexical_candidates_fetched += fetched;
                    telemetry.lexical_tie_bound_hits += bound_hits;
                    *telemetry.stage_us.entry("lexical".into()).or_default() +=
                        micros(start.elapsed());
                }
                if let Ok(vector) = &vector {
                    dense = self
                        .dense(vector, dense_candidates.max(limit), false, &mut telemetry)
                        .await?;
                }
            }
            let stage = Instant::now();
            let depth = crate::rerank::configured(self, args.rerank.as_ref())
                .map_or(limit, |rerank| limit.max(rerank.candidates));
            ranked.push(if sub.query.is_some() {
                compose(
                    dense,
                    lexical,
                    &self.config.namespace.composition,
                    depth,
                    &self.config.namespace.retrieval,
                )?
            } else {
                crate::retrieval::dense_only(dense, depth)?
            });
            self.stage(&mut telemetry, "fusion", stage, self.mirror.counters());
        }
        // Every row was admitted by the caller's permissions and its own
        // sub-query's filter; reranking and hydration check the permissions again.
        self.filter_selection = None;
        let mut degraded = if embedding_failed {
            vec![Lane::Dense]
        } else {
            vec![]
        };
        // Nothing to rerank against without text; a request may supply it.
        let scored_text = args.rerank.as_ref().and_then(|r| r.query.as_deref());
        let texts: Vec<&str> = subs
            .iter()
            .map(|sub| scored_text.or(sub.query.as_deref()).unwrap_or(""))
            .collect();
        let unscored: Vec<_> = (0..subs.len())
            .filter(|index| texts[*index].is_empty())
            .map(|index| (index, std::mem::take(&mut ranked[index])))
            .collect();
        if crate::rerank::rerank(
            self,
            &texts,
            &mut ranked,
            args.rerank.as_ref(),
            &mut telemetry,
        )
        .await?
        {
            degraded.push(Lane::Rerank);
        }
        for (index, rows) in unscored {
            ranked[index] = rows;
        }
        // Rows past a sub-query's limit, kept for the reranker, never reach the fusion.
        for (sub, rows) in subs.iter().zip(&mut ranked) {
            rows.truncate(sub.limit as usize);
        }
        let queries: Vec<&str> = subs
            .iter()
            .map(|sub| sub.query.as_deref().unwrap_or(""))
            .collect();
        let selected = select_hits(
            ranked,
            &queries,
            args,
            args.output.unwrap_or(OutputMode::Excerpt),
            args.effective_limit() as usize,
            max_documents,
        );
        self.hydrate_selected(selected, degraded, telemetry, None, args.fields.as_ref())
            .await
    }

    async fn lexical_batch(
        &self,
        args: &SearchInput,
        queries: &[&str],
        lexical_predicate: Option<&str>,
        fallback_needed: bool,
        fallback_telemetry: Telemetry,
    ) -> (Vec<LexicalQuery>, u64, u64, Option<Result<SearchOutput>>) {
        let arm_started = Instant::now();
        let k = args.effective_limit() as usize;
        let output = args.output.unwrap_or(OutputMode::Excerpt);
        let max_documents = if args.queries.is_some() {
            self.config.namespace.retrieval.max_documents
        } else {
            k
        };
        let mut results = Vec::with_capacity(queries.len());
        let mut elapsed = 0;
        for query in queries {
            let start = Instant::now();
            // A vector without text has no lexical lane.
            let result = if !args.has_text() {
                Ok((Vec::new(), 0, 0))
            } else {
                Self::lexical(
                    &self.dataset,
                    self.selected_rows(),
                    lexical_predicate,
                    query,
                    args.query_match == Some(TermMatch::All),
                    self.config.namespace.retrieval.lexical_candidates,
                )
                .await
            };
            elapsed += micros(start.elapsed());
            results.push(result);
        }
        let fallback = if fallback_needed {
            Some(
                async {
                    let mut ranked = Vec::with_capacity(results.len());
                    for result in &results {
                        let (rows, _, _) = result.as_ref().map_err(|code| *code)?;
                        ranked.push(compose(
                            Vec::new(),
                            rows.clone(),
                            &self.config.namespace.composition,
                            k,
                            &self.config.namespace.retrieval,
                        )?);
                    }
                    let selected = select_hits(ranked, queries, args, output, k, max_documents);
                    self.hydrate_selected(
                        selected,
                        vec![Lane::Dense],
                        fallback_telemetry,
                        args.text_hits
                            .filter(|_| args.order == Some(SearchOrder::UpdatedAt))
                            .map(|count| (count, output)),
                        args.fields.as_ref(),
                    )
                    .await
                }
                .await,
            )
        } else {
            None
        };
        if let Some(slot) = &self.execution_slot {
            slot.lexical_finished();
            #[cfg(test)]
            if !slot.held() {
                self.test_hooks.embedding_parked.notify_one();
            }
        }
        (results, elapsed, micros(arm_started.elapsed()), fallback)
    }

    async fn dense_batch(
        &self,
        args: &SearchInput,
        queries: &[&str],
        embedding_deadline: Instant,
        runtime: &embedding::QueryEmbeddings,
        vectors: &mut embedding::QueryWork,
        telemetry: &mut Telemetry,
    ) -> Result<(Vec<DenseQuery>, bool)> {
        self.authorization()?;
        let waiting = self.config.embedding.reader_live
            && self.selected_rows().max_len().unwrap_or(0) != 0
            && vectors.vectors.iter().any(Result::is_err);
        if waiting && let Some(slot) = &self.execution_slot {
            slot.begin_provider_wait();
        }
        runtime
            .complete(
                vectors,
                queries,
                self.selected_rows().max_len().unwrap_or(0) != 0,
                embedding_deadline,
            )
            .await;
        let mut failed = vectors.vectors.iter().any(Result::is_err);
        if waiting && let Some(slot) = &self.execution_slot {
            if failed {
                // A failed provider never queues away the lexical answer.
                slot.try_resume();
            } else {
                match tokio::time::timeout_at(
                    tokio::time::Instant::from_std(embedding_deadline),
                    slot.resume(),
                )
                .await
                {
                    Ok(result) => result?,
                    Err(_) => failed = true,
                }
            }
        }
        // An exact search never settles for the lexical lane alone.
        if failed && args.exact == Some(true) {
            return Err(EmbeddingUnavailable);
        }
        let mut results = Vec::with_capacity(queries.len());
        for vector in &mut vectors.vectors {
            let vector = if failed {
                Err(EmbeddingUnavailable)
            } else {
                std::mem::replace(vector, Err(EmbeddingUnavailable))
            };
            let result = match vector {
                Ok(vector) => self
                    .dense(
                        &vector,
                        // The request limit is the least depth that can fill it.
                        self.config
                            .namespace
                            .retrieval
                            .dense_candidates
                            .max(args.effective_limit() as usize),
                        args.exact == Some(true),
                        telemetry,
                    )
                    .await
                    .map(|rows| (rows, Some(vector))),
                Err(_) => Ok((Vec::new(), None)),
            };
            results.push(result);
        }
        Ok::<_, ReaderError>((results, failed))
    }

    fn compose_queries(
        &mut self,
        args: &SearchInput,
        predicate: &str,
        lexical: Vec<LexicalQuery>,
        dense: Vec<DenseQuery>,
        completion: &mut Option<(&mut Option<DenseCompletion>, usize)>,
        telemetry: &mut Telemetry,
    ) -> Result<Vec<Vec<Candidate>>> {
        let k = args.effective_limit() as usize;
        let mut ranked_queries = Vec::new();
        for (query_index, (lexical, dense)) in lexical.into_iter().zip(dense).enumerate() {
            let (dense, vector) = dense?;
            let stage = Instant::now();
            let (lexical, fetched, bound_hits) = lexical?;
            telemetry.lexical_candidates_fetched += fetched;
            telemetry.lexical_tie_bound_hits += bound_hits;
            #[cfg(test)]
            self.trace_stages(|stages| {
                let keys = |rows: &[Candidate]| {
                    rows.iter()
                        .map(|row| serde_json::json!([row.source, row.root, row.record, row.score]))
                        .collect::<Vec<_>>()
                };
                *stages = serde_json::json!({"dense":keys(&dense),"lexical":keys(&lexical)});
            });
            if let (Some((completion, completion_query)), Some(vector)) =
                (completion.as_mut(), vector)
                && query_index == *completion_query
                && completion.is_none()
                // The recall reference scans by SQL, which `contains` is not.
                && self.filter_selection.as_ref().is_none_or(|f| !f.masked)
            {
                let mut rows = [0; fyrr_core::limits::mcp::MAX_SEARCH_LIMIT as usize];
                for (id, candidate) in rows.iter_mut().zip(&dense) {
                    *id = candidate.row_id;
                }
                **completion = Some((
                    vector,
                    rows,
                    dense
                        .len()
                        .min(fyrr_core::limits::mcp::MAX_SEARCH_LIMIT as usize),
                    predicate.to_owned(),
                ));
            }
            let depth = crate::rerank::configured(self, args.rerank.as_ref())
                .map_or(k, |rerank| k.max(rerank.candidates));
            let selected = if args.has_text() {
                compose(
                    dense,
                    lexical,
                    &self.config.namespace.composition,
                    depth,
                    &self.config.namespace.retrieval,
                )?
            } else {
                crate::retrieval::dense_only(dense, depth)?
            };
            #[cfg(test)]
            self.trace_stages(|stages| {
                stages["composition"] = serde_json::json!(
                    selected
                        .iter()
                        .map(|row| serde_json::json!([row.source, row.root, row.record]))
                        .collect::<Vec<_>>()
                );
            });
            self.stage(telemetry, "fusion", stage, self.mirror.counters());
            ranked_queries.push(selected);
        }

        Ok(ranked_queries)
    }

    async fn finish_search_batch(
        &mut self,
        args: &SearchInput,
        queries: &[&str],
        mut ranked_queries: Vec<Vec<Candidate>>,
        embedding_failed: bool,
        mut fallback: Option<Result<SearchOutput>>,
        mut telemetry: Telemetry,
    ) -> Result<SearchOutput> {
        let k = args.effective_limit() as usize;
        let output = args.output.unwrap_or(OutputMode::Excerpt);
        let max_documents = if args.queries.is_some() {
            self.config.namespace.retrieval.max_documents
        } else {
            k
        };
        let mut degraded = if embedding_failed {
            vec![Lane::Dense]
        } else {
            vec![]
        };
        if self
            .execution_slot
            .as_ref()
            .is_some_and(|slot| !slot.held())
        {
            let mut result = fallback.take().ok_or(StorageUnavailable)??;
            for (stage, micros) in result.metadata.telemetry.stage_us {
                if matches!(stage.as_str(), "metadata" | "excerpts") {
                    telemetry.stage_us.insert(stage, micros);
                }
            }
            // No native hydration or rerank preparation may run on a parked slot.
            // Optional reranking fails open to the prepared composition in this
            // contended deadline path; normal reranking retains its existing path.
            if crate::rerank::configured(self, args.rerank.as_ref()).is_some()
                && !result.hits.is_empty()
            {
                degraded.push(Lane::Rerank);
            }
            result.metadata.degraded = degraded;
            result.metadata.telemetry = telemetry;
            return Ok(result);
        }
        drop(fallback);
        // Nothing to rerank against without text; a request may supply it.
        let scored_text = args.rerank.as_ref().and_then(|r| r.query.as_deref());
        let texts: Vec<&str> = queries
            .iter()
            .map(|query| scored_text.unwrap_or(query))
            .collect();
        if (args.has_text() || scored_text.is_some())
            && crate::rerank::rerank(
                self,
                &texts,
                &mut ranked_queries,
                args.rerank.as_ref(),
                &mut telemetry,
            )
            .await?
        {
            degraded.push(Lane::Rerank);
        }
        let selected = select_hits(ranked_queries, queries, args, output, k, max_documents);
        self.hydrate_selected(
            selected,
            degraded,
            telemetry,
            args.text_hits
                .filter(|_| args.order == Some(SearchOrder::UpdatedAt))
                .map(|count| (count, output)),
            args.fields.as_ref(),
        )
        .await
    }

    pub(crate) fn stage(
        &self,
        telemetry: &mut Telemetry,
        name: &str,
        start: Instant,
        before: crate::mirror::IoSnapshot,
    ) {
        let after = self.mirror.counters();
        telemetry
            .stage_us
            .entry(name.into())
            .and_modify(|us| *us += micros(start.elapsed()))
            .or_insert_with(|| micros(start.elapsed()));
        telemetry
            .stage_s3_requests
            .entry(name.into())
            .and_modify(|count| *count += after.requests - before.requests)
            .or_insert(after.requests - before.requests);
        telemetry
            .stage_s3_bytes
            .entry(name.into())
            .and_modify(|bytes| *bytes += after.bytes - before.bytes)
            .or_insert(after.bytes - before.bytes);
    }
}
