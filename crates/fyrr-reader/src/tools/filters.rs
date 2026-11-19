use super::*;
use crate::search::FilterSelection;

impl PreparedReader {
    pub(crate) fn metadata(&self, telemetry: Telemetry) -> ResponseMetadata {
        ResponseMetadata {
            schema_version: MCP_CONTRACT_VERSION,
            publication_id: self.manifest.publication_id.clone(),
            content_version: self.manifest.lance_version,
            committed_at: McpTime(self.manifest.committed_at),
            degraded: vec![],
            hidden_sources: self.reported_hidden_sources(),
            stale_version: false,
            omitted_bytes: 0,
            telemetry,
        }
    }
    pub(crate) fn finish_metadata(
        &self,
        metadata: &mut ResponseMetadata,
        start: Instant,
        before: crate::mirror::IoSnapshot,
    ) {
        let after = self.mirror.counters();
        metadata.stale_version = self.config.reader.version_check
            == fyrr_core::config::VersionCheck::PerRequest
            && self
                .head
                .content
                .as_ref()
                .is_some_and(|p| p.sequence != self.manifest.sequence);
        metadata.telemetry.s3_requests = after.requests - before.requests;
        metadata.telemetry.s3_bytes = after.bytes - before.bytes;
        metadata.telemetry.cache = if after.hits > before.hits {
            CacheState::Warm
        } else {
            CacheState::Hot
        };
        metadata
            .telemetry
            .stage_us
            .insert("total".into(), micros(start.elapsed()));
    }
    /// The rows of `dataset` that satisfy `predicate`, which is the admission
    /// predicate and the request filter. The scan carries no row mask, so
    /// Lance answers both sides from their scalar indexes; an external mask
    /// would demote the whole filter to a read of every admitted row. The
    /// predicate was built from the same state as `admitted`, and every row it
    /// selects is checked against `admitted`, so a row outside the admission
    /// fails closed.
    pub(crate) async fn filter_rows(
        dataset: &lance::Dataset,
        predicate: &str,
        admitted: &RowAddrMask,
    ) -> Result<RowAddrTreeMap> {
        let scan = Self::filter_scan(dataset, predicate)?;
        let mut allowed = RowAddrTreeMap::new();
        let mut stream = scan
            .try_into_stream()
            .await
            .map_err(|_| StorageUnavailable)?;
        while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
            for id in ids(&batch)?.values() {
                if !admitted.selected(*id) {
                    return Err(AuthorizationUnavailable);
                }
                allowed.insert(*id);
            }
        }
        Ok(allowed)
    }
    /// The scan of `filter_rows`: no projection and no row mask.
    pub(crate) fn filter_scan(
        dataset: &lance::Dataset,
        predicate: &str,
    ) -> Result<lance::dataset::scanner::Scanner> {
        let mut scan = dataset.scan();
        scan.project::<&str>(&[]).map_err(|_| IncompatibleIndex)?;
        scan.filter(predicate)
            .map_err(|_| IncompatibleIndex)?
            .with_row_id();
        Ok(scan)
    }
    pub(crate) async fn apply_filters(&mut self, args: &SearchInput) -> Result<()> {
        let mut terms = Vec::new();
        for (column, values) in [
            ("source_id", &args.sources),
            ("source_type", &args.source_types),
            ("project", &args.projects),
            ("author", &args.authors),
        ] {
            if let Some(values) = values {
                terms.push(format!(
                    "{column} IN ({})",
                    values.iter().map(|v| sql(v)).collect::<Vec<_>>().join(",")
                ));
            }
        }
        for (op, bound) in [(">", args.updated_after), ("<", args.updated_before)] {
            if let Some(bound) = bound {
                terms.push(format!(
                    "updated_at {op} timestamp '{}'",
                    bound.0.format("%Y-%m-%d %H:%M:%S%.6f")
                ));
            }
        }
        if let Some(excluded) = &args.exclude {
            terms.push(format!(
                "NOT ({})",
                excluded
                    .iter()
                    .map(|k| format!(
                        "(source_id={} AND root_id={})",
                        sql(&k.source_id),
                        sql(&k.root_id)
                    ))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ));
        }
        if let Some(attributes) = &args.attributes {
            for attribute in attributes {
                terms.push(format!(
                    "array_has_any(attributes,[{}])",
                    attribute
                        .values
                        .iter()
                        .map(|v| sql(&format!("{}={v}", attribute.key)))
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
        }
        if terms.is_empty() {
            return Ok(());
        }
        let filter = terms.join(" AND ");
        let predicate = format!("({}) AND ({filter})", self.admission_predicate()?);
        #[cfg(test)]
        self.test_hooks.filter_scan.wait_once().await;
        let allowed =
            Self::filter_rows(&self.dataset, &predicate, &self.admission.eligible).await?;
        self.filter_selection = Some(FilterSelection {
            predicate: filter,
            eligible: RowAddrMask::from_allowed(allowed),
        });
        Ok(())
    }
    pub(crate) async fn listing(
        &self,
        args: &SearchInput,
        telemetry: Telemetry,
    ) -> Result<SearchOutput> {
        let mut scan = self.dataset.scan();
        scan.project(&["source_id", "root_id", "record_id"])
            .map_err(|_| IncompatibleIndex)?;
        scan.filter(&self.request_predicate()?)
            .map_err(|_| IncompatibleIndex)?
            .with_row_id()
            .with_row_id_prefilter(self.selected_rows().clone());
        scan.order_by(Some(vec![
            ordering("updated_at", false),
            ordering("source_id", true),
            ordering("root_id", true),
            ordering("record_id", true),
        ]))
        .map_err(|_| IncompatibleIndex)?;
        let mut stream = scan
            .try_into_stream()
            .await
            .map_err(|_| StorageUnavailable)?;
        let mut selected = Vec::new();
        let mut roots = BTreeSet::new();
        let limit = args.limit.unwrap_or(50) as usize;
        'scan: while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
            for row in 0..batch.num_rows() {
                let source = strings(&batch, "source_id")?.value(row);
                let root = strings(&batch, "root_id")?.value(row);
                if roots.insert((source.to_owned(), root.to_owned())) {
                    selected.push(Candidate {
                        row_id: ids(&batch)?.value(row),
                        source: source.into(),
                        root: root.into(),
                        record: strings(&batch, "record_id")?.value(row).into(),
                        score: 0.0,
                    });
                    if selected.len() == limit {
                        break 'scan;
                    }
                }
            }
        }
        self.hydrate_selected(
            selected
                .into_iter()
                .enumerate()
                .map(|(rank, candidate)| crate::search::SelectedHit {
                    candidate,
                    query: "",
                    query_index: None,
                    output: if rank < args.text_hits.unwrap_or(args.effective_limit()) as usize {
                        args.output.unwrap_or(OutputMode::Excerpt)
                    } else {
                        OutputMode::Metadata
                    },
                    excerpt_chars: args
                        .excerpt_chars
                        .unwrap_or(fyrr_core::limits::mcp::EXCERPT_CODEPOINTS),
                })
                .collect(),
            vec![],
            telemetry,
            None,
        )
        .await
    }
    pub async fn list_filters(&mut self, args: &ListFiltersInput) -> Result<ListFiltersOutput> {
        args.validate().map_err(|_| InvalidArgument)?;
        let (start, before, mut telemetry, _pin) = self.request_prologue().await?;
        let outcome = async {
            let stage = Instant::now();
            let counters = self.mirror.counters();
            let mut scan = self.dataset.scan();
            scan.project(&["source_id", "root_id", "project", "source_type", "author"])
                .map_err(|_| IncompatibleIndex)?;
            scan.filter(&self.request_predicate()?)
                .map_err(|_| IncompatibleIndex)?
                .with_row_id_prefilter(self.selected_rows().clone());
            scan.limit(
                Some((self.config.reader.filter_count_max_rows + 1) as i64),
                None,
            )
            .map_err(|_| IncompatibleIndex)?;
            let mut stream = scan
                .try_into_stream()
                .await
                .map_err(|_| StorageUnavailable)?;
            let mut counts: [BTreeMap<String, u64>; 3] = Default::default();
            let mut seen = BTreeSet::new();
            let mut cost = 0;
            while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
                cost += batch.num_rows();
                if cost > self.config.reader.filter_count_max_rows {
                    return Err(Busy);
                }
                for row in 0..batch.num_rows() {
                    let key = (
                        strings(&batch, "source_id")?.value(row).to_owned(),
                        strings(&batch, "root_id")?.value(row).to_owned(),
                    );
                    if !seen.insert(key) {
                        continue;
                    }
                    for (map, column) in counts.iter_mut().zip(["project", "source_type", "author"])
                    {
                        let values = strings(&batch, column)?;
                        if !values.is_null(row) && map.len() <= 64 {
                            *map.entry(values.value(row).into()).or_default() += 1;
                        }
                    }
                }
            }
            let values = |map: BTreeMap<String, u64>| {
                if map.len() > 64 {
                    FilterValues {
                        status: FilterStatus::TooMany,
                        values: vec![],
                    }
                } else {
                    FilterValues {
                        status: FilterStatus::Values,
                        values: map
                            .into_iter()
                            .map(|(value, count)| FilterValue { value, count })
                            .collect(),
                    }
                }
            };
            let [projects, source_types, authors] = counts;
            telemetry.filter_rows_scanned = cost as u64;
            self.stage(&mut telemetry, "filter_counts", stage, counters);
            Ok(ListFiltersOutput {
                projects: values(projects),
                source_types: values(source_types),
                authors: values(authors),
                metadata: self.metadata(telemetry),
            })
        }
        .await;
        let stage = Instant::now();
        let counters = self.mirror.counters();
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
        self.finish_metadata(&mut result.metadata, start, before);
        Ok(result)
    }
}
