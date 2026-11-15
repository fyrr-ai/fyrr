use super::*;

impl PreparedReader {
    pub(super) fn fts_requires_sql_prefilter(
        dataset: &Dataset,
        indexes: &[lance_table::format::IndexMetadata],
    ) -> bool {
        // Flat FTS computes corpus statistics before applying an external mask.
        // Unknown coverage and overlays conservatively retain the SQL filter;
        // checking resident manifest metadata needs no storage reads or bitmap copy.
        dataset.manifest().fragments.iter().any(|fragment| {
            !fragment.overlays.is_empty()
                || !indexes.iter().any(|index| {
                    index.name == Layout::FTS_INDEX
                        && index.fragment_bitmap.as_ref().is_some_and(|coverage| {
                            coverage.contains(coverage_fragment(fragment.id))
                        })
                })
        })
    }

    pub(super) async fn lexical(
        dataset: &Dataset,
        eligible: &RowAddrMask,
        predicate: Option<&str>,
        query: &str,
        candidates: usize,
    ) -> Result<(Vec<Candidate>, u64, u64)> {
        if eligible.max_len().unwrap_or(0) == 0 {
            return Ok((Vec::new(), 0, 0));
        }
        let limit = candidates;
        let fetch_bound = candidates
            .saturating_mul(1024)
            .div_ceil(50)
            .max(candidates + 1);
        // One lookahead is sufficient when the cutoff is strictly above its lookahead.
        // Lance's top-k does not resolve equal scores by our logical key.
        let mut requested = limit + 1;
        let mut fetched = 0;
        loop {
            let mut scan = dataset.scan();
            scan.project(&["source_id", "root_id", "record_id"])
                .map_err(|_| IncompatibleIndex)?;
            // The mask narrows indexed top-k. Flat tail/overlay scoring also
            // needs SQL admission before collecting N, avgdl and term frequency.
            if let Some(predicate) = predicate {
                scan.filter(predicate).map_err(|_| IncompatibleIndex)?;
            }
            scan.prefilter(true)
                .with_row_id_prefilter(eligible.clone())
                .with_row_id();
            scan.full_text_search(
                lance_index::scalar::FullTextSearchQuery::new(query.into())
                    .with_column("searchable_text".into())
                    .map_err(|_| IncompatibleIndex)?
                    .limit(Some(requested as i64)),
            )
            .map_err(|_| IncompatibleIndex)?;
            let batch = scan
                .try_into_batch()
                .await
                .map_err(|_| StorageUnavailable)?;
            let mut rows = Self::lane_rows(&batch, "_score", false, eligible)?;
            fetched += rows.len() as u64;
            let unresolved = rows.len() == requested
                && rows.len() > limit
                && rows[limit - 1]
                    .score
                    .total_cmp(&rows[rows.len() - 1].score)
                    .is_eq();
            if !unresolved || requested == fetch_bound {
                rows.truncate(limit);
                // At the bound only the fetched subset is stable-key ordered;
                // telemetry explicitly records incomplete cutoff-tie coverage.
                return Ok((rows, fetched, u64::from(unresolved)));
            }
            requested = (requested * 2).min(fetch_bound);
        }
    }
    pub(super) fn lane_rows(
        batch: &RecordBatch,
        score_column: &str,
        distance: bool,
        eligible: &RowAddrMask,
    ) -> Result<Vec<Candidate>> {
        let scores = column::<Float32Array>(batch, score_column, SearchResultInvalid)?;
        let mut rows = Vec::new();
        for row in 0..batch.num_rows() {
            let id = ids(batch)?.value(row);
            if !eligible.selected(id) {
                return Err(AuthorizationUnavailable);
            }
            let score = f64::from(scores.value(row)) * if distance { -1.0 } else { 1.0 };
            if !score.is_finite() {
                return Err(StorageUnavailable);
            }
            rows.push(Candidate {
                row_id: id,
                source: strings(batch, "source_id")?.value(row).into(),
                root: strings(batch, "root_id")?.value(row).into(),
                record: strings(batch, "record_id")?.value(row).into(),
                score,
            });
        }
        rows.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then((&a.source, &a.root, &a.record).cmp(&(&b.source, &b.root, &b.record)))
        });
        Ok(rows)
    }
}
