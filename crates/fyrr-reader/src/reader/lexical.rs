use super::*;
use fyrr_core::attributes::{AttributeType, attribute_index_name};
use lance_index::scalar::{
    FullTextSearchQuery,
    inverted::query::{BooleanQuery, FtsQuery, MatchQuery, Occur, Operator},
};

/// What the text lane of a query matches and scores: whether a document needs
/// every term, and the full-text attributes it scores with their weights. None
/// named: `searchable_text`.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct TextMatch {
    pub(crate) all_terms: bool,
    pub(crate) attributes: Vec<(String, f32)>,
}

impl TextMatch {
    const TEXT: &'static str = "searchable_text";

    /// The match query of the text lane. One attribute is a match query with
    /// its weight as the boost; several are a boolean query of one optional
    /// match per attribute, which scores a document by the sum of the weighted
    /// scores (the pin test records that its best ten are the true best ten).
    fn query(&self, text: &str) -> FtsQuery {
        let operator = if self.all_terms {
            Operator::And
        } else {
            Operator::Or
        };
        let one = |attribute: &str, weight: f32| {
            FtsQuery::Match(
                MatchQuery::new(text.into())
                    .with_column(Some(attribute.into()))
                    .with_operator(operator)
                    .with_boost(weight),
            )
        };
        match self.attributes.as_slice() {
            [] => one(Self::TEXT, 1.0),
            [(attribute, weight)] => one(attribute, *weight),
            several => FtsQuery::Boolean(BooleanQuery::new(
                several
                    .iter()
                    .map(|(attribute, weight)| (Occur::Should, one(attribute, *weight))),
            )),
        }
    }

    /// The names of the full-text indexes the query reads.
    fn indexes(&self) -> Vec<String> {
        if self.attributes.is_empty() {
            return vec![Layout::FTS_INDEX.into()];
        }
        self.attributes
            .iter()
            .map(|(attribute, _)| {
                if attribute == Self::TEXT {
                    Layout::FTS_INDEX.into()
                } else {
                    attribute_index_name(attribute, IndexOption::FullText)
                }
            })
            .collect()
    }
}

impl PreparedReader {
    /// The text lane of a request: each attribute it ranks by must have a
    /// full-text index a query may read (as for `contains`).
    /// shortcut: a string array stands alone, because Lance takes no query over
    /// an index of one document per element and one of one per row together
    /// (the pin test records it); lift it when a newer Lance does.
    pub(super) fn text_match(&self, args: &SearchInput) -> Result<TextMatch> {
        let mut attributes = Vec::new();
        for named in args.query_attributes.iter().flatten() {
            if named.attribute != TextMatch::TEXT {
                self.check_full_text(&named.attribute)?;
            }
            // The weight is validated to lie in (0, 100]: no precision that matters is lost.
            #[allow(clippy::cast_possible_truncation)]
            attributes.push((named.attribute.clone(), named.weight() as f32));
        }
        if attributes.len() > 1
            && attributes.iter().any(|(attribute, _)| {
                matches!(
                    self.manifest.attributes.get(attribute),
                    Some(AttributeType::Array(_))
                )
            })
        {
            return Err(InvalidArgument);
        }
        Ok(TextMatch {
            all_terms: args.query_match == Some(TermMatch::All),
            attributes,
        })
    }

    pub(super) fn fts_requires_sql_prefilter(
        dataset: &Dataset,
        indexes: &[lance_table::format::IndexMetadata],
        text: &TextMatch,
    ) -> bool {
        // Flat FTS computes corpus statistics before applying an external mask.
        // Unknown coverage and overlays conservatively retain the SQL filter;
        // checking resident manifest metadata needs no storage reads or bitmap copy.
        // Every index the query reads must cover the fragment: one that does not
        // is read flat for it, whichever attribute it indexes.
        let names = text.indexes();
        dataset.manifest().fragments.iter().any(|fragment| {
            !fragment.overlays.is_empty()
                || names.iter().any(|name| {
                    !indexes.iter().any(|index| {
                        index.name == *name
                            && index.fragment_bitmap.as_ref().is_some_and(|coverage| {
                                coverage.contains(coverage_fragment(fragment.id))
                            })
                    })
                })
        })
    }

    pub(super) async fn lexical(
        dataset: &Dataset,
        eligible: &RowAddrMask,
        predicate: Option<&str>,
        query: &str,
        text: &TextMatch,
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
                FullTextSearchQuery::new_query(text.query(query)).limit(Some(requested as i64)),
            )
            .map_err(|_| IncompatibleIndex)?;
            let batch = scan
                .try_into_batch()
                .await
                .map_err(|_| StorageUnavailable)?;
            let mut rows = Self::lane_rows(&batch, "_score", false, eligible)?;
            fetched += rows.len() as u64;
            let saturated = rows.len() == requested;
            let cutoff = rows.last().map(|row| row.score);
            // The best element ranks a row. An array indexed per element returns
            // a row once for each element that matches, and the fetch is bounded:
            // one row of more matching elements than `fetch_bound` fills it, and
            // the rows after it are lost.
            // shortcut: no refill beyond `fetch_bound`, which the memory claim of
            // a request covers; the bound is reported as `lexical_tie_bound_hits`.
            // Lift it with a claim for a larger fetch when arrays of thousands of
            // matching elements are common.
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| seen.insert(row.row_id));
            let unresolved = saturated
                && (rows.len() <= limit
                    || cutoff
                        .is_some_and(|cutoff| rows[limit - 1].score.total_cmp(&cutoff).is_eq()));
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
                lexical: None,
                distance: None,
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
