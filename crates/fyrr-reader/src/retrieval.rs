//! Complete-union logistic-z scoring, document grouping, value caps and slot composition.
use crate::error::*;
use fyrr_core::config::{Composition, RetrievalConfig};
use fyrr_core::mcp::Fusion;
use std::collections::{BTreeMap, BTreeSet};

/// An authorized lane row; text is loaded only after document selection.
#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub row_id: u64,
    pub source: String,
    pub root: String,
    pub record: String,
    pub score: f64,
    /// The raw lexical score and vector distance, kept for `fields.scores`.
    pub lexical: Option<f64>,
    pub distance: Option<f64>,
}
impl Candidate {
    fn key(&self) -> (&str, &str, &str) {
        (&self.source, &self.root, &self.record)
    }
}
fn ranked(mut rows: Vec<Candidate>) -> Result<Vec<Candidate>> {
    if rows.iter().any(|r| !r.score.is_finite()) {
        return Err(StorageUnavailable);
    }
    rows.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.key().cmp(&b.key())));
    let mut seen = BTreeSet::new();
    rows.retain(|r| seen.insert((r.source.clone(), r.root.clone(), r.record.clone())));
    Ok(rows)
}
fn normalize(rows: &mut [Candidate]) {
    if rows.is_empty() {
        return;
    }
    let mean = rows.iter().map(|r| r.score).sum::<f64>() / rows.len() as f64;
    let std =
        (rows.iter().map(|r| (r.score - mean).powi(2)).sum::<f64>() / rows.len() as f64).sqrt();
    for row in rows {
        row.score = if std == 0.0 {
            0.5
        } else {
            1.0 / (1.0 + (-((row.score - mean) / std).clamp(-60.0, 60.0)).exp())
        };
    }
}
/// What a request changes in the selection of hits beyond the namespace's
/// composition. The default is today's: the best record of each document.
#[derive(Debug, Default)]
pub(crate) struct Selection {
    /// Every record is a hit of its own, not only the best of its document.
    pub records: bool,
    /// The caps on the hits per value of an attribute.
    pub caps: Vec<ValueCap>,
}
/// At most `max` hits per value of one attribute; a row without a value is not
/// capped.
#[derive(Debug)]
pub(crate) struct ValueCap {
    pub max: usize,
    /// The value of each candidate row, by row id.
    pub values: BTreeMap<u64, String>,
}
impl Selection {
    /// What counts as one hit: a document, or with `records` a record.
    fn unit(&self, row: &Candidate) -> (String, String, String) {
        (
            row.source.clone(),
            row.root.clone(),
            if self.records {
                row.record.clone()
            } else {
                String::new()
            },
        )
    }
    /// The best row of each document, or with `records` every row, in the order of `rows`.
    fn group(&self, rows: Vec<Candidate>) -> Vec<Candidate> {
        if self.records {
            // `ranked` already left one row per record.
            return rows;
        }
        let mut seen = BTreeSet::new();
        rows.into_iter()
            .filter(|row| seen.insert(self.unit(row)))
            .collect()
    }
}
/// How many hits each value of each capped attribute has among those selected.
struct Tally<'a> {
    caps: &'a [ValueCap],
    counts: Vec<BTreeMap<&'a str, usize>>,
}
impl<'a> Tally<'a> {
    fn new(selection: &'a Selection) -> Self {
        Self {
            caps: &selection.caps,
            counts: vec![BTreeMap::new(); selection.caps.len()],
        }
    }
    /// Whether a value of the row already has its most hits.
    fn full(&self, row: &Candidate) -> bool {
        self.caps.iter().zip(&self.counts).any(|(cap, counts)| {
            cap.values
                .get(&row.row_id)
                .is_some_and(|value| counts.get(value.as_str()).copied().unwrap_or(0) >= cap.max)
        })
    }
    fn take(&mut self, row: &Candidate) {
        for (cap, counts) in self.caps.iter().zip(&mut self.counts) {
            if let Some(value) = cap.values.get(&row.row_id) {
                *counts.entry(value.as_str()).or_default() += 1;
            }
        }
    }
}
/// What a request's boosts add to the score of a row, by row id; a row it does
/// not name gets nothing.
pub(crate) type Bonus = BTreeMap<u64, f64>;
/// A search without text has one lane, so the documents keep its order by
/// distance: no fusion weight may reorder or zero them.
pub(crate) fn dense_only(
    dense: Vec<Candidate>,
    limit: usize,
    bonus: &Bonus,
    selection: &Selection,
) -> Result<Vec<Candidate>> {
    let mut rows = ranked(dense)?;
    for row in &mut rows {
        row.distance = Some(-row.score);
    }
    if bonus.values().any(|bonus| *bonus != 0.0) {
        // A distance has no fixed scale: normalize it as a lane is, so that a
        // bonus counts against a score between 0 and 1.
        normalize(&mut rows);
        for row in &mut rows {
            row.score += bonus.get(&row.row_id).copied().unwrap_or(0.0);
        }
        rows = ranked(rows)?;
    }
    let mut tally = Tally::new(selection);
    let mut rows = selection.group(rows);
    // A cap is hard: a hit over it is dropped, and the next one takes its place.
    rows.retain(|row| {
        let keep = !tally.full(row);
        if keep {
            tally.take(row);
        }
        keep
    });
    rows.truncate(limit);
    Ok(rows)
}
pub(crate) fn compose(
    dense: Vec<Candidate>,
    lexical: Vec<Candidate>,
    config: &Composition,
    limit: usize,
    retrieval: &RetrievalConfig,
    bonus: &Bonus,
    selection: &Selection,
) -> Result<Vec<Candidate>> {
    let mut dense = ranked(dense)?;
    let mut lexical = ranked(lexical)?;
    // The lane scores are normalized below; the hit reports the raw ones.
    for row in &mut dense {
        row.distance = Some(-row.score);
    }
    for row in &mut lexical {
        row.lexical = Some(row.score);
    }
    normalize(&mut dense);
    normalize(&mut lexical);
    let mut union: BTreeMap<(String, String, String), Candidate> = BTreeMap::new();
    for (lane, weight) in [
        (&dense, retrieval.dense_weight),
        (&lexical, 1.0 - retrieval.dense_weight),
    ] {
        for row in lane {
            let key = (row.source.clone(), row.root.clone(), row.record.clone());
            let entry = union.entry(key).or_insert_with(|| Candidate {
                score: 0.0,
                ..row.clone()
            });
            entry.score += row.score * weight;
            entry.lexical = entry.lexical.or(row.lexical);
            entry.distance = entry.distance.or(row.distance);
        }
    }
    // The boosts weigh in the fused score, before the composition picks its slots.
    for row in union.values_mut() {
        row.score += bonus.get(&row.row_id).copied().unwrap_or(0.0);
    }
    let fusion = selection.group(ranked(union.values().cloned().collect())?);
    let dense = selection.group(dense);
    let lexical = selection.group(lexical);
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    let mut tally = Tally::new(selection);
    for (rows, slots) in [
        (fusion, config.fusion),
        (dense, config.dense),
        (lexical, config.lexical),
    ] {
        let mut sources = BTreeMap::<String, usize>::new();
        let mut skipped = Vec::new();
        let mut added = 0;
        for row in rows {
            if added == slots {
                break;
            }
            if seen.contains(&selection.unit(&row)) || tally.full(&row) {
                continue;
            }
            if retrieval.source_cap != 0
                && sources.get(&row.source).copied().unwrap_or(0) == retrieval.source_cap
            {
                skipped.push(row);
                continue;
            }
            seen.insert(selection.unit(&row));
            tally.take(&row);
            *sources.entry(row.source.clone()).or_default() += 1;
            selected.push(row);
            added += 1;
        }
        // The source cap spreads each bucket's head; it never shortens the bucket.
        // A cap on a value does: a row over it is no hit.
        for row in skipped {
            if added == slots {
                break;
            }
            if tally.full(&row) {
                continue;
            }
            seen.insert(selection.unit(&row));
            tally.take(&row);
            selected.push(row);
            added += 1;
        }
    }
    for row in &mut selected {
        // Reserved slots retain the union score, rather than exposing lane normalization.
        let fused = union
            .get(&(row.source.clone(), row.root.clone(), row.record.clone()))
            .ok_or(StorageUnavailable)?;
        row.score = fused.score;
        row.lexical = fused.lexical;
        row.distance = fused.distance;
    }
    selected.truncate(limit);
    Ok(selected)
}

/// Reciprocal rank fusion of ranked lists of documents, one per sub-query: a
/// document scores the sum of `weight / (rank_constant + rank)`, rank counted
/// from 1, over the lists that hold it. Best first, ties by document key. A
/// hit is the candidate of the list that gave it most, with that list's index.
pub(crate) fn reciprocal_rank_fusion(
    lists: Vec<Vec<Candidate>>,
    fusion: &Fusion,
) -> Vec<(usize, Candidate)> {
    let constant = f64::from(fusion.rank_constant());
    // Per document: its score so far, and what its best list gave it.
    let mut fused: BTreeMap<(String, String), (f64, f64, usize, Candidate)> = BTreeMap::new();
    for (list, rows) in lists.into_iter().enumerate() {
        for (rank, row) in rows.into_iter().enumerate() {
            let part = fusion.weight(list) / (constant + (rank + 1) as f64);
            match fused.entry((row.source.clone(), row.root.clone())) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((part, part, list, row));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let (total, best, from, hit) = entry.get_mut();
                    *total += part;
                    if part > *best {
                        (*best, *from) = (part, list);
                        *hit = row;
                    }
                }
            }
        }
    }
    let mut rows: Vec<_> = fused
        .into_values()
        .map(|(score, _, list, row)| (list, Candidate { score, ..row }))
        .collect();
    // The map holds documents in key order; the sort is stable.
    rows.sort_by(|a, b| b.1.score.total_cmp(&a.1.score));
    rows
}

#[cfg(test)]
#[path = "retrieval/tests.rs"]
mod tests;
