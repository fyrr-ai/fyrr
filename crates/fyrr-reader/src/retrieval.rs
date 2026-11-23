//! Complete-union logistic-z scoring, document grouping and slot composition.
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
fn documents(rows: Vec<Candidate>) -> Vec<Candidate> {
    let mut seen = BTreeSet::new();
    rows.into_iter()
        .filter(|r| seen.insert((r.source.clone(), r.root.clone())))
        .collect()
}
/// Adds `bonus[i]` to the score of `rows[i]` and ranks the rows again, best
/// first, ties by record key. Rows scored by distance have no fixed scale, so
/// they are normalized first, as a lane is: a bonus counts against a score
/// between 0 and 1.
pub(crate) fn rescored(rows: &mut [Candidate], bonus: &[f64], by_distance: bool) {
    if by_distance {
        normalize(rows);
    }
    for (row, bonus) in rows.iter_mut().zip(bonus) {
        row.score += bonus;
    }
    rows.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.key().cmp(&b.key())));
}
/// A search without text has one lane, so the documents keep its order by
/// distance: no fusion weight may reorder or zero them.
pub(crate) fn dense_only(dense: Vec<Candidate>, limit: usize) -> Result<Vec<Candidate>> {
    let mut rows = ranked(dense)?;
    for row in &mut rows {
        row.distance = Some(-row.score);
    }
    let mut rows = documents(rows);
    rows.truncate(limit);
    Ok(rows)
}
pub(crate) fn compose(
    dense: Vec<Candidate>,
    lexical: Vec<Candidate>,
    config: &Composition,
    limit: usize,
    retrieval: &RetrievalConfig,
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
    let fusion = documents(ranked(union.values().cloned().collect())?);
    let dense = documents(dense);
    let lexical = documents(lexical);
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
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
            if seen.contains(&(row.source.clone(), row.root.clone())) {
                continue;
            }
            if retrieval.source_cap != 0
                && sources.get(&row.source).copied().unwrap_or(0) == retrieval.source_cap
            {
                skipped.push(row);
                continue;
            }
            seen.insert((row.source.clone(), row.root.clone()));
            *sources.entry(row.source.clone()).or_default() += 1;
            selected.push(row);
            added += 1;
        }
        // The source cap spreads each bucket's head; it never shortens the bucket.
        for row in skipped.into_iter().take(slots - added) {
            seen.insert((row.source.clone(), row.root.clone()));
            selected.push(row);
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
