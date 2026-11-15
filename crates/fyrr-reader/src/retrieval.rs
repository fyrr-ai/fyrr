//! Complete-union logistic-z scoring, document grouping and slot composition.
use crate::error::*;
use fyrr_core::config::{Composition, RetrievalConfig};
use std::collections::{BTreeMap, BTreeSet};

/// An authorized lane row; text is loaded only after document selection.
#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub row_id: u64,
    pub source: String,
    pub root: String,
    pub record: String,
    pub score: f64,
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
pub(crate) fn compose(
    dense: Vec<Candidate>,
    lexical: Vec<Candidate>,
    config: &Composition,
    limit: usize,
    retrieval: &RetrievalConfig,
) -> Result<Vec<Candidate>> {
    let mut dense = ranked(dense)?;
    let mut lexical = ranked(lexical)?;
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
        row.score = union
            .get(&(row.source.clone(), row.root.clone(), row.record.clone()))
            .ok_or(StorageUnavailable)?
            .score;
    }
    selected.truncate(limit);
    Ok(selected)
}

#[cfg(test)]
#[path = "retrieval/tests.rs"]
mod tests;
