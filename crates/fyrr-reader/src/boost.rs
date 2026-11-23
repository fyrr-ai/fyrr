//! Attribute and recency boosts: weighted terms added to the relevance score of
//! the candidates of a ranking, which is then ordered again.
use crate::error::*;
use crate::retrieval::{Candidate, rescored};
use crate::search::{PreparedReader, ids};
use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array,
};
use fyrr_core::attributes::AttributeType;
use fyrr_core::config::Composition;
use fyrr_core::mcp::{Boost, SearchInput};
use std::collections::BTreeMap;

/// The candidates of a ranking that a boost can lift: those its composition
/// keeps (`fusion + dense + lexical` slots, 50 by default), or `depth` when
/// that is more, as a request's `limit` or its reranker's candidates may be.
pub(crate) fn candidate_depth(composition: &Composition, depth: usize) -> usize {
    depth.max(composition.fusion + composition.dense + composition.lexical)
}

/// The type of the attribute a boost names: a declared one, or `updated_at`.
fn kind(reader: &PreparedReader, name: &str) -> Option<AttributeType> {
    match name {
        "updated_at" => Some(AttributeType::Datetime),
        name => reader.manifest.attributes.get(name).copied(),
    }
}

/// Refuses a boost on an attribute the namespace does not declare, a numeric
/// boost on a type that is not a number, and recency on one that is not a
/// `datetime`.
pub(crate) fn check(reader: &PreparedReader, boosts: Option<&[Boost]>) -> Result<()> {
    for boost in boosts.into_iter().flatten() {
        let kind = kind(reader, boost.attribute());
        let fits = match boost {
            Boost::Attribute { .. } => matches!(
                kind,
                Some(AttributeType::Int | AttributeType::Uint | AttributeType::Float)
            ),
            Boost::Recency { .. } => kind == Some(AttributeType::Datetime),
        };
        if !fits {
            return Err(InvalidArgument);
        }
    }
    Ok(())
}

/// `x / (x + midpoint)` for a midpoint above 0, written `1 / (1 + midpoint / x)`
/// so that it is exact at 0 and at infinity. A negative `x` counts as 0.
pub(crate) fn attribute(x: f64, midpoint: f64) -> f64 {
    1.0 / (1.0 + midpoint / x.max(0.0))
}

/// `midpoint / (midpoint + distance)`, both in the same unit, a midpoint above 0
/// and a distance of 0 or more.
pub(crate) fn recency(distance: f64, midpoint: f64) -> f64 {
    midpoint / (midpoint + distance)
}

/// One number per row of the batch for an attribute of `kind`: microseconds
/// for a `datetime`, nothing for a missing value.
fn numbers(array: &ArrayRef, kind: AttributeType) -> Result<Vec<Option<f64>>> {
    fn typed<T: 'static>(array: &ArrayRef) -> Result<&T> {
        array.as_any().downcast_ref().ok_or(PublicationRowsInvalid)
    }
    Ok(match kind {
        AttributeType::Int => typed::<Int64Array>(array)?
            .iter()
            .map(|v| v.map(|v| v as f64))
            .collect(),
        AttributeType::Uint => typed::<UInt64Array>(array)?
            .iter()
            .map(|v| v.map(|v| v as f64))
            .collect(),
        AttributeType::Float => typed::<Float64Array>(array)?.iter().collect(),
        AttributeType::Datetime => typed::<TimestampMicrosecondArray>(array)?
            .iter()
            .map(|v| v.map(|v| v as f64))
            .collect(),
        _ => return Err(InvalidArgument),
    })
}

/// Adds the request's boosts to the score of each ranking's candidates and
/// ranks them again. Only candidates the caller was admitted to are read, so
/// a boost never reaches a value of a document the caller may not read.
pub(crate) async fn apply(
    reader: &PreparedReader,
    args: &SearchInput,
    ranked: &mut [Vec<Candidate>],
) -> Result<()> {
    let Some(boosts) = args.boosts.as_deref() else {
        return Ok(());
    };
    let mut names: Vec<&str> = boosts.iter().map(Boost::attribute).collect();
    names.sort_unstable();
    names.dedup();
    for rows in ranked.iter_mut().filter(|rows| !rows.is_empty()) {
        let row_ids: Vec<u64> = rows.iter().map(|row| row.row_id).collect();
        let batch: RecordBatch = reader
            .rows_by_id(&row_ids, &names, PublicationInvalid)
            .await?;
        let position: BTreeMap<u64, usize> = ids(&batch)?
            .values()
            .iter()
            .enumerate()
            .map(|(index, id)| (*id, index))
            .collect();
        let mut bonus = vec![0.0; rows.len()];
        for boost in boosts {
            let name = boost.attribute();
            let kind = kind(reader, name).ok_or(InvalidArgument)?;
            let array = batch.column_by_name(name).ok_or(PublicationRowsInvalid)?;
            let values = numbers(array, kind)?;
            for (row, bonus) in rows.iter().zip(&mut bonus) {
                let index = *position.get(&row.row_id).ok_or(StorageUnavailable)?;
                let Some(value) = values[index] else {
                    continue;
                };
                *bonus += match boost {
                    Boost::Attribute {
                        weight, midpoint, ..
                    } => weight * attribute(value, *midpoint),
                    Boost::Recency {
                        weight,
                        midpoint,
                        origin,
                        ..
                    } => {
                        let distance_ms = (value - origin.0.timestamp_micros() as f64).abs() / 1e3;
                        weight * recency(distance_ms, midpoint.as_millis() as f64)
                    }
                };
            }
        }
        // A search without text is scored by distance, which has no fixed scale.
        rescored(rows, &bonus, !args.has_text());
    }
    Ok(())
}

#[cfg(test)]
#[path = "boost/tests.rs"]
mod tests;
