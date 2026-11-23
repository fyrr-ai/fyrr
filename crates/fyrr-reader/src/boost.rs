//! Attribute and recency boosts: weighted terms added to the relevance score of
//! the candidates a query's lanes return, before the composition picks its
//! slots.
use crate::error::*;
use crate::retrieval::{Bonus, Candidate};
use crate::search::{PreparedReader, ids};
use arrow_array::{ArrayRef, Float64Array, Int64Array, TimestampMicrosecondArray, UInt64Array};
use fyrr_core::attributes::AttributeType;
use fyrr_core::mcp::Boost;
use std::collections::{BTreeMap, BTreeSet};

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

/// `x / (x + midpoint)` for a midpoint above 0. A negative `x`, or one that is
/// no number, counts as 0. Computed from the ratio of the smaller to the
/// larger, so that no sum of two finite numbers overflows.
pub(crate) fn attribute(x: f64, midpoint: f64) -> f64 {
    let x = x.max(0.0);
    if x <= midpoint {
        let ratio = x / midpoint;
        ratio / (1.0 + ratio)
    } else {
        1.0 / (1.0 + midpoint / x)
    }
}

/// `midpoint / (midpoint + distance)`, both in the same unit, a midpoint above 0
/// and a distance of 0 or more.
pub(crate) fn recency(distance: f64, midpoint: f64) -> f64 {
    midpoint / (midpoint + distance)
}

/// One number per row of the batch for a numeric attribute; nothing for a
/// missing value.
fn numbers(array: &ArrayRef, kind: AttributeType) -> Result<Vec<Option<f64>>> {
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
        _ => return Err(InvalidArgument),
    })
}

fn typed<T: 'static>(array: &ArrayRef) -> Result<&T> {
    array.as_any().downcast_ref().ok_or(PublicationRowsInvalid)
}

/// What the request's boosts add to the score of each candidate of the lanes,
/// by row id. Only rows the caller was admitted to are read, so a boost never
/// reaches a value of a document the caller may not read.
pub(crate) async fn bonuses(
    reader: &PreparedReader,
    boosts: Option<&[Boost]>,
    lanes: [&[Candidate]; 2],
) -> Result<Bonus> {
    // A request whose boosts all weigh nothing reads no attribute.
    let Some(boosts) = boosts.filter(|boosts| boosts.iter().any(|boost| boost.weight() > 0.0))
    else {
        return Ok(Bonus::new());
    };
    let row_ids: Vec<u64> = lanes
        .into_iter()
        .flatten()
        .map(|row| row.row_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if row_ids.is_empty() {
        return Ok(Bonus::new());
    }
    let mut names: Vec<&str> = boosts.iter().map(Boost::attribute).collect();
    names.sort_unstable();
    names.dedup();
    let batch = reader
        .rows_by_id(&row_ids, &names, PublicationInvalid)
        .await?;
    let position: BTreeMap<u64, usize> = ids(&batch)?
        .values()
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, index))
        .collect();
    let mut bonus: Bonus = row_ids.iter().map(|id| (*id, 0.0)).collect();
    for boost in boosts {
        let name = boost.attribute();
        let array = batch.column_by_name(name).ok_or(PublicationRowsInvalid)?;
        let terms: Vec<Option<f64>> = match boost {
            Boost::Attribute {
                weight, midpoint, ..
            } => numbers(array, kind(reader, name).ok_or(InvalidArgument)?)?
                .into_iter()
                .map(|x| x.map(|x| weight * attribute(x, *midpoint)))
                .collect(),
            Boost::Recency {
                weight,
                midpoint,
                origin,
                ..
            } => typed::<TimestampMicrosecondArray>(array)?
                .iter()
                .map(|time| {
                    // The difference is taken in whole microseconds.
                    let distance = time?.abs_diff(origin.0.timestamp_micros()) as f64 / 1e3;
                    Some(weight * recency(distance, midpoint.as_millis() as f64))
                })
                .collect(),
        };
        for (id, sum) in &mut bonus {
            let index = *position.get(id).ok_or(StorageUnavailable)?;
            *sum += terms[index].unwrap_or(0.0);
        }
    }
    Ok(bonus)
}

#[cfg(test)]
#[path = "boost/tests.rs"]
mod tests;
