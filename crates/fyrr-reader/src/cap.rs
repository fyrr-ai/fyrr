//! Caps on the hits per value of an attribute: what each candidate holds of the
//! attributes a request caps, read for the rows the caller may read.
use crate::error::*;
use crate::retrieval::{Candidate, Selection, ValueCap as Cap};
use crate::search::{PreparedReader, ids, strings};
use fyrr_core::attributes::AttributeType;
use fyrr_core::mcp::{SearchInput, ValueCap};
use std::collections::BTreeSet;

// shortcut: only text values can be capped; cap a number or a flag when a request needs it.
/// The columns of a document, besides the declared `string` attributes, that a
/// cap may name.
const BUILT_IN: [&str; 3] = ["author", "project", "source_type"];

/// Refuses a cap on a column that is neither one of `BUILT_IN` nor a declared
/// `string` attribute.
pub(crate) fn check(reader: &PreparedReader, caps: Option<&[ValueCap]>) -> Result<()> {
    for cap in caps.into_iter().flatten() {
        if !BUILT_IN.contains(&cap.attribute.as_str())
            && reader
                .manifest
                .settings
                .namespace
                .attributes
                .get(&cap.attribute)
                .copied()
                != Some(AttributeType::String)
        {
            return Err(InvalidArgument);
        }
    }
    Ok(())
}

/// How the request selects hits from the candidates of the lanes. The values
/// of a capped attribute are read only for those candidates, which the caller
/// may read: `rows_by_id` refuses any other row.
pub(crate) async fn selection(
    reader: &PreparedReader,
    args: &SearchInput,
    lanes: [&[Candidate]; 2],
) -> Result<Selection> {
    let mut selection = Selection {
        records: args.members.group_documents == Some(false),
        caps: Vec::new(),
    };
    let Some(caps) = args.members.caps.as_deref() else {
        return Ok(selection);
    };
    let row_ids: Vec<u64> = lanes
        .into_iter()
        .flatten()
        .map(|row| row.row_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if row_ids.is_empty() {
        return Ok(selection);
    }
    let names: Vec<&str> = caps.iter().map(|cap| cap.attribute.as_str()).collect();
    let batch = reader
        .rows_by_id(&row_ids, &names, PublicationInvalid)
        .await?;
    let rows = ids(&batch)?;
    for cap in caps {
        let values = strings(&batch, &cap.attribute)?;
        selection.caps.push(Cap {
            max: cap.max as usize,
            values: rows
                .values()
                .iter()
                .zip(values)
                .filter_map(|(id, value)| Some((*id, value?.to_owned())))
                .collect(),
        });
    }
    Ok(selection)
}
