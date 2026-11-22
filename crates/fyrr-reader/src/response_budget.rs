use crate::error::*;
use fyrr_core::mcp::{SearchHit, SearchOutput, Truncation};
use serde::Serialize;
use std::collections::BTreeMap;

fn serialized_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| StorageUnavailable)?;
    #[cfg(test)]
    BUDGET_SERIALIZED_BYTES.with(|count| count.set(count.get() + bytes.len()));
    Ok(bytes)
}

fn serialized_len<T: Serialize>(value: &T) -> Result<usize> {
    Ok(serialized_json(value)?.len())
}

#[cfg(test)]
thread_local! {
    static BUDGET_SERIALIZED_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Size of the MCP tool result, including its text and structured JSON copies.
fn response_len(output: &SearchOutput) -> Result<usize> {
    let value = serde_json::to_value(output).map_err(|_| StorageUnavailable)?;
    let mut result = rmcp::model::CallToolResult::structured(value);
    result.result_type = None;
    serialized_len(&result)
}

/// A hit appears as JSON and inside a JSON string in the MCP text copy.
fn hit_wire_len(hit: &SearchHit) -> Result<usize> {
    let json = serialized_json(hit)?;
    let text = std::str::from_utf8(&json).map_err(|_| StorageUnavailable)?;
    Ok(json.len() + serialized_len(&text)? - 2)
}

fn sized_response_len(output: &SearchOutput, hit_bytes: usize) -> Result<usize> {
    let empty = SearchOutput {
        hits: Vec::new(),
        truncated: output.truncated.clone(),
        metadata: output.metadata.clone(),
    };
    // Each separating comma appears once in each copy. Rechecking the small
    // envelope accounts for counter digit changes without rereading hit text.
    Ok(response_len(&empty)? + hit_bytes + 2 * output.hits.len().saturating_sub(1))
}

/// Trim authorized, ranked hits after telemetry is finalized. Each query keeps
/// four hits while another still has a tail; text goes next, then the floor yields.
/// Every size check includes the counters and the complete MCP tool result.
/// Hit contributions are cached so trimming does not repeatedly serialize text.
pub(crate) fn fit_response(output: &mut SearchOutput, max: usize) -> Result<()> {
    use fyrr_core::limits::mcp::RESPONSE_MIN_HITS_PER_QUERY;
    let mut sizes: Vec<_> = output
        .hits
        .iter()
        .map(hit_wire_len)
        .collect::<Result<_, _>>()?;
    let had_hits = !output.hits.is_empty();
    let mut hit_bytes: usize = sizes.iter().sum();
    if sized_response_len(output, hit_bytes)? <= max {
        return Ok(());
    }
    output.truncated = Some(Truncation {
        text_dropped: 0,
        hits_dropped: 0,
    });
    let mut remaining = BTreeMap::<Option<u32>, usize>::new();
    for hit in &output.hits {
        *remaining.entry(hit.query_index).or_default() += 1;
    }
    for index in (0..output.hits.len()).rev() {
        if sized_response_len(output, hit_bytes)? <= max {
            return Ok(());
        }
        let left = remaining.entry(output.hits[index].query_index).or_default();
        if *left <= RESPONSE_MIN_HITS_PER_QUERY {
            continue;
        }
        *left -= 1;
        drop_hit(output, index)?;
        hit_bytes -= sizes.remove(index);
    }
    for index in (0..output.hits.len()).rev() {
        if sized_response_len(output, hit_bytes)? <= max {
            return Ok(());
        }
        let hit = &mut output.hits[index];
        if hit.excerpt.is_none() && hit.content.is_none() {
            continue;
        }
        let bytes = hit.excerpt.take().map_or(0, |e| e.text.len())
            + hit.content.take().map_or(0, |c| c.len());
        output.metadata.omitted_bytes = output.metadata.omitted_bytes.saturating_add(bytes as u64);
        let count = &mut output
            .truncated
            .as_mut()
            .ok_or(StorageUnavailable)?
            .text_dropped;
        *count = count.saturating_add(1);
        let size = hit_wire_len(&output.hits[index])?;
        hit_bytes -= sizes[index] - size;
        sizes[index] = size;
    }
    while sized_response_len(output, hit_bytes)? > max {
        let Some(index) = output.hits.len().checked_sub(1) else {
            return Err(Busy);
        };
        drop_hit(output, index)?;
        hit_bytes -= sizes.remove(index);
    }
    // A page that loses every hit is an error: an empty result reads as the end
    // of a listing, and as no match for a search.
    if had_hits && output.hits.is_empty() {
        return Err(Busy);
    }
    Ok(())
}

fn drop_hit(output: &mut SearchOutput, index: usize) -> Result<()> {
    let bytes = serialized_len(&output.hits[index])?;
    output.hits.remove(index);
    output.metadata.omitted_bytes = output.metadata.omitted_bytes.saturating_add(bytes as u64);
    let count = &mut output
        .truncated
        .as_mut()
        .ok_or(StorageUnavailable)?
        .hits_dropped;
    *count = count.saturating_add(1);
    Ok(())
}

#[cfg(test)]
mod tests;
