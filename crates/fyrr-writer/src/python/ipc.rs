//! Arrow IPC streams from the Python host, inspected before they are decoded.
//!
//! Arrow's decoder honours what a stream declares: a compressed batch is
//! inflated to the size its producer states, before any of fyrr's limits can
//! run. So the messages are read here first, as plain length-prefixed
//! flatbuffers, and the stream is decoded only when it is exactly what the
//! contract allows: one schema message that passes the contract check and
//! carries no metadata beyond the contract's two entries, then one record batch
//! without compression whose declared rows and buffers are within the limits.
//! Nothing the stream declares is allocated or trusted until then, and every
//! refusal is a value-free [`Refusal`].

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::{Endianness, MessageHeader, root_as_message};
use arrow_schema::{DataType, Field};
use fyrr_core::connector::check_document_changes_schema;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// The marker that starts every message of the current IPC format.
const CONTINUATION: u32 = 0xFFFF_FFFF;
/// A message's flatbuffer (schema or batch description) is at most this long:
/// far above the contract's own, far below anything worth allocating for.
const METADATA_MAX: usize = 256 << 10;
/// Column nodes and buffers of one batch; the contract has a few dozen.
const NODES_MAX: usize = 4096;
const BUFFERS_MAX: usize = 16384;

/// What a batch may declare.
#[derive(Debug, Clone, Copy)]
pub(super) struct Bounds {
    /// The total bytes of the batch's buffers.
    pub(super) max_bytes: u64,
    /// The rows of the batch, where the count is limited as rows arrive.
    pub(super) max_rows: Option<u64>,
}

/// Why a stream was refused. None carries a value of the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Refusal {
    /// Not the framing of an IPC stream, or a message that is not whole.
    Framing,
    /// Not exactly one schema message and then one record batch.
    Messages,
    /// A schema that is not the contract's.
    Schema,
    /// Schema or field metadata beyond the contract's two entries.
    Metadata,
    /// A compressed batch; fyrr's limits would not see its real size.
    Compressed,
    /// A dictionary batch; the contract has no dictionary types.
    Dictionary,
    /// More rows than allowed.
    Rows,
    /// More buffer bytes than allowed.
    Bytes,
    /// A batch whose buffers or nodes are implausible or point outside it.
    Layout,
    /// The decoder refused what inspection accepted.
    Decode,
}

/// Exactly one record batch of an Arrow IPC stream: inspected, then decoded.
/// The batch's buffers are slices of the frame, not copies.
pub(super) fn decode(frame: Vec<u8>, bounds: Bounds) -> Result<RecordBatch, Refusal> {
    inspect(&frame, bounds)?;
    let mut buffer = Buffer::from_vec(frame);
    let mut decoder = StreamDecoder::new();
    let mut batch = None;
    while !buffer.is_empty() {
        let decoded = decoder.decode(&mut buffer).map_err(|_| Refusal::Decode)?;
        if let Some(rows) = decoded
            && batch.replace(rows).is_some()
        {
            return Err(Refusal::Messages);
        }
    }
    decoder.finish().map_err(|_| Refusal::Decode)?;
    batch.ok_or(Refusal::Messages)
}

/// Checks the stream's messages against the contract and `bounds` without
/// decoding any data.
fn inspect(frame: &[u8], bounds: Bounds) -> Result<(), Refusal> {
    let mut offset = 0usize;
    let (mut schema, mut batches) = (false, 0u32);
    loop {
        if offset == frame.len() {
            break;
        }
        let head = frame.get(offset..offset + 8).ok_or(Refusal::Framing)?;
        if u32::from_le_bytes([head[0], head[1], head[2], head[3]]) != CONTINUATION {
            return Err(Refusal::Framing);
        }
        let length = i32::from_le_bytes([head[4], head[5], head[6], head[7]]);
        offset += 8;
        if length == 0 {
            // The end of the stream: nothing may follow.
            return if offset == frame.len() {
                break;
            } else {
                Err(Refusal::Framing)
            };
        }
        let length = usize::try_from(length)
            .ok()
            .filter(|length| *length <= METADATA_MAX)
            .ok_or(Refusal::Framing)?;
        let metadata = frame
            .get(offset..offset.saturating_add(length))
            .ok_or(Refusal::Framing)?;
        offset += length;
        let message = root_as_message(metadata).map_err(|_| Refusal::Framing)?;
        let body = usize::try_from(message.bodyLength()).map_err(|_| Refusal::Framing)?;
        if body > frame.len() - offset {
            return Err(Refusal::Framing);
        }
        match message.header_type() {
            MessageHeader::Schema if !schema && batches == 0 => {
                let header = message.header_as_schema().ok_or(Refusal::Framing)?;
                check_schema(header)?;
                schema = true;
            }
            MessageHeader::RecordBatch if schema && batches == 0 => {
                let header = message.header_as_record_batch().ok_or(Refusal::Framing)?;
                check_batch(header, body as u64, bounds)?;
                batches += 1;
            }
            MessageHeader::DictionaryBatch => return Err(Refusal::Dictionary),
            _ => return Err(Refusal::Messages),
        }
        offset += body;
    }
    if schema && batches == 1 {
        Ok(())
    } else {
        Err(Refusal::Messages)
    }
}

fn check_schema(header: arrow_ipc::Schema<'_>) -> Result<(), Refusal> {
    if header.endianness() != Endianness::Little {
        return Err(Refusal::Schema);
    }
    // Arrow's conversion panics on a type it cannot represent; the flatbuffer
    // is verified, but its content is the peer's.
    let schema =
        catch_unwind(AssertUnwindSafe(|| fb_to_schema(header))).map_err(|_| Refusal::Schema)?;
    check_document_changes_schema(&schema).map_err(|_| Refusal::Schema)?;
    // The check demands the contract's two entries; any more is bytes that no
    // buffer limit counts.
    if schema.metadata().len() != 2 || schema.fields().iter().any(|field| has_metadata(field)) {
        return Err(Refusal::Metadata);
    }
    Ok(())
}

/// Whether the field or any field nested in it carries metadata.
fn has_metadata(field: &Field) -> bool {
    if !field.metadata().is_empty() {
        return true;
    }
    match field.data_type() {
        DataType::Struct(children) => children.iter().any(|child| has_metadata(child)),
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::FixedSizeList(child, _)
        | DataType::Map(child, _) => has_metadata(child),
        _ => false,
    }
}

fn check_batch(
    batch: arrow_ipc::RecordBatch<'_>,
    body: u64,
    bounds: Bounds,
) -> Result<(), Refusal> {
    if batch.compression().is_some() {
        return Err(Refusal::Compressed);
    }
    let rows = u64::try_from(batch.length()).map_err(|_| Refusal::Layout)?;
    if bounds.max_rows.is_some_and(|max| rows > max) {
        return Err(Refusal::Rows);
    }
    if batch.variadicBufferCounts().is_some()
        || batch.nodes().is_some_and(|nodes| nodes.len() > NODES_MAX)
    {
        return Err(Refusal::Layout);
    }
    let buffers = batch.buffers().ok_or(Refusal::Layout)?;
    if buffers.len() > BUFFERS_MAX {
        return Err(Refusal::Layout);
    }
    let mut total = 0u64;
    for buffer in &buffers {
        let (offset, length) = (
            u64::try_from(buffer.offset()).map_err(|_| Refusal::Layout)?,
            u64::try_from(buffer.length()).map_err(|_| Refusal::Layout)?,
        );
        if offset.checked_add(length).is_none_or(|end| end > body) {
            return Err(Refusal::Layout);
        }
        total = total.checked_add(length).ok_or(Refusal::Layout)?;
    }
    if total > bounds.max_bytes {
        return Err(Refusal::Bytes);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int8Type;
    use arrow_array::{
        ArrayRef, DictionaryArray, LargeStringArray, ListArray, RecordBatchOptions, StringArray,
        new_empty_array, new_null_array,
    };
    use arrow_buffer::OffsetBuffer;
    use arrow_ipc::CompressionType;
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
    use arrow_schema::Schema;
    use fyrr_core::connector::document_changes_arrow_schema;
    use std::collections::HashMap;
    use std::sync::Arc;

    const OPEN: Bounds = Bounds {
        max_bytes: 1 << 20,
        max_rows: Some(100),
    };

    /// A column of `rows` values that the contract's schema allows.
    fn column(field: &Field, rows: usize) -> ArrayRef {
        match field.data_type() {
            DataType::Dictionary(..) => {
                Arc::new(DictionaryArray::<Int8Type>::from_iter(vec![
                    Some("a");
                    rows
                ]))
            }
            _ if field.is_nullable() => new_null_array(field.data_type(), rows),
            DataType::Utf8 => Arc::new(StringArray::from(vec!["x"; rows])),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec!["x"; rows])),
            DataType::List(item) => Arc::new(ListArray::new(
                item.clone(),
                OffsetBuffer::from_lengths(vec![0; rows]),
                new_empty_array(item.data_type()),
                None,
            )),
            other => unreachable!("{other:?}"),
        }
    }

    fn batch(schema: &Schema, rows: usize) -> RecordBatch {
        RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            schema
                .fields()
                .iter()
                .map(|field| column(field, rows))
                .collect(),
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .unwrap()
    }

    fn stream_with(schema: &Schema, batches: usize, options: IpcWriteOptions) -> Vec<u8> {
        let mut writer = StreamWriter::try_new_with_options(Vec::new(), schema, options).unwrap();
        for _ in 0..batches {
            writer.write(&batch(schema, 1)).unwrap();
        }
        writer.finish().unwrap();
        writer.into_inner().unwrap()
    }

    fn stream(schema: &Schema, batches: usize) -> Vec<u8> {
        stream_with(schema, batches, IpcWriteOptions::default())
    }

    fn contract() -> Schema {
        document_changes_arrow_schema()
    }

    fn refused(frame: Vec<u8>, bounds: Bounds) -> Refusal {
        decode(frame, bounds).unwrap_err()
    }

    /// Each message of a stream, with its framing, as byte ranges.
    fn messages(frame: &[u8]) -> Vec<std::ops::Range<usize>> {
        let mut ranges = Vec::new();
        let mut offset = 0;
        while offset < frame.len() {
            let length =
                i32::from_le_bytes(frame[offset + 4..offset + 8].try_into().unwrap()) as usize;
            let body = if length == 0 {
                0
            } else {
                let meta = &frame[offset + 8..offset + 8 + length];
                root_as_message(meta).unwrap().bodyLength() as usize
            };
            ranges.push(offset..offset + 8 + length + body);
            offset += 8 + length + body;
        }
        ranges
    }

    #[test]
    fn the_contract_stream_is_accepted_and_decoded_to_its_rows() {
        let rows = decode(stream(&contract(), 1), OPEN).unwrap();
        assert_eq!(rows.num_rows(), 1);
        assert_eq!(rows.schema().as_ref(), &contract());
        // A stream without the end-of-stream marker is whole as well.
        let mut frame = stream(&contract(), 1);
        frame.truncate(frame.len() - 8);
        assert!(decode(frame, OPEN).is_ok());
        assert_eq!(
            decode(
                stream(&contract(), 1),
                Bounds {
                    max_rows: None,
                    ..OPEN
                }
            )
            .unwrap()
            .num_rows(),
            1
        );
    }

    #[test]
    fn a_compressed_batch_is_refused_before_anything_is_inflated() {
        for codec in [CompressionType::LZ4_FRAME, CompressionType::ZSTD] {
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(codec))
                .expect("the linked arrow-ipc has this codec");
            let frame = stream_with(&contract(), 1, options);
            assert_eq!(refused(frame, OPEN), Refusal::Compressed, "{codec:?}");
        }
    }

    #[test]
    fn dictionary_batches_are_refused() {
        let mut schema = contract();
        let tag = Field::new(
            "tag",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        );
        schema = Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .cloned()
                .chain([Arc::new(tag)])
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        );
        // The contract allows a nullable addition, so the schema passes and the
        // dictionary batch before the rows is what is refused.
        assert_eq!(refused(stream(&schema, 1), OPEN), Refusal::Dictionary);
    }

    #[test]
    fn exactly_one_schema_and_one_record_batch_are_required() {
        let one = stream(&contract(), 1);
        // Two record batches.
        assert_eq!(refused(stream(&contract(), 2), OPEN), Refusal::Messages);
        // A schema and no batch.
        assert_eq!(refused(stream(&contract(), 0), OPEN), Refusal::Messages);
        // A batch with no schema before it.
        let ranges = messages(&one);
        let without_schema = [&one[ranges[1].clone()], &one[ranges[2].clone()]].concat();
        assert_eq!(refused(without_schema, OPEN), Refusal::Messages);
        // A second schema.
        let again = [&one[..one.len() - 8], &one[..]].concat();
        assert_eq!(refused(again, OPEN), Refusal::Messages);
        // Nothing at all.
        assert_eq!(refused(Vec::new(), OPEN), Refusal::Messages);
    }

    #[test]
    fn framing_that_is_not_an_ipc_stream_is_refused() {
        let one = stream(&contract(), 1);
        // The legacy framing, without the continuation marker.
        assert_eq!(refused(one[4..].to_vec(), OPEN), Refusal::Framing);
        // Bytes after the end-of-stream marker.
        let mut trailing = one.clone();
        trailing.extend_from_slice(b"more bytes");
        assert_eq!(refused(trailing, OPEN), Refusal::Framing);
        // A frame cut inside a body, and inside a message's metadata.
        assert_eq!(
            refused(one[..one.len() - 20].to_vec(), OPEN),
            Refusal::Framing
        );
        assert_eq!(refused(one[..30].to_vec(), OPEN), Refusal::Framing);
        assert_eq!(
            refused(b"garbage that is no stream".to_vec(), OPEN),
            Refusal::Framing
        );
        // A metadata length that is negative, or that exceeds the frame.
        for length in [-5i32, 1 << 20] {
            let mut bad = one.clone();
            bad[4..8].copy_from_slice(&length.to_le_bytes());
            assert_eq!(refused(bad, OPEN), Refusal::Framing, "{length}");
        }
    }

    #[test]
    fn a_schema_that_is_not_the_contracts_is_refused() {
        let swap = |name: &str, replacement: Option<Field>| {
            let fields: Vec<_> = contract()
                .fields()
                .iter()
                .filter_map(|field| {
                    if field.name() == name {
                        replacement.clone().map(Arc::new)
                    } else {
                        Some(field.clone())
                    }
                })
                .collect();
            Schema::new_with_metadata(fields, contract().metadata().clone())
        };
        // A changed type, a missing field, and a producer that names another contract.
        let large = swap(
            "root_id",
            Some(Field::new("root_id", DataType::LargeUtf8, false)),
        );
        assert_eq!(refused(stream(&large, 1), OPEN), Refusal::Schema);
        assert_eq!(refused(stream(&swap("op", None), 1), OPEN), Refusal::Schema);
        let other = contract().with_metadata(
            [
                ("fyrr.contract".to_owned(), "something.else".to_owned()),
                ("fyrr.schema_version".to_owned(), "1.0".to_owned()),
            ]
            .into(),
        );
        assert_eq!(refused(stream(&other, 1), OPEN), Refusal::Schema);
    }

    #[test]
    fn metadata_beyond_the_contracts_two_entries_is_refused() {
        // The extra entry sits where no buffer limit counts it.
        let mut metadata = contract().metadata().clone();
        metadata.insert("note".into(), "x".into());
        let schema = contract().with_metadata(metadata.clone());
        assert_eq!(refused(stream(&schema, 1), OPEN), Refusal::Metadata);
        // A schema that would make a one-row batch megabytes large is refused
        // by its length alone, before it is parsed.
        metadata.insert("note".into(), "x".repeat(2 << 20));
        let schema = contract().with_metadata(metadata);
        assert_eq!(refused(stream(&schema, 1), OPEN), Refusal::Framing);
        // Metadata on a field, nested or not.
        let marked = |name: &str| {
            let fields: Vec<_> = contract()
                .fields()
                .iter()
                .map(|field| {
                    if field.name() == name {
                        Arc::new(
                            field
                                .as_ref()
                                .clone()
                                .with_metadata(HashMap::from([("k".to_owned(), "v".to_owned())])),
                        )
                    } else {
                        field.clone()
                    }
                })
                .collect();
            Schema::new_with_metadata(fields, contract().metadata().clone())
        };
        assert_eq!(
            refused(stream(&marked("title"), 1), OPEN),
            Refusal::Metadata
        );
    }

    #[test]
    fn rows_and_buffer_bytes_over_the_limits_are_refused() {
        let frame = stream(&contract(), 1);
        let rows = Bounds {
            max_rows: Some(0),
            ..OPEN
        };
        assert_eq!(refused(frame.clone(), rows), Refusal::Rows);
        let bytes = Bounds {
            max_bytes: 1,
            ..OPEN
        };
        assert_eq!(refused(frame, bytes), Refusal::Bytes);
    }

    #[test]
    fn buffers_that_point_outside_the_batch_are_refused() {
        let mut frame = stream(&contract(), 1);
        let batch_message = messages(&frame)[1].clone();
        let metadata = batch_message.start + 8
            ..batch_message.start
                + 8
                + i32::from_le_bytes(
                    frame[batch_message.start + 4..batch_message.start + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
        let last = root_as_message(&frame[metadata.clone()])
            .unwrap()
            .header_as_record_batch()
            .unwrap()
            .buffers()
            .unwrap()
            .iter()
            .next_back()
            .copied()
            .unwrap();
        let mut pattern = last.offset().to_le_bytes().to_vec();
        pattern.extend(last.length().to_le_bytes());
        let at = frame[metadata.clone()]
            .windows(16)
            .rposition(|window| window == pattern)
            .expect("the buffer entry is in the message");
        let length = metadata.start + at + 8;
        frame[length..length + 8].copy_from_slice(&(1i64 << 40).to_le_bytes());
        assert_eq!(refused(frame, OPEN), Refusal::Layout);
    }
}
