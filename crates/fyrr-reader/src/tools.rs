//! Authorized tool operations on the existing pinned reader.
use crate::embedding::digest;
use crate::error::*;
use crate::retrieval::Candidate;
use crate::search::{PreparedReader, column, ids, strings};
use crate::storage::{hash_principal, sql};
use crate::telemetry::micros;
use arrow_array::{Array, RecordBatch, UInt64Array};
use futures::TryStreamExt;
use fyrr_core::connector::DocumentMetadata;
use fyrr_core::limits::mcp::{
    FETCH_MAX_CONTENT_BYTES, FETCH_MAX_DOCUMENTS, FETCH_MAX_RECORDS, LIST_FILTERS_MAX_VALUES,
};
use fyrr_core::mcp::*;
use lance::dataset::scanner::{ColumnOrdering, RowAddrMask, RowAddrTreeMap};
use object_store::ObjectStoreExt;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

/// Authenticated logical pagination, with a fixed expiry across pages.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorWire<'a> {
    namespace: Cow<'a, str>,
    subject: Cow<'a, str>,
    content_version: u64,
    expires: i64,
    uri: Option<Cow<'a, str>>,
    source: Option<Cow<'a, str>>,
    root: Option<Cow<'a, str>>,
    after: Cow<'a, (String, String, String)>,
    offset: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "cursor_documents")]
    documents: Option<Cow<'a, [FetchDocument]>>,
    #[serde(default, skip_serializing_if = "is_zero_index")]
    document_index: usize,
}
// Length-prefixed UTF-8 avoids JSON escaping growth in the bounded cursor.
mod cursor_documents {
    use super::*;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde::{Deserializer, Serializer};
    pub(super) fn serialize<S: Serializer>(
        documents: &Option<Cow<'_, [FetchDocument]>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        documents.as_deref().map(encode).serialize(serializer)
    }
    fn encode(documents: &[FetchDocument]) -> String {
        let Ok(count) = u8::try_from(documents.len()) else {
            unreachable!("validated cursor document count fits u8");
        };
        let mut bytes = vec![count];
        for document in documents {
            for text in [&document.source_id, &document.root_id] {
                push_text(&mut bytes, text);
            }
            bytes.push(u8::from(document.window.is_some()));
            if let Some(window) = &document.window {
                for text in [&window.record_id, &window.revision] {
                    push_text(&mut bytes, text);
                }
                bytes.extend_from_slice(&window.start.to_le_bytes());
                bytes.push(u8::from(window.end.is_some()));
                bytes.extend_from_slice(&window.end.or(window.context).unwrap_or(0).to_le_bytes());
            }
        }
        URL_SAFE_NO_PAD.encode(bytes)
    }

    fn push_text(bytes: &mut Vec<u8>, text: &str) {
        let Ok(length) = u16::try_from(text.len()) else {
            unreachable!("validated cursor identifier length fits u16");
        };
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(text.as_bytes());
    }
    pub(super) fn deserialize<'de, 'a, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Cow<'a, [FetchDocument]>>, D::Error> {
        let encoded = Option::<String>::deserialize(deserializer)?;
        encoded
            .map(|encoded| {
                decode(&encoded)
                    .map(Cow::Owned)
                    .ok_or_else(|| serde::de::Error::custom("stale_cursor"))
            })
            .transpose()
    }
    fn decode(encoded: &str) -> Option<Vec<FetchDocument>> {
        let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
        let mut input = bytes.as_slice();
        let count = take(&mut input, 1)?[0];
        if !(1..=5).contains(&count) {
            return None;
        }
        let mut documents = Vec::new();
        for _ in 0..count {
            let source_id = text(&mut input, 512)?;
            let root_id = text(&mut input, 512)?;
            let window = match take(&mut input, 1)?[0] {
                0 => None,
                1 => {
                    let record_id = text(&mut input, 512)?;
                    let revision = text(&mut input, 256)?;
                    let start = number(&mut input)?;
                    let end = take(&mut input, 1)?[0];
                    let bound = number(&mut input)?;
                    Some(FetchWindow {
                        record_id,
                        revision,
                        start,
                        end: match end {
                            0 => None,
                            1 => Some(bound),
                            _ => return None,
                        },
                        context: (end == 0).then_some(bound),
                    })
                }
                _ => return None,
            };
            documents.push(FetchDocument {
                source_id,
                root_id,
                window,
            });
        }
        input.is_empty().then_some(documents)
    }
    fn take<'a>(input: &mut &'a [u8], count: usize) -> Option<&'a [u8]> {
        let (value, rest) = input.split_at_checked(count)?;
        *input = rest;
        Some(value)
    }
    fn text(input: &mut &[u8], max: usize) -> Option<String> {
        let length = u16::from_le_bytes(take(input, 2)?.try_into().ok()?) as usize;
        if length == 0 || length > max {
            return None;
        }
        String::from_utf8(take(input, length)?.to_vec()).ok()
    }
    fn number(input: &mut &[u8]) -> Option<u32> {
        Some(u32::from_le_bytes(take(input, 4)?.try_into().ok()?))
    }
}

fn is_zero_index(index: &usize) -> bool {
    *index == 0
}
#[derive(Clone, Copy)]
struct FetchPageLimits {
    records: usize,
    bytes: usize,
    list: bool,
}
fn ordering(name: &str, ascending: bool) -> ColumnOrdering {
    ColumnOrdering {
        column_name: name.into(),
        ascending,
        // Nulls sort first ascending and last descending.
        nulls_first: ascending,
    }
}
pub(crate) use fyrr_core::limits::record_revision as revision;
fn clip(text: &str, max: usize) -> &str {
    &text[..text.floor_char_boundary(text.len().min(max))]
}
mod aggregate;
mod fetch;
mod filters;
pub use filters::filter_sql;
mod page;

#[cfg(test)]
#[path = "tools/cursor_tests.rs"]
mod cursor_tests;

/// Internal pagination state. The wire envelope remains untagged and unchanged.
#[derive(Deserialize)]
#[serde(try_from = "CursorWire")]
enum FetchCursor {
    Lookup(LookupCursor),
    Documents(DocumentsCursor),
}
#[derive(Clone)]
struct CursorScope {
    namespace: String,
    subject: String,
    content_version: u64,
    expires: i64,
}
#[derive(Clone)]
struct CursorPosition {
    after: (String, String, String),
    offset: usize,
}
#[derive(Clone)]
enum FetchSelector {
    Uri(String),
    SourceRoot { source: String, root: String },
}

#[derive(Clone)]
struct LookupCursor {
    scope: CursorScope,
    selector: FetchSelector,
    position: CursorPosition,
}
struct DocumentsCursor {
    scope: CursorScope,
    documents: Vec<FetchDocument>,
    document_index: usize,
    position: CursorPosition,
}
impl TryFrom<CursorWire<'_>> for FetchCursor {
    type Error = crate::ReaderError;
    fn try_from(wire: CursorWire<'_>) -> Result<Self, Self::Error> {
        let scope = CursorScope {
            namespace: wire.namespace.into_owned(),
            subject: wire.subject.into_owned(),
            content_version: wire.content_version,
            expires: wire.expires,
        };
        let position = CursorPosition {
            after: wire.after.into_owned(),
            offset: wire.offset,
        };
        Ok(if let Some(documents) = wire.documents {
            Self::Documents(DocumentsCursor {
                scope,
                documents: documents.into_owned(),
                document_index: wire.document_index,
                position,
            })
        } else {
            let selector = if let Some(uri) = wire.uri {
                FetchSelector::Uri(uri.into_owned())
            } else {
                FetchSelector::SourceRoot {
                    source: wire.source.ok_or(StaleCursor)?.into_owned(),
                    root: wire.root.ok_or(StaleCursor)?.into_owned(),
                }
            };
            Self::Lookup(LookupCursor {
                scope,
                selector,
                position,
            })
        })
    }
}
impl FetchCursor {
    fn scope(&self) -> &CursorScope {
        match self {
            Self::Lookup(cursor) => &cursor.scope,
            Self::Documents(cursor) => &cursor.scope,
        }
    }
}
impl CursorScope {
    fn wire<'a>(&'a self, position: &'a CursorPosition) -> CursorWire<'a> {
        CursorWire {
            namespace: Cow::Borrowed(&self.namespace),
            subject: Cow::Borrowed(&self.subject),
            content_version: self.content_version,
            expires: self.expires,
            uri: None,
            source: None,
            root: None,
            after: Cow::Borrowed(&position.after),
            offset: position.offset,
            documents: None,
            document_index: 0,
        }
    }
}
impl Serialize for LookupCursor {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut wire = self.scope.wire(&self.position);
        match &self.selector {
            FetchSelector::Uri(uri) => wire.uri = Some(Cow::Borrowed(uri)),
            FetchSelector::SourceRoot { source, root } => {
                wire.source = Some(Cow::Borrowed(source));
                wire.root = Some(Cow::Borrowed(root));
            }
        }
        wire.serialize(serializer)
    }
}

impl Serialize for DocumentsCursor {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        CursorWire {
            documents: Some(Cow::Borrowed(&self.documents)),
            document_index: self.document_index,
            ..self.scope.wire(&self.position)
        }
        .serialize(serializer)
    }
}
#[cfg(test)]
impl Serialize for FetchCursor {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Lookup(cursor) => cursor.serialize(serializer),
            Self::Documents(cursor) => cursor.serialize(serializer),
        }
    }
}
