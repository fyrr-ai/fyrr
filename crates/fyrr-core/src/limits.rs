//! Bounds shared by the contracts and the MCP tool schemas. The public request
//! contract fixes the MCP request limits; the remaining bounds are initial defaults that keep every
//! contract field bounded (they can only grow in a minor version).

/// Source, root, record, container and listing identifiers.
pub const MAX_ID_BYTES: usize = 512;
/// Canonical source URIs.
pub const MAX_URI_BYTES: usize = 4096;
/// Small metadata strings: title, author, team, project, status, labels.
pub const MAX_METADATA_VALUE_BYTES: usize = 1024;
/// At most 32 labels per document.
pub const MAX_LABELS: usize = 32;
/// The connector-owned map is at most 16 KiB of JSON.
pub const MAX_CONNECTOR_MAP_BYTES: usize = 16 * 1024;
/// Bounded filterable `key=value` attributes per document.
pub const MAX_ATTRIBUTES: usize = 64;
pub const MAX_ATTRIBUTE_KEY_BYTES: usize = 128;
pub const MAX_ATTRIBUTE_BYTES: usize = 512;
/// Reader references per document, section, grant or group.
pub const MAX_READERS: usize = 16_384;
/// Opaque connector cursors (e.g. Drive page tokens).
pub const MAX_CURSOR_BYTES: usize = 16 * 1024;
/// Source revisions and permission revisions.
pub const MAX_REVISION_BYTES: usize = 256;
/// Reader and member references (`usr:<id>` / `grp:<id>`).
pub const MAX_READER_REF_BYTES: usize = 600;

/// Public request contract.
pub mod mcp {
    pub const MAX_QUERY_BYTES: usize = 4096;
    pub const DEFAULT_SEARCH_LIMIT: u32 = 50;
    pub const MAX_SEARCH_LIMIT: u32 = 75;
    /// The most documents a listing reaches: `offset + limit`. The reader
    /// reads every matching row before the first batch of an ordered scan, so a
    /// page deeper than this is a scan by the document key instead.
    pub const MAX_LISTING_WINDOW: u32 = 1_000;
    pub const MAX_QUERIES: usize = 16;
    pub const MULTI_QUERY_LIMIT: u32 = 20;
    pub const EXCERPT_CODEPOINTS: u32 = 1200;
    pub const MAX_EXCLUSIONS: usize = 100;
    pub const MAX_FILTER_VALUES: usize = 64;
    pub const MAX_ATTRIBUTE_FILTERS: usize = 16;
    /// Rows an `exact` search may scan: those that pass the filter and the
    /// caller's permissions. Each is read as a stored vector, so the scan costs
    /// the rows times twice the dimensions in bytes. Set from that estimate, not
    /// yet from a measurement.
    pub const MAX_EXACT_ROWS: u64 = 100_000;
    /// Sort keys of a listing's `order_by`.
    pub const MAX_ORDER_KEYS: usize = 4;
    /// Nodes of a `filter` tree: every `and`, `or`, `not` and comparison counts.
    pub const MAX_FILTER_NODES: usize = 64;
    /// Levels of a `filter` tree; each is a parenthesis of the SQL, and Lance's
    /// parser stops at 50.
    pub const MAX_FILTER_DEPTH: usize = 16;
    pub const DEFAULT_DEADLINE_MS: u32 = 10_000;
    pub const MAX_DEADLINE_MS: u32 = 300_000;
    pub const FETCH_MAX_DOCUMENTS: u32 = 5;
    pub const FETCH_MAX_RECORDS: u32 = 32;
    pub const FETCH_MAX_CONTENT_BYTES: u32 = 64 * 1024;
    pub const MAX_CURSOR_BYTES: usize = 16 * 1024;
    pub const MAX_WINDOW_CONTEXT_CODEPOINTS: u32 = 65_536;
    pub const LIST_FILTERS_MAX_VALUES: usize = 64;
    /// Hits each query keeps while the response budget still has tail hits to drop.
    pub const RESPONSE_MIN_HITS_PER_QUERY: usize = 4;
}

/// Bounds of the write API (not of connector documents).
pub mod write {
    /// The most documents one write filter may match, `delete_where` and
    /// `patch_where` each. A filter that matches more fails its request and
    /// changes nothing. The committer rewrites a matched document as a record
    /// of the envelope, so the bound keeps one envelope far below
    /// `writer.commit`'s limits.
    pub const MAX_FILTER_WRITE_DOCUMENTS: usize = 10_000;
}

/// Bounds of the namespace listing (not of connector documents).
pub mod listing {
    /// Namespaces one listing returns: more names than this match, and it fails.
    pub const MAX_NAMESPACES: usize = 10_000;
}

/// Bounds of a namespace's typed attribute schema (not of connector documents).
pub mod schema {
    /// Typed attributes a namespace can declare, each its own column and index.
    pub const MAX_ATTRIBUTES: usize = 128;
    /// Bytes in the name of a typed attribute.
    pub const MAX_NAME_BYTES: usize = 128;
    /// Bytes in the value of a `string` attribute.
    pub const MAX_VALUE_BYTES: usize = 4096;
}

/// Checks a required identifier: non-empty, no surrounding whitespace, no
/// control characters, at most [`MAX_ID_BYTES`]. The message names the field,
/// never the value.
pub fn check_id(field: &str, value: &str) -> Result<(), String> {
    check_text(field, value, MAX_ID_BYTES)
}

/// Checks bounded single-line text: non-empty, trimmed, no control characters.
pub fn check_text(field: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{field} is empty"));
    }
    if value.len() > max_bytes {
        return Err(format!("{field} exceeds {max_bytes} bytes"));
    }
    if value.trim() != value {
        return Err(format!("{field} has surrounding whitespace"));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{field} contains control characters"));
    }
    Ok(())
}

/// Checks a list bound.
pub fn check_count(field: &str, count: usize, max: usize) -> Result<(), String> {
    if count > max {
        return Err(format!("{field} has {count} entries, more than {max}"));
    }
    Ok(())
}

/// A `sha256:<64 lowercase hex>` digest.
pub fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

/// Exact record-text revision, independent of document revisions and siblings.
pub fn record_revision(source: &str, root: &str, record: &str, text: &str) -> String {
    digest(
        serde_json::json!([source, root, record, text])
            .to_string()
            .as_bytes(),
    )
}

/// Validates the canonical digest representation used by every contract.
pub fn check_digest(field: &str, value: &str) -> Result<(), String> {
    let valid = prefixed_hex(value, "sha256:", 64);
    if valid {
        Ok(())
    } else {
        Err(format!("{field} is not a sha256:<hex> digest"))
    }
}

/// ASCII hexadecimal text, including uppercase where existing inputs accept it.
pub fn ascii_hex(value: &str, digits: usize) -> bool {
    value.len() == digits && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn lowercase_hex(value: &str, digits: usize) -> bool {
    value.len() == digits
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn prefixed_hex(value: &str, prefix: &str, digits: usize) -> bool {
    value
        .strip_prefix(prefix)
        .is_some_and(|value| lowercase_hex(value, digits))
}

pub(crate) fn canonical_number<T: std::str::FromStr>(text: &str) -> Option<T> {
    (!text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text == "0" || !text.starts_with('0')))
    .then(|| text.parse().ok())
    .flatten()
}

#[cfg(test)]
mod tests {
    #[test]
    fn hex_checks_preserve_case_and_length_rules() {
        for (value, digits, ascii, lowercase) in [
            ("0123abcd", 8, true, true),
            ("0123ABCD", 8, true, false),
            ("", 0, true, true),
            ("abcd", 8, false, false),
            ("éA", 3, false, false),
            ("ab-g", 4, false, false),
        ] {
            assert_eq!(super::ascii_hex(value, digits), ascii, "{value}");
            assert_eq!(super::lowercase_hex(value, digits), lowercase, "{value}");
        }
    }
}
