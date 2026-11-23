# Limits

> **In short**
>
> - One table lists every constant of
>   [`crates/fyrr-core/src/limits.rs`](../crates/fyrr-core/src/limits.rs),
>   the bounds that the contracts, the MCP tools and the HTTP routes share.
> - `scripts/check-docs` compares the table with that file: a constant
>   without a row, a row without a constant and a different value fail the
>   check.
> - Limits that are settings (concurrency, memory, mirror size, body sizes of
>   the write route and the HTTP listener) are not constants; they are in the
>   pages listed [below the table](#limits-outside-the-table).

What a request over a limit gets depends on the route, and the table does not
repeat it: the codes are in the [MCP tool errors](mcp-tools.md#errors), the
[HTTP errors](http-api.md#errors) and the [write errors](http-api.md#write-documents).
For example, a search argument over its limit is `400 invalid_argument`, a write
row over its size is `413 row_too_large`, a listing of too many names is
`400 too_many_namespaces` and a filter write that matches too many documents is
`409 rejected`. A connector document or batch over one of the first twelve
limits is invalid ([How fyrr works](architecture.md#publication)). Sizes are in
bytes unless it says codepoints. A row with `default` in its meaning is not a
bound; it is what applies when the request names none.

| Constant | Value | Meaning |
| --- | --- | --- |
| `limits::MAX_ID_BYTES` | 512 | Source, root, record, container and listing identifiers. |
| `limits::MAX_URI_BYTES` | 4,096 | Canonical source URIs. |
| `limits::MAX_METADATA_VALUE_BYTES` | 1,024 | Small metadata strings of a connector document: title, author, team, project, status, labels. |
| `limits::MAX_LABELS` | 32 | Labels per connector document. |
| `limits::MAX_CONNECTOR_MAP_BYTES` | 16,384 | The connector-owned map of a document, as JSON. |
| `limits::MAX_ATTRIBUTES` | 64 | Filterable `key=value` attributes per connector document. |
| `limits::MAX_ATTRIBUTE_KEY_BYTES` | 128 | The key of such an attribute. |
| `limits::MAX_ATTRIBUTE_BYTES` | 512 | The value of such an attribute. |
| `limits::MAX_READERS` | 16,384 | Reader references per document, section, grant or group. |
| `limits::MAX_CURSOR_BYTES` | 16,384 | An opaque connector cursor. |
| `limits::MAX_REVISION_BYTES` | 256 | A source revision or a permission revision. |
| `limits::MAX_READER_REF_BYTES` | 600 | A reader or member reference, `usr:<id>` or `grp:<id>`. |
| `limits::mcp::MAX_QUERY_BYTES` | 4,096 | The text of one `query`. |
| `limits::mcp::DEFAULT_SEARCH_LIMIT` | 50 | `limit` of a `search` with one `query` or a listing, default. |
| `limits::mcp::MAX_SEARCH_LIMIT` | 75 | `limit` of a `search` with a `query` or a `vector`, at most. |
| `limits::mcp::MAX_LISTING_WINDOW` | 1,000 | `offset + limit` of a listing, at most; a deeper page is a scan by the document key. |
| `limits::mcp::MAX_QUERIES` | 16 | Entries of `queries`. |
| `limits::mcp::MAX_CONTAINS` | 16 | `contains` conditions of one query. |
| `limits::mcp::MAX_BOOSTS` | 8 | `boosts` of one query. |
| `limits::mcp::MAX_CAPS` | 4 | Attributes `caps` limits in one query. |
| `limits::mcp::MAX_BOOST_WEIGHT` | 1,000,000 | The largest weight of a boost. |
| `limits::mcp::DEFAULT_RANK_CONSTANT` | 60 | The rank constant of a reciprocal rank fusion that names none, default. |
| `limits::mcp::MAX_FUSION_WEIGHT` | 1,000,000 | The largest weight of a fusion. |
| `limits::mcp::MULTI_QUERY_LIMIT` | 20 | `limit` per query of a search with `queries`: its default and its maximum. |
| `limits::mcp::EXCERPT_CODEPOINTS` | 1,200 | Codepoints of an excerpt: its default and its maximum. |
| `limits::mcp::MAX_HIGHLIGHTS` | 100 | Match ranges one excerpt reports. |
| `limits::mcp::MAX_EXCLUSIONS` | 100 | Entries of `exclude`. |
| `limits::mcp::MAX_FILTER_VALUES` | 64 | Values in one list of a filter or a source, project, author or attribute list. |
| `limits::mcp::MAX_REGEX_BYTES` | 512 | The regular expression of a `matches` filter. |
| `limits::mcp::MAX_REGEX_PROGRAM_BYTES` | 262,144 | The compiled program of a `matches` filter. |
| `limits::mcp::MAX_ATTRIBUTE_FILTERS` | 16 | Entries of the `attributes` filter of a search. |
| `limits::mcp::MAX_EXACT_ROWS` | 100,000 | Rows an `exact` search may scan, after the filter and the caller's permissions. |
| `limits::mcp::MAX_ORDER_KEYS` | 4 | Sort keys of a listing's `order_by`. |
| `limits::mcp::MAX_FILTER_NODES` | 64 | Nodes of a `filter` tree: every `and`, `or`, `not` and comparison counts. |
| `limits::mcp::MAX_FILTER_DEPTH` | 16 | Levels of a `filter` tree. |
| `limits::mcp::DEFAULT_DEADLINE_MS` | 10,000 | `deadline_ms` of a request that names none, default. |
| `limits::mcp::MAX_DEADLINE_MS` | 300,000 | `deadline_ms`, at most. |
| `limits::mcp::FETCH_MAX_DOCUMENTS` | 5 | Documents of one `fetch` page; the rest follows by `next_cursor`. |
| `limits::mcp::FETCH_MAX_RECORDS` | 32 | Records of one `fetch` page; the rest follows by `next_cursor`. |
| `limits::mcp::FETCH_MAX_CONTENT_BYTES` | 65,536 | Content bytes of one `fetch` page; the rest follows by `next_cursor`. |
| `limits::mcp::MAX_CURSOR_BYTES` | 16,384 | The longest `fetch` cursor the reader issues or accepts. |
| `limits::mcp::MAX_WINDOW_CONTEXT_CODEPOINTS` | 65,536 | `context` of a `fetch` window. |
| `limits::mcp::LIST_FILTERS_MAX_VALUES` | 64 | Values `list_filters` reports per field; more, and the field answers `too_many` with no values. |
| `limits::mcp::MAX_AGGREGATIONS` | 16 | Aggregates one `aggregate` request computes. |
| `limits::mcp::MAX_GROUP_BY_FIELDS` | 4 | Fields one `aggregate` request groups by. |
| `limits::mcp::MAX_AGGREGATE_GROUPS` | 1,000 | Groups of one `aggregate` answer; more, and the request fails. |
| `limits::mcp::RESPONSE_MIN_HITS_PER_QUERY` | 4 | Hits each query keeps while `reader.max_response_bytes` still drops tail hits. |
| `limits::write::MAX_FILTER_WRITE_DOCUMENTS` | 10,000 | Documents one `delete_where` or `patch_where` may match; more, and the request is `409 rejected` and changes nothing. |
| `limits::recall::MAX_SAMPLES` | 100 | Stored vectors one `recall` request samples. |
| `limits::listing::MAX_NAMESPACES` | 10,000 | Namespaces one listing returns; more match, and it fails with `too_many_namespaces`. |
| `limits::schema::MAX_ATTRIBUTES` | 128 | Typed attributes a namespace can declare. |
| `limits::schema::MAX_NAME_BYTES` | 128 | The name of a typed attribute. |
| `limits::schema::MAX_VALUE_BYTES` | 4,096 | The value of a `string` attribute. |

## Limits outside the table

These are not constants of `limits.rs`:

- Every `[reader]`, `[writer]`, `[namespace]` and `[identity]` key with a range
  is in [Configuration](configuration.md), [Reader and writer](reader-writer.md),
  [Tuning](tuning.md) and [Identity and access](identity.md). `scripts/check-docs`
  checks each range against `schemas/config.schema.json`.
- The argument ranges of the MCP tools are in [MCP tools](mcp-tools.md),
  checked against `fixtures/mcp/tools-list.json`.
- The bounds of the HTTP listener (connections, requests in flight, body and
  header sizes) are in [HTTP JSON API](http-api.md#limits). The sizes of a
  write request (rows, record text, settings, request body) are in
  [Write documents](http-api.md#write-documents). No script checks these two
  against the code yet.
