"""Typed requests and responses of the fyrr HTTP API.

Generated from schemas/openapi.json by tools/generate_models.py. Do not edit.
"""

from __future__ import annotations

from typing import Any, Literal, NotRequired, TypeAlias, TypedDict


class PatchNamespaceRequest(TypedDict):
    """The body of `PATCH /v1/namespaces/{namespace}`: freezes the namespace or thaws it."""
    read_only: bool
    """`true` refuses writes and connector syncs; `false` allows them again."""


class DeleteNamespaceRequest(TypedDict):
    """The body of `DELETE /v1/namespaces/{namespace}`: the name, repeated."""
    namespace: str


class QueryRequest(TypedDict):
    """The body of `POST /v1/namespaces/{namespace}/query`: the arguments of the
    `search` tool, `consistency` and eleven members that are no tool arguments.
    """
    query: NotRequired[str | None]
    """Query text, 1-4096 UTF-8 bytes. Omit only for a filter-only listing with order=updated_at or order_by."""
    queries: NotRequired[list[str] | None]
    """Alternative to query: 2–16 queries, sharing filters and deadline."""
    vector: NotRequired[list[float] | None]
    """Query vector of the namespace's dimensions, instead of an embedding of query. Without query the search is dense only and makes no embedding call."""
    vectors: NotRequired[list[list[float]] | None]
    """Query vectors of a multi-vector `vector_attribute`, 1-32 of its dimensions, searched as one query. Not with vector."""
    exact: NotRequired[bool | None]
    """Exact search: score every row that passes the filter and the caller's permissions instead of probing the index. Refused as invalid_argument when more rows pass than the bound of an exact search."""
    vector_attribute: NotRequired[str | None]
    """Named vector attribute the dense search reads; default the built-in `vector`. Text needs an attribute with `from`, else send `vector`."""
    text_hits: NotRequired[int | None]
    """Hits carrying text per query, before deduplication; default all."""
    excerpt_chars: NotRequired[int | None]
    """Excerpt length in Unicode codepoints; default 1200; excerpt output only."""
    limit: NotRequired[int | None]
    """Hits per query; default 50 for query, default/max 20 for queries. A listing takes up to 1000 less its offset."""
    offset: NotRequired[int | None]
    """Documents a listing skips: only without query or queries, and offset plus limit is at most 1000."""
    output: NotRequired[OutputMode | None]
    """Text per hit; default excerpt."""
    order: NotRequired[SearchOrder | None]
    """Default relevance; updated_at sorts newest first, null dates last."""
    order_by: NotRequired[list[OrderKey] | None]
    """Listing order without query or queries, instead of order: 1-4 keys, the first most significant.
    Nulls sort first ascending and last descending; ties are broken by the document key.
    """
    sources: NotRequired[list[str] | None]
    """Source IDs."""
    source_types: NotRequired[list[str] | None]
    """Source types."""
    projects: NotRequired[list[str] | None]
    """Project IDs."""
    authors: NotRequired[list[str] | None]
    """Authors."""
    updated_after: NotRequired[McpTime | None]
    """Exclusive lower bound on updated_at (RFC 3339)."""
    updated_before: NotRequired[McpTime | None]
    """Exclusive upper bound on updated_at (RFC 3339)."""
    attributes: NotRequired[list[AttributeFilter] | None]
    """Connector attributes; a hit matches every entry."""
    exclude: NotRequired[list[DocumentKey] | None]
    """Documents to omit, e.g. ones already returned."""
    filter: NotRequired[Filter | None]
    """Filter tree of `and`, `or`, `not` and comparisons; a hit also matches the arguments above."""
    deadline_ms: NotRequired[int | None]
    """Deadline in milliseconds; default 10000."""
    consistency: NotRequired[Consistency | None]
    """Defaults to `cached`."""
    fields: NotRequired[ReturnFields | None]
    """What a hit returns besides its document fields."""
    fusion: NotRequired[Fusion | None]
    """With `queries`: one ranking of all sub-queries instead of their round-robin merge."""
    rerank: NotRequired[RerankChoice | None]
    """Switches the configured reranker off, or names what it scores."""
    sub_queries: NotRequired[list[SubQuery] | None]
    """1-16 searches of their own, each with its filter, ranking and limit, instead of `query`, `queries` and `vector`."""
    query_match: NotRequired[TermMatch | None]
    """With `query`: `all` ranks only the documents that hold every term; default `any`."""
    query_attributes: NotRequired[list[QueryAttribute] | None]
    """With `query` or `queries`: 1-8 attributes with a full-text index that the text ranking scores, each with a weight; default `searchable_text`. A string array attribute stands alone."""
    phrase: NotRequired[Phrase | None]
    """With `query`, `queries` or a vector ranking: the tokens of `text` occur in the document next to one another in this order; also required of a hit, like `filter`. Needs `fts_positions`. Not with a listing or `sub_queries`."""
    contains: NotRequired[list[Contains] | None]
    """Conditions on full-text attributes, each also required of a hit, like `filter`. Not with a listing."""
    boosts: NotRequired[list[Boost] | None]
    """1-8 terms added to the relevance score of a text or vector ranking: a weight times a function of an attribute. Not with a listing, `sub_queries` or `fusion`."""
    group_documents: NotRequired[bool | None]
    """`false` returns every matching record, not the best one of each document; default `true`. Not with a listing, `queries` or `sub_queries`."""
    caps: NotRequired[list[ValueCap] | None]
    """1-4 caps of at most `max` hits per value of an attribute; the total `limit` still applies. Not with a listing, `queries` or `sub_queries`."""


class OrderKey(TypedDict):
    """A built-in field or a typed attribute of the namespace to sort by. The
    reader refuses a field it does not know or whose values are not an orderable
    scalar: the connector labels, an array or `bytes`.
    """
    field: str
    direction: SortDirection


class AttributeFilter(TypedDict):
    """Equality (one value) or IN (several values) on one connector attribute."""
    key: str
    values: list[str]


class DocumentKey(TypedDict):
    """A document's logical identity."""
    source_id: str
    root_id: str


FilterAnd = TypedDict("FilterAnd", {"and": "list[Filter]"})


FilterOr = TypedDict("FilterOr", {"or": "list[Filter]"})


FilterNot = TypedDict("FilterNot", {"not": "Filter"})


class Comparison(TypedDict):
    """A field compared with a value; `in` and `not_in` take an array of values.
    The reader checks the field and the value against the namespace's schema.
    """
    field: str
    op: Op
    value: Any


class ReturnFields(TypedDict):
    """What a hit returns besides the document fields: the `fields` member of a
    JSON `query` request. `include` names the attributes to return, `exclude`
    the ones to leave out of all of them (an empty list leaves out none). The
    vector of a row is returned only when `include` names `vector`.
    """
    include: NotRequired[list[str] | None]
    exclude: NotRequired[list[str] | None]
    scores: NotRequired[bool]
    """Report the lexical score and the vector distance of each hit."""
    highlights: NotRequired[bool]
    """Report the ranges of the query terms in each excerpt, as the index analyzer matches them."""


class Fusion(TypedDict):
    """The `fusion` member of a JSON `query` request with `queries` or
    `sub_queries`: one ranking of the documents of all sub-queries, in place of
    their round-robin merge or their answers one after the other.
    A document scores the sum of `weight / (rank_constant + rank)` over the
    sub-queries that return it, rank counted from 1.
    """
    method: FusionMethod
    rank_constant: NotRequired[int | None]
    """Default 60."""
    weights: NotRequired[list[float] | None]
    """One weight per sub-query, in request order; default 1 each."""


class RerankChoice(TypedDict):
    """The `rerank` member of a JSON `query` request: it applies to the namespace's
    configured reranker, within that reranker's candidate count, deadline and
    spend limit. A namespace without a reranker refuses it.
    """
    enabled: NotRequired[bool | None]
    """`false` skips the reranker for this request; default `true`."""
    attribute: NotRequired[str | None]
    """The attribute whose value is scored: a string attribute, or the text
    attribute of the namespace. Default the text of the record.
    """
    query: NotRequired[str | None]
    """The text candidates are scored against, 1-4096 UTF-8 bytes. Default the
    text of each sub-query; with it a search by vector alone is reranked too.
    """


class SubQuery(TypedDict):
    """One member of `sub_queries` in a JSON `query` request: a search of its own
    by text, by vector, or by both as in the `search` tool, with its own
    filter and limit. The filter arguments and `filter` of the request apply to
    every sub-query as well.
    """
    query: NotRequired[str | None]
    """Query text, 1-4096 UTF-8 bytes."""
    vector: NotRequired[list[float] | None]
    """Query vector of the namespace's dimensions, instead of an embedding of query."""
    vectors: NotRequired[list[list[float]] | None]
    """Query vectors of a multi-vector `vector_attribute`, searched as one query. Not with vector."""
    vector_attribute: NotRequired[str | None]
    """Named vector attribute this sub-query searches; default the built-in `vector`. Text needs an attribute with `from`, else send `vector`."""
    filter: NotRequired[Filter | None]
    """Filter tree of this sub-query, joined by `and` with the request's filters."""
    limit: int
    """Hits of this sub-query. Without `fusion` the limits add up to at most the namespace's `max_documents`."""


class QueryAttribute(TypedDict):
    """One member of the `query_attributes` member of a JSON `query` request: an
    attribute with a full-text index that the text ranking scores, and how much
    its score counts. A document scores the sum of the weighted scores of the
    attributes it matches.
    """
    attribute: str
    """`searchable_text`, the text of the document, or a `string` or string
    array attribute with a full-text index.
    """
    weight: NotRequired[float | None]
    """0.000001 to 100; default 1."""


class Phrase(TypedDict):
    """The `phrase` member of a JSON `query` request: a condition on the text of the
    document, also required of a hit, like `filter`. The tokens of `text`, as the
    full-text analyzer cuts them, occur in the document in this order. Needs a
    namespace with `fts_positions`.
    """
    text: str
    """1-4096 UTF-8 bytes."""
    slop: NotRequired[int | None]
    """Tokens allowed between two tokens of the phrase; only 0 (adjacent) until a newer Lance."""


class Contains(TypedDict):
    """One condition of the `contains` member of a JSON `query` request, on the
    full-text index of a string or string array attribute, which is not built
    with the `ngram` tokenizer. Exactly one of `all` and `any`.
    """
    attribute: str
    """A string or string array attribute with a full-text index."""
    all: NotRequired[str | None]
    """The value, or one element of an array, holds every token of this text, 1-4096 UTF-8 bytes."""
    any: NotRequired[str | None]
    """The value, or one element of an array, holds at least one token of this text, 1-4096 UTF-8 bytes."""


class BoostAttribute(TypedDict):
    """`weight * x / (x + midpoint)` for a numeric attribute `x`: half the
    weight at the midpoint, the whole of it as `x` grows. A negative `x`
    counts as 0.
    """
    function: Literal["attribute"]
    attribute: str
    """An `int`, `uint` or `float` attribute."""
    weight: float
    """0 to 1000000."""
    midpoint: float
    """Greater than 0, the value of `x` that earns half the weight."""


class BoostRecency(TypedDict):
    """`weight * midpoint / (midpoint + d)`, where `d` is the distance of a
    `datetime` attribute from `origin`, before or after it: the whole
    weight at the origin, half of it a midpoint away. The reader has no
    clock, so the request names the origin, usually the time it is sent.
    """
    function: Literal["recency"]
    attribute: str
    """A `datetime` attribute, or `updated_at`."""
    weight: float
    """0 to 1000000."""
    midpoint: Duration
    """Greater than 0: an integer followed by ms, s, m, h or d."""
    origin: McpTime
    """RFC 3339."""


class ValueCap(TypedDict):
    """One member of the `caps` member of a JSON `query` request: at most `max`
    hits for each value of `attribute`. A hit that has no value of it is not
    capped.
    """
    attribute: str
    """`author`, `project`, `source_type` or a declared `string` attribute."""
    max: int
    """1 to 75."""


class FetchRequest(TypedDict):
    """The body of `POST /v1/namespaces/{namespace}/fetch`: the arguments of the
    `fetch` tool and `consistency`, which is no tool argument.
    """
    documents: NotRequired[list[FetchDocument] | None]
    """Ordered documents with individual record windows; alternative to other lookups."""
    uri: NotRequired[str | None]
    """Exact canonical source URI, matched byte for byte after trimming whitespace."""
    source_id: NotRequired[str | None]
    root_id: NotRequired[str | None]
    cursor: NotRequired[str | None]
    """next_cursor of the previous page."""
    window: NotRequired[FetchWindow | None]
    """A span of one record returned by search; needs source_id and root_id."""
    deadline_ms: NotRequired[int | None]
    """Deadline in milliseconds; default 10000."""
    consistency: NotRequired[Consistency | None]
    """Defaults to `cached`."""


class FetchDocument(TypedDict):
    """One ordered fetch lookup and its optional record window."""
    source_id: str
    root_id: str
    window: NotRequired[FetchWindow | None]


class FetchWindow(TypedDict):
    """A codepoint span of a record: start..end, or start with context on both sides."""
    record_id: str
    revision: str
    """The hit's revision token; a changed record returns document_changed."""
    start: int
    end: NotRequired[int | None]
    context: NotRequired[int | None]


class AggregateRequest(TypedDict):
    """The body of `POST /v1/namespaces/{namespace}/aggregate`: the aggregate
    arguments and `consistency`.
    """
    aggregations: list[Aggregation]
    """The aggregates to compute, in the order of every group's `values`."""
    group_by: NotRequired[list[str] | None]
    """Scalar fields to group by; without any, the whole selection is one group."""
    filter: NotRequired[Filter | None]
    """The documents to aggregate, as in `search`."""
    deadline_ms: NotRequired[int | None]
    """Deadline in milliseconds; default 10000."""
    consistency: NotRequired[Consistency | None]
    """Defaults to `cached`."""


class Aggregation(TypedDict):
    """One aggregate: `count` takes an optional field, the others need one."""
    op: AggregateOp
    field: NotRequired[str | None]


class RecallRequest(TypedDict):
    """The body of `POST /v1/namespaces/{namespace}/recall`: the recall arguments
    and `consistency`.
    """
    count: int
    """Stored vectors to sample, 1 to 100: each is searched by the index and
    exactly, and the two answers are compared.
    """
    filter: NotRequired[Filter | None]
    """The rows to sample and search, as in `search`: they must also be rows the caller may read."""
    deadline_ms: NotRequired[int | None]
    """Deadline in milliseconds; default 10000."""
    consistency: NotRequired[Consistency | None]
    """Defaults to `cached`."""


class WriteRequest(TypedDict):
    """The body of `POST /v1/namespaces/{namespace}/documents`."""
    settings: NotRequired[dict[str, Any] | None]
    """Part of the settings of a namespace that does not exist yet, laid over the
    process defaults. For a namespace that is published, a request with
    `settings` alone and only `attributes` and `attribute_indexes` in them
    changes its schema: attributes are added and indexes set; another type
    or a removed attribute is `attribute_type_change_refused` or
    `attribute_removal_refused`, rows with it `schema_change_with_rows`, and a
    namespace with no publication yet `namespace_not_published`.
    """
    upserts: NotRequired[list[WriteRow]]
    """Rows that replace the documents with their ids."""
    deletes: NotRequired[list[str]]
    """IDs to delete."""
    patches: NotRequired[list[WriteRow]]
    """Rows that write some attributes of a document that exists."""
    conditions: NotRequired[dict[str, Filter]]
    """From an id of the request to the filter that the stored document with
    that id must meet for the change to apply. Needs the `admin` role.
    """
    delete_where: NotRequired[Filter | None]
    """Deletes every document that matches the filter, before the rest of the
    request is applied. Needs the `admin` role.
    """
    patch_where: NotRequired[PatchWhere | None]
    """Patches every document that matches a filter, after `delete_where` and
    before the rest of the request. Needs the `admin` role.
    """


class PatchWhere(TypedDict):
    """The `patch_where` member of a write."""
    filter: Filter
    set: dict[str, Any]
    """A patch row without its `id`: attributes and `readers`, never a text."""


class NamespaceList(TypedDict):
    """What `GET /v1/namespaces` answers."""
    namespaces: list[str]
    """Names in name order."""


class NamespaceReport(TypedDict):
    """What `GET /v1/namespaces/{namespace}` answers: what the reader holds of the
    namespace's published version.
    """
    namespace: str
    publication_id: str
    schema: dict[str, AttributeType]
    """The typed attributes of the version, name to type."""
    settings: NamespaceSettings
    """The namespace's settings, as its policy records them."""
    dense_metric: DenseMetric
    """The distance of the vector index and the rescoring, fixed at creation."""
    dense_element: DenseElement
    """The element type of the stored vectors, fixed at creation."""
    read_only: bool
    """Whether the namespace is frozen: writes are refused while it stays readable."""
    rows: int
    """Approximate live rows."""
    logical_bytes: int
    """Approximate bytes of the data files of those rows, without indexes."""
    first_published_at: str
    """When the namespace's first content publication was committed."""
    unindexed: Unindexed
    """The data that an index of the version does not cover yet."""
    last_write_at: str
    """When the version was published."""


class AttributeTypeArray(TypedDict):
    """A list of values of one scalar type, written `{"array": "string"}`."""
    array: ElementType


class NamespaceSettings(TypedDict):
    """The index and retrieval settings a namespace is created with and keeps for
    its life. The process configuration only supplies them when the namespace is
    created; every reader and writer afterwards takes them from the namespace's
    policy artifact, which each content manifest pins.
    """
    embedding: EmbeddingIdentity
    fts: FtsSettings
    dense_rq_bits: int
    """RaBitQ bits per dimension of the vector index."""
    dense_partition_rows: int
    """Target rows per partition of the vector index."""
    dense_metric: NotRequired[DenseMetric]
    """The distance of the vector index and the rescoring; absent: cosine."""
    dense_element: NotRequired[DenseElement]
    """The element type of the stored vectors; absent: 16-bit floats."""
    retrieval: RetrievalSettings
    composition: CompositionSettings
    rerank: NotRequired[RerankSettings | None]
    attributes: NotRequired[dict[str, AttributeType]]
    """The typed attributes the namespace declares. Each is a nullable column;
    a request that changes the schema adds attributes and never changes or
    removes one.
    """
    attribute_indexes: NotRequired[dict[str, AttributeIndex]]
    """The indexes the attributes are to have where they are not the default.
    An index switched on is built by the next maintenance; the manifest
    records the ones that are built.
    """
    vectors: NotRequired[dict[str, VectorAttribute]]
    """The named vector attributes, fixed at creation."""
    write_api: NotRequired[WriteApiSettings | None]
    """Present: the namespace is written through the HTTP API and takes no
    connector source. Fixed with the rest of the settings.
    """


class EmbeddingIdentity(TypedDict):
    """The embedding identity bound into envelopes and publications."""
    model: str
    dimensions: int
    fingerprint: str
    """Digest of the model settings and text transformation (prefix,
    normalization) that keys the embedding cache.
    """
    settings: EmbeddingSettings


class EmbeddingSettings(TypedDict):
    """Vector-affecting settings recorded with a publication's model/dimensions."""
    api_style: EmbeddingApiStyle
    prefix: str
    roles: EmbeddingRoles
    send_dimensions: bool


class EmbeddingRoles(TypedDict):
    """Persisted role transformations accept future minor-version additions.
    Configuration uses strict types at its TOML boundary.
    """
    document: EmbeddingRole
    query: EmbeddingRole


class EmbeddingRole(TypedDict):
    prefix: NotRequired[str | None]
    input_type: NotRequired[str | None]


class FtsSettings(TypedDict):
    """FTS analyzer settings, part of the publication identity."""
    analyzer: Analyzer
    """How the index of `searchable_text` cuts a text into terms; the highlights
    and the excerpts of a hit cut it the same way.
    """
    with_position: bool
    """Positional postings, which a phrase condition needs."""


class Analyzer(TypedDict):
    """The settings of a full-text index that decide which terms a text yields.
    A text is lowercased always. Changing them on an attribute changes its index,
    which the writer's maintenance rebuilds while the old one keeps serving; the
    analyzer of `searchable_text` is fixed when the namespace is created.
    """
    tokenizer: Tokenizer
    language: NotRequired[Language | None]
    """Stems the terms and removes the stop words of the language. Absent: no
    stemming, and no stop words but `stop_words`.
    """
    stop_words: NotRequired[list[str] | None]
    """The stop words to remove instead of the language's: compared with a term
    after it is lowercased and stemmed.
    """
    ascii_folding: bool
    max_token_length: int
    """Tokens of this many UTF-8 bytes or more are dropped: a token is kept when it is
    shorter than this.
    """
    ngram_min_length: int
    """Only with the `ngram` tokenizer."""
    ngram_max_length: int
    """Only with the `ngram` tokenizer; not below `ngram_min_length`."""
    prefix_only: bool
    """Only with the `ngram` tokenizer: only the n-grams that start a token."""


class RetrievalSettings(TypedDict):
    """The persisted copy of [`RetrievalConfig`]."""
    rescore_depth: NotRequired[RescoreSettings | None]
    dense_weight: float
    dense_candidates: int
    lexical_candidates: int
    source_cap: int
    max_documents: int


class RescoreSettingsFixed(TypedDict):
    mode: Literal["fixed"]
    candidates: int


class RescoreSettingsAdaptive(TypedDict):
    mode: Literal["adaptive"]
    min_candidates: int
    max_candidates: int
    min_gap: float


class CompositionSettings(TypedDict):
    """The persisted copy of [`Composition`]."""
    fusion: int
    dense: int
    lexical: int


class RerankSettings(TypedDict):
    """What of a reranker decides results: recorded in the namespace when it is
    created. The endpoint, credential, spend caps and deadline stay process
    configuration ([`RerankConfig`]). Persisted types accept the fields a newer
    minor version adds, unlike their configuration counterparts.
    """
    model: str
    candidates: int
    """Authorized composed documents to rerank before the request limit."""
    weight: float
    """Logistic population-z score blend; 1 uses raw reranker order, 0 fusion order."""
    text_chars: NotRequired[int | None]
    """Per-candidate text bound in characters; absent sends the whole record."""


class AttributeIndex(TypedDict):
    """The indexes an attribute has: the scalar one unless it says otherwise, and a
    full-text or an n-gram index when it asks for one. An attribute without an
    index is still filtered and sorted: by a scan of the table. The settings of
    the analyzer belong to the full-text index and are only set with it; an
    index without one takes the namespace's text analyzer settings.
    """
    index: bool
    full_text: bool
    ngram: bool
    analyzer: NotRequired[Analyzer | None]


VectorAttribute = TypedDict("VectorAttribute", {"dimensions": "int", "from": "NotRequired[str | None]", "multi": "NotRequired[bool]"})


class WriteApiSettings(TypedDict):
    """How a namespace written through the HTTP API stores and shows its rows. Fixed
    for the life of the namespace.
    """
    text_attribute: str
    """The key of a row that holds its text: embedded, full-text indexed and
    returned as the content of the record. It is not a typed attribute and
    not one of `attributes`.
    """
    open: bool
    """`false`: a row is readable by the reader tokens it names, and a row
    that names none by nobody. `true`: every authenticated subject of the
    deployment reads every row.
    """


class Unindexed(TypedDict):
    """`rows` and `logical_bytes` of the data that no index covers yet."""
    rows: int
    logical_bytes: int


class PatchNamespaceResponse(TypedDict):
    """What a namespace switch answers."""
    namespace: str
    read_only: bool
    changed: bool
    """`false` when the namespace was already so."""


class DeleteNamespaceResponse(TypedDict):
    """What a namespace deletion answers."""
    deleted: bool


class SearchOutput(TypedDict):
    """`search` result."""
    hits: list[SearchHit]
    truncated: NotRequired[Truncation | None]
    """Set when the response budget trimmed text or hits (narrow or lower `limit`)."""
    metadata: ResponseMetadata


class SearchHit(TypedDict):
    """One hit, in a deterministic total order."""
    query_index: NotRequired[int | None]
    """Winning query's zero-based index, present only for multi-query search and sub-queries."""
    source_id: str
    root_id: str
    record_id: str
    revision: str
    """Revision token of the record."""
    source_uri: str
    source_type: str
    title: str | None
    author: str | None
    team: str | None
    project: str | None
    status: str | None
    labels: list[str]
    created_at: McpTime | None
    updated_at: McpTime | None
    score: float
    """Orders hits only."""
    excerpt: Excerpt | None
    content: str | None
    attributes: NotRequired[dict[str, Any]]
    """Typed attributes and vector asked for in `fields`."""
    lexical_score: NotRequired[float]
    """Raw lexical score, with `fields.scores`; absent when the lexical lane did not find the hit."""
    distance: NotRequired[float]
    """Raw vector distance, with `fields.scores`; absent when the dense lane did not find the hit."""


class Excerpt(TypedDict):
    """A literal passage of the record; offsets are Unicode codepoints in the record text."""
    text: str
    start: int
    end: int
    highlights: NotRequired[list[list[int]]]
    """With `fields.highlights`: the `[start, end)` ranges of the tokens that match the query, as the index analyzer cuts and reduces them, in the codepoints `start` and `end` count."""


class Truncation(TypedDict):
    text_dropped: int
    hits_dropped: int


class ResponseMetadata(TypedDict):
    """Metadata on every response."""
    schema_version: SchemaVersion
    publication_id: str
    content_version: int
    committed_at: McpTime
    degraded: list[Lane]
    """Lanes skipped by degradation (e.g. dense after a failed query embedding)."""
    stale_version: bool
    """The answer comes from an older version than the serving head the
    reader last read.
    """
    omitted_bytes: int
    """UTF-8 bytes omitted by clipping or the payload budget."""
    telemetry: Telemetry


class Telemetry(TypedDict):
    """Bounded per-request telemetry."""
    rerank_us: NotRequired[int | None]
    """Rerank preparation and concurrent provider wait, separate from retrieval."""
    cache: CacheState
    queue_wait_ms: int
    routing_strategy: NotRequired[RoutingMethod | None]
    """Effective centroid route; mixed means different routes across index deltas."""
    routing_fallback: NotRequired[RoutingFallback | None]
    """First optional routing failure across index deltas, never an I/O or auth error."""
    stage_us: NotRequired[dict[str, int]]
    """Fixed stage names, microseconds; no query or identity labels."""
    stage_s3_requests: NotRequired[dict[str, int]]
    """Requests and bytes on the same bounded stages."""
    stage_s3_bytes: NotRequired[dict[str, int]]
    unindexed_rows_scanned: int
    filter_rows_scanned: NotRequired[int]
    """Authorized rows inspected for this call's document counts, never cached."""
    lexical_candidates_fetched: NotRequired[int]
    """Lexical candidates returned, including repeated fetches for cutoff ties."""
    lexical_tie_bound_hits: NotRequired[int]
    """Unresolved lexical cutoff ties at the fixed fetch bound (zero or one)."""
    dense_candidates_fetched: NotRequired[int]
    """Dense candidates read, including repeated fetches for cutoff ties."""
    dense_tie_bound_hits: NotRequired[int]
    """Unresolved dense cutoff ties at the fixed fetch bound (zero or one)."""
    s3_requests: int
    s3_bytes: int


class FetchOutput(TypedDict):
    """`fetch` result."""
    documents: list[FetchedDocument]
    next_cursor: str | None
    document_changed: bool
    """Single-lookup window status; list requests report status on each document."""
    window: ServedWindow | None
    """Codepoint range served for a window, after the content limit."""
    metadata: ResponseMetadata


class FetchedDocument(TypedDict):
    """A document with its records in order."""
    document_changed: NotRequired[bool | None]
    """Per-document window status for a list request; changed records have no text."""
    window: NotRequired[ServedWindow | None]
    source_id: str
    root_id: str
    source_uri: str
    source_type: str
    title: str | None
    author: str | None
    team: str | None
    project: str | None
    status: str | None
    labels: list[str]
    created_at: McpTime | None
    updated_at: McpTime | None
    connector_map: dict[str, Any] | None
    """Untrusted connector metadata."""
    records: list[FetchedRecord]


class ServedWindow(TypedDict):
    """The span a window request returned."""
    record_id: str
    start: int
    end: int


class FetchedRecord(TypedDict):
    """One record's text."""
    record_id: str
    revision: str
    text: str


class AggregateOutput(TypedDict):
    """The `aggregate` result: groups in key order."""
    groups: list[AggregateGroup]
    metadata: ResponseMetadata


class AggregateGroup(TypedDict):
    """One group: its values of the `group_by` fields and its aggregates, both in
    request order. A `null` aggregate is one over no value.
    """
    key: list[Any]
    values: list[Any]


class RecallOutput(TypedDict):
    """The `recall` result: how closely the vector index of the rows the caller
    may read agrees with an exact search. It names no document.
    """
    samples: int
    """Stored vectors measured: `count`, or fewer when the selection holds fewer rows."""
    rows: int
    """Rows the caller may read that the filter selects: what each exact search scans."""
    approximate: bool
    """`false` when at least one sample was answered by an exact scan, not by the vector index, as a selective `filter`, a namespace without a vector index or a caller who reads only unindexed rows brings about: that sample compares two exact answers and says nothing of an index. Also `false` without a sample."""
    recall_at_10: float | None
    """The share of the ten nearest rows by an exact search that the index also found, averaged over `samples`; `null` without a sample."""
    recall_at_75: float | None
    """As `recall_at_10` for the 75 nearest rows."""


class WriteResponse(TypedDict):
    """What a write answers: the changes that applied."""
    upserted: int
    deleted: int
    patched: NotRequired[int | None]
    """Present when the request has patches."""
    skipped: NotRequired[int | None]
    """The patches and conditional changes that did not apply; present when the request has any."""


class ErrorBody(TypedDict):
    """The body of an error answer of the reader and of the write route. The
    namespace deletion answers a SCIM error instead.
    """
    error: str
    """A code that [`docs/http-api.md`](../docs/http-api.md) lists."""


# How much text a hit carries.
OutputMode: TypeAlias = Literal["metadata", "excerpt", "content"]


# Result order.
SearchOrder: TypeAlias = Literal["relevance", "updated_at"]


SortDirection: TypeAlias = Literal["asc", "desc"]


McpTime: TypeAlias = str


# A filter tree: `and` and `or` of one or more filters, `not` of one, or a
Filter: TypeAlias = FilterAnd | FilterOr | FilterNot | Comparison


# How a field is compared with a value. `starts_with`, `matches` and their
Op: TypeAlias = Literal["eq", "ne", "in", "not_in", "lt", "lte", "gt", "gte", "contains", "not_contains", "starts_with", "not_starts_with", "matches", "not_matches", "contains_any", "contains_all", "contains_none"]


# How current the version is that a `query` or `fetch` answer is read from.
Consistency: TypeAlias = Literal["cached", "strong"]


# How sub-queries are fused.
FusionMethod: TypeAlias = Literal["rrf"]


# The `query_match` member of a JSON `query` request: whether a document
TermMatch: TypeAlias = Literal["all", "any"]


# One term of the `boosts` member of a JSON `query` request: a weight times a
Boost: TypeAlias = BoostAttribute | BoostRecency


# Duration: an integer followed by ms, s, m, h or d.
Duration: TypeAlias = str


AggregateOp: TypeAlias = Literal["count", "sum", "avg", "min", "max"]


# One row of `upserts` or `patches`.
WriteRow: TypeAlias = dict[str, Any]


# The type of an attribute.
AttributeType: TypeAlias = Literal["string", "bool", "int", "float", "datetime", "uint", "uuid", "bytes"] | AttributeTypeArray


# The type of the elements of an array: every scalar type but `bytes`.
ElementType: TypeAlias = Literal["string", "int", "float", "bool", "datetime", "uint", "uuid"]


# Fixed protocol identity recorded in persisted embedding settings.
EmbeddingApiStyle: TypeAlias = Literal["openai-compatible"]


# The way a text is cut into tokens.
Tokenizer: TypeAlias = Literal["simple", "whitespace", "unicode", "ngram"]


# A language with a stemmer and a stop-word list.
Language: TypeAlias = Literal["arabic", "danish", "dutch", "english", "finnish", "french", "german", "greek", "hungarian", "italian", "norwegian", "portuguese", "romanian", "russian", "spanish", "swedish", "tamil", "turkish"]


# The distance of the dense index. Lance's IVF_RQ index supports all three on
DenseMetric: TypeAlias = Literal["cosine", "squared_l2", "dot"]


# The element type of a stored vector. A 16-bit float halves the vector
DenseElement: TypeAlias = Literal["f16", "f32"]


# The persisted copy of [`RescoreDepth`].
RescoreSettings: TypeAlias = RescoreSettingsFixed | RescoreSettingsAdaptive


# Contract schema version `<major>.<minor>`. Readers accept any minor of their major and reject other majors.
SchemaVersion: TypeAlias = str


# A retrieval lane.
Lane: TypeAlias = Literal["dense", "lexical", "rerank"]


# Cache state of the request.
CacheState: TypeAlias = Literal["hot", "warm", "cold"]


# Effective routing used by this request's native vector indexes.
RoutingMethod: TypeAlias = Literal["exact", "rabitq", "mixed"]


# Bounded reasons for using exact centroid routing instead of RaBitQ.
RoutingFallback: TypeAlias = Literal["absent", "build_failed", "memory_budget", "eligible_shortlist"]
