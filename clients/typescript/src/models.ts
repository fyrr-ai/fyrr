// Typed requests and responses of the fyrr HTTP API.
//
// Generated from schemas/openapi.json by tools/generate-models.ts. Do not edit.

/** The body of `PATCH /v1/namespaces/{namespace}`: freezes the namespace or thaws it. */
export interface PatchNamespaceRequest {
  /** `true` refuses writes and connector syncs; `false` allows them again. */
  read_only: boolean;
}

/** The body of `DELETE /v1/namespaces/{namespace}`: the name, repeated. */
export interface DeleteNamespaceRequest {
  namespace: string;
}

/**
 * The body of `POST /v1/namespaces/{namespace}/query`: the arguments of the
 * `search` tool and twelve members that are no tool arguments.
 */
export interface QueryRequest {
  /** Query text, 1-4096 UTF-8 bytes. Omit only for a filter-only listing with order=updated_at or order_by. */
  query?: string | null;
  /** Alternative to query: 2–16 queries, sharing filters and deadline. */
  queries?: string[] | null;
  /** Query vector of the namespace's dimensions, instead of an embedding of query. Without query the search is dense only and makes no embedding call. */
  vector?: number[] | null;
  /** Query vectors of a multi-vector `vector_attribute`, 1-32 of its dimensions, searched as one query. Not with vector. */
  vectors?: number[][] | null;
  /** Exact search: score every row that passes the filter and the caller's permissions instead of probing the index. Refused as invalid_argument when more rows pass than the bound of an exact search. */
  exact?: boolean | null;
  /** Named vector attribute the dense search reads; default the built-in `vector`. Text needs an attribute with `from`, else send `vector`. */
  vector_attribute?: string | null;
  /** Hits carrying text per query, before deduplication; default all. */
  text_hits?: number | null;
  /** Excerpt length in Unicode codepoints; default 1200; excerpt output only. */
  excerpt_chars?: number | null;
  /** Hits per query; default 50 for query, default/max 20 for queries. A listing takes up to 1000 less its offset. */
  limit?: number | null;
  /** Documents a listing skips: only without query or queries, and offset plus limit is at most 1000. */
  offset?: number | null;
  /** Text per hit; default excerpt. */
  output?: OutputMode | null;
  /** Default relevance; updated_at sorts newest first, null dates last. */
  order?: SearchOrder | null;
  /**
   * Listing order without query or queries, instead of order: 1-4 keys, the first most significant.
   * Nulls sort first ascending and last descending; ties are broken by the document key.
   */
  order_by?: OrderKey[] | null;
  /** Source IDs. */
  sources?: string[] | null;
  /** Source types. */
  source_types?: string[] | null;
  /** Project IDs. */
  projects?: string[] | null;
  /** Authors. */
  authors?: string[] | null;
  /** Exclusive lower bound on updated_at (RFC 3339). */
  updated_after?: McpTime | null;
  /** Exclusive upper bound on updated_at (RFC 3339). */
  updated_before?: McpTime | null;
  /** Connector attributes; a hit matches every entry. */
  attributes?: AttributeFilter[] | null;
  /** Documents to omit, e.g. ones already returned. */
  exclude?: DocumentKey[] | null;
  /** Filter tree of `and`, `or`, `not` and comparisons; a hit also matches the arguments above. */
  filter?: Filter | null;
  /** Deadline in milliseconds; default 10000. */
  deadline_ms?: number | null;
  /** Defaults to `cached`. */
  consistency?: Consistency | null;
  /** What a hit returns besides its document fields. */
  fields?: ReturnFields | null;
  /** With `queries`: one ranking of all sub-queries instead of their round-robin merge. */
  fusion?: Fusion | null;
  /** Switches the configured reranker off, or names what it scores. */
  rerank?: RerankChoice | null;
  /** 1-16 searches of their own, each with its filter, ranking and limit, instead of `query`, `queries` and `vector`. */
  sub_queries?: SubQuery[] | null;
  /** With `query`: `all` ranks only the documents that hold every term; default `any`. */
  query_match?: TermMatch | null;
  /** With `query` or `queries`: 1-8 attributes with a full-text index that the text ranking scores, each with a weight; default `searchable_text`. A string array attribute stands alone. */
  query_attributes?: QueryAttribute[] | null;
  /** With `query`, `queries` or a vector ranking: the tokens of `text` occur in the document next to one another in this order; also required of a hit, like `filter`. Needs `fts_positions`. Not with a listing or `sub_queries`. */
  phrase?: Phrase | null;
  /** Conditions on full-text attributes, each also required of a hit, like `filter`. Not with a listing. */
  contains?: Contains[] | null;
  /** 1-8 terms added to the relevance score of a text or vector ranking: a weight times a function of an attribute. Not with a listing, `sub_queries` or `fusion`. */
  boosts?: Boost[] | null;
  /** `false` returns every matching record, not the best one of each document; default `true`. Not with a listing, `queries` or `sub_queries`. */
  group_documents?: boolean | null;
  /** 1-4 caps of at most `max` hits per value of an attribute; the total `limit` still applies. Not with a listing, `queries` or `sub_queries`. */
  caps?: ValueCap[] | null;
}

/** How much text a hit carries. */
export type OutputMode = "metadata" | "excerpt" | "content";

/** Result order. */
export type SearchOrder = "relevance" | "updated_at";

/**
 * A built-in field or a typed attribute of the namespace to sort by. The
 * reader refuses a field it does not know or whose values are not an orderable
 * scalar: the connector labels, an array or `bytes`.
 */
export interface OrderKey {
  field: string;
  direction: SortDirection;
}

export type SortDirection = "asc" | "desc";

export type McpTime = string;

/** Equality (one value) or IN (several values) on one connector attribute. */
export interface AttributeFilter {
  key: string;
  values: string[];
}

/** A document's logical identity. */
export interface DocumentKey {
  source_id: string;
  root_id: string;
}

export interface FilterAnd {
  and: Filter[];
}

export interface FilterOr {
  or: Filter[];
}

export interface FilterNot {
  not: Filter;
}

/**
 * A filter tree: `and` and `or` of one or more filters, `not` of one, or a
 * comparison of a field with a value. At most [`MAX_FILTER_NODES`] nodes in
 * [`MAX_FILTER_DEPTH`] levels. It is read from the request in one pass that
 * stops at the first bound it exceeds.
 */
export type Filter = FilterAnd | FilterOr | FilterNot | Comparison;

/**
 * A field compared with a value; `in` and `not_in` take an array of values.
 * The reader checks the field and the value against the namespace's schema.
 */
export interface Comparison {
  field: string;
  op: Op;
  value: unknown;
}

/**
 * How a field is compared with a value. `starts_with`, `matches` and their
 * negations test the text of a `string`; `contains` and `not_contains` test
 * the text of a `string` or one element of an array. The last three test the
 * elements of an array attribute and apply to nothing else.
 */
export type Op = "eq" | "ne" | "in" | "not_in" | "lt" | "lte" | "gt" | "gte" | "contains" | "not_contains" | "starts_with" | "not_starts_with" | "matches" | "not_matches" | "contains_any" | "contains_all" | "contains_none";

/** How current the version is that a `query` or `fetch` answer is read from. */
export type Consistency = "cached" | "strong";

/**
 * What a hit returns besides the document fields: the `fields` member of a
 * JSON `query` request. `include` names the attributes to return, `exclude`
 * the ones to leave out of all of them (an empty list leaves out none). The
 * vector of a row is returned only when `include` names `vector`.
 */
export interface ReturnFields {
  include?: string[] | null;
  exclude?: string[] | null;
  /** Report the lexical score and the vector distance of each hit. */
  scores?: boolean;
  /** Report the ranges of the query terms in each excerpt, as the index analyzer matches them. */
  highlights?: boolean;
}

/**
 * The `fusion` member of a JSON `query` request with `queries` or
 * `sub_queries`: one ranking of the documents of all sub-queries, in place of
 * their round-robin merge or their answers one after the other.
 * A document scores the sum of `weight / (rank_constant + rank)` over the
 * sub-queries that return it, rank counted from 1.
 */
export interface Fusion {
  method: FusionMethod;
  /** Default 60. */
  rank_constant?: number | null;
  /** One weight per sub-query, in request order; default 1 each. */
  weights?: number[] | null;
}

/** How sub-queries are fused. */
export type FusionMethod = "rrf";

/**
 * The `rerank` member of a JSON `query` request: it applies to the namespace's
 * configured reranker, within that reranker's candidate count, deadline and
 * spend limit. A namespace without a reranker refuses it.
 */
export interface RerankChoice {
  /** `false` skips the reranker for this request; default `true`. */
  enabled?: boolean | null;
  /**
   * The attribute whose value is scored: a string attribute, or the text
   * attribute of the namespace. Default the text of the record.
   */
  attribute?: string | null;
  /**
   * The text candidates are scored against, 1-4096 UTF-8 bytes. Default the
   * text of each sub-query; with it a search by vector alone is reranked too.
   */
  query?: string | null;
}

/**
 * One member of `sub_queries` in a JSON `query` request: a search of its own
 * by text, by vector, or by both as in the `search` tool, with its own
 * filter and limit. The filter arguments and `filter` of the request apply to
 * every sub-query as well.
 */
export interface SubQuery {
  /** Query text, 1-4096 UTF-8 bytes. */
  query?: string | null;
  /** Query vector of the namespace's dimensions, instead of an embedding of query. */
  vector?: number[] | null;
  /** Query vectors of a multi-vector `vector_attribute`, searched as one query. Not with vector. */
  vectors?: number[][] | null;
  /** Named vector attribute this sub-query searches; default the built-in `vector`. Text needs an attribute with `from`, else send `vector`. */
  vector_attribute?: string | null;
  /** Filter tree of this sub-query, joined by `and` with the request's filters. */
  filter?: Filter | null;
  /** Hits of this sub-query. Without `fusion` the limits add up to at most the namespace's `max_documents`. */
  limit: number;
}

/**
 * The `query_match` member of a JSON `query` request: whether a document
 * needs every term of the text query or any one of them. Default `any`.
 */
export type TermMatch = "all" | "any";

/**
 * One member of the `query_attributes` member of a JSON `query` request: an
 * attribute with a full-text index that the text ranking scores, and how much
 * its score counts. A document scores the sum of the weighted scores of the
 * attributes it matches.
 */
export interface QueryAttribute {
  /**
   * `searchable_text`, the text of the document, or a `string` or string
   * array attribute with a full-text index.
   */
  attribute: string;
  /** 0.000001 to 100; default 1. */
  weight?: number | null;
}

/**
 * The `phrase` member of a JSON `query` request: a condition on the text of the
 * document, also required of a hit, like `filter`. The tokens of `text`, as the
 * full-text analyzer cuts them, occur in the document in this order. Needs a
 * namespace with `fts_positions`.
 */
export interface Phrase {
  /** 1-4096 UTF-8 bytes. */
  text: string;
  /** Tokens allowed between two tokens of the phrase; only 0 (adjacent) until a newer Lance. */
  slop?: number | null;
}

/**
 * One condition of the `contains` member of a JSON `query` request, on the
 * full-text index of a string or string array attribute, which is not built
 * with the `ngram` tokenizer. Exactly one of `all` and `any`.
 */
export interface Contains {
  /** A string or string array attribute with a full-text index. */
  attribute: string;
  /** The value, or one element of an array, holds every token of this text, 1-4096 UTF-8 bytes. */
  all?: string | null;
  /** The value, or one element of an array, holds at least one token of this text, 1-4096 UTF-8 bytes. */
  any?: string | null;
}

/**
 * `weight * x / (x + midpoint)` for a numeric attribute `x`: half the
 * weight at the midpoint, the whole of it as `x` grows. A negative `x`
 * counts as 0.
 */
export interface BoostAttribute {
  function: "attribute";
  /** An `int`, `uint` or `float` attribute. */
  attribute: string;
  /** 0 to 1000000. */
  weight: number;
  /** Greater than 0, the value of `x` that earns half the weight. */
  midpoint: number;
}

/**
 * `weight * midpoint / (midpoint + d)`, where `d` is the distance of a
 * `datetime` attribute from `origin`, before or after it: the whole
 * weight at the origin, half of it a midpoint away. The reader has no
 * clock, so the request names the origin, usually the time it is sent.
 */
export interface BoostRecency {
  function: "recency";
  /** A `datetime` attribute, or `updated_at`. */
  attribute: string;
  /** 0 to 1000000. */
  weight: number;
  /** Greater than 0: an integer followed by ms, s, m, h or d. */
  midpoint: Duration;
  /** RFC 3339. */
  origin: McpTime;
}

/**
 * One term of the `boosts` member of a JSON `query` request: a weight times a
 * function of an attribute of the document, added to its relevance score. The
 * score of a text, vector or fused ranking lies between 0 and 1, and each
 * function lies between 0 and 1 too, so a weight is the most a term can add.
 * A document without a value of the attribute gets 0 from the term.
 */
export type Boost = BoostAttribute | BoostRecency;

/** Duration: an integer followed by ms, s, m, h or d. */
export type Duration = string;

/**
 * One member of the `caps` member of a JSON `query` request: at most `max`
 * hits for each value of `attribute`. A hit that has no value of it is not
 * capped.
 */
export interface ValueCap {
  /** `author`, `project`, `source_type` or a declared `string` attribute. */
  attribute: string;
  /** 1 to 75. */
  max: number;
}

/**
 * The body of `POST /v1/namespaces/{namespace}/fetch`: the arguments of the
 * `fetch` tool and `consistency`, which is no tool argument.
 */
export interface FetchRequest {
  /** Ordered documents with individual record windows; alternative to other lookups. */
  documents?: FetchDocument[] | null;
  /** Exact canonical source URI, matched byte for byte after trimming whitespace. */
  uri?: string | null;
  source_id?: string | null;
  root_id?: string | null;
  /** next_cursor of the previous page. */
  cursor?: string | null;
  /** A span of one record returned by search; needs source_id and root_id. */
  window?: FetchWindow | null;
  /** Deadline in milliseconds; default 10000. */
  deadline_ms?: number | null;
  /** Defaults to `cached`. */
  consistency?: Consistency | null;
}

/** One ordered fetch lookup and its optional record window. */
export interface FetchDocument {
  source_id: string;
  root_id: string;
  window?: FetchWindow | null;
}

/** A codepoint span of a record: start..end, or start with context on both sides. */
export interface FetchWindow {
  record_id: string;
  /** The hit's revision token; a changed record returns document_changed. */
  revision: string;
  start: number;
  end?: number | null;
  context?: number | null;
}

/**
 * The body of `POST /v1/namespaces/{namespace}/aggregate`: the aggregate
 * arguments and `consistency`.
 */
export interface AggregateRequest {
  /** The aggregates to compute, in the order of every group's `values`. */
  aggregations: Aggregation[];
  /** Scalar fields to group by; without any, the whole selection is one group. */
  group_by?: string[] | null;
  /** The documents to aggregate, as in `search`. */
  filter?: Filter | null;
  /** Deadline in milliseconds; default 10000. */
  deadline_ms?: number | null;
  /** Defaults to `cached`. */
  consistency?: Consistency | null;
}

/** One aggregate: `count` takes an optional field, the others need one. */
export interface Aggregation {
  op: AggregateOp;
  field?: string | null;
}

export type AggregateOp = "count" | "sum" | "avg" | "min" | "max";

/**
 * The body of `POST /v1/namespaces/{namespace}/recall`: the recall arguments
 * and `consistency`.
 */
export interface RecallRequest {
  /**
   * Stored vectors to sample, 1 to 100: each is searched by the index and
   * exactly, and the two answers are compared.
   */
  count: number;
  /** The rows to sample and search, as in `search`: they must also be rows the caller may read. */
  filter?: Filter | null;
  /** Deadline in milliseconds; default 10000. */
  deadline_ms?: number | null;
  /** Defaults to `cached`. */
  consistency?: Consistency | null;
}

/** The body of `POST /v1/namespaces/{namespace}/documents`. */
export interface WriteRequest {
  /**
   * Part of the settings of a namespace that does not exist yet, laid over the
   * process defaults. For a namespace that is published, a request with
   * `settings` alone and only `attributes` and `attribute_indexes` in them
   * changes its schema: attributes are added and indexes set; another type
   * or a removed attribute is `attribute_type_change_refused` or
   * `attribute_removal_refused`, rows with it `schema_change_with_rows`, and a
   * namespace with no publication yet `namespace_not_published`.
   */
  settings?: Record<string, unknown> | null;
  /** Rows that replace the documents with their ids. */
  upserts?: WriteRow[];
  /** IDs to delete. */
  deletes?: string[];
  /** Rows that write some attributes of a document that exists. */
  patches?: WriteRow[];
  /**
   * From an id of the request to the filter that the stored document with
   * that id must meet for the change to apply. Needs the `admin` role.
   */
  conditions?: Record<string, Filter>;
  /**
   * Deletes every document that matches the filter, before the rest of the
   * request is applied. Needs the `admin` role.
   */
  delete_where?: Filter | null;
  /**
   * Patches every document that matches a filter, after `delete_where` and
   * before the rest of the request. Needs the `admin` role.
   */
  patch_where?: PatchWhere | null;
}

/** One row of `upserts` or `patches`. */
export interface WriteRow {
  /** The row's key in the namespace. */
  id: string;
  /** `usr:<id>` or `grp:<id>`; ignored by an open namespace. */
  readers?: string[] | null;
  /** The vector of the row's text, of the namespace's dimension. */
  vector?: number[] | null;
  [key: string]: unknown;
}

/** The `patch_where` member of a write. */
export interface PatchWhere {
  filter: Filter;
  /** A patch row without its `id`: attributes and `readers`, never a text. */
  set: Record<string, unknown>;
}

/** What `GET /v1/namespaces` answers. */
export interface NamespaceList {
  /** Names in name order. */
  namespaces: string[];
}

/**
 * What `GET /v1/namespaces/{namespace}` answers: what the reader holds of the
 * namespace's published version.
 */
export interface NamespaceReport {
  namespace: string;
  publication_id: string;
  /** The typed attributes of the version, name to type. */
  schema: Record<string, AttributeType>;
  /** The namespace's settings, as its policy records them. */
  settings: NamespaceSettings;
  /** The distance of the vector index and the rescoring, fixed at creation. */
  dense_metric: DenseMetric;
  /** The element type of the stored vectors, fixed at creation. */
  dense_element: DenseElement;
  /** Whether the namespace is frozen: writes are refused while it stays readable. */
  read_only: boolean;
  /** Approximate live rows. */
  rows: number | bigint;
  /** Approximate bytes of the data files of those rows, without indexes. */
  logical_bytes: number | bigint;
  /** When the namespace's first content publication was committed. */
  first_published_at: string;
  /** The data that an index of the version does not cover yet. */
  unindexed: Unindexed;
  /** When the version was published. */
  last_write_at: string;
}

/** A list of values of one scalar type, written `{"array": "string"}`. */
export interface AttributeTypeArray {
  array: ElementType;
}

/** The type of an attribute. */
export type AttributeType = "string" | "bool" | "int" | "float" | "datetime" | "uint" | "uuid" | "bytes" | AttributeTypeArray;

/** The type of the elements of an array: every scalar type but `bytes`. */
export type ElementType = "string" | "int" | "float" | "bool" | "datetime" | "uint" | "uuid";

/**
 * The index and retrieval settings a namespace is created with and keeps for
 * its life. The process configuration only supplies them when the namespace is
 * created; every reader and writer afterwards takes them from the namespace's
 * policy artifact, which each content manifest pins.
 */
export interface NamespaceSettings {
  embedding: EmbeddingIdentity;
  fts: FtsSettings;
  /** RaBitQ bits per dimension of the vector index. */
  dense_rq_bits: number;
  /** Target rows per partition of the vector index. */
  dense_partition_rows: number;
  /** The distance of the vector index and the rescoring; absent: cosine. */
  dense_metric?: DenseMetric;
  /** The element type of the stored vectors; absent: 16-bit floats. */
  dense_element?: DenseElement;
  retrieval: RetrievalSettings;
  composition: CompositionSettings;
  rerank?: RerankSettings | null;
  /**
   * The typed attributes the namespace declares. Each is a nullable column;
   * a request that changes the schema adds attributes and never changes or
   * removes one.
   */
  attributes?: Record<string, AttributeType>;
  /**
   * The indexes the attributes are to have where they are not the default.
   * An index switched on is built by the next maintenance; the manifest
   * records the ones that are built.
   */
  attribute_indexes?: Record<string, AttributeIndex>;
  /**
   * The named vector attributes, fixed at creation. An older build would
   * not know their columns and indexes.
   */
  vectors?: Record<string, VectorAttribute>;
  /**
   * Present: the namespace is written through the HTTP API and takes no
   * connector source. Fixed with the rest of the settings.
   */
  write_api?: WriteApiSettings | null;
}

/** The embedding identity bound into envelopes and publications. */
export interface EmbeddingIdentity {
  model: string;
  dimensions: number;
  /**
   * Digest of the model settings and text transformation (prefix,
   * normalization) that keys the embedding cache.
   */
  fingerprint: string;
  settings: EmbeddingSettings;
}

/** Vector-affecting settings recorded with a publication's model/dimensions. */
export interface EmbeddingSettings {
  api_style: EmbeddingApiStyle;
  prefix: string;
  roles: EmbeddingRoles;
  send_dimensions: boolean;
}

/** Fixed protocol identity recorded in persisted embedding settings. */
export type EmbeddingApiStyle = "openai-compatible";

/**
 * Persisted role transformations accept future minor-version additions.
 * Configuration uses strict types at its TOML boundary.
 */
export interface EmbeddingRoles {
  document: EmbeddingRole;
  query: EmbeddingRole;
}

export interface EmbeddingRole {
  prefix?: string | null;
  input_type?: string | null;
}

/** FTS analyzer settings, part of the publication identity. */
export interface FtsSettings {
  tokenizer: string;
  max_token_length: number;
  lowercase: boolean;
  stem: boolean;
  remove_stop_words: boolean;
  ascii_folding: boolean;
  /** Positional postings, which a phrase condition needs. */
  with_position: boolean;
  /**
   * The analyzer of `searchable_text`; present, the keys above are the
   * defaults and do not apply. Absent: the keys above.
   */
  analyzer?: Analyzer | null;
}

/**
 * The settings of a full-text index that decide which terms a text yields.
 * A text is lowercased always. Changing them on an attribute changes its index,
 * which the writer's maintenance rebuilds while the old one keeps serving; the
 * analyzer of `searchable_text` is fixed when the namespace is created.
 */
export interface Analyzer {
  tokenizer: Tokenizer;
  /**
   * Stems the terms and removes the stop words of the language. Absent: no
   * stemming, and no stop words but `stop_words`.
   */
  language?: Language | null;
  /**
   * The stop words to remove instead of the language's: compared with a term
   * after it is lowercased and stemmed.
   */
  stop_words?: string[] | null;
  ascii_folding: boolean;
  /** Tokens longer than this, in UTF-8 bytes, are dropped. */
  max_token_length: number;
  /** Only with the `ngram` tokenizer. */
  ngram_min_length: number;
  /** Only with the `ngram` tokenizer; not below `ngram_min_length`. */
  ngram_max_length: number;
  /** Only with the `ngram` tokenizer: only the n-grams that start a token. */
  prefix_only: boolean;
}

/** The way a text is cut into tokens. */
export type Tokenizer = "simple" | "whitespace" | "unicode" | "ngram";

/** A language with a stemmer and a stop-word list. */
export type Language = "arabic" | "danish" | "dutch" | "english" | "finnish" | "french" | "german" | "greek" | "hungarian" | "italian" | "norwegian" | "portuguese" | "romanian" | "russian" | "spanish" | "swedish" | "tamil" | "turkish";

/**
 * The distance of the dense index. Lance's IVF_RQ index supports all three on
 * 16-bit and 32-bit float vectors. Its `_distance` is lower for a nearer row:
 * `cosine` is one minus the cosine similarity, `squared_l2` the squared
 * Euclidean distance and `dot` one minus the dot product.
 */
export type DenseMetric = "cosine" | "squared_l2" | "dot";

/**
 * The element type of a stored vector. A 16-bit float halves the vector
 * column; a 32-bit one keeps the numbers the caller sent.
 */
export type DenseElement = "f16" | "f32";

/** The persisted copy of [`RetrievalConfig`]. */
export interface RetrievalSettings {
  rescore_depth?: RescoreSettings | null;
  dense_weight: number;
  dense_candidates: number | bigint;
  lexical_candidates: number | bigint;
  source_cap: number | bigint;
  max_documents: number | bigint;
}

export interface RescoreSettingsFixed {
  mode: "fixed";
  candidates: number | bigint;
}

export interface RescoreSettingsAdaptive {
  mode: "adaptive";
  min_candidates: number | bigint;
  max_candidates: number | bigint;
  min_gap: number;
}

/** The persisted copy of [`RescoreDepth`]. */
export type RescoreSettings = RescoreSettingsFixed | RescoreSettingsAdaptive;

/** The persisted copy of [`Composition`]. */
export interface CompositionSettings {
  fusion: number | bigint;
  dense: number | bigint;
  lexical: number | bigint;
}

/**
 * What of a reranker decides results: recorded in the namespace when it is
 * created. The endpoint, credential, spend caps and deadline stay process
 * configuration ([`RerankConfig`]). Persisted types accept the fields a newer
 * minor version adds, unlike their configuration counterparts.
 */
export interface RerankSettings {
  model: string;
  /** Authorized composed documents to rerank before the request limit. */
  candidates: number | bigint;
  /** Logistic population-z score blend; 1 uses raw reranker order, 0 fusion order. */
  weight: number;
  /** Per-candidate text bound in characters; absent sends the whole record. */
  text_chars?: number | bigint | null;
}

/**
 * The indexes an attribute has: the scalar one unless it says otherwise, and a
 * full-text or an n-gram index when it asks for one. An attribute without an
 * index is still filtered and sorted: by a scan of the table. The settings of
 * the analyzer belong to the full-text index and are only set with it; an
 * index without one takes the namespace's text analyzer settings.
 */
export interface AttributeIndex {
  index: boolean;
  full_text: boolean;
  ngram: boolean;
  analyzer?: Analyzer | null;
}

/**
 * A named vector attribute: a nullable column of its own, of vectors of one
 * dimension, with a vector index of its own. The metric, the element type and
 * the index settings are the namespace's. The built-in `vector` column is not
 * one: it is the default attribute of every namespace and is not declared.
 */
export interface VectorAttribute {
  /** Numbers in each vector of the column, a multiple of 8 from 8 to 8192. */
  dimensions: number;
  /**
   * A declared `string` attribute whose value the writer embeds, with the
   * namespace's embedding model, into a row that brings no vector of its
   * own for this attribute. The model's dimensions must be `dimensions`.
   * Without it, rows carry the vector or leave it null.
   */
  from?: string | null;
  /**
   * A row holds a list of vectors of `dimensions` numbers, not one, and a
   * search of the attribute takes several query vectors as one query: Lance
   * scores a row by the sum, over the query vectors, of the best cosine
   * similarity to any vector of the row. Only a namespace whose `dense_metric`
   * is `cosine` offers it, and a row's vectors are not embedded from text
   * (no `from`).
   */
  multi?: boolean;
}

/**
 * How a namespace written through the HTTP API stores and shows its rows. Fixed
 * for the life of the namespace.
 */
export interface WriteApiSettings {
  /**
   * The key of a row that holds its text: embedded, full-text indexed and
   * returned as the content of the record. It is not a typed attribute and
   * not one of `attributes`.
   */
  text_attribute: string;
  /**
   * `false`: a row is readable by the reader tokens it names, and a row
   * that names none by nobody. `true`: every authenticated subject of the
   * deployment reads every row.
   */
  open: boolean;
}

/** `rows` and `logical_bytes` of the data that no index covers yet. */
export interface Unindexed {
  rows: number | bigint;
  logical_bytes: number | bigint;
}

/** What a namespace switch answers. */
export interface PatchNamespaceResponse {
  namespace: string;
  read_only: boolean;
  /** `false` when the namespace was already so. */
  changed: boolean;
}

/** What a namespace deletion answers. */
export interface DeleteNamespaceResponse {
  deleted: boolean;
}

/** `search` result. */
export interface SearchOutput {
  hits: SearchHit[];
  /** Set when the response budget trimmed text or hits (narrow or lower `limit`). */
  truncated?: Truncation | null;
  metadata: ResponseMetadata;
}

/** One hit, in a deterministic total order. */
export interface SearchHit {
  /** Winning query's zero-based index, present only for multi-query search and sub-queries. */
  query_index?: number | null;
  source_id: string;
  root_id: string;
  record_id: string;
  /** Revision token of the record. */
  revision: string;
  source_uri: string;
  source_type: string;
  title: string | null;
  author: string | null;
  team: string | null;
  project: string | null;
  status: string | null;
  labels: string[];
  created_at: McpTime | null;
  updated_at: McpTime | null;
  /** Orders hits only. */
  score: number;
  excerpt: Excerpt | null;
  content: string | null;
  /** Typed attributes and vector asked for in `fields`. */
  attributes?: Record<string, unknown>;
  /** Raw lexical score, with `fields.scores`; absent when the lexical lane did not find the hit. */
  lexical_score?: number;
  /** Raw vector distance, with `fields.scores`; absent when the dense lane did not find the hit. */
  distance?: number;
}

/** A literal passage of the record; offsets are Unicode codepoints in the record text. */
export interface Excerpt {
  text: string;
  start: number;
  end: number;
  /** With `fields.highlights`: the `[start, end)` ranges of the tokens that match the query, as the index analyzer cuts and reduces them, in the codepoints `start` and `end` count. */
  highlights?: number[][];
}

export interface Truncation {
  text_dropped: number;
  hits_dropped: number;
}

/** Metadata on every response. */
export interface ResponseMetadata {
  schema_version: SchemaVersion;
  publication_id: string;
  content_version: number | bigint;
  committed_at: McpTime;
  /** Lanes skipped by degradation (e.g. dense after a failed query embedding). */
  degraded: Lane[];
  /**
   * The answer comes from an older version than the serving head the
   * reader last read.
   */
  stale_version: boolean;
  /** UTF-8 bytes omitted by clipping or the payload budget. */
  omitted_bytes: number | bigint;
  telemetry: Telemetry;
}

/** Contract schema version `<major>.<minor>`. Readers accept any minor of their major and reject other majors. */
export type SchemaVersion = string;

/** A retrieval lane. */
export type Lane = "dense" | "lexical" | "rerank";

/** Bounded per-request telemetry. */
export interface Telemetry {
  /** Rerank preparation and concurrent provider wait, separate from retrieval. */
  rerank_us?: number | bigint | null;
  cache: CacheState;
  queue_wait_ms: number | bigint;
  /** Effective centroid route; mixed means different routes across index deltas. */
  routing_strategy?: RoutingMethod | null;
  /** First optional routing failure across index deltas, never an I/O or auth error. */
  routing_fallback?: RoutingFallback | null;
  /** Fixed stage names, microseconds; no query or identity labels. */
  stage_us?: Record<string, number | bigint>;
  /** Requests and bytes on the same bounded stages. */
  stage_s3_requests?: Record<string, number | bigint>;
  stage_s3_bytes?: Record<string, number | bigint>;
  unindexed_rows_scanned: number | bigint;
  /** Authorized rows inspected for this call's document counts, never cached. */
  filter_rows_scanned?: number | bigint;
  /** Lexical candidates returned, including repeated fetches for cutoff ties. */
  lexical_candidates_fetched?: number | bigint;
  /** Unresolved lexical cutoff ties at the fixed fetch bound (zero or one). */
  lexical_tie_bound_hits?: number | bigint;
  /** Dense candidates read, including repeated fetches for cutoff ties. */
  dense_candidates_fetched?: number | bigint;
  /** Unresolved dense cutoff ties at the fixed fetch bound (zero or one). */
  dense_tie_bound_hits?: number | bigint;
  s3_requests: number | bigint;
  s3_bytes: number | bigint;
}

/** Cache state of the request. */
export type CacheState = "hot" | "warm" | "cold";

/** Effective routing used by this request's native vector indexes. */
export type RoutingMethod = "exact" | "rabitq" | "mixed";

/** Bounded reasons for using exact centroid routing instead of RaBitQ. */
export type RoutingFallback = "absent" | "build_failed" | "memory_budget" | "eligible_shortlist";

/** `fetch` result. */
export interface FetchOutput {
  documents: FetchedDocument[];
  next_cursor: string | null;
  /** Single-lookup window status; list requests report status on each document. */
  document_changed: boolean;
  /** Codepoint range served for a window, after the content limit. */
  window: ServedWindow | null;
  metadata: ResponseMetadata;
}

/** A document with its records in order. */
export interface FetchedDocument {
  /** Per-document window status for a list request; changed records have no text. */
  document_changed?: boolean | null;
  window?: ServedWindow | null;
  source_id: string;
  root_id: string;
  source_uri: string;
  source_type: string;
  title: string | null;
  author: string | null;
  team: string | null;
  project: string | null;
  status: string | null;
  labels: string[];
  created_at: McpTime | null;
  updated_at: McpTime | null;
  /** Untrusted connector metadata. */
  connector_map: Record<string, unknown> | null;
  records: FetchedRecord[];
}

/** The span a window request returned. */
export interface ServedWindow {
  record_id: string;
  start: number;
  end: number;
}

/** One record's text. */
export interface FetchedRecord {
  record_id: string;
  revision: string;
  text: string;
}

/** The `aggregate` result: groups in key order. */
export interface AggregateOutput {
  groups: AggregateGroup[];
  metadata: ResponseMetadata;
}

/**
 * One group: its values of the `group_by` fields and its aggregates, both in
 * request order. A `null` aggregate is one over no value.
 */
export interface AggregateGroup {
  key: unknown[];
  values: unknown[];
}

/**
 * The `recall` result: how closely the vector index of the rows the caller
 * may read agrees with an exact search. It names no document.
 */
export interface RecallOutput {
  /** Stored vectors measured: `count`, or fewer when the selection holds fewer rows. */
  samples: number;
  /** Rows the caller may read that the filter selects: what each exact search scans. */
  rows: number | bigint;
  /** `false` when at least one sample was answered by an exact scan, not by the vector index, as a selective `filter`, a namespace without a vector index or a caller who reads only unindexed rows brings about: that sample compares two exact answers and says nothing of an index. Also `false` without a sample. */
  approximate: boolean;
  /** The share of the ten nearest rows by an exact search that the index also found, averaged over `samples`; `null` without a sample. */
  recall_at_10: number | null;
  /** As `recall_at_10` for the 75 nearest rows. */
  recall_at_75: number | null;
}

/** What a write answers: the changes that applied. */
export interface WriteResponse {
  upserted: number | bigint;
  deleted: number | bigint;
  /** Present when the request has patches. */
  patched?: number | bigint | null;
  /** The patches and conditional changes that did not apply; present when the request has any. */
  skipped?: number | bigint | null;
}

/**
 * The body of an error answer of the reader and of the write route. The
 * namespace deletion answers a SCIM error instead.
 */
export interface ErrorBody {
  /** A code that [`docs/http-api.md`](../docs/http-api.md) lists. */
  error: string;
}
