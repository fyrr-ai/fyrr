# Changelog

All notable changes to fyrr are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The binaries follow
[Semantic Versioning](https://semver.org/); published contracts carry their
own `major.minor` versions (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## [Unreleased]

### Changed

- The reader's workers share the dataset its generation opened instead of
  each opening the pinned version again. Each worker still counts its own
  storage reads.
- An explicit `null` for a member of a JSON `query` request that is no tool
  argument (`fields`, `fusion`, `rerank`, `sub_queries`, `query_match`,
  `query_attributes`, `phrase`, `contains`, `boosts`, `group_documents`,
  `caps`), and for `consistency` on `query`, `fetch`, `aggregate` and `recall`,
  now means the default instead of `400 invalid_argument`, as the OpenAPI schema
  says. A required member that is `null` is still refused.
- Each stored contract has one major, whatever a document holds. v0.2.0
  writes and reads content manifest 5.0, policy artifact 4.0, prepared
  envelope and envelope rejection 6.0, and serving head, envelope checkpoint
  and history cut 4.1. The majors that the older entries below name for plain
  documents (manifest 4.1, policy 3.1, envelope 5.0) are no longer read: this
  build refuses any other major, an older one before it reads the document. A manifest always carries `stats` and
  `envelope_evidence`, so the namespace report always has `rows`,
  `logical_bytes` and `first_published_at`, never `null`. The retrieval digest
  always includes the retrieval settings, and a connector batch must have
  `sections[].vector` (a contract 1.3 batch without it is refused). A namespace
  written by an unreleased build before this change is still read when its
  manifest is 5.0 and its policy 4.0, as every connector namespace's are; a
  manifest 4.1, a policy 3.1 and a queued envelope 5.0 are refused, so recreate
  such a namespace.
- `rescore_depth` has no `legacy` mode. Leave it unset for the default: four
  times the dense lane depth, which for one-bit codes grows with the rows the
  probes inspect. A configuration that sets `{ mode = "legacy" }` is refused.
  See [tuning](docs/tuning.md).
- **Breaking (v0.2.0):** every local storage root written by v0.1.x or an
  earlier build (it holds `.fyrr-storage.lock` or `.fyrr-storage.stage`) is
  refused with `storage_root_old`. Recreate its namespaces in a new root with
  v0.2.0; remove those files only once no older writer runs. A namespace
  that v0.1.x or an earlier build wrote is refused with `namespace_layout_old`
  in any root, S3 included: one of its stored contracts is of an older major.
  Recreate it under a
  new name (or, with every writer stopped and a copy kept, after deleting its
  `namespaces/<name>/` prefix), sync its sources again and write the API
  documents again. A
  local put no longer opens a root lock file. See
  [operations](docs/operations.md#upgrades).
- The excerpt of a hit in a namespace with an `[namespace.analyzer]`, or with
  `fts_stem`, `fts_remove_stop_words` or `fts_ascii_folding`, is chosen with the
  tokens of its full-text index, as the lexical lane and `highlights` already
  are. A passage whose only match is a stemmed or folded word can now be the
  one returned. Any other namespace keeps its excerpts, chosen by the literal
  words of the query. See [tuning](docs/tuning.md#the-analyzer-of-the-text).
- A namespace created for connectors stores `title`, `team`, `status`,
  `labels` (an array of `string`) and `created_at` (`datetime`) of its
  documents as typed columns, declared with the namespace like the attributes
  of the file, so a filter or `order_by` can use them. `search`, `fetch` and
  `list_filters` return the same fields as before. A connector namespace
  created without the columns is refused: recreate it under a new name with
  v0.2.0 and sync its sources again. A manifest of the new layout is
  content-manifest 5.0 and its policy artifact 4.0, the versions of attribute
  index options and of the read-only switch, so an older build refuses it
  instead of reading nulls. The
  `key=value` attributes of a connector document stay in the `attributes`
  column. The `title` has a full-text index, so a query can rank by it with
  `query_attributes`. A namespace or a configuration made by an unreleased build between
  typed attributes (#225) and this change can be refused: one that declares the
  five connector names with exactly their types, one at the limit of declared
  attributes (the five count), or one that sets one of the five to another type.
  Namespaces and roots of v0.1.x are refused, as above. See
  [tuning](docs/tuning.md#namespaceattributes).

### Added

- `[namespace.analyzer]` gives the full-text index of `searchable_text` the
  settings of an attribute's analyzer: the tokenizer (`simple`, `whitespace`,
  `unicode`, `ngram`), one of 18 languages for stemming and stop words, stop
  words of its own, ASCII folding and the maximum token length. The lexical
  lane, `phrase` and `highlights` follow it. It replaces `fts_stem`,
  `fts_remove_stop_words`, `fts_ascii_folding` and `fts_max_token_length`,
  which stay as they were: a namespace without an analyzer keeps its index and
  its policy digest. The analyzer is fixed when the namespace is created, like
  those keys; a namespace with one writes the indexed policy and manifest
  versions. See [tuning](docs/tuning.md#the-analyzer-of-the-text).

- A namespace can store token positions in the full-text index of
  `searchable_text` (`fts_positions = true` in `[namespace]`), and the JSON
  `query` route takes `phrase`: the tokens of a text next to one another in
  order, required of a hit like `filter`, for a text and a vector ranking. Its
  `slop` accepts only 0 until a newer Lance keeps every match for a larger one. A phrase on a namespace without positions is
  `invalid_argument`. The MCP `search` tool does not take the member. A
  namespace with positions writes the indexed policy and manifest versions.

- An array of strings can have a full-text index, with one document per
  element, and the JSON `query` route takes `query_attributes`: the full-text
  attributes a text query scores, each with a weight, instead of only
  `searchable_text`. A document scores the sum of the weighted scores of the
  attributes it matches; `contains` takes an array too. An array stands alone
  in `query_attributes`, and an attribute indexed with `ngram` is refused.
  The MCP `search` tool does not take the member.

- A named vector attribute can be `multi`: a row holds a list of vectors of the
  attribute's `dimensions` (`"multi": true` in `namespace.vectors`), for models
  that represent a document by one vector per token. `search` takes `vectors`
  (1 to 32 vectors, instead of `vector`) with such a `vector_attribute`, as does a
  sub-query of the JSON `query` route, and Lance scores the query as one
  multi-vector query: a row's score is the sum, over the query vectors, of the
  best cosine similarity to any vector of the row. Only a namespace whose
  `dense_metric` is `cosine` offers it, and a multi-vector attribute has no
  `from`. The index is `IVF_RQ` as for the others, and the scores come from its
  quantized codes, so they are not exact. A patch cannot change it. See
  [tuning](docs/tuning.md#namespacevectors).

- A namespace written through the document write API can declare named vector
  attributes when it is created (`namespace.vectors`, or `settings.vectors` in
  the first write): each has its own `dimensions` and its own vector index, and
  may name a `string` attribute (`from`) that the writer embeds the vector from
  when a row carries none. Rows may leave an attribute null, and a search of it
  does not return them. `search` and the JSON `query` route take
  `vector_attribute` (and a sub-query takes one) to name the attribute the dense
  search reads; without it the built-in `vector` is searched, as before. A
  search of a named attribute uses the same permissions and filters as one of
  `vector`, but not its partition maps and centroid routing. At most 16 can be
  declared (`limits::schema::MAX_VECTOR_ATTRIBUTES`). A patch keeps the stored
  vectors and cannot change the attribute one was embedded from. The recall of
  a namespace stays that of the built-in vector. A namespace with vector
  attributes is content-manifest 5.0 and policy-artifact 4.0, so an older build
  refuses it; one without keeps its versions, files and retrieval-policy
  digest. See [tuning](docs/tuning.md#namespacevectors).

- The JSON `query` route takes `group_documents` and `caps`.
  `group_documents: false` makes every matching record a hit, not only the best
  record of each document. `caps` limits the hits per value of 1 to 4
  attributes (`author`, `project`, `source_type` or a declared `string`
  attribute), for example at most 3 per author; the request's `limit` still
  applies. Both work for text, vector and fused rankings, and read only rows
  the caller may read. They are refused with a listing, `queries` and
  `sub_queries`. The MCP `search` tool does not take them. See
  [Grouping and caps](docs/http-api.md#grouping-and-caps).

- The JSON `query` route takes `boosts`: 1 to 8 terms added to the relevance
  score of a text, vector or fused ranking, each a weight times an
  `attribute` function `x / (x + midpoint)` of a numeric attribute or a
  `recency` function `midpoint / (midpoint + d)` of the distance of a
  `datetime` attribute from an `origin` the request gives (the reader has no
  clock). A document among the lanes' candidates can reach the first rank
  through a boost, the composition's slot rules still apply, and only rows the
  caller may read are boosted. They do not go with `sub_queries` or `fusion`. The MCP
  `search` tool does not take them. See [Boosts](docs/http-api.md#boosts).

- `namespace.dense_metric` (`cosine`, the default, `squared_l2` or `dot`) and
  `namespace.dense_element` (`f16`, the default, or `f32`) set the distance of
  the vector index and the type a vector is stored in. Both are fixed when the
  namespace is created, recorded in its policy and manifest and reported by
  `GET /v1/namespaces/{namespace}` as `dense_metric` and `dense_element`. The
  index build, the partition routing, the rescoring and the recall sample use
  the namespace's metric. Under `dot` Lance ranks the partitions itself, so
  `reader.routing = "rabitq"` is not used for it. A namespace with another
  metric or element type is content-manifest 5.0 and policy-artifact 4.0, so
  an older build refuses it; a namespace with the defaults keeps its versions,
  files and retrieval-policy digest. See
  [tuning](docs/tuning.md#namespace-vector-index).
- The JSON `query` route takes `query_match`, which makes a text ranking
  require all of its terms, and `contains`, which requires a hit to hold all
  or any tokens of a string attribute with a full-text index. Both work next to
  `filter`, with a text and with a vector ranking, and under the caller's
  permissions. An attribute indexed with the `ngram` tokenizer is refused:
  prefix matching waits for a newer Lance. The MCP `search` tool does not take
  them.
- The JSON `query` route takes `fields.highlights`: each excerpt then lists
  the `[start, end)` ranges of its tokens that match the query, in the
  codepoints of the record like its `start` and `end`. A token matches as the
  namespace's full-text analyzer (stemming, folding, case) reduces it, so a
  stemmed match is marked. At most 100 ranges per excerpt. The MCP tools do
  not take it, and a request without it answers as before. See
  [Returned fields](docs/http-api.md#returned-fields).

- The `query` route of the HTTP API takes `sub_queries`: 1 to 16 searches in
  one request, each with its own `filter`, ranking (`query` text, `vector`, or
  both) and `limit`, read from one published version. Their hits follow one
  another in request order, or `fusion` ranks the documents of all of them.
  Every sub-query passes the same permissions as a single query, and the
  memory, response size, `max_documents` and deadline bound the whole request.
  It is no tool argument, so the MCP contract is unchanged. See
  [Sub-queries](docs/http-api.md#sub-queries).
- `writer namespace copy --to-config FILE` copies the configured namespace, as
  the one version it serves, into the `[storage]` of another configuration file
  (another bucket or region) under the same name. The copy is independent and
  writable and finds the same documents with the same access. A different
  `identity.hashing_key` fails with `hashing_key_mismatch`; a copy in the same
  storage, which would need another name and so other hashes, is not supported
  (`copy_same_storage`); an interrupted copy leaves no namespace and the same
  command completes it. See [Backup and copy](docs/operations.md#backup-and-copy).
- `filter` tests the text of a `string` with `starts_with`, `contains`,
  `matches` (a regular expression) and the negations `not_starts_with`,
  `not_contains` and `not_matches`. A prefix is answered from the B-tree of a
  `string` attribute and a substring or an expression from its n-gram index
  (`ngram = true`); the negations and the patterns without a literal of three
  alphanumeric characters are scans bounded by the request deadline. A value is
  never empty, and an expression is bounded to 512 bytes and a compiled size of
  256 KiB. No case-insensitive test is offered. The write conditions and the
  filters of `delete_where` and `patch_where` take the same tests. See
  [Text tests](docs/mcp-tools.md#text-tests).
- The `query` route of the HTTP API takes `fusion` and `rerank`. `fusion`
  (`{"method":"rrf","rank_constant":60,"weights":[…]}`, with `queries`) ranks the
  documents of all sub-queries by reciprocal rank fusion instead of merging
  them round-robin. `rerank` (`{"enabled":false}`, or `attribute` and `query`)
  switches the namespace's reranker off for a request or names what it scores;
  the reranker's candidate count, deadline and spend limit still apply.
  Neither is a tool argument, so the MCP contract is unchanged. A request
  without them is answered as before. See
  [Fusion and reranking](docs/http-api.md#fusion-and-reranking).
- `POST /v1/namespaces/{namespace}/aggregate` on the reader computes `count`,
  `sum`, `avg`, `min` and `max` over the documents a caller may read and a
  `filter` selects, optionally grouped by up to four scalar fields, in key
  order. It has no MCP tool. `list_filters` now runs the same scan. See
  [Aggregate](docs/http-api.md#aggregate).
- `POST /v1/namespaces/{namespace}/recall` on the reader (an API key with the
  `admin` and `read` roles) measures the vector index against an exact search on up to 100
  stored vectors, optionally under a filter, and returns `recall_at_10`,
  `recall_at_75` and the row counts, never a document. It runs within the
  request deadline and holds one query slot. It has no MCP tool. The background
  recall sampler is now described too. See
  [Recall](docs/http-api.md#recall) and
  [Vector recall](docs/operations.md#vector-recall).
- `fyrr writer namespace read-only true|false` and `PATCH /v1/namespaces/{namespace}`
  with `{"read_only": …}` (an API key with the `admin` role) freeze a namespace or
  thaw it. While it is frozen API writes and connector syncs are refused with
  `namespace_read_only`, judged against the published head as each envelope
  becomes durable; envelopes prepared before the switch are still committed, and
  maintenance and the switch itself still work. The namespace report shows
  `read_only`. A policy that sets `read_only` is `policy-artifact` 4.0, the same
  unreleased 4.0 as a policy with index options: both enter it together, so a
  build with index options but without the switch must not be released alone,
  and this change ships in the release of the index options. A build without
  the switch refuses the policy, readers included. See
  [Freezing a namespace](docs/http-api.md#freezing-a-namespace).
- `fyrr writer namespace list [--prefix P]` and `GET /v1/namespaces?prefix=P` on
  the reader list the namespaces of the storage root, in name order, that start
  with `P` and with the `namespace_prefix` of the caller's key. A namespace is
  listed while any object exists under it. One call is one delimited listing of
  `namespaces/`, and more than 10,000 matching names fail with
  `too_many_namespaces`. The route needs an API key with the `admin` or `list`
  role. See [Listing namespaces](docs/http-api.md#listing-namespaces).
- `GET /v1/namespaces/{namespace}` on the reader reports the namespace's
  schema, settings, approximate row count and logical bytes, the time of the
  namespace's first publication, last write time, and the rows and bytes no
  index covers yet. It is answered
  from the manifest the reader holds, and needs an API key with the `admin` or
  `list` role, because its numbers are not filtered by row permissions. The
  content manifest gains `stats` (publication contract 4.1). See
  [Namespace report](docs/http-api.md#namespace-report).
- `fyrr writer serve` serves `POST /v1/namespaces/{namespace}/documents` when
  `[namespace.write_api]` is set: upserts (rows with an `id`, a text and typed
  attributes) and deletes by id, one request published whole or refused whole.
  The answer is sent when the served head holds the request. An API key with
  the `write` role authenticates it. A namespace takes its documents from
  connectors or from the API, as it was created. The path names any namespace
  the key may write, and the first request for a name creates it. See
  [HTTP write API](docs/http-api.md#write-documents).
- The write API takes `patches`: rows that write some attributes, the readers
  or the text, with its vector, of an existing document. What a patch does not
  name stays as stored, the vector included, and nothing is embedded. A patch of
  a document that does not exist is skipped and counted in the answer
  (`patched`, `skipped`). A `vector` without the text, or a text without its
  vector, is refused. See
  [HTTP write API](docs/http-api.md#write-documents).
- A schema says per attribute whether it has an index (`attribute_indexes`, the
  default is the index of its type) and whether a `string` also has a
  full-text or an n-gram index. A filter or sort on an attribute without an
  index is answered by a scan of the table, bounded by the request deadline. A
  write request that carries only `settings` adds attributes to a namespace that
  exists and sets the indexes of its attributes, in one publication; a new
  attribute reads null for the documents written before it, an index switched
  off is dropped with it, and one switched on is built by the next
  maintenance. Another type for an attribute and the removal of one are refused
  (`attribute_type_change_refused`, `attribute_removal_refused`); a schema
  change carries nothing else and waits for the first publication of its
  namespace (`schema_change_with_rows`, `namespace_not_published`). A manifest that
  records index options has contract version 5.0 and a policy artifact whose
  settings hold them 4.0, which an older build refuses; others keep 4.1 and 3.1,
  and this build reads both. See
  [Changing the schema](docs/http-api.md#changing-the-schema) and
  [`[namespace.attribute_indexes]`](docs/tuning.md#namespaceattribute_indexes).
- The full-text index of an attribute has its own `analyzer`: the tokenizer
  (`simple`, `whitespace`, `unicode` for Unicode word segmentation, which finds
  the words of Chinese and Japanese text by a dictionary, or `ngram` with a
  minimum and a maximum length and a prefix-only switch), a `language` for
  stemming and stop words (18 are offered), a custom `stop_words` list,
  `ascii_folding` and `max_token_length`. The analyzer is an option of the
  index, so it travels in the contract versions that carry index options
  (content-manifest 5.0, policy-artifact 4.0). A change of it is a schema
  change: the next maintenance builds the index again and publishes it, and the
  index it replaces serves until then. An index without an analyzer takes the
  namespace's `fts_*` settings, as before. See [Analyzer](docs/tuning.md#analyzer-of-a-full-text-index).
- The write API takes `delete_where` and `patch_where`: a filter whose matching
  documents are deleted, or patched with the attributes or readers of `set`.
  The committer evaluates it once against the state the request is applied to;
  more than 10,000 matches refuse the request and change nothing. Deletes by
  filter apply before the rest of the request, patches by filter next. A filter
  write ignores row permissions and needs an API key with the `admin` role
  besides `write`. An envelope with a filter write has contract version 6.0,
  which enters together with conditions: every committer that reads 6.0 knows
  both. See
  [HTTP write API](docs/http-api.md#write-documents).
- The write API takes `conditions`: a filter by the `id` of an upsert, a patch
  or a delete that the stored document must meet when the committer applies
  the change, which may compare it with the change's own row. A change whose
  condition is not met writes nothing and is counted in `skipped`; a document
  that is not there meets the condition of an upsert only. Setting conditions
  needs an API key with the `admin` role besides `write`. An envelope with a
  condition has contract version 6.0, which a committer that does not know
  conditions refuses; other envelopes stay 5.0 and this build reads both. See
  [Conditions](docs/http-api.md#conditions).
- A row of the write API may carry its own `vector`, and so may a section of a
  connector document (the optional `vector` column of the document-change
  contract, version 1.4). Such a row is stored with that vector and its text is
  not embedded. A vector of another dimension, with a non-finite element or
  all zeros is refused as `vector_invalid`. A creating request declares the
  namespace's vector `model` and `dimensions` in `settings.embedding`, and a
  deployment whose documents all carry vectors needs no `[embedding]` table.
  See [HTTP write API](docs/http-api.md#write-documents).
- The JSON `query` route takes `fields`: the typed attributes to return (an
  `include` list, or all of them but an `exclude` list), the vector of a row as
  an array of numbers, and with `scores` the raw lexical score and vector
  distance next to the final score. Only those columns are read, and only of
  rows the caller may read. The MCP tools keep their output. See
  [Returned fields](docs/http-api.md#returned-fields).
- `search` takes a `vector`, a query vector used instead of an embedding of the
  query text; alone, it searches the dense lane only and makes no embedding
  call. `exact: true` scans every row that passes the filters and the caller's
  permissions instead of probing the index, up to 100000 rows and the request
  deadline. The dense candidate depth is at least the request `limit`. See
  [`search`](docs/mcp-tools.md#search).
- `search` takes one `filter` argument, a tree of `and`, `or`, `not` and
  comparisons (`eq`, `ne`, `in`, `not_in`, `lt`, `lte`, `gt`, `gte`) of
  at most 64 nodes and 16 levels over the built-in fields and the namespace's typed
  attributes, over MCP and the JSON HTTP API. Values are checked against the
  attribute types; the other filter arguments compile to the same tree. See
  [Filters](docs/mcp-tools.md#filters).
- `search` without `query` takes `order_by`: up to 4 sort keys, each a built-in
  field or a typed attribute, ascending or descending. Missing values sort
  first ascending and last descending, and ties are broken by the document key.
  See [Ordering a listing](docs/mcp-tools.md#ordering-a-listing).
- A listing (`search` without `query` or `queries`) takes `offset` and a
  `limit` of up to 1000, with `offset + limit` at most 1000. Ordering by the
  document key and filtering for the key after the last one seen scans a whole
  namespace; see [Paging and scanning](docs/mcp-tools.md#paging-and-scanning)
  and the [export recipe](docs/http-api.md#export-a-namespace).
- The reader serves `search` and `fetch` as plain JSON at
  `POST /v1/namespaces/{namespace}/query` and `.../fetch`, next to MCP, with
  the same authentication, scheduler, deadline and rate limits. Errors map to
  HTTP statuses, and 429 and 503 answers carry `Retry-After`. See
  [HTTP JSON API](docs/http-api.md).
- The JSON routes serve the namespace the request path names, not only
  `namespace.name`: the reader opens it on its first request, after the
  caller's API key role and `namespace_prefix` have admitted it, and keeps up
  to 8 such namespaces open. OIDC users read the configured namespace only. A
  namespace that does not exist costs one read of its head and is answered as
  one that does not bind the caller (previously `404`). `/mcp` still serves the
  configured namespace. See
  [HTTP JSON API](docs/http-api.md).
- The Jira connector indexes the custom fields listed in the new
  `scope.fields` (rich text, string or number values) as `field` sections
  labelled with the field ID, for example acceptance criteria. See
  [Jira](docs/connectors/jira.md#documents).
- The Jira connector records each issue's key as the document attribute
  `jira.key` and starts the summary section with it (`PROJ-123 Summary`).
  `search` with `attributes: [{"key": "jira.key", "values": ["PROJ-123"]}]`
  and `order: "updated_at"` returns that issue without a `query`. Every Jira
  issue gets a new revision and its summary is embedded again: at its next
  update, or at the connector's next full rescan (the first content sync at
  least 12 hours after the previous one) and the commit that follows. To do
  it at once, run `fyrr writer sync <source> --kind content --full`, then
  `fyrr writer commit`. The key counts toward `document_max_bytes`. See
  [Jira](docs/connectors/jira.md#documents).
- HTTP MCP clients can authenticate with operator-issued API keys without an
  identity provider. Give each role its own `[[identity.api_keys]]` entry and
  `readers` grants. To revoke a key, remove it from every writer
  configuration, republish identity, and refresh the readers. An HTTP
  reader without an authentication method is now refused when `reader serve`
  starts, and `config check` reports it as a warning, instead of being a
  configuration error for every command. See
  [HTTP login and users](docs/http-users.md#api-keys).
- API keys carry `roles` (`read`, `write`, `list`, `admin`; default `read`)
  and an optional `namespace_prefix`. `fyrr writer keys` publishes the keys of
  a deployment as one artifact under the storage root and is required: readers
  refuse every API key until they can read it, refuse a key removed from it
  within two version checks, and answer `403 forbidden` to a key without the
  role a route needs. See
  [identity](docs/identity.md#identityapi_keys).
- The reader sends `instructions` in the MCP `initialize` result: what
  fyrr searches and a request to search it before looking through files.
  Clients that pass instructions on tell their agent when to search.
  `reader.instructions` (absent by default) replaces that text; an empty
  string sends none. See
  [reader and writer settings](docs/reader-writer.md).
- An optional Claude Code plugin (`plugins/fyrr`, marketplace `fyrr`)
  adds a reminder of its own at the start of every session. See
  [connect an agent](docs/connect.md).
- `writer.threads` (absent by default) bounds the CPU threads of each writer
  process: its async runtime, Lance's compute runtime, the parallel-iterator
  pool used by vector index builds and the full-text build workers. See
  [reader and writer settings](docs/reader-writer.md).
- `namespace.dense_rq_bits` (1 to 9, default 1) sets the RaBitQ bits per
  dimension of the vector index. The value is recorded in the content
  manifest (publication contract 4.0) and fixed once a namespace is
  published; see [tuning](docs/tuning.md).
- `namespace.dense_partition_rows` (32 to 4096, default 550) sets the target
  rows per partition of the vector index, recorded and fixed like
  `dense_rq_bits`. `writer maintain` re-partitions the index as the namespace grows,
  without leaving it unindexed or blocking readers. Each vector index entry
  of the content manifest records its `partitions`.
- `writer.fts_partition_memory_mb` and `reader.fts_search_chunk` (both absent
  by default) build a full-text index of several partitions and let one query
  score them concurrently. Scores and rankings do not change; see
  [tuning](docs/tuning.md#full-text-partitions).

- `[namespace.attributes]` declares typed attributes for a new namespace:
  `string`, `int`, `float`, `bool` or `datetime`. Each is a nullable table
  column of its own with a B-tree (bitmap for `bool`) index, and the schema is
  recorded in the namespace's policy artifact and in every content manifest
  (contracts 3.0 and 4.0, unreleased). The columns currently contain only nulls.
  See [tuning](docs/tuning.md#namespaceattributes).
- Typed attributes take three more scalar types, `uint` (64-bit unsigned),
  `uuid` (stored in 16 bytes) and `bytes` (base64 in requests; stored, never
  filtered, sorted or indexed), and arrays of every scalar type but `bytes`,
  declared as `{ array = "string" }`. An array has a label-list index. `filter`
  tests an array with `contains`, `not_contains`, `contains_any`,
  `contains_all` and `contains_none`; `order_by` refuses an array and `bytes`. A query's `fields` returns
  them as a number, a UUID string, base64 and a JSON array. Existing schemas
  are unchanged. See
  [tuning](docs/tuning.md#namespaceattributes) and
  [Filters](docs/mcp-tools.md#filters).

### Changed

- A namespace opened by its request path that has served no request for one
  `reader.version_check_interval` makes no storage request until the next one,
  which reads the head first and is answered from that read. When the memory
  budget cannot fund a namespace in full, the least recently used namespace
  that serves no request is closed for it. See
  [HTTP JSON API](docs/http-api.md#routes) and
  [Revocation](docs/security.md#revocation).

- **Breaking: a namespace keeps its own index and retrieval settings.**
  `writer namespace create` records the embedding profile (`model`,
  `dimensions`, prefixes, `roles`, `send_dimensions`), the analyzer
  (`namespace.fts_*`), the vector index (`dense_rq_bits`,
  `dense_partition_rows`), `composition`, `retrieval` and the reranker's
  `model`, `candidates`, `weight` and `text_chars` from the configuration file in the namespace's policy artifact (policy
  artifact contract 3.0; republish older namespaces). The file only supplies
  defaults for a new namespace: a reader or writer whose file differs serves
  the namespace with the settings it was created with, and creating a
  namespace again with other settings fails with `namespace_settings_fixed`.
  Changing a setting means a new namespace; the in-place rebuild or
  republication that followed an edit of these keys is gone. See
  [upgrades](docs/operations.md#upgrades).
- `writer serve` no longer takes the cursor lease, or lists the envelopes,
  to find that a kind of a source is not due: it reads the head and the
  kind's cursor, which are read only. Each sync worker reports when its
  source is next due, and the service starts the next worker then, instead
  of once a minute. A change of the served head, which carries rejections
  and rewinds, makes every source due at the next committer timer tick, as
  before. A permissions or members kind that ever had a rejection still
  takes the lease to decide.
- The `search` tool description tells agents to search a ticket's title and
  body as two `queries` for task context.
- The software factory guide and the Claude Code plugin tell an agent to
  start a task with one `search` whose two `queries` are the ticket's title
  and body. The agent takes its current ticket from the tracker; fyrr
  supplies the earlier tickets, pull requests and threads around it.
- To use connectors, install this release's `fyrr-connectors`
  wheel with your provider extras in a Python 3.11+ virtual environment (tested
  on 3.11, 3.12, 3.13 and 3.14). Set
  `VIRTUAL_ENV` to its absolute path and run `fyrr writer providers` to verify
  the installation. The full image includes the SDK and bundled providers.
  Commands that run no connector need no Python. Each sync kind runs in a
  fresh Python process; the worker memory limit applies to that process
  separately. What breaks for existing users:
  - The embedded interpreter is gone: the binary links no Python
    (`PYO3_PYTHON`, `libpython`).
  - SDK 0.1.2 or newer is needed; 0.1.0 and 0.1.1 do not work.
  - A provider that aborts fails its source with `the provider failed`.
  - `PATH`, `VIRTUAL_ENV` and names starting with `PYTHON` or `LD_` are
    reserved names for stored credentials a source references;
    `fyrr config check` refuses them.
  See [how a provider runs](docs/connectors/authoring.md#how-a-provider-runs).
- The Jira connector keeps link targets (`text (url)`, smart cards as their
  URL). Under `permissions = "fixed"` it adds a `links` section with the
  parent and issue links, keys only (`blocks: PROJ-140`). Affected issues are
  republished at their next update, or at the next full rescan and commit;
  `sync --kind content --full` followed by `writer commit` does it at once. See
  [Jira](docs/connectors/jira.md#documents).
- GitHub and Jira comments start with a line naming their author
  (`ada (member):` with GitHub's `author_association`; `Ada Lovelace:`).
  Conversations and issues with comments that still fit `document_max_bytes`
  get a new revision at their next update or rotation (12-hour target) and
  the commit after it;
  `writer sync <source> --kind content --full`, then `writer commit`, does it at once.
- **Revocation latency.** A request no longer reads the serving head from
  storage for its authorization checks. It checks its grants against the
  head the reader's latest version check read
  (`reader.version_check_interval`, 30 s), and reads the head itself only
  when that read is older than two intervals. A warm query then makes no
  storage request instead of two. A published revocation, membership
  change, retired source or deleted namespace now reaches a running reader
  with its next version check, at the latest two intervals (60 s by
  default) after it was published, rather than with its next request.
  `SIGHUP` and `POST /admin/refresh` start a check at once.
- **Breaking: the reader has no clock.** Results change only with a new
  publication: the same query over the same published state returns the same
  result, and nothing disappears on its own. The reader serves the last
  published content, permissions and identity with no validity deadline, so
  while a connector, the directory provider or the writer is down, a revoked
  grant keeps serving until a publication carries the revocation; retire a
  source whose connector stays broken. The writer publishes only on a real
  change: an idle namespace gets no freshness envelope, no directory
  attestation and no publication from the commit timer (the lease is still
  taken and released). A member read whose snapshot equals the published one
  (as a set) moves only its cursor and writes no envelope; a permission batch
  with no document changes or container grants does the same, but a permission
  batch that repeats rights is still written. Removed: the configuration key `identity.validity_deadline` (a
  configuration that sets it is refused; configuration schema 6.0),
  `freshness` in the serving head and `writer status`, the SCIM health proof
  and the directory attestation of `writer serve`, the
  `directory_health_invalid` error, the reader's directory-age,
  identity-age and per-source hiding, `read_at` in the permission and member
  envelope payloads, the 3.x defaults for manifests without `dense_rq_bits`
  and `dense_partition_rows`, and `metadata.hidden_sources` in every MCP
  response. Contract versions: MCP 4.0, serving head and content manifest
  4.0, prepared envelope 5.0, identity artifact 2.0 (no `built_at`); republish
  older namespaces. `writer status`
  now reports `source_reads`: when each `permissions = "sync"` source's
  permissions and members were last read, from the source's cursor state, with
  nulls for a read that never completed. `writer sync --directory` no longer takes `--due`: the
  `writer serve` timer or your scheduler decides when to pull. A pull holds the
  namespace lease from before its read until its publication, so pulls cannot
  publish out of order.
- **Breaking: `reader.version_check` and `reader.per_request_wait` are
  removed** (configuration schema 6.0); a configuration that sets either is
  refused. `reader.version_check_interval` is the one setting. The reader
  always checks on that interval, and a reader that did not run for a
  second or more, such as an environment frozen between requests, runs a
  due check before it answers: requests wait up to 2 s for it. This
  replaces the `per-request` mode, which read the serving head for every
  request; such a reader now reads it at most once per interval and an
  answer can lag a publication by up to one interval. The `off` mode has no
  replacement. `metadata.stale_version` is now set in every reader whenever
  the answer comes from an older version than the head the reader last
  read.
- The reader's mirror reads an index file (`_indices/`: a vector partition, a
  full-text posting list, a scalar index page) of more than 2 MiB in verified
  4 KiB pages as it does a data file, instead of downloading it whole on its
  first read. A cold full-text or vector query fetches only the pages it
  touches; an index file of up to 2 MiB is still downloaded whole.
- The reader's mirror downloads a data file of up to 2 MiB whole on its
  first read instead of in pages. Opening a reader reads the footer, the
  column metadata and the first page of every data file, two or three
  dependent requests each, and warming then downloaded the file anyway.
  Opening a 100,000-row namespace on an empty mirror made 412 storage
  requests instead of 1,044 and took 0.93 s instead of 1.32 s with 25 ms
  per request, and its first search made 7 requests instead of 726. Open
  transfers the small files at once (313 MB instead of 44 MB there), which
  warming would have downloaded moments later; over a slow link the first
  answer can therefore take longer. A small file that does not fit the
  mirror is still read in pages.
- An idle `writer serve` no longer reads the whole publication history at
  every committer tick. The committer remembers the evidence of the
  publications it has read and reads only newer ones, and it lists and
  reads the retained envelopes only when the history records a rejection
  or the served manifest has a cursor rewind. With five publications, eight idle ticks made 152 storage
  requests instead of 472, and the manifest reads per tick no longer grow
  with the history.
- Building a subject's admission no longer reads the `source_id` column of
  every data file when no source is hidden (expired or retired); the
  predicate's scalar indexes give the rows. Opening a 100,000-row namespace
  on an empty mirror made 19 storage requests instead of 1,044 and took
  0.42 s instead of 1.32 s with 25 ms per request; its first search took
  446 ms instead of 513 ms. A namespace with fixed grants still reads
  `source_id`, which has no index.
- The reader no longer asks storage for a version manifest its publication
  does not list. Lance looks for a version under its other manifest name
  each time it opens the dataset, once per generation; the mirror now answers
  that miss from the publication's file list. Opening a 100,000-row namespace
  on an empty mirror made 35 fewer storage requests.
- The reader's mirror reads data files in 4 KiB pages: a read of part of a
  data file fetches only the pages it lacks, instead of the whole file, and
  checks each kept page against its digest once per process before serving
  it. The size of a pinned data file is taken from the content manifest
  rather than by downloading the file. Opening a 100,000-row namespace on an empty mirror transferred 44 MB
  instead of 313 MB. Index files are still copied whole, and warming still
  fills every file completely.
- The reader shares one immutable admission per subject between its
  authorization cache and its workers instead of copying it, and the cache
  charges an entry by an estimate of its size rather than 32 bytes per
  readable row. A subject with millions of readable rows is now cached.
- The reader fills its mirror with up to 16 files at once instead of one
  after another and reads the manifest, policy and identity of a publication
  together. When a fill fails for a storage reason and another runs out of
  mirror space in the same batch, the storage failure is the one reported.
- A caller who may read only part of a namespace searches more vector-index
  partitions: nearest first, until they hold as many of the caller's rows as
  `nprobes` partitions of average size hold. Recall of such callers no longer
  drops with their share of the rows; see
  [tuning](docs/tuning.md#callers-who-read-few-rows).
- A search with a request filter searches the vector index when that reads
  fewer bytes than the exact scan of the selected rows, and then probes
  partitions until they are expected to hold enough selected rows; see
  [tuning](docs/tuning.md#dense-search-under-a-request-filter). A filtered
  selection on the index returns approximate dense candidates, as an
  unfiltered search does.
- A search with a request filter (sources, authors, time range, labels)
  selects its rows from the scalar indexes of both the filter and the
  subject's grants. It no longer reads the filter columns of every row the
  subject may read. Results and authorization are unchanged.
- A query-less `search` (listing by `updated_at`) and `list_filters` select
  their rows through the scalar indexes too, instead of passing the
  subject's rows to Lance as a row mask. Results and authorization are
  unchanged.
- The GitHub connector indexes an issue or PR title as the first line of its
  first section, so title-only words are searchable. Conversations with a body
  get a new revision and are re-embedded as they refresh; run
  `fyrr writer sync github --kind content --full` to do it at once. The title
  counts toward `document_max_bytes`. See
  [GitHub documents](docs/connectors/github.md#documents).
- The Notion and Google Drive connectors index titles. A standalone Notion
  page's title and a Google Doc's file name are the first line of the first
  section; a Sheet's or Slides deck's file name is a section of its own
  before the first one. A word that occurs only in a title is found by
  full-text search. The title counts toward `document_max_bytes`: a page or
  file that was just under the limit is quarantined when it is read again
  and keeps its last good revision until the limit is raised.
- Existing pages and files get their title when next edited; `fyrr writer sync
  <source> --kind content --full` followed by a commit republishes all that
  fit the limit. Going back to an earlier release on the same index is not
  supported for Google Drive: its revision order now includes the rendering,
  so the earlier release would skip edits and deletions of published files.

### Fixed

- The TypeScript client treated a write with an empty `conditions` object as a
  conditional one and never repeated it after a lost connection; the Python
  client and the server read it as no conditions. All three now agree: only a
  non-empty `conditions` stops the retry of a write.
- A GitHub issue, PR or file the connector contract rejects no longer stalls
  the content sync: display values are fitted (control characters to spaces,
  cut to 1,024 bytes, at most 32 labels), anything still rejected is
  quarantined, and the sync continues. Valid documents keep their revision.
- `fyrr reader search` accepts a query that starts with `-`, such as an issue
  title; put `--` before a query that is exactly an option name.
- A sync worker no longer reads every publication manifest the namespace ever
  wrote. Each publication records which envelopes its chain committed or
  rejected: an `envelope-checkpoint` object plus the few ids since
  (`envelope_evidence` in the content manifest, both additive), so the newest
  manifest and one checkpoint are enough. The first publication after the
  upgrade still walks the older chain once. `fyrr writer maintain` deletes
  checkpoints that no retained manifest names.
- `fyrr writer maintain` deletes the publication manifests and envelope
  checkpoints that no publication inside `writer.purge_window` names, so
  these no longer grow with the number of commits. `fyrr writer commit
  --receipt` still tells that an envelope of the deleted part was applied,
  without its publication (`publication_id` is null). The oldest manifest it
  keeps is recorded first in the `history-cut` object (additive to the
  unreleased 4.0 publication contract); a walk of the history ends there, and
  a manifest missing anywhere else is `publication_invalid`.
- `fyrr writer maintain` also deletes the policy and identity generations
  older than any generation the serving head or a retained publication names,
  so `policy/` and `identity/` no longer grow with the number of commits.

### Security

- The README, quickstart and software factory guide no longer store
  `FYRR_IDENTITY_KEY` in Claude Code's settings: they add the reader
  without `-e` and start Claude Code from a shell with the key exported.
  If you added fyrr with `-e FYRR_IDENTITY_KEY=...`, remove it and add it
  again as the updated docs show: after the README or quickstart, run
  `claude mcp remove fyrr --scope user`; after the software factory guide,
  run `claude mcp remove fyrr --scope local` in each project where you
  added it.

## [0.1.1] - 2026-11-19

### Added

- Releases carry `linux-aarch64` binaries next to `linux-x86_64`, and the
  `fyrr` and `fyrr-reader` images are published for `linux/amd64` and
  `linux/arm64` under one tag.

### Changed

- Release binaries and image archives carry their platform in the file
  name, for example `fyrr-image-<version>-linux-x86_64.tar.gz`. Each
  platform has its own `manifest-<platform>.txt`; `manifest.txt` lists the
  platforms.

## [0.1.0] - 2026-11-18

### Changed

- Local storage (`file://`) no longer serializes every write through one
  lock on the storage root. Creating an object takes no lock, replacing or
  removing one locks only that object, and nothing is flushed under a lock,
  so a busy disk no longer fails `writer sync` with `storage_unavailable` or
  `sync_lease_lost`. Each write is tagged with 128 random bits instead of a
  number from a counter on `.fyrr-storage.lock`, and stages in
  `.fyrr-storage.stage.d/`. Existing roots need no migration: the first
  write of this release removes the counter, and objects keep the tags they
  have. Earlier releases then refuse to write to that root, so upgrade every
  process that writes to it together; going back to an earlier release is
  not supported.
- Reader concurrency defaults to eight independent requests (configuration
  schema 5.0). Pin `reader.concurrency = 2` for the previous limit; memory sizing
  accounts for 64 bytes per physical row per prepared reader plus shared caches.

- Default fusion weights favour the dense lane more (dense_weight 0.5,
  lexical_candidates 200). Configuration schema 4.0 records the new defaults;
  dense candidates and rescoring are unchanged. Existing publications retain
  their policy digest: pin `dense_weight = 0.25` and `lexical_candidates = 50`
  to keep it, or run `fyrr writer commit` with the new settings before starting
  readers that use them. A policy mismatch is refused with `incompatible_index`.
- MCP contract 3.0 removes query embedding cache-hit and provider-call
  counters and provider-stage timing from responses to prevent disclosure of
  shared cache activity; search responses also suppress explicit durations.

### Fixed

- **A late head read is not used for authorization.** When a check waited for
  a shared head read that storage answered after more than two version-check
  intervals, the check was served with the head as it was when the read
  began, so a revocation published meanwhile could be missed for longer than
  the documented two intervals. Such a read now fails the checks that waited
  for it with `authorization_unavailable`, and the next check reads again.
  If the reader answers so persistently, storage takes longer than two
  intervals to return the head: raise `reader.version_check_interval`.
- Prepared sync timeout errors name the elapsed worker limit and `--timeout`,
  including deadline exhaustion during cleanup. Deployment guidance covers
  larger initial loads, durable envelopes, source leases and vector-cache reuse.

- Local-file extraction replaces NUL and other non-whitespace C0 controls with
  spaces instead of quarantining otherwise valid UTF-8 text. Raw-byte source
  revisions remain unchanged.

### Added

- A controlled reader-memory investigation and offline replay driver. Four
  fresh-process runs at 2304/1536/1536/2304 MiB returned identical ranks and scores
  for all 250 queries; the report retains the original Lambda uncertainty.

- Opt-in ObjectStore boundary diagnostics in a private JSONL file, including
  hashed keys, fixed error codes, latency, and observed S3 HTTP status/retries.
  Native transport facts that the boundary cannot observe remain null.

- MCP tools `search`, `fetch` and `list_filters`, served over stdio for one
  user or over Streamable HTTP with per-user OIDC tokens.
- Permission-aware hybrid retrieval: full-text and vector candidates are
  generated only from rows the caller may read, fused, grouped per document
  and optionally reranked.
- Fail-closed authorization: missing or malformed policy or identity refuses
  the request with `authorization_unavailable`, sources whose permission
  reads stop succeeding are hidden at their freshness deadline, and the
  reader rechecks live authorization before returning any result or error.
- Publication on S3: per-source sync into durable prepared batches, a single
  committer, immutable Lance publications switched atomically, restore and
  rollback that never revive revoked grants, retention and purge.
- Local storage for a single host: `storage.uri = "file:///absolute/path"`
  keeps the namespace in a private directory, with the same conditional
  writes, leases and publications as on S3, and runs the quickstart without
  Docker (`examples/quickstart/fyrr.toml`).
- Two builds of one binary: the full build (writer and reader, embedded
  Python for connectors) and the reader-only build (no Python).
- Python connector SDK `fyrr-connectors` 0.1.0 with bundled providers for
  local files, Google Drive, Notion, GitHub, Slack and Jira, and an example
  third-party provider.
- Directory sync from Keycloak, SCIM or a snapshot file, or operator-managed
  identities without a directory (`provider = "none"`).
- Container images for both builds, generated JSON Schemas and contract
  fixtures, and `scripts/ci`, which checks the reference documentation
  against the binaries.

[Unreleased]: https://github.com/fyrr-ai/fyrr/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/fyrr-ai/fyrr/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/fyrr-ai/fyrr/releases/tag/v0.1.0
