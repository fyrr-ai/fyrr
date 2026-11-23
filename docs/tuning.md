# Namespace and search tuning

> **In short**
>
> - `[namespace]` names the deployment (`name` is the one required key) and
>   holds the full-text, composition, retrieval and reranker settings.
> - Every key but `name` is the default for a namespace created from this
>   file. `writer namespace create` records them in the namespace, which keeps
>   them for its life: a process with another file serves it with the
>   namespace's own settings. To change one, create a new namespace; only the
>   attributes and their indexes can change, by a
>   [write request](http-api.md#changing-the-schema).
> - The keys under Advanced tuning are best left at their defaults unless a
>   measurement says otherwise.

Secrets, `[storage]`, `[embedding]` and `[[sources]]` are in
[configuration](configuration.md), `[identity]` in [identity](identity.md),
and the other `[reader]` and `[writer]` keys in
[reader and writer settings](reader-writer.md).

## `[namespace]`

| Key | Default | Meaning |
| --- | --- | --- |
| `name` | required | Namespace ID: lowercase letters, digits and `-`, at most 63 bytes. All objects live under it in the bucket. |
| `fts_max_token_length` | `40` | 1–1024: native full-text `max_token_length` in UTF-8 bytes; part of the publication identity. |
| `fts_stem` | `false` | English stemming at publication level. |
| `fts_remove_stop_words` | `false` | Stop-word removal. |
| `fts_ascii_folding` | `false` | ASCII folding. |
| `composition` | see below | Document slots of a search response. |
| `retrieval` | see below | Candidate depths, fusion weight and output bounds. |
| `rerank` | absent | Optional hosted reranker; absent makes no reranker calls. |
| `attributes` | `{}` | Typed attributes of the namespace: name to type, see below. |
| `attribute_indexes` | `{}` | The attributes whose indexes are not the default: `index = false`, `full_text` and `ngram`, see below. |
| `vectors` | `{}` | Named vector attributes besides the built-in `vector`: name to `dimensions` and an optional `from`, see below. |
| `write_api` | absent | Serves the [document write API](http-api.md#write-documents) and creates API namespaces, see below. |

### `[namespace.attributes]`

```toml
[namespace.attributes]
created = "datetime"
priority = "int"
groups = { array = "string" }
```

A namespace can declare up to 128 typed attributes when it is created. The
types are `string`, `int` (64-bit signed), `uint` (64-bit unsigned), `float`
(64-bit), `bool`, `datetime` (UTC microseconds, like `updated_at`), `uuid`
(8-4-4-4-12 hexadecimal digits in requests, either case; stored in 16 bytes)
and `bytes` (base64 in requests, at most 4096 characters). An array is
`{ array = "<type>" }` of any type but `bytes`: a JSON array of values of that
type, none of them `null`. These types are declared, never inferred: an
undeclared attribute takes the type of its first value, `string`, `int`,
`float` or `bool`, or an array of one of them. The first content publication
creates a nullable column for each declaration. The
[`filter` argument](mcp-tools.md#filters) can compare those columns. The
[document write API](http-api.md#write-documents) supplies their values.
Connector rows leave them null, so a range comparison such as `priority >= 3`
returns no hits on those rows. Use the `attributes` argument to match the
`key=value` string labels supplied by
[connector documents](connectors/authoring.md#documents).

A namespace created for connectors also declares five attributes itself:
`title`, `team` and `status` (`string`), `labels` (an array of `string`) and
`created_at` (`datetime`), the fields of a connector document's metadata. They
count toward the 128, are filtered and sorted by like any other attribute, and
are not stored in the document's metadata. A file cannot declare them with
another type. A namespace written through the API has none of them, and a
namespace created before them keeps the fields in its documents' metadata, as
it was written, and cannot be filtered by them.

A `string`, number, `datetime` or `uuid` attribute gets a B-tree index, a
`bool` a bitmap index and an array a label-list index. A `bytes` attribute has
no index: it is stored, and cannot be filtered or sorted by. A reader opens each index: about two storage requests per
attribute when it opens a namespace (1.8 measured with 64 attributes; the
storage request budget test holds it).

### `[namespace.vectors]`

```toml
[namespace.vectors.image]
dimensions = 512

[namespace.vectors.title_vec]
dimensions = 1024
from = "title"
```

Every namespace has the built-in `vector`: the embedding of a record's text, or
the vector a row carries, of the `[embedding]` dimensions. A namespace written
through the [document write API](http-api.md#write-documents) can declare more
vector attributes when it is created, for example the embedding of a title or of
an image next to that of the text. Each is a nullable column of its own, with
`dimensions` (a multiple of 8, 8 to 8192; a name is not `id`) and a vector index of its own, built
as the index of `vector` is: the namespace's `dense_metric`, `dense_element`,
`dense_rq_bits` and `dense_partition_rows` apply to all of them. At most 16
([`MAX_VECTOR_ATTRIBUTES`](limits.md)) can be declared. A reader opens an
attribute's index when it first searches it, which costs 2 storage requests (2.00
measured with 16 attributes; the storage request budget test holds it), so the
first search of every attribute costs 32, within the 36 requests of opening a
namespace and answering its first search. Each index is also built and updated
with every publication. The name follows the rules of an attribute's, and no
attribute, no built-in column and no `text_attribute` has it. The declaration is
fixed for the life of the namespace (`409 namespace_settings_fixed`).

`from` names a declared `string` attribute. A row that carries no vector for
the attribute and has a value for `from` gets the embedding of that value, made
by the writer with the namespace's `[embedding]` model, so `dimensions` must be
the model's. Without `from`, a row carries its vector or leaves the attribute
null. A row that leaves it null, or has no value to embed, is not returned by a
search of that attribute. A search names the attribute with
[`vector_attribute`](mcp-tools.md#search); the built-in `vector` is searched
when it names none. A vector attribute is searched, not returned, and the
[recall](http-api.md#recall) of a namespace measures the built-in vector only.

A search of a vector attribute asks Lance for the nearest rows of its column
under the same permissions and filters as a search of `vector`, and is checked
row by row against them. It does not use the partition maps and the compressed
centroid routing (`reader.routing`) that the built-in vector's dense lane does:
Lance probes `reader.nprobes` partitions. A request with a filter (and a text
query, which selects the rows that have a vector) makes it a flat scan over the
selection, not an index probe, and an answer of the index that is shorter than
the request asks for is scanned again flat: the cost grows with the selection,
as an `exact` search does. An attribute that has no vector yet has no index,
and a search of it reads the column: the commit that brings its first vector
builds the index.

A namespace with vector attributes is
[content-manifest](../CONTRIBUTING.md#rules-for-changes) 5.0 and its policy
artifact 4.0, which a build that does not know them refuses; one without keeps
its versions, files and retrieval-policy digest.

### `[namespace.attribute_indexes]`

```toml
[namespace.attribute_indexes.note]
index = false
ngram = true
```

An attribute has the index of its type unless the table says otherwise. The
keys of an entry are `index` (default `true`: the index above), and
`full_text` and `ngram` (default `false`: a full-text or an n-gram index on the
column). A `string` attribute may have both; an array of strings may have
`full_text`, which indexes one document per element, so that a match never
spans two elements. An entry names a declared attribute, differs
from the default and asks only for what the type can have, else the schema is
refused (`settings_invalid` over HTTP). Each index is one more storage request
or two when a reader opens the namespace, and one more index to build and to
update with every publication.

An attribute without an index is still filtered and sorted by: the reader
scans its column, fragment by fragment, and the request deadline (`deadline_ms`,
10 s by default) bounds the scan like any other work of the request. That is
cheap on a small namespace and costs the time of reading the column on a large
one, so leave `index = false` to attributes that are rarely filtered. The n-gram
index answers the `contains` and `matches` filters on a `string`
([text tests](mcp-tools.md#text-tests)); the full-text index is built and kept,
and no filter operator reads it yet.

A full-text index takes the namespace's `fts_*` keys, as the text index of the
documents does, unless it sets an [analyzer](#analyzer-of-a-full-text-index) of
its own.

A new namespace's first publication builds every index its schema asks for.
After that the attributes and their indexes change by a
[schema request](http-api.md#changing-the-schema): the manifest of a publication
records the indexes that are built, and the next maintenance builds the ones
that were switched on since.

A name is a lowercase letter followed by lowercase letters, digits and `_`, at
most 128 bytes; a built-in column name or a name starting with `_` is refused.
The schema is recorded in the namespace and in every content manifest. Like the
other settings it is fixed for the life of the namespace, but for new attributes
and the indexes of attributes, which a
[write request](http-api.md#changing-the-schema) changes on a namespace that
exists. A type is never changed and an attribute is never removed.

The document write API refuses an undeclared attribute with
`attribute_undeclared`, and a value that does not fit its type with a named
error: `attribute_type_mismatch`, `attribute_value_invalid` or
`attribute_value_too_large` (a `string` over 4096 bytes). A schema is refused
with `attribute_limit_exceeded` (a 129th attribute), `attribute_name_invalid` or
`attribute_name_reserved`. A namespace that the write API creates cannot declare `id` (the key of a row) as an attribute or as its `text_attribute`.

### Analyzer of a full-text index

```toml
[namespace.attribute_indexes.title]
full_text = true
[namespace.attribute_indexes.title.analyzer]
tokenizer = "unicode"
language = "german"
ascii_folding = true
```

`analyzer` sets how the full-text index of an attribute cuts a text into terms,
instead of the namespace's `fts_*` keys. It is an option of a `full_text` entry
and is refused without it; the keys it does not set have the defaults below, not
the namespace's. A text is always lowercased. Every key is optional.

| Key | Default | Meaning |
| --- | --- | --- |
| `tokenizer` | `simple` | `simple`: runs of letters and digits, so a run of Chinese, Japanese or Korean characters is one token. `whitespace`: runs of characters that are not white space. `unicode`: Unicode word segmentation, which finds the words of Chinese and Japanese text by a dictionary and cuts what it does not know into what it does. `ngram`: substrings of the whole value, which cross spaces. |
| `language` | absent | One of the languages below: stems the terms and removes the stop words of that language. Absent: no stemming and no stop words. |
| `stop_words` | absent | A list of at most 1024 words of 1–64 bytes that is removed instead of the language's. A word is compared with a term after it is lowercased and stemmed. |
| `ascii_folding` | `false` | Folds accented letters to ASCII, `é` to `e`. |
| `max_token_length` | `40` | 1–1024: a longer token, in UTF-8 bytes, is dropped; an n-gram is a token too. |
| `ngram_min_length` | `3` | 1–16: shortest n-gram; only with `tokenizer = "ngram"`. |
| `ngram_max_length` | `3` | `ngram_min_length`–16: longest n-gram; only with `tokenizer = "ngram"`. |
| `prefix_only` | `false` | Only the n-grams that start the value; only with `tokenizer = "ngram"`. A [`contains` condition](http-api.md#matching-tokens) refuses an attribute with this tokenizer. |

The `ngram` tokenizer keeps every n-gram, so it takes no `language` and no
`stop_words`, and its index holds up to `ngram_max_length - ngram_min_length + 1`
terms per character of the value: it is several times the size of a word index.
With `unicode` and a `language` but no `stop_words`, the stop words removed are
those of every language the pinned Lance knows, not only the language's; give
`stop_words` to remove exactly the words you want.

Dictionary tokenizers for Chinese, Japanese and Korean are not
offered; `unicode` is the one that segments them.

The languages are `arabic`, `danish`, `dutch`, `english`, `finnish`, `french`,
`german`, `greek`, `hungarian`, `italian`, `norwegian`, `portuguese`,
`romanian`, `russian`, `spanish`, `swedish`, `tamil` and `turkish`: the 18 that
the pinned Lance has a stemmer and a stop-word list for.

A [schema request](http-api.md#changing-the-schema) changes the analyzer of an
index that exists. The request is published at once and the index stays as it
was built: a reader keeps serving it. The next maintenance (`fyrr writer serve`
starts it when the manifest records another analyzer than the settings, `fyrr
writer maintain` runs it by hand) builds the index again and publishes it, and
a reader moves to it with that publication. The rebuild reads every row of the
attribute, so it takes the time of building the index of a namespace that size.

### `[namespace.write_api]`

```toml
[namespace.write_api]
text_attribute = "text"
open = false
```

Present, it makes this process serve the
[document write API](http-api.md#write-documents) (which needs
`[writer.serve.admin]` and the committer) and gives a namespace created from
this file or through the API its documents from the API alone: no
`[[sources]]`, and a connector namespace refuses an API write. The keys are the
defaults of a new namespace, recorded in it and fixed for its life.

| Key | Default | Meaning |
| --- | --- | --- |
| `text_attribute` | `"text"` | The key of a row that holds its text, which is embedded when the row carries no `vector`, full-text indexed and returned as the content of its record. Not one of `attributes`. |
| `open` | `false` | `false`: a row is readable by the reader tokens it names and a row without any by nobody. `true`: every authenticated subject of the deployment reads every row. |

### `[namespace.composition]`

Fused results come first, then further unique dense and lexical hits. The
slots total at most 75.

| Key | Default | Meaning |
| --- | --- | --- |
| `fusion` | `50` | 1–75 fused document slots. |
| `dense` | `0` | 0–74 further dense-only document slots. |
| `lexical` | `0` | 0–74 further lexical-only document slots. |

### `[namespace.retrieval]`

| Key | Default | Range | Meaning |
| --- | --- | --- | --- |
| `dense_weight` | `0.5` | 0–1 | Weight of the dense lane in fusion; lexical gets the rest. |
| `dense_candidates` | `75` | 1–75 | Dense candidates per query; a request whose `limit` is larger fetches `limit`. |
| `lexical_candidates` | `200` | 1–200 | Lexical candidates per query. |
| `source_cap` | `2` | 0–75 | Soft cap of documents per source in each composition bucket; `0` disables it. With [`group_documents: false`](http-api.md#grouping-and-caps) it counts records. |
| `max_documents` | `75` | 1–320 | Unique documents in a multi-query response. |

### `[namespace.rerank]`

| Key | Default | Meaning |
| --- | --- | --- |
| `endpoint` | required | HTTPS endpoint of the reranker (loopback HTTP is allowed for a local [provider](architecture.md#terms)). |
| `model` | required | Model name sent to the provider. |
| `api_key` | absent | `{ env = "VAR" }`. |
| `candidates` | required | Authorized composed documents to rerank before the request limit (1–75). |
| `weight` | required | 0–1 blend of reranker order (1) and fusion order (0). |
| `deadline` | required | One deadline for every provider call of a search (1ms–300s). |
| `spend_limit_microusd` | required | Process-lifetime cap for the namespace, in millionths of a US dollar. Reservations are not refunded. |
| `max_call_cost_microusd` | required | At least 1: operator-attested cost bound of one request; reserved before each call. |
| `text_chars` | absent | 200–32768: send the window with the most distinct query terms instead of the whole record. |

A namespace records only what decides its results: `model`, `candidates`,
`weight` and `text_chars`. `endpoint`, `api_key`, `deadline` and the two spend
keys are process configuration: change them in any process's file. A process
that serves or writes a namespace with a reranker needs `[namespace.rerank]`
for them, and a reader without it refuses the namespace with
`incompatible_index`.

Reranker calls run at most four at once and occupy one reader concurrency
slot until they finish or the deadline expires. There are no retries or
redirects.

## Advanced tuning

These keys change dense-search depth, routing and reader memory. Leave them
at their defaults unless a measurement says otherwise.

### `[namespace.retrieval]` refinement

The dense lane first finds candidates through the vector index, then
rescores them with their stored float16 vectors.

| Key | Default | Meaning |
| --- | --- | --- |
| `rescore_depth` | `{ mode = "legacy" }` | How many index candidates are rescored. `{ mode = "legacy" }` rescores four times `dense_candidates`; `{ mode = "fixed", candidates = N }` rescores `N` (1 to 1200), but never fewer than `dense_candidates + 1`; `{ mode = "adaptive", min_candidates, max_candidates, min_gap }` starts at `min_candidates` and doubles, up to `max_candidates`, while the score gap at the cut is below `min_gap`. |

### `[namespace]` vector index

| Key | Default | Meaning |
| --- | --- | --- |
| `dense_rq_bits` | `1` | 1–9: RaBitQ bits per dimension of the vector index. More bits make the index candidates closer to the exact order at the cost of a larger index. The value is recorded when the namespace is created and fixed for its life. To change it, create a new namespace. |
| `dense_partition_rows` | `550` | 32–4096: target rows per partition of the vector index. Smaller partitions scan fewer rows per probe but give more centroids to route over and a costlier index build; larger ones do the opposite. The value is recorded when the namespace is created and fixed for its life. To change it, create a new namespace. As a namespace grows, `writer maintain` re-partitions it: a partition above four times the target is split and one below a quarter of it is joined. `writer serve` does not do this. It costs about one first-build training of the split partitions in RAM and CPU. The split is one step: if it does not fit `writer.maintenance.step_max_duration`, `writer maintain` fails like any step that runs out of time, so raise that setting for a large re-partition. |
| `dense_metric` | `cosine` | `cosine`, `squared_l2` or `dot`: the distance the vector index, the partition routing, the rescoring and the recall sample measure, and the one the embeddings were trained for. `cosine` ignores the length of a vector; `squared_l2` is the squared Euclidean distance; `dot` ranks by the dot product, so a longer vector ranks higher. Recorded when the namespace is created and fixed for its life, reported by the [namespace report](http-api.md#namespace-report). To change it, create a new namespace. Lance's IVF_RQ index supports all three. Under `dot` the dense lane lets Lance choose the partitions, because the compressed centroid scan of `reader.routing = "rabitq"` ranks by Euclidean distance; the setting then has no effect on that namespace. The `min_gap` of an adaptive `rescore_depth` is measured in the distance of the metric. |
| `dense_element` | `f16` | `f16` or `f32`: the type a vector is stored in. `f16` halves the column and rounds each number to 16 bits; `f32` keeps the number as sent. Fixed like `dense_metric`. A vector must be finite and not zero; a 16-bit one must also stay so after rounding, so a number of magnitude 65520 or more (it rounds to infinity) is refused (`400 vector_invalid`) by an `f16` namespace and kept by an `f32` one. No number may exceed 1e6 in magnitude, so that a distance of the widest vector stays finite inside the index, and the squares of a vector must sum to a normal 32-bit number (a vector of numbers near 1e-20 or smaller is refused): `400 vector_invalid`. A vector that an embedding provider returns is checked the same way for the namespace's type. Lance's IVF_RQ index builds over both; 8-bit integer and 64-bit float vectors are not offered. |

A namespace with a `dense_metric` or `dense_element` other than the defaults is
[content-manifest](../CONTRIBUTING.md#rules-for-changes) 5.0 and its policy
artifact 4.0, which a build that does not know them refuses.

### `[reader]` search and memory

| Key | Default | Meaning |
| --- | --- | --- |
| `nprobes` | `256` | 1–65536: the fewest vector-index partitions searched per index segment, counting only partitions that hold rows the caller may read. A caller who may read only part of the rows searches more partitions, nearest first, until they hold as many of its rows as `nprobes` partitions of average size hold; see [callers who read few rows](#callers-who-read-few-rows). |
| `routing` | `exact` | How the dense lane picks partitions: `exact` compares the query with every partition centroid, `rabitq` with compressed (RaBitQ) centroids, `auto` uses `rabitq` from 8192 centroids. |
| `sparse_read` | `masked_scan` | How the stored vectors of candidates are read for rescoring: `masked_scan` (a scan restricted to the candidate rows) or `take_rows` (direct row reads). |
| `memory_max_bytes` | `2304MiB` | Memory budget for native caches and for preparing a new publication. |
| `memory_headroom_bytes` | `128MiB` | Left unused while preparing a new publication; must be smaller than `memory_max_bytes`. |
| `fts_search_chunk` | absent (16, [Lance](architecture.md#terms)'s own default) | 1–1024 full-text partitions scored one after another by one compute task of a query. With `1`, every partition of an index segment is its own task, so one query scores several partitions at once; see [full-text partitions](#full-text-partitions). Needs a full-text index of several partitions. Does not change scores. |

#### Callers who read few rows

A caller's rows are spread over many partitions, so the `nprobes` nearest
partitions that hold any of them may hold only a few. The dense lane therefore
keeps searching partitions in order of distance, skipping those without a row
the caller may read, until the searched partitions hold at least
`nprobes` × (indexed rows ÷ partitions) of the caller's rows, or until it has
searched every partition that holds one. A caller who may read every row
searches `nprobes` partitions, or one or two more when the nearest ones are
smaller than average. A caller whose few rows are spread over most partitions
searches most of the index; the cost of a query then grows with the partitions
that hold the caller's rows, not with `nprobes`.

#### Dense search under a request filter

A search with `sources`, `authors`, `updated_after` or another filter ranks
only the rows that pass both the caller's permissions and the filter. The dense
lane takes whichever of two paths reads fewer bytes:

- **Exact scan:** reads the stored vector of every selected row and returns
  the exact top candidates. Its cost grows with the selection.
- **Vector index:** reads the compressed code of every row in the partitions
  it probes, plus the stored vectors of the rescored candidates. It probes
  partitions nearest first, as for [callers who read few rows](#callers-who-read-few-rows),
  until they are expected to hold as many selected rows as `nprobes`
  partitions of average size hold. The estimate assumes the selected rows
  spread over partitions like the caller's readable rows.

A small selection therefore stays exact; a larger one takes the index and
returns approximate candidates, with about the recall of an unfiltered search
at the same `nprobes`. More bits per dimension (`dense_rq_bits`) make the codes
larger and keep more selections on the exact scan. If the index returns fewer
candidates than wanted, the request is answered by the exact scan. Rows the
filter or the permissions exclude are never scored.

##### Recall under a filter, measured

`scripts/bench-filtered-recall` drives the real `fyrr writer serve` and
`fyrr reader serve` on local storage: 100,000 rows of 128-dimension
(`f16`, `cosine`) vectors in 200 clusters, written in two requests (two
vector index segments), default `nprobes`, `limit` 10. A filter keeps the rows
below a bound of an integer attribute. `spread` values are an even sample of
every cluster; `clustered` values are whole clusters, so the kept rows sit in
a few partitions. Recall@10 is the share of the answer of the same request
with `exact: true` that the default request returns, averaged over 200 queries
that do not depend on the filter. Measured on one laptop-class machine (20
logical CPUs, 31 GiB), local storage, not object storage, at commit `d86a4236`
plus the script's final config change:

| Filter keeps | Values | Recall@10 mean | Queries under 0.90 | Lowest | Rows read | Rows read by the exact request |
| --- | --- | --- | --- | --- | --- | --- |
| 100% | none | 0.988 | 5 of 200 | 0.70 | 592 | 100,093 |
| 50% | spread | 1.000 | 0 | 1.00 | 1,347 | 100,090 |
| 10% | spread | 1.000 | 0 | 1.00 | 16,931 | 16,931 |
| 1% | spread | 1.000 | 0 | 1.00 | 2,213 | 2,213 |
| 0.1% | spread | 1.000 | 0 | 1.00 | 2,832 | 2,832 |
| 50% | clustered | 0.951 | 28 of 200 | 0.40 | 611 | 50,108 |
| 10% | clustered | 1.000 | 0 | 1.00 | 10,214 | 10,215 |
| 1% | clustered | 1.000 | 0 | 1.00 | 1,079 | 1,079 |
| 0.1% | clustered | 1.000 | 0 | 1.00 | 159 | 159 |

Every case holds a mean of 0.90 or more, so the planner is unchanged. At 10%
and below the request reads exactly what the exact request reads: the planner
took the exact scan, which is exact by construction. Only the 50% filters use
the vector index, and a filter that keeps whole clusters loses some recall
there (0.951 against 0.988 unfiltered) with a tail of queries well below the
mean: a filter that keeps a large share of the rows but whose values cluster
in a few partitions can send the probes to partitions the filter excludes.

"Rows read" is a diagnosis, not a counter of the reader: the median of the
bytes the reader process read from files per request (`rchar` of
`/proc/<pid>/io`), over the 256 bytes of one stored vector. It counts the
stored vectors a request scored, a page at a time, so it overstates a small
read; the compressed codes of the index are cached in memory and are not in it.

Memory sizing must include private prepared readers as well as shared caches.
The preparation accounting allows 64 bytes per physical Lance row per prepared
reader for admission bitmaps and worst-case unindexed row sets. For `R` rows and
concurrency `C`, the cold-generation allowance is
`32 MiB + 64 * R * (1 + C * (1 + reader_live))` bytes; `reader_live` is 0 or 1.
At 20,000 rows, each prepared reader adds 1.22 MiB; the default `C = 8` needs
10.99 MiB of row state, or 20.75 MiB with live embeddings, plus the 32 MiB base.
At 1,000,000 physical rows, each prepared reader adds 64,000,000 bytes
(61.04 MiB). Eight workers plus the template require 576,000,000 bytes
(549.32 MiB) of row state; with live embeddings, sixteen workers plus the
template require 1,088,000,000 bytes (1037.60 MiB), in addition to the 32 MiB
base. Two warm generation claims at this scale can exceed the default budget
once native caches and startup RSS are included, so raise the memory envelope
or lower concurrency before serving that index.

Native Lance sessions, vector roots, identity/policy artifacts and query-vector
caches are shared within the serving generation. Their memory is additional,
not eight copies. Warm preparation adds decoded vector-index inputs and scratch;
a publication handover reserves two warm claims and native cache capacity.
The shared authorization cache has a separate 128 MiB allowance, and startup RSS
and configured headroom are subtracted before allocating the preparation pool.

This derivation is an accounting allowance, not an RSS measurement. Leave space
for active scans, embeddings, response buffers and allocator retention; measure
peak RSS with representative row counts, query concurrency and publication
handover. Increase the process/container limit with the reader budget, or lower
`concurrency` if requests exhaust the envelope. Eight is a fixed default because
I/O and provider waits overlap even on a two-core host; CPU count alone does not
predict concurrent client demand. Keep the documented 1–16 bound.


A separate idle sizing measurement on the preserved 20,000-document index
(20,020 physical rows) compared fresh processes at concurrency 1/8/8/1, with live
embeddings, two CPUs and the same frozen query vector. Each process had one
template plus 2 or 16 prepared workers. Median RSS was 102,830,080 / 108,007,424 /
107,204,608 / 102,801,408 bytes: the mean difference divided by fourteen extra
workers was about **342,162 bytes (0.33 MiB) per prepared reader**. This measures
idle incremental RSS on that index, including private metadata; it is not a peak
request or handover bound. The conservative row-state allowance there is
**1,281,280 bytes (1.22 MiB) per prepared reader**. Use the allowance and peak-load
measurements for sizing rather than extrapolating the smaller idle measurement.
Raw RSS/PSS/private samples are retained under
`target/investigation/prepared-reader-cost/`.

### Full-text partitions

A full-text index is built as one or more partitions, and a query scores the
partitions with global statistics, so a document's score does not depend on
how many partitions the index has. Lance's default partition size is 2 GiB of
builder memory, so the index of a small or mid-sized corpus is one partition
and one query scores it on one thread. Two settings let a single query use
spare cores; both are off by default, and neither changes scores or rankings:

- [`writer.fts_partition_memory_mb`](reader-writer.md#writer) builds the index
  as several partitions. The count is roughly the full-text builder memory
  divided by the target (between that and about twice that plus one), not an
  exact number; a smaller target makes more partitions. The writer applies the
  same size to index merges, so merging segments does not collapse the layout.
  It takes effect at the next full-text build or merge; a published index keeps
  its layout until the full-text settings change or a merge rewrites it.
- `reader.fts_search_chunk = 1` makes each partition of a segment its own
  compute task. Lance reads the value once per process, so `fyrr reader` sets
  it before its first Lance call. Without it, or with a value at least the
  partition count, the partitions of a segment are scored one after another in
  one task and extra partitions give no speed-up. A value of `2` spreads four
  partitions over two tasks.

At start, and for every new serving generation, the reader logs one line with
the partition counts and what they allow, for example:

```text
telemetry fts_search=layout segments=1 partitions_min=4 partitions_max=4 search_chunk=1 compute_threads=6 parallel_tasks=4 concurrent=yes
```

`concurrent=no` with `partitions_max` above 1 means a query still scores that
index on one thread: the chunk is at least the partition count, or the compute
pool has one thread.

What to expect:

- Partitions shorten one query on a host with idle cores. They do not raise
  throughput under load: the tasks of concurrent requests share the same
  compute threads (`LANCE_CPU_THREADS`, by default the CPU count minus two),
  and each request adds some CPU for repeated per-partition work.
- A host with three or fewer CPUs gains nothing: Lance's compute pool then has
  one thread.
- The settings add no threads and no cache. Tasks run on the existing compute
  pool, so reader memory sizing changes only through the index growth below.
  With `concurrency` requests in flight, up to
  `concurrency × partitions × segments` tasks queue for the compute threads.
- Each partition has its own token dictionary and document table, so the index
  grows with the partition count. An earlier measurement of a four-partition
  rebuild on another Lance version found about 11 % more index bytes; it has
  not been measured on this version. The reader's warm reservation scales with
  index bytes.
- Choose the target by building once and reading the logged partition count;
  start from a quarter of the corpus's full-text builder memory. A build uses
  up to the target per full-text worker (`writer.threads` sets the worker
  count), so the writer's peak memory is not larger than before.
