# Namespace and search tuning

> **In short**
>
> - `[namespace]` names the deployment (`name` is the one required key) and
>   holds the full-text, composition, retrieval and reranker settings.
> - Composition and retrieval changes publish a new retrieval policy without
>   rewriting text; an analyzer change rebuilds the lexical index at the next
>   commit.
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
| `dense_candidates` | `75` | 1–75 | Dense candidates per query. |
| `lexical_candidates` | `200` | 1–200 | Lexical candidates per query. |
| `source_cap` | `2` | 0–75 | Soft cap of documents per source in each composition bucket; `0` disables it. |
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
| `dense_rq_bits` | `1` | 1–9: RaBitQ bits per dimension of the vector index. More bits make the index candidates closer to the exact order at the cost of a larger index. The value is recorded in each publication and fixed once the namespace has one: a writer configured with another value fails with `dense_rq_bits_mismatch` instead of extending the index. To change it, publish into a new namespace. |
| `dense_partition_rows` | `550` | 32–4096: target rows per partition of the vector index. Smaller partitions scan fewer rows per probe but give more centroids to route over and a costlier index build; larger ones do the opposite. The value is recorded in each publication and fixed once the namespace has one: a writer configured with another value fails with `dense_partition_rows_mismatch` instead of extending the index. To change it, publish into a new namespace. |

### `[reader]` search and memory

| Key | Default | Meaning |
| --- | --- | --- |
| `nprobes` | `256` | 1–65536 vector-index partitions searched per index segment, counting only partitions that hold rows the caller may read. |
| `routing` | `exact` | How the dense lane picks partitions: `exact` compares the query with every partition centroid, `rabitq` with compressed (RaBitQ) centroids, `auto` uses `rabitq` from 8192 centroids. |
| `sparse_read` | `masked_scan` | How the stored vectors of candidates are read for rescoring: `masked_scan` (a scan restricted to the candidate rows) or `take_rows` (direct row reads). |
| `memory_max_bytes` | `2304MiB` | Memory budget for native caches and for preparing a new publication. |
| `memory_headroom_bytes` | `128MiB` | Left unused while preparing a new publication; must be smaller than `memory_max_bytes`. |
| `fts_search_chunk` | absent (16, [Lance](architecture.md#terms)'s own default) | 1–1024 full-text partitions scored one after another by one compute task of a query. With `1`, every partition of an index segment is its own task, so one query scores several partitions at once; see [full-text partitions](#full-text-partitions). Needs a full-text index of several partitions. Does not change scores. |

#### Dense search under a request filter

A search with `sources`, `authors`, `updated_after` or another filter ranks
only the rows that pass both the caller's permissions and the filter. When
that selection is small, the dense lane scans exactly those rows: the result
is the exact top candidates and the cost grows with the selection. When the
partitions that `nprobes` visits are expected to hold at least 64 selected rows
for every rescored candidate, the dense lane searches the vector index with
the selection as its row filter, as it does without a filter, and so returns
approximate candidates. The expected count is the selection times the probed partitions divided by
the partitions that hold readable rows, summed over index segments; it assumes
the selected rows spread like the caller's readable rows. A larger `nprobes`, a smaller `dense_candidates` or
`rescore_depth`, or a larger selection therefore moves a request to the index.
If the index returns fewer candidates than wanted, the request is answered by
the exact scan. Rows the filter or the permissions exclude are never scored.

#### Which partitions hold readable rows

Building a caller's admission for a publication does not read the vector
index. The first request that ranks the partitions for a query asks, for each
partition it walks nearest first, whether it holds a row the caller may read,
and stops at the `nprobes` partitions that do (for `rabitq` routing, at the
shortlist of `4 * nprobes`). Each answer is kept with the cached admission, one
byte per partition, so a partition is read at most once per caller and
publication. The partitions probed are the same as if every partition had been
checked first. A caller who can read few partitions near the query is therefore
served after reading a few partitions; one whose readable partitions are all
far away, or fewer than `nprobes` (or than the `rabitq` shortlist), makes the
first request read every partition. A request filter with a
large selection also needs the number of partitions that hold readable rows
and resolves the remaining ones once.

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
