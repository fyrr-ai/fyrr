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

### `[reader]` search and memory

| Key | Default | Meaning |
| --- | --- | --- |
| `nprobes` | `256` | 1–65536 vector-index partitions searched per index segment, counting only partitions that hold rows the caller may read. |
| `routing` | `exact` | How the dense lane picks partitions: `exact` compares the query with every partition centroid, `rabitq` with compressed (RaBitQ) centroids, `auto` uses `rabitq` from 8192 centroids. |
| `sparse_read` | `masked_scan` | How the stored vectors of candidates are read for rescoring: `masked_scan` (a scan restricted to the candidate rows) or `take_rows` (direct row reads). |
| `memory_max_bytes` | `2304MiB` | Memory budget for native caches and for preparing a new publication. |
| `memory_headroom_bytes` | `128MiB` | Left unused while preparing a new publication; must be smaller than `memory_max_bytes`. |


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
