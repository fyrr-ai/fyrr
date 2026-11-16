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

Changing an `fts_*` analyzer setting rebuilds the lexical index at the next
commit. Composition and retrieval changes publish a new retrieval policy
without rewriting text.

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
| `dense_weight` | `0.25` | 0–1 | Weight of the dense lane in fusion; lexical gets the rest. |
| `dense_candidates` | `75` | 1–75 | Dense candidates per query. |
| `lexical_candidates` | `50` | 1–200 | Lexical candidates per query. |
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

### `[reader]` search and memory

| Key | Default | Meaning |
| --- | --- | --- |
| `nprobes` | `256` | 1–65536 vector-index partitions searched per index segment, counting only partitions that hold rows the caller may read. |
| `routing` | `exact` | How the dense lane picks partitions: `exact` compares the query with every partition centroid, `rabitq` with compressed (RaBitQ) centroids, `auto` uses `rabitq` from 8192 centroids. |
| `sparse_read` | `masked_scan` | How the stored vectors of candidates are read for rescoring: `masked_scan` (a scan restricted to the candidate rows) or `take_rows` (direct row reads). |
| `memory_max_bytes` | `2304MiB` | Memory budget for native caches and for preparing a new publication. |
| `memory_headroom_bytes` | `128MiB` | Left unused while preparing a new publication; must be smaller than `memory_max_bytes`. |
