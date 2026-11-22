# Reader and writer settings

> **In short**
>
> - `[reader]` bounds concurrency, the local mirror, version checks and
>   response size; `[reader.http]` turns on Streamable HTTP MCP.
> - `[writer]` sets retention windows, document and envelope limits, commit
>   groups, maintenance, backpressure and `writer serve`.
> - Keys have defaults or may stay absent, except `listen` and `public_url`
>   in `[reader.http]` and `listen` and `operator_token` in `[writer.serve.admin]`.

Durations and byte sizes are written as described under
[secrets](configuration.md#secrets). The `[reader]` keys for search depth
and memory are under [advanced tuning](tuning.md#advanced-tuning).

## `[reader]`

| Key | Default | Meaning |
| --- | --- | --- |
| `concurrency` | `8` | 1–16 requests executing at once; the waiting queue has the same bound, then `busy`. |
| `filter_count_max_rows` | `1000000` | 1–10,000,000 authorized rows one `list_filters` call may inspect; over it, `busy`. |
| `mirror_path` | absent (`$XDG_CACHE_HOME/fyrr/<namespace>`, else `~/.cache/fyrr/<namespace>`) | Private local directory of immutable read-through files (mode 0700, files 0600, owned by the process, not a symlink), created on first use. The default does not depend on the working directory; a relative `mirror_path` resolves against it. Without the setting and without an absolute `XDG_CACHE_HOME` or `HOME`, the reader refuses to start (`storage_configuration_invalid`). |
| `mirror_max_bytes` | `8589934592` | Disk limit of the mirror in bytes (8 GiB). An integer; unit suffixes are refused. A data file read in pages counts at its full length plus a page map of about 1/256 of it from its first page; an object that does not fit is read from storage without being kept. |
| `version_check_interval` | `30s` | How often the reader reads the serving head; the background scheduler caps the effective interval at 30 s. The read finds new publications and is what requests check their grants against, so a published revocation or membership change takes effect with the next check, at the latest two intervals after it was published. `SIGHUP` and `POST /admin/refresh` start a check at once. A reader that did not run for a second or more (an environment frozen between requests, a suspended host) and finds a check due starts it at once; requests wait up to 2 s for that check (at most half of their deadline), then are answered from the version the reader holds, flagged `stale_version` when a newer one is published. |
| `max_preparation` | `15m` | Upper bound on preparing a new generation. |
| `max_response_bytes` | absent | 4096–16777216: budget for a serialized search result. Over it, whole hits are dropped from the end of the rank order (each query keeps at least four), then the lowest-ranked hits lose their excerpt or content; `truncated` and `omitted_bytes` report what was removed. |
| `instructions` | absent (built-in text) | 0–4096 characters sent to MCP clients as `instructions` in the `initialize` result, on stdio and HTTP. Absent sends the [built-in text](mcp-tools.md#transport); a non-empty string replaces it; an empty string `""` sends no `instructions`. Not a secret. |
| `http` | absent | Streamable HTTP MCP, see below. |

The fixed default of eight supports eight simultaneous clients even on two-core
hosts, where embedding and storage waits can overlap. Set `concurrency` explicitly
(1–16) for your workload. Each request uses an independent prepared reader;
shared native caches are not multiplied by concurrency. Its accounted private
row-state cost is **64 bytes per physical Lance row per prepared reader**:
20,000 rows cost 1,280,000 bytes (1.22 MiB). Documents can span many rows.
The scheduler prepares `concurrency` workers, or twice that when
`embedding.reader_live = true`, plus one template. At eight and 20,000 rows,
that is 10.99 MiB (20.75 MiB with live embeddings), plus the shared 32 MiB
base reservation. These are derived preparation allowances, not measured total
RSS or a ceiling on per-request scan and response allocations. See
[memory sizing](tuning.md#reader-search-and-memory) before raising concurrency.
On the preserved 20,000-document index (20,020 physical rows), a measurement
with fresh processes found about 0.33 MiB of additional idle RSS per prepared
reader; the conservative row-state allowance is 1.22 MiB. Active requests and
handover require additional space. See the measurement and 1M-row example in
[memory sizing](tuning.md#reader-search-and-memory).
Pin `concurrency = 2` to retain the previous request and queue limits.

### `[reader.http]`

| Key | Default | Meaning |
| --- | --- | --- |
| `listen` | required | IP socket address (`IP:port`). The listener speaks plain HTTP; see [HTTP boundary](http-users.md#http-boundary). |
| `public_url` | required | 1–2048 characters: the `https://…/mcp` URL clients use (the path must be `/mcp`; plain HTTP only on loopback); the `Host` header must match its authority on every route except `/readyz`. |
| `operator_credential` | absent | `{ env = "VAR" }` for `POST /admin/refresh`; separate from OIDC access tokens and API keys. Absent, the route is not served (404); readers still pick up new versions with their version check. |
| `authorization_cache_subjects` | `64` | 1–1024 subjects whose admission is cached (LRU, 128 MiB accounted bound, charged by an estimate of each entry's size; workers share a cached admission instead of copying it). |
| `user_requests_per_minute` | `120` | At least 1, per [canonical user](architecture.md#terms); linked subjects share it. Each API key is counted as one user. |
| `group_requests_per_minute` | `120` | At least 1, per published group membership. |

## `[writer]`

| Key | Default | Meaning |
| --- | --- | --- |
| `purge_window` | `1d` | Deleted or revoked content leaves old versions and mirrors within it. Must exceed `version_grace`. |
| `version_grace` | `1h` | Minimum retention of a version after it stopped serving. Must exceed 300 s plus `reader.max_preparation`. |
| `max_envelope_age` | `3d` | Older pending envelopes are rejected, forcing a re-scan. At most 6 days: [Lance](architecture.md#terms) keeps unreferenced files for 7 days. |
| `document_max_bytes` | `8MiB` | Larger documents are quarantined. |
| `envelope_max_rows` | `50000` | At least 1. Provider content batches are further capped by `writer.commit.group_max_rows` and 256. |
| `envelope_max_bytes` | `256MiB` | Must exceed every document size limit. |
| `threads` | absent | At least 1. Bounds the CPU threads of each writer process: its async runtime, [Lance](architecture.md#terms)'s compute runtime, the parallel-iterator pool used by vector index builds, and the full-text build (`threads / 2` workers, at least 1, each holding up to 2 GiB while building). Absent uses every CPU. `fyrr writer sync` and `serve` run up to `--jobs` provider workers, each with the same bound. On a small host start with half its cores, for example `threads = 2` on 4 CPUs and 8 GiB. |
| `fts_partition_memory_mb` | absent (2048, Lance's default) | 1–1048576 MiB of full-text builder memory per worker at which the build closes a partition, and to which an index merge repacks. Smaller values build a full-text index of several [partitions](tuning.md#full-text-partitions) that `reader.fts_search_chunk` lets one query score concurrently; scores do not change. Not part of the publication's identity: a changed value applies to the next full-text build or merge. Each partition adds a token dictionary and document table to the index. |
| `worker_memory_max_bytes` | `2GiB` | Committed-memory ceiling (`RLIMIT_DATA`) of one [provider](architecture.md#terms) worker and, separately, of the Python host that worker starts: each process may commit this much, so a worker with its host may use up to twice it. |
| `credentials` | `{}` | Maps a provider's environment reference to a stored credential name (`fyrr writer credential`). |

### `[writer.commit]`

| Key | Default | Meaning |
| --- | --- | --- |
| `group_max_rows` | `1000000` | At least 1 row per publication group. |
| `group_max_bytes` | `4GiB` | Bytes per publication group; a group must hold a full envelope. |
| `coalescing_timer` | `1m` | `writer serve` closes a group after this long. |
| `lease_ttl` | `1m` | Namespace lease duration. |
| `merge_fts_segments` | `false` | Merge lexical deltas before publication. |

### `[writer.maintenance]`, `[writer.backpressure]`, `[writer.serve]`

| Key | Default | Meaning |
| --- | --- | --- |
| `maintenance.step_max_duration` | `5m` | Time budget of one lease-held maintenance step. |
| `backpressure.max_pending_content_envelopes` | `1000` | At least 1. Content sync pauses above it; permissions and members still publish. |
| `backpressure.max_pending_content_bytes` | `16GiB` | |
| `serve.committer` | `true` | Run the [committer](architecture.md#terms) role in `writer serve`; off keeps polling and leaves publication to batch `commit` jobs. |
| `serve.maintenance` | `true` | Run [`writer maintain`](cli.md) from `writer serve` when the served version has work for it; off leaves maintenance to you. Needs `serve.committer`. |
| `serve.admin` | absent | Writer administration listener: `listen`, `operator_token = { env = "VAR" }`, `tls_terminated` (required for a non-loopback listener). Needs `identity.encryption_key`. |
