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
| `concurrency` | `2` | 1–16 requests executing at once; the waiting queue has the same bound, then `busy`. |
| `filter_count_max_rows` | `1000000` | 1–10,000,000 authorized rows one `list_filters` call may inspect; over it, `busy`. |
| `mirror_path` | absent (`$XDG_CACHE_HOME/fyrr/<namespace>`, else `~/.cache/fyrr/<namespace>`) | Private local directory of immutable read-through files (mode 0700, owned by the process, not a symlink), created on first use. The default does not depend on the working directory; a relative `mirror_path` resolves against it. Without the setting and without an absolute `XDG_CACHE_HOME` or `HOME`, the reader refuses to start (`storage_configuration_invalid`). |
| `mirror_max_bytes` | `8589934592` | Disk limit of the mirror in bytes (8 GiB). An integer; unit suffixes are refused. |
| `version_check` | `interval` | `interval`, `off` or `per-request`. |
| `version_check_interval` | `30s` | Background scheduler caps the effective interval at 30 s. |
| `per_request_wait` | `2s` | `per-request` mode: wait this long for a newer version, then serve the old one flagged `stale_version`. |
| `max_preparation` | `15m` | Upper bound on preparing a new generation. |
| `max_response_bytes` | absent | 4096–16777216: budget for a serialized search result. Over it, whole hits are dropped from the end of the rank order (each query keeps at least four), then the lowest-ranked hits lose their excerpt or content; `truncated` and `omitted_bytes` report what was removed. |
| `http` | absent | Streamable HTTP MCP, see below. |

### `[reader.http]`

| Key | Default | Meaning |
| --- | --- | --- |
| `listen` | required | IP socket address (`IP:port`); Socket address. The listener speaks plain HTTP; see [HTTP boundary](http-users.md#http-boundary). |
| `public_url` | required | URL path must be `/mcp`; 1–2048 characters: the `https://…/mcp` URL clients use (plain HTTP only on loopback); the `Host` header must match its authority on every route except `/readyz`. |
| `operator_credential` | absent | `{ env = "VAR" }` for `POST /admin/refresh`; separate from OIDC access tokens. Absent, the route is not served (404); readers still pick up new versions through `version_check`. |
| `authorization_cache_subjects` | `64` | 1–1024 subjects whose admission is cached (LRU, 128 MiB accounted bound). |
| `user_requests_per_minute` | `120` | At least 1, per [canonical user](architecture.md#terms); linked subjects share it. |
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
| `worker_memory_max_bytes` | `2GiB` | Committed-memory ceiling of one [provider](architecture.md#terms) worker (`RLIMIT_DATA`). |
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
| `serve.admin` | absent | Writer administration listener: `listen`, `operator_token = { env = "VAR" }`, `tls_terminated` (required for a non-loopback listener). Needs `identity.encryption_key`. |
