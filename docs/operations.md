# Operations

> **In short**
>
> - How to watch a running deployment: `fyrr writer status`,
>   `fyrr reader check`, `/readyz` and the telemetry in every successful tool
>   response.
> - How readers pick up new versions, what `fyrr writer maintain` does, and
>   how to restore, roll back, retire or purge.
> - Which changes need a fresh namespace, and how to switch to one.

Setting a deployment up (topology, images, scheduling) is in
[deployment](deployment.md).

## Monitoring

- `fyrr writer status` prints the serving `content_sequence`, envelope and
  pending-envelope counts, quarantined entries per reason, the directory's
  identity generation, the age of the last permission and member read per
  source (`source_reads`; null for a read that never completed) and a `warnings` list with fixed codes (for example
  `physical_purge_deadline_at_risk`). It never names documents or users.
- `fyrr reader check` exits nonzero when the publication cannot be admitted;
  the HTTP server answers `/readyz`. A reader whose request scheduler fails
  stops serving (HTTP 503 from `/readyz`, or exit on stdio) and must be
  restarted.
- [Metrics](#metrics) are served in the Prometheus text format when a
  `metrics_listen` address is configured.
- [Vector recall](#vector-recall) says how closely the vector index agrees with
  an exact search: measured by the reader in the background, and on request.
- Every successful MCP tool response carries stage timings and S3 request counts in
  `metadata.telemetry`.

## Metrics

`reader.http.metrics_listen` (reader) and `writer.serve.metrics_listen`
(`fyrr writer serve`) open an extra listener that serves `GET /metrics` in the
Prometheus text format. Without them there is no listener. It has no
authentication, so bind it to a loopback or private address. No label or
value carries a namespace name, an identity, a query or document text, and
every label has a few fixed values.

| Process | Metric | Meaning |
| --- | --- | --- |
| reader | `fyrr_reader_http_request_duration_seconds{route,code}` | Histogram of HTTP requests by route (`mcp`, `search`, `fetch`, `aggregate`, `recall`, `report`, `list`, `readyz`, `refresh`, `metadata`, `other`) and HTTP status code; its `_count` is the request count. |
| reader | `fyrr_reader_queue_wait_seconds` | Histogram of the time from the arrival of a request to the start of its execution. |
| reader | `fyrr_reader_mirror_hits_total` | Reads answered from the local mirror. |
| reader | `fyrr_reader_object_store_requests_total`, `fyrr_reader_object_store_bytes_total` | Reads the mirror sent to the object store, and their bytes. |
| reader | `fyrr_reader_mirror_bytes` | Bytes of the local mirrors, as last measured. |
| reader | `fyrr_reader_mirror_evictions_total` | Mirror files removed to make room. |
| reader | `fyrr_reader_open_namespaces` | Namespaces held open: the configured one and those opened by request path. |
| reader | `fyrr_reader_recall_samples_total{outcome}` | Background recall samples: `completed`, or the reason one was skipped. |
| writer | `fyrr_writer_publications_total` | Content publications: each new manifest version, from a commit group or a maintenance run. A change of the head without one, such as a rejection before the first version, is not counted. |
| writer | `fyrr_writer_commit_rounds_total{outcome}` | Commit rounds: `published` (at least one group), `idle`, `busy` or `failed`. |
| writer | `fyrr_writer_commit_duration_seconds` | Histogram of the duration of the rounds that published; a round may publish several groups. |
| writer | `fyrr_writer_pending_envelopes` | Envelopes waiting, summed over the namespaces the writer committed, each as of its last read; an idle round does not read them again. |
| writer | `fyrr_writer_maintenance_steps_total{step}` | Maintenance steps done: `compaction` or `index_merge`. |
| writer | `fyrr_writer_rejected_envelopes_total` | Envelopes the committer rejected. |

Histogram buckets are 5 ms to 120 s. The metrics count since the process
started.

## Vector recall

The vector index finds the nearest rows approximately. Its recall is the share
of the rows an exact search over the same rows would return that the index
returned too, at the 10 and at the 75 nearest (`recall_at_10`, `recall_at_75`).
The reader measures it two ways, both over the rows the caller may read, under
the same filter and with the namespace's distance.

**In the background.** The reader picks every 100th successful dense search
and, when it has no other work, replays that query as an exact search, one at a time, within
2 seconds. A foreground request cancels it. Each pick prints one `telemetry`
event to stderr with `recall_successful`, `recall_selected`, `recall_completed`,
`recall_coverage` (completed over successful searches) and, for a completed
sample, `recall_at_10`, `recall_at_75` and `recall_admitted_rows` (the rows the
exact search scanned); a skipped one has `recall_skip_<reason>` instead.
`fyrr_reader_recall_samples_total{outcome}` counts them by `completed` or the
reason. The sampler needs dense searches: with the default
`embedding.reader_live = false` the reader embeds no query text, so only
searches by a supplied `vector` or by a cached embedding are counted, and a
reader without them takes no sample. The request below does not wait for them.

**On request.** `POST /v1/namespaces/{namespace}/recall` measures a number of
stored vectors at once, whatever the traffic. It needs the `admin` and the `read` role of an
[API key](identity.md#identityapi_keys), and the key reads only the rows its
`readers` bind it to: they are the rows it samples and searches. See
[Recall](http-api.md#recall) for the body and the answer. The request holds one
query slot, like a query, and its `deadline_ms` bounds the whole run: each
sampled vector costs one index search and one exact scan of every row the key
may read and the filter selects, so size `count` and the deadline to the
namespace. The answer holds counts and recall, never a document's key or text.

## Reader refresh

Readers check the serving head every `reader.version_check_interval` (30 s),
or on `SIGHUP`, or HTTP `POST /admin/refresh` when
`reader.http.operator_credential` is set (rate-limited to once per five seconds). A new version is
prepared in the background and activated between requests; a failed
preparation keeps the old version and retries. A pinned version older than
`writer.purge_window` stops serving. When the namespace at the reader's
storage URI is deleted, a running reader refuses every request
(`storage_unavailable`) from its next version check on. When it is created again there, the reader refuses
with `authorization_unavailable` until it has prepared the new namespace's
first publication, then switches to it without a restart; it never serves the old namespace's rows.
Memory and disk are bounded by
`reader.memory_max_bytes` and `reader.mirror_max_bytes`, for all open
namespaces together. A reader of an earlier release kept the mirrors of namespaces opened by
request path in `<reader.mirror_path>.paths`; after stopping those readers,
remove that directory (`rm -rf <reader.mirror_path>.paths`), because this
release neither reads nor sweeps it.

## Maintenance and retention

Published versions are kept for `writer.purge_window` (one day) and at least
`writer.version_grace` (one hour) after they stop serving. `fyrr writer
maintain` runs in lease-held steps: it publishes pending permission changes,
removes unreadable rows, compacts fragments, updates indexes and deletes
retired versions, applied envelopes and the publication manifests and
checkpoints that only older versions named. `fyrr writer serve` runs it when
the served version has a vector index with more than 64 segments and two neighbors that fit one merge step (see `writer.maintenance.step_max_duration`), or, at most once
per `writer.version_grace`, has fragments with deletions or has gone
`writer.purge_window` minus `writer.version_grace` (or `writer.max_envelope_age`,
if shorter) without it; `writer.serve.maintenance =
false` turns that off. That cadence does not bound how long deleted content
stays in retained old versions: run `writer maintain` often enough that it
physically leaves namespace storage within the purge window. Only `writer maintain`
re-partitions the vector index of a grown namespace (see
[`dense_partition_rows`](tuning.md)); `writer serve` merges segments only,
because splitting a partition trains in memory for as long as a first build of
it and cannot be interrupted. Physical purge
removes one revoked document root per bounded step and publishes that step
before continuing. A large revoke
therefore needs multiple publications; permission changes are drained
between steps.

### Working space

Maintenance, restore and rollback use disposable SQLite indexes. Provide a
writable `TMPDIR` or writable mirror parent; there is no working-directory
fallback. History work grows with namespace lifetime. Scratch or disk failures
report `restore_working_space_exhausted`; bounded active summaries report
`restore_history_summary_limit`. Cleanup preserves pending work, unresolved
rejections, latest complete reads and receipts. Repeat commands for further
cleanup; reduce `writer.max_envelope_age` or sync frequency if protected
work reaches the summary bound.

## Restore and rollback

```sh
fyrr writer restore PUBLICATION_ID      # new version from that point
fyrr writer rollback                    # the preceding retained publication
```

`fyrr reader check` prints the `publication_id` serving now, and every
`fyrr writer commit` prints the latest one; keep those to restore an earlier
point within the retention window. `fyrr writer commit --receipt ENVELOPE_ID`
finds the publication that applied an envelope; for an envelope of a publication
older than the retention window it only says that the envelope was applied.

Both publish a new version under the namespace lease, with the **current**
policy, identity, permissions and tombstones: a restore never revives a
revoked grant or a retired source's rows. `fyrr writer verify` scans the
whole pinned publication and needs only namespace storage.

## Backup and copy

```sh
fyrr --config prod.toml writer namespace copy --to-config backup.toml
```

copies the namespace of `prod.toml`, as the one version its head serves when the
command starts, into the `[storage]` of `backup.toml`: another bucket or region.
The source is only read and can be written meanwhile. The copy has the same name:
the keyed hashes in its rows are made with it, so a copy under another name, or
in the same storage, is not supported yet. `identity.hashing_key` must be the same
in both files; another key fails with `hashing_key_mismatch` and writes nothing.
The copy is a namespace of its own and writable: point a reader or writer at it
with the storage of `backup.toml`, and it finds the same documents with the same
access. It holds no history, so `writer restore` reaches nothing older than the
copied version, and its connector sources read again from their start. A namespace
whose identity holds snapshots (`permissions = "sync"` sources or a directory)
cannot be copied (`copy_inputs_unsupported`). The head is written last, so a copy
that was interrupted is no namespace at all; run the same command again and it
removes what is left and starts over. A copy that finished fails with
`namespace_exists` when run again; run one copy to a destination at a time.

A backup is a scheduled copy into a storage of its own, and its retention is
deleting the old storages. Give `backup.toml` a `uri` ending in `DAY` and the
namespace name of `prod.toml`:

```sh
# every night, from a host that has both files and the key
name=docs                      # the namespace of prod.toml
day=$(date -u +%Y%m%d)
sed "s/DAY/$day/" backup.toml > "backup-$day.toml"
if fyrr --config prod.toml writer namespace copy --to-config "backup-$day.toml"; then
  # Only after a copy succeeded, keep the seven newest completed copies: those
  # that a reader can open. A night that failed is neither counted nor pruned.
  for c in backup-*.toml; do
    fyrr --config "$c" reader check >/dev/null 2>&1 && echo "$c"
  done | sort | head -n -7 | while read -r old; do
    fyrr --config "$old" writer namespace delete --delete-data "$name" && rm "$old"
  done
fi
```

Do not give the bucket a lifecycle rule that expires objects by age: it would
remove files a namespace still names. To restore, copy a backup back with the
two files exchanged into a new storage (a new `uri`, as the nightly copies have),
or delete the namespace of the storage first with `writer namespace delete`:
a copy into a storage that holds the namespace fails with `namespace_exists`.
Check the result with `reader check` and `writer verify`, and
switch the readers and writers to it. A version stays for `writer.version_grace`
after a newer one serves, so a copy that takes longer than that while the source
is written and maintained can fail with `copy_incomplete` or
`storage_unavailable`; run it again and it copies the then-current version. Every
object is read once and written once, so a copy costs its requests and, across
regions, its transfer.

Measured throughput: a namespace of 1,536 rows (4.3 MB of files, vectors of
1,024 dimensions) copied in 0.28 to 0.31 s over four runs, 14 to 15 MB/s, by the
native binary on local `file://` storage, source and destination on one XFS volume
of a shared build host. The time includes the command's fixed costs, which dominate
a copy this small, so it is a floor for local storage and says nothing about S3,
where the object count and the round trip decide; measure a copy of your own
namespace with the `bytes` and `seconds` it prints.

## Retiring and purging sources

```sh
fyrr writer source retire SOURCE_ID
fyrr writer source purge SOURCE_ID            # prints the plan
fyrr writer source purge SOURCE_ID --confirm  # retires and deletes
```

Retirement is final: every reader denies the source from its next version
check on (`reader.version_check_interval`) and
the [committer](architecture.md#terms) rejects its envelopes. Purge also removes its rows, envelopes
and cursors; each step is idempotent, so an interrupted purge is run again.

## Upgrades

The current configuration schema is `6.0`, the MCP contract is `4.0`, the prepared-envelope and
rejection contracts are `6.0`, content manifests are `5.0`, serving heads are
`4.1`, and policy artifacts are `4.0`. Each contract has one major; this build
refuses every other. Embedding settings are required even
for the default BGE-M3 model. Republish older namespaces before using this
build.

v0.2.0 refuses every local storage root that v0.1.x or an earlier build wrote,
with `storage_root_old`: the root holds `.fyrr-storage.lock` or
`.fyrr-storage.stage`, which this build never creates. Recreate its namespaces
in a new root with v0.2.0. Remove the two files only once no older writer runs.
A namespace that v0.1.x or an earlier build wrote is refused with
`namespace_layout_old`, in any root, S3 included: one of its stored contracts (the serving
head, the policy, the manifest) is of an older major than v0.2.0 writes. Recreate the namespace
under a new name with v0.2.0 (`writer namespace create`), sync its sources again
and write again what was written through the API. Or, after stopping every
writer of the namespace and keeping a recoverable copy, delete its
`namespaces/<name>/` prefix and recreate it under the same name. The command line prints what to do under
`error: <code>`.

Configuration schema `4.0` changed the default retrieval policy to
`dense_weight = 0.5` and `lexical_candidates = 200`. A namespace created from
a file that omits them gets those values. An existing namespace keeps the
policy it was created with; its readers and writers use it whatever their
files say.

Configuration schema `6.0` removes `reader.version_check`,
`reader.per_request_wait` and `identity.validity_deadline`; delete all three
keys. The reader checks for a new version every `reader.version_check_interval`
and catches up by itself after it did not run. The reader has no clock: results
change only with a new publication. The serving head (`4.1`), the prepared envelope (`6.0`) and the identity
artifact (`2.0`) carry no
freshness instants and MCP `4.0` has no `metadata.hidden_sources`. Republish
older namespaces.

Contracts carry `major.minor` versions; a new minor reads older data. These
changes need a fresh namespace instead: a new contract major, and any change
of the settings a namespace is created with: the embedding `model`,
`dimensions`, prefixes, `roles` and `send_dimensions`, the analyzer
(`namespace.fts_*`), the vector index, composition, retrieval and the reranker's `model`,
`candidates`, `weight` and `text_chars`. `writer namespace create` records them from the
configuration file. A process whose file differs still serves the namespace
with the settings it was created with, and creating the namespace again with
other settings fails with `namespace_settings_fixed`.
Publish the new namespace next to the old one, verify it, then switch
readers:

```sh
# fyrr-next.toml: the old file with a new namespace.name and the new settings
fyrr --config fyrr-next.toml writer namespace create
fyrr --config fyrr-next.toml writer sync --directory   # Keycloak or snapshot directory only
fyrr --config fyrr-next.toml writer sync --full
fyrr --config fyrr-next.toml writer commit
fyrr --config fyrr-next.toml writer verify
fyrr --config fyrr-next.toml reader check
```

With a SCIM directory, provision users and groups into the new namespace
through `writer serve` with its administration listener before
`reader check`.

Then point writers and readers at the new file. Keep the old file for
cleanup: first copy every namespace object byte-for-byte into an export
directory, preserving paths relative to the namespace root. The delete
command checks each saved object against storage before it deletes anything:

`fyrr --config fyrr-old.toml writer namespace delete --export EXPORT_DIR`
unpublishes the old namespace and then deletes every object under its prefix.
Without an export, `--delete-data NAME` with the namespace name does the same.
Deletion cannot be undone. Run at most one `namespace create` or `delete` for a
namespace at a time; concurrent administration commands are not supported.
Stop every writer of the namespace first: the
command waits `writer.commit.lease_ttl` for leases to expire, twice. It removes
current objects only, so a versioned S3 bucket needs a lifecycle rule for
noncurrent versions. If it stops part way, run the same command again
([CLI](cli.md)).

On S3, deletion first probes whether the server honors `If-Match` on DELETE.
If it does not, `conditional_delete_unsupported` leaves the serving head
in place. Use a backend supporting conditional deletion before retrying.

If a commit reports `head_cas_failed`, the publication may still have become
visible. `fyrr writer commit --receipt ENVELOPE_ID` checks without writing,
and a normal commit retry resumes the pending work.

If a sync reports `sync_lease_lost`, it can no longer prove ownership of its
source cursor lease, so the run stops using that lease rather than advancing
the cursor without ownership.


## Object-store diagnostics

Set `FYRR_OBJECT_STORE_DIAGNOSTICS` to an absolute JSONL file path before starting
any reader or writer process. It is off by default. The parent directory must
already exist; use a private operator-owned directory. A new file is mode 0600.
Existing files must be regular, owned by the process user, have no group/other
permissions, and have one hard link; symlinks are refused. Invalid destinations
emit only `object_store_diagnostics=unavailable` and leave storage behavior
unchanged. Workers inherit the destination, so source sync diagnostics survive
the suppression of provider stdout/stderr. Logs append across processes and
restarts. Arrange rotation and retention outside fyrr; restart processes after
renaming the active log.

```sh
install -d -m 700 /var/log/fyrr
FYRR_OBJECT_STORE_DIAGNOSTICS=/var/log/fyrr/objects.jsonl fyrr writer sync --directory
```

Each completed boundary operation records `method`, `key_sha256`,
`http_status`, `error_code`, `retry_count` and `latency_us`. Only the SHA-256 of
the object key or operation prefix is retained: no full keys or raw prefixes,
URLs, query strings, headers, bodies, credentials, backend error strings or
provider messages. Error codes are a fixed vocabulary (`not_found`,
`permission_denied`, `precondition`, `transport_timeout`, `storage_error`, etc.),
not arbitrary service error text. The hash supports correlating repeated access;
it is not encryption and may allow guessing predictable keys.

Control-store S3 operations report the actual last observed HTTP status and
repeated HTTP request count, including retries that eventually succeed. List
operations count repeated requests per page, not page count as retries. Request
identity accounting is bounded to 1024 distinct requests per operation; beyond
that, retry count is null.
Local operations have `http_status: null` and `retry_count: 0`. Native Lance
stores own their transport and do not expose retry/status facts at the wrapper;
those fields remain `null`, including successful operations. `DELETE_STREAM`
preserves backend batching and logs the hashed namespace/native-root prefix;
its retry count is null for remote stores because repeated bulk requests cannot
be separated from retries without inspecting bodies. `null` means
unavailable, never zero retries or an inferred HTTP success code. A transport
failure before a response also has a null status.

Streamed GETs and lists log completion, errors, or `cancelled` when dropped
early, including body consumption in latency and retry accounting. They remain
streaming without buffering. HEAD and file-handle payloads complete when the
underlying ObjectStore method returns. Multipart creation, parts, completion and abort are separate
operations. Latency covers the named boundary phase, including retries, rather
than an invented per-attempt duration. Diagnostics do not enable additional
retries. Failure to write the diagnostic destination does not change a storage
operation's result.
