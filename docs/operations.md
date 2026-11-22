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
- Every successful MCP tool response carries stage timings and S3 request counts in
  `metadata.telemetry`.

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
`reader.memory_max_bytes` and `reader.mirror_max_bytes`.

## Maintenance and retention

Published versions are kept for `writer.purge_window` (one day) and at least
`writer.version_grace` (one hour) after they stop serving. `fyrr writer
maintain` runs in lease-held steps: it publishes pending permission changes,
removes unreadable rows, compacts fragments, updates indexes and deletes
retired versions and applied envelopes. Run it often enough that deleted
content physically leaves namespace storage within the purge window; until then it remains
in retained old versions. Physical purge removes one revoked document root
per bounded step and publishes that step before continuing. A large revoke
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
finds the publication that applied an envelope.

Both publish a new version under the namespace lease, with the **current**
policy, identity, permissions and tombstones: a restore never revives a
revoked grant or a retired source's rows. `fyrr writer verify` scans the
whole pinned publication and needs only namespace storage.

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
rejection contracts are `5.0`, and content manifests and serving heads are
`4.0`, and policy artifacts are `3.0`. Embedding settings are required even
for the default BGE-M3 model. Republish older namespaces before using this
build.

Configuration schema `4.0` changed the default retrieval policy to
`dense_weight = 0.5` and `lexical_candidates = 200`. A namespace created from
a file that omits them gets those values. An existing namespace keeps the
policy it was created with; its readers and writers use it whatever their
files say.

Configuration schema `6.0` removes `reader.version_check`,
`reader.per_request_wait` and `identity.validity_deadline`; delete all three
keys. The reader checks for a new version every `reader.version_check_interval`
and catches up by itself after it did not run. The reader has no clock: results
change only with a new publication. The serving head (`4.0`), the prepared envelope (`5.0`) and the identity
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
command checks each saved object against storage before unpublishing:

`fyrr --config fyrr-old.toml writer namespace delete --export EXPORT_DIR`
unpublishes the old namespace; its objects stay in storage until you remove
that namespace's objects ([CLI](cli.md)).

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
