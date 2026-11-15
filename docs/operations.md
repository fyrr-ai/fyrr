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
  identity generation and age, per-source permission freshness and a
  `warnings` list with fixed codes (for example
  `physical_purge_deadline_at_risk`). It never names documents or users.
- `fyrr reader check` exits nonzero when the publication cannot be admitted;
  the HTTP server answers `/readyz`. A reader whose request scheduler fails
  stops serving (HTTP 503 from `/readyz`, or exit on stdio) and must be
  restarted.
- Every successful MCP tool response carries stage timings and S3 request counts in
  `metadata.telemetry`, and `metadata.hidden_sources` names sources hidden
  for stale permissions.

## Reader refresh

Readers check the serving head every `reader.version_check_interval` (30 s),
or at once on `SIGHUP` or HTTP `POST /admin/refresh`. A new version is
prepared in the background and activated between requests; a failed
preparation keeps the old version and retries. A pinned version older than
`writer.purge_window` stops serving. When the namespace at the reader's
storage URI is deleted, a running reader refuses every request
(`storage_unavailable`). When it is created again there, the reader refuses
with `authorization_unavailable` until it has prepared the new namespace's
first publication, then switches to it without a restart, whatever
`reader.version_check` says; it never serves the old namespace's rows.
Memory and disk are bounded by
`reader.memory_max_bytes` and `reader.mirror_max_bytes`.

## Maintenance and retention

Published versions are kept for `writer.purge_window` (one day) and at least
`writer.version_grace` (one hour) after they stop serving. `fyrr writer
maintain` runs in lease-held steps: it publishes pending permission changes,
removes unreadable rows, compacts fragments, updates indexes and deletes
retired versions and applied envelopes. Run it often enough that deleted
content physically leaves namespace storage within the purge window; until then it remains
in retained old versions.

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

Retirement is final: every reader denies the source on its next request and
the committer rejects its envelopes. Purge also removes its rows, envelopes
and cursors; each step is idempotent, so an interrupted purge is run again.

## Upgrades

Contracts carry `major.minor` versions; a new minor reads older data. These
changes need a fresh namespace instead: a new contract major, and any change
of the embedding `profile`, `model`, `dimensions`, prefixes, `roles` or
`send_dimensions` (a sync, commit, maintain, verify or reader against the
old namespace fails with `embedding_profile_mismatch`).
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
cleanup: after preparing a verified export,
`fyrr --config fyrr-old.toml writer namespace delete --export EXPORT_DIR`
unpublishes the old namespace; its objects stay in storage until you remove
that namespace's objects ([CLI](cli.md)).

Analyzer (`namespace.fts_*`), composition, retrieval and
`[namespace.rerank]` changes stay in place: the next commit rebuilds the lexical index or publishes the new
retrieval policy. Readers must run the same settings; a reader with the old
ones refuses the new publication with `incompatible_index`.

If a commit reports `head_cas_failed`, the publication may still have become
visible. `fyrr writer commit --receipt ENVELOPE_ID` checks without writing,
and a normal commit retry resumes the pending work.
