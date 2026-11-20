# Changelog

All notable changes to fyrr are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The binaries follow
[Semantic Versioning](https://semver.org/); published contracts carry their
own `major.minor` versions (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## [Unreleased]

### Added

- API keys for the HTTP reader (`[[identity.api_keys]]`): static bearer
  secrets with operator-declared grants, either reader tokens (`readers`, as
  `stdio_readers`) or a canonical directory user (`user`), with an optional
  `expires` and `requests_per_minute`. They let unattended agents and small
  teams use `[reader.http]` without an identity provider: `reader serve`
  with `[reader.http]` now needs `identity.oidc`, at least one API key or
  both to start (the writer does not, so removing the last key still
  revokes it). The writer publishes only a keyed hash of each secret; the
  identity artifact moves to 1.2 (an optional `expires` on subject
  bindings, enforced by the reader before a response leaves) and the
  configuration schema stays 5.1. A key added, removed or rebound takes
  effect on a running reader like any other revocation: with its next
  version check, at the latest two intervals (60 s by default) after the
  identity is published; `SIGHUP` and `POST /admin/refresh` apply it at once.
  An `expires` is checked against the reader's clock and takes effect at
  once. See
  [HTTP login and users](docs/http-users.md#api-keys).
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
  manifest (publication contract 3.1) and fixed once a namespace is
  published; see [tuning](docs/tuning.md).
- `namespace.dense_partition_rows` (32 to 4096, default 550) sets the target
  rows per partition of the vector index, recorded and fixed like
  `dense_rq_bits`.
- `writer.fts_partition_memory_mb` and `reader.fts_search_chunk` (both absent
  by default) build a full-text index of several partitions and let one query
  score them concurrently. Scores and rankings do not change; see
  [tuning](docs/tuning.md#full-text-partitions).

### Changed

- **Revocation latency.** A request no longer reads the serving head from
  storage for its authorization checks. It checks its grants against the
  head the reader's latest version check read
  (`reader.version_check_interval`, 30 s), and reads the head itself only
  when that read is older than two intervals. A warm query then makes no
  storage request instead of two. A published revocation, membership
  change, retired source or deleted namespace now reaches a running reader
  with its next version check, at the latest two intervals (60 s by
  default) after it was published, rather than with its next request.
  `SIGHUP` and `POST /admin/refresh` start a check at once. In
  `per-request` mode `metadata.stale_version` compares with that head too.
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

### Security

- A reader no longer skips the published directory freshness check because
  its own `identity.directory.provider` is `none` (the Lambda example omits
  it). When the serving head carries a directory freshness timestamp, every
  reader now refuses the namespace once that read is older than
  `identity.validity_deadline`: before this, such a reader served OIDC
  subjects, the stdio subject and any other directory-bound subject from an
  expired directory snapshot. For an existing deployment this means: a reader
  with `provider = "none"` that reads a namespace whose writer pulls a
  directory now gets `authorization_unavailable` once the pulls have stopped
  for longer than `validity_deadline`, until the directory is pulled again.
  A head without a timestamp, such as a snapshot imported by hand with
  `writer directory --snapshot`, is served as before by a reader with
  `provider = "none"`. A deployment that moved from `keycloak`, `scim` or
  `snapshot` to manual imports under `provider = "none"` is served again by
  running one `writer directory --snapshot` import: such an import attests no
  directory freshness, so it withdraws the timestamp the earlier provider
  left. With a provider configured, an import still keeps the timestamp the
  provider attested.
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
