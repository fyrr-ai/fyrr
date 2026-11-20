# Changelog

All notable changes to fyrr are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The binaries follow
[Semantic Versioning](https://semver.org/); published contracts carry their
own `major.minor` versions (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## [Unreleased]

### Added

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
- The GitHub connector indexes an issue or PR title as the first line of its
  first section, so title-only words are searchable. Conversations with a body
  get a new revision and are re-embedded as they refresh; run
  `fyrr writer sync github --kind content --full` to do it at once. The title
  counts toward `document_max_bytes`. See
  [GitHub documents](docs/connectors/github.md#documents).

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
