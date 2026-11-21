# Changelog

All notable changes to fyrr are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The binaries follow
[Semantic Versioning](https://semver.org/); published contracts carry their
own `major.minor` versions (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## [Unreleased]

### Added

- The Jira connector indexes the custom fields listed in the new
  `scope.fields` (rich text, string or number values) as `field` sections
  labelled with the field ID, for example acceptance criteria. See
  [Jira](docs/connectors/jira.md#documents).
- The Jira connector records each issue's key as the document attribute
  `jira.key` and starts the summary section with it (`PROJ-123 Summary`).
  `search` with `attributes: [{"key": "jira.key", "values": ["PROJ-123"]}]`
  and `order: "updated_at"` returns that issue without a `query`. Every Jira
  issue gets a new revision and its summary is embedded again: at its next
  update, or at the connector's next full rescan (the first content sync at
  least 12 hours after the previous one) and the commit that follows. To do
  it at once, run `fyrr writer sync <source> --kind content --full`, then
  `fyrr writer commit`. The key counts toward `document_max_bytes`. See
  [Jira](docs/connectors/jira.md#documents).
- HTTP MCP clients can authenticate with operator-issued API keys without an
  identity provider. Give each role its own `[[identity.api_keys]]` entry and
  `readers` grants. To revoke a key, remove it from every writer
  configuration, republish identity, and refresh the readers. An HTTP
  reader without an authentication method is now refused when `reader serve`
  starts, and `config check` reports it as a warning, instead of being a
  configuration error for every command. See
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

- The `search` tool description tells agents to search a ticket's title and
  body as two `queries` for task context.
- The software factory guide and the Claude Code plugin tell an agent to
  start a task with one `search` whose two `queries` are the ticket's title
  and body. The agent takes its current ticket from the tracker; fyrr
  supplies the earlier tickets, pull requests and threads around it.
- To use connectors, install this release's `fyrr-connectors`
  wheel with your provider extras in a Python 3.11+ virtual environment (tested
  on 3.11, 3.12, 3.13 and 3.14). Set
  `VIRTUAL_ENV` to its absolute path and run `fyrr writer providers` to verify
  the installation. The full image includes the SDK and bundled providers.
  Commands that run no connector need no Python. Each sync kind runs in a
  fresh Python process; the worker memory limit applies to that process
  separately. What breaks for existing users:
  - The embedded interpreter is gone: the binary links no Python
    (`PYO3_PYTHON`, `libpython`).
  - SDK 0.1.2 or newer is needed; 0.1.0 and 0.1.1 do not work.
  - A provider that aborts fails its source with `the provider failed`.
  - `PATH`, `VIRTUAL_ENV` and names starting with `PYTHON` or `LD_` are
    reserved names for stored credentials a source references;
    `fyrr config check` refuses them.
  See [how a provider runs](docs/connectors/authoring.md#how-a-provider-runs).
- The Jira connector keeps link targets (`text (url)`, smart cards as their
  URL). Under `permissions = "fixed"` it adds a `links` section with the
  parent and issue links, keys only (`blocks: PROJ-140`). Affected issues are
  republished at their next update, or at the next full rescan and commit;
  `sync --kind content --full` followed by `writer commit` does it at once. See
  [Jira](docs/connectors/jira.md#documents).
- GitHub and Jira comments start with a line naming their author
  (`ada (member):` with GitHub's `author_association`; `Ada Lovelace:`).
  Conversations and issues with comments that still fit `document_max_bytes`
  get a new revision at their next update or rotation (12-hour target) and
  the commit after it;
  `writer sync <source> --kind content --full`, then `writer commit`, does it at once.
- **Revocation latency.** A request no longer reads the serving head from
  storage for its authorization checks. It checks its grants against the
  head the reader's latest version check read
  (`reader.version_check_interval`, 30 s), and reads the head itself only
  when that read is older than two intervals. A warm query then makes no
  storage request instead of two. A published revocation, membership
  change, retired source or deleted namespace now reaches a running reader
  with its next version check, at the latest two intervals (60 s by
  default) after it was published, rather than with its next request.
  `SIGHUP` and `POST /admin/refresh` start a check at once.
- **Breaking: `reader.version_check` and `reader.per_request_wait` are
  removed** (configuration schema 6.0); a configuration that sets either is
  refused. `reader.version_check_interval` is the one setting. The reader
  always checks on that interval, and a reader that did not run for a
  second or more, such as an environment frozen between requests, runs a
  due check before it answers: requests wait up to 2 s for it. This
  replaces the `per-request` mode, which read the serving head for every
  request; such a reader now reads it at most once per interval and an
  answer can lag a publication by up to one interval. The `off` mode has no
  replacement. `identity.validity_deadline` must now cover the interval in
  every configuration. `metadata.stale_version` is now set in every reader whenever
  the answer comes from an older version than the head the reader last
  read.
- The reader's mirror downloads a data file of up to 2 MiB whole on its
  first read instead of in pages. Opening a reader reads the footer, the
  column metadata and the first page of every data file, two or three
  dependent requests each, and warming then downloaded the file anyway.
  Opening a 100,000-row namespace on an empty mirror made 412 storage
  requests instead of 1,044 and took 0.93 s instead of 1.32 s with 25 ms
  per request, and its first search made 7 requests instead of 726. Open
  transfers the small files at once (313 MB instead of 44 MB there), which
  warming would have downloaded moments later; over a slow link the first
  answer can therefore take longer. A small file that does not fit the
  mirror is still read in pages.
- An idle `writer serve` no longer reads the whole publication history at
  every committer tick. The committer remembers the evidence of the
  publications it has read and reads only newer ones, and it lists and
  reads the retained envelopes only when the history records a rejection
  or the served manifest has a cursor rewind. With five publications, eight idle ticks made 152 storage
  requests instead of 472, and the manifest reads per tick no longer grow
  with the history.
- Building a subject's admission no longer reads the `source_id` column of
  every data file when no source is hidden (expired or retired); the
  predicate's scalar indexes give the rows. Opening a 100,000-row namespace
  on an empty mirror made 19 storage requests instead of 1,044 and took
  0.42 s instead of 1.32 s with 25 ms per request; its first search took
  446 ms instead of 513 ms. A namespace with fixed grants still reads
  `source_id`, which has no index.
- The reader no longer asks storage for a version manifest its publication
  does not list. Lance looks for a version under its other manifest name
  each time it opens the dataset, once per worker of every generation; the
  mirror now answers that miss from the publication's file list. Opening a
  100,000-row namespace on an empty mirror made 35 fewer storage requests.
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
- The GitHub connector indexes an issue or PR title as the first line of its
  first section, so title-only words are searchable. Conversations with a body
  get a new revision and are re-embedded as they refresh; run
  `fyrr writer sync github --kind content --full` to do it at once. The title
  counts toward `document_max_bytes`. See
  [GitHub documents](docs/connectors/github.md#documents).
- The Notion and Google Drive connectors index titles. A standalone Notion
  page's title and a Google Doc's file name are the first line of the first
  section; a Sheet's or Slides deck's file name is a section of its own
  before the first one. A word that occurs only in a title is found by
  full-text search. The title counts toward `document_max_bytes`: a page or
  file that was just under the limit is quarantined when it is read again
  and keeps its last good revision until the limit is raised.
- Existing pages and files get their title when next edited; `fyrr writer sync
  <source> --kind content --full` followed by a commit republishes all that
  fit the limit. Going back to an earlier release on the same index is not
  supported for Google Drive: its revision order now includes the rendering,
  so the earlier release would skip edits and deletions of published files.

### Fixed

- A GitHub issue, PR or file the connector contract rejects no longer stalls
  the content sync: display values are fitted (control characters to spaces,
  cut to 1,024 bytes, at most 32 labels), anything still rejected is
  quarantined, and the sync continues. Valid documents keep their revision.
- `fyrr reader search` accepts a query that starts with `-`, such as an issue
  title; put `--` before a query that is exactly an option name.

### Security

- A reader with `identity.directory.provider = "none"` (the Lambda example
  omits it) no longer skips the directory freshness check when the serving
  head carries a timestamp. Reading a namespace whose writer pulls a
  directory, it now answers `authorization_unavailable` once the pulls have
  stopped for longer than `identity.validity_deadline`. A deployment that
  moved to manual imports under `provider = "none"` is served again by one
  `writer directory --snapshot` import, which withdraws the timestamp; the
  snapshot must not be older than the last pull, or it is refused as stale.
  Lambda readers should use the writer host's `validity_deadline`.
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

- **A late head read is not used for authorization.** When a check waited for
  a shared head read that storage answered after more than two version-check
  intervals, the check was served with the head as it was when the read
  began, so a revocation published meanwhile could be missed for longer than
  the documented two intervals. Such a read now fails the checks that waited
  for it with `authorization_unavailable`, and the next check reads again.
  If the reader answers so persistently, storage takes longer than two
  intervals to return the head: raise `reader.version_check_interval`.
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
