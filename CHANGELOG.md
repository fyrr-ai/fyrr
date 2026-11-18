# Changelog

All notable changes to fyrr are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The binaries follow
[Semantic Versioning](https://semver.org/); published contracts carry their
own `major.minor` versions (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## [Unreleased]

### Changed

- Update Rust dependencies, including Arrow 58.4.1, while retaining Lance
  14.0.3 and its Arrow 58, DataFusion 54 and PyO3 0.28 interfaces.
- Update the connector SDK and example provider's development Ruff to 0.17.5.
- Update the website to React 19.3.1, TypeScript 7.0.2 and Node types
  26.6.6; remove the compiler's obsolete `baseUrl` option.

### Fixed

- Include the reader-memory investigation in the website's documentation
  navigation so the static export can render every document.

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

[Unreleased]: https://github.com/fyrr-ai/fyrr/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/fyrr-ai/fyrr/releases/tag/v0.1.0
