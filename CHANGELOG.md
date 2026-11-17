# Changelog

All notable changes to fyrr are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The binaries follow
[Semantic Versioning](https://semver.org/); published contracts carry their
own `major.minor` versions (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## [Unreleased]

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

- Local-file extraction replaces NUL and other non-whitespace C0 controls with
  spaces instead of quarantining otherwise valid UTF-8 text. Raw-byte source
  revisions remain unchanged.

### Added

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
