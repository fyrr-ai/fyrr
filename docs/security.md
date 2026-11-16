# Security model

> **In short**
>
> - What fyrr trusts and what a deployment must protect itself: provider code,
>   the embeddings and reranker services, the bucket and the reader mirror.
> - How reader tokens, subjects, permission modes and freshness deadlines
>   decide who may read what, and when a revocation reaches readers.
> - How secrets, stored credentials and provider workers are handled.

How fyrr decides who may read what, and what a deployment must protect
itself. To report a vulnerability, see [SECURITY.md](../SECURITY.md).
Serving many users over HTTP is in [HTTP login and users](http-users.md).

## Trust boundaries

- **Provider code is trusted.** Connector providers are Python packages that
  run with the writer's user and credentials. fyrr isolates them in worker
  processes for robustness (below), not as a sandbox. Install only providers
  you would run as that user.
- **Text leaves the deployment for embedding and reranking.** The writer
  sends document text to the configured embeddings endpoint; readers with
  `embedding.reader_live = true` send query text after authorization; an
  optional reranker (`[namespace.rerank]`) receives the authorized
  candidate documents and the query. Choose these services as you would any
  processor of the documents.
- **S3 and the reader mirror hold readable content.** Document text,
  metadata and vectors are stored in Lance files under `storage.uri`, and
  readers cache them in `reader.mirror_path`. fyrr does not encrypt them;
  protect the bucket and the mirror host with your own access control and
  storage encryption. Anyone who can read the bucket can read every
  document.
- **Identity values are hashed, not encrypted.** Reader tokens and subject
  keys are stored as HMAC-SHA256 digests keyed by `identity.hashing_key`, so
  the artifacts do not list user or group names. This hides identities; it
  does not protect document content.

## Principals and reader tokens

A *reader token* names who may read a row: `usr:<id>` for a user or
`grp:<id>` for a group, as the source or the operator names them (see
[Terms](architecture.md#terms); OIDC access tokens are a different thing and
only authenticate HTTP callers). The writer hashes every reader token before
it enters a Lance row, a grant snapshot or the identity artifact. A reader
resolves the caller to the set of hashed reader tokens it holds and matches
rows and container grants against that set.

A **subject** is the caller of a request:

- over stdio, `identity.stdio_user` with `identity.stdio_readers` (one
  process, one identity);
- over HTTP, the `sub` claim of a verified OIDC access token, which must be
  bound in the published identity artifact under its issuer (see
  [HTTP login and users](http-users.md)).

A subject with more than `identity.max_tokens_per_user` reader tokens (5000
by default) is refused with `authorization_unavailable`; other subjects are
unaffected.

## Permission modes

Each source is either:

- `permissions = "sync"`: the connector delivers the source's own access
  rights as per-document readers and container grants (a shared drive, a
  repository, a channel, a project) plus a full snapshot of the source's
  users and groups. Nested groups are expanded by the writer. Unknown state
  is denied.
- `permissions = "fixed"`: every document gets the configured `readers`; the
  connector makes no permission or member reads. `config check` warns that
  the operator owns the over-sharing risk.

Restricted parts of a document, such as a comment with its own audience, are
separate records with their own readers. Public and link-only sharing,
unknown identities and unsupported holder types grant nobody.

## Freshness and revocation

The authorization gates themselves are described in
[How fyrr works](architecture.md#authorization). What a deployment has to
plan for:

- A source's permission and member reads renew its freshness only when they
  complete. A failed or partial read keeps the last published rights until
  `identity.validity_deadline`; after that the source alone is hidden from
  every reader (`metadata.hidden_sources`).
- A configured directory provider (`snapshot`, `keycloak` or `scim`) needs a
  completed read within `identity.validity_deadline`, or the whole namespace
  is refused. With the default `provider = "none"` no directory clock
  applies.
- A revocation at the source reaches readers after the next permission sync
  and commit; schedule `intervals.permissions` accordingly.
- A retired source (`fyrr writer source retire`) is denied on every reader's
  next request, and restore and rollback never bring it back.

## Canonical directory

`identity.directory.provider` selects where canonical users and groups come
from: `none`, a `snapshot` file, a `keycloak` realm, or `scim` pushes to the
writer's SCIM 2.0 endpoint. Source users link to canonical users through
explicit account links in the directory snapshot, or through an e-mail
address that the source reports as verified and organization-managed in an
approved domain (`verified_domains`). Personal or unverified addresses never
link. An inactive canonical user is denied everywhere. The keys are in
[identity](identity.md#identitydirectory).

## Secrets and stored credentials

- Configuration holds `{ env = "VAR" }` references only. Inline secrets,
  including under secret-like keys in provider settings, are refused without
  echoing the value; `config show` redacts secret fields.
- Error messages, logs, cursors, telemetry and status output never carry
  document text, titles, identities, e-mail addresses, query text or secret
  values. `crates/fyrr/tests/privacy.rs` plants markers in all of these and
  asserts that the CLI paths it drives do not print or write them.
- Directory snapshots, membership batches, SCIM state and stored provider
  credentials are encrypted with AES-256-GCM under `identity.encryption_key`.
  This covers those writer inputs only, not published content.
- `fyrr writer credential --name NAME --from-env VARIABLE` stores a
  credential encrypted. A provider credential is decrypted only in the
  isolated worker, into the provider's environment; a directory or writer
  administration credential mapped to a stored one is decrypted by the
  writer process. Readers never load stored credentials.

## Worker isolation

All Python runs in `fyrr writer __worker` child processes, one per source
(provider discovery uses its own worker), with a timeout that also kills descendants, a memory limit
(`writer.worker_memory_max_bytes`), no core dumps and provider stdout and
stderr discarded. Results return over a size-capped socket with fixed
diagnostic codes, never Python exception text. A crash or hang fails only
that source.

The reader-only build links no Python and no writer code, so a serving host
needs neither an interpreter nor provider credentials.

`reader.mirror_path` (by default `$XDG_CACHE_HOME/fyrr/<namespace>`) must be a
private directory (mode 0700) owned by the process and not a symlink; the
reader creates it 0700 and cached files 0600.

## HTTP boundary

- Every route but `/readyz` requires the `Host` header to equal the
  authority of `reader.http.public_url`; other requests get 403
  `host_mismatch`, and the reader logs `http refused code=host_mismatch`.
  `/readyz` answers any `Host`, but a foreign one gets only the status code
  with an empty body. Requests with an `Origin` header are refused on every
  route.
- `POST /admin/refresh` exists only when `reader.http.operator_credential`
  is set.
- Access tokens are checked as described in
  [HTTP login and users](http-users.md#tokens).
