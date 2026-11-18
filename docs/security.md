# Security model

> **In short**
>
> - What fyrr trusts and what a deployment must protect itself: [provider](architecture.md#terms) code,
>   the embeddings and reranker services, the bucket and the reader mirror.
> - How reader tokens, subjects, permission modes and freshness deadlines
>   decide who may read what, and when a revocation reaches readers.
> - How secrets, stored credentials and provider workers are handled.

To report a vulnerability, see [SECURITY.md](../SECURITY.md).
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
  metadata and vectors are stored in [Lance](architecture.md#terms) files under `storage.uri`, and
  readers cache them in `reader.mirror_path`. fyrr does not encrypt them;
  protect the bucket and the mirror host with your own access control and
  storage encryption. Anyone who can read the bucket can read every
  document.
- **Identity values are hashed, not encrypted.** Reader tokens and subject
  keys are stored as HMAC-SHA256 digests keyed by `identity.hashing_key`, so
  the artifacts do not list user or group names. This hides identities; it
  does not protect document content.

## Principals and reader tokens

[Reader tokens and principals](architecture.md#terms) are checked at the
[authorization gates](architecture.md#authorization).

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
- A retired source (`fyrr writer source retire`) is denied on every reader's
  next request, and restore and rollback never bring it back.

### Source polling

Connectors reuse permission and membership snapshots read during a sync run,
so a rights change at the source during that run may still leave old grants
in its publication. Revocation takes effect after the next successful sync
of that source that reads the changed rights and a commit publishes them;
Keycloak membership changes similarly need a successful directory pull and
commit. Sync intervals alone provide no end-to-end wall-clock bound.

## Canonical directory

`identity.directory.provider` selects where [canonical users](architecture.md#terms) and groups come
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

The [deployment guide](deployment.md#topology) covers reader hosts; the
[reader settings](reader-writer.md#reader) specify mirror ownership and modes.

### macOS

macOS is a development platform: fyrr is built from source there and has
no release binaries or images. Its worker isolation is weaker, so run only
providers you trust there and do not use it for production:

- the memory limit is set but the kernel does not enforce it, so a provider
  can use as much memory as the machine allows;
- a timeout or the end of a worker kills the worker's process group. A
  provider process that started its own session or process group is not
  found and keeps running. On Linux the writer adopts and kills such
  descendants too;
- the worker's result socket is a file in a private (0700) directory under
  `TMPDIR`, removed afterwards, instead of a socket without a file.

Timeouts, the absence of core dumps and the discarded provider output are
the same as on Linux. Storage durability is the same too: every flush uses
`F_FULLFSYNC`.

## HTTP boundary

See [HTTP boundary](http-users.md#http-boundary) for host and origin checks,
request limits, readiness and administration, and [tokens](http-users.md#tokens)
for access-token validation.

A caller can still infer shared query-embedding cache membership by choosing
a `deadline_ms` shorter than provider latency and observing whether dense
retrieval degrades.


Opt-in [object-store diagnostics](operations.md#object-store-diagnostics) write
only method names, hashed keys, numeric transport facts and fixed error codes to
an operator-owned private file. They never format backend errors or record bodies,
headers or credentials. Treat key hashes and operational timing as restricted
operator metadata and configure log retention accordingly.
