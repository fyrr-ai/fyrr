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

No Python runs in the `fyrr` executable. Provider code runs in a Python
host process that a `fyrr writer __worker` child process starts, one worker
per source (provider discovery uses its own worker). The host stays in the
worker's process group, so the worker's timeout kills it and every
descendant, and the supervisor reaps what is left. Each of the two processes
has the memory limit (`writer.worker_memory_max_bytes`, applied per process,
so provider memory is bounded as before), no core dumps and no inherited
stdin, stdout or stderr. The worker and the host talk over a private socket.

What the worker guarantees about what the host sends:

- Every frame's length is bounded before anything is allocated for it, and
  an Arrow stream's message lengths, row count and buffer bytes are bounded
  before it is decoded.
- The stream is inspected before it is decoded: one schema message and one
  uncompressed record batch. Compression and dictionary batches are refused.
  The schema's fields (at every nesting level) are counted against the
  contract's own plus a small allowance before it is converted; it carries
  no metadata beyond the contract's two entries and no field metadata; it
  must be the contract's schema, with only nullable additions.
- The decoder is not given a validator of Arrow's layout rules. Inspection,
  conversion and decoding run under one panic guard instead: a stream that
  makes any of them panic is a refused batch, with the same closed diagnostic
  as any other refused batch. The panic message goes to the worker's stderr,
  which is `/dev/null`.
- Memory exhaustion is not caught: a worker or host that exhausts its memory
  limit ends, and the supervisor reports the closed diagnostic for a worker
  that failed, as it does for a provider that crashes.
- A malformed, oversized or out-of-order frame, a host that exits early and
  a host that hangs all end in fixed diagnostic codes, never Python
  exception text, and none carries a value the host chose except a version
  built from numbers and a few fixed suffixes. Results return over a
  size-capped socket. A crash or hang fails only that source.

What the host guarantees about itself: the interpreter starts without site
code, and a bootstrap that uses only built-in modules moves the protocol
socket (its stdin) to a close-on-exec descriptor, points stdin at `/dev/null`
and closes every other inherited descriptor, before `.pth` files,
`sitecustomize` or any provider import run. A process that provider code
forks closes the socket too. Stored credentials are sent after the host
starts and set in its environment before any provider code is imported; their
names are removed from the environment the host inherits, so they are not in
the process's initial environment, and a credential may not be named `PATH`,
`VIRTUAL_ENV` or anything starting with `PYTHON` or `LD_`.

The [deployment guide](deployment.md#topology) covers reader hosts; the
[reader settings](reader-writer.md#reader) specify mirror ownership and modes.

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
