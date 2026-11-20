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
- A retired source (`fyrr writer source retire`) is denied by every reader
  once it has read the serving head again, and restore and rollback never
  bring it back.
- A reader reads the serving head at every version check
  (`reader.version_check_interval`, default and at most 30 s), `SIGHUP` and
  `POST /admin/refresh`, and its requests check their grants against that
  read. A published revocation, membership change, retired source or
  deleted namespace therefore reaches a running reader with its next
  version check, not with its next request, and at the latest two
  intervals after it was published (60 s by default): while the reader
  prepares a new version its checks pause, and a request then reads the
  head itself once the held one is that old. `SIGHUP` and
  `POST /admin/refresh` start a check at once.

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
host process that a `fyrr writer __worker` child process starts, one host per
sync kind of a source (provider discovery uses its own worker). The host stays
in the worker's process group, so the worker's timeout kills it and every
descendant. Each of the two processes has the memory limit
(`writer.worker_memory_max_bytes`, applied per process), no core dumps and no
inherited stdin, stdout or stderr. They talk over a private socket. Provider
code is trusted (above): the checks below keep a faulty provider from taking
the worker down or being mistaken for a valid batch, and are not a sandbox.

What the worker checks about what the host sends:

- Every frame's length is bounded before anything is allocated for it.
- An Arrow stream is inspected before it is decoded: one schema message that
  is the contract's (only nullable additions) and one uncompressed record
  batch, no dictionary batches, with the declared row count and buffer ranges
  within the limits. The schema carries no metadata beyond the contract's two
  entries and no field metadata. Its fields, at every nesting level, are
  counted against the contract's own plus a small allowance of added fields
  before it is converted, because a flatbuffer can share offsets, so a small
  message can describe a far larger tree.
- Inspection, schema conversion and decoding run under one panic guard: a
  stream that makes any of them panic is a refused batch, with the same closed
  diagnostic as any other. The panic message goes to the worker's stderr,
  which is `/dev/null`.
- A malformed, oversized or out-of-order frame, a host that exits early and
  a host that hangs all end in fixed diagnostic codes, never Python
  exception text, and none carries a value the host chose except a version
  built from numbers and a few fixed suffixes. A crash, hang or exhausted
  memory limit of the host (`the provider failed`) fails only that source.

When a host's conversation ends, the worker shuts its side of the socket
down. The host then closes the provider's generator, so its `finally` blocks
run, and exits; it is killed five seconds after the shutdown, or at once when
the conversation was out of step.

What the host guarantees about itself: the interpreter starts without site
code, and a bootstrap that uses only built-in modules moves the protocol
socket (its stdin) to a close-on-exec descriptor, points stdin at `/dev/null`
and closes every other inherited descriptor, before `.pth` files,
`sitecustomize` or any provider import run. A process that provider code
forks closes the socket too, site code included. Start-up code can see the
socket as an open descriptor that no child inherits.

Stored credentials a source references are sent to the host after it starts
and set in its environment before the SDK imports any provider code; their
names are removed from the environment the host inherits, so they are not in
its initial environment. `fyrr-core` owns the name rule (`config check`
refuses a referenced stored credential named `PATH`, `VIRTUAL_ENV`,
`PYTHON*` or `LD_*`, or over 256 bytes, naming the key); the worker checks the
names and the values, which only it can know, once before it starts the host,
and the host checks the message again before it sets any of it.

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
