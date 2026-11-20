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
  [HTTP login and users](http-users.md));
- over HTTP, an [API key](#api-keys): a static secret that the published
  identity binds to the grants the operator declared for it.

A subject with more than `identity.max_tokens_per_user` reader tokens (5000
by default) is refused with `authorization_unavailable`; other subjects are
unaffected.

## API keys

An API key is for a caller that cannot sign in: an unattended agent, or a
team without an identity provider. What to know before you issue one:

- **What it is trusted for.** A key proves only that its holder knows the
  secret. It reads what the operator declared for it in
  `[[identity.api_keys]]`: `readers`, reader tokens like the stdio subject's
  `stdio_readers`, or `user`, which acts as one canonical directory user.
  Nothing a caller sends changes those grants, and a document outside them
  looks like one that does not exist. A key that stands in for a user is as
  powerful as that user.
- **Grants are the operator's.** They come from the configuration file, not
  from a directory or a source, so a source cannot widen them. Give each
  agent role its own key with only the groups its work needs
  ([software factories](software-factory.md#limit-what-an-injected-agent-can-reach)).
  A key with `readers` is checked against `identity.max_tokens_per_user` and
  the request allowances like any subject.
- **The secret is high-entropy, and only a keyed hash is published.** A key is
  `fyrr_key_` and at least 43 characters, which is at least 256 bits when
  generated as documented. The writer stores the HMAC of it under
  `identity.hashing_key`, in a domain of its own, so the identity artifact
  holds nothing that can be turned back into the secret. Whoever reads the
  artifact and holds the hashing key can still test a guess, which is why a
  low-entropy secret is refused by shape and the documented generation is
  random. The configuration and the writer's environment hold the key; the
  reader needs neither.
- **Same answer for every refusal.** An unknown, malformed, removed and
  expired key all get `401 unauthorized`. The reader hashes first and looks
  the hash up second, and no key, name or hash is written to logs, errors,
  telemetry or metrics.
- **Use TLS.** The key is a bearer secret in every request. Terminate TLS in
  front of the reader as for OIDC ([HTTP boundary](http-users.md#3-start-the-writer-and-the-reader));
  never send a key over plain HTTP outside loopback. Prefer a header from a
  secret store or an environment variable over a key typed into a shell
  history.
- **Rotation.** Add a second key, move the clients to it, remove the first.
  Two keys never share a secret.
- **Revocation.** Remove the table (or set `expires`) and republish the
  identity. `expires` binds without a republish: the reader refuses the key
  at the first request after that instant. Removal takes effect on the first
  request after the writer commits the new identity, because the reader reads
  the serving head on every HTTP request: at most
  `writer.commit.coalescing_timer` (60 s) after a running `writer serve` has
  the new configuration, which means after it restarts. To force it, restart
  `writer serve` or run `fyrr writer commit` with the new file. A key added to
  the file is served once readers pick up the publication, within
  `reader.version_check_interval` (30 s) or at once through
  [`POST /admin/refresh`](http-users.md#http-boundary).

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
- An API key is refused at the first request after its `expires` instant and,
  once removed from the configuration, after the next identity publication
  ([API keys](#api-keys)).
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
  document text, titles, identities, e-mail addresses, query text, API-key
  secrets, names or hashes, or other secret values. `crates/fyrr/tests/privacy.rs` plants markers in all of these and
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
