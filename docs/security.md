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
  secret. Nothing a caller sends changes what the key may read, and a
  document outside it looks like one that does not exist. Only a `readers`
  key has exactly the grants the operator declared in
  `[[identity.api_keys]]`: reader tokens like the stdio subject's
  `stdio_readers`. A `user` key acts as one canonical directory user, so it
  also gains whatever the sources grant that user: the documents they may
  read and the groups they belong to, as the writer derives them from the
  sources, which can change without any edit of the configuration. A key
  that stands in for a user is as powerful as that user.
- **`readers` grants are the operator's.** They come from the configuration
  file, not from a directory or a source, so a source cannot widen them.
  Give each agent role its own key with only the groups its work needs
  ([software factories](software-factory.md#limit-what-an-injected-agent-can-reach)).
  A key with `readers` is checked against `identity.max_tokens_per_user`. It
  is no member of any group, so `group_requests_per_minute` does not apply to
  it: it has an allowance of its own, `requests_per_minute` or else
  `reader.http.user_requests_per_minute`. A `user` key shares that user's
  allowance and their groups' allowances.
- **The secret must be random, and only a keyed hash is published.** A key is
  `fyrr_key_` and 43–128 characters, which is at least 256 bits when
  generated with the documented command (`openssl rand -hex 32` or better).
  The shape does not prove randomness: `config check` and the writer refuse a
  secret with fewer than 10 distinct characters after the prefix, which turns
  away `aaaa…`-style placeholders and nothing cleverer, and a hex secret has
  up to 16. The generation command is what makes the key unguessable. The
  writer stores the HMAC of the secret under `identity.hashing_key`, in a
  domain of its own, so the identity artifact holds nothing that can be turned
  back into the secret. Whoever reads the artifact and holds the hashing key
  can still test a guess, which is why the secret has to be random. The
  configuration and the writer's environment hold the key; the reader needs
  neither.
- **What the identity artifact reveals.** The artifact does not label a
  binding as an API key, but someone who can read the bucket sees how many
  subjects are bound (keys and OIDC subjects together); the `expires` of
  each binding that has one, which only keys have; and which principal each
  subject is bound to. A `user` key appears as one more subject on a
  directory user's principal, so it shows which directory principals have a
  key. A `readers` key has a principal of its own, and its reader tokens,
  as hashes, are its grants. Every value is a keyed hash: the artifact holds
  no secret, no key name and no e-mail address, and a key the writer does not
  bind (unusable secret, missing or inactive user) is not in it at all.
- **Same answer for every refusal.** An unknown, malformed, removed and
  expired key all get `401 unauthorized`. The reader looks the key's hash up
  in the identity of the serving head it holds, the one its latest version
  check read, as it does for every request, and it does so before it checks
  the namespace or the directory's freshness. A hash the identity does not
  bind is refused at once, and neither the directory's freshness nor the
  state of storage changes that answer while the reader holds a head it
  trusts, so a caller with a random key cannot learn whether the directory is
  stale (`503`). Only a key that is bound can be answered with `503` for the
  directory's freshness or its grants. One more `503` is the same for every
  caller, bound or not: the held head is older than two version-check
  intervals, the reader's own read of it fails, and no request can be
  decided ([Freshness and revocation](#freshness-and-revocation)). No key,
  name or hash is written to logs, errors, telemetry or metrics.
- **Garbage keys cost no storage reads.** A request resolves its key
  against the held head and the identity that head names, both in memory, so
  a flood of unknown keys makes no storage request of its own. The version
  check that reads a new head also loads and verifies the identity it names, so
  this holds after an identity-only change too: a well-shaped unknown key gets
  the same `401` as a malformed one and never starts an identity read. The one read
  any request may start is the shared read of the head when the held one is
  older than two intervals, the same for every caller.
- **Use TLS.** The key is a bearer secret in every request. Terminate TLS in
  front of the reader as for OIDC ([HTTP boundary](http-users.md#3-start-the-writer-and-the-reader));
  never send a key over plain HTTP outside loopback. Prefer a header from a
  secret store or an environment variable over a key typed into a shell
  history.
- **Rotation.** Add a second key, move the clients to it, remove the first.
  Two keys never share a secret.
- **Revocation.** Remove the `[[identity.api_keys]]` table (or set
  `expires`) and republish the identity. The writer builds the identity from
  its own configuration, so the change must reach **every host that publishes
  identity**: every `fyrr writer serve` and every scheduled `fyrr writer
  commit` (or `sync`, or directory sync). Edit the file there, then restart
  `writer serve` and rerun the scheduled commands; the 60 s
  `writer.commit.coalescing_timer` is not a bound, because a running
  `writer serve` keeps the configuration it started with. A writer that still
  runs with the old file republishes the removed key at its next identity
  build, and the reader accepts it again. Removing the last key works like
  removing any other: the writer publishes an identity with no key bindings.
  A restarted reader with no `[[identity.api_keys]]` and no `identity.oidc`
  refuses to start.

  A reader applies the published identity as it applies every other
  revocation: with its next version check, at the latest two
  `reader.version_check_interval`s (60 s by default) after the identity was
  published. For an urgent revocation, remove the key on every host that
  publishes identity, publish (`fyrr writer commit`, or restart `writer
  serve`), then send `POST /admin/refresh` to each reader (it needs
  `reader.http.operator_credential`): it returns after the version check has
  completed, and the reader refuses the key from its next request on. `SIGHUP`
  starts a check without waiting for it, so it confirms nothing. Without
  either, the key keeps working until the next check.
- **Adding a key** takes the same time: a key published by the writer is
  unknown to a running reader, and gets `401`, until its next version check
  (or `POST /admin/refresh`, or the check `SIGHUP` starts), at the latest two
  intervals later.
- **Expiry.** `expires` is an RFC 3339 timestamp with an offset, to the whole
  second (a fraction is dropped). A reader knows an expiry only from the
  published binding. Once a reader holds it, nothing more is needed when its
  time comes: no publication and no version check, the reader's own clock
  decides. Setting or changing an `expires` is an identity change, published
  and applied like any other. A key is expired from that second on: a request is refused when
  the reader's clock, truncated to whole seconds, is at or after the second
  named by `expires`, and so is a request admitted before it that has not
  finished. The check runs again at two later points: before the candidates'
  text is sent to an external reranker, and at the HTTP boundary, after the
  inner service has produced the response and before any of it is returned
  (earlier checks inside the reader save work but are not the guarantee).
  A subject that expired by then gets the refusal an expired request gets, a
  tool error `authorization_unavailable` with the request's id and no result,
  so no search or fetch result is returned over HTTP once `expires` has
  passed by the reader's clock at that moment, and no reranker request body
  is released after it. What is not covered: bytes already handed to the
  socket (a response whose check passed an instant before `expires` is out of
  the reader's hands), a reranker body already sent, and stdio, which has no
  HTTP boundary (its last check is inside the reader, before the reply is
  handed on). A reader that has not yet seen an identity that changed an
  `expires` applies the one it holds.

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
- A published directory read has a clock. When the serving head carries a
  directory freshness timestamp (a completed `keycloak` or `snapshot` pull,
  or a SCIM attestation), every reader refuses the whole namespace once that
  read is older than `identity.validity_deadline`, whatever its own
  `identity.directory.provider` says: the publication attests the directory,
  so a reader's local setting cannot waive the clock for OIDC subjects, `user`
  keys or any other directory-bound subject. A head with no timestamp is
  served as before by a reader with `provider = "none"` (a snapshot imported
  by hand with `writer directory --snapshot` is not a freshness proof), and
  refused by a reader that has a directory provider configured.
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
  head itself once the held one is that old. `POST /admin/refresh` runs a
  check and returns when it has completed; `SIGHUP` starts one without
  waiting for it.
  The held head is trusted for two intervals. A request whose held head is
  older (the version check did not run, or the host was suspended; the age
  counts on the monotonic and the wall clock) reads the head itself, one read
  shared by the requests that wait for it, and is refused with `503` if that
  read fails: `storage_unavailable` over stdio, `authorization_unavailable`
  over HTTP.
- An API key follows the same rule. A key that is added, removed or rebound
  by an identity publication takes effect on a running reader with its next
  version check, at the latest two intervals after the identity is published
  (60 s by default); `POST /admin/refresh` applies it before it returns, and
  `SIGHUP` only starts the check. A key's `expires` is the exception once the
  reader holds the binding: the reader's own clock refuses the key from the
  second it names, even in a request that was admitted before it
  ([API keys](#api-keys)). The `expires` itself is published like any other
  identity change.

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
- Error messages, logs, telemetry and status output never carry document
  text, titles, identities, e-mail addresses, query text, API-key secrets,
  names or hashes, or other secret values. A fetch cursor is signed but not
  encrypted: it carries the requester's principal as a keyed hash (for a
  `readers` API key, the hash of its name), never an e-mail address, a key's
  name or its secret, and the cursor is refused for any other principal. `crates/fyrr/tests/privacy.rs` plants markers in all of these and
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
