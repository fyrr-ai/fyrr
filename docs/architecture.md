# How fyrr works

> **In short**
>
> - fyrr has two halves, a writer and a reader, that share nothing but an S3
>   prefix.
> - This page defines the terms the other pages use and explains publication,
>   authorization and retrieval.
> - Read it when you want to know what happens between a sync and a search
>   result.

![fyrr architecture](assets/architecture.svg)

fyrr has two halves that share nothing but an S3 prefix (`storage.uri`), or
on a single host a [local directory](configuration.md#local-storage):

- The **writer** (full build) runs connector providers, prepares their
  output and publishes it.
- The **reader** (reader-only build, or the full build) serves the published
  data over MCP. The reader-only build links no Python and no writer code;
  `scripts/check-reader-only` proves it in CI.

## Terms

| Term | Meaning |
| --- | --- |
| namespace | One deployment's data under `storage.uri`, named by `namespace.name`. Every writer and reader of a deployment uses the same namespace. |
| source | One `[[sources]]` entry: a connector type, what it syncs (`scope`) and how its readers are decided (`permissions`). |
| reader token | A user or group grant on a row: `usr:<id>` or `grp:<id>`. Stored only as an HMAC digest. Not to be confused with OIDC access tokens, which authenticate HTTP callers. |
| publication | One immutable, published version of the namespace: the Lance content plus the policy and identity it is served with. |
| serving head | The single S3 object that names the current publication, policy and identity. Each commit replaces it in one conditional write. |
| pin | The publication one request reads from start to finish. |
| envelope | A durable batch a sync has validated and embedded, waiting for the committer to publish it. |
| mirror | The reader's local cache of immutable published files (`reader.mirror_path`). |
| admission | The reader's check, at the start of every request, that the caller is known and the pinned publication may be served. |

## Publication

1. **Sync.** `fyrr writer sync` runs each source in its own worker process
   under an expiring S3 lease per source and sync kind. Every batch is
   validated against the connector contract (Arrow schema, row rules, cursor
   chain, size bounds), embedded, and written as a durable envelope. The
   source's cursor advances only after its envelope is durable. An invalid
   or oversized document is quarantined and its last good revision stays
   visible.
2. **Commit.** `fyrr writer commit` takes the namespace lease, which lives
   in the serving head; a second committer exits instead of waiting. The
   committer publishes permission and membership changes before content,
   appends rows and index deltas to the Lance dataset, writes an immutable
   content manifest and replaces the serving head with one conditional
   write (compare-and-swap on the object version). A failed conditional
   write leaves the writer refusing to continue (`head_cas_failed`).
3. **Serve.** Each reader request pins one publication. Readers notice new
   publications in the background (`reader.version_check_interval`, 30 s,
   or `SIGHUP`) and switch between requests; in-flight requests keep their
   pin. A reader whose pin predates a published revocation stops serving
   until it has prepared the newer publication. Each namespace creation
   writes a new random incarnation into the serving head and every
   publication; a reader whose pin belongs to a namespace that was deleted
   and created again at the same storage URI stops serving at once and
   prepares the new namespace's publication.

Published versions are retained for `writer.purge_window` and can be
restored; see [operations](operations.md#restore-and-rollback).

Every artifact carries a `major.minor` contract version. A reader refuses an
unknown major, and a publication whose retrieval policy it does not match,
with `incompatible_index`. The exact formats are generated into `schemas/`
and `fixtures/`.

## Authorization

The writer replaces every reader token with an HMAC-SHA256 digest keyed by
`identity.hashing_key` before it is stored. A published *identity artifact*
maps each subject (the stdio user, or an OIDC subject over HTTP) to the
hashed reader tokens it holds.

Every `search`, `fetch` and `list_filters` call passes three gates:

1. **Admission.** The reader reads the live serving head, policy and
   identity. Any of them missing, malformed or from another namespace
   refuses the request with `authorization_unavailable`, as does a subject
   that is not in the identity artifact or holds more than
   `identity.max_tokens_per_user` reader tokens, and an expired directory
   read (`identity.validity_deadline`) when a directory provider is
   configured. A `permissions = "sync"` source whose last successful
   permission or membership read is older than `identity.validity_deadline`
   is excluded, and named in `metadata.hidden_sources` if the caller could
   otherwise read it. A retired source is excluded silently.
2. **Permission predicate.** Lexical and dense candidates are generated under
   a filter on the row's hashed readers, so a row the caller may not read is
   never scored, counted or returned. For `permissions = "fixed"` sources
   the filter checks the readers in the current policy, not the stored rows.
3. **Final check.** Before a result or error is returned, the reader reads
   the serving head again. If the policy, identity or hidden sources changed
   during the request, the request is refused rather than answered under the
   old grant.

Unknown and denied documents look the same to `fetch`: an empty result.
Revocations at a source reach readers through publication: a sync reads the
new rights and the next commit publishes them.

## Retrieval

A `search` runs two candidate lanes over the permission-filtered rows:

- **lexical**: native full-text search (BM25) on the Lance `fts_idx` index;
- **dense**: IVF vector search on the query embedding. The query is embedded
  only after admission and only when `embedding.reader_live = true`;
  otherwise, or when the embedding call fails, the lexical lane answers
  alone and the response reports `degraded: ["dense"]`.

The two score lists are normalized, fused (weight 0.75 lexical, 0.25 dense
by default) and grouped to one hit per document. A soft per-source cap
spreads the top of the list across sources; remaining slots may still come
from the same source. An optional reranker (`[namespace.rerank]`) reorders
the authorized documents. Excerpts are passages of up to 1200 codepoints
around the query terms; `fetch` returns whole documents or a window of one
record.

Fusion weights, depths, analyzer settings and composition enter one
retrieval-policy digest that is published with each version, so readers and
writers cannot silently disagree about ranking. The tuning keys are in
[tuning](tuning.md); the tool arguments and outputs are in
[MCP tools](mcp-tools.md).
