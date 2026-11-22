# HTTP JSON API

> **In short**
>
> - A reader with `[reader.http]` also serves `search` and `fetch` as plain
>   JSON: `POST /v1/namespaces/{namespace}/query` and
>   `POST /v1/namespaces/{namespace}/fetch`.
> - They take the arguments of the [MCP tools](mcp-tools.md) and return their
>   structured results. Authentication, permissions, deadlines, rate limits
>   and the scheduler are those of `/mcp`.
> - [API errors](#errors) carry an HTTP status and `{"error":"<code>"}`.
>   `429` and `503` carry `Retry-After`. The HTTP parser can return `431`
>   with an empty body.

## Routes

| Route | Body | Answer |
| --- | --- | --- |
| `POST /v1/namespaces/{namespace}/query` | The arguments of [`search`](mcp-tools.md#search) | The `search` result |
| `POST /v1/namespaces/{namespace}/fetch` | The arguments of [`fetch`](mcp-tools.md#fetch) | The `fetch` result |

`{namespace}` names the namespace to read: any namespace under
`<storage root>/namespaces/`, not only `namespace.name`. The reader opens it
on the first request, with the settings it is published with, and keeps it
open for later ones. It keeps at most 8 namespaces open besides the
configured one, and each holds a place until its scheduler has stopped; a
request that finds none waits up to 5 seconds, then answers `503 busy`. When
another namespace needs a place, the one used least recently that is serving no
request is closed, and if every one is serving, the request answers `503 busy`.
A request for a closed one opens it again, with a mirror
filled anew: closing deletes the mirror of that namespace, so only the open
ones have one. The mirrors are kept in a directory of the process next to the
configured mirror, `<reader.mirror_path>.paths`, so opening a namespace needs
write access to the parent of `reader.mirror_path`; without it such a request
answers `503 storage_unavailable` and the configured namespace is served as
before. The 8 share the reader's `reader.memory_max_bytes` with the
configured namespace, and each gets an eighth of `reader.mirror_max_bytes` and
of the 128 MiB subject cache the configured namespace has, so together they add
one more mirror and cache budget of the configured size. Embedding and rerank
spend limits are per process: every namespace charges the same counters.
`/mcp`, `/readyz` and `/admin/refresh` keep serving the configured namespace
alone. `list_filters` has no JSON route.

Both routes, like `/mcp`, need the `read` role of an
[API key](identity.md#identityapi_keys); users signed in through OIDC hold
`read` only. Other namespaces than the configured one are for API keys: an OIDC
user reads the configured namespace through the path and answers `503
authorization_unavailable` for any other. An API key opens a namespace where
its role and `namespace_prefix` in the published keys allow it, and reads there
what that namespace's identity binds it to (declare the key in the
configuration of every namespace where it reads). A key without the role, or
limited by `namespace_prefix` to other namespaces, answers `403 forbidden`.

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"backup","limit":5}'
```

The body is one JSON object, at most 1 MiB, with the arguments of the tool
and no wrapper. Arguments outside the tool's schema or limits, and bodies
that are not such an object, answer `400 invalid_argument`; nothing is
clamped. A `200` body is the tool's `structuredContent`, with the same
`metadata` as over MCP. A request that the caller drops before the answer
is cancelled like a cancelled MCP call.

A request is checked in this order: `Host` and `Origin`, the
[limits](#limits), the bearer token, the role and `namespace_prefix` of an
API key, the caller's rate (counted by the key's or the user's own ID, not per
namespace), the shape of the namespace name, the namespace, the arguments, then the scheduler and the permission checks that every
tool call passes ([how](architecture.md#authorization)). Nothing of the
namespace is read, and none is opened, before the key's role and prefix and
the caller's rate have admitted the caller, so a caller that may not read it
is answered alike whether it exists or not. `deadline_ms` counts from arrival,
as for MCP.

A namespace that does not exist, or has no content yet, is answered as one
whose identity does not bind the caller: `401 unauthorized` to an API key and
`503 authorization_unavailable` to an OIDC user. Finding that out costs one
read of `head.json`; nothing is kept for the name. A key whose prefix allows a
namespace can tell by the time the answer takes whether it exists. A name that is not a namespace name (lowercase letters,
digits and `-`, at most 63 characters, not starting with `-`) answers `404
invalid_argument`.

## Errors

The nine tool error codes of [MCP tools](mcp-tools.md#errors) map to these
statuses, and the boundary adds `forbidden`. The body is `{"error":"<code>"}` and nothing else.

| Code | Status | When |
| --- | --- | --- |
| `invalid_argument` | `400` | The body or its arguments are invalid. `404` when `{namespace}` is not a namespace name. |
| `authorization_unavailable` | `503` | The caller's identity or policy cannot be decided. |
| `storage_unavailable` | `503` | Storage could not be read. |
| `incompatible_index` | `503` | The published index does not match this reader. |
| `deadline_exceeded` | `408` | The deadline passed, in the queue or while executing. |
| `cancelled` | `499` | The request was cancelled; the caller has normally gone. |
| `stale_cursor` | `409` | The `fetch` cursor is expired, belongs to another subject or its content version changed. |
| `busy` | `503` | The in-flight limit, the concurrency cap or the queue is exhausted. |
| `rate_limited` | `429` | A per-user or per-group request limit was reached. |
| `forbidden` | `403` | An API key lacks the role the route needs or may not act in this namespace. This code is not a tool error: only the HTTP boundary answers it. |

`429` and `503` answers carry `Retry-After` in seconds: 60 for `429` (the
rate window) and 1 for `503`. These routes also answer the refusals of the
[HTTP boundary](http-users.md#http-boundary), for example `401
unauthorized` with a `WWW-Authenticate` header, `403 host_mismatch`, `413`
for an oversized body and `504` when a request outlasts 301 seconds.

## Limits

The listener bounds every route, `/mcp` included:

| Limit | Value | Beyond it |
| --- | --- | --- |
| Open connections | 64 | The connection is closed at accept. |
| Requests in flight | 64 | `503 busy`. Cancellation notices to `/mcp` have their own 8. |
| Concurrently executing requests | `reader.concurrency` (default 8), with an equal queue | `503 busy`. |
| Body | 1 MiB | `413 invalid_argument`. |
| Headers | 64 fields, 32 KiB | `431` (see below). |
| Requests per user and per group | `user_requests_per_minute`, `group_requests_per_minute` (default 120) | `429 rate_limited`. |
| Deadline | `deadline_ms`, default 10 s, at most 300 s | `408 deadline_exceeded`. |

When the HTTP parser rejects a request for exceeding a header limit, it
returns `431` with an empty body. If the API detects oversized headers
after parsing, it returns `431` with `{"error":"invalid_argument"}`.

## Export a namespace

A scan by the document key lists every document the token may read once, in
pages of at most 1000 ([how it works](mcp-tools.md#paging-and-scanning)). The
script needs `curl` and `jq`, and writes one hit per line; read the text of a
document with [`fetch`](mcp-tools.md#fetch).

```sh
url=https://fyrr.example.com/v1/namespaces/main/query
last=null
while :; do
  body=$(jq -n --argjson last "$last" '
    {order_by: [{field: "source_id", direction: "asc"}, {field: "root_id", direction: "asc"}],
     output: "metadata", limit: 1000}
    + ($last | if . == null then {} else {filter: {or: [
        {field: "source_id", op: "gt", value: .source_id},
        {and: [{field: "source_id", op: "eq", value: .source_id},
               {field: "root_id", op: "gt", value: .root_id}]}]}} end)')
  page=$(curl -sf "$url" -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H "Content-Type: application/json" -d "$body") || exit 1
  [ "$(echo "$page" | jq '.hits | length')" -gt 0 ] || break
  echo "$page" | jq -c '.hits[]' >> export.jsonl
  last=$(echo "$page" | jq -c '.hits[-1] | {source_id, root_id}')
done
```

With `reader.max_response_bytes` set, a page can hold fewer hits than `limit`;
the script goes on from the last hit it got, so nothing is skipped.

## Write documents

> **In short**
>
> - `fyrr writer serve` with `[namespace.write_api]`, `[writer.serve.admin]`
>   and the committer serves `POST /v1/namespaces/{namespace}/documents` on
>   the writer listener.
> - A request upserts rows and deletes ids, all or none. It is answered when
>   the head that readers serve holds it, or has refused it.
> - It needs an [API key](identity.md#identityapi_keys) with the `write` role,
>   published with `fyrr writer keys`.

```sh
curl https://writer.example.com/v1/namespaces/main/documents \
  -H "Authorization: Bearer $FYRR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"settings":{"attributes":{"price":"float"}},
       "upserts":[{"id":"a1","text":"Blue shirt","price":12.5,"readers":["usr:ann"]}],
       "deletes":["a0"]}'
```

The body is one JSON object (`application/json`, no `Content-Encoding`):

| Key | Meaning |
| --- | --- |
| `upserts` | Rows. A row is an object with an `id` (string, its key in the namespace), its text under the key `text_attribute`, `readers` (`usr:<id>` or `grp:<id>`; ignored by an open namespace) and a value for any declared [attribute](tuning.md#namespaceattributes). A row replaces the document with that `id`, readers included. |
| `deletes` | IDs to delete. An ID that does not exist is a delete of nothing. |
| `settings` | The [settings](tuning.md#namespace) of a namespace that does not exist yet, laid over the process defaults. For an existing namespace it must not change any: `409 namespace_settings_fixed`. |

The namespace is created by the first request, with its settings, and a
request with only `settings` creates it. A row is one record: its text is at
most 16 KiB, and it is not chunked. The same `id` twice in one request is
`400 duplicate_id`. The answer is `{"upserted":n,"deleted":m}`, the rows of the
request. A body over `writer.envelope_max_bytes` or with more rows than
`writer.envelope_max_rows` is `413`; the body is counted as it arrives. A
request holds one envelope: more than `writer.commit.group_max_rows` records
or `group_max_bytes` is `413 request_too_large`.

A request is published whole or refused whole. The answer comes after the
publication that holds it is the served head. An answer `503 outcome_unknown`
or `503 unavailable` says nothing about the outcome: the request may still be
published, and repeating it upserts and deletes the same documents again. Requests
are applied in the order they are published, the last one wins: a repeat can
overwrite a later change to the same IDs. A
request that was answered `200` is never refused later. A key revoked after a
request was accepted does not cancel it.

| Code | Status | When |
| --- | --- | --- |
| `unauthorized` | `401` | No API key, or a key that is not among the published keys. |
| `forbidden` | `403` | The key lacks the `write` role or its `namespace_prefix` excludes the namespace. |
| `not_found` | `404` | Another namespace, or the process has no `[namespace.write_api]`. |
| `unsupported_media_type` | `415` | Not `application/json`, or a `Content-Encoding`. |
| `body_too_large`, `too_many_rows`, `row_too_large` (1 MiB), `settings_too_large` (64 KiB), `text_too_large`, `document_too_large`, `request_too_large` | `413` | A limit above. |
| `body_invalid`, `empty_request`, `id_missing`, `duplicate_id`, `text_missing`, `readers_invalid`, `document_invalid`, `settings_invalid`, `attribute_undeclared` and the value codes of [tuning](tuning.md#namespaceattributes) (`attribute_type_mismatch`, `attribute_value_invalid`, `attribute_value_too_large`) | `400` | The body or a row is invalid, or the settings of a namespace to be created are (a schema that is too large or has a bad name included: `settings_invalid`). |
| `namespace_settings_fixed`, `namespace_not_writable`, `namespace_not_empty`, `namespace_changed`, `rejected` | `409` | The namespace has other settings, takes its documents from connectors, is to be created over envelopes that a deleted namespace left, was replaced by another while the request was prepared, or the committer refused the request (nothing of it is applied). |
| `busy`, `unavailable`, `outcome_unknown` | `503` | The writer is busy, storage or the published keys cannot be read, or the answer did not come within 10 minutes; `Retry-After` is 1. |
