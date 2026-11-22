# HTTP JSON API

> **In short**
>
> - A reader with `[reader.http]` also serves `search` and `fetch` as plain
>   JSON: `POST /v1/namespaces/{namespace}/query` and
>   `POST /v1/namespaces/{namespace}/fetch`.
> - They take the arguments of the [MCP tools](mcp-tools.md) and return their
>   structured results. Authentication, permissions, deadlines, rate limits
>   and the scheduler are those of `/mcp`.
> - `GET /v1/namespaces/{namespace}` [reports](#namespace-report) a
>   namespace's schema, settings, size and index state to an API key with the
>   `admin` or `list` role.
> - `GET /v1/namespaces` [lists](#listing-namespaces) the namespaces the key
>   may act in, to the same roles.
> - [API errors](#errors) carry an HTTP status and `{"error":"<code>"}`.
>   `429` and `503` carry `Retry-After`. The HTTP parser can return `431`
>   with an empty body.

## Routes

| Route | Body | Answer |
| --- | --- | --- |
| `POST /v1/namespaces/{namespace}/query` | The arguments of [`search`](mcp-tools.md#search) | The `search` result |
| `POST /v1/namespaces/{namespace}/fetch` | The arguments of [`fetch`](mcp-tools.md#fetch) | The `fetch` result |
| `GET /v1/namespaces/{namespace}` | None | The [namespace report](#namespace-report) |
| `GET /v1/namespaces?prefix=` | None | The [namespace list](#listing-namespaces) |

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
configured namespace: a namespace that the memory left cannot fund in full
closes the least recently used one that serves no request, and opens again,
so the open ones stay inside the budget. Each gets an eighth of
`reader.mirror_max_bytes` and of the 128 MiB subject cache the configured
namespace has, so together they add one more mirror and cache budget of the
configured size; a reader with API keys takes the memory of those caches
from `reader.memory_max_bytes` too. One that has served no request for
`reader.version_check_interval` makes no storage request until the next one,
which reads the head first and is answered from that read. Embedding and rerank
spend limits are per process: every namespace charges the same counters.
`/mcp`, `/readyz` and `/admin/refresh` keep serving the configured namespace
alone. `list_filters` has no JSON route.

`query` and `fetch`, like `/mcp`, need the `read` role of an
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

## Namespace report

`GET /v1/namespaces/{namespace}` answers what the reader holds of the
namespace's published version, with no storage request of its own:

| Field | Meaning |
| --- | --- |
| `namespace`, `publication_id` | The namespace and the content publication the answer describes. |
| `schema` | The typed attributes of the version, name to type; `{}` when it has none. |
| `settings` | The namespace's [settings](configuration.md), as its policy records them. |
| `rows` | Approximate live rows (a document is one or more rows). `null` for a version published before contract 4.1. |
| `logical_bytes` | Approximate bytes of the data files of those rows, without indexes. `null` as `rows`. |
| `first_published_at` | When the namespace's first content publication was committed. `null` for a namespace published before contract 4.1: that time is not known. |
| `last_write_at` | When the version was published. |
| `unindexed` | `rows` and `logical_bytes` of the data that an index of the version does not cover yet. A reader serves a version only when every index covers all its data, so both are `0` for now. |

The reader holds all of it for the namespace it has open, so the report reads
nothing from storage; a request for a namespace that is not open yet pays its
open first, like any first request (see above).

`rows` and `logical_bytes` are as of the publication the reader serves, so
they follow a write when the reader checks its version. The numbers are not
filtered by row permissions, so the route needs an API key with the `admin` or
`list` role; a key that holds `read` only, and users signed in through OIDC,
answer `403 forbidden`. Like the other routes it is for a namespace the key's
`namespace_prefix` allows. A key with these roles alone is not bound to any
reader tokens and reads no rows.

```sh
curl -H "Authorization: Bearer $FYRR_KEY" \
  https://fyrr.example.com/v1/namespaces/acme
```

## Listing namespaces

`GET /v1/namespaces` answers `{"namespaces":["acme","acme-eu"]}`: the names
under `<storage root>/namespaces/`, in name order. `prefix` (lowercase letters,
digits and `-`, at most 63 characters) keeps the names that start with it. It
is the only query member; anything else answers `400 invalid_argument`. The
route needs an API key with the `admin` or `list` role. A key limited by
`namespace_prefix` lists the names that start with that prefix and no others,
whatever `prefix` asks for. It reads no rows and opens no namespace, and it is
the same list as `fyrr writer namespace list [--prefix P]`, which has no key
and lists every name.

A name is listed while any object exists under it, so a namespace whose
[deletion](cli.md#namespace-deletion-failures) has not finished is listed, and
so is one that was never created but holds a stray object. On
[local storage](configuration.md#local-storage) the directory of a deleted
namespace can remain, empty, and its name is then still listed. One call is one
delimited listing of `namespaces/`: 1 LIST request per 1,000 names in the
storage root, and no other request. More than 10,000 matching names answer
`400 too_many_namespaces` (`error: too_many_namespaces` on the command line);
narrow `prefix`.

```sh
curl -H "Authorization: Bearer $FYRR_KEY" \
  "https://fyrr.example.com/v1/namespaces?prefix=acme"
```

## Consistency

Both routes take one more member besides the arguments of the tool,
`consistency`. It is no argument of the tool and `/mcp` has no such member.

| Value | Answer |
| --- | --- |
| `cached` (default) | From the version the reader holds, found by its latest [version check](reader-writer.md#reader). A write acknowledged a moment ago may not show yet. |
| `strong` | After a version check that started once the request arrived: the answer includes every write acknowledged before the request began. |

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"backup","consistency":"strong"}'
```

A `strong` request costs one `GET` of the serving head when the head has not
moved. When it has, the request waits for the reader to switch to the new
version, within its deadline. The check is the reader's own version check, so
the request is still answered through the caller's permission filter and
admission, as any other. If the check fails, the request answers `503
storage_unavailable` with `Retry-After`; it is never answered from the held
version, and retrying it is safe. Any other value than the two answers `400
invalid_argument`.

## Returned fields

The `query` route takes one more member, `fields`, that chooses what a hit
returns besides its document fields. It is no argument of the tool: `fetch`
and `/mcp` refuse it, and their output does not change. A request without it
returns what it returned before.

| Member | Effect |
| --- | --- |
| `include` | The typed [attributes](tuning.md#namespaceattributes) to return: one or more names. `vector` names the row's vector, returned as an array of numbers. A name the namespace does not declare is `400 invalid_argument`. |
| `exclude` | Returns every typed attribute except these; `[]` returns all of them. A name the namespace does not declare is ignored. The vector is never returned by `exclude`. |
| `scores` | `true` adds `lexical_score`, the raw lexical score, and `distance`, the raw vector distance, next to the final `score`. A hit the lexical lane did not find has no `lexical_score`; one the dense lane did not find has no `distance`. |

`include` and `exclude` are exclusive. The attributes of a hit are the
object `attributes`, with the JSON type of each attribute (`datetime` as an
RFC 3339 string, `uint` as a number, `uuid` as 8-4-4-4-12 hexadecimal digits,
`bytes` as base64 and an array as a JSON array of its elements); an attribute the document does not set is `null`. Only the
columns asked for are read, in the same read as the hit's other fields, and
only for rows the caller may read.

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"backup","fields":{"include":["price","vector"],"scores":true}}'
```

## Errors

The nine tool error codes of [MCP tools](mcp-tools.md#errors) map to these
statuses, and the boundary adds `forbidden`. The body is `{"error":"<code>"}` and nothing else.

| Code | Status | When |
| --- | --- | --- |
| `invalid_argument` | `400` | The body or its arguments are invalid. `404` when `{namespace}` is not a namespace name. |
| `authorization_unavailable` | `503` | The caller's identity or policy cannot be decided. |
| `storage_unavailable` | `503` | Storage could not be read, or the head check of a [`strong`](#consistency) request failed. |
| `incompatible_index` | `503` | The published index does not match this reader. |
| `deadline_exceeded` | `408` | The deadline passed, in the queue or while executing. |
| `cancelled` | `499` | The request was cancelled; the caller has normally gone. |
| `stale_cursor` | `409` | The `fetch` cursor is expired, belongs to another subject or its content version changed. |
| `busy` | `503` | The in-flight limit, the concurrency cap or the queue is exhausted. |
| `rate_limited` | `429` | A per-user or per-group request limit was reached. |
| `forbidden` | `403` | An API key lacks the role the route needs or may not act in this namespace. This code is not a tool error: only the HTTP boundary answers it. |
| `too_many_namespaces` | `400` | The [namespace list](#listing-namespaces) matches more than 10,000 names. Only the HTTP boundary answers it. |

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
script needs `curl`, `jq` and `mktemp`. A completed scan replaces
`export.jsonl` with one hit per line. If a request fails, any previous export
stays in place. Read the text of a document with [`fetch`](mcp-tools.md#fetch).

```sh
url=https://fyrr.example.com/v1/namespaces/main/query
output=$(mktemp ./export.jsonl.XXXXXX) || exit 1
trap 'rm -f "$output"' EXIT
last=null
while :; do
  body=$(jq -n --argjson last "$last" '
    {order_by: [{field: "source_id", direction: "asc"}, {field: "root_id", direction: "asc"}],
     output: "metadata", limit: 1000}
    + ($last | if . == null then {} else {filter: {or: [
        {field: "source_id", op: "gt", value: .source_id},
        {and: [{field: "source_id", op: "eq", value: .source_id},
               {field: "root_id", op: "gt", value: .root_id}]}]}} end)') || exit 1
  page=$(curl -sf "$url" -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H "Content-Type: application/json" -d "$body") || exit 1
  count=$(printf '%s\n' "$page" | jq -e '.hits | arrays | length') || exit 1
  [ "$count" -gt 0 ] || break
  printf '%s\n' "$page" | jq -c '.hits[]' || exit 1
  last=$(printf '%s\n' "$page" | jq -c '.hits[-1] | {source_id, root_id}') || exit 1
done > "$output" || exit 1
mv -f "$output" export.jsonl
```

With `reader.max_response_bytes` set, a page can hold fewer hits than `limit`;
the script goes on from the last hit it got, so nothing is skipped. If not even
one hit fits the budget, the answer is `503 busy`, and the script stops.

## Write documents

> **In short**
>
> - `fyrr writer serve` with `[namespace.write_api]`, `[writer.serve.admin]`
>   and the committer serves `POST /v1/namespaces/{namespace}/documents` on
>   the writer listener.
> - `{namespace}` is any namespace name: the first request for a name creates
>   the namespace, and each namespace has its own lease and its own head.
> - A request upserts rows, patches rows and deletes ids, all or none. It is answered when
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
| `upserts` | Rows. A row is an object with an `id` (string, its key in the namespace), its text under the key `text_attribute`, `readers` (`usr:<id>` or `grp:<id>`; ignored by an open namespace), optionally its `vector` (a list of numbers) and a value for any declared [attribute](tuning.md#namespaceattributes). A row replaces the document with that `id`, readers included. |
| `patches` | Rows that change a document without replacing it. A row is an object with an `id`, and the attributes to write: a declared [attribute](tuning.md#namespaceattributes) (`null` clears it), `readers`, or the text under the key `text_attribute` with its `vector`. Everything a patch does not name stays as stored, the vector included, and nothing is embedded. A patch of an `id` that does not exist when the request is published is skipped and never creates a document. |
| `deletes` | IDs to delete. An ID that does not exist is a delete of nothing. |
| `conditions` | An object from an `id` of the request, in `upserts`, `patches` or `deletes`, to a [filter](mcp-tools.md#filters) that the stored document with that `id` must meet for the change to apply. A comparison's `value` may be `{"row": "<attribute>"}`, which is what the change's own row holds of that declared attribute (`null` when it holds none); a delete has no row. See [Conditions](#conditions). |
| `settings` | The [settings](tuning.md#namespace) of a namespace that does not exist yet, laid over the process defaults. For an existing namespace it must not change any: `409 namespace_settings_fixed`. |

A row that carries a `vector` is stored with it, and its text is not
embedded. The vector has the dimension of the namespace, finite float32
elements and a length above zero, or the request is `400 vector_invalid`. A
row without a vector is embedded by the [`[embedding]`](configuration.md#embedding)
provider; without one it is `400 embedding_not_configured`. A creating request
declares the vectors of the namespace in `settings.embedding` (`model` and
`dimensions`, which default to the process's; the dimension is a multiple of 8 or the request is `400 settings_invalid`); a request whose rows all carry
vectors makes 0 embedding calls and needs no `[embedding]` table. The profile `settings.embedding.fingerprint` is derived from them unless the request sends it; a fingerprint that does not match is `400 settings_invalid` for a new namespace and `409 namespace_settings_fixed` for an existing one.

`{namespace}` names the namespace to write: any namespace under
`<storage root>/namespaces/` that the key may write, not only
`namespace.name`. The settings of the file are the defaults of a namespace
that is created. The committer looks at a namespace besides the configured one
only while a request for it waits for its publication, so the namespaces that a
process has written cost an idle process nothing; the configured namespace is
looked at on every coalescing period as before. The writer keeps the history
evidence of the 16 namespaces it published to most recently. A restart forgets
which namespaces had an envelope waiting: an envelope whose request ended
without an answer is published with the next request for its namespace.

The namespace is created by the first request, with its settings, and a
request with only `settings` creates it. A row is one record: its text is at
most 16 KiB, and it is not chunked. The same `id` twice in one request is
`400 duplicate_id`, across `upserts`, `patches` and `deletes`. The answer is
`{"upserted":n,"deleted":m}`, the rows of the request. A request with `patches`
adds `"patched":p`, the patches of documents that exist, and `"skipped":q`, the
patches of those that do not (and the changes whose [condition](#conditions) is
not met), as the committer applied the request in the order of publication. A patch is weighed against `writer.commit.group_max_bytes` by the largest record it can write, capped at what is left of that limit, so no request is refused for the estimate alone; the committer refuses (`409 rejected`) a request whose rewritten records really exceed it. The counts are best effort: a patch of a document that a later
request of the same publication replaces or deletes counts as skipped.

### Conditions

Setting conditions needs an [API key](identity.md#identityapi_keys) with the
`admin` role besides `write`: whether a condition holds tells something about
a document that the key may not be allowed to read. A request with `conditions`
from a key without `admin` is `403 forbidden`.

The committer tests a condition against the stored document with that `id` when
it applies the change, in the order of publication, so it sees what every
request published before it left, also those it publishes in the same round.
Nothing is decided when the request is accepted. A condition that is not met
skips the change: it writes nothing, and the other changes of the request apply
as they would have. A document that is not there (never written, or deleted)
meets the condition of an upsert, which creates it; a patch or a delete of it is
skipped. A document meets a condition when one of its records does, which for a
row of this API is the row. A stored attribute that is missing is `null`, and
every comparison but `eq` with `null` is false for it
([how](mcp-tools.md#filters)).

An upsert that writes only when the stored `modified` is older than its own:

```json
{"upserts": [{"id": "a1", "text": "Blue shirt", "modified": "2026-02-01T00:00:00Z"}],
 "conditions": {"a1": {"field": "modified", "op": "lt", "value": {"row": "modified"}}}}
```

The answer of a request with a patch or a condition adds `"skipped":q`, the
changes that were skipped, and `upserted`, `deleted` and `patched` count the
changes that applied. The filter is checked as the request arrives: a field,
operator or value it does not allow, a reference to an attribute the namespace
does not declare, a condition for an `id` that is not in the request or a
reference to a row in the condition of a delete is `400 condition_invalid`.
Like the counts of patches, these are best effort: a change that a later
request of the same publication replaces or deletes counts as skipped.

A patch changes the text only together with the `vector` of the new text, which
replaces the stored one; nothing is embedded. A `vector` without the text is
`400 vector_not_patchable`. A text without a `vector` would keep the embedding of
the old text: it is `400 text_not_patchable` where an
[`[embedding]`](configuration.md#embedding) provider is configured and
`400 embedding_not_configured` where none is. A body over `writer.envelope_max_bytes` or with more rows than
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
| `forbidden` | `403` | The key lacks the `write` role or its `namespace_prefix` excludes the namespace, or the request sets `conditions` and the key lacks the `admin` role. |
| `not_found` | `404` | `{namespace}` is not a namespace name, or the process has no `[namespace.write_api]`. |
| `unsupported_media_type` | `415` | Not `application/json`, or a `Content-Encoding`. |
| `body_too_large`, `too_many_rows`, `row_too_large` (1 MiB), `settings_too_large` (64 KiB), `text_too_large`, `document_too_large`, `request_too_large` | `413` | A limit above. |
| `body_invalid`, `empty_request`, `id_missing`, `duplicate_id`, `text_missing`, `readers_invalid`, `vector_invalid`, `vector_not_patchable`, `text_not_patchable`, `embedding_not_configured`, `document_invalid`, `condition_invalid`, `settings_invalid`, `attribute_undeclared` and the value codes of [tuning](tuning.md#namespaceattributes) (`attribute_type_mismatch`, `attribute_value_invalid`, `attribute_value_too_large`) | `400` | The body or a row is invalid, or the settings of a namespace to be created are (a schema that is too large or has a bad name included: `settings_invalid`). |
| `namespace_settings_fixed`, `namespace_not_writable`, `namespace_not_empty`, `namespace_changed`, `rejected` | `409` | The namespace has other settings, takes its documents from connectors, is to be created over envelopes that a deleted namespace left, was replaced by another while the request was prepared, or the committer refused the request (nothing of it is applied). |
| `busy`, `unavailable`, `outcome_unknown` | `503` | The writer is busy, storage or the published keys cannot be read, or the answer did not come within 10 minutes; `Retry-After` is 1. |
