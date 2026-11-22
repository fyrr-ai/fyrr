# HTTP JSON API

> **In short**
>
> - A reader with `[reader.http]` also serves `search` and `fetch` as plain
>   JSON: `POST /v1/namespaces/{namespace}/query` and
>   `POST /v1/namespaces/{namespace}/fetch`.
> - They take the arguments of the [MCP tools](mcp-tools.md) and return their
>   structured results. Authentication, permissions, deadlines, rate limits
>   and the scheduler are those of `/mcp`.
> - [Errors](#errors) are an HTTP status and `{"error":"<code>"}`; 429 and
>   503 carry `Retry-After`.

## Routes

| Route | Body | Answer |
| --- | --- | --- |
| `POST /v1/namespaces/{namespace}/query` | The arguments of [`search`](mcp-tools.md#search) | The `search` result |
| `POST /v1/namespaces/{namespace}/fetch` | The arguments of [`fetch`](mcp-tools.md#fetch) | The `fetch` result |

`{namespace}` is `namespace.name`. A reader serves one namespace, so any
other value answers `404`. `list_filters` has no JSON route.

Both routes, like `/mcp`, need the `read` role of an
[API key](identity.md#identityapi_keys); users signed in through OIDC hold
`read` only. A key without the role, or limited by `namespace_prefix` to other
namespaces, answers `403 forbidden`.

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
[limits](#limits), the bearer token and the caller's rate, the namespace,
the arguments, then the scheduler and the permission checks that every
tool call passes ([how](architecture.md#authorization)). `deadline_ms`
counts from arrival, as for MCP.

## Errors

The nine tool error codes of [MCP tools](mcp-tools.md#errors) map to these
statuses, and the boundary adds `forbidden`. The body is `{"error":"<code>"}` and nothing else.

| Code | Status | When |
| --- | --- | --- |
| `invalid_argument` | `400` | The body or its arguments are invalid. `404` when `{namespace}` is not the namespace the reader serves. |
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
| Headers | 64 fields, 32 KiB | `431 invalid_argument`. |
| Requests per user and per group | `user_requests_per_minute`, `group_requests_per_minute` (default 120) | `429 rate_limited`. |
| Deadline | `deadline_ms`, default 10 s, at most 300 s | `408 deadline_exceeded`. |
