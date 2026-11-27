# API reference

> **In short**
>
> - One section per route of [`schemas/openapi.json`](../schemas/openapi.json),
>   the OpenAPI 3.1 description of the HTTP API. The sections are rendered from
>   that file: `scripts/check-docs` fails when they differ, and
>   `scripts/check-docs --render` rewrites them.
> - Each section says whether the **reader** or the **writer** serves the route.
>   The reader serves it on `[reader.http]`; the writer serves it on the
>   administration listener of `fyrr writer serve` (`[writer.serve.admin]`), and
>   the documents route also needs `[namespace.write_api]`.
> - The behavior of a route is in [HTTP JSON API](http-api.md); this page
>   lists the routes. The reader routes answer errors as
>   `{"error":"<code>"}` ([codes](http-api.md#errors)); the write route and the
>   writer's `PATCH` and `DELETE` of a namespace likewise
>   ([codes](http-api.md#write-documents)).

Every route but `GET /metrics` takes `Authorization: Bearer <token>`: an
[API key](identity.md#identityapi_keys), or where a section says so an OIDC
access token. A key holds roles (`read`, `write`, `list`, `admin`); a key
whose role or `namespace_prefix` does not fit the route answers
`403 forbidden`. A write request with `conditions`, `delete_where` or
`patch_where` needs the `admin` role besides `write`. `{namespace}` is a
namespace name: lowercase letters, digits and `-`, at most 63 characters, not
starting with `-`.

<!-- routes:begin (scripts/check-docs --render) -->

## `GET /metrics`

Prometheus metrics of the reader or the writer

Served by: reader and writer.

Needs: Served only on the extra listener of `reader.http.metrics_listen` or `writer.serve.metrics_listen`, with no credential. No label or value carries a namespace name, an identity, a query or document text.

## `GET /v1/namespaces`

List the namespaces the key may act in

Served by: reader.

Needs: Needs an API key with the `admin` or `list` role.

Answer: `NamespaceList`.

## `GET /v1/namespaces/{namespace}`

Report a namespace's schema, settings, size and index state

Served by: reader.

Needs: Needs an API key with the `admin` or `list` role.

Answer: `NamespaceReport`.

## `PATCH /v1/namespaces/{namespace}`

Freeze a namespace or thaw it

Served by: writer.

Needs: Needs an API key with the `admin` role.

Body: `PatchNamespaceRequest`. Answer: `PatchNamespaceResponse`.

## `DELETE /v1/namespaces/{namespace}`

Delete a namespace and its data

Served by: writer.

Needs: Needs an API key with the `admin` role.

Body: `DeleteNamespaceRequest`. Answer: `DeleteNamespaceResponse`.

## `POST /v1/namespaces/{namespace}/query`

Search the namespace: the `search` tool as JSON

Served by: reader.

Needs: Needs an API key with the `read` role, or an OIDC user.

Body: `QueryRequest`. Answer: `SearchOutput`.

## `POST /v1/namespaces/{namespace}/fetch`

Fetch documents of the namespace: the `fetch` tool as JSON

Served by: reader.

Needs: Needs an API key with the `read` role, or an OIDC user.

Body: `FetchRequest`. Answer: `FetchOutput`.

## `POST /v1/namespaces/{namespace}/aggregate`

Aggregate the documents the key may read: count, sum, average, minimum and maximum, optionally per group

Served by: reader.

Needs: Needs an API key with the `read` role, or an OIDC user.

Body: `AggregateRequest`. Answer: `AggregateOutput`.

## `POST /v1/namespaces/{namespace}/recall`

Measure how closely the vector index agrees with an exact search, over stored vectors the key may read

Served by: reader.

Needs: Needs an API key with the `admin` and the `read` role.

Body: `RecallRequest`. Answer: `RecallOutput`.

## `POST /v1/namespaces/{namespace}/documents`

Upsert, patch and delete documents, all or none; or change the schema

Served by: writer.

Needs: Needs an API key with the `write` role.

Body: `WriteRequest`. Answer: `WriteResponse`.

<!-- routes:end -->
