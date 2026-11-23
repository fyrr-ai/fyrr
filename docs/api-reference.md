# API reference

> **In short**
>
> - One section per route of [`schemas/openapi.json`](../schemas/openapi.json),
>   the OpenAPI 3.1 description of the HTTP API. `scripts/check-docs` fails when a route has no
>   section, a section has no route, or a section disagrees with the file on
>   the process that serves the route, the role it needs or the names of its
>   body and answer.
> - Each section says whether the **reader** or the **writer** serves the route.
>   The reader serves it on `[reader.http]`; the writer serves it on the
>   listener of `fyrr writer serve` that the section names.
> - The behavior of a route is in [HTTP JSON API](http-api.md); this page is
>   the index to it. Errors are [one table](http-api.md#errors) for the reader routes and
>   [one](http-api.md#write-documents) for the write route; every `default`
>   answer of the file is `{"error":"<code>"}` with an HTTP status.

Every route but `GET /metrics` takes `Authorization: Bearer <token>`: an
[API key](identity.md#identityapi_keys), or where a section says so an OIDC
access token. A key holds roles (`read`, `write`, `list`, `admin`); a key
whose role or `namespace_prefix` does not fit the route answers
`403 forbidden`. `{namespace}` is a namespace name: lowercase letters,
digits and `-`, at most 63 characters, not starting with `-`.

## `GET /metrics`

Prometheus metrics of the reader or the writer

Served by: reader and writer, on the optional metrics listener only
(`reader.http.metrics_listen`, `writer.serve.metrics_listen`).

Needs: Served only on the extra listener of `reader.http.metrics_listen` or `writer.serve.metrics_listen`, with no credential. No label or value carries a namespace name, an identity, a query or document text.

Details: [Metrics](operations.md#metrics).

## `GET /v1/namespaces`

List the namespaces the key may act in

Served by: reader.

Needs: Needs an API key with the `admin` or `list` role.

Answer: `NamespaceList`. Details: [Listing namespaces](http-api.md#listing-namespaces).

## `GET /v1/namespaces/{namespace}`

Report a namespace's schema, settings, size and index state

Served by: reader.

Needs: Needs an API key with the `admin` or `list` role.

Answer: `NamespaceReport`. Details: [Namespace report](http-api.md#namespace-report).

## `PATCH /v1/namespaces/{namespace}`

Freeze a namespace or thaw it

Served by: writer, on the administration listener (`[writer.serve.admin]`).

Needs: Needs an API key with the `admin` role.

Body: `PatchNamespaceRequest`. Answer: `PatchNamespaceResponse`.
Details: [Freezing a namespace](http-api.md#freezing-a-namespace).

## `DELETE /v1/namespaces/{namespace}`

Delete a namespace and its data

Served by: writer, on the administration listener (`[writer.serve.admin]`).

Needs: Needs an API key with the `admin` role.

Body: `DeleteNamespaceRequest`, the name repeated. Answer: `DeleteNamespaceResponse`.
Details: [`fyrr writer namespace delete`](cli.md), which does the same, and
[Identity and access](identity.md#identityapi_keys).

## `POST /v1/namespaces/{namespace}/query`

Search the namespace: the `search` tool as JSON

Served by: reader.

Needs: Needs an API key with the `read` role, or an OIDC user.

Body: `QueryRequest`. Answer: `SearchOutput`. Details:
[`search`](mcp-tools.md#search), [HTTP JSON API](http-api.md#routes).

## `POST /v1/namespaces/{namespace}/fetch`

Fetch documents of the namespace: the `fetch` tool as JSON

Served by: reader.

Needs: Needs an API key with the `read` role, or an OIDC user.

Body: `FetchRequest`. Answer: `FetchOutput`. Details:
[`fetch`](mcp-tools.md#fetch), [HTTP JSON API](http-api.md#routes).

## `POST /v1/namespaces/{namespace}/aggregate`

Aggregate the documents the key may read: count, sum, average, minimum and maximum, optionally per group

Served by: reader.

Needs: Needs an API key with the `read` role, or an OIDC user.

Body: `AggregateRequest`. Answer: `AggregateOutput`. Details: [Aggregate](http-api.md#aggregate).

## `POST /v1/namespaces/{namespace}/recall`

Measure how closely the vector index agrees with an exact search, over stored vectors the key may read

Served by: reader.

Needs: Needs an API key with the `admin` and the `read` role.

Body: `RecallRequest`. Answer: `RecallOutput`. Details: [Recall](http-api.md#recall).

## `POST /v1/namespaces/{namespace}/documents`

Upsert, patch and delete documents, all or none; or change the schema

Served by: writer, on the administration listener (`[writer.serve.admin]`), with `[namespace.write_api]`.

Needs: Needs an API key with the `write` role.

Body: `WriteRequest`. Answer: `WriteResponse`. Details:
[Write documents](http-api.md#write-documents); what the answer promises is in
[Guarantees](guarantees.md).
