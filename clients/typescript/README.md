# fyrr-client

A TypeScript client of the [fyrr](https://github.com/fyrr-ai/fyrr) HTTP API:
namespaces (list, describe, delete), writes, queries, and helpers that page
through every namespace and every document. It has no runtime dependency: it
uses `node:http` and `node:https`, needs Node.js 22 or newer, and is tested on
24.21.0. It is a separate package: the `fyrr` binary contains no JavaScript.

**Server runtimes only.** The reader refuses a request that carries an `Origin`
header, and a browser adds one to a request to another origin, so this client
does not work in a browser. Node.js sends no `Origin`.

```ts
import { Client } from "fyrr-client";

const client = new Client({
  readerUrl: "http://127.0.0.1:8090", // queries, namespace list and report
  writerUrl: "http://127.0.0.1:8091", // writes and namespace deletion
  apiKey: "fyrr_key_...",
});

await client.write("notes", { upserts: [{ id: "a1", text: "Restore a backup" }] });
const found = await client.query("notes", { query: "backup", consistency: "strong" });
for await (const hit of client.iterDocuments("notes")) {
  console.log(hit.root_id);
}
```

| Method | Route | Served by |
| --- | --- | --- |
| `listNamespaces(prefix?)` | `GET /v1/namespaces` | reader |
| `describeNamespace(namespace)` | `GET /v1/namespaces/{namespace}` | reader |
| `deleteNamespace(namespace)` | `DELETE /v1/namespaces/{namespace}` | writer |
| `query(namespace, request)` | `POST /v1/namespaces/{namespace}/query` | reader |
| `write(namespace, request)` | `POST /v1/namespaces/{namespace}/documents` | writer |
| `iterNamespaces(prefix = "")` | `GET /v1/namespaces`, narrowing the prefix | reader |
| `iterDocuments(namespace, { filter, pageSize = 1000, consistency })` | `query`, by the document key | reader |

The key is stripped of surrounding whitespace, so one read from a file works;
a key with a space, a control character or a non-ASCII character is refused
with a `TypeError` that does not show it. The key is kept in a private field:
`console.log(client)`, `util.inspect` and `JSON.stringify` do not show it, and
no error carries it. A URL that is not a plain `http` or `https` base URL (one
with a user or password, a query or a fragment), and a timeout or retry count that
is not a number in range, are refused when the client is made.

`writerUrl` is optional: a client without it rejects the writer routes. The API
key (or OIDC access token) needs the role of each route
([API reference](https://github.com/fyrr-ai/fyrr/blob/main/docs/api-reference.md)).
The other routes (`fetch`, `aggregate`, `recall`, freezing a namespace) are not
wrapped yet; their request and response types are exported.

## Types

Requests and responses are the interfaces of `src/models.ts`, exported from the
package and generated from
[`schemas/openapi.json`](https://github.com/fyrr-ai/fyrr/blob/main/schemas/openapi.json).
A request is a plain object and an answer is the decoded JSON, which is not
validated: a server newer than the package may add fields.

**Large integers.** An integer in an answer that a `number` cannot hold exactly
(beyond ±2^53 − 1) is a `bigint`; every other integer is a `number`. The 64-bit
fields are typed `number | bigint` for that reason. A `bigint` in a request is
written as the exact integer. A row of a write
(`WriteRow`) has `id`, `text` and any declared attribute. After a change of the
OpenAPI file, regenerate the models and review the diff:

```sh
cd clients/typescript && npm run generate
```

The workflow of the client fails when `src/models.ts` is stale
(`npm run check:models`).

## Connections and timeouts

The clients of a process share keep-alive agents of `node:http` and
`node:https`, so connections are reused and there is nothing to close. The
transport asks for `Accept-Encoding: identity` and refuses an answer with another
`Content-Encoding`, or a `101`, with an error. It sends each request once: it follows no redirect and repeats nothing,
a `421` included, so only the retry policy below ever sends a request again. Pass
your own `transport` (a function from a request to a response, which may wrap
the exported `nodeTransport`) to use a proxy. The default `timeoutMs` is **30,000**, above the 10 seconds that
the server gives a read before it answers `408 deadline_exceeded`
(`deadline_ms`), and it applies to each attempt. A request with a longer
`deadline_ms` (at most 300,000) needs a larger `timeoutMs`. A write is answered
when readers serve it, and the server may take up to 10 minutes for that before
it answers `503 outcome_unknown`; and `deleteNamespace` waits until the lease of
the namespace's writer has expired, a minute by default. A client that writes
large batches or deletes namespaces should set a larger `timeoutMs`. A timeout
of a write is raised, not retried (below).

## Retries

A **read** (`query`, `listNamespaces`, `describeNamespace` and the paging
helpers) is retried up to `maxRetries` times (default 3) on a connection error,
a timeout, and the statuses 408, 429 and 5xx. The wait is `backoffMs`
milliseconds (default 500) doubling with each attempt, at most 30 seconds, with
jitter. When the answer has `Retry-After`, the client waits that long instead
(at most 120 seconds; a longer `Retry-After` is raised with `retryAfterMs` set,
not waited for). A request that is wrong, such as a header with an invalid character, and
a TLS certificate error are not retried.

A **write** and `deleteNamespace` change data, and a lost answer does not say
whether they applied. A repeat could overwrite a later change to the same ids,
evaluate `delete_where` against newer documents, or delete a namespace that was
created again ([guarantees](https://github.com/fyrr-ai/fyrr/blob/main/docs/guarantees.md)).
They are retried only when the request provably did not reach the server (the
connection could not be made: `ECONNREFUSED`, `ENOTFOUND`, `EAI_AGAIN`,
`EHOSTUNREACH` or `ENETUNREACH` from `connect` or `getaddrinfo`; the same codes
from `read` come after the request was written and do not count): no answer, not even a `429` from a proxy, proves
that it was not applied. A write that sets a non-empty `conditions` is **never** retried.
Every other failure, a timeout or `503 outcome_unknown` included, is raised for
the caller to decide.

The last answer is raised as `FyrrError` (`status`, `code`, `retryAfterMs`); a
connection error or a timeout is raised as the error of `node:http`, or as the
`TimeoutError` of the timeout.

## Paging

`iterDocuments` scans a namespace by the document key in pages of `pageSize`
(at most 1000) and yields every document the key may read once, as the hits of
a `query` with `output: "metadata"`; read the text with `fetch`. Pages are not
a snapshot: documents written during the scan may or may not appear. `filter`
narrows the scan.

`iterNamespaces` lists the names that start with `prefix` in name order. The
API answers `too_many_namespaces` above 10,000 names; the helper then lists
each first character instead. If one of those lists is still too long, the
helper raises that error rather than return a list that may lack a name: the
name equal to a prefix is listed by no narrower prefix. Give a longer `prefix`
to list a part of the names.

## Tests

The tests run with `node --test`. Those of `tests/deployment.test.ts` start a
local `file://` deployment with `examples/guides/start` and need the full
`fyrr` binary on `PATH` (`cargo build -p fyrr`, then `target/debug`); the others
need nothing. In a checkout:

```sh
cd clients/typescript
npm ci
npm run typecheck && npm run check:models && npm test
```

The [`typescript-client` workflow](https://github.com/fyrr-ai/fyrr/blob/main/.github/workflows/typescript-client.yml)
runs these on Node.js 24.21.0, the version of `.node-version`.
