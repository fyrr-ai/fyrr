# fyrr-client

A Python client of the [fyrr](https://github.com/fyrr-ai/fyrr) HTTP API:
namespaces (list, describe, delete), writes, queries, and helpers that page
through every namespace and every document. It has one dependency, `httpx`, and
needs Python 3.11 or newer. It is a separate package: the `fyrr` binary contains
no Python.

```python
from fyrr_client import Client

with Client(
    "http://127.0.0.1:8090",  # reader: queries, namespace list and report
    "http://127.0.0.1:8091",  # writer: writes and namespace deletion
    api_key="fyrr_key_...",
) as client:
    client.write("notes", {"upserts": [{"id": "a1", "text": "Restore a backup"}]})
    found = client.query("notes", {"query": "backup", "consistency": "strong"})
    for hit in client.iter_documents("notes"):
        print(hit["root_id"])
```

`AsyncClient` has the same methods as coroutines, and `iter_namespaces` and
`iter_documents` as async iterators:

```python
async with AsyncClient(reader_url, writer_url, api_key=key) as client:
    async for hit in client.iter_documents("notes"):
        ...
```

| Method | Route | Served by |
| --- | --- | --- |
| `list_namespaces(prefix=None)` | `GET /v1/namespaces` | reader |
| `describe_namespace(namespace)` | `GET /v1/namespaces/{namespace}` | reader |
| `delete_namespace(namespace)` | `DELETE /v1/namespaces/{namespace}` | writer |
| `query(namespace, request)` | `POST /v1/namespaces/{namespace}/query` | reader |
| `write(namespace, request)` | `POST /v1/namespaces/{namespace}/documents` | writer |
| `iter_namespaces(prefix="")` | `GET /v1/namespaces`, narrowing the prefix | reader |
| `iter_documents(namespace, filter=None, page_size=1000, consistency=None)` | `query`, by the document key | reader |

The key is stripped of surrounding whitespace, so one read from a file works;
a key with a space, a control character or a non-ASCII character is refused
with a `ValueError` that does not show it.

`writer_url` is optional: a client without it refuses the writer routes with a
`ValueError`. The API key (or OIDC access token) needs the role of each route
([API reference](https://github.com/fyrr-ai/fyrr/blob/main/docs/api-reference.md)).
The other routes (`fetch`, `aggregate`, `recall`, freezing a namespace) are not
wrapped yet; their request and response types are in `fyrr_client.models`.

## Types

Requests and responses are the `TypedDict` classes of `fyrr_client.models`,
generated from [`schemas/openapi.json`](https://github.com/fyrr-ai/fyrr/blob/main/schemas/openapi.json).
A request is a plain `dict` and an answer is the decoded JSON, which is not
validated: a server newer than the package may add fields. A row of a write is
a `dict[str, Any]` (`WriteRow`): `id`, `text` and any declared attribute.
After a change of the OpenAPI file, regenerate the models and review the diff:

```sh
cd clients/python && uv run python tools/generate_models.py
```

`scripts/ci` fails when `models.py` is stale (`--check`).

## Connections and timeouts

A client keeps one connection pool for the reader and the writer; close it, or
use it in a `with` block. The default `timeout` is **30 seconds**, above the
10 seconds that the server gives a read before it answers
`408 deadline_exceeded` (`deadline_ms`). A request with a longer `deadline_ms`
(at most 300,000) needs a larger `timeout`. A write is answered when readers
serve it, and the server may take up to 10 minutes for that before it answers
`503 outcome_unknown`: a client that writes large batches should set a larger
`timeout`. A timeout of a write is raised, not retried (below).

## Retries

A **read** (`query`, `list_namespaces`, `describe_namespace` and the paging
helpers) is retried up to `max_retries` times (default 3) on a connection
error, a timeout, and the statuses 408, 429 and 5xx. The wait is `backoff`
seconds (default 0.5) doubling with each attempt, at most 30 seconds, with
jitter. When the answer has `Retry-After`, the client waits that long instead
(at most 120 seconds; a longer `Retry-After` is raised with `retry_after` set,
not waited for). A request that is wrong, such as an unsupported URL
scheme, is not retried.

A **write** and `delete_namespace` change data, and a lost answer does not say
whether they applied. A repeat could overwrite a later change to the same ids,
evaluate `delete_where` against newer documents, or delete a namespace that was
created again ([guarantees](https://github.com/fyrr-ai/fyrr/blob/main/docs/guarantees.md)).
They are retried only when the request provably did not reach the server (the
connection could not be made): no answer, not even a `429` from a proxy, proves
that it was not applied. A write
that sets `conditions` is **never** retried. Every other failure, a timeout or
`503 outcome_unknown` included, is raised for the caller to decide.

The last answer is raised as `FyrrError` (`status`, `code`, `retry_after`); a
connection error or a timeout is raised as the `httpx` error.

## Paging

`iter_documents` scans a namespace by the document key in pages of `page_size`
(at most 1000) and yields every document the key may read once, as the hits of
a `query` with `output: "metadata"`; read the text with `fetch`. Pages are not
a snapshot: documents written during the scan may or may not appear. `filter`
narrows the scan.

`iter_namespaces` lists the names that start with `prefix` in name order. The
API answers `too_many_namespaces` above 10,000 names; the helper then lists
each first character instead. If one of those lists is still too long, the
helper raises that error rather than return a list that may lack a name: the
name equal to a prefix is listed by no narrower prefix. Give a longer `prefix`
to list a part of the names.

## Tests

The tests start a local `file://` deployment with `examples/guides/start` and
need the `fyrr` binary on `PATH`. From a checkout, `scripts/ci --step="python client"`
builds it, then runs the type check, the lint, the drift check of the models
and the tests.
