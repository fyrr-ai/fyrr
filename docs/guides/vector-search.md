# Vector search

> **In short**
>
> - A row can carry a `vector`. A search with a `vector` ranks the rows by
>   their distance to it, and makes no embedding call.
> - The distance is the namespace's `dense_metric`: `cosine` (the default),
>   `squared_l2` or `dot`. It is chosen in the first write and fixed for the
>   namespace's life.
> - A `filter` narrows the rows before the ranking, and `exact: true` scores
>   every row instead of probing the index.

Use this guide when your vectors come from your own model. If fyrr should
embed the text, send the text: a row without a `vector` is embedded by the
`[embedding]` provider, and a `query` is embedded the same way
([hybrid search](hybrid-search.md)).

Start fyrr as in [Test against local storage](testing.md):

```sh
source examples/guides/start
```

## 1. Create a namespace for your vectors

The first write creates the namespace. Its `settings` say how long a vector is
(a multiple of 8) and which attributes a row may have:

```sh
curl -fsS "$WRITER/v1/namespaces/vectors/documents" \
  -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
  -d '{"settings":{"embedding":{"model":"my-model","dimensions":8},
                   "attributes":{"kind":"string"}},
       "upserts":[
         {"id":"short","text":"short","kind":"a","vector":[1,0,0,0,0,0,0,0]},
         {"id":"long","text":"long","kind":"b","vector":[4,2,0,0,0,0,0,0]},
         {"id":"side","text":"side","kind":"a","vector":[0,1,0,0,0,0,0,0]}]}'
```

```text
{"upserted":3,"deleted":0}
```

A vector has the namespace's dimensions and finite numbers, and is not all
zeros, or the write is `400 vector_invalid`. Each row also has a `text`, which
is what a hit returns; it is not embedded when the row has a vector.

## 2. Search by vector

```sh
curl -fsS "$READER/v1/namespaces/vectors/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"vector":[1,0,0,0,0,0,0,0],"limit":3,"consistency":"strong"}' |
  jq -r '.hits[].root_id'
```

```text
short
long
side
```

`root_id` is the `id` of the row. A search with only a `vector` uses the dense
lane alone: there is no text, so nothing is matched by words. A vector of the
wrong length, or all zeros, is `400 invalid_argument`.

## 3. Choose the metric

The metric decides what "near" means. Create a second namespace with the same
rows and `dense_metric: "dot"`, and search it with the same vector:

```sh
curl -fsS "$WRITER/v1/namespaces/vectors-dot/documents" \
  -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
  -d '{"settings":{"embedding":{"model":"my-model","dimensions":8},
                   "dense_metric":"dot"},
       "upserts":[
         {"id":"short","text":"short","vector":[1,0,0,0,0,0,0,0]},
         {"id":"long","text":"long","vector":[4,2,0,0,0,0,0,0]},
         {"id":"side","text":"side","vector":[0,1,0,0,0,0,0,0]}]}'
```

```text
{"upserted":3,"deleted":0}
```

```sh
curl -fsS "$READER/v1/namespaces/vectors-dot/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"vector":[1,0,0,0,0,0,0,0],"limit":3,"consistency":"strong"}' |
  jq -r '.hits[].root_id'
```

```text
long
short
side
```

`cosine` ignores a vector's length, so `short` and `long` point mostly the same
way and `short` is the exact match. `dot` ranks by the dot product, so the
longer vector wins. `squared_l2` is the squared Euclidean distance. Use the
metric your model was trained for. The namespace report says which one a
namespace has ([`dense_metric`](../http-api.md#namespace-report)); to change it,
write the rows into a new namespace.

## 4. Filter and scan exactly

A `filter` on a declared attribute is applied with the caller's permissions
before the ranking, so the limit is filled with rows that qualify:

```sh
curl -fsS "$READER/v1/namespaces/vectors/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"vector":[1,0,0,0,0,0,0,0],"limit":3,"consistency":"strong",
       "filter":{"field":"kind","op":"eq","value":"b"}}' |
  jq -r '.hits[].root_id'
```

```text
long
```

The vector index is approximate: it probes part of the namespace. To score
every row that passes the filter and the caller's permissions instead, ask for
an exact scan. It is bounded to 100,000 rows ([limits](../limits.md)):

```sh
curl -fsS "$READER/v1/namespaces/vectors/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"vector":[0,1,0,0,0,0,0,0],"limit":1,"exact":true,"consistency":"strong"}' |
  jq -r '.hits[].root_id'
```

```text
side
```

To measure how closely the index agrees with an exact search on your own data,
use [recall](../http-api.md#recall).

## 5. Stop

```sh
fyrr_stop
```
