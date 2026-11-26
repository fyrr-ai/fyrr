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

[`schemas/openapi.json`](../schemas/openapi.json) describes these routes and the
[write route](#write-documents) as OpenAPI 3.1; it ships in the release as
`fyrr-openapi-<version>.json`.

The [API reference](api-reference.md) lists every route with the process that
serves it, the role it needs, and its body and answer. This page describes the
reader's `/v1/` routes and the write route.

`GET /metrics` is none of these: only the optional
[metrics listener](operations.md#metrics) serves it, on its own address and
without a credential. The OpenAPI file describes it too.

`{namespace}` names the namespace to read: any namespace under
`<storage root>/namespaces/`, not only `namespace.name`. The reader opens it
on the first request, with the settings it is published with, and keeps it
open for later ones. It keeps at most 8 namespaces open besides the
configured one, and each holds a place until its scheduler has stopped; a
request that finds none waits up to 5 seconds, then answers `503 busy`. When
another namespace needs a place, the one used least recently that is serving no
request is closed, and if every one is serving, the request answers `503 busy`.
All of them keep their files in the mirror of the configured namespace, under
`reader.mirror_path` (`$XDG_CACHE_HOME/fyrr/<namespace.name>`, or
`$HOME/.cache/fyrr/<namespace.name>` when that is unset, if the setting is
absent), so the open namespaces together stay inside `reader.mirror_max_bytes`.
Only files that no open namespace serves from are evicted, oldest first. A
namespace whose files would fit the mirror alone, but find no room, asks the
reader to close the least recently used other one that serves no request, and
warms again once that has ended. With no such namespace it is read from storage
without keeping the files, which is slower and as correct. The 8 share the
reader's `reader.memory_max_bytes` with
the configured namespace: a namespace that the memory left cannot fund in full
closes the least recently used one that serves no request, and opens again, so
the open ones stay inside the budget. Each gets an eighth of the 128 MiB
subject cache the configured namespace has, so together they add one more cache
budget of the configured size; a reader with API keys takes the memory of those
caches from `reader.memory_max_bytes` too. One that has served no request for
`reader.version_check_interval` makes no storage request until the next one,
which reads the head first and is answered from that read. Embedding and rerank
spend limits are per process: every namespace charges the same counters.
`/mcp`, `/readyz` and `/admin/refresh` keep serving the configured namespace
alone. `list_filters` has no JSON route.

`query`, `fetch` and `aggregate`, like `/mcp`, need the `read` role of an
[API key](identity.md#identityapi_keys); [`recall`](#recall) needs `admin` and `read`.
Users signed in through OIDC hold `read` only. Other namespaces than the configured one are for API keys: an OIDC
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
| `dense_metric`, `dense_element` | The distance (`cosine`, `squared_l2` or `dot`) and the element type (`f16` or `f32`) of the namespace's vectors, fixed when it was created. |
| `settings.vectors` | The namespace's named [vector attributes](tuning.md#namespacevectors); absent when it has none. |
| `read_only` | Whether the namespace is [frozen](#freezing-a-namespace). Unlike the rest, it is read from the current head (the one the reader's authorization checks share), because a switch is published without a new version. |
| `rows` | Approximate live rows (a document is one or more rows). |
| `logical_bytes` | Approximate bytes of the data files of those rows, without indexes. |
| `first_published_at` | When the namespace's first content publication was committed. |
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

## Freezing a namespace

`PATCH /v1/namespaces/{namespace}` on the writer's administration listener
takes `{"read_only": true}` to freeze the namespace and `{"read_only": false}`
to thaw it, and answers `{"namespace","read_only","changed"}`; `changed` is
`false` when the namespace was already so. It needs an API key with the `admin`
role whose `namespace_prefix` allows the namespace. The CLI does the same:
`fyrr writer namespace read-only true` (or `false`).

While the namespace is frozen it stays readable, and:

- a document write is `409 namespace_read_only`;
- a connector sync fails with `namespace_read_only`, whatever its kind;
- maintenance, retiring and purging a source, a restore and the switch itself
  still work.

The switch is a new policy generation published under the namespace lease. A
writer judges it against the published head when an envelope is about to become
durable, not only when its request arrived, so a request that was accepted
before the switch but had not written its envelope yet is refused too. An
envelope that was durable before the switch is still committed, also after it.
The committer holds the namespace lease while it publishes, so a switch made
then answers `503 busy` and is repeated. `PATCH` and `DELETE` of a namespace answer
errors as the [write route](#write-documents) does, `{"error":"<code>"}`.

The check and the write of an envelope are not atomic with the switch: a
request whose check ran before the switch was published can still write its
envelope after it, and the committer publishes it, as a request that came
before the switch. So the answer of the switch does not mean that the namespace
is quiet. To copy a stable namespace, freeze it, wait until `fyrr writer status`
shows nothing pending and until `writer.commit.lease_ttl` has passed (the
longest a writer holds its cursor lease without renewing it), then copy.

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

`query`, `fetch` and `aggregate` take one more member besides the arguments of the tool,
`consistency`. It is no argument of the tool and `/mcp` has no such member. As for
every optional member at the top level of these routes, `null` means the default; a
member that is required stays required.

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
| `highlights` | `true` adds `highlights` to the excerpt of each hit: the `[start, end)` ranges of its tokens that match the query, in the same Unicode codepoints of the record as the excerpt's `start` and `end`. A token matches when the namespace's full-text analyzer (`[namespace.analyzer]`) reduces it to a token of the query, so a stemmed match is marked. An excerpt marks at most 100 ranges; one with no match has `[]`. Only the excerpt output has them. The excerpt itself is chosen with the same tokens, so a passage with a stemmed or folded match can be the one returned. |

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

## Sub-queries

The `query` route takes one more member, `sub_queries`, that searches several
ways in one request: 1 to 16 searches, each with its own filter, ranking and
limit, instead of `query`, `queries` and `vector`. It is no argument of the
tool: `fetch` and `/mcp` refuse it, and their output does not change. A request
without it is answered as before.

| Member of a sub-query | Effect |
| --- | --- |
| `query` | The text to search, 1 to 4096 bytes: a lexical search and a dense one by its embedding. |
| `vector` | The query vector, of the namespace's dimensions, instead of an embedding of `query`. Alone it searches the dense lane only. A sub-query needs `query`, `vector` or both. |
| `vector_attribute` | The named [vector attribute](tuning.md#namespacevectors) this sub-query searches, as for [`search`](mcp-tools.md#search); default the built-in `vector`. |
| `vectors` | The query vectors of a `multi` `vector_attribute`, as for [`search`](mcp-tools.md#search), instead of `vector`. |
| `filter` | A [filter tree](mcp-tools.md#filters) for this sub-query. |
| `limit` | Required: 1 to 75 hits. |

The request's own filters (`sources`, `projects`, `filter` and the others)
apply to every sub-query as well, joined with the sub-query's `filter` by
`and`. Every sub-query reads the same published version, and its documents
are admitted by the caller's permissions and then by its own filter, as those
of any other request are: a filter cannot reach a document the caller may not
read. Such a document is no hit, and no error.

Without `fusion` the hits of the sub-queries follow one another in request
order, each in its own order, and a hit's `query_index` names its sub-query. A
document that several sub-queries find is a hit of each. The `limit`s add up
to at most the namespace's `max_documents` (75 by default), else the request
is `400 invalid_argument`. With [`fusion`](#fusion-and-reranking), the answer
is one ranking of the documents of all sub-queries, and the request's `limit`
(default 20, at most `max_documents`) is its length; the `limit` of a
sub-query is then how many documents it gives to the ranking. `weights` has
one number per sub-query.

`output`, `text_hits`, `excerpt_chars`, `fields`, `rerank`, `deadline_ms` and
`consistency` apply to the whole request, and the deadline to the sum of
the sub-queries. `text_hits` counts per sub-query, or with `fusion` over the
fused ranking. A sub-query without text is
reranked only when `rerank.query` names the text. `query`, `queries`,
`vector`, `vectors`, `vector_attribute`, `exact`, `order`, `order_by` and `offset` are refused with
`sub_queries`, and so is a request `limit` without `fusion`.

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sub_queries":[
         {"query":"backup policy","limit":5,
          "filter":{"field":"project","op":"eq","value":"ops"}},
         {"vector":[0.12,-0.40],"limit":3}]}'
```

## Fusion and reranking

The `query` route takes two more members that choose, for one request, what
the namespace's settings otherwise decide. Neither is an argument of the tool:
`fetch` and `/mcp` refuse them, and their output does not change. A request
without them is answered as before.

`fusion` applies to a request with `queries` or [`sub_queries`](#sub-queries).
Without it, the hits of the sub-queries of `queries` are merged round-robin and
`limit` counts the hits of each sub-query. With it, the answer is one ranking of
the documents of all sub-queries, at most `limit` hits and at most
`max_documents` documents long.

| Member | Effect |
| --- | --- |
| `method` | Required: `rrf`, reciprocal rank fusion. A document scores the sum of `weight / (rank_constant + rank)` over the sub-queries that return it, with `rank` counted from 1 in the sub-query's own order, after the reranker. |
| `rank_constant` | A positive integer; default 60. |
| `weights` | One number from 0 to 1,000,000 per sub-query, in request order; default 1 each. |

A hit's `score` is its fused score, and its `query_index` names the sub-query
that gave it most.

`rerank` acts on the reranker the namespace is configured with. It adds no
candidates, calls, spend or time: the namespace's candidate count, deadline and
spend limit apply as they do to any request, and a namespace without a reranker
answers `400 invalid_argument` to a `rerank` that does not switch it off.

| Member | Effect |
| --- | --- |
| `enabled` | `false` makes no reranker call for this request; default `true`. |
| `attribute` | The value each candidate is scored by: a typed `string` attribute, or the namespace's text attribute. Default the text of the record. Any other name is `400 invalid_argument`. |
| `query` | The text candidates are scored against, for every sub-query. Default the text of each sub-query. A search by `vector` alone is reranked only when this is set. |

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"queries":["backup policy","restore runbook"],
       "fusion":{"method":"rrf","weights":[2,1]},
       "rerank":{"query":"how do we restore a backup"}}'
```

## Matching tokens

The `query` route takes four more members that decide which documents a text
or a vector ranking may return, and by which text it scores them. None is an
argument of the tool: `fetch` and `/mcp` refuse them, and their output does not
change. A request without them is answered as before. None is taken together
with `sub_queries`, which have filters and rankings of their own: that is
`400 invalid_argument`.

`query_match` applies to the text of `query` or `queries`. `any`, the default,
ranks the documents that hold at least one term; `all` ranks only the documents
that hold every term. It narrows the text lane only: when the request also has
a vector, a document the vector finds can come back without every term, as it
can without any term. It is `invalid_argument` without a text query. The
excerpt of a hit follows the terms of the text query.

`query_attributes` names the full-text attributes a text query scores, 1–8 of
them, each with a `weight` from 0.000001 to 100 (default 1). Without
it the query scores `searchable_text`, the text of the document, as before. A
document scores the sum of the weighted BM25 scores of the attributes it
matches, and the best ten of that sum are the true best ten of every match.
`searchable_text` may be one of the attributes. With `query_match: "all"` each
attribute needs every term on its own: a document that holds half of them in
one attribute and half in another does not match. Only the scale of the
weights between attributes counts: the lane's scores are normalized before they
are fused with the vector lane. Each attribute is a `string` or an array of
strings whose [index options](tuning.md#namespaceattributes) include `full_text`;
any other, one without the index, or one whose index is built with the `ngram`
tokenizer, is `400 invalid_argument`, as is a repeated attribute or a weight out
of range. It is `invalid_argument` without a text query. A hit's excerpt still
comes from the text of the document.

| Member | Meaning |
| --- | --- |
| `attribute` | `searchable_text`, or an attribute with a full-text index. |
| `weight` | How much the attribute's score counts. |

An array is indexed with one document per element: a query that needs every
term finds an array only if one element holds them all, never across two, and
an array scores by its best element. An array stands alone in `query_attributes`:
Lance takes no query over such an index and one of a whole value together, so
an array next to another attribute is `invalid_argument` until a newer Lance
allows it.

A known limit of an array: the text lane fetches at most about 20 elements for
each of its `lexical_candidates` (41 for 2, 4,096 for 200), and a row appears
once for each element that matches. A document with more matching elements than
that fills the fetch, and the documents after it are missing from the answer
although they belong in it. The answer reports it as `telemetry.lexical_tie_bound_hits`. Keep the elements that can match a query few,
or rank by an attribute that holds one value per document.

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"waterproof boots",
       "query_attributes":[{"attribute":"title","weight":3},
                           {"attribute":"description"}]}'
```

`contains` is a list of 1–16 conditions on string and string array attributes
whose [index options](tuning.md#namespaceattributes) include `full_text`. A hit meets
every condition and also the `filter` and the caller's permissions, as with
`filter`; it works with a text ranking and with a `vector`. It is
`invalid_argument` for a listing, which has no ranking to narrow.

| Member | Meaning |
| --- | --- |
| `attribute` | The string or string array attribute to match; it must have a full-text index. |
| `all` | The value, or one element of an array, holds every token of this text, cut as the attribute's index cuts a text. |
| `any` | The value, or one element of an array, holds at least one token of this text. Exactly one of `all` and `any`. |

An attribute that does not exist or has no full-text index is `400
invalid_argument`, and so is a condition with both or neither of `all` and
`any`. So is an attribute whose index is built with the `ngram` tokenizer: Lance
gives every n-gram of a value the same position and takes them as alternatives,
so a condition on all tokens would find a value that has any one of them.
Matching the start of a value, for search as you type, waits for a newer Lance
pin ([#170](https://github.com/fyrr-ai/fyrr/issues/170)).

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"waterproof","query_match":"all",
       "contains":[{"attribute":"title","all":"red shoes"}]}'
```

`phrase` requires the tokens of its `text` to occur in the text of a hit in this
order: `{"text":"restore a backup"}` finds the documents that hold those tokens
next to one another. A hit meets the phrase and also the `filter`, `contains`
and the caller's permissions, as with `filter`; it works with a text ranking
and with a `vector`, and it narrows the lexical and the dense lane alike. The
text is cut as the index of `searchable_text` cuts it, so with a
`[namespace.analyzer]` with a language `restoring` and `restore` are one token. It is `invalid_argument` for a
listing, which has no ranking to narrow, and with `sub_queries`.

| Member | Meaning |
| --- | --- |
| `text` | The phrase, 1–4096 UTF-8 bytes. |
| `slop` | Tokens allowed between two tokens of the phrase. Only 0, the default (adjacent tokens), is accepted: Lance 14.0.3 loses matches for a larger slop, so it is `400 invalid_argument` until a newer Lance ([#170](https://github.com/fyrr-ai/fyrr/issues/170)). |

A phrase needs the positions of the tokens, which a namespace stores only when
it is created with `fts_positions = true` in `[namespace]` (see
[tuning](tuning.md#namespace)). A phrase on a namespace without them is `400
invalid_argument`. Positions make the full-text index larger: the
[measured growth](tuning.md#the-size-of-the-positions) is in the tuning guide.
The phrase reads `searchable_text` only; an attribute's index has no positions.

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"backup","phrase":{"text":"restore a backup"}}'
```

## Boosts

The `query` route takes `boosts`, 1–8 terms that add to the relevance score of
a text ranking (`query` or `queries`), a vector ranking, or both fused. It is
no argument of the tool: `fetch` and `/mcp` refuse it, and their output does
not change. A request without it is answered as before. It is
`400 invalid_argument` for a listing, which has no ranking, and with
`sub_queries` or `fusion`, which rank by their own rules.

The fused score of a text or hybrid ranking lies between 0 and 1. A ranking by
vector alone is scored by distance, so when a boost adds anything to it, the
distances are normalized to that range first, and its `score` reads as that sum. Each
term adds its `weight` times a function that lies between 0 and 1, so a weight
of 0.2 moves a document at most 0.2 up the scale, and a weight of 1 lets a
boost outweigh relevance.

| `function` | Other members | Adds |
| --- | --- | --- |
| `attribute` | `attribute`, `weight`, `midpoint` | `weight × x / (x + midpoint)` for a numeric attribute `x`: half the weight at the midpoint, the whole of it as `x` grows. A negative `x` counts as 0. |
| `recency` | `attribute`, `weight`, `midpoint`, `origin` | `weight × midpoint / (midpoint + d)`, where `d` is the distance of a `datetime` attribute from `origin`, before it or after it: the whole weight at the origin, half of it a midpoint away. |

`attribute` names an `int`, `uint` or `float` attribute of the namespace for
`attribute`, and a `datetime` attribute or `updated_at` for `recency`. A
document without a value of it gets 0 from the term. `weight` is a number from
0 to 1000000; `midpoint` is a number above 0, or for `recency` a duration above
0 such as `12h` or `30d` (an integer followed by `ms`, `s`, `m`, `h` or `d`);
`origin` is an RFC 3339 time. The reader has no clock and never takes the
current time as the origin: send the time of the request. An attribute that is
not declared or has another type, a weight or midpoint out of range and a
missing `origin` are `400 invalid_argument`.

Boosts apply to the candidates the lanes retrieve, not to the whole namespace:
the text lane's `namespace.retrieval.lexical_candidates` and the vector lane's
`dense_candidates` (at least `limit`), see [tuning](tuning.md#namespaceretrieval).
They are the rows the caller may read and the request's filters select, so a
boost never reads an attribute of a document the caller cannot read. The boost
is added to a candidate's fused score before the
[composition](tuning.md#namespacecomposition) picks its slots, so its
`source_cap` and slot rules still apply: a boost that adds 0 everywhere returns
the answer a request without `boosts` returns. A document among the candidates
can reach the first rank through a boost whatever its relevance rank; one
outside them cannot. A reranker blends its score with the boosted one; with a
reranker weight of 1 the reranker alone decides the order.

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"restore a backup","limit":10,
       "boosts":[
         {"function":"attribute","attribute":"views","weight":0.2,"midpoint":1000},
         {"function":"recency","attribute":"modified","weight":0.3,"midpoint":"30d",
          "origin":"2026-11-23T09:00:00Z"}]}'
```

## Grouping and caps

A document holds one or more records (chunks), and the lanes rank records. By
default a search answers with one hit per document: the record that ranks best,
whichever lane found it. The [composition](tuning.md#namespacecomposition)
fills its slots with the best record of each document, a document that an
earlier slot took is skipped, and a search by vector alone keeps the first
record of each document in the order of distance. `source_cap` counts these
hits. The `query` route changes two things about that. Both are no argument of
the tool: `fetch` and `/mcp` refuse them, and a request without them is
answered exactly as before.

| Member | Meaning |
| --- | --- |
| `group_documents` | `false` makes every record a hit of its own, so a document that matches with several records is returned with each of them. Default `true`; sending `true` changes nothing. `source_cap` and the composition's slots then count records, not documents: a source that holds many records of one document reaches its `source_cap` sooner, and the soft cap gives back the records it skipped after the others, so the answer is not strictly best first across sources. |
| `caps` | 1–4 caps `{"attribute": ..., "max": ...}`: at most `max` hits (1–75) for each value of the attribute, taken in rank order. `author`, `project`, `source_type` and the declared `string` attributes can be capped, each once; any other name or type is `400 invalid_argument`. A hit that has no value of the attribute is not capped. The hits of every cap count together: a hit is dropped when any value of it is at its cap. |

Both work for a text ranking, a vector ranking and the two fused, and for the
text-only answer given when the embedding provider fails. They are
`400 invalid_argument` for a listing, which has no ranking, and with `queries`
or `sub_queries`, which merge rankings: a cap or a record per hit would
count over the merge, or over each list, and the request does not say which.
(`fusion` needs one of them, so it is refused too.) `group_documents: true`
is the default and is never refused.

The request's `limit` still bounds the answer. A cap is hard, unlike
`source_cap`: a hit over it is dropped, and the next candidate takes its place,
but there may be no next one. The caps and the records apply to the candidates
the lanes retrieve (the text lane's `namespace.retrieval.lexical_candidates`
and the vector lane's `dense_candidates`, at least `limit`), so an answer can be
shorter than `limit` when the candidates run out. Before a reranker reorders
them, the caps and the records are applied to the composed candidates; the
reranker never adds a hit. The values of a capped attribute are read only for
candidates the caller may read, so a cap never reveals a value of a document
the caller cannot read. A request with both `boosts` and `caps` boosts the
candidates first, and then caps the boosted ranking.

```sh
curl https://fyrr.example.com/v1/namespaces/main/query \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"quarterly report","limit":20,
       "group_documents":false,
       "caps":[{"attribute":"author","max":3}]}'
```

## Aggregate

`POST /v1/namespaces/{namespace}/aggregate` computes aggregates over the
documents the caller may read and `filter` selects, optionally per group. It is
a JSON route only: no MCP tool takes it.

| Member | Meaning |
| --- | --- |
| `aggregations` | 1–16 aggregates `{ "op": ..., "field": ... }`. `op` is `count`, `sum`, `avg`, `min` or `max`. `count` without `field` counts documents; with one it counts the documents that have a value. The others need a `field`: `sum` and `avg` of an `int`, `uint` or `float`, `min` and `max` of any scalar. |
| `group_by` | 1–4 scalar fields (built-in or declared attributes, not the labels, an array or `bytes`). Without it the whole selection is one group, present even over no document. |
| `filter` | A [filter](mcp-tools.md#filters) tree, as for `search`. |
| `deadline_ms` | 1–300000; default 10 s. |

```sh
curl https://fyrr.example.com/v1/namespaces/main/aggregate \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"group_by":["status"],"aggregations":[{"op":"count"},{"op":"avg","field":"price"}]}'
```

The answer is `{"groups": [{"key": [...], "values": [...]}], "metadata": {...}}`.
A group's `key` holds its value of each `group_by` field and `values` its
aggregates, both in request order; the groups come in key order, numbers as
numbers, text bytewise and `datetime` by time. A document with no value in a
`group_by` field is in no group. Every count is of documents, not rows: a
document that has several records counts once and gives one value, the one of
its first row. A `null` value is skipped, so the `sum`, `avg`, `min` or `max`
of no value is `null`. A `sum` of whole numbers is exact while it fits 64 bits
(a signed integer, or an unsigned one when it is above the signed range) and an
approximate float beyond; an `avg` is a float, and so is the `sum` of floats
(`null` when not finite). `-0.0` is `0.0`.
`datetime` and `uuid` values are written as in a hit's attributes.

Only documents the caller may read are counted. More than
`reader.filter_count_max_rows` readable rows answers `503 busy`, as
[`list_filters`](mcp-tools.md#list_filters) does, which runs the same scan. More than 1000
groups is `400 invalid_argument`: narrow the filter or group by fewer fields.

## Recall

`POST /v1/namespaces/{namespace}/recall` measures how closely the vector index
agrees with an exact search, as the [background sampler](operations.md#vector-recall)
does, but for the number of stored vectors it is asked. It needs both the `admin`
and the `read` role of an API key: `read` binds the key to the rows it samples,
which `readers` name, and `admin` keeps the route to operators, since a filter
and `samples` would tell any caller something of values it may not see. A key
with only one of the two, and an OIDC user, answers `403 forbidden`. It is a JSON route only: no MCP tool takes it.

| Member | Meaning |
| --- | --- |
| `count` | 1–100 stored vectors to measure. They are the first rows, in storage order, that the key may read and `filter` selects. |
| `filter` | A [filter](mcp-tools.md#filters) tree, as for `search`, which every search of the run uses; the rows the key may read always apply on top of it. |
| `deadline_ms` | 1–300000; default 10 s. |
| `consistency` | As for [`query`](#consistency). |

```sh
curl https://fyrr.example.com/v1/namespaces/main/recall \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"count":20,"filter":{"field":"status","op":"eq","value":"open"},"deadline_ms":60000}'
```

The answer is `{"samples": 20, "rows": 41873, "approximate": true,
"recall_at_10": 0.97, "recall_at_75": 0.94}`. For each vector the reader
searches the namespace by that vector (75 nearest, the namespace's distance)
the way a query does, and exactly, over the rows the key may read and `filter`
selects, and compares the two answers; `recall_at_10` and `recall_at_75` are
the means over `samples`. `samples` is `count`, or fewer when the selection
holds fewer rows, and `rows` is the size of that selection, the rows each exact
scan reads. The whole run uses one selection: a change of the key's permissions
while it runs fails it with `503 authorization_unavailable`. Without a sample
both recalls are `null`.

It samples the built-in `vector` only; a [vector attribute](tuning.md#namespacevectors) has no recall report.

A query may answer by an exact scan instead of the index, as one with a
selective `filter` does, or one on a namespace without a vector index, or for a
key that reads only rows the index does not cover yet. `approximate: false`
means that at least one sample was answered by an exact scan: its recall
compares two exact answers and measures no index. The answer names no document
and returns no text or vector.

The run holds one query slot and stops at the deadline with `408
deadline_exceeded` and no partial answer. Each sample scans every selected
row, so a large `count` over a large namespace needs a long `deadline_ms`; the
other errors are those of [`query`](#errors).

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
| `upserts` | Rows. A row is an object with an `id` (string, its key in the namespace), its text under the key `text_attribute`, `readers` (`usr:<id>` or `grp:<id>`; ignored by an open namespace), optionally its `vector` (a list of numbers), a value for any [attribute](tuning.md#namespaceattributes), declared or [inferred](tuning.md#inferred-attributes) from the value, and a list of numbers, or `null`, for any declared [vector attribute](tuning.md#namespacevectors) (a list of such lists for a `multi` one). A row replaces the document with that `id`, readers included. |
| `patches` | Rows that change a document without replacing it. A row is an object with an `id`, and the attributes to write: an [attribute](tuning.md#namespaceattributes), declared or [inferred](tuning.md#inferred-attributes) (`null` clears a declared one), `readers`, or the text under the key `text_attribute` with its `vector`. Everything a patch does not name stays as stored, the vector included, and nothing is embedded. A patch of an `id` that does not exist when the request is published is skipped and never creates a document. |
| `deletes` | IDs to delete. An ID that does not exist is a delete of nothing. |
| `delete_where` | A [filter](mcp-tools.md#filters): every document that matches it is deleted. The deletes by filter apply before everything else in the request, so that one request can replace the documents of a group. |
| `patch_where` | `{"filter": …, "set": {…}}`: every document that matches the filter is patched with `set`, a patch row without its `id` (declared attributes and `readers`, never a text; an undeclared attribute is `400 attribute_undeclared`, as it is not [inferred](tuning.md#inferred-attributes) here). The patches by filter apply after `delete_where` and before the rest of the request. |
| `conditions` | An object from an `id` of the request, in `upserts`, `patches` or `deletes`, to a [filter](mcp-tools.md#filters) that the stored document with that `id` must meet for the change to apply. A comparison's `value` may be `{"row": "<attribute>"}`, which is what the change's own row holds of that declared attribute (`null` when it holds none); a delete has no row. See [Conditions](#conditions). |
| `settings` | The [settings](tuning.md#namespace) of a namespace that does not exist yet, laid over the process defaults. For an existing namespace they may add `attributes` and set `attribute_indexes` ([Changing the schema](#changing-the-schema)) and must change no other setting: `409 namespace_settings_fixed`. |

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

### Changing the schema

A request whose `settings` hold `attributes` and `attribute_indexes` changes
the schema of a namespace that exists; the request may carry nothing else: with
rows, deletes or conditions it is `400 schema_change_with_rows` and changes
nothing. A row that carries an attribute the schema does not declare is not a schema
change: its type is [inferred](tuning.md#inferred-attributes). The settings are laid over the namespace's: an attribute named again with its type
and an index option named again change nothing.

```json
{"settings": {"attributes": {"stock": "int", "note": "string"},
              "attribute_indexes": {"price": {"index": false}, "note": {"ngram": true}}}}
```

The answer is `{"upserted":0,"deleted":0}` once the head that readers serve
holds the new schema; readers keep the version they serve until then. The
settings are laid over the ones the namespace has when the change is made, so
two changes at once both apply. A request for a namespace that was replaced
meanwhile is `409 namespace_changed`. A namespace that has no publication yet
(created with `settings` alone) keeps its schema until its first rows are
published: `409 namespace_not_published`.

- A new attribute is a nullable column: it reads `null` for every document
  written before it, and a filter with `{"op": "eq", "value": null}` finds them.
  Its index is built by the next maintenance, like every index that is switched
  on; until then a filter or a sort on the attribute is answered by a scan of
  the table, bounded by the request deadline ([filters](mcp-tools.md#filters)).
  `fyrr writer serve` runs the maintenance when a manifest lacks an index that
  the settings ask for; `fyrr writer maintain` runs it by hand.
- An index switched off (`"index": false`) is dropped by the publication of the
  schema, and the attribute is scanned from then on. `"index": true` switches it
  on again. `full_text` and `ngram` ask for the text indexes of a `string`, and `full_text` for an array of strings.
- A change of the `analyzer` of a full-text index is published at once; the index
  is built again by the next maintenance, which publishes it, and the index it
  replaces serves until then ([analyzer](tuning.md#analyzer-of-a-full-text-index)).
- A type is never changed and an attribute is never removed, as that would change
  what the values already stored mean: another type, also an array of another
  element type, is `409 attribute_type_change_refused`, and `null` for an
  attribute is `409 attribute_removal_refused`. Any other setting, a name that
  is not declared or an option the type cannot have is refused as it is for a
  namespace that is created (`409 namespace_settings_fixed`, `400 settings_invalid`).
  A change of the schema waits about 5 s for the committer to publish, then is `503 busy`: send it again.

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

`delete_where` and `patch_where` need an [API key](identity.md#identityapi_keys)
with the `admin` role besides `write`: how many documents a filter matches, and
whether it matches more than the bound below, tells something about documents
that the key may not be allowed to read. A request with either from a key
without `admin` is `403 forbidden`. They match the documents of this namespace
that the API wrote, whatever their `readers`: the roles are the authorization,
not the row permissions. The committer evaluates each filter once, against the
documents the request is applied to, that is, after every request published
before it, so the last request wins as it does for ids. A filter that matches
more than 10,000 documents (`MAX_FILTER_WRITE_DOCUMENTS`) refuses the request
(`409 rejected`) and changes no document; the same bound holds for each of the two. An attribute that the upserts or patches of that request [inferred](tuning.md#inferred-attributes) stays declared.
A document the request names (`upserts`, `patches`, `deletes`) is written as
the row says: an upsert or a delete is not changed by a filter, and a patch by
`id` is applied over the patch by filter, except that the patch by `id` of a
document `delete_where` deletes finds it deleted. The answer does not count the
documents a filter matched. The request is `400 filter_invalid` when a filter
names a field or a value that the namespace's schema does not allow.

A patch changes the text only together with the `vector` of the new text, which
replaces the stored one; nothing is embedded. A `vector` without the text is
`400 vector_not_patchable`. A text without a `vector` would keep the embedding of
the old text: it is `400 text_not_patchable` where an
[`[embedding]`](configuration.md#embedding) provider is configured and
`400 embedding_not_configured` where none is. A patch keeps the stored vectors of the vector attributes, and brings none: a vector attribute in a patch is `400 vector_not_patchable`, and the `string` attribute a vector attribute is embedded from is `400 text_not_patchable`, because the stored vector would then disagree with it. Replace the document with an upsert instead. A body over `writer.envelope_max_bytes` or with more rows than
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
| `forbidden` | `403` | The key lacks the `write` role or its `namespace_prefix` excludes the namespace, or the request sets `conditions`, `delete_where` or `patch_where` and the key lacks the `admin` role. |
| `not_found` | `404` | `{namespace}` is not a namespace name, or the process has no `[namespace.write_api]`; for a `PATCH` of a namespace, also a namespace that does not exist. |
| `method_not_allowed` | `405` | A method that `/v1/namespaces/{namespace}` of the administration listener does not serve (it serves `PATCH` and `DELETE`), or a write route that is not `POST`. |
| `request_timeout` | `408` | The body of the request stopped arriving: the idle limit of the write route, 5 seconds for the body of `PATCH` and `DELETE` of a namespace. |
| `unsupported_media_type` | `415` | Not `application/json`, or a `Content-Encoding`. |
| `body_too_large`, `too_many_rows`, `row_too_large` (1 MiB), `settings_too_large` (64 KiB), `text_too_large`, `document_too_large`, `request_too_large` | `413` | A limit above. |
| `schema_change_with_rows`, `body_invalid`, `empty_request`, `id_missing`, `duplicate_id`, `text_missing`, `readers_invalid`, `vector_invalid`, `vector_not_patchable`, `text_not_patchable`, `embedding_not_configured`, `document_invalid`, `condition_invalid`, `filter_invalid`, `settings_invalid`, `attribute_undeclared` (a value that infers no type, or a namespace with no publication yet, [inference](tuning.md#inferred-attributes)) and the value codes of [tuning](tuning.md#namespaceattributes) (`attribute_type_mismatch`, `attribute_value_invalid`, `attribute_value_too_large`) | `400` | The body or a row is invalid, or the settings of a namespace to be created are (a schema that is too large or has a bad name included: `settings_invalid`). |
| `namespace_read_only`, `namespace_settings_fixed`, `namespace_not_writable`, `namespace_not_empty`, `namespace_changed`, `rejected` | `409` | The namespace is [frozen](#freezing-a-namespace), has other settings, takes its documents from connectors, is to be created over envelopes that a deleted namespace left, was replaced by another while the request was prepared, or the committer refused the request (nothing of it is applied). |
| `namespace_layout_old`, `storage_root_old` | `409` | The namespace or the local storage root was written by v0.1.x or an earlier build; recreate it with v0.2.0 ([upgrades](operations.md#upgrades)). Not retried: no `Retry-After`. |
| `attribute_type_change_refused`, `attribute_removal_refused`, `namespace_not_published` | `409` | The settings give an attribute of the namespace another type, or remove it, or change the schema of a namespace that has no publication yet. |
| `busy`, `unavailable`, `outcome_unknown` | `503` | The writer is busy, storage or the published keys cannot be read, or the answer did not come within 10 minutes; `Retry-After` is 1. |
