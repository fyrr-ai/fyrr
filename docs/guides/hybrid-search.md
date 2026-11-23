# Hybrid search with fusion

> **In short**
>
> - A text `query` is a hybrid search: the lexical lane finds words and the
>   dense lane finds meaning, and fyrr fuses the two rankings.
> - `queries` or `sub_queries` with `fusion` search several ways at once and
>   return one ranking. `weights` says how much each search counts.
> - `boosts` add a numeric attribute to the score, so that, for example, a
>   popular document can outrank a slightly better match.

One search is rarely enough. Words find the document that says `backup`; an
embedding finds the one that says `restore the archive`. A text `query` runs
both, and a hit's score is their fusion. This guide shows what each lane found,
fuses several searches into one ranking, and nudges the ranking with a number.
The embeddings come from the toy service of the quickstart, which matches
words, so the dense lane here is a weak stand-in for a real model: the
mechanics are the same.

Start fyrr as in [Test against local storage](testing.md):

```sh
source examples/guides/start
```

## 1. Write some documents

```sh
curl -fsS "$WRITER/v1/namespaces/handbook/documents" \
  -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
  -d '{"settings":{"attributes":{"views":"int"}},
       "upserts":[
         {"id":"restore","views":3,
          "text":"To restore a backup, run the restore job and verify the checksum."},
         {"id":"deploy","views":900,
          "text":"Before a deploy, check that the last backup succeeded."},
         {"id":"rollback","views":40,
          "text":"A rollback returns production to the previous release."},
         {"id":"holidays","views":5,
          "text":"Offices are closed on public holidays."}]}'
```

```text
{"upserted":4,"deleted":0}
```

## 2. See both lanes

`fields.scores` adds the raw score of each lane to a hit: `lexical_score` from
the words and `distance` from the vectors. A lane that did not find the hit
leaves its field out. This prints one line per hit: its `id`, and whether the
lexical and the dense lane found it:

```sh
curl -fsS "$READER/v1/namespaces/handbook/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"restore backup","consistency":"strong","fields":{"scores":true}}' |
  jq -c '.hits[] | [.root_id, .lexical_score != null, .distance != null]'
```

```text
["restore",true,true]
["deploy",true,true]
["holidays",false,true]
["rollback",false,true]
```

The `score` of a hit is its fused score. How much the dense lane counts against
the lexical one is the namespace's `retrieval.dense_weight`, 0.5 by default
([tuning](../tuning.md#namespaceretrieval)).

## 3. Fuse several searches

`queries` takes 2 to 16 texts. Without `fusion`, the hits of the searches are
interleaved by rank. With `fusion` the answer is one ranking of the documents
of all searches. The method is reciprocal rank fusion (`rrf`): a document
scores the sum of `weight / (rank_constant + rank)` over the searches that
return it, with `rank` counted from 1 in each search's own order:

```sh
curl -fsS "$READER/v1/namespaces/handbook/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"queries":["restore backup","deploy release"],"limit":4,
       "fusion":{"method":"rrf"},"consistency":"strong"}' |
  jq -c '[.hits[].root_id]'
```

```text
["deploy","restore","rollback","holidays"]
```

`weights` has one number per search, in request order, and `rank_constant`
(default 60) flattens the difference between ranks when it is larger. Weight
the second search up and it leads:

```sh
curl -fsS "$READER/v1/namespaces/handbook/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"queries":["restore backup","deploy release"],"limit":4,
       "fusion":{"method":"rrf","weights":[1,5]},"consistency":"strong"}' |
  jq -c '[.hits[].root_id]'
```

```text
["rollback","deploy","holidays","restore"]
```

## 4. Give each search its own filter and limit

`sub_queries` are 1 to 16 searches with a `filter` and a `limit` of their own.
Every sub-query also obeys the filters of the request and the caller's
permissions. This one fuses the best of all documents with the best of the
popular ones (`views` of 100 or more):

```sh
curl -fsS "$READER/v1/namespaces/handbook/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"sub_queries":[
         {"query":"restore backup","limit":3},
         {"query":"restore backup","limit":3,
          "filter":{"field":"views","op":"gte","value":100}}],
       "fusion":{"method":"rrf"},"limit":4,"consistency":"strong"}' |
  jq -c '[.hits[] | [.root_id, .query_index]]'
```

```text
[["deploy",1],["restore",0],["holidays",0]]
```

A hit's `query_index` names the sub-query that gave it most. A reranker, when
the namespace has one, reorders the candidates of every sub-query before the
fusion ([fusion and reranking](../http-api.md#fusion-and-reranking)); these
examples have none.

## 5. Boost by a number

A document that many people read may deserve to rank above a slightly better
match. `boosts` add to the relevance of a text or hybrid ranking: an
`attribute` boost adds `weight × x / (x + midpoint)` for a numeric attribute
`x`, so it adds half the weight at the midpoint and approaches the whole weight
as `x` grows:

```sh
curl -fsS "$READER/v1/namespaces/handbook/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"restore backup","limit":4,"consistency":"strong"}' |
  jq -c '[.hits[].root_id]'
curl -fsS "$READER/v1/namespaces/handbook/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"restore backup","limit":4,"consistency":"strong",
       "boosts":[{"function":"attribute","attribute":"views",
                  "weight":0.5,"midpoint":100}]}' |
  jq -c '[.hits[].root_id]'
```

```text
["restore","deploy","holidays","rollback"]
["deploy","restore","rollback","holidays"]
```

A weight of 0.5 moves a document at most 0.5 up a score that lies between 0 and
1, and a weight of 1 lets a boost outweigh relevance. Boosts apply to the
candidates the lanes retrieve, and are refused with `fusion` and `sub_queries`,
which rank by their own rules ([boosts](../http-api.md#boosts)). A `recency`
boost does the same for a `datetime` attribute.

## 6. Stop

```sh
fyrr_stop
```
