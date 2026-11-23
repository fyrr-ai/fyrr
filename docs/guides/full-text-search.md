# Full-text search

> **In short**
>
> - A `query` searches the text of every row with a full-text index. The
>   lexical lane scores the rows that hold the query's words; a hit's
>   `lexical_score` is that score.
> - `highlights` returns the ranges of the excerpt that matched, and
>   `fts_stem` makes `restore` match `restores` and `restoring`.
> - `query_match: "all"` ranks only rows that hold every word, and `contains`
>   requires words in a string attribute with a `full_text` index.

A text query runs two lanes at once: the lexical lane and the dense lane, which
searches by the embedding of the query. [Hybrid search](hybrid-search.md) is
about their fusion. This guide is about the lexical lane, which finds words.
The examples print `lexical_score != null` to show which hits the lexical lane
found: a hit the dense lane found alone has no `lexical_score`.

Start fyrr as in [Test against local storage](testing.md):

```sh
source examples/guides/start
```

## 1. Index the text

Every row's `text` is full-text indexed. The first write also declares a `title`
attribute with a full-text index of its own. Stemming is a setting of the
namespace, `fts_stem` in `[namespace]` of the
[configuration](../tuning.md#namespace), and the guides' configuration turns it
on; the namespace takes it from the file when its first write creates it and
keeps it for life:

```sh
curl -fsS "$WRITER/v1/namespaces/articles/documents" \
  -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
  -d '{"settings":{"attributes":{"title":"string"},
                   "attribute_indexes":{"title":{"full_text":true}}},
       "upserts":[
         {"id":"restore","title":"Restoring a backup",
          "text":"To restore a backup, run the restore job and verify the checksum."},
         {"id":"deploy","title":"Deploy checklist",
          "text":"Before a deploy, check that the last backup succeeded."},
         {"id":"rollback","title":"Rolling back a deploy",
          "text":"A rollback restores the previous release, not the data."},
         {"id":"holidays","title":"Holiday calendar",
          "text":"Offices are closed on public holidays."}]}'
```

```text
{"upserted":4,"deleted":0}
```

## 2. See which rows the words found

```sh
curl -fsS "$READER/v1/namespaces/articles/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"restore backup","consistency":"strong",
       "fields":{"scores":true}}' |
  jq -c '.hits[] | [.root_id, .lexical_score != null]'
```

```text
["restore",true]
["deploy",true]
["rollback",true]
["holidays",false]
```

`restore` holds both words, `deploy` holds `backup`, and `rollback` holds
`restores`, which the stemmer reduces to `restore`. `holidays` holds none; the
dense lane can still return it, because it always returns the nearest rows.

## 3. Require every word

`query_match: "all"` ranks only the rows that hold every word of the query:

```sh
curl -fsS "$READER/v1/namespaces/articles/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"restore backup","query_match":"all","consistency":"strong",
       "fields":{"scores":true}}' |
  jq -c '.hits[] | [.root_id, .lexical_score != null]'
```

```text
["restore",true]
["deploy",false]
["holidays",false]
["rollback",false]
```

It narrows the lexical lane only: the dense lane still returns the other rows,
as the output shows. To make a word a requirement of the answer,
use `contains` on an attribute with a `full_text` index. A hit must meet every
condition, and so must the permissions of the caller:

```sh
curl -fsS "$READER/v1/namespaces/articles/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"backup","consistency":"strong",
       "contains":[{"attribute":"title","all":"restoring backup"}]}' |
  jq -r '.hits[].root_id'
```

```text
restore
```

`all` wants every token of its text and `any` at least one; a condition has
exactly one of them. The text is cut the way the attribute's index cuts it, so
`restoring` and `restore` are the same token here. The tokenizer, the language
and the stop words of an index are its
[analyzer](../tuning.md#analyzer-of-a-full-text-index).

## 4. Show what matched

The excerpt of a hit is the passage with the most distinct terms of the query.
With an analyzer, `fts_stem`, `fts_remove_stop_words` or `fts_ascii_folding`
the terms are the tokens the index cuts, so a passage that holds a stemmed or
folded match is the one returned; otherwise they are the words of the query.

`highlights` adds the ranges of each excerpt that match the query, in the
codepoints of the record, as the excerpt's `start` and `end` are. A matched
token is the one the namespace's analyzer reduces to a token of the query, so
a stemmed match is marked:

```sh
curl -fsS "$READER/v1/namespaces/articles/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"restore backup","limit":1,"consistency":"strong",
       "fields":{"highlights":true}}' |
  jq -c '.hits[0].excerpt as $e |
         [$e.highlights[] | $e.text[(.[0] - $e.start):(.[1] - $e.start)]]'
```

```text
["restore","backup","restore"]
```

An excerpt marks at most 100 ranges.

## 5. Stop

```sh
fyrr_stop
```
