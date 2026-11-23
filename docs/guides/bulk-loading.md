# Bulk loading

> **In short**
>
> - Declare the schema in a first request, then send the rows in batches. A
>   batch is one request, published whole or refused whole.
> - A row carries its own `vector` or is embedded by the provider. Rows with
>   vectors need no embedding call, so a load costs only the writes.
> - A write is keyed by the row `id`, so sending a batch again is safe, and a
>   `delete_where` with the new rows replaces a whole group in one request.

A bulk load is many writes. Each one is answered when the head that readers
serve holds it. This guide loads 12 rows in three batches, repeats one,
replaces a group, and counts the result.

Start fyrr as in [Test against local storage](testing.md):

```sh
source examples/guides/start
```

## 1. Declare the schema first

A request with only `settings` creates the namespace. The rows here carry
their own vectors of 8 numbers, so the settings name the dimension, and they
have one `batch` number to tell the loads apart:

```sh
curl -fsS "$WRITER/v1/namespaces/bulk/documents" \
  -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
  -d '{"settings":{"embedding":{"model":"my-model","dimensions":8},
                   "attributes":{"batch":"int"}}}'
```

```text
{"upserted":0,"deleted":0}
```

A schema can grow later, but a type never changes and an attribute is never
removed ([changing the schema](../http-api.md#changing-the-schema)).

## 2. Send the rows in batches

Keep the rows as JSON Lines, one row per line, and let `split` cut the file:

```sh
rows="$(mktemp -d)"
seq 1 12 | jq -c '{id: "row-\(.)", text: "Row number \(.)", batch: ((. - 1) / 4 | floor),
                   vector: [., 1, 0, 0, 0, 0, 0, 0]}' >"$rows/all.jsonl"
split -l 4 "$rows/all.jsonl" "$rows/part-"
for part in "$rows"/part-*; do
  jq -cs '{upserts: .}' "$part" |
    curl -fsS "$WRITER/v1/namespaces/bulk/documents" \
      -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
      --data-binary @-
  echo
done
```

```text
{"upserted":4,"deleted":0}
{"upserted":4,"deleted":0}
{"upserted":4,"deleted":0}
```

```text
{"upserted":4,"deleted":0}
{"upserted":4,"deleted":0}
{"upserted":4,"deleted":0}
```

Make a batch as large as is comfortable. A request is at most
`writer.envelope_max_rows` rows (50,000 by default) and `envelope_max_bytes`,
and a row is at most 1 MiB, with at most 16 KiB of text
([write documents](../http-api.md#write-documents)). A row without a `vector` is
embedded by the `[embedding]` provider, under its spend limit, so a load of
text rows costs what the provider charges.

## 3. Count what arrived

`aggregate` counts documents exactly, and `strong` sees every write already
answered. Count them all, and per batch:

```sh
curl -fsS "$READER/v1/namespaces/bulk/aggregate" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"group_by":["batch"],"aggregations":[{"op":"count"}],"consistency":"strong"}' |
  jq -c '.groups[] | [.key[0], .values[0]]'
```

```text
[0,4]
[1,4]
[2,4]
```

## 4. Repeat a batch safely

A request that ends in `503 outcome_unknown` or `503 unavailable` says nothing
about its outcome: it may still be published. Because a row is keyed by its
`id`, sending the batch again writes the same documents again:

```sh
jq -cs '{upserts: .}' "$rows/part-aa" |
  curl -fsS "$WRITER/v1/namespaces/bulk/documents" \
    -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
    --data-binary @-
echo
curl -fsS "$READER/v1/namespaces/bulk/aggregate" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"aggregations":[{"op":"count"}],"consistency":"strong"}' |
  jq -c '.groups[0].values[0]'
```

```text
{"upserted":4,"deleted":0}
12
```

Requests apply in the order they are published and the last one wins, so a
repeat of an old batch can overwrite a newer change to the same ids. Load a
namespace from one sender, or guard each row with a
[condition](../http-api.md#conditions).

## 5. Replace a group

To reload a group, delete it and write its new rows in one request, so that no
reader sees it half done. `delete_where` needs an `admin` key besides `write`,
and applies before everything else in the request:

```sh
curl -fsS "$WRITER/v1/namespaces/bulk/documents" \
  -H "Authorization: Bearer $ADMIN_KEY" -H "Content-Type: application/json" \
  -d '{"delete_where":{"field":"batch","op":"eq","value":2},
       "upserts":[
         {"id":"row-9","text":"Row number 9, loaded again","batch":2,
          "vector":[9,2,0,0,0,0,0,0]}]}'
echo
curl -fsS "$READER/v1/namespaces/bulk/aggregate" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"group_by":["batch"],"aggregations":[{"op":"count"}],"consistency":"strong"}' |
  jq -c '.groups[] | [.key[0], .values[0]]'
```

```text
{"upserted":1,"deleted":0}
[0,4]
[1,4]
[2,1]
```

A filter write that matches more than 10,000 documents is refused whole
(`409 rejected`), so reload a larger group in parts.

## 6. Let the indexes catch up

The writer builds the indexes of the namespace between writes: the vector
index, and the indexes of the attributes you declared. `fyrr writer serve` does
it by itself when a version has work for it, and `fyrr writer maintain` does it
on demand, which is what a job that loads and exits runs after its last write
([cli](../cli.md)). Until an index is built a filter on its attribute is
answered by a scan, which is correct and slower. `fyrr writer status` lists the
data files (`fragments`) and the segments of each index (`index_segments`) of
the served version. This checks that it has a vector index:

```sh
fyrr writer status | jq -c '.index_segments | has("vector_idx")'
```

```text
true
```

## 7. Stop

```sh
rm -rf "$rows"
fyrr_stop
```
