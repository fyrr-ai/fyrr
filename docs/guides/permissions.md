# Permissions on written documents

> **In short**
>
> - A row written through the API names the readers that may see it:
>   `usr:<id>` and `grp:<id>`. A caller reads a row when one of the caller's
>   reader tokens is among them.
> - A namespace created with `write_api.open: false` hides a row from every
>   caller its `readers` do not name, and a row with no `readers` from all.
> - Search, fetch and aggregate never return a row the caller may not read, and
>   an upsert or a patch of `readers` takes effect for a `strong` read at once.

The caller's reader tokens come from its API key: `readers` in
[`[[identity.api_keys]]`](../identity.md#identityapi_keys). In this guide `READ_KEY`
acts as `usr:ann` and `grp:engineering`, and `BOB_KEY` as `usr:bob`.

Start fyrr as in [Test against local storage](testing.md):

```sh
source examples/guides/start
```

## 1. Write rows with readers

A namespace is open (every caller that may read, reads every row) or not,
and says so in its first write. This one is not open. Each row names its
readers:

```sh
curl -fsS "$WRITER/v1/namespaces/company/documents" \
  -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
  -d '{"settings":{"write_api":{"open":false}},
       "upserts":[
         {"id":"backup","text":"To restore a backup, run the restore job.",
          "readers":["grp:engineering"]},
         {"id":"salaries","text":"The salary bands of the company.",
          "readers":["usr:bob"]},
         {"id":"holidays","text":"Offices are closed on public holidays.",
          "readers":["usr:ann","usr:bob"]},
         {"id":"onboarding","text":"Welcome to the team.",
          "readers":["usr:ann"]},
         {"id":"draft","text":"A draft nobody has been granted yet."}]}'
```

```text
{"upserted":5,"deleted":0}
```

## 2. Each caller sees its own rows

Both callers send the same question:

```sh
for key in "$READ_KEY" "$BOB_KEY"; do
  curl -fsS "$READER/v1/namespaces/company/query" \
    -H "Authorization: Bearer $key" -H "Content-Type: application/json" \
    -d '{"query":"company backup salary holidays welcome","consistency":"strong"}' |
    jq -c '[.hits[].root_id] | sort'
done
```

```text
["backup","holidays","onboarding"]
["holidays","salaries"]
```

Ann reads `backup`, through her group, `holidays` and `onboarding`. Bob reads
`salaries` and `holidays`. Nobody reads `draft`: a row without `readers` has no
reader. A row the caller may not read is no hit and no
error, and `fetch` answers the same way:

```sh
curl -fsS "$READER/v1/namespaces/company/fetch" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"source_id":"_api","root_id":"salaries","consistency":"strong"}' |
  jq -c '.documents | length'
```

```text
0
```

Counts obey the same rule. `aggregate` counts the documents the caller may
read, so the numbers differ per caller:

```sh
for key in "$READ_KEY" "$BOB_KEY"; do
  curl -fsS "$READER/v1/namespaces/company/aggregate" \
    -H "Authorization: Bearer $key" -H "Content-Type: application/json" \
    -d '{"aggregations":[{"op":"count"}],"consistency":"strong"}' |
    jq -c '.groups[0].values'
done
```

```text
[3]
[2]
```

## 3. Grant and revoke

A `patch` changes `readers` and nothing else: the text and the vector stay, and
nothing is embedded again. Grant `salaries` to engineering, and take `holidays`
from Ann:

```sh
curl -fsS "$WRITER/v1/namespaces/company/documents" \
  -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
  -d '{"patches":[
         {"id":"salaries","readers":["usr:bob","grp:engineering"]},
         {"id":"holidays","readers":["usr:bob"]}]}'
```

```text
{"upserted":0,"deleted":0,"patched":2,"skipped":0}
```

```sh
curl -fsS "$READER/v1/namespaces/company/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"company backup salary holidays welcome","consistency":"strong"}' |
  jq -c '[.hits[].root_id] | sort'
```

```text
["backup","onboarding","salaries"]
```

An upsert of a row replaces the document with its `readers`, so send the
readers again when you rewrite a row. A `delete` removes the row for everyone.
The write is answered when the head that readers serve holds it; a `strong`
read sees it at once, and a reader that has not yet checked for the new head
may still answer from the old one ([guarantees](../guarantees.md#what-a-read-sees)).

## 4. Open namespaces and keys

A namespace created with `open: true` (the default of this configuration) is
read by every key that may read, whatever a row's `readers` say. Use it for
data that is public inside the deployment. A key reads where it holds `read`
and its `namespace_prefix`, when it has one, matches the namespace name; the
tokens it reads with are its `readers` in the configuration
([identity and access](../identity.md#identityapi_keys)).

## 5. Stop

```sh
fyrr_stop
```
