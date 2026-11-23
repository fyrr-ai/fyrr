# Test against local storage

> **In short**
>
> - A fyrr on a `file://` storage root needs no bucket, no Docker and no
>   account. It is the fastest way to test code that writes and searches
>   documents.
> - `examples/guides/start` starts one in a private temporary directory, on
>   free ports, and `fyrr_stop` removes it. Every guide begins that way.
> - A write is answered once the head that readers serve holds it, and a
>   `strong` read includes every acknowledged write. A test that writes, then
>   reads with `consistency: "strong"`, needs no sleep and no retry.

Local storage is for tests and for one machine. The storage root is a private
directory on a local file system that no other host shares
([configuration](../configuration.md#local-storage)). Use object storage for
anything else.

The commands of this page and of the other guides run in CI by
[`examples/guides/run`](../../examples/guides/run). Run them from the root of a
checkout, in bash, with `fyrr` on `PATH`
([build it](../quickstart.md#1-build-and-start-toy-embeddings)).

## 1. Start fyrr

```sh
source examples/guides/start
```

The script copies [`examples/guides/fyrr.toml`](../../examples/guides/fyrr.toml)
into a new directory, and starts three processes on free loopback ports:

- the toy embeddings service of the [quickstart](../quickstart.md), which
  matches words and is no semantic model. A row that carries its own `vector`
  makes no call to it;
- `fyrr writer serve`, which serves the [write API](../http-api.md#write-documents)
  at `$WRITER` and commits what it receives;
- `fyrr reader serve`, which serves the [read API](../http-api.md) at `$READER`.

The pieces of the configuration that matter for a test are these:

```toml
[storage]
uri = "file:///tmp/a-fresh-directory/index"

[namespace.write_api]
open = true

[[identity.api_keys]]
name = "ann"
key = { env = "READ_KEY" }
readers = ["usr:ann", "grp:engineering"]
```

`uri` is where every object lives. `[namespace.write_api]` turns on the write
route and says that a namespace is created by its first write. The keys are
the bearer tokens of the callers: a `write` key writes, a key with `readers`
reads the rows those readers may read ([API keys](../identity.md#identityapi_keys)).
The script exports `WRITE_KEY`, `ADMIN_KEY`, `READ_KEY` and `BOB_KEY`, makes
every secret fresh, and publishes the keys with `fyrr writer keys` before the
reader starts.

## 2. Write, then read

A first write creates its namespace and answers when the head holds it:

```sh
curl -fsS "$WRITER/v1/namespaces/testing/documents" \
  -H "Authorization: Bearer $WRITE_KEY" -H "Content-Type: application/json" \
  -d '{"upserts":[
        {"id":"a","text":"The backup job runs at night."},
        {"id":"b","text":"The deploy needs two approvals."}]}'
```

```text
{"upserted":2,"deleted":0}
```

The reader checks for a new publication on an interval, so a default
(`cached`) read can still answer from the version before the write. A `strong`
read first checks the head, and its answer includes the write:

```sh
curl -fsS "$READER/v1/namespaces/testing/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"backup","limit":1,"consistency":"strong"}' |
  jq -r '.hits[].root_id'
```

```text
a
```

A test asserts on the answer like any other output. `jq -e` exits nonzero when
its last value is `false` or `null`:

```sh
curl -fsS "$READER/v1/namespaces/testing/query" \
  -H "Authorization: Bearer $READ_KEY" -H "Content-Type: application/json" \
  -d '{"query":"deploy approvals","limit":1,"consistency":"strong"}' |
  jq -e '.hits[0].root_id == "b"'
```

```text
true
```

A `strong` read costs one storage request when the head has not moved
([consistency](../http-api.md#consistency)); on local storage that is a file
read. Use it in every test that reads after a write.

## 3. Start every test from nothing

Give each test file its own namespace name, or its own directory. A namespace
is cheap: the first write creates it. A directory is cleaner. Start fyrr in a
new one, and nothing a test left behind can reach the next. This lists what the
directory holds:

```sh
curl -fsS "$READER/v1/namespaces" -H "Authorization: Bearer $ADMIN_KEY" |
  jq -c '.namespaces'
```

```text
["main","testing"]
```

`main` is the namespace of the configuration. The reader needs it to start, so
the script writes its first document.

## 4. Stop and clean up

```sh
fyrr_stop
test ! -e "$FYRR_GUIDE_DIR" && echo removed
```

```text
removed
```

`fyrr_stop` ends the three processes (the writer finishes its commit and
releases its lease first) and deletes the directory with the storage, the
reader mirror and the logs. Nothing outlives the test.

Next: [vector search](vector-search.md), [full-text search](full-text-search.md),
[hybrid search](hybrid-search.md), [permissions](permissions.md) and
[bulk loading](bulk-loading.md).
