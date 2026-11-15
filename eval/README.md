# fyrr-eval

A maintainer quality-measurement tool. It is not part of any release, and the
product crates do not depend on it: it uses only the public API of
`fyrr-reader`.

```sh
cargo run --locked -p fyrr-eval -- --help
```

- `serve-embeddings` serves frozen vectors (`--frozen-queries`, or
  `--frozen-index` with `--frozen-vectors`) as an embedding provider on
  loopback HTTP and prints its endpoint. Point `embedding.endpoint` at it,
  with `reader_live = true` and a `spend` limit, and the normal writer embeds
  documents and the normal reader embeds queries through it; a vector it does
  not hold is an error, never a paid call. The input files are opened
  read-only and must match the configured embedding profile.
- `search --queries batch.json` runs a JSON array of
  `{"query": "...", "filters": {...}}` requests through the reader and prints
  one MCP-shaped JSON line per request. `--stages FILE` also writes each
  search's dense, lexical and composed keys to a new file under
  `FYRR_EVAL_ROOT`, outside this checkout.

Errors print closed codes, never inputs. The tests run in `scripts/ci` as
the `eval` row and, against the S3 fixture, the `s3-evaluation` row.
