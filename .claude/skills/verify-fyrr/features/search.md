# Search

Search finds the documents the caller may read for a question, with lexical
and dense retrieval fused per document, and returns each hit with its
metadata and an excerpt. The CLI, the MCP tool and the HTTP route return the
same JSON.

## Sub-features

- `search-query` returns the matching document first, with an excerpt.
- `search-output` returns metadata only, an excerpt (the default) or the whole content.
- `search-filter` narrows the result by a filter tree, and to nothing for an unknown source.
- `search-queries` answers several phrasings in one call, `limit` hits for each.
- `search-listing` lists documents without a query.
- `search-invalid` refuses arguments outside the schema instead of clamping.

## How to get to it (user POV)

- Run `fyrr reader search QUERY [--limit N] [--output metadata|excerpt|content]` in a terminal.
- Let an agent call the MCP tool `search` of `fyrr reader serve` (stdio).
- Send `POST /v1/namespaces/{namespace}/query` to the HTTP reader with a bearer key.
- Call the tool `search` through `POST /mcp` on the HTTP reader.

## Driving it with control-fyrr

Preconditions:

- Profile `notes` at the baseline, and `env.sh` loaded.

- **CLI query.** Ask the quickstart question. Run `control-fyrr run search-cli -- fyrr reader search 'How do I restore a backup?' --limit 3 | jq -c '.hits[0] | {source_id, root_id, text: .excerpt.text}'`. It prints `{"source_id":"notes","root_id":"backup.md","text":"To restore a backup, run the restore job and verify the checksum."}`.
- **MCP query.** Ask the same over MCP stdio. Run `control-fyrr mcp search-mcp search '{"query":"How do I restore a backup?","limit":3}' | docs`. The first line is `notes/backup.md`.
- **HTTP query.** Ask the same over the JSON route. Run `control-fyrr http search-http POST /v1/namespaces/notes/query '{"query":"How do I restore a backup?","limit":3}' | docs`. Stderr says `HTTP 200` and the first line is `notes/backup.md`.
- **MCP over HTTP query.** Ask the same through `POST /mcp`. Run `control-fyrr http search-mcp-http POST /mcp '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{"query":"How do I restore a backup?","limit":3}}}' | mcp_result | docs`. Stderr says `HTTP 200` and the first line is `notes/backup.md`.
- **Content output.** Ask for whole documents. Run `control-fyrr run search-content -- fyrr reader search backup --limit 1 --output content | jq -c '.hits[0] | {excerpt, content}'`. It prints `{"excerpt":null,"content":"To restore a backup, run the restore job and verify the checksum.\n"}`.
- **Metadata output.** Ask for no text. Run `control-fyrr run search-metadata -- fyrr reader search backup --limit 1 --output metadata | jq -c '.hits[0] | {excerpt, content}'`. It prints `{"excerpt":null,"content":null}`.
- **Filter tree.** Exclude one document. Run `control-fyrr mcp search-filter search '{"query":"backup","filter":{"and":[{"field":"source_id","op":"eq","value":"notes"},{"not":{"field":"root_id","op":"eq","value":"deploys.md"}}]}}' | docs`. The only line is `notes/backup.md`.
- **Unknown source.** Filter by a source that does not exist. Run `control-fyrr mcp search-unknown-source search '{"query":"backup","sources":["nope"]}' | jq -c '.hits'`. It prints `[]`: an empty result is an ordinary answer. `null` would mean the call failed.
- **Several queries.** Send two phrasings. Run `control-fyrr mcp search-queries search '{"queries":["restore a backup","when are deploys"],"limit":1}' | jq -c '[.hits[] | {root_id, query_index}]'`. It prints `[{"root_id":"backup.md","query_index":0},{"root_id":"deploys.md","query_index":1}]`.
- **Listing.** List without a query. Run `control-fyrr mcp search-listing search '{"order":"updated_at"}' | docs`. It lists `notes/backup.md` and `notes/deploys.md`.
- **Refused argument, MCP.** Ask for too many hits. Run `control-fyrr mcp search-invalid-mcp search '{"query":"backup","limit":5000}' | jq -c '{isError: .result.isError, code: .result.structuredContent.code}'`. It prints `{"isError":true,"code":"invalid_argument"}`.
- **Refused argument, HTTP.** Run `control-fyrr http search-invalid-http POST /v1/namespaces/notes/query '{"query":"backup","limit":5000}'`. Stderr says `HTTP 400` and the body is `{"error":"invalid_argument"}`.
- **Refused argument, CLI.** Run `control-fyrr run search-invalid-cli -- fyrr reader search backup --limit 0`. Exit code `1`, stdout is empty and the last stderr line is `error: invalid_argument`.
- **Usage error.** Omit the query. Run `control-fyrr run search-usage -- fyrr reader search`. Exit code `2`.
- **Proof.** Cite the transcripts of `search-cli`, `search-mcp`, `search-http` and `search-mcp-http` in the evidence directory: each shows the request and `notes/backup.md` as the first hit.

## Gotchas

- With the toy embeddings and three tiny documents, the dense lane returns
  every readable document for almost any query. Assert on the first hit or on
  a document's absence, not on the number of hits. Only a filter that matches
  nothing gives an empty result.
- `--limit` over the CLI is 1 to 75 with a query. Out-of-range values are
  refused, never clamped.
- A query may start with `-`; put `--` before a query that is exactly an
  option, such as `--limit`.
- The CLI writes telemetry lines to stderr, on success and on failure. Read
  results from stdout only.
- `control-fyrr mcp` prints the tool's `structuredContent` on success and
  the whole JSON-RPC answer on a tool error, and then exits `1`. So `docs`
  after a failed call stops with a `jq` error, and `jq` paths differ between
  the two.
- `score` only orders the hits of one answer. Do not compare it across calls.
