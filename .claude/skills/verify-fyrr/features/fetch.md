# Fetch

Fetch reads the text of documents the caller may read: one document by its
source and root ID or by its URI, up to five at once, or one span of a
record that a search hit pointed at. An unknown document gives an empty
answer, not an error.

## Sub-features

- `fetch-id` returns a document by `source_id` and `root_id`.
- `fetch-uri` returns a document by the `source_uri` of a hit.
- `fetch-batch` returns several documents in the order asked.
- `fetch-window` returns one span of one record.
- `fetch-unknown` answers an unknown document with no documents.

## How to get to it (user POV)

- Let an agent call the MCP tool `fetch` of `fyrr reader serve` (stdio).
- Send `POST /v1/namespaces/{namespace}/fetch` to the HTTP reader with a bearer key.
- Call the tool `fetch` through `POST /mcp` on the HTTP reader.
- There is no `fyrr reader fetch` command: the CLI searches only.

## Driving it with control-fyrr

Preconditions:

- Profile `notes` at the baseline, and `env.sh` loaded.

- **MCP by ID.** Fetch the backup page over MCP stdio. Run `control-fyrr mcp fetch-id-mcp fetch '{"source_id":"notes","root_id":"backup.md"}' | texts`. It prints `To restore a backup, run the restore job and verify the checksum.`
- **HTTP by ID.** Fetch the same over the JSON route. Run `control-fyrr http fetch-id-http POST /v1/namespaces/notes/fetch '{"source_id":"notes","root_id":"backup.md"}' | texts`. Stderr says `HTTP 200` and stdout is the same sentence.
- **MCP over HTTP by ID.** Fetch the same through `POST /mcp`. Run `control-fyrr http fetch-id-mcp-http POST /mcp '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch","arguments":{"source_id":"notes","root_id":"backup.md"}}}' | mcp_result | texts`. Stderr says `HTTP 200` and stdout is the same sentence.
- **By URI.** Take the URI of a hit and fetch by it. Run `URI=$(control-fyrr mcp fetch-uri-search search '{"query":"backup","limit":1,"exclude":[{"source_id":"notes","root_id":"deploys.md"}]}' | jq -r '.hits[0].source_uri')`, then `control-fyrr mcp fetch-uri fetch "$(jq -nc --arg uri "$URI" '{uri: $uri}')" | jq -r '.documents[].root_id'`. It prints `backup.md`.
- **Batch.** Fetch two documents in a chosen order. Run `control-fyrr mcp fetch-batch fetch '{"documents":[{"source_id":"notes","root_id":"deploys.md"},{"source_id":"notes","root_id":"backup.md"}]}' | jq -c '[.documents[].root_id]'`. It prints `["deploys.md","backup.md"]`.
- **Window.** Read a span of the record a hit names. Run `HIT=$(control-fyrr mcp fetch-window-search search '{"query":"checksum","limit":1,"exclude":[{"source_id":"notes","root_id":"deploys.md"}]}' | jq -c '.hits[0]')`, then `control-fyrr mcp fetch-window fetch "$(jq -c '{source_id, root_id, window: {record_id, revision, start: 3, end: 19}}' <<<"$HIT")" | jq -c '{window, texts: [.documents[].records[].text]}'`. It prints `{"window":{"record_id":"backup.md:0","start":3,"end":19},"texts":["restore a backup"]}`.
- **Unknown document.** Fetch a root ID that does not exist. Run `control-fyrr mcp fetch-unknown fetch '{"source_id":"notes","root_id":"nope.md"}' | jq -c '.documents'`. It prints `[]`, an ordinary answer; `null` would mean the call failed.
- **Same bytes as the file.** Compare the fetched text with the source file. Run `control-fyrr mcp fetch-compare fetch '{"source_id":"notes","root_id":"backup.md"}' | texts | cmp - examples/quickstart/handbook/backup.md && echo same`, from the repository root. It prints `same`.
- **Proof.** Cite the transcripts of `fetch-id-mcp`, `fetch-id-http` and `fetch-id-mcp-http`: each shows the request and the text of `backup.md`.

## Gotchas

- `root_id` of a `local-files` document is its path inside the source
  folder (`backup.md`), not the absolute path; `source_uri` is the `file://`
  URI of the instance's private copy and differs between runs.
- A window needs the `record_id` and `revision` of the current text, taken
  from a hit of the same publication. After the document changes, the answer
  sets `document_changed` and returns no span.
- Window offsets count codepoints, not bytes.
- Fetched text keeps the file's trailing newline; an excerpt of a search hit
  may not. `texts` prints the text as stored, without adding a newline.
- The two commands of **By URI** and of **Window** share a shell variable:
  run each pair in one shell. Their searches exclude `deploys.md`, so the one
  hit is `backup.md` whatever the ranking.
- A denied document and an unknown one answer alike, by design. To prove a
  denial, use [Permissions](./permissions.md), which adds the control read.
