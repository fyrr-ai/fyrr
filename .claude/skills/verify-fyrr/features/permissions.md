# Permissions

Every source lists the reader tokens that may read it, and every caller acts
with its own tokens. A caller finds and fetches only documents of sources
that share a token with it; a forbidden document looks exactly like one that
does not exist.

## Sub-features

- `perm-search` leaves forbidden documents out of every search result.
- `perm-fetch` answers a fetch of a forbidden document with no documents.
- `perm-control` returns the same document to a reader who may read it.
- `perm-indistinguishable` answers forbidden and unknown documents alike.

## How to get to it (user POV)

- Run `fyrr reader search` as the stdio subject of the configuration (`ada`).
- Let an agent call `search` and `fetch` over MCP stdio as the same subject.
- Send the HTTP JSON routes with the API key of a reader (`ada` or `grace`).
- Call the tools through `POST /mcp` on the HTTP reader with such a key.

## Driving it with control-fyrr

Preconditions:

- Profile `permissions` at the baseline (`control-fyrr launch --profile
  permissions`), and `env.sh` loaded; the namespace is `quickstart`.
- `ada` (stdio subject and API key) holds `grp:engineering`; API key `grace`
  holds `grp:finance`. Source `handbook` admits `grp:engineering` and
  `grp:leadership`; source `payroll` admits `grp:finance`.
- All three documents contain the word `backup`.

- **Ada searches, CLI.** Search a word every document has. Run `control-fyrr run perm-ada-cli -- fyrr reader search backup --limit 5 | docs`. It lists `handbook/backup.md` and `handbook/deploys.md` and no `payroll/` line.
- **Ada searches, MCP.** Run `control-fyrr mcp perm-ada-mcp search '{"query":"backup","limit":5}' | docs`. The same two lines, no `payroll/` line.
- **Ada searches, HTTP.** Run `control-fyrr http perm-ada-http --as ada POST /v1/namespaces/quickstart/query '{"query":"salary bands payroll","limit":5}' | docs`. `HTTP 200`, and no `payroll/` line although the query is the payroll document's own words.
- **Ada fetches payroll, MCP.** Ask for the forbidden document by ID. Run `control-fyrr mcp perm-ada-fetch-mcp fetch '{"source_id":"payroll","root_id":"salaries.md"}' | jq -c '.documents'`. It prints `[]`, an ordinary answer; `null` would mean the call failed.
- **Ada fetches payroll, HTTP.** Run `control-fyrr http perm-ada-fetch-http --as ada POST /v1/namespaces/quickstart/fetch '{"source_id":"payroll","root_id":"salaries.md"}' | jq -c '.documents'`. `HTTP 200` and `[]`.
- **Ada fetches payroll, MCP over HTTP.** Run `control-fyrr http perm-ada-fetch-mcp-http --as ada POST /mcp '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch","arguments":{"source_id":"payroll","root_id":"salaries.md"}}}' | mcp_result | jq -c '.documents'`. `HTTP 200` and `[]`.
- **Forbidden looks like unknown.** Fetch the forbidden document and one that does not exist, and drop the metadata. Run `control-fyrr mcp perm-ada-denied fetch '{"source_id":"payroll","root_id":"salaries.md"}' | jq -c 'del(.metadata)'` and `control-fyrr mcp perm-ada-unknown fetch '{"source_id":"payroll","root_id":"nope.md"}' | jq -c 'del(.metadata)'`. Both print `{"documents":[],"next_cursor":null,"document_changed":false,"window":null}`.
- **Control: Grace searches.** Run the same search as the finance reader. Run `control-fyrr http perm-grace-http --as grace POST /v1/namespaces/quickstart/query '{"query":"backup","limit":5}' | docs`. The only line is `payroll/salaries.md`.
- **Control: Grace fetches payroll.** Run `control-fyrr http perm-grace-fetch-http --as grace POST /v1/namespaces/quickstart/fetch '{"source_id":"payroll","root_id":"salaries.md"}' | texts`. It prints `Salary bands for 2026 and the backup payroll contact.`
- **Control: Grace fetches payroll, MCP over HTTP.** Run `control-fyrr http perm-grace-fetch-mcp-http --as grace POST /mcp '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch","arguments":{"source_id":"payroll","root_id":"salaries.md"}}}' | mcp_result | texts`. It prints the same sentence.
- **Control: Grace cannot read the handbook.** Run `control-fyrr http perm-grace-fetch-handbook --as grace POST /v1/namespaces/quickstart/fetch '{"source_id":"handbook","root_id":"backup.md"}' | jq -c '.documents'`. It prints `[]`.
- **Proof.** Cite the denied reads (`perm-ada-fetch-mcp`, `perm-ada-fetch-http`, `perm-ada-fetch-mcp-http`, `perm-ada-http`) next to the controls (`perm-grace-http`, `perm-grace-fetch-http`, `perm-grace-fetch-mcp-http`). A denial without its control proves nothing: an empty answer is also what a broken index gives.

## Gotchas

- The stdio transport has one subject, the configuration's `stdio_user`
  (`ada`). Grace is reachable over HTTP only. Reading as Grace over stdio
  means publishing another identity (`stdio_user = "grace"`,
  `stdio_readers = ["grp:finance"]` and `fyrr writer commit`), which changes
  the instance: do it in a run of its own, as `examples/quickstart/run` does.
- A denial is a `200` with no documents, never `403`. `403 forbidden` means
  the key lacks the route's role, which is a different feature.
- Reader tokens are matched exactly. `grp:leadership` is on the handbook
  source but no key of this profile holds it.
- This profile uses `permissions = "fixed"`: every listed reader may read
  every document of the source. Per-document permissions synced from a
  connector are not covered by this instance.
- A changed grant reaches a running HTTP reader with its next version check,
  up to 30 seconds after the commit.
