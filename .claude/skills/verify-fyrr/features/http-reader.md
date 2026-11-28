# HTTP reader

`fyrr reader serve` with `[reader.http]` is the reader a team shares: it
answers a readiness probe, authenticates every request by a bearer key, and
serves search and fetch as JSON routes and as MCP over HTTP.

## Sub-features

- `http-ready` answers `GET /readyz` with 200 and the serving publication.
- `http-auth` refuses a request without a bearer key or with an unknown one.
- `http-json` serves `query` and `fetch` as plain JSON.
- `http-mcp` serves the same tools at `POST /mcp`.
- `http-role` refuses a route whose role the key lacks.
- `http-namespace` refuses a malformed namespace name and hides a missing one.

## How to get to it (user POV)

- Probe `GET /readyz`, as a load balancer does.
- Send `POST /v1/namespaces/{namespace}/query` or `.../fetch` with `Authorization: Bearer <key>`.
- Add the reader to an MCP client as `http://<address>/mcp`; the client sends `POST /mcp`.
- Send `GET /v1/namespaces/{namespace}` for the namespace report (needs a key with the `admin` or `list` role).

## Driving it with control-fyrr

Preconditions:

- Profile `notes` at the baseline, and `env.sh` loaded. Its only API key is
  `me`, with the `read` role; `control-fyrr http` uses it unless `--as`
  names another or `none`.
- `control-fyrr http` prints `HTTP <status>` on stderr and the body on
  stdout, and exits `0` whatever the status.

- **Readiness.** Probe without a key. Run `control-fyrr http http-ready --as none GET /readyz | jq -c '{ready, publication: (.publication_id != null)}'`. `HTTP 200` and `{"ready":true,"publication":true}`.
- **No key.** Search without a bearer token. Run `control-fyrr http http-no-key --as none POST /v1/namespaces/notes/query '{"query":"backup"}'`. `HTTP 401` and `{"error":"unauthorized"}`.
- **Unknown key.** Search with a well-formed key that is not published. Run `control-fyrr http http-unknown-key --as invalid POST /v1/namespaces/notes/query '{"query":"backup"}'`. `HTTP 401` and `{"error":"unauthorized"}`.
- **JSON query.** Search with the key. Run `control-fyrr http http-query POST /v1/namespaces/notes/query '{"query":"backup","limit":5}' | docs`. `HTTP 200` and the first line is `notes/backup.md`.
- **JSON fetch.** Fetch with the key. Run `control-fyrr http http-fetch POST /v1/namespaces/notes/fetch '{"source_id":"notes","root_id":"backup.md"}' | texts`. `HTTP 200` and `To restore a backup, run the restore job and verify the checksum.`
- **MCP initialize.** Open MCP over HTTP. Run `control-fyrr http http-mcp-initialize POST /mcp '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"verify","version":"1"}}}' | jq -c '.result | {name: .serverInfo.name, instructions: (.instructions | contains("call search first"))}'`. `HTTP 200` and `{"name":"fyrr","instructions":true}`.
- **MCP tools.** List the tools. Run `control-fyrr http http-mcp-tools POST /mcp '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' | jq -c '[.result.tools[].name] | sort'`. It prints `["aggregate","fetch","list_filters","search"]`.
- **MCP tool call.** Search through MCP. Run `control-fyrr http http-mcp-search POST /mcp '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search","arguments":{"query":"backup","limit":5}}}' | mcp_result | docs`. `HTTP 200` and the first line is `notes/backup.md`.
- **Role.** Ask for the namespace report with a `read`-only key. Run `control-fyrr http http-report GET /v1/namespaces/notes`. `HTTP 403` and `{"error":"forbidden"}`.
- **Malformed namespace.** Use a name that is not a namespace name. Run `control-fyrr http http-bad-namespace POST /v1/namespaces/Bad_Name/query '{"query":"backup"}'`. `HTTP 404` and `{"error":"invalid_argument"}`: the status is 404 for a malformed name, although the code is the one a refused argument has (`docs/http-api.md`, Routes).
- **Missing namespace.** Use a well-formed name that does not exist. Run `control-fyrr http http-missing-namespace POST /v1/namespaces/other/query '{"query":"backup"}'`. `HTTP 401` and `{"error":"unauthorized"}`: the same answer as a namespace the key is not bound in.
- **Proof.** Cite `http-ready`, `http-no-key`, `http-unknown-key`, `http-query` and `http-mcp-search`: readiness, the refusals without a key and with an unknown one, and the same first hit over the JSON route and over MCP.

## Gotchas

- The reader requires `Host` to equal the authority of `public_url` on every
  route but `/readyz`. `control-fyrr http` sends the right one; a hand-made
  `curl` to `localhost:<port>` instead of `127.0.0.1:<port>` gets `403
  host_mismatch`.
- `POST /mcp` answers `406` unless the client accepts both
  `application/json` and `text/event-stream`. `control-fyrr http` sends that
  `Accept` header on every request.
- A request with an `Origin` header is refused on every route (`403`
  `invalid_argument`).
- The reader picks up a new publication at its next version check, every 30
  seconds; a JSON request with `"consistency":"strong"` checks at once.
- Each key is limited to 120 requests per minute; a loop of drives over it
  answers `429 rate_limited`.
- The profile's keys have the `read` role only. The positive paths of the
  namespace report, `GET /v1/namespaces` and `recall` need a key with
  `list` or `admin`, which this instance does not publish: report those
  entry points as not reached, not as verified.
- `POST /admin/refresh` answers `404` here: the instance configures no
  `operator_credential`.
- The listener speaks plain HTTP on loopback. TLS and OIDC login are not
  part of this instance.
