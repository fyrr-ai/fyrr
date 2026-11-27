# fyrr verification map

This directory is the maintained source for verifying the user-facing
behavior of fyrr. Read this index before driving an instance, then use the
matching feature file as the recipe. [SKILL.md](../SKILL.md) says how to
build, launch, check and clean up.

## Baseline preconditions

- `target/debug/fyrr` is built from the code under test.
- The shell has loaded `env.sh`:
  `. "$(git rev-parse --show-toplevel)/.claude/skills/verify-fyrr/env.sh"`.
  It defines `control-fyrr` and the filters `docs`, `texts` and
  `mcp_result`. A tool that starts a new shell per command repeats the line
  in every command.
- `control-fyrr launch` (or `launch --profile permissions`, where a feature
  says so) printed `ready:`. With more than one instance running,
  `FYRR_VERIFY_RUN` is exported as `launch` printed it.
- `control-fyrr doctor` ends with `doctor: run <RUN> (<profile>): OK`.
- The seed is the three example documents. Profile `notes`: source `notes`
  with `backup.md` and `deploys.md`, readable by stdio user `me` and API key
  `me`. Profile `permissions`: source `handbook` with the same two files
  (`grp:engineering`, `grp:leadership`) and source `payroll` with
  `salaries.md` (`grp:finance`); stdio user and API key `ada` hold
  `grp:engineering`, API key `grace` holds `grp:finance`.
- Never drive an instance this run did not launch.

## Driving conventions

- Start every recipe from the baseline state unless its preconditions say
  otherwise. A recipe that publishes changes leaves the instance changed:
  `cleanup` and `launch` again before a recipe that needs the baseline.
- Treat every command as literal: with `env.sh` loaded it runs as written.
  Keep quoted JSON, names and flags unchanged.
- CLI actions go through `control-fyrr run LABEL -- fyrr …`, MCP tool calls
  through `control-fyrr mcp LABEL TOOL [JSON_ARGUMENTS]` and HTTP requests
  through `control-fyrr http LABEL [--as KEY|none|invalid] METHOD PATH
  [JSON_BODY]`. Without `--as` the request carries the profile's first key:
  `me`, or `ada`.
- Give every action its own label: the label names its transcript.
- Search and fetch print JSON on stdout. `| docs` lists the documents of a
  search result, one `source_id/root_id` per line; `| texts` prints the text
  of a fetch result; `| mcp_result` takes the tool result out of the JSON-RPC
  answer of `POST /mcp`, so that `docs` and `texts` can follow it.
- Results are on stdout. Stderr carries `evidence: <file>` from the helper,
  `HTTP <status>` from `control-fyrr http`, diagnostic lines from `fyrr
  reader` (`telemetry …`, `authorization_cache …`) and a `warning:
  sources[0].permissions: fixed connection: …` line from every `fyrr writer`
  command. None of them is a failure.
- An exit code a bullet states is that of `control-fyrr`; such a bullet has
  no pipe. Behind a pipe the exit code is the filter's, so read the printed
  value: a failed call never prints what the bullet expects. After a failed
  MCP call `docs` and `texts` stop with a `jq` error, and `jq -c
  '.documents'` or `jq -c '.hits'` prints `null`, not `[]`.
- Assert on which documents are present or absent and on their text. Never
  assert on `score`, on telemetry or on `publication_id` values.

## Proof and skip reporting

- Capture the action and the resulting state, not only the final state.
- A read proof is the transcript of the request and its result on each
  entry point the feature lists: CLI, MCP stdio and HTTP.
- A publish proof includes the read before the change, the `writer sync` and
  `writer commit` transcripts, the read after it and `fyrr writer status`.
- A permission proof includes the denied read and a control: the same
  request by a reader who may read the document, returning it.
- Name the feature ID and the entry point with every transcript you cite,
  and cite it by file name (`NN-<label>.txt`): a label used twice gives two
  files.
- Report an entry point you could not reach with the command you tried and
  the precondition that was not met.
- Do not report a skipped entry point as verified through another one.
- When an expectation fails, report the failure with its transcript. Do not
  change the recipe so that it passes.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the
user-visible behavior. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behavior.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with control-fyrr` starts with `Preconditions:` and uses
   labeled bullets that pair each user action with an exact command and its
   observable result. The last bullet, `Proof`, names the transcripts to cite.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable
handles, required state, commands, and observable proof.

## Features

- [Publish documents](./publish.md) covers adding, changing and deleting
  documents of a source, the dry run, and what readers see before and after
  the commit.
- [Search](./search.md) covers a query over the CLI, MCP and HTTP, output
  modes, filters, several queries, listing and refused arguments.
- [Fetch](./fetch.md) covers reading a document by ID, by URI, as a batch
  and as a window of a hit, and the empty answer for an unknown document.
- [Permissions](./permissions.md) covers that each reader finds and fetches
  only what its reader tokens allow, on every entry point.
- [HTTP reader](./http-reader.md) covers readiness, bearer keys, the JSON
  routes, MCP over HTTP and the error answers.

## Not mapped yet

The document write API (`POST /v1/namespaces/{namespace}/documents` on
`fyrr writer serve`), the MCP tool `list_filters`, maintenance and
recovery (`writer maintain`, `verify`, `restore`, `rollback`, `source
retire`, `source purge`, namespace `copy`, `delete`, `read-only`), S3
storage, OIDC login, directories and the cloud connectors. A proof that
needs one of them says so and adds its feature file.
