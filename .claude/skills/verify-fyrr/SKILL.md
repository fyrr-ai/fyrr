---
name: verify-fyrr
description: Launch a disposable fyrr instance (the `fyrr` CLI, its MCP stdio server and its HTTP reader) on local storage, drive it the way a user or an agent would, and capture transcripts as proof. Use when asked to verify, prove, reproduce or demo fyrr behavior on the real binary rather than through tests: publishing documents, search, fetch, permissions, the HTTP JSON API.
---

# Verify fyrr

fyrr has no UI. A user touches it in three ways, and a proof drives the same
three:

- the **CLI**: `fyrr config`, `fyrr writer` (publish) and `fyrr reader` (read);
- **MCP over stdio**: `fyrr reader serve`, as Claude Code or Codex start it;
- the **HTTP reader**: `fyrr reader serve` with `[reader.http]`, which serves
  `/readyz`, `/mcp` and the JSON routes under `/v1/namespaces/`.

The helper `control-fyrr` (next to this file) starts one disposable instance
and runs every drive against it, writing a transcript of each. It is not on
`PATH`. Load it, and the three filters the recipes pipe into, at the start of
every shell that runs a command of this skill (bash or zsh, anywhere inside
the checkout):

```sh
. "$(git rev-parse --show-toplevel)/.claude/skills/verify-fyrr/env.sh"
```

A tool that starts a new shell for each command needs that line in each
command. With it, every command in this skill and in the feature files runs
exactly as written. Paths in this file are relative to the repository root.

Read [features/README.md](features/README.md) before driving: it is the map
of what to verify and how. This file is how to get an instance.

Not covered here, each with its own entry point: the document write API and
`fyrr writer serve` (`scripts/bench-write` shows the configuration), S3
storage (`examples/quickstart/run --s3`, `scripts/s3-fixture`), OIDC login
(`scripts/keycloak-fixture`), the cloud connectors, the images and the Helm
chart (`scripts/images`), the website in `site/` and the clients in
`clients/`.

## Launch

Build once per checkout, and again after every change to `crates/`. Always
build this way, through mise and mr-boxington:

```sh
mise install                                # the tools mise pins, mbx among them
mise exec -- scripts/doctor --local         # toolchain, protoc, connectors/.venv
mise exec -- bash -c 'source scripts/lib.sh && cargo build --locked -p fyrr'
```

The first two prepare a checkout and change nothing when it is prepared; run
them when unsure. The build goes through mise because `scripts/lib.sh` wraps `cargo build` in
`mbx` (mr-boxington), which `mise.toml` pins and which shares compiler
results between checkouts. `mise exec --` puts that `mbx` on `PATH` whether
or not the shell has mise shims; `lib.sh` itself puts rustup's Cargo first.
The build writes `target/debug/fyrr`: seconds when the shared store has the
results, several minutes when it does not. Never run two builds in one
checkout at once, and do not mix in a plain `cargo build`: it does not share
the store and recompiles the dependency graph. `python3` and `jq`, which the
drives use, are not pinned by `mise.toml`; `scripts/doctor` checks that they
are on `PATH`. This skill proves behavior and replaces no test: the gate for
a change is still `scripts/ci --quick` (CONTRIBUTING.md), which builds through
the same mbx.

Then start one instance, with the profile the feature file names:

```sh
control-fyrr launch                         # profile notes: one user, one folder
```

or

```sh
control-fyrr launch --profile permissions   # two folders, readers ada and grace
```

`launch` needs nothing but the binary and no data from an earlier run.
Starting from nothing, it

1. creates `target/verify-fyrr/runs/<RUN>/` and copies the example documents
   of both profiles into it: `examples/quickstart/handbook` to
   `<RUN>/docs/handbook` and `examples/quickstart/payroll` to
   `<RUN>/docs/payroll`. It also creates `/dev/shm/verify-fyrr-<RUN>/` for
   the storage, the CLI's cache and the HTTP reader's mirror (about 1 MiB of
   memory; inside the run directory on a host without `/dev/shm`);
2. starts `examples/quickstart/toy-embeddings.py` on a free port;
3. writes `fyrr.toml` from the example configuration
   (`examples/quickstart/fyrr.toml` or `permissions.toml`) with only the
   storage URI, the embeddings endpoint and the document paths replaced, and
   one API key per reader added;
4. publishes: `fyrr config check`, `writer namespace create`, `writer keys`,
   `writer sync`, `writer commit`, `reader check`;
5. starts the HTTP reader from `fyrr-http.toml` (the same file plus
   `[reader.http]`) on a free loopback port and waits for `GET /readyz` to
   answer 200.

It is ready when it prints `ready: namespace …`, followed by the lines
`export FYRR_VERIFY_RUN=<RUN>` and `evidence: <directory>`; it exits nonzero
with the reason when a step fails. On an idle machine a launch takes about 3
seconds, a drive 0.1 to 0.2 seconds and a `fyrr writer sync` 1 second, so
all five feature files run in under a minute. With every core busy
compiling, a launch has taken up to 30 seconds and each step two to four
times as long.

The storage is in memory on purpose: publishing makes over a hundred `fsync`
calls, and on a disk that other builds are writing to each can take seconds,
which turned a launch on disk into minutes. Give `launch` a timeout of 5
minutes all the same.

Every later command acts on `$FYRR_VERIFY_RUN`. Without the variable they act
on the only run that exists, and refuse to choose between two. So with one
instance nothing needs exporting; with two, put the export line next to the
`env.sh` line in every shell.

| Profile | Namespace | Sources | stdio subject | API keys (`http --as`) |
| --- | --- | --- | --- | --- |
| `notes` | `notes` | `notes` = `<RUN>/docs/handbook/**/*.md` | `me` (`usr:me`) | `me` |
| `permissions` | `quickstart` | `handbook` = `<RUN>/docs/handbook` (`grp:engineering`, `grp:leadership`), `payroll` = `<RUN>/docs/payroll` (`grp:finance`) | `ada` (`grp:engineering`) | `ada`, `grace` (`grp:finance`) |

`control-fyrr http` without `--as` uses the first key of the profile, `me` or
`ada`. Seed documents: `handbook/backup.md` ("To restore a backup, run the restore
job and verify the checksum."), `handbook/deploys.md`, `payroll/salaries.md`.

Instances are isolated: each has its own storage, mirror, ports, keys and
document copies, so several run side by side, also in several checkouts.
Never drive an instance you did not launch in this run, and never point a
drive at a user's own index (`~/.config/fyrr`, `~/.local/share/fyrr`).

## Doctor

```sh
control-fyrr doctor
```

Read-only towards the published namespace. It prints one `ok` or `FAIL` line
per check, then `doctor: run <RUN> (<profile>): OK` or the number of
problems, and exits 1 on any `FAIL`. Run it after `launch` and before the
first drive, after every rebuild, and whenever anything looks off. It checks
that

- the binary is the full build;
- the binary is as new as every file it was compiled from (Cargo's
  `target/debug/fyrr.d`, plus `Cargo.toml` and `Cargo.lock`), so an edited
  test does not count and an edited source does. On `FAIL`: rebuild;
- the toy embeddings and the HTTP reader are the processes this run started,
  and both answer;
- the HTTP reader runs the binary that is on disk. A rebuild replaces the
  file, and a reader started before it keeps running the old code. On
  `FAIL`: `cleanup`, then `launch` again;
- `fyrr config check` and `fyrr reader check` pass.

The CLI and `control-fyrr mcp` start the binary on disk each time, so only
the HTTP reader can be behind a rebuild.

## Drive

Every drive takes a label, which names its transcript, and prints
`evidence: <file>` on stderr.

```sh
# The CLI, with fyrr on PATH and the instance's FYRR_CONFIG and identity key,
# from the repository root. Exits with the command's exit code.
control-fyrr run example-cli -- fyrr reader search 'How do I restore a backup?' --limit 3

# One MCP stdio session started in another directory, as a client starts it:
# initialize, then one tools/call. Prints the tool's structuredContent. On a
# JSON-RPC error or a tool error it prints the whole answer and exits 1.
control-fyrr mcp example-mcp fetch '{"source_id":"notes","root_id":"backup.md"}'

# The HTTP reader, with the API key of a reader as the bearer token. Prints
# the body, and `HTTP <status>` on stderr; exits 0 whatever the status.
# `--as none` sends no key, `--as invalid` a key that is not published.
control-fyrr http example-http POST /v1/namespaces/notes/query '{"query":"backup","limit":5}'
control-fyrr http example-ready --as none GET /readyz
```

`run` takes any command, not only `fyrr`: its environment also has
`FYRR_VERIFY_DOCS` (the instance's document folder, `<RUN>/docs`, for
journeys that edit documents) and the API keys as `FYRR_VERIFY_KEY_ME`, or
`FYRR_VERIFY_KEY_ADA` and `FYRR_VERIFY_KEY_GRACE`. Tools are `search`,
`fetch`, `list_filters` and `aggregate`; their arguments are in `docs/mcp-tools.md`, the
JSON routes in `docs/http-api.md`, the commands in `docs/cli.md`.

Stable handles: a document is `source_id` plus `root_id` (the path inside
its source folder, such as `backup.md`); hits are `.hits[]` of a search
result, documents `.documents[]` of a fetch. Assert on those with `jq`, never
on scores or on the order of fields.

## Evidence

Each `run`, `mcp` and `http` writes
`target/verify-fyrr/evidence/<RUN>/NN-<label>.txt`: the command or request,
stdout, stderr and the exit code, or the JSON-RPC frames in both directions,
or the HTTP status and body. `launch` writes the publish steps the same way.
`NN` counts the transcripts of the run in order, so a label used twice gives
two files; cite a transcript by its file name. The run's secrets are replaced
by `<redacted>` in the transcripts and in what the helper prints.

A proof must:

- drive the real path: the CLI, an MCP session or the HTTP listener of the
  built binary. A `cargo test` run, a call into a crate or a reading of the
  code is not a proof of user-visible behavior;
- show the action and the resulting state, not only the final state: for a
  change, the search before it, the `writer sync` and `writer commit` that
  publish it, and the search after it;
- verify side effects from a second view: after a publish, `fyrr writer
  status` and a read through another surface (CLI and MCP, or CLI and HTTP);
- cover every entry point the feature file lists, or name the ones it
  skipped. A feature proven over the CLI alone is not proven over MCP;
- report a failed expectation as a failure, with the transcript. Do not
  change the journey to make it pass.

Mocks: the toy embeddings service is the only stand-in, at the boundary
where production calls an embeddings endpoint. It matches words, not
meaning, so it proves nothing about semantic ranking. `fyrr writer sync
--dry-run` runs the providers and validates their output without writing;
when a proof relies on that, show it (`fyrr writer status` before and after).

## Cleanup

```sh
control-fyrr cleanup
```

Stops the two processes this run started, by PID and only if the PID still
belongs to the process it started, copies the service logs into the evidence
directory and removes `target/verify-fyrr/runs/<RUN>/` and
`/dev/shm/verify-fyrr-<RUN>/`. It never touches
`target/verify-fyrr/evidence/`. Run it after a failed `launch` too; `launch`
prints the command with the run ID before it starts anything. A second
`cleanup` of the same run finds no instance and exits 1. Never `pkill fyrr`
or kill by name: other checkouts and the user's own readers run the same
binary.

## Helpers

| Command | Does |
| --- | --- |
| `. "$(git rev-parse --show-toplevel)/.claude/skills/verify-fyrr/env.sh"` | Defines `control-fyrr` and the filters `docs`, `texts` and `mcp_result`. |
| `control-fyrr launch [--profile notes\|permissions]` | Starts an instance and publishes the seed documents. |
| `control-fyrr doctor` | Checks that the instance is worth driving. |
| `control-fyrr run LABEL -- COMMAND [ARG...]` | Runs a command in the instance's environment and records it. |
| `control-fyrr mcp LABEL TOOL [JSON_ARGUMENTS]` | One MCP stdio session with one tool call. |
| `control-fyrr http LABEL [--as KEY\|none\|invalid] METHOD PATH [JSON_BODY]` | One request to the HTTP reader. |
| `control-fyrr cleanup` | Stops the instance, keeps the evidence. |
