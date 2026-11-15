# Search your Markdown from Claude Code

Index a Markdown folder on your machine, then search and fetch it from
Claude Code over MCP. No Docker, accounts or embedding keys are needed.
Allow about ten minutes, mostly for the first build. You need Linux x86-64
and the [build prerequisites](../CONTRIBUTING.md#build-host), except Docker.
Run the commands from the repository root in one shell.

## 1. Build and start toy embeddings

```sh
scripts/doctor --local
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --locked -p fyrr
export PATH="$PWD/target/debug:$PATH" VIRTUAL_ENV="$PWD/connectors/.venv"
python3 examples/quickstart/toy-embeddings.py &
```

The toy service matches words. For semantic results, choose an
[OpenAI-compatible embeddings endpoint](configuration.md#embedding) and
publish into a fresh namespace when changing models.

## 2. Choose the folder and publish

Copy the [example configuration](../examples/quickstart/fyrr.toml), setting
its storage URI to a private directory under your home:

```sh
mkdir -p "$HOME/.config/fyrr"
export FYRR_CONFIG="$HOME/.config/fyrr/quickstart.toml"
python3 - "$HOME" "$FYRR_CONFIG" <<'PY'
from pathlib import Path
import json, sys
text = Path("examples/quickstart/fyrr.toml").read_text()
uri = (Path(sys.argv[1]) / ".local/share/fyrr/notes").as_uri()
text = text.replace('"file:///home/you/.local/share/fyrr/notes"', json.dumps(uri))
Path(sys.argv[2]).write_text(text)
PY
export FYRR_IDENTITY_KEY="${FYRR_IDENTITY_KEY:-$(openssl rand -hex 32)}"   # keep this value for future sessions
fyrr config check
fyrr writer namespace create && fyrr writer sync && fyrr writer commit
fyrr reader search mirror --limit 3
```

The example indexes `docs/` as source `notes`, for stdio user `me` with
reader token `usr:me`. To index your notes, change `sources.scope.paths`
to their absolute path. Relative source paths use the writer's working
directory. Repeat `fyrr writer sync && fyrr writer commit` after edits.
Keep the identity key: readers and subsequent writes need the same value.
The storage directory must be on a local filesystem, used by one host and
user. For a shared/production deployment using S3, see
[deployment](deployment.md#minimal-production-setup).

## 3. Connect Claude Code

```sh
claude mcp add fyrr --scope user \
  -e FYRR_IDENTITY_KEY="$FYRR_IDENTITY_KEY" \
  -- "$PWD/target/debug/fyrr" --config "$FYRR_CONFIG" reader serve
```

Start Claude anywhere and ask:

> Use fyrr search and fetch to find where the reader keeps its local mirror,
> and quote the documentation.

Claude calls `search` and `fetch`. The default mirror is
`~/.cache/fyrr/notes`. `--scope user` registers fyrr for all your projects;
other MCP clients use the same executable, arguments and environment.

## Optional: see permissions in action

`examples/quickstart/run` checks local search, fetch and permissions without
Docker; `examples/quickstart/run --s3` checks the same flow on the fixture.
The [permission configuration](../examples/quickstart/permissions.toml)
publishes handbook and payroll folders. Ada (`grp:engineering`) sees only
the handbook; Grace (`grp:finance`) sees payroll. The runner verifies both
identities and refuses Ada's payroll fetch.

## 4. Add Slack or another connector

Add a source to your configuration and publish again:

```toml
[[sources]]
id = "slack"
type = "slack"
permissions = "fixed"
readers = ["usr:me"]
[sources.scope]
include = ["general", "eng-*"]
[sources.settings]
workspace_id = "T0123ABCD"
token = { env = "SLACK_USER_TOKEN" }
```

```sh
export SLACK_USER_TOKEN="xoxp-…"
fyrr writer sync && fyrr writer commit
```

See [Slack](connectors/slack.md) for token scopes and channel rules, or the
[connectors index](connectors/README.md). Only the writer needs the token;
Claude sees publications within 30 seconds. Stop toy embeddings with
`kill %1`; remove the client entry with `claude mcp remove fyrr --scope user`.
