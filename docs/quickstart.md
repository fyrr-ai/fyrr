# Quickstart

Index a Markdown folder on your machine, then search and fetch it from
Claude Code over MCP. No accounts or embedding keys are needed. With Docker,
[use the published image](#fastest-the-published-image) and skip the build.
The first build downloads dependencies. You need Linux x86-64, git, curl,
unzip, openssl, Python 3.11+, rustup, uv, a C compiler, jq, binutils and
about 10 GiB free. Install Claude Code before connecting the client.
Clone below, or use an existing checkout. Run the remaining commands from
its root in one shell.

For a larger first load, use `fyrr writer sync --full --timeout 1h` instead of
relying on the ten-minute per-source default. A killed sync keeps durable
prepared envelopes and cached vectors; wait for its source lease to expire before
retrying. See [interrupted syncs](deployment.md#scheduling) for details.

## Fastest: the published image

With Docker you can skip the build. This indexes a folder of Markdown with
the release image and needs only Docker, curl, openssl and Python 3:

```sh
mkdir fyrr-try && cd fyrr-try
curl -fsSLO https://raw.githubusercontent.com/fyrr-ai/fyrr/v0.1.0/examples/quickstart/toy-embeddings.py
python3 toy-embeddings.py &
openssl rand -hex 32 > identity.key && chmod 600 identity.key
export FYRR_IDENTITY_KEY="$(cat identity.key)"
export NOTES="$HOME/notes"          # your folder of Markdown
cat > fyrr.toml <<'TOML'
[namespace]
name = "notes"
[storage]
uri = "file:///home/fyrr/index"
[embedding]
endpoint = "http://127.0.0.1:8089/v1"
model = "toy-hash-1024"
dimensions = 1024
reader_live = true
[embedding.spend]
limit_microusd = 100000
max_call_microusd = 1
[identity]
stdio_user = "me"
stdio_readers = ["usr:me"]
hashing_key = { env = "FYRR_IDENTITY_KEY" }
[[sources]]
id = "notes"
type = "local-files"
permissions = "fixed"
readers = ["usr:me"]
[sources.scope]
paths = ["/notes"]
include = ["**/*.md"]
TOML
fyrr() {
  docker run --rm -i --network host -e FYRR_IDENTITY_KEY \
    -v "$PWD/fyrr.toml:/etc/fyrr/fyrr.toml:ro" -v "$NOTES:/notes:ro" \
    -v fyrr-quickstart:/home/fyrr ghcr.io/fyrr-ai/fyrr:0.1.0 --config /etc/fyrr/fyrr.toml "$@"
}
fyrr writer namespace create && fyrr writer sync && fyrr writer commit
fyrr reader search 'a question about your notes' --limit 3
```

The index lives in the Docker volume `fyrr-quickstart`; the folder must be
readable by other users, because the image runs as its own user. Connect
Claude Code to the same volume, then start Claude Code from this shell so
that the container gets `FYRR_IDENTITY_KEY` (in a new shell, first run
`export FYRR_IDENTITY_KEY="$(cat identity.key)"` in `fyrr-try`):

```sh
claude mcp add fyrr --scope user \
  -- docker run --rm -i --network host -e FYRR_IDENTITY_KEY \
  -v "$PWD/fyrr.toml:/etc/fyrr/fyrr.toml:ro" -v fyrr-quickstart:/home/fyrr \
  ghcr.io/fyrr-ai/fyrr:0.1.0 --config /etc/fyrr/fyrr.toml reader serve
```

The toy service matches words only; keep it running while you search. The
rest of this page builds fyrr from source and explains each step.

## 1. Build and start toy embeddings

```sh
git clone https://github.com/fyrr-ai/fyrr.git
cd fyrr
export PATH="$HOME/.cargo/bin:$PATH"
scripts/doctor --local
cargo build --locked -p fyrr
export PATH="$PWD/target/debug:$PATH" VIRTUAL_ENV="$PWD/connectors/.venv"
python3 examples/quickstart/toy-embeddings.py &
FYRR_TOY_EMBEDDINGS_PID=$!
for attempt in $(seq 1 50); do
  curl -fsS -o /dev/null http://127.0.0.1:8089/v1/models 2>/dev/null && break
  sleep 0.2
done
kill -0 "$FYRR_TOY_EMBEDDINGS_PID" && curl -fsS -o /dev/null http://127.0.0.1:8089/v1/models
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
uri = (Path(sys.argv[1]) / ".local/share/fyrr/notes").resolve().as_uri()
text = text.replace('"file:///home/you/.local/share/fyrr/notes"', json.dumps(uri))
Path(sys.argv[2]).write_text(text)
PY
[ -f "$HOME/.config/fyrr/identity.key" ] || (umask 077; openssl rand -hex 32 > "$HOME/.config/fyrr/identity.key")
chmod 600 "$HOME/.config/fyrr/identity.key"
export FYRR_IDENTITY_KEY="$(cat "$HOME/.config/fyrr/identity.key")"
fyrr config check
fyrr writer namespace create && fyrr writer sync && fyrr writer commit
fyrr reader search 'How do I restore a backup?' --limit 3
```

The example indexes the small, checked-in `examples/quickstart/handbook/`
as source `notes`, for stdio user `me` with reader token `usr:me`.
The top hit is `notes/backup.md`. To index your notes, change `sources.scope.paths`
to their absolute path. Relative source paths use the writer's working
directory. Repeat `fyrr writer sync && fyrr writer commit` after edits.
In a new shell, return to the checkout and restore `PATH`, `VIRTUAL_ENV`,
`FYRR_CONFIG` and `FYRR_IDENTITY_KEY` using the exports above. Keep
`identity.key`: readers and subsequent writes need the same value.
The storage directory must be on a local filesystem, used by one host and
user. For a shared/production deployment using S3, see
[deployment](deployment.md#minimal-production-setup).

## 3. Connect Claude Code

```sh
claude mcp add fyrr --scope user \
  -- "$PWD/target/debug/fyrr" --config "$FYRR_CONFIG" reader serve
```

Claude Code passes its own environment to the reader. Start Claude Code in
any directory, from a shell that has `FYRR_IDENTITY_KEY` exported; the key
is then not stored in Claude Code's settings. Without it, `/mcp` in Claude
Code shows the fyrr server as failed. Ask:

> Use fyrr search and fetch to answer: How do I restore a backup? Quote the handbook.

Claude calls `search` and `fetch` for `notes/backup.md`. Its answer should quote:
"To restore a backup, run the restore job and verify the checksum."
The checked-in handbook keeps this example independent of reference-doc edits.
`--scope user` registers fyrr for all your projects;
other MCP clients use the same executable, arguments and environment.
[Connect an agent](connect.md) covers Codex, HTTP readers and the optional
Claude Code plugin.

## 4. Add Slack or another connector

Use a user token with the [Slack scopes](connectors/slack.md#setup),
including `users:read`; its user must belong to matched private channels.
Add this source to your configuration and publish again:

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
token = { env = "FYRR_SLACK_TOKEN" }
```

```sh
(cd connectors && uv sync --locked --extra slack)
export FYRR_SLACK_TOKEN="xoxp-…"
fyrr writer sync && fyrr writer commit
```

See [Slack](connectors/slack.md) for token scopes and channel rules, or the
[connectors index](connectors/README.md). Only the writer needs the token;
the reader checks for publications automatically. Stop toy embeddings with
`kill "$FYRR_TOY_EMBEDDINGS_PID"`; remove the client entry with `claude mcp remove fyrr --scope user`.

Stopping toy embeddings leaves Claude registered; searches then use lexical
retrieval until the service is restarted.

## Optional: see permissions in action

`examples/quickstart/run` checks local search, fetch and permissions without
Docker; `examples/quickstart/run --s3` checks the same flow on the fixture.
The [permission configuration](../examples/quickstart/permissions.toml)
publishes handbook and payroll folders. Ada (`grp:engineering`) sees only
the handbook; Grace (`grp:finance`) sees payroll. The runner verifies both
identities; Ada's payroll fetch returns no documents.
