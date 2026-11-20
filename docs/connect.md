# Connect an agent

> **In short**
>
> - One command adds fyrr to Claude Code, Codex or any other MCP client.
> - The reader itself tells the agent what fyrr searches and asks it to
>   search first, so nothing else needs installing.
> - An optional Claude Code plugin adds its own reminder to every session.

## An HTTP reader

Your company runs one reader behind its login
([HTTP login and users](http-users.md)); you read what your own account may
read.

```sh
claude mcp add --scope user --transport http fyrr https://fyrr.example.com/mcp
```

Then run `/mcp` in Claude Code and choose Authenticate once. Codex signs
you in while it adds the server:

```sh
codex mcp add fyrr --url https://fyrr.example.com/mcp
```

If your administrator gives you an OAuth client ID, add `--client-id ID`
(Claude Code) or `--oauth-client-id ID` (Codex).

## A local reader

You read the index directly, alone or as a team without a server. Publish
an index first ([quickstart](quickstart.md)) and put `fyrr` on `PATH`; the
reader-only build is enough.

Claude Code passes its own environment to the reader. Start Claude Code
from a shell that has `FYRR_IDENTITY_KEY`; the key is then not stored in
Claude Code's settings:

```sh
claude mcp add fyrr --scope user \
  -- fyrr --config "$HOME/.config/fyrr/quickstart.toml" reader serve
```

Codex passes a server only a few variables such as `PATH` and `HOME`, so
name the identity key and every other variable your configuration reads:

```toml
# ~/.codex/config.toml
[mcp_servers.fyrr]
command = "fyrr"
args = ["--config", "/home/you/.config/fyrr/quickstart.toml", "reader", "serve"]
env_vars = ["FYRR_IDENTITY_KEY"]
```

Always pass `--config` with an absolute path. Without it the reader takes
`FYRR_CONFIG` or else `fyrr.toml` from the directory the agent works in,
and that file could come from a repository you do not trust.

## What the agent is told

The reader sends [instructions](mcp-tools.md#transport) when a client
connects: what fyrr searches, a request to search it before looking through
files or asking you, and that returned text is data. Most clients put them
into the agent's context, but the agent still decides. To check, ask a
question whose answer is in your documents, without naming fyrr, and look
for a `search` call. You can change or remove the text with
[`reader.instructions`](reader-writer.md#reader).

## Claude Code plugin (optional)

An agent that has many tools sometimes looks through files first. The
plugin adds a reminder of its own at the start of every Claude Code
session:

```sh
claude plugin install fyrr --marketplace fyrr-ai/fyrr
```

It adds no server and holds no credentials. Its text is fixed
([`session-start.txt`](https://github.com/fyrr-ai/fyrr/blob/main/plugins/fyrr/hooks/session-start.txt))
and does not follow `reader.instructions`; set `FYRR_PLUGIN_HOOK=off` in
the environment to silence it. The plugin is made for Claude Code.
