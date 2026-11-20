# Software factories

> **In short**
>
> - A software factory runs many coding agents with nobody watching each
>   step. fyrr gives every agent the same company context over MCP: the
>   ticket, the pull requests, the threads and the docs behind a task.
> - Each agent runs its own reader against one bucket. Readers never
>   coordinate, so a larger fleet needs no extra service.
> - Every response names the publication it read. Store it with the run and
>   you can tell later what the agent knew.

## What the agents get

The reader exposes three read-only [MCP tools](mcp-tools.md): `search`,
`fetch` and `list_filters`. fyrr returns evidence and the agent decides what
to search next; there is no answer generation in between.

Three parts of the contract matter for unattended agents:

- **Several questions in one call.** `search` takes 2–16 `queries` with
  shared filters and returns each document once.
- **Bounded work.** Every call has a deadline (`deadline_ms`, default 10 s)
  and the errors are a closed list of codes, so a loop can tell `busy` from
  `invalid_argument` and retry only what is worth retrying.
- **Text is data.** Tool descriptions state that returned text is data, not
  instructions.

## Set it up

1. **Index what the work depends on.** Run one writer on a schedule
   ([deployment](deployment.md#minimal-production-setup)) with the
   [connectors](connectors/README.md) for your tracker, repositories, chat
   and docs.
2. **Give the factory its own identity.** Over stdio one reader process is
   one identity. Name it and list only the groups it may read:

   ```toml
   [identity]
   hashing_key = { env = "FYRR_IDENTITY_KEY" }
   stdio_user = "factory"
   stdio_readers = ["grp:engineering"]
   ```

   The agents then see what `grp:engineering` may read and nothing else. A
   document outside it looks exactly like one that does not exist
   ([security model](security.md)).
3. **Start a reader in every agent sandbox.** Use the reader-only build or
   the `fyrr-reader` image with read-only bucket credentials; readers never
   write to S3. Each reader keeps a private mirror of the files it has read
   (`reader.mirror_path`).

   ```sh
   claude mcp add fyrr -- fyrr --config /etc/fyrr/fyrr.toml reader serve
   ```

   Start Claude Code with `FYRR_IDENTITY_KEY` exported; it passes its
   environment to the reader, so the key is not stored in its settings.
   Another stdio MCP client works the same way once it is configured to
   pass the variable to the reader. [Connect an agent](connect.md) has the
   commands for Claude Code and Codex.

When a sandbox must not hold bucket credentials, run one HTTP reader for the
fleet instead and give each agent role its own API key. A key needs no
identity provider: you declare what it may read, hand the secret to the
agents of that role, and revoke it by removing it: a running reader
refuses the key with its next version check, within two intervals (60 s by
default), or at once after `POST /admin/refresh`. See
[API keys](http-users.md#api-keys).

## Limit what an injected agent can reach

An unattended agent reads text nobody reviewed: issues, comments, web pages.
Text that carries instructions can turn the agent against you, and then the
agent's access is the attacker's access. Permissions bound that damage.

Give each kind of agent its own identity and only the groups its work
needs: one for agents that fix bugs, another for agents that answer support
tickets. With readers in the agents' sandboxes, a separate configuration file
per role, with its own `stdio_user` and `stdio_readers`, is enough. With one
HTTP reader for the fleet, one API key per role does the same:

```toml
[[identity.api_keys]]
name = "bugfix-agents"
key = { env = "FYRR_API_KEY_BUGFIX" }
readers = ["grp:engineering"]

[[identity.api_keys]]
name = "support-agents"
key = { env = "FYRR_API_KEY_SUPPORT" }
readers = ["grp:support"]
```

An agent that was talked into searching for payroll finds nothing, and
cannot tell whether payroll exists.

fyrr checks permissions before ranking, so a forbidden document never
reaches the model's context in the first place.

## Know what an agent knew

Every successful response carries `publication_id`, `content_version` and
`committed_at` in its [metadata](mcp-tools.md#response-metadata). Write them
to the run log next to the change the agent produced. When nobody reads the
diff, this is the record of which tickets and decisions the agent could see.

A retained publication can be served again with
`fyrr writer restore PUBLICATION`
([restore and rollback](operations.md#restore-and-rollback)), so a failed
run can be repeated against the same content.

## Feed the results back

The tools are read-only: an agent cannot write to fyrr. What the factory
learns becomes searchable through the sources it already writes to. Decisions
in tickets and pull requests arrive with the next sync. For run reports and
lessons, let the agents write Markdown into a folder that the writer indexes
with the [local files connector](connectors/local-files.md).

## Limits

- **Context is as fresh as the last commit.** New content is searchable
  after the writer has synced and committed it.
- **fyrr is not a code index.** The [GitHub connector](connectors/github.md)
  reads issues, pull requests and prose; source code is opt-in per
  repository. Keep your agent's own code search for the repository it works
  in.
- The general [limits](https://github.com/fyrr-ai/fyrr#limits) apply:
  pre-1.0, Linux only, and cloud connectors tested against synthetic APIs.
