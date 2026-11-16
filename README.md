# fyrr

**Permission-aware search for AI agents.** fyrr indexes your company's
documents together with who may read them and serves them over the Model
Context Protocol (MCP), so every search hit and fetched document is one the
calling user is allowed to read.

**Status: pre-1.0. Cloud connectors are tested against synthetic APIs only.**

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust 1.97+](https://img.shields.io/badge/rust-1.97%2B-orange.svg)](rust-toolchain.toml)

An agent that searches Drive, Slack, Jira or GitHub on a user's behalf must
not show that user a document they could not open themselves, yet a search
index built with one service account sees everything. fyrr syncs each
source's access rights along with its content and checks the caller's
rights inside the search itself, before anything is ranked or returned.
The index lives in an S3 bucket or a private local directory, with no
database or cluster to operate.

<picture>
  <source media="(prefers-color-scheme: light)" srcset="docs/assets/architecture-light.svg">
  <img src="docs/assets/architecture.svg" alt="Sources sync through the writer into S3 or local disk; the reader checks permissions before returning MCP results.">
</picture>

## Highlights

- **Storage is a bucket or a directory.** No database, queue or search cluster: the
  index, permissions and writer coordination live in S3 or on local disk.
  Writers run as scheduled commands or one `fyrr writer serve`; readers
  read publications and keep local caches ([topology](docs/deployment.md#topology)).
- **Permission filter before ranking.** Candidates come only from rows the
  caller may read, so a forbidden document is never scored or returned and
  looks exactly like one that does not exist ([how](docs/architecture.md#authorization)).
- **Fails closed.** Missing authorization data, an unknown caller or a source
  whose permission reads went stale refuses or hides; it never guesses
  ([security](docs/security.md)).


- **MCP over stdio or HTTP.** `search`, `fetch` and `list_filters` for one
  user per stdio process, or for many over Streamable HTTP with OIDC access
  tokens ([MCP tools](docs/mcp-tools.md)).
- **Connectors and an SDK.** Local files, Google Drive, Notion, GitHub, Slack
  and Jira, plus a Python SDK for your own ([connectors](docs/connectors/README.md)).


## Quick start

Follow the [quickstart](docs/quickstart.md) to search a Markdown folder from
Claude Code using a private local directory. It needs Linux x86-64 and the
tools git, curl, unzip, openssl, Python 3.11+, rustup, uv, a C compiler, jq,
binutils, network access and about 10 GiB free; no accounts
or embedding keys are needed. The guide also covers adding Slack and trying
Ada/Grace permissions. For shared deployments, see
[deployment](docs/deployment.md).

After setup in the quickstart:

```sh
fyrr writer namespace create
fyrr writer sync
fyrr writer commit
claude mcp add fyrr --scope user -e FYRR_IDENTITY_KEY="$FYRR_IDENTITY_KEY" -- "$PWD/target/debug/fyrr" --config "$FYRR_CONFIG" reader serve
```

## Limits

- **You need a stable release.** fyrr is pre-1.0: storage formats and the
  MCP schema may break before 1.0; data is then republished, not migrated.
  Releases (binaries, images, the SDK wheel) will be published from `v*` tags.
- **You need proven cloud connectors.** Google Drive, Notion, GitHub, Slack
  and Jira have been tested only against synthetic APIs, not real workspaces.
- **Your documents are not text.** There is no PDF, Office or image
  extraction.
- **You cannot send text to an embeddings service.** You provide an
  OpenAI-compatible `/embeddings` endpoint, and document and query text is
  sent to it.
- **You build on macOS or Windows.** Linux x86-64 is the only supported
  build host.
- **You need phrase queries.** The full-text index stores no term positions.
- **You want TLS built in.** The HTTP listener speaks plain HTTP; outside
  loopback it needs a TLS-terminating proxy.

## Documentation

| Page | Content |
| --- | --- |
| [Quickstart](docs/quickstart.md) | Build, configure and search a local folder |
| [How fyrr works](docs/architecture.md) | Terms, publication, authorization and retrieval |
| [Security model](docs/security.md), [HTTP login and users](docs/http-users.md) | Trust boundaries, permissions, secrets; enrolling HTTP users |
| [Deployment](docs/deployment.md), [Operations](docs/operations.md) | Production setup, images, scheduling; monitoring, restore, upgrades |
| [Configuration](docs/configuration.md), [identity](docs/identity.md), [tuning](docs/tuning.md), [reader and writer](docs/reader-writer.md) | Configuration reference |
| [CLI](docs/cli.md), [MCP tools](docs/mcp-tools.md) | Command and tool references |
| [Connectors](docs/connectors/README.md) | Setup and limits per source; [writing a provider](docs/connectors/authoring.md) |

## Contributing

Bug reports, connector requests and pull requests are welcome; for larger
changes, open an [issue](https://github.com/fyrr-ai/fyrr/issues/new/choose)
first. [CONTRIBUTING.md](CONTRIBUTING.md) covers the build, `scripts/ci` and
the contract rules. Report vulnerabilities privately as described in
[SECURITY.md](SECURITY.md), and follow the
[code of conduct](CODE_OF_CONDUCT.md). Notable changes are listed in the
[changelog](CHANGELOG.md).

## License

fyrr is licensed under the [Apache License 2.0](LICENSE); see [NOTICE](NOTICE).
