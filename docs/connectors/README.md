# Connectors

> **In short**
>
> - Six bundled providers: local files, Google Drive, Notion, GitHub, Slack
>   and Jira; the table links each one's page.
> - The rules every source shares, such as `sync` and `fixed` permissions,
>   reconcile listings and [quarantine](../architecture.md#terms).
> - For a source of your own, see [writing a provider](authoring.md).

A connector [provider](../architecture.md#terms) is a Python package. The
`fyrr` binary contains no connectors and no Python: the commands that run
connectors start a Python provider host in the environment `VIRTUAL_ENV`
names (or the `python3` next to `fyrr` or on `PATH`; see
[downloadable binaries](../deployment.md#downloadable-binaries)), which
discovers providers through the `fyrr.connectors` entry point group. Each
isolated worker process starts its own host and runs the providers in it.
The bundled providers ship in the `fyrr-connectors` package (a wheel that
each release publishes; the package index has only `fyrr-connectors` 0.1.0
and the 0.1.1 wheel was built for the embedded interpreter too, so neither
works with this fyrr: install 0.1.2 or newer, the wheel of this release) and in the `fyrr` image; each cloud provider needs its optional extra (`google-drive`,
`notion`, `github`, `slack`, `jira`), which the image installs. Without the
package, the commands that need providers stop with a message that says to
install the release's wheel, with the extras you need, into a virtual
environment and set `VIRTUAL_ENV`.

| Source `type` | Reads | Permission modes | Page |
| --- | --- | --- | --- |
| `local-files` | UTF-8 text files under configured directories | `fixed` | [local-files](local-files.md) |
| `google-drive` | Google Docs, Sheets and Slides exported to text | `sync`, `fixed` | [google-drive](google-drive.md) |
| `notion` | pages and database rows under root pages | `fixed` | [notion](notion.md) |
| `github` | issues, pull requests, branch prose; code opt-in | `sync` (private repositories), `fixed` | [github](github.md) |
| `slack` | channel conversation windows and threads | `sync`, `fixed` | [slack](slack.md) |
| `jira` | Jira Cloud issues with per-comment audiences | `sync`, `fixed` | [jira](jira.md) |

**Maturity.** `local-files` runs against real directories. The Google Drive,
Notion, GitHub, Slack and Jira providers have been exercised only against
synthetic APIs in the SDK's tests and through the real writer on an S3
fixture in CI, not yet against real workspaces.

Every provider page covers setup (credentials and API scopes), the
`settings` and `scope` keys, what becomes a document, how permissions map
to reader tokens (user and group grants), and known limits.

## Python versions

The SDK and the bundled providers need Python 3.11 or newer. The connector
test suite and `mypy` were run by hand under CPython 3.11, 3.12, 3.13 and 3.14
for this release; CI runs the default, 3.13. The `fyrr`
binary does not link Python, so the interpreter only has to be one the
SDK supports; it need not match the one `fyrr` was built with.

## Shared configuration

In a checkout, install a cloud provider's extra with
`(cd connectors && uv sync --locked --extra slack)`, replacing `slack` with
its source type. Rerun this after `scripts/doctor`, which syncs the
environment without extras. The full image includes all bundled extras.
A source's `settings.api_endpoint` can select a trusted proxy or test server;
leave it at its default for the upstream API.
Slack and Jira `settings.corporate_domains` default to
`identity.directory.verified_domains`; only corporate e-mails in these domains
link automatically to canonical users. Set `[]` to require explicit links.
See [identity configuration](../identity.md).

For `permissions = "sync"`, set `identity.encryption_key` and intervals:

```toml
[sources.intervals]
permissions = "15m"
members = "15m"
```

A failed or partial permission/member read keeps the last published rights
until the [freshness deadline](../architecture.md#terms), then hides that
source. See [freshness and revocation](../security.md#freshness-and-revocation).

Common rules for every source:

- `permissions = "sync"` publishes the source's own access rights and needs
  `intervals.permissions`; `intervals.members` is optional and, when set,
  its reads count toward freshness too. `permissions = "fixed"` grants the
  configured `readers` to every document and makes no permission or member
  reads.
- Every source runs a full-ID listing on `intervals.reconcile` (1 day);
  only a listing the provider marks complete may delete documents.
- Documents over `document_max_bytes`, invalid text or a malformed
  representation are quarantined: the last good revision stays visible and
  `writer status` counts the quarantine.
- A provider failure, a partial permission read or a transport error fails
  that sync pass; it never produces an empty permission set.
- Settings secrets are `{ env = "VAR" }` references, validated by the
  provider's schema (`fyrr config check`, `fyrr config schema`).
- `fyrr writer sync --dry-run` runs the provider and validates its output
  without writing anything.

[Writing a provider](authoring.md) describes the SDK contract for your own
sources.
