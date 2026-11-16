# Connectors

> **In short**
>
> - Six bundled providers: local files, Google Drive, Notion, GitHub, Slack
>   and Jira; the table links each one's page.
> - The rules every source shares, such as `sync` and `fixed` permissions,
>   reconcile listings and [quarantine](../architecture.md#terms).
> - For a source of your own, see [writing a provider](authoring.md).

A connector [provider](../architecture.md#terms) is a Python package that fyrr's full build discovers
through the `fyrr.connectors` entry point group and runs in isolated worker
processes. The bundled providers ship in the `fyrr-connectors` package and
in the `fyrr` image; each cloud provider needs its optional extra
(`fyrr-connectors[google-drive]`, `[notion]`, `[github]`, `[slack]`,
`[jira]`), which the image installs.

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
