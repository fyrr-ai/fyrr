# Jira Cloud connector

> **In short**
>
> - Reads issues and comments from Jira Cloud with a scoped service-account
>   API token.
> - Modes: `sync` for company-managed software and business projects, `fixed`
>   for any project.

## Setup

1. Create a dedicated service account and a scoped API token for it. Put
   the token in an environment variable; the client sends it as Bearer
   authentication. Use the numeric `id` from project API responses for
   `scope.projects`. Requests go through the Cloud
   gateway `https://api.atlassian.com/ex/jira/<cloud-id>`.
2. Find the site's cloud ID (`https://<site>.atlassian.net/_edge/tenant_info`).
   The connector verifies the site URL and cloud ID every run and sends
   credentials only to that gateway.
3. Rights for `sync`: the account needs **Administer Jira** (security-level
   members and application roles), **Browse users and groups**, the
   project rights to read permission schemes and roles, and the API scopes
   for issues, comments, users, groups, projects, roles, permission schemes
   and security schemes. Administrator rights do not make content visible:
   the account must also hold every Browse permission, security level and
   comment-visibility group of the content to sync.
4. Rights for `fixed`: Browse Projects plus the security levels and comment
   visibilities of the content to read.

## Configuration

See [shared configuration](README.md#shared-configuration) for installation,
permission intervals, encryption keys and identity links.

```toml
[[sources]]
id = "jira-eng"
type = "jira"
permissions = "sync"                   # or "fixed" with readers = [...]

[sources.settings]
site_url = "https://acme.atlassian.net"
cloud_id = "11111111-1111-4111-8111-111111111111"
api_token = { env = "FYRR_JIRA_API_TOKEN" }

[sources.scope]
projects = [10001, 10002]              # stable numeric project IDs ...
# jql = "project = 10001 AND status != Closed"   # ... or one bounded static JQL expression
issue_types = []                       # issue type IDs; empty = all
labels = []                            # any-of exact labels
skip_comment_authors = []              # account IDs whose comments are omitted
```

| Key | Meaning |
| --- | --- |
| `settings.site_url`, `cloud_id` | Pinned HTTPS site and its Cloud UUID; both verified. |
| `settings.api_token` | Environment reference to the scoped token. |
| `settings.api_endpoint` | Optional HTTPS or loopback origin instead of the cloud gateway. |
| `scope.projects` or `scope.jql` | Exactly one. JQL is a bounded static expression: no `ORDER BY`, saved filters, `currentUser()`, `membersOf()` or relative-time functions. |
| `scope.issue_types`, `labels`, `skip_comment_authors` | Optional narrowing (at most 256, 64 and 256 entries). |

Issues are identified by numeric ID (`jira:<cloud-id>:issue:<id>`), so
key changes, project renames and moves keep their identity.

## Documents

- One document per issue: summary, description and the selected standard
  fields (status, type, priority, labels, reporter, assignee) as
  located sections (the summary section starts with the issue key, as
  `PROJ-123 Summary text`, so excerpts show it and a plain query for the key
  can match its tokens), then **one section per comment** with that comment's
  own audience. Long text is split into units of at most 16 KiB.
- The issue key is the document attribute `jira.key`, exactly as Jira
  returns it. `search` with `attributes: [{"key": "jira.key", "values":
  ["PROJ-123"]}]` returns that issue if the caller may read it, also without
  a `query` (then `order` must be `updated_at`). Add `sources` when two Jira
  sites are indexed: keys are unique per site only. A moved
  issue keeps its document and gets its new key when it is next read.
- Rich text (Atlassian Document Format) is rendered to plain text; mentions
  keep readable labels. A link renders as `text (url)` and a smart-link card
  (a pasted pull request or design document) as its URL, for `http` and
  `https` only. Media is dropped; attachments and linked issues are not
  followed.
- A `links` section lists the issue's parent and issue links, one line each
  (`parent: PROJ-100`, `blocks: PROJ-140`, `is blocked by: PROJ-88`), at most
  64. It carries keys only, no summaries: the section has the issue's
  audience, and a reader of this issue may not be allowed to see the linked
  one. Look a key up with `jira.key`.
- Title, author, project, status and timestamps come from issue fields,
  never from hidden comments. Unknown dates stay null.
- Issues are discovered through the enhanced search API ordered by ID
  with a two-minute `updated` overlap and a full rescan every 12 hours; comment edits and deletions that do not touch
  the parent are caught by a rotation over published issues targeting 12
  hours. A project that leaves the scope, is archived or deleted loses its
  grants and its documents at the next complete listing.
- Not synced: attachments, worklogs, team-managed project permissions and
  Jira Service Management portal rules under `sync` (those projects are
  refused there; `fixed` reads their visible fields and omits service-desk
  comments).

## Permissions

Permission snapshots are reused within a run; see [source revocation timing](../security.md#source-polling).

Under `sync` the audience of an issue is the intersection of three layers:
the project's static Browse holders, the issue's security level and each
comment's visibility restriction. Jira users, groups, project roles,
project leads, application access and "any logged-in user" holders are
resolved to concrete active human accounts; reporter, assignee and
user-picker holders add per-issue users.

| Jira case | Result |
| --- | --- |
| Unsecured issue in a static project | project container `jira:<cloud-id>:project:<id>` plus policy groups |
| Secured or user-dependent issue | the issue's own readers; no container |
| Comment with group or role visibility | a separate record readable by the matching policy group only |
| Comment readable only through a reporter, assignee or picker field | deliberately omitted (under-shared) |
| Public or anonymous Browse | refused under `sync`; use `fixed` |
| Unknown holder type or unknown comment visibility | grants nobody (under-shared) |
| Malformed scheme, incomplete member page | the permission pass fails ([failure handling](README.md#shared-configuration)) |

User tokens are `usr:atlassian.net:<cloud-id>:<accountId>`; groups are
synthetic policy groups per project, level and visibility target. There is
no administrator, creator or author bypass. See [corporate identity links](README.md#shared-configuration) for member
e-mail handling.

Under `fixed` the configured readers apply; restricted and
unknown-visibility comments are omitted, and no scheme, role, group or
directory call is made.

## Limits

- Each search is limited to 100,000 issues. Keep each source's configured
  project or JQL scope within that bound; split larger scopes into separate
  sources. A source above the limit cannot complete reconciliation, so its
  listing never authorizes deletions.
- Serial requests honoring 429 `Retry-After` and the rate-limit headers;
  six attempts with bounded backoff per request. No writes, webhooks or
  audit-log dependency.
- Bounded JQL (8 KiB), IDs, lists, document depth and response sizes.
  Offset-paged catalogs (comments, users, members) are read as one checked
  snapshot and reread once on movement; instability fails the pass.
- Policy groups per project are bounded by levels and visibility targets in
  use; a subject exceeding `identity.max_tokens_per_user` is refused by the
  reader, not truncated.
- Cursors hold timestamps, numeric IDs and digests only.
