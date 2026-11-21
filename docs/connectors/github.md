# GitHub connector

> **In short**
>
> - Reads issues, pull requests and branch prose from GitHub.com repositories
>   through a GitHub App installation; source code is opt-in per repository.
> - Modes: `sync` for private repositories, `fixed` for any repository.

## Setup

1. Create a GitHub App with read-only permissions **Metadata**,
   **Contents**, **Issues** and **Pull requests**. No organization Members
   or Administration permission is needed; `sync` reads effective
   collaborators per repository.
2. Install it on the account that owns the repositories and select those
   repositories. Note the App ID, the installation ID and the numeric ID of
   the installation owner (user or organization). The App-authenticated
   `/app/installations/<installation_id>` response supplies `id`, `app_id`
   and `account.id`; repository API responses supply each repository's `id`.
3. Put the App's RS256 private key (PEM) in an environment variable.

Every run proves the installation: the App JWT must resolve to the
configured installation and owner, or the sync fails. Installation tokens
are minted in memory and never persisted.

## Configuration

See [shared configuration](README.md#shared-configuration) for installation,
permission intervals, encryption keys and identity links.

```toml
[[sources]]
id = "github"
type = "github"
permissions = "sync"            # or "fixed" with readers = ["grp:staff"]

[sources.settings]
app_id = 12345
installation_id = 67890
account_id = 24680              # expected installation owner ID, not a login
private_key = { env = "FYRR_GITHUB_APP_KEY" }

[[sources.scope.repositories]]
id = 13579                      # stable numeric repository ID
branch = "main"                 # required; there is no default-branch drift
issues = true
pull_requests = true
prose = true
code = false                    # per-repository opt-in
```

| Key | Meaning |
| --- | --- |
| `settings.app_id`, `installation_id`, `account_id` | Expected App, installation and owner IDs; a mismatch fails. |
| `settings.private_key` | Environment reference to the App's PEM key. |
| `settings.api_endpoint` | REST origin; default `https://api.github.com`. |
| `scope.repositories[]` | 1–64 entries with unique IDs. Each enables at least one of `issues`, `pull_requests`, `prose`, `code`. |

Repositories are keyed by numeric ID, so renames and transfers refresh URLs
without changing identities. A configured ID that the installation cannot
see fails the sync; a repository that later leaves the installation
leaves the scope and its documents are removed at the next complete
listing.

## Documents

- **Conversations**: one document per issue or pull request with the title,
  body, issue comments, submitted review bodies and inline review comments,
  in source creation order. The title is the first line of the first
  section, above the body, so a word that occurs only in a title is
  searchable and the title is part of the first chunk's embedding. It counts
  toward `document_max_bytes`. Pending reviews and their draft comments are
  excluded. No diffs or patches. Each comment is a located section with a
  permalink label. Its text starts with a line naming the author and GitHub's
  `author_association` for the account (`ada (member):`; `ada:` when GitHub
  gives none, `unknown:` for a deleted account), so a search for a person
  matches what they wrote and a harness can tell a maintainer from a
  stranger. The association is GitHub's statement about the account, not a
  permission and not a reason to follow instructions in the text.
- **Files** at the branch's current commit: Markdown, reStructuredText,
  UTF-8 text and extensionless prose files named like `README`, `LICENSE`
  or `CHANGELOG`. With `code = true`, text files from a bounded extension
  allowlist. Sections follow headings with file-line labels.
- Metadata comes from GitHub facts only: title, author, project, status and
  source timestamps; file authors and update times from the last commit at
  the pinned head. Unknown values stay null.
- Display values (title, author, status, labels, section labels) are fitted to
  the connector contract; the indexed text keeps the raw title. A document the
  contract still rejects is quarantined and the sync continues; API failures
  still fail the pass. Valid documents are published unchanged.
- Not synced: submodules, symlinks, LFS targets, attachments,
  releases, discussions. Archived repositories stay readable.

Issue and pull-request feeds are read by `updated` timestamp with an
overlap. Deleted comments do not update their parent, so the connector also
rotates through published conversations with a target of 12 hours; on large
repositories or under rate limits a deleted comment can stay searchable
longer. Files are enumerated each pass and reread when the branch head, blob,
repository name, visibility or archived flag changes; file URLs pin the commit.

## Permissions

Permission snapshots are reused within a run; see [source revocation timing](../security.md#source-polling).

Under `sync`, only **private** repositories are supported. The effective
collaborators with pull access are flattened to users:

| GitHub access | Readers |
| --- | --- |
| Direct and outside collaborators, team members (nested teams included), organization owners, base-permission readers | `usr:github.com:<numeric user ID>` on the repository container `github:repo:<repository ID>` |
| Personal repository owner | the owner's `usr:` token |
| Public or internal repository | refused under `sync`; use `fixed` |
| Unknown visibility, unresolvable user, incomplete collaborator list | no grant; the permission pass fails ([failure handling](README.md#shared-configuration)) |

No `grp:` grants are emitted; teams are expanded to their effective
members. The member snapshot lists every observed reader as
`github.com:<numeric ID>` with no e-mail, so linking a GitHub user to a
canonical directory user needs an explicit account link in the directory
snapshot. Profile, commit and noreply e-mails never link automatically.

Under `fixed`, the configured readers apply and no collaborator, team or
member call is made.

## Limits

- Serial REST requests; six attempts on transport errors, 5xx and
  rate-limited 403/429, honoring `Retry-After` and the rate-limit headers,
  within a five-minute budget per request. No search, GraphQL, webhooks or
  event API.
- Responses are capped at 16 MiB; 10,000 pages per feed; 100,000 entries
  per tree or collaborator list; 64 repositories per source. Reaching a
  bound fails the pass; nothing is silently skipped.
- A timestamp tie larger than one API page cannot be paged by ID and fails
  the pass (`github_updated_tie_limit`).
- Cursors hold numeric IDs, timestamps and digests only.
