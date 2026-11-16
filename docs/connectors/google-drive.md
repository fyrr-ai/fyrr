# Google Drive connector

> **In short**
>
> - Exports Google Docs, Sheets and Slides from My Drive and shared drives
>   through a service account with domain-wide delegation.
> - Modes: `sync` (Drive permissions and the Directory become readers) and
>   `fixed`.

## Setup

1. Create a service account and enable domain-wide delegation for it.
2. Authorize the OAuth scope `https://www.googleapis.com/auth/drive.readonly`;
   for `sync` also `admin.directory.user.readonly` and
   `admin.directory.group.readonly`, and name an administrator in
   `settings.admin_email` whom the sync impersonates for the Directory.
3. Put the service-account key JSON in an environment variable.

## Configuration

See [shared configuration](README.md#shared-configuration) for installation,
permission intervals, encryption keys and identity links.

```toml
[[sources]]
id = "drive"
type = "google-drive"
permissions = "sync"                     # or "fixed" with readers = [...]

[sources.settings]
service_account = { env = "FYRR_GDRIVE_SERVICE_ACCOUNT" }  # key JSON
admin_email = "admin@example.com"        # required for sync, unused for fixed

[sources.scope]
my_drive = true                          # files owned by the users below
shared_drives = ["*"]                    # "*" = every drive those users can open, or drive IDs
shared_with_me = false                   # files the users can open but do not own; fixed only
owners = ["a@example.com"]               # user e-mails; empty = all active users (sync only)
folders = []                             # folder IDs; empty = no folder restriction
extensions = ["gdoc", "gsheet", "gslides"]
```

Under `fixed`, `scope.owners` must list the users whose Drive is read.
`shared_with_me = true` with a `sync` connection is refused, because another
owner's permission list is not reliably readable. Widening the scope of a
source that already synced needs `fyrr writer sync --full`.

## Documents

- One document per Google-native file in scope: [`root_id`](../architecture.md#terms) is the file ID,
  `source_type` is `google-doc`, `google-sheet` or `google-slides`, the
  author is the owner (else the last modifier) and `project` the shared
  drive's name.
- Docs are exported as Markdown and split at headings; Sheets as xlsx with
  one section per sheet in row groups of at most 16 KiB that repeat the
  header row; Slides as one section per slide (speaker notes are not
  exported). Embedded images are dropped. Other file types and shortcuts
  are ignored; a document without text is not published.
- Changes come from the Drive changes feed, one feed per user and per
  shared drive; the start token is taken before the first full listing so
  nothing is lost. A sharing-only change does not re-export the file.
- Trashed files are deleted when seen. Hard deletes, files that left the
  scope and dropped feeds are removed at the next complete listing, which
  re-lists every feed and re-checks files that moved between feeds while
  it ran.
- Exports over Google's 10 MB limit, non-rate-limit 403 refusals, unreadable
  exports and text over `document_max_bytes` are quarantined. A 404 is left
  to the listing; other errors fail the sync.

## Permissions

Under `sync`, each in-scope file's complete permission list is read
(inline when the API proves it complete, else fully paged) and mapped:

| Drive permission | Readers |
| --- | --- |
| `user` with an e-mail, any role | `usr:<email>`; an address outside the domain links to nobody |
| `group` | `grp:<email>`, members from the Directory snapshot |
| `domain` with file discovery allowed | `grp:domain:<domain>` |
| `domain` link-only; `anyone` | nobody |
| deleted, expiring or view-restricted permissions | nobody |
| shared-drive membership | the drive as a container grant, only when the API shows every member's access inherited from the drive and no limited-access folder sits above the file; otherwise membership grants are dropped (over-restricted) |
| unreadable or partial permission list | the permission pass fails ([failure handling](README.md#shared-configuration)) |

The member snapshot comes from the Directory: users by lower-cased primary
e-mail, marked verified corporate (so they link to the canonical directory
automatically), inactive when suspended or archived; groups with direct
user and group members; and one synthetic `domain:<domain>` group per
domain. Feeds are read for active users only.

Under `fixed`, no Directory or permission call is made and the users are
exactly `scope.owners`.

## Limits

- JSON success and error bodies are streamed with a 16 MiB limit. Each
  paginated list or changes read allows at most 10,000 pages as a backstop
  against a nonterminating upstream read. File listings request 100 files
  per page, allowing up to 1,000,000 files per feed when pages are full.
  Repeated or malformed page tokens fail the read. These limits refuse an
  incomplete read rather than reporting it as complete.
- 429, rate-limit 403s, 5xx and transport errors back off with jitter and
  `Retry-After` for six tries, then the sync fails; a file that keeps
  failing stalls its feed rather than being skipped. Other 403 and 404
  responses are authoritative.
- Sheets are read up to 20 million cells per workbook. Export runs on a
  pool of eight requests per page.
- The cursor is capped at 16 KiB; split sources with disjoint `owners`
  if their feed state exceeds that bound.
- A suspended user's My Drive leaves the scope at the next listing; an
  active user whose Drive cannot be read fails the source.
