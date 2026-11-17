# Local files connector

> **In short**
>
> - Reads UTF-8 text files from directories on the writer host.
> - Always a `fixed` connection; every listed reader token reads every file.

## Configuration

```toml
[[sources]]
id = "handbook"
type = "local-files"
permissions = "fixed"
readers = ["grp:engineering"]       # one or more reader tokens

[sources.scope]
paths = ["/srv/docs"]               # one or more directories
include = ["**/*.md", "**/*.txt"]   # relative glob patterns; default ["**/*"]
```

There are no `settings`. Patterns use Python `Path.match`: they match from
the end of the relative path; a leading `**/` is also tried without that
prefix. Do not assume arbitrary recursive `**` glob semantics.

## Documents

- Every regular file beneath a path that matches an `include` pattern is
  one document. With one path the [`root_id`](../architecture.md#terms) is the relative path; with
  several it is `<path index>/<relative path>`. Symlinks are skipped, and
  directories are walked without following links.
- A file that is not valid UTF-8 or exceeds `document_max_bytes` is
  quarantined; the last good revision stays visible.
- Extraction replaces NUL and other C0 controls with spaces, preserving tabs,
  LF and CR. This keeps surrounding words separate. Invalid UTF-8 and oversized
  files still follow the quarantine path. Content digests and embedding inputs
  use the extracted text; source revisions continue to use the original bytes.
- Revisions hash the file bytes and the configured readers; an unchanged
  file is not republished. A content sync deletes previously indexed files
  that no longer exist, and the complete listing at `intervals.reconcile`
  catches any it missed.
- An unreadable directory fails the sync pass with `source_unavailable`.

## Limits

- Text only: no PDF, Office or binary extraction.
- No per-file permissions: every reader token holder reads every file.
- The writer process must be able to read the directories; in the `fyrr`
  image mount them read-only.
