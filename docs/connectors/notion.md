# Notion connector

> **In short**
>
> - Reads the pages and database rows beneath configured root pages through an
>   internal Notion integration.
> - Always a `fixed` connection: the configured readers apply to every
>   document.
> - Read it when you add a `type = "notion"` source: setup, configuration,
>   documents and limits.

Reads the pages and database rows beneath configured root pages through an
internal Notion integration. Always a `fixed` connection: Notion exposes no
usable permission model to an integration, so the configured readers apply
to every document and no permission or member read exists.

## Setup

1. Create an internal integration in the workspace with read content
   capabilities. The user-information capability is optional; without it
   authors are null.
2. Share each root page with the integration; sharing a page shares its
   subtree.
3. Put the integration secret in an environment variable.
4. Install the `notion` extra (`fyrr-connectors[notion]`: `requests`); the
   `fyrr` image has it.

## Configuration

```toml
[[sources]]
id = "notion"
type = "notion"
permissions = "fixed"                    # the only mode
readers = ["grp:staff"]

[sources.settings]
token = { env = "FYRR_NOTION_TOKEN" }
# api_endpoint = "https://api.notion.com"   # change only for a proxy or test server

[sources.scope]
root_pages = ["11111111-2222-3333-4444-555555555555"]   # page IDs, dashes optional
```

## Documents

- A document is a page under a root (a sub-page at any block depth, a row
  of a database under a root, and the pages beneath those). `root_id` is
  the dashed page ID; `source_type` is `notion-page` or `notion-row`;
  `project` is the root page's title; `connector_map` holds the page, root
  and database IDs.
- Blocks render in document order with one section per heading. Rich text
  keeps light Markdown marks for lists, to-dos, quotes, code, equations and
  table rows; bookmarks and embeds give their URL; media and file blocks
  only their caption. A database row starts with a `properties` section
  (`Name: value` per non-empty typed property; relations are skipped,
  values are cut at 25 entries by the API). A page without text is not
  published.
- Each run first reads every root: a root that answers 404 (not shared,
  deleted, wrong workspace) fails the run, so a wrong token never looks
  like an empty scope.
- Incremental syncs use Notion's search as a change hint with a one-hour
  overlap for index lag, reread each candidate by ID, and walk the subtree
  beneath a changed page in full. Notion stamps edits to the minute, so a
  page read within two minutes of its stamp is reread on the next run.
- Trashed pages are deleted when seen; pages that vanished, lost their
  sharing or left the roots are removed at the next complete listing,
  which is the full walk without text.

## Limits

- At most about three requests per second; 429, 409, 5xx and transport
  errors back off with jitter and `Retry-After` for six attempts, then the
  run fails with the cursor unmoved. 401 and 403 fail the run.
- Responses stop at 16 MiB, lists at 10,000 entries, each tree run at
  100,000 visited entries, parent walks at 100 levels; reaching a bound
  fails the pass and never marks a listing complete.
- The full listing reads every block list, so a large workspace needs a
  matching `--timeout` for `fyrr writer sync --kind reconcile`.
- A page moved between branches during a walk can be missing from that
  listing and reappear on the next sync. Pages that Notion's search never
  reports arrive with `fyrr writer sync --full`.
