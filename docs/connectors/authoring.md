# Writing a connector provider

> **In short**
>
> - A [provider](../architecture.md#terms) is an ordinary Python distribution that depends on
>   `fyrr-connectors` and registers an entry point in the group
>   `fyrr.connectors`.
> - This page covers the package layout, the connector class, sync kinds,
>   documents, permissions, cursors, limits, testing and compatibility.

## Package layout

```toml
# pyproject.toml
[project]
name = "fyrr-provider-markdown"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["fyrr-connectors>=0.1.2,<0.2"]   # the SDK series is the compatibility promise

[project.entry-points."fyrr.connectors"]
markdown-dir = "fyrr_provider_markdown:provider_info"   # the source `type`

[dependency-groups]
dev = ["fyrr-connectors[testkit]", "pytest"]
```

The entry point returns `provider_info(...)`:

```python
from fyrr_connectors import provider_info as _provider_info
from .connector import MarkdownConnector

def provider_info():
    return _provider_info(
        name="markdown-dir",                 # must equal the entry point name
        connector=MarkdownConnector,
        min_sdk_version="0.1.0",             # oldest SDK the provider works with
        connection_types=("fixed",),         # "sync" and/or "fixed"
        description="Markdown files in a local directory",
    )
```

The provider's version is the distribution's version. Its sync kinds and
config schema are derived from the connector class.

## The connector class

```python
from fyrr_connectors import (
    ConfigModel, Connector, ContentBatch, ListingBatch, Location, Metadata,
    Secret, Section, Upsert, document_changes, secret, setting,
)

class Settings(ConfigModel):
    token: Secret = secret(description="API token")           # { env = "VAR" } only
    batch_size: int = setting(description="Documents per batch.", default=100, minimum=1)

class Scope(ConfigModel):
    root: str = setting(description="Directory to read.")

class MarkdownConnector(Connector):
    Settings = Settings
    Scope = Scope

    def content(self, cursor): ...
    def listing(self, cursor): ...
```

`Settings` and `Scope` are frozen dataclasses built from `setting(...)` and
`secret(...)` fields (`str`, `int`, `float`, `bool`, `Secret`,
`Literal[...]`, nested models, lists, optionals). Their JSON Schema is
published in the provider metadata, so `fyrr config check` validates a
source before any worker starts, and `Secret` fields are marked
`x-fyrr-secret` and accept only environment references;
`Secret.reveal()` reads the value. Unknown keys are refused. Use `NoConfig`
for an empty table. A `__post_init__` may raise `ConfigError(path, message)`
for cross-field rules; messages must never contain values.

The connector receives a `SourceContext`: `source_id`, `permissions`
(`"sync"` or `"fixed"`), `readers` (the fixed tokens), parsed `settings` and
`scope`, `limits` (`max_rows`, `max_bytes`, `document_max_bytes`, optional
row thresholds and retention) and two optional callbacks:
`lookup_revisions(root_ids)` returns the published or pending revision of
each root (at most 256 per call) and `next_revisions()` pages every published and pending `(root_id, revision)`, so a resumed sync can
skip what fyrr already has. They answer from the process that runs your
connector, so call them while your `content` iterator is producing a batch
(from any thread), not between batches; a failed call raises
`RuntimeError`.

## Sync kinds

fyrr calls one method per kind with the last durable cursor (`None` for a
first or reset sync). Each yielded batch carries the opaque cursor *after*
it. For `content`, `listing` and `members`, fyrr stores that cursor once the
batch's envelope is durable. For `permissions`, fyrr holds the batches of a
read and stores the cursor after each completed read. Its roots are written
in parts of at most as many roots as a batch may hold
(`writer.envelope_max_rows`, at most 256) and `writer.envelope_max_bytes`,
then an envelope that ends the read, with its cursors and no root; the
committer publishes all of a read in one publication, or none of it, writing
the rows it rewrites in steps within `writer.commit.group_max_rows` and
`group_max_bytes`. A permission read that fails confirms nothing: the parts it
wrote stay unconfirmed, are never published, and are removed by cleanup after
`writer.max_envelope_age`; the next read starts from the cursor before it. A
root that does not fit a part alone under `writer.envelope_max_bytes` fails
the read the same way, as it is seen, with `permission_root_oversized`: raise
the bound, or narrow the root's readers, and the next read publishes. Each part of a read carries the time it was
written, so a read that takes longer than `writer.max_envelope_age` is
rejected (`envelope_expired`) and read again, every time: keep a read shorter. A connector keeps no other
state. The provider metadata lists the kinds the class overrides.

| Method | Yields | Required |
| --- | --- | --- |
| `content(cursor)` | `ContentBatch(changes, cursor)` with an Arrow batch from `document_changes([...])` | yes |
| `listing(cursor)` | `ListingBatch(listing_id, part, last, complete, root_ids, cursor)`: a full-ID listing in parts; `complete=True` only on the last part and only if every document in scope was seen. fyrr derives deletes from a complete listing and never from an incomplete one. | yes for reconcile |
| `permissions(cursor)` | `PermissionBatch(documents, container_grants, cursor)`: `DocumentPermission(root_id, permission_revision, readers, containers)` and `ContainerGrant(container_id, container_kind, permission_revision, readers)` | for `sync` connections |
| `members(cursor)` | `PrincipalBatch(snapshot_id, part, last, users, groups, cursor)`: a full snapshot of `SourceUser(id, active, email, email_verification)` and `SourceGroup(id, members)` | for `sync` connections |

A document never spans batches; keep every batch within `context.limits`.
A failed read must raise, not yield partial results: a partial permission
or membership read leaves the last applied rights in place, which is the
safe outcome; the operator retires a source that stays broken.

## Documents

`Upsert` replaces every record of a [`root_id`](../architecture.md#terms); `Delete` removes them.

```python
Upsert(
    root_id="guides/restore.md",
    source_revision="sha256:…",                   # changes when the content changes
    metadata=Metadata(
        source_uri="file:///docs/guides/restore.md",
        source_type="markdown.file",
        title="Restore", author=None, team=None, project=None, status=None,
        labels=["ops"], created_at=None, updated_at=modified,   # UTC datetimes, or None
        connector_map={"path": "guides/restore.md"},             # free-form, returned by fetch
    ),
    sections=[Section(text="…", location=Location(kind="heading", label="Backups", index=0))],
    attributes=["markdown.directory=guides"],      # key=value labels for search filters
    readers=(),                                 # fixed readers are applied by the writer
    containers=(),
    revision_order=None, expected_prior_revision=None, permission_revision=None,
)
```

- Sections are the document's text in order. Give every section a stable
  `Location` (`heading`, `sheet`, `slide`, `page`, `message`, …) with a
  label or index that survives edits elsewhere in the document; fyrr
  derives record IDs from it, so a hidden section cannot renumber a
  readable one. Several sections with the same location are refused.
- `Section.readers` narrows a restricted part (a comment with its own
  audience). A part must never be wider than its document.
- `Section.vector` is the section's own embedding. It is stored in place of
  one computed from the text, so the section's text must be one chunk (at
  most 16 KiB) and the vector must have the namespace's dimension, finite
  elements and a nonzero length; a document with another vector is
  quarantined as invalid. Sections without one are embedded by the
  `[embedding]` provider; a deployment whose sections all carry vectors needs
  none.
- Unknown metadata stays `None`; never substitute the ingest time.
- `source_revision` must change whenever text, sections or metadata
  change. `revision_order` is optional and lets fyrr order edits that
  arrive out of sequence; omit it when timestamps cannot order them. When
  you set it, a change whose order is not above the published revision's is
  not applied, so a document rendered again without an upstream change
  needs a higher order.
- Under `permissions = "fixed"` the writer applies the configured readers;
  setting `context.readers` on an upsert is optional.
  Under `permissions = "sync"` content carries no readers, containers or
  `permission_revision`: the permission stream is the only source of
  permission state.
- Text over `document_max_bytes` or invalid UTF-8 should be emitted as an
  invalid-document upsert (a single `"\0"` section, as the bundled
  providers do) so fyrr quarantines it, keeps the last good revision and
  continues.

`document_changes([...])` builds the exact Arrow schema of
`schemas/connector-document-changes.arrow.json` and checks it; fyrr checks
it again on the Rust side (types, nullability at every depth, row rules,
cursor chain, row and byte bounds).

## Permissions and identity

Reader tokens are `usr:<source-scoped id>` and `grp:<source-scoped id>`;
fyrr hashes them. Prefer stable numeric or opaque IDs, never display names
or e-mail addresses, and scope them to the tenant
(`usr:github.com:12345`, `grp:slack.com:T123:workspace`).

- A `ContainerGrant` names a shared container (drive, repository, channel,
  project) with its readers; documents reference it in `containers`, so a
  membership change rewrites one grant instead of every document.
- `members()` must be a complete snapshot: every user the grants mention,
  with `active=False` for deactivated accounts, and groups with direct
  members. Nested groups are expanded by fyrr.
- Set `email_verification=EmailVerification.VERIFIED_CORPORATE` only when
  the source itself verified an organization-managed address; that is the
  only evidence that links a source user to a canonical directory user
  automatically. Everything else needs an explicit account link in the
  directory snapshot.
- Public, link-only, anonymous or unknown holders grant nobody.

## Cursors

Cursors are opaque strings of at most 16 KiB. Put only positions in them
(timestamps, IDs, page tokens, digests); never [principals](../architecture.md#terms), text or secrets.
Yield intermediate batches with the previous safe cursor and advance only
when a pass completed; a changed scope must restart from `None`.

## Limits and failures

`context.limits.max_rows` is the minimum of `writer.envelope_max_rows`,
`writer.commit.group_max_rows` and 256. Use the supplied limit for every
content batch, even when the configured envelope limit is larger.

Providers run in a Python host process with a time limit, a memory limit
and no stdin, stdout or stderr. Raise
an exception for any failure; fyrr reports a fixed diagnostic code and
never the exception text. Bound everything you read from the network
(response sizes, page counts, traversal depth) and honor the service's
rate-limit headers with bounded retries. Do not persist anything outside
the cursor.

## Testing

`fyrr-connectors[testkit]` checks a provider in its own tests:

```python
from fyrr_connectors import testkit

info = testkit.check_provider("markdown-dir")               # installed metadata vs. the published schema
result = testkit.run_sync("markdown-dir", "content", scope={"root": "/srv/docs"})
testkit.assert_golden(result, GOLDEN / "content.json")      # FYRR_UPDATE_GOLDEN=1 rewrites it
```

`run_sync` runs one kind the way fyrr does and checks every batch (schema,
nullability, row rules, cursor chain, row bounds); it accepts an installed
name or the metadata dict before installation. Build the wheel with
`uv build --wheel` and install it into a derived image or a mounted wheel
directory as described in [deployment](../deployment.md#providers-in-the-image).

## Compatibility

The SDK follows Semantic Versioning, and its series is the compatibility
promise: the major version decides the series, or the minor for `0.y.z`, so
`0.y.z` is series `0.y` and `x.y.z` with `x > 0` is series `x`. Declare the
oldest SDK version you need in `min_sdk_version`; fyrr refuses a provider
whose minimum is in another series than the installed SDK, or newer than
it, and refuses every provider when the installed `fyrr-connectors` is in
another series than the one this build loads. `fyrr writer providers` names
that series. Pin the dependency to one series
(`fyrr-connectors>=0.1.2,<0.2`). Contract documents carry `major.minor` too:
a newer minor only adds optional fields.

The package index has `fyrr-connectors` 0.1.0, and fyrr 0.1.1 shipped a
0.1.1 wheel; both were built for the embedded interpreter of earlier fyrr
versions, have no provider host and do not work with this fyrr. This fyrr
needs the SDK wheel of its own release (0.1.2 or newer), installed from the
release, or `[tool.uv.sources]` pointing at `connectors/` in a checkout, as
[the example provider](../../examples/markdown-provider/pyproject.toml) does.

## How a provider runs

Providers run in a Python host process that fyrr's worker starts. This is
what a provider author can observe:

- **A fresh process per sync kind.** Each kind of each sync (content,
  permissions, members, reconcile) runs in a new Python process, and
  discovery runs in another. Module-level state, caches and open connections
  do not carry from one kind to the next; keep what must last in the cursor.
  The memory limit `writer.worker_memory_max_bytes` applies to that process
  separately from the worker.
- **A provider that aborts reports "the provider failed".** `os.abort()`,
  `os._exit()`, a crash and running out of memory end the host process, not
  fyrr's worker, and nothing the provider printed or raised is reported.
- **Revision callbacks work only while a batch is being produced.**
  `lookup_revisions` and `next_revisions` answer while the generator runs
  towards its next `yield`; called from another thread that outlives that
  step, or after the generator has finished, they raise
  `RuntimeError("host_callback_failed")`. When the worker refuses a request
  (more than 256 roots, a storage failure), catching the error and carrying
  on never lets the run count as a complete read.
- **Credentials are in `os.environ` before the SDK imports your provider**,
  set by the host from the stored credentials the source references, and
  never in its initial environment (`/proc/<pid>/environ`). Start-up code
  (`.pth` files, `sitecustomize`) runs earlier and sees neither the
  credentials nor a conversation with fyrr, though it can see the protocol
  socket as an open descriptor, which no process it starts inherits.
- **Cleanup has five seconds.** When fyrr stops a run before your generator is
  exhausted (a refused batch, or any failure), it ends the host's conversation
  and the host closes the generator, so `finally` blocks run. Five seconds
  after that fyrr kills the host; when the conversation is out of step it
  kills it at once.
- **The host needs `/proc`.** It reads `/proc/self/fd` to close the
  descriptors it inherited, and without it the host fails to start.
- **`multiprocessing` children started with `spawn` or `forkserver` inherit
  `-S`.** They get the parent's `sys.path`, but they do not run `.pth` hooks
  or `sitecustomize`. Before Python 3.14 they also report the base prefix,
  not the virtual environment's.
- **Rows carry only what the contract allows.** The batch's schema carries
  the contract's two metadata entries and nothing else, and fyrr refuses a
  batch whose schema has metadata on any field. A nullable column you add must
  not use a dictionary type, `string_view` or `binary_view` (the list-view
  types are accepted); fyrr also refuses compression.
