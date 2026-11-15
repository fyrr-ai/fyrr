"""Worker-style revision view and acknowledgments for the stateful fake."""

from dataclasses import dataclass, field

from fyrr_connectors.arrow import rows_to_json
from fyrr_connectors.connector import Limits, SourceContext
from fyrr_connectors.github import GitHub
from fyrr_connectors.github.config import Scope, Settings


@dataclass
class Index:
    world: object
    mode: str = "fixed"
    readers: tuple[str, ...] = ("grp:staff", "usr:lead")
    scope: dict = field(default_factory=lambda: {"repositories": [{"id": 1, "branch": "main"}]})
    settings: dict = field(
        default_factory=lambda: {
            "app_id": 10,
            "installation_id": 20,
            "account_id": 30,
            "private_key": {"env": "FYRR_TEST_GITHUB_KEY"},
        }
    )
    revisions: dict[str, str] = field(default_factory=dict)
    published: dict[str, dict] = field(default_factory=dict)
    cursor: str | None = None
    limits: Limits = field(
        default_factory=lambda: Limits(max_rows=100, max_bytes=1 << 20, document_max_bytes=1 << 16)
    )

    def connector(self):
        pages = iter([list(self.revisions.items())])
        return GitHub(
            SourceContext(
                source_id="github",
                permissions=self.mode,
                readers=self.readers,
                settings=Settings.parse(self.settings),
                scope=Scope.parse(self.scope),
                limits=self.limits,
                lookup_revisions=lambda roots: {
                    root: self.revisions[root] for root in roots if root in self.revisions
                },
                next_revisions=lambda: next(pages, []),
            )
        )

    def run(self, kind="content", cursor=None):
        return self.connector().run(kind, cursor)

    def apply(self, batch):
        changes = rows_to_json(batch.changes)
        for change in changes:
            root = change["root_id"]
            if change["op"] == "delete":
                self.revisions.pop(root, None)
                self.published.pop(root, None)
            elif not any("\0" in section["text"] for section in change["sections"]):
                self.revisions[root] = change["source_revision"]
                self.published[root] = change
        self.cursor = batch.cursor
        return changes

    def sync(self):
        changes = {}
        for batch in self.run(cursor=self.cursor):
            changes.update((change["root_id"], change) for change in self.apply(batch))
        return changes

    def listing(self):
        parts = list(self.run("reconcile"))
        assert parts[-1].complete
        return {root for part in parts for root in part.root_ids}
