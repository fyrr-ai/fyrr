"""Writer-style publication/pending revisions and batch acknowledgments."""

from dataclasses import dataclass, field

from fyrr_connectors.arrow import rows_to_json
from fyrr_connectors.connector import Limits, SourceContext
from fyrr_connectors.slack import Slack
from fyrr_connectors.slack.config import Scope, Settings


@dataclass
class Index:
    world: object
    mode: str = "fixed"
    readers: tuple = ("grp:staff",)
    settings: dict = field(
        default_factory=lambda: {"token": {"env": "FYRR_TEST_SLACK_TOKEN"}, "workspace_id": "T001"}
    )
    scope: dict = field(default_factory=lambda: {"include": ["*"]})
    limits: Limits = field(
        default_factory=lambda: Limits(max_rows=100, max_bytes=1 << 20, document_max_bytes=1 << 16)
    )
    cursor: str | None = None
    revisions: dict = field(default_factory=dict)
    published: dict = field(default_factory=dict)

    def connector(self):
        pages = iter([list(self.revisions.items())])
        return Slack(
            SourceContext(
                source_id="slack",
                permissions=self.mode,
                readers=self.readers,
                settings=Settings.parse(self.settings),
                scope=Scope.parse(self.scope),
                limits=self.limits,
                next_revisions=lambda: next(pages, []),
                lookup_revisions=lambda roots: {
                    key: self.revisions[key] for key in roots if key in self.revisions
                },
            )
        )

    def run(self, kind="content", cursor=None):
        return self.connector().run(kind, cursor)

    def apply(self, batch):
        changes = rows_to_json(batch.changes)
        for change in changes:
            key = change["root_id"]
            if change["op"] == "delete":
                self.revisions.pop(key, None)
                self.published.pop(key, None)
            elif not any("\0" in section["text"] for section in change["sections"]):
                self.revisions[key] = change["source_revision"]
                self.published[key] = change
        self.cursor = batch.cursor
        return changes

    def sync(self):
        result = {}
        for batch in self.run(cursor=self.cursor):
            result.update((item["root_id"], item) for item in self.apply(batch))
        return result

    def listing(self):
        parts = list(self.run("reconcile"))
        assert parts[-1].complete
        return {key for part in parts for key in part.root_ids}
