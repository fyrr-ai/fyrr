from dataclasses import dataclass, field

from fyrr_connectors.arrow import rows_to_json
from fyrr_connectors.connector import Limits, SourceContext
from fyrr_connectors.jira import Jira
from fyrr_connectors.jira.config import Scope, Settings


@dataclass
class Index:
    world: object
    mode: str = "fixed"
    readers: tuple = ("grp:staff",)
    settings: dict = field(
        default_factory=lambda: {
            "site_url": "https://synthetic.atlassian.net",
            "cloud_id": "11111111-1111-4111-8111-111111111111",
            "api_token": {"env": "FYRR_TEST_JIRA_TOKEN"},
        }
    )
    scope: dict = field(default_factory=lambda: {"projects": [1]})
    limits: Limits = field(
        default_factory=lambda: Limits(max_rows=100, max_bytes=1 << 20, document_max_bytes=65536)
    )
    cursor: str | None = None
    revisions: dict = field(default_factory=dict)
    published: dict = field(default_factory=dict)

    def run(self, kind="content", cursor=None):
        pages = iter([list(self.revisions.items())])
        return Jira(
            SourceContext(
                source_id="jira",
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
        ).run(kind, cursor)

    def apply(self, batch):
        changes = rows_to_json(batch.changes)
        for change in changes:
            key = change["root_id"]
            if change["op"] == "delete":
                self.revisions.pop(key, None)
                self.published.pop(key, None)
            elif not any(section["text"] == "\0" for section in change["sections"]):
                self.revisions[key] = change["source_revision"]
                self.published[key] = change
        self.cursor = batch.cursor
        return changes

    def sync(self):
        return {
            change["root_id"]: change
            for batch in self.run(cursor=self.cursor)
            for change in self.apply(batch)
        }

    def listing(self):
        batches = list(self.run("reconcile"))
        assert batches[-1].complete
        return {root for batch in batches for root in batch.root_ids}
