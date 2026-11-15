"""A way to run a sync kind the way the worker does, with or without its
revision lookup, and to read the outcome by the names things were created as."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from fake_notion import FakeNotion, ident

from fyrr_connectors import bundled, runtime
from fyrr_connectors.arrow import rows_to_json

SETTINGS = {"token": {"env": "FYRR_TEST_NOTION_TOKEN"}}
LIMITS = {"max_rows": 100, "max_bytes": 1 << 20, "document_max_bytes": 1 << 16}
READERS = ["grp:staff", "usr:lead"]


@dataclass
class Index:
    """What fyrr holds for the source: applied batches and the cursor."""

    world: FakeNotion
    roots: list[str] = field(default_factory=lambda: ["root"])
    revisions: dict[str, str] = field(default_factory=dict)
    cursor: str | None = None
    batches: list[dict[str, dict[str, Any]]] = field(default_factory=list)
    lookup: bool = True
    limits: dict[str, int] = field(default_factory=lambda: dict(LIMITS))
    settings: dict[str, Any] = field(default_factory=lambda: dict(SETTINGS))

    def run(
        self, kind: str = "content", cursor: str | None = None, permissions: str = "fixed"
    ) -> Iterator[Any]:
        source = {
            "source_id": "notion",
            "provider": "notion",
            "permissions": permissions,
            "readers": READERS,
            "settings": self.settings,
            "scope": {"root_pages": [ident(root) for root in self.roots]},
            "limits": self.limits,
        }
        remaining = iter([list(self.revisions.items())])
        view = (
            lambda roots: {r: self.revisions[r] for r in roots if r in self.revisions},
            lambda: next(remaining, []),
        )
        connector = runtime.open_connector(source, bundled.notion(), view if self.lookup else None)
        return connector.run(kind, cursor)

    def apply(self, batch: Any) -> dict[str, dict[str, Any]]:
        """Acknowledge one batch: its changes by name."""
        changes = {}
        for change in rows_to_json(batch.changes):
            if change["op"] == "delete":
                self.revisions.pop(change["root_id"], None)
            else:
                self.revisions[change["root_id"]] = change["source_revision"]
            changes[self.world.names[change["root_id"]]] = change
        self.cursor = batch.cursor
        self.batches.append(changes)
        return changes

    def sync(self) -> dict[str, dict[str, Any]]:
        """Run a content sync, apply every batch as it is acknowledged and
        return the changes by name."""
        seen: dict[str, dict[str, Any]] = {}
        for batch in self.run(cursor=self.cursor):
            seen.update(self.apply(batch))
        return seen

    def listing(self) -> list[str]:
        parts = list(self.run("reconcile"))
        assert [part.last for part in parts] == [False] * (len(parts) - 1) + [True]
        assert parts[-1].complete
        return sorted(self.world.names[root] for part in parts for root in part.root_ids)

    def published(self) -> list[str]:
        return sorted(self.world.names[root] for root in self.revisions)


def text(change: dict[str, Any]) -> str:
    return "\n---\n".join(section["text"] for section in change["sections"])
