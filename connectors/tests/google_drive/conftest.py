"""Fixtures: the fake Drive attached in process, and a way to run a sync kind
the way the worker does, with or without its revision lookup."""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest
from fake_drive import ADMIN, FakeDrive
from openpyxl import Workbook

from fyrr_connectors import bundled, runtime
from fyrr_connectors.arrow import rows_to_json
from fyrr_connectors.google_drive import client

ALICE = "alice@example.com"
BOB = "bob@example.com"
SETTINGS = {"service_account": {"env": "FYRR_TEST_GDRIVE_KEY"}, "admin_email": ADMIN}
LIMITS = {"max_rows": 100, "max_bytes": 1 << 20, "document_max_bytes": 1 << 16}


@pytest.fixture
def drive(monkeypatch: pytest.MonkeyPatch) -> FakeDrive:
    world = FakeDrive()
    world.user(ALICE)
    world.user(BOB)
    monkeypatch.setattr(
        client, "authorized_session", lambda _key, subject, _api: world.session(subject)
    )
    monkeypatch.setattr(client.time, "sleep", world.sleeps.append)
    monkeypatch.setenv("FYRR_TEST_GDRIVE_KEY", "{}")
    return world


@dataclass
class Index:
    """What fyrr holds for the source: applied batches and the cursor."""

    revisions: dict[str, str] = field(default_factory=dict)
    cursor: str | None = None
    permission_cursor: str | None = None
    permission_batches: int = 0
    batches: list[dict[str, dict[str, Any]]] = field(default_factory=list)
    lookup: bool = True
    limits: dict[str, int] = field(default_factory=lambda: dict(LIMITS))
    settings: dict[str, Any] = field(default_factory=lambda: dict(SETTINGS))

    def run(
        self,
        kind: str = "content",
        permissions: str = "fixed",
        cursor: str | None = None,
        **scope: Any,
    ) -> Iterator[Any]:
        scope.setdefault("owners", [ALICE, BOB])
        source = {
            "source_id": "drive",
            "provider": "google-drive",
            "permissions": permissions,
            "readers": ["grp:staff"],
            "settings": self.settings,
            "scope": scope,
            "limits": self.limits,
        }
        view = (lambda roots: {r: self.revisions[r] for r in roots if r in self.revisions}, list)
        connector = runtime.open_connector(
            source, bundled.google_drive(), view if self.lookup else None
        )
        return connector.run(kind, cursor)

    def sync(self, permissions: str = "fixed", **scope: Any) -> dict[str, dict[str, Any]]:
        """Run a content sync, apply every batch as it is acknowledged and
        return the changes by document."""
        seen: dict[str, dict[str, Any]] = {}
        for batch in self.run(permissions=permissions, cursor=self.cursor, **scope):
            changes = {change["root_id"]: change for change in rows_to_json(batch.changes)}
            for root, change in changes.items():
                if change["op"] == "delete":
                    self.revisions.pop(root, None)
                else:
                    self.revisions[root] = change["source_revision"]
            self.cursor = batch.cursor
            self.batches.append(changes)
            seen.update(changes)
        return seen

    def permissions(self, **scope: Any) -> tuple[dict[str, Any], dict[str, Any]]:
        """Run a permission sync as acknowledged; the documents and the
        container grants it handed over."""
        documents, grants = {}, {}
        for batch in self.run("permissions", "sync", self.permission_cursor, **scope):
            documents.update({document.root_id: document for document in batch.documents})
            grants.update({grant.container_id: grant for grant in batch.container_grants})
            self.permission_cursor = batch.cursor
            self.permission_batches += 1
        return documents, grants

    def listing(self, **scope: Any) -> list[str]:
        parts = list(self.run("reconcile", **scope))
        assert [part.last for part in parts] == [False] * (len(parts) - 1) + [True]
        assert parts[-1].complete
        return sorted(root for part in parts for root in part.root_ids)


@pytest.fixture
def index() -> Index:
    return Index()


def xlsx(sheets: dict[str, list[list[Any]]]) -> bytes:
    workbook = Workbook()
    workbook.remove(workbook.active)
    for title, rows in sheets.items():
        sheet = workbook.create_sheet(title)
        for row in rows:
            sheet.append(row)
    data = io.BytesIO()
    workbook.save(data)
    return data.getvalue()


_P = "http://schemas.openxmlformats.org/presentationml/2006/main"
_A = "http://schemas.openxmlformats.org/drawingml/2006/main"
_R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def pptx(slides: list[list[str]]) -> bytes:
    """A minimal deck. Slide files are numbered in reverse, so only the
    presentation's own slide list gives the right order."""
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        names = [f"slides/slide{len(slides) - number}.xml" for number in range(len(slides))]
        archive.writestr(
            "ppt/presentation.xml",
            f'<p:presentation xmlns:p="{_P}" xmlns:r="{_R}"><p:sldIdLst>'
            + "".join(f'<p:sldId id="{256 + n}" r:id="rId{n}"/>' for n in range(len(slides)))
            + "</p:sldIdLst></p:presentation>",
        )
        archive.writestr(
            "ppt/_rels/presentation.xml.rels",
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + "".join(
                f'<Relationship Id="rId{n}" Target="{name}"/>' for n, name in enumerate(names)
            )
            + "</Relationships>",
        )
        for name, paragraphs in zip(names, slides, strict=True):
            body = "".join(f"<a:p><a:r><a:t>{text}</a:t></a:r></a:p>" for text in paragraphs)
            archive.writestr(
                f"ppt/{name}",
                f'<p:sld xmlns:p="{_P}" xmlns:a="{_A}"><p:cSld><p:spTree><p:sp><p:txBody>'
                f"{body}</p:txBody></p:sp></p:spTree></p:cSld></p:sld>",
            )
    return data.getvalue()
