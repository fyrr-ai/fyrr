"""File-controlled provider for separate-process, real-S3 incremental proofs."""

import json
import os
import time
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from fyrr_connectors import (
    Connector,
    ContainerGrant,
    ContentBatch,
    Delete,
    DocumentPermission,
    ListingBatch,
    Location,
    Metadata,
    PermissionBatch,
    PrincipalBatch,
    Section,
    SourceGroup,
    SourceUser,
    Upsert,
    document_changes,
    provider_info,
)

from . import FixtureScope


class Incremental(Connector):
    Scope = FixtureScope

    def data(self) -> dict:
        data = json.loads(Path(self.context.scope.fixture).read_text())
        if data.get("mode") == "crash":
            os._exit(23)
        if data.get("mode") == "hang":
            Path(self.context.scope.fixture).with_name("entered").write_text("hung")
            time.sleep(300)
        return data

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        data = self.data()
        if "record_revisions" in data:
            self.record_revisions(data)
        if data.get("capture_cursors"):
            Path(self.context.scope.fixture).with_name("content-cursor.json").write_text(
                json.dumps(cursor)
            )
        for index, documents in enumerate(data.get("content", [])):
            changes = []
            for doc in documents:
                common = {
                    key: doc[key]
                    for key in (
                        "revision_order",
                        "expected_prior_revision",
                        "permission_revision",
                        "containers",
                    )
                    if key in doc
                }
                if doc.get("delete"):
                    changes.append(
                        Delete(root_id=doc["id"], source_revision=doc.get("revision"), **common)
                    )
                else:
                    changes.append(
                        Upsert(
                            root_id=doc["id"],
                            source_revision=doc["revision"],
                            readers=doc.get("readers", ["grp:group"]),
                            metadata=Metadata(
                                source_uri="test://" + doc["id"],
                                source_type="test.doc",
                                author=doc.get("author"),
                                title=doc.get("title"),
                                team=doc.get("team"),
                                status=doc.get("status"),
                                labels=doc.get("labels", ()),
                                created_at=(
                                    datetime.fromisoformat(doc["created_at"])
                                    if "created_at" in doc
                                    else None
                                ),
                                **(
                                    {"updated_at": datetime.fromisoformat(doc["updated_at"])}
                                    if "updated_at" in doc
                                    else {}
                                ),
                            ),
                            sections=[
                                Section(
                                    text=text,
                                    location=(
                                        Location(kind="heading", label=f"section-{section}")
                                        if "section_readers" in doc
                                        else None
                                    ),
                                    readers=(
                                        doc["section_readers"][section]
                                        if "section_readers" in doc
                                        else None
                                    ),
                                )
                                for section, text in enumerate(doc["texts"])
                            ],
                            **common,
                        )
                    )
            yield ContentBatch(changes=document_changes(changes), cursor=f"{data['epoch']}-{index}")

    def record_revisions(self, data: dict) -> None:
        """Asks the worker for the stored revisions, both ways a provider can,
        and records the answers beside the fixture for the test to read."""
        seen: dict = {"lookup": self.context.lookup_revisions(data["record_revisions"]), "next": []}
        while page := self.context.next_revisions():
            seen["next"].extend(page)
        Path(self.context.scope.fixture).with_name("revisions-seen.json").write_text(
            json.dumps(seen)
        )

    def catch_revision_failure(self, data: dict) -> None:
        callback = data.get("catch_revision_failure")
        if callback is None:
            return
        marker = Path(self.context.scope.fixture).with_name("revision-callback")
        marker.write_text("calling")
        try:
            if callback == "lookup":
                self.context.lookup_revisions(["a"])
            elif callback == "lookup_many":
                self.context.lookup_revisions([f"r{n}" for n in range(257)])
            else:
                while self.context.next_revisions():
                    pass
        except RuntimeError:
            marker.write_text("caught")

    def permissions(self, cursor: str | None) -> Iterator[PermissionBatch]:
        data = self.data()
        self.catch_revision_failure(data)
        for index, documents in enumerate(
            data.get("permissions", [[]] if data.get("container_grants") else [])
        ):
            yield PermissionBatch(
                documents=[
                    DocumentPermission(
                        root_id=d["id"],
                        permission_revision=d["revision"],
                        readers=d["readers"],
                        containers=d.get("containers", []),
                    )
                    for d in documents
                ],
                container_grants=[
                    ContainerGrant(**g)
                    for g in (
                        data.get("container_grants", [])[index]
                        if index < len(data.get("container_grants", []))
                        else []
                    )
                ],
                cursor=f"{data['epoch']}-{index}",
            )

    def members(self, cursor: str | None) -> Iterator[PrincipalBatch]:
        data = self.data()
        self.catch_revision_failure(data)
        for index, part in enumerate(data.get("members", [])):
            yield PrincipalBatch(
                snapshot_id=str(data["epoch"]),
                part=index,
                last=part.get("last", True),
                users=[SourceUser(**u) for u in part.get("users", [])],
                groups=[SourceGroup(**g) for g in part.get("groups", [])],
                cursor=f"{data['epoch']}-{index}",
            )

    def listing(self, cursor: str | None) -> Iterator[ListingBatch]:
        data = self.data()
        for index, part in enumerate(data.get("listing", [])):
            yield ListingBatch(
                listing_id=str(data["epoch"]),
                part=index,
                last=part["last"],
                complete=part["complete"],
                root_ids=part["roots"],
                cursor=f"{data['epoch']}-{index}",
            )


class ContentOnly(Connector):
    """`Incremental` without a permission stream: content alone states the readers."""

    Scope = FixtureScope
    data = Incremental.data
    content = Incremental.content


def info() -> dict:
    return provider_info(name="incremental-test", connector=Incremental, min_sdk_version="0.1.0")


def content_only_info() -> dict:
    return provider_info(
        name="incremental-content-only", connector=ContentOnly, min_sdk_version="0.1.0"
    )
