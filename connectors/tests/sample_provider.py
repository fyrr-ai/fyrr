"""A provider used by the SDK's own tests (not installed as an entry point)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Literal

from fyrr_connectors import (
    ConfigModel,
    Connector,
    ContentBatch,
    ListingBatch,
    Location,
    Metadata,
    Secret,
    Section,
    Upsert,
    document_changes,
    provider_info,
    secret,
    setting,
)


class Settings(ConfigModel):
    """Connection settings."""

    api_token: Secret = secret(description="API token.")
    page_size: int = setting(description="Documents per batch.", default=2, minimum=1, maximum=100)
    mode: Literal["fast", "careful"] = setting(description="Mode.", default="fast")


class Scope(ConfigModel):
    """What to sync."""

    titles: list[str] = setting(description="Document titles.", min_items=1)


class SampleConnector(Connector):
    Settings = Settings
    Scope = Scope

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        titles = list(self.context.scope.titles)
        size = self.context.settings.page_size
        start = int(cursor or 0)
        for offset in range(start, len(titles), size):
            changes = [
                Upsert(
                    root_id=f"doc-{index}",
                    source_revision="r1",
                    readers=["grp:sample"],
                    metadata=Metadata(
                        source_uri=f"sample://doc-{index}",
                        source_type="sample.doc",
                        title=title,
                        created_at=datetime(2026, 9, 30, 9, 41, 12, 512000, tzinfo=UTC),
                    ),
                    sections=[Section(text=f"Body of {title}.", location=Location(kind="body"))],
                )
                for index, title in enumerate(titles[offset : offset + size], start=offset)
            ]
            yield ContentBatch(changes=document_changes(changes), cursor=str(offset + size))

    def listing(self, cursor: str | None) -> Iterator[ListingBatch]:
        yield ListingBatch(
            listing_id="sample-listing",
            part=0,
            last=True,
            complete=True,
            root_ids=[f"doc-{index}" for index in range(len(self.context.scope.titles))],
            cursor="listed",
        )


def info() -> dict:
    return provider_info(
        name="sample",
        connector=SampleConnector,
        min_sdk_version="0.1.0",
        connection_types=("fixed",),
        description="SDK test provider",
    )
