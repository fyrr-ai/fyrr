"""The connector API: one method per sync kind, each yielding contract batches.

fyrr runs one sync kind of one source per call, in the source's own process.
It passes the last durable cursor (`None` for a first or reset sync);
every yielded batch carries the opaque cursor *after* it, which fyrr stores
once the batch's envelope is durable. A connector keeps no other
state. A document never spans batches; keep each content batch
within `context.limits`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

import pyarrow as pa

from .config import ConfigModel, NoConfig


class SyncKind(StrEnum):
    """A source's independent syncs."""

    CONTENT = "content"
    PERMISSIONS = "permissions"
    MEMBERS = "members"
    RECONCILE = "reconcile"


@dataclass(frozen=True, kw_only=True)
class Limits:
    """Bounds from the fyrr configuration."""

    #: Rows per content batch: min(envelope_max_rows, commit.group_max_rows, 256).
    max_rows: int
    #: Complete Arrow buffer bytes per content batch (`writer.envelope_max_bytes`).
    max_bytes: int
    #: Text bytes per document (`document_max_bytes`); larger ones are quarantined.
    document_max_bytes: int
    row_warning: int | None = None
    row_error: int | None = None
    retention_days: int | None = None


@dataclass(frozen=True, kw_only=True)
class SourceContext[SettingsT: ConfigModel = ConfigModel, ScopeT: ConfigModel = ConfigModel]:
    """One configured source, as the connector sees it."""

    source_id: str
    #: `sync` (map the source's permissions) or `fixed`.
    permissions: str
    #: The configured readers of a `fixed` connection.
    readers: tuple[str, ...]
    settings: SettingsT
    scope: ScopeT
    limits: Limits
    #: Bounded callbacks over the pinned publication and durable pending envelopes.
    lookup_revisions: Callable[[list[str]], dict[str, str]] | None = None
    next_revisions: Callable[[], list[tuple[str, str]]] | None = None


@dataclass(frozen=True, kw_only=True)
class ContentBatch:
    """Complete document changes, built with `fyrr_connectors.document_changes`."""

    changes: pa.RecordBatch
    cursor: str


@dataclass(frozen=True, kw_only=True)
class DocumentPermission:
    root_id: str
    permission_revision: str
    readers: Sequence[str]
    containers: Sequence[str] = ()


@dataclass(frozen=True, kw_only=True)
class ContainerGrant:
    container_id: str
    container_kind: str
    permission_revision: str
    readers: Sequence[str]


@dataclass(frozen=True, kw_only=True)
class PermissionBatch:
    documents: Sequence[DocumentPermission] = ()
    container_grants: Sequence[ContainerGrant] = ()
    cursor: str


class EmailVerification(StrEnum):
    """Source evidence; only verified organization-managed addresses auto-link."""

    UNVERIFIED = "unverified"
    VERIFIED_CORPORATE = "verified_corporate"
    VERIFIED_PERSONAL = "verified_personal"


@dataclass(frozen=True, kw_only=True)
class SourceUser:
    id: str
    active: bool
    email: str | None = None
    email_verification: EmailVerification = EmailVerification.UNVERIFIED


@dataclass(frozen=True, kw_only=True)
class SourceGroup:
    id: str
    members: Sequence[str]


@dataclass(frozen=True, kw_only=True)
class PrincipalBatch:
    """One part of a full snapshot of the source's users and groups."""

    snapshot_id: str
    part: int
    last: bool
    users: Sequence[SourceUser] = ()
    groups: Sequence[SourceGroup] = ()
    cursor: str


@dataclass(frozen=True, kw_only=True)
class ListingBatch:
    """One part of a full-ID listing. Set `complete` only on the final part
    and only when the listing saw every document in scope: an incomplete
    listing never deletes anything."""

    listing_id: str
    part: int
    last: bool
    complete: bool
    root_ids: Sequence[str]
    cursor: str


Batch = ContentBatch | PermissionBatch | PrincipalBatch | ListingBatch


class Connector[SettingsT: ConfigModel = ConfigModel, ScopeT: ConfigModel = ConfigModel]:
    """Base class of a connector. Override `content` and, when the source
    supports them, `permissions`, `members` and `listing`; the provider
    metadata derives its sync kinds from the overridden methods."""

    #: The model of the source's `settings` table.
    Settings: ClassVar[type[ConfigModel]] = NoConfig
    #: The model of the source's `scope` table.
    Scope: ClassVar[type[ConfigModel]] = NoConfig

    def __init__(self, context: SourceContext[SettingsT, ScopeT]) -> None:
        self.context = context

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        """Document changes since `cursor`."""
        raise NotImplementedError

    def permissions(self, cursor: str | None) -> Iterator[PermissionBatch]:
        """Per-document readers and container grants since `cursor`."""
        raise NotImplementedError

    def members(self, cursor: str | None) -> Iterator[PrincipalBatch]:
        """A full snapshot of the source's users and groups."""
        raise NotImplementedError

    def listing(self, cursor: str | None) -> Iterator[ListingBatch]:
        """A full-ID listing of every document in scope (reconcile)."""
        raise NotImplementedError

    @classmethod
    def sync_kinds(cls) -> list[SyncKind]:
        """The kinds this connector implements, in contract order."""
        methods = {
            SyncKind.CONTENT: "content",
            SyncKind.PERMISSIONS: "permissions",
            SyncKind.MEMBERS: "members",
            SyncKind.RECONCILE: "listing",
        }
        return [
            kind
            for kind, method in methods.items()
            if kind is SyncKind.CONTENT or getattr(cls, method) is not getattr(Connector, method)
        ]

    def run(self, kind: SyncKind, cursor: str | None) -> Iterator[Batch]:
        """Dispatch one sync kind."""
        methods: dict[SyncKind, Callable[[str | None], Iterator[Batch]]] = {
            SyncKind.CONTENT: self.content,
            SyncKind.PERMISSIONS: self.permissions,
            SyncKind.MEMBERS: self.members,
            SyncKind.RECONCILE: self.listing,
        }
        return methods[SyncKind(kind)](cursor)
