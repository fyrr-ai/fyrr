"""The Google Drive connector: Docs, Sheets and Slides as text.

With `permissions = "sync"` the permission stream is the only source of
permission state: content carries no readers, and a document is readable once
its permission envelope is applied.

Design, the reference review and the tenant items: docs/connectors/google-drive.md.
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from functools import partial, wraps
from itertools import chain, islice
from typing import Any
from urllib.parse import quote

from ..arrow import Delete, Metadata, Section, Upsert, document_changes
from ..connector import (
    Connector,
    ContainerGrant,
    ContentBatch,
    DocumentPermission,
    ListingBatch,
    PermissionBatch,
    PrincipalBatch,
    SourceGroup,
    SourceUser,
)
from .bounds import Budget
from .client import DIRECTORY, ApiError, Client
from .config import MIME_TYPES, Scope, Settings
from .export import FORMATS, Unexportable, clean
from .feeds import (
    FILE_FIELDS,
    FOLDER,
    Cursor,
    Feed,
    Folders,
    beneath,
    discover,
    listed,
    pages,
    start_token,
)
from .permissions import PERMISSION_FIELDS, principals, readers, revision

#: Bumped when the exported text of an unchanged file would differ.
EXPORT_FORMAT = "1"
EXPORT_WORKERS = 8
#: The worker's revision lookup takes at most this many documents.
LOOKUP_ROWS = 256
#: Principals per part of a membership snapshot.
MEMBER_ROWS = 500
#: A My Drive file's own resource carries its permissions for who may share
#: it; other users and shared-drive files need a list call.
PERMISSION_FILE_FIELDS = (
    f"{FILE_FIELDS},inheritedPermissionsDisabled,permissionIds,permissions({PERMISSION_FIELDS})"
)
#: What the full-ID listing needs to tell which feed reports a file.
LISTING_FIELDS = "id,parents,driveId,ownedByMe,owners(emailAddress)"
#: Passes over the changes made while a listing ran. A pass that adds no file
#: older than the listing ends it; without one the listing is not complete.
CATCH_UP_PASSES = 3
#: Drive's clock against ours: a file created this long into a listing may
#: still be older than the listing.
CLOCK_SKEW = timedelta(minutes=5)
#: Marks a document the worker must quarantine, as `local_files` does.
QUARANTINE = [Section(text="\0")]


def _revision(file: dict[str, Any]) -> str:
    """Identity of what an export of `file` would publish; a sharing change
    leaves it alone."""
    parts = [EXPORT_FORMAT, file.get("modifiedTime"), file.get("name"), file.get("mimeType")]
    parts += file.get("parents") or []
    return hashlib.sha256(repr(parts).encode()).hexdigest()[:32]


def _instant(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _run(operation):
    @wraps(operation)
    def run(self, *args, **kwargs):
        try:
            with ThreadPoolExecutor(EXPORT_WORKERS) as pool:
                self._pool = pool
                self._drives_lock = threading.Lock()
                yield from operation(self, *args, **kwargs)
        finally:
            if hasattr(self, "_client"):
                self._client.close()

    return run


class GoogleDrive(Connector[Settings, Scope]):
    _pool: ThreadPoolExecutor
    _drives_lock: threading.Lock
    Settings = Settings
    Scope = Scope

    def _connect(self, sync_only: bool = False) -> None:
        settings, scope = self.context.settings, self.context.scope
        assert isinstance(settings, Settings) and isinstance(scope, Scope)
        self._sync = self.context.permissions == "sync"
        if sync_only and not self._sync:
            raise ValueError("google_drive_fixed_reads_no_permissions")
        if self._sync and not settings.admin_email:
            raise ValueError("google_drive_admin_email_required")
        if not self._sync and not scope.owners:
            raise ValueError("google_drive_owners_required")
        if set(scope.extensions) - set(MIME_TYPES):
            raise ValueError("google_drive_extension_unknown")
        if self._sync and scope.shared_with_me:
            # Another owner's permission list is not reliably readable, and a
            # list that stops being readable would keep its last readers.
            raise ValueError("google_drive_shared_with_me_requires_fixed_permissions")
        self._admin = settings.admin_email or ""
        self._mime_types = {MIME_TYPES[extension] for extension in scope.extensions}
        self._client = Client(settings.service_account.reveal(), settings.api_endpoint)
        self._folders = Folders(self._client, scope.folders)

    def _directory(self, path: str, key: str, **params: Any) -> Iterator[dict[str, Any]]:
        """Every entry of a Directory list, read as the administrator."""
        params["fields"] = f"nextPageToken,{key}({params['fields']})"
        for page in self._client.pages(self._admin, path, api=DIRECTORY, **params):
            yield from page.get(key, [])

    def _directory_users(self) -> Iterator[dict[str, Any]]:
        return self._directory(
            "users",
            "users",
            customer="my_customer",
            maxResults=500,
            orderBy="email",
            fields="primaryEmail,suspended,archived",
        )

    def _open(self, sync_only: bool = False) -> list[Feed]:
        """The feeds: of `scope.owners`, or under `sync` of every active
        Directory user (those of `scope.owners`, if any are listed)."""
        self._connect(sync_only)
        scope = self.context.scope
        assert isinstance(scope, Scope)
        users = [owner.strip().lower() for owner in scope.owners]
        if self._sync:
            active = [
                user["primaryEmail"].lower()
                for user in self._directory_users()
                if not (user.get("suspended") or user.get("archived"))
            ]
            users = [user for user in active if not users or user in users]
        return discover(self._client, scope, users)

    def _published(self, roots: list[str]) -> dict[str, str] | None:
        """Published and pending revisions of `roots`; None without the worker."""
        lookup = self.context.lookup_revisions
        if lookup is None:
            return None
        found: dict[str, str] = {}
        for start in range(0, len(roots), LOOKUP_ROWS):
            found.update(lookup(roots[start : start + LOOKUP_ROWS]))
        return found

    def _export(self, feed: Feed, file: dict[str, Any]) -> list[Section] | None:
        """The file's sections; `QUARANTINE` when it cannot be exported; None
        when it vanished since the change was read."""
        export_type, parse, _, _ = FORMATS[file["mimeType"]]
        try:
            data = self._client.export(feed.subject, file["id"], export_type)
            return parse(data, self.context.limits.document_max_bytes)
        except Unexportable:
            return QUARANTINE
        except ApiError as error:
            # A rate limit or an outage fails the sync instead (client.py).
            if error.transient or error.status not in (403, 404):
                raise
            return QUARANTINE if error.status == 403 else None

    def _upsert(self, feed: Feed, file: dict[str, Any], sections: list[Section]) -> Upsert:
        _, _, source_type, path = FORMATS[file["mimeType"]]
        people = [*file.get("owners", []), file.get("lastModifyingUser") or {}]
        author = next((who["emailAddress"] for who in people if who.get("emailAddress")), None)
        return Upsert(
            root_id=file["id"],
            source_revision=_revision(file),
            revision_order=int(file["version"]),
            metadata=Metadata(
                source_uri=f"https://docs.google.com/{path}/d/{file['id']}/edit",
                source_type=source_type,
                title=clean(file.get("name", "")),
                author=clean(author or ""),
                project=clean(feed.name or ""),
                created_at=_instant(file.get("createdTime")),
                updated_at=_instant(file.get("modifiedTime")),
                connector_map={"file_id": file["id"], "drive_id": feed.drive_id},
            ),
            sections=sections,
            readers=() if self._sync else self.context.readers,
        )

    @staticmethod
    def _delete(file: dict[str, Any], revision: str) -> Delete:
        # Its own revision, so a restored file never looks already published.
        return Delete(
            root_id=file["id"], source_revision=revision, revision_order=int(file["version"])
        )

    def _changes(
        self, feed: Feed, files: list[dict[str, Any]], pending: dict[str, Upsert | Delete]
    ) -> Iterator[Upsert | Delete]:
        """Document changes for current file resources of this feed. A file
        that merely left the scope is removed by the full-ID listing.
        A delete is due for what is published and for what this sync is about
        to hand over (`pending`): a file can change twice within one sync."""
        files = [
            file for file in files if file.get("mimeType") in self._mime_types and feed.owns(file)
        ]
        published = self._published([file["id"] for file in files])
        exports = []
        for file in files:
            known = None if published is None else published.get(file["id"])
            waiting = pending.get(file["id"])
            if isinstance(waiting, Upsert):
                # Exported for this batch already: by an earlier entry, or by another feed.
                known = waiting.source_revision
            if file.get("trashed"):
                if published is None or known is not None or file["id"] in pending:
                    yield self._delete(file, f"trashed:{file['version']}")
            elif known != _revision(file) and self._folders.contain(feed, file):
                exports.append(file)
        exported = self._pool.map(lambda file: self._export(feed, file), exports)
        for file, sections in zip(exports, exported, strict=True):
            if sections:
                yield self._upsert(feed, file, sections)
            elif sections is not None and (
                published is None or file["id"] in published or file["id"] in pending
            ):
                # Nothing left to index: an emptied document leaves.
                yield self._delete(file, _revision(file))

    def _candidates(
        self,
        feed: Feed,
        files: list[dict[str, Any]],
        fields: str = FILE_FIELDS,
        every_folder: bool = False,
    ) -> Iterator[list[dict[str, Any]]]:
        """`files`, and the files beneath changed folders, which have no
        change entry of their own: for content, a folder moved into a folder
        scope; for permissions (`every_folder`), any folder, since its sharing
        is inherited by everything beneath it."""
        for file in files:
            if file.get("mimeType") == FOLDER:
                # The changed resource is newer than a parent looked up earlier in this sync.
                self._folders.learn(feed, file)
        yield files
        if every_folder or self._folders:
            for file in files:
                if (
                    file.get("mimeType") == FOLDER
                    and not file.get("trashed")
                    and (every_folder or self._folders.contain(feed, file))
                ):
                    yield from beneath(self._client, feed, file["id"], self._mime_types, fields)

    @_run
    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        feeds = self._open()
        limits = self.context.limits
        state = Cursor.decode(cursor)
        state.retain(feeds)
        # A batch carries the cursor of the last page that is handed over in
        # full. Changes of a later page may ride along: after a crash that
        # page is read again and the revision lookup skips what is durable.
        durable, sent = state.encode(), cursor
        pending: dict[str, Upsert | Delete] = {}
        size = 0

        def flush() -> ContentBatch:
            nonlocal size, sent
            batch = ContentBatch(changes=document_changes(pending.values()), cursor=durable)
            pending.clear()
            size, sent = 0, durable
            return batch

        for feed in feeds:
            for files, step in pages(self._client, feed, state, self._mime_types):
                for candidates in self._candidates(feed, files):
                    for change in self._changes(feed, candidates, pending):
                        sections = change.sections if isinstance(change, Upsert) else ()
                        weight = 4096 + sum(len(section.text.encode()) for section in sections)
                        if pending and (
                            len(pending) >= limits.max_rows or size + weight > limits.max_bytes // 2
                        ):
                            yield flush()
                        pending[change.root_id] = change
                        size += weight
                step()
                durable = state.encode()
        if pending or durable != sent:
            yield flush()

    @_run
    def listing(self, cursor: str | None) -> Iterator[ListingBatch]:
        feeds = self._open()
        listing_id = hashlib.sha256(f"{cursor}:{time.time_ns()}".encode()).hexdigest()
        rows = min(LOOKUP_ROWS, self.context.limits.max_rows)
        roots: list[str] = []
        part = 0
        fields = LISTING_FIELDS
        # Every listed ID: another owner's file can be listed by each user it
        # is shared with, and the catch-up must know what is still missing.
        seen: set[str] = set()
        budget = Budget()

        def take(feed: Feed, file: dict[str, Any]) -> bool:
            if file["id"] in seen or not feed.owns(file) or not self._folders.contain(feed, file):
                return False
            budget.add(file["id"])
            seen.add(file["id"])
            roots.append(file["id"])
            return True

        def full_parts() -> Iterator[ListingBatch]:
            nonlocal part
            while len(roots) > rows:
                yield ListingBatch(
                    listing_id=listing_id,
                    part=part,
                    last=False,
                    complete=False,
                    root_ids=roots[:rows],
                    cursor=f"{listing_id}:{part}",
                )
                part += 1
                del roots[:rows]

        # The feeds are listed one after another, so a file that moves from a
        # feed not yet listed into one already listed is in neither listing.
        # The changes of every feed since before the listing name such a file.
        began = datetime.now(UTC) + CLOCK_SKEW
        since = Cursor(feeds={feed.key: start_token(self._client, feed) for feed in feeds})
        # Any failed page raises: no final part, so nothing is deleted.
        for feed in feeds:
            for files in listed(self._client, feed, self._mime_types, fields):
                for file in files:
                    take(feed, file)
                yield from full_parts()
        changed = f"{fields},mimeType,trashed,createdTime"
        drives = {feed.drive_id for feed in feeds if feed.drive_id}
        owners = {feed.subject for feed in feeds if feed.drive_id is None and feed.owned}

        def elsewhere(file: dict[str, Any]) -> bool:
            """Whether another feed of this listing reports the file."""
            if file.get("driveId"):
                return file["driveId"] in drives
            return any(
                str(owner.get("emailAddress", "")).lower() in owners
                for owner in file.get("owners", [])
            )

        settled = False
        for _ in range(CATCH_UP_PASSES):
            # Files the listing missed: taken here, or seen in a feed they
            # have left for another one, whose changes the next pass reads (a
            # file that changed feed again while this pass ran). A file
            # created since the listing began is listed too but asks for no
            # further pass: it cannot have been missed by moving, and new
            # files never stop arriving in a large domain.
            missed = 0
            for feed in feeds:
                for files, step in pages(
                    self._client, feed, since, self._mime_types, changed, recover_invalid=False
                ):
                    for candidates in self._candidates(feed, files, changed, every_folder=True):
                        for file in candidates:
                            if file.get("mimeType") not in self._mime_types or file.get("trashed"):
                                continue
                            unlisted = file["id"] not in seen
                            if take(feed, file) or (
                                unlisted and not feed.owns(file) and elsewhere(file)
                            ):
                                created = _instant(file.get("createdTime"))
                                missed += created is None or created <= began
                    step()
                    yield from full_parts()
            if not missed:
                settled = True
                break
        yield ListingBatch(
            listing_id=listing_id,
            part=part,
            last=True,
            complete=settled,
            root_ids=roots,
            cursor=f"{listing_id}:{part}",
        )

    def _listed_permissions(
        self, subject: str, file_id: str, **params: Any
    ) -> list[dict[str, Any]]:
        """The complete permission list, or an error: never a partial one."""
        pages = self._client.pages(
            subject,
            f"files/{file_id}/permissions",
            supportsAllDrives="true",
            pageSize=100,
            fields=f"nextPageToken,permissions({PERMISSION_FIELDS})",
            **params,
        )
        return [permission for page in pages for permission in page.get("permissions", [])]

    def _drive_permissions(self, drive_id: str) -> list[dict[str, Any]]:
        return self._listed_permissions(self._admin, drive_id, useDomainAdminAccess="true")

    def _permission(
        self, feed: Feed, file: dict[str, Any], drives: dict[str, frozenset[str]]
    ) -> DocumentPermission | None:
        """The file's readers; None when it vanished since the change was
        read. Any other failed read raises: nothing is derived from it."""
        listed = file.get("permissions")
        if (
            feed.drive_id is not None
            or listed is None
            or {p.get("id") for p in listed} != set(file.get("permissionIds") or ())
        ):
            try:
                listed = self._listed_permissions(feed.subject, file["id"])
            except ApiError as error:
                if error.transient or error.status != 404:
                    raise
                return None
        with self._drives_lock:
            drive = drives.get(feed.drive_id or "")
            tokens, inherits = readers(listed, drive, feed.drive_id)
            if drive is not None and not inherits:
                # A member left since the drive was read, or a limited-access folder.
                drive = drives[feed.drive_id or ""] = frozenset(
                    permission["id"] for permission in self._drive_permissions(feed.drive_id or "")
                )
                tokens, inherits = readers(listed, drive, feed.drive_id)
        # The list states the inheritance; a folder above may still cut it.
        inherits = inherits and not self._folders.limited(feed, file)
        containers = [feed.drive_id] if inherits and feed.drive_id else []
        return DocumentPermission(
            root_id=file["id"],
            permission_revision=revision(tokens, containers),
            readers=tokens,
            containers=containers,
        )

    @_run
    def permissions(self, cursor: str | None) -> Iterator[PermissionBatch]:
        feeds = self._open(sync_only=True)
        state = Cursor.decode(cursor)
        state.retain(feeds)
        # Shared-drive membership first, so a revoke never waits behind files.
        drives: dict[str, frozenset[str]] = {}
        grants = []
        for feed in feeds:
            if feed.drive_id is not None:
                members = self._drive_permissions(feed.drive_id)
                drives[feed.drive_id] = frozenset(member["id"] for member in members)
                tokens, _ = readers(members)
                if state.grants.get(feed.drive_id) != revision(tokens):
                    state.grants[feed.drive_id] = revision(tokens)
                    grants.append(
                        ContainerGrant(
                            container_id=feed.drive_id,
                            container_kind="drive",
                            permission_revision=revision(tokens),
                            readers=tokens,
                        )
                    )
        durable, sent = state.encode(), cursor
        pending: dict[str, DocumentPermission] = {}

        def flush() -> PermissionBatch:
            nonlocal sent
            batch = PermissionBatch(
                documents=list(pending.values()), container_grants=list(grants), cursor=durable
            )
            pending.clear()
            grants.clear()
            sent = durable
            return batch

        if grants:
            yield flush()
        for feed in feeds:
            for files, step in pages(
                self._client, feed, state, self._mime_types, PERMISSION_FILE_FIELDS
            ):
                for candidates in self._candidates(
                    feed, files, PERMISSION_FILE_FIELDS, every_folder=True
                ):
                    wanted = [
                        file
                        for file in candidates
                        if file.get("mimeType") in self._mime_types
                        and feed.owns(file)
                        and not file.get("trashed")
                        and self._folders.contain(feed, file)
                    ]
                    read = self._pool.map(partial(self._permission, feed, drives=drives), wanted)
                    for document in read:
                        if document is not None:
                            if len(pending) >= LOOKUP_ROWS:
                                yield flush()
                            pending[document.root_id] = document
                step()
                durable = state.encode()
        if pending or durable != sent:
            yield flush()

    @_run
    def members(self, cursor: str | None) -> Iterator[PrincipalBatch]:
        self._connect(sync_only=True)
        budget = Budget()

        def bounded(values, *, edges=False):
            for value in values:
                budget.add(value, edges=int(edges), entries=int(not edges))
                yield value

        groups = {
            group["email"]: list(
                bounded(
                    self._directory(
                        f"groups/{quote(group['email'], safe='@')}/members",
                        "members",
                        maxResults=200,
                        fields="email,type",
                    ),
                    edges=True,
                )
            )
            for group in bounded(
                self._directory(
                    "groups", "groups", customer="my_customer", maxResults=200, fields="email"
                )
            )
        }
        users, found = principals(self._directory_users(), groups)
        total = len(users) + len(found)
        entries = iter(chain(users, found))
        snapshot_id = hashlib.sha256(f"{cursor}:{time.time_ns()}".encode()).hexdigest()
        # A failed Directory read raised above: no part exists, nothing is applied.
        for part, start in enumerate(range(0, max(total, 1), MEMBER_ROWS)):
            page = list(islice(entries, MEMBER_ROWS))
            yield PrincipalBatch(
                snapshot_id=snapshot_id,
                part=part,
                last=start + MEMBER_ROWS >= total,
                users=[entry for entry in page if isinstance(entry, SourceUser)],
                groups=[entry for entry in page if isinstance(entry, SourceGroup)],
                cursor=f"{snapshot_id}:{part}",
            )
