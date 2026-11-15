"""Feeds: what is read as whom, where each feed stands, and what is in scope.

A feed is one user's Drive (`u:<digest of the address>`, only the files that user owns) or one
shared drive (`d:<id>`, read through one member), so every file belongs to
exactly one feed. With `scope.shared_with_me` a user's feed also takes the
files of other owners, unless that owner's own feed reports them; such a file
can then reach fyrr through several feeds, each time as the same change. A
feed is listed in full once and then followed through the Drive changes feed;
its page token is the only state.
"""

from __future__ import annotations

import base64
import hashlib
import json
import zlib
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from ..validate import MAX_CURSOR_BYTES
from .client import ApiError, Client
from .config import ALL_DRIVES, Scope

FOLDER = "application/vnd.google-apps.folder"
PAGE_SIZE = 100
FILE_FIELDS = (
    "id,name,mimeType,trashed,parents,driveId,version,createdTime,modifiedTime,"
    "ownedByMe,owners(emailAddress),lastModifyingUser(emailAddress)"
)
#: A parent chain longer than this is treated as out of scope.
MAX_DEPTH = 100


@dataclass(frozen=True)
class Feed:
    key: str
    #: The user the feed is read as.
    subject: str
    drive_id: str | None = None
    #: The shared drive's name, used as the source project.
    name: str | None = None
    #: A user feed takes the files its user owns (`scope.my_drive`) ...
    owned: bool = True
    #: ... and those of other owners (`scope.shared_with_me`), except the
    #: `peers`: users whose own feed takes the files they own.
    shared: bool = False
    peers: frozenset[str] = frozenset()

    def owns(self, file: dict[str, Any]) -> bool:
        """Whether this feed reports `file`."""
        if self.drive_id is not None:
            return file.get("driveId") == self.drive_id
        if file.get("driveId"):
            return False
        if file.get("ownedByMe"):
            return self.owned
        owners = {str(owner.get("emailAddress", "")).lower() for owner in file.get("owners", [])}
        return self.shared and not owners & self.peers

    def corpus(self) -> dict[str, Any]:
        if self.drive_id is None:
            return {"corpora": "user"}
        return {
            "corpora": "drive",
            "driveId": self.drive_id,
            "includeItemsFromAllDrives": "true",
            "supportsAllDrives": "true",
        }


@dataclass
class Cursor:
    """Per feed, the changes page token to continue from; and for the one
    feed whose first listing is in progress, its start token and list page."""

    feeds: dict[str, str] = field(default_factory=dict)
    listing: tuple[str, str, str | None] | None = None
    #: Permission sync only: the grant revision last handed over per shared drive.
    grants: dict[str, str] = field(default_factory=dict)

    @classmethod
    def decode(cls, text: str | None) -> Cursor:
        if text is None:
            return cls()
        try:
            state = json.loads(zlib.decompress(base64.urlsafe_b64decode(text)))
            if state["v"] != 1:
                raise ValueError
            listing = state["listing"]
            return cls(
                feeds=dict(state["feeds"]),
                listing=tuple(listing) if listing else None,
                grants=dict(state["grants"]),
            )
        except (ValueError, KeyError, TypeError, zlib.error):
            raise ValueError("google_drive_cursor_invalid") from None

    def encode(self) -> str:
        state = {"v": 1, "feeds": self.feeds, "listing": self.listing, "grants": self.grants}
        packed = json.dumps(state, separators=(",", ":"), sort_keys=True).encode()
        text = base64.urlsafe_b64encode(zlib.compress(packed, 9)).decode()
        if len(text) > MAX_CURSOR_BYTES:
            # Never a truncated map: a dropped feed would be listed again forever.
            raise ValueError("google_drive_cursor_too_large")
        return text

    def retain(self, feeds: Iterable[Feed]) -> None:
        """Forget feeds that left the scope; the full-ID listing removes their documents."""
        keys = {feed.key for feed in feeds}
        self.feeds = {key: token for key, token in self.feeds.items() if key in keys}
        if self.listing is not None and self.listing[0] not in keys:
            self.listing = None
        self.grants = {key: value for key, value in self.grants.items() if f"d:{key}" in keys}


def discover(client: Client, scope: Scope, users: Sequence[str]) -> list[Feed]:
    """The feeds of the scope, in a stable order. A shared drive is read
    through the first user who lists it."""
    users = sorted({user.strip().lower() for user in users})
    # The stored cursor holds feed keys: a digest keeps addresses out of it.
    feeds = [
        Feed(
            key=f"u:{hashlib.sha256(user.encode()).hexdigest()[:16]}",
            subject=user,
            owned=scope.my_drive,
            shared=scope.shared_with_me,
            peers=frozenset(users) if scope.my_drive else frozenset(),
        )
        for user in users
        if scope.my_drive or scope.shared_with_me
    ]
    wanted = set(scope.shared_drives)
    drives: dict[str, Feed] = {}
    for user in users if wanted else ():
        pages = client.pages(
            user, "drives", pageSize=PAGE_SIZE, fields="nextPageToken,drives(id,name)"
        )
        for page in pages:
            for drive in page.get("drives", []):
                if ALL_DRIVES in wanted or drive["id"] in wanted:
                    drives.setdefault(
                        drive["id"],
                        Feed(
                            key=f"d:{drive['id']}",
                            subject=user,
                            drive_id=drive["id"],
                            name=drive.get("name"),
                        ),
                    )
    return feeds + [drives[drive_id] for drive_id in sorted(drives)]


def _query(feed: Feed, mime_types: Iterable[str], parent: str | None = None) -> str:
    kinds = " or ".join(f"mimeType = '{mime}'" for mime in sorted(mime_types))
    clauses = ["trashed = false", f"({kinds})"]
    if parent is not None:
        clauses.append(f"'{parent}' in parents")
    elif feed.drive_id is None and not feed.shared:
        clauses.append("'me' in owners")
    elif feed.drive_id is None and not feed.owned:
        clauses.append("not 'me' in owners")
    return " and ".join(clauses)


def _list(client: Client, feed: Feed, query: str, fields: str, page: str | None) -> dict[str, Any]:
    params = feed.corpus() | {
        "q": query,
        "pageSize": PAGE_SIZE,
        "fields": f"nextPageToken,files({fields})",
    }
    if page is not None:
        params["pageToken"] = page
    return client.get(feed.subject, "files", **params)


def _invalid_page(error: ApiError) -> bool:
    return error.status == 410 or (error.status == 400 and error.location == "pageToken")


def listed(
    client: Client, feed: Feed, mime_types: Iterable[str], fields: str = FILE_FIELDS
) -> Iterator[list[dict[str, Any]]]:
    """Every page of the feed's files of `mime_types`, or an error: never a
    short listing."""
    query, page = _query(feed, mime_types), None
    while True:
        body = _list(client, feed, query, fields, page)
        yield body.get("files", [])
        page = body.get("nextPageToken")
        if not page:
            return


def beneath(
    client: Client, feed: Feed, folder: str, mime_types: Iterable[str], fields: str = FILE_FIELDS
) -> Iterator[list[dict[str, Any]]]:
    """The files of `mime_types` under `folder`, at any depth, page by page."""
    wanted, queue, seen = set(mime_types), [folder], {folder}
    while queue:
        query, page = _query(feed, wanted | {FOLDER}, parent=queue.pop()), None
        while True:
            body = _list(client, feed, query, fields, page)
            files = body.get("files", [])
            for file in files:
                if file.get("mimeType") == FOLDER and file["id"] not in seen:
                    seen.add(file["id"])
                    queue.append(file["id"])
            yield [file for file in files if file.get("mimeType") in wanted]
            page = body.get("nextPageToken")
            if not page:
                break


def start_token(client: Client, feed: Feed) -> str:
    """The feed's changes token for "from now on"."""
    drive = {"driveId": feed.drive_id, "supportsAllDrives": "true"} if feed.drive_id else {}
    return client.get(feed.subject, "changes/startPageToken", **drive)["startPageToken"]


def _first_listing(
    client: Client, feed: Feed, cursor: Cursor, mime_types: Iterable[str], fields: str
) -> Iterator[tuple[list[dict[str, Any]], Callable[[], None]]]:
    """List the feed in full. Its changes token is taken before the first
    page, so nothing that changes during the listing is missed."""
    if cursor.listing is not None and cursor.listing[0] == feed.key:
        _, start, page = cursor.listing
    else:
        start = start_token(client, feed)
        page = None
    query = _query(feed, mime_types)

    def step(following: str | None) -> None:
        if following:
            cursor.listing = (feed.key, start, following)
        else:
            cursor.listing = None
            cursor.feeds[feed.key] = start

    while True:
        try:
            body = _list(client, feed, query, fields, page)
        except ApiError as error:
            if page is None or not _invalid_page(error):
                raise
            page = None  # An expired list page: list again, keep the start token.
            continue
        page = body.get("nextPageToken")
        yield body.get("files", []), partial(step, page)
        if not page:
            return


def pages(
    client: Client,
    feed: Feed,
    cursor: Cursor,
    mime_types: Iterable[str],
    fields: str = FILE_FIELDS,
    *,
    recover_invalid: bool = True,
) -> Iterator[tuple[list[dict[str, Any]], Callable[[], None]]]:
    """The feed's news since `cursor` as `(files, step)` per page: `files`
    are current file resources (in changes also folders and other types), and
    `step()` moves `cursor` past the page once its documents are handed over.
    A feed without a token is listed in full first. A bare `removed` entry is
    left to the full-ID listing: it cannot be told from one user losing
    access or from a move between feeds."""
    relisted = False
    while True:
        if feed.key not in cursor.feeds:
            yield from _first_listing(client, feed, cursor, mime_types, fields)
        try:
            body = client.get(
                feed.subject,
                "changes",
                pageToken=cursor.feeds[feed.key],
                pageSize=PAGE_SIZE,
                includeRemoved="true",
                supportsAllDrives="true",
                includeItemsFromAllDrives="true" if feed.drive_id else "false",
                fields="nextPageToken,newStartPageToken,"
                f"changes(removed,changeType,fileId,file({fields}))",
                **({"driveId": feed.drive_id} if feed.drive_id else {}),
            )
        except ApiError as error:
            if not recover_invalid or relisted or not _invalid_page(error):
                raise
            # The token expired: list the feed again from a fresh start token.
            del cursor.feeds[feed.key]
            relisted = True
            continue
        following = body.get("nextPageToken")
        files = [
            change["file"]
            for change in body.get("changes", [])
            if change.get("changeType", "file") == "file" and "file" in change
        ]
        yield (
            files,
            partial(cursor.feeds.__setitem__, feed.key, following or body["newStartPageToken"]),
        )
        if not following:
            return


class Folders:
    """The `scope.folders` rule: a file is in scope when one of its ancestors
    is a configured folder. Parents are looked up once per run and user; an
    ancestor the user cannot read ends the walk out of scope."""

    def __init__(self, client: Client, folders: Sequence[str]) -> None:
        self._client = client
        self._folders = frozenset(folders)
        self._parents: dict[tuple[str, str], str | None] = {}
        #: Whether the folder disables inherited permissions; None: unreadable.
        self._limits: dict[tuple[str, str], bool | None] = {}

    def __bool__(self) -> bool:
        return bool(self._folders)

    def _parent(self, feed: Feed, folder: str, limits: bool = False) -> str | None:
        """The folder's parent. With `limits` its inheritance flag is read
        too: only the permission mapping asks for it."""
        key = (feed.subject, folder)
        if key not in self._parents or (limits and key not in self._limits):
            fields = "parents,inheritedPermissionsDisabled" if limits else "parents"
            try:
                body = self._client.get(
                    feed.subject, f"files/{folder}", fields=fields, supportsAllDrives="true"
                )
                if limits:
                    self._limits[key] = bool(body.get("inheritedPermissionsDisabled"))
            except ApiError as error:
                if error.transient or error.status not in (403, 404):
                    raise
                body = {}
                self._limits[key] = None
            self._parents[key] = (body.get("parents") or [None])[0]
        return self._parents[key]

    def learn(self, feed: Feed, folder: dict[str, Any]) -> None:
        """Take a folder's parent from its current resource (a change entry)."""
        key = (feed.subject, folder["id"])
        self._parents[key] = (folder.get("parents") or [None])[0]
        if "inheritedPermissionsDisabled" in folder:
            self._limits[key] = bool(folder["inheritedPermissionsDisabled"])
        else:
            self._limits.pop(key, None)

    def limited(self, feed: Feed, file: dict[str, Any]) -> bool:
        """Whether a folder above the file disables inherited permissions (a
        limited-access folder), or cannot be read: the drive's members are
        then not known to reach the file."""
        parent = (file.get("parents") or [None])[0]
        for _ in range(MAX_DEPTH):
            if parent is None or parent == feed.drive_id:
                return False
            above = self._parent(feed, parent, limits=True)
            if self._limits[feed.subject, parent] is not False:
                return True
            parent = above
        return True

    def contain(self, feed: Feed, file: dict[str, Any]) -> bool:
        if not self._folders:
            return True
        if file.get("mimeType") == FOLDER and file["id"] in self._folders:
            return True
        parent = (file.get("parents") or [None])[0]
        for _ in range(MAX_DEPTH):
            if parent is None:
                return False
            if parent in self._folders:
                return True
            parent = self._parent(feed, parent)
        return False
