"""Where a content sync stands, and which pages changed since.

The cursor is the only state: the watermark, the roots that were
walked, and the subtrees that still await a walk with the path the first of
them reached. It holds page IDs, root revision hashes and one timestamp, never principals.

Notion has no change feed. `search`, newest edit first, hints at what
changed; it is neither immediate nor exhaustive, so it is read down to the
watermark less an overlap, and the walk stays the enumeration that counts.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from ..validate import MAX_CURSOR_BYTES
from .client import Client
from .tree import ident

#: How far below the watermark search is read again: its index lags.
OVERLAP = timedelta(hours=1)
#: Notion stamps an edit to the minute; a read this soon after the stamp may
#: miss a later edit that carries the same stamp.
SETTLE = timedelta(minutes=2)
#: Subtrees one rendered page may queue; beyond it the page itself is walked.
MAX_WALKS = 64
#: Levels of a walk's path the cursor keeps; a deeper walk continues from
#: its ancestor at this depth.
PATH_LEVELS = 100
_SEARCH = {
    "filter": {"property": "object", "value": "page"},
    "sort": {"direction": "descending", "timestamp": "last_edited_time"},
}


def instant(value: object) -> datetime | None:
    """A Notion timestamp; None for anything else."""
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        return None
    return parsed if parsed is not None and parsed.tzinfo is not None else None


def later(stamp: object, than: str) -> str:
    """The later of two Notion timestamps; `than` when `stamp` is none."""
    one, other = instant(stamp), instant(than)
    if isinstance(stamp, str) and one is not None and (other is None or one > other):
        return stamp
    return than


def settled(page: Mapping[str, Any], read_at: datetime) -> bool:
    """Whether no later edit can still carry the page's `last_edited_time`."""
    stamp = instant(page.get("last_edited_time"))
    return stamp is None or read_at - stamp >= SETTLE


@dataclass
class Cursor:
    #: The newest `last_edited_time` search had shown when the last pass
    #: began; empty when it showed nothing; None before the first pass.
    since: str | None = None
    #: The roots whose walk was queued; a root added later is walked then.
    roots: list[str] = field(default_factory=list)
    #: `(kind, ID)` of the subtrees still to walk, and how far the first got.
    walks: list[tuple[str, str]] = field(default_factory=list)
    path: list[str] = field(default_factory=list)
    root_revisions: dict[str, str] = field(default_factory=dict)

    @classmethod
    def decode(cls, text: str | None) -> Cursor:
        if text is None:
            return cls()
        try:
            if len(text.encode()) > MAX_CURSOR_BYTES:
                raise ValueError
            state = json.loads(text)
            if not isinstance(state, dict) or state.get("v") != 1:
                raise ValueError
            since = state["since"]
            if since is not None and (
                not isinstance(since, str) or (since and instant(since) is None)
            ):
                raise ValueError
            for key in ("roots", "walks", "path"):
                if not isinstance(state[key], list):
                    raise ValueError
            root_revisions = state["root_revisions"]
            if not isinstance(root_revisions, dict) or any(
                not isinstance(value, str)
                or len(value) != 32
                or any(char not in "0123456789abcdef" for char in value)
                for value in root_revisions.values()
            ):
                raise ValueError
            root_revisions = {ident(key): value for key, value in root_revisions.items()}
            walks = []
            for entry in state["walks"]:
                if (
                    not isinstance(entry, list)
                    or len(entry) != 2
                    or entry[0] not in ("page", "database")
                ):
                    raise ValueError
                walks.append((entry[0], ident(entry[1])))
            if len(state["path"]) > PATH_LEVELS:
                raise ValueError
            return cls(
                since=since,
                roots=[ident(root) for root in state["roots"]],
                walks=walks,
                path=[ident(node) for node in state["path"]],
                root_revisions=root_revisions,
            )
        except (ValueError, KeyError, TypeError):
            raise ValueError("notion_cursor_invalid") from None

    def encode(self) -> str:
        state = {
            "v": 1,
            "since": self.since,
            "roots": self.roots,
            "walks": self.walks,
            "path": self.path,
            "root_revisions": self.root_revisions,
        }
        text = json.dumps(state, separators=(",", ":"), sort_keys=True)
        if len(text) > MAX_CURSOR_BYTES:
            # Never a truncated list: a dropped root would never be walked.
            raise ValueError("notion_cursor_too_large")
        return text


def changed(client: Client, since: str | None) -> Iterator[list[Mapping[str, Any]]]:
    """What search shows of the pages edited since `since` less the overlap,
    newest first, answer by answer. Reading stops after an answer that holds
    nothing that new."""
    watermark = instant(since)
    bound = None if watermark is None else watermark - OVERLAP
    for results in client.pages("search", _SEARCH):
        fresh = []
        for page in results:
            stamp = instant(page.get("last_edited_time"))
            if page.get("object") == "page" and (bound is None or stamp is None or stamp >= bound):
                fresh.append(page)
        if results and not fresh:
            return
        yield fresh


def newest(client: Client) -> str:
    """The newest `last_edited_time` search shows; empty when it shows nothing."""
    head = ""
    for page in next(client.pages("search", _SEARCH), []):
        head = later(page.get("last_edited_time"), head)
    return head
