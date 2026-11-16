"""The Notion connector: pages and database rows under the configured root
pages as text.

Notion's API does not expose who a page is shared with, so a connection is
always `permissions = "fixed"`: every document carries the configured readers
and no permission or member is read.

Design, the reference review and the tenant items: docs/connectors/notion.md.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import replace
from typing import Any

from ..arrow import Delete, Metadata, Section, Upsert, document_changes
from ..connector import Connector, ContentBatch, ListingBatch
from ..validate import MAX_URI_BYTES
from .changes import (
    MAX_WALKS,
    PATH_LEVELS,
    SETTLE,
    Cursor,
    changed,
    earlier,
    instant,
    later,
    newest,
    settled,
)
from .client import ApiError, Client
from .config import Scope, Settings
from .render import InvalidText, Oversized, Text, clean, title
from .tree import Ref, Tree, ident, trashed

#: Bumped when the rendered text of an unchanged page would differ.
RENDER_FORMAT = "2"
#: The worker's revision lookup takes at most this many documents.
LOOKUP_ROWS = 256
#: Marks a document the worker must quarantine, as `local_files` does.
QUARANTINE = [Section(text="\0")]
_ROW_PARENTS = ("data_source_id", "database_id")


def _revision(page: Mapping[str, Any], mark: str) -> str:
    """Identity of what a read of `page` publishes. Notion stamps edits to
    the minute, so `mark` tells a read that may be followed by another edit
    with the same stamp (`unsettled`) from one that cannot."""
    parts = [RENDER_FORMAT, page.get("last_edited_time"), mark]
    return hashlib.sha256(repr(parts).encode()).hexdigest()[:32]


class Notion(Connector[Settings, Scope]):
    Settings = Settings
    Scope = Scope

    def _open(self) -> list[str]:
        """Connect and read every root page: the live roots, in their
        configured order. A root that is gone or not shared fails the run, so
        a wrong token or workspace never looks like an empty scope."""
        settings, scope = self.context.settings, self.context.scope
        assert isinstance(settings, Settings) and isinstance(scope, Scope)
        if self.context.permissions != "fixed":
            raise ValueError("notion_permissions_fixed_only")
        self._client = Client(settings.token.reveal(), settings.api_endpoint)
        self._tree = Tree(self._client)
        #: Published revisions as far as they were looked up, then as this run changes them.
        self._known: dict[str, str | None] = {}
        self._examined: dict[str, str] = {}
        self._view = self.context.lookup_revisions is not None
        self._projects: dict[str, str | None] = {}
        self._unsettled: str | None = None
        live = []
        for root in dict.fromkeys(ident(root) for root in scope.root_pages):
            ref = self._tree.page(root)
            if ref is None or ref.page is None:
                raise ValueError("notion_root_unreadable")
            self._projects[root] = title(ref.page)
            if not trashed(ref.page):
                live.append(root)
        self._tree.roots = frozenset(live)
        return live

    def _lookup(self, pages: Sequence[str]) -> None:
        """Learn the published and pending revisions of `pages`."""
        lookup = self.context.lookup_revisions
        unknown = [page for page in dict.fromkeys(pages) if page not in self._known]
        if lookup is None:
            return
        for start in range(0, len(unknown), LOOKUP_ROWS):
            part = unknown[start : start + LOOKUP_ROWS]
            found = lookup(part)
            self._known.update({page: found.get(page) for page in part})

    def _author(self, page: Mapping[str, Any]) -> str | None:
        """The name of the page's creator; None when Notion will not tell."""
        user = page.get("created_by")
        return clean(user.get("name") or "") if isinstance(user, Mapping) else None

    def _upsert(
        self,
        page: Mapping[str, Any],
        page_id: str,
        root: str,
        revision: str,
        sections: list[Section],
    ) -> Upsert:
        parent = page.get("parent")
        parent = parent if isinstance(parent, Mapping) else {}
        database = parent.get("database_id")
        return Upsert(
            root_id=page_id,
            source_revision=revision,
            metadata=Metadata(
                source_uri=clean(page.get("url") or "", MAX_URI_BYTES)
                or f"https://www.notion.so/{page_id.replace('-', '')}",
                source_type="notion-row" if parent.get("type") in _ROW_PARENTS else "notion-page",
                title=title(page),
                author=self._author(page),
                project=self._projects.get(root),
                created_at=instant(page.get("created_time")),
                updated_at=instant(page.get("last_edited_time")),
                connector_map={
                    "page_id": page_id,
                    "root_page_id": root,
                    "database_id": database if isinstance(database, str) else None,
                },
            ),
            sections=sections,
            readers=self.context.readers,
        )

    def _examine(
        self, ref: Ref, root: str, descend: bool
    ) -> tuple[Upsert | Delete | None, list[Ref]]:
        """The change the page asks for, and the pages and databases beneath
        it. A page whose published revision is current is read only to
        `descend`. A delete is due for what is published or was handed over
        earlier in this run; without the worker's revisions, for any trashed page."""
        page = ref.page
        assert page is not None and ref.at is not None
        known = self._known.get(ref.id)
        if trashed(page):
            removed = Delete(root_id=ref.id, source_revision=_revision(page, "trashed"))
            return (removed if known is not None or not self._view else None), []
        is_settled = settled(page, ref.at)
        stamp = page.get("last_edited_time")
        if not is_settled:
            self._unsettled = earlier(stamp, self._unsettled)
        revision = _revision(page, "settled" if is_settled else "unsettled")
        self._examined[ref.id] = revision
        if ref.id in self._tree.roots:
            self._root_revisions[ref.id] = revision
        current = known == revision
        if current and not descend:
            return None, []
        text = None if current else Text(self.context.limits.document_max_bytes)
        if text is not None and _parent_type(page) in _ROW_PARENTS:
            text.properties(page.get("properties"))
        try:
            children = self._tree.body(ref.id, text)
        except ApiError as error:
            if error.missing:
                # It vanished since it was read; the full-ID listing judges that.
                return None, []
            if not error.refused:
                raise
            # A block list Notion will not serve: a bad document, not a failed sync.
            bad = None if text is None else self._upsert(page, ref.id, root, revision, QUARANTINE)
            return bad, []
        if text is None:
            return None, children
        try:
            sections = text.sections()
        except (Oversized, InvalidText):
            sections = QUARANTINE
        if sections:
            return self._upsert(page, ref.id, root, revision, sections), children
        # Nothing to index: a page without text is not published, an emptied one leaves.
        emptied = Delete(root_id=ref.id, source_revision=_revision(page, "empty"))
        return (emptied if known is not None else None), children

    def _done(self, ref: Ref, batch: tuple[str, ...]) -> bool:
        """Whether a row a continued walk passes over was handed over before."""
        self._lookup(batch)
        assert ref.page is not None and ref.at is not None
        # An unsettled row may have gained a descendant in the same minute.
        # Its published revision does not attest that its subtree is finished.
        return settled(ref.page, ref.at) and self._known.get(ref.id) == _revision(
            ref.page, "settled"
        )

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        live = self._open()
        limits = self.context.limits
        state = Cursor.decode(cursor)
        state.root_revisions = {
            key: value for key, value in state.root_revisions.items() if key in live
        }
        self._root_revisions = state.root_revisions
        # A root that was not walked yet: the first sync, or a root added since.
        state.walks += [("page", root) for root in live if root not in state.roots]
        state.roots = live
        if state.since is None:
            # Before the first walk, so no edit made during it is missed.
            state.since = newest(self._client)
        # A batch carries the walk's position as of its last page. The
        # watermark moves only with the last batch of the pass: after a crash
        # search is read again and the revision lookup skips what is durable.
        durable, sent = state.encode(), cursor
        pending: dict[str, Upsert | Delete] = {}
        touched: set[str] = set()
        size = 0

        def flush() -> ContentBatch:
            nonlocal size, sent
            batch = ContentBatch(changes=document_changes(pending.values()), cursor=durable)
            pending.clear()
            size, sent = 0, durable
            return batch

        def hand(change: Upsert | Delete) -> Iterator[ContentBatch]:
            nonlocal size
            # Include located-section metadata and Arrow offsets in the bound.
            weight = document_changes([change]).get_total_buffer_size()
            if weight > limits.max_bytes and isinstance(change, Upsert):
                change = replace(change, sections=QUARANTINE)
                weight = document_changes([change]).get_total_buffer_size()
            if weight > limits.max_bytes:
                raise ValueError("notion_batch_limit")
            previous = pending.get(change.root_id)
            old_weight = document_changes([previous]).get_total_buffer_size() if previous else 0
            if pending and (
                (previous is None and len(pending) >= limits.max_rows)
                or size - old_weight + weight > limits.max_bytes
            ):
                yield flush()
                old_weight = 0
            # A later change of a document replaces an earlier one of this batch.
            pending[change.root_id] = change
            touched.add(change.root_id)
            size += weight - old_weight
            self._known[change.root_id] = (
                change.source_revision if isinstance(change, Upsert) else None
            )

        def walks() -> Iterator[ContentBatch]:
            nonlocal durable
            while state.walks:
                kind, start = state.walks[0]
                endpoint = "pages" if kind == "page" else "databases"
                # None: it left the roots while it waited; the listing removes it.
                root = self._tree.root_of(start, endpoint)
                visits = self._tree.walk(Ref(kind, start), state.path, self._done) if root else ()
                for visit in visits:
                    self._lookup(visit.batch)
                    ref = visit.ref if visit.ref.page is not None else self._tree.page(visit.ref.id)
                    actual_root = self._tree.scope(ref) if ref is not None else None
                    if ref is not None and actual_root is not None:
                        change, visit.children = self._examine(ref, actual_root, descend=True)
                        if change is not None:
                            yield from hand(change)
                    state.path = list(visit.path[:PATH_LEVELS])
                    state.since = earlier(self._unsettled, state.since)
                    durable = state.encode()
                del state.walks[0]
                state.path = []
                durable = state.encode()

        yield from walks()
        head = state.since
        for hints in changed(self._client, state.since):
            self._lookup([ident(hint.get("id")) for hint in hints])
            for hint in hints:
                page_id = ident(hint.get("id"))
                head = later(hint.get("last_edited_time"), head)
                # Refresh a parent looked up for an earlier candidate even
                # when its hint has the already-published revision. A second
                # hint of a page changed in this run may contradict the first.
                if (
                    not trashed(hint)
                    and (
                        self._known.get(page_id) == _revision(hint, "settled")
                        or self._root_revisions.get(page_id) == _revision(hint, "settled")
                        or self._examined.get(page_id) == _revision(hint, "settled")
                    )
                    and page_id not in self._tree.ancestors
                    and page_id not in touched
                ):
                    continue
                # Search can be behind: the page is read again by ID.
                try:
                    ref = self._tree.page(page_id)
                except ApiError as error:
                    if (
                        error.transient
                        or error.status != 403
                        or page_id not in self._tree.ancestors
                    ):
                        raise
                    continue
                if ref is None or ref.page is None:
                    continue
                self._tree.learn(ref.page)
                root = self._tree.root_of(page_id)
                if root is None and not trashed(ref.page):
                    # Outside the roots. One that left them leaves at the listing.
                    continue
                change, children = self._examine(ref, root or "", descend=False)
                if change is not None:
                    yield from hand(change)
                queued = [(child.kind, child.id) for child in children]
                if queued:
                    # With the page's own change, so a crash loses neither.
                    state.walks = queued if len(queued) <= MAX_WALKS else [("page", page_id)]
                    durable = state.encode()
                    yield from walks()
        state.since = earlier(self._unsettled, head)
        durable = state.encode()
        if pending or durable != sent:
            yield flush()

    def listing(self, cursor: str | None) -> Iterator[ListingBatch]:
        live = self._open()
        started = self._client.server_time - SETTLE
        listing_id = hashlib.sha256(f"{cursor}:{time.time_ns()}".encode()).hexdigest()
        rows = min(LOOKUP_ROWS, self.context.limits.max_rows)
        roots: list[str] = []
        included: set[str] = set()
        part = 0

        def scan(start: str, visited: set[str]) -> Iterator[str]:
            for visit in self._tree.walk(Ref("page", start), visited=visited):
                ref = visit.ref if visit.ref.page is not None else self._tree.page(visit.ref.id)
                if ref is None or self._tree.scope(ref) is None:
                    continue
                try:
                    visit.children = self._tree.body(ref.id, None)
                except ApiError as error:
                    if not error.missing:
                        raise
                    continue
                if ref.id not in included:
                    included.add(ref.id)
                    yield ref.id

        def append(ids: Iterator[str]) -> Iterator[ListingBatch]:
            nonlocal part
            for page_id in ids:
                roots.append(page_id)
                if len(roots) > rows:
                    yield ListingBatch(
                        listing_id=listing_id,
                        part=part,
                        last=False,
                        complete=False,
                        root_ids=roots[:rows],
                        cursor=f"{listing_id}:{part}",
                    )
                    del roots[:rows]
                    part += 1

        visited: set[str] = set()
        for root in live:
            yield from append(scan(root, visited))
        # Ascending updates put a page edited again ahead of the scan, not
        # behind it. Search is only a hint, so it cannot be the deletion guard.
        converged = False
        query = {
            "filter": {"property": "object", "value": "page"},
            "sort": {"direction": "ascending", "timestamp": "last_edited_time"},
        }
        for _ in range(4):
            before = len(included)
            for hints in self._client.pages("search", query):
                for hint in hints:
                    if hint.get("object") != "page":
                        raise ValueError("notion_response_invalid")
                    stamp = instant(hint.get("last_edited_time"))
                    if stamp is None:
                        raise ValueError("notion_response_invalid")
                    if stamp < started:
                        continue
                    self._tree.refresh()
                    yield from append(scan(ident(hint.get("id")), set()))
            if len(included) == before:
                converged = True
                break
        # Search can lag, and moves may not stamp the page on a real tenant:
        # check every published/pending root absent from the traversal afresh.
        # Without a revision view, never attest a deletion-safe listing.
        next_revisions = self.context.next_revisions
        if next_revisions is not None:
            while revisions := next_revisions():
                for page_id, _ in revisions:
                    if page_id not in included:
                        self._tree.refresh()
                        yield from append(scan(ident(page_id), set()))
        yield ListingBatch(
            listing_id=listing_id,
            part=part,
            last=True,
            complete=converged and next_revisions is not None,
            root_ids=roots,
            cursor=f"{listing_id}:{part}",
        )


def _parent_type(page: Mapping[str, Any]) -> object:
    parent = page.get("parent")
    return parent.get("type") if isinstance(parent, Mapping) else None
