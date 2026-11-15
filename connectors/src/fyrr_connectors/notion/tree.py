"""Reading a Notion tree: a page's blocks and what lies beneath it, a
database's rows, the way up from a page to a root, and the depth-first walk
that is the only complete enumeration Notion offers.

Nothing here turns a failed read into an empty one. Only Notion's 404 (the
object is gone or not shared with the integration) means "not ours"; every
other error is raised, so a walk either reaches its end or fails.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import Any

from .client import ApiError, Client
from .render import NESTING, Text

#: A parent chain longer than this is treated as out of scope.
MAX_DEPTH = 100
MAX_NODES = 100_000
#: Parent type -> the endpoint that returns the parent.
_PARENTS = {
    "page_id": "pages",
    "database_id": "databases",
    "data_source_id": "data_sources",
    "block_id": "blocks",
}
_OLDEST_FIRST = {"sorts": [{"timestamp": "created_time", "direction": "ascending"}]}


def ident(value: object) -> str:
    """The canonical (dashed) form of a Notion ID. Nothing else reaches a
    request path, a document ID or the cursor."""
    try:
        return str(uuid.UUID(str(value)))
    except ValueError:
        raise ValueError("notion_id_invalid") from None


def trashed(resource: Mapping[str, Any]) -> bool:
    return resource.get("in_trash") is True or resource.get("archived") is True


def _parent(resource: Mapping[str, Any]) -> tuple[str, str] | None:
    """Where `resource` hangs: the endpoint and ID of its parent; None at the
    top of the workspace and in the trash."""
    parent = resource.get("parent")
    if trashed(resource) or not isinstance(parent, Mapping):
        return None
    kind = parent.get("type")
    if kind == "data_source_id" and parent.get("database_id"):
        kind = "database_id"
    return (_PARENTS[kind], ident(parent.get(kind))) if kind in _PARENTS else None


@dataclass(frozen=True)
class Ref:
    """A page or a database; for a page Notion already answered with, the
    page and Notion's clock at that answer."""

    kind: str
    id: str
    page: Mapping[str, Any] | None = None
    at: datetime | None = None


@dataclass
class Visit:
    """One page of a walk. The consumer sets `children` before it takes the
    next visit; left empty, nothing beneath the page is visited."""

    path: tuple[str, ...]
    ref: Ref
    #: The IDs that arrived together with this one: one revision lookup.
    batch: tuple[str, ...]
    children: Sequence[Ref] = ()


@dataclass
class _Level:
    """The children of one node, list by list, and where in them the walk is."""

    open: Callable[[], Iterator[list[Ref]]]
    path: tuple[str, ...]
    #: The child a continued walk passes the others over to reach.
    target: str | None
    lists: Iterator[list[Ref]] = field(init=False)
    left: list[Ref] = field(default_factory=list)
    batch: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        self.lists = self.open()


class Tree:
    def __init__(self, client: Client) -> None:
        self._client = client
        #: The root pages that exist and are not trashed.
        self.roots: frozenset[str] = frozenset()
        self._parents: dict[str, tuple[str, str] | None] = {}
        self.ancestors: set[str] = set()
        self._nodes = 0

    def _take(self) -> None:
        self._nodes += 1
        if self._nodes > MAX_NODES:
            raise ValueError("notion_tree_limit")

    def page(self, page_id: str) -> Ref | None:
        """The page as Notion has it now; None when it is gone or not shared."""
        try:
            page = self._client.get(f"pages/{page_id}")
        except ApiError as error:
            if not error.missing:
                raise
            return None
        if page.get("object") != "page" or ident(page.get("id")) != page_id:
            raise ValueError("notion_response_invalid")
        return Ref("page", page_id, page, self._client.server_time)

    def _blocks(self, parent: str) -> Iterator[Mapping[str, Any]]:
        for blocks in self._client.pages(f"blocks/{parent}/children"):
            yield from blocks

    def body(self, page_id: str, text: Text | None) -> list[Ref]:
        """Read the page's blocks to the bottom: their text goes to `text`,
        and the pages and databases beneath the page are returned in document
        order. A nested block list that is gone is left out; the page's own
        list, and any other failure, raises."""
        children: list[Ref] = []
        seen = {page_id}
        levels = [(self._blocks(page_id), 0, True)]
        while levels:
            blocks, indent, own = levels[-1]
            try:
                block = next(blocks, None)
            except ApiError as error:
                if len(levels) == 1 or not error.missing:
                    raise
                block = None
            if block is None:
                levels.pop()
                continue
            self._take()
            if block.get("object") != "block" or not isinstance(block.get("type"), str):
                raise ValueError("notion_response_invalid")
            kind = block.get("type")
            if trashed(block):
                continue
            block_id = ident(block.get("id"))
            if block_id in seen:
                continue
            seen.add(block_id)
            if kind in ("child_page", "child_database"):
                # Its blocks are its own document, never this page's text.
                if own:
                    children.append(Ref(kind.removeprefix("child_"), block_id))
                    self._parents.setdefault(block_id, ("pages", page_id))
                continue
            if text is not None:
                text.block(indent, block)
            if block.get("has_children") is True:
                data = block.get(kind) if isinstance(kind, str) else None
                # A synced copy shows the original's blocks; what lies beneath
                # them belongs to the original's page. A table holds only cells.
                copy = isinstance(data, Mapping) and data.get("synced_from") is not None
                levels.append(
                    (
                        self._blocks(block_id),
                        indent + (kind in NESTING),
                        own and kind != "table" and not copy,
                    )
                )
        return children

    def rows(self, database_id: str) -> Iterator[list[Ref]]:
        """The rows of a database, one list per answer, oldest first, to the
        end or an error. A database that is gone, trashed or only a linked
        view of one elsewhere has none."""
        try:
            database = self._client.get(f"databases/{database_id}")
        except ApiError as error:
            if not error.missing:
                raise
            return
        if database.get("object") != "database" or ident(database.get("id")) != database_id:
            raise ValueError("notion_response_invalid")
        sources = database.get("data_sources")
        if trashed(database):
            return
        if not isinstance(sources, list):
            raise ValueError("notion_response_invalid")
        for source in sources:
            self._take()
            if not isinstance(source, Mapping):
                raise ValueError("notion_response_invalid")
            try:
                query = f"data_sources/{ident(source.get('id'))}/query"
                for rows in self._client.pages(query, _OLDEST_FIRST):
                    at = self._client.server_time
                    for _ in rows:
                        self._take()
                    if any(row.get("object") != "page" for row in rows):
                        raise ValueError("notion_response_invalid")
                    yield [
                        Ref("page", ident(row.get("id")), row, at)
                        for row in rows
                        if row.get("object") == "page" and not trashed(row)
                    ]
            except ApiError as error:
                if not error.missing:
                    raise

    def learn(self, page: Mapping[str, Any]) -> None:
        """Take a page's parent from its current resource: it is newer than
        what an earlier look at that page gave."""
        self._parents[ident(page.get("id"))] = _parent(page)

    def refresh(self) -> None:
        """Catch-up/negative checks must not reuse a pre-move parent chain."""
        self._parents.clear()

    def scope(self, ref: Ref) -> str | None:
        """Validate the fetched parent against the one that queued the page."""
        assert ref.page is not None
        actual = _parent(ref.page)
        if actual != self._parents.get(ref.id):
            self.refresh()
        self.learn(ref.page)
        return None if trashed(ref.page) else self.root_of(ref.id)

    def root_of(self, node: str, endpoint: str = "pages") -> str | None:
        """The root page `node` lies under (itself, if it is one). None when
        its way up leaves the roots, meets something trashed, gone or not
        shared, or is too long. A parent is looked up once per run."""
        for _ in range(MAX_DEPTH):
            if node in self.roots:
                return node
            if node not in self._parents:
                self.ancestors.add(node)
                self._take()
                try:
                    self._parents[node] = _parent(self._client.get(f"{endpoint}/{node}"))
                except ApiError as error:
                    if not error.missing and (error.transient or error.status != 403):
                        raise
                    self._parents[node] = None
            parent = self._parents[node]
            if parent is None:
                return None
            endpoint, node = parent
        return None

    def walk(
        self,
        start: Ref,
        resume: Sequence[str] = (),
        done: Callable[[Ref, tuple[str, ...]], bool] = lambda _ref, _batch: True,
        visited: set[str] | None = None,
    ) -> Iterator[Visit]:
        """Depth-first from `start`, a page before what lies beneath it; a
        database is passed through to its rows. `resume` is the path of the
        last visit of a walk that stopped: at each level the entries before
        it are passed over, and an entry that vanished starts its level
        again. Rows created in one minute have no promised order, so a row
        before the entry is passed over only if it is `done`. A page is
        visited once per walk. A listing shares `visited` across roots;
        content walks start afresh so a later change can add descendants."""
        visited = set() if visited is None else visited
        levels = [_Level(lambda: iter([[start]]), (), resume[0] if resume else None)]
        while levels:
            level = levels[-1]
            if not level.left:
                refs = next(level.lists, None)
                if refs is not None:
                    level.left = refs[::-1]
                    level.batch = tuple(ref.id for ref in refs if ref.kind == "page")
                elif level.target is not None:
                    level.target = None
                    level.lists = level.open()
                else:
                    levels.pop()
                continue
            ref = level.left.pop()
            self._take()
            path = (*level.path, ref.id)
            below = None
            if level.target is None:
                pass
            elif ref.id == level.target:
                level.target = None
                below = resume[len(path)] if len(path) < len(resume) else None
            elif ref.page is None or done(ref, level.batch):
                continue
            if ref.id in visited:
                continue
            visited.add(ref.id)
            if ref.kind == "database":
                levels.append(_Level(partial(self.rows, ref.id), path, below))
                continue
            visit = Visit(path, ref, level.batch)
            yield visit
            children = list(visit.children)
            if children:
                levels.append(_Level(partial(iter, [children]), path, below))
