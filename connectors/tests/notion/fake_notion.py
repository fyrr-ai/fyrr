"""A stateful, synthetic Notion workspace for the connector's tests.

It keeps pages with their block trees, databases with data sources and rows,
users, a search index that can lag behind the workspace, and a clock that
stamps every change to the minute, and answers the Notion REST calls the
connector makes through a `requests` transport adapter, so the connector's
real URL building, paging and parsing run. Lists hold two entries per answer,
the token and the `Notion-Version` header are checked, any request can be
made to fail, and every request is recorded. Things are created by name;
`ident(name)` is the ID. All names, texts and IDs are invented.

`FakeServer` puts the same world behind HTTP for a connector in another
process (`fyrr writer sync`); `python fake_notion.py` runs it for the
real-S3 tests.
"""

from __future__ import annotations

import json
import re
import sys
import threading
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import requests

# Run as a script, the fake finds `fake_http` next to the test packages.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fake_http import CONTROL_PATH, Holds, JsonHandler, Serving, WorldAdapter

VERSION = "2025-09-03"
TOKEN = "fake-integration-secret"
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
_NAMESPACE = uuid.UUID("6f1d2c1e-0000-4000-8000-000000000000")
_MEDIA = frozenset({"image", "video", "file", "pdf", "audio"})
_LINKS = frozenset({"bookmark", "embed", "link_preview"})
_BARE = frozenset({"divider", "column_list", "column", "table", "unsupported", "synced_block"})
#: World changes `POST /fake/<name>` passes straight on, arguments by name.
_WORLD_CALLS = frozenset(
    {
        "user",
        "page",
        "database",
        "row",
        "move",
        "trash",
        "restore",
        "remove",
        "unshare",
        "share",
        "scribble",
        "reindex",
    }
)
_SETTINGS = frozenset({"lag", "page_size", "user_capability", "search_trashed"})

Match = Callable[[str, dict[str, Any]], bool]


def ident(name: str) -> str:
    """The ID of the thing created as `name` (a block: `<page>/<block name>`)."""
    return str(uuid.uuid5(_NAMESPACE, name))


def rich(text: str) -> list[dict[str, Any]]:
    if not text:
        return []
    return [{"type": "text", "text": {"content": text}, "plain_text": text, "href": None}]


def block(kind: str, text: str = "", *children: dict[str, Any], name: str = "", **data: Any):
    """A block to put on a page: its type, its text and the blocks inside it."""
    return {"kind": kind, "text": text, "children": list(children), "name": name, "data": data}


def _error(status: int, code: str) -> tuple[int, dict[str, Any]]:
    # The message must never reach a log or an exception of the connector.
    message = "fake-notion-message-that-must-not-leak"
    return status, {"object": "error", "status": status, "code": code, "message": message}


class FakeNotion:
    def __init__(self, page_size: int = 2) -> None:
        self.page_size = page_size
        self.token = TOKEN
        self.pages: dict[str, dict[str, Any]] = {}
        self.databases: dict[str, dict[str, Any]] = {}
        self.sources: dict[str, dict[str, Any]] = {}
        self.users: dict[str, str] = {}
        #: The name each ID was created as; kept when the thing is removed.
        self.names: dict[str, str] = {}
        self.unshared: set[str] = set()
        #: Search answers from its index; with `lag` the index stops following the workspace.
        self.lag = False
        self.hidden: set[str] = set()
        self.search_trashed = True
        self.user_capability = True
        self.inline_authors = True
        #: How far Notion's clock is past the last change; small means "this very minute".
        self.lead = timedelta(minutes=10)
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        self.sleeps: list[float] = []
        self._index: dict[str, dict[str, Any]] = {}
        self._blocks: dict[str, dict[str, Any]] = {}
        self._faults: list[dict[str, Any]] = []
        self._after: list[tuple[Match, Callable[[], None]]] = []
        self._clock = 0
        self._lock = threading.Lock()

    # The world.

    def _stamp(self) -> str:
        return (EPOCH + timedelta(minutes=self._clock)).strftime("%Y-%m-%dT%H:%M:00.000Z")

    def _edited(self, page_id: str) -> None:
        """Stamp a change of the page with a new minute and let search see it."""
        self._clock += 1
        self.pages[page_id]["edited"] = self._stamp()
        if not self.lag:
            self._index[page_id] = self._page(page_id)

    def reindex(self) -> None:
        """Search catches up with the workspace."""
        self._index = {page_id: self._page(page_id) for page_id in self.pages}

    def user(self, name: str, display: str) -> None:
        self.users[ident(name)] = display

    def _place(self, parent: str | None, under: str) -> tuple[str, str | None]:
        if under:
            return "block", ident(f"{parent}/{under}")
        if parent is None:
            return "workspace", None
        return ("database" if ident(parent) in self.databases else "page"), ident(parent)

    def _bump(self, place: tuple[str, str | None]) -> None:
        """A page gains or loses something beneath it: its own blocks changed."""
        kind, node = place
        if kind == "block":
            node = self._blocks[node]["page"]
        if node in self.pages:
            self._edited(node)

    def page(
        self,
        name: str,
        parent: str | None = None,
        *,
        title: str | None = None,
        blocks: list[dict[str, Any]] | None = None,
        under: str = "",
        author: str = "ada",
        properties: dict[str, Any] | None = None,
    ) -> str:
        """Create the page, or change what is given of an existing one."""
        page_id = ident(name)
        self.names[page_id] = name
        if page_id not in self.pages:
            self._clock += 1
            self.pages[page_id] = {
                "name": name,
                "parent": self._place(parent, under),
                "title": name if title is None else title,
                "properties": {},
                "blocks": [],
                "created": self._stamp(),
                "author": ident(author),
                "trashed": False,
            }
            self._bump(self.pages[page_id]["parent"])
        page = self.pages[page_id]
        if title is not None:
            page["title"] = title
        if properties is not None:
            page["properties"] = properties
        if blocks is not None:
            self._write(name, blocks)
        self._edited(page_id)
        return page_id

    def _write(self, name: str, blocks: list[dict[str, Any]]) -> None:
        page_id = ident(name)
        self._blocks = {k: b for k, b in self._blocks.items() if b["page"] != page_id}
        self.pages[page_id]["blocks"] = self._build(name, page_id, ("page", page_id), blocks, "")

    def scribble(self, name: str, blocks: list[dict[str, Any]]) -> None:
        """Another edit within the minute of the last one: new blocks, the same stamp."""
        self._write(name, blocks)
        if not self.lag:
            self._index[ident(name)] = self._page(ident(name))

    def _build(
        self,
        page: str,
        page_id: str,
        parent: tuple[str, str],
        specs: list[dict[str, Any]],
        prefix: str,
    ) -> list[str]:
        built = []
        for number, spec in enumerate(specs):
            block_id = ident(f"{page}/{spec.get('name') or prefix + str(number)}")
            record = {
                "kind": spec["kind"],
                "text": spec.get("text", ""),
                "data": dict(spec.get("data") or {}),
                "parent": parent,
                "page": page_id,
            }
            self._blocks[block_id] = record
            record["children"] = self._build(
                page, page_id, ("block", block_id), spec.get("children") or [], f"{number}."
            )
            built.append(block_id)
        return built

    def database(
        self, name: str, parent: str, *, title: str = "", sources: int = 1, under: str = ""
    ) -> str:
        database_id = ident(name)
        self.names[database_id] = name
        self._clock += 1
        made = [ident(f"{name}#{number}") for number in range(sources)]
        self.databases[database_id] = {
            "name": name,
            "parent": self._place(parent, under),
            "title": title or name,
            "sources": made,
            "trashed": False,
        }
        for source in made:
            self.sources[source] = {"database": database_id, "rows": []}
        self._bump(self.databases[database_id]["parent"])
        return database_id

    def row(self, name: str, database: str, *, source: int = 0, **page: Any) -> str:
        """A row of the database: a page whose parent is one of its data sources."""
        source_id = self.databases[ident(database)]["sources"][source]
        new = ident(name) not in self.pages
        page_id = self.page(name, **page)
        if new:
            self.pages[page_id]["parent"] = ("source", source_id)
            self.sources[source_id]["rows"].append(page_id)
            self._index.pop(page_id, None)
            self._edited(page_id)
        return page_id

    def move(self, name: str, parent: str | None = None, under: str = "") -> None:
        """Hang a page or a database somewhere else. Only the moved thing and
        its new parent report a change; nothing beneath it does."""
        node = ident(name)
        place = self._place(parent, under)
        if node in self.databases:
            self.databases[node]["parent"] = place
        else:
            self.pages[node]["parent"] = place
            self._edited(node)
        self._bump(place)

    def trash(self, name: str) -> None:
        self.pages[ident(name)]["trashed"] = True
        self._edited(ident(name))

    def restore(self, name: str) -> None:
        self.pages[ident(name)]["trashed"] = False
        self._edited(ident(name))

    def remove(self, name: str) -> None:
        """Delete for good: nothing reports it, the page just answers 404."""
        page = self.pages.pop(ident(name))
        self._index.pop(ident(name), None)
        if page["parent"][0] == "source":
            self.sources[page["parent"][1]]["rows"].remove(ident(name))

    def unshare(self, name: str) -> None:
        """Withdraw the page or database, and all beneath it, from the integration."""
        self.unshared.add(ident(name))

    def share(self, name: str) -> None:
        self.unshared.discard(ident(name))

    def fail(
        self,
        match: Match,
        status: int,
        code: str = "",
        times: int = 1,
        retry_after: str | None = None,
    ) -> None:
        self._faults.append({"match": match, "n": times, "response": (status, code, retry_after)})

    def after(self, match: Match, change: Callable[[], None]) -> None:
        """Change the world once, right after the first matching request was
        answered: what happens in Notion while a sync is running."""
        self._after.append((match, change))

    def paths(self) -> list[str]:
        return [path for _, path, _ in self.requests]

    def count(self, pattern: str) -> int:
        return sum(1 for path in self.paths() if re.search(pattern, path))

    # The API.

    def attach(self, session: requests.Session) -> requests.Session:
        """Answer the session's requests from this world instead of the network."""
        adapter = WorldAdapter(self.answer_request)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def now(self) -> datetime:
        return EPOCH + timedelta(minutes=self._clock) + self.lead

    def _visible(self, kind: str, node: str | None) -> bool:
        """Whether the integration can reach the thing: it exists, and neither
        it nor anything above it was withdrawn."""
        for _ in range(100_000):
            if kind == "workspace":
                return True
            if node in self.unshared:
                return False
            if kind == "page":
                holder = self.pages.get(node)
            elif kind == "database":
                holder = self.databases.get(node)
            elif kind == "source":
                holder = self.sources.get(node)
                holder = holder and {"parent": ("database", holder["database"])}
            else:
                holder = self._blocks.get(node)
            if holder is None:
                return False
            kind, node = holder["parent"]
        return False

    @staticmethod
    def _parent(place: tuple[str, str | None], database: str | None = None) -> dict[str, Any]:
        kind, node = place
        if kind == "workspace":
            return {"type": "workspace", "workspace": True}
        if kind == "source":
            return {"type": "data_source_id", "data_source_id": node, "database_id": database}
        return {"type": f"{kind}_id", f"{kind}_id": node}

    def _page(self, page_id: str) -> dict[str, Any]:
        page = self.pages[page_id]
        source = self.sources.get(page["parent"][1]) if page["parent"][0] == "source" else None
        author = {"object": "user", "id": page["author"]}
        if self.inline_authors and self.user_capability and page["author"] in self.users:
            author["name"] = self.users[page["author"]]
        return {
            "object": "page",
            "id": page_id,
            "created_time": page["created"],
            "last_edited_time": page["edited"],
            "created_by": author,
            "last_edited_by": author,
            "archived": page["trashed"],
            "in_trash": page["trashed"],
            "parent": self._parent(page["parent"], source and source["database"]),
            "properties": {
                "Name": {"id": "title", "type": "title", "title": rich(page["title"])},
                **json.loads(json.dumps(page["properties"])),
            },
            "url": f"https://www.notion.so/{page['name']}-{page_id.replace('-', '')}",
        }

    def _beneath(self, kind: str, node: str) -> list[dict[str, Any]]:
        """The child-page and child-database blocks that hang at `node`."""
        found = []
        for page_id, page in self.pages.items():
            if page["parent"] == (kind, node) and not page["trashed"]:
                found.append(
                    {
                        "object": "block",
                        "id": page_id,
                        "type": "child_page",
                        "has_children": bool(page["blocks"]),
                        "in_trash": False,
                        "child_page": {"title": page["title"]},
                    }
                )
        for database_id, database in self.databases.items():
            if database["parent"] == (kind, node) and not database["trashed"]:
                found.append(
                    {
                        "object": "block",
                        "id": database_id,
                        "type": "child_database",
                        "has_children": False,
                        "in_trash": False,
                        "child_database": {"title": database["title"]},
                    }
                )
        return found

    def _block(self, block_id: str) -> dict[str, Any]:
        record = self._blocks[block_id]
        kind, text, data = record["kind"], record["text"], record["data"]
        if kind == "table_row":
            payload = {"cells": [rich(cell) for cell in data["cells"]]}
        elif kind in _LINKS:
            payload = {"url": data["url"], "caption": rich(text)}
        elif kind in _MEDIA:
            payload = {"caption": rich(text), "type": "external", "external": {"url": "x://y"}}
        elif kind == "equation":
            payload = {"expression": text}
        elif kind == "synced_block":
            origin = data.get("synced_from")
            payload = {"synced_from": origin and {"type": "block_id", "block_id": ident(origin)}}
        elif kind in _BARE:
            payload = dict(data)
        else:
            payload = {"rich_text": rich(text), **data}
        return {
            "object": "block",
            "id": block_id,
            "parent": self._parent(record["parent"]),
            "type": kind,
            "has_children": self._holds(block_id),
            "archived": False,
            "in_trash": False,
            kind: payload,
        }

    def _origin(self, block_id: str) -> str:
        """The block whose children `block_id` shows: a synced copy shows its original's."""
        record = self._blocks[block_id]
        origin = record["data"].get("synced_from") if record["kind"] == "synced_block" else ""
        return ident(origin) if origin else block_id

    def _holds(self, block_id: str) -> bool:
        block_id = self._origin(block_id)
        return bool(self._blocks[block_id]["children"] or self._beneath("block", block_id))

    def _children(self, node: str) -> list[dict[str, Any]]:
        if node in self.pages:
            own, kind = self.pages[node]["blocks"], "page"
        else:
            node = self._origin(node)
            own, kind = self._blocks[node]["children"], "block"
        return [self._block(block_id) for block_id in own] + self._beneath(kind, node)

    def _listed(self, items: list[Any], arguments: dict[str, Any]) -> dict[str, Any]:
        start = int(arguments.get("start_cursor") or 0)
        size = min(self.page_size, int(arguments.get("page_size") or self.page_size))
        more = start + size < len(items)
        return {
            "object": "list",
            "results": items[start : start + size],
            "has_more": more,
            "next_cursor": str(start + size) if more else None,
        }

    def answer_request(
        self, request: requests.PreparedRequest
    ) -> tuple[int, dict[str, Any], dict[str, str]]:
        url = urlsplit(request.url)
        arguments: dict[str, Any] = {k: v[0] for k, v in parse_qs(url.query).items()}
        if request.body:
            arguments = json.loads(request.body)
        return self.handle(request.method or "", url.path, arguments, request.headers)

    def handle(
        self, method: str, path: str, arguments: dict[str, Any], headers: Mapping[str, str]
    ) -> tuple[int, dict[str, Any], dict[str, str]]:
        """Answer one request: status, body and response headers."""
        with self._lock:
            sent = {"Date": format_datetime(self.now(), usegmt=True)}
            if headers.get("Authorization") != f"Bearer {self.token}":
                return (*_error(401, "unauthorized"), sent)
            self.requests.append((method, path, arguments))
            if headers.get("Notion-Version") != VERSION:
                return (*_error(400, "missing_version"), sent)
            for fault in self._faults:
                if fault["n"] and fault["match"](path, arguments):
                    fault["n"] -= 1
                    status, code, retry_after = fault["response"]
                    if retry_after is not None:
                        sent["Retry-After"] = retry_after
                    return (*_error(status, code), sent)
            answer = self._answer(method, path, arguments)
            for hook in [hook for hook in self._after if hook[0](path, arguments)]:
                self._after.remove(hook)
                hook[1]()
            return (*answer, sent)

    def _answer(self, method: str, path: str, arguments: dict[str, Any]) -> tuple[int, Any]:
        route = re.fullmatch(r"/v1/([a-z_]+)(?:/([^/]+))?(?:/([a-z]+))?", path)
        if route is None:
            return _error(400, "invalid_request_url")
        resource, node, action = route.groups()
        call = (method, resource, action)
        if call == ("POST", "search", None) and node is None:
            return 200, self._search(arguments)
        if call == ("GET", "pages", None) and node:
            if node not in self.pages or not self._visible("page", node):
                return _error(404, "object_not_found")
            return 200, self._page(node)
        if call == ("GET", "blocks", "children") and node:
            known = node in self.pages or node in self._blocks
            if not known or not self._visible("page" if node in self.pages else "block", node):
                return _error(404, "object_not_found")
            return 200, self._listed(self._children(node), arguments)
        if call == ("GET", "blocks", None) and node:
            if node not in self._blocks or not self._visible("block", node):
                return _error(404, "object_not_found")
            return 200, self._block(node)
        if call == ("GET", "databases", None) and node:
            database = self.databases.get(node)
            if database is None or not self._visible("database", node):
                return _error(404, "object_not_found")
            return 200, {
                "object": "database",
                "id": node,
                "title": rich(database["title"]),
                "parent": self._parent(database["parent"]),
                "in_trash": database["trashed"],
                "data_sources": [{"id": source, "name": ""} for source in database["sources"]],
            }
        if call == ("GET", "data_sources", None) and node:
            if node not in self.sources or not self._visible("source", node):
                return _error(404, "object_not_found")
            database = self.sources[node]["database"]
            parent = {"type": "database_id", "database_id": database}
            return 200, {"object": "data_source", "id": node, "parent": parent}
        if call == ("POST", "data_sources", "query") and node:
            if node not in self.sources or not self._visible("source", node):
                return _error(404, "object_not_found")
            rows = [
                self._page(row)
                for row in self.sources[node]["rows"]
                if not self.pages[row]["trashed"]
            ]
            if (arguments.get("sorts") or [{}])[0].get("direction") == "descending":
                rows.reverse()
            return 200, self._listed(rows, arguments)
        if call == ("GET", "users", None) and node:
            if not self.user_capability:
                return _error(403, "restricted_resource")
            if node not in self.users:
                return _error(404, "object_not_found")
            return 200, {"object": "user", "id": node, "type": "person", "name": self.users[node]}
        return _error(400, "invalid_request_url")

    def _search(self, arguments: dict[str, Any]) -> dict[str, Any]:
        wanted = (arguments.get("filter") or {}).get("value", "page")
        found = [
            page
            for page_id, page in self._index.items()
            if wanted == "page"
            and page_id not in self.hidden
            and self._visible("page", page_id)
            and (self.search_trashed or not page["in_trash"])
        ]
        newest_first = (arguments.get("sort") or {}).get("direction") != "ascending"
        found.sort(key=lambda page: page["id"])
        found.sort(key=lambda page: page["last_edited_time"], reverse=newest_first)
        return self._listed(found, arguments)


def matcher(spec: Mapping[str, Any]) -> Match:
    """A request predicate from JSON: a `path` pattern, and `arguments`
    patterns searched in the query parameters or the JSON body; an argument
    the request lacks never matches."""
    path = re.compile(spec.get("path", ""))
    wanted = {name: re.compile(pattern) for name, pattern in spec.get("arguments", {}).items()}

    def match(request_path: str, arguments: dict[str, Any]) -> bool:
        return bool(path.search(request_path)) and all(
            name in arguments and pattern.search(json.dumps(arguments[name]))
            for name, pattern in wanted.items()
        )

    return match


class FakeServer(Serving):
    """`world` over HTTP, for a connector in another process.

    `settings.api_endpoint` is `endpoint` and the token is `world.token`; a
    request with another token is refused.

    `POST /fake/<name>` with JSON arguments drives the world from the test:
    the changes of `FakeNotion` (`user`, `page` with blocks as `block()`
    makes them, `database`, `row`, `move`, `trash`, `restore`, `remove`,
    `unshare`, `share`, `reindex`), `set` (`lag`, `page_size`,
    `user_capability`, `search_trashed`, `lead_seconds`), `fail` with a
    `match` as for `matcher`, `hold` (park the first matching request until
    `release`: Notion changing, or the worker dying, in the middle of a
    sync), `parked` and `requests` (the paths a `match` selects).
    """

    def __init__(self, world: FakeNotion) -> None:
        super().__init__(_Handler, fake=self)
        self.world = world
        self.holds = Holds()

    def close(self) -> None:
        self.holds.release()
        super().close()

    def api(
        self, method: str, path: str, arguments: dict[str, Any], headers: Mapping[str, str]
    ) -> tuple[int, dict[str, Any], dict[str, str]]:
        self.holds.wait(path, arguments)
        return self.world.handle(method, path, arguments, headers)

    def control(self, name: str, arguments: dict[str, Any]) -> Any:
        world = self.world
        if name == "hold":
            return self.holds.hold(matcher(arguments["match"]))
        if name == "release":
            return self.holds.release()
        if name == "parked":
            return self.holds.parked()
        # The lock of `handle`: a change never lands in the middle of an answer.
        with world._lock:
            if name == "requests":
                match = matcher(arguments.get("match", {}))
                return [path for _, path, sent in world.requests if match(path, sent)]
            if name == "fail":
                return world.fail(matcher(arguments.pop("match")), **arguments)
            if name == "set":
                if "lead_seconds" in arguments:
                    world.lead = timedelta(seconds=arguments.pop("lead_seconds"))
                if set(arguments) - _SETTINGS:
                    raise KeyError("set")
                for setting, value in arguments.items():
                    setattr(world, setting, value)
                return None
            if name in _WORLD_CALLS:
                return getattr(world, name)(**arguments)
        raise KeyError(name)


class _Handler(JsonHandler):
    def _api(self, method: str, arguments: dict[str, Any]) -> None:
        fake: FakeServer = self.server.fake  # type: ignore[attr-defined]
        self.send_json(*fake.api(method, urlsplit(self.path).path, arguments, self.headers))

    def do_GET(self) -> None:
        query = parse_qs(urlsplit(self.path).query)
        self._api("GET", {key: values[0] for key, values in query.items()})

    def do_POST(self) -> None:
        fake: FakeServer = self.server.fake  # type: ignore[attr-defined]
        body = self.read_body()
        if not self.path.startswith(CONTROL_PATH):
            return self._api("POST", json.loads(body or b"{}"))
        try:
            result = fake.control(self.path.removeprefix(CONTROL_PATH), json.loads(body or b"{}"))
        except (KeyError, TypeError, ValueError) as error:
            # A wrong call in a test must fail that test, not be skipped.
            return self.send_json(400, {"error": type(error).__name__, "detail": str(error)})
        self.send_json(200, {"result": result})


def main() -> None:
    """Serve an empty world until standard input closes. The first output
    line is how to reach it: the endpoint and the (synthetic) token."""
    with FakeServer(FakeNotion()) as server:
        print(json.dumps({"endpoint": server.endpoint, "token": server.world.token}), flush=True)
        sys.stdin.read()


if __name__ == "__main__":
    main()
