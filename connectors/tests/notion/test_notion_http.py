"""The fake Notion over HTTP: what `fyrr writer sync` talks to on real S3.

Nothing replaces `client.session` here: the connector's own session, token
and headers go over a socket.
"""

import threading
from collections.abc import Iterator
from typing import Any

import pytest
import requests
from fake_notion import FakeNotion, FakeServer, block, ident
from notion_kit import Index, text

from fyrr_connectors.notion import client
from fyrr_connectors.notion.client import ApiError


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeServer]:
    world = FakeNotion()
    world.user("ada", "Ada Example")
    world.page("root", title="Handbook")
    monkeypatch.setattr(client.time, "sleep", world.sleeps.append)
    monkeypatch.setattr(client, "MIN_INTERVAL_SECONDS", 0.0)
    with FakeServer(world) as served:
        monkeypatch.setenv("FYRR_TEST_NOTION_TOKEN", world.token)
        yield served


@pytest.fixture
def remote(server: FakeServer) -> Index:
    index = Index(server.world)
    index.settings["api_endpoint"] = server.endpoint
    return index


def _call(server: FakeServer, call: str, /, **arguments: Any) -> Any:
    response = requests.post(f"{server.endpoint}/fake/{call}", json=arguments, timeout=10)
    assert response.status_code == 200, response.text
    return response.json()["result"]


def test_a_sync_runs_over_http_with_the_connectors_own_session(
    server: FakeServer, remote: Index
) -> None:
    world = server.world
    world.page("plan", "root", blocks=[block("heading_1", "Plan"), block("paragraph", "Ship.")])
    world.database("tasks", "root")
    for number in range(3):
        world.row(f"task-{number}", "tasks", title=f"Task {number}")
    seen = remote.sync()
    assert sorted(seen) == ["plan", "task-0", "task-1", "task-2"]
    assert text(seen["plan"]) == "# Plan\n\nShip."
    assert seen["plan"]["metadata"]["author"] == "Ada Example"
    assert remote.listing() == ["plan", "root", "task-0", "task-1", "task-2"]
    assert remote.sync() == {}


def test_a_wrong_token_is_refused_and_fails_the_sync(server: FakeServer, remote: Index) -> None:
    server.world.token = "rotated"
    with pytest.raises(ApiError) as caught:
        remote.sync()
    assert caught.value.status == 401
    assert server.world.requests == [] and remote.cursor is None
    headers = {"Authorization": "Bearer rotated"}
    plain = requests.get(f"{server.endpoint}/v1/pages/{ident('root')}", headers=headers, timeout=10)
    assert plain.json()["code"] == "missing_version"


def test_an_injected_fault_reaches_the_connector(server: FakeServer, remote: Index) -> None:
    server.world.page("plan", "root", blocks=[block("paragraph", "Ship it.")])
    children = f"/blocks/{ident('plan')}/children$"
    _call(server, "fail", match={"path": "/search$"}, status=429, times=2, retry_after="0")
    _call(server, "fail", match={"path": children}, status=400, code="validation_error")
    assert remote.sync()["plan"]["sections"] == [{"text": "\0"}]
    assert len(server.world.sleeps) == 2
    paged = {"path": "/search$", "arguments": {"sort": "descending"}}
    assert len(_call(server, "requests", match=paged)) >= 2
    assert _call(server, "requests", match={"path": "/search$", "arguments": {"no": "x"}}) == []


def test_a_parked_request_waits_for_its_release(server: FakeServer, remote: Index) -> None:
    server.world.page("plan", "root", blocks=[block("paragraph", "Ship it.")])
    _call(server, "hold", match={"path": f"/blocks/{ident('plan')}/children$"})
    done: list[dict[str, Any]] = []
    sync = threading.Thread(target=lambda: done.append(remote.sync()))
    sync.start()
    while not _call(server, "parked"):
        sync.join(0.01)
        assert sync.is_alive()
    # Notion changes while the page is being read: the watermark is older.
    _call(server, "page", name="later", parent="root", blocks=[block("paragraph", "Later.")])
    assert not done
    _call(server, "release")
    sync.join(30)
    assert sorted(done[0]) == ["later", "plan"] and _call(server, "parked") == 0
    assert remote.sync() == {}
