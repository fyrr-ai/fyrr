"""The fake Drive over HTTP: what `fyrr writer sync` talks to on real S3.

Nothing replaces `client.authorized_session` here, so the connector's own
delegated sign-in runs against the fake's token endpoint.
"""

import json
import threading
from collections.abc import Iterator
from typing import Any

import pytest
import requests
from conftest import ALICE, BOB, Index
from fake_drive import ADMIN, FakeDrive, FakeServer
from google.auth.exceptions import RefreshError

from fyrr_connectors.google_drive import client


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch, index: Index) -> Iterator[FakeServer]:
    world = FakeDrive()
    world.user(ALICE)
    world.user(BOB)
    monkeypatch.setattr(client.time, "sleep", world.sleeps.append)
    with FakeServer(world) as served:
        monkeypatch.setenv("FYRR_TEST_GDRIVE_KEY", served.service_account)
        index.settings["api_endpoint"] = served.endpoint
        yield served


def _call(server: FakeServer, call: str, /, **arguments: Any) -> Any:
    response = requests.post(f"{server.endpoint}/fake/{call}", json=arguments, timeout=10)
    assert response.status_code == 200, response.text
    return response.json()["result"]


def test_each_call_is_answered_as_the_user_the_connector_signed_in_as(
    server: FakeServer, index: Index
) -> None:
    world = server.world
    world.drive("drive-eng", "Engineering", [BOB])
    world.put("mine", b"Alice writes.", owner=ALICE, shared_with=(BOB,))
    world.put("theirs", b"Bob writes.", owner=BOB)
    world.put("shared", b"The team writes.", drive="drive-eng")
    world.group("staff@example.com", (ALICE, "USER"))

    assert sorted(index.sync("sync", owners=[])) == ["mine", "shared", "theirs"]
    documents, grants = index.permissions(owners=[])
    assert documents["mine"].readers == [f"usr:{ALICE}", f"usr:{BOB}"]
    assert grants["drive-eng"].readers == [f"usr:{BOB}"]
    members = list(index.run("members", "sync"))
    assert [group.id for group in members[0].groups] == ["staff@example.com", "domain:example.com"]

    subjects: dict[str, set[str]] = {}
    for subject, path, _ in world.requests:
        subjects.setdefault(path, set()).add(subject)
    assert subjects["/admin/directory/v1/users"] == {ADMIN}
    assert subjects["/admin/directory/v1/groups/staff@example.com/members"] == {ADMIN}
    assert subjects["/drive/v3/files/mine/export"] == {ALICE}
    assert subjects["/drive/v3/files/theirs/export"] == {BOB}
    assert subjects["/drive/v3/files/shared/export"] == {BOB}
    assert subjects["/drive/v3/files/drive-eng/permissions"] == {ADMIN}


def test_a_user_who_cannot_sign_in_fails_the_sync(server: FakeServer, index: Index) -> None:
    server.world.put("mine", b"Alice writes.", owner=ALICE)
    with pytest.raises(RefreshError):
        index.sync(owners=[ALICE, "nobody@example.com"])
    assert index.cursor is None or "mine" in index.revisions


def test_a_call_without_its_scope_or_token_is_refused(server: FakeServer) -> None:
    session = client.authorized_session(json.loads(server.service_account), ADMIN, client.DRIVE)
    users = f"{server.endpoint}/admin/directory/v1/users"
    assert session.get(users, timeout=10).json()["error"]["errors"][0]["reason"] == (
        "insufficientPermissions"
    )
    assert requests.get(users, timeout=10).status_code == 401
    assert server.world.requests == []


def test_the_world_is_driven_over_http(server: FakeServer, index: Index) -> None:
    _call(server, "user", email="carol@example.com", suspended=True)
    _call(server, "group", email="staff@example.com", members=[[ALICE, "USER"]])
    _call(server, "drive", drive_id="drive-eng", name="Engineering", members=[ALICE])
    _call(server, "put", file_id="plan", content="# Plan\n\nShip it.", owner=ALICE, name="Plan")
    _call(server, "put", file_id="notes", content="Notes.", drive="drive-eng")
    _call(server, "share", file_id="plan", type="group", emailAddress="staff@example.com")
    _call(server, "join", drive_id="drive-eng", type="user", emailAddress=BOB)

    seen = index.sync("sync", owners=[])
    assert seen["plan"]["sections"][0]["text"] == "# Plan\n\nShip it."
    documents, grants = index.permissions(owners=[])
    assert documents["plan"].readers == ["grp:staff@example.com", f"usr:{ALICE}"]
    assert documents["notes"].containers == ["drive-eng"]
    assert grants["drive-eng"].readers == [f"usr:{ALICE}", f"usr:{BOB}"]

    _call(server, "unshare", file_id="plan", permission_id="p-staff@example.com")
    _call(server, "leave", drive_id="drive-eng", permission_id=f"p-{BOB}")
    _call(server, "touch", file_id="notes", trashed=True)
    _call(server, "remove", file_id="plan")
    documents, grants = index.permissions(owners=[])
    assert grants["drive-eng"].readers == [f"usr:{ALICE}"]
    assert index.sync("sync", owners=[])["notes"]["op"] == "delete"
    assert index.listing(owners=[ALICE]) == []

    exports = _call(server, "requests", match={"path": "/export$"})
    assert sorted(exports) == [
        [ALICE, "/drive/v3/files/notes/export"],
        [ALICE, "/drive/v3/files/plan/export"],
    ]
    wrong = requests.post(f"{server.endpoint}/fake/put", json={"file": "x"}, timeout=10)
    assert wrong.status_code == 400
    assert requests.post(f"{server.endpoint}/fake/erase", json={}, timeout=10).status_code == 400


def test_an_injected_fault_reaches_the_connector(server: FakeServer, index: Index) -> None:
    server.world.put("plan", b"Ship it.", owner=ALICE)
    _call(
        server,
        "fail",
        match={"path": "/files$", "params": {"q": "'me' in owners"}},
        status=500,
        times=2,
        retry_after="0",
    )
    _call(server, "fail", match={"path": "/export$"}, status=403, reason="cannotExportFile")
    assert index.sync(owners=[ALICE])["plan"]["sections"] == [{"text": "\0"}]
    assert len(server.world.sleeps) == 2


def test_a_parked_request_waits_for_its_release(server: FakeServer, index: Index) -> None:
    server.world.put("plan", b"Ship it.", owner=ALICE)
    _call(server, "hold", match={"path": "/export$"})
    done: list[dict[str, Any]] = []
    sync = threading.Thread(target=lambda: done.append(index.sync(owners=[ALICE])))
    sync.start()
    while not _call(server, "parked"):
        sync.join(0.01)
        assert sync.is_alive()
    # Drive changes while the export is on its way: the feed's token is older.
    _call(server, "put", file_id="later", content="Later.", owner=ALICE)
    assert not done
    _call(server, "release")
    sync.join(30)
    assert sorted(done[0]) == ["later", "plan"] and _call(server, "parked") == 0
    assert index.sync(owners=[ALICE]) == {}


@pytest.mark.parametrize("status", [200, 503])
def test_body_disconnects_retry_without_advancing_pagination(monkeypatch, status):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import parse_qs, urlsplit

    calls, opened = [], []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            params = parse_qs(urlsplit(self.path).query)
            calls.append(params)
            body = {"files": []}
            if "pageToken" not in params:
                body["nextPageToken"] = "next"
            data = json.dumps(body).encode()
            self.send_response(status if len(calls) == 1 else 200)
            self.send_header("Content-Length", str(len(data) + (8 if len(calls) == 1 else 0)))
            self.end_headers()
            self.wfile.write(data)
            self.close_connection = True

        def log_message(self, *_):
            pass

    session = requests.Session()
    real = session.get

    def get(*args, **kwargs):
        response = real(*args, **kwargs)
        opened.append(response)
        return response

    session.get = get
    monkeypatch.setattr(client, "authorized_session", lambda *_: session)
    sleeps = []
    monkeypatch.setattr(client.time, "sleep", sleeps.append)
    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as served:
        thread = threading.Thread(target=served.serve_forever, daemon=True)
        thread.start()
        try:
            endpoint = f"http://127.0.0.1:{served.server_port}"
            pages = list(client.Client("{}", endpoint).pages("user", "files"))
        finally:
            served.shutdown()
            thread.join()
            session.close()
    assert pages == [{"files": [], "nextPageToken": "next"}, {"files": []}]
    assert calls == [{}, {}, {"pageToken": ["next"]}]
    assert len(sleeps) == 1
    assert len(opened) == 3 and all(response.raw.closed for response in opened)
