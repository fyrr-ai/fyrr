"""Feed discovery, the cursor codec and the folder scope rule."""

import base64
import zlib

import pytest
from conftest import ALICE, BOB
from fake_drive import FOLDER, FakeDrive

from fyrr_connectors.google_drive.client import Client
from fyrr_connectors.google_drive.config import Scope
from fyrr_connectors.google_drive.feeds import Cursor, Feed, Folders, discover
from fyrr_connectors.validate import MAX_CURSOR_BYTES


def _client() -> Client:
    return Client("{}", "https://www.googleapis.com")


def test_the_cursor_round_trips_and_refuses_what_it_cannot_read() -> None:
    cursor = Cursor(feeds={"u:a@example.com": "17", "d:drive": "4"}, listing=("d:x", "9", "page"))
    assert Cursor.decode(cursor.encode()) == cursor
    assert Cursor.decode(None) == Cursor()
    with_grants = Cursor(feeds={"d:drive": "4"}, grants={"drive": "0f3a"})
    assert Cursor.decode(with_grants.encode()) == with_grants
    assert cursor.encode() == Cursor.decode(cursor.encode()).encode()
    for text in ("", "not a cursor", Cursor().encode()[:-4]):
        with pytest.raises(ValueError, match="google_drive_cursor_invalid"):
            Cursor.decode(text)


def test_a_cursor_over_the_contract_limit_fails_instead_of_dropping_feeds() -> None:
    def cursor(feeds: int) -> Cursor:
        return Cursor(
            feeds={
                f"u:user{n}.name{n * 7919 % 9973}@example.com": str(n * 104729)
                for n in range(feeds)
            }
        )

    assert len(cursor(500).encode()) <= MAX_CURSOR_BYTES
    with pytest.raises(ValueError, match="google_drive_cursor_too_large"):
        cursor(5000).encode()


def test_retain_forgets_feeds_that_left_the_scope() -> None:
    cursor = Cursor(
        feeds={"u:a": "1", "u:b": "2", "d:x": "3"},
        listing=("u:c", "0", None),
        grants={"x": "r1", "gone": "r2"},
    )
    cursor.retain([Feed(key="u:b", subject="b"), Feed(key="d:x", subject="b", drive_id="x")])
    assert cursor == Cursor(feeds={"u:b": "2", "d:x": "3"}, grants={"x": "r1"})


def test_each_shared_drive_is_read_once_through_its_first_member(drive: FakeDrive) -> None:
    drive.drive("drive-eng", "Engineering", [BOB, ALICE])
    drive.drive("drive-ops", "Operations", [BOB])
    drive.drive("drive-hr", "People", ["carol@example.com"])
    feeds = discover(_client(), Scope(), [BOB, "Alice@Example.com "])
    assert [(feed.key, feed.subject, feed.name) for feed in feeds] == [
        ("u:ff8d9819fc0e12bf", ALICE, None),
        ("u:5ff860bf1190596c", BOB, None),
        ("d:drive-eng", ALICE, "Engineering"),
        ("d:drive-ops", BOB, "Operations"),
    ]
    stored = Cursor(feeds={feed.key: "1" for feed in feeds}).encode()
    stored = zlib.decompress(base64.urlsafe_b64decode(stored)).decode()
    assert "@" not in stored and "example" not in stored, "no address in the stored cursor"
    only = discover(_client(), Scope(my_drive=False, shared_drives=["drive-ops"]), [ALICE, BOB])
    assert [feed.key for feed in only] == ["d:drive-ops"]
    before = len(drive.requests)
    assert [f.subject for f in discover(_client(), Scope(shared_drives=[]), [ALICE])] == [ALICE]
    assert len(drive.requests) == before, "no drive is listed when none is in scope"


def test_the_folder_scope_walks_parents_once_and_stops_at_unreadable_ones(drive: FakeDrive) -> None:
    drive.put("top", owner=ALICE, mime=FOLDER)
    drive.put("inner", owner=ALICE, mime=FOLDER, parent="top")
    drive.put("hidden", owner=BOB, mime=FOLDER, parent="top")
    drive.fail(lambda path, _: path.endswith("files/hidden"), 404, "notFound", times=9)
    feed = Feed(key=f"u:{ALICE}", subject=ALICE)
    folders = Folders(_client(), ["top"])
    assert folders.contain(feed, {"id": "a", "parents": ["inner"]})
    assert folders.contain(feed, {"id": "b", "parents": ["inner"]})
    assert folders.contain(feed, {"id": "top", "mimeType": FOLDER, "parents": ["root"]})
    assert not folders.contain(feed, {"id": "c", "parents": ["hidden"]})
    assert not folders.contain(feed, {"id": "d", "parents": [f"root-{ALICE}"]})
    assert not folders.contain(feed, {"id": "e"})
    assert drive.paths().count("/drive/v3/files/inner") == 1
    assert Folders(_client(), []).contain(feed, {"id": "f"})
