"""Feed discovery, the cursor codec and the folder scope rule."""

import pytest
from conftest import ALICE, BOB
from fake_drive import FOLDER, FakeDrive

from fyrr_connectors.google_drive.client import Client
from fyrr_connectors.google_drive.feeds import Cursor, Feed, Folders
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
