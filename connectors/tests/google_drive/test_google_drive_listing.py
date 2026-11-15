"""The full-ID listing while Drive changes under it: a file that is in scope
the whole time must be listed, whichever feed it is in when that feed is read."""

import pytest
from conftest import ALICE, BOB, Index
from fake_drive import FakeDrive

from fyrr_connectors.google_drive.client import ApiError


def _user_feed_listed(path: str, params: dict[str, str]) -> bool:
    """The listing of the first user feed (Alice's) was answered."""
    return path.endswith("/files") and params.get("corpora") == "user"


def _world(drive: FakeDrive) -> None:
    drive.drive("drive-eng", "Engineering", [ALICE])
    drive.put("mine", b"Alice writes.", owner=ALICE)
    drive.put("bobs", b"Bob writes.", owner=BOB)
    drive.put("moving", b"On the move.", drive="drive-eng")
    drive.put("handed", b"Handed over.", owner=BOB)
    drive.put("stays", b"Team notes.", drive="drive-eng")


@pytest.mark.parametrize(
    ("file_id", "arrival"),
    [
        # Out of the shared drive, which is listed last, into Alice's Drive.
        ("moving", {"owner": ALICE, "drive": None, "parent": f"root-{ALICE}"}),
        # From Bob, whose feed is listed after Alice's, to Alice.
        ("handed", {"owner": ALICE}),
    ],
)
def test_a_file_that_changes_feed_during_the_listing_stays_listed(
    drive: FakeDrive, index: Index, file_id: str, arrival: dict
) -> None:
    _world(drive)
    drive.after(_user_feed_listed, lambda: drive.move(file_id, **arrival))
    assert index.listing() == ["bobs", "handed", "mine", "moving", "stays"]


def test_an_invalid_token_during_listing_catch_up_never_completes(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    drive.fail(lambda path, _: path.endswith("/changes"), 400, "invalid", "pageToken")
    parts = []
    try:
        parts.extend(index.run("reconcile"))
    except ApiError as error:
        assert error.status == 400
    assert not any(part.last and part.complete for part in parts)
    assert "/drive/v3/changes" in drive.paths()
