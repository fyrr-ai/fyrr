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


def test_a_file_that_changes_feed_twice_asks_for_another_pass(
    drive: FakeDrive, index: Index
) -> None:
    """Out of the shared drive to Bob once his feed is listed, and on to Alice
    once the catch-up has read her changes. Bob's changes then name a file he
    no longer reports and nobody has listed: it is in another feed."""
    _world(drive)
    listed = iter(range(10))

    def second_user_feed_listed(path: str, params: dict[str, str]) -> bool:
        return _user_feed_listed(path, params) and next(listed) == 1

    def onwards() -> None:
        drive.share("moving", type="user", emailAddress=BOB)
        drive.move("moving", owner=ALICE)

    drive.after(
        second_user_feed_listed,
        lambda: drive.move("moving", owner=BOB, drive=None, parent=f"root-{BOB}"),
    )
    drive.after(lambda path, _: path.endswith("/changes"), onwards)
    assert index.listing() == ["bobs", "handed", "mine", "moving", "stays"]


def test_a_file_created_or_trashed_during_the_listing(drive: FakeDrive, index: Index) -> None:
    _world(drive)

    def change() -> None:
        drive.put("late", b"Written meanwhile.", owner=ALICE)
        drive.touch("mine", trashed=True)

    drive.after(_user_feed_listed, change)
    listed = index.listing()
    assert "late" in listed and {"bobs", "handed", "moving", "stays"} <= set(listed)


def test_a_quiet_listing_costs_two_calls_per_feed(drive: FakeDrive, index: Index) -> None:
    _world(drive)
    assert index.listing() == ["bobs", "handed", "mine", "moving", "stays"]
    paths = drive.paths()
    assert paths.count("/drive/v3/changes/startPageToken") == 3
    assert paths.count("/drive/v3/changes") == 3


@pytest.mark.parametrize("created", [None, "2100-01-01T00:00:00.000Z"])
def test_only_files_older_than_the_listing_keep_it_from_settling(
    drive: FakeDrive, index: Index, created: str | None
) -> None:
    """Files keep arriving in the feed that every pass reads last. Old ones
    (the fake's clock is in the past) may have been missed by moving, so the
    listing does not settle; files created since it began do not hold it up."""
    _world(drive)
    counter = iter(range(1000))

    def changes_read(path: str, _: dict[str, str]) -> bool:
        return path.endswith("/changes")

    def change() -> None:
        file_id = f"new-{next(counter)}"
        drive.put(file_id, b"Again.", drive="drive-eng")
        if created:
            drive.files[file_id]["created"] = created
        drive.after(changes_read, change)

    drive.after(changes_read, change)
    parts = list(index.run("reconcile"))
    assert parts[-1].last and parts[-1].complete == bool(created)
    listed = {root for part in parts for root in part.root_ids}
    assert {"bobs", "handed", "mine", "moving", "stays", "new-0"} <= listed
    reads = drive.paths().count("/drive/v3/changes")
    assert reads == 3 if created else reads >= 9, "one pass, or all three"


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
