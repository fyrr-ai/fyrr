"""`scope.shared_with_me`: files in a user's Drive that someone else owns."""

import pytest
from conftest import ALICE, BOB, SETTINGS, Index
from fake_drive import FakeDrive

from fyrr_connectors import testkit

CAROL = "carol@elsewhere.example"
SHARED = {"owners": [ALICE], "shared_with_me": True}


def _exports(drive: FakeDrive, file_id: str) -> list[str]:
    """Who exported the file, in order."""
    return [subject for subject, path, _ in drive.requests if path.endswith(f"{file_id}/export")]


def _permission_reads(drive: FakeDrive) -> list[str]:
    return [path for path in drive.paths() if path.endswith("/permissions")]


def test_other_owners_files_are_out_of_scope_by_default(drive: FakeDrive, index: Index) -> None:
    drive.put("mine", b"Alice writes.", owner=ALICE)
    drive.put("theirs", b"Carol writes.", owner=CAROL, shared_with=(ALICE,))
    assert sorted(index.sync(owners=[ALICE])) == ["mine"]
    assert index.listing(owners=[ALICE]) == ["mine"]
    # Widening the scope takes a reset cursor: the feed's token is past the file.
    assert index.sync(**SHARED) == {}
    index.cursor = None
    assert sorted(index.sync(**SHARED)) == ["theirs"]
    assert testkit.check_provider("google-drive")["config_schema"]["scope"]["properties"][
        "shared_with_me"
    ] == {
        "default": False,
        "description": "Files in the owners' Drive that someone else owns; needs fixed "
        "permissions, a sync connection is refused",
        "type": "boolean",
    }


def test_a_fixed_connection_takes_shared_files_and_reads_no_permissions(
    drive: FakeDrive, index: Index
) -> None:
    drive.put("mine", b"Alice writes.", owner=ALICE)
    drive.put("theirs", b"Carol writes.", owner=CAROL, shared_with=(ALICE,))
    drive.put("unshared", b"Carol keeps.", owner=CAROL)
    seen = index.sync(**SHARED)
    assert sorted(seen) == ["mine", "theirs"]
    assert seen["theirs"]["readers"] == ["grp:staff"]
    assert seen["theirs"]["metadata"]["author"] == CAROL
    assert index.listing(**SHARED) == ["mine", "theirs"]

    drive.put("theirs", b"Carol rewrites.")
    assert index.sync(**SHARED)["theirs"]["sections"][0]["text"] == "Carol rewrites."
    # Alice loses the file: her feed only says `removed`, the listing decides.
    drive.unshare("theirs", f"p-{ALICE}")
    assert index.sync(**SHARED) == {}
    assert index.listing(**SHARED) == ["mine"]
    assert not _permission_reads(drive)
    assert not any("/admin/" in path for path in drive.paths())


def test_a_trashed_shared_file_is_deleted(drive: FakeDrive, index: Index) -> None:
    drive.put("theirs", b"Carol writes.", owner=CAROL, shared_with=(ALICE,))
    assert sorted(index.sync(**SHARED)) == ["theirs"]
    drive.touch("theirs", trashed=True)
    assert index.sync(**SHARED)["theirs"]["op"] == "delete"
    assert index.listing(**SHARED) == []


def test_an_owner_whose_drive_is_read_reports_the_file(drive: FakeDrive, index: Index) -> None:
    drive.put("bobs", b"Bob writes.", owner=BOB, shared_with=(ALICE,))
    assert sorted(index.sync(shared_with_me=True)) == ["bobs"]
    assert _exports(drive, "bobs") == [BOB]
    assert index.listing(shared_with_me=True) == ["bobs"]

    # Without My Drive nobody owns it here: it is taken where it is shared.
    apart = Index()
    assert sorted(apart.sync(shared_with_me=True, my_drive=False)) == ["bobs"]
    assert _exports(drive, "bobs") == [BOB, ALICE]
    assert apart.listing(shared_with_me=True, my_drive=False) == ["bobs"]
    drive.put("alices", b"Alice writes.", owner=ALICE)
    assert apart.sync(shared_with_me=True, my_drive=False) == {}


def test_a_file_shared_with_several_users_is_published_once(drive: FakeDrive, index: Index) -> None:
    drive.put("theirs", b"Carol writes.", owner=CAROL, shared_with=(ALICE, BOB))
    assert sorted(index.sync(shared_with_me=True)) == ["theirs"]
    assert len(index.batches) == 1 and _exports(drive, "theirs") == [ALICE]
    assert index.listing(shared_with_me=True) == ["theirs"]
    # Each of the two feeds reports the edit; the second finds it handed over.
    drive.put("theirs", b"Carol rewrites.")
    assert sorted(index.sync(shared_with_me=True)) == ["theirs"]
    assert _exports(drive, "theirs") == [ALICE, ALICE]


def test_a_sync_connection_refuses_the_shared_scope(drive: FakeDrive, index: Index) -> None:
    """Another owner's permission list is not reliably readable; a published
    file whose list became unreadable would keep its last readers."""
    drive.put("theirs", b"Carol writes.", owner=CAROL, shared_with=(ALICE,))
    for kind in ("content", "permissions", "members", "reconcile"):
        with pytest.raises(
            ValueError, match="google_drive_shared_with_me_requires_fixed_permissions"
        ):
            list(index.run(kind, "sync", **SHARED))
    assert drive.requests == []


def test_the_shared_scope_passes_the_test_kit(drive: FakeDrive) -> None:
    drive.put("theirs", b"Carol writes.", owner=CAROL, shared_with=(ALICE,))
    drive.put("mine", b"Alice writes.", owner=ALICE)
    run = {"settings": SETTINGS, "scope": SHARED, "permissions": "fixed"}
    content = testkit.run_sync("google-drive", "content", **run)
    changes = [change for batch in content.documents for change in batch["body"]["changes"]]
    assert sorted(change["root_id"] for change in changes) == ["mine", "theirs"]
    listing = testkit.run_sync("google-drive", "reconcile", **run)
    assert listing.documents[-1]["body"]["complete"]
    assert sorted(root for part in listing.documents for root in part["body"]["root_ids"]) == [
        "mine",
        "theirs",
    ]
