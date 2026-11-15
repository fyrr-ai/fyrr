"""`scope.shared_with_me`: files in a user's Drive that someone else owns."""

import pytest
from conftest import ALICE, Index
from fake_drive import FakeDrive

from fyrr_connectors import testkit

CAROL = "carol@elsewhere.example"
SHARED = {"owners": [ALICE], "shared_with_me": True}


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
