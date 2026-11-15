"""The connector end to end on the fake Drive, fixed permissions (block A)."""

from pathlib import Path

import pytest
from conftest import ALICE, BOB, SETTINGS, Index, pptx, xlsx
from fake_drive import FOLDER, SHEET, SLIDES, FakeDrive

from fyrr_connectors import testkit
from fyrr_connectors.google_drive.client import ApiError

GOLDEN = Path(__file__).parent / "golden"
PLAN = b"Draft.\n\n# Plan\n\nShip the connector.\n\n## Risks\n\nFew.\n"
QUARANTINED = [{"text": "\0"}]


def _world(drive: FakeDrive) -> None:
    drive.drive("drive-eng", "Engineering", [ALICE])
    drive.put("doc-plan", PLAN, owner=ALICE, name="Plan 2026")
    budget = xlsx(
        {"Costs": [["item", "eur"], ["rent", 1200], ["tea", 7.5]], "Team": [["name"], ["Ada"]]}
    )
    drive.put("sheet-budget", budget, owner=ALICE, mime=SHEET, name="Budget")
    deck = pptx([["Roadmap", "2026"], ["Q1", "Drive connector"]])
    drive.put("deck-roadmap", deck, drive="drive-eng", mime=SLIDES, name="Roadmap")


def _exports(drive: FakeDrive, file_id: str) -> int:
    return drive.paths().count(f"/drive/v3/files/{file_id}/export")


def test_the_provider_is_installed_and_valid() -> None:
    info = testkit.check_provider("google-drive")
    assert info["sync_kinds"] == ["content", "permissions", "members", "reconcile"]
    assert info["connection_types"] == ["sync", "fixed"]
    assert info["config_schema"]["settings"]["properties"]["service_account"]["x-fyrr-secret"]


def test_docs_sheets_and_slides_match_the_golden_output(drive: FakeDrive) -> None:
    _world(drive)
    result = testkit.run_sync(
        "google-drive",
        "content",
        settings=SETTINGS,
        scope={"owners": [ALICE]},
        permissions="fixed",
        readers=("grp:staff",),
    )
    testkit.assert_golden(result, GOLDEN / "content.json")
    listing = testkit.run_sync(
        "google-drive",
        "reconcile",
        settings=SETTINGS,
        scope={"owners": [ALICE]},
        permissions="fixed",
    )
    assert listing.documents[-1]["body"]["complete"]
    assert sorted(root for part in listing.documents for root in part["body"]["root_ids"]) == [
        "deck-roadmap",
        "doc-plan",
        "sheet-budget",
    ]


def test_create_edit_trash_and_restore_propagate(drive: FakeDrive, index: Index) -> None:
    _world(drive)
    assert sorted(index.sync()) == ["deck-roadmap", "doc-plan", "sheet-budget"]
    batches = len(index.batches)
    assert index.sync() == {} and len(index.batches) == batches, "an idle run hands over nothing"

    drive.put("doc-plan", b"# Plan\n\nChanged.\n")
    drive.put("doc-new", b"New in the drive.", drive="drive-eng")
    changed = index.sync()
    assert sorted(changed) == ["doc-new", "doc-plan"]
    assert changed["doc-plan"]["sections"][0]["text"] == "# Plan\n\nChanged."
    assert changed["doc-plan"]["revision_order"] == 2

    drive.touch("sheet-budget", trashed=True)
    drive.touch("deck-roadmap", trashed=True)
    deleted = index.sync()
    assert {root: change["op"] for root, change in deleted.items()} == {
        "sheet-budget": "delete",
        "deck-roadmap": "delete",
    }
    assert deleted["sheet-budget"]["revision_order"] == 2

    drive.touch("sheet-budget", trashed=False)
    restored = index.sync()
    assert restored["sheet-budget"]["op"] == "upsert"
    assert restored["sheet-budget"]["revision_order"] == 3
    assert index.listing() == ["doc-new", "doc-plan", "sheet-budget"]


def test_a_sharing_change_is_not_exported_again_but_a_rename_is(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    index.sync()
    drive.share("doc-plan", type="user", emailAddress=BOB)
    assert index.sync() == {}
    assert _exports(drive, "doc-plan") == 1
    drive.touch("doc-plan", name="Plan 2027")
    assert index.sync()["doc-plan"]["metadata"]["title"] == "Plan 2027"


def test_a_file_is_reported_once_by_its_owner(drive: FakeDrive, index: Index) -> None:
    drive.put("doc-shared", b"Shared with Alice.", owner=BOB, shared_with=(ALICE,))
    assert sorted(index.sync()) == ["doc-shared"]
    assert _exports(drive, "doc-shared") == 1
    drive.put("doc-shared", b"Edited.")
    index.sync()
    assert _exports(drive, "doc-shared") == 2
    assert index.sync(owners=[ALICE]) == {}, "Alice's feed does not report Bob's file"


def test_a_bare_removed_entry_deletes_nothing_and_the_listing_repairs_it(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    index.sync()
    drive.remove("doc-plan")
    assert index.sync() == {}
    assert index.listing() == ["deck-roadmap", "sheet-budget"]


def test_bad_files_are_quarantined_and_the_sync_goes_on(drive: FakeDrive, index: Index) -> None:
    _world(drive)
    drive.put("doc-huge", b"too large to export", owner=ALICE)
    drive.put("doc-fat", b"# Fat\n\n" + b"y" * 70_000, owner=ALICE)
    drive.put("sheet-broken", b"not a workbook", owner=ALICE, mime=SHEET)
    drive.put("doc-gone", b"vanishes before the export", owner=ALICE)
    drive.put("doc-last", b"Still exported.", owner=ALICE)
    drive.fail(lambda path, _: path.endswith("doc-huge/export"), 403, "exportSizeLimitExceeded")
    drive.fail(lambda path, _: path.endswith("doc-gone/export"), 404, "notFound")
    changes = index.sync()
    assert {root for root, change in changes.items() if change["sections"] == QUARANTINED} == {
        "doc-huge",
        "doc-fat",
        "sheet-broken",
    }
    assert "doc-gone" not in changes
    assert changes["doc-last"]["sections"][0]["text"] == "Still exported."
    assert len(changes) == 7
    requests = len(drive.requests)
    assert index.sync() == {}, "the cursor moved past the quarantined files"
    assert not any("export" in path for path in drive.paths()[requests:])


def test_a_rate_limit_or_outage_fails_the_sync_instead_of_skipping(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    drive.fail(
        lambda path, _: path.endswith("doc-plan/export"), 403, "userRateLimitExceeded", times=99
    )
    with pytest.raises(ApiError) as caught:
        index.sync()
    assert caught.value.transient
    assert "doc-plan" not in index.revisions


def test_an_emptied_document_leaves(drive: FakeDrive, index: Index) -> None:
    drive.put("doc-empty", b"\n", owner=ALICE)
    assert index.sync() == {}
    drive.put("doc-empty", b"Now with text.")
    assert index.sync()["doc-empty"]["op"] == "upsert"
    drive.put("doc-empty", b"")
    assert index.sync()["doc-empty"]["op"] == "delete"


def test_the_scope_is_honored_and_narrowing_shows_in_the_listing(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    drive.drive("drive-ops", "Operations", [BOB])
    drive.put("doc-ops", b"Runbook.", drive="drive-ops")
    drive.put("doc-bob", b"Bob's notes.", owner=BOB)
    everything = ["deck-roadmap", "doc-bob", "doc-ops", "doc-plan", "sheet-budget"]
    cases = [
        ({}, everything),
        ({"extensions": ["gdoc"]}, ["doc-bob", "doc-ops", "doc-plan"]),
        ({"shared_drives": []}, ["doc-bob", "doc-plan", "sheet-budget"]),
        ({"shared_drives": ["drive-ops"]}, ["doc-bob", "doc-ops", "doc-plan", "sheet-budget"]),
        ({"my_drive": False}, ["deck-roadmap", "doc-ops"]),
        ({"owners": [BOB]}, ["doc-bob", "doc-ops"]),
    ]
    for scope, expected in cases:
        assert sorted(Index().sync(**scope)) == expected, scope
        assert Index().listing(**scope) == expected, scope
    assert sorted(index.sync()) == everything
    assert index.sync(owners=[BOB]) == {}, "leaving the scope is not a change"
    assert index.listing(owners=[BOB]) == ["doc-bob", "doc-ops"]


def test_the_folder_scope_follows_folders_moved_into_it(drive: FakeDrive, index: Index) -> None:
    drive.put("in", owner=ALICE, mime=FOLDER)
    drive.put("in-deep", owner=ALICE, mime=FOLDER, parent="in")
    drive.put("out", owner=ALICE, mime=FOLDER)
    drive.put("doc-in", b"Inside.", owner=ALICE, parent="in-deep")
    drive.put("doc-out", b"Outside.", owner=ALICE, parent="out")
    drive.put("doc-root", b"At the root.", owner=ALICE)
    assert sorted(index.sync(folders=["in"])) == ["doc-in"]
    assert index.listing(folders=["in"]) == ["doc-in"]
    drive.touch("out", parent="in")
    assert sorted(index.sync(folders=["in"])) == ["doc-out"], "no change entry of its own"
    assert index.listing(folders=["in"]) == ["doc-in", "doc-out"]
    drive.touch("out", parent=f"root-{ALICE}")
    assert index.sync(folders=["in"]) == {}
    assert index.listing(folders=["in"]) == ["doc-in"]


def test_a_failed_listing_page_never_yields_a_complete_listing(drive: FakeDrive) -> None:
    drive.page_size = 50
    drive.drive("drive-eng", "Engineering", [ALICE])
    for number in range(130):
        drive.put(f"doc-{number}", owner=ALICE)
        drive.put(f"deck-{number}", drive="drive-eng", mime=SLIDES)
    assert len(Index().listing(owners=[ALICE])) == 260
    requests = len(drive.requests)
    assert requests >= 7
    for status, reason in ((500, ""), (403, "insufficientFilePermissions"), (404, "notFound")):
        for failing in range(requests):
            seen: list[int] = []

            def nth(
                _path: str, _params: dict, seen: list[int] = seen, failing: int = failing
            ) -> bool:
                seen.append(1)
                return len(seen) > failing

            drive.fail(nth, status, reason, times=99)
            parts = []
            with pytest.raises(ApiError):
                for part in Index().run("reconcile", owners=[ALICE]):
                    parts.append(part)
            assert not any(part.complete or part.last for part in parts), (status, failing)
            drive._faults.clear()


def test_a_fixed_connection_makes_no_permission_or_directory_request(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    drive.put("folder", owner=ALICE, mime=FOLDER)
    drive.put("doc-in", b"Inside.", owner=ALICE, parent="folder")
    index.sync()
    drive.share("doc-plan", type="user", emailAddress=BOB)
    index.sync(folders=["folder"])
    index.listing()
    allowed = ("drives", "files", "changes", "changes/startPageToken")
    for subject, path, params in drive.requests:
        assert subject in (ALICE, BOB)
        resource = path.removeprefix("/drive/v3/")
        assert resource in allowed or resource.startswith("files/"), path
        assert "permission" not in f"{path} {params}".lower()
    for changes in index.batches:
        for change in changes.values():
            assert change["readers"] == (["grp:staff"] if change["op"] == "upsert" else [])


def test_the_cursor_moves_only_with_handed_over_batches_and_a_resume_loses_nothing(
    drive: FakeDrive,
) -> None:
    for lookup in (True, False):
        drive.files.clear()
        drive.logs.clear()
        drive.requests.clear()
        for number in range(7):
            drive.put(f"doc-{number}", f"Text {number}.".encode(), owner=ALICE)
        index = Index(
            lookup=lookup,
            limits={"max_rows": 2, "max_bytes": 1 << 20, "document_max_bytes": 1 << 16},
        )
        drive.fail(lambda path, _: path.endswith("doc-5/export"), 500, times=99)
        with pytest.raises(ApiError):
            index.sync(owners=[ALICE])
        assert sorted(index.revisions) == ["doc-0", "doc-1", "doc-2", "doc-3"]
        assert index.cursor is not None
        drive._faults.clear()
        index.sync(owners=[ALICE])
        assert sorted(index.revisions) == [f"doc-{number}" for number in range(7)]
        assert all(len(batch) <= 2 for batch in index.batches)
        if lookup:
            assert _exports(drive, "doc-0") == 1, "durable documents are not exported again"
        drive.put("doc-0", b"Edited after the resume.")
        assert sorted(index.sync(owners=[ALICE])) == ["doc-0"]


def test_a_reset_cursor_rescans_without_exporting_and_loses_no_delete(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    index.sync()
    drive.touch("doc-plan", trashed=True)
    index.cursor = None
    requests = len(drive.requests)
    assert index.sync() == {}
    assert not any("export" in path for path in drive.paths()[requests:])
    assert index.cursor is not None
    assert index.listing() == ["deck-roadmap", "sheet-budget"], "the listing proves the delete"
    drive.put("sheet-budget", xlsx({"Costs": [["item"], ["new"]]}))
    assert sorted(index.sync()) == ["sheet-budget"]


def test_an_expired_token_lists_the_feed_again(drive: FakeDrive, index: Index) -> None:
    _world(drive)
    for number in range(5):
        drive.put(f"doc-{number}", b"Text.", owner=ALICE)
    index.sync()
    drive.put("doc-plan", b"Edited while the token expired.")
    drive.fail(lambda path, _: path.endswith("/changes"), 400, "invalid", "pageToken")
    assert sorted(index.sync()) == ["doc-plan"]
    drive.put("doc-0", b"Edited later.")
    assert sorted(index.sync()) == ["doc-0"]
    # A list page token that expired between two runs restarts that listing.
    fresh = Index(limits={"max_rows": 2, "max_bytes": 1 << 20, "document_max_bytes": 1 << 16})
    drive.fail(lambda path, _: path.endswith("doc-3/export"), 500, times=99)
    with pytest.raises(ApiError):
        fresh.sync(owners=[ALICE], shared_drives=[])
    drive._faults.clear()
    drive.fail(
        lambda path, params: path.endswith("/files") and "pageToken" in params,
        400,
        "invalid",
        "pageToken",
    )
    fresh.sync(owners=[ALICE], shared_drives=[])
    assert len(fresh.revisions) == 7
    drive.fail(lambda path, _: path.endswith("/changes"), 400, "invalid", "pageToken", times=99)
    with pytest.raises(ApiError):
        index.sync()


def test_configurations_the_connector_cannot_serve_are_refused(
    drive: FakeDrive, index: Index
) -> None:
    with pytest.raises(ValueError, match="google_drive_owners_required"):
        index.sync(owners=[])
    with pytest.raises(ValueError, match="google_drive_extension_unknown"):
        index.sync(extensions=["pdf"])
    for kind in ("permissions", "members"):
        with pytest.raises(ValueError, match="google_drive_fixed_reads_no_permissions"):
            list(index.run(kind))
    del index.settings["admin_email"]
    with pytest.raises(ValueError, match="google_drive_admin_email_required"):
        list(index.run(permissions="sync"))


@pytest.mark.parametrize("emptied", [True, False])
def test_a_document_created_and_then_emptied_or_trashed_in_one_sync_is_not_published(
    drive: FakeDrive, index: Index, emptied: bool
) -> None:
    drive.put("doc-x", b"Short-lived text.", owner=ALICE)

    def vanish() -> None:
        if emptied:
            drive.put("doc-x", b"")
        else:
            drive.touch("doc-x", trashed=True)

    # The second change lands on a later page of the same sync, after the export.
    drive.after(lambda path, _: path.endswith("doc-x/export"), vanish)
    index.sync(owners=[ALICE])
    assert _exports(drive, "doc-x") == (2 if emptied else 1)
    assert "doc-x" not in index.revisions, "the stale upsert must not outlive the later change"


def test_a_folder_moved_into_scope_during_the_sync_is_judged_by_its_new_parent(
    drive: FakeDrive, index: Index
) -> None:
    drive.put("in", owner=ALICE, mime=FOLDER)
    drive.put("out", owner=ALICE, mime=FOLDER)
    drive.put("doc-out", b"Outside, for now.", owner=ALICE, parent="out")
    assert index.sync(owners=[ALICE], folders=["in"]) == {}
    drive.put("doc-out", b"Edited outside.")
    for number in range(3):
        drive.put(f"doc-root-{number}", b"Elsewhere.", owner=ALICE)
    # While the first page is read, the folder moves; its change entry is on a later page.
    drive.after(lambda path, _: path.endswith("files/out"), lambda: drive.touch("out", parent="in"))
    before = len(drive.requests)
    assert sorted(index.sync(owners=[ALICE], folders=["in"])) == ["doc-out"]
    lookups = drive.paths()[before:].count("/drive/v3/files/out")
    assert lookups == 1, "the new parent came from the change entry, not from a second lookup"


def test_a_transient_ancestor_failure_during_the_listing_yields_no_complete_part(
    drive: FakeDrive,
) -> None:
    drive.put("in", owner=ALICE, mime=FOLDER)
    drive.put("sub", owner=ALICE, mime=FOLDER, parent="in")
    drive.put("doc-in", b"Inside.", owner=ALICE, parent="sub")
    assert Index().listing(owners=[ALICE], folders=["in"]) == ["doc-in"]
    drive.fail(lambda path, _: path.endswith("files/sub"), 503, times=99)
    parts = []
    with pytest.raises(ApiError) as caught:
        for part in Index().run("reconcile", owners=[ALICE], folders=["in"]):
            parts.append(part)
    assert caught.value.transient and not any(part.complete or part.last for part in parts)
