"""`permissions = "sync"`: the mapping, the permission stream and
the membership snapshot (block B)."""

from pathlib import Path

import pytest
from conftest import ALICE, BOB, SETTINGS, Index, pptx
from fake_drive import ADMIN, FOLDER, SLIDES, FakeDrive

from fyrr_connectors import EmailVerification, SourceGroup, SourceUser, testkit
from fyrr_connectors.google_drive import connector
from fyrr_connectors.google_drive.client import ApiError
from fyrr_connectors.google_drive.permissions import principals, reader, readers, revision

GOLDEN = Path(__file__).parent / "golden"
CAROL = "carol@example.com"


def _p(kind: str, principal: str | None = None, **more: object) -> dict:
    key = "domain" if kind == "domain" else "emailAddress"
    return {"id": f"p-{principal or kind}", "type": kind, "role": "reader", key: principal} | more


@pytest.mark.parametrize(
    ("permission", "token"),
    [
        (_p("user", "Alice@Example.com", role="owner"), "usr:alice@example.com"),
        *[(_p("user", BOB, role=r), f"usr:{BOB}") for r in ("writer", "commenter", "reader")],
        (
            _p("user", "partner@elsewhere.example", role="fileOrganizer"),
            "usr:partner@elsewhere.example",
        ),
        (_p("group", "Eng@Example.com", role="organizer"), "grp:eng@example.com"),
        (_p("domain", "Example.com", allowFileDiscovery=True), "grp:domain:example.com"),
        (_p("domain", "example.com", allowFileDiscovery=False), None),
        (_p("domain", "example.com"), None),
        (_p("anyone", allowFileDiscovery=False), None),
        (_p("anyone", allowFileDiscovery=True), None),
        (_p("user", BOB, deleted=True), None),
        (_p("user", None), None),
        (_p("user", "two words@example.com"), None),
        (_p("user", BOB, view="published"), None),
        (_p("user", BOB, expirationTime="2026-12-31T00:00:00Z"), None),
        (_p("group", "eng@example.com", expirationTime="2026-12-31T00:00:00Z"), None),
        (_p("serviceAccount", "robot@example.com"), None),
    ],
)
def test_each_permission_kind_maps_to_one_token_or_none(
    permission: dict, token: str | None
) -> None:
    assert reader(permission) == token


def test_shared_drive_members_stay_off_the_rows() -> None:
    inherited = [{"inherited": True, "inheritedFrom": "drive-eng"}]
    direct, folder = [{"inherited": False}], [{"inherited": True, "inheritedFrom": "folder"}]
    member = _p("user", ALICE, permissionDetails=inherited)
    team = _p("group", "eng@example.com", permissionDetails=inherited)
    drive = frozenset({member["id"], team["id"]})
    guest = _p("user", CAROL, permissionDetails=direct)
    from_folder = _p("group", "ops@example.com", permissionDetails=folder)

    def mapped(*permissions: dict) -> tuple[list[str], bool]:
        return readers(permissions, drive, "drive-eng")

    assert mapped(member, team, guest, from_folder) == (
        ["grp:ops@example.com", f"usr:{CAROL}"],
        True,
    )
    # Under a limited-access folder a member is missing: no container, and the
    # members still listed are dropped rather than frozen onto the rows.
    assert mapped(member, guest) == ([f"usr:{CAROL}"], False)
    both = _p("user", ALICE, permissionDetails=[*inherited, *direct])
    assert mapped(both, team, guest) == ([f"usr:{ALICE}", f"usr:{CAROL}"], True)
    # Every member is on the file, but one of them by a grant of their own or
    # through a folder: the API does not state that the drive is inherited.
    for other in (direct, folder, [], [{"inherited": True}]):
        granted = _p("group", "eng@example.com", permissionDetails=other)
        tokens, inherits = mapped(member, granted)
        assert not inherits
        assert tokens == (["grp:eng@example.com"] if other in (direct, folder) else [])
    assert readers([member, guest]) == ([f"usr:{ALICE}", f"usr:{CAROL}"], False)
    assert revision(["b", "a"], ["d"]) == revision(["a", "b"], ["d"]) != revision(["a", "b"])


def _world(drive: FakeDrive) -> None:
    drive.drive("drive-eng", "Engineering", [ALICE])
    drive.join("drive-eng", type="group", emailAddress="eng@example.com")
    drive.put("doc-plan", b"# Plan\n\nShip it.\n", owner=ALICE, name="Plan")
    drive.share("doc-plan", type="user", emailAddress=BOB, role="writer")
    drive.share("doc-plan", type="group", emailAddress="eng@example.com")
    drive.share("doc-plan", type="domain", domain="example.com", allowFileDiscovery=True)
    drive.share("doc-plan", type="anyone", allowFileDiscovery=False)
    drive.put("doc-private", b"Private.", owner=BOB)
    drive.put("deck-roadmap", pptx([["Roadmap"]]), drive="drive-eng", mime=SLIDES)
    drive.share("deck-roadmap", type="user", emailAddress=CAROL)


def _state(documents: dict) -> dict:
    return {root: (list(d.readers), list(d.containers)) for root, d in documents.items()}


def test_the_permission_stream_maps_every_document_and_drive(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    documents, grants = index.permissions()
    assert _state(documents) == {
        "doc-plan": (
            ["grp:domain:example.com", "grp:eng@example.com", f"usr:{ALICE}", f"usr:{BOB}"],
            [],
        ),
        "doc-private": ([f"usr:{BOB}"], []),
        "deck-roadmap": ([f"usr:{CAROL}"], ["drive-eng"]),
    }
    assert {k: (g.container_kind, list(g.readers)) for k, g in grants.items()} == {
        "drive-eng": ("drive", ["grp:eng@example.com", f"usr:{ALICE}"])
    }
    plan = documents["doc-plan"]
    assert plan.permission_revision == revision(plan.readers)
    # My Drive files carry their permissions inline: no list call for them.
    listed = [path for path in drive.paths() if path.endswith("/permissions")]
    assert sorted(set(listed)) == [
        "/drive/v3/files/deck-roadmap/permissions",
        "/drive/v3/files/drive-eng/permissions",
    ]
    batches = index.permission_batches
    assert index.permissions() == ({}, {}) and index.permission_batches == batches


def test_the_stream_matches_the_published_contract_and_golden(drive: FakeDrive) -> None:
    _world(drive)
    result = testkit.run_sync(
        "google-drive", "permissions", settings=SETTINGS, scope={}, permissions="sync"
    )
    testkit.assert_golden(result, GOLDEN / "permissions.json")
    members = testkit.run_sync(
        "google-drive", "members", settings=SETTINGS, scope={}, permissions="sync"
    )
    assert members.documents[-1]["body"]["last"]


def test_content_under_sync_carries_no_readers_and_reads_no_permissions(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    changes = index.sync(permissions="sync")
    assert sorted(changes) == ["deck-roadmap", "doc-plan", "doc-private"]
    for change in changes.values():
        assert change["readers"] == [] and change["containers"] == []
        assert "permission_revision" not in change
    assert not any("permission" in f"{path} {params}".lower() for _, path, params in drive.requests)
    assert index.listing(permissions="sync") == sorted(changes)


def test_grants_and_revokes_arrive_through_the_stream(drive: FakeDrive, index: Index) -> None:
    _world(drive)
    first, _ = index.permissions()
    drive.share("doc-private", type="user", emailAddress=ALICE)
    drive.unshare("doc-plan", f"p-{BOB}")
    drive.put("doc-plan", b"# Plan\n\nEdited, same sharing.\n")
    documents, grants = index.permissions()
    assert _state(documents) == {
        "doc-private": ([f"usr:{ALICE}", f"usr:{BOB}"], []),
        "doc-plan": (["grp:domain:example.com", "grp:eng@example.com", f"usr:{ALICE}"], []),
    }
    assert grants == {}
    assert documents["doc-plan"].permission_revision != first["doc-plan"].permission_revision


def test_shared_drive_membership_changes_without_any_file_change(
    drive: FakeDrive, index: Index
) -> None:
    _world(drive)
    index.permissions()
    drive.join("drive-eng", type="user", emailAddress=BOB)
    documents, grants = index.permissions()
    assert documents == {}
    assert list(grants["drive-eng"].readers) == [
        "grp:eng@example.com",
        f"usr:{ALICE}",
        f"usr:{BOB}",
    ]
    drive.leave("drive-eng", f"p-{BOB}")
    documents, grants = index.permissions()
    assert documents == {}
    assert list(grants["drive-eng"].readers) == ["grp:eng@example.com", f"usr:{ALICE}"]
    assert index.permissions() == ({}, {}), "an unchanged drive is not handed over again"
    # A reset cursor reads everything again.
    index.permission_cursor = None
    documents, grants = index.permissions()
    assert (
        sorted(documents) == ["deck-roadmap", "doc-plan", "doc-private"] and "drive-eng" in grants
    )


def test_a_changed_folder_rereads_the_files_beneath_it(drive: FakeDrive, index: Index) -> None:
    drive.put("folder", owner=ALICE, mime=FOLDER)
    drive.put("sub", owner=ALICE, mime=FOLDER, parent="folder")
    drive.put("doc-deep", b"Deep.", owner=ALICE, parent="sub")
    drive.put("doc-else", b"Elsewhere.", owner=ALICE)
    index.permissions()
    requests = len(drive.logs[ALICE])
    drive.share("folder", type="user", emailAddress=BOB)
    assert drive.logs[ALICE][requests:] == ["folder"], "the file has no change entry of its own"
    documents, _ = index.permissions()
    assert _state(documents) == {"doc-deep": ([f"usr:{ALICE}", f"usr:{BOB}"], [])}
    drive.unshare("folder", f"p-{BOB}")
    documents, _ = index.permissions()
    assert _state(documents) == {"doc-deep": ([f"usr:{ALICE}"], [])}


def test_a_limited_access_folder_never_inherits_the_drive(drive: FakeDrive, index: Index) -> None:
    drive.drive("drive-eng", "Engineering", [ALICE])
    drive.join("drive-eng", type="user", emailAddress=BOB)
    drive.put("vault", drive="drive-eng", mime=FOLDER)
    drive.files["vault"]["limited"] = True
    drive.put("doc-secret", b"Secret.", drive="drive-eng", parent="vault")
    drive.share("doc-secret", type="user", emailAddress=CAROL)
    drive.put("doc-open", b"Open.", drive="drive-eng")
    documents, grants = index.permissions()
    assert _state(documents) == {
        "doc-secret": ([f"usr:{CAROL}"], []),
        "doc-open": ([], ["drive-eng"]),
    }
    assert list(grants["drive-eng"].readers) == [f"usr:{ALICE}", f"usr:{BOB}"]


def test_explicit_grants_to_every_member_are_not_drive_inheritance(
    drive: FakeDrive, index: Index
) -> None:
    """Under a limited-access folder the file shows every drive member, the
    manager by inheritance and Bob by a grant of his own: that is not the
    drive's membership, so a member who joins later must not be admitted."""
    drive.drive("drive-eng", "Engineering", [ALICE])
    drive.join("drive-eng", type="user", emailAddress=BOB)
    drive.put("vault", drive="drive-eng", mime=FOLDER)
    drive.files["vault"]["limited"] = True
    drive.put("doc-secret", b"Secret.", drive="drive-eng", parent="vault")
    drive.share("doc-secret", type="user", emailAddress=BOB)
    documents, _ = index.permissions()
    assert _state(documents) == {"doc-secret": ([f"usr:{BOB}"], [])}
    drive.join("drive-eng", type="user", emailAddress=CAROL)
    drive.put("doc-secret", b"Secret, edited.")
    documents, grants = index.permissions()
    assert _state(documents) in ({}, {"doc-secret": ([f"usr:{BOB}"], [])})
    assert f"usr:{CAROL}" in grants["drive-eng"].readers


def test_inheritance_disabled_above_the_file_refuses_the_container(
    drive: FakeDrive, index: Index
) -> None:
    """Every member is a manager, so the file under the limited-access folder
    still shows them all as inherited from the drive. A reader who joins the
    drive later cannot open it: no container."""
    drive.drive("drive-eng", "Engineering", [ALICE, BOB])
    drive.put("vault", drive="drive-eng", mime=FOLDER)
    drive.files["vault"]["limited"] = True
    drive.put("deep", drive="drive-eng", mime=FOLDER, parent="vault")
    drive.put("doc-secret", b"Secret.", drive="drive-eng", parent="deep")
    drive.put("doc-open", b"Open.", drive="drive-eng")
    documents, _ = index.permissions()
    assert _state(documents) == {"doc-secret": ([], []), "doc-open": ([], ["drive-eng"])}


def test_an_unreadable_ancestor_refuses_the_container(drive: FakeDrive, index: Index) -> None:
    drive.drive("drive-eng", "Engineering", [ALICE])
    drive.put("box", drive="drive-eng", mime=FOLDER)
    drive.put("doc-boxed", b"Boxed.", drive="drive-eng", parent="box")
    drive.fail(lambda path, _: path.endswith("/files/box"), 404, "notFound", times=99)
    documents, _ = index.permissions()
    assert _state(documents) == {"doc-boxed": ([], [])}


def test_long_and_cut_permission_lists_are_read_to_the_end(drive: FakeDrive, index: Index) -> None:
    drive.drive("drive-eng", "Engineering", [ALICE])
    drive.put("deck", pptx([["x"]]), drive="drive-eng", mime=SLIDES)
    drive.put("doc", b"x", owner=ALICE)
    guests = [f"guest{number}@example.com" for number in range(5)]
    for guest in guests:
        drive.share("deck", type="user", emailAddress=guest)
        drive.share("doc", type="user", emailAddress=guest)
    drive.inline_permissions = 3
    documents, _ = index.permissions()
    assert list(documents["deck"].readers) == [f"usr:{guest}" for guest in guests]
    assert list(documents["doc"].readers) == sorted(f"usr:{user}" for user in [ALICE, *guests])
    assert "/drive/v3/files/doc/permissions" in drive.paths(), "a cut inline list is listed in full"


@pytest.mark.parametrize(
    ("failing", "status", "reason"),
    [
        (lambda path, params: path.endswith("drive-eng/permissions"), 500, ""),
        (
            lambda path, params: path.endswith("drive-eng/permissions"),
            403,
            "insufficientFilePermissions",
        ),
        (
            lambda path, params: path.endswith("deck-roadmap/permissions"),
            403,
            "insufficientFilePermissions",
        ),
        (lambda path, params: path.endswith("deck-roadmap/permissions"), 429, "rateLimitExceeded"),
        (
            lambda path, params: (
                path.endswith("deck-roadmap/permissions") and "pageToken" in params
            ),
            500,
            "",
        ),
        (lambda path, params: path.endswith("/users"), 503, ""),
    ],
)
def test_a_failed_or_partial_permission_read_yields_nothing_from_it(
    drive: FakeDrive, index: Index, failing, status: int, reason: str
) -> None:
    _world(drive)
    for number in range(3):
        drive.share("deck-roadmap", type="user", emailAddress=f"guest{number}@example.com")
    drive.fail(failing, status, reason, times=99)
    documents: dict = {}
    with pytest.raises(ApiError):
        for batch in index.run("permissions", "sync"):
            documents.update({document.root_id: document for document in batch.documents})
            index.permission_cursor = batch.cursor
    assert "deck-roadmap" not in documents
    drive._faults.clear()
    documents, _ = index.permissions()
    assert len(documents["deck-roadmap"].readers) == 4, "the next run reads it in full"


def test_a_file_that_vanished_is_skipped_not_a_failure(drive: FakeDrive, index: Index) -> None:
    _world(drive)
    drive.fail(lambda path, _: path.endswith("deck-roadmap/permissions"), 404, "notFound")
    documents, _ = index.permissions()
    assert sorted(documents) == ["doc-plan", "doc-private"]


def test_only_active_directory_users_are_read(drive: FakeDrive, index: Index) -> None:
    _world(drive)
    drive.user("dana@example.com", suspended=True)
    drive.user("erin@example.com", archived=True)
    drive.put("doc-dana", b"Suspended owner.", owner="dana@example.com")
    assert sorted(index.sync(permissions="sync", owners=[])) == [
        "deck-roadmap",
        "doc-plan",
        "doc-private",
    ]
    subjects = {subject for subject, _, _ in drive.requests}
    assert subjects == {ADMIN, ALICE, BOB}
    assert sorted(Index().sync(permissions="sync", owners=[BOB, "dana@example.com"])) == [
        "doc-private"
    ]


def test_the_membership_snapshot_holds_users_groups_and_domains(
    drive: FakeDrive, index: Index, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive.user("dana@example.com", suspended=True)
    drive.user("frank@other.example")
    drive.group(
        "eng@example.com",
        (ALICE, "USER"),
        ("leads@example.com", "GROUP"),
        ("x@out.example", "USER"),
    )
    drive.group("leads@example.com", (BOB, "USER"))
    drive.group("all@example.com", ("", "CUSTOMER"))
    monkeypatch.setattr(connector, "MEMBER_ROWS", 3)
    parts = list(index.run("members", "sync"))
    assert [part.last for part in parts] == [False, False, True]
    assert len({part.snapshot_id for part in parts}) == 1 and [p.part for p in parts] == [0, 1, 2]
    users = [user for part in parts for user in part.users]
    groups = {group.id: list(group.members) for part in parts for group in part.groups}
    assert [(user.id, user.active) for user in users] == [
        (ALICE, True),
        (BOB, True),
        ("dana@example.com", False),
        ("frank@other.example", True),
    ]
    assert all(user.email_verification is EmailVerification.VERIFIED_CORPORATE for user in users)
    assert groups == {
        "eng@example.com": ["grp:leads@example.com", f"usr:{ALICE}", "usr:x@out.example"],
        "leads@example.com": [f"usr:{BOB}"],
        "all@example.com": ["grp:domain:example.com", "grp:domain:other.example"],
        "domain:example.com": [f"usr:{ALICE}", f"usr:{BOB}"],
        "domain:other.example": ["usr:frank@other.example"],
    }
    for failing in ("/users", "/groups", "/members"):
        drive.fail(lambda path, _, failing=failing: path.endswith(failing), 500, times=99)
        with pytest.raises(ApiError):
            assert not list(index.run("members", "sync")), "no part without a complete read"
        drive._faults.clear()


def test_principals_are_plain_values() -> None:
    users, groups = principals(
        [{"primaryEmail": "A@Example.com"}, {"primaryEmail": None}], {"g@example.com": []}
    )
    assert users == [
        SourceUser(
            id="a@example.com",
            active=True,
            email="a@example.com",
            email_verification=EmailVerification.VERIFIED_CORPORATE,
        )
    ]
    assert groups == [
        SourceGroup(id="g@example.com", members=[]),
        SourceGroup(id="domain:example.com", members=["usr:a@example.com"]),
    ]
