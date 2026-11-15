"""The approved permission table, one row and failure mode at a time."""

from dataclasses import replace

import pytest

from fyrr_connectors.github.client import ATTEMPTS, ApiError


def grants(index):
    return {
        grant.container_id: grant
        for batch in index.run("permissions")
        for grant in batch.container_grants
    }


@pytest.mark.parametrize(
    "origin",
    [
        "direct",
        "outside",
        "team",
        "nested_team",
        "org_owner",
        "base_read",
        "base_write",
        "base_admin",
    ],
)
def test_effective_readers_are_flat_stable_users(github, index, origin):
    index.mode = "sync"
    github.collaborator(
        1, 7, origin=origin, login="a-login", email="public@example.test", role_name="custom-role"
    )
    grant = grants(index)["github:repo:1"]
    assert grant.readers == ("usr:github.com:7",)
    assert grant.container_kind == "github_repository"
    members = list(index.run("members"))
    users = [user for batch in members for user in batch.users]
    assert [user.id for user in users] == ["github.com:7"]
    assert all(user.email is None and user.email_verification == "unverified" for user in users)
    assert all(not batch.groups for batch in members)


def test_personal_owner_is_proven_by_numeric_metadata(github, index):
    index.mode = "sync"
    github.repos[1]["owner"]["type"] = "User"
    assert grants(index)["github:repo:1"].readers == ("usr:github.com:30",)


def test_base_none_bots_unknown_types_and_no_read_grant_nobody(github, index):
    index.mode = "sync"
    github.collaborator(1, 7, read=False, role_name="admin")
    github.collaborator(1, 8, kind="Bot")
    github.collaborator(1, 9, kind="Unknown")
    assert grants(index)["github:repo:1"].readers == ()
    assert list(index.run("members"))[-1].users == ()


@pytest.mark.parametrize("visibility", ["public", "internal"])
@pytest.mark.parametrize("kind", ["content", "permissions", "members", "reconcile"])
def test_sync_refuses_visibility_floors(github, index, visibility, kind):
    index.mode = "sync"
    github.repos[1]["visibility"] = visibility
    with pytest.raises(ValueError, match="sync_private_only"):
        list(index.run(kind))
    assert not any("collaborators" in path for _, path, _ in github.requests)


@pytest.mark.parametrize("visibility", ["public", "internal", "private"])
def test_fixed_can_read_and_never_runs_authorization_feeds(github, index, visibility):
    github.repos[1]["visibility"] = visibility
    github.topic()
    assert index.sync()
    before = list(github.requests)
    assert list(index.run("permissions")) == []
    assert list(index.run("members")) == []
    assert github.requests == before
    assert not any(
        "collaborators" in path or "/members" in path or "/teams" in path for _, path, _ in before
    )


def test_sync_content_makes_no_permission_statement(github, index):
    index.mode = "sync"
    github.topic()
    github.branch(1, {"notes.md": "Notes."})
    changes = index.sync()
    assert len(changes) == 2
    assert all(
        change["readers"] == []
        and change["containers"] == []
        and "permission_revision" not in change
        for change in changes.values()
    )
    assert all(
        "readers" not in section for change in changes.values() for section in change["sections"]
    )
    permissions = list(index.run("permissions"))
    documents = [document for batch in permissions for document in batch.documents]
    assert {document.root_id for document in documents} == set(changes)
    assert all(
        document.readers == () and document.containers == ("github:repo:1",)
        for document in documents
    )


def test_login_rename_and_reassignment_never_transfer_user_grant(github, index):
    index.mode = "sync"
    github.collaborator(1, 7, login="old")
    first = grants(index)["github:repo:1"]
    github.acls[1][0]["login"] = "renamed"
    assert grants(index)["github:repo:1"].permission_revision == first.permission_revision
    github.acls[1] = []
    github.collaborator(1, 8, login="old")
    assert grants(index)["github:repo:1"].readers == ("usr:github.com:8",)


def test_later_contradicting_permission_entry_revokes(github, index):
    index.mode = "sync"
    github.collaborator(1, 7)
    github.collaborator(1, 8)
    github.collaborator(1, 7, read=False)
    assert grants(index)["github:repo:1"].readers == ("usr:github.com:8",)


@pytest.mark.parametrize("status", [401, 403, 404, 429, 503])
@pytest.mark.parametrize("kind", ["permissions", "members"])
def test_failed_acl_pass_publishes_no_partial_state(github, index, status, kind):
    index.mode = "sync"
    github.collaborator(1, 7)
    github.repo(2)
    github.branch(2, {})
    index.scope["repositories"].append({"id": 2, "branch": "main"})
    github.fail("/repos/synthetic/repo-2/collaborators", status, times=ATTEMPTS)
    parts = []
    with pytest.raises((ApiError, ValueError)):
        for batch in index.run(kind):
            parts.append(batch)
    assert parts == [], "no freshness can be renewed before all ACL reads succeed"


def test_malformed_permissions_fail_instead_of_empty_acl_success(github, index):
    index.mode = "sync"
    github.collaborator(1, 7)
    github.acls[1][0]["permissions"] = {"pull": "true"}
    with pytest.raises(ValueError, match="permissions_incomplete"):
        list(index.run("permissions"))


def test_revoked_collaborator_and_removed_repo_get_empty_grant(github, index):
    github.topic()
    index.sync()
    index.mode = "sync"
    github.collaborator(1, 7)
    assert grants(index)["github:repo:1"].readers
    github.acls[1].clear()
    assert grants(index)["github:repo:1"].readers == ()
    github.repos[1]["installed"] = False
    assert grants(index)["github:repo:1"].readers == ()


def test_complete_member_union_deduplicates_repositories(github, index):
    index.mode = "sync"
    github.repo(2)
    github.branch(2, {})
    index.scope["repositories"].append({"id": 2, "branch": "main"})
    for repo in (1, 2):
        github.collaborator(repo, 7)
    index.limits = replace(index.limits, max_rows=1)
    parts = list(index.run("members"))
    assert len(parts) == 1 and parts[0].last
    assert [user.id for user in parts[0].users] == ["github.com:7"]


def test_authorization_frames_are_bounded_before_first_yield(github, index):
    index.mode = "sync"
    for user in range(1, 20):
        github.collaborator(1, user)
    index.limits = replace(index.limits, max_bytes=128)
    for kind in ("permissions", "members"):
        stream = index.run(kind)
        with pytest.raises(ValueError, match="batch_limit"):
            next(stream)


def test_review_with_invalid_submitted_time_is_not_publication_evidence(github, index):
    github.topic(kind="pr")
    review = github.comment(kind="pr", state="APPROVED", body="Unproven review.")
    review["submitted_at"] = True
    with pytest.raises(ValueError, match="review_invalid"):
        index.sync()


def test_boolean_inline_review_reference_cannot_inherit_submitted_status(github, index):
    github.topic(kind="pr")
    github.comment(kind="pr", state="APPROVED")
    github.comment(kind="pr", review_id=True, body="Unproven inline.")
    with pytest.raises(ValueError, match="review_invalid"):
        index.sync()


def test_permission_and_member_contract_goldens(github, index, monkeypatch):
    from pathlib import Path

    from fyrr_connectors import testkit
    from fyrr_connectors.github import GitHub, connector
    from fyrr_connectors.provider import provider_info

    monkeypatch.setattr(connector.time, "time_ns", lambda: 123)
    github.topic()
    github.collaborator(1, 7, email="unverified@example.test")
    info = provider_info(name="github", connector=GitHub, min_sdk_version="0.1.0")
    for kind in ("permissions", "members"):
        result = testkit.run_sync(
            info, kind, permissions="sync", settings=index.settings, scope=index.scope
        )
        testkit.assert_golden(result, Path(__file__).parent / f"golden/{kind}.json")
