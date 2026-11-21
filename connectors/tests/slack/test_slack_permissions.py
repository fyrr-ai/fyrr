"""Approved public/private mapping, identity evidence and atomic failure paths."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from fyrr_connectors import testkit
from fyrr_connectors.connector import EmailVerification
from fyrr_connectors.runtime import encode
from fyrr_connectors.slack.client import ApiError

GROUP = "grp:slack.com:T001:workspace"
CONTAINER = "slack:T001:channel:C001"


def grants(index):
    index.mode = "sync"
    return {
        item.container_id: item
        for batch in index.run("permissions")
        for item in batch.container_grants
    }


def members(index):
    index.mode = "sync"
    batches = list(index.run("members"))
    assert batches[-1].last
    return {user.id: user for batch in batches for user in batch.users}, batches[-1].groups[0]


def test_public_floor_contains_full_humans_but_only_channel_guests(slack, slack_index):
    slack.user("U002")  # Full member, not in the channel.
    slack.user("U003", is_restricted=True)
    slack.user("U004", is_ultra_restricted=True)
    slack.user("U005", is_restricted=True)  # Guest outside the channel.
    slack.user("U006", deleted=True)
    slack.user("U007", is_bot=True)
    slack.members["C001"] = ["U001", "U003", "U004", "U006", "U007"]
    assert grants(slack_index)[CONTAINER].readers == (
        GROUP,
        "usr:slack.com:T001:U003",
        "usr:slack.com:T001:U004",
    )
    users, group = members(slack_index)
    assert group.members == ("usr:slack.com:T001:U001", "usr:slack.com:T001:U002")
    assert not users["slack.com:T001:U006"].active
    assert not users["slack.com:T001:U007"].active


@pytest.mark.parametrize(
    "flag", [None, "is_restricted", "is_ultra_restricted", "deleted", "is_bot"]
)
def test_private_grants_only_active_proven_humans(slack, slack_index, flag):
    slack.channels["C001"]["is_private"] = True
    slack.user("U002", **({flag: True} if flag else {}))
    slack.user("U003")
    slack.members["C001"] = ["U002"]
    expected = () if flag in ("deleted", "is_bot") else ("usr:slack.com:T001:U002",)
    assert grants(slack_index)[CONTAINER].readers == expected


@pytest.mark.parametrize("private", [False, True])
def test_conversion_uses_flags_after_one_paged_membership_snapshot(slack, slack_index, private):
    for identity in ("U002", "U003"):
        slack.user(identity)
    slack.members["C001"] = ["U001", "U002", "U003"]
    slack.channels["C001"]["is_private"] = not private
    slack.after(
        lambda method, params: method == "conversations.members" and params.get("cursor") == "2",
        lambda: slack.channels["C001"].update(is_private=private),
    )
    readers = grants(slack_index)[CONTAINER].readers
    assert readers == (
        tuple(f"usr:slack.com:T001:{user}" for user in ("U001", "U002", "U003"))
        if private
        else (GROUP,)
    )
    assert len([1 for method, _ in slack.requests if method == "conversations.members"]) == 2


def test_removed_private_member_revokes_all_existing_root_bindings(slack, slack_index):
    slack.channels["C001"]["is_private"] = True
    parent = slack.post()
    slack.post(thread=parent["ts"])
    slack.post(seconds=400)
    published = slack_index.sync()
    assert len(published) == 2
    assert grants(slack_index)[CONTAINER].readers == ("usr:slack.com:T001:U001",)
    slack.members["C001"] = []
    batches = list(slack_index.run("permissions"))
    assert all(not grant.readers for batch in batches for grant in batch.container_grants)
    docs = [document for batch in batches for document in batch.documents]
    assert {doc.root_id for doc in docs} == set(published)
    assert all(doc.readers == () and doc.containers == (CONTAINER,) for doc in docs)


@pytest.mark.parametrize("kind", ["permissions", "members"])
@pytest.mark.parametrize("method", ["users.list", "conversations.members"])
def test_late_snapshot_failure_yields_no_batch(slack, slack_index, kind, method):
    slack_index.mode = "sync"
    for identity in ("U002", "U003"):
        slack.user(identity)
    slack.members["C001"] = ["U001", "U002", "U003"]
    slack.after(lambda name, _: name == method, lambda: slack.fail(method, "missing_scope"))
    yielded = []
    with pytest.raises(ApiError):
        for batch in slack_index.run(kind):
            yielded.append(batch)
    assert yielded == []


@pytest.mark.parametrize("error", ["channel_not_found", "not_in_channel"])
@pytest.mark.parametrize("phase", ["acl", "listing"])
def test_authoritative_published_channel_loss_empties_grant_and_keeps_other_channel(
    slack, slack_index, error, phase
):
    slack.post()
    slack.channel("C002", "other")
    slack.post("C002", seconds=400)
    published = slack_index.sync()
    slack_index.mode = "sync"
    trigger = "users.list" if phase == "acl" else "conversations.members"
    slack.after(
        lambda name, params: name == trigger and (phase == "acl" or params["channel"] == "C002"),
        lambda: slack.fail(
            "conversations.members" if phase == "acl" else "conversations.history", error
        ),
    )
    result = grants(slack_index)
    assert result[CONTAINER].readers == ()
    assert result["slack:T001:channel:C002"].readers == (GROUP,)
    del slack.channels["C001"]
    assert slack_index.listing() == {key for key in published if ":C002:" in key}


@pytest.mark.parametrize(
    "error", ["invalid_auth", "missing_scope", "internal_error", "thread_not_found"]
)
def test_ambiguous_auth_or_transient_channel_failure_yields_no_batch(
    slack, slack_index, error
):
    slack.post()
    slack_index.sync()
    slack_index.mode = "sync"
    slack.fail("conversations.members", error, times=6)
    yielded = []
    with pytest.raises(ApiError):
        for batch in slack_index.run("permissions"):
            yielded.append(batch)
    assert yielded == []


def test_new_unavailable_channel_is_configuration_error(slack, slack_index):
    slack_index.mode = "sync"
    slack.fail("conversations.members", "not_in_channel")
    with pytest.raises(ApiError):
        list(slack_index.run("permissions"))


def test_sync_content_leaves_permission_state_to_writer(slack, slack_index):
    slack_index.mode = "sync"
    slack.post()
    changes = slack_index.sync()
    assert all(
        item["readers"] == [] and item["containers"] == [] and "permission_revision" not in item
        for item in changes.values()
    )
    assert not any(
        method in ("users.list", "conversations.members") for method, _ in slack.requests
    )
    assert grants(slack_index)[CONTAINER].readers == (GROUP,)


def test_fixed_permission_and_member_calls_do_not_touch_provider(slack, slack_index):
    assert list(slack_index.run("permissions")) == []
    assert list(slack_index.run("members")) == []
    assert slack.requests == []


@pytest.mark.parametrize("shared_flag", ["is_ext_shared", "is_org_shared", "is_shared"])
def test_shared_visibility_refused_under_sync_and_only_fixed_opt_in(
    slack, slack_index, shared_flag
):
    slack.channels["C001"][shared_flag] = True
    slack_index.mode = "sync"
    assert grants(slack_index) == {}
    slack_index.scope["include_shared"] = True
    with pytest.raises(ValueError, match="shared_requires_fixed"):
        list(slack_index.run("permissions"))
    slack_index.mode = "fixed"
    slack.post()
    assert slack_index.sync()


@pytest.mark.parametrize("flag", ["is_im", "is_mpim"])
def test_dm_visibility_never_creates_a_grant(slack, slack_index, flag):
    slack.channels["C001"][flag] = True
    with pytest.raises(ValueError, match="dm_unsupported"):
        grants(slack_index)


@pytest.mark.parametrize(
    "domains,email,expected",
    [
        ([], "person@corp.test", None),
        (["corp.test"], "person@external.test", None),
        (["corp.test"], None, None),
        (["corp.test"], "person@CORP.test", "person@CORP.test"),
        (["corp.test"], "person@sub.corp.test", None),
        (["corp.test"], "person@corp.test\n", None),
    ],
)
def test_corporate_evidence_requires_exact_approved_domain(
    slack, slack_index, domains, email, expected
):
    slack_index.settings["corporate_domains"] = domains
    slack.users["U001"]["profile"]["email"] = email
    users, _ = members(slack_index)
    user = users["slack.com:T001:U001"]
    assert user.email == expected
    assert user.email_verification == (
        EmailVerification.VERIFIED_CORPORATE if expected else EmailVerification.UNVERIFIED
    )
    slack.post()
    batches = list(slack_index.run())
    assert all(not email or email not in batch.cursor for batch in batches)
    assert all(not email or email not in batch.cursor for batch in slack_index.run("members"))
    assert all(not email or email not in batch.cursor for batch in slack_index.run("permissions"))


def test_corporate_guest_does_not_become_a_full_workspace_member(slack, slack_index):
    slack_index.settings["corporate_domains"] = ["corp.test"]
    slack.user("U002", is_restricted=True, profile={"email": "guest@corp.test"})
    users, group = members(slack_index)
    assert users["slack.com:T001:U002"].email_verification == EmailVerification.VERIFIED_CORPORATE
    assert "usr:slack.com:T001:U002" not in group.members
    assert grants(slack_index)[CONTAINER].readers == (GROUP,)


def test_email_change_replaces_evidence_without_changing_numeric_principal(slack, slack_index):
    slack_index.settings["corporate_domains"] = ["corp.test"]
    slack.users["U001"]["profile"]["email"] = "old@corp.test"
    first, _ = members(slack_index)
    slack.users["U001"]["profile"]["email"] = "new@external.test"
    second, _ = members(slack_index)
    assert first.keys() == second.keys() and second["slack.com:T001:U001"].email is None


def test_foreign_unknown_or_malformed_member_fails_closed(slack, slack_index):
    slack.user("U002", team_id="T002", is_stranger=True)
    slack.members["C001"] = ["U002"]
    with pytest.raises(ValueError, match="workspace_or_flags"):
        grants(slack_index)


@pytest.mark.parametrize("invalid", [None, 123, "not-an-id"])
def test_bad_member_identity_never_grants(slack, slack_index, invalid):
    slack.members["C001"] = [invalid]
    with pytest.raises((ValueError, ApiError)):
        grants(slack_index)


def test_old_workspace_container_is_emptied_after_scope_change(slack, slack_index):
    key = "slack:T002:C001:window:1767225600000000"
    slack_index.revisions[key] = "old"
    assert grants(slack_index)["slack:T002:channel:C001"].readers == ()


@pytest.mark.parametrize("kind", ["permissions", "members"])
def test_frame_bound_preflight_yields_nothing(slack, slack_index, kind):
    slack_index.mode = "sync"
    slack_index.limits = replace(slack_index.limits, max_bytes=100)
    yielded = []
    with pytest.raises(ValueError, match="envelope_limit"):
        for batch in slack_index.run(kind):
            yielded.append(batch)
    assert yielded == []


def test_members_contract_golden(slack, slack_index):
    slack_index.mode = "sync"
    slack.user("U002", is_restricted=True)
    slack.user("U003", deleted=True)
    parts = []
    for batch in slack_index.run("members"):
        header, _ = encode("slack", "members", None, batch)
        header.pop("cursor")
        header["body"]["snapshot_id"] = "<snapshot>"
        parts.append(header)
    testkit.assert_golden(
        testkit.SyncResult(kind="members", documents=parts),
        Path(__file__).parent / "golden" / "members.json",
    )
    assert "email@" not in json.dumps(parts)


def test_missing_user_flags_cannot_form_a_workspace_group(slack, slack_index):
    del slack.users["U001"]["is_restricted"]
    with pytest.raises(ValueError, match="workspace_or_flags"):
        members(slack_index)


def test_member_missing_from_list_is_proved_without_expanding_workspace_floor(slack, slack_index):
    slack.user("U002")
    slack.members["C001"] = ["U002"]
    original = slack._answer

    def answer(method, params):
        if method == "users.list":
            return slack._page([slack.users["U001"]], params, "members")
        return original(method, params)

    slack._answer = answer
    assert grants(slack_index)[CONTAINER].readers == (GROUP,)
    users, group = members(slack_index)
    assert "slack.com:T001:U002" in users
    assert group.members == ("usr:slack.com:T001:U001",)
    slack.channels["C001"]["is_private"] = True
    assert grants(slack_index)[CONTAINER].readers == ("usr:slack.com:T001:U002",)


@pytest.mark.parametrize("kind", ["permissions", "members"])
def test_rate_limit_exhaustion_raises_after_bounded_retries(slack, slack_index, kind):
    slack_index.mode = "sync"
    slack.fail("users.list", times=6, status=429, headers={"Retry-After": "1"})
    with pytest.raises(ApiError, match="rate_limited"):
        next(iter(slack_index.run(kind)))
    assert len(slack.sleeps) == 5
    assert all(1 <= delay <= 2 for delay in slack.sleeps)


@pytest.mark.parametrize("kind", ["permissions", "members"])
def test_reader_bounds_fail_before_any_snapshot_part(slack, slack_index, monkeypatch, kind):
    from fyrr_connectors.slack import permissions

    monkeypatch.setattr(permissions, "MAX_READERS", 2)
    slack_index.mode = "sync"
    for user in ("U002", "U003"):
        slack.user(user)
    with pytest.raises(ValueError, match="workspace_group_limit"):
        next(iter(slack_index.run(kind)))


def test_all_member_frames_are_preflighted_before_first_yield(slack, slack_index):
    slack_index.mode = "sync"
    slack.page_size = 100
    for number in range(2, 1001):
        slack.user(f"U{number:03}")
    # The first 500-user frame fits; the last frame's workspace group does not.
    slack_index.limits = replace(slack_index.limits, max_bytes=40000)
    with pytest.raises(ValueError, match="envelope_limit"):
        next(iter(slack_index.run("members")))


def test_no_provider_reads_after_member_yield_begins(slack, slack_index):
    slack_index.mode = "sync"
    slack.page_size = 100
    for number in range(2, 503):
        slack.user(f"U{number:03}")
    iterator = iter(slack_index.run("members"))
    first = next(iterator)
    assert not first.last
    requests = list(slack.requests)
    second = next(iterator)
    assert second.last and second.groups
    assert slack.requests == requests
