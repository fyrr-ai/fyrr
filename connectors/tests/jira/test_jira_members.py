"""Complete member snapshots, identity evidence and bounded policies."""

import pytest

from fyrr_connectors.connector import EmailVerification
from fyrr_connectors.jira.client import ApiError
from fyrr_connectors.jira.members import Directory
from fyrr_connectors.jira.permissions import PolicyGraph


@pytest.mark.parametrize(
    "domain,email,verified",
    [
        ([], "synthetic@corp.invalid", False),
        (["corp.invalid"], "synthetic@corp.invalid", True),
        (["corp.invalid"], "synthetic@external.invalid", False),
        (["corp.invalid"], None, False),
    ],
)
def test_jira_email_evidence_is_opt_in_returned_corporate_only(
    jira, jira_index, domain, email, verified
):
    jira_index.mode = "sync"
    jira_index.settings["corporate_domains"] = domain
    jira.users["U001"]["emailAddress"] = email
    jira.issue(101)
    batches = list(jira_index.run("members"))
    user = next(u for b in batches for u in b.users if u.id.endswith(":U001"))
    assert user.email == (email if verified else None)
    assert user.email_verification == (
        EmailVerification.VERIFIED_CORPORATE if verified else EmailVerification.UNVERIFIED
    )
    assert email is None or all(email not in b.cursor for b in batches)
    assert all("U001" not in b.cursor for b in batches)


@pytest.mark.parametrize("active,kind", [(False, "atlassian"), (True, "app"), (True, "customer")])
def test_jira_inactive_app_and_portal_accounts_never_receive_policy_groups(
    jira, jira_index, active, kind
):
    jira_index.mode = "sync"
    jira.users["U001"].update(active=active, accountType=kind)
    jira.issue(101)
    batches = list(jira_index.run("members"))
    assert not any(g.members for b in batches for g in b.groups)
    assert all(not u.active for b in batches for u in b.users)


@pytest.mark.parametrize(
    "path",
    [
        "/users/search",
        "/group/member",
        "/project/1/permissionscheme",
        "/issuesecurityschemes/level/member",
    ],
)
def test_jira_incomplete_acl_reads_yield_nothing(jira, jira_index, path):
    jira_index.mode = "sync"
    jira.user("U002")
    jira.user("U003")
    jira.group("g1", ["U001", "U002", "U003"])
    jira.levels[1][20] = [{"type": "group", "value": "g1"}]
    jira.issue(101, security={"id": "20"})
    if path in ("/users/search", "/group/member", "/issuesecurityschemes/level/member"):
        jira.after(lambda m, p, a, b: p == path, lambda: jira.fail(path, 503, times=6))
    else:
        jira.fail(path, 403)
    iterator = jira_index.run("permissions")
    with pytest.raises(ApiError):
        next(iterator)


def test_jira_company_managed_only_sync_fixed_accepts_other_project_types(jira, jira_index):
    jira.issue(101)
    for attrs in ({"simplified": True}, {"projectTypeKey": "service_desk"}):
        jira.projects[1].update(attrs)
        jira_index.mode = "sync"
        with pytest.raises(ValueError, match="company_managed_only"):
            list(jira_index.run("permissions"))
        jira_index.mode = "fixed"
        jira_index.sync()


def test_jira_fixed_all_kinds_make_no_acl_reads(jira, jira_index):
    jira.issue(101)
    assert list(jira_index.run("permissions")) == []
    assert list(jira_index.run("members")) == []
    jira_index.sync()
    jira_index.listing()
    assert not any(
        any(
            token in path
            for token in (
                "/user",
                "/group",
                "/role",
                "permissionscheme",
                "securitylevelscheme",
                "issuesecurityschemes",
            )
        )
        for _, path, _ in jira.requests
    )


def test_jira_token_ceiling_fails_explicitly_instead_of_truncating(jira, jira_index):
    graph = PolicyGraph(
        None, type("Settings", (), {"cloud_id": jira.cloud})(), Directory(jira.users)
    )
    graph.audiences = {f"policy-{i}": {"U001"} for i in range(5000)}
    with pytest.raises(ValueError, match="subject_token_limit_5000"):
        graph.finish()


def test_jira_permission_reader_identity_cannot_come_from_another_project(jira, jira_index):
    jira_index.mode = "sync"
    jira.schemes[1] = [
        {"permission": "BROWSE_PROJECTS", "holder": {"type": "projectRole", "value": "10"}}
    ]
    jira.roles[1][10]["id"] = 20
    jira.issue(101)
    with pytest.raises(ValueError, match="role_identity_mismatch"):
        next(jira_index.run("permissions"))


def test_jira_binding_group_ids_are_retained_outside_the_cursor(jira, jira_index):
    jira_index.mode = "sync"
    jira.schemes[1] = [
        {
            "permission": "BROWSE_PROJECTS",
            "holder": {"type": "groupCF", "value": "customfield_10000"},
        }
    ]
    jira.issue(101, customfield_10000={"groupId": "g1"})
    jira_index.sync()
    from fyrr_connectors.jira.feeds import IssueBinding

    binding = IssueBinding.from_revision(next(iter(jira_index.revisions.values())))
    assert binding.groups == ("g1",)
    assert "g1" not in jira_index.cursor and "U001" not in jira_index.cursor


def test_jira_logged_in_holder_publishes_licensed_workspace_group(jira, jira_index):
    jira_index.mode = "sync"
    jira.user("U002", accountType="customer")
    jira.groups["g1"]["members"].append("U002")
    jira.applications = [{"key": "jira-software", "groupDetails": [{"groupId": "g1"}]}]
    jira.schemes[1] = [
        {"permission": "BROWSE_PROJECTS", "holder": {"type": "applicationRole", "parameter": ""}}
    ]
    jira.issue(101)
    group = next(
        g for b in jira_index.run("members") for g in b.groups if g.id.endswith(":licensed")
    )
    assert group.members == ("usr:atlassian.net:" + jira.cloud + ":U001",)


def test_jira_policy_group_members_are_source_user_tokens(jira_index):
    jira_index.mode = "sync"
    batches = list(jira_index.run("members"))
    members = [member for batch in batches for group in batch.groups for member in group.members]
    assert members
    assert all(member.startswith("usr:") for member in members)
    users = {user.id for batch in batches for user in batch.users}
    assert all(member[4:] in users for member in members)


@pytest.mark.parametrize("kind", ["permissions", "members"])
def test_jira_sync_batches_honor_the_context_row_limit(jira, jira_index, kind):
    from fyrr_connectors.connector import Limits

    jira.issue(101)
    jira.issue(102)
    jira.user("U002")
    jira_index.mode = "sync"
    jira_index.limits = Limits(max_rows=1, max_bytes=1 << 20, document_max_bytes=65536)
    batches = list(jira_index.run(kind))
    assert batches
    for batch in batches:
        if kind == "permissions":
            assert len(batch.documents) + len(batch.container_grants) <= 1
        else:
            assert len(batch.users) + len(batch.groups) <= 1
