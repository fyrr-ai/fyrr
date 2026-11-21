"""Permission layer proofs at the batch boundary, with the stateful transport."""

import pytest

from fyrr_connectors.arrow import rows_to_json


def test_jira_static_browse_and_secured_comment_intersections(jira, jira_index):
    jira_index.mode = "sync"
    jira.user("U002")
    jira.group("g1", ["U001", "U002"])
    jira.group("g2", ["U001"])
    jira.levels[1][20] = [{"type": "group", "value": "g2"}]
    jira.issue(101, security={"id": "20"})
    jira.comment(101, 201, "Restricted.", visibility={"type": "group", "identifier": "g1"})
    permissions = list(jira_index.run("permissions"))
    grants = [d for b in permissions for d in b.documents]
    assert len(grants) == 1 and not grants[0].containers
    members = list(jira_index.run("members"))
    groups = {g.id: set(g.members) for b in members for g in b.groups}
    assert any(v == {"usr:atlassian.net:" + jira.cloud + ":U001"} for v in groups.values())
    content = [r for b in jira_index.run() for r in rows_to_json(b.changes)]
    restricted = next(s for s in content[0]["sections"] if s["text"].endswith("\nRestricted."))
    assert not content[0]["readers"]
    assert len(restricted["readers"]) == 1 and restricted["readers"][0].startswith("grp:")
    assert restricted["readers"][0] in grants[0].readers


def readers(index, cursor=None):
    batches = list(index.run("permissions", cursor))
    return {d.root_id: d for b in batches for d in b.documents}, batches[-1].cursor


def tokens(index):
    return {g.id: set(g.members) for b in index.run("members") for g in b.groups}


def allowed(record, groups, cloud, user):
    principal = "usr:atlassian.net:" + cloud + ":" + user
    owned = {principal} | {"grp:" + g for g, m in groups.items() if principal in m}
    if record.containers:
        owned.update(
            {
                "container:" + c
                for c in record.containers
                if principal in groups.get("atlassian.net:" + cloud + ":policy:1:none:none", set())
            }
        )
    return bool(set(record.readers) & owned) or bool(
        record.containers and "container:" + record.containers[0] in owned
    )


@pytest.mark.parametrize(
    "holder", [{"type": "anyone"}, {"type": "public"}, {"type": "unknown_portal_holder"}]
)
def test_jira_public_refused_unknown_holder_under_restricted(jira, jira_index, holder):
    jira_index.mode = "sync"
    jira.schemes[1] = [{"permission": "BROWSE_PROJECTS", "holder": holder}]
    jira.issue(101)
    if holder["type"] != "unknown_portal_holder":
        with pytest.raises(ValueError, match="public_browse_refused"):
            readers(jira_index)
    else:
        records, _ = readers(jira_index)
        assert not allowed(next(iter(records.values())), tokens(jira_index), jira.cloud, "U001")


def test_jira_dynamic_only_browse_gets_fields_but_never_restricted_usr(jira, jira_index):
    jira_index.mode = "sync"
    jira.schemes[1] = [{"permission": "BROWSE_PROJECTS", "holder": {"type": "reporter"}}]
    jira.issue(101)
    jira.comment(101, 201, "Restricted.", visibility={"type": "role", "identifier": "10"})
    records, _ = readers(jira_index)
    assert allowed(next(iter(records.values())), tokens(jira_index), jira.cloud, "U001")
    content = [r for b in jira_index.run() for r in rows_to_json(b.changes)]
    own = content[0]["sections"][-1]["readers"]
    assert len(own) == 1 and own[0].startswith("grp:")
    assert tokens(jira_index)[own[0][4:]] == set()


@pytest.mark.parametrize("scope", ["level", "browse"])
def test_jira_secured_dynamic_usr_revoked_in_same_permission_pass(jira, jira_index, scope):
    jira_index.mode = "sync"
    jira.user("U002")
    jira.group("g2", ["U001"])
    jira.schemes[1] = [{"permission": "BROWSE_PROJECTS", "holder": {"type": "reporter"}}]
    jira.levels[1][20] = [{"type": "group", "value": "g2"}]
    jira.issue(101, security={"id": "20"})
    jira_index.sync()
    first, cursor = readers(jira_index)
    assert next(iter(first.values())).readers[-1].startswith("usr:")
    jira.clock += 1500
    if scope == "level":
        jira.groups["g2"]["members"] = ["U002"]
    else:
        jira.schemes[1] = []
    latest, _ = readers(jira_index, cursor)
    assert next(iter(first)) in latest
    assert not any(t.startswith("usr:") for t in latest[next(iter(first))].readers)


def test_jira_unsecured_group_picker_usr_revoked_without_issue_update(jira, jira_index):
    jira_index.mode = "sync"
    jira.user("U002")
    jira.group("g2", ["U001"])
    jira.schemes[1] = [
        {
            "permission": "BROWSE_PROJECTS",
            "holder": {"type": "groupCF", "value": "customfield_10000"},
        }
    ]
    jira.issue(101, customfield_10000={"groupId": "g2"})
    jira_index.sync()
    first, cursor = readers(jira_index)
    assert any(t.endswith(":U001") for t in next(iter(first.values())).readers)
    jira.groups["g2"]["members"] = ["U002"]
    jira.clock += 1500
    latest, _ = readers(jira_index, cursor)
    row = latest[next(iter(first))]
    assert not any(t.endswith(":U001") for t in row.readers)
    assert any(t.endswith(":U002") for t in row.readers)


def test_jira_membership_exception_never_reads_static_unsecured_issue(jira, jira_index):
    jira_index.mode = "sync"
    jira.user("U002")
    jira.groups["g1"]["members"] = ["U002"]
    jira.group("g2", ["U001"])
    jira.schemes[1].append({"permission": "BROWSE_PROJECTS", "holder": {"type": "reporter"}})
    jira.levels[1][20] = [{"type": "group", "value": "g2"}]
    jira.issue(101, security={"id": "20"})
    jira.issue(102, security={"id": "20"})
    jira.issue(103, security={"id": "20"})
    jira.issue(202, reporter=None)
    jira_index.sync()
    _, cursor = readers(jira_index)
    jira.clock += 1500
    _, cursor = readers(jira_index, cursor)
    jira.clock += 1500
    jira.groups["g2"]["members"] = ["U002"]
    jira.requests.clear()
    _, _ = readers(jira_index, cursor)
    assert not any(path == "/issue/202" for _, path, _ in jira.requests)


def test_jira_removed_project_is_emptied_without_failing_other_project(jira, jira_index):
    jira_index.mode = "sync"
    jira.project(2)
    jira.issue(101)
    jira.issue(202, project=2)
    jira_index.scope = {"projects": [1, 2]}
    jira_index.sync()
    del jira.projects[1]
    batches = list(jira_index.run("permissions"))
    grants = {g.container_id: g.readers for b in batches for g in b.container_grants}
    assert grants["jira:" + jira.cloud + ":project:1"] == ()
    assert grants["jira:" + jira.cloud + ":project:2"]
    old = next(d for b in batches for d in b.documents if d.root_id.endswith(":101"))
    assert not old.readers and not old.containers


def test_jira_permission_rotation_does_not_mask_membership_revocation(jira, jira_index):
    jira_index.mode = "sync"
    jira.user("U002")
    jira.groups["g1"]["members"] = ["U002"]
    jira.group("g2", ["U001"])
    jira.schemes[1].append({"permission": "BROWSE_PROJECTS", "holder": {"type": "reporter"}})
    jira.levels[1][20] = [{"type": "group", "value": "g2"}]
    for identity in (101, 102, 103):
        jira.issue(identity, reporter=None)
    jira.issue(300, security={"id": "20"})
    jira_index.sync()
    _, cursor = readers(jira_index)
    jira.clock += 1500
    _, cursor = readers(jira_index, cursor)
    jira.clock += 1500
    jira.groups["g2"]["members"] = ["U002"]
    records, _ = readers(jira_index, cursor)
    root = next(k for k in jira_index.published if k.endswith(":300"))
    assert root in records
    assert not any(t.endswith(":U001") for t in records[root].readers)


def test_jira_permission_records_replay_without_advancing_partial_cursor(jira, jira_index):
    jira_index.mode = "sync"
    for identity in range(101, 205):
        jira.issue(identity)
    batches = list(jira_index.run("permissions"))
    from fyrr_connectors.jira.feeds import Cursor

    initial = Cursor.decode(batches[0].cursor, __import__("json").loads(batches[0].cursor)["scope"])
    assert initial.updated == 0
    assert all(b.cursor == batches[0].cursor for b in batches[:-1])
    replay = list(jira_index.run("permissions"))
    assert replay == batches


def test_jira_unknown_security_and_comment_visibility_are_denied(jira, jira_index):
    jira_index.mode = "sync"
    jira.issue(101, security={"id": "999"})
    jira.comment(101, 201, visibility={"type": "unknown"})
    record = next(d for b in jira_index.run("permissions") for d in b.documents)
    assert not allowed(record, tokens(jira_index), jira.cloud, "U001")
    row = next(r for b in jira_index.run() for r in rows_to_json(b.changes))
    assert row["sections"][-1]["readers"] == []


def test_jira_failed_secured_enumeration_yields_no_permission_heartbeat(jira, jira_index):
    jira_index.mode = "sync"
    jira.levels[1][20] = [{"type": "group", "value": "g1"}]
    for identity in range(101, 105):
        jira.issue(identity, security={"id": "20"})
    jira_index.sync()
    _, cursor = readers(jira_index)
    jira.users["U001"]["active"] = False
    jira.after(
        lambda m, p, a, b: p == "/search/jql" and b and "level = 20" in b["jql"],
        lambda: jira.fail("/search/jql", 503, times=6),
    )
    from fyrr_connectors.jira.client import ApiError

    with pytest.raises(ApiError):
        next(jira_index.run("permissions", cursor))


def test_jira_permission_envelope_is_checked_before_first_yield(jira, jira_index):
    from dataclasses import replace

    jira_index.mode = "sync"
    jira.issue(101)
    jira_index.limits = replace(jira_index.limits, max_bytes=100)
    with pytest.raises(ValueError, match="snapshot_envelope_limit"):
        next(jira_index.run("permissions"))


def test_jira_member_envelope_is_checked_before_first_yield(jira, jira_index):
    from dataclasses import replace

    jira_index.mode = "sync"
    jira.issue(101)
    jira_index.limits = replace(jira_index.limits, max_bytes=100)
    with pytest.raises(ValueError, match="snapshot_envelope_limit"):
        next(jira_index.run("members"))


@pytest.mark.parametrize("kind", ["permissions", "members"])
@pytest.mark.parametrize("existing_target", [False, True])
def test_jira_changed_comment_visibility_cannot_refresh_stale_content(
    jira, jira_index, kind, existing_target
):
    jira_index.mode = "sync"
    jira.issue(101)
    jira.comment(101, 201, "Initially plain.")
    if existing_target:
        jira.comment(101, 202, visibility={"type": "group", "identifier": "g1"})
    jira_index.sync()
    cursor = list(jira_index.run(kind))[-1].cursor
    jira.edit_comment(101, 201, visibility={"type": "group", "identifier": "g1"})
    stream = jira_index.run(kind, cursor)
    with pytest.raises(ValueError, match="jira_published_visibility_stale"):
        next(stream)
    jira_index.sync()
    assert list(jira_index.run(kind, cursor))


def test_jira_permission_cursor_without_holder_type_evidence_is_refused(jira, jira_index):
    import json

    jira_index.mode = "sync"
    jira.issue(101)
    jira_index.sync()
    _, cursor = readers(jira_index)
    incomplete = json.loads(cursor)
    del incomplete["holder_types"]
    with pytest.raises(ValueError, match="jira_cursor_invalid"):
        readers(jira_index, json.dumps(incomplete))


def test_jira_custom_field_holder_replacement_revisits_erased_dynamic_bindings(jira, jira_index):
    jira_index.mode = "sync"
    holder = {"type": "groupCF", "value": "customfield_10000"}
    jira.schemes[1] = [{"permission": "BROWSE_PROJECTS", "holder": holder}]
    for identity in range(101, 111):
        jira.issue(identity, customfield_10000={"groupId": "g1"})
    jira_index.sync()
    original, cursor = readers(jira_index)
    assert any(t.endswith(":U001") for t in next(iter(original.values())).readers)
    jira.clock += 1500
    _, cursor = readers(jira_index, cursor)
    jira.clock += 1500
    holder["value"] = "customfield_10001"
    jira_index.sync()
    latest, _ = readers(jira_index, cursor)
    assert set(latest) == set(original)
    assert all(not any(t.startswith("usr:") for t in d.readers) for d in latest.values())
