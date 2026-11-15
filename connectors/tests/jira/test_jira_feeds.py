"""Public batch/ack paths and scope/reconcile races through one HTTP fake."""

import json
from dataclasses import replace

import pytest

from fyrr_connectors.arrow import rows_to_json
from fyrr_connectors.jira.client import ApiError


def text(index):
    return "\n".join(s["text"] for item in index.published.values() for s in item["sections"])


def test_jira_ack_replay_and_final_cursor_only(jira, jira_index):
    for identity in range(101, 105):
        jira.issue(identity)
    jira_index.limits = replace(jira_index.limits, max_rows=1)
    stream = jira_index.run()
    first = next(stream)
    before = json.loads(first.cursor)
    assert before["updated"] == 0
    first_rows = rows_to_json(first.changes)
    stream.close()
    replay = list(jira_index.run())
    assert rows_to_json(replay[0].changes) == first_rows
    assert all(json.loads(b.cursor)["updated"] == 0 for b in replay[:-1])
    assert json.loads(replay[-1].cursor)["updated"] > 0
    for batch in replay:
        jira_index.apply(batch)
    assert len(jira_index.published) == 4


def test_jira_edit_and_deleted_comment_without_parent_updated(jira, jira_index):
    jira.issue(101)
    jira.comment(101, 201, "Old child.")
    jira_index.sync()
    jira.clock += 300
    jira_index.sync()
    jira.clock += 900
    jira.delete_comment(101, 201)
    jira_index.sync()
    assert "Old child." not in text(jira_index)
    jira.edit(101, summary="Changed issue.")
    jira_index.sync()
    assert "Changed issue." in text(jira_index)


def test_jira_delayed_new_issue_is_found_by_periodic_full_scan(jira, jira_index):
    jira.issue(101)
    jira_index.sync()
    jira.issue(102)
    jira.issues[102]["fields"]["updated"] = jira.issues[101]["fields"]["updated"]
    jira.clock += 43201
    jira_index.sync()
    assert len(jira_index.published) == 2


def test_jira_scope_exit_return_at_same_revision_replays(jira, jira_index):
    jira.issue(101)
    initial = jira_index.sync()
    jira_index.scope = {"projects": [1], "labels": ["selected"]}
    assert jira_index.listing() == set()
    jira_index.revisions.clear()
    jira_index.published.clear()
    jira_index.scope = {"projects": [1]}
    assert set(jira_index.sync()) == set(initial)


@pytest.mark.parametrize("change", ["move", "type", "label", "archive", "delete"])
def test_jira_complete_reconcile_scope_loss(jira, jira_index, change):
    jira.project(2)
    jira.issue(101, labels=["selected"])
    jira_index.scope = {"projects": [1], "issue_types": [1], "labels": ["selected"]}
    jira_index.sync()
    if change == "move":
        jira.edit(101, project={"id": "2"})
    elif change == "type":
        jira.edit(101, issuetype={"id": "2", "name": "Other"})
    elif change == "label":
        jira.edit(101, labels=[])
    elif change == "archive":
        jira.projects[1]["archived"] = True
    else:
        jira.delete(101)
    assert jira_index.listing() == set()


def test_jira_failed_listing_yields_no_complete_part(jira, jira_index):
    for identity in range(101, 105):
        jira.issue(identity)
    jira_index.sync()
    jira.after(lambda m, p, a, b: p == "/search/jql", lambda: jira.fail("/search/jql", times=6))
    with pytest.raises(ApiError):
        assert not list(jira_index.run("reconcile"))
    assert len(jira_index.published) == 4


def test_jira_authoritative_project_loss_does_not_stop_other_project(jira, jira_index):
    jira.project(2)
    jira.issue(101)
    jira.issue(102, project=2)
    jira_index.scope = {"projects": [1, 2]}
    jira_index.sync()
    del jira.projects[1]
    assert jira_index.listing() == {jira_index.world.cloud.join(["jira:", ":issue:102"])}


def test_jira_fetched_parent_must_match_queued_project(jira, jira_index):
    jira.project(2)
    jira.issue(101)
    jira.after(lambda m, p, a, b: p == "/search/jql", lambda: jira.edit(101, project={"id": "2"}))
    with pytest.raises(ValueError, match="project_changed"):
        list(jira_index.run())
    assert not jira_index.published


def test_jira_later_contradiction_replaces_staged_document(jira, jira_index):
    jira.issue(101)
    # Change after the second canonical read: first staged result is obsolete.
    jira.after(
        lambda m, p, a, b: p == "/issue/101",
        lambda: jira.after(
            lambda m, p, a, b: p == "/issue/101", lambda: jira.edit(101, summary="Latest.")
        ),
    )
    changes = jira_index.sync()
    assert len(changes) == 1
    assert "Latest." in text(jira_index) and "Issue 101" not in text(jira_index)


def test_jira_quarantine_preserves_good_and_continues(jira, jira_index):
    jira.issue(101)
    jira_index.sync()
    prior = text(jira_index)
    jira.edit(101, description="\0")
    jira.issue(102, summary="Healthy.")
    jira_index.sync()
    assert prior in text(jira_index) and "Healthy." in text(jira_index)
    assert json.loads(jira_index.cursor)["updated"] > 0


def test_jira_transient_canonical_ancestor_failure_fails_listing(jira, jira_index):
    jira.issue(101)
    jira_index.sync()
    jira.fail("/project/1", times=6)
    with pytest.raises(ApiError):
        list(jira_index.run("reconcile"))


def test_jira_static_jql_narrowing_has_canonical_negative_check(jira, jira_index):
    jira.issue(101)
    jira_index.scope = {"jql": "project = 1 AND status != Closed"}
    jira_index.sync()
    jira.edit(101, status={"name": "Closed"})
    assert jira_index.listing() == set()


def test_jira_ambiguous_forbidden_is_not_authoritative_scope_loss(jira, jira_index):
    jira.issue(101)
    jira_index.sync()
    jira.fail("/project/1", 403)
    with pytest.raises(ApiError):
        list(jira_index.run("reconcile"))
    assert jira_index.published


def test_jira_later_change_within_same_second_replaces_staged_document(jira, jira_index):
    jira.issue(101)

    def changed():
        fields = jira.issues[101]["fields"]
        fields.update(
            summary="Latest within second.",
            updated=fields["updated"].replace("+00:00", ".500000+00:00"),
        )

    jira.after(
        lambda m, p, a, b: p == "/issue/101",
        lambda: jira.after(lambda m, p, a, b: p == "/issue/101", changed),
    )
    jira_index.sync()
    assert "Latest within second." in text(jira_index)


def test_jira_later_dynamic_holder_within_same_second_replaces_permission(jira, jira_index):
    jira_index.mode = "sync"
    jira.user("U002")
    jira.groups["g1"]["members"].append("U002")
    jira.schemes[1] = [{"permission": "BROWSE_PROJECTS", "holder": {"type": "reporter"}}]
    jira.issue(101)

    def changed():
        fields = jira.issues[101]["fields"]
        fields.update(
            reporter={"accountId": "U002", "displayName": "Fixture author"},
            updated=fields["updated"].replace("+00:00", ".500000+00:00"),
        )

    jira.after(
        lambda m, p, a, b: p == "/issue/101",
        lambda: jira.after(lambda m, p, a, b: p == "/issue/101", changed),
    )
    records = [d for b in jira_index.run("permissions") for d in b.documents]
    assert any(t.endswith(":U002") for t in records[0].readers)
    assert not any(t.endswith(":U001") for t in records[0].readers)


def test_jira_rotation_proves_jql_membership_after_a_project_move(jira, jira_index):
    jira_index.scope = {"jql": "project = 1"}
    jira.project(2)
    jira.issue(101)
    jira_index.sync()
    jira.clock += 1500
    jira_index.sync()
    jira.edit(101, project={"id": "2"})
    jira.clock += 1500
    batches = list(jira_index.run(cursor=jira_index.cursor))
    changes = [r for b in batches for r in rows_to_json(b.changes)]
    assert changes and all(r["op"] == "delete" for r in changes)
    assert jira_index.listing() == set()


def test_jira_metadata_list_is_bounded_and_bad_metadata_is_quarantined(jira, jira_index):
    jira.issue(101, labels=[f"label-{i:02}" for i in reversed(range(40))])
    jira.issue(102, reporter={"displayName": "A" * 1025})
    jira.issue(103)
    jira_index.sync()
    roots = jira_index.published
    assert len(roots) == 2
    row = next(v for k, v in roots.items() if k.endswith(":101"))
    assert row["metadata"]["labels"] == [f"label-{i:02}" for i in range(32)]
    assert any(k.endswith(":103") for k in roots)


def test_jira_first_batch_is_durable_before_a_later_document_failure(jira, jira_index):
    jira.issue(101)
    jira.issue(102)
    jira_index.limits = replace(jira_index.limits, max_rows=1)
    jira.fail("/issue/102", times=6)
    stream = jira_index.run()
    first = next(stream)
    assert rows_to_json(first.changes)[0]["root_id"].endswith(":101")
    assert json.loads(first.cursor)["updated"] == 0
    assert not any(path == "/issue/102" for _, path, _ in jira.requests)
    jira_index.apply(first)
    with pytest.raises(ApiError):
        next(stream)
    assert len(jira_index.published) == 1
