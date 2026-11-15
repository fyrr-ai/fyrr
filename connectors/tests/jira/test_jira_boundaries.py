"""Bounded API/config/text checks, with synthetic values only."""

from dataclasses import replace

import pytest

from fyrr_connectors.config import ConfigError
from fyrr_connectors.jira.adf import ADFError, text
from fyrr_connectors.jira.client import ApiError
from fyrr_connectors.jira.config import Scope
from fyrr_connectors.jira.feeds import Cursor, IssueBinding, rotation


@pytest.mark.parametrize(
    "scope",
    [
        {},
        {"projects": [1], "jql": "project = 1"},
        {"projects": [-1]},
        {"projects": [1, 1]},
        {"jql": "project = 1 ORDER BY updated"},
        {"jql": "filter = 1"},
        {"jql": "updated > now()"},
        {"projects": [1], "labels": ["\0"]},
    ],
)
def test_jira_invalid_scope_is_configuration_error(scope):
    with pytest.raises(ConfigError):
        Scope.parse(scope)


@pytest.mark.parametrize("elapsed,expected", [(1, 1), (900, 209), (43200, 10000), (86400, 10000)])
def test_jira_rotation_budget(elapsed, expected):
    selected, _, _ = rotation(range(1, 10001), 0, elapsed)
    assert len(selected) == expected


@pytest.mark.parametrize(
    "value",
    ["{}", "[]", "\0", '{"scope":42}', pytest.param("x" * 16385, id="oversized")],
)
def test_jira_bad_cursor_rejected(value):
    with pytest.raises(ValueError, match="cursor"):
        Cursor.decode(value, "scope")


def test_jira_cursor_contains_only_numeric_progress_and_hashes():
    state = Cursor("f" * 64, updated=1, step=2, start=3, full=4)
    assert Cursor.decode(state.encode(), "f" * 64) == state
    assert Cursor.decode(state.encode(), "e" * 64) == Cursor("e" * 64)
    binding = IssueBinding(1, 2, True, (("role", "10"), ("group", "g1")))
    assert IssueBinding.from_revision(binding.revision("a" * 64)) == binding


@pytest.mark.parametrize(
    "value",
    [
        "\0",
        {"type": "doc", "version": 2},
        {"type": "doc", "version": 1, "content": None},
        {"type": "doc", "version": 1, "content": [42]},
    ],
)
def test_jira_bad_adf(value):
    with pytest.raises(ADFError):
        text(value)


def test_jira_adf_depth_and_media_exclusion():
    value = {
        "type": "doc",
        "version": 1,
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "Safe."}]},
            {"type": "media", "attrs": {"url": "https://invalid/secret"}},
        ],
    }
    assert text(value) == "Safe."
    node = {"type": "text", "text": "Too deep."}
    for _ in range(70):
        node = {"type": "paragraph", "content": [node]}
    with pytest.raises(ADFError):
        text({"type": "doc", "version": 1, "content": [node]})


def test_jira_comment_snapshot_movement_retries_once(jira, jira_index):
    jira.issue(101)
    for c in range(201, 205):
        jira.comment(101, c, str(c))
    jira.after(
        lambda m, p, a, b: p == "/issue/101/comment" and a.get("startAt") == "0",
        lambda: jira.delete_comment(101, 201),
    )
    # The changed parent is unchanged on purpose; checked comment census repairs offset shift.
    changes = jira_index.sync()
    assert "202" in "\n".join(s["text"] for c in changes.values() for s in c["sections"])
    assert "204" in "\n".join(s["text"] for c in changes.values() for s in c["sections"])


def test_jira_rate_limit_retry_after(jira, jira_index):
    jira.issue(101)
    jira.fail("/serverInfo", 429, headers={"Retry-After": "7"})
    jira_index.sync()
    assert jira.sleeps == [7.0]


@pytest.mark.parametrize("status", [401, 403, 302])
def test_jira_global_failures_are_not_empty_source(jira, jira_index, status):
    jira.fail("/serverInfo", status)
    with pytest.raises(ApiError):
        list(jira_index.run())


def test_jira_mismatched_project_id_fails_even_fixed(jira, jira_index):
    jira.projects[1]["id"] = "2"
    with pytest.raises(ValueError, match="identity_mismatch"):
        list(jira_index.run())


def test_jira_mismatched_issue_id_fails(jira, jira_index):
    jira.issue(101)
    jira.after(lambda m, p, a, b: p == "/search/jql", lambda: jira.issues[101].update(id="102"))
    with pytest.raises(ValueError, match="identity_mismatch"):
        list(jira_index.run())


def test_jira_comment_parent_identity_mismatch_fails(jira, jira_index):
    jira.issue(101)
    jira.comment(101, 201)["self"] = jira.site + "/rest/api/3/issue/102/comment/201"
    with pytest.raises(ValueError, match="parent_mismatch"):
        list(jira_index.run())


def test_jira_skipped_authors_and_jsm_comments(jira, jira_index):
    jira.issue(101)
    jira.comment(101, 201, "Skipped.")
    jira_index.scope = {"projects": [1], "skip_comment_authors": ["U001"]}
    assert "Skipped." not in str(jira_index.sync())
    jira_index.scope = {"projects": [1]}
    jira.projects[1]["projectTypeKey"] = "service_desk"
    assert "Skipped." not in str(jira_index.sync())


def test_jira_oversized_document_does_not_stop_following_issue(jira, jira_index):
    jira_index.limits = replace(jira_index.limits, document_max_bytes=64)
    jira.issue(101, description="x" * 65)
    jira.issue(102, description="Healthy.")
    jira_index.sync()
    assert len(jira_index.published) == 1
    assert next(iter(jira_index.published)).endswith(":102")


def test_jira_long_fields_are_split_into_utf8_safe_units(jira, jira_index):
    jira.issue(101, description="ě" * 10000)
    changes = jira_index.sync()
    sections = next(iter(changes.values()))["sections"]
    description = [s for s in sections if s["location"]["label"] == "description"]
    assert len(description) == 2
    assert "".join(s["text"] for s in description) == "ě" * 10000
    assert all(len(s["text"].encode()) <= 16384 for s in description)


def test_jira_unknown_source_dates_remain_null(jira, jira_index):
    jira.issue(101, created=None)
    # Updated is required for discovery; no ingest-time replacement of creation.
    change = next(iter(jira_index.sync().values()))
    assert "created_at" not in change["metadata"]


def test_jira_continuous_comment_movement_fails_after_one_retry(jira, jira_index):
    jira.issue(101)
    for identity in range(201, 208):
        jira.comment(101, identity, str(identity))

    def move():
        jira.delete_comment(101, int(jira.comments[101][0]["id"]))
        jira.after(lambda m, p, a, b: p == "/issue/101/comment" and a.get("startAt") == "0", move)

    jira.after(lambda m, p, a, b: p == "/issue/101/comment" and a.get("startAt") == "0", move)
    with pytest.raises(ApiError, match="snapshot_changed"):
        list(jira_index.run())
    assert len([p for _, p, _ in jira.requests if p == "/issue/101/comment"]) < 20


def test_jira_ambiguous_clock_header_fails_before_any_cursor(jira, jira_index):
    jira.issue(101)
    jira.fail(
        "/serverInfo",
        200,
        headers={"Date": "Thu, 01 Jan 2026 00:00:03 GMT, Thu, 01 Jan 2026 00:00:03 GMT"},
    )
    with pytest.raises(ApiError, match="clock_header_invalid"):
        list(jira_index.run())
    assert not jira_index.published
