"""Fixed content, aggregates, revisions, quarantine and acknowledged cursors."""

import json
from dataclasses import replace

import pytest
from github_kit import Index

from fyrr_connectors import testkit
from fyrr_connectors.arrow import rows_to_json
from fyrr_connectors.github import GitHub
from fyrr_connectors.github.client import ApiError
from fyrr_connectors.provider import provider_info


def test_issues_prs_prose_and_metadata(github, index):
    github.topic(body="Issue body.")
    github.comment(body="Issue comment.")
    github.topic(kind="pr", number=2, body="PR body.")
    github.comment(kind="pr", number=2, body="PR discussion.")
    review = github.comment(kind="pr", number=2, body="Submitted review.", state="APPROVED")
    github.comment(kind="pr", number=2, body="Inline comment.", review_id=review["id"])
    draft = github.comment(kind="pr", number=2, body="Private draft.", state="PENDING")
    github.comment(kind="pr", number=2, body="Private inline draft.", review_id=draft["id"])
    github.branch(
        1, {"README": "Prose.", "README.py": "Code.", "module.rs": "Rust.", "photo.png": b"binary"}
    )
    changes = index.sync()
    assert len(changes) == 3
    combined = "\n".join(
        section["text"] for change in changes.values() for section in change["sections"]
    )
    for body in (
        "Issue body.",
        "Issue comment.",
        "PR body.",
        "PR discussion.",
        "Submitted review.",
        "Inline comment.",
        "Prose.",
    ):
        assert body in combined
    assert "Private" not in combined and "Code." not in combined and "Rust." not in combined
    assert all(change["readers"] == list(index.readers) for change in changes.values())
    assert all(change["metadata"]["author"] for change in changes.values())
    assert all(change["metadata"]["updated_at"] for change in changes.values())
    assert all("revision_order" not in change for change in changes.values())
    assert not any(
        "collaborators" in path or "/teams" in path or "/members" in path
        for _, path, _ in github.requests
    )
    assert index.sync() == {}
    assert index.listing() == set(changes)


@pytest.mark.parametrize("toggle", ["issues", "pull_requests", "prose", "code"])
def test_each_content_toggle(github, index, toggle):
    github.topic()
    github.topic(kind="pr", number=2)
    github.branch(1, {"readme.md": "Prose.", "file.py": "Code."})
    item = index.scope["repositories"][0]
    item.update({key: key == toggle for key in ("issues", "pull_requests", "prose", "code")})
    changes = index.sync()
    assert len(changes) == 1
    kind = next(iter(changes.values()))["metadata"]["source_type"]
    assert (
        kind
        == {
            "issues": "github-issue",
            "pull_requests": "github-pr",
            "prose": "github-file",
            "code": "github-file",
        }[toggle]
    )


def test_deleted_comments_without_parent_timestamp_are_rotated(github, index):
    github.topic()
    github.comment(body="Deleted comment.")
    index.sync()
    parent_stamp = github.topics[1, "issue", 1]["updated_at"]
    github.comments[1, "issue", 1].clear()
    changes = index.sync()
    assert len(changes) == 1
    assert all(
        "Deleted comment." not in section["text"]
        for change in changes.values()
        for section in change["sections"]
    )
    assert github.topics[1, "issue", 1]["updated_at"] == parent_stamp


def test_force_push_old_commit_and_file_delete(github, index):
    old = github.branch(1, {"old.md": "Old prose."})
    before = index.sync()
    github.branch(1, {"new.md": "New prose."})
    changed = index.sync()
    assert any(change["op"] == "delete" for change in changed.values())
    github.heads[1, "main"] = old
    reverted = index.sync()
    assert set(index.published) == set(before)
    assert any(change["op"] == "upsert" for change in reverted.values())


def test_ack_and_replay_uses_pending_revision_view(github, index):
    for ordinal in range(1, 5):
        github.topic(number=ordinal)
    index.limits = replace(index.limits, max_rows=1)
    stream = index.run()
    first = next(stream)
    assert len(rows_to_json(first.changes)) == 1
    index.apply(first)
    prior = json.loads(index.cursor)
    assert prior["repos"] == {}, "the pass watermark has not advanced"
    stream.close()
    changes = index.sync()
    assert len(index.published) == 4
    assert len([change for change in changes.values() if change["op"] == "upsert"]) == 3
    assert json.loads(index.cursor)["repos"]["1"]["since"]


@pytest.mark.parametrize("bad", ["\0", "x" * 100, b"\xff"])
def test_bad_or_oversized_document_does_not_stall(github, index, bad):
    index.limits = replace(index.limits, document_max_bytes=64)
    github.branch(1, {"bad.md": bad, "good.md": "Good."})
    changes = index.sync()
    assert len(changes) == 2
    assert any(change["sections"] == [{"text": "\0"}] for change in changes.values())
    assert len(index.published) == 1 and index.cursor


def test_quarantine_keeps_last_good_revision(github, index):
    github.topic(body="Good.")
    index.sync()
    old = dict(index.published)
    github.topic(body="\0")
    assert next(iter(index.sync().values()))["sections"] == [{"text": "\0"}]
    assert index.published == old


def test_child_listing_failure_is_not_a_delete(github, index):
    github.topic()
    index.sync()
    old_cursor = index.cursor
    github.fail("/repos/synthetic/repo-1/issues/1/comments", 404)
    with pytest.raises(ApiError):
        index.sync()
    assert index.cursor == old_cursor and index.published


def test_later_contradicting_hint_replaces_stale_content(github, index):
    github.topic()
    github.comment()
    index.sync()
    github.topic(body="Intermediate.")
    github.after(
        lambda method, path, params: path.endswith("/issues/comments"),
        lambda: github.topic(body="Final."),
    )
    changes = index.sync()
    assert next(iter(changes.values()))["sections"][0]["text"] == "Final."


def test_rendered_file_has_locations_and_unknown_creation_time(github, index):
    github.branch(1, {"guide.rst": "Intro\n=====\nText.", "notes.md": "# One\nBody.\n# Two\nMore."})
    changes = index.sync()
    assert len(changes) == 2
    assert all("created_at" not in change["metadata"] for change in changes.values())
    assert all(
        section.get("location") for change in changes.values() for section in change["sections"]
    )


def test_golden_sdk_arrow_and_cursors(github):
    from pathlib import Path

    github.topic(body="Golden body.")
    info = provider_info(
        name="github", connector=GitHub, min_sdk_version="0.1.0", connection_types=("fixed",)
    )
    result = testkit.run_sync(
        info,
        "content",
        permissions="fixed",
        settings=Index(github).settings,
        scope=Index(github).scope,
    )
    testkit.assert_golden(result, Path(__file__).parent / "golden/content.json")


def test_arrow_location_overflow_quarantines_and_continues(github, index):
    github.topic(number=1, body="Large metadata.")
    for _ in range(40):
        github.comment(number=1, body="Small text.")
    github.topic(number=2, body="Good.")
    index.limits = replace(index.limits, max_bytes=2600)
    batches = list(index.run())
    assert all(batch.changes.get_total_buffer_size() <= 2600 for batch in batches)
    changes = [change for batch in batches for change in rows_to_json(batch.changes)]
    assert len(changes) == 2
    assert changes[0]["sections"] == [{"text": "\0"}]
    assert changes[1]["sections"][0]["text"] == "Good."


def test_rotation_marker_disambiguates_issue_and_pr_id_collision(github, index):
    github.topic(number=1)
    github.topic(kind="pr", number=2, id=101)
    github.comment(kind="pr", number=2, body="Remove this.")
    index.sync()
    # No parent or comment hint can report this deletion.
    for parent in github.topics.values():
        parent["updated_at"] = "2025-01-01T00:00:00+00:00"
    github.comments[1, "pr", 2].clear()
    changes = index.sync()
    pr = changes["github:repo:1:pr:101:2"]
    assert all("Remove this." not in section["text"] for section in pr["sections"])


def test_oversized_aggregate_stops_accumulating_and_continues(github, index):
    github.topic(number=1, body="Body.")
    for _ in range(20):
        github.comment(number=1, body="x" * 40)
    github.topic(number=2, body="Good.")
    index.limits = replace(index.limits, document_max_bytes=64)
    changes = index.sync()
    assert changes["github:repo:1:issue:101:1"]["sections"] == [{"text": "\0"}]
    assert changes["github:repo:1:issue:102:2"]["sections"][0]["text"] == "Good."
    reads = [params for _, path, params in github.requests if path.endswith("/issues/1/comments")]
    assert reads and all(int(params["page"]) == 1 for params in reads)
