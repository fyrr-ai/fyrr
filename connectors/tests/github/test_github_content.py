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
    assert changes[1]["sections"][0]["text"] == "Synthetic issue 2\n\nGood."


def test_oversized_aggregate_stops_accumulating_and_continues(github, index):
    github.topic(number=1, body="Body.")
    for _ in range(20):
        github.comment(number=1, body="x" * 40)
    github.topic(number=2, body="Good.")
    index.limits = replace(index.limits, document_max_bytes=64)
    changes = index.sync()
    assert changes["github:repo:1:issue:101:1"]["sections"] == [{"text": "\0"}]
    assert (
        changes["github:repo:1:issue:102:2"]["sections"][0]["text"] == "Synthetic issue 2\n\nGood."
    )
    reads = [params for _, path, params in github.requests if path.endswith("/issues/1/comments")]
    assert reads and all(int(params["page"]) == 1 for params in reads)


def test_changed_files_request_count(github, index):
    github.branch(1, {f"file-{number}.md": "Prose." for number in range(10)})
    index.sync()
    blobs = sum("/git/blobs/" in path for _, path, _ in github.requests)
    commits = sum(path.endswith("/commits") for _, path, _ in github.requests)
    verification = sum(path == "/repositories/1" for _, path, _ in github.requests)
    print(
        f"synthetic requests: blob={blobs} history={commits} "
        f"repository={verification} total={len(github.requests)}"
    )
    assert blobs == commits == 10
    assert verification == 5
    assert len(github.requests) == 33


def test_title_opens_the_first_section_so_it_is_searchable(github, index):
    github.topic(kind="pr", number=2, title="Quokka marshaller", body="PR body.")
    github.topic(number=3, title="Title only", body="")
    github.topic(number=4, title="Null body", body=None)
    changes = index.sync()
    pr = changes["github:repo:1:pr:1002:2"]["sections"]
    assert pr[0]["text"] == "Quokka marshaller\n\nPR body."
    assert pr[0]["location"]["label"] == "Quokka marshaller"
    for root, title in (
        ("github:repo:1:issue:103:3", "Title only"),
        ("github:repo:1:issue:104:4", "Null body"),
    ):
        assert [section["text"] for section in changes[root]["sections"]] == [title]


def test_title_counts_toward_the_document_limit(github, index):
    index.limits = replace(index.limits, document_max_bytes=64)
    github.topic(number=1, title="t" * 31, body="b" * 31)
    github.topic(number=2, title="t" * 30, body="b" * 40)
    changes = index.sync()
    # 31 + 2 + 31 bytes is exactly the limit and is kept.
    assert (
        changes["github:repo:1:issue:101:1"]["sections"][0]["text"] == "t" * 31 + "\n\n" + "b" * 31
    )
    assert changes["github:repo:1:issue:102:2"]["sections"] == [{"text": "\0"}]


def test_title_counts_while_comment_pages_are_read(github, index):
    # Body and comments alone stay under the limit; with the title the first
    # comment page already exceeds it, so the second page is never requested.
    index.limits = replace(index.limits, document_max_bytes=64)
    github.topic(number=1, title="t" * 30, body="b" * 5)
    for _ in range(3):
        github.comment(number=1, body="c" * 15)
    changes = index.sync()
    assert changes["github:repo:1:issue:101:1"]["sections"] == [{"text": "\0"}]
    reads = [params for _, path, params in github.requests if path.endswith("/issues/1/comments")]
    assert reads and all(int(params["page"]) == 1 for params in reads)
