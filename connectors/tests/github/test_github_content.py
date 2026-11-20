"""Fixed content, aggregates, revisions, quarantine and acknowledged cursors."""

import hashlib
import json
from dataclasses import replace

import pyarrow as pa
import pytest
from github_kit import Index

from fyrr_connectors import testkit
from fyrr_connectors.arrow import rows_to_json
from fyrr_connectors.github import GitHub, render
from fyrr_connectors.github.client import ApiError
from fyrr_connectors.github.feeds import ConversationRef, FeedKind, Repository
from fyrr_connectors.github.tree import FileRef
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


QUARANTINED = [{"text": "\0"}]
ISSUE = "github:repo:1:issue:{}:{}"


def file_root(path, branch="main"):
    branch_id = hashlib.sha256(branch.encode()).hexdigest()[:32]
    return f"github:repo:1:file:{branch_id}:{hashlib.sha256(path.encode()).hexdigest()}"


def synced(index, quarantined, good):
    """Sync once; exactly `quarantined` roots are quarantined, `good` is
    published, and the repository frontier is the server time of this sync."""
    changes = index.sync()
    assert {root for root, change in changes.items() if change["sections"] == QUARANTINED} == set(
        quarantined
    )
    assert good in index.published
    assert not set(quarantined) & set(index.published)
    assert json.loads(index.cursor)["repos"]["1"]["since"] == index.world.server_time()
    return changes


@pytest.mark.parametrize(
    ("title", "stored"),
    [
        ("   ", None),
        ("  Padded\t", "Padded"),
        ("x" * 1025, "x" * 1024),
        ("x" + "\u00e9" * 512, "x" + "\u00e9" * 511),
        ("\u00e9" * 1000, "\u00e9" * 512),
        ("x" * 2000, "x" * 1024),
    ],
    ids=["blank", "padded", "1025-bytes", "split-char", "multibyte-1000", "2000-bytes"],
)
def test_unrepresentable_title_is_normalised_and_sync_continues(github, index, title, stored):
    github.topic(number=1, body="Body.", title=title)
    github.topic(number=2, body="Good.")
    synced(index, [], ISSUE.format(102, 2))
    bad = index.published[ISSUE.format(101, 1)]
    assert bad["sections"][0]["text"] == f"{title}\n\nBody."
    assert bad["metadata"].get("title") == stored


def inline_path(github, path):
    github.topic(kind="pr", number=1, body="PR.")
    review = github.comment(kind="pr", number=1, body="Review.", state="COMMENTED")
    github.comment(kind="pr", number=1, body="Inline.", review_id=review["id"])["path"] = path


def test_control_character_in_inline_comment_path_only_decorates_the_label(github, index):
    inline_path(github, "dir\n/a\x1b.py")
    github.topic(number=2, body="Good.")
    synced(index, [], ISSUE.format(102, 2))
    label = index.published["github:repo:1:pr:1001:1"]["sections"][-1]["location"]["label"]
    assert label.endswith("discussion_r1: dir /a .py:3") and "\n" not in label


def test_overlong_inline_comment_suffix_is_left_out_whole(github, index):
    inline_path(github, "p" * 1000)
    github.topic(number=2, body="Good.")
    synced(index, [], ISSUE.format(102, 2))
    label = index.published["github:repo:1:pr:1001:1"]["sections"][-1]["location"]["label"]
    assert label == "https://github.com/synthetic/repo-1/pull/1#discussion_r1"


@pytest.mark.parametrize(
    "path",
    ["p" * 1025, "a\0b.py", "\ud800.py", 7],
    ids=["1025-bytes", "nul", "lone-surrogate", "not-a-string"],
)
def test_unusable_inline_comment_path_is_decoration_only(github, index, path):
    inline_path(github, path)
    github.topic(number=2, body="Good.")
    synced(index, [], ISSUE.format(102, 2))
    pr = index.published["github:repo:1:pr:1001:1"]
    assert [section["text"] for section in pr["sections"]] == [
        "Synthetic pr 1\n\nPR.",
        "Review.",
        "Inline.",
    ]
    assert pr["sections"][-1]["location"]["label"] == (
        "https://github.com/synthetic/repo-1/pull/1#discussion_r1"
    )


@pytest.mark.parametrize(
    ("line", "suffix"),
    [("7", ""), ({"n": 1}, ""), (-3, ""), (0, ""), (7, ":7")],
    ids=["string", "object", "negative", "zero", "valid"],
)
def test_malformed_inline_comment_line_is_left_out(github, index, line, suffix):
    inline_path(github, "sample.py")
    github.inline[(1, "pr", 1)][0].update(line=line)
    github.topic(number=2, body="Good.")
    synced(index, [], ISSUE.format(102, 2))
    label = index.published["github:repo:1:pr:1001:1"]["sections"][-1]["location"]["label"]
    assert label.endswith("discussion_r1: sample.py" + suffix)


def test_markdown_headings_are_repaired_only_when_invalid(github, index):
    long = "\u00e9" * 600
    github.branch(
        1,
        {
            "a.md": f"Intro.\n#\n#\tTab\x01head\n# {long}\n# Title  \nEnd.",
            "good.md": "Good.",
        },
    )
    synced(index, [], file_root("good.md"))
    labels = [s["location"]["label"] for s in index.published[file_root("a.md")]["sections"]]
    assert labels[0] == "a.md (line 1)"
    assert labels[1] == "(line 2)"
    assert labels[2] == "Tab head (line 3)"
    assert labels[3] == long[:507] + " (line 4)"
    assert labels[4] == "Title   (line 5)"


def test_heading_over_the_label_bound_is_cut_with_room_for_the_line(github, index):
    github.branch(1, {"a.md": "Intro.\n# \x01" + "\u00e9" * 700, "good.md": "Good."})
    synced(index, [], file_root("good.md"))
    label = index.published[file_root("a.md")]["sections"][1]["location"]["label"]
    assert label == "\u00e9" * 507 + " (line 2)"


def over_long_uri(github):
    path = "/".join(["\u00e9" * 100] * 7) + "/a.md"
    github.branch(1, {path: "Prose.", "good.md": "Good."})
    return [file_root(path)]


def many_labels(github):
    github.topic(number=1, body="Body.", labels=[{"name": f"l{n}"} for n in range(33)])
    github.topic(number=2, body="Good.")
    return [ISSUE.format(101, 1)]


def bad_title(title):
    def build(github):
        github.topic(number=1, body="Body.", title=title)
        github.topic(number=2, body="Good.")
        return [ISSUE.format(101, 1)]

    return build


def bad_state(github):
    github.topic(number=1, body="Body.", state=" open ")
    github.topic(number=2, body="Good.")
    return []


def bad_author(github):
    github.topic(number=1, body="Body.", user={"id": 7, "login": "a\nb"})
    github.topic(number=2, body="Good.")
    return [ISSUE.format(101, 1)]


def bad_label_name(github):
    github.topic(number=1, body="Body.", labels=[{"name": "a\nb"}])
    github.topic(number=2, body="Good.")
    return [ISSUE.format(101, 1)]


def bad_path(github):
    github.branch(1, {"bad\nname.md": "Bad.", "good.md": "Good."})
    return [file_root("bad\nname.md")]


@pytest.mark.parametrize(
    "build",
    [
        bad_title("A\nB"),
        bad_title("A\x1bB"),
        bad_title("A\0B"),
        many_labels,
        bad_author,
        bad_label_name,
        bad_path,
        over_long_uri,
    ],
    ids=["newline", "escape", "nul", "33-labels", "author", "label-name", "path", "over-long-uri"],
)
def test_contract_violation_quarantines_one_document_and_sync_continues(github, index, build):
    bad = build(github)
    good = file_root("good.md") if bad and ":file:" in bad[0] else ISSUE.format(102, 2)
    synced(index, bad, good)


def test_padded_state_is_stripped(github, index):
    bad_state(github)
    synced(index, [], ISSUE.format(102, 2))
    assert index.published[ISSUE.format(101, 1)]["metadata"]["status"] == "open"


def test_blank_provider_metadata_is_dropped_and_the_sync_continues(github, index):
    github.topic(number=1, body="Blank author.", user={"id": 7, "login": " "})
    github.topic(number=2, body="Blank label.", labels=[{"name": " "}, {"name": " bug "}])
    github.topic(number=3, body="Good.")
    synced(index, [], ISSUE.format(103, 3))
    assert "author" not in index.published[ISSUE.format(101, 1)]["metadata"]
    assert index.published[ISSUE.format(102, 2)]["metadata"]["labels"] == ["bug"]


def test_guard_quarantines_a_document_no_field_level_rule_handles(github, index, monkeypatch):
    original = render.conversation

    def render_with_bad_project(repo, ref, parent, parts, limit):
        rendered = original(repo, ref, parent, parts, limit)
        if ref.number == 1:
            return replace(rendered, metadata=replace(rendered.metadata, project="a\nb"))
        return rendered

    monkeypatch.setattr(render, "conversation", render_with_bad_project)
    github.topic(number=1, body="Body.")
    github.topic(number=2, body="Good.")
    synced(index, [ISSUE.format(101, 1)], ISSUE.format(102, 2))


def test_mixed_comment_timestamps_quarantine_one_document_and_sync_continues(github, index):
    github.topic(number=1, body="Body.")
    github.comment(number=1, body="One.")["created_at"] = "2026-01-01T00:00:00Z"
    github.comment(number=1, body="Two.")["created_at"] = {"day": 1}
    github.topic(number=2, body="Good.")
    synced(index, [ISSUE.format(101, 1)], ISSUE.format(102, 2))


def test_unrepresentable_instant_quarantines_one_document_and_sync_continues(github, index):
    github.topic(number=1, body="Body.", created_at="0001-01-01T00:00:00+01:00")
    github.topic(number=2, body="Good.")
    synced(index, [ISSUE.format(101, 1)], ISSUE.format(102, 2))


def test_fetch_failure_inside_a_document_is_not_quarantined(github, index):
    github.topic(number=1, body="Body.")
    github.topic(number=2, body="Good.")
    github.fail("/repos/synthetic/repo-1/issues/1/comments", 500, times=99)
    with pytest.raises(ApiError):
        index.sync()
    assert index.cursor is None and not index.published


def test_resource_failure_while_checking_a_document_is_not_quarantined(github, index, monkeypatch):
    def out_of_memory(changes):
        raise pa.ArrowMemoryError("malloc of 64 bytes failed")

    monkeypatch.setattr("fyrr_connectors.github.connector.document_changes", out_of_memory)
    github.topic(number=1, body="Body.")
    github.topic(number=2, body="Good.")
    with pytest.raises(pa.ArrowMemoryError):
        index.sync()
    assert index.cursor is None and not index.published


def valid_document_revisions(renderer):
    """`source_revision` of documents the contract already accepted, by name."""
    repo = Repository(1, "synthetic/repo-1", "private", 5, "Organization", False)
    topic = {"title": "\u00e9" * 300, "body": "Body.", "state": "open", "labels": []}
    inline = {"kind": "inline", "id": 3, "body": "Inline.", "path": "src/a.py", "line": 7}
    ref = ConversationRef(1, FeedKind.PR, 1001, 1)

    def conversation(**fields):
        return renderer.conversation(repo, ref, {**topic, **fields}, [inline], 1 << 16).revision

    def file(path, source):
        file_ref = FileRef(1, "main", path, "b" * 40, "c" * 40, len(source))
        return renderer.file(repo, file_ref, source.encode(), None, 1 << 16).revision

    return {
        "title-600-bytes": conversation(),
        "title-1024-bytes": conversation(title="\u00e9" * 512),
        "title-and-labels": conversation(title="Plain", labels=[{"name": "bug"}], state="open"),
        "heading-trailing-spaces": file("a.md", "Intro.\n# Title  \nText."),
        "heading-over-512-bytes": file("a.md", "Intro.\n# " + "\u00e9" * 500 + "\nText."),
        "path-label-over-512-bytes": file("\u00e9" * 300 + ".md", "Text."),
    }


# Produced by running valid_document_revisions against render.py as of commit
# 34fa1eb (the base of this change), loaded as a temporary sibling module.
PINNED_REVISIONS = {
    "title-600-bytes": "4f54f2d05d35fcb95a9e006ff4acaa15c5a154614a2b173afc7b70e743ab0ce3",
    "title-1024-bytes": "11d72d6751cb71f0605e88d4d9a4e4292b633472bb27046909cb7bcbe8a5903f",
    "title-and-labels": "0ecd1e2b5255df995e48a91217b44fefebdd092762f51a702132ef7b731aee52",
    "heading-trailing-spaces": "9a67d0a343ef77b29f13c2cf6e48b6bc252802a0716497c45f3be7aa113576d4",
    "heading-over-512-bytes": "32b8daeb8ba3e9e73232a45fb2274d665a2604ea1bb0623050606f78403e71d3",
    "path-label-over-512-bytes": "6bfc44e1cb1e7169c3c874525b39a688f023ffcb59d1dc78b6434fcd4c7fa8a3",
}


def test_documents_the_contract_accepted_keep_their_revision():
    assert valid_document_revisions(render) == PINNED_REVISIONS


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
