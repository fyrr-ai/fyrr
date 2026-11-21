"""Fixed content, aggregates, revisions, quarantine and acknowledged cursors."""

import hashlib
import json
from dataclasses import replace

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


def test_comments_name_their_author_and_association(github, index):
    github.topic(kind="pr", number=2, body="PR body.")
    github.comment(kind="pr", number=2, body="Default.")
    github.comment(kind="pr", number=2, body="Drive-by.", author_association="NONE")
    github.comment(kind="pr", number=2, body="No association.", author_association=None)
    github.comment(kind="pr", number=2, body="Deleted.", user=None)
    github.comment(kind="pr", number=2, body="Ghost.", user={"login": "ghost"})
    review = github.comment(kind="pr", number=2, body="", state="APPROVED")
    github.comment(kind="pr", number=2, body="Inline.", review_id=review["id"])
    texts = [s["text"] for s in next(iter(index.sync().values()))["sections"]]
    assert texts[0] == "Synthetic pr 2\n\nPR body."
    assert texts[1:] == [
        "synthetic-commenter (member):\nDefault.",
        "synthetic-commenter (none):\nDrive-by.",
        "synthetic-commenter:\nNo association.",
        "unknown:\nDeleted.",
        "unknown:\nGhost.",
        "synthetic-commenter (member):",
        "synthetic-commenter (member):\nInline.",
    ]


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
    github.comment(body="Golden comment.")
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


QUARANTINED = [{"text": "\0"}]
ISSUE = "github:repo:1:issue:{}:{}"


def file_root(path):
    branch = hashlib.sha256(b"main").hexdigest()[:32]
    return f"github:repo:1:file:{branch}:{hashlib.sha256(path.encode()).hexdigest()}"


def synced(index, quarantined, good):
    """Sync once: exactly `quarantined` roots are quarantined, `good` is
    published and the repository frontier is the server time of this sync."""
    changes = index.sync()
    assert {root for root, change in changes.items() if change["sections"] == QUARANTINED} == set(
        quarantined
    )
    assert good in index.published
    assert json.loads(index.cursor)["repos"]["1"]["since"] == index.world.server_time()


def test_file_the_contract_rejects_is_quarantined_and_the_sync_continues(github, index):
    # Percent-encoded, the URI is 4,200 bytes: an exact-match key, never cut.
    path = "/".join(["\u00e9" * 100] * 7) + "/a.md"
    github.branch(1, {path: "Prose.", "good.md": "Good."})
    synced(index, [file_root(path)], file_root("good.md"))


@pytest.mark.parametrize(
    ("fields", "field", "expected"),
    [
        ({"title": "  Padded\t"}, "title", "Padded"),
        ({"title": "A\nB"}, "title", "A B"),
        (
            {"labels": [{"name": f"l{n}"} for n in range(33)]},
            "labels",
            [f"l{n}" for n in range(32)],
        ),
        ({"state": " open "}, "status", "open"),
    ],
    ids=["padded-title", "control-character", "33-labels", "padded-status"],
)
def test_display_values_are_fitted_and_the_sync_continues(github, index, fields, field, expected):
    github.topic(number=1, body="Body.", **fields)
    github.topic(number=2, body="Good.")
    synced(index, [], ISSUE.format(102, 2))
    assert index.published[ISSUE.format(101, 1)]["metadata"][field] == expected


def test_markdown_headings_are_fitted(github, index):
    github.branch(1, {"a.md": "Intro.\n#\n# Ta\tb\x01c\n# Title\nEnd."})
    index.sync()
    labels = [s["location"]["label"] for s in index.published[file_root("a.md")]["sections"]]
    assert labels == ["a.md (line 1)", "(line 2)", "Ta b c (line 3)", "Title (line 4)"]


def test_control_character_in_an_inline_comment_path_only_changes_the_label(github, index):
    github.topic(kind="pr", number=1, body="PR.")
    review = github.comment(kind="pr", number=1, body="Review.", state="COMMENTED")
    github.comment(kind="pr", number=1, body="Inline.", review_id=review["id"])["path"] = "a\n/b.py"
    github.topic(number=2, body="Good.")
    synced(index, [], ISSUE.format(102, 2))
    sections = index.published["github:repo:1:pr:1001:1"]["sections"]
    assert [section["text"] for section in sections][1:] == [
        "synthetic-commenter (member):\nReview.",
        "synthetic-commenter (member):\nInline.",
    ]
    assert sections[-1]["location"]["label"].endswith("discussion_r1: a /b.py:3")


def valid_document_revisions(renderer):
    repo = Repository(1, "synthetic/repo-1", "private", 5, "Organization", False)
    topic = {
        "title": "\u00e9" * 500,
        "body": "Body.",
        "state": "open",
        "labels": [{"name": "z"}, {"name": "a"}],
    }
    inline = {"kind": "inline", "id": 3, "body": "Inline.", "path": "src/a.py", "line": 7}
    ref = ConversationRef(1, FeedKind.PR, 1001, 1)

    def file(path, source):
        file_ref = FileRef(1, "main", path, "b" * 40, "c" * 40, len(source))
        return renderer.file(repo, file_ref, source.encode(), None, 1 << 16).revision

    return {
        "multibyte-title": renderer.conversation(repo, ref, topic, [inline], 1 << 16).revision,
        "heading-trailing-spaces": file("a.md", "Intro.\n# Title  \nText."),
        "long-path-label": file("\u00e9" * 300 + ".md", "Text."),
    }


# Produced by loading render.py of commit 34fa1eb (before this change) as a
# temporary module and running valid_document_revisions against it. The
# multibyte-title value was recomputed when comments gained an author line.
PINNED_REVISIONS = {
    "multibyte-title": "af2fe7a2717b636b2f3e39525c90b11e20175db1e9d14d6035080442146d606c",
    "heading-trailing-spaces": "9a67d0a343ef77b29f13c2cf6e48b6bc252802a0716497c45f3be7aa113576d4",
    "long-path-label": "6bfc44e1cb1e7169c3c874525b39a688f023ffcb59d1dc78b6434fcd4c7fa8a3",
}


def test_documents_the_contract_accepted_keep_their_revision():
    assert valid_document_revisions(render) == PINNED_REVISIONS
