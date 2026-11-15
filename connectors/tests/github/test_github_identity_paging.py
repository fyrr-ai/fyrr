"""Repository identity, timestamp paging and replay after reinstallation."""

import pytest

from fyrr_connectors.github.feeds import Repository, comment_hints


def test_acl_name_reuse_cannot_grant_another_repositorys_readers(github, index):
    index.mode = "sync"
    github.repo(2)
    github.collaborator(1, 7)
    github.collaborator(2, 8)
    old = github.repos[1]["full_name"]

    def rename():
        github.repos[1]["full_name"] = "synthetic/moved"
        github.repos[2]["full_name"] = old

    github.after(
        lambda method, path, params: (
            path == "/installation/repositories"
            and sum(p == path for _, p, _ in github.requests) == 2
        ),
        rename,
    )
    stream = index.run("permissions")
    with pytest.raises(ValueError, match=r"repository.*changed|repository.*moved"):
        next(stream)


def test_updated_issue_key_paging_never_skips_the_101st_issue(github, index):
    github.page_size = 100
    for number in range(1, 104):
        github.topic(number=number)
    github.after(
        lambda method, path, params: path.endswith("/issues"),
        lambda: github.topic(number=1, body="Edited during paging."),
    )
    index.sync()
    assert {int(root.rsplit(":", 1)[1]) for root in index.published} == set(range(1, 104))


@pytest.mark.parametrize("poll_when_absent", [False, True])
def test_reinstalled_repository_restores_files_at_the_same_head(github, index, poll_when_absent):
    github.branch(1, {"README.md": "Unchanged prose."})
    original = index.sync()
    github.repos[1]["installed"] = False
    if poll_when_absent:
        index.sync()
    assert index.listing() == set()
    index.published.clear()
    index.revisions.clear()
    github.repos[1]["installed"] = True
    assert set(index.sync()) == set(original)


def test_pr_discovery_uses_the_keyed_issue_stub_feed(github, index):
    github.page_size = 100
    for number in range(1, 104):
        github.topic(kind="pr", number=number)
    github.after(
        lambda method, path, params: path.endswith("/issues"),
        lambda: github.topic(kind="pr", number=1, body="Edited PR."),
    )
    assert len(index.sync()) == 103
    assert not any(path.endswith("/pulls") for _, path, _ in github.requests)


def test_comment_hint_key_paging_does_not_skip_parent_101(github, index):
    github.page_size = 100
    for number in range(1, 104):
        github.topic(number=number)
        github.comment(number=number)

    def edit():
        github.comments[1, "issue", 1][0]["updated_at"] = github.stamp()

    github.after(lambda method, path, params: path.endswith("/issues/comments"), edit)
    connector = index.connector()
    connector._open()
    refs = comment_hints(
        connector.client,
        Repository.read(connector.client, 1),
        connector.scope.repositories[0],
        "2025-01-01T00:00:00+00:00",
    )
    assert {ref.number for ref in refs} == set(range(1, 104))


def test_unseekable_timestamp_bucket_never_advances_the_cursor(github, index):
    for number in range(1, 5):
        github.topic(number=number, updated_at="2026-01-01T00:01:00+00:00")
    with pytest.raises(ValueError, match="updated_tie_limit"):
        index.sync()
    assert index.cursor is None


def test_file_reads_recheck_repository_identity_before_yield(github, index):
    github.repo(2)
    github.branch(1, {"README.md": "A body."})
    old = github.repos[1]["full_name"]

    def rename():
        github.repos[1]["full_name"] = "synthetic/moved"
        github.repos[2]["full_name"] = old

    github.after(lambda method, path, params: "/branches/" in path, rename)
    with pytest.raises(ValueError, match="repository_changed"):
        next(index.run())


def test_child_snapshot_retries_a_paging_shift_even_without_parent_edit(github, index):
    github.topic()
    for number in range(1, 5):
        github.comment(body=f"Comment {number}.")
    github.after(
        lambda method, path, params: path.endswith("/issues/1/comments"),
        lambda: github.comments[1, "issue", 1].pop(0),
    )
    changes = index.sync()
    body = str(next(iter(changes.values()))["sections"])
    assert "Comment 1." not in body
    assert all(f"Comment {number}." in body for number in range(2, 5))


def test_partial_reinstall_replay_restores_each_missing_file_at_same_head(github, index):
    github.branch(1, {"a.md": "A.", "b.md": "B."})
    initial = index.sync()
    missing = next(iter(initial))
    index.revisions.pop(missing)
    index.published.pop(missing)
    assert missing in index.sync()


def test_rename_at_same_head_refreshes_file_metadata(github, index):
    github.branch(1, {"README.md": "Prose."})
    index.sync()
    github.repos[1]["full_name"] = "synthetic/renamed"
    changes = index.sync()
    assert len(changes) == 1
    assert "synthetic/renamed" in next(iter(changes.values()))["metadata"]["source_uri"]
