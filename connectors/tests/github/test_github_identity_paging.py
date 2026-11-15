"""Repository identity, timestamp paging and replay after reinstallation."""

import pytest


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
