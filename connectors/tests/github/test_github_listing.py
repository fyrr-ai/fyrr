"""Completeness is evidence, never successful traversal alone."""

import pytest

from fyrr_connectors.github.client import ATTEMPTS, ApiError


def test_narrowing_and_branch_switch_remove_old_roots(github, index):
    github.topic()
    github.topic(kind="pr", number=2)
    github.branch(1, {"old.md": "Old."})
    before = index.sync()
    index.scope["repositories"][0].update(issues=False, prose=False)
    remaining = index.listing()
    assert len(remaining) == 1 and remaining < set(before)
    github.branch(1, {"other.md": "Other."}, branch="other")
    index.scope["repositories"][0].update(branch="other", prose=True)
    assert not {root for root in before if ":file:" in root} & index.listing()


@pytest.mark.parametrize("status", [401, 403, 404, 429, 503])
def test_failed_page_never_completes(github, index, status):
    github.topic()
    github.fail("/repos/synthetic/repo-1/issues", status, times=ATTEMPTS)
    parts = []
    with pytest.raises((ApiError, ValueError)):
        for part in index.run("reconcile"):
            parts.append(part)
    assert not any(part.complete for part in parts)


def test_changed_branch_never_completes(github, index):
    github.branch(1, {"old.md": "Old."})
    github.after(
        lambda method, path, params: "/git/trees/" in path,
        lambda: github.branch(1, {"new.md": "New."}),
    )
    with pytest.raises(ValueError, match="branch_moved"):
        list(index.run("reconcile"))


def test_changed_repository_locator_never_completes(github, index):
    github.topic()
    github.after(
        lambda method, path, params: path.endswith("/issues"),
        lambda: github.repos[1].update(full_name="synthetic/renamed"),
    )
    with pytest.raises((ApiError, ValueError)):
        list(index.run("reconcile"))


def test_proven_repository_removal_omits_old_roots(github, index):
    github.topic()
    index.sync()
    github.repos[1]["installed"] = False
    assert index.listing() == set()


def test_initially_uninstalled_repository_fails(github, index):
    github.repos[1]["installed"] = False
    with pytest.raises(ValueError, match="repository_not_installed"):
        index.sync()


def test_missing_branch_fails_instead_of_empty_tree(github, index):
    index.scope["repositories"][0]["branch"] = "missing"
    with pytest.raises(ApiError):
        index.listing()


def test_fetched_issue_parent_is_verified(github, index, monkeypatch):
    github.topic()
    original = github._topic

    def changed(*args):
        result = original(*args)
        result["repository_url"] = "https://api.github.com/repos/other/private"
        return result

    monkeypatch.setattr(github, "_topic", changed)
    with pytest.raises(ValueError, match="parent_changed"):
        index.sync()
    assert not index.published


def test_fetched_pr_parent_is_verified(github, index, monkeypatch):
    github.topic(kind="pr")
    original = github._topic

    def changed(*args):
        result = original(*args)
        result["base"]["repo"]["id"] = 999
        return result

    monkeypatch.setattr(github, "_topic", changed)
    with pytest.raises(ValueError, match="parent_changed"):
        index.sync()


def test_fetched_comment_parent_is_verified(github, index, monkeypatch):
    github.topic()
    github.comment()
    original = github._parts

    def changed(*args):
        result = original(*args)
        for part in result:
            part["issue_url"] = "https://api.github.com/repos/other/private/issues/1"
        return result

    monkeypatch.setattr(github, "_parts", changed)
    with pytest.raises(ValueError, match="child_parent_changed"):
        index.sync()


def test_offset_race_does_not_delete_missing_published_issue(github, index, monkeypatch):
    for ordinal in range(1, 5):
        github.topic(number=ordinal)
    original_roots = set(index.sync())
    original = github._answer

    def answer(method, path, params):
        status, body, headers = original(method, path, params)
        if path.endswith("/issues"):
            # Apply the synthetic omission before paging so both offset and
            # key-seeking clients see the same valid filtered collection.
            entries = [github._topic(1, "issue", n) for n in (1, 3, 4)]
            entries = [entry for entry in entries if entry["updated_at"] >= params.get("since", "")]
            body, headers = github._page(entries, path, params)
        return status, body, headers

    monkeypatch.setattr(github, "_answer", answer)
    assert index.listing() == original_roots


def test_truncated_nonrecursive_tree_fails(github, index, monkeypatch):
    original = github._answer

    def answer(method, path, params):
        status, body, headers = original(method, path, params)
        if "/git/trees/" in path:
            body["truncated"] = True
        return status, body, headers

    monkeypatch.setattr(github, "_answer", answer)
    with pytest.raises(ValueError, match="tree_incomplete"):
        index.listing()
