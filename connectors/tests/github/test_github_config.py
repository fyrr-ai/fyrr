"""Explicit repository scope and environment-only App credentials."""

import pytest

from fyrr_connectors.config import ConfigError
from fyrr_connectors.github.config import Scope, Settings


def test_scope_defaults():
    repo = Scope.parse({"repositories": [{"id": 1, "branch": "main"}]}).repositories[0]
    assert (repo.issues, repo.pull_requests, repo.prose, repo.code) == (True, True, True, False)


@pytest.mark.parametrize(
    "repo",
    [
        {"id": 0, "branch": "main"},
        {"id": True, "branch": "main"},
        {"id": "1", "branch": "main"},
        {"id": 1},
        {"id": 1, "branch": ""},
        {"id": 1, "branch": "\0"},
        {"id": 1, "branch": "main", "unknown": True},
        {
            "id": 1,
            "branch": "main",
            "issues": False,
            "pull_requests": False,
            "prose": False,
            "code": False,
        },
    ],
)
def test_invalid_repository_is_refused(repo):
    with pytest.raises(ConfigError):
        Scope.parse({"repositories": [repo]})


def test_scope_rejects_duplicate_repositories():
    with pytest.raises(ConfigError):
        Scope.parse({"repositories": [{"id": 1, "branch": "a"}, {"id": 1, "branch": "b"}]})


def test_private_key_only_accepts_environment_references(index):
    with pytest.raises(ConfigError):
        Settings.parse({**index.settings, "private_key": "synthetic-secret"})
    schema = Settings.json_schema()
    assert schema["properties"]["private_key"]["x-fyrr-secret"]
