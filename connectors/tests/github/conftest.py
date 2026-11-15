"""The sole GitHub fake transport; no request can reach a tenant."""

import pytest
from fake_github import FakeGitHub
from github_kit import Index

from fyrr_connectors.github import client


@pytest.fixture
def github(monkeypatch):
    world = FakeGitHub()
    original = client.session
    monkeypatch.setattr(client, "session", lambda: world.attach(original()))
    monkeypatch.setattr(client.time, "sleep", world.sleeps.append)
    monkeypatch.setenv("FYRR_TEST_GITHUB_KEY", world.key)
    return world


@pytest.fixture
def index(github):
    return Index(github)
