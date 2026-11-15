"""Fixtures: the fake Notion attached in process. Helpers live in
`notion_kit`; nothing is imported from this module by name."""

from __future__ import annotations

import pytest
from fake_notion import FakeNotion
from notion_kit import Index

from fyrr_connectors.notion import client


@pytest.fixture
def notion(monkeypatch: pytest.MonkeyPatch) -> FakeNotion:
    world = FakeNotion()
    world.user("ada", "Ada Example")
    world.page("root", title="Handbook")
    # The connector's own session, with its headers, on the fake's transport.
    real = client.session
    monkeypatch.setattr(client, "session", lambda token: world.attach(real(token)))
    monkeypatch.setattr(client.time, "sleep", world.sleeps.append)
    monkeypatch.setattr(client, "MIN_INTERVAL_SECONDS", 0.0)
    monkeypatch.setenv("FYRR_TEST_NOTION_TOKEN", world.token)
    return world


@pytest.fixture
def index(notion: FakeNotion) -> Index:
    return Index(notion)
