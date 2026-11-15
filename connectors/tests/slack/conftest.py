"""All Slack U paths use this single stateful fake; no tenant can be reached."""

import pytest
from fake_slack import FakeSlack
from slack_kit import Index

from fyrr_connectors.slack import client


@pytest.fixture
def slack(monkeypatch):
    world = FakeSlack()
    original = client.session
    monkeypatch.setattr(client, "session", lambda: world.attach(original()))
    monkeypatch.setattr(client.time, "sleep", world.sleeps.append)
    monkeypatch.setenv("FYRR_TEST_SLACK_TOKEN", world.token)
    return world


@pytest.fixture
def slack_index(slack):
    return Index(slack)
