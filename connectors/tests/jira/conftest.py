import pytest


@pytest.fixture
def jira(monkeypatch):
    from fake_jira import FakeJira

    from fyrr_connectors.jira import client

    world = FakeJira()
    original = client.session
    monkeypatch.setattr(client, "session", lambda: world.attach(original()))
    monkeypatch.setattr(client.time, "sleep", world.sleeps.append)
    monkeypatch.setenv("FYRR_TEST_JIRA_TOKEN", world.token)
    return world


@pytest.fixture
def jira_index(jira):
    from jira_kit import Index

    return Index(jira)
