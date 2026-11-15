"""The real requests transport against the same stateful loopback fake."""

from fake_github import FakeServer

from fyrr_connectors.github import client

REAL_SESSION = client.session


def test_fixed_sync_and_listing_use_real_http(github, index, monkeypatch):
    monkeypatch.setattr(client, "session", REAL_SESSION)
    for number in range(1, 6):
        github.topic(number=number, body="HTTP body.")
    github.comment(body="HTTP comment.")
    with FakeServer(github) as server:
        index.settings["api_endpoint"] = server.endpoint
        changes = index.sync()
        assert len(changes) == 5
        assert index.listing() == set(changes)
        assert index.sync() == {}
    assert github.sleeps == []
    assert not any("collaborators" in path for _, path, _ in github.requests)
