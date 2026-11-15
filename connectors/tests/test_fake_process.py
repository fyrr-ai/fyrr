"""Fake servers publish a connection, serve HTTP and shut down on input EOF."""

import json
import selectors
import sys
from pathlib import Path

import pytest
import requests
from owned_child import owned_child


def _drive(connection):
    from fyrr_connectors.google_drive import client

    endpoint = connection["endpoint"]
    key = json.loads(connection["service_account"])
    assert key["token_uri"] == f"{endpoint}/token"
    added = requests.post(f"{endpoint}/fake/user", json={"email": "alice@example.com"}, timeout=10)
    assert added.json() == {"result": None}
    with client.authorized_session(key, "alice@example.com", client.DRIVE) as session:
        start = session.get(f"{endpoint}/drive/v3/changes/startPageToken", timeout=10)
        assert start.json() == {"startPageToken": "0"}


def _notion(connection):
    from fyrr_connectors.notion import client

    endpoint = connection["endpoint"]
    added = requests.post(f"{endpoint}/fake/page", json={"name": "root"}, timeout=10)
    root = added.json()["result"]
    with client.session(connection["token"]) as session:
        page = session.get(f"{endpoint}/v1/pages/{root}", timeout=10)
        assert page.json()["object"] == "page" and "Date" in page.headers


def _github(connection):
    assert connection["private_key"].startswith("-----BEGIN PRIVATE KEY-----")
    response = requests.post(connection["endpoint"] + "/fake/topic", json={}, timeout=10)
    assert response.status_code == 200
    paths = requests.post(connection["endpoint"] + "/fake/requests", json={}, timeout=10)
    assert paths.json() == {"result": []}


def _slack(connection):
    response = requests.post(
        connection["endpoint"] + "/api/auth.test",
        headers={"Authorization": "Bearer xoxp-synthetic-only"},
        timeout=10,
    )
    assert response.json()["ok"] is True
    assert response.json()["team_id"] == "T001"


def _jira(connection):
    response = requests.get(
        connection["endpoint"] + "/rest/api/3/serverInfo",
        headers={"Authorization": "Bearer jira-synthetic-only"},
        timeout=10,
    )
    assert response.json()["deploymentType"] == "Cloud"


@pytest.mark.parametrize(
    ("fake", "check"),
    [
        pytest.param("google_drive/fake_drive.py", _drive, id="google-drive"),
        pytest.param("notion/fake_notion.py", _notion, id="notion"),
        pytest.param("github/fake_github.py", _github, id="github"),
        pytest.param("slack/fake_slack.py", _slack, id="slack"),
        pytest.param("jira/fake_jira.py", _jira, id="jira"),
    ],
)
def test_fake_process_lifecycle_cases(fake, check):
    with owned_child([sys.executable, str(Path(__file__).parent / fake)]) as process:
        with selectors.DefaultSelector() as ready:
            ready.register(process.stdout, selectors.EVENT_READ)
            assert ready.select(timeout=10), "fake did not publish its connection"
        connection = json.loads(process.stdout.readline())
        check(connection)
        process.stdin.close()
        assert process.wait(10) == 0
