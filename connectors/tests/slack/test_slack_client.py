"""Slack-specific rate, capability, pagination and bounded-input failures."""

import pytest
from fake_slack import FakeServer

from fyrr_connectors.slack.auth import Identity
from fyrr_connectors.slack.client import ApiError, Client
from fyrr_connectors.slack.config import Settings
from fyrr_connectors.slack.feeds import Channel, history


@pytest.mark.parametrize("status", [429, 503])
def test_slack_retry_after_and_transient_retry(slack, status):
    slack.fail("auth.test", status=status, headers={"Retry-After": "3"})
    client = Client(slack.origin, slack.token)
    assert client.get("auth.test")["ok"]
    assert len(slack.sleeps) == 1
    expected = 3 if status == 429 else 1
    assert expected <= slack.sleeps[0] <= expected + 1
    before = list(slack.sleeps)
    client.get("auth.test")
    assert slack.sleeps == before


def test_slack_refuses_early_retry_past_the_wait_budget(slack):
    slack.fail("auth.test", status=429, headers={"Retry-After": "301"})
    with pytest.raises(ApiError, match="rate_header_invalid"):
        Client(slack.origin, slack.token).get("auth.test")
    assert not slack.sleeps


def test_slack_auth_pins_workspace_and_user_token(slack, slack_index):
    settings = Settings.parse(slack_index.settings)
    with pytest.raises(ValueError, match="user_token_required"):
        Identity.read(Client(slack.origin, "xoxb-synthetic"), settings)
    slack.workspace = "TOTHER"
    with pytest.raises(ValueError, match="workspace_identity_invalid"):
        slack_index.sync()


@pytest.mark.parametrize("fault", ["repeat", "missing", "limited", "wrong_channel"])
def test_slack_history_never_completes_corrupted_paging(slack, slack_index, monkeypatch, fault):
    for _ in range(4):
        slack.post()
    original = slack._answer

    def answer(method, params):
        body = original(method, params)
        if method == "conversations.history":
            if fault == "repeat":
                body["response_metadata"]["next_cursor"] = "same"
            elif fault == "missing":
                body["response_metadata"]["next_cursor"] = ""
            elif fault == "limited":
                body["is_limited"] = True
            elif body["messages"]:
                body["messages"][0]["channel"] = "COTHER"
        return body

    monkeypatch.setattr(slack, "_answer", answer)
    with pytest.raises((ApiError, ValueError)):
        next(slack_index.run("reconcile"))


def test_slack_unknown_payload_errors_are_value_free(slack):
    marker = "synthetic-payload-marker"
    slack.fail("auth.test", marker)
    with pytest.raises(ApiError) as raised:
        Client(slack.origin, slack.token).get("auth.test")
    assert marker not in str(raised.value)


@pytest.mark.parametrize("method", ["users.info", "conversations.list"])
def test_slack_channel_loss_codes_on_unscoped_methods_are_ambiguous(slack, method):
    slack.fail(method, "not_in_channel")
    with pytest.raises(ApiError) as error:
        Client(slack.origin, slack.token).get(method)
    assert not error.value.gone


def test_slack_a_new_explicit_unavailable_channel_is_not_successful_empty_scope(slack, slack_index):
    slack_index.scope["include"] = ["CMISSING"]
    with pytest.raises(ApiError):
        slack_index.sync()


def test_slack_one_fake_runs_over_http(slack, monkeypatch):
    import requests

    slack.post(text="HTTP body.")
    fake = FakeServer(slack)
    try:
        client = Client(fake.endpoint, slack.token)
        monkeypatch.setattr(client, "session", requests.Session())
        channel = Channel.read(client, "C001")
        assert len(history(client, channel, 0, slack.clock + 1_000_000)) == 1
    finally:
        fake.close()
