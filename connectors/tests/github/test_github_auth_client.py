"""App identity/auth, bounded rate waits and hostile REST responses."""

import pytest

from fyrr_connectors import _http
from fyrr_connectors.github.auth import AppAuth
from fyrr_connectors.github.client import ATTEMPTS, ApiError, Client
from fyrr_connectors.github.config import Settings


def test_auth_verifies_installation_and_mints_read_only_token(github, index):
    transport = Client(github.origin)
    auth = AppAuth(transport, Settings.parse(index.settings))
    assert auth.token() == github.token
    assert auth.installation.account_id == 30
    calls = [params for method, path, params in github.requests if method == "POST"]
    assert len(calls) == 1 and set(calls[0]["permissions"].values()) == {"read"}
    assert auth.token() == github.token
    assert len([method for method, _, _ in github.requests if method == "POST"]) == 1


def test_a_token_request_that_may_have_been_sent_is_not_sent_again(github, index, monkeypatch):
    import requests

    handled = github.handle

    def lost(method, path, params, headers):
        reply = handled(method, path, params, headers)  # the server issued a token ...
        if method == "POST":
            raise requests.ReadTimeout("slow")  # ... and the answer was lost
        return reply

    monkeypatch.setattr(github, "handle", lost)
    auth = AppAuth(Client(github.origin), Settings.parse(index.settings))
    with pytest.raises(ApiError):
        auth.refresh()
    assert len([m for m, _, _ in github.requests if m == "POST"]) == 1


@pytest.mark.parametrize("status", [403, 429])
def test_a_token_request_refused_for_the_rate_limit_is_sent_again(github, index, status):
    github.fail("/app/installations/20/access_tokens", status, headers={"Retry-After": "1"})
    auth = AppAuth(Client(github.origin), Settings.parse(index.settings))
    assert auth.token() == github.token
    assert github.sleeps == [1]
    assert len([m for m, _, _ in github.requests if m == "POST"]) == 2


def test_a_token_request_is_sent_again_after_a_failed_connection(github, index, monkeypatch):
    import requests
    import urllib3

    handled = github.handle
    refused = []

    def unreachable(method, path, params, headers):
        if method == "POST" and not refused:
            refused.append(path)
            reason = urllib3.exceptions.NewConnectionError(None, "refused")
            raise requests.ConnectionError(urllib3.exceptions.MaxRetryError(None, "/", reason))
        return handled(method, path, params, headers)

    monkeypatch.setattr(github, "handle", unreachable)
    auth = AppAuth(Client(github.origin), Settings.parse(index.settings))
    assert auth.token() == github.token
    assert len([m for m, _, _ in github.requests if m == "POST"]) == 1


@pytest.mark.parametrize(
    "field,value", [("app_id", 99), ("installation_id", 99), ("account_id", 99)]
)
def test_wrong_app_installation_or_account_fails(github, index, field, value):
    index.settings[field] = value
    with pytest.raises((ApiError, ValueError)):
        index.sync()
    assert not index.published


def test_suspended_installation_fails(github, index, monkeypatch):
    original = github._answer

    def answer(method, path, params):
        status, body, headers = original(method, path, params)
        if path == "/app/installations/20":
            body["suspended_at"] = "2026-01-01T00:00:00Z"
        return status, body, headers

    monkeypatch.setattr(github, "_answer", answer)
    with pytest.raises(ValueError, match="installation_mismatch"):
        index.sync()


def test_token_refreshes_once_on_401(github, index):
    github.topic()
    github.fail("/installation/repositories", 401)
    assert index.sync()
    assert len([path for method, path, _ in github.requests if method == "POST"]) == 2


@pytest.mark.parametrize(
    "status,headers,expected",
    [(429, {"Retry-After": "2"}, 2), (403, {"Retry-After": "3"}, 3), (429, {}, 60)],
)
def test_rate_limits_honor_server_waits(github, index, status, headers, expected):
    github.fail("/installation/repositories", status, headers=headers)
    index.sync()
    assert github.sleeps == [expected]


@pytest.mark.parametrize("status", [403, 429])
def test_a_retry_after_date_in_the_past_is_no_wait_and_retries_are_bounded(github, index, status):
    past = {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}
    github.fail("/installation/repositories", status, headers=past)
    index.sync()
    assert github.sleeps == []
    github.requests.clear()
    github.fail("/installation/repositories", status, headers=past, times=ATTEMPTS + 3)
    with pytest.raises(ApiError) as caught:
        index.sync()
    assert caught.value.status == status and caught.value.transient
    asked = [path for _, path, _ in github.requests if path == "/installation/repositories"]
    assert len(asked) == ATTEMPTS  # the first call and five retries


@pytest.mark.parametrize("value", ["-1", "nan", "soon"])
def test_an_invalid_retry_after_is_refused_without_a_wait(github, index, value):
    github.fail("/installation/repositories", 429, headers={"Retry-After": value})
    with pytest.raises(ValueError, match="rate_header_invalid"):
        index.sync()
    assert github.sleeps == []


def test_plain_permission_403_is_not_retried(github, index):
    github.fail("/installation/repositories", 403, times=ATTEMPTS)
    with pytest.raises(ApiError) as caught:
        index.sync()
    assert caught.value.status == 403 and not caught.value.transient
    assert github.sleeps == []


def test_rate_wait_is_not_clamped_or_retried_early(github, index):
    github.fail("/installation/repositories", 429, headers={"Retry-After": "301"})
    with pytest.raises(ValueError, match="wait_budget"):
        index.sync()
    assert github.sleeps == []


@pytest.mark.parametrize(
    "corruption", ["cross_origin", "repeated_page", "malformed_entries", "partial_inventory"]
)
def test_hostile_pages_are_refused(github, index, monkeypatch, corruption):
    github.repo(2)
    github.repo(3)
    original = github._answer

    def answer(method, path, params):
        status, body, headers = original(method, path, params)
        if path == "/installation/repositories":
            if corruption == "cross_origin":
                headers["Link"] = (
                    "<https://other.invalid/installation/repositories?page=2&per_page=100>; "
                    'rel="next"'
                )
            elif corruption == "repeated_page":
                headers["Link"] = (
                    f'<{github.origin}/installation/repositories?page=1&per_page=100>; rel="next"'
                )
            elif corruption == "malformed_entries":
                body["repositories"] = ["bad"]
            elif corruption == "partial_inventory":
                headers.pop("Link", None)
        return status, body, headers

    monkeypatch.setattr(github, "_answer", answer)
    with pytest.raises(ValueError):
        index.sync()


def test_response_size_is_bounded(github, index, monkeypatch):
    monkeypatch.setattr(_http, "MAX_RESPONSE", 64)
    with pytest.raises(ValueError, match="response_limit"):
        index.sync()


def test_page_work_is_bounded(github, index, monkeypatch):
    github.repo(2)
    github.repo(3)
    monkeypatch.setattr(_http, "MAX_PAGES", 1)
    with pytest.raises(ValueError, match="listing_limit"):
        index.sync()


def test_invalid_key_and_error_payloads_never_escape(github, index, monkeypatch):
    monkeypatch.setenv("FYRR_TEST_GITHUB_KEY", "synthetic-invalid-private-key")
    with pytest.raises(ValueError) as caught:
        index.sync()
    assert str(caught.value) == "github_app_key_invalid"


@pytest.mark.parametrize(
    "field,value",
    [
        ("since", "private@example.test"),
        ("commit", "private@example.test"),
        ("rotate", "private@example.test"),
    ],
)
def test_cursor_contains_only_validated_ids_timestamps_digests(github, index, field, value):
    github.topic()
    index.sync()
    import json

    state = json.loads(index.cursor)
    state["repos"]["1"][field] = value
    with pytest.raises(ValueError, match="cursor_invalid"):
        list(index.run(cursor=json.dumps(state)))
    assert "synthetic-author" not in index.cursor and "@" not in index.cursor
