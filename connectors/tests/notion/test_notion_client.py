"""The client: which responses are retried, which are final, pacing and paging."""

import io
import json

import pytest
import requests
from fake_notion import TOKEN, VERSION, FakeNotion, ident
from requests.adapters import BaseAdapter

from fyrr_connectors.notion import client
from fyrr_connectors.notion.client import ATTEMPTS, ApiError, Client

ROOT = f"pages/{ident('root')}"


def _client() -> Client:
    return Client(TOKEN, "https://api.notion.com")


def _any(_path: str, _arguments: dict) -> bool:
    return True


class _Scripted(BaseAdapter):
    """Answers with the given bodies, one per request."""

    def __init__(self, *bodies: bytes) -> None:
        super().__init__()
        self.bodies = list(bodies)
        self.sent: list[dict] = []

    def send(self, request: requests.PreparedRequest, **_) -> requests.Response:
        self.sent.append(json.loads(request.body or b"{}"))
        response = requests.Response()
        response.status_code = 200
        response.raw = io.BytesIO(self.bodies.pop(0))
        return response

    def close(self) -> None:
        pass


def _scripted(monkeypatch: pytest.MonkeyPatch, *bodies: bytes) -> _Scripted:
    adapter = _Scripted(*bodies)
    session = requests.Session()
    session.mount("https://", adapter)
    monkeypatch.setattr(client, "session", lambda _token: session)
    return adapter


@pytest.mark.parametrize(
    ("status", "code"),
    [(429, "rate_limited"), (409, "conflict_error"), (500, "internal_server_error"), (503, "")],
)
def test_rate_limits_conflicts_and_server_errors_are_retried(
    notion: FakeNotion, status: int, code: str
) -> None:
    notion.fail(_any, status, code, times=2)
    assert _client().get(ROOT)["id"] == ident("root")
    assert len(notion.requests) == 3
    assert len(notion.sleeps) == 2 and notion.sleeps[0] < notion.sleeps[1]


def test_retry_after_is_honored_and_bounded(notion: FakeNotion) -> None:
    notion.fail(_any, 429, "rate_limited", retry_after="7")
    notion.fail(_any, 429, "rate_limited", retry_after="900")
    notion.fail(_any, 429, "rate_limited", retry_after="0")
    notion.fail(_any, 429, "rate_limited", retry_after="soon")
    _client().get(ROOT)
    assert 7 <= notion.sleeps[0] < 8.01
    assert notion.sleeps[1] <= client.MAX_WAIT_SECONDS + 1
    assert notion.sleeps[2] < 1.01
    assert 8 <= notion.sleeps[3] < 9.01, "an unreadable header leaves the backoff alone"


def test_exhausted_retries_are_transient_failures(notion: FakeNotion) -> None:
    notion.fail(_any, 502, "bad_gateway", times=99)
    with pytest.raises(ApiError) as caught:
        _client().get(ROOT)
    assert caught.value.transient and caught.value.status == 502
    assert not caught.value.missing and not caught.value.refused
    assert len(notion.requests) == ATTEMPTS


def test_transport_errors_are_retried(notion: FakeNotion, monkeypatch: pytest.MonkeyPatch) -> None:
    session = notion.attach(requests.Session())
    session.headers.update({"Authorization": f"Bearer {TOKEN}", "Notion-Version": VERSION})
    real, calls = session.request, []

    def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) < 3:
            raise requests.ConnectionError
        return real(*args, **kwargs)

    session.request = flaky
    monkeypatch.setattr(client, "session", lambda _token: session)
    assert _client().get(ROOT)["object"] == "page"
    assert len(calls) == 3


@pytest.mark.parametrize(
    ("status", "code", "missing", "refused"),
    [
        (404, "object_not_found", True, False),
        (403, "restricted_resource", False, False),
        (401, "unauthorized", False, False),
        (400, "validation_error", False, True),
    ],
)
def test_other_errors_are_final_and_carry_no_body(
    notion: FakeNotion, status: int, code: str, missing: bool, refused: bool
) -> None:
    notion.fail(_any, status, code)
    with pytest.raises(ApiError) as caught:
        _client().get(ROOT)
    error = caught.value
    assert (error.status, error.code, error.transient) == (status, code, False)
    assert (error.missing, error.refused) == (missing, refused)
    assert str(error) == f"notion_api_{status}_{code}"
    assert len(notion.requests) == 1 and notion.sleeps == []


def test_an_error_code_that_is_not_a_code_is_dropped(notion: FakeNotion) -> None:
    notion.fail(_any, 400, "Some text, perhaps with a page title")
    with pytest.raises(ApiError) as caught:
        _client().get(ROOT)
    assert str(caught.value) == "notion_api_400_"


def test_requests_are_paced(notion: FakeNotion, monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [100.0]

    def sleep(seconds: float) -> None:
        notion.sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(client, "MIN_INTERVAL_SECONDS", 0.35)
    monkeypatch.setattr(client.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(client.time, "sleep", sleep)
    reader = _client()
    for _ in range(3):
        reader.get(ROOT)
    clock[0] += 5
    reader.get(ROOT)
    assert notion.sleeps == [pytest.approx(0.35), pytest.approx(0.35)]


def test_the_clock_is_the_servers(notion: FakeNotion) -> None:
    reader = _client()
    reader.get(ROOT)
    assert reader.server_time == notion.now()


def test_a_list_is_read_to_its_end_or_fails(notion: FakeNotion) -> None:
    for number in range(5):
        notion.page(f"p{number}", "root")
    children = f"/v1/blocks/{ident('root')}/children"
    listed = [block["id"] for page in _client().pages(children[4:]) for block in page]
    assert listed == [ident(f"p{number}") for number in range(5)]
    assert [sent.get("start_cursor") for _, _, sent in notion.requests] == [None, "2", "4"]

    notion.fail(lambda path, sent: sent.get("start_cursor") == "4", 500, times=99)
    with pytest.raises(ApiError):
        list(_client().pages(children[4:]))


def test_a_list_cursor_that_does_not_advance_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    again = json.dumps({"results": [{}], "has_more": True, "next_cursor": "same"}).encode()
    adapter = _scripted(monkeypatch, again, again, again)
    with pytest.raises(ValueError, match="notion_paging_invalid"):
        list(_client().pages("search", {}))
    assert [sent.get("start_cursor") for sent in adapter.sent] == [None, "same"]

    _scripted(monkeypatch, json.dumps({"results": [], "has_more": True}).encode())
    with pytest.raises(ValueError, match="notion_paging_invalid"):
        list(_client().pages("search", {}))
    _scripted(monkeypatch, json.dumps({"results": "none"}).encode())
    with pytest.raises(ValueError, match="notion_response_invalid"):
        list(_client().pages("search", {}))


def test_an_answer_that_is_not_json_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client.time, "sleep", lambda _seconds: None)
    _scripted(monkeypatch, b"<html>busy</html>", b"[]", b'{"object": "page"}')
    assert _client().get("pages/x") == {"object": "page"}
    _scripted(monkeypatch, *[b"<html>busy</html>"] * ATTEMPTS)
    with pytest.raises(ApiError) as caught:
        _client().get("pages/x")
    assert caught.value.transient and str(caught.value) == "notion_api_200_body"
