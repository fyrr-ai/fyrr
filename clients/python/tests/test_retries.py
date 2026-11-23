"""Retry, backoff and paging rules, against a transport that answers as scripted."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import SimpleNamespace

import httpx
import pytest

from fyrr_client import AsyncClient, Client, FyrrError
from fyrr_client import client as client_module
from fyrr_client.models import WriteRequest

READER, WRITER = "http://reader.test", "http://writer.test"


def ok() -> httpx.Response:
    return httpx.Response(200, json={"upserted": 1, "deleted": 0})


class Script:
    """Answers each request with the next of `answers`; a callable answer may raise."""

    def __init__(
        self, *answers: httpx.Response | Callable[[httpx.Request], httpx.Response]
    ) -> None:
        self.answers = list(answers)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.answers.pop(0)
        return answer(request) if callable(answer) else answer


@pytest.fixture
def slept(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Records the waits instead of making them."""
    waits: list[float] = []

    async def async_sleep(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr(client_module, "time", SimpleNamespace(sleep=waits.append))
    monkeypatch.setattr(client_module, "asyncio", SimpleNamespace(sleep=async_sleep))
    return waits


def found() -> httpx.Response:
    return httpx.Response(200, json={"hits": []})


def timed_out(request: httpx.Request) -> httpx.Response:
    raise httpx.ReadTimeout("slow", request=request)


def connect_timeout(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectTimeout("slow", request=request)


def refused(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("refused", request=request)


def client(script: Script, max_retries: int = 3, backoff: float = 0.5) -> Client:
    return Client(
        READER,
        WRITER,
        api_key="k",
        transport=httpx.MockTransport(script),
        max_retries=max_retries,
        backoff=backoff,
    )


WRITE: WriteRequest = {"upserts": [{"id": "a", "text": "t"}]}


def write(client: Client) -> object:
    return client.write("ns", WRITE)


def delete(client: Client) -> object:
    return client.delete_namespace("ns")


@pytest.mark.parametrize("status", [408, 429, 500, 503, 504])
def test_a_read_is_retried_on_the_retryable_statuses(status: int, slept: list[float]) -> None:
    script = Script(httpx.Response(status, json={"error": "busy"}), found())
    assert client(script).query("ns", {})["hits"] == []
    assert len(script.requests) == 2 and len(slept) == 1


def test_a_read_is_retried_after_a_connection_error_and_a_timeout(slept: list[float]) -> None:
    script = Script(refused, timed_out, found())
    assert client(script).query("ns", {})["hits"] == []
    assert len(slept) == 2


@pytest.mark.parametrize("send", [write, delete])
def test_a_change_is_retried_only_when_it_was_not_sent_or_refused_unread(
    send: Callable[[Client], object], slept: list[float]
) -> None:
    script = Script(
        refused,
        connect_timeout,
        httpx.Response(429, headers={"Retry-After": "1"}, json={"error": "rate_limited"}),
        ok(),
    )
    send(client(script))
    assert len(script.requests) == 4 and len(slept) == 3


@pytest.mark.parametrize("send", [write, delete])
@pytest.mark.parametrize(
    "answer", [httpx.Response(503, json={"error": "outcome_unknown"}), timed_out]
)
def test_a_change_that_may_have_applied_is_not_retried(
    send: Callable[[Client], object],
    answer: httpx.Response | Callable[[httpx.Request], httpx.Response],
    slept: list[float],
) -> None:
    script = Script(answer, ok())
    with pytest.raises((FyrrError, httpx.TimeoutException)):
        send(client(script))
    assert len(script.requests) == 1 and slept == []


def test_a_wrong_request_is_not_retried(slept: list[float]) -> None:
    def unsupported(request: httpx.Request) -> httpx.Response:
        raise httpx.UnsupportedProtocol("no scheme", request=request)

    script = Script(unsupported, found())
    with pytest.raises(httpx.UnsupportedProtocol):
        client(script).query("ns", {})
    assert len(script.requests) == 1


def test_an_answer_that_is_not_json_is_a_fyrr_error() -> None:
    script = Script(httpx.Response(200, text="<html>"))
    with pytest.raises(FyrrError) as raised:
        client(script).query("ns", {})
    assert (raised.value.status, raised.value.code) == (200, "invalid_json")


def test_other_errors_are_final(slept: list[float]) -> None:
    script = Script(httpx.Response(400, json={"error": "invalid_argument"}))
    with pytest.raises(FyrrError) as raised:
        client(script).query("ns", {})
    assert (raised.value.status, raised.value.code) == (400, "invalid_argument")
    assert slept == []


def test_retry_after_is_honoured_and_capped(slept: list[float]) -> None:
    script = Script(
        httpx.Response(408, headers={"Retry-After": "7"}, json={"error": "deadline_exceeded"}),
        httpx.Response(503, headers={"Retry-After": "100000"}, json={"error": "busy"}),
        found(),
    )
    client(script).query("ns", {})
    assert slept == [7.0, client_module.MAX_RETRY_AFTER]


def test_backoff_doubles_and_gives_up(slept: list[float]) -> None:
    script = Script(*[httpx.Response(503, json={"error": "busy"})] * 3)
    with pytest.raises(FyrrError) as raised:
        client(script, max_retries=2, backoff=1.0).query("ns", {})
    assert raised.value.status == 503
    assert len(script.requests) == 3
    assert 0.5 <= slept[0] <= 1.0 and 1.0 <= slept[1] <= 2.0


def test_a_conditional_write_is_never_retried(slept: list[float]) -> None:
    conditional: WriteRequest = {
        **WRITE,
        "conditions": {"a": {"field": "version", "op": "lt", "value": 2}},
    }
    busy = Script(httpx.Response(503, json={"error": "outcome_unknown"}), ok())
    with pytest.raises(FyrrError):
        client(busy).write("ns", conditional)
    assert len(busy.requests) == 1
    dropped = Script(refused, ok())
    with pytest.raises(httpx.ConnectError):
        client(dropped).write("ns", conditional)
    assert len(dropped.requests) == 1 and slept == []


def test_the_async_client_retries_alike(slept: list[float]) -> None:
    script = Script(
        httpx.Response(429, headers={"Retry-After": "3"}, json={"error": "rate_limited"}), ok()
    )

    async def scenario() -> None:
        async with AsyncClient(
            READER, WRITER, api_key="k", transport=httpx.MockTransport(script)
        ) as asynchronous:
            assert (await asynchronous.write("ns", WRITE))["upserted"] == 1

    asyncio.run(scenario())
    assert slept == [3.0]


def test_a_missing_writer_url_is_refused() -> None:
    with pytest.raises(ValueError, match="writer_url"):
        Client(READER, api_key="k").write("ns", WRITE)


def test_iter_namespaces_narrows_the_prefix_above_the_listing_limit() -> None:
    existing = ["a", "a1", "a2", "a3", "ab", "b", "c"]
    limit = 3  # the server's is 10,000

    def server(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/v1/namespaces":  # a namespace report
            found = request.url.path.rsplit("/", 1)[1] in existing
            return httpx.Response(200 if found else 401, json={"error": "unauthorized"})
        matching = [n for n in existing if n.startswith(request.url.params.get("prefix", ""))]
        if len(matching) > limit:
            return httpx.Response(400, json={"error": "too_many_namespaces"})
        return httpx.Response(200, json={"namespaces": matching})

    with Client(READER, api_key="k", transport=httpx.MockTransport(server)) as listing:
        assert list(listing.iter_namespaces()) == existing


def test_the_async_iter_namespaces_narrows_alike() -> None:
    def server(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("prefix"):
            return httpx.Response(200, json={"namespaces": ["x1"]})
        return httpx.Response(400, json={"error": "too_many_namespaces"})

    async def scenario() -> list[str]:
        transport = httpx.MockTransport(server)
        async with AsyncClient(READER, api_key="k", transport=transport) as listing:
            return [name async for name in listing.iter_namespaces()]

    # Every first character lists "x1": a stand-in server that is enough to see the narrowing.
    assert asyncio.run(scenario()) == ["x1"] * 36
