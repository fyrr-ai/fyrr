"""Retry, backoff and paging rules, against a transport that answers as scripted."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
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
def test_a_change_is_retried_only_when_it_was_not_sent(
    send: Callable[[Client], object], slept: list[float]
) -> None:
    script = Script(refused, connect_timeout, ok())
    send(client(script))
    assert len(script.requests) == 3 and len(slept) == 2


@pytest.mark.parametrize("send", [write, delete])
@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(503, json={"error": "outcome_unknown"}),
        httpx.Response(429, headers={"Retry-After": "1"}, json={"error": "rate_limited"}),
        timed_out,
    ],
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


def test_retry_after_is_honoured_until_the_cap(slept: list[float]) -> None:
    script = Script(
        httpx.Response(408, headers={"Retry-After": "7"}, json={"error": "deadline_exceeded"}),
        httpx.Response(503, headers={"Retry-After": "100000"}, json={"error": "busy"}),
        found(),
    )
    with pytest.raises(FyrrError) as raised:
        client(script).query("ns", {})
    assert slept == [7.0]
    assert raised.value.retry_after == 100000.0 and len(script.requests) == 2


def test_backoff_doubles_and_gives_up(slept: list[float]) -> None:
    script = Script(*[httpx.Response(503, json={"error": "busy"})] * 3)
    with pytest.raises(FyrrError) as raised:
        client(script, max_retries=2, backoff=1.0).query("ns", {})
    assert raised.value.status == 503
    assert len(script.requests) == 3
    assert 0.5 <= slept[0] <= 1.0 and 1.0 <= slept[1] <= 2.0


Answer = httpx.Response | Callable[[httpx.Request], httpx.Response]


def shared_answer(name: str) -> Answer:
    answers: dict[str, Answer] = {
        "refused": refused,
        "ok": ok(),
        "429": httpx.Response(429, json={"error": "rate_limited"}),
        "503 outcome_unknown": httpx.Response(503, json={"error": "outcome_unknown"}),
    }
    return answers[name]


async def write_async(script: Script, request: WriteRequest) -> object:
    transport = httpx.MockTransport(script)
    async with AsyncClient(READER, WRITER, api_key="k", transport=transport) as asynchronous:
        return await asynchronous.write("ns", request)


def outcome_of(flavour: str, script: Script, request: WriteRequest) -> str:
    """What a write ends with, in the words of `clients/retry-cases.json`."""
    try:
        if flavour == "sync":
            client(script).write("ns", request)
        else:
            asyncio.run(write_async(script, request))
    except FyrrError as error:
        return f"{error.status} {error.code}"
    except httpx.ConnectError:
        return "connection refused"
    return "ok"


def test_a_write_is_retried_as_the_shared_cases_say(slept: list[float]) -> None:
    """The cases that the TypeScript client tests too: `clients/retry-cases.json`."""
    shared = json.loads((Path(__file__).parents[2] / "retry-cases.json").read_text())
    for case in shared["cases"]:
        request: WriteRequest = {**WRITE}
        conditions = shared["conditions"][case["conditions"]]
        if conditions is not None:
            request["conditions"] = conditions
        for flavour in ("sync", "async"):
            script = Script(*(shared_answer(name) for name in case["answers"]))
            outcome = outcome_of(flavour, script, request)
            assert len(script.requests) == case["requests"], (flavour, case)
            assert outcome == case["outcome"], (flavour, case)


def test_the_async_client_retries_alike(slept: list[float]) -> None:
    script = Script(
        httpx.Response(429, headers={"Retry-After": "3"}, json={"error": "rate_limited"}),
        refused,
        found(),
    )

    async def scenario() -> None:
        async with AsyncClient(
            READER, WRITER, api_key="k", transport=httpx.MockTransport(script)
        ) as asynchronous:
            assert (await asynchronous.query("ns", {}))["hits"] == []
            script.answers[:0] = [refused, ok()]
            assert (await asynchronous.write("ns", WRITE))["upserted"] == 1

    asyncio.run(scenario())
    assert len(slept) == 3 and slept[0] == 3.0


def test_a_key_is_stripped_checked_and_never_shown() -> None:
    secret = "fyrr_key_secret"
    seen: list[str] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["Authorization"])
        return found()

    with Client(READER, api_key=f"{secret}\n", transport=httpx.MockTransport(record)) as read:
        read.query("ns", {})
        assert secret not in repr(read)
    assert seen == [f"Bearer {secret}"]
    for bad in ["", "  ", f"{secret}\x00", f"{secret} two", "kéy"]:
        with pytest.raises(ValueError) as raised:
            Client(READER, api_key=bad)
        assert secret not in str(raised.value) and "kéy" not in str(raised.value)


def test_a_missing_writer_url_is_refused() -> None:
    with pytest.raises(ValueError, match="writer_url"):
        Client(READER, api_key="k").write("ns", WRITE)


def listing_server(existing: list[str], limit: int) -> Callable[[httpx.Request], httpx.Response]:
    """A server that lists names by prefix and refuses more than `limit` (10,000 in fyrr)."""

    def server(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/namespaces"  # the listing alone, no report
        matching = [n for n in existing if n.startswith(request.url.params.get("prefix", ""))]
        if len(matching) > limit:
            return httpx.Response(400, json={"error": "too_many_namespaces"})
        return httpx.Response(200, json={"namespaces": matching})

    return server


def test_iter_namespaces_lists_each_first_character_above_the_listing_limit() -> None:
    existing = ["a1", "a2", "a3", "ab", "b", "c"]
    transport = httpx.MockTransport(listing_server(existing, limit=4))
    with Client(READER, api_key="k", transport=transport) as listing:
        assert list(listing.iter_namespaces()) == existing


def test_iter_namespaces_raises_when_a_first_character_still_overflows() -> None:
    # The name "a" would be listed by no narrower prefix: no complete list can be promised.
    transport = httpx.MockTransport(listing_server(["a", "a1", "a2", "b"], limit=2))
    with Client(READER, api_key="k", transport=transport) as listing:
        with pytest.raises(FyrrError) as raised:
            list(listing.iter_namespaces())
        assert raised.value.code == "too_many_namespaces"
        assert list(listing.iter_namespaces("a1")) == ["a1"]


def test_the_async_iter_namespaces_lists_and_raises_alike() -> None:
    async def names(existing: list[str], limit: int) -> list[str]:
        transport = httpx.MockTransport(listing_server(existing, limit))
        async with AsyncClient(READER, api_key="k", transport=transport) as listing:
            return [name async for name in listing.iter_namespaces()]

    assert asyncio.run(names(["a1", "b", "c"], limit=2)) == ["a1", "b", "c"]
    with pytest.raises(FyrrError):
        asyncio.run(names(["a", "a1", "a2", "b"], limit=2))
