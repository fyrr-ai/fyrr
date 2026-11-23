"""Synchronous and asynchronous clients of the fyrr HTTP API."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, NamedTuple, cast
from urllib.parse import quote

import httpx

from .models import (
    Consistency,
    DeleteNamespaceResponse,
    Filter,
    NamespaceList,
    NamespaceReport,
    QueryRequest,
    SearchHit,
    SearchOutput,
    WriteRequest,
    WriteResponse,
)

# Longer than the 10 s default deadline of a read (`deadline_ms`), so the server
# answers `408 deadline_exceeded` before the client gives up. A request with a
# longer `deadline_ms` needs a longer `timeout`.
DEFAULT_TIMEOUT = 30.0
# The server asks for 60 s after a `429`; a larger `Retry-After` is not waited for.
MAX_RETRY_AFTER = 120.0
MAX_BACKOFF = 30.0
RETRY_STATUSES = frozenset({408, 429, *range(500, 600)})
# Name order of the characters of a namespace name, which cannot start with "-".
NAME_CHARACTERS = "-0123456789abcdefghijklmnopqrstuvwxyz"


class FyrrError(Exception):
    """An error answer: `{"error": "<code>"}` of the API, or a SCIM error."""

    def __init__(self, status: int, code: str | None, retry_after: float | None = None) -> None:
        super().__init__(f"{status} {code}" if code else str(status))
        self.status = status
        self.code = code
        self.retry_after = retry_after


class _Request(NamedTuple):
    method: str
    url: str
    body: Any
    params: dict[str, str] | None
    retry: bool


class _Base:
    """What both clients share: URLs, retry policy and the request of each route."""

    def __init__(
        self,
        reader_url: str,
        writer_url: str | None,
        api_key: str,
        timeout: float,
        max_retries: int,
        backoff: float,
    ) -> None:
        self._urls = {
            "reader": reader_url.rstrip("/"),
            "writer": writer_url and writer_url.rstrip("/"),
        }
        self._headers = {"Authorization": f"Bearer {api_key}"}
        self._timeout = httpx.Timeout(timeout)
        self._max_retries = max_retries
        self._backoff = backoff

    def _request(
        self,
        service: str,
        method: str,
        path: str,
        body: Any = None,
        params: dict[str, str] | None = None,
        retry: bool = True,
    ) -> _Request:
        base = self._urls[service]
        if base is None:
            raise ValueError(f"this client has no {service}_url")
        return _Request(method, base + path, body, params, retry)

    def _retry_delay(
        self,
        request: _Request,
        attempt: int,
        response: httpx.Response | None,
        error: httpx.TransportError | None,
    ) -> float | None:
        """Seconds to wait before the next attempt, or None when the outcome is final."""
        if not request.retry or attempt >= self._max_retries:
            return None
        if response is not None and response.status_code not in RETRY_STATUSES:
            return None
        if response is not None and (wait := _retry_after(response)) is not None:
            return min(wait, MAX_RETRY_AFTER)
        delay = min(self._backoff * (1 << attempt), MAX_BACKOFF)
        return delay / 2 + random.uniform(0, delay / 2)

    @staticmethod
    def _decode(response: httpx.Response) -> Any:
        if response.is_success:
            return response.json()
        try:
            body = response.json()
        except ValueError:
            body = None
        code = None
        if isinstance(body, dict):
            code = body.get("error") or body.get("scimType") or body.get("detail")
        raise FyrrError(response.status_code, code, _retry_after(response))


def _retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        return max(0.0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
    except (TypeError, ValueError):
        return None


def _namespace_path(namespace: str) -> str:
    return f"/v1/namespaces/{quote(namespace, safe='')}"


def _is_conditional(request: WriteRequest) -> bool:
    return bool(request.get("conditions"))


def _scan_request(
    after: SearchHit | None, filter: Filter | None, page_size: int, consistency: Consistency | None
) -> QueryRequest:
    """The next page of a scan by the document key (docs/mcp-tools.md, Paging and scanning)."""
    conditions: list[Filter] = [filter] if filter else []
    if after is not None:
        source, root = after["source_id"], after["root_id"]
        conditions.append(
            {
                "or": [
                    {"field": "source_id", "op": "gt", "value": source},
                    {
                        "and": [
                            {"field": "source_id", "op": "eq", "value": source},
                            {"field": "root_id", "op": "gt", "value": root},
                        ]
                    },
                ]
            }
        )
    request: QueryRequest = {
        "order_by": [
            {"field": "source_id", "direction": "asc"},
            {"field": "root_id", "direction": "asc"},
        ],
        "output": "metadata",
        "limit": page_size,
    }
    if conditions:
        request["filter"] = conditions[0] if len(conditions) == 1 else {"and": conditions}
    if consistency:
        request["consistency"] = consistency
    return request


def _child_prefixes(prefix: str) -> list[str]:
    return [prefix + c for c in NAME_CHARACTERS if prefix or c != "-"]


class Client(_Base):
    """A synchronous client. Reuses its connections; close it, or use it in a `with` block.

    `reader_url` serves the read routes. `writer_url`, the administration listener of
    `fyrr writer serve`, serves writes and namespace deletion; leave it out for a reader-only
    deployment. `api_key` is an API key or an OIDC access token.

    A request is retried on a connection error and on 408, 429 and 5xx, up to `max_retries`
    times, after `backoff` seconds doubling each time (jittered), or after the server's
    `Retry-After`. A write with `conditions` is never retried: raise or repeat it yourself.
    """

    def __init__(
        self,
        reader_url: str,
        writer_url: str | None = None,
        *,
        api_key: str,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = 3,
        backoff: float = 0.5,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        super().__init__(reader_url, writer_url, api_key, timeout, max_retries, backoff)
        self._http = httpx.Client(headers=self._headers, timeout=self._timeout, transport=transport)

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def _send(self, request: _Request) -> Any:
        attempt = 0
        while True:
            response = error = None
            try:
                response = self._http.request(
                    request.method, request.url, json=request.body, params=request.params
                )
            except httpx.TransportError as exc:
                error = exc
            delay = self._retry_delay(request, attempt, response, error)
            if delay is None:
                if error:
                    raise error
                assert response is not None
                return self._decode(response)
            time.sleep(delay)
            attempt += 1

    def list_namespaces(self, prefix: str | None = None) -> NamespaceList:
        """One call: the names that start with `prefix`, or `too_many_namespaces` above 10,000."""
        params = {"prefix": prefix} if prefix else None
        return cast(
            NamespaceList,
            self._send(self._request("reader", "GET", "/v1/namespaces", params=params)),
        )

    def iter_namespaces(self, prefix: str = "") -> Iterator[str]:
        """Every name that starts with `prefix`, in name order, however many there are."""
        try:
            names = self.list_namespaces(prefix)["namespaces"]
        except FyrrError as error:
            if error.code != "too_many_namespaces":
                raise
        else:
            yield from names
            return
        # More than 10,000 names: list the narrower prefixes. The name `prefix` itself sorts
        # first and no narrower prefix lists it, so ask for it directly.
        if prefix and self._exists(prefix):
            yield prefix
        for child in _child_prefixes(prefix):
            yield from self.iter_namespaces(child)

    def _exists(self, namespace: str) -> bool:
        try:
            self.describe_namespace(namespace)
        except FyrrError as error:
            if error.status in (401, 404):
                return False
            raise
        return True

    def describe_namespace(self, namespace: str) -> NamespaceReport:
        path = _namespace_path(namespace)
        return cast(NamespaceReport, self._send(self._request("reader", "GET", path)))

    def delete_namespace(self, namespace: str) -> DeleteNamespaceResponse:
        path = _namespace_path(namespace)
        request = self._request("writer", "DELETE", path, {"namespace": namespace})
        return cast(DeleteNamespaceResponse, self._send(request))

    def query(self, namespace: str, request: QueryRequest) -> SearchOutput:
        path = _namespace_path(namespace) + "/query"
        return cast(SearchOutput, self._send(self._request("reader", "POST", path, request)))

    def write(self, namespace: str, request: WriteRequest) -> WriteResponse:
        path = _namespace_path(namespace) + "/documents"
        retry = not _is_conditional(request)
        return cast(
            WriteResponse, self._send(self._request("writer", "POST", path, request, retry=retry))
        )

    def iter_documents(
        self,
        namespace: str,
        *,
        filter: Filter | None = None,
        page_size: int = 1000,
        consistency: Consistency | None = None,
    ) -> Iterator[SearchHit]:
        """Every document the key may read that passes `filter`, once, by the document key."""
        after = None
        while True:
            hits = self.query(namespace, _scan_request(after, filter, page_size, consistency))[
                "hits"
            ]
            if not hits:
                return
            yield from hits
            after = hits[-1]


class AsyncClient(_Base):
    """The asynchronous twin of `Client`: the same methods as coroutines and async iterators."""

    def __init__(
        self,
        reader_url: str,
        writer_url: str | None = None,
        *,
        api_key: str,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = 3,
        backoff: float = 0.5,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(reader_url, writer_url, api_key, timeout, max_retries, backoff)
        self._http = httpx.AsyncClient(
            headers=self._headers, timeout=self._timeout, transport=transport
        )

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _send(self, request: _Request) -> Any:
        attempt = 0
        while True:
            response = error = None
            try:
                response = await self._http.request(
                    request.method, request.url, json=request.body, params=request.params
                )
            except httpx.TransportError as exc:
                error = exc
            delay = self._retry_delay(request, attempt, response, error)
            if delay is None:
                if error:
                    raise error
                assert response is not None
                return self._decode(response)
            await asyncio.sleep(delay)
            attempt += 1

    async def list_namespaces(self, prefix: str | None = None) -> NamespaceList:
        params = {"prefix": prefix} if prefix else None
        request = self._request("reader", "GET", "/v1/namespaces", params=params)
        return cast(NamespaceList, await self._send(request))

    async def iter_namespaces(self, prefix: str = "") -> AsyncIterator[str]:
        try:
            names = (await self.list_namespaces(prefix))["namespaces"]
        except FyrrError as error:
            if error.code != "too_many_namespaces":
                raise
        else:
            for name in names:
                yield name
            return
        if prefix and await self._exists(prefix):
            yield prefix
        for child in _child_prefixes(prefix):
            async for name in self.iter_namespaces(child):
                yield name

    async def _exists(self, namespace: str) -> bool:
        try:
            await self.describe_namespace(namespace)
        except FyrrError as error:
            if error.status in (401, 404):
                return False
            raise
        return True

    async def describe_namespace(self, namespace: str) -> NamespaceReport:
        path = _namespace_path(namespace)
        return cast(NamespaceReport, await self._send(self._request("reader", "GET", path)))

    async def delete_namespace(self, namespace: str) -> DeleteNamespaceResponse:
        path = _namespace_path(namespace)
        request = self._request("writer", "DELETE", path, {"namespace": namespace})
        return cast(DeleteNamespaceResponse, await self._send(request))

    async def query(self, namespace: str, request: QueryRequest) -> SearchOutput:
        path = _namespace_path(namespace) + "/query"
        sent = self._request("reader", "POST", path, request)
        return cast(SearchOutput, await self._send(sent))

    async def write(self, namespace: str, request: WriteRequest) -> WriteResponse:
        path = _namespace_path(namespace) + "/documents"
        retry = not _is_conditional(request)
        sent = self._request("writer", "POST", path, request, retry=retry)
        return cast(WriteResponse, await self._send(sent))

    async def iter_documents(
        self,
        namespace: str,
        *,
        filter: Filter | None = None,
        page_size: int = 1000,
        consistency: Consistency | None = None,
    ) -> AsyncIterator[SearchHit]:
        after = None
        while True:
            page = await self.query(namespace, _scan_request(after, filter, page_size, consistency))
            if not page["hits"]:
                return
            for hit in page["hits"]:
                yield hit
            after = page["hits"][-1]
