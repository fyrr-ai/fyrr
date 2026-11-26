"""Paced Notion REST and complete paging with categorical, value-free errors.

Retries cover conflicts, rate limits, server errors and broken transports.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from .. import _http

API_VERSION = "2025-09-03"
PAGE_SIZE = 100
ATTEMPTS = 6
MAX_WAIT_SECONDS = 60.0
TIMEOUT_SECONDS = 60
#: Notion allows an integration about three requests per second.
MIN_INTERVAL_SECONDS = 0.35
# Only known codes may enter diagnostics; a syntactically plausible string
# can still contain document or principal data.
_CODES = frozenset(
    {
        "invalid_json",
        "invalid_request_url",
        "invalid_request",
        "validation_error",
        "missing_version",
        "unauthorized",
        "restricted_resource",
        "object_not_found",
        "conflict_error",
        "rate_limited",
        "internal_server_error",
        "bad_gateway",
        "service_unavailable",
        "database_connection_unavailable",
        "gateway_timeout",
    }
)


class ApiError(_http.ApiError):
    """A Notion error response. `transient` means the retries ran out; the
    message holds the status and Notion's error code, never the response body."""

    def __init__(self, status: int, code: str, transient: bool = False):
        super().__init__(f"notion_api_{status}_{code}", status=status, transient=transient)
        self.code = code

    @property
    def missing(self) -> bool:
        """The object is deleted or not shared with the integration."""
        return self.status == 404 and not self.transient

    @property
    def refused(self) -> bool:
        """Notion will not serve this object; asking again does not help."""
        return (
            not self.transient and 400 <= self.status < 500 and self.status not in (401, 403, 404)
        )


def session(token: str) -> requests.Session:
    """A session of the integration. The tests replace this function."""
    return _http.session({"Authorization": f"Bearer {token}", "Notion-Version": API_VERSION})


def _code(response: requests.Response) -> str:
    try:
        code = json.loads(response.content)["code"]
    except (ValueError, KeyError, TypeError):
        return ""
    return code if isinstance(code, str) and code in _CODES else ""


class Client:
    def __init__(self, token: str, endpoint: str) -> None:
        self._session = session(token)
        self._endpoint = endpoint
        self._ready = 0.0
        #: Notion's clock when it last answered (the `Date` header).
        self.server_time = datetime.now(UTC)

    @_http.retry(
        _http.RetryPolicy(
            ATTEMPTS,
            float("inf"),
            MAX_WAIT_SECONDS,
            jitter="wait",
            budget=300,
            retry_statuses=(409, 429),
        ),
        lambda: ApiError(0, "transport", transient=True),
    )
    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        *,
        _retry: _http.Retries,
    ) -> dict[str, Any] | _http.Retry:
        pause = self._ready - time.monotonic()
        if pause > 0:
            time.sleep(pause)
        self._ready = time.monotonic() + MIN_INTERVAL_SECONDS
        response = self._session.request(
            method,
            f"{self._endpoint}/v1/{path}",
            params=params,
            json=body,
            timeout=TIMEOUT_SECONDS,
            stream=True,
        )
        try:
            response._content = _http.read_body(
                response, _http.MAX_RESPONSE, ValueError("notion_response_limit")
            )
        finally:
            response.close()
        status = response.status_code
        if status < 300:
            try:
                answer = response.json()
            except ValueError:
                answer = None
            if isinstance(answer, dict):
                self._clock(response)
                return answer
            # Not Notion's JSON: a proxy in between.
            return _http.Retry(ApiError(status, "body", transient=True))
        code = _code(response)
        if _retry.policy.retryable(status):
            after = _http.retry_after(response.headers.get("Retry-After", ""))
            wait = None if after is None else max(after, _retry.wait)
            return _http.Retry(ApiError(status, code, transient=True), wait)
        raise ApiError(status, code)

    def _clock(self, response: requests.Response) -> None:
        try:
            self.server_time = parsedate_to_datetime(response.headers["Date"]).astimezone(UTC)
        except (KeyError, TypeError, ValueError):
            self.server_time = datetime.now(UTC)

    def get(self, path: str) -> dict[str, Any]:
        return self._request("GET", path)

    def pages(
        self, path: str, query: dict[str, Any] | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        """Every page of a list call, or an error: never a short list. With a
        `query` the call is a POST (search, data-source query)."""
        cursor: str | None = None
        seen: set[str] = set()
        for _ in range(_http.MAX_PAGES):
            paging: dict[str, Any] = {"page_size": PAGE_SIZE}
            if cursor is not None:
                paging["start_cursor"] = cursor
            if query is None:
                body = self._request("GET", path, params=paging)
            else:
                body = self._request("POST", path, body=query | paging)
            results = _http.entries(body.get("results"), ValueError("notion_response_invalid"))
            if not isinstance(body.get("has_more"), bool):
                raise ValueError("notion_response_invalid")
            yield results
            if not body["has_more"]:
                return
            cursor = body.get("next_cursor")
            # A cursor that does not advance would page forever.
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                raise ValueError("notion_paging_invalid")
            seen.add(cursor)
        raise ValueError("notion_listing_limit")
