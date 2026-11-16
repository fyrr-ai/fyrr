"""Serial bounded REST, origin-safe paging and primary/secondary limits."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qs, urlsplit

import requests

from .. import _http

ATTEMPTS = 6
MAX_RESPONSE_BYTES = 16 << 20
MAX_PAGES = 10_000
MAX_ENTRIES = 100_000
WAIT_BUDGET = 300
API_VERSION = "2026-03-10"


def session() -> requests.Session:
    return _http.session(
        {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": API_VERSION}
    )


class ResponseLimit(ValueError):
    def __init__(self):
        super().__init__("github_response_limit")


class ApiError(_http.ApiError):
    def __init__(self, status: int, *, transient: bool = False, moved: bool = False):
        self.moved = moved
        super().__init__(f"github_http_{status}", status=status, transient=transient)


def number(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 < value < 1 << 63:
        raise ValueError("github_response_invalid")
    return value


def object_id(body: Mapping[str, Any]) -> int:
    return number(body.get("id"))


def instant(value: object) -> datetime | None:
    try:
        result = (
            datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else None
        )
    except ValueError:
        return None
    return result if result is not None and result.tzinfo is not None else None


def delay(headers: Mapping[str, str], status: int, attempt: int) -> float:
    retry = headers.get("Retry-After")
    if retry is not None:
        try:
            result = float(retry)
        except ValueError:
            try:
                result = parsedate_to_datetime(retry).timestamp() - time.time()
            except (ValueError, TypeError, OverflowError):
                raise ValueError("github_rate_header_invalid") from None
        if not 0 <= result <= WAIT_BUDGET:
            raise ValueError("github_rate_wait_budget")
        return result
    if headers.get("X-RateLimit-Remaining") == "0":
        try:
            result = max(0, float(headers["X-RateLimit-Reset"]) - time.time() + 1)
        except (KeyError, ValueError):
            raise ValueError("github_rate_header_invalid") from None
        if result > WAIT_BUDGET:
            raise ValueError("github_rate_wait_budget")
        return result
    return 60 * 2**attempt if status in (403, 429) else min(2**attempt, 30)


class Client:
    def __init__(self, origin: str):
        self.origin = origin.rstrip("/")
        self.session = session()
        self.token: Callable[[], str] | None = None
        self.refresh: Callable[[], None] | None = None
        self.server_time = datetime.now(UTC)
        self._wait = 0.0

    def request(self, method: str, path: str, *, params=None, body=None, token: str | None = None):
        if not path.startswith("/") or path.startswith("//") or "#" in path:
            raise ValueError("github_path_invalid")
        return self._request(method, path, params, body, token, _initial_wait=self._wait)

    @_http.retry(
        _http.RetryPolicy(ATTEMPTS, budget=WAIT_BUDGET),
        lambda: ApiError(0, transient=True),
        lambda: ValueError("github_rate_wait_budget"),
    )
    def _request(self, method, path, params, body, token, *, _retry):
        self._wait = 0
        credential = token if token is not None else self.token() if self.token else None
        with _http.response(
            self.session, method, self.origin + path, credential, params=params, json=body
        ) as response:
            status = response.status_code
            response_headers = response.headers
            data = _http.read_body(response, MAX_RESPONSE_BYTES, ResponseLimit())
            date = response_headers.get("Date")
            if date:
                try:
                    self.server_time = parsedate_to_datetime(date)
                except (ValueError, TypeError):
                    raise ValueError("github_response_invalid") from None
            limited = status == 429 or (
                status == 403
                and (
                    response_headers.get("Retry-After") is not None
                    or response_headers.get("X-RateLimit-Remaining") == "0"
                    or b"secondary rate limit" in bytes(data).lower()
                )
            )
            if 200 <= status < 300:
                if response_headers.get("X-RateLimit-Remaining") == "0":
                    self._wait = delay(response_headers, status, _retry.attempt)
                return _http.decode_json(
                    data, ValueError("github_response_invalid")
                ), response_headers
            if (
                status == 401
                and token is None
                and self.refresh is not None
                and not _retry.refreshed
            ):
                self.refresh()
                _retry.refreshed = True
                return _http.Retry(ApiError(401), 0)
            if limited or _retry.policy.retryable(status):
                wait = (
                    delay(response_headers, status, _retry.attempt)
                    if _retry.attempt + 1 < ATTEMPTS
                    else None
                )
                return _http.Retry(ApiError(status, transient=True), wait)
            location = urlsplit(response_headers.get("Location", ""))
            target = urlsplit(self.origin)
            moved = status in (301, 308) and (
                (location.scheme, location.netloc) == (target.scheme, target.netloc)
                and re.fullmatch(r"/repos/[^/]+/[^/]+/issues/[1-9][0-9]*", location.path)
                is not None
                and not location.query
                and not location.fragment
            )
            raise ApiError(status, moved=moved)

    def get(self, path: str, params=None) -> dict[str, Any]:
        result, _ = self.request("GET", path, params=params)
        if not isinstance(result, dict):
            raise ValueError("github_response_invalid")
        return result

    def updated(self, path: str, params) -> Iterator[list[dict[str, Any]]]:
        """Seek by timestamp, replaying the boundary; never offset a mutable feed."""
        arguments = {**params, "per_page": 100, "page": 1}
        boundary = None
        seen = set()
        for _ in range(MAX_PAGES):
            entries, headers = self.request("GET", path, params=arguments)
            entries = _http.entries(entries, ValueError("github_response_invalid"))
            stamps: list[datetime] = []
            for item in entries:
                stamp = instant(item.get("updated_at"))
                if stamp is None:
                    raise ValueError("github_updated_order_invalid")
                stamps.append(stamp)
            if stamps != sorted(stamps):
                raise ValueError("github_updated_order_invalid")
            fresh = []
            for item in entries:
                key = (
                    object_id(item),
                    hashlib.sha256(json.dumps(item, sort_keys=True).encode()).digest(),
                )
                if key not in seen:
                    seen.add(key)
                    fresh.append(item)
                if len(seen) > MAX_ENTRIES:
                    raise ValueError("github_listing_limit")
            yield fresh
            if (
                _http.next_link(headers, self.origin, path, ValueError("github_paging_invalid"))
                is None
            ):
                return
            if not stamps or (boundary is not None and stamps[-1] <= boundary):
                # The API offers no ID key within a >page-sized timestamp tie.
                # Refuse to advance rather than permanently skipping that bucket.
                raise ValueError("github_updated_tie_limit")
            boundary = stamps[-1]
            arguments["since"] = (boundary - timedelta(seconds=1)).isoformat()
        raise ValueError("github_paging_limit")

    def pages(
        self, path: str, params=None, *, key: str | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        arguments = {**(params or {}), "per_page": 100}
        current = 1
        count = 0
        for _ in range(MAX_PAGES):
            result, headers = self.request("GET", path, params={**arguments, "page": current})
            entries = result.get(key) if key and isinstance(result, dict) else result
            entries = _http.entries(entries, ValueError("github_response_invalid"))
            count += len(entries)
            if count > MAX_ENTRIES:
                raise ValueError("github_listing_limit")
            if key == "repositories" and (
                not isinstance(result.get("total_count"), int) or result["total_count"] < count
            ):
                raise ValueError("github_response_invalid")
            yield entries
            target = _http.next_link(
                headers, self.origin, path, ValueError("github_paging_invalid")
            )
            if target is None:
                if key == "repositories" and count != result["total_count"]:
                    raise ValueError("github_listing_incomplete")
                return
            query = parse_qs(target.query, strict_parsing=True)
            wanted = {k: [str(v)] for k, v in arguments.items()}
            try:
                page = int(query.pop("page")[0])
            except (KeyError, ValueError):
                raise ValueError("github_paging_invalid") from None
            if target.fragment or query != wanted or page != current + 1:
                raise ValueError("github_paging_invalid")
            current = page
        raise ValueError("github_listing_limit")
