"""Shared HTTP resources and retries; providers own response classification."""

from __future__ import annotations

import json
import math
import random
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from functools import wraps
from urllib.parse import urlsplit

import requests

#: Bounds of one provider read, shared by every client.
MAX_RESPONSE = 16 << 20
MAX_PAGES = 10_000
MAX_ENTRIES = 100_000


class ApiError(Exception):
    """A value-free provider diagnostic with its HTTP status and retry result."""

    def __init__(self, message: str, *, status: int = 0, transient: bool = False):
        super().__init__(message)
        self.status = status
        self.transient = transient


def session(headers: Mapping[str, str] | None = None) -> requests.Session:
    opened = requests.Session()
    opened.headers.update(headers or {})
    return opened


def seconds(value: str) -> float | None:
    try:
        result = float(value)
    except ValueError:
        return None
    return result if math.isfinite(result) and result >= 0 else None


def retry_after(value: str) -> float | None:
    numeric = seconds(value)
    if numeric is not None:
        return numeric
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            return None
        return max(0.0, date.timestamp() - time.time())
    except (ValueError, TypeError, OverflowError):
        return None


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 6
    backoff_cap: float = 30
    wait_cap: float = math.inf
    jitter: str = "none"
    budget: float = math.inf
    retry_statuses: tuple[int, ...] = (429,)
    server_error: str = "transient_failure"
    status_errors: tuple[tuple[int, str], ...] = ()

    def retryable(self, status: int) -> bool:
        return status in self.retry_statuses or status >= 500


class Retries:
    """Serial attempts; a response can override the next wait with Retry-After."""

    def __init__(self, policy: RetryPolicy, error: Exception, wait: float = 0):
        self.policy, self.error, self.wait = policy, error, wait
        self.attempt, self.refreshed = 0, False

    def __iter__(self) -> Iterator[int]:
        began = time.monotonic()
        for attempt in range(self.policy.attempts):
            wait = self.wait
            if wait > self.policy.wait_cap:
                raise self.error
            if attempt and self.policy.jitter == "wait":
                wait += random.uniform(0, 1)
            if wait:
                if wait + time.monotonic() - began > self.policy.budget:
                    raise self.error
                time.sleep(wait)
            self.wait = min(2**attempt, self.policy.backoff_cap)
            self.attempt = attempt
            yield attempt


@dataclass
class Retry:
    error: Exception
    wait: float | None = None


def retry(policy: RetryPolicy, transport_error, budget_error=None):
    """Retry a provider operation while keeping its exceptions and wait policy."""

    def decorate(operation):
        @wraps(operation)
        def run(self, *args, _initial_wait=0, **kwargs):
            retries = Retries(policy, (budget_error or transport_error)(), _initial_wait)
            failure = transport_error()
            for _ in retries:
                try:
                    result = operation(self, *args, _retry=retries, **kwargs)
                    if not isinstance(result, Retry):
                        return result
                    failure = result.error
                    if budget_error is None:
                        retries.error = failure
                    if result.wait is not None:
                        retries.wait = result.wait
                except (requests.RequestException, OSError):
                    failure = transport_error()
            raise failure from None

        return run

    return decorate


def response(opened, method, url, token=None, **kwargs):
    return opened.request(
        method,
        url,
        headers={"Authorization": f"Bearer {token}"} if token is not None else {},
        timeout=(10, 30),
        stream=True,
        allow_redirects=False,
        **kwargs,
    )


def json_response(response, retries, error, limit, http_error=None):
    """Strict JSON/200 status handling, with provider-selected diagnostics."""
    policy, status = retries.policy, response.status_code
    http_error = http_error or (lambda status, code: error(code))
    if policy.retryable(status):
        wait = None
        if status == 429:
            wait = retry_after(response.headers.get("Retry-After", ""))
            if wait is None or wait > policy.budget:
                raise error("rate_header_invalid")
        code = "rate_limited" if status == 429 else policy.server_error
        return Retry(http_error(status, code), wait)
    if status != 200:
        raise http_error(status, dict(policy.status_errors).get(status, "http_failure"))
    return decode_json(
        read_body(response, limit, error("response_limit")), error("response_invalid")
    )


def decode_json(data: bytes, error: Exception):
    try:
        return json.loads(data)
    except (ValueError, UnicodeError):
        raise error from None


def entries(value, error: Exception, item_type=dict):
    if not isinstance(value, list) or any(not isinstance(item, item_type) for item in value):
        raise error
    return value


def read_body(response: requests.Response, limit: int, error: Exception) -> bytes:
    data = bytearray()
    for part in response.iter_content(64 << 10):
        if len(data) + len(part) > limit:
            raise error
        data.extend(part)
    return bytes(data)


def next_link(headers, origin, path, error):
    links = requests.utils.parse_header_links(headers.get("Link", ""))
    following = [link.get("url", "") for link in links if link.get("rel") == "next"]
    if not following:
        return None
    target, source = urlsplit(following[0]), urlsplit(origin)
    if len(following) != 1 or (target.scheme, target.netloc, target.path) != (
        source.scheme,
        source.netloc,
        path,
    ):
        raise error
    return target
