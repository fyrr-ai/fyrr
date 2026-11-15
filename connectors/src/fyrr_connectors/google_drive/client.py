"""Delegated Drive/Directory REST, complete paging and bounded exports.

Retries fail the sync on exhaustion; other statuses retain Google's reason.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any

import requests
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from .. import _http
from .._http import time as time

DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
DIRECTORY_SCOPES = (
    "https://www.googleapis.com/auth/admin.directory.user.readonly",
    "https://www.googleapis.com/auth/admin.directory.group.readonly",
)
DRIVE, DIRECTORY = "drive/v3/", "admin/directory/v1/"
RATE_LIMIT_REASONS = frozenset({"userRateLimitExceeded", "rateLimitExceeded"})
ATTEMPTS = 6
MAX_WAIT_SECONDS = 60.0
TIMEOUT_SECONDS = 120
#: Drive caps an export at 10 MB; a larger body is refused instead of buffered.
EXPORT_MAX_BYTES = 16 << 20


class ApiError(_http.ApiError):
    """A Drive error response. `transient` means the retries ran out; the
    message holds Google's status and reason code, never the response body."""

    def __init__(self, status: int, reason: str, location: str = "", transient: bool = False):
        super().__init__(f"drive_api_{status}_{reason}", status=status, transient=transient)
        self.reason = reason
        self.location = location


def authorized_session(key: Mapping[str, Any], subject: str, api: str) -> requests.Session:
    """A session that acts as `subject` on one API. The tests replace this function."""
    scopes = [DRIVE_SCOPE] if api == DRIVE else list(DIRECTORY_SCOPES)
    credentials = service_account.Credentials.from_service_account_info(
        key, scopes=scopes, subject=subject
    )
    return AuthorizedSession(credentials)


def _error(response: requests.Response) -> tuple[str, str]:
    """Google's first error reason and location, if the body has them."""
    try:
        first = response.json()["error"]["errors"][0]
        return str(first.get("reason", "")), str(first.get("location", ""))
    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
        return "", ""


class Client:
    def __init__(self, service_account_key: str, endpoint: str) -> None:
        try:
            self._key = json.loads(service_account_key)
        except ValueError:
            raise ValueError("google_drive_service_account_invalid") from None
        self._endpoint = endpoint
        self._sessions: dict[tuple[str, str], requests.Session] = {}

    def _session(self, subject: str, api: str) -> requests.Session:
        if (subject, api) not in self._sessions:
            # One feed is read at a time: keep only its user's connections.
            for session in self._sessions.values():
                session.close()
            self._sessions = {(subject, api): authorized_session(self._key, subject, api)}
        return self._sessions[subject, api]

    def _request(
        self,
        subject: str,
        path: str,
        params: Mapping[str, Any],
        stream: bool = False,
        api: str = DRIVE,
    ) -> requests.Response:
        status, reason = 0, "transport"
        retries = _http.Retries(
            _http.RetryPolicy(ATTEMPTS, float("inf"), MAX_WAIT_SECONDS, jitter="wait"),
            ApiError(0, "transport", transient=True),
        )
        for _attempt in retries:
            try:
                response = self._session(subject, api).get(
                    f"{self._endpoint}/{api}{path}",
                    params=params,
                    timeout=TIMEOUT_SECONDS,
                    stream=stream,
                )
            except (requests.ConnectionError, requests.Timeout):
                status, reason = 0, "transport"
                continue
            status = response.status_code
            if status < 300:
                return response
            reason, location = _error(response)
            retry_after = response.headers.get("Retry-After", "")
            response.close()
            if retries.policy.retryable(status) or reason in RATE_LIMIT_REASONS:
                if retry_after.isdigit():
                    retries.wait = float(retry_after)
                continue
            raise ApiError(status, reason, location)
        raise ApiError(status, reason, transient=True)

    def get(self, subject: str, path: str, **params: Any) -> dict[str, Any]:
        return self._request(subject, path, params).json()

    def pages(
        self, subject: str, path: str, api: str = DRIVE, **params: Any
    ) -> Iterator[dict[str, Any]]:
        """Every page of a list call, or an error: never a short list. `fields`
        must include `nextPageToken`."""
        while True:
            body = self._request(subject, path, params, api=api).json()
            yield body
            if not body.get("nextPageToken"):
                return
            params["pageToken"] = body["nextPageToken"]

    def export(self, subject: str, file_id: str, mime_type: str) -> bytes:
        response = self._request(
            subject, f"files/{file_id}/export", {"mimeType": mime_type}, stream=True
        )
        data = bytearray()
        with response:
            for piece in response.iter_content(chunk_size=1 << 20):
                data.extend(piece)
                if len(data) > EXPORT_MAX_BYTES:
                    raise ApiError(403, "exportSizeLimitExceeded")
        return bytes(data)
