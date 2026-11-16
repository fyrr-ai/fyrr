"""Serial bounded REST, cursor validation and categorical Slack errors."""

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from .. import _http
from .._http import session as session
from .._http import time as time

ATTEMPTS = 6
WAIT_BUDGET = 300
MAX_RESPONSE = 16 << 20
MAX_ENTRIES = 100_000
MAX_PAGES = 10_000
GONE = frozenset({"channel_not_found", "not_in_channel"})


class ApiError(_http.ApiError):
    def __init__(self, code="failure", *, gone=False):
        self.code, self.gone = code, gone
        super().__init__("slack_" + code)


class Client:
    def __init__(self, origin, token):
        self.origin = origin.rstrip("/")
        self.token = token
        self.session = session()
        self.now = int(datetime.now(UTC).timestamp()) * 1_000_000

    def get(self, method, **params):
        if method not in {
            "auth.test",
            "conversations.list",
            "conversations.info",
            "conversations.history",
            "conversations.replies",
            "conversations.members",
            "users.list",
            "users.info",
        }:
            raise ValueError("slack_method_invalid")

        return self._get(method, params)

    @_http.retry(
        _http.RetryPolicy(ATTEMPTS, budget=WAIT_BUDGET, jitter="wait", server_error="http_failure"),
        lambda: ApiError("transport_failure"),
        lambda: ApiError("rate_wait_budget"),
    )
    def _get(self, method, params, *, _retry):
        verb = "POST" if method == "auth.test" else "GET"
        with _http.response(
            self.session, verb, self.origin + "/api/" + method, self.token, params=params
        ) as response:
            body = _http.json_response(response, _retry, ApiError, MAX_RESPONSE)
            if isinstance(body, _http.Retry):
                return body
            try:
                if response.headers.get("Date"):
                    self.now = (
                        int(parsedate_to_datetime(response.headers["Date"]).timestamp()) * 1_000_000
                    )
            except (ValueError, TypeError, UnicodeError):
                raise ApiError("response_invalid") from None
            if not isinstance(body, dict) or not isinstance(body.get("ok"), bool):
                raise ApiError("response_invalid")
            if not body["ok"]:
                code = body.get("error")
                if (
                    code in GONE
                    and method.startswith("conversations.")
                    and method != "conversations.list"
                ):
                    raise ApiError("channel_gone", gone=True)
                if code == "user_not_found":
                    raise ApiError("user_not_found")
                if code == "thread_not_found":
                    raise ApiError("thread_not_found")
                if code in {
                    "invalid_auth",
                    "token_revoked",
                    "token_expired",
                    "account_inactive",
                    "not_authed",
                }:
                    raise ApiError("auth_failure")
                raise ApiError()
            return body

    def pages(self, method, key, **params):
        cursor, seen, count = "", set(), 0
        for _ in range(MAX_PAGES):
            body = self.get(method, **params, limit=100, cursor=cursor)
            if body.get("is_limited") is True:
                raise ApiError("history_limited")
            entries = _http.entries(
                body.get(key),
                ApiError("response_invalid"),
                str if method == "conversations.members" else dict,
            )
            count += len(entries)
            if count > MAX_ENTRIES:
                raise ApiError("entry_limit")
            yield entries
            metadata = body.get("response_metadata", {})
            if not isinstance(metadata, dict):
                raise ApiError("paging_invalid")
            following = metadata.get("next_cursor", "")
            if not isinstance(following, str) or len(following) > 2048 or following in seen:
                raise ApiError("paging_invalid")
            if not following:
                if body.get("has_more") is True:
                    raise ApiError("paging_incomplete")
                return
            seen.add(following)
            cursor = following
        raise ApiError("paging_limit")
