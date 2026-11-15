"""Bounded serial REST with native token discovery and checked full snapshots."""

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from .. import _http
from .._http import session as session
from .._http import time as time

ATTEMPTS = 6
MAX_ENTRIES = 100_000
MAX_RESPONSE = 16 << 20
MAX_PAGES = 10_000


class ApiError(_http.ApiError):
    def __init__(self, code="failure", status=0):
        super().__init__("jira_" + code, status=status)

    @property
    def gone(self):
        return self.status == 404


class Client:
    def __init__(self, settings):
        self.settings = settings
        self.origin = (
            settings.api_endpoint.rstrip("/")
            if settings.api_endpoint
            else "https://api.atlassian.com/ex/jira/" + settings.cloud_id
        )
        self.session = session()
        self.token = settings.api_token.reveal()
        self.now = int(datetime.now(UTC).timestamp())
        self.rate_wait = 0

    def call(self, path, *, method="GET", params=None, body=None, public=False):
        if not path.startswith("/") or ".." in path or "?" in path or "#" in path:
            raise ValueError("jira_path_invalid")
        url = (
            (self.settings.api_endpoint or self.settings.site_url).rstrip("/") + path
            if public
            else self.origin + "/rest/api/3" + path
        )
        wait, self.rate_wait = self.rate_wait, 0

        return self._call(method, url, params, body, public, _initial_wait=wait)

    @_http.retry(
        _http.RetryPolicy(
            ATTEMPTS, jitter="backoff", budget=300, status_errors=((401, "auth_failure"),)
        ),
        lambda: ApiError("transport_failure"),
        lambda: ApiError("rate_wait_budget"),
    )
    def _call(self, method, url, params, body, public, *, _retry):
        with _http.response(
            self.session, method, url, None if public else self.token, params=params, json=body
        ) as response:
            body = _http.json_response(
                response,
                _retry,
                ApiError,
                MAX_RESPONSE,
                lambda status, code: ApiError(code, status),
            )
            if isinstance(body, _http.Retry):
                return body
            if response.headers.get("Date"):
                clock = parsedate_to_datetime(response.headers["Date"])
                if clock.tzinfo is None:
                    raise ApiError("clock_header_invalid")
                self.now = int(clock.timestamp())
            if response.headers.get("X-RateLimit-Remaining") == "0":
                try:
                    reset = datetime.fromisoformat(
                        response.headers["X-RateLimit-Reset"].replace("Z", "+00:00")
                    )
                    if reset.tzinfo is None:
                        raise ValueError
                    self.rate_wait = max(0, reset.timestamp() - self.now)
                except (KeyError, ValueError):
                    raise ApiError("rate_header_invalid") from None
            return body

    def search(self, jql, *, reconcile=()):
        token = None
        seen_tokens = set()
        seen_ids = set()
        count = 0
        previous = 0
        for _ in range(MAX_PAGES):
            body = {
                "jql": jql + " ORDER BY id ASC",
                "maxResults": 100,
                "fields": ["id", "project", "updated", "security", "issuetype", "labels"],
            }
            if token:
                body["nextPageToken"] = token
            if reconcile:
                body["reconcileIssues"] = list(reconcile)
            page = self.call("/search/jql", method="POST", body=body)
            if (
                not isinstance(page, dict)
                or not isinstance(page.get("issues"), list)
                or not isinstance(page.get("isLast"), bool)
            ):
                raise ApiError("search_invalid")
            for issue in page["issues"]:
                identity = number(issue.get("id"))
                if identity <= previous or identity in seen_ids:
                    raise ApiError("search_order_changed")
                previous = identity
                seen_ids.add(identity)
                count += 1
                if count > MAX_ENTRIES:
                    raise ApiError("search_limit")
                yield issue
            if page["isLast"]:
                return
            token = page.get("nextPageToken")
            if not isinstance(token, str) or not token or len(token) > 8192 or token in seen_tokens:
                raise ApiError("search_paging_invalid")
            seen_tokens.add(token)
        raise ApiError("page_limit")

    def snapshot(self, path, key, *, params=None, order=None):
        # Jira supplies offset-only catalogs. Read a complete ephemeral snapshot,
        # recheck its first boundary/total, and retry exactly once on movement.
        arguments = {**(params or {}), "maxResults": 100}
        if order:
            arguments["orderBy"] = order
        for _ in range(2):
            values = []
            offset = 0
            first = None
            first_boundary = None
            count = 0
            totals_moved = False
            for _ in range(MAX_PAGES):
                page = self.call(path, params={**arguments, "startAt": offset})
                if isinstance(page, list):
                    entries = page
                    total = None
                    last = not entries
                elif isinstance(page, dict):
                    entries = page.get(key)
                    total = page.get("total")
                    last = page.get(
                        "isLast",
                        offset + len(entries or []) >= total if type(total) is int else False,
                    )
                    if page.get("startAt", offset) != offset or type(total) is not int or total < 0:
                        raise ApiError("snapshot_invalid")
                else:
                    raise ApiError("snapshot_invalid")
                if not isinstance(entries, list) or not isinstance(last, bool):
                    raise ApiError("snapshot_invalid")
                if offset == 0:
                    first = (total, last)
                    first_boundary = entries[:1]
                if first is not None and total != first[0]:
                    totals_moved = True
                count += len(entries)
                if count > MAX_ENTRIES:
                    raise ApiError("snapshot_limit")
                values.extend(entries)
                if last:
                    break
                if not entries:
                    raise ApiError("snapshot_partial")
                offset += len(entries)
            else:
                raise ApiError("page_limit")
            again = self.call(path, params={**arguments, "startAt": 0})
            start = (
                again
                if isinstance(again, list)
                else again.get(key)
                if isinstance(again, dict)
                else None
            )
            last_total = None if isinstance(again, list) else again.get("total")
            if (
                not totals_moved
                and isinstance(start, list)
                and start[:1] == first_boundary
                and last_total == first[0]
            ):
                if first[0] is not None and len(values) != first[0]:
                    continue
                return values
        raise ApiError("snapshot_changed")


def number(value):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, str))
        or not str(value).isdigit()
        or not 0 < int(value) < 10**18
    ):
        raise ValueError("jira_id_invalid")
    return int(value)
