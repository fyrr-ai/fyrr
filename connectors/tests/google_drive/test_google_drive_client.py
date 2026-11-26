"""The client: which responses are retried, which are final, and paging."""

import json
import sys

import pytest
import requests
from conftest import ALICE
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fake_drive import FakeDrive

from fyrr_connectors import _http, bundled, runtime
from fyrr_connectors.google_drive import client
from fyrr_connectors.google_drive.client import ATTEMPTS, ApiError, Client


def _client() -> Client:
    return Client("{}", "https://www.googleapis.com")


def _files(path: str, _params: dict) -> bool:
    return path.endswith("/files")


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (429, "rateLimitExceeded"),
        (403, "userRateLimitExceeded"),
        (403, "rateLimitExceeded"),
        (503, ""),
    ],
)
def test_rate_limits_and_server_errors_are_retried(
    drive: FakeDrive, status: int, reason: str
) -> None:
    drive.fail(_files, status, reason, times=2)
    assert _client().get(ALICE, "files", corpora="user") == {"files": []}
    assert len(drive.requests) == 3
    assert len(drive.sleeps) == 2 and drive.sleeps[0] < drive.sleeps[1]


def test_retry_after_is_honored_and_bounded(drive: FakeDrive) -> None:
    drive.fail(_files, 429, retry_after="7")
    drive.fail(_files, 429, retry_after="900")
    with pytest.raises(ApiError) as caught:
        _client().get(ALICE, "files", corpora="user")
    assert caught.value.status == 429 and caught.value.transient
    assert 7 <= drive.sleeps[0] < 8.01
    assert len(drive.sleeps) == 1


def test_exhausted_retries_are_transient_failures(drive: FakeDrive) -> None:
    drive.fail(_files, 500, times=99)
    with pytest.raises(ApiError) as caught:
        _client().get(ALICE, "files", corpora="user")
    assert caught.value.transient and caught.value.status == 500
    assert len(drive.requests) == ATTEMPTS


def test_transport_errors_are_retried(drive: FakeDrive, monkeypatch: pytest.MonkeyPatch) -> None:
    session = drive.session(ALICE)
    real, calls = session.get, []

    def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) < 3:
            raise requests.ConnectionError
        return real(*args, **kwargs)

    session.get = flaky
    monkeypatch.setattr(client, "authorized_session", lambda _key, _subject, _api: session)
    assert _client().get(ALICE, "files", corpora="user") == {"files": []}
    assert len(calls) == 3


@pytest.mark.parametrize(
    ("status", "reason"),
    [(403, "insufficientFilePermissions"), (404, "notFound"), (400, "invalid")],
)
def test_other_errors_are_final_and_carry_no_body(
    drive: FakeDrive, status: int, reason: str
) -> None:
    drive.fail(_files, status, reason, location="pageToken")
    with pytest.raises(ApiError) as caught:
        _client().get(ALICE, "files", corpora="user")
    error = caught.value
    assert (error.status, error.reason, error.location, error.transient) == (
        status,
        reason,
        "pageToken",
        False,
    )
    assert str(error) == f"drive_api_{status}_{reason}"
    assert len(drive.requests) == 1 and not drive.sleeps


def test_an_export_over_the_cap_is_refused_not_buffered(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive.put("doc", b"x" * 64, owner=ALICE)
    assert _client().export(ALICE, "doc", "text/markdown") == b"x" * 64
    monkeypatch.setattr(client, "EXPORT_MAX_BYTES", 63)
    with pytest.raises(ApiError) as caught:
        _client().export(ALICE, "doc", "text/markdown")
    assert caught.value.reason == "exportSizeLimitExceeded" and not caught.value.transient


def test_a_malformed_key_is_reported_without_its_content() -> None:
    with pytest.raises(ValueError, match="google_drive_service_account_invalid"):
        Client("SENTINEL not json", "https://www.googleapis.com")


def test_sessions_are_delegated_to_the_user_with_the_read_only_scope() -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    key = {
        "type": "service_account",
        "client_email": "sync@project.iam.gserviceaccount.example",
        "token_uri": "https://oauth2.googleapis.com/token",
        "private_key": private_key.decode(),
    }
    credentials = client.authorized_session(key, ALICE, client.DRIVE).credentials
    assert credentials.scopes == [client.DRIVE_SCOPE]
    assert credentials._subject == ALICE
    directory = client.authorized_session(key, "admin@example.com", client.DIRECTORY).credentials
    assert directory.scopes == list(client.DIRECTORY_SCOPES)
    assert Client(json.dumps(key), "https://www.googleapis.com")._key == key


def test_a_missing_extra_fails_only_this_provider_with_a_clear_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "fyrr_connectors.google_drive", None)
    with pytest.raises(ImportError, match="google-drive\\] extra"):
        bundled.google_drive()
    providers = {p["entry_point"]: p for p in json.loads(runtime.discover())["providers"]}
    assert providers["google-drive"]["error"] == {"type": "provider_failure"}
    assert all("info" in p for name, p in providers.items() if name != "google-drive")


@pytest.mark.parametrize("status", [200, 403])
def test_json_response_body_is_bounded_and_closed(drive, monkeypatch, status):
    session = drive.session(ALICE)
    opened = []
    real = session.get

    def get(*args, **kwargs):
        response = real(*args, **kwargs)
        opened.append(response)
        return response

    session.get = get
    monkeypatch.setattr(client, "authorized_session", lambda *_: session)
    monkeypatch.setattr(_http, "MAX_RESPONSE", 8)
    if status != 200:
        drive.fail(_files, status, "insufficientFilePermissions")
    with pytest.raises(ApiError, match="response_limit"):
        _client().get(ALICE, "files", corpora="user")
    assert len(opened) == 1
    assert opened[0].raw.closed


def test_page_limit_refuses_an_incomplete_listing(drive, monkeypatch):
    for number in range(5):
        drive.put(f"doc-{number}", owner=ALICE)
    monkeypatch.setattr(_http, "MAX_PAGES", 1)
    with pytest.raises(ApiError, match="pagination_limit"):
        list(_client().pages(ALICE, "files", corpora="user", fields="nextPageToken,files(id)"))


def test_page_token_cycle_is_rejected(drive, monkeypatch):
    real = drive._answer

    def answer(subject, path, params):
        if path.endswith("/files"):
            return 200, {"files": [], "nextPageToken": "repeated"}
        return real(subject, path, params)

    monkeypatch.setattr(drive, "_answer", answer)
    pages = _client().pages(ALICE, "files", corpora="user", fields="nextPageToken,files(id)")
    with pytest.raises(ApiError, match="pagination_cycle"):
        # Limit the pre-fix run rather than letting an unbounded generator hang.
        for _ in range(3):
            next(pages)


def test_large_listing_completes_reconciliation(drive, index, monkeypatch):
    real = drive._answer
    total = 100_001

    def answer(subject, path, params):
        if path.endswith("/files"):
            start = int(params.get("pageToken", "0"))
            end = min(start + 100, total)
            body = {
                "files": [
                    {"id": f"doc-{number}", "ownedByMe": True} for number in range(start, end)
                ]
            }
            if end < total:
                body["nextPageToken"] = str(end)
            return 200, body
        return real(subject, path, params)

    monkeypatch.setattr(drive, "_answer", answer)
    assert set(index.listing(owners=[ALICE])) == {f"doc-{number}" for number in range(total)}


@pytest.mark.parametrize("kind", ["content", "permissions", "reconcile"])
def test_sync_paths_propagate_pagination_failure(drive, index, monkeypatch, kind):
    for number in range(5):
        drive.put(f"doc-{number}", owner=ALICE)
    monkeypatch.setattr(_http, "MAX_PAGES", 1)
    batches = []
    with pytest.raises(ApiError, match="pagination_limit"):
        for batch in index.run(kind, "sync", owners=[ALICE]):
            batches.append(batch)
    # Partial work must not be reported as a successful full read.
    assert all(not getattr(batch, "complete", False) for batch in batches)


def test_non_json_server_errors_keep_the_retry_policy(drive, monkeypatch):
    real = drive._answer
    calls = []

    def answer(subject, path, params):
        if path.endswith("/files"):
            calls.append(path)
            if len(calls) < 3:
                return 503, b"<html>upstream unavailable</html>"
        return real(subject, path, params)

    monkeypatch.setattr(drive, "_answer", answer)
    assert _client().get(ALICE, "files", corpora="user") == {"files": []}
    assert len(calls) == 3
    assert len(drive.sleeps) == 2


def test_worker_sessions_are_owned_by_each_thread(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    barrier = Barrier(2)
    opened = []

    class Session:
        closed = False

        def close(self):
            self.closed = True

    def factory(*_):
        session = Session()
        opened.append(session)
        return session

    monkeypatch.setattr(client, "authorized_session", factory)
    shared = _client()

    def worker(subject):
        first = shared._session(subject, client.DRIVE)
        barrier.wait(timeout=5)
        assert not first.closed
        assert shared._session(subject, client.DRIVE) is first
        barrier.wait(timeout=5)
        return first

    with ThreadPoolExecutor(2) as pool:
        sessions = list(pool.map(worker, ["alice", "bob"]))
    assert sessions[0] is not sessions[1]
    assert len(opened) == 2


@pytest.mark.parametrize("limit", ["MAX_ENTRIES", "MAX_EDGES", "MAX_BYTES"])
def test_aggregate_limits_abort(limit, monkeypatch):
    from fyrr_connectors.google_drive import bounds

    monkeypatch.setattr(bounds, limit, 1)
    budget = bounds.Budget()
    with pytest.raises(ValueError, match="google_drive_aggregate_limit"):
        budget.add("large", edges=2)
        budget.add("large", edges=2)


def test_listing_over_aggregate_limit_never_completes(drive, index, monkeypatch):
    from fyrr_connectors.google_drive import bounds

    drive.put("doc", owner=ALICE)
    monkeypatch.setattr(bounds, "MAX_ENTRIES", 0)
    batches = []
    with pytest.raises(ValueError, match="google_drive_aggregate_limit"):
        for batch in index.run("reconcile", "sync", owners=[ALICE]):
            batches.append(batch)
    assert all(not getattr(batch, "complete", False) for batch in batches)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("7", 7),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 10),
        ("Wed, 21 Oct 2015 07:27:00 GMT", 0),
        ("soon", None),
        ("nan", None),
        ("-1", None),
    ],
)
def test_shared_retry_after_parser(value, expected, monkeypatch):
    from fyrr_connectors import _http

    monkeypatch.setattr(_http.time, "time", lambda: 1445412470)
    assert _http.retry_after(value) == expected


def test_retry_after_over_remaining_budget_fails_without_sleep(monkeypatch):
    from fyrr_connectors import _http

    sleeps = []
    monkeypatch.setattr(_http.time, "sleep", sleeps.append)
    retries = _http.Retries(_http.RetryPolicy(budget=1), ValueError("budget"))
    attempts = iter(retries)
    next(attempts)
    retries.wait = 2
    with pytest.raises(ValueError, match="budget"):
        next(attempts)
    assert sleeps == []


def test_members_over_aggregate_limit_emit_no_snapshot(drive, index, monkeypatch):
    from fyrr_connectors.google_drive import bounds

    monkeypatch.setattr(bounds, "MAX_BYTES", 0)
    batches = []
    with pytest.raises(ValueError, match="google_drive_aggregate_limit"):
        for batch in index.run("members", "sync", owners=[ALICE]):
            batches.append(batch)
    assert batches == []


def test_run_reuses_executor_and_closes_sessions(drive, index, monkeypatch):
    from fyrr_connectors.google_drive import connector

    pools, sessions, closed = [], [], []
    original_pool = connector.ThreadPoolExecutor
    original_session = client.authorized_session

    def pool(*args):
        created = original_pool(*args)
        pools.append(created)
        return created

    def session(*args):
        created = original_session(*args)
        sessions.append(created)
        close = created.close

        def finish():
            closed.append(created)
            close()

        created.close = finish
        return created

    monkeypatch.setattr(connector, "ThreadPoolExecutor", pool)
    monkeypatch.setattr(client, "authorized_session", session)
    for number in range(105):
        drive.put(f"doc-{number}", owner=ALICE)
    list(index.run("content", "fixed", owners=[ALICE]))
    assert len(pools) == 1
    assert sessions and len(sessions) <= 9
    assert len(closed) == len(sessions)


def test_thread_sessions_keep_subject_api_pairs_until_shutdown(monkeypatch):
    opened = []

    class Session:
        closed = False

        def close(self):
            self.closed = True

    def factory(*args):
        result = Session()
        opened.append(result)
        return result

    monkeypatch.setattr(client, "authorized_session", factory)
    shared = _client()
    first = shared._session(ALICE, client.DRIVE)
    shared._session("admin", client.DRIVE)
    shared._session("admin", client.DIRECTORY)
    assert shared._session(ALICE, client.DRIVE) is first
    assert len(opened) == 3 and not any(session.closed for session in opened)
    shared.close()
    assert all(session.closed for session in opened)


@pytest.mark.parametrize("group_count", [20, 101])
def test_large_domain_counts_members_as_edges(drive, index, monkeypatch, group_count):
    from fyrr_connectors.google_drive.connector import GoogleDrive

    def directory(self, path, key, **params):
        if key == "groups":
            yield from ({"email": f"group-{number}@example.com"} for number in range(group_count))
        else:
            yield from (
                {"email": f"user-{number}@example.com", "type": "USER"} for number in range(10_000)
            )

    def users(self):
        yield from ({"primaryEmail": f"user-{number}@example.com"} for number in range(10_000))

    monkeypatch.setattr(GoogleDrive, "_directory", directory)
    monkeypatch.setattr(GoogleDrive, "_directory_users", users)
    batches = []
    if group_count == 101:
        with pytest.raises(ValueError, match="google_drive_aggregate_limit"):
            for batch in index.run("members", "sync", owners=[ALICE]):
                batches.append(batch)
        assert batches == []
    else:
        batches = list(index.run("members", "sync", owners=[ALICE]))
        assert sum(len(batch.users) for batch in batches) == 10_000
        assert sum(len(batch.groups) for batch in batches) == 21
        assert batches[-1].last


def test_folders_budget_tracks_retained_evidence(monkeypatch):
    from fyrr_connectors.google_drive import bounds
    from fyrr_connectors.google_drive.feeds import Feed, Folders

    monkeypatch.setattr(bounds, "MAX_ENTRIES", 1)
    monkeypatch.setattr(bounds, "MAX_BYTES", 150)
    feed = Feed("user", ALICE)
    folders = Folders(_client(), [])
    for _ in range(1000):
        folders.learn(feed, {"id": "folder", "parents": ["parent"], "unused": "x" * 1000})
    assert folders._budget.entries == 1
    initial = folders._budget.bytes
    folders.learn(feed, {"id": "folder", "parents": ["longer-parent"]})
    assert folders._budget.bytes == initial + len("longer-")
    with pytest.raises(ValueError, match="google_drive_aggregate_limit"):
        folders.learn(feed, {"id": "second"})
    assert (ALICE, "second") not in folders._parents


@pytest.mark.parametrize("header", ["0", "Wed, 21 Oct 2015 07:28:00 GMT"])
def test_retry_after_does_not_shorten_backoff(drive, header):
    drive.fail(_files, 429, retry_after=header, times=2)
    _client().get(ALICE, "files", corpora="user")
    assert 1 <= drive.sleeps[0] <= 2
    assert 2 <= drive.sleeps[1] <= 3


def test_cumulative_retry_wait_budget_exhaustion(monkeypatch):
    from fyrr_connectors import _http

    now, sleeps = [0.0], []

    def sleep(delay):
        sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(_http.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(_http.time, "sleep", sleep)
    retries = _http.Retries(_http.RetryPolicy(budget=5), ValueError("budget"))
    attempts = iter(retries)
    next(attempts)
    retries.wait = 3
    next(attempts)
    retries.wait = 3
    with pytest.raises(ValueError, match="budget"):
        next(attempts)
    assert sleeps == [3] and now[0] == 3
