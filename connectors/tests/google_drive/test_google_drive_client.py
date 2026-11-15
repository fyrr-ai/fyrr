"""The client: which responses are retried, which are final, and paging."""

import json
import sys

import pytest
import requests
from conftest import ALICE
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fake_drive import FakeDrive

from fyrr_connectors import bundled, runtime
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
    _client().get(ALICE, "files", corpora="user")
    assert 7 <= drive.sleeps[0] < 8.01
    assert drive.sleeps[1] <= client.MAX_WAIT_SECONDS + 1


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
    monkeypatch.setattr(client, "MAX_RESPONSE_BYTES", 8, raising=False)
    if status != 200:
        drive.fail(_files, status, "insufficientFilePermissions")
    with pytest.raises(ApiError, match="response_limit"):
        _client().get(ALICE, "files", corpora="user")
    assert len(opened) == 1
    assert opened[0].raw.closed


def test_page_limit_refuses_an_incomplete_listing(drive, monkeypatch):
    for number in range(5):
        drive.put(f"doc-{number}", owner=ALICE)
    monkeypatch.setattr(client, "MAX_PAGES", 1, raising=False)
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
    monkeypatch.setattr(client, "MAX_PAGES", 1)
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
