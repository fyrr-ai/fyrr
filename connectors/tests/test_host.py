"""The provider host's wire protocol, end to end over a real socket."""

import json
import os
import socket
import struct
import subprocess
import sys
import threading
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest
import sample_provider

from fyrr_connectors import Connector, ContentBatch, document_changes, host, provider_info, runtime

LIMITS = {"max_rows": 100, "max_bytes": 1 << 20, "document_max_bytes": 1 << 16}
FRAME = struct.Struct(">cI")


class Peer:
    """The worker's side: sends requests, reads frames."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock

    def send(self, message: dict[str, Any]) -> None:
        payload = json.dumps(message).encode()
        self.sock.sendall(FRAME.pack(b"J", len(payload)) + payload)

    def frame(self) -> tuple[bytes, bytes]:
        kind, length = FRAME.unpack(self._exact(FRAME.size))
        return kind, self._exact(length)

    def message(self) -> dict[str, Any]:
        kind, payload = self.frame()
        assert kind == b"J", kind
        return json.loads(payload)

    def _exact(self, count: int) -> bytes:
        data = bytearray()
        while len(data) < count:
            chunk = self.sock.recv(count - len(data))
            assert chunk, "the host closed the socket"
            data += chunk
        return bytes(data)


@pytest.fixture
def served() -> Iterator[Peer]:
    ours, theirs = socket.socketpair()
    thread = threading.Thread(target=host.Host(host.Connection(theirs)).serve, daemon=True)
    thread.start()
    peer = Peer(ours)
    yield peer
    ours.close()
    thread.join(timeout=5)
    assert not thread.is_alive(), "the host must stop when the worker closes the socket"
    theirs.close()


class Counting(Connector):
    """Two content batches; records whether its generator was closed."""

    closed = 0

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        try:
            yield ContentBatch(changes=document_changes([]), cursor="1")
            yield ContentBatch(changes=document_changes([]), cursor="2")
        finally:
            Counting.closed += 1


SEEN: dict[str, Any] = {}


class Asking(Connector):
    def content(self, cursor):
        SEEN["lookup"] = self.context.lookup_revisions(["a", "b"])
        SEEN["next"] = self.context.next_revisions()
        yield ContentBatch(changes=document_changes([]), cursor="1")


class AskingBadly(Connector):
    def content(self, cursor):
        try:
            self.context.lookup_revisions(["a"])
        except RuntimeError as error:
            SEEN["error"] = str(error)
        yield ContentBatch(changes=document_changes([]), cursor="1")


class Reading(Connector):
    def content(self, cursor):
        SEEN["credential"] = os.environ.get("FYRR_TEST_CREDENTIAL")
        yield ContentBatch(changes=document_changes([]), cursor="1")


class Noisy(Connector):
    """Rows whose schema carries metadata of the provider's own."""

    def content(self, cursor):
        rows = document_changes([])
        noisy = rows.replace_schema_metadata({**(rows.schema.metadata or {}), b"note": b"x" * 4096})
        yield ContentBatch(changes=noisy, cursor="1")


class Plain(Connector):
    def content(self, cursor):
        SEEN["no_callbacks"] = self.context.lookup_revisions is None
        yield ContentBatch(changes=document_changes([]), cursor="1")


def use(monkeypatch: pytest.MonkeyPatch, connector: type[Connector]) -> str:
    info = provider_info(name="x", connector=connector, min_sdk_version="0.1.0")

    class Entry:
        def load(self):
            return lambda: info

    monkeypatch.setattr(runtime, "provider_entry_point", lambda name: Entry())
    return json.dumps(
        {
            "source_id": "s",
            "provider": "x",
            "permissions": "fixed",
            "settings": {},
            "scope": {},
            "limits": LIMITS,
        }
    )


def start_run(peer: Peer, source: str, revisions: bool = False) -> None:
    peer.send(
        {"t": "run", "source": source, "kind": "content", "cursor": None, "revisions": revisions}
    )
    assert peer.message() == {"t": "started"}


@pytest.mark.parametrize(
    ("connector", "settings", "scope"),
    [
        (
            sample_provider.SampleConnector,
            {"api_token": {"env": "SAMPLE_TOKEN"}},
            {"titles": ["one"]},
        ),
        # The schema's own metadata is no way around the limit either.
        (Noisy, {}, {}),
    ],
)
def test_a_batch_over_the_byte_limit_is_refused_before_it_is_serialized(
    served: Peer,
    monkeypatch: pytest.MonkeyPatch,
    connector: type[Connector],
    settings: dict[str, Any],
    scope: dict[str, Any],
) -> None:
    source = json.loads(use(monkeypatch, connector))
    source["settings"], source["scope"] = settings, scope
    source["limits"] = dict(LIMITS, max_bytes=8)
    served.message()
    start_run(served, json.dumps(source))
    served.send({"t": "next"})
    assert served.message() == {"t": "oversize"}


def test_end_of_file_runs_the_generators_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    Counting.closed = 0
    source = use(monkeypatch, Counting)
    peer, thread = serve_in_thread()
    peer.message()
    start_run(peer, source)
    peer.send({"t": "next"})
    assert peer.frame()[0] == b"B"
    peer.frame()
    peer.sock.shutdown(socket.SHUT_WR)
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert Counting.closed == 1


def test_a_callback_error_raises_in_the_provider_without_a_value(
    served: Peer, monkeypatch: pytest.MonkeyPatch
) -> None:
    SEEN.clear()
    source = use(monkeypatch, AskingBadly)
    served.message()
    start_run(served, source, revisions=True)
    served.send({"t": "next"})
    assert served.message()["t"] == "lookup_revisions"
    served.send({"t": "error"})
    assert served.frame()[0] == b"B"
    assert SEEN == {"error": "host_callback_failed"}


def test_without_revisions_the_provider_has_no_callbacks(
    served: Peer, monkeypatch: pytest.MonkeyPatch
) -> None:
    SEEN.clear()
    source = use(monkeypatch, Plain)
    served.message()
    start_run(served, source)
    served.send({"t": "next"})
    assert served.frame()[0] == b"B"
    assert SEEN == {"no_callbacks": True}


LAUNCHER = "import sys; from fyrr_connectors import host; sys.exit(host.main(0))"


def spawn_host() -> tuple[subprocess.Popen[bytes], Peer]:
    """The real host process with the socket as stdin. fyrr's worker adds a
    bootstrap that moves the socket off stdin first; its descriptor handling is
    tested through the real worker, where the bootstrap is."""
    ours, theirs = socket.socketpair()
    child = subprocess.Popen(
        [sys.executable, "-c", LAUNCHER],
        stdin=theirs,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    theirs.close()
    return child, Peer(ours)


def test_the_host_process_serves_over_its_socket_and_exits_on_eof() -> None:
    child, peer = spawn_host()
    assert peer.message()["protocol"] == host.PROTOCOL_VERSION
    peer.send({"t": "inventory"})
    assert peer.frame()[0] == b"R"
    peer.sock.close()
    assert child.wait(timeout=30) == 0


def test_credentials_set_the_environment_before_any_provider_code_and_are_not_echoed(
    served: Peer, monkeypatch: pytest.MonkeyPatch
) -> None:
    SEEN.clear()
    # The host sets it in this process's environment; this restores it.
    monkeypatch.setenv("FYRR_TEST_CREDENTIAL", "placeholder")
    source = use(monkeypatch, Reading)
    served.message()
    served.send({"t": "environment", "variables": {"FYRR_TEST_CREDENTIAL": "SECRET-VALUE"}})
    kind, payload = served.frame()
    assert (kind, json.loads(payload)) == (b"J", {"t": "environment_set"})
    assert b"SECRET" not in payload
    start_run(served, source)
    served.send({"t": "next"})
    assert served.frame()[0] == b"B"
    assert SEEN == {"credential": "SECRET-VALUE"}


def serve_in_thread() -> tuple[Peer, threading.Thread]:
    ours, theirs = socket.socketpair()
    thread = threading.Thread(target=host.Host(host.Connection(theirs)).serve, daemon=True)
    thread.start()
    return Peer(ours), thread


@pytest.mark.parametrize(
    "message",
    [
        {"t": "environment", "variables": {"1BAD": "v"}},
        {"t": "environment", "variables": {"A=B": "v"}},
        {"t": "environment", "variables": {"lower": "v"}},
        {"t": "environment", "variables": {"N" * 257: "v"}},
        {"t": "environment", "variables": {"NAME": "has\u0000nul"}},
        {"t": "environment", "variables": {"NAME": 5}},
        {"t": "environment", "variables": {f"N{n}": "v" for n in range(300)}},
        {"t": "environment", "variables": ["not", "a", "map"]},
        {"t": "environment"},
        # Names that change which interpreter, libraries or modules the host runs.
        {"t": "environment", "variables": {"PATH": "/nowhere"}},
        {"t": "environment", "variables": {"VIRTUAL_ENV": "/nowhere"}},
        {"t": "environment", "variables": {"PYTHONPATH": "/nowhere"}},
        {"t": "environment", "variables": {"PYTHONHOME": "/nowhere"}},
        {"t": "environment", "variables": {"LD_PRELOAD": "/nowhere"}},
        {"t": "environment", "variables": {"LD_LIBRARY_PATH": "/nowhere"}},
        {"t": "environment", "variables": {"NAME": "v", "PYTHONSTARTUP": "/nowhere"}},
    ],
)
def test_an_invalid_environment_message_ends_the_conversation(message: dict[str, Any]) -> None:
    before = dict(os.environ)
    peer, thread = serve_in_thread()
    peer.message()
    peer.send(message)
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert dict(os.environ) == before


def test_a_name_of_the_full_configured_length_is_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = "N" * 256
    monkeypatch.setenv(name, "placeholder")
    peer, thread = serve_in_thread()
    peer.message()
    peer.send({"t": "environment", "variables": {name: "SECRET-VALUE"}})
    assert peer.message() == {"t": "environment_set"}
    assert os.environ[name] == "SECRET-VALUE"
    peer.sock.close()
    thread.join(timeout=5)


def test_the_environment_may_only_be_set_before_any_other_request() -> None:
    peer, thread = serve_in_thread()
    peer.message()
    peer.send({"t": "inventory"})
    assert peer.frame()[0] == b"R"
    peer.send({"t": "environment", "variables": {"FYRR_TEST_LATE": "v"}})
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert "FYRR_TEST_LATE" not in os.environ


def test_rows_leave_with_only_the_contract_metadata(
    served: Peer, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = use(monkeypatch, Noisy)
    served.message()
    start_run(served, source)
    served.send({"t": "next"})
    assert served.frame()[0] == b"B"
    kind, stream = served.frame()
    assert kind == b"A"
    schema = pa.ipc.open_stream(stream).schema
    assert set(schema.metadata) == {b"fyrr.contract", b"fyrr.schema_version"}
    # A batch the provider built without the contract's entries leaves without them.
    assert b"note" not in stream
