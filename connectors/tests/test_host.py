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


class Failing(Connector):
    def content(self, cursor):
        raise ValueError("secret document text")


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


def test_the_handshake_names_the_protocol_and_the_sdk(served: Peer) -> None:
    ready = served.message()
    assert ready == {"t": "ready", "protocol": host.PROTOCOL_VERSION, "sdk_version": "0.1.1"}


def test_inventory_and_discover_answer_with_documents(served: Peer) -> None:
    served.message()
    served.send({"t": "inventory"})
    kind, payload = served.frame()
    assert kind == b"R"
    names = [entry["entry_point"] for entry in json.loads(payload)["providers"]]
    assert "local-files" in names
    served.send({"t": "discover", "name": "local-files"})
    kind, payload = served.frame()
    assert kind == b"R"
    [entry] = json.loads(payload)["providers"]
    assert entry["info"]["name"] == "local-files"


def test_content_rows_cross_as_an_arrow_ipc_stream(
    served: Peer, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = use(monkeypatch, sample_provider.SampleConnector)
    served.message()
    source = json.loads(source)
    source["settings"] = {"api_token": {"env": "SAMPLE_TOKEN"}}
    source["scope"] = {"titles": ["one", "two", "three"]}
    start_run(served, json.dumps(source))
    rows = 0
    while True:
        served.send({"t": "next"})
        kind, payload = served.frame()
        if kind == b"J":
            assert json.loads(payload) == {"t": "done"}
            break
        assert kind == b"B"
        assert json.loads(payload)["kind"] == "content"
        rows_kind, stream = served.frame()
        assert rows_kind == b"A"
        batches = list(pa.ipc.open_stream(stream))
        assert len(batches) == 1
        rows += batches[0].num_rows
    assert rows == 3


def test_a_batch_over_the_byte_limit_is_refused_before_it_is_serialized(
    served: Peer, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = json.loads(use(monkeypatch, sample_provider.SampleConnector))
    source["settings"] = {"api_token": {"env": "SAMPLE_TOKEN"}}
    source["scope"] = {"titles": ["one"]}
    source["limits"] = dict(LIMITS, max_bytes=8)
    served.message()
    start_run(served, json.dumps(source))
    served.send({"t": "next"})
    assert served.message() == {"t": "oversize"}


def test_provider_failures_cross_as_a_closed_code(
    served: Peer, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = use(monkeypatch, Failing)
    served.message()
    start_run(served, source)
    served.send({"t": "next"})
    kind, payload = served.frame()
    assert (kind, json.loads(payload)) == (b"J", {"t": "failed"})
    assert b"secret" not in payload


def test_close_runs_the_generators_cleanup(served: Peer, monkeypatch: pytest.MonkeyPatch) -> None:
    Counting.closed = 0
    source = use(monkeypatch, Counting)
    served.message()
    start_run(served, source)
    served.send({"t": "next"})
    assert served.frame()[0] == b"B"
    served.frame()
    served.send({"t": "close"})
    assert served.message() == {"t": "closed"}
    assert Counting.closed == 1


def test_revision_callbacks_round_trip_while_a_batch_is_produced(
    served: Peer, monkeypatch: pytest.MonkeyPatch
) -> None:
    SEEN.clear()
    source = use(monkeypatch, Asking)
    served.message()
    start_run(served, source, revisions=True)
    served.send({"t": "next"})
    assert served.message() == {"t": "lookup_revisions", "roots": ["a", "b"]}
    served.send({"t": "revisions", "map": {"a": "r1"}})
    assert served.message() == {"t": "next_revisions"}
    served.send({"t": "revisions", "items": [["a", "r1"]]})
    assert served.frame()[0] == b"B"
    served.frame()
    assert SEEN == {"lookup": {"a": "r1"}, "next": [("a", "r1")]}


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


def test_the_host_process_serves_over_an_inherited_descriptor_and_exits_on_eof() -> None:
    ours, theirs = socket.socketpair()
    stdio = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    child = subprocess.Popen(
        [sys.executable, "-P", "-m", "fyrr_connectors.host", str(theirs.fileno())],
        pass_fds=[theirs.fileno()],
        **stdio,
    )
    theirs.close()
    peer = Peer(ours)
    assert peer.message()["protocol"] == host.PROTOCOL_VERSION
    peer.send({"t": "inventory"})
    assert peer.frame()[0] == b"R"
    ours.close()
    assert child.wait(timeout=30) == 0


def test_the_host_refuses_a_missing_or_invalid_descriptor_argument() -> None:
    for argument in ([], ["x"], ["1", "2"]):
        result = subprocess.run(
            [sys.executable, "-P", "-m", "fyrr_connectors.host", *argument],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            env=dict(os.environ),
        )
        assert result.returncode == 2, argument
        assert result.stdout == b""
