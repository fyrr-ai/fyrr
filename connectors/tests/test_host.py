"""The provider host's wire protocol, end to end over a real socket."""

import json
import os
import socket
import struct
import subprocess
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
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


LAUNCHER = "import sys; from fyrr_connectors import host; sys.exit(host.main(sys.argv, 0))"


def spawn_host(*arguments: str) -> tuple[subprocess.Popen[bytes], Peer]:
    """The real host process with the socket as stdin. fyrr's worker adds a
    bootstrap that moves the socket off stdin first; its descriptor handling is
    tested through the real worker, where the bootstrap is."""
    ours, theirs = socket.socketpair()
    child = subprocess.Popen(
        [sys.executable, "-c", LAUNCHER, *arguments],
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


def test_a_process_that_provider_code_forks_does_not_hold_the_socket() -> None:
    script = """
import os, socket
from fyrr_connectors import host

ours, theirs = socket.socketpair()
connection = host.Connection(theirs)
fd = theirs.fileno()
os.register_at_fork(after_in_child=connection.abandon)
pid = os.fork()
if pid == 0:
    try:
        os.fstat(fd)
    except OSError:
        os._exit(0)
    os._exit(1)
_, status = os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0, "the child held the socket"
os.fstat(fd)  # the parent still does
theirs.sendall(b"x")
assert ours.recv(1) == b"x"
"""
    subprocess.run([sys.executable, "-c", script], check=True, timeout=30)


def test_the_host_refuses_arguments_and_a_descriptor_that_is_no_socket() -> None:
    for arguments in (["1"], ["x", "2"]):
        child, peer = spawn_host(*arguments)
        assert child.wait(timeout=30) == 2, arguments
        peer.sock.close()
    result = subprocess.run(
        [sys.executable, "-c", LAUNCHER],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 2
    assert result.stdout == b""


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


def test_the_real_host_keeps_credentials_out_of_its_initial_environment() -> None:
    child, peer = spawn_host()
    peer.message()
    peer.send({"t": "environment", "variables": {"FYRR_TEST_CREDENTIAL": "SECRET-VALUE"}})
    assert peer.message() == {"t": "environment_set"}
    environ = Path(f"/proc/{child.pid}/environ").read_bytes()
    assert b"SECRET-VALUE" not in environ and b"FYRR_TEST_CREDENTIAL" not in environ
    peer.sock.close()
    assert child.wait(timeout=30) == 0


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


def test_a_batch_over_the_byte_limit_is_refused_even_with_metadata(
    served: Peer, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = json.loads(use(monkeypatch, Noisy))
    source["limits"] = dict(LIMITS, max_bytes=8)
    served.message()
    start_run(served, json.dumps(source))
    served.send({"t": "next"})
    assert served.message() == {"t": "oversize"}
