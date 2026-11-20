"""The provider host: the process in which fyrr runs connector providers.

fyrr's worker starts the interpreter (`python -S -c ...`) with the connected
socket of the conversation as stdin and `/dev/null` as stdout and stderr. Its
bootstrap, which uses only built-in modules, first moves the socket to a
descriptor that no child process inherits, points stdin at `/dev/null` and
closes every other inherited descriptor; only then does it run what
start-up would have (`site.main()`: `.pth` files, `sitecustomize`) and import
this module, whose `main` takes that descriptor. So neither site code nor
provider code, nor the processes they start, hold the socket or anything else
the worker's parent left open; a process that provider code forks closes the
socket too. The host serves the operations of `fyrr_connectors.runtime` on that
socket and nothing else: providers may print, and nothing they print is read.

The conversation is synchronous, one request at a time, in length-prefixed
frames: a kind byte, a big-endian `u32` payload length and the payload.

========  ======================================================
kind      payload
========  ======================================================
``J``     a JSON object with a ``"t"`` member (a control message)
``R``     a JSON document: the result of `inventory` or `discover`
``H``     the contract header (JSON) of a batch without rows
``B``     the contract header (JSON) of a batch; an ``A`` follows
``A``     the rows of that batch as an Arrow IPC stream
========  ======================================================

The host sends ``ready`` first; the worker then sends one request at a time:

``environment``
    Optional, and only as the first request: ``{"t": "environment",
    "variables": {NAME: value, ...}}`` sets the provider's credentials in
    `os.environ` before any provider code is imported; the worker has removed
    their names from the environment it starts the host with, so they are never
    in the process's initial environment (`/proc/<pid>/environ`). The names are
    what a configuration may reference (`[A-Z_][A-Z0-9_]*`, at most 256
    characters) except ``PATH``, ``VIRTUAL_ENV`` and anything starting with
    ``PYTHON`` or ``LD_``. The host validates the names and values, answers
    ``environment_set`` and never echoes them; an invalid message ends the
    conversation.
``inventory`` / ``discover``
    One ``R`` frame, or ``failed``.
``run``
    ``started``. The worker then sends ``next`` once per batch and gets a
    batch (``H``, or ``B`` and ``A``), ``done``, ``failed`` or ``oversize``.
    While a ``next`` runs, the provider may call back: the host sends
    ``lookup_revisions`` or ``next_revisions`` and blocks for the worker's
    ``revisions`` or ``error`` reply.
``close``
    Closes the running generator (so its cleanup runs); replies ``closed``.

Replies and failures are closed codes: no exception text and no
provider-supplied value ever crosses the socket, only contract data. When the
worker closes the socket the host closes the generator and exits.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import socket
import struct
import threading
from collections.abc import Callable, Generator
from typing import Any

from . import runtime
from .arrow import CONTRACT_KEY, VERSION_KEY

#: Bumped when the frame layout or the messages change incompatibly.
PROTOCOL_VERSION = 1

JSON, DOCUMENT, HEADER, BATCH, ROWS = b"J", b"R", b"H", b"B", b"A"
_FRAME = struct.Struct(">cI")
#: A worker reply is revisions the worker itself read from storage; this only
#: guards the host against a corrupt length before it allocates.
_MAX_INBOUND = 64 << 20
_SMALL = 1 << 16
#: Credentials the worker may set in the environment: how many, how long.
_ENV_NAME = re.compile(r"[A-Z_][A-Z0-9_]{0,255}")
_MAX_ENV_VARIABLES = 256
_MAX_ENV_VALUE = 1 << 20


def _may_set(name: str) -> bool:
    """Whether a credential may use this environment name: not one that changes
    which interpreter, libraries or modules the host runs."""
    return (
        _ENV_NAME.fullmatch(name) is not None
        and name not in ("PATH", "VIRTUAL_ENV")
        and not name.startswith(("PYTHON", "LD_"))
    )


class ProtocolError(Exception):
    """The peer broke the protocol or closed the socket."""


class Connection:
    """Frames on one connected, blocking socket."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        # Held for a whole request and reply by provider threads, and for
        # every frame by the serving thread, so frames never interleave.
        self.lock = threading.RLock()

    def send(self, kind: bytes, payload: bytes | memoryview) -> None:
        header = _FRAME.pack(kind, len(payload))
        with self.lock:
            try:
                if len(payload) <= _SMALL:
                    self._sock.sendall(header + bytes(payload))
                else:
                    self._sock.sendall(header)
                    self._sock.sendall(payload)
            except OSError as error:
                raise ProtocolError from error

    def send_json(self, message: dict[str, Any]) -> None:
        self.send(JSON, json.dumps(message, separators=(",", ":")).encode())

    def recv(self) -> tuple[bytes, bytes]:
        with self.lock:
            kind, length = _FRAME.unpack(self._exact(_FRAME.size))
            if length > _MAX_INBOUND:
                raise ProtocolError
            return kind, self._exact(length)

    def recv_json(self) -> dict[str, Any]:
        kind, payload = self.recv()
        if kind != JSON:
            raise ProtocolError
        try:
            message = json.loads(payload)
        except ValueError as error:
            raise ProtocolError from error
        if not isinstance(message, dict) or not isinstance(message.get("t"), str):
            raise ProtocolError
        return message

    def abandon(self) -> None:
        """For a process forked by provider code: forget the descriptor. The
        bootstrap's at-fork hook has already closed it, before any site code
        could fork, so the object only lets go of the number here and never
        closes it, which could by now be another file. Takes no lock: it runs
        in a child that may have inherited a held one."""
        with contextlib.suppress(OSError, ValueError):
            self._sock.detach()

    def _exact(self, count: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < count:
            try:
                chunk = self._sock.recv(count - len(chunks))
            except OSError as error:
                raise ProtocolError from error
            if not chunk:
                raise ProtocolError
            chunks += chunk
        return bytes(chunks)


class _Callbacks:
    """The two callbacks a prepared run hands to the provider. They work only
    while a `next` is being served and are safe to call from provider
    threads."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self.active = False

    def lookup_revisions(self, roots: list[str]) -> dict[str, str]:
        reply = self._call({"t": "lookup_revisions", "roots": list(roots)})
        return {str(root): str(revision) for root, revision in reply["map"].items()}

    def next_revisions(self) -> list[tuple[str, str]]:
        reply = self._call({"t": "next_revisions"})
        return [(str(root), str(revision)) for root, revision in reply["items"]]

    def _call(self, request: dict[str, Any]) -> dict[str, Any]:
        with self._connection.lock:
            if not self.active:
                raise RuntimeError("host_callback_failed")
            self._connection.send_json(request)
            reply = self._connection.recv_json()
        if reply["t"] != "revisions":
            raise RuntimeError("host_callback_failed")
        return reply


def _rows_frame(rows: Any, max_bytes: int) -> memoryview | None:
    """The rows as an Arrow IPC stream, or None when they exceed `max_bytes`.
    The check comes first: a slice retains its whole backing buffer, which the
    IPC stream would not carry. The stream's schema carries only the two
    contract metadata entries: any other schema metadata would be bytes the
    byte budget does not count, and fyrr refuses it."""
    if rows.get_total_buffer_size() > max_bytes:
        return None
    import pyarrow as pa

    metadata = rows.schema.metadata or {}
    contract = {
        key: metadata[key.encode()]
        for key in (CONTRACT_KEY, VERSION_KEY)
        if key.encode() in metadata
    }
    rows = rows.replace_schema_metadata(contract)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, rows.schema) as writer:
        writer.write_batch(rows)
    return memoryview(sink.getvalue())


class Host:
    """Serves one worker; at most one provider run is open at a time."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._run: Generator[tuple[str, Any]] | None = None
        self._callbacks: _Callbacks | None = None
        self._max_bytes = 0
        self._requests = 0

    def serve(self) -> None:
        connection = self._connection
        connection.send_json(
            {
                "t": "ready",
                "protocol": PROTOCOL_VERSION,
                "sdk_version": runtime.sdk_version(),
            }
        )
        handlers: dict[str, Callable[[dict[str, Any]], None]] = {
            "environment": self._environment,
            "inventory": lambda _: self._document(runtime.inventory),
            "discover": lambda request: self._document(lambda: runtime.discover(request["name"])),
            "run": self._start,
            "next": lambda _: self._next(),
            "close": lambda _: self._close(),
        }
        while True:
            try:
                request = connection.recv_json()
            except ProtocolError:
                break
            handler = handlers.get(request["t"])
            if handler is None:
                break
            try:
                handler(request)
            except ProtocolError:
                break
            self._requests += 1
        self._close(reply=False)

    def _environment(self, request: dict[str, Any]) -> None:
        """Sets the provider's credentials before any provider code runs."""
        variables = request.get("variables")
        if self._requests or not isinstance(variables, dict):
            raise ProtocolError
        if len(variables) > _MAX_ENV_VARIABLES:
            raise ProtocolError
        for name, value in variables.items():
            if not (
                _may_set(name)
                and isinstance(value, str)
                and "\0" not in value
                and len(value.encode()) <= _MAX_ENV_VALUE
            ):
                raise ProtocolError
        os.environ.update(variables)
        self._connection.send_json({"t": "environment_set"})

    def _document(self, produce: Callable[[], str]) -> None:
        try:
            document = produce().encode()
        except Exception:
            self._connection.send_json({"t": "failed"})
            return
        self._connection.send(DOCUMENT, document)

    def _start(self, request: dict[str, Any]) -> None:
        self._close(reply=False)
        try:
            source = json.loads(request["source"])
            self._max_bytes = int(source["limits"]["max_bytes"])
            self._callbacks = _Callbacks(self._connection) if request["revisions"] else None
            view = (
                (self._callbacks.lookup_revisions, self._callbacks.next_revisions)
                if self._callbacks
                else None
            )
            self._run = runtime.run(request["source"], request["kind"], request["cursor"], view)
        except Exception:
            self._connection.send_json({"t": "failed"})
            return
        self._connection.send_json({"t": "started"})

    def _next(self) -> None:
        connection = self._connection
        if self._run is None:
            connection.send_json({"t": "failed"})
            return
        callbacks = self._callbacks
        if callbacks:
            callbacks.active = True
        try:
            header, rows = next(self._run)
            frame = None if rows is None else _rows_frame(rows, self._max_bytes)
        except StopIteration:
            self._run = None
            connection.send_json({"t": "done"})
            return
        except ProtocolError:
            raise
        except BaseException:
            self._run = None
            connection.send_json({"t": "failed"})
            return
        finally:
            if callbacks:
                callbacks.active = False
        if rows is None:
            connection.send(HEADER, header.encode())
        elif frame is None:
            connection.send_json({"t": "oversize"})
        else:
            connection.send(BATCH, header.encode())
            connection.send(ROWS, frame)

    def _close(self, reply: bool = True) -> None:
        run, self._run = self._run, None
        if run is not None:
            # Cleanup failure never replaces the worker's diagnostic.
            with contextlib.suppress(BaseException):
                run.close()
        if reply:
            self._connection.send_json({"t": "closed"})


def main(argv: list[str], protocol_fd: int) -> int:
    """Serves the worker on `protocol_fd`, the connected socket that the
    bootstrap moved off stdin and closes in every forked child; returns only
    on a refusal to start."""
    if len(argv) != 1:
        return 2
    try:
        sock = socket.socket(fileno=protocol_fd)
    except (OSError, ValueError):
        return 2
    connection = Connection(sock)
    os.register_at_fork(after_in_child=connection.abandon)
    with contextlib.suppress(ProtocolError):
        Host(connection).serve()
    # Providers may leave non-daemon threads behind; the worker has the
    # last word, so leave without waiting for them.
    os._exit(0)
