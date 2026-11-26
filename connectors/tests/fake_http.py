"""What the five fake provider servers share.

A fake world answers in two ways: mounted on a `requests` session
(`WorldAdapter`), or over HTTP for a connector in another process (`Serving`,
with a `JsonHandler`, or a `KeepAliveHandler`). `Holds` parks a request until
the test lets it go.
"""

from __future__ import annotations

import io
import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import requests
from requests.adapters import BaseAdapter

#: A parked request is answered after this long even if nobody releases it.
HOLD_SECONDS = 120.0
#: `POST /fake/<name>` with JSON arguments drives a world from the test.
CONTROL_PATH = "/fake/"

Answer = tuple[int, Any, "dict[str, str]"]


def json_response(
    request: requests.PreparedRequest, status: int, body: Any, headers: dict[str, str] | None = None
) -> requests.Response:
    """The response of a world: JSON, or raw bytes."""
    response = requests.Response()
    response.status_code = status
    response.headers.update(headers or {})
    response.url, response.request = request.url, request
    response.raw = io.BytesIO(body if isinstance(body, bytes) else json.dumps(body).encode())
    return response


class WorldAdapter(BaseAdapter):
    """A `requests` adapter that answers from `answer(request) -> (status, body, headers)`."""

    def __init__(self, answer: Callable[[requests.PreparedRequest], Answer]) -> None:
        super().__init__()
        self._answer = answer

    def send(self, request: requests.PreparedRequest, **_: Any) -> requests.Response:
        return json_response(request, *self._answer(request))

    def close(self) -> None:
        pass


class Serving:
    """`handler` served on a free local port from a thread, until `close`."""

    def __init__(self, handler: type[BaseHTTPRequestHandler], **attributes: Any) -> None:
        self._http = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._http.daemon_threads = True
        for name, value in attributes.items():
            setattr(self._http, name, value)
        self.endpoint = f"http://127.0.0.1:{self._http.server_port}"
        self._thread = threading.Thread(target=self._http.serve_forever, args=(0.02,), daemon=True)
        self._thread.start()

    def __enter__(self) -> Serving:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.shutdown()
        self._http.server_close()
        self._thread.join()


class Holds:
    """Requests parked until `release`: the provider changing, or the worker
    dying, in the middle of a sync."""

    def __init__(self) -> None:
        self._holds: list[dict[str, Any]] = []
        self._guard = threading.Lock()

    def hold(self, match: Callable[..., bool]) -> None:
        with self._guard:
            self._holds.append({"match": match, "released": threading.Event(), "parked": False})

    def release(self) -> None:
        with self._guard:
            holds, self._holds = self._holds, []
        for hold in holds:
            hold["released"].set()

    def parked(self) -> int:
        """Requests waiting for `release` right now."""
        with self._guard:
            return sum(hold["parked"] for hold in self._holds)

    def wait(self, *request: Any) -> None:
        """Park the request, if a hold matches it, until `release`."""
        with self._guard:
            hold = next((h for h in self._holds if not h["parked"] and h["match"](*request)), None)
            if hold is not None:
                hold["parked"] = True
        if hold is not None:
            hold["released"].wait(HOLD_SECONDS)


class JsonHandler(BaseHTTPRequestHandler):
    """A handler that logs nothing and answers JSON, or raw bytes. One request to a
    connection, as HTTP/1.0 has it: a body a handler leaves unread cannot be taken for
    the next request."""

    def log_message(self, *_: Any) -> None:
        pass

    def read_body(self) -> bytes:
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def send_json(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        kind = "application/octet-stream" if isinstance(body, bytes) else "application/json"
        try:
            self.send_response_only(status)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(data)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(data)
        except OSError:
            # The client is gone: a worker killed while its request was parked.
            self.close_connection = True


class KeepAliveHandler(JsonHandler):
    """A `JsonHandler` that keeps the connection for the next request."""

    protocol_version = "HTTP/1.1"
    # Headers and body are two writes; Nagle would hold the second for 40 ms.
    disable_nagle_algorithm = True
