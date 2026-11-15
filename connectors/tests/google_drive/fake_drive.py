"""A stateful, synthetic Google Drive for the connector's tests.

It keeps users' files, shared drives, permissions with their inheritance, a
Directory of users and groups and one change log per feed, and answers the
Drive v3 and Directory REST calls the connector makes through a `requests` transport
adapter, so the connector's real URL building, paging and parsing run. Pages
hold two entries, `fields` masks are honored (a missing `nextPageToken` in a
mask hides the token, as in Drive), and any request can be made to fail.
Every request is recorded. All names, addresses and IDs are invented.

`FakeServer` puts the same world behind HTTP for a connector in another
process (`fyrr writer sync`), with the delegated sign-in the tests otherwise
replace; `python fake_drive.py` runs it for the real-S3 tests.
"""

from __future__ import annotations

import io
import json
import re
import secrets
import sys
import threading
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth import exceptions, jwt
from requests.adapters import BaseAdapter

DOC = "application/vnd.google-apps.document"
SHEET = "application/vnd.google-apps.spreadsheet"
SLIDES = "application/vnd.google-apps.presentation"
FOLDER = "application/vnd.google-apps.folder"
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
ADMIN = "admin@example.com"
TOKEN_PATH = "/token"
#: google-auth addresses every grant to Google's token endpoint, whatever the key's token URI.
TOKEN_AUDIENCE = "https://oauth2.googleapis.com/token"
CONTROL_PATH = "/fake/"
#: A parked request is answered after this long even if nobody releases it.
HOLD_SECONDS = 120.0
_SCOPE = "https://www.googleapis.com/auth/"
#: World changes `POST /fake/<name>` passes straight on, arguments by name.
_WORLD_CALLS = frozenset({"user", "drive", "join", "leave", "share", "unshare", "touch", "remove"})


def _grant(**permission: Any) -> dict[str, Any]:
    """A Drive permission; like Drive, its ID is the same wherever the principal is granted."""
    principal = permission.get("emailAddress") or permission.get("domain") or permission["type"]
    return {"id": f"p-{principal}", "role": "reader"} | permission


class FakeDrive:
    def __init__(self, page_size: int = 2) -> None:
        self.page_size = page_size
        self.files: dict[str, dict[str, Any]] = {}
        self.exports: dict[str, bytes] = {}
        self.drives: dict[str, dict[str, Any]] = {}
        self.users: dict[str, dict[str, Any]] = {}
        self.groups: dict[str, list[dict[str, str]]] = {}
        self.logs: dict[str, list[str]] = {}
        self.requests: list[tuple[str, str, dict[str, str]]] = []
        self._faults: list[dict[str, Any]] = []
        self._after: list[tuple[Callable[[str, dict[str, str]], bool], Callable[[], None]]] = []
        self.sleeps: list[float] = []
        #: Cut the permissions a file resource carries inline (None: all of them).
        self.inline_permissions: int | None = None
        self._clock = 0
        self._lock = threading.Lock()

    # The world.

    def _tick(self) -> str:
        self._clock += 1
        return (EPOCH + timedelta(minutes=self._clock)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _feeds(self, file: dict[str, Any]) -> set[str]:
        """The change logs that report the file: its drive's, or those of its
        owner and of every user it is shared with."""
        if file["drive"]:
            return {file["drive"]}
        users = {g.get("emailAddress") for g in self._effective(file) if g["type"] == "user"}
        return {file["owner"], *users} - {None}

    def _log(self, file: dict[str, Any], also: set[str] = frozenset()) -> None:
        for feed in self._feeds(file) | also:
            self.logs.setdefault(feed, []).append(file["id"])

    def user(self, email: str, **flags: bool) -> None:
        self.users[email] = {"primaryEmail": email, **flags}

    def group(self, email: str, *members: tuple[str, str]) -> None:
        self.groups[email] = [{"email": member, "type": kind} for member, kind in members]

    def drive(self, drive_id: str, name: str, members: list[str]) -> None:
        self.drives[drive_id] = {
            "id": drive_id,
            "name": name,
            "members": set(members),
            "grants": [_grant(type="user", emailAddress=m, role="organizer") for m in members],
        }
        self.logs.setdefault(drive_id, [])

    def join(self, drive_id: str, role: str = "reader", **permission: Any) -> None:
        """Add a member to a shared drive. As in Drive, no file reports a change."""
        drive = self.drives[drive_id]
        drive["grants"].append(_grant(role=role, **permission))
        if permission["type"] == "user":
            drive["members"].add(permission["emailAddress"])

    def leave(self, drive_id: str, permission_id: str) -> None:
        drive = self.drives[drive_id]
        drive["grants"] = [grant for grant in drive["grants"] if grant["id"] != permission_id]
        drive["members"].discard(permission_id.removeprefix("p-"))

    def share(self, file_id: str, **permission: Any) -> None:
        """Grant on a file or folder. Only that item reports a change: the
        files beneath a folder inherit silently."""
        file = self.files[file_id]
        file["grants"].append(_grant(**permission))
        file["version"] += 1
        self._log(file)

    def unshare(self, file_id: str, permission_id: str) -> None:
        file = self.files[file_id]
        before = self._feeds(file)
        file["grants"] = [grant for grant in file["grants"] if grant["id"] != permission_id]
        file["version"] += 1
        self._log(file, before)

    def _effective(self, file: dict[str, Any]) -> list[dict[str, Any]]:
        """The file's permissions as Drive lists them: its own, those of its
        folders and, unless a folder limits access, its shared drive's."""
        found: dict[str, dict[str, Any]] = {}

        def add(grant: dict[str, Any], source: str | None) -> None:
            """`source`: the folder or drive the grant is inherited from, if any."""
            entry = found.setdefault(grant["id"], {**grant, "permissionDetails": []})
            detail = (
                {"inherited": True, "inheritedFrom": source} if source else {"inherited": False}
            )
            entry["permissionDetails"].append(detail)

        limited, item = False, file
        for inherited in (False, *[True] * 50):
            for grant in item["grants"]:
                add(grant, item["id"] if inherited else None)
            limited = limited or item.get("limited", False)
            item = self.files.get(item["parent"])
            if item is None:
                break
        for grant in self.drives[file["drive"]]["grants"] if file["drive"] else ():
            if not limited or grant["role"] == "organizer":
                add(grant, file["drive"])
        if not file["drive"]:
            for entry in found.values():
                del entry["permissionDetails"]
        return list(found.values())

    def put(
        self,
        file_id: str,
        content: bytes = b"",
        *,
        owner: str | None = None,
        drive: str | None = None,
        mime: str = DOC,
        name: str | None = None,
        parent: str | None = None,
        shared_with: tuple[str, ...] = (),
    ) -> None:
        """Create a file, or edit its content."""
        now = self._tick()
        file = self.files.get(file_id)
        if file is None:
            file = self.files[file_id] = {
                "id": file_id,
                "owner": owner,
                "drive": drive,
                "mime": mime,
                "name": name or file_id,
                "parent": parent or drive or f"root-{owner}",
                "grants": [
                    _grant(type="user", emailAddress=user, role=role)
                    for user, role in [(owner, "owner"), *[(u, "reader") for u in shared_with]]
                    if user
                ],
                "trashed": False,
                "version": 0,
                "created": now,
            }
        file["modified"] = now
        file["version"] += 1
        self.exports[file_id] = content
        self._log(file)

    def touch(self, file_id: str, **changes: Any) -> None:
        """A change that leaves the content alone: a move, a rename, trash."""
        file = self.files[file_id]
        file.update(changes)
        file["version"] += 1
        self._log(file)

    def move(self, file_id: str, **changes: Any) -> None:
        """Move a file to another owner, drive or folder: the feeds it leaves
        and the feeds it enters report it."""
        file = self.files[file_id]
        before = self._feeds(file)
        file.update(changes)
        file["version"] += 1
        self._log(file, before)

    def remove(self, file_id: str) -> None:
        """Delete for good: the feeds get a bare `removed` entry."""
        file = self.files.pop(file_id)
        self._log(file)

    def fail(
        self,
        match: Callable[[str, dict[str, str]], bool],
        status: int,
        reason: str = "",
        location: str = "",
        times: int = 1,
        retry_after: str | None = None,
    ) -> None:
        self._faults.append(
            {
                "match": match,
                "n": times,
                "response": (status, _error(reason, location), retry_after),
            }
        )

    def after(
        self, match: Callable[[str, dict[str, str]], bool], change: Callable[[], None]
    ) -> None:
        """Change the world once, right after the first matching request was
        answered: what happens in Drive while a sync is running."""
        self._after.append((match, change))

    def paths(self) -> list[str]:
        return [path for _, path, _ in self.requests]

    # The API.

    def session(self, subject: str) -> requests.Session:
        session = requests.Session()
        adapter = _Adapter(self, subject)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _visible(self, subject: str, file: dict[str, Any]) -> bool:
        if file["drive"]:
            return subject in self.drives[file["drive"]]["members"]
        return subject in self._feeds(file)

    def _resource(self, subject: str, file: dict[str, Any], fields: str) -> dict[str, Any]:
        resource: dict[str, Any] = {
            "id": file["id"],
            "name": file["name"],
            "mimeType": file["mime"],
            "trashed": file["trashed"],
            "parents": [file["parent"]],
            "version": str(file["version"]),
            "createdTime": file["created"],
            "modifiedTime": file["modified"],
            "ownedByMe": subject == file["owner"],
            "lastModifyingUser": {"emailAddress": "editor@example.com"},
            "inheritedPermissionsDisabled": file.get("limited", False),
        }
        effective = self._effective(file)
        resource["permissionIds"] = [permission["id"] for permission in effective]
        if file["drive"]:
            resource["driveId"] = file["drive"]
        else:
            resource["owners"] = [{"emailAddress": file["owner"]}]
            if subject == file["owner"]:  # Only who can share sees the list.
                resource["permissions"] = effective[: self.inline_permissions]
        return {key: value for key, value in resource.items() if re.search(rf"\b{key}\b", fields)}

    def _matches(self, subject: str, file: dict[str, Any], params: dict[str, str]) -> bool:
        query = params.get("q", "")
        if params.get("corpora") == "drive":
            if file["drive"] != params["driveId"]:
                return False
        elif file["drive"] or not self._visible(subject, file):
            return False
        if "trashed = false" in query and file["trashed"]:
            return False
        if "not 'me' in owners" in query:
            if file["owner"] == subject:
                return False
        elif "'me' in owners" in query and file["owner"] != subject:
            return False
        kinds = re.findall(r"mimeType = '([^']+)'", query)
        if kinds and file["mime"] not in kinds:
            return False
        parents = [p for p in re.findall(r"'([^']+)' in parents", query)]
        return not parents or file["parent"] in parents

    def _page(self, items: list[Any], params: dict[str, str]) -> tuple[list[Any], str | None]:
        start = int(params.get("pageToken", "0"))
        size = min(self.page_size, int(params.get("pageSize", self.page_size)))
        following = str(start + size) if start + size < len(items) else None
        return items[start : start + size], following

    def handle(
        self, subject: str, path: str, params: dict[str, str]
    ) -> tuple[int, Any, str | None]:
        with self._lock:
            self.requests.append((subject, path, params))
            for fault in self._faults:
                if fault["n"] and fault["match"](path, params):
                    fault["n"] -= 1
                    return fault["response"]
            answer = self._answer(subject, path, params)
            for hook in [hook for hook in self._after if hook[0](path, params)]:
                self._after.remove(hook)
                hook[1]()
            return (*answer, None)

    def _answer(self, subject: str, path: str, params: dict[str, str]) -> tuple[int, Any]:
        fields = params.get("fields", "")
        masked = "fields" in params and "nextPageToken" not in fields
        if path.startswith("/admin/directory/v1/"):
            path = path.removeprefix("/admin/directory/v1/")
            if subject != ADMIN:
                return 403, _error("forbidden")
            if path == "users":
                key, items = "users", list(self.users.values())
            elif path == "groups":
                key, items = "groups", [{"email": email} for email in self.groups]
            elif path.endswith("/members") and path.split("/")[1] in self.groups:
                key, items = "members", self.groups[path.split("/")[1]]
            else:
                return 404, _error("notFound")
            page, following = self._page(items, params)
            body: dict[str, Any] = {key: page}
        elif (path := path.removeprefix("/drive/v3/")) == "drives":
            mine = [
                {"id": drive["id"], "name": drive["name"]}
                for drive in self.drives.values()
                if subject in drive["members"]
            ]
            page, following = self._page(mine, params)
            body = {"drives": page}
        elif path == "files":
            found = [f for f in self.files.values() if self._matches(subject, f, params)]
            page, following = self._page(found, params)
            body = {"files": [self._resource(subject, file, fields) for file in page]}
        elif path == "changes/startPageToken":
            return 200, {"startPageToken": str(len(self.logs.get(self._feed(subject, params), [])))}
        elif path == "changes":
            log = self.logs.get(self._feed(subject, params), [])
            if not params["pageToken"].isdigit() or int(params["pageToken"]) > len(log):
                return 400, _error("invalid", "pageToken")
            page, following = self._page(log, params)
            body = {"changes": [self._change(subject, file_id, fields) for file_id in page]}
            if following is None:
                body["newStartPageToken"] = str(len(log))
        elif path.endswith("/permissions"):
            item = path.split("/")[1]
            file = self.files.get(item)
            if item in self.drives:
                admin = subject == ADMIN and params.get("useDomainAdminAccess") == "true"
                if not admin and subject not in self.drives[item]["members"]:
                    return 404, _error("notFound")
                listed = [
                    {**grant, "permissionDetails": [{"inherited": False}]}
                    for grant in self.drives[item]["grants"]
                ]
            elif file is not None and self._visible(subject, file):
                listed = self._effective(file)
            else:
                return 404, _error("notFound")
            page, following = self._page(listed, params)
            body = {"permissions": page}
        elif path.endswith("/export"):
            file = self.files.get(path.split("/")[1])
            if file is None or not self._visible(subject, file):
                return 404, _error("notFound")
            return 200, self.exports[file["id"]]
        elif path.startswith("files/"):
            file = self.files.get(path.split("/")[1])
            if file is None or not self._visible(subject, file):
                return 404, _error("notFound")
            return 200, self._resource(subject, file, fields)
        else:
            return 404, _error("notFound")
        if following is not None and not masked:
            body["nextPageToken"] = following
        return 200, body

    def _feed(self, subject: str, params: dict[str, str]) -> str:
        return params.get("driveId", subject)

    def _change(self, subject: str, file_id: str, fields: str) -> dict[str, Any]:
        file = self.files.get(file_id)
        if file is None or not self._visible(subject, file):
            return {"changeType": "file", "fileId": file_id, "removed": True}
        return {
            "changeType": "file",
            "fileId": file_id,
            "removed": False,
            "file": self._resource(subject, file, fields),
        }


def _error(reason: str, location: str = "") -> dict[str, Any]:
    return {"error": {"errors": [{"reason": reason, "location": location}]}}


class _Adapter(BaseAdapter):
    def __init__(self, world: FakeDrive, subject: str) -> None:
        super().__init__()
        self._world = world
        self._subject = subject

    def send(self, request: requests.PreparedRequest, **_: Any) -> requests.Response:
        url = urlsplit(request.url)
        params = {key: values[0] for key, values in parse_qs(url.query).items()}
        status, body, retry_after = self._world.handle(self._subject, url.path, params)
        response = requests.Response()
        response.status_code = status
        if retry_after is not None:
            response.headers["Retry-After"] = retry_after
        response.url = request.url
        response.request = request
        response.raw = io.BytesIO(body if isinstance(body, bytes) else json.dumps(body).encode())
        return response

    def close(self) -> None:
        pass


def matcher(spec: Mapping[str, Any]) -> Callable[[str, dict[str, str]], bool]:
    """A request predicate from JSON: a `path` pattern and `params` patterns,
    each searched in the request; a parameter the request lacks never matches."""
    path = re.compile(spec.get("path", ""))
    wanted = {name: re.compile(pattern) for name, pattern in spec.get("params", {}).items()}

    def match(request_path: str, params: dict[str, str]) -> bool:
        return bool(path.search(request_path)) and all(
            name in params and pattern.search(params[name]) for name, pattern in wanted.items()
        )

    return match


class FakeServer:
    """`world` over HTTP, for a connector in another process.

    `settings.api_endpoint` is `endpoint` and the service-account key is
    `service_account`: a key made for this server whose token URI is this
    server, so the connector's real delegated sign-in runs. It signs a grant
    for the user it impersonates; the server checks the signature, that the
    user is an active one and the scope of every later call, and answers as
    the user of the access token.

    `POST /fake/<name>` with JSON arguments drives the world from the test:
    the changes of `FakeDrive` (`user`, `group`, `drive`, `join`, `leave`,
    `put` with text content, `share`, `unshare`, `touch`, `remove`), `fail`
    with a `match` as for `matcher`, `hold` (park the first matching request
    until `release`: Drive changing, or the worker dying, in the middle of a
    sync), `parked` and `requests` (the log entries a `match` selects).
    """

    def __init__(self, world: FakeDrive) -> None:
        self.world = world
        self._http = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._http.daemon_threads = True
        self._http.fake = self  # type: ignore[attr-defined]
        self.endpoint = f"http://127.0.0.1:{self._http.server_address[1]}"
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self._public_key = key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        self.service_account = json.dumps(
            {
                "type": "service_account",
                "client_email": "fyrr-test@fake.iam.gserviceaccount.com",
                "private_key_id": "fake",
                "private_key": key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                ).decode(),
                "token_uri": self.endpoint + TOKEN_PATH,
            }
        )
        self._tokens: dict[str, tuple[str, frozenset[str]]] = {}
        self._holds: list[dict[str, Any]] = []
        self._guard = threading.Lock()
        self._thread = threading.Thread(target=self._http.serve_forever, args=(0.02,), daemon=True)
        self._thread.start()

    def __enter__(self) -> FakeServer:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self.release()
        self._http.shutdown()
        self._http.server_close()
        self._thread.join()

    def hold(self, match: Callable[[str, dict[str, str]], bool]) -> None:
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

    def token(self, form: dict[str, str]) -> tuple[int, Any]:
        """The OAuth JWT-bearer grant: an access token for the `sub` of a
        grant this server's key signed, if that user can sign in."""
        try:
            claims = jwt.decode(
                form.get("assertion", ""),
                certs=self._public_key,
                audience=TOKEN_AUDIENCE,
            )
        except (ValueError, exceptions.GoogleAuthError):
            return 400, {"error": "invalid_grant"}
        subject = claims.get("sub", "")
        user = self.world.users.get(subject)
        if subject != ADMIN and (user is None or user.get("suspended") or user.get("archived")):
            return 400, {"error": "invalid_grant"}
        token = secrets.token_urlsafe(16)
        self._tokens[token] = (subject, frozenset(claims.get("scope", "").split()))
        return 200, {"access_token": token, "token_type": "Bearer", "expires_in": 3600}

    def api(
        self, authorization: str, path: str, params: dict[str, str]
    ) -> tuple[int, Any, str | None]:
        subject, scopes = self._tokens.get(authorization.removeprefix("Bearer "), ("", frozenset()))
        if not subject:
            return 401, _error("authError"), None
        if path.startswith("/admin/directory/v1/"):
            kind = "user" if path.endswith("/users") else "group"
            scope = f"admin.directory.{kind}.readonly"
        else:
            scope = "drive.readonly"
        if _SCOPE + scope not in scopes:
            return 403, _error("insufficientPermissions"), None
        with self._guard:
            hold = next(
                (h for h in self._holds if not h["parked"] and h["match"](path, params)), None
            )
            if hold is not None:
                hold["parked"] = True
        if hold is not None:
            hold["released"].wait(HOLD_SECONDS)
        return self.world.handle(subject, path, params)

    def control(self, name: str, arguments: dict[str, Any]) -> Any:
        world = self.world
        if name == "hold":
            return self.hold(matcher(arguments["match"]))
        if name == "release":
            return self.release()
        if name == "parked":
            return self.parked()
        # The lock of `handle`: a change never lands in the middle of an answer.
        with world._lock:
            if name == "requests":
                match = matcher(arguments.get("match", {}))
                return [
                    [subject, path]
                    for subject, path, params in world.requests
                    if match(path, params)
                ]
            if name == "fail":
                return world.fail(matcher(arguments.pop("match")), **arguments)
            if name == "put":
                content = arguments.pop("content", "").encode()
                return world.put(arguments.pop("file_id"), content, **arguments)
            if name == "group":
                members = [tuple(member) for member in arguments.get("members", ())]
                return world.group(arguments["email"], *members)
            if name in _WORLD_CALLS:
                return getattr(world, name)(**arguments)
        raise KeyError(name)


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    # Headers and body are two writes; Nagle would hold the second for 40 ms.
    disable_nagle_algorithm = True

    def log_message(self, *_: Any) -> None:
        pass

    def _reply(self, status: int, body: Any, retry_after: str | None = None) -> None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        kind = "application/octet-stream" if isinstance(body, bytes) else "application/json"
        try:
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(data)))
            if retry_after is not None:
                self.send_header("Retry-After", retry_after)
            self.end_headers()
            self.wfile.write(data)
        except OSError:
            # The client is gone: a worker killed while its request was parked.
            self.close_connection = True

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        params = {key: values[0] for key, values in parse_qs(url.query).items()}
        fake: FakeServer = self.server.fake  # type: ignore[attr-defined]
        self._reply(*fake.api(self.headers.get("Authorization", ""), url.path, params))

    def do_POST(self) -> None:
        fake: FakeServer = self.server.fake  # type: ignore[attr-defined]
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.path == TOKEN_PATH:
            form = {key: values[0] for key, values in parse_qs(body.decode()).items()}
            return self._reply(*fake.token(form))
        try:
            if not self.path.startswith(CONTROL_PATH):
                raise KeyError(self.path)
            result = fake.control(self.path.removeprefix(CONTROL_PATH), json.loads(body or b"{}"))
        except (KeyError, TypeError, ValueError) as error:
            # A wrong call in a test must fail that test, not be skipped.
            return self._reply(400, {"error": type(error).__name__, "detail": str(error)})
        self._reply(200, {"result": result})


def main() -> None:
    """Serve an empty world until standard input closes. The first output
    line is how to reach it: the endpoint and the service-account key."""
    with FakeServer(FakeDrive()) as server:
        connection = {"endpoint": server.endpoint, "service_account": server.service_account}
        print(json.dumps(connection), flush=True)
        sys.stdin.read()


if __name__ == "__main__":
    main()
