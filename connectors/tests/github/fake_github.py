"""One synthetic stateful GitHub, shared by unit and worker HTTP paths."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlencode, urlsplit

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from requests.adapters import BaseAdapter


class FakeGitHub:
    def __init__(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.key = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode()
        self.public_key = key.public_key()
        self.app_id, self.installation_id, self.account_id = 10, 20, 30
        self.token = "synthetic-installation-token"
        self.origin = "https://api.github.com"
        self.page_size = 2
        self.repos: dict[int, dict] = {}
        self.topics: dict[tuple[int, str, int], dict] = {}
        self.comments: dict[tuple[int, str, int], list[dict]] = {}
        self.reviews: dict[tuple[int, str, int], list[dict]] = {}
        self.inline: dict[tuple[int, str, int], list[dict]] = {}
        self.heads: dict[tuple[int, str], str] = {}
        self.commits: dict[str, dict[str, str]] = {}
        self.blobs: dict[str, bytes] = {}
        self.acls: dict[int, list[dict]] = {}
        self.requests: list[tuple[str, str, dict]] = []
        self.sleeps: list[float] = []
        self._tick = 0
        self._hooks: list[tuple[Callable, Callable]] = []
        self._faults: list[dict] = []
        self._lock = threading.RLock()
        self.repo(1)
        self.branch(1, {})

    def stamp(self):
        self._tick += 1
        return (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=self._tick)).isoformat()

    def server_time(self):
        """The time of the `Date` header every response carries right now."""
        return (
            datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=self._tick, seconds=30)
        ).isoformat()

    def repo(
        self,
        repo_id: int,
        *,
        name: str | None = None,
        visibility="private",
        installed=True,
        owner_type="Organization",
        archived=False,
    ):
        self.repos[repo_id] = {
            "id": repo_id,
            "full_name": name or f"synthetic/repo-{repo_id}",
            "visibility": visibility,
            "owner": {"id": self.account_id, "type": owner_type},
            "archived": archived,
            "installed": installed,
        }
        self.acls.setdefault(repo_id, [])

    def topic(self, repo_id=1, kind="issue", number=1, body="Body.", **fields):
        key = (repo_id, kind, number)
        entry = {
            "id": (1000 if kind == "pr" else 100) + number,
            "number": number,
            "title": f"Synthetic {kind} {number}",
            "body": body,
            "state": "open",
            "labels": [],
            "created_at": self.topics.get(key, {}).get("created_at") or self.stamp(),
            "updated_at": self.stamp(),
            "user": {"id": 7, "login": "synthetic-author"},
            **fields,
        }
        self.topics[key] = entry
        self.comments.setdefault(key, [])
        self.reviews.setdefault(key, [])
        self.inline.setdefault(key, [])
        return entry

    def comment(
        self, repo_id=1, kind="issue", number=1, body="Comment.", *, review_id=None, state=None
    ):
        key = (repo_id, kind, number)
        target = (
            self.reviews[key]
            if state is not None
            else self.inline[key]
            if review_id is not None
            else self.comments[key]
        )
        entry = {
            "id": len(target) + 1,
            "body": body,
            "created_at": self.stamp(),
            "updated_at": self.stamp(),
        }
        if state is not None:
            entry.update(state=state, submitted_at=None if state == "PENDING" else self.stamp())
        if review_id is not None:
            entry.update(pull_request_review_id=review_id, path="sample.py", line=3)
        target.append(entry)
        return entry

    def branch(self, repo_id: int, files: dict[str, str | bytes], branch="main"):
        self._tick += 1
        paths = {}
        for path, content in files.items():
            data = content.encode() if isinstance(content, str) else content
            blob = hashlib.sha1(data).hexdigest()
            self.blobs[blob] = data
            paths[path] = blob
        commit = hashlib.sha1(
            json.dumps([repo_id, self._tick, paths], sort_keys=True).encode()
        ).hexdigest()
        self.commits[commit] = paths
        self.heads[repo_id, branch] = commit
        return commit

    def collaborator(
        self, repo_id: int, user_id: int, *, read=True, kind="User", login=None, **fields
    ):
        entry = {
            "id": user_id,
            "login": login or f"synthetic-{user_id}",
            "type": kind,
            "permissions": {"pull": read},
            **fields,
        }
        self.acls[repo_id].append(entry)
        return entry

    def after(self, match: Callable, change: Callable):
        self._hooks.append((match, change))

    def fail(self, path: str, status: int, *, times=1, headers=None):
        self._faults.append(
            {"path": path, "status": status, "times": times, "headers": headers or {}}
        )

    def attach(self, session):
        session.mount(self.origin, _Adapter(self))
        return session

    def _topic(self, repo_id, kind, ordinal):
        key = repo_id, kind, ordinal
        entry = self.topics.get(key)
        if entry is None:
            return None
        result = dict(entry)
        name = self.repos[repo_id]["full_name"]
        result["repository_url"] = self.origin + f"/repos/{name}"
        result["html_url"] = (
            f"https://github.com/{name}/{'pull' if kind == 'pr' else 'issues'}/{ordinal}"
        )
        if kind == "pr":
            result["base"] = {"repo": {"id": repo_id}}
        return result

    def _parts(self, repo_id, kind, ordinal, entries, tag):
        name = self.repos[repo_id]["full_name"]
        url = self.origin + f"/repos/{name}/{'issues' if tag == 'comment' else 'pulls'}/{ordinal}"
        return [
            {
                **entry,
                "issue_url" if tag == "comment" else "pull_request_url": url,
                "html_url": (
                    f"https://github.com/{name}/{'pull' if kind == 'pr' else 'issues'}/"
                    f"{ordinal}#{tag}-{entry['id']}"
                ),
            }
            for entry in entries
        ]

    def _page(self, entries, path, params):
        page = int(params.get("page", 1))
        start = (page - 1) * self.page_size
        result = entries[start : start + self.page_size]
        headers = {}
        if start + self.page_size < len(entries):
            next_url = self.origin + path + "?" + urlencode({**params, "page": page + 1})
            headers["Link"] = f'<{next_url}>; rel="next"'
        return result, headers

    def handle(self, method, path, params, headers):
        with self._lock:
            auth = headers.get("Authorization", "")
            if path.startswith("/app/"):
                try:
                    import jwt

                    jwt.decode(
                        auth.removeprefix("Bearer "),
                        self.public_key,
                        algorithms=["RS256"],
                        issuer=str(self.app_id),
                    )
                except Exception:
                    return 401, {"message": "synthetic-auth-error"}, {}
            elif auth != "Bearer " + self.token:
                return 401, {"message": "synthetic-auth-error"}, {}
            self.requests.append((method, path, dict(params)))
            status, body, answer_headers = 200, None, {}
            fault = next((f for f in self._faults if f["times"] and f["path"] == path), None)
            if fault:
                fault["times"] -= 1
                status, body, answer_headers = (
                    fault["status"],
                    {"message": "synthetic-provider-payload-must-not-leak"},
                    fault["headers"],
                )
            else:
                status, body, answer_headers = self._answer(method, path, params)
            now = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=self._tick, seconds=30)
            answer_headers = {"Date": format_datetime(now, usegmt=True), **answer_headers}
            # Freeze the response before the late mutation.
            body = json.loads(json.dumps(body))
            hook = next((hook for hook in self._hooks if hook[0](method, path, params)), None)
            if hook:
                self._hooks.remove(hook)
                hook[1]()
            return status, body, answer_headers

    def _answer(self, method, path, params):
        if method == "GET" and path == f"/app/installations/{self.installation_id}":
            return (
                200,
                {
                    "id": self.installation_id,
                    "app_id": self.app_id,
                    "account": {"id": self.account_id},
                    "suspended_at": None,
                },
                {},
            )
        if method == "POST" and path == f"/app/installations/{self.installation_id}/access_tokens":
            return (
                201,
                {
                    "token": self.token,
                    "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "permissions": params["permissions"],
                },
                {},
            )
        if path == "/installation/repositories":
            installed = [repo for repo in self.repos.values() if repo["installed"]]
            entries, paging = self._page(installed, path, params)
            return 200, {"total_count": len(installed), "repositories": entries}, paging
        if path.startswith("/repositories/"):
            repo = self.repos.get(int(path.split("/")[-1]))
            return (200, repo, {}) if repo and repo["installed"] else (404, {}, {})
        segments = path.split("/")
        if len(segments) < 5 or segments[1] != "repos":
            return 404, {}, {}
        name = "/".join(segments[2:4])
        repo_id = next(
            (
                key
                for key, repo in self.repos.items()
                if repo["full_name"] == name and repo["installed"]
            ),
            None,
        )
        if repo_id is None:
            return 404, {}, {}
        tail = segments[4:]
        route = "/".join(tail)
        if route == "collaborators":
            entries, paging = self._page(self.acls[repo_id], path, params)
            return 200, entries, paging
        if route in ("issues", "pulls"):
            entries = []
            for owner, kind, ordinal in self.topics:
                if owner != repo_id or (route == "pulls" and kind != "pr"):
                    continue
                topic = self._topic(owner, kind, ordinal)
                if route == "issues" and kind == "pr":
                    topic["id"] = 2000 + ordinal
                    topic["pull_request"] = {}
                if params.get("since") and topic["updated_at"] < params["since"]:
                    continue
                entries.append(topic)
            entries.sort(
                key=lambda entry: (entry["updated_at"], entry["id"]),
                reverse=params.get("direction") == "desc",
            )
            entries, paging = self._page(entries, path, params)
            return 200, entries, paging
        if route in ("issues/comments", "pulls/comments"):
            entries = []
            for key, parts in (
                self.comments if route.startswith("issues") else self.inline
            ).items():
                if key[0] == repo_id:
                    entries.extend(
                        self._parts(
                            *key, parts, "comment" if route.startswith("issues") else "inline"
                        )
                    )
            if params.get("since"):
                entries = [entry for entry in entries if entry["updated_at"] >= params["since"]]
            if params.get("sort") == "updated":
                entries.sort(key=lambda entry: (entry["updated_at"], entry["id"]))
            entries, paging = self._page(entries, path, params)
            return 200, entries, paging
        if tail[0] in ("issues", "pulls") and len(tail) >= 2 and tail[1].isdigit():
            ordinal = int(tail[1])
            kind = "pr" if (repo_id, "pr", ordinal) in self.topics else "issue"
            key = repo_id, kind, ordinal
            if len(tail) == 2:
                topic = self._topic(*key)
                if topic and kind == "pr" and tail[0] == "issues":
                    topic["pull_request"] = {}
                    topic["id"] = 2000 + ordinal
                return (200, topic, {}) if topic else (404, {}, {})
            collection = (
                self.reviews
                if tail[2] == "reviews"
                else self.inline
                if tail[0] == "pulls"
                else self.comments
            )
            entries = self._parts(
                *key,
                collection.get(key, []),
                "review" if tail[2] == "reviews" else "inline" if tail[0] == "pulls" else "comment",
            )
            entries, paging = self._page(entries, path, params)
            return 200, entries, paging
        if tail[0] == "branches" and len(tail) == 2:
            branch = unquote(tail[1])
            commit = self.heads.get((repo_id, branch))
            return (
                (200, {"name": branch, "commit": {"sha": commit}}, {}) if commit else (404, {}, {})
            )
        if tail[0] == "commits" and len(tail) == 2:
            commit = self.heads.get((repo_id, tail[1]), tail[1])
            return (200, {"sha": commit}, {}) if commit in self.commits else (404, {}, {})
        if route == "commits":
            return (
                200,
                [
                    {
                        "sha": params["sha"],
                        "commit": {
                            "author": {"name": "Synthetic Author"},
                            "committer": {"date": "2026-01-01T00:00:00Z"},
                        },
                    }
                ],
                {},
            )
        if tail[:2] == ["git", "commits"]:
            return 200, {"tree": {"sha": tail[2]}}, {}
        if tail[:2] == ["git", "trees"]:
            commit = tail[2]
            tree = [
                {
                    "path": path,
                    "sha": blob,
                    "type": "blob",
                    "mode": "100644",
                    "size": len(self.blobs[blob]),
                }
                for path, blob in self.commits[commit].items()
            ]
            return 200, {"sha": commit, "tree": tree, "truncated": False}, {}
        if tail[:2] == ["git", "blobs"]:
            blob = tail[2]
            data = self.blobs[blob]
            return (
                200,
                {
                    "sha": blob,
                    "encoding": "base64",
                    "size": len(data),
                    "content": base64.b64encode(data).decode(),
                },
                {},
            )
        return 404, {}, {}


class _Adapter(BaseAdapter):
    def __init__(self, world):
        self.world = world
        super().__init__()

    def send(self, request, **kwargs):
        url = urlsplit(request.url)
        params = {key: values[0] for key, values in parse_qs(url.query).items()}
        if request.body:
            params = json.loads(request.body)
        status, body, headers = self.world.handle(request.method, url.path, params, request.headers)
        response = requests.Response()
        response.status_code = status
        response.headers.update(headers)
        response.url, response.request = request.url, request
        response.raw = io.BytesIO(json.dumps(body).encode())
        return response

    def close(self):
        pass


class FakeServer:
    def __init__(self, world):
        self.world = world
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.world = world
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self):
        self.world.origin = self.endpoint
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        url = urlsplit(self.path)
        params = {key: values[0] for key, values in parse_qs(url.query).items()}
        self.reply(*self.server.world.handle("GET", url.path, params, self.headers))

    def do_POST(self):
        params = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path.startswith("/fake/"):
            world = self.server.world
            operation = self.path.removeprefix("/fake/")
            with world._lock:
                if operation in ("repo", "topic", "comment", "branch", "collaborator", "fail"):
                    result = getattr(world, operation)(**params)
                elif operation == "clear_comments":
                    world.comments[params["repo_id"], params["kind"], params["number"]].clear()
                    result = None
                elif operation == "clear_readers":
                    world.acls[params["repo_id"]].clear()
                    result = None
                elif operation == "remove_topic":
                    key = params["repo_id"], params["kind"], params["number"]
                    for collection in (world.topics, world.comments, world.reviews, world.inline):
                        collection.pop(key, None)
                    result = None
                elif operation == "requests":
                    result = [path for _, path, _ in world.requests]
                else:
                    self.reply(404, {}, {})
                    return
            self.reply(200, {"result": result}, {})
            return
        self.reply(*self.server.world.handle("POST", self.path, params, self.headers))

    def reply(self, status, body, headers):
        data = json.dumps(body).encode()
        self.send_response_only(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    import sys

    world = FakeGitHub()
    with FakeServer(world) as server:
        # Only synthetic, generated fixture credentials; consumed through a pipe.
        print(json.dumps({"endpoint": server.endpoint, "private_key": world.key}), flush=True)
        sys.stdin.read()
