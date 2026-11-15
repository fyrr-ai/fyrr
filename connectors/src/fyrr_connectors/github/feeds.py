"""Repository identity, safe cursors and conversation discovery hints."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import Any

from ..validate import MAX_CURSOR_BYTES
from .client import Client, instant, number, object_id
from .config import RepositoryScope, Scope


class FeedKind(StrEnum):
    ISSUE = "issue"
    PR = "pr"


@dataclass(frozen=True)
class Repository:
    id: int
    name: str
    visibility: str
    owner_id: int
    owner_type: str
    archived: bool

    @property
    def path(self) -> str:
        return f"/repos/{self.name}"

    def verify(self, client: Client):
        if self.read(client, self.id) != self:
            raise ValueError("github_repository_changed_during_read")

    @classmethod
    def read(cls, client: Client, repo_id: int) -> Repository:
        body = client.get(f"/repositories/{repo_id}")
        name, owner = body.get("full_name"), body.get("owner")
        if (
            object_id(body) != repo_id
            or not isinstance(name, str)
            or len(name.encode()) > 256
            or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", name)
        ):
            raise ValueError("github_repository_invalid")
        if not isinstance(owner, dict) or owner.get("type") not in ("User", "Organization"):
            raise ValueError("github_repository_invalid")
        if any(part in (".", "..") for part in name.split("/")):
            raise ValueError("github_repository_invalid")
        visibility = body.get("visibility")
        if visibility not in ("private", "public", "internal") or not isinstance(
            body.get("archived"), bool
        ):
            raise ValueError("github_repository_invalid")
        return cls(repo_id, name, visibility, object_id(owner), owner["type"], body["archived"])


def inventory(client: Client) -> set[int]:
    found: set[int] = set()
    for entries in client.pages("/installation/repositories", key="repositories"):
        for entry in entries:
            repo_id = object_id(entry)
            if repo_id in found:
                raise ValueError("github_inventory_duplicate")
            found.add(repo_id)
    return found


@dataclass(frozen=True)
class ConversationRef:
    repo_id: int
    kind: FeedKind
    id: int
    number: int

    @property
    def root(self) -> str:
        return f"github:repo:{self.repo_id}:{self.kind}:{self.id}:{self.number}"

    @classmethod
    def decode(cls, root: str) -> ConversationRef | None:
        match = re.fullmatch(
            r"github:repo:([1-9][0-9]*):(issue|pr):([1-9][0-9]*):([1-9][0-9]*)", root
        )
        if not match:
            return None
        repo, kind, item, ordinal = match.groups()
        return cls(number(int(repo)), FeedKind(kind), number(int(item)), number(int(ordinal)))


@dataclass
class RepoCursor:
    since: str | None = None
    commit: str | None = None
    rotate: tuple[int, int] = (0, 0)
    refresh_step: int | None = None
    rotation_start: int | None = None


REFRESH_TARGET = 12 * 60 * 60


def refresh_budget(count: int, elapsed: int) -> int:
    """Complete within the whole polls available in 12h at the observed cadence."""
    polls = max(1, REFRESH_TARGET // max(1, elapsed))
    return (count + polls - 1) // polls


@dataclass
class Cursor:
    fingerprint: str
    repos: dict[int, RepoCursor] = field(default_factory=dict)
    observed: set[int] = field(default_factory=set)

    @classmethod
    def decode(cls, text: str | None, scope: Scope) -> Cursor:
        fingerprint = hashlib.sha256(
            json.dumps(
                [
                    [r.id, r.branch, r.issues, r.pull_requests, r.prose, r.code]
                    for r in scope.repositories
                ],
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        fresh = cls(fingerprint)
        if text is None:
            return fresh
        try:
            if len(text.encode()) > MAX_CURSOR_BYTES:
                raise ValueError
            body = json.loads(text)
            if (
                body["v"] != 1
                or not isinstance(body["repos"], dict)
                or not isinstance(body["observed"], list)
            ):
                raise ValueError
            observed = {number(value) for value in body["observed"]}
            if len(observed) > 64:
                raise ValueError
            fresh.observed = observed & {repo.id for repo in scope.repositories}
            if body["scope"] != fingerprint:
                return fresh
            for key, value in body["repos"].items():
                repo_id = number(int(key))
                if (
                    key != str(repo_id)
                    or not isinstance(value, dict)
                    or set(value) != {"since", "commit", "rotate", "refresh_step", "rotation_start"}
                ):
                    raise ValueError
                since, commit, rotate = value["since"], value["commit"], value["rotate"]
                if since is not None and instant(since) is None:
                    raise ValueError
                if commit is not None and (
                    not isinstance(commit, str)
                    or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", commit)
                ):
                    raise ValueError
                if (
                    not isinstance(rotate, list)
                    or len(rotate) != 2
                    or any(
                        not isinstance(value, int) or isinstance(value, bool) for value in rotate
                    )
                    or not 0 <= rotate[0] < 1 << 63
                    or rotate[1] not in (0, 1)
                ):
                    raise ValueError
                timestamps = [value[key] for key in ("refresh_step", "rotation_start")]
                if any(
                    stamp is not None
                    and (
                        not isinstance(stamp, int)
                        or isinstance(stamp, bool)
                        or not 0 <= stamp < 1 << 63
                    )
                    for stamp in timestamps
                ):
                    raise ValueError
                fresh.repos[repo_id] = RepoCursor(since, commit, tuple(rotate), *timestamps)
            if len(fresh.repos) > 64 or set(fresh.repos) - {r.id for r in scope.repositories}:
                raise ValueError
            return fresh
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ValueError("github_cursor_invalid") from None

    def encode(self) -> str:
        text = json.dumps(
            {
                "v": 1,
                "scope": self.fingerprint,
                "observed": sorted(self.observed),
                "repos": {
                    str(key): {
                        "since": r.since,
                        "commit": r.commit,
                        "rotate": r.rotate,
                        "refresh_step": r.refresh_step,
                        "rotation_start": r.rotation_start,
                    }
                    for key, r in sorted(self.repos.items())
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        if len(text.encode()) > MAX_CURSOR_BYTES:
            raise ValueError("github_cursor_limit")
        return text


def conversations(client: Client, repo: Repository, scope: RepositoryScope, since: str | None):
    bound = instant(since)
    overlap = (bound - timedelta(hours=1)).isoformat() if bound else None
    if scope.issues or scope.pull_requests:
        params: dict[str, Any] = {"state": "all", "sort": "updated", "direction": "asc"}
        if overlap:
            params["since"] = overlap
        for entries in client.updated(repo.path + "/issues", params):
            repo.verify(client)
            for entry in entries:
                if "pull_request" not in entry and scope.issues:
                    yield ConversationRef(
                        repo.id, FeedKind.ISSUE, object_id(entry), number(entry.get("number"))
                    )
                elif "pull_request" in entry and scope.pull_requests:
                    ordinal = number(entry.get("number"))
                    parent = client.get(repo.path + f"/pulls/{ordinal}")
                    repo.verify(client)
                    yield ConversationRef(repo.id, FeedKind.PR, object_id(parent), ordinal)


def comment_hints(client: Client, repo: Repository, scope: RepositoryScope, since: str | None):
    if since is None:
        return
    params = {
        "since": (instant(since) - timedelta(hours=1)).isoformat(),
        "sort": "updated",
        "direction": "asc",
    }
    feeds = ["/issues/comments"] if scope.issues or scope.pull_requests else []
    if scope.pull_requests:
        feeds.append("/pulls/comments")
    for endpoint in feeds:
        for entries in client.updated(repo.path + endpoint, params):
            repo.verify(client)
            for entry in entries:
                link = entry.get("issue_url") or entry.get("pull_request_url")
                prefix = client.origin + repo.path
                match = (
                    re.fullmatch(re.escape(prefix) + r"/(issues|pulls)/([1-9][0-9]*)", link)
                    if isinstance(link, str)
                    else None
                )
                if match is None:
                    raise ValueError("github_response_invalid")
                kind, ordinal = match.groups()
                parent = client.get(repo.path + f"/{kind}/{ordinal}")
                is_pr = kind == "pulls" or "pull_request" in parent
                if is_pr and scope.pull_requests:
                    if kind != "pulls":
                        parent = client.get(repo.path + f"/pulls/{ordinal}")
                    yield ConversationRef(
                        repo.id, FeedKind.PR, object_id(parent), number(int(ordinal))
                    )
                elif not is_pr and scope.issues:
                    yield ConversationRef(
                        repo.id, FeedKind.ISSUE, object_id(parent), number(int(ordinal))
                    )
