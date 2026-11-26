"""Eligible immutable branch files; truncated trees never imply completion."""

from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote

from .._http import MAX_ENTRIES
from .client import Client
from .config import RepositoryScope
from .feeds import Repository

PROSE = {".md", ".markdown", ".rst", ".txt", ".text"}
BASENAMES = {
    "readme",
    "license",
    "licence",
    "changelog",
    "changes",
    "authors",
    "contributing",
    "notice",
}
CODE = {
    ".py",
    ".rs",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".c",
    ".h",
    ".cpp",
    ".hpp",
    ".java",
    ".go",
    ".rb",
    ".sh",
    ".sql",
    ".css",
    ".toml",
    ".yaml",
    ".yml",
    ".json",
}


def sha(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", value):
        raise ValueError("github_sha_invalid")
    return value


def eligible(path: str, scope: RepositoryScope) -> bool:
    name = PurePosixPath(path)
    suffix = name.suffix.lower()
    return (scope.prose and (suffix in PROSE or name.name.lower() in BASENAMES)) or (
        scope.code and suffix in CODE
    )


@dataclass(frozen=True)
class FileRef:
    repo_id: int
    branch: str
    path: str
    blob: str
    commit: str
    size: int

    @property
    def root(self) -> str:
        branch = hashlib.sha256(self.branch.encode()).hexdigest()[:32]
        path = hashlib.sha256(self.path.encode()).hexdigest()
        return f"github:repo:{self.repo_id}:file:{branch}:{path}"


class Tree:
    def __init__(self, client: Client):
        self.client = client

    def head(self, repo: Repository, scope: RepositoryScope) -> str:
        body = self.client.get(repo.path + "/branches/" + quote(scope.branch, safe=""))
        if body.get("name") != scope.branch or not isinstance(body.get("commit"), dict):
            raise ValueError("github_branch_invalid")
        return sha(body["commit"].get("sha"))

    def files(self, repo: Repository, scope: RepositoryScope, commit: str):
        body = self.client.get(repo.path + "/git/commits/" + commit)
        tree_sha = sha(body.get("tree", {}).get("sha"))
        result = self.client.get(repo.path + "/git/trees/" + tree_sha, {"recursive": "1"})
        if not isinstance(result.get("truncated"), bool):
            raise ValueError("github_tree_invalid")
        levels: list[tuple[str, str, dict[str, Any] | None, frozenset[str]]] = [
            ("", tree_sha, result if not result["truncated"] else None, frozenset())
        ]
        seen: set[str] = set()
        count = 0
        while levels:
            prefix, node, cached, ancestors = levels.pop()
            if node in ancestors or len(ancestors) >= 100:
                raise ValueError("github_tree_cycle")
            current = (
                cached if cached is not None else self.client.get(repo.path + "/git/trees/" + node)
            )
            if current.get("truncated") is not False or not isinstance(current.get("tree"), list):
                raise ValueError("github_tree_incomplete")
            if sha(current.get("sha")) != node:
                raise ValueError("github_tree_invalid")
            for entry in current["tree"]:
                count += 1
                if count > MAX_ENTRIES or not isinstance(entry, dict):
                    raise ValueError("github_tree_limit")
                path, kind = entry.get("path"), entry.get("type")
                if (
                    not isinstance(path, str)
                    or not path
                    or len(path.encode()) > 4096
                    or "\0" in path
                    or any(part in ("", ".", "..") for part in path.split("/"))
                ):
                    raise ValueError("github_tree_invalid")
                path = prefix + path
                if len(path.encode()) > 4096:
                    raise ValueError("github_tree_invalid")
                if path in seen:
                    raise ValueError("github_tree_duplicate")
                seen.add(path)
                blob = sha(entry.get("sha"))
                modes = {
                    "tree": {"040000"},
                    "blob": {"100644", "100755", "120000"},
                    "commit": {"160000"},
                }
                if kind not in modes or entry.get("mode") not in modes[kind]:
                    raise ValueError("github_tree_invalid")
                if kind == "tree" and cached is None:
                    levels.append((path + "/", blob, None, ancestors | {node}))
                elif (
                    kind == "blob"
                    and entry.get("mode") in ("100644", "100755")
                    and eligible(path, scope)
                ):
                    size = entry.get("size")
                    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                        raise ValueError("github_tree_invalid")
                    yield FileRef(repo.id, scope.branch, path, blob, commit, size)
                elif kind not in ("tree", "blob", "commit"):
                    raise ValueError("github_tree_invalid")

    def blob(self, repo: Repository, file: FileRef) -> bytes:
        body = self.client.get(repo.path + "/git/blobs/" + file.blob)
        if (
            sha(body.get("sha")) != file.blob
            or body.get("encoding") != "base64"
            or not isinstance(body.get("content"), str)
        ):
            raise ValueError("github_blob_invalid")
        try:
            data = base64.b64decode(body["content"].replace("\n", ""), validate=True)
        except ValueError:
            raise ValueError("github_blob_invalid") from None
        if body.get("size") != len(data):
            raise ValueError("github_blob_invalid")
        return data
