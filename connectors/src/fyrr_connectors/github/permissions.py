"""Effective private-repository access, flattened to stable human user IDs."""

from __future__ import annotations

from dataclasses import dataclass

from ..connector import SourceUser
from ..validate import MAX_READERS
from .client import MAX_ENTRIES, Client, object_id
from .feeds import Repository


def container(repo_id: int) -> str:
    return f"github:repo:{repo_id}"


@dataclass(frozen=True)
class AccessSnapshot:
    readers: dict[int, tuple[str, ...]]
    users: tuple[SourceUser, ...]

    @classmethod
    def read(cls, client: Client, repos: dict[int, Repository]) -> AccessSnapshot:
        grants: dict[int, tuple[str, ...]] = {}
        humans: set[int] = set()
        count = 0
        for repo_id, repo in repos.items():
            if repo.visibility != "private":
                raise ValueError("github_sync_private_only")
            effective: dict[int, bool] = {}
            for entries in client.pages(repo.path + "/collaborators", {"affiliation": "all"}):
                for entry in entries:
                    count += 1
                    if count > MAX_ENTRIES:
                        raise ValueError("github_acl_snapshot_limit")
                    user_id = object_id(entry)
                    kind = entry.get("type")
                    permissions = entry.get("permissions")
                    if not isinstance(permissions, dict) or not isinstance(
                        permissions.get("pull"), bool
                    ):
                        raise ValueError("github_permissions_incomplete")
                    # A later contradictory entry wins. Never use login,
                    # profile e-mail, role name, invitations or a bot grant.
                    effective[user_id] = kind == "User" and permissions["pull"]
            repo.verify(client)
            if repo.owner_type == "User":
                effective[repo.owner_id] = True
            readers = sorted(user_id for user_id, allowed in effective.items() if allowed)
            if len(readers) > MAX_READERS:
                raise ValueError("github_readers_limit")
            humans.update(readers)
            grants[repo_id] = tuple(f"usr:github.com:{user_id}" for user_id in readers)
        for repo in repos.values():
            repo.verify(client)
        users = tuple(
            SourceUser(id=f"github.com:{user_id}", active=True) for user_id in sorted(humans)
        )
        return cls(grants, users)
