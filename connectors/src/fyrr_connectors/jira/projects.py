"""Stable numeric project identity and authoritative per-container scope loss."""

from dataclasses import dataclass

from .client import number


@dataclass(frozen=True)
class Project:
    id: int
    key: str
    name: str
    archived: bool
    kind: str
    simplified: bool

    @classmethod
    def read(cls, client, identity):
        body = client.call(f"/project/{identity}")
        if not isinstance(body, dict) or number(body.get("id")) != identity:
            raise ValueError("jira_project_identity_mismatch")
        if (
            not all(
                isinstance(body.get(key), str) and body[key] and len(body[key].encode()) <= 256
                for key in ("key", "name", "projectTypeKey")
            )
            or not isinstance(body.get("archived", False), bool)
            or not isinstance(body.get("simplified", False), bool)
        ):
            raise ValueError("jira_project_invalid")
        return cls(
            identity,
            body["key"],
            body["name"],
            body.get("archived", False),
            body["projectTypeKey"],
            body.get("simplified", False),
        )
