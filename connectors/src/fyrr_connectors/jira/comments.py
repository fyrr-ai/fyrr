"""Canonical parent-checked comments and visibility descriptors."""

import hashlib
import json
from dataclasses import dataclass
from urllib.parse import urlsplit

from .client import number


@dataclass(frozen=True)
class Comment:
    id: int
    body: dict

    @classmethod
    def read(cls, client, issue, skip):
        result = {}
        for body in client.snapshot(f"/issue/{issue}/comment", "comments", order="created"):
            if not isinstance(body, dict):
                raise ValueError("jira_comment_invalid")
            identity = number(body.get("id"))
            if not isinstance(body.get("self"), str) or not urlsplit(body["self"]).path.endswith(
                f"/issue/{issue}/comment/{identity}"
            ):
                raise ValueError("jira_comment_parent_mismatch")
            if body.get("author", {}).get("accountId") not in skip:
                result[identity] = cls(identity, body)
        return tuple(result[key] for key in sorted(result))

    @property
    def restricted(self):
        return self.body.get("visibility") is not None


def visibility_signature(comments):
    """Bind each restricted row, including unknown audiences, without per-row groups."""
    return hashlib.sha256(
        json.dumps(
            [(c.id, c.body["visibility"]) for c in comments if c.restricted],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
