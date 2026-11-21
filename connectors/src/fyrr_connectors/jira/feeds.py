"""Acknowledged timestamps, bounded bindings and rotating numeric issue IDs."""

import base64
import hashlib
import json
import zlib
from dataclasses import asdict, dataclass, field
from datetime import datetime

from ..validate import MAX_CURSOR_BYTES, MAX_REVISION_BYTES


def stamp(value):
    if not isinstance(value, str):
        raise ValueError("jira_timestamp_invalid")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError
        return int(result.timestamp())
    except (ValueError, OverflowError):
        raise ValueError("jira_timestamp_invalid") from None


def fingerprint(settings, scope):
    # An unset `fields` is left out, so cursors written before the key existed stay valid.
    described = {key: value for key, value in asdict(scope).items() if value or key != "fields"}
    return hashlib.sha256(
        json.dumps(["jira-1", settings.cloud_id, described], sort_keys=True).encode()
    ).hexdigest()


@dataclass
class Cursor:
    scope: str
    updated: int = 0
    marker: int = 0
    step: int = 0
    start: int = 0
    full: int = 0
    policies: str = ""
    holder_types: str = ""

    def encode(self):
        value = json.dumps(self.__dict__, sort_keys=True, separators=(",", ":"))
        if len(value.encode()) > MAX_CURSOR_BYTES:
            raise ValueError("jira_cursor_limit")
        return value

    @classmethod
    def decode(cls, value, key):
        if value is None:
            return cls(key)
        if not isinstance(value, str) or len(value.encode()) > MAX_CURSOR_BYTES:
            raise ValueError("jira_cursor_invalid")
        try:
            body = json.loads(value)
        except (ValueError, TypeError):
            raise ValueError("jira_cursor_invalid") from None
        if (
            not isinstance(body, dict)
            or set(body) != set(cls(key).__dict__)
            or any(
                type(body[k]) is not int or not 0 <= body[k] < 10**18
                for k in ("updated", "marker", "step", "start", "full")
            )
            or not isinstance(body["scope"], str)
            or not isinstance(body["policies"], str)
            or len(body["policies"]) > 64
            or not isinstance(body["holder_types"], str)
            or len(body["holder_types"]) > 64
        ):
            raise ValueError("jira_cursor_invalid")
        return cls(**body) if body["scope"] == key else cls(key)


def rotation(ids, marker, elapsed):
    ids = sorted(ids)
    if not ids:
        return [], 0, True
    budget = min(len(ids), max(1, (len(ids) * max(1, elapsed) + 43199) // 43200))
    after = [v for v in ids if v > marker]
    ordered = after + [v for v in ids if v <= marker]
    selected = ordered[:budget]
    return selected, selected[-1], budget == len(ids) or len(after) <= budget


@dataclass(frozen=True)
class IssueBinding:
    project: int
    level: int | None = None
    dynamic: bool = False
    targets: tuple = field(default_factory=tuple)
    groups: tuple = field(default_factory=tuple)
    visibility: str | None = None

    def suffix(self):
        descriptor: list[object] = [
            self.project,
            self.level,
            self.dynamic,
            self.targets,
            self.groups,
        ]
        if self.visibility is not None:
            descriptor.append(self.visibility)
        raw = json.dumps(
            descriptor,
            separators=(",", ":"),
        ).encode()
        return base64.urlsafe_b64encode(zlib.compress(raw)).decode().rstrip("=")

    def revision(self, digest):
        value = digest + ":" + self.suffix()
        if len(value.encode()) > MAX_REVISION_BYTES:
            raise ValueError("jira_binding_revision_limit")
        return value

    @classmethod
    def from_revision(cls, value):
        try:
            raw = value.split(":", 1)[1]
            data = zlib.decompressobj()
            body = data.decompress(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)), 2048)
            if not data.eof or data.unconsumed_tail:
                raise ValueError
            decoded = json.loads(body)
            p, level, d, t = decoded[:4]
            groups = decoded[4] if len(decoded) >= 5 else []
            visibility = decoded[5] if len(decoded) == 6 else None
            if len(decoded) not in (4, 5, 6):
                raise ValueError
            if (
                type(p) is not int
                or p < 1
                or (level is not None and (type(level) is not int or level < 1))
                or type(d) is not bool
                or not isinstance(t, list)
            ):
                raise ValueError
            if not isinstance(groups, list) or any(
                not isinstance(v, str) or not v or len(v) > 256 for v in groups
            ):
                raise ValueError
            if visibility is not None and (
                not isinstance(visibility, str)
                or len(visibility) != 64
                or any(c not in "0123456789abcdef" for c in visibility)
            ):
                raise ValueError
            return cls(p, level, d, tuple(tuple(v) for v in t), tuple(groups), visibility)
        except (ValueError, TypeError, IndexError, zlib.error):
            return None
