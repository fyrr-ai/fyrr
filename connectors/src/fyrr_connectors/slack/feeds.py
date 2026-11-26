"""Channel identity and bounded numeric cursor state; no principals or payloads."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field

from .._http import MAX_ENTRIES
from ..validate import MAX_CURSOR_BYTES
from .client import ApiError

PER_CHANNEL_ANCHORS = 8
TOTAL_ANCHORS = 64
TARGET = 12 * 3600 * 1_000_000


def timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,12}\.[0-9]{6}", value):
        raise ValueError("slack_timestamp_invalid")
    seconds, micros = value.split(".")
    return int(seconds) * 1_000_000 + int(micros)


def wire(value):
    return f"{value // 1_000_000}.{value % 1_000_000:06}"


def channel_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[CG][A-Z0-9]{2,31}", value):
        raise ValueError("slack_channel_identity_invalid")
    return value


@dataclass(frozen=True)
class Channel:
    id: str
    name: str
    private: bool
    shared: bool
    archived: bool

    @classmethod
    def parse(cls, body):
        identity = channel_id(body.get("id"))
        name = body.get("name")
        flags = [
            body.get(key)
            for key in ("is_private", "is_archived", "is_shared", "is_ext_shared", "is_org_shared")
        ]
        if (
            not isinstance(name, str)
            or not name
            or len(name.encode()) > 128
            or any(not isinstance(flag, bool) for flag in flags)
        ):
            raise ValueError("slack_channel_invalid")
        if body.get("is_im") or body.get("is_mpim"):
            raise ValueError("slack_dm_unsupported")
        return cls(identity, name, flags[0], any(flags[2:]), flags[1])

    @classmethod
    def read(cls, client, identity):
        body = client.get("conversations.info", channel=identity).get("channel")
        if not isinstance(body, dict) or body.get("id") != identity:
            raise ValueError("slack_channel_parent_invalid")
        return cls.parse(body)


def catalog(client, scope, known_ids):
    listed = {}
    for entries in client.pages(
        "conversations.list",
        "channels",
        types=",".join(scope.channel_types),
        exclude_archived="false",
    ):
        for body in entries:
            item = Channel.parse(body)
            listed[item.id] = item
    candidates = {key for key, value in listed.items() if scope.matches(value)} | set(known_ids)
    candidates.update(
        value
        for value in scope.include
        if re.fullmatch(r"[CG][A-Z0-9]{2,31}", value)
        and value not in scope.exclude
        and "*" not in scope.exclude
    )
    found, gone = {}, set()
    for identity in sorted(candidates):
        try:
            item = Channel.read(client, identity)
        except ApiError as error:
            if error.gone and identity in known_ids:
                gone.add(identity)
                continue
            raise
        if scope.matches(item):
            found[identity] = item
    if len(found) > 64:
        raise ValueError("slack_scope_exceeds_64_channels_use_several_sources")
    return found, gone


@dataclass
class ChannelCursor:
    since: int = 0
    windows: int = 0
    threads: int = 0
    step: int = 0
    start: int = 0
    anchors: list[list[int]] = field(default_factory=list)


@dataclass
class Cursor:
    scope: str
    channels: dict[str, ChannelCursor] = field(default_factory=dict)

    @classmethod
    def decode(cls, text, scope, gap, workspace=""):
        fingerprint = hashlib.sha256(
            json.dumps([asdict(scope), gap, workspace], sort_keys=True).encode()
        ).hexdigest()
        result = cls(fingerprint)
        if text is None:
            return result
        try:
            if len(text.encode()) > MAX_CURSOR_BYTES:
                raise ValueError
            body = json.loads(text)
            if (
                set(body) != {"v", "scope", "channels"}
                or body["v"] != 1
                or not isinstance(body["channels"], dict)
                or len(body["channels"]) > 64
            ):
                raise ValueError
            if body["scope"] != fingerprint:
                return result
            count = 0
            for identity, value in body["channels"].items():
                channel_id(identity)
                if set(value) != {"since", "windows", "threads", "step", "start", "anchors"}:
                    raise ValueError
                for key in ("since", "windows", "threads", "step", "start"):
                    if (
                        not isinstance(value[key], int)
                        or isinstance(value[key], bool)
                        or not 0 <= value[key] < 10**18
                    ):
                        raise ValueError
                anchors = value["anchors"]
                if (
                    not isinstance(anchors, list)
                    or len(anchors) > PER_CHANNEL_ANCHORS
                    or any(
                        not isinstance(pair, list)
                        or len(pair) != 2
                        or any(
                            not isinstance(n, int) or isinstance(n, bool) or not 0 <= n < 10**18
                            for n in pair
                        )
                        or pair[0] > pair[1]
                        for pair in anchors
                    )
                ):
                    raise ValueError
                count += len(anchors)
                result.channels[identity] = ChannelCursor(**value)
            if count > TOTAL_ANCHORS:
                raise ValueError
            return result
        except (ValueError, KeyError, TypeError, AttributeError):
            raise ValueError("slack_cursor_invalid") from None

    def encode(self):
        # Fair bounded allocation, independent of a channel's lifetime.
        count = 0
        allocation = min(PER_CHANNEL_ANCHORS, TOTAL_ANCHORS // max(1, len(self.channels)))
        for item in self.channels.values():
            item.anchors = item.anchors[-allocation:]
            count += len(item.anchors)
        result = json.dumps(
            {
                "v": 1,
                "scope": self.scope,
                "channels": {key: asdict(value) for key, value in sorted(self.channels.items())},
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        if len(result.encode()) > MAX_CURSOR_BYTES:
            raise ValueError("slack_cursor_limit")
        return result


def rotation(values, marker, elapsed):
    polls = max(1, TARGET // max(1, elapsed))
    budget = (len(values) + polls - 1) // polls
    tail = [value for value in values if value > marker]
    selected = (tail + values[: max(0, budget - len(tail))])[:budget]
    complete = budget >= len(tail)
    return selected, (selected[-1] if selected and budget != len(tail) else 0), complete


def history(client, channel, oldest, latest):
    result = {}
    for entries in client.pages(
        "conversations.history",
        "messages",
        channel=channel.id,
        oldest=wire(oldest),
        latest=wire(latest),
        inclusive="true",
    ):
        for body in entries:
            if body.get("channel", channel.id) != channel.id:
                raise ValueError("slack_message_parent_invalid")
            stamp = timestamp(body.get("ts"))
            if not oldest <= stamp <= latest:
                raise ValueError("slack_history_bounds_invalid")
            result[stamp] = body
            if len(result) > MAX_ENTRIES:
                raise ValueError("slack_history_limit")
    return result
