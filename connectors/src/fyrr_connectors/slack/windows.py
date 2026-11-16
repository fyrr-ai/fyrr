"""Pure timestamp-anchored conversation windows with bounded local rebuilding."""

from dataclasses import dataclass

from .feeds import timestamp

UNIT = 16 * 1024


@dataclass(frozen=True)
class Message:
    ts: int
    body: object
    user: str | None
    thread: int | None
    edited: int | None
    bot: bool
    eligible: bool
    bot_name: str | None = None

    @classmethod
    def parse(cls, body, bots):
        stamp = timestamp(body.get("ts"))
        if body.get("type") != "message":
            return None
        subtype = body.get("subtype")
        if subtype not in (None, "", "bot_message", "me_message", "file_share", "thread_broadcast"):
            return None
        bot = bool(body.get("bot_id") or body.get("app_id") or subtype == "bot_message")
        thread = timestamp(body["thread_ts"]) if body.get("thread_ts") else None
        replies = body.get("reply_count", 0)
        if not isinstance(replies, int) or isinstance(replies, bool) or replies < 0:
            raise ValueError("slack_reply_count_invalid")
        if replies:
            thread = stamp
        edited = body.get("edited", {})
        if not isinstance(edited, dict):
            raise ValueError("slack_edit_invalid")
        profile = body.get("bot_profile", {})
        return cls(
            stamp,
            body.get("text"),
            body.get("user"),
            thread,
            timestamp(edited["ts"]) if "ts" in edited else None,
            bot,
            not bot or bots,
            profile.get("name") if isinstance(profile, dict) else None,
        )


@dataclass(frozen=True)
class WindowAnchor:
    start: int
    end: int


@dataclass(frozen=True)
class Window:
    start: int
    messages: tuple[Message, ...]

    @property
    def end(self):
        return self.messages[-1].ts


def size(message):
    if not isinstance(message.body, str) or "\0" in message.body:
        return UNIT + 1
    try:
        return len(message.body.encode()) + 2  # a separator between message units
    except UnicodeError:
        return UNIT + 1


def build(messages, anchors, gap):
    ordered = sorted(
        (item for item in messages if item.eligible and item.thread is None),
        key=lambda item: item.ts,
    )
    anchors = sorted(anchors, key=lambda item: item.start)
    result: list[Window] = []
    current: list[Message] = []
    used: set[int] = set()
    start, bytes_used = 0, 0
    for item in ordered:
        anchor = next((a.start for a in anchors if a.start <= item.ts <= a.end), item.ts)
        barrier = any(current and current[-1].ts < a.start <= item.ts for a in anchors)
        if current and (
            item.ts - current[-1].ts > gap * 1_000_000 or bytes_used + size(item) > UNIT or barrier
        ):
            result.append(Window(start, tuple(current)))
            current, bytes_used = [], 0
        if not current:
            start = anchor if anchor not in used else item.ts
            used.add(start)
        current.append(item)
        bytes_used += size(item)
    if current:
        result.append(Window(start, tuple(current)))
    return result
