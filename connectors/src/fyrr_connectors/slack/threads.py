"""Full thread snapshots, including canonical parent verification."""

from dataclasses import dataclass, replace

from .client import ApiError
from .feeds import Channel, history, timestamp, wire
from .windows import Message


@dataclass(frozen=True)
class Thread:
    start: int
    messages: tuple[Message, ...]


def read(client, channel, start, bots, limit):
    before = history(client, channel, start, start)
    if start not in before:
        Channel.read(client, channel.id)
        return None
    found = {}
    size = 0
    try:
        for entries in client.pages(
            "conversations.replies", "messages", channel=channel.id, ts=wire(start)
        ):
            for body in entries:
                stamp = timestamp(body.get("ts"))
                if body.get("channel", channel.id) != channel.id or (
                    stamp != start and timestamp(body.get("thread_ts")) != start
                ):
                    raise ValueError("slack_thread_parent_invalid")
                item = Message.parse(body, bots)
                if item is not None:
                    if item.eligible:
                        try:
                            size += (
                                len(item.body.encode()) if isinstance(item.body, str) else limit + 1
                            )
                        except UnicodeError:
                            size = limit + 1
                        if size > limit:
                            return Thread(start, (replace(item, body="\0", eligible=True),))
                    found[stamp] = item
    except ApiError:
        raise
    if start not in found:
        raise ValueError("slack_thread_incomplete")
    after = history(client, channel, start, start)
    if before != after or Channel.read(client, channel.id) != channel:
        raise ValueError("slack_thread_changed_during_read")
    return Thread(start, tuple(found[key] for key in sorted(found)))
