"""Slack text, display mentions, source metadata and message locations."""

import hashlib
import html
import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from ..arrow import Location, Metadata, Section
from .windows import UNIT

FORMAT = "1"


class InvalidDocument(Exception):
    pass


def text(value, limit):
    if not isinstance(value, str) or "\0" in value:
        raise InvalidDocument
    try:
        if len(value.encode()) > limit:
            raise InvalidDocument
    except UnicodeError:
        raise InvalidDocument from None
    return value


def display(value):
    if not isinstance(value, str):
        return None
    try:
        return (
            " ".join("".join(c for c in value if unicodedata.category(c) != "Cc").split())
            .encode()[:512]
            .decode(errors="ignore")
            or None
        )
    except UnicodeError:
        raise InvalidDocument from None


def date(value):
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value)


def mentions(body, users, channels):
    body = text(body, UNIT)
    body = re.sub(r"<@([UW][A-Z0-9]+)(?:\|[^>]+)?>", lambda m: "@" + (users(m[1]) or m[1]), body)
    body = re.sub(
        r"<#([CG][A-Z0-9]+)(?:\|([^>]+))?>",
        lambda m: "#" + (channels.get(m[1]) or display(m[2]) or "channel"),
        body,
    )
    body = re.sub(r"<([^<>]+)\|([^<>]+)>", r"\2 (\1)", body)
    return text(html.unescape(body), UNIT)


@dataclass(frozen=True)
class Rendered:
    revision: str
    metadata: Metadata
    sections: tuple[Section, ...]


def document(identity, channel, kind, anchor, messages, users, channels, limit):
    eligible = [item for item in messages if item.eligible]
    if not eligible:
        return None
    uri = f"{identity.url}/archives/{channel.id}/p{anchor}"
    sections, authors, total = [], set(), 0
    for item in sorted(eligible, key=lambda item: item.ts):
        author = display(item.bot_name) if item.bot else users(item.user)
        if author:
            authors.add(author)
        body = mentions(item.body, users, channels)
        stamp = date(item.ts)
        rendered = body
        text(rendered, UNIT)
        total += len(rendered.encode())
        if total > limit or (kind == "window" and total > UNIT):
            raise InvalidDocument
        sections.append(
            Section(
                text=rendered,
                location=Location(
                    kind="message",
                    label=(
                        f"{identity.url}/archives/{channel.id}/p{item.ts} · "
                        f"{author or 'unknown'} · {stamp.isoformat()}"
                    ),
                    index=len(sections),
                ),
            )
        )
    metadata = Metadata(
        source_uri=uri,
        source_type="slack-" + kind,
        title=f"#{channel.name} {date(anchor).isoformat()}",
        author=next(iter(authors)) if len(authors) == 1 else None,
        project=channel.name,
        team=identity.workspace,
        status="archived" if channel.archived else "active",
        created_at=date(min(m.ts for m in eligible)),
        updated_at=date(max(m.edited or m.ts for m in eligible)),
        connector_map={"channel_id": channel.id, "conversation_ts": str(anchor)},
    )
    revision = hashlib.sha256(
        json.dumps(
            [
                FORMAT,
                asdict(metadata),
                [asdict(s) for s in sections],
                [[m.ts, m.edited] for m in eligible],
            ],
            sort_keys=True,
            default=str,
        ).encode()
    ).hexdigest()
    return Rendered(revision, metadata, tuple(sections))
