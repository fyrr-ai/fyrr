"""Pure bounded text and source metadata; no patches or attachments."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import quote

from ..arrow import Location, Metadata, Section
from .client import instant
from .feeds import ConversationRef, Repository
from .tree import FileRef

FORMAT = "1"


class InvalidDocument(Exception):
    def __init__(self):
        super().__init__("github_document_invalid")


def text(value: object, limit: int) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise InvalidDocument
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        raise InvalidDocument from None
    if "\0" in value or size > limit:
        raise InvalidDocument
    return value


@dataclass(frozen=True)
class Rendered:
    metadata: Metadata
    sections: list[Section]
    revision: str


def finish(metadata: Metadata, sections: list[Section], facts: object, limit: int) -> Rendered:
    if sum(len(section.text.encode()) for section in sections) > limit:
        raise InvalidDocument
    body = [FORMAT, asdict(metadata), [asdict(section) for section in sections], facts]
    revision = hashlib.sha256(
        json.dumps(body, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()
    return Rendered(metadata, sections, revision)


def conversation(
    repo: Repository,
    ref: ConversationRef,
    parent: dict[str, Any],
    parts: list[dict[str, Any]],
    limit: int,
) -> Rendered:
    uri = f"https://github.com/{repo.name}/{'pull' if ref.kind == 'pr' else 'issues'}/{ref.number}"
    title = text(parent.get("title"), min(limit, 4096))
    sections = [
        Section(
            text=text(parent.get("body") or title, limit),
            location=Location(kind="heading", label=title[:512], index=0),
        )
    ]
    size = len(sections[0].text.encode())
    stamps = [instant(parent.get("updated_at"))]
    for part in sorted(
        parts, key=lambda item: (item.get("created_at") or "", item["kind"], item["id"])
    ):
        body = text(part.get("body"), limit)
        size += len(body.encode())
        if size > limit:
            raise InvalidDocument
        anchor = {
            "comment": "issuecomment-",
            "review": "pullrequestreview-",
            "inline": "discussion_r",
        }[part["kind"]]
        label = f"{uri}#{anchor}{part['id']}"
        if part.get("path") is not None:
            label += ": " + text(part["path"], 1024)
            line = part.get("line") or part.get("original_line")
            if isinstance(line, int) and line > 0:
                label += f":{line}"
        sections.append(
            Section(text=body, location=Location(kind="message", label=label, index=len(sections)))
        )
        stamps.append(instant(part.get("updated_at") or part.get("submitted_at")))
    user = parent.get("user")
    author = text(user.get("login"), 320) if isinstance(user, dict) else None
    labels = parent.get("labels", [])
    if (
        not isinstance(labels, list)
        or len(labels) > 256
        or any(not isinstance(label, dict) for label in labels)
    ):
        raise InvalidDocument
    metadata = Metadata(
        source_uri=uri,
        source_type="github-pr" if ref.kind == "pr" else "github-issue",
        title=title or None,
        author=author or None,
        project=repo.name,
        status="merged" if parent.get("merged_at") else text(parent.get("state"), 32) or None,
        labels=tuple(text(label.get("name"), 256) for label in labels),
        created_at=instant(parent.get("created_at")),
        updated_at=max((stamp for stamp in stamps if stamp is not None), default=None),
        connector_map={"repository_id": repo.id, "object_id": ref.id, "number": ref.number},
    )
    return finish(
        metadata,
        sections,
        [
            [part["kind"], part["id"], part.get("state")]
            for part in sorted(parts, key=lambda part: (part["kind"], part["id"]))
        ],
        limit,
    )


def file(
    repo: Repository, ref: FileRef, data: bytes, commit: dict[str, Any] | None, limit: int
) -> Rendered:
    try:
        source = text(data.decode("utf-8"), limit)
    except UnicodeError:
        raise InvalidDocument from None
    sections: list[Section] = []
    buffer: list[str] = []
    start = 1
    heading = ref.path
    for ordinal, line in enumerate(source.splitlines(), 1):
        if line.startswith("#") and buffer:
            sections.append(
                Section(
                    text="\n".join(buffer),
                    location=Location(
                        kind="heading", label=f"{heading} (line {start})", index=start
                    ),
                )
            )
            buffer = []
            start, heading = ordinal, line.lstrip("# ")[:512]
        buffer.append(line)
    if buffer:
        sections.append(
            Section(
                text="\n".join(buffer),
                location=Location(kind="heading", label=f"{heading} (line {start})", index=start),
            )
        )
    facts = commit.get("commit") if isinstance(commit, dict) else None
    author = facts.get("author") if isinstance(facts, dict) else None
    committer = facts.get("committer") if isinstance(facts, dict) else None
    metadata = Metadata(
        source_uri=f"https://github.com/{repo.name}/blob/{ref.commit}/{quote(ref.path, safe='/')}",
        source_type="github-file",
        title=ref.path,
        project=repo.name,
        author=text(author.get("name"), 320) or None if isinstance(author, dict) else None,
        updated_at=instant(committer.get("date")) if isinstance(committer, dict) else None,
        connector_map={
            "repository_id": repo.id,
            "branch": ref.branch,
            "path": ref.path,
            "blob_sha": ref.blob,
            "commit_sha": ref.commit,
        },
    )
    return finish(metadata, sections, ref.blob, limit)
