"""Pure bounded text and source metadata; no patches or attachments."""

from __future__ import annotations

import hashlib
import json
import unicodedata
from collections.abc import Callable
from dataclasses import asdict, dataclass
from functools import wraps
from typing import Any
from urllib.parse import quote

from ..arrow import Location, Metadata, Section
from ..validate import MAX_METADATA_VALUE_BYTES, ContractViolation, check_text
from .client import instant
from .feeds import ConversationRef, Repository
from .tree import FileRef

FORMAT = "1"


class InvalidDocument(Exception):
    def __init__(self):
        super().__init__("github_document_invalid")


# What a malformed JSON shape (a wrong type, a missing key, an out-of-range
# number) can raise while one fetched document is rendered.
MALFORMED = (TypeError, ValueError, OverflowError, KeyError, AttributeError)


def malformed_is_invalid[**P, R](render: Callable[P, R]) -> Callable[P, R]:
    """Rendering one document from fetched JSON: malformed values are an
    `InvalidDocument` for that document, never a failure of the pass."""

    @wraps(render)
    def guarded(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return render(*args, **kwargs)
        except MALFORMED as error:
            raise InvalidDocument from error

    return guarded


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


def bounded(value: str, max_bytes: int) -> str:
    """`value` without surrounding whitespace, cut on a character boundary.

    A value the contract already accepts comes back unchanged. The result may be
    empty; a control character inside it is left for the per-document guard.
    """
    return value.strip().encode()[:max_bytes].decode("utf-8", "ignore").strip()


def fits(label: str) -> bool:
    try:
        check_text("location.label", label, MAX_METADATA_VALUE_BYTES)
    except ContractViolation:
        return False
    return True


def spaced(value: str) -> str:
    return "".join(" " if unicodedata.category(c) == "Cc" else c for c in value)


def title_label(title: str) -> str | None:
    """The first section's label: `title[:512]` as always while the contract
    accepts it (a changed label changes the revision), else a repaired one."""
    legacy = title[:512]
    if fits(legacy):
        return legacy
    # A control character also sits in the metadata title, which quarantines
    # the document, so only length and whitespace are repaired here.
    return bounded(title, MAX_METADATA_VALUE_BYTES) or None


def heading_label(heading: str, line: int) -> str:
    """`<heading> (line N)` as always while the contract accepts it, else the
    heading repaired to leave room for the suffix; a blank one gives `(line N)`."""
    suffix = f" (line {line})"
    if fits(heading + suffix):
        return heading + suffix
    room = MAX_METADATA_VALUE_BYTES - len(suffix.encode())
    return f"{bounded(spaced(heading), room)}{suffix}".strip()


def message_label(prefix: str, suffix: str) -> str:
    """The permalink `prefix` plus an optional `: path:line` suffix. The suffix is
    decoration: control characters become spaces, and one that still does not
    fit is left out whole."""
    if fits(prefix + suffix):
        return prefix + suffix
    repaired = (prefix + spaced(suffix)).strip()
    return repaired if fits(repaired) else prefix


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


def opening(parent: dict[str, Any], limit: int) -> str:
    """The first section's text: the title on its own line above the body."""
    title = text(parent.get("title"), min(limit, 4096))
    body = text(parent.get("body"), limit)
    return text(f"{title}\n\n{body}" if title and body else title or body, limit)


@malformed_is_invalid
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
            text=opening(parent, limit),
            location=Location(kind="heading", label=title_label(title), index=0),
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
        prefix = f"{uri}#{anchor}{part['id']}"
        # The path and line only decorate the label; an unusable one is left out.
        suffix = ""
        if part.get("path") is not None:
            try:
                suffix = ": " + text(part["path"], 1024)
            except InvalidDocument:
                pass
            else:
                line = part.get("line") or part.get("original_line")
                # `isinstance` on purpose: it is what the label was always
                # built with, and a changed label changes the revision.
                if isinstance(line, int) and line > 0:
                    suffix += f":{line}"
        sections.append(
            Section(
                text=body,
                location=Location(
                    kind="message", label=message_label(prefix, suffix), index=len(sections)
                ),
            )
        )
        stamps.append(instant(part.get("updated_at") or part.get("submitted_at")))
    user = parent.get("user")
    author = bounded(text(user.get("login"), 320), 320) if isinstance(user, dict) else None
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
        title=bounded(title, 1024) or None,
        author=author or None,
        project=repo.name,
        status="merged"
        if parent.get("merged_at")
        else bounded(text(parent.get("state"), 32), 32) or None,
        labels=tuple(
            name for label in labels if (name := bounded(text(label.get("name"), 256), 256))
        ),
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


@malformed_is_invalid
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
                        kind="heading", label=heading_label(heading, start), index=start
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
                location=Location(kind="heading", label=heading_label(heading, start), index=start),
            )
        )
    facts = commit.get("commit") if isinstance(commit, dict) else None
    author = facts.get("author") if isinstance(facts, dict) else None
    committer = facts.get("committer") if isinstance(facts, dict) else None
    metadata = Metadata(
        source_uri=f"https://github.com/{repo.name}/blob/{ref.commit}/{quote(ref.path, safe='/')}",
        source_type="github-file",
        title=bounded(ref.path, 1024) or None,
        project=repo.name,
        author=bounded(text(author.get("name"), 320), 320) or None
        if isinstance(author, dict)
        else None,
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
