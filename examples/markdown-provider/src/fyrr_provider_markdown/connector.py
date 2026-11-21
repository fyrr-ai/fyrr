"""Markdown files in a local directory, as a fixed connection.

Content: one document per file, ordered by (modification time, path); the
cursor after a batch is the last file's `mtime_ns:path`, so a later sync
resumes after it. Deletes are not seen by content syncs; the full-ID listing
(reconcile) lists every file in scope and is complete, so fyrr derives them.
Unavailable roots, directory symlinks and enumeration failures refuse the read.
"""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Iterator
from datetime import UTC, datetime
from fnmatch import fnmatchcase
from pathlib import Path, PurePath

from fyrr_connectors import (
    ConfigModel,
    Connector,
    ContentBatch,
    ListingBatch,
    Location,
    Metadata,
    Section,
    Upsert,
    document_changes,
    setting,
)


class Settings(ConfigModel):
    """How files are read."""

    max_file_bytes: int = setting(
        description="Files larger than this are skipped.", default=1 << 20, minimum=1
    )
    batch_size: int = setting(
        description="Documents per batch.", default=100, minimum=1, maximum=10_000
    )


class Scope(ConfigModel):
    """Which files are synced."""

    root: str = setting(description="Directory to read.")
    pattern: str = setting(
        description="Nonempty relative file glob below the root.", default="**/*.md"
    )

    def __post_init__(self) -> None:
        if (
            not self.pattern
            or "\0" in self.pattern
            or self.pattern.endswith("/")
            or Path(self.pattern).is_absolute()
            or ".." in Path(self.pattern).parts
        ):
            raise ValueError("scope_pattern_invalid")


def _matches(parts: tuple[str, ...], pattern: tuple[str, ...]) -> bool:
    """`PurePath.full_match`, which needs Python 3.13: `*` stays inside one
    path part and a `**` part matches any number of parts."""
    if not pattern:
        return not parts
    if pattern == ("**",):
        return bool(parts)
    if pattern[0] == "**":
        return any(_matches(parts[skip:], pattern[1:]) for skip in range(len(parts) + 1))
    return bool(parts) and fnmatchcase(parts[0], pattern[0]) and _matches(parts[1:], pattern[1:])


def _front_matter(text: str) -> tuple[dict[str, str], str]:
    if not text.startswith("---\n"):
        return {}, text
    head, separator, body = text[4:].partition("\n---\n")
    if not separator:
        return {}, text
    fields = {}
    for line in head.splitlines():
        key, _, value = line.partition(":")
        if key.strip() and value.strip():
            fields[key.strip().lower()] = value.strip()
    return fields, body


def _sections(body: str) -> list[Section]:
    sections: list[Section] = []
    heading: str | None = None
    lines: list[str] = []

    def flush() -> None:
        text = "\n".join(lines).strip()
        if text:
            location = Location(kind="heading", label=heading) if heading else None
            sections.append(Section(text=text, location=location))

    for line in body.splitlines():
        if line.startswith("#"):
            flush()
            heading = line.lstrip("#").strip() or None
            lines = []
        else:
            lines.append(line)
    flush()
    return sections


class MarkdownConnector(Connector[Settings, Scope]):
    Settings = Settings
    Scope = Scope

    def _files(self) -> list[tuple[int, str, Path]]:
        root = Path(self.context.scope.root)
        if not root.is_dir():
            raise ValueError("source_unavailable")

        def unavailable(error: OSError) -> None:
            raise error

        found = []
        try:
            for parent, directories, names in os.walk(root, onerror=unavailable):
                # `os.walk` lists a directory symlink with the directories, not
                # the files; it must still refuse the read.
                for name in (*directories, *names):
                    path = Path(parent, name)
                    relative = path.relative_to(root)
                    if path.is_symlink() and stat.S_ISDIR(path.stat().st_mode):
                        raise ValueError("source_unavailable")
                    if not _matches(relative.parts, PurePath(self.context.scope.pattern).parts):
                        continue
                    info = path.stat()
                    if (
                        stat.S_ISREG(info.st_mode)
                        and info.st_size <= self.context.settings.max_file_bytes
                    ):
                        found.append((info.st_mtime_ns, relative.as_posix(), path))
        except OSError as error:
            raise ValueError("source_unavailable") from error
        return sorted(found)

    def _document(self, relative: str, path: Path) -> Upsert | None:
        raw = path.read_bytes()
        fields, body = _front_matter(raw.decode("utf-8", errors="replace"))
        sections = _sections(body)
        if not sections:
            return None
        title = fields.get("title")
        if title is None and sections[0].location is not None:
            title = sections[0].location.label
        labels = [label.strip() for label in fields.get("labels", "").split(",") if label.strip()]
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        return Upsert(
            root_id=relative,
            source_revision="sha256:" + hashlib.sha256(raw).hexdigest(),
            metadata=Metadata(
                source_uri=path.resolve().as_uri(),
                source_type="markdown.file",
                title=title or path.stem,
                author=fields.get("author"),
                labels=labels,
                updated_at=modified,
            ),
            attributes=[f"markdown.directory={Path(relative).parent.as_posix()}"],
            sections=sections,
        )

    def content(self, cursor: str | None) -> Iterator[ContentBatch]:
        after: tuple[int, str] | None = None
        if cursor:
            cursor_mtime, _, relative = cursor.partition(":")
            after = (int(cursor_mtime), relative)
        pending: list[Upsert] = []
        last = emitted = cursor
        for mtime, relative, path in self._files():
            if after is not None and (mtime, relative) <= after:
                continue
            document = self._document(relative, path)
            if document is not None:
                pending.append(document)
            last = f"{mtime}:{relative}"
            if len(pending) >= self.context.settings.batch_size:
                yield ContentBatch(changes=document_changes(pending), cursor=last)
                pending, emitted = [], last
        if last is not None and last != emitted:
            yield ContentBatch(changes=document_changes(pending), cursor=last)

    def listing(self, cursor: str | None) -> Iterator[ListingBatch]:
        root_ids = [relative for _, relative, _ in self._files()]
        listing_id = "listing-" + hashlib.sha256("\n".join(root_ids).encode()).hexdigest()[:16]
        yield ListingBatch(
            listing_id=listing_id,
            part=0,
            last=True,
            complete=True,
            root_ids=root_ids,
            cursor=listing_id,
        )
