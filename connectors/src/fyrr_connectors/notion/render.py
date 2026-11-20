"""Notion blocks and page properties as located sections.

Pure: no request is made here. Blocks arrive in document order, a parent
before its children, and become one section per heading; a database row
starts with a section of its properties. Rich text is taken as Notion's
`plain_text`. File and media blocks give only their caption.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from typing import Any

from ..arrow import Location, Section
from ..validate import MAX_METADATA_VALUE_BYTES

#: Blocks whose children are indented beneath them.
NESTING = frozenset({"bulleted_list_item", "numbered_list_item", "to_do", "toggle"})
_HEADINGS = {"heading_1": 1, "heading_2": 2, "heading_3": 3}
_LINKS = frozenset({"bookmark", "embed", "link_preview"})
_MEDIA = frozenset({"image", "video", "file", "pdf", "audio"})
#: Lines that follow each other without a blank line.
_TIGHT = frozenset({"bulleted_list_item", "numbered_list_item", "to_do", "table_row"})
#: A rollup holds values, a formula one value: nothing nests deeper.
_VALUE_DEPTH = 3


class Oversized(Exception):
    """The page's text exceeds the document limit; it is quarantined."""


class InvalidText(Exception):
    """Text cannot be represented as UTF-8; quarantine without echoing it."""


def clean(value: object, max_bytes: int = MAX_METADATA_VALUE_BYTES) -> str | None:
    """A contract-valid single-line value (no control characters, no
    surrounding whitespace, bounded), or None when nothing is left."""
    text = "".join(
        " " if unicodedata.category(character) == "Cc" else character for character in str(value)
    )
    text = text.encode(errors="replace")[:max_bytes].decode(errors="ignore").strip()
    return text or None


def plain(rich_text: object) -> str:
    """The text of a rich-text array; mentions and equations as Notion spells them."""
    if not isinstance(rich_text, list):
        return ""
    return "".join(
        item["plain_text"]
        for item in rich_text
        if isinstance(item, Mapping) and isinstance(item.get("plain_text"), str)
    )


def title(page: Mapping[str, Any]) -> str | None:
    properties = page.get("properties")
    if isinstance(properties, Mapping):
        for value in properties.values():
            if isinstance(value, Mapping) and value.get("type") == "title":
                return clean(plain(value.get("title")))
    return None


def _named(value: object) -> str:
    return str(value["name"]) if isinstance(value, Mapping) and value.get("name") else ""


def _number(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return ""
    return str(int(value)) if float(value).is_integer() else str(value)


def _date(value: object) -> str:
    if not isinstance(value, Mapping) or not value.get("start"):
        return ""
    return " → ".join(str(part) for part in (value["start"], value.get("end")) if part)


def _value(prop: object, depth: int = 0) -> str:
    """One property value as text; empty for no value and for types that
    hold no text of their own (relations, buttons)."""
    if not isinstance(prop, Mapping) or depth > _VALUE_DEPTH:
        return ""
    kind = prop.get("type")
    value = prop.get(kind) if isinstance(kind, str) else None
    if value is None:
        return ""
    if kind in ("title", "rich_text"):
        return plain(value)
    if kind == "number":
        return _number(value)
    if kind in ("select", "status", "created_by", "last_edited_by"):
        return _named(value)
    if kind in ("multi_select", "people", "files"):
        names = [_named(item) for item in value] if isinstance(value, list) else []
        return ", ".join(name for name in names if name)
    if kind == "date":
        return _date(value)
    if kind in ("checkbox", "boolean"):
        return "yes" if value is True else "no"
    if kind in ("url", "email", "phone_number", "string", "created_time", "last_edited_time"):
        return str(value) if isinstance(value, str) else ""
    if kind == "formula":
        return _value(value, depth + 1)
    if kind == "rollup":
        if isinstance(value, Mapping) and value.get("type") == "array":
            items = value.get("array")
            values = [_value(item, depth + 1) for item in items] if isinstance(items, list) else []
            return ", ".join(text for text in values if text)
        return _value(value, depth + 1)
    if kind == "unique_id" and isinstance(value, Mapping) and value.get("number") is not None:
        return "-".join(str(part) for part in (value.get("prefix"), value["number"]) if part)
    if kind == "verification":
        return str(value.get("state") or "") if isinstance(value, Mapping) else ""
    return ""


def property_lines(properties: object) -> list[str]:
    """`Name: value` per non-empty property, the title first, then by name."""
    if not isinstance(properties, Mapping):
        return []
    lines = []
    for name, prop in properties.items():
        text = " ".join(_value(prop).split())
        if text:
            first = isinstance(prop, Mapping) and prop.get("type") == "title"
            lines.append((not first, str(name), f"{' '.join(str(name).split())}: {text}"))
    return [line for _, _, line in sorted(lines)]


def _line(block: Mapping[str, Any], number: int) -> str:
    kind = block.get("type")
    data = block.get(kind) if isinstance(kind, str) else None
    if not isinstance(data, Mapping):
        return ""
    text = plain(data.get("rich_text"))
    if kind in _HEADINGS:
        return f"{'#' * _HEADINGS[kind]} {text}" if text.strip() else ""
    if kind == "bulleted_list_item":
        return f"- {text}"
    if kind == "numbered_list_item":
        return f"{number}. {text}"
    if kind == "to_do":
        return f"- [{'x' if data.get('checked') is True else ' '}] {text}"
    if kind == "quote":
        return "\n".join(f"> {line}" for line in text.splitlines())
    if kind == "code":
        language = data.get("language") if isinstance(data.get("language"), str) else ""
        return f"```{language}\n{text}\n```" if text.strip() else ""
    if kind == "equation":
        return str(data.get("expression") or "")
    if kind == "table_row":
        cells = data.get("cells")
        return " | ".join(plain(cell) for cell in cells) if isinstance(cells, list) else ""
    if kind in _LINKS:
        url = data.get("url") if isinstance(data.get("url"), str) else ""
        return " ".join(part for part in (plain(data.get("caption")), url) if part)
    if kind in _MEDIA:
        return plain(data.get("caption"))
    return text


class Text:
    """One page's text, taken block by block and kept within `limit` bytes."""

    def __init__(self, limit: int) -> None:
        self._left = limit
        self.oversized = False
        self.invalid = False
        self._sections: list[Section] = []
        self._label: str | None = None
        #: The page's title until it has opened the first section.
        self._title: str | None = None
        #: Each line with how it joins the one before: 0 after a blank line,
        #: 1 directly after another list or table line, 2 directly (nested).
        self._lines: list[tuple[int, str]] = []
        #: Per indent, how many numbered items the running list has.
        self._numbers: dict[int, int] = {}

    def _take(self, tight: int, text: str) -> None:
        separator = 0
        if self._lines:
            separator = 1 if tight == 2 or (tight and self._lines[-1][0]) else 2
        try:
            weight = len(text.encode())
        except UnicodeEncodeError:
            self.invalid = True
            weight = 0
        self._left -= weight + separator
        if self._left < 0 or self.invalid:
            # Nothing of an invalid or oversized page is kept: it is quarantined.
            self.oversized = self._left < 0
            self._sections.clear()
            self._lines.clear()
        else:
            self._lines.append((tight, text))

    def _close(self, kind: str = "heading") -> None:
        pieces = []
        for index, (tight, text) in enumerate(self._lines):
            joined = tight == 2 or (tight and self._lines[index - 1][0])
            pieces.append(("\n" if joined else "\n\n") * bool(index) + text)
        body = "".join(pieces)
        if body.strip():
            body = self._opening(body)
            if self._left < 0:
                self.oversized = True
                self._sections.clear()
            else:
                location = Location(kind=kind, label=self._label, index=len(self._sections))
                self._sections.append(Section(text=body, location=location))
        self._lines.clear()

    def _opening(self, body: str) -> str:
        """The first section's text with the title as its opening line, unless
        that line already is the title. The title counts toward the limit."""
        title, self._title = self._title, None
        if not title or body.partition("\n")[0].strip().lstrip("#").strip() == title:
            return body
        self._left -= len(title.encode()) + 2
        return f"{title}\n\n{body}"

    def title(self, title: str | None) -> None:
        """A standalone page's title, searchable as the first line of its first section."""
        self._title = title

    def properties(self, properties: object) -> None:
        """A database row's properties, as the first section."""
        for line in property_lines(properties):
            if not self.oversized and not self.invalid:
                self._take(1, line)
        self._close("properties")

    def block(self, indent: int, block: Mapping[str, Any]) -> None:
        if self.oversized or self.invalid:
            return
        kind = block.get("type")
        if kind == "unsupported":
            return
        self._numbers = {depth: n for depth, n in self._numbers.items() if depth <= indent}
        if kind == "numbered_list_item":
            self._numbers[indent] = self._numbers.get(indent, 0) + 1
        else:
            self._numbers.pop(indent, None)
        text = _line(block, self._numbers.get(indent, 0))
        if not text.strip():
            return
        if kind in _HEADINGS:
            self._close()
            self._label = clean(text.lstrip("#"))
        pad = "  " * indent
        tight = 2 if indent else int(kind in _TIGHT)
        self._take(tight, "\n".join(pad + line for line in text.splitlines()))

    def sections(self) -> list[Section]:
        if self.invalid:
            raise InvalidText
        self._close()
        if self.oversized:
            raise Oversized
        return list(self._sections)
