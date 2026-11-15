"""Exported Google documents as located sections.

Pure parsers over `files.export` bytes: Docs as Markdown split at headings,
Sheets as xlsx with every sheet in row groups that repeat the header row,
Slides as pptx with one section per slide. A document that cannot be parsed
or exceeds `limit` text bytes raises `Unexportable` and is quarantined.
"""

from __future__ import annotations

import io
import re
import unicodedata
import zipfile
from collections.abc import Callable, Iterator
from datetime import date, datetime, time
from xml.etree import ElementTree

from openpyxl import load_workbook

from ..arrow import Location, Section
from ..validate import MAX_METADATA_VALUE_BYTES
from .config import MIME_TYPES

#: A row group stays within one 16 KiB record of the chunker.
GROUP_BYTES = 16_384
#: Declared uncompressed bytes of an exported archive we are willing to open.
ARCHIVE_MAX_BYTES = 256 << 20
#: Cells of one workbook we are willing to visit. A Google spreadsheet holds at
#: most 10 million; a sparse sheet from elsewhere can declare billions.
WORKBOOK_MAX_CELLS = 20_000_000

_HEADING = re.compile(r"^#{1,6}\s+(.*?)\s*#*\s*$")
_FENCE = re.compile(r"^\s*(```|~~~)")
# The Markdown export appends every image as a base64 reference definition.
_IMAGE_DEFINITION = re.compile(r"^\[[^\]]+\]:\s*<?data:")
_IMAGE_REFERENCE = re.compile(r"!\[[^\]]*\]\[[^\]]*\]")
_DRAWING = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
_PRESENTATION = "{http://schemas.openxmlformats.org/presentationml/2006/main}"
_RELATIONSHIP = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"


class Unexportable(Exception):
    """The file cannot become text within the limits; it is quarantined."""


def clean(value: object, max_bytes: int = MAX_METADATA_VALUE_BYTES) -> str | None:
    """A contract-valid single-line value (no control characters, no
    surrounding whitespace, bounded), or None when nothing is left."""
    text = "".join(
        " " if unicodedata.category(character) == "Cc" else character for character in str(value)
    )
    text = text.encode()[:max_bytes].decode(errors="ignore").strip()
    return text or None


class _Budget:
    """Counts work against a limit: emitted text bytes, visited cells."""

    def __init__(self, limit: int) -> None:
        self.left = limit

    def spend(self, amount: int) -> None:
        self.left -= amount
        if self.left < 0:
            raise Unexportable("over the limit")

    def take(self, text: str) -> str:
        self.spend(len(text.encode()))
        return text


def document(data: bytes, limit: int) -> list[Section]:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise Unexportable("export is not UTF-8") from None
    budget = _Budget(limit)
    sections: list[Section] = []
    label: str | None = None
    lines: list[str] = []
    fenced = False

    def close() -> None:
        body = "\n".join(lines).strip()
        if body:
            location = Location(kind="heading", label=label, index=len(sections))
            sections.append(Section(text=budget.take(body), location=location))
        lines.clear()

    for line in text.splitlines():
        if _FENCE.match(line):
            fenced = not fenced
        heading = None if fenced else _HEADING.match(line)
        if heading:
            close()
            label = clean(heading.group(1))
        elif not fenced and _IMAGE_DEFINITION.match(line):
            continue
        lines.append(_IMAGE_REFERENCE.sub("", line))
    close()
    return sections


def _archive(data: bytes) -> zipfile.ZipFile:
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise Unexportable("export is not an archive") from None
    if sum(entry.file_size for entry in archive.infolist()) > ARCHIVE_MAX_BYTES:
        raise Unexportable("archive over the size limit")
    return archive


def _cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    return " ".join(str(value).split())


def _rows(sheet: object, cells_left: _Budget) -> Iterator[tuple[int, str]]:
    """Non-empty rows as `(row number, "a | b | c")`."""
    for number, row in enumerate(sheet.iter_rows(values_only=True), start=1):  # type: ignore[attr-defined]
        cells_left.spend(max(1, len(row)))
        cells = [_cell(value) for value in row]
        while cells and not cells[-1]:
            cells.pop()
        if cells:
            yield number, " | ".join(cells)


def _groups(rows: Iterator[tuple[int, str]]) -> Iterator[tuple[int, str]]:
    """Row groups of at most `GROUP_BYTES`, each opening with the header row,
    as `(first row number, text)`."""
    first = next(rows, None)
    if first is None:
        return
    start, header = first
    group, size = [header], len(header.encode())
    for number, line in rows:
        if len(group) > 1 and size + len(line.encode()) + 1 > GROUP_BYTES:
            yield start, "\n".join(group)
            start, group, size = number, [header], len(header.encode())
        group.append(line)
        size += len(line.encode()) + 1
    yield start, "\n".join(group)


def spreadsheet(data: bytes, limit: int) -> list[Section]:
    _archive(data).close()
    budget, cells_left = _Budget(limit), _Budget(WORKBOOK_MAX_CELLS)
    sections: list[Section] = []
    try:
        workbook = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        for sheet in workbook.worksheets:
            for start, text in _groups(_rows(sheet, cells_left)):
                location = Location(kind="sheet", label=clean(sheet.title), index=start)
                sections.append(Section(text=budget.take(text), location=location))
        workbook.close()
    except Unexportable:
        raise
    except Exception:
        raise Unexportable("spreadsheet export is unreadable") from None
    return sections


def presentation(data: bytes, limit: int) -> list[Section]:
    budget = _Budget(limit)
    sections: list[Section] = []
    try:
        with _archive(data) as archive:
            relations = ElementTree.fromstring(archive.read("ppt/_rels/presentation.xml.rels"))
            targets = {item.get("Id"): item.get("Target", "") for item in relations}
            deck = ElementTree.fromstring(archive.read("ppt/presentation.xml"))
            for number, slide in enumerate(deck.iter(f"{_PRESENTATION}sldId"), start=1):
                target = targets[slide.get(f"{_RELATIONSHIP}id")]
                root = ElementTree.fromstring(archive.read(f"ppt/{target.lstrip('/')}"))
                paragraphs = (
                    "".join(run.text or "" for run in paragraph.iter(f"{_DRAWING}t")).strip()
                    for paragraph in root.iter(f"{_DRAWING}p")
                )
                text = "\n".join(paragraph for paragraph in paragraphs if paragraph)
                if text:
                    location = Location(kind="slide", index=number)
                    sections.append(Section(text=budget.take(text), location=location))
    except Unexportable:
        raise
    except (KeyError, ElementTree.ParseError, zipfile.BadZipFile):
        raise Unexportable("presentation export is unreadable") from None
    return sections


#: Google-native MIME type -> (export MIME type, parser, source type, URL part).
FORMATS: dict[str, tuple[str, Callable[[bytes, int], list[Section]], str, str]] = {
    MIME_TYPES["gdoc"]: ("text/markdown", document, "google-doc", "document"),
    MIME_TYPES["gsheet"]: (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        spreadsheet,
        "google-sheet",
        "spreadsheets",
    ),
    MIME_TYPES["gslides"]: (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        presentation,
        "google-slides",
        "presentation",
    ),
}
