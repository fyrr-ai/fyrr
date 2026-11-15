"""Blocks and properties as text: pure, no request."""

import pytest

from fyrr_connectors.notion.render import Oversized, Text, clean, property_lines


def _rich(text: str) -> list[dict]:
    return [{"type": "text", "plain_text": text}]


def _block(kind: str, text: str = "", **data) -> dict:
    return {"type": kind, kind: {"rich_text": _rich(text), **data}}


def _sections(*blocks: tuple[int, dict], limit: int = 10_000) -> list[tuple[str | None, str]]:
    page = Text(limit)
    for indent, block in blocks:
        page.block(indent, block)
    return [(section.location.label, section.text) for section in page.sections()]


def test_links_give_their_url_and_files_only_their_caption() -> None:
    link = {
        "type": "bookmark",
        "bookmark": {"url": "https://example.com", "caption": _rich("Docs")},
    }
    image = {"type": "image", "image": {"caption": _rich("A chart"), "file": {"url": "s3://x"}}}
    bare = {"type": "pdf", "pdf": {"caption": [], "file": {"url": "s3://secret"}}}
    assert _sections((0, link), (0, image), (0, bare)) == [
        (None, "Docs https://example.com\n\nA chart")
    ]


def test_a_value_nested_without_end_stops() -> None:
    nested: dict = {"type": "number", "number": 1}
    for _ in range(50):
        nested = {"type": "formula", "formula": nested}
    assert property_lines({"Deep": nested}) == []


def test_text_over_the_limit_keeps_nothing_and_is_oversized() -> None:
    page = Text(64)
    page.block(0, _block("paragraph", "short"))
    page.block(0, _block("paragraph", "x" * 100))
    assert page.oversized
    page.block(0, _block("paragraph", "ignored"))
    with pytest.raises(Oversized):
        page.sections()


def test_clean_makes_a_contract_valid_value() -> None:
    assert clean("  a\tb\n ") == "a b"
    assert clean("\x00") is None
    assert clean("é" * 600) == "é" * 512
