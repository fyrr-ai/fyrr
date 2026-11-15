"""Blocks and properties as text: pure, no request."""

import pytest

from fyrr_connectors.notion.render import Oversized, Text, clean, plain, property_lines, title


def _rich(text: str) -> list[dict]:
    return [{"type": "text", "plain_text": text}]


def _block(kind: str, text: str = "", **data) -> dict:
    return {"type": kind, kind: {"rich_text": _rich(text), **data}}


def _sections(*blocks: tuple[int, dict], limit: int = 10_000) -> list[tuple[str | None, str]]:
    page = Text(limit)
    for indent, block in blocks:
        page.block(indent, block)
    return [(section.location.label, section.text) for section in page.sections()]


def test_rich_text_is_its_plain_text() -> None:
    mention = {"type": "mention", "plain_text": "@Ada", "mention": {"user": {"id": "u"}}}
    assert plain([*_rich("Ask "), mention, {"type": "equation", "plain_text": "e=mc^2"}]) == (
        "Ask @Ada" + "e=mc^2"
    )
    assert plain(None) == "" and plain([{"plain_text": 7}, "x"]) == ""


def test_a_page_splits_at_its_headings_and_keeps_light_marks() -> None:
    sections = _sections(
        (0, _block("paragraph", "Before any heading.")),
        (0, _block("heading_1", "Plan")),
        (0, _block("bulleted_list_item", "first")),
        (1, _block("bulleted_list_item", "nested")),
        (0, _block("bulleted_list_item", "second")),
        (0, _block("numbered_list_item", "one")),
        (0, _block("numbered_list_item", "two")),
        (0, _block("paragraph", "A break.")),
        (0, _block("numbered_list_item", "one again")),
        (0, _block("to_do", "done", checked=True)),
        (0, _block("to_do", "open")),
        (0, _block("heading_2", "Notes")),
        (0, _block("quote", "Said\nonce.")),
        (0, _block("code", "print(1)", language="python")),
        (0, {"type": "equation", "equation": {"expression": "a^2"}}),
        (0, {"type": "table_row", "table_row": {"cells": [_rich("a"), _rich("b")]}}),
        (0, {"type": "table_row", "table_row": {"cells": [_rich("1"), _rich("2")]}}),
    )
    assert sections == [
        (None, "Before any heading."),
        (
            "Plan",
            "# Plan\n\n- first\n  - nested\n- second\n1. one\n2. two\n\nA break.\n\n"
            "1. one again\n- [x] done\n- [ ] open",
        ),
        ("Notes", "## Notes\n\n> Said\n> once.\n\n```python\nprint(1)\n```\n\na^2\n\na | b\n1 | 2"),
    ]


def test_sections_have_distinct_locations() -> None:
    page = Text(10_000)
    page.properties({"Name": {"type": "title", "title": _rich("Row")}})
    for _ in range(2):
        page.block(0, _block("heading_3", "Same"))
        page.block(0, _block("paragraph", "Text."))
    locations = [(s.location.kind, s.location.label, s.location.index) for s in page.sections()]
    assert locations == [("properties", None, 0), ("heading", "Same", 1), ("heading", "Same", 2)]


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


def test_blocks_without_text_give_nothing() -> None:
    silent = [
        {"type": "divider", "divider": {}},
        {"type": "unsupported", "unsupported": {}},
        {"type": "child_page", "child_page": {"title": "Not this page's text"}},
        {"type": "mystery"},
        {"type": "heading_1", "heading_1": {"rich_text": _rich("  ")}},
        {"type": 5},
        {},
    ]
    assert _sections(*[(0, block) for block in silent]) == []
    assert _sections((0, {"type": "new_kind", "new_kind": {"rich_text": _rich("Kept.")}})) == [
        (None, "Kept.")
    ]


def test_every_property_type_has_its_text() -> None:
    properties = {
        "Name": {"type": "title", "title": _rich("Fix the login")},
        "Notes": {"type": "rich_text", "rich_text": _rich("Two\nlines")},
        "Points": {"type": "number", "number": 3.0},
        "Ratio": {"type": "number", "number": 0.5},
        "Stage": {"type": "status", "status": {"name": "Doing"}},
        "Kind": {"type": "select", "select": {"name": "Bug"}},
        "Tags": {"type": "multi_select", "multi_select": [{"name": "auth"}, {"name": "web"}]},
        "Due": {"type": "date", "date": {"start": "2026-02-01", "end": "2026-02-03"}},
        "Owner": {"type": "people", "people": [{"id": "u1", "name": "Ada"}, {"id": "u2"}]},
        "Files": {"type": "files", "files": [{"name": "spec.pdf", "file": {"url": "s3://x"}}]},
        "Done": {"type": "checkbox", "checkbox": False},
        "Link": {"type": "url", "url": "https://example.com/1"},
        "Mail": {"type": "email", "email": "team@example.com"},
        "Sum": {"type": "formula", "formula": {"type": "number", "number": 7}},
        "Late": {"type": "formula", "formula": {"type": "boolean", "boolean": True}},
        "Seen": {
            "type": "rollup",
            "rollup": {
                "type": "array",
                "array": [{"type": "title", "title": _rich("A")}, {"type": "number", "number": 2}],
            },
        },
        "Total": {"type": "rollup", "rollup": {"type": "number", "number": 9}},
        "Key": {"type": "unique_id", "unique_id": {"prefix": "ENG", "number": 12}},
        "By": {"type": "created_by", "created_by": {"id": "u1", "name": "Ada"}},
        "Blocks": {"type": "relation", "relation": [{"id": "p1"}]},
        "Empty": {"type": "rich_text", "rich_text": []},
        "Unset": {"type": "select", "select": None},
        "Go": {"type": "button", "button": {}},
        "Odd": "not a property",
    }
    assert property_lines(properties) == [
        "Name: Fix the login",
        "By: Ada",
        "Done: no",
        "Due: 2026-02-01 → 2026-02-03",
        "Files: spec.pdf",
        "Key: ENG-12",
        "Kind: Bug",
        "Late: yes",
        "Link: https://example.com/1",
        "Mail: team@example.com",
        "Notes: Two lines",
        "Owner: Ada",
        "Points: 3",
        "Ratio: 0.5",
        "Seen: A, 2",
        "Stage: Doing",
        "Sum: 7",
        "Tags: auth, web",
        "Total: 9",
    ]
    assert title({"properties": properties}) == "Fix the login"
    assert title({"properties": {"Name": {"type": "title", "title": []}}}) is None
    assert title({}) is None and property_lines(None) == []


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
