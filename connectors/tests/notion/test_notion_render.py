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


def _titled(title: str | None, *blocks: tuple[int, dict], limit: int = 10_000) -> list[tuple]:
    page = Text(limit)
    page.title(title)
    for indent, block in blocks:
        page.block(indent, block)
    return [(section.location.label, section.text) for section in page.sections()]


def test_the_title_opens_the_first_section_above_the_body() -> None:
    body = (0, _block("paragraph", "Body."))
    assert _titled("Quokka marshaller", body) == [(None, "Quokka marshaller\n\nBody.")]
    # With a heading first, the title joins that heading's section; later ones stay as they are.
    assert _titled(
        "Quokka marshaller",
        (0, _block("heading_1", "Goals")),
        body,
        (0, _block("heading_2", "More")),
        (0, _block("paragraph", "Rest.")),
    ) == [("Goals", "Quokka marshaller\n\n# Goals\n\nBody."), ("More", "## More\n\nRest.")]


def test_a_title_equal_to_the_first_line_is_not_repeated() -> None:
    assert _titled("Quokka", (0, _block("paragraph", " Quokka \nmore"))) == [
        (None, " Quokka \nmore")
    ]
    assert _titled("Quokka", (0, _block("heading_1", "Quokka"))) == [("Quokka", "# Quokka")]
    assert _titled("Quokka", (0, _block("paragraph", "Quokka is here."))) == [
        (None, "Quokka\n\nQuokka is here.")
    ]


def test_a_missing_title_or_an_empty_body_changes_nothing() -> None:
    body = (0, _block("paragraph", "Body."))
    assert _titled(None, body) == [(None, "Body.")]
    assert _titled("", body) == [(None, "Body.")]
    assert _titled("Quokka") == [], "a page without text is not published"


def test_the_title_counts_toward_the_limit_in_bytes() -> None:
    title, body = "Žluťoučký kůň", "úpěl ďábelské ódy"
    exact = len(title.encode()) + 2 + len(body.encode())
    assert len(title) + 2 + len(body) < exact, "the title is multibyte"
    assert _titled(title, (0, _block("paragraph", body)), limit=exact) == [
        (None, f"{title}\n\n{body}")
    ]
    page = Text(exact - 1)
    page.title(title)
    page.block(0, _block("paragraph", body))
    with pytest.raises(Oversized):
        page.sections()
    # A title that does not fit is noticed where the first section closes: the rest is not read.
    page = Text(4)
    page.title("T")
    page.block(0, _block("paragraph", "body"))
    assert not page.oversized
    page.block(0, _block("heading_1", "Next"))
    assert page.oversized
