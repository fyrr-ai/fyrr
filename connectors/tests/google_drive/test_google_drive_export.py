"""The export parsers: located sections from Docs, Sheets and Slides exports."""

import io

import pytest
from conftest import pptx, xlsx

from fyrr_connectors.google_drive import export
from fyrr_connectors.google_drive.export import Unexportable

LIMIT = 1 << 20

MARKDOWN = """Intro before any heading.

# Plan

First paragraph.

```
# not a heading
```

## Budget \\& risks

Second ![][image1] paragraph.

[image1]: <data:image/png;base64,AAAA>
"""


def _located(sections: list) -> list[tuple]:
    return [(s.location.kind, s.location.label, s.location.index, s.text) for s in sections]


def test_a_document_splits_at_headings_and_drops_embedded_images() -> None:
    assert _located(export.document(MARKDOWN.encode(), LIMIT)) == [
        ("heading", None, 0, "Intro before any heading."),
        ("heading", "Plan", 1, "# Plan\n\nFirst paragraph.\n\n```\n# not a heading\n```"),
        ("heading", "Budget \\& risks", 2, "## Budget \\& risks\n\nSecond  paragraph."),
    ]
    assert export.document(b"\n\n", LIMIT) == []


def test_a_spreadsheet_keeps_every_sheet_and_repeats_the_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(export, "GROUP_BYTES", 40)
    data = xlsx(
        {
            "Costs": [["item", "eur", None], [], ["rent", 1200.0], ["power", 80.5], ["tea", 7]],
            "Empty": [],
            "People": [["name"], ["Ada\nL."]],
        }
    )
    assert _located(export.spreadsheet(data, LIMIT)) == [
        ("sheet", "Costs", 1, "item | eur\nrent | 1200\npower | 80.5"),
        ("sheet", "Costs", 5, "item | eur\ntea | 7"),
        ("sheet", "People", 1, "name\nAda L."),
    ]


def test_a_presentation_has_one_section_per_slide_in_deck_order() -> None:
    data = pptx([["Roadmap", "2026"], [], ["Q1 &amp; Q2", "ship"]])
    assert _located(export.presentation(data, LIMIT)) == [
        ("slide", None, 1, "Roadmap\n2026"),
        ("slide", None, 3, "Q1 & Q2\nship"),
    ]


@pytest.mark.parametrize(
    ("parse", "data"),
    [
        (export.document, b"\xff\xfe broken"),
        (export.document, b"# big\n" + b"x" * 200),
        (export.spreadsheet, b"not a workbook"),
        (export.spreadsheet, xlsx({"S": [["h"], ["x" * 200]]})),
        (export.presentation, b"not a deck"),
        (export.presentation, xlsx({"S": [["a workbook is not a deck"]]})),
        (export.presentation, pptx([["x" * 200]])),
    ],
    ids=[
        "document-invalid-encoding",
        "document-oversized",
        "spreadsheet-invalid",
        "spreadsheet-oversized",
        "presentation-invalid",
        "presentation-wrong-format",
        "presentation-oversized",
    ],
)
def test_unreadable_or_oversized_exports_are_unexportable(parse, data: bytes) -> None:
    with pytest.raises(Unexportable):
        parse(data, 100)


def test_clean_yields_contract_valid_single_line_values() -> None:
    assert export.clean("  a\tb\n ") == "a b"
    assert export.clean("\n ") is None
    assert export.clean("é" * 600) == "é" * 512


def test_a_sparse_sheet_is_refused_before_its_empty_cells_are_walked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openpyxl import Workbook

    workbook = Workbook()
    workbook.active["A1"] = "header"
    workbook.active.cell(row=1_048_576, column=2_000, value="far away")
    data = io.BytesIO()
    workbook.save(data)
    assert len(data.getvalue()) < 10_000, "tiny as a file, two billion cells as a grid"
    with pytest.raises(Unexportable):
        export.spreadsheet(data.getvalue(), LIMIT)
    monkeypatch.setattr(export, "WORKBOOK_MAX_CELLS", 5)
    assert export.spreadsheet(xlsx({"S": [["a", "b"], ["c", "d"]]}), LIMIT)
    with pytest.raises(Unexportable):
        export.spreadsheet(xlsx({"S": [["a", "b"], ["c", "d"]], "T": [["e", "f"]]}), LIMIT)
