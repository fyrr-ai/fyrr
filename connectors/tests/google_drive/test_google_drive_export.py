"""The export parsers: located sections from Docs, Sheets and Slides exports."""

import io

import pytest
from conftest import pptx, xlsx

from fyrr_connectors.arrow import Location
from fyrr_connectors.google_drive import export
from fyrr_connectors.google_drive.export import Unexportable

LIMIT = 1 << 20


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


def _texts(sections: list) -> list[str]:
    return [section.text for section in sections]


def test_a_docs_title_opens_its_first_section_above_the_text() -> None:
    plain = export.document(b"Body.\n\n# Next\n\nRest.\n", LIMIT, "Quokka plan")
    assert _texts(plain) == ["Quokka plan\n\nBody.", "# Next\n\nRest."]
    headed = export.document(b"# Intro\n\nBody.\n\n# Next\n\nRest.\n", LIMIT, "Quokka plan")
    assert _texts(headed) == ["Quokka plan\n\n# Intro\n\nBody.", "# Next\n\nRest."]
    assert headed[0].location.label == "Intro"


def test_a_docs_title_equal_to_its_first_line_is_not_repeated() -> None:
    for data in (b"  Quokka plan \nBody.\n", b"# Quokka plan\n\nBody.\n"):
        assert _texts(export.document(data, LIMIT, "Quokka plan"))[0] == data.decode().strip()
    assert _texts(export.document(b"Quokka plan is here.\n", LIMIT, "Quokka plan")) == [
        "Quokka plan\n\nQuokka plan is here."
    ]
    # Only a heading is told from its marker: this line is text.
    assert _texts(export.document(b"#Quokka plan\n", LIMIT, "Quokka plan")) == [
        "Quokka plan\n\n#Quokka plan"
    ]


def test_a_missing_title_changes_nothing() -> None:
    assert _texts(export.document(b"Body.\n", LIMIT, None)) == ["Body."]
    assert _texts(export.spreadsheet(xlsx({"S": [["h"], ["x"]]}), LIMIT, None)) == ["h\nx"]
    assert _texts(export.presentation(pptx([["Slide"]]), LIMIT, None)) == ["Slide"]


def test_a_file_without_text_stays_without_sections_whatever_its_title() -> None:
    assert export.document(b"\n", LIMIT, "Quokka plan") == []
    assert export.spreadsheet(xlsx({"S": []}), LIMIT, "Quokka plan") == []
    assert export.presentation(pptx([]), LIMIT, "Quokka plan") == []


def test_a_sheet_or_deck_title_is_its_own_section_before_the_first_sheet_or_slide() -> None:
    sheet = export.spreadsheet(xlsx({"Costs": [["item", "eur"], ["rent", 1200]]}), LIMIT, "Quokka")
    assert _texts(sheet) == ["Quokka", "item | eur\nrent | 1200"]
    assert sheet[0].location == Location(kind="title")
    assert sheet[1].location == Location(kind="sheet", label="Costs", index=1)
    deck = export.presentation(pptx([["Roadmap", "2026"]]), LIMIT, "Quokka")
    assert _texts(deck) == ["Quokka", "Roadmap\n2026"]
    assert deck[1].location == Location(kind="slide", index=1)


def test_the_title_counts_toward_the_limit_in_bytes() -> None:
    title, body = "Žluťoučký kůň", "úpěl ďábelské ódy"
    exact = len(title.encode()) + 2 + len(body.encode())
    assert _texts(export.document(body.encode(), exact, title)) == [f"{title}\n\n{body}"]
    with pytest.raises(Unexportable):
        export.document(body.encode(), exact - 1, title)
    cells = len(title.encode()) + len(body.encode())
    assert len(export.spreadsheet(xlsx({"S": [[body]]}), cells, title)) == 2
    with pytest.raises(Unexportable):
        export.spreadsheet(xlsx({"S": [[body]]}), cells - 1, title)
    assert len(export.presentation(pptx([[body]]), cells, title)) == 2
    with pytest.raises(Unexportable):
        export.presentation(pptx([[body]]), cells - 1, title)
