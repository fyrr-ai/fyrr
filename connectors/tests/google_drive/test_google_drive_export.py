"""The export parsers: located sections from Docs, Sheets and Slides exports."""

import io

import pytest
from conftest import pptx, xlsx

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
