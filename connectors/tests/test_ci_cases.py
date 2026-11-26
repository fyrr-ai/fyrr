"""The CI ownership guard must not let a test fall out of every row."""

import importlib.machinery
import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts/ci-cases"
loader = importlib.machinery.SourceFileLoader("ci_cases", str(SCRIPT))
ci_cases = importlib.util.module_from_spec(importlib.util.spec_from_loader("ci_cases", loader))
loader.exec_module(ci_cases)

SDK = "python:connectors/tests/test_a.py::test_a"
LISTING = "python:examples/markdown-provider/tests/test_listing.py::test_listing_is_complete"
EXAMPLE = "python:examples/markdown-provider/tests/test_other.py::test_other"
QUICK = {"tier": "quick"}


def python_rows(moved):
    return {
        "python-sdk": {
            "title": "sdk",
            "kind": "python",
            "owns": {"python:connectors/": QUICK},
            **({"cases": {LISTING: QUICK}} if moved else {}),
        },
        "python-example": {
            "title": "example",
            "kind": "python",
            "owns": {"python:examples/markdown-provider/": QUICK},
        },
    }


def test_a_row_runs_every_python_project_it_owns_tests_from():
    manifest = python_rows(moved=True)
    actual = {SDK: False, LISTING: False, EXAMPLE: False}
    ci_cases.validate(manifest, actual)
    rows = ci_cases.expand(manifest, actual)[0]
    assert set(rows["python-sdk"]["cases"]) == {SDK, LISTING}
    assert set(rows["python-example"]["cases"]) == {EXAMPLE}
    for row, spec in manifest.items():
        directories = ci_cases.python_directories(spec)
        for case in rows[row]["cases"]:
            assert any(case.startswith(f"python:{directory}/") for directory in directories)


def test_a_case_cannot_be_both_a_case_and_a_child_of_one_row():
    case = "full:fyrr:test:dense_path/mod::parent"
    manifest = {
        "units": {
            "title": "units",
            "kind": "rust",
            "owns": {"full:fyrr:test:dense_path/mod::": {"tier": "quick", "seconds": 0}},
            "cases": {case: {"tier": "full", "seconds": 0}},
            "children": {case: {"tier": "full", "parents": [case]}},
        }
    }
    with pytest.raises(ValueError, match="duplicate owner"):
        ci_cases.validate(manifest, {case: False})
