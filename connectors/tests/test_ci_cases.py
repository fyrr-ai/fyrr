"""The ownership gate must reject gaps, overlaps and empty proof rows."""

import importlib.machinery
import importlib.util
import unittest
from pathlib import Path

loader = importlib.machinery.SourceFileLoader(
    "ci_cases", str(Path(__file__).parents[2] / "scripts/ci-cases")
)
spec = importlib.util.spec_from_loader(loader.name, loader)
ci_cases = importlib.util.module_from_spec(spec)
loader.exec_module(ci_cases)


class OwnershipTests(unittest.TestCase):
    def test_missing_case(self):
        with self.assertRaisesRegex(ValueError, "unowned cases"):
            ci_cases.validate({"unit": {"cases": ["a"]}}, {"a": False, "b": False})

    def test_duplicate_owner(self):
        with self.assertRaisesRegex(ValueError, "duplicate owner"):
            ci_cases.validate({"unit": {"cases": ["a"]}, "s3": {"cases": ["a"]}}, {"a": False})

    def test_zero_case_row(self):
        with self.assertRaisesRegex(ValueError, "zero-case row"):
            ci_cases.validate({"empty": {"cases": []}}, {})

    def test_named_child_requires_executed_parent(self):
        with self.assertRaisesRegex(ValueError, "parent is not executed"):
            ci_cases.validate(
                {"unit": {"cases": ["a"], "children": {"b": ["c"]}}}, {"a": False, "b": True}
            )

    def test_named_child_has_one_owner(self):
        ci_cases.validate(
            {"unit": {"cases": ["a", "c"], "children": {"b": ["a", "c"]}}},
            {"a": False, "b": True, "c": False},
        )


if __name__ == "__main__":
    unittest.main()
