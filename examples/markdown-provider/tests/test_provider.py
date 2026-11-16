"""The example provider, checked with the SDK's provider test kit."""

import json
import os
import shutil
from pathlib import Path

import pytest
from fyrr_connectors import testkit

DATA = Path(__file__).parent / "data"
GOLDEN = Path(__file__).parent / "golden"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """The test files with fixed modification times, so output is stable."""
    target = tmp_path / "docs"
    shutil.copytree(DATA, target)
    for index, path in enumerate(sorted(target.rglob("*.md"))):
        stamp = 1_759_225_000 + index * 60
        os.utime(path, (stamp, stamp))
    return target


def _stable(result: testkit.SyncResult, root: Path) -> list[dict]:
    """Replace the temporary path in URIs so the golden file is portable."""
    text = json.dumps(result.documents)
    return json.loads(text.replace(root.resolve().as_uri(), "file:///docs"))


def test_metadata_matches_the_published_schema() -> None:
    info = testkit.check_provider("markdown-dir")
    assert info["sync_kinds"] == ["content", "reconcile"]
    assert info["connection_types"] == ["fixed"]


def test_content_matches_the_golden_output(root: Path) -> None:
    result = testkit.run_sync("markdown-dir", "content", scope={"root": str(root)})
    result.documents = _stable(result, root)
    testkit.assert_golden(result, GOLDEN / "content.json")
    assert sum(batch.num_rows for batch in result.record_batches) == 2


def test_content_resumes_after_its_cursor(root: Path) -> None:
    first = testkit.run_sync(
        "markdown-dir", "content", scope={"root": str(root)}, settings={"batch_size": 1}
    )
    assert len(first.documents) == 2
    resumed = testkit.run_sync(
        "markdown-dir",
        "content",
        scope={"root": str(root)},
        cursor=first.documents[0]["cursor"]["end"],
    )
    assert [change["root_id"] for change in resumed.documents[0]["body"]["changes"]] == [
        first.documents[1]["body"]["changes"][0]["root_id"]
    ]


def test_listing_is_complete(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = testkit.run_sync("markdown-dir", "reconcile", scope={"root": str(root)})
    body = result.documents[0]["body"]
    assert body["root_ids"] == ["guides/restore.md", "onboarding.md"]
    assert body["last"] and body["complete"]

    empty = root / "empty"
    empty.mkdir()
    result = testkit.run_sync("markdown-dir", "reconcile", scope={"root": str(empty)})
    assert result.documents[0]["body"]["complete"]
    assert result.documents[0]["body"]["root_ids"] == []
    for unavailable in (root / "missing", root / "onboarding.md"):
        with pytest.raises(ValueError, match="source_unavailable"):
            testkit.run_sync("markdown-dir", "reconcile", scope={"root": str(unavailable)})

    linked = root / "linked"
    linked.symlink_to(root / "guides", target_is_directory=True)
    try:
        with pytest.raises(ValueError, match="source_unavailable"):
            testkit.run_sync(
                "markdown-dir", "reconcile", scope={"root": str(root), "pattern": "linked/*.md"}
            )
    finally:
        linked.unlink()

    scandir = os.scandir

    def denied(path):
        if Path(path) == root / "guides":
            raise PermissionError("unavailable fixture directory")
        return scandir(path)

    monkeypatch.setattr(os, "scandir", denied)
    with pytest.raises(ValueError, match="source_unavailable"):
        testkit.run_sync("markdown-dir", "reconcile", scope={"root": str(root)})


def test_invalid_scope_is_refused(root: Path) -> None:
    with pytest.raises(AssertionError, match="scope does not match"):
        testkit.run_sync("markdown-dir", "content", scope={"directory": "/srv"})

    for pattern in ("", "/absolute/*.md", "../*.md", "**/../*.md", "**/", "*.md\0"):
        with pytest.raises(ValueError, match="scope_pattern_invalid"):
            testkit.run_sync(
                "markdown-dir", "reconcile", scope={"root": str(root), "pattern": pattern}
            )
