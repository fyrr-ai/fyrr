"""Scope and incremental changes: what search hints at, what the walk finds,
and what happens when Notion changes in the middle of a sync."""

import json
import re

import pytest
from fake_notion import FakeNotion, block, ident
from notion_kit import Index


def _note(words: str) -> list[dict]:
    return [block("paragraph", words)]


def _reads(notion: FakeNotion, name: str) -> int:
    """How often the page's own block list was asked for."""
    return notion.count(f"/blocks/{ident(name)}/children$")


def _family(notion: FakeNotion) -> None:
    notion.page("plan", "root", blocks=_note("Plan."))
    notion.page("mid", "root", blocks=_note("Middle."))
    notion.page("leaf", "mid", blocks=_note("Leaf."))


def test_only_pages_under_a_root_are_read(notion: FakeNotion, index: Index) -> None:
    _family(notion)
    notion.page("elsewhere", None, blocks=_note("Outside."))
    notion.page("under-elsewhere", "elsewhere", blocks=_note("Outside too."))
    assert sorted(index.sync()) == ["leaf", "mid", "plan"]
    notion.page("under-elsewhere", blocks=_note("Edited outside."))
    assert index.sync() == {}
    assert _reads(notion, "elsewhere") == 0 and _reads(notion, "under-elsewhere") == 0
    assert index.listing() == ["leaf", "mid", "plan", "root"]


def test_a_page_moved_out_of_the_roots_leaves_at_the_listing(
    notion: FakeNotion, index: Index
) -> None:
    _family(notion)
    index.sync()
    notion.move("mid", None)
    assert index.sync() == {}, "no upsert of a page outside the roots, and no delete yet"
    notion.page("leaf", blocks=_note("Edited after the move."))
    assert index.sync() == {}
    assert index.listing() == ["plan", "root"]


def test_a_subtree_moved_into_scope_is_published_whole(notion: FakeNotion, index: Index) -> None:
    _family(notion)
    notion.page("box", None, blocks=_note("Box."))
    notion.page("inner", "box")
    notion.page("deepest", "inner", blocks=_note("Deepest."))
    notion.database("records", "box")
    for number in range(5):
        notion.row(f"record-{number}", "records", title=f"Record {number}")
    index.sync()
    notion._clock += 600  # Hours pass: none of it is within the overlap any more.
    notion.move("box", "root")
    seen = index.sync()
    assert sorted(seen) == ["box", "deepest", *(f"record-{number}" for number in range(5))]
    assert json.loads(index.cursor)["walks"] == []
    assert index.sync() == {}
    assert _reads(notion, "deepest") >= 1, "unchanged descendants may be walked, not republished"


def test_the_watermark_moves_only_with_the_last_batch(notion: FakeNotion, index: Index) -> None:
    _family(notion)
    index.sync()
    before = json.loads(index.cursor)["since"]
    for name in ("plan", "mid", "leaf"):
        notion.page(name, blocks=_note(f"{name}, edited."))
    index.limits["max_rows"] = 1
    cursors = [json.loads(batch.cursor)["since"] for batch in index.run(cursor=index.cursor)]
    assert cursors == [before, before, notion.pages[ident("leaf")]["edited"]]

    run = index.run(cursor=index.cursor)
    assert sorted(index.apply(next(run))) == ["leaf"]
    run.close()
    assert json.loads(index.cursor)["since"] == before
    assert sorted(index.sync()) == ["mid", "plan"], "search is read again; the lookup skips leaf"


def test_a_reset_cursor_scans_again_without_duplicates_or_lost_deletes(
    notion: FakeNotion, index: Index
) -> None:
    _family(notion)
    index.sync()
    notion.search_trashed = False
    notion.trash("plan")
    notion.page("mid", blocks=_note("Edited."))
    index.cursor = None
    reads = _reads(notion, "leaf")
    assert sorted(index.sync()) == ["mid"], "what is current is not published again"
    assert _reads(notion, "leaf") == reads + 1, "but everything is walked again"
    assert index.listing() == ["leaf", "mid", "root"]


def test_the_cursor_holds_page_ids_and_a_timestamp_only(notion: FakeNotion, index: Index) -> None:
    notion.user("grace", "Grace Example <grace@example.com>")
    _family(notion)
    notion.page("signed", "root", blocks=_note("By Grace."), author="grace")
    index.limits["max_rows"] = 1
    cursors = [batch.cursor for batch in index.run()]
    assert len(cursors) == 4
    for cursor in cursors:
        state = json.loads(cursor)
        assert set(state) == {"v", "since", "roots", "walks", "path", "root_revisions"}
        assert set(state["root_revisions"]) <= set(state["roots"])
        assert all(re.fullmatch(r"[0-9a-f]{32}", rev) for rev in state["root_revisions"].values())
        assert "Example" not in cursor and "@" not in cursor and "staff" not in cursor
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:00\.000Z", state["since"])
        walks = [node for kind, node in state["walks"] if kind == "page"]
        assert len(walks) == len(state["walks"])
        assert {*state["roots"], *walks, *state["path"]} <= set(notion.pages)


@pytest.mark.parametrize("cursor", ["", "{}", '{"v":2}', "[1]", '{"v":1,"since":7}', "no json"])
def test_an_invalid_cursor_is_refused(notion: FakeNotion, index: Index, cursor: str) -> None:
    with pytest.raises(ValueError, match="notion_cursor_invalid"):
        list(index.run(cursor=cursor))


def test_too_many_roots_for_the_cursor_fail_instead_of_being_cut(
    notion: FakeNotion, index: Index
) -> None:
    index.roots = [f"root-{number}" for number in range(300)]
    for root in index.roots:
        notion.page(root, None)
    with pytest.raises(ValueError, match="notion_cursor_too_large"):
        index.sync()
    assert index.cursor is None
