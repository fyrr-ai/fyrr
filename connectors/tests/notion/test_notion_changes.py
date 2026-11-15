"""Scope and incremental changes: what search hints at, what the walk finds,
and what happens when Notion changes in the middle of a sync."""

import json
import re
from datetime import timedelta

import pytest
from fake_notion import FakeNotion, block, ident
from notion_kit import Index, text

from fyrr_connectors.notion import connector
from fyrr_connectors.notion.client import ATTEMPTS, ApiError


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


def test_a_root_added_later_is_walked_and_a_removed_one_leaves_at_the_listing(
    notion: FakeNotion, index: Index
) -> None:
    _family(notion)
    notion.page("second", None, title="Second root")
    notion.page("quiet", "second", blocks=_note("Never in search."))
    notion.hidden.add(ident("quiet"))
    assert sorted(index.sync()) == ["leaf", "mid", "plan"]

    index.roots = ["root", "second"]
    seen = index.sync()
    assert sorted(seen) == ["quiet"]
    assert seen["quiet"]["metadata"]["project"] == "Second root"
    assert _reads(notion, "plan") == 1, "the roots already walked are not walked again"

    index.roots = ["second"]
    assert index.sync() == {}, "narrowing deletes nothing by itself"
    assert index.listing() == ["quiet", "second"]
    notion.page("plan", blocks=_note("Edited outside the scope."))
    assert index.sync() == {}


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


def test_a_database_moved_under_a_published_page_is_read(notion: FakeNotion, index: Index) -> None:
    _family(notion)
    notion.page("box", None)
    notion.database("records", "box")
    for number in range(3):
        notion.row(f"record-{number}", "records", title=f"Record {number}")
    notion.database("known", "plan")
    notion.row("known-row", "known", title="Known")
    index.sync()
    notion._clock += 600
    queries = notion.count("/query$")
    notion.move("records", "plan")
    # The page reports the change (its block list grew); the rows do not.
    assert sorted(index.sync()) == ["plan", "record-0", "record-1", "record-2"]
    # Every row page is examined, including the already published database.
    assert notion.count("/query$") - queries >= 3


def test_a_moved_subtree_survives_a_kill_between_its_batches(
    notion: FakeNotion, index: Index
) -> None:
    _family(notion)
    notion.page("box", None, blocks=_note("Box."))
    for number in range(4):
        notion.page(f"item-{number}", "box", blocks=_note(f"Item {number}."))
    index.sync()
    notion._clock += 600
    notion.move("box", "root")
    notion.page("box", blocks=_note("Box, moved and edited."))  # Search shows it first.
    index.limits["max_rows"] = 1
    run = index.run(cursor=index.cursor)
    first = [index.apply(next(run)) for _ in range(2)]
    run.close()
    assert [sorted(batch) for batch in first] == [["box"], ["item-0"]]
    state = json.loads(index.cursor)
    assert [node for _, node in state["walks"]] == [ident(f"item-{n}") for n in (1, 2, 3)]
    assert sorted(index.sync()) == ["item-1", "item-2", "item-3"]
    assert json.loads(index.cursor)["walks"] == []


def test_more_new_children_than_the_cursor_queues_walks_the_page_itself(
    notion: FakeNotion, index: Index, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(connector, "MAX_WALKS", 2)
    _family(notion)
    notion.page("box", None, blocks=_note("Box."))
    for number in range(4):
        notion.page(f"item-{number}", "box", blocks=_note(f"Item {number}."))
    index.sync()
    notion._clock += 600
    notion.move("box", "root")
    notion.page("box", blocks=_note("Box, moved and edited."))
    index.limits["max_rows"] = 1
    run = index.run(cursor=index.cursor)
    index.apply(next(run))
    index.apply(next(run))
    run.close()
    assert json.loads(index.cursor)["walks"] == [["page", ident("box")]]
    assert sorted(index.sync()) == ["item-1", "item-2", "item-3"]


def test_search_is_read_down_to_the_watermark_only(notion: FakeNotion, index: Index) -> None:
    for number in range(9):
        notion.page(f"old-{number}", "root", blocks=_note("Old."))
    index.sync()
    notion._clock += 600
    notion.page("old-3", blocks=_note("New."))
    assert sorted(index.sync()) == ["old-3"]
    notion._clock += 600
    searches = notion.count("/search$")
    assert index.sync() == {}
    # One answer holds the page at the watermark, the next nothing that new.
    assert notion.count("/search$") - searches == 2


def test_search_behind_the_workspace_is_caught_up_within_the_overlap(
    notion: FakeNotion, index: Index
) -> None:
    _family(notion)
    index.sync()
    notion.lag = True
    notion.page("plan", blocks=_note("Edited, but search does not know yet."))
    notion.lag = False
    notion.page("mid", blocks=_note("Edited later; search knows at once."))
    assert sorted(index.sync()) == ["mid"]
    assert json.loads(index.cursor)["since"] > notion.pages[ident("plan")]["edited"]
    notion.reindex()
    assert text(index.sync()["plan"]) == "Edited, but search does not know yet."


def test_a_stale_search_entry_is_read_again_by_id(notion: FakeNotion, index: Index) -> None:
    _family(notion)
    index.sync()
    notion.page("plan", blocks=_note("First edit."))
    notion.lag = True
    notion.page("plan", title="Renamed", blocks=_note("Second edit."))
    changed = index.sync()["plan"]
    assert text(changed) == "Second edit." and changed["metadata"]["title"] == "Renamed"
    edited = notion.pages[ident("plan")]["edited"]
    assert changed["metadata"]["updated_at"] == edited.replace(".000Z", "Z")
    notion.lag = False
    notion.reindex()
    assert index.sync() == {}, "what was published is the page as it is, not as search had it"


def test_a_page_read_within_its_edit_minute_is_read_again(notion: FakeNotion, index: Index) -> None:
    notion.lead = timedelta(seconds=30)
    notion.page("plan", "root", blocks=_note("One."))
    first = index.sync()["plan"]
    assert text(first) == "One."
    notion.scribble("plan", _note("Two, in the same minute."))
    assert index.sync() == {}, "still within the minute: the same unsettled revision"

    notion.lead = timedelta(minutes=5)
    second = index.sync()["plan"]
    assert text(second) == "Two, in the same minute."
    assert second["source_revision"] != first["source_revision"]
    assert second["metadata"]["updated_at"] == first["metadata"]["updated_at"]
    reads = _reads(notion, "plan")
    assert index.sync() == {} and _reads(notion, "plan") == reads


def test_a_row_is_judged_by_the_clock_of_the_answer_it_came_with(
    notion: FakeNotion, index: Index
) -> None:
    notion.database("tasks", "root")
    notion.row("task", "tasks", title="One")
    notion.hidden.add(ident("task"))
    notion.lead = timedelta(seconds=30)

    def later() -> None:
        # The row is edited again within its minute, right after the answer
        # that held it; minutes pass before the connector looks at it.
        notion.pages[ident("task")]["title"] = "Two, in the same minute"
        notion.lead = timedelta(minutes=5)

    notion.after(lambda path, _: path.endswith("/query"), later)
    first = index.sync()["task"]
    assert text(first) == "Name: One"
    notion.hidden.clear()
    notion.reindex()
    second = index.sync()["task"]
    assert text(second) == "Name: Two, in the same minute"
    assert second["source_revision"] != first["source_revision"]


def test_a_removed_or_unshared_page_leaves_at_the_listing(notion: FakeNotion, index: Index) -> None:
    _family(notion)
    index.sync()
    notion.remove("plan")
    notion.unshare("mid")
    assert index.sync() == {}
    assert index.listing() == ["root"]
    assert index.published() == ["leaf", "mid", "plan"], "content alone deletes none of them"


@pytest.mark.parametrize("rows", [100, 1])
def test_a_trash_later_in_the_same_sync_outlives_the_upsert(
    notion: FakeNotion, index: Index, rows: int
) -> None:
    index.limits["max_rows"] = rows
    _family(notion)
    # Notion changes after the walk has read the page and before search is read.
    last = f"/v1/blocks/{ident('leaf')}/children"
    notion.after(lambda path, _: path == last, lambda: notion.trash("plan"))
    seen = index.sync()
    ops = [batch["plan"]["op"] for batch in index.batches if "plan" in batch]
    assert ops == (["delete"] if rows == 100 else ["upsert", "delete"])
    assert seen["plan"]["op"] == "delete" and index.published() == ["leaf", "mid"]


def test_an_edit_later_in_the_same_sync_outlives_the_first_read(
    notion: FakeNotion, index: Index
) -> None:
    _family(notion)
    last = f"/v1/blocks/{ident('leaf')}/children"
    notion.after(
        lambda path, _: path == last,
        lambda: notion.page("plan", blocks=_note("Edited during the sync.")),
    )
    assert text(index.sync()["plan"]) == "Edited during the sync."
    assert [len(batch) for batch in index.batches] == [3], "one row for the page, the later one"
    assert _reads(notion, "plan") == 2


def test_a_parent_read_earlier_in_the_run_is_replaced_by_its_change(
    notion: FakeNotion, index: Index
) -> None:
    notion.page("parent", "root", blocks=_note("Parent."))
    notion.page("older", "parent", blocks=_note("Older child."))
    notion.page("newer", "parent", blocks=_note("Newer child."))
    index.sync()
    notion._clock += 600
    notion.page("older", blocks=_note("Older child, edited."))
    notion.page("parent", blocks=_note("Parent, edited."))
    notion.page("newer", blocks=_note("Newer child, edited."))
    # Search shows newer, parent, older. The parent is first looked up as the
    # ancestor of `newer`, and moves out of the roots right after that.
    parent = f"/v1/pages/{ident('parent')}"
    notion.after(lambda path, _: path == parent, lambda: notion.move("parent", None))
    assert sorted(index.sync()) == ["newer"]
    assert index.listing() == ["root"]


def test_an_ancestor_that_is_gone_is_out_of_scope_and_one_that_fails_fails_the_sync(
    notion: FakeNotion, index: Index
) -> None:
    _family(notion)
    index.sync()
    cursor = index.cursor
    notion._clock += 600
    notion.page("leaf", blocks=_note("Edited."))
    middle = f"/v1/pages/{ident('mid')}"
    asked = notion.count(middle + "$")
    notion.fail(lambda path, _: path == middle, 503, times=ATTEMPTS)
    with pytest.raises(ApiError):
        index.sync()
    assert index.cursor == cursor and _reads(notion, "leaf") == 1

    notion.fail(lambda path, _: path == middle, 404, "object_not_found")
    assert index.sync() == {}, "not shared: out of scope, and no failure"
    assert index.cursor != cursor and _reads(notion, "leaf") == 1
    assert notion.count(middle + "$") - asked == ATTEMPTS + 2, "ancestor plus its own hint"


def test_a_page_nested_in_blocks_reaches_its_root(notion: FakeNotion, index: Index) -> None:
    notion.page(
        "plan",
        "root",
        blocks=[block("toggle", "Open", block("column_list", "", name="inner"), name="outer")],
    )
    notion.page("nested", "plan", under="inner", blocks=_note("Nested."))
    notion.database("tasks", "plan", under="outer")
    notion.row("task", "tasks", title="Task")
    index.sync()
    notion._clock += 600
    asked = notion.count("/v1/blocks/[0-9a-f-]+$")
    notion.page("nested", blocks=_note("Nested, edited."))
    notion.row("task", "tasks", title="Task, edited")
    assert sorted(index.sync()) == ["nested", "task"]
    assert notion.count("/v1/blocks/[0-9a-f-]+$") - asked == 2, "each block on the way up, once"


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
