"""The walk: a first sync that is killed and continues, the full-ID listing
that is complete or absent, and bounded work on a hostile workspace."""

import json

import pytest
from fake_notion import FakeNotion, block, ident
from notion_kit import Index, text

from fyrr_connectors.notion import tree
from fyrr_connectors.notion.client import ATTEMPTS, ApiError, Client


def _note(words: str) -> list[dict]:
    return [block("paragraph", words)]


def _reads(notion: FakeNotion, name: str) -> int:
    return notion.count(f"/blocks/{ident(name)}/children$")


def _forest(notion: FakeNotion) -> list[str]:
    """Pages, nested pages and a database with rows; their names in walk order."""
    notion.page("a", "root", blocks=_note("A."))
    notion.page("a1", "a", blocks=_note("A1."))
    notion.page("a2", "a", blocks=_note("A2."))
    notion.page("b", "root", blocks=_note("B."))
    notion.page("b1", "b", blocks=_note("B1."))
    notion.database("rows", "root")
    for number in range(5):
        notion.row(f"row-{number}", "rows", title=f"Row {number}")
    notion.page("under-row", "row-1", blocks=_note("Beneath a row."))
    return ["a", "a1", "a2", "b", "b1", "row-0", "row-1", "under-row", "row-2", "row-3", "row-4"]


def _first(index: Index, batches: int) -> list[str]:
    """Run a content sync and kill it after `batches` acknowledged batches."""
    run = index.run(cursor=index.cursor)
    names = [name for _ in range(batches) for name in index.apply(next(run))]
    run.close()
    return names


def _path(index: Index) -> list[str]:
    return [index.world.names[node] for node in json.loads(index.cursor)["path"]]


def test_a_walk_visits_a_page_before_what_lies_beneath_it(notion: FakeNotion, index: Index) -> None:
    order = _forest(notion)
    index.limits["max_rows"] = 1
    index.sync()
    assert [name for batch in index.batches for name in batch] == order


@pytest.mark.parametrize("killed_after", range(1, 11))
def test_a_killed_first_sync_continues_where_it_stopped(
    notion: FakeNotion, index: Index, killed_after: int
) -> None:
    order = _forest(notion)
    index.limits["max_rows"] = 1
    assert _first(index, killed_after) == order[:killed_after]
    state = json.loads(index.cursor)
    assert state["walks"] == [["page", ident("root")]]
    spine = _path(index)
    assert spine[-1] == order[killed_after - 1]
    since = state["since"]

    before = {name: _reads(notion, name) for name in order}
    rest = index.sync()
    assert sorted(rest) == sorted(order[killed_after:])
    assert index.published() == sorted(order)
    # What was finished before the position is not read again: only the
    # pages on the way down to it are.
    again = {name for name in order if _reads(notion, name) > before[name]}
    assert again & set(order[:killed_after]) <= set(spine)
    final = json.loads(index.cursor)
    assert final["walks"] == [] and final["path"] == [] and final["since"] >= since
    assert index.sync() == {}


def test_an_edit_of_a_page_the_killed_walk_had_passed_is_not_lost(
    notion: FakeNotion, index: Index
) -> None:
    _forest(notion)
    index.limits["max_rows"] = 1
    _first(index, 6)
    notion._clock += 30  # Still within the overlap of the watermark taken before the walk.
    notion.page("a1", blocks=_note("A1, edited while the sync was down."))
    assert text(index.sync()["a1"]) == "A1, edited while the sync was down."


def test_a_walk_position_that_vanished_starts_its_level_again(
    notion: FakeNotion, index: Index
) -> None:
    order = _forest(notion)
    index.limits["max_rows"] = 1
    _first(index, 3)
    assert _path(index) == ["root", "a", "a2"]
    notion.remove("a")
    rest = index.sync()
    assert sorted(rest) == sorted(order[3:])

    index.cursor, index.revisions = None, {}
    notion.page("a", "root", blocks=_note("A."))
    _first(index, 7)
    assert _path(index) == ["root", "rows", "row-1"]
    notion.remove("row-1")
    assert sorted(index.sync()) == ["row-2", "row-3", "row-4"]


def test_rows_in_another_order_after_a_kill_are_not_passed_over(
    notion: FakeNotion, index: Index
) -> None:
    _forest(notion)
    index.limits["max_rows"] = 1
    _first(index, 7)
    assert _path(index) == ["root", "rows", "row-1"]
    source = notion.sources[ident("rows#0")]
    source["rows"].reverse()
    rest = index.sync()
    assert sorted(rest) == ["row-2", "row-3", "row-4", "under-row"]
    assert index.published() == sorted(
        ["a", "a1", "a2", "b", "b1", *(f"row-{n}" for n in range(5)), "under-row"]
    )


def test_a_deep_walk_keeps_a_bounded_path(
    notion: FakeNotion, index: Index, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fyrr_connectors.notion import connector

    monkeypatch.setattr(connector, "PATH_LEVELS", 3)
    parent = "root"
    for depth in range(6):
        notion.page(f"level-{depth}", parent, blocks=_note(f"Level {depth}."))
        parent = f"level-{depth}"
    index.limits["max_rows"] = 1
    _first(index, 5)
    assert _path(index) == ["root", "level-0", "level-1"]
    assert sorted(index.sync()) == ["level-5"], "it continues from the ancestor it kept"
    assert len(index.published()) == 6


def test_the_listing_comes_in_parts_and_only_the_last_is_complete(
    notion: FakeNotion, index: Index
) -> None:
    order = _forest(notion)
    index.limits["max_rows"] = 2
    parts = list(index.run("reconcile"))
    assert len(parts) == 6 and all(len(part.root_ids) <= 2 for part in parts)
    assert [part.part for part in parts] == list(range(6))
    assert [(part.last, part.complete) for part in parts] == [(False, False)] * 5 + [(True, True)]
    assert len({part.listing_id for part in parts}) == 1
    assert index.listing() == sorted(["root", *order])
    assert notion.count("/search$") >= 1, "search catches up traversal races"


def test_a_failed_read_anywhere_in_the_walk_leaves_no_complete_listing(
    notion: FakeNotion, index: Index
) -> None:
    _forest(notion)
    notion.page(
        "a",
        blocks=[block("toggle", "Open", block("paragraph", "Nested list."), name="nested")],
    )
    index.limits["max_rows"] = 2
    index.listing()
    requests = list(notion.requests)
    assert requests and any(path.endswith("/search") for _, path, _ in requests)
    for failing in requests:
        del notion.requests[:]
        same = lambda path, sent, failing=failing: (path, sent) == failing[1:]  # noqa: E731
        notion.fail(same, 500, times=ATTEMPTS)
        parts = []
        with pytest.raises(ApiError):
            for part in index.run("reconcile"):
                parts.append(part)
        assert not any(part.complete or part.last for part in parts), failing[1]
    notion.fail(lambda path, _: path.endswith("/query"), 400, "validation_error")
    with pytest.raises(ApiError):
        index.listing()
    notion.fail(lambda path, _: "/blocks/" in path, 400, "validation_error")
    with pytest.raises(ApiError):
        index.listing()


def test_what_is_not_shared_is_not_listed_and_the_listing_stays_complete(
    notion: FakeNotion, index: Index
) -> None:
    order = _forest(notion)
    notion.unshare("a")
    notion.unshare("rows")
    assert index.listing() == ["b", "b1", "root"]
    notion.share("a")
    notion.share("rows")
    notion.unshare("row-1")
    assert index.listing() == sorted(set(["root", *order]) - {"row-1", "under-row"})


def test_cycles_and_endless_chains_end(notion: FakeNotion, index: Index) -> None:
    notion.page("plan", "root", blocks=[block("toggle", "Loop", name="loop")])
    loop = notion._blocks[ident("plan/loop")]
    loop["children"] = [ident("plan/loop")]
    notion.database("rows", "plan")
    notion.row("row", "rows", title="Row")
    # The row holds the database it is a row of, and the page that holds that.
    notion.databases[ident("rows")]["parent"] = ("page", ident("row"))
    notion.pages[ident("plan")]["parent"] = ("page", ident("row"))
    notion.pages[ident("row")]["parent"] = ("page", ident("root"))
    assert sorted(index.sync()) == ["plan"]
    assert index.listing() == ["plan", "root", "row"]

    # Two pages outside the roots that are each other's parent.
    notion.page("yin", None, blocks=_note("Yin."))
    notion.page("yang", "yin", blocks=_note("Yang."))
    notion.pages[ident("yin")]["parent"] = ("page", ident("yang"))
    asked = len(notion.requests)
    assert index.sync() == {}
    assert len(notion.requests) - asked < 30, "empty pages may be revisited, but cycles terminate"


def test_a_parent_chain_longer_than_the_bound_is_out_of_scope(
    notion: FakeNotion, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tree, "MAX_DEPTH", 5)
    parent = "root"
    for depth in range(8):
        notion.page(f"level-{depth}", parent)
        parent = f"level-{depth}"
    reader = tree.Tree(Client(notion.token, "https://api.notion.com"))
    reader.roots = frozenset({ident("root")})
    assert reader.root_of(ident("level-3")) == ident("root")
    assert reader.root_of(ident("level-7")) is None
    assert reader.root_of(ident("no such page")) is None


def test_deep_block_nesting_needs_no_recursion(notion: FakeNotion, index: Index) -> None:
    notion.page("plan", "root", blocks=[block("bulleted_list_item", "0", name="b0")])
    page = ident("plan")
    for depth in range(1, 3000):
        notion._blocks[ident(f"plan/b{depth}")] = {
            "kind": "bulleted_list_item",
            "text": str(depth),
            "data": {},
            "parent": ("block", ident(f"plan/b{depth - 1}")),
            "page": page,
            "children": [],
        }
        notion._blocks[ident(f"plan/b{depth - 1}")]["children"] = [ident(f"plan/b{depth}")]
    index.limits["document_max_bytes"] = 64 << 20
    index.limits["max_bytes"] = 128 << 20
    lines = text(index.sync()["plan"]).splitlines()
    assert len(lines) == 3000 and lines[-1] == "  " * 2999 + "- 2999"


def test_an_id_that_is_no_notion_id_never_reaches_a_request(
    notion: FakeNotion, index: Index
) -> None:
    notion.page("plan", "root", blocks=_note("Plan."))
    real = notion._beneath

    def hostile(kind: str, node: str) -> list[dict]:
        return [{**found, "id": "../users/me"} for found in real(kind, node)]

    notion._beneath = hostile
    with pytest.raises(ValueError, match="notion_id_invalid"):
        index.sync()
    assert all("users" not in path for path in notion.paths())
