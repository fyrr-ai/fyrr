"""Scope changes, subtree discovery and listing convergence."""

from datetime import timedelta

from fake_notion import block, ident


def test_returned_subtree_checks_below_published_descendants(notion, index):
    notion.page("a", "root", blocks=[block("paragraph", "A.")])
    notion.page("b", "a", blocks=[block("paragraph", "B.")])
    index.sync()
    notion.move("a", None)
    notion._clock += 1440
    notion.page("new", "b", blocks=[block("paragraph", "New.")])
    notion.hidden.update([ident("b"), ident("new")])
    notion._clock += 1440
    notion.move("a", "root")
    assert "new" in index.sync()


def test_listing_catches_a_move_into_an_already_listed_branch(notion, index):
    notion.lead = timedelta(0)
    notion.page("a", "root")
    notion.page("b", "root")
    notion.page("p", "b", blocks=[block("paragraph", "P.")])
    notion.page("leaf", "p", blocks=[block("paragraph", "Leaf.")])
    notion.after(
        lambda path, _: path == f"/v1/blocks/{ident('a')}/children",
        lambda: notion.move("p", "a"),
    )
    assert index.listing() == ["a", "b", "leaf", "p", "root"]


def test_queued_child_moved_outside_is_not_ingested(notion, index):
    notion.page("child", "root", blocks=[block("paragraph", "Outside scope.")])
    notion.after(
        lambda path, _: path == f"/v1/blocks/{ident('root')}/children",
        lambda: notion.move("child", None),
    )
    assert "child" not in index.sync()


def test_fixed_author_without_inline_name_never_reads_users(notion, index):
    notion.inline_authors = False
    notion.page("page", "root", blocks=[block("paragraph", "Body.")])
    seen = index.sync()
    assert notion.count("/users/") == 0
    assert "author" not in seen["page"]["metadata"]


def test_nonconverging_catchup_never_completes(notion, index, monkeypatch):
    notion.lead = timedelta(0)
    original = notion._search
    counter = 0

    def search(arguments):
        nonlocal counter
        if arguments.get("sort", {}).get("direction") == "ascending" and not arguments.get(
            "start_cursor"
        ):
            counter += 1
            notion.page(f"new-{counter}", "root")
        return original(arguments)

    monkeypatch.setattr(notion, "_search", search)
    parts = list(index.run("reconcile"))
    assert counter >= 4
    assert parts[-1].last and not parts[-1].complete


def test_lagged_unstamped_move_preserves_missing_published_subtree(notion, index):
    notion.page("a", "root")
    notion.page("b", "root")
    notion.page("p", "b", blocks=[block("paragraph", "P.")])
    notion.page("leaf", "p", blocks=[block("paragraph", "Leaf.")])
    index.sync()
    notion.lag = True
    notion.hidden.update(notion.pages)
    old_stamp = notion.pages[ident("p")]["edited"]

    def move():
        notion.move("p", "a")
        notion.pages[ident("p")]["edited"] = old_stamp

    notion.after(lambda path, _: path == f"/v1/blocks/{ident('a')}/children", move)
    assert index.listing() == ["a", "b", "leaf", "p", "root"]


def test_listing_without_revision_view_cannot_authorize_deletes(notion, index):
    index.lookup = False
    parts = list(index.run("reconcile"))
    assert parts[-1].last and not parts[-1].complete


def test_catchup_includes_moves_stamped_in_the_start_minute(notion, index):
    notion.lead = timedelta(0)
    notion.page("a", "root")
    notion.page("b", "root")
    notion.page("p", "b")
    stamp = notion.now().replace(second=0, microsecond=0).isoformat()
    notion.hidden.update([ident("root"), ident("a"), ident("b")])

    def move():
        notion.move("p", "a")
        notion.pages[ident("p")]["edited"] = stamp
        notion._index[ident("p")] = notion._page(ident("p"))

    notion.after(lambda path, _: path == f"/v1/blocks/{ident('a')}/children", move)
    assert index.listing() == ["a", "b", "p", "root"]
