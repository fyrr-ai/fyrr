"""Scope changes, subtree discovery and listing convergence."""

from datetime import timedelta

from fake_notion import block, ident


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


def test_listing_without_revision_view_cannot_authorize_deletes(notion, index):
    index.lookup = False
    parts = list(index.run("reconcile"))
    assert parts[-1].last and not parts[-1].complete
