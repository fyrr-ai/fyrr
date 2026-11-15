"""Cursor recovery, listing completeness and bounded document reads."""

import json
from datetime import timedelta

import pytest
from fake_notion import block, ident
from notion_kit import text

from fyrr_connectors.notion.client import ATTEMPTS, ApiError


def test_a_repeated_hint_can_contradict_an_earlier_one(notion, index, monkeypatch):
    notion.page("plan", "root", blocks=[block("paragraph", "Initial.")])
    index.sync()
    notion.page("plan", blocks=[block("paragraph", "Edited.")])

    def search(arguments):
        more = not arguments.get("start_cursor")
        if not more:
            notion.trash("plan")
        return {
            "results": [notion._page(ident("plan"))],
            "has_more": more,
            "next_cursor": "second" if more else None,
        }

    monkeypatch.setattr(notion, "_search", search)
    assert index.sync()["plan"]["op"] == "delete"
    assert "plan" not in index.published()


def test_an_unsettled_read_holds_the_watermark(notion, index, monkeypatch):
    notion.lead = timedelta(seconds=30)
    notion.page("plan", "root", blocks=[block("paragraph", "One.")])
    stamp = notion.pages[ident("plan")]["edited"]
    now = notion.now()
    monkeypatch.setattr(notion, "now", lambda: now)
    notion.after(
        lambda path, _: path == f"/v1/blocks/{ident('plan')}/children",
        lambda: notion.page("later", "root", blocks=[block("paragraph", "Later.")]),
    )
    index.sync()
    assert json.loads(index.cursor)["since"] <= stamp
    notion.scribble("plan", [block("paragraph", "Two.")])
    notion._clock += 600
    monkeypatch.setattr(notion, "now", lambda: now + timedelta(hours=10))
    assert text(index.sync()["plan"]) == "Two."


def test_a_new_row_after_published_rows_is_discovered(notion, index):
    notion.database("tasks", "root", sources=2)
    for number in range(4):
        notion.row(f"old-{number}", "tasks", title=f"Old {number}")
    index.sync()
    notion._clock += 600
    notion.row("new", "tasks", source=1, title="New")
    notion.hidden.add(ident("new"))
    notion.page("root", title="Changed root")
    assert text(index.sync()["new"]) == "Name: New"


@pytest.mark.parametrize("status", [403, 404, 503])
def test_ancestor_denial_is_out_of_scope_but_outage_raises(notion, index, status):
    notion.page("parent", "root")
    notion.page("leaf", "parent", blocks=[block("paragraph", "One.")])
    index.sync()
    notion._clock += 600
    notion.page("leaf", blocks=[block("paragraph", "Two.")])
    notion.fail(lambda path, _: path == f"/v1/pages/{ident('parent')}", status, times=ATTEMPTS)
    if status == 503:
        with pytest.raises(ApiError):
            index.sync()
    else:
        assert index.sync() == {}


@pytest.mark.parametrize(
    "corruption",
    ["missing_has_more", "wrong_has_more", "invalid_entry", "missing_sources", "invalid_row"],
)
def test_malformed_data_never_completes_a_listing(notion, index, monkeypatch, corruption):
    notion.database("tasks", "root")
    notion.row("row", "tasks", title="Row")
    original = notion._answer

    def answer(method, path, arguments):
        status, body = original(method, path, arguments)
        if path.endswith(f"{ident('root')}/children"):
            if corruption == "missing_has_more":
                body.pop("has_more")
            elif corruption == "wrong_has_more":
                body["has_more"] = "false"
            elif corruption == "invalid_entry":
                body["results"].append("bad entry")
        if "/databases/" in path and corruption == "missing_sources":
            body.pop("data_sources")
        if path.endswith("/query") and corruption == "invalid_row":
            body["results"][0]["object"] = "not-a-page"
        return status, body

    monkeypatch.setattr(notion, "_answer", answer)
    parts = []
    with pytest.raises(ValueError, match="notion_response_invalid"):
        for part in index.run("reconcile"):
            parts.append(part)
    assert not any(part.complete for part in parts)


@pytest.mark.parametrize("field", ["since", "roots", "walks", "path"])
def test_cursor_payloads_are_validated(notion, index, field):
    state = {"v": 1, "since": "", "roots": [], "walks": [], "path": []}
    if field == "since":
        state[field] = "private@example.test"
    elif field == "walks":
        state[field] = [["page", "private@example.test"]]
    else:
        state[field] = ["private@example.test"]
    with pytest.raises(ValueError, match="notion_cursor_invalid"):
        list(index.run(cursor=json.dumps(state)))


def test_error_codes_cannot_echo_payloads(notion, index):
    notion.fail(lambda _path, _args: True, 400, "private_payload_value")
    with pytest.raises(ApiError) as caught:
        index.sync()
    assert "private_payload_value" not in str(caught.value)


def test_page_reads_refresh_ancestry_without_content_changes(notion, index, monkeypatch):
    notion.page("parent", "root", blocks=[block("paragraph", "Parent.")])
    notion.page("first", "parent", blocks=[block("paragraph", "First.")])
    notion.page("last", "parent", blocks=[block("paragraph", "Last.")])
    index.sync()
    notion._clock += 600
    notion.page("first", blocks=[block("paragraph", "First edited.")])
    notion.page("last", blocks=[block("paragraph", "Last edited.")])
    parent = notion._page(ident("parent"))
    notion.after(
        lambda path, _: path == f"/v1/pages/{ident('parent')}", lambda: notion.move("parent", None)
    )
    hints = [notion._page(ident("first")), parent, notion._page(ident("last"))]
    monkeypatch.setattr(notion, "_search", lambda args: notion._listed(hints, args))
    assert sorted(index.sync()) == ["first"]


def test_traversal_limit_never_completes_a_listing(notion, index, monkeypatch):
    from fyrr_connectors.notion import tree

    for number in range(4):
        notion.page(f"p-{number}", "root", blocks=[block("paragraph", "Text.")])
    monkeypatch.setattr(tree, "MAX_NODES", 3)
    parts = []
    with pytest.raises(ValueError, match="notion_tree_limit"):
        for part in index.run("reconcile"):
            parts.append(part)
    assert not any(part.complete for part in parts)


def test_unique_paging_cursors_are_still_bounded(notion, index, monkeypatch):
    from fyrr_connectors.notion import client

    for number in range(4):
        notion.page(f"p-{number}", "root")
    monkeypatch.setattr(client, "MAX_LIST_PAGES", 1)
    with pytest.raises(ValueError, match="notion_listing_limit"):
        index.listing()


def test_transport_response_size_is_bounded(notion, index, monkeypatch):
    from fyrr_connectors.notion import client

    notion.page("plan", "root", blocks=[block("paragraph", "x" * 10_000)])
    monkeypatch.setattr(client, "MAX_RESPONSE_BYTES", 4096)
    with pytest.raises(ValueError, match="notion_response_limit"):
        index.sync()
    assert index.cursor is None


def test_text_limit_counts_exact_utf8_bytes_and_separators():
    from fyrr_connectors.notion.render import Oversized, Text

    def paragraph(value):
        return {"type": "paragraph", "paragraph": {"rich_text": [{"plain_text": value}]}}

    rendered = Text(6)
    rendered.block(0, paragraph("é"))
    rendered.block(0, paragraph("é"))
    assert rendered.sections()[0].text == "é\n\né"
    rendered = Text(5)
    rendered.block(0, paragraph("é"))
    rendered.block(0, paragraph("é"))
    with pytest.raises(Oversized):
        rendered.sections()


def test_many_section_locations_fit_the_actual_arrow_batch_bound(notion, index):
    from fyrr_connectors.arrow import rows_to_json

    for name in ("a", "b"):
        notion.page(name, "root", blocks=[block("heading_1", "x" * 80) for _ in range(20)])
    index.limits["max_bytes"] = 3000
    index.limits["document_max_bytes"] = 2900
    batches = list(index.run())
    assert all(batch.changes.get_total_buffer_size() <= 3000 for batch in batches)
    changes = [change for batch in batches for change in rows_to_json(batch.changes)]
    assert len(changes) == 2
    assert all(change["sections"] == [{"text": "\0"}] for change in changes)


def test_an_unsupported_container_does_not_hide_scoped_pages(notion, index):
    notion.page("plan", "root", blocks=[block("unsupported", name="wrapper")])
    notion.page("leaf", "plan", under="wrapper", blocks=[block("paragraph", "Leaf.")])
    assert text(index.sync()["leaf"]) == "Leaf."
    assert index.listing() == ["leaf", "plan", "root"]


def test_a_changed_walked_parent_can_be_walked_again_for_new_children(notion, index, monkeypatch):
    from fyrr_connectors.notion import connector

    monkeypatch.setattr(connector, "MAX_WALKS", 2)
    notion.page("plan", "root", blocks=[block("paragraph", "Plan.")])
    notion.page("last", "root", blocks=[block("paragraph", "Last.")])

    def add_children():
        for number in range(4):
            name = f"new-{number}"
            notion.page(name, "plan", blocks=[block("paragraph", "New child.")])
            notion.hidden.add(ident(name))

    notion.after(lambda path, _: path == f"/v1/blocks/{ident('last')}/children", add_children)
    assert sorted(index.sync()) == ["last", *(f"new-{number}" for number in range(4)), "plan"]


def test_overlapping_roots_are_listed_once(notion, index):
    notion.page("plan", "root", blocks=[block("paragraph", "Plan.")])
    notion.page("leaf", "plan", blocks=[block("paragraph", "Leaf.")])
    index.roots = ["root", "plan"]
    assert index.listing() == ["leaf", "plan", "root"]


def test_resume_does_not_skip_an_unsettled_rows_new_descendants(notion, index, monkeypatch):
    notion.database("tasks", "root")
    notion.row("a", "tasks", title="A")
    stamp = notion.pages[ident("a")]["edited"]
    now = notion.now() - timedelta(minutes=10) + timedelta(seconds=30)
    notion.page("leaf", "a", blocks=[block("paragraph", "Leaf.")])
    notion.pages[ident("a")]["edited"] = stamp
    notion.row("b", "tasks", title="B")
    notion.row("c", "tasks", title="C")
    notion.hidden.add(ident("a"))
    monkeypatch.setattr(notion, "now", lambda: now)
    index.limits["max_rows"] = 1
    run = index.run()
    for _ in range(3):
        index.apply(next(run))
    run.close()
    assert json.loads(index.cursor)["path"][-1] == ident("b")
    notion.page("new-leaf", "a", blocks=[block("paragraph", "Same-minute child.")])
    notion.pages[ident("a")]["edited"] = stamp
    notion.hidden.add(ident("new-leaf"))
    assert text(index.sync()["new-leaf"]) == "Same-minute child."


def test_invalid_utf8_quarantines_one_document_and_keeps_walking(notion, index):
    notion.page("invalid", "root", blocks=[block("paragraph", "bad\ud800text")])
    notion.page("leaf", "invalid", blocks=[block("paragraph", "Child still found.")])
    notion.page("fine", "root", blocks=[block("paragraph", "Fine.")])
    seen = index.sync()
    assert seen["invalid"]["sections"] == [{"text": "\0"}]
    assert text(seen["leaf"]) == "Child still found." and text(seen["fine"]) == "Fine."
    assert json.loads(index.cursor)["walks"] == []
