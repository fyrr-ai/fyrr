"""The connector end to end on the fake Notion: content, changes, deletes,
quarantine and the full-ID listing."""

import json
import re
from pathlib import Path

import pytest
from fake_notion import FakeNotion, block, ident
from notion_kit import READERS, SETTINGS, Index, text

from fyrr_connectors import testkit
from fyrr_connectors.notion.client import ATTEMPTS, ApiError

GOLDEN = Path(__file__).parent / "golden"
QUARANTINED = [{"text": "\0"}]
#: Every call a Notion source may make: content, never a permission or a member list.
CONTENT_CALLS = re.compile(
    r"^/v1/(search|pages/[0-9a-f-]{36}|blocks/[0-9a-f-]{36}(/children)?|databases/[0-9a-f-]{36}"
    r"|data_sources/[0-9a-f-]{36}(/query)?)$"
)


def _world(notion: FakeNotion) -> None:
    notion.page(
        "plan",
        "root",
        title="Plan 2026",
        blocks=[
            block("paragraph", "Draft."),
            block("heading_1", "Goals"),
            block(
                "bulleted_list_item", "Ship the connector", block("to_do", "Tests", checked=True)
            ),
            block("numbered_list_item", "First"),
            block("numbered_list_item", "Second"),
            block(
                "toggle",
                "Details",
                block("paragraph", "Inside the toggle."),
                name="details",
            ),
            block("heading_2", "Numbers"),
            block(
                "table",
                "",
                block("table_row", cells=["item", "eur"]),
                block("table_row", cells=["rent", "1200"]),
            ),
            block(
                "column_list",
                "",
                block("column", "", block("paragraph", "Left.")),
                block("column", "", block("code", "x = 1", language="python")),
            ),
            block("bookmark", "Reference", url="https://example.com/spec"),
            block("image", "Architecture sketch"),
            block("unsupported"),
        ],
    )
    notion.page("risks", "plan", title="Risks", under="details", blocks=[block("quote", "Few.")])
    notion.database("tasks", "root", title="Tasks")
    notion.row(
        "task-login",
        "tasks",
        title="Fix the login",
        properties={
            "Stage": {"type": "status", "status": {"name": "Doing"}},
            "Tags": {"type": "multi_select", "multi_select": [{"name": "auth"}]},
        },
        blocks=[block("heading_1", "Steps"), block("paragraph", "Reproduce, then fix.")],
    )
    notion.row("task-empty", "tasks", title="Write the docs")
    notion.page("notes", "task-login", title="Notes", blocks=[block("paragraph", "Row sub-page.")])
    notion.page("elsewhere", None, title="Not under a root", blocks=[block("paragraph", "No.")])


def _scope(*roots: str) -> dict:
    return {"root_pages": [ident(root) for root in roots or ("root",)]}


def test_the_provider_is_installed_and_valid() -> None:
    info = testkit.check_provider("notion")
    assert info["sync_kinds"] == ["content", "reconcile"]
    assert info["connection_types"] == ["fixed"]
    assert info["config_schema"]["settings"]["properties"]["token"]["x-fyrr-secret"]
    assert info["config_schema"]["scope"]["required"] == ["root_pages"]


def test_pages_and_rows_match_the_golden_output(notion: FakeNotion) -> None:
    _world(notion)
    result = testkit.run_sync(
        "notion", "content", settings=SETTINGS, scope=_scope(), readers=("grp:staff",)
    )
    testkit.assert_golden(result, GOLDEN / "content.json")
    changes = [change for batch in result.documents for change in batch["body"]["changes"]]
    assert sorted(notion.names[change["root_id"]] for change in changes) == [
        "notes",
        "plan",
        "risks",
        "task-empty",
        "task-login",
    ]
    for change in changes:
        metadata = change["metadata"]
        assert metadata["author"] == "Ada Example" and metadata["project"] == "Handbook"
        assert metadata["created_at"] and metadata["updated_at"] and metadata["title"]
        assert change["readers"] == ["grp:staff"] and "revision_order" not in change
    cursor = json.loads(result.cursor)
    assert cursor["walks"] == [] and cursor["path"] == [] and cursor["roots"] == [ident("root")]

    listing = testkit.run_sync("notion", "reconcile", settings=SETTINGS, scope=_scope())
    assert not listing.documents[-1]["body"]["complete"], "testkit has no revision view"
    listed = sorted(notion.names[r] for part in listing.documents for r in part["body"]["root_ids"])
    assert listed == ["notes", "plan", "risks", "root", "task-empty", "task-login"]


def test_text_follows_the_document_order(notion: FakeNotion, index: Index) -> None:
    _world(notion)
    seen = index.sync()
    assert text(seen["plan"]) == (
        "Draft.\n---\n"
        "# Goals\n\n- Ship the connector\n  - [x] Tests\n1. First\n2. Second\n\n"
        "Details\n  Inside the toggle.\n---\n"
        "## Numbers\n\nitem | eur\nrent | 1200\n\nLeft.\n\n```python\nx = 1\n```\n\n"
        "Reference https://example.com/spec\n\nArchitecture sketch"
    )
    assert "Few." not in text(seen["plan"]), "a sub-page is its own document"
    assert text(seen["task-login"]) == (
        "Name: Fix the login\nStage: Doing\nTags: auth\n---\n# Steps\n\nReproduce, then fix."
    )
    assert seen["task-login"]["sections"][0]["location"] == {"kind": "properties", "index": 0}
    assert seen["task-login"]["metadata"]["source_type"] == "notion-row"
    assert seen["task-login"]["metadata"]["connector_map"] == {
        "page_id": ident("task-login"),
        "root_page_id": ident("root"),
        "database_id": ident("tasks"),
    }
    assert text(seen["task-empty"]) == "Name: Write the docs"
    assert seen["plan"]["metadata"]["source_type"] == "notion-page"
    assert seen["plan"]["metadata"]["source_uri"].startswith("https://www.notion.so/plan-")
    assert "root" not in seen, "a page without text is not published"


def test_create_edit_trash_and_restore_propagate(notion: FakeNotion, index: Index) -> None:
    _world(notion)
    assert sorted(index.sync()) == ["notes", "plan", "risks", "task-empty", "task-login"]
    batches = len(index.batches)
    assert index.sync() == {} and len(index.batches) == batches, "an idle run hands over nothing"

    notion.page("plan", blocks=[block("paragraph", "Changed.")])
    notion.page("fresh", "root", blocks=[block("paragraph", "New page.")])
    notion.row("task-new", "tasks", title="A new row")
    changed = index.sync()
    assert sorted(changed) == ["fresh", "plan", "task-new"]
    assert text(changed["plan"]) == "Changed."

    notion.trash("task-login")
    notion.trash("fresh")
    deleted = index.sync()
    assert {name: change["op"] for name, change in deleted.items()} == {
        "task-login": "delete",
        "fresh": "delete",
    }
    assert index.sync() == {}

    notion.restore("fresh")
    assert index.sync()["fresh"]["op"] == "upsert"
    # What hung beneath the trashed row and in the removed toggle has no
    # change entry of its own: it leaves at the full-ID listing.
    assert index.listing() == ["fresh", "plan", "root", "task-empty", "task-new"]
    assert index.published() == ["fresh", "notes", "plan", "risks", "task-empty", "task-new"]


def test_a_rename_and_a_property_change_are_edits(notion: FakeNotion, index: Index) -> None:
    _world(notion)
    index.sync()
    notion.page("plan", title="Plan 2027")
    notion.page("task-empty", properties={"Done": {"type": "checkbox", "checkbox": True}})
    changed = index.sync()
    assert changed["plan"]["metadata"]["title"] == "Plan 2027"
    assert text(changed["task-empty"]) == "Name: Write the docs\nDone: yes"


def test_an_emptied_page_leaves(notion: FakeNotion, index: Index) -> None:
    notion.page("plan", "root", blocks=[block("paragraph", "Text.")])
    index.sync()
    notion.page("plan", blocks=[])
    assert index.sync()["plan"]["op"] == "delete"
    assert index.sync() == {}


def test_a_fixed_connection_admits_its_readers_and_reads_no_permissions(
    notion: FakeNotion, index: Index
) -> None:
    _world(notion)
    seen = index.sync()
    notion.page("plan", blocks=[block("paragraph", "Changed.")])
    seen.update(index.sync())
    index.listing()
    assert all(change["readers"] == READERS for change in seen.values())
    assert all("permission_revision" not in change for change in seen.values())
    assert all(
        "readers" not in section for change in seen.values() for section in change["sections"]
    )
    assert notion.requests and all(CONTENT_CALLS.match(path) for path in notion.paths())
    assert {method for method, _, _ in notion.requests} == {"GET", "POST"}
    assert notion.count("/users/") == 0
    with pytest.raises(ValueError, match="notion_permissions_fixed_only"):
        list(index.run(permissions="sync"))
    with pytest.raises(AssertionError):
        testkit.run_sync("notion", "permissions", settings=SETTINGS, scope=_scope())


def test_an_author_notion_will_not_name_is_left_out(notion: FakeNotion, index: Index) -> None:
    notion.user_capability = False
    notion.page("a", "root", blocks=[block("paragraph", "A.")])
    notion.page("b", "root", blocks=[block("paragraph", "B.")], author="nobody")
    seen = index.sync()
    assert "author" not in seen["a"]["metadata"] and "author" not in seen["b"]["metadata"]
    assert notion.count("/users/") == 0

    notion.user_capability = True
    notion.page("c", "root", blocks=[block("paragraph", "C.")], author="nobody")
    notion.page("d", "root", blocks=[block("paragraph", "D.")])
    seen = index.sync()
    assert "author" not in seen["c"]["metadata"], "an unknown user has no name"
    assert seen["d"]["metadata"]["author"] == "Ada Example"

    notion.fail(lambda path, _: "/users/" in path, 500, times=ATTEMPTS)
    notion.page("e", "root", blocks=[block("paragraph", "E.")])
    assert index.sync()["e"]["metadata"]["author"] == "Ada Example"
    assert notion.count("/users/") == 0


def test_bad_pages_are_quarantined_and_the_sync_goes_on(notion: FakeNotion, index: Index) -> None:
    notion.page("fine", "root", blocks=[block("paragraph", "Published before.")])
    notion.page("broken", "root", blocks=[block("paragraph", "Good revision.")])
    index.sync()

    notion.page("huge", "root", blocks=[block("paragraph", "y" * 70_000)])
    notion.page("under-huge", "huge", blocks=[block("paragraph", "Still found.")])
    notion.page("broken", blocks=[block("paragraph", "Now unreadable.")])
    notion.page("last", "root", blocks=[block("paragraph", "Still published.")])
    children = f"/v1/blocks/{ident('broken')}/children"
    notion.fail(lambda path, _: path == children, 400, "validation_error")
    changes = index.sync()
    assert changes["huge"]["sections"] == QUARANTINED
    assert changes["broken"]["sections"] == QUARANTINED
    assert text(changes["last"]) == "Still published."
    assert text(changes["under-huge"]) == "Still found."
    assert changes["huge"]["metadata"]["title"] == "huge"
    assert notion.sleeps == [], "a bad document is not retried"
    assert json.loads(index.cursor)["since"] == notion.pages[ident("last")]["edited"]

    notion.page("huge", blocks=[block("paragraph", "Small again.")])
    assert text(index.sync()["huge"]) == "Small again."


def test_a_block_list_that_is_gone_inside_a_page_is_left_out(
    notion: FakeNotion, index: Index
) -> None:
    notion.page(
        "plan",
        "root",
        blocks=[
            block("paragraph", "Before."),
            block("toggle", "Open", block("paragraph", "Hidden."), name="gone"),
            block("paragraph", "After."),
        ],
    )
    nested = f"/v1/blocks/{ident('plan/gone')}/children"
    notion.fail(lambda path, _: path == nested, 404, "object_not_found", times=99)
    assert text(index.sync()["plan"]) == "Before.\n\nOpen\n\nAfter."
    assert index.listing() == ["plan", "root"]


def test_a_page_that_vanishes_before_it_is_read_is_skipped(
    notion: FakeNotion, index: Index
) -> None:
    notion.page("stays", "root", blocks=[block("paragraph", "Here.")])
    notion.page("goes", "root", blocks=[block("paragraph", "Away for a moment.")])
    # Gone throughout this run, including any queued revisit of its subtree.
    notion.unshare("goes")
    assert sorted(index.sync()) == ["stays"]
    notion.share("goes")
    assert sorted(index.sync()) == ["goes"], "back again: it is still within the overlap"

    notion.page("goes", blocks=[block("paragraph", "Edited.")])
    children = f"/v1/blocks/{ident('goes')}/children"
    notion.fail(lambda path, _: path == children, 404, "object_not_found")
    assert index.sync() == {}, "its last good revision stays; the listing judges what is gone"
    assert text(index.sync()["goes"]) == "Edited."


@pytest.mark.usefixtures("notion")
def test_a_rate_limit_or_an_outage_fails_the_sync_and_keeps_the_cursor(
    notion: FakeNotion, index: Index
) -> None:
    _world(notion)
    index.sync()
    cursor = index.cursor
    notion.page("plan", blocks=[block("paragraph", "Changed.")])
    children = f"/v1/blocks/{ident('plan')}/children"
    notion.fail(lambda path, _: path == children, 429, "rate_limited", times=ATTEMPTS)
    with pytest.raises(ApiError) as caught:
        index.sync()
    assert caught.value.transient and index.cursor == cursor
    assert text(index.sync()["plan"]) == "Changed."


@pytest.mark.parametrize("status", [401, 403])
def test_a_token_that_may_not_read_fails_the_sync(
    notion: FakeNotion, index: Index, status: int
) -> None:
    _world(notion)
    index.sync()
    notion.page("plan", blocks=[block("paragraph", "Changed.")])
    children = f"/v1/blocks/{ident('plan')}/children"
    notion.fail(lambda path, _: path == children, status, "restricted_resource", times=99)
    with pytest.raises(ApiError):
        index.sync()
    with pytest.raises(ApiError):
        index.listing()


def test_a_root_that_cannot_be_read_fails_the_run(notion: FakeNotion, index: Index) -> None:
    _world(notion)
    index.sync()
    notion.unshare("root")
    with pytest.raises(ValueError, match="notion_root_unreadable"):
        index.sync()
    with pytest.raises(ValueError, match="notion_root_unreadable"):
        index.listing()
    notion.share("root")
    index.roots = ["root", "typo"]
    with pytest.raises(ValueError, match="notion_root_unreadable"):
        index.listing()


def test_a_root_must_be_a_notion_id(notion: FakeNotion, index: Index) -> None:
    source = {
        "source_id": "notion",
        "provider": "notion",
        "permissions": "fixed",
        "readers": READERS,
        "settings": SETTINGS,
        "scope": {"root_pages": ["../users"]},
        "limits": index.limits,
    }
    from fyrr_connectors import bundled, runtime

    with pytest.raises(ValueError, match="notion_id_invalid"):
        list(runtime.open_connector(source, bundled.notion()).run("content", None))
    assert notion.requests == []
    source["scope"] = {"root_pages": [ident("root").replace("-", "").upper()]}
    notion.page("plan", "root", blocks=[block("paragraph", "Found.")])
    batches = list(runtime.open_connector(source, bundled.notion()).run("content", None))
    assert batches[0].changes.num_rows == 1


def test_a_trashed_root_is_an_empty_tree(notion: FakeNotion, index: Index) -> None:
    notion.page("second", None, title="Second root")
    notion.page("kept", "second", blocks=[block("paragraph", "Kept.")])
    notion.page("lost", "root", blocks=[block("paragraph", "Under the trashed root.")])
    index.roots = ["root", "second"]
    assert sorted(index.sync()) == ["kept", "lost"]
    notion.trash("root")
    notion.page("lost", blocks=[block("paragraph", "Edited under a trashed root.")])
    assert index.sync() == {}
    assert index.listing() == ["kept", "second"]
    notion.restore("root")
    assert text(index.sync()["lost"]) == "Edited under a trashed root."


def test_databases_with_several_sources_and_linked_views(notion: FakeNotion, index: Index) -> None:
    notion.database("wiki", "root", sources=2)
    notion.row("a", "wiki", title="In the first source")
    notion.row("b", "wiki", title="In the second source", source=1)
    notion.row("c", "wiki", title="Third row")
    notion.row("d", "wiki", title="Fourth row")
    notion.database("view", "root")
    notion.unshare("view")
    notion.page("under-row", "c", blocks=[block("paragraph", "Beneath a row.")])
    notion.database("inner", "under-row")
    notion.row("deep", "inner", title="A row of a database beneath a row")
    seen = index.sync()
    assert sorted(seen) == ["a", "b", "c", "d", "deep", "under-row"]
    assert index.listing() == ["a", "b", "c", "d", "deep", "root", "under-row"]
    notion.trash("d")
    notion.search_trashed = False
    assert index.sync() == {}
    assert index.listing() == ["a", "b", "c", "deep", "root", "under-row"]


def test_a_synced_copy_gives_text_but_not_the_pages_beneath_it(
    notion: FakeNotion, index: Index
) -> None:
    notion.page("outside", None, blocks=[block("synced_block", "", name="original")])
    notion.page("private", "outside", under="original", blocks=[block("paragraph", "Private.")])
    outside = notion.pages[ident("outside")]
    notion._write(
        "outside",
        [block("synced_block", "", block("paragraph", "Shared text."), name="original")],
    )
    assert outside["blocks"]
    notion.page("plan", "root", blocks=[block("synced_block", synced_from="outside/original")])
    seen = index.sync()
    assert sorted(seen) == ["plan"] and text(seen["plan"]) == "Shared text."
    assert index.listing() == ["plan", "root"]
