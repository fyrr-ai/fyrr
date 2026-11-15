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


def test_pages_and_rows_match_the_golden_output(notion: FakeNotion) -> None:
    _world(notion)
    result = testkit.run_sync(
        "notion", "content", settings=SETTINGS, scope=_scope(), readers=("grp:staff",)
    )
    testkit.assert_golden(result, GOLDEN / "content.json")
    listing = testkit.run_sync("notion", "reconcile", settings=SETTINGS, scope=_scope())
    assert not listing.documents[-1]["body"]["complete"], "testkit has no revision view"
    listed = sorted(notion.names[r] for part in listing.documents for r in part["body"]["root_ids"])
    assert listed == ["notes", "plan", "risks", "root", "task-empty", "task-login"]


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
