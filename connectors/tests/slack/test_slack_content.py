"""Fixed windows/threads, authoritative scope, ack, listing and quarantine."""

import json
from dataclasses import replace
from pathlib import Path

import pytest
from slack_kit import Index

from fyrr_connectors import testkit
from fyrr_connectors.arrow import rows_to_json
from fyrr_connectors.provider import provider_info
from fyrr_connectors.slack import Slack
from fyrr_connectors.slack.client import ApiError


def bodies(changes):
    return "\n".join(
        section["text"] for change in changes.values() for section in change.get("sections", [])
    )


def test_slack_windows_gaps_metadata_mentions_and_exact_fixed_readers(slack, slack_index):
    slack.post(text="Hello <@U001> in <#C001|platform>.")
    slack.post(text="Second.", seconds=300)
    slack.post(text="Separate.", seconds=301)
    changes = slack_index.sync()
    assert len(changes) == 2
    assert "@Fixture author" in bodies(changes) and "#platform" in bodies(changes)
    assert all(item["readers"] == ["grp:staff"] for item in changes.values())
    assert all(item["metadata"]["author"] == "Fixture author" for item in changes.values())
    assert all(
        item["metadata"]["created_at"] and item["metadata"]["updated_at"]
        for item in changes.values()
    )
    assert all(
        section["location"]["kind"] == "message"
        for item in changes.values()
        for section in item["sections"]
    )
    assert not any(
        method in ("users.list", "conversations.members") for method, _ in slack.requests
    )
    assert slack_index.sync() == {}
    assert slack_index.listing() == set(changes)


def test_slack_first_deleted_anchor_stays_stable_within_ledger(slack, slack_index):
    first = slack.post(text="First.")
    slack.post(text="Survivor.")
    initial = slack_index.sync()
    slack.remove("C001", first["ts"])
    updated = slack_index.sync()
    assert set(updated) == set(initial)
    assert bodies(updated) == "Survivor."
    assert slack_index.listing() == set(initial)


def test_slack_old_message_edit_refreshes_the_published_window(slack, slack_index):
    first = slack.post(text="Old.")
    slack_index.sync()
    slack.clock += 2 * 86400 * 1_000_000
    slack.edit("C001", first["ts"], "Changed old text.")
    assert bodies(slack_index.sync()) == "Changed old text."


def test_slack_old_thread_growth_and_reply_deletion_without_new_parent_time(slack, slack_index):
    parent = slack.post(text="Parent.")
    slack.post(text="Newer conversation.", seconds=7200)
    initial = slack_index.sync()
    slack.clock += 2 * 86400 * 1_000_000
    reply = slack.post(text="Late reply.", thread=parent["ts"])
    changes = slack_index.sync()
    assert "Late reply." in bodies(changes)
    assert any("thread" in key for key in changes)
    assert min(initial) not in slack_index.published
    slack.remove("C001", reply["ts"])
    slack_index.sync()
    assert "Late reply." not in bodies(slack_index.published)


@pytest.mark.parametrize(
    "subtype",
    [
        "channel_join",
        "channel_leave",
        "channel_name",
        "channel_topic",
        "channel_purpose",
        "unknown_privileged",
    ],
)
def test_slack_system_messages_are_dropped(slack, slack_index, subtype):
    slack.post(text="System marker.", subtype=subtype)
    assert slack_index.sync() == {}


@pytest.mark.parametrize("bots", [False, True])
def test_slack_bot_policy_applies_inside_threads_without_losing_human_replies(
    slack, slack_index, bots
):
    slack_index.scope["bots"] = bots
    parent = slack.post(text="Bot parent.", bot=True)
    slack.post(text="Human reply.", thread=parent["ts"])
    slack.post(text="Bot reply.", bot=True, thread=parent["ts"])
    text = bodies(slack_index.sync())
    assert "Human reply." in text
    assert ("Bot parent." in text) is bots and ("Bot reply." in text) is bots


@pytest.mark.parametrize("subtype", ["me_message", "file_share"])
def test_slack_user_text_is_kept_without_dereferencing_files(slack, slack_index, subtype):
    slack.post(
        text="User prose.",
        subtype=subtype,
        files=[{"url_private": "https://files.invalid/private"}],
    )
    assert bodies(slack_index.sync()) == "User prose."


@pytest.mark.parametrize(
    "bad", ["\0", "x" * 17000, {"not": "text"}], ids=["nul", "oversize", "shape"]
)
def test_slack_bad_document_quarantines_without_stalling(slack, slack_index, bad):
    slack.post(text=bad)
    slack.post(text="Good.", seconds=400)
    changes = slack_index.sync()
    assert len(changes) == 2
    assert any(item["sections"] == [{"text": "\0"}] for item in changes.values())
    assert "Good." in bodies(changes)
    assert slack_index.cursor


def test_slack_quarantine_keeps_last_good_and_recovers(slack, slack_index):
    message = slack.post(text="Good.")
    slack_index.sync()
    old = dict(slack_index.published)
    slack.edit("C001", message["ts"], "\0")
    assert "\0" in bodies(slack_index.sync())
    assert slack_index.published == old
    slack.edit("C001", message["ts"], "Recovered.")
    assert bodies(slack_index.sync()) == "Recovered."


def test_slack_ack_cursor_and_replay_use_pending_revisions(slack, slack_index):
    for _ in range(3):
        slack.post(text="Part.", seconds=400)
    slack_index.limits = replace(slack_index.limits, max_rows=1)
    stream = slack_index.run()
    first = next(stream)
    slack_index.apply(first)
    assert json.loads(first.cursor)["channels"] == {}
    stream.close()
    slack_index.sync()
    assert len(slack_index.published) == 3
    assert all("@" not in batch for batch in [slack_index.cursor])


def test_slack_scope_narrowing_and_archived_channels(slack, slack_index):
    slack.channel("C002", "private", private=True)
    slack.channels["C002"]["is_archived"] = True
    slack.post("C001")
    slack.post("C002")
    changes = slack_index.sync()
    assert len(changes) == 2
    assert any(item["metadata"]["status"] == "archived" for item in changes.values())
    slack_index.scope["exclude"] = ["private"]
    assert all(":C002:" not in key for key in slack_index.listing())


@pytest.mark.parametrize("error", ["channel_not_found", "not_in_channel"])
def test_slack_authoritative_channel_loss_does_not_take_other_channels_down(
    slack, slack_index, error
):
    slack.channel("C002", "second")
    slack.post("C001")
    slack.post("C002")
    slack_index.sync()
    slack.after(
        lambda method, params: method == "conversations.list", lambda: slack.channels.pop("C001")
    )
    changes = slack_index.sync()
    assert not changes
    assert all(":C001:" not in key for key in slack_index.listing())
    assert any(":C002:" in key for key in slack_index.listing())


@pytest.mark.parametrize(
    "error", ["internal_error", "missing_scope", "invalid_auth", "ekm_access_denied"]
)
def test_slack_ambiguous_or_wide_failure_never_completes_listing(slack, slack_index, error):
    slack.post()
    slack_index.sync()
    slack.fail("conversations.history", error)
    stream = slack_index.run("reconcile")
    with pytest.raises(ApiError):
        next(stream)


def test_slack_late_history_page_failure_preserves_cursor(slack, slack_index):
    for _ in range(4):
        slack.post(seconds=400)
    slack.after(
        lambda method, params: method == "conversations.history",
        lambda: slack.fail("conversations.history", "internal_error"),
    )
    with pytest.raises(ApiError):
        slack_index.sync()
    assert slack_index.cursor is None


def test_slack_reply_parent_is_verified_before_ingestion(slack, slack_index):
    parent = slack.post()
    slack.post(thread=parent["ts"], thread_override=True)
    slack.messages["C001"][1]["channel"] = "COTHER"
    with pytest.raises(ValueError, match="thread_parent_invalid"):
        slack_index.sync()


def test_slack_channel_conversion_during_listing_fails_without_completion(slack, slack_index):
    slack.post()
    slack_index.sync()
    slack.after(
        lambda method, params: method == "conversations.history",
        lambda: slack.channels["C001"].update(is_private=True),
    )
    with pytest.raises(ValueError, match="changed_during_listing"):
        next(slack_index.run("reconcile"))


def test_slack_64_selected_channel_limit_is_a_clear_configuration_error(slack, slack_index):
    for number in range(2, 66):
        slack.channel(f"C{number:03}", f"channel-{number}")
    with pytest.raises(ValueError, match="64_channels_use_several_sources"):
        slack_index.sync()


def test_slack_window_units_split_before_16_kib(slack, slack_index):
    for _ in range(3):
        slack.post(text="x" * 8000)
    assert len(slack_index.sync()) == 2
    assert all(
        sum(len(s["text"].encode()) for s in item["sections"]) <= 16384
        for item in slack_index.published.values()
    )


def test_slack_content_golden(slack):
    slack.post(text="Golden body.")
    info = provider_info(
        name="slack", connector=Slack, min_sdk_version="0.1.0", connection_types=("fixed",)
    )
    result = testkit.run_sync(
        info,
        "content",
        permissions="fixed",
        settings=Index(slack).settings,
        scope=Index(slack).scope,
    )
    testkit.assert_golden(result, Path(__file__).parent / "golden/content.json")


def test_slack_deleting_all_tracked_messages_emits_a_delete(slack, slack_index):
    message = slack.post()
    original = slack_index.sync()
    slack.remove("C001", message["ts"])
    changes = slack_index.sync()
    assert set(changes) == set(original)
    assert all(item["op"] == "delete" for item in changes.values())
    assert slack_index.listing() == set()


def test_slack_ledger_overflow_rekeys_deleted_first_message_until_complete_listing(
    slack, slack_index
):
    first = slack.post(text="First.", seconds=400)
    slack.post(text="Survivor.")
    for _ in range(9):
        slack.post(text="Other window.", seconds=400)
    original = slack_index.sync()
    old = min(original)
    assert len(json.loads(slack_index.cursor)["channels"]["C001"]["anchors"]) == 8
    slack.remove("C001", first["ts"])
    changes = slack_index.sync()
    assert old in slack_index.published
    assert any(
        item.get("sections") == [{"text": "Survivor.", "location": item["sections"][0]["location"]}]
        for item in changes.values()
    )
    assert old not in slack_index.listing()


@pytest.mark.parametrize("max_rows", [1, 2])
def test_slack_thread_batch_counts_arrow_rows_not_documents(slack, slack_index, max_rows):
    parent = slack.post(text="Parent.")
    slack.post(text="Reply.", thread=parent["ts"])
    slack_index.limits = replace(slack_index.limits, max_rows=max_rows)
    batches = list(slack_index.run())
    assert all(batch.changes.num_rows <= max_rows for batch in batches)
    changes = [item for batch in batches for item in rows_to_json(batch.changes)]
    # The SDK's Arrow row is a document; its sections remain nested together.
    assert len(changes) == 1 and len(changes[0]["sections"]) == 2
