"""Cursor recovery across channel migration and listing."""

from dataclasses import replace

import pytest


@pytest.mark.parametrize("max_rows", [1, 2, 100])
def test_slack_migration_restart_after_delete_ack_has_replacement(slack, slack_index, max_rows):
    parent = slack.post(text="Old parent.")
    old = next(iter(slack_index.sync()))
    slack_index.limits = replace(slack_index.limits, max_rows=max_rows)
    slack.post(seconds=7200, text="New tail.")
    slack.post(thread=parent["ts"], text="Late reply.")
    replacement = old.replace(":window:", ":thread:")
    deleted = False
    for _ in range(2):
        iterator = iter(slack_index.run(cursor=slack_index.cursor))
        for batch in iterator:
            existed = replacement in slack_index.published
            changes = slack_index.apply(batch)
            if any(item["op"] == "delete" and item["root_id"] == old for item in changes):
                assert replacement in slack_index.published
                if not existed:
                    roots = [item["root_id"] for item in changes]
                    assert roots.index(replacement) < roots.index(old)
                deleted = True
                break  # Crash immediately after acknowledgment of the delete.
        iterator.close()
        if deleted:
            break
    assert deleted
    slack_index.sync()
    assert replacement in slack_index.published
    assert any(
        "Late reply." in section["text"]
        for section in slack_index.published[replacement]["sections"]
    )


def test_slack_listing_before_refresh_keeps_unpublished_replacement_anchor(slack, slack_index):
    parent = slack.post(text="Deleted first.")
    survivor = slack.post(text="Survivor.")
    for _ in range(9):
        slack.post(seconds=400)
    first = min(slack_index.sync())
    slack.remove("C001", parent["ts"])
    replacement = first.rsplit(":", 1)[0] + ":" + survivor["ts"].replace(".", "")
    assert replacement not in slack_index.published
    roots = slack_index.listing()
    assert first in roots
    for key in set(slack_index.published) - roots:
        slack_index.published.pop(key)
        slack_index.revisions.pop(key)
    slack.clock += 43200_000000
    slack_index.sync()
    assert replacement in slack_index.published
    assert first not in slack_index.listing()


def test_slack_channel_return_replays_history_after_listing_removed_roots(slack, slack_index):
    slack_index.scope["include"] = ["platform*"]
    parent = slack.post(text="Old thread.")
    slack.post(thread=parent["ts"], text="Old reply.")
    slack.post(seconds=7200, text="Recent window.")
    original = set(slack_index.sync())
    slack.channels["C001"]["name"] = "outside"
    assert slack_index.listing() == set()
    slack_index.revisions.clear()
    slack_index.published.clear()
    slack.channels["C001"]["name"] = "platform-return"
    slack.clock += 3 * 86400_000000
    slack_index.sync()
    assert set(slack_index.published) == original
