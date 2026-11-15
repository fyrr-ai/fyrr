"""Cursor recovery across channel migration and listing."""

from dataclasses import replace

import pytest


@pytest.mark.parametrize("max_rows", [1])
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
