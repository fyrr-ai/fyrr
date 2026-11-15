"""Boundary configuration, numeric timestamps, cursor size and rotation arithmetic."""

import json

import pytest

from fyrr_connectors.slack.config import Scope
from fyrr_connectors.slack.feeds import ChannelCursor, Cursor, timestamp, wire


@pytest.mark.parametrize("bad", [None, True, "1.0", "1.0000000", "bad", "-1.000000"])
def test_slack_timestamps_are_exact_integer_microseconds(bad):
    with pytest.raises(ValueError):
        timestamp(bad)
    assert wire(timestamp("1767225600.123456")) == "1767225600.123456"


def test_slack_cursor_ledger_is_bounded_for_64_channels_over_a_lifetime():
    cursor = Cursor.decode(None, Scope.parse({"include": ["*"]}), 300)
    for number in range(64):
        cursor.channels[f"C{number:031}"] = ChannelCursor(
            since=10**18 - 1,
            step=10**18 - 1,
            start=10**18 - 1,
            windows=10**18 - 1,
            threads=10**18 - 1,
            anchors=[[n, n + 1] for n in range(500)],
        )
    for _ in range(5):
        encoded = cursor.encode()
        assert len(encoded.encode()) <= 16384
        assert (
            sum(len(value["anchors"]) for value in json.loads(encoded)["channels"].values()) <= 64
        )
        cursor = Cursor.decode(encoded, Scope.parse({"include": ["*"]}), 300)


@pytest.mark.parametrize("field", ["since", "windows", "threads", "step", "start"])
def test_slack_cursor_refuses_principals_in_numeric_state(field):
    scope = Scope.parse({"include": ["*"]})
    state = Cursor.decode(None, scope, 300)
    state.channels["C001"] = ChannelCursor()
    body = json.loads(state.encode())
    body["channels"]["C001"][field] = "private@example.test"
    with pytest.raises(ValueError, match="cursor_invalid"):
        Cursor.decode(json.dumps(body), scope, 300)


def test_slack_changing_workspace_resets_discovery_without_reusing_its_frontier():
    scope = Scope.parse({"include": ["*"]})
    state = Cursor.decode(None, scope, 300, "T001")
    state.channels["C001"] = ChannelCursor(since=10)
    assert Cursor.decode(state.encode(), scope, 300, "T002").channels == {}
