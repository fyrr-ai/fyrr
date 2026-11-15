"""The observed polling cadence determines the bounded 12-hour rotation budget."""

import math

import pytest

from fyrr_connectors.github.feeds import REFRESH_TARGET, refresh_budget


@pytest.mark.parametrize(
    ("count", "elapsed", "expected"),
    [
        (0, 900, 0),
        (1, 900, 1),
        (10_000, 900, 209),
        (10_000, 1800, 417),
        (10_000, 18_000, 5000),
        (10_000, 25_200, 10_000),
        (100_000, REFRESH_TARGET * 3, 100_000),
        (100_000, 0, 3),
    ],
)
def test_adaptive_refresh_budget(count, elapsed, expected):
    assert refresh_budget(count, elapsed) == expected


@pytest.mark.parametrize("elapsed", [1, 60, 900, 1800, 17_000, 25_200, REFRESH_TARGET])
def test_budget_completes_a_rotation_within_target_at_observed_cadence(elapsed):
    count = 10_000
    budget = refresh_budget(count, elapsed)
    assert math.ceil(count / budget) * elapsed <= REFRESH_TARGET
    assert 1 <= budget <= count
