"""Unit tests for the in-memory cost/rate guard (deterministic, injected clock)."""

from __future__ import annotations

import pytest

from data_sources.base import CostBudgetExceeded, VendorRateLimitError
from data_sources.guards import InMemoryCostRateGuard, NullCostRateGuard


async def test_null_guard_always_allows():
    guard = NullCostRateGuard()
    for _ in range(100):
        await guard.acquire("polygon")


async def test_rate_limit_within_window_then_resets():
    now = {"t": 0.0}
    guard = InMemoryCostRateGuard(max_calls_per_window=2, window_seconds=60, clock=lambda: now["t"])

    await guard.acquire("polygon")
    await guard.acquire("polygon")
    with pytest.raises(VendorRateLimitError):
        await guard.acquire("polygon")

    # advance past the window -> counter resets
    now["t"] = 61.0
    await guard.acquire("polygon")
    assert guard.snapshot("polygon")["window_count"] == 1


async def test_rate_limit_is_per_vendor():
    guard = InMemoryCostRateGuard(max_calls_per_window=1, window_seconds=60, clock=lambda: 0.0)
    await guard.acquire("polygon")
    await guard.acquire("fmp")  # different vendor, own window
    with pytest.raises(VendorRateLimitError):
        await guard.acquire("polygon")


async def test_cost_must_be_positive_and_does_not_mutate_state():
    guard = InMemoryCostRateGuard(max_calls_per_window=5, window_seconds=60, clock=lambda: 0.0)
    for bad in (0, -1, -100):
        with pytest.raises(ValueError, match="cost"):
            await guard.acquire("polygon", cost=bad)
    # a rejected call must not touch the counters (no limit bypass)
    assert guard.snapshot("polygon") == {"window_count": 0, "spent": 0}


def test_constructor_rejects_bad_config():
    with pytest.raises(ValueError, match="max_calls_per_window"):
        InMemoryCostRateGuard(max_calls_per_window=0, window_seconds=60)
    with pytest.raises(ValueError, match="window_seconds"):
        InMemoryCostRateGuard(max_calls_per_window=5, window_seconds=0)
    with pytest.raises(ValueError, match="total_budget"):
        InMemoryCostRateGuard(max_calls_per_window=5, window_seconds=60, total_budget=-1)


async def test_total_budget_exhausts():
    guard = InMemoryCostRateGuard(
        max_calls_per_window=1000,
        window_seconds=1,
        total_budget=3,
        clock=lambda: 0.0,
    )
    for _ in range(3):
        await guard.acquire("polygon")
    with pytest.raises(CostBudgetExceeded):
        await guard.acquire("polygon")
    assert guard.snapshot("polygon")["spent"] == 3
