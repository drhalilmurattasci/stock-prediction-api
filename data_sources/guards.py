"""Lightweight, testable cost/rate guards for vendor calls.

Enforcement is deliberately minimal for P1: an in-memory per-vendor fixed-window
rate limit plus an optional total call budget. A Redis-backed, worker-shared
implementation is a later concern; the ``CostRateGuard`` interface (in
``data_sources.base``) stays stable across that change.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable

from data_sources.base import CostBudgetExceeded, VendorRateLimitError


class NullCostRateGuard:
    """No-op guard: always allows. The default when no budgeting is configured."""

    async def acquire(self, vendor: str, *, cost: int = 1, endpoint: str | None = None) -> None:
        return None


class InMemoryCostRateGuard:
    """Per-vendor fixed-window rate limit + optional total call budget.

    Single-process only. Deterministic and testable: inject a monotonic ``clock``
    (seconds) so tests never touch real time.
    """

    def __init__(
        self,
        *,
        max_calls_per_window: int,
        window_seconds: float,
        total_budget: int | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if max_calls_per_window <= 0:
            raise ValueError("max_calls_per_window must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if total_budget is not None and total_budget < 0:
            raise ValueError("total_budget must be None or non-negative")
        self.max_calls = max_calls_per_window
        self.window = window_seconds
        self.total_budget = total_budget
        self._clock = clock or time.monotonic
        self._window_start: dict[str, float] = {}
        self._window_count: dict[str, int] = defaultdict(int)
        self._spent: dict[str, int] = defaultdict(int)

    async def acquire(self, vendor: str, *, cost: int = 1, endpoint: str | None = None) -> None:
        if cost < 1:
            raise ValueError("cost must be >= 1")
        now = self._clock()
        start = self._window_start.get(vendor)
        if start is None or now - start >= self.window:
            self._window_start[vendor] = now
            self._window_count[vendor] = 0

        if self._window_count[vendor] + cost > self.max_calls:
            raise VendorRateLimitError(
                f"{vendor}: exceeded {self.max_calls} calls per {self.window}s window"
            )
        if self.total_budget is not None and self._spent[vendor] + cost > self.total_budget:
            raise CostBudgetExceeded(f"{vendor}: exceeded total call budget of {self.total_budget}")

        self._window_count[vendor] += cost
        self._spent[vendor] += cost

    def snapshot(self, vendor: str) -> dict[str, int]:
        """Current window count and total spend for a vendor (for metrics/tests)."""
        return {
            "window_count": self._window_count[vendor],
            "spent": self._spent[vendor],
        }


class AsyncPacingCostRateGuard:
    """Serialize calls and wait for fixed-window capacity instead of dropping work.

    This guard is intended for endpoints where one logical batch necessarily
    expands to many vendor requests (for example, one official daily-close
    request per exchange session). Instances are event-loop local; create one
    with the provider rather than caching it across ``asyncio.run`` calls.
    """

    def __init__(
        self,
        *,
        max_calls_per_window: int,
        window_seconds: float,
        total_budget: int | None = None,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if max_calls_per_window <= 0:
            raise ValueError("max_calls_per_window must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if total_budget is not None and total_budget < 0:
            raise ValueError("total_budget must be None or non-negative")
        self.max_calls = max_calls_per_window
        self.window = window_seconds
        self.total_budget = total_budget
        self._clock = clock or time.monotonic
        self._sleep = sleep
        self._window_start: dict[str, float] = {}
        self._window_count: dict[str, int] = defaultdict(int)
        self._spent: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    async def acquire(self, vendor: str, *, cost: int = 1, endpoint: str | None = None) -> None:
        del endpoint
        if cost < 1:
            raise ValueError("cost must be >= 1")
        if cost > self.max_calls:
            raise ValueError("cost must not exceed max_calls_per_window")

        async with self._lock:
            if self.total_budget is not None and self._spent[vendor] + cost > self.total_budget:
                raise CostBudgetExceeded(
                    f"{vendor}: exceeded total call budget of {self.total_budget}"
                )

            while True:
                now = self._clock()
                start = self._window_start.get(vendor)
                if start is None or now - start >= self.window:
                    self._window_start[vendor] = now
                    self._window_count[vendor] = 0
                    start = now

                if self._window_count[vendor] + cost <= self.max_calls:
                    self._window_count[vendor] += cost
                    self._spent[vendor] += cost
                    return

                await self._sleep(max(start + self.window - now, 0.0))

    def snapshot(self, vendor: str) -> dict[str, int]:
        """Current window count and total spend for a vendor."""

        return {
            "window_count": self._window_count[vendor],
            "spent": self._spent[vendor],
        }
