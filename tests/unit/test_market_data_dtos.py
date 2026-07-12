"""Validation tests for market-data DTOs (OHLCVBar OHLC/price consistency)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from data_sources.base import OHLCVBar

TS = datetime(2026, 7, 6, tzinfo=UTC)


def _bar(**overrides) -> OHLCVBar:
    fields = {
        "symbol": "AAPL",
        "timestamp": TS,
        "timespan": "day",
        "multiplier": 1,
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "volume": 100.0,
        "source": "polygon",
        "fetched_at": TS,
    }
    fields.update(overrides)
    return OHLCVBar(**fields)


def test_valid_bar_is_accepted():
    bar = _bar()
    assert bar.high == 12.0 and bar.low == 9.0


@pytest.mark.parametrize(
    "bad",
    [
        {"high": 8.0},  # high below low
        {"high": 10.5},  # high below close (11)
        {"low": 11.5},  # low above open (10)
        {"high": 9.0, "low": 9.5},  # high below low
    ],
)
def test_impossible_ohlc_is_rejected(bad):
    with pytest.raises(ValidationError):
        _bar(**bad)


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
def test_negative_values_rejected(field):
    with pytest.raises(ValidationError):
        _bar(**{field: -1.0})


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume", "vwap"])
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_values_rejected_at_ingestion(field: str, bad: float):
    # The OHLC comparison validator is blind to NaN (every </> is False) and
    # the DB nonnegativity CHECKs pass NaN in Postgres, so allow_inf_nan=False
    # is the load-bearing guard: a non-finite vendor value must never persist —
    # a stored one would 500 every finite-only /v1/prices read of its page.
    with pytest.raises(ValidationError):
        _bar(**{field: bad})


def test_naive_timestamp_rejected():
    with pytest.raises(ValidationError):
        _bar(timestamp=datetime(2026, 7, 6))  # noqa: DTZ001 - intentionally naive
