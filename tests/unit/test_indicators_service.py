"""Adversarial service tests for the bounded stored-indicators window."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import exchange_calendars as xcals
import pandas as pd
import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.bars import Bar
from app.schemas.indicators import IndicatorFilters
from app.services.indicators import (
    MAX_OBSERVATIONS,
    WINDOW_POLICY_HASH,
    InsufficientIndicatorHistory,
    InvalidIndicatorWindow,
    build_indicators_statement,
    read_indicators,
)
from ml.features.technical import IndicatorConfig, indicator_policy_hash

_CALENDAR = xcals.get_calendar("XNYS")
_SESSION_LABELS = tuple(
    _CALENDAR.sessions_in_range(pd.Timestamp("2025-01-02"), pd.Timestamp("2026-07-13"))
)
_INDICATOR_POLICY_HASH = "sha256:fca9dc9c0feaeb26ee851da6b2ac127eff72b0a972ed6b5385616869fd45e0c1"
_WINDOW_POLICY_HASH = "sha256:59bc7fdbbdd7f8153ace97bcc75f2a445370720536fdb9d80d25c1b7204d9310"


class _FakeScalarResult:
    def __init__(self, rows: list[Bar]) -> None:
        self._rows = rows

    def all(self) -> list[Bar]:
        return self._rows


class _FakeResult:
    def __init__(self, rows: list[Bar]) -> None:
        self._rows = rows

    def scalars(self) -> _FakeScalarResult:
        return _FakeScalarResult(self._rows)


class _FakeSession:
    def __init__(self, rows_descending: list[Bar]) -> None:
        self._rows_descending = rows_descending
        self.statements: list[Any] = []
        self.transaction_active = False
        self.rollback_calls = 0

    def in_transaction(self) -> bool:
        return self.transaction_active

    async def execute(self, statement: Any) -> _FakeResult:
        self.statements.append(statement)
        self.transaction_active = True
        return _FakeResult(self._rows_descending)

    async def rollback(self) -> None:
        self.rollback_calls += 1
        self.transaction_active = False


def _bar(label: pd.Timestamp, index: int, *, close: float | None = None) -> Bar:
    timestamp = _CALENDAR.session_close(label).to_pydatetime().astimezone(UTC)
    value = (
        close
        if close is not None
        else 100.0 + index * 0.017 + ((index * index + 3 * index) % 17) * 0.11
    )
    fetched_at = timestamp + timedelta(minutes=1)
    return Bar(
        symbol="MSFT",
        timespan="day",
        multiplier=1,
        ts=timestamp,
        source="polygon_open_close",
        adjustment_basis="raw",
        open=value - 0.25,
        high=value + 0.75,
        low=value - 0.75,
        close=value,
        volume=1_000_000.0 + index,
        vwap=value + 0.05,
        trade_count=10_000 + index,
        fetched_at=fetched_at,
        as_of=fetched_at + timedelta(minutes=1),
        recorded_at=fetched_at + timedelta(minutes=2),
        version_creator_xid=1,
    )


def _rows(count: int, *, end_offset: int = 0) -> list[Bar]:
    """Return chronological canonical bars ending ``end_offset`` sessions ago."""

    stop = len(_SESSION_LABELS) - end_offset if end_offset else len(_SESSION_LABELS)
    labels = _SESSION_LABELS[stop - count : stop]
    first_index = stop - count
    return [_bar(label, first_index + offset) for offset, label in enumerate(labels)]


def _session(rows_chronological: list[Bar]) -> _FakeSession:
    # Postgres returns the query's DESC order; the service reverses its retained window.
    return _FakeSession(list(reversed(rows_chronological)))


async def _read(rows_chronological: list[Bar]):
    session = _session(rows_chronological)
    response = await read_indicators(
        cast(AsyncSession, session),
        " msft ",
        IndicatorFilters(),
    )
    return response, session


def _normalized_sql(statement: Any) -> str:
    compiled = statement.compile(
        dialect=postgresql.dialect(),
        compile_kwargs={"literal_binds": True},
    )
    return " ".join(str(compiled).split())


def test_statement_is_the_exact_bounded_canonical_series_query():
    plus_three = timezone(timedelta(hours=3))
    filters = IndicatorFilters(end=datetime(2026, 7, 14, 3, tzinfo=plus_three))

    sql = _normalized_sql(build_indicators_statement(" msft ", filters))

    assert sql == (
        "SELECT bars.symbol, bars.timespan, bars.multiplier, bars.ts, bars.source, "
        "bars.adjustment_basis, bars.open, bars.high, bars.low, bars.close, bars.volume, "
        "bars.vwap, bars.trade_count, bars.fetched_at, bars.as_of, bars.recorded_at, "
        "bars.version_creator_xid FROM bars WHERE bars.symbol = 'MSFT' AND "
        "bars.source = 'polygon_open_close' AND bars.timespan = 'day' AND "
        "bars.multiplier = 1 AND bars.adjustment_basis = 'raw' AND "
        "bars.ts < '2026-07-14 00:00:00+00:00' ORDER BY bars.ts DESC LIMIT 259"
    )
    assert "bars.as_of" not in sql.split(" WHERE ", maxsplit=1)[1]


async def test_259_query_rows_drop_only_the_oldest_and_retain_newest_258():
    rows = _rows(MAX_OBSERVATIONS + 1)

    response, session = await _read(rows)

    assert response.count == MAX_OBSERVATIONS
    assert response.window.older_data_excluded is True
    assert response.window.input_start == rows[1].ts
    assert response.window.input_end == rows[-1].ts
    assert [row.timestamp for row in response.observations] == [row.ts for row in rows[1:]]
    assert _normalized_sql(session.statements[0]).endswith("ORDER BY bars.ts DESC LIMIT 259")


async def test_exactly_258_rows_do_not_claim_that_older_data_was_excluded():
    rows = _rows(MAX_OBSERVATIONS)

    response, session = await _read(rows)

    assert response.count == MAX_OBSERVATIONS
    assert response.window.older_data_excluded is False
    assert response.window.input_start == rows[0].ts
    assert response.window.input_end == rows[-1].ts
    assert session.rollback_calls == 1
    assert session.transaction_active is False


async def test_empty_series_returns_an_honest_empty_window():
    end = datetime(2026, 7, 14, tzinfo=UTC)
    session = _FakeSession([])

    response = await read_indicators(
        cast(AsyncSession, session),
        " msft ",
        IndicatorFilters(end=end),
    )

    assert response.symbol == "MSFT"
    assert response.count == 0
    assert response.observations == []
    assert response.data_as_of is None
    assert response.data_recorded_at is None
    assert response.window.requested_end == end
    assert response.window.input_start is None
    assert response.window.input_end is None
    assert response.window.input_sha256 is None
    assert response.window.continuity is None
    assert response.window.older_data_excluded is False


@pytest.mark.parametrize("count", [1, 33])
async def test_nonempty_windows_below_34_observations_fail_with_controlled_409(count: int):
    session = _session(_rows(count))

    with pytest.raises(InsufficientIndicatorHistory) as excinfo:
        await read_indicators(
            cast(AsyncSession, session),
            "MSFT",
            IndicatorFilters(),
        )

    assert excinfo.value.status_code == 409
    assert excinfo.value.code == "insufficient_indicator_history"
    assert excinfo.value.details == {
        "observed": count,
        "required": 34,
        "symbol": "MSFT",
    }


async def test_34_observations_succeed_with_exact_structural_warmup_topology():
    rows = _rows(34)

    response, _ = await _read(rows)

    assert response.count == 34
    assert response.window.required_observations == 34
    assert response.window.continuity == "exact_consecutive_regular_session_closes"
    assert response.window.warmup_semantics == "structural_nulls"
    assert response.window.recursive_seed_semantics == "window_relative"
    assert response.window.latest_session_completeness == "not_evaluated"
    assert [row.timestamp for row in response.observations] == [row.ts for row in rows]

    fields_and_first_values = {
        "simple_return": 1,
        "log_return": 1,
        "sma": 19,
        "ema": 19,
        "return_volatility": 20,
        "rsi": 14,
        "macd_line": 25,
        "macd_signal": 33,
        "macd_histogram": 33,
        "bollinger_lower": 19,
        "bollinger_middle": 19,
        "bollinger_upper": 19,
        "atr": 14,
    }
    for field, first_value in fields_and_first_values.items():
        values = [getattr(row, field) for row in response.observations]
        assert values[:first_value] == [None] * first_value
        assert values[first_value] is not None
        assert all(value is not None for value in values[first_value:])


async def test_a_missing_xnys_session_is_rejected_even_with_34_rows():
    rows = _rows(35)
    del rows[17]

    with pytest.raises(InvalidIndicatorWindow) as excinfo:
        await _read(rows)

    assert excinfo.value.status_code == 409
    assert excinfo.value.details == {
        "reason": "session_gap_or_duplicate",
        "observed_count": 34,
        "expected_count": 35,
    }


async def test_a_session_date_at_any_time_other_than_regular_close_is_rejected():
    rows = _rows(34)
    rows[17].ts += timedelta(microseconds=1)

    with pytest.raises(InvalidIndicatorWindow) as excinfo:
        await _read(rows)

    assert excinfo.value.status_code == 409
    assert excinfo.value.details == {"reason": "not_regular_session_close"}


async def test_timestamp_outside_pinned_calendar_range_is_controlled_409():
    rows = _rows(34)
    rows[0].ts = datetime(1980, 1, 2, 21, tzinfo=UTC)

    with pytest.raises(InvalidIndicatorWindow) as excinfo:
        await _read(rows)

    assert excinfo.value.status_code == 409
    assert excinfo.value.details == {"reason": "calendar_out_of_bounds"}


async def test_zero_close_is_a_controlled_invalid_window_not_a_math_or_schema_error():
    rows = _rows(34)
    row = rows[20]
    row.open = 0.0
    row.high = 0.0
    row.low = 0.0
    row.close = 0.0

    with pytest.raises(InvalidIndicatorWindow) as excinfo:
        await _read(rows)

    assert excinfo.value.status_code == 409
    assert excinfo.value.code == "invalid_indicator_window"
    assert excinfo.value.details == {"reason": "nonpositive_close"}


async def test_extreme_finite_inputs_that_overflow_calculation_fail_as_controlled_409():
    rows = _rows(34)
    for index, row in enumerate(rows):
        value = 1e-308 if index % 2 == 0 else 1e308
        row.open = value
        row.high = value
        row.low = value
        row.close = value

    with pytest.raises(InvalidIndicatorWindow) as excinfo:
        await _read(rows)

    assert excinfo.value.status_code == 409
    assert excinfo.value.details == {"reason": "calculation_input_invalid"}


async def test_input_digest_is_stable_but_changes_for_same_timestamp_restatement():
    rows = _rows(34)
    original, _ = await _read(rows)
    replay, _ = await _read(rows)

    restated = list(rows)
    prior = rows[-1]
    revised_close = prior.close + 3.0
    restated[-1] = Bar(
        symbol=prior.symbol,
        timespan=prior.timespan,
        multiplier=prior.multiplier,
        ts=prior.ts,
        source=prior.source,
        adjustment_basis=prior.adjustment_basis,
        open=prior.open,
        high=revised_close + 0.5,
        low=prior.low,
        close=revised_close,
        volume=prior.volume + 10.0,
        vwap=revised_close,
        trade_count=(prior.trade_count or 0) + 1,
        fetched_at=prior.fetched_at + timedelta(hours=1),
        as_of=prior.as_of + timedelta(hours=1),
        recorded_at=prior.recorded_at + timedelta(hours=1),
        version_creator_xid=2,
    )
    revised, _ = await _read(restated)

    assert original.window.input_sha256 == replay.window.input_sha256
    assert original.window.input_sha256 != revised.window.input_sha256
    assert original.window.input_start == revised.window.input_start
    assert original.window.input_end == revised.window.input_end
    assert original.observations[-1].timestamp == revised.observations[-1].timestamp
    assert original.observations[-1].close != revised.observations[-1].close
    assert original.data_as_of != revised.data_as_of


async def test_rolling_window_changes_digest_bounds_and_window_relative_recursive_seed():
    all_rows = _rows(MAX_OBSERVATIONS + 1)
    earlier, _ = await _read(all_rows[:-1])
    rolled, _ = await _read(all_rows)

    assert earlier.window.input_start == all_rows[0].ts
    assert earlier.window.input_end == all_rows[-2].ts
    assert rolled.window.input_start == all_rows[1].ts
    assert rolled.window.input_end == all_rows[-1].ts
    assert earlier.window.input_sha256 != rolled.window.input_sha256
    assert earlier.window.older_data_excluded is False
    assert rolled.window.older_data_excluded is True

    common_timestamp = all_rows[20].ts
    earlier_value = next(
        row.ema for row in earlier.observations if row.timestamp == common_timestamp
    )
    rolled_value = next(row.ema for row in rolled.observations if row.timestamp == common_timestamp)
    assert earlier_value is not None
    assert rolled_value is not None
    assert earlier_value != pytest.approx(rolled_value, rel=1e-15, abs=1e-15)


async def test_service_does_not_rollback_a_caller_owned_transaction():
    rows = _rows(34)
    session = _session(rows)
    session.transaction_active = True

    response = await read_indicators(
        cast(AsyncSession, session),
        "MSFT",
        IndicatorFilters(),
    )

    assert response.count == 34
    assert session.rollback_calls == 0
    assert session.transaction_active is True


def test_default_formula_and_window_policy_hashes_match_reviewed_goldens():
    assert indicator_policy_hash(IndicatorConfig()) == _INDICATOR_POLICY_HASH
    assert WINDOW_POLICY_HASH == _WINDOW_POLICY_HASH
