"""Bounded current-snapshot technical indicators for one canonical price lane.

Version one deliberately supports only raw, daily ``polygon_open_close`` bars.
The calculation uses the newest 258 stored observations before an optional
exclusive timestamp.  Recursive indicators are therefore window-relative;
this service does not claim point-in-time reconstruction or canonical values
that can be stitched across different windows.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import lru_cache
from importlib.metadata import version
from typing import Any

import exchange_calendars as xcals
import pandas as pd
from fastapi import status
from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import AppError
from app.db.models.bars import Bar
from app.schemas.indicators import (
    IndicatorFilters,
    IndicatorObservation,
    IndicatorParameters,
    IndicatorsResponse,
    IndicatorWindow,
)
from ml.features.technical import (
    CALCULATION_VERSION,
    IndicatorConfig,
    calculate_indicators,
    indicator_policy_hash,
    required_observations,
)

ENDPOINT_VERSION = "stored-indicators-v1"
MAX_OBSERVATIONS = 258
SOURCE = "polygon_open_close"
TIMESPAN = "day"
MULTIPLIER = 1
ADJUSTMENT_BASIS = "raw"
CALENDAR_NAME = "XNYS"
_CALENDAR_ENGINE_VERSION = "4.13.2"
_PANDAS_VERSION = version("pandas")
_TZDATA_VERSION = version("tzdata")
_CALENDAR_START = "1990-01-01"
_CALENDAR_END = "2100-12-31"
CALENDAR_RULESET = (
    f"exchange-calendars=={_CALENDAR_ENGINE_VERSION};pandas=={_PANDAS_VERSION};"
    f"tzdata=={_TZDATA_VERSION};{CALENDAR_NAME}:{_CALENDAR_START}:{_CALENDAR_END}"
)
SELECTION = "newest_exact_series_before_exclusive_end"
CONTINUITY = "exact_consecutive_regular_session_closes"
LATEST_SESSION_COMPLETENESS = "not_evaluated"
RECURSIVE_SEED_SEMANTICS = "window_relative"
WARMUP_SEMANTICS = "structural_nulls"

_CONFIG = IndicatorConfig()
_REQUIRED_OBSERVATIONS = required_observations(_CONFIG)
_PARAMETERS = IndicatorParameters(**asdict(_CONFIG))


@dataclass(frozen=True, slots=True)
class _IndicatorInput:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None
    trade_count: int | None
    fetched_at: datetime
    as_of: datetime
    recorded_at: datetime


class InsufficientIndicatorHistory(AppError):
    """The selected series exists but cannot produce the v1 bundle."""

    status_code = status.HTTP_409_CONFLICT
    code = "insufficient_indicator_history"


class InvalidIndicatorWindow(AppError):
    """Stored rows violate the canonical v1 calculation-window contract."""

    status_code = status.HTTP_409_CONFLICT
    code = "invalid_indicator_window"


def _sha256_document(document: object) -> str:
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


_WINDOW_POLICY_DOCUMENT: dict[str, object] = {
    "calendar": {
        "engine": "exchange_calendars",
        "engine_version": _CALENDAR_ENGINE_VERSION,
        "name": CALENDAR_NAME,
        "pandas_version": _PANDAS_VERSION,
        "schedule_end": _CALENDAR_END,
        "schedule_start": _CALENDAR_START,
        "target_timestamp": "session_close_utc",
        "tzdata_version": _TZDATA_VERSION,
    },
    "continuity": CONTINUITY,
    "data_semantics": "current_snapshot_not_point_in_time",
    "endpoint_version": ENDPOINT_VERSION,
    "formula_policy_hash": indicator_policy_hash(_CONFIG),
    "input_digest_schema": "ordered-current-bar-ieee754-hex-v1",
    "latest_completed_session": LATEST_SESSION_COMPLETENESS,
    "max_observations": MAX_OBSERVATIONS,
    "recursive_seed_semantics": RECURSIVE_SEED_SEMANTICS,
    "required_observations": _REQUIRED_OBSERVATIONS,
    "selection": SELECTION,
    "series": {
        "adjustment_basis": ADJUSTMENT_BASIS,
        "multiplier": MULTIPLIER,
        "source": SOURCE,
        "timespan": TIMESPAN,
    },
    "warmup_semantics": WARMUP_SEMANTICS,
}
WINDOW_POLICY_HASH = _sha256_document(_WINDOW_POLICY_DOCUMENT)


def build_indicators_statement(
    symbol: str,
    filters: IndicatorFilters,
) -> Select[tuple[Bar]]:
    """Select at most one extra row beyond the fixed calculation window."""

    statement = select(Bar).where(
        Bar.symbol == symbol.strip().upper(),
        Bar.source == SOURCE,
        Bar.timespan == TIMESPAN,
        Bar.multiplier == MULTIPLIER,
        Bar.adjustment_basis == ADJUSTMENT_BASIS,
    )
    if filters.end is not None:
        statement = statement.where(Bar.ts < filters.end)
    return statement.order_by(Bar.ts.desc()).limit(MAX_OBSERVATIONS + 1)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise InvalidIndicatorWindow(
            "Stored indicator timestamps do not satisfy the v1 window contract."
        )
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


@lru_cache(maxsize=1)
def _calendar() -> Any:
    if xcals.__version__ != _CALENDAR_ENGINE_VERSION:
        raise RuntimeError("exchange_calendars version differs from the indicator policy")
    try:
        return xcals.get_calendar(
            CALENDAR_NAME,
            start=_CALENDAR_START,
            end=_CALENDAR_END,
        )
    except (KeyError, ValueError) as exc:
        raise RuntimeError("the pinned indicator exchange calendar is unavailable") from exc


def _validate_consecutive_xnys(rows: list[_IndicatorInput]) -> None:
    if not rows:
        return
    calendar = _calendar()
    labels: list[pd.Timestamp] = []
    try:
        for row in rows:
            timestamp = _utc(row.ts)
            label = pd.Timestamp(timestamp.date())
            if not calendar.is_session(label):
                raise InvalidIndicatorWindow(
                    "Stored indicator observations are not exact consecutive XNYS closes.",
                    details={"reason": "non_session_timestamp"},
                )
            expected_close = _utc(calendar.session_close(label).to_pydatetime())
            if timestamp != expected_close:
                raise InvalidIndicatorWindow(
                    "Stored indicator observations are not exact consecutive XNYS closes.",
                    details={"reason": "not_regular_session_close"},
                )
            labels.append(label)
        expected = tuple(calendar.sessions_in_range(labels[0], labels[-1]))
    except InvalidIndicatorWindow:
        raise
    except (KeyError, OverflowError, ValueError) as exc:
        raise InvalidIndicatorWindow(
            "Stored indicator observations fall outside the pinned XNYS calendar policy.",
            details={"reason": "calendar_out_of_bounds"},
        ) from exc
    if tuple(labels) != expected:
        raise InvalidIndicatorWindow(
            "Stored indicator observations are not exact consecutive XNYS closes.",
            details={
                "reason": "session_gap_or_duplicate",
                "observed_count": len(labels),
                "expected_count": len(expected),
            },
        )


def _input_digest(symbol: str, rows: list[_IndicatorInput]) -> str:
    document: dict[str, Any] = {
        "schema": "ordered-current-bar-ieee754-hex-v1",
        "series": {
            "adjustment_basis": ADJUSTMENT_BASIS,
            "multiplier": MULTIPLIER,
            "source": SOURCE,
            "symbol": symbol,
            "timespan": TIMESPAN,
        },
        "rows": [
            {
                "as_of": _iso(row.as_of),
                "close": float(row.close).hex(),
                "fetched_at": _iso(row.fetched_at),
                "high": float(row.high).hex(),
                "low": float(row.low).hex(),
                "open": float(row.open).hex(),
                "recorded_at": _iso(row.recorded_at),
                "timestamp": _iso(row.ts),
                "trade_count": row.trade_count,
                "volume": float(row.volume).hex(),
                "vwap": None if row.vwap is None else float(row.vwap).hex(),
            }
            for row in rows
        ],
    }
    return _sha256_document(document)


def _window(
    filters: IndicatorFilters,
    rows: list[_IndicatorInput],
    *,
    older_data_excluded: bool,
) -> IndicatorWindow:
    return IndicatorWindow(
        selection=SELECTION,
        calendar=CALENDAR_NAME,
        calendar_ruleset=CALENDAR_RULESET,
        max_observations=MAX_OBSERVATIONS,
        required_observations=_REQUIRED_OBSERVATIONS,
        requested_end=filters.end,
        input_start=rows[0].ts if rows else None,
        input_end=rows[-1].ts if rows else None,
        input_count=len(rows),
        older_data_excluded=older_data_excluded,
        continuity=CONTINUITY if rows else None,
        latest_session_completeness=LATEST_SESSION_COMPLETENESS,
        recursive_seed_semantics=RECURSIVE_SEED_SEMANTICS,
        warmup_semantics=WARMUP_SEMANTICS,
        input_digest_schema="ordered-current-bar-ieee754-hex-v1",
        input_sha256=_input_digest(rows[0].symbol, rows) if rows else None,
    )


def _empty_response(symbol: str, filters: IndicatorFilters) -> IndicatorsResponse:
    return IndicatorsResponse(
        symbol=symbol,
        source=SOURCE,
        timespan=TIMESPAN,
        multiplier=MULTIPLIER,
        adjustment_basis=ADJUSTMENT_BASIS,
        data_semantics="current_snapshot_not_point_in_time",
        endpoint_version=ENDPOINT_VERSION,
        calculation_version=CALCULATION_VERSION,
        indicator_policy_hash=indicator_policy_hash(_CONFIG),
        window_policy_hash=WINDOW_POLICY_HASH,
        parameters=_PARAMETERS,
        window=_window(filters, [], older_data_excluded=False),
        data_as_of=None,
        data_recorded_at=None,
        count=0,
        observations=[],
    )


def _materialize(row: Bar) -> _IndicatorInput:
    """Detach every field needed after the database transaction is released."""

    return _IndicatorInput(
        symbol=row.symbol,
        ts=row.ts,
        open=row.open,
        high=row.high,
        low=row.low,
        close=row.close,
        volume=row.volume,
        vwap=row.vwap,
        trade_count=row.trade_count,
        fetched_at=row.fetched_at,
        as_of=row.as_of,
        recorded_at=row.recorded_at,
    )


async def read_indicators(
    session: AsyncSession,
    symbol: str,
    filters: IndicatorFilters,
) -> IndicatorsResponse:
    """Read, validate, and calculate one bounded current-snapshot window."""

    normalized_symbol = symbol.strip().upper()
    transaction_was_active = bool(session.in_transaction())
    try:
        result = await session.execute(build_indicators_statement(normalized_symbol, filters))
        selected_desc = [_materialize(row) for row in result.scalars().all()]
    finally:
        if not transaction_was_active and session.in_transaction():
            await session.rollback()
    older_data_excluded = len(selected_desc) > MAX_OBSERVATIONS
    rows = list(reversed(selected_desc[:MAX_OBSERVATIONS]))
    if not rows:
        return _empty_response(normalized_symbol, filters)
    if len(rows) < _REQUIRED_OBSERVATIONS:
        raise InsufficientIndicatorHistory(
            "The stored series has too few observations for the v1 indicator bundle.",
            details={
                "observed": len(rows),
                "required": _REQUIRED_OBSERVATIONS,
                "symbol": normalized_symbol,
            },
        )

    _validate_consecutive_xnys(rows)
    if any(row.close <= 0.0 for row in rows):
        raise InvalidIndicatorWindow(
            "Stored indicator inputs do not satisfy the v1 calculation contract.",
            details={"reason": "nonpositive_close"},
        )
    try:
        calculated = calculate_indicators(
            (row.ts for row in rows),
            (row.open for row in rows),
            (row.high for row in rows),
            (row.low for row in rows),
            (row.close for row in rows),
            _CONFIG,
        )
    except (OverflowError, TypeError, ValueError) as exc:
        raise InvalidIndicatorWindow(
            "Stored indicator inputs do not satisfy the v1 calculation contract.",
            details={"reason": "calculation_input_invalid"},
        ) from exc

    observations = [
        IndicatorObservation(
            timestamp=row.ts,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            vwap=row.vwap,
            trade_count=row.trade_count,
            fetched_at=row.fetched_at,
            as_of=row.as_of,
            recorded_at=row.recorded_at,
            simple_return=calculated.simple_return[index],
            log_return=calculated.log_return[index],
            sma=calculated.sma[index],
            ema=calculated.ema[index],
            return_volatility=calculated.return_volatility[index],
            rsi=calculated.rsi[index],
            macd_line=calculated.macd.line[index],
            macd_signal=calculated.macd.signal[index],
            macd_histogram=calculated.macd.histogram[index],
            bollinger_lower=calculated.bollinger.lower[index],
            bollinger_middle=calculated.bollinger.middle[index],
            bollinger_upper=calculated.bollinger.upper[index],
            atr=calculated.atr[index],
        )
        for index, row in enumerate(rows)
    ]
    return IndicatorsResponse(
        symbol=normalized_symbol,
        source=SOURCE,
        timespan=TIMESPAN,
        multiplier=MULTIPLIER,
        adjustment_basis=ADJUSTMENT_BASIS,
        data_semantics="current_snapshot_not_point_in_time",
        endpoint_version=ENDPOINT_VERSION,
        calculation_version=calculated.calculation_version,
        indicator_policy_hash=calculated.policy_hash,
        window_policy_hash=WINDOW_POLICY_HASH,
        parameters=_PARAMETERS,
        window=_window(filters, rows, older_data_excluded=older_data_excluded),
        data_as_of=max(row.as_of for row in rows),
        data_recorded_at=max(row.recorded_at for row in rows),
        count=len(observations),
        observations=observations,
    )


__all__ = [
    "ENDPOINT_VERSION",
    "MAX_OBSERVATIONS",
    "WINDOW_POLICY_HASH",
    "InsufficientIndicatorHistory",
    "InvalidIndicatorWindow",
    "build_indicators_statement",
    "read_indicators",
]
