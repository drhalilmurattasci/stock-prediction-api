"""Celery lane for final, raw XNYS regular-session closes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from typing import Any

import exchange_calendars as xcals
import pandas as pd
import structlog
from celery import shared_task
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.db.models.bars import Bar
from app.db.session import build_engine, build_sessionmaker
from app.services.market_calendar import latest_completed_xnys_session
from data_sources.base import MarketDataProvider, OHLCVBar
from data_sources.guards import AsyncPacingCostRateGuard
from data_sources.polygon_open_close import PolygonOpenCloseProvider
from ingestion.automation import require_automation_enabled
from ingestion.locks import exclusive_vendor_operation
from ingestion.tasks.ingest_prices import (
    _advisory_xact_lock,
    _effective_start_date,
    _is_retryable_symbol_error,
    _normalize_symbols,
    _upsert_symbol_bars,
)
from ingestion.upsert import BarUpsertPlan, upsert_bars

log = structlog.get_logger(__name__)

# About 289 XNYS sessions in an ordinary 420-calendar-day span: comfortably
# above the snapshot policy's 258-observation minimum while staying well inside
# the Basic plan's two-year history.
DEFAULT_BOOTSTRAP_DAYS = 420
ProviderFactory = Callable[[Settings], MarketDataProvider]
UpsertFn = Callable[[AsyncSession, Sequence[OHLCVBar]], Awaitable[BarUpsertPlan]]
OperationLockFn = Callable[[Settings], AbstractAsyncContextManager[None]]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _earliest_missing_xnys_session(
    existing_dates: set[date],
    start: date,
    end: date,
) -> date | None:
    """Return the earliest expected session absent from a persisted window."""

    calendar = xcals.get_calendar("XNYS")
    sessions = calendar.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
    return next((label.date() for label in sessions if label.date() not in existing_dates), None)


async def _resolve_fetch_start(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    symbol: str,
    source: str,
    timespan: str,
    adjustment_basis: str,
    requested_start: date,
    end: date,
    use_watermark: bool,
) -> date | None:
    """Resolve a watermark while repairing any internal XNYS-session gap."""

    async with sessionmaker() as session, session.begin():
        await _advisory_xact_lock(session, symbol, source, timespan)
        if not use_watermark:
            return requested_start
        lower = datetime.combine(requested_start, datetime.min.time(), tzinfo=UTC)
        upper = datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
        persisted = await session.execute(
            select(Bar.ts).where(
                Bar.symbol == symbol,
                Bar.source == source,
                Bar.timespan == timespan,
                Bar.adjustment_basis == adjustment_basis,
                Bar.ts >= lower,
                Bar.ts < upper,
            )
        )
        existing_dates = {value.date() for value in persisted.scalars()}
        missing = _earliest_missing_xnys_session(existing_dates, requested_start, end)
        if missing is not None:
            return missing
        return await _effective_start_date(
            session,
            symbol=symbol,
            source=source,
            timespan=timespan,
            adjustment_basis=adjustment_basis,
            requested_start=requested_start,
            end=end,
            use_watermark=True,
        )


@shared_task(
    name="ingestion.ingest_forecast_closes",
    bind=True,
    max_retries=3,
    acks_late=True,
    reject_on_worker_lost=True,
)
def ingest_forecast_closes(
    self: Any,
    symbols: list[str] | None = None,
    *,
    start: str | None = None,
    end: str | None = None,
    use_watermark: bool = True,
) -> dict[str, Any]:
    """Celery entrypoint for the separate forecast-input source lane."""

    settings = get_settings()
    require_automation_enabled(settings, require_polygon_budget=True)
    result = asyncio.run(
        ingest_forecast_closes_async(
            symbols=symbols,
            start=date.fromisoformat(start) if start else None,
            end=date.fromisoformat(end) if end else None,
            use_watermark=use_watermark,
            settings=settings,
        )
    )
    if result["retryable_failures"]:
        raise self.retry(exc=RuntimeError("regular-session close ingestion is incomplete"))
    if result["failures"]:
        raise RuntimeError("regular-session close ingestion failed deterministic checks")
    return result


async def ingest_forecast_closes_async(
    *,
    symbols: Sequence[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    use_watermark: bool = True,
    settings: Settings | None = None,
    provider_factory: ProviderFactory | None = None,
    sessionmaker: async_sessionmaker[AsyncSession] | None = None,
    engine: AsyncEngine | None = None,
    upsert_fn: UpsertFn = upsert_bars,
    clock: Callable[[], datetime] = _utcnow,
    include_error_details: bool = True,
    operation_lock_fn: OperationLockFn = exclusive_vendor_operation,
) -> dict[str, Any]:
    """Fetch and persist official open/close responses for completed sessions."""

    settings = settings or get_settings()
    end_date = end or latest_completed_xnys_session(clock())
    requested_start = start or (end_date - timedelta(days=DEFAULT_BOOTSTRAP_DAYS))
    if requested_start > end_date:
        raise ValueError("start must be on or before end")

    symbol_list = _normalize_symbols(symbols)
    owns_engine = engine is None and sessionmaker is None
    if sessionmaker is None:
        engine = engine or build_engine(settings)
        sessionmaker = build_sessionmaker(engine)

    provider_name = "polygon_open_close"
    started_at = clock().astimezone(UTC)
    per_symbol: list[dict[str, Any]] = []
    total_rows = 0
    total_revisions = 0
    successes = 0
    failures = 0
    retryable_failures = 0

    try:
        async with operation_lock_fn(settings):
            provider = _build_provider(settings, provider_factory)
            provider_name = provider.name
            async with provider:
                for symbol in symbol_list:
                    try:
                        fetch_start = await _resolve_fetch_start(
                            sessionmaker,
                            symbol=symbol,
                            source=provider.name,
                            timespan="day",
                            adjustment_basis="raw",
                            requested_start=requested_start,
                            end=end_date,
                            use_watermark=use_watermark and start is None,
                        )
                        if fetch_start is None:
                            bars: list[OHLCVBar] = []
                            plan = BarUpsertPlan(rows=[], revisions=[])
                        else:
                            bars = await provider.get_daily_bars(
                                symbol,
                                fetch_start,
                                end_date,
                                adjusted=False,
                            )
                            plan = await _upsert_symbol_bars(
                                sessionmaker,
                                symbol=symbol,
                                source=provider.name,
                                timespan="day",
                                bars=bars,
                                upsert_fn=upsert_fn,
                            )
                    except Exception as exc:  # noqa: BLE001 - isolate per-symbol failures.
                        failures += 1
                        retryable = _is_retryable_symbol_error(exc)
                        retryable_failures += int(retryable)
                        per_symbol.append(
                            {
                                "symbol": symbol,
                                "status": "failed",
                                "error_type": type(exc).__name__,
                                "error": (
                                    str(exc) if include_error_details else "details suppressed"
                                ),
                                "retryable": retryable,
                            }
                        )
                        log.warning(
                            "ingest_forecast_closes.symbol_failed",
                            symbol=symbol,
                            error_type=type(exc).__name__,
                            retryable=retryable,
                            exc_info=include_error_details,
                        )
                        continue

                    row_count = len(plan.rows)
                    revision_count = len(plan.revisions)
                    total_rows += row_count
                    total_revisions += revision_count
                    successes += 1
                    per_symbol.append(
                        {
                            "symbol": symbol,
                            "status": "skipped" if fetch_start is None else "ok",
                            "fetch_start": fetch_start.isoformat() if fetch_start else None,
                            "fetch_end": end_date.isoformat(),
                            "bars": len(bars),
                            "rows_upserted": row_count,
                            "revisions": revision_count,
                        }
                    )
    finally:
        if owns_engine and engine is not None:
            await engine.dispose()

    status = "ok"
    if failures and successes:
        status = "degraded"
    elif failures and not successes:
        status = "failed"

    result = {
        "status": status,
        "provider": provider_name,
        "symbols": symbol_list,
        "requested_start": requested_start.isoformat(),
        "end": end_date.isoformat(),
        "watermark_enabled": use_watermark and start is None,
        "rows_upserted": total_rows,
        "revisions": total_revisions,
        "failures": failures,
        "retryable_failures": retryable_failures,
        "per_symbol": per_symbol,
        "started_at": started_at.isoformat(),
        "finished_at": clock().astimezone(UTC).isoformat(),
    }
    log.info("ingest_forecast_closes.complete", **result)
    return result


def _build_provider(
    settings: Settings,
    provider_factory: ProviderFactory | None,
) -> MarketDataProvider:
    if provider_factory is not None:
        return provider_factory(settings)
    if not settings.polygon_api_key:
        raise ValueError("POLYGON_API_KEY is required for regular-session close ingestion")
    return PolygonOpenCloseProvider(
        settings.polygon_api_key,
        guard=_polygon_open_close_guard(
            settings.polygon_max_calls_per_window,
            settings.polygon_rate_window_seconds,
            settings.polygon_total_call_budget,
        ),
    )


@lru_cache(maxsize=16)
def _polygon_open_close_guard(
    max_calls_per_window: int,
    window_seconds: float,
    total_budget: int,
) -> AsyncPacingCostRateGuard:
    """Keep pacing and total spend cumulative for one worker process."""

    return AsyncPacingCostRateGuard(
        max_calls_per_window=max_calls_per_window,
        window_seconds=window_seconds,
        total_budget=total_budget or None,
    )
