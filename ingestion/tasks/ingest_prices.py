"""Celery task: ingest OHLCV/price bars into TimescaleDB."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from typing import Any

import structlog
from celery import shared_task
from sqlalchemy import Select, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.db.models.bars import Bar
from app.db.session import build_engine, build_sessionmaker
from data_sources.base import MarketDataProvider, OHLCVBar, ProviderHTTPError, SymbolNotFoundError
from data_sources.guards import InMemoryCostRateGuard
from data_sources.polygon import PolygonProvider
from ingestion.upsert import BarUpsertPlan, upsert_bars

log = structlog.get_logger(__name__)

DEFAULT_SYMBOLS: tuple[str, ...] = ("AAPL", "MSFT", "NVDA", "SPY", "QQQ")
DEFAULT_LOOKBACK_DAYS = 7
ProviderFactory = Callable[[Settings], MarketDataProvider]
UpsertFn = Callable[[AsyncSession, Sequence[OHLCVBar]], Awaitable[BarUpsertPlan]]


@shared_task(
    name="ingestion.ingest_prices",
    bind=True,
    max_retries=3,
    acks_late=True,
    reject_on_worker_lost=True,
)
def ingest_prices(
    self,
    symbols: list[str] | None = None,
    *,
    start: str | None = None,
    end: str | None = None,
    adjusted: bool = False,
    use_watermark: bool = True,
) -> dict[str, Any]:
    """Celery sync entrypoint that bridges once into async ingestion code."""
    result = asyncio.run(
        ingest_prices_async(
            symbols=symbols,
            start=_parse_date(start) if start else None,
            end=_parse_date(end) if end else None,
            adjusted=adjusted,
            use_watermark=use_watermark,
        )
    )
    if result["status"] == "failed" and result["retryable_failures"]:
        raise self.retry(exc=RuntimeError("all price-ingestion symbols failed"))
    return result


async def ingest_prices_async(
    *,
    symbols: Sequence[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    adjusted: bool = False,
    use_watermark: bool = True,
    settings: Settings | None = None,
    provider_factory: ProviderFactory | None = None,
    sessionmaker: async_sessionmaker[AsyncSession] | None = None,
    engine: AsyncEngine | None = None,
    upsert_fn: UpsertFn = upsert_bars,
) -> dict[str, Any]:
    """Async implementation used by Celery and unit tests."""
    settings = settings or get_settings()
    end_date = end or datetime.now(UTC).date()
    requested_start = start or (end_date - timedelta(days=DEFAULT_LOOKBACK_DAYS))
    if requested_start > end_date:
        raise ValueError("start must be on or before end")

    symbol_list = _normalize_symbols(symbols)
    owns_engine = engine is None and sessionmaker is None
    if sessionmaker is None:
        engine = engine or build_engine(settings)
        sessionmaker = build_sessionmaker(engine)

    provider = _build_provider(settings, provider_factory)
    started_at = datetime.now(UTC)
    per_symbol: list[dict[str, Any]] = []
    total_rows = 0
    total_revisions = 0
    successes = 0
    failures = 0
    retryable_failures = 0

    try:
        async with provider:
            for symbol in symbol_list:
                try:
                    fetch_start = await _resolve_fetch_start(
                        sessionmaker,
                        symbol=symbol,
                        source=provider.name,
                        timespan="day",
                        adjustment_basis=_adjustment_basis(adjusted),
                        requested_start=requested_start,
                        end=end_date,
                        use_watermark=use_watermark and start is None,
                    )
                    if fetch_start is None:
                        bars = []
                        plan = BarUpsertPlan(rows=[], revisions=[])
                    else:
                        bars = await provider.get_daily_bars(
                            symbol,
                            fetch_start,
                            end_date,
                            adjusted=adjusted,
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
                            "error": str(exc),
                            "retryable": retryable,
                        }
                    )
                    log.warning(
                        "ingest_prices.symbol_failed",
                        symbol=symbol,
                        error_type=type(exc).__name__,
                        retryable=retryable,
                        exc_info=True,
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
                log.info(
                    "ingest_prices.symbol_complete",
                    symbol=symbol,
                    bars=len(bars),
                    fetch_start=fetch_start.isoformat() if fetch_start else None,
                    rows_upserted=row_count,
                    revisions=revision_count,
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
        "provider": provider.name,
        "symbols": symbol_list,
        "requested_start": requested_start.isoformat(),
        "end": end_date.isoformat(),
        "adjusted": adjusted,
        "watermark_enabled": use_watermark and start is None,
        "rows_upserted": total_rows,
        "revisions": total_revisions,
        "failures": failures,
        "retryable_failures": retryable_failures,
        "per_symbol": per_symbol,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
    }
    log.info("ingest_prices.complete", **result)
    return result


def _build_provider(
    settings: Settings,
    provider_factory: ProviderFactory | None,
) -> MarketDataProvider:
    if provider_factory is not None:
        return provider_factory(settings)
    if not settings.polygon_api_key:
        raise ValueError("POLYGON_API_KEY is required for price ingestion")
    return PolygonProvider(
        settings.polygon_api_key,
        guard=_polygon_guard(
            settings.polygon_max_calls_per_window,
            settings.polygon_rate_window_seconds,
            settings.polygon_total_call_budget,
        ),
    )


@lru_cache(maxsize=16)
def _polygon_guard(
    max_calls_per_window: int,
    window_seconds: float,
    total_budget: int,
) -> InMemoryCostRateGuard:
    """Share the configured temporary guard across tasks in one worker process."""

    return InMemoryCostRateGuard(
        max_calls_per_window=max_calls_per_window,
        window_seconds=window_seconds,
        total_budget=total_budget or None,
    )


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    source = symbols or DEFAULT_SYMBOLS
    normalized = sorted({symbol.strip().upper() for symbol in source if symbol.strip()})
    if not normalized:
        raise ValueError("at least one symbol is required")
    return normalized


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


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
    async with sessionmaker() as session, session.begin():
        await _advisory_xact_lock(session, symbol, source, timespan)
        return await _effective_start_date(
            session,
            symbol=symbol,
            source=source,
            timespan=timespan,
            adjustment_basis=adjustment_basis,
            requested_start=requested_start,
            end=end,
            use_watermark=use_watermark,
        )


async def _effective_start_date(
    session: AsyncSession,
    *,
    symbol: str,
    source: str,
    timespan: str,
    adjustment_basis: str,
    requested_start: date,
    end: date,
    use_watermark: bool,
) -> date | None:
    if not use_watermark:
        return requested_start
    last_ts = await _latest_bar_ts(
        session,
        symbol=symbol,
        source=source,
        timespan=timespan,
        adjustment_basis=adjustment_basis,
    )
    if last_ts is None:
        return requested_start
    next_start = last_ts.date() + timedelta(days=1)
    if next_start > end:
        return requested_start
    return min(requested_start, next_start)


async def _latest_bar_ts(
    session: AsyncSession,
    *,
    symbol: str,
    source: str,
    timespan: str,
    adjustment_basis: str,
) -> datetime | None:
    result = await session.execute(
        _latest_bar_ts_statement(symbol, source, timespan, adjustment_basis)
    )
    return result.scalar_one_or_none()


def _latest_bar_ts_statement(
    symbol: str,
    source: str,
    timespan: str,
    adjustment_basis: str,
) -> Select[tuple[datetime]]:
    return select(func.max(Bar.ts)).where(
        Bar.symbol == symbol,
        Bar.source == source,
        Bar.timespan == timespan,
        Bar.adjustment_basis == adjustment_basis,
    )


async def _upsert_symbol_bars(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    symbol: str,
    source: str,
    timespan: str,
    bars: Sequence[OHLCVBar],
    upsert_fn: UpsertFn,
) -> BarUpsertPlan:
    async with sessionmaker() as session, session.begin():
        await _advisory_xact_lock(session, symbol, source, timespan)
        return await upsert_fn(session, bars)


async def _advisory_xact_lock(
    session: AsyncSession,
    symbol: str,
    source: str,
    timespan: str,
) -> None:
    """Serialize writers for one logical symbol/source/timespan lane."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_id)"),
        {"lock_id": _lock_id(symbol, source, timespan)},
    )


def _lock_id(symbol: str, source: str, timespan: str) -> int:
    digest = hashlib.blake2b(
        f"{source}:{timespan}:{symbol}".encode(),
        digest_size=8,
    ).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


def _adjustment_basis(adjusted: bool) -> str:
    return "split_dividend_adjusted" if adjusted else "raw"


def _is_retryable_symbol_error(exc: Exception) -> bool:
    if isinstance(exc, SymbolNotFoundError):
        return False
    if isinstance(exc, ProviderHTTPError) and exc.status_code is not None:
        return exc.status_code == 429 or exc.status_code >= 500
    return True
