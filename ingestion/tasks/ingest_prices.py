"""Celery task: ingest OHLCV/price bars into TimescaleDB."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

import structlog
from celery import shared_task
from sqlalchemy import Select, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.db.models.bars import Bar
from app.db.session import build_engine, build_sessionmaker
from data_sources.base import MarketDataProvider, OHLCVBar
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
    del self  # Celery bind kept for retry APIs once provider errors are classified.
    return asyncio.run(
        ingest_prices_async(
            symbols=symbols,
            start=_parse_date(start) if start else None,
            end=_parse_date(end) if end else None,
            adjusted=adjusted,
            use_watermark=use_watermark,
        )
    )


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
    status = "ok"

    try:
        async with provider:
            for symbol in symbol_list:
                async with sessionmaker() as session, session.begin():
                    await _advisory_xact_lock(session, symbol, provider.name, "day")
                    fetch_start = await _effective_start_date(
                        session,
                        symbol=symbol,
                        source=provider.name,
                        timespan="day",
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
                        plan = await upsert_fn(session, bars)
                row_count = len(plan.rows)
                revision_count = len(plan.revisions)
                total_rows += row_count
                total_revisions += revision_count
                per_symbol.append(
                    {
                        "symbol": symbol,
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
    except Exception:
        status = "failed"
        log.exception("ingest_prices.failed")
        raise
    finally:
        if owns_engine and engine is not None:
            await engine.dispose()

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
    return PolygonProvider(settings.polygon_api_key)


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    source = symbols or DEFAULT_SYMBOLS
    normalized = sorted({symbol.strip().upper() for symbol in source if symbol.strip()})
    if not normalized:
        raise ValueError("at least one symbol is required")
    return normalized


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


async def _effective_start_date(
    session: AsyncSession,
    *,
    symbol: str,
    source: str,
    timespan: str,
    requested_start: date,
    end: date,
    use_watermark: bool,
) -> date | None:
    if not use_watermark:
        return requested_start
    last_ts = await _latest_bar_ts(session, symbol=symbol, source=source, timespan=timespan)
    if last_ts is None:
        return requested_start
    next_start = last_ts.date() + timedelta(days=1)
    if next_start > end:
        return None
    return max(requested_start, next_start)


async def _latest_bar_ts(
    session: AsyncSession,
    *,
    symbol: str,
    source: str,
    timespan: str,
) -> datetime | None:
    result = await session.execute(_latest_bar_ts_statement(symbol, source, timespan))
    return result.scalar_one_or_none()


def _latest_bar_ts_statement(
    symbol: str,
    source: str,
    timespan: str,
) -> Select[tuple[datetime]]:
    return select(func.max(Bar.ts)).where(
        Bar.symbol == symbol,
        Bar.source == source,
        Bar.timespan == timespan,
    )


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
