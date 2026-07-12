"""Current-snapshot reads for one exact stored OHLCV series."""

from __future__ import annotations

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.bars import Bar
from app.schemas.prices import PriceBar, PriceFilters, PricePage, PricesResponse


def build_prices_statement(symbol: str, filters: PriceFilters) -> Select[tuple[Bar]]:
    """Build the bounded newest-first query used for keyset pagination."""

    normalized_symbol = symbol.strip().upper()
    statement = select(Bar).where(
        Bar.symbol == normalized_symbol,
        Bar.source == filters.source,
        Bar.timespan == filters.timespan,
        Bar.multiplier == filters.multiplier,
        Bar.adjustment_basis == filters.adjustment_basis,
    )
    if filters.start is not None:
        statement = statement.where(Bar.ts >= filters.start)
    if filters.end is not None:
        statement = statement.where(Bar.ts < filters.end)
    return statement.order_by(Bar.ts.desc()).limit(filters.limit + 1)


async def read_prices(
    session: AsyncSession,
    symbol: str,
    filters: PriceFilters,
) -> PricesResponse:
    """Read the newest page and return its bars in chronological order."""

    normalized_symbol = symbol.strip().upper()
    result = await session.execute(build_prices_statement(normalized_symbol, filters))
    rows = list(result.scalars().all())

    has_more = len(rows) > filters.limit
    retained = rows[: filters.limit]
    next_end = retained[-1].ts if has_more else None
    chronological = reversed(retained)
    bars = [
        PriceBar(
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
        )
        for row in chronological
    ]

    return PricesResponse(
        symbol=normalized_symbol,
        source=filters.source,
        timespan=filters.timespan,
        multiplier=filters.multiplier,
        adjustment_basis=filters.adjustment_basis,
        data_as_of=max((bar.as_of for bar in bars), default=None),
        data_recorded_at=max((bar.recorded_at for bar in bars), default=None),
        count=len(bars),
        page=PricePage(limit=filters.limit, has_more=has_more, next_end=next_end),
        bars=bars,
    )
