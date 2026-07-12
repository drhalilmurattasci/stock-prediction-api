"""Schema and query-service tests for current-snapshot OHLCV reads."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.bars import Bar
from app.schemas.prices import PriceBar, PriceFilters, PricePage, PricesResponse
from app.services.prices import build_prices_statement, read_prices

TS = datetime(2026, 7, 1, tzinfo=UTC)


class FakeScalarResult:
    def __init__(self, rows: list[Bar]) -> None:
        self._rows = rows

    def all(self) -> list[Bar]:
        return self._rows


class FakeResult:
    def __init__(self, rows: list[Bar]) -> None:
        self._rows = rows

    def scalars(self) -> FakeScalarResult:
        return FakeScalarResult(self._rows)


class FakeSession:
    def __init__(self, rows: list[Bar]) -> None:
        self._rows = rows
        self.statements: list[Any] = []

    async def execute(self, statement: Any) -> FakeResult:
        self.statements.append(statement)
        return FakeResult(self._rows)


def _row(
    day: int,
    *,
    close: float | None = None,
    as_of_hour: int = 12,
    offset: timezone = UTC,
    as_of: datetime | None = None,
) -> Bar:
    value = close if close is not None else float(day + 100)
    timestamp = datetime(2026, 7, day, tzinfo=offset)
    observed = as_of if as_of is not None else datetime(2026, 7, day, as_of_hour, tzinfo=offset)
    return Bar(
        symbol="AAPL",
        timespan="day",
        multiplier=1,
        ts=timestamp,
        source="polygon",
        adjustment_basis="raw",
        open=value - 1.0,
        high=value + 1.0,
        low=value - 2.0,
        close=value,
        volume=1000.0 + day,
        vwap=value - 0.5,
        trade_count=100 + day,
        fetched_at=observed,
        as_of=observed,
    )


def _price_bar(timestamp: datetime = TS) -> PriceBar:
    return PriceBar(
        timestamp=timestamp,
        open=99.0,
        high=102.0,
        low=98.0,
        close=101.0,
        volume=1000.0,
        vwap=100.5,
        trade_count=42,
        fetched_at=TS + timedelta(hours=1),
        as_of=TS + timedelta(hours=1),
    )


def _response(**overrides: Any) -> PricesResponse:
    bar = _price_bar()
    fields: dict[str, Any] = {
        "symbol": "AAPL",
        "source": "polygon",
        "timespan": "day",
        "multiplier": 1,
        "adjustment_basis": "raw",
        "data_as_of": bar.as_of,
        "count": 1,
        "page": PricePage(limit=100, has_more=False),
        "bars": [bar],
    }
    fields.update(overrides)
    return PricesResponse(**fields)


def test_price_filters_have_safe_current_series_defaults():
    filters = PriceFilters()

    assert filters.source == "polygon"
    assert filters.timespan == "day"
    assert filters.multiplier == 1
    assert filters.adjustment_basis == "raw"
    assert filters.start is None
    assert filters.end is None
    assert filters.limit == 100
    assert "as_of" not in PriceFilters.model_fields


def test_price_filters_normalize_source_and_bounds_to_utc():
    plus_three = timezone(timedelta(hours=3))
    filters = PriceFilters(
        source="PoLyGoN",
        start=datetime(2026, 7, 1, 3, tzinfo=plus_three),
        end=datetime(2026, 7, 2, 3, tzinfo=plus_three),
    )

    assert filters.source == "polygon"
    assert filters.start == datetime(2026, 7, 1, tzinfo=UTC)
    assert filters.end == datetime(2026, 7, 2, tzinfo=UTC)
    assert filters.start is not None and filters.start.utcoffset() == timedelta(0)


@pytest.mark.parametrize("field", ["start", "end"])
def test_price_filters_reject_naive_bounds(field: str):
    with pytest.raises(ValidationError, match="timezone"):
        PriceFilters(**{field: datetime(2026, 7, 1)})


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (datetime(2026, 7, 2, tzinfo=UTC), datetime(2026, 7, 1, tzinfo=UTC)),
        (datetime(2026, 7, 1, tzinfo=UTC), datetime(2026, 7, 1, tzinfo=UTC)),
    ],
)
def test_price_filters_require_strictly_increasing_range(start: datetime, end: datetime):
    with pytest.raises(ValidationError, match="start must be earlier than end"):
        PriceFilters(start=start, end=end)


@pytest.mark.parametrize("limit", [0, 1001])
def test_price_filters_enforce_limit_bounds(limit: int):
    with pytest.raises(ValidationError):
        PriceFilters(limit=limit)


@pytest.mark.parametrize("multiplier", [0, 10_001])
def test_price_filters_enforce_multiplier_bounds(multiplier: int):
    with pytest.raises(ValidationError):
        PriceFilters(multiplier=multiplier)


def test_price_filters_reject_unknown_or_computed_series_values():
    with pytest.raises(ValidationError):
        PriceFilters(timespan="quarter")
    with pytest.raises(ValidationError):
        PriceFilters(adjustment_basis="computed")
    with pytest.raises(ValidationError):
        PriceFilters(source="polygon vendor")


def test_price_schemas_are_frozen_and_forbid_extra_fields():
    models: list[Any] = [
        PriceFilters(),
        _price_bar(),
        PricePage(limit=10, has_more=False),
        _response(),
    ]
    for model in models:
        with pytest.raises(ValidationError, match="frozen"):
            model.__setattr__(next(iter(type(model).model_fields)), None)
    with pytest.raises(ValidationError, match="Extra inputs"):
        PriceFilters(cursor=TS)  # type: ignore[call-arg]


def test_price_bar_normalizes_provenance_times_and_validates_ohlc():
    plus_three = timezone(timedelta(hours=3))
    timestamp = datetime(2026, 7, 1, 3, tzinfo=plus_three)
    bar = _price_bar(timestamp)

    assert bar.timestamp == datetime(2026, 7, 1, tzinfo=UTC)
    assert bar.timestamp.utcoffset() == timedelta(0)
    invalid = _price_bar().model_dump()
    invalid["high"] = 97.0
    with pytest.raises(ValidationError, match="high must be greater"):
        PriceBar.model_validate(invalid)


def test_price_bar_rejects_naive_provenance_and_nonfinite_values():
    fields = _price_bar().model_dump()
    fields["as_of"] = datetime(2026, 7, 1)
    with pytest.raises(ValidationError, match="timezone"):
        PriceBar.model_validate(fields)

    fields = _price_bar().model_dump()
    fields["volume"] = float("nan")
    with pytest.raises(ValidationError):
        PriceBar.model_validate(fields)


def test_page_requires_cursor_exactly_when_more_data_exists():
    with pytest.raises(ValidationError, match="if and only if"):
        PricePage(limit=10, has_more=True)
    with pytest.raises(ValidationError, match="if and only if"):
        PricePage(limit=10, has_more=False, next_end=TS)


def test_response_normalizes_identity_and_enforces_count():
    response = _response(symbol="aapl", source="PoLyGoN")

    assert response.symbol == "AAPL"
    assert response.source == "polygon"
    with pytest.raises(ValidationError, match="count must equal"):
        _response(count=2)


def test_statement_filters_exact_series_uses_half_open_bounds_and_overfetches():
    filters = PriceFilters(
        source="PoLyGoN",
        timespan="hour",
        multiplier=2,
        adjustment_basis="split_adjusted",
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 4, tzinfo=UTC),
        limit=7,
    )
    statement = build_prices_statement(" aapl ", filters)
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "bars.symbol = 'AAPL'" in sql
    assert "bars.source = 'polygon'" in sql
    assert "bars.timespan = 'hour'" in sql
    assert "bars.multiplier = 2" in sql
    assert "bars.adjustment_basis = 'split_adjusted'" in sql
    assert "bars.ts >= '2026-07-01 00:00:00+00:00'" in sql
    assert "bars.ts < '2026-07-04 00:00:00+00:00'" in sql
    assert "bars.ts <=" not in sql
    assert "ORDER BY bars.ts DESC" in sql
    assert "LIMIT 8" in sql
    assert "bars.as_of <=" not in sql


def test_statement_omits_absent_time_bounds():
    sql = str(
        build_prices_statement("AAPL", PriceFilters()).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    where_sql = sql.split("WHERE", maxsplit=1)[1].split("ORDER BY", maxsplit=1)[0]
    assert "bars.ts" not in where_sql
    assert "LIMIT 101" in sql


@pytest.mark.asyncio
async def test_read_prices_returns_newest_page_in_chronological_order():
    # The database query is DESC; the fourth row is the overfetch sentinel.
    rows = [
        _row(4, as_of_hour=10),
        _row(3, as_of_hour=15),
        _row(2, as_of_hour=11),
        _row(1, as_of_hour=12),
    ]
    session = FakeSession(rows)

    response = await read_prices(
        cast(AsyncSession, session),
        " aapl ",
        PriceFilters(source="POLYGON", limit=3),
    )

    assert response.symbol == "AAPL"
    assert response.count == 3
    assert [bar.timestamp.day for bar in response.bars] == [2, 3, 4]
    assert [bar.close for bar in response.bars] == [102.0, 103.0, 104.0]
    assert response.page == PricePage(
        limit=3,
        has_more=True,
        next_end=datetime(2026, 7, 2, tzinfo=UTC),
    )
    assert response.data_as_of == datetime(2026, 7, 4, 10, tzinfo=UTC)

    sql = str(
        session.statements[0].compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "ORDER BY bars.ts DESC" in sql
    assert "LIMIT 4" in sql


@pytest.mark.asyncio
async def test_read_prices_normalizes_row_times_to_utc():
    plus_three = timezone(timedelta(hours=3))
    session = FakeSession([_row(1, offset=plus_three)])

    response = await read_prices(cast(AsyncSession, session), "AAPL", PriceFilters())

    assert response.bars[0].timestamp == datetime(2026, 6, 30, 21, tzinfo=UTC)
    assert response.bars[0].as_of == datetime(2026, 7, 1, 9, tzinfo=UTC)
    assert response.data_as_of == response.bars[0].as_of


@pytest.mark.asyncio
async def test_read_prices_data_as_of_is_page_max_not_newest_bar():
    # A vendor restatement gives an OLDER-ts bar a NEWER as_of than the
    # newest-ts bar. data_as_of is documented as the page max, so it must come
    # from the restated day-2 bar — a bars[-1].as_of shortcut would silently
    # under-report freshness and mask the correction.
    restated_as_of = datetime(2026, 7, 20, tzinfo=UTC)
    rows = [
        _row(4, as_of_hour=10),
        _row(3, as_of_hour=15),
        _row(2, as_of=restated_as_of),
    ]
    session = FakeSession(rows)

    response = await read_prices(cast(AsyncSession, session), "AAPL", PriceFilters(limit=3))

    assert [bar.timestamp.day for bar in response.bars] == [2, 3, 4]
    assert response.data_as_of == restated_as_of
    assert response.data_as_of != response.bars[-1].as_of


@pytest.mark.asyncio
async def test_read_prices_exact_limit_has_no_next_page():
    session = FakeSession([_row(2), _row(1)])

    response = await read_prices(
        cast(AsyncSession, session),
        "AAPL",
        PriceFilters(limit=2),
    )

    assert response.count == 2
    assert response.page.has_more is False
    assert response.page.next_end is None


@pytest.mark.asyncio
async def test_read_prices_returns_complete_empty_200_shape():
    session = FakeSession([])

    response = await read_prices(
        cast(AsyncSession, session),
        "msft",
        PriceFilters(timespan="week", adjustment_basis="raw", limit=25),
    )

    assert response == PricesResponse(
        symbol="MSFT",
        source="polygon",
        timespan="week",
        multiplier=1,
        adjustment_basis="raw",
        data_as_of=None,
        count=0,
        page=PricePage(limit=25, has_more=False, next_end=None),
        bars=[],
    )
