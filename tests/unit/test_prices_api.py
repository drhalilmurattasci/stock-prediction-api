"""Contract and conditional-HTTP tests for ``GET /v1/prices/{symbol}``."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.core.etag import if_none_match_matches, strong_etag
from app.db.models.bars import Bar
from app.db.session import get_session
from app.main import create_app


class _ScalarRows:
    def __init__(self, rows: Sequence[Bar]) -> None:
        self._rows = rows

    def all(self) -> list[Bar]:
        return list(self._rows)


class _Result:
    def __init__(self, rows: Sequence[Bar]) -> None:
        self._rows = rows

    def scalars(self) -> _ScalarRows:
        return _ScalarRows(self._rows)


class _Session:
    def __init__(self, rows: Sequence[Bar]) -> None:
        self.rows = rows
        self.statements: list[object] = []

    async def execute(self, statement: object) -> _Result:
        self.statements.append(statement)
        return _Result(self.rows)


def _bar(day: int, *, close: float, as_of_hour: int) -> Bar:
    timestamp = datetime(2026, 7, day, tzinfo=UTC)
    return Bar(
        symbol="AAPL",
        timespan="day",
        multiplier=1,
        ts=timestamp,
        source="polygon",
        adjustment_basis="raw",
        open=close - 1.0,
        high=close + 1.0,
        low=close - 2.0,
        close=close,
        volume=1_000.0 + day,
        vwap=close - 0.25,
        trade_count=100 + day,
        fetched_at=timestamp + timedelta(hours=as_of_hour - 1),
        as_of=timestamp + timedelta(hours=as_of_hour),
    )


def _app_with_rows(rows: Sequence[Bar]) -> tuple[FastAPI, _Session]:
    app = create_app(Settings(app_env="test", rate_limit_enabled=False))
    fake_session = _Session(rows)

    async def override_session() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, fake_session)

    app.dependency_overrides[get_session] = override_session
    return app, fake_session


def test_prices_returns_chronological_exact_bytes_and_cache_headers() -> None:
    app, session = _app_with_rows(
        [_bar(3, close=103.0, as_of_hour=18), _bar(2, close=102.0, as_of_hour=17)]
    )

    with TestClient(app) as client:
        response = client.get("/v1/prices/aapl?source=POLYGON")

    assert response.status_code == 200
    assert response.headers["ETag"] == strong_etag(response.content)
    assert response.headers["Cache-Control"] == "private, no-cache"
    assert response.headers["Vary"] == "X-API-Key"
    assert len(session.statements) == 1
    body = response.json()
    assert body["symbol"] == "AAPL"
    assert body["source"] == "polygon"
    assert body["count"] == 2
    assert body["data_as_of"] == "2026-07-03T18:00:00Z"
    assert [bar["timestamp"] for bar in body["bars"]] == [
        "2026-07-02T00:00:00Z",
        "2026-07-03T00:00:00Z",
    ]


@pytest.mark.parametrize("validator", ["exact", "list-weak", "wildcard"])
def test_matching_if_none_match_returns_bodyless_304(validator: str) -> None:
    app, session = _app_with_rows([_bar(3, close=103.0, as_of_hour=18)])
    with TestClient(app) as client:
        first = client.get("/v1/prices/AAPL")
        etag = first.headers["ETag"]
        header = {
            "exact": etag,
            "list-weak": f'"stale", W/{etag}',
            "wildcard": "*",
        }[validator]
        response = client.get("/v1/prices/AAPL", headers={"If-None-Match": header})

    assert response.status_code == 304
    assert response.content == b""
    assert response.headers["ETag"] == etag
    assert len(session.statements) == 2


def test_stale_etag_returns_current_representation() -> None:
    app, _ = _app_with_rows([_bar(3, close=103.0, as_of_hour=18)])
    with TestClient(app) as client:
        response = client.get(
            "/v1/prices/AAPL",
            headers={"If-None-Match": '"not-the-current-representation"'},
        )

    assert response.status_code == 200
    assert response.content


def test_repeated_if_none_match_header_lines_are_combined() -> None:
    app, _ = _app_with_rows([_bar(3, close=103.0, as_of_hour=18)])
    with TestClient(app) as client:
        etag = client.get("/v1/prices/AAPL").headers["ETag"]
        response = client.get(
            "/v1/prices/AAPL",
            headers=[("If-None-Match", '"stale"'), ("If-None-Match", etag)],
        )

    assert response.status_code == 304


@pytest.mark.parametrize(
    "malformed",
    [
        # Every case embeds the CURRENT tag body, so a parser lenient about the
        # malformation would match and flip the assertion — each case is
        # load-bearing for the strict grammar, not a "different tag" tautology.
        '"lowercase-weak-prefix',  # unterminated quote
        '"lowercase-weak-prefix", *',  # wildcard must appear alone, not in a list
        'w/"lowercase-weak-prefix"',  # weak prefix must be uppercase W/
        'W/ "lowercase-weak-prefix"',  # no whitespace allowed after W/
        '"lowercase-weak-prefix",',  # trailing comma
    ],
)
def test_malformed_if_none_match_never_produces_false_match(malformed: str) -> None:
    assert if_none_match_matches(malformed, '"lowercase-weak-prefix"') is False


def test_entity_tag_with_an_internal_comma_is_parsed_as_one_tag() -> None:
    assert if_none_match_matches('"opaque,tag", "current"', '"current"') is True


def test_etag_changes_when_price_data_changes() -> None:
    app_one, _ = _app_with_rows([_bar(3, close=103.0, as_of_hour=18)])
    app_two, _ = _app_with_rows([_bar(3, close=104.0, as_of_hour=18)])

    with TestClient(app_one) as first_client:
        first_etag = first_client.get("/v1/prices/AAPL").headers["ETag"]
    with TestClient(app_two) as second_client:
        second_etag = second_client.get("/v1/prices/AAPL").headers["ETag"]

    assert first_etag != second_etag


def test_empty_series_has_a_complete_stable_shape() -> None:
    app, _ = _app_with_rows([])
    with TestClient(app) as client:
        response = client.get("/v1/prices/MISSING")

    assert response.status_code == 200
    assert response.json() == {
        "symbol": "MISSING",
        "source": "polygon",
        "timespan": "day",
        "multiplier": 1,
        "adjustment_basis": "raw",
        "data_as_of": None,
        "count": 0,
        "page": {"limit": 100, "has_more": False, "next_end": None},
        "bars": [],
    }


@pytest.mark.parametrize(
    "path",
    [
        "/v1/prices/bad%20symbol",
        "/v1/prices/AAPL?start=2026-07-01T00:00:00",
        ("/v1/prices/AAPL?start=2026-07-02T00:00:00Z&end=2026-07-02T00:00:00Z"),
        "/v1/prices/AAPL?adjustment_basis=secretly_adjusted",
        "/v1/prices/AAPL?source=bad%2Fsource",
        "/v1/prices/AAPL?as_of=2026-07-01T00:00:00Z",
        "/v1/prices/AAPL?limit=0",
        "/v1/prices/AAPL?limit=1001",
    ],
)
def test_invalid_symbol_or_filters_return_validation_envelope(path: str) -> None:
    app, _ = _app_with_rows([])
    with TestClient(app) as client:
        response = client.get(path)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


def test_openapi_documents_prices_response_and_not_modified() -> None:
    app, _ = _app_with_rows([])
    operation: dict[str, Any] = app.openapi()["paths"]["/v1/prices/{symbol}"]["get"]

    schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
    assert schema["$ref"].endswith("/PricesResponse")
    assert {"401", "422", "304"} <= operation["responses"].keys()
    for status_code in ("200", "304"):
        assert operation["responses"][status_code]["headers"].keys() == {
            "ETag",
            "Cache-Control",
            "Vary",
        }
    assert operation["responses"]["401"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "/ErrorResponse"
    )
    assert operation["security"] == [{"ApiKeyAuth": []}]
    security_scheme = app.openapi()["components"]["securitySchemes"]["ApiKeyAuth"]
    assert security_scheme["type"] == "apiKey"
    assert security_scheme["in"] == "header"
    assert security_scheme["name"] == "X-API-Key"
    query_parameters = {
        parameter["name"] for parameter in operation["parameters"] if parameter["in"] == "query"
    }
    assert query_parameters == {
        "start",
        "end",
        "timespan",
        "multiplier",
        "source",
        "adjustment_basis",
        "limit",
    }
    header_parameters = {
        parameter["name"] for parameter in operation["parameters"] if parameter["in"] == "header"
    }
    assert "If-None-Match" in header_parameters
