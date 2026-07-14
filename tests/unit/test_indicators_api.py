"""Public contract tests for ``GET /v1/indicators/{symbol}``."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta
from typing import Any, cast

import exchange_calendars as xcals
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.core.etag import strong_etag
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
        self.transaction_active = False
        self.rollback_calls = 0

    def in_transaction(self) -> bool:
        return self.transaction_active

    async def execute(self, statement: object) -> _Result:
        self.statements.append(statement)
        self.transaction_active = True
        return _Result(self.rows)

    async def rollback(self) -> None:
        self.rollback_calls += 1
        self.transaction_active = False


def _app_with_rows(rows: Sequence[Bar]) -> tuple[FastAPI, _Session]:
    app = create_app(Settings(app_env="test", rate_limit_enabled=False))
    fake_session = _Session(rows)

    async def override_session() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, fake_session)

    app.dependency_overrides[get_session] = override_session
    return app, fake_session


def _bar(timestamp: datetime, index: int = 0) -> Bar:
    close = 100.0 + index / 10.0
    fetched_at = timestamp + timedelta(minutes=1)
    return Bar(
        symbol="MSFT",
        timespan="day",
        multiplier=1,
        ts=timestamp,
        source="polygon_open_close",
        adjustment_basis="raw",
        open=close - 0.5,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=1_000_000.0 + index,
        vwap=close - 0.1,
        trade_count=10_000 + index,
        fetched_at=fetched_at,
        as_of=fetched_at + timedelta(minutes=1),
        recorded_at=fetched_at + timedelta(minutes=2),
        version_creator_xid=1,
    )


def _xnys_rows(count: int) -> tuple[list[Bar], list[datetime]]:
    calendar = xcals.get_calendar("XNYS")
    labels = list(calendar.sessions_in_range("2025-05-01", "2026-07-13"))[-count:]
    closes = [calendar.session_close(label).to_pydatetime() for label in labels]
    chronological = [_bar(timestamp, index) for index, timestamp in enumerate(closes)]
    return list(reversed(chronological)), closes


def _assert_private_cache_headers(response: Any, etag: str) -> None:
    assert response.headers["ETag"] == etag
    assert response.headers["Cache-Control"] == "private, no-cache"
    assert response.headers["Vary"] == "X-API-Key"


def test_empty_series_has_complete_stable_200_shape_and_exact_byte_etag() -> None:
    app, session = _app_with_rows([])

    with TestClient(app) as client:
        first = client.get("/v1/indicators/missing")
        second = client.get("/v1/indicators/MISSING")

    assert first.status_code == second.status_code == 200
    assert first.content == second.content
    assert first.headers["ETag"] == second.headers["ETag"]
    _assert_private_cache_headers(first, strong_etag(first.content))
    assert len(session.statements) == 2
    assert first.json() == {
        "symbol": "MISSING",
        "source": "polygon_open_close",
        "timespan": "day",
        "multiplier": 1,
        "adjustment_basis": "raw",
        "data_semantics": "current_snapshot_not_point_in_time",
        "endpoint_version": "stored-indicators-v1",
        "calculation_version": "owned-indicators-v1",
        "indicator_policy_hash": (
            "sha256:fca9dc9c0feaeb26ee851da6b2ac127eff72b0a972ed6b5385616869fd45e0c1"
        ),
        "window_policy_hash": (
            "sha256:59bc7fdbbdd7f8153ace97bcc75f2a445370720536fdb9d80d25c1b7204d9310"
        ),
        "parameters": {
            "return_period": 1,
            "sma_period": 20,
            "ema_period": 20,
            "volatility_period": 20,
            "rsi_period": 14,
            "macd_fast_period": 12,
            "macd_slow_period": 26,
            "macd_signal_period": 9,
            "bollinger_period": 20,
            "bollinger_standard_deviations": 2.0,
            "atr_period": 14,
        },
        "window": {
            "selection": "newest_exact_series_before_exclusive_end",
            "calendar": "XNYS",
            "calendar_ruleset": (
                "exchange-calendars==4.13.2;pandas==3.0.3;tzdata==2026.2;XNYS:1990-01-01:2100-12-31"
            ),
            "max_observations": 258,
            "required_observations": 34,
            "requested_end": None,
            "input_start": None,
            "input_end": None,
            "input_count": 0,
            "older_data_excluded": False,
            "continuity": None,
            "latest_session_completeness": "not_evaluated",
            "recursive_seed_semantics": "window_relative",
            "warmup_semantics": "structural_nulls",
            "input_digest_schema": "ordered-current-bar-ieee754-hex-v1",
            "input_sha256": None,
        },
        "data_as_of": None,
        "data_recorded_at": None,
        "count": 0,
        "observations": [],
    }


@pytest.mark.parametrize("validator_kind", ["exact", "weak", "list", "wildcard"])
def test_matching_if_none_match_returns_bodyless_304_with_private_headers(
    validator_kind: str,
) -> None:
    app, session = _app_with_rows([])
    with TestClient(app) as client:
        first = client.get("/v1/indicators/MSFT")
        etag = first.headers["ETag"]
        validator = {
            "exact": etag,
            "weak": f"W/{etag}",
            "list": f'"stale", W/{etag}',
            "wildcard": "*",
        }[validator_kind]
        response = client.get(
            "/v1/indicators/MSFT",
            headers={"If-None-Match": validator},
        )

    assert response.status_code == 304
    assert response.content == b""
    _assert_private_cache_headers(response, etag)
    # Revalidation still selects and hashes the current representation.
    assert len(session.statements) == 2


def test_repeated_if_none_match_header_lines_are_combined() -> None:
    app, _ = _app_with_rows([])
    with TestClient(app) as client:
        etag = client.get("/v1/indicators/MSFT").headers["ETag"]
        response = client.get(
            "/v1/indicators/MSFT",
            headers=[("If-None-Match", '"stale"'), ("If-None-Match", etag)],
        )

    assert response.status_code == 304
    assert response.content == b""
    _assert_private_cache_headers(response, etag)


def test_stale_if_none_match_returns_the_current_representation() -> None:
    app, _ = _app_with_rows([])
    with TestClient(app) as client:
        response = client.get(
            "/v1/indicators/MSFT",
            headers={"If-None-Match": '"stale"'},
        )

    assert response.status_code == 200
    assert response.json()["symbol"] == "MSFT"
    _assert_private_cache_headers(response, strong_etag(response.content))


@pytest.mark.parametrize(
    "malformation",
    ["unterminated", "wildcard-list", "lowercase-weak", "weak-space", "trailing-comma"],
)
def test_malformed_if_none_match_cannot_false_match_current_etag(malformation: str) -> None:
    app, _ = _app_with_rows([])
    with TestClient(app) as client:
        current = client.get("/v1/indicators/MSFT")
        etag = current.headers["ETag"]
        malformed = {
            "unterminated": etag[:-1],
            "wildcard-list": f"{etag}, *",
            "lowercase-weak": f"w/{etag}",
            "weak-space": f"W/ {etag}",
            "trailing-comma": f"{etag},",
        }[malformation]
        response = client.get(
            "/v1/indicators/MSFT",
            headers={"If-None-Match": malformed},
        )

    assert response.status_code == 200
    assert response.content == current.content
    _assert_private_cache_headers(response, etag)


def test_insufficient_history_is_structured_409_without_exception_leakage() -> None:
    rows, _ = _xnys_rows(1)
    app, _ = _app_with_rows(rows)

    with TestClient(app) as client:
        response = client.get("/v1/indicators/msft")

    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "insufficient_indicator_history"
    assert error["message"] == (
        "The stored series has too few observations for the v1 indicator bundle."
    )
    assert error["details"] == {"observed": 1, "required": 34, "symbol": "MSFT"}
    assert error["request_id"]
    serialized = response.text.lower()
    for internal in ("traceback", "insufficientindicatorhistory", "app.services", "select bars"):
        assert internal not in serialized


@pytest.mark.parametrize(
    "path",
    [
        "/v1/indicators/bad%20symbol",
        "/v1/indicators/MSFT?end=2026-07-13T20:00:00",
        "/v1/indicators/MSFT?as_of=2026-07-13T20:00:00Z",
        "/v1/indicators/MSFT?sma_period=5",
    ],
)
def test_invalid_symbol_naive_end_and_extra_query_fields_are_rejected_before_read(
    path: str,
) -> None:
    app, session = _app_with_rows([])

    with TestClient(app) as client:
        response = client.get(path)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert session.statements == []


def test_api_key_auth_short_circuits_database_session_resolution() -> None:
    app = create_app(
        Settings(app_env="test", rate_limit_enabled=False, api_keys="fixture-good-key")
    )
    session_requested = False

    async def unexpected_session() -> AsyncSession:
        nonlocal session_requested
        session_requested = True
        raise AssertionError("database dependency must not run before API-key auth")

    app.dependency_overrides[get_session] = unexpected_session
    with TestClient(app) as client:
        response = client.get("/v1/indicators/MSFT")

    assert response.status_code == 401
    assert response.json()["error"]["message"] == "Missing or invalid API key."
    assert response.headers["WWW-Authenticate"] == "X-API-Key"
    assert session_requested is False


def test_current_snapshot_window_is_bounded_disclosed_and_not_availability_filtered() -> None:
    rows_descending, closes = _xnys_rows(259)
    app, session = _app_with_rows(rows_descending)

    with TestClient(app) as client:
        response = client.get(
            "/v1/indicators/msft",
            params={"end": "2026-07-14T00:00:00+03:00"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["data_semantics"] == "current_snapshot_not_point_in_time"
    assert body["count"] == body["window"]["input_count"] == 258
    assert body["window"] == {
        **body["window"],
        "selection": "newest_exact_series_before_exclusive_end",
        "calendar": "XNYS",
        "calendar_ruleset": (
            "exchange-calendars==4.13.2;pandas==3.0.3;tzdata==2026.2;XNYS:1990-01-01:2100-12-31"
        ),
        "max_observations": 258,
        "required_observations": 34,
        "requested_end": "2026-07-13T21:00:00Z",
        "input_start": closes[1].isoformat().replace("+00:00", "Z"),
        "input_end": closes[-1].isoformat().replace("+00:00", "Z"),
        "input_count": 258,
        "older_data_excluded": True,
        "continuity": "exact_consecutive_regular_session_closes",
        "latest_session_completeness": "not_evaluated",
        "recursive_seed_semantics": "window_relative",
        "warmup_semantics": "structural_nulls",
        "input_digest_schema": "ordered-current-bar-ieee754-hex-v1",
    }
    assert [body["observations"][index]["timestamp"] for index in (0, -1)] == [
        closes[1].isoformat().replace("+00:00", "Z"),
        closes[-1].isoformat().replace("+00:00", "Z"),
    ]

    statement = cast(Any, session.statements[0])
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "bars.symbol = 'MSFT'" in sql
    assert "bars.source = 'polygon_open_close'" in sql
    assert "bars.timespan = 'day'" in sql
    assert "bars.multiplier = 1" in sql
    assert "bars.adjustment_basis = 'raw'" in sql
    assert "bars.ts < '2026-07-13 21:00:00+00:00'" in sql
    assert "ORDER BY bars.ts DESC" in sql
    assert "LIMIT 259" in sql
    where_sql = sql.split("WHERE", maxsplit=1)[1].split("ORDER BY", maxsplit=1)[0]
    assert "bars.as_of" not in where_sql
    assert session.rollback_calls == 1
    assert session.transaction_active is False


def test_openapi_documents_response_auth_errors_and_only_the_owned_window_query() -> None:
    app, _ = _app_with_rows([])
    schema = app.openapi()
    operation: dict[str, Any] = schema["paths"]["/v1/indicators/{symbol}"]["get"]

    response_schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
    assert response_schema["$ref"].endswith("/IndicatorsResponse")
    assert {"200", "304", "401", "409", "422"} <= operation["responses"].keys()
    for status_code in ("200", "304"):
        assert operation["responses"][status_code]["headers"].keys() == {
            "ETag",
            "Cache-Control",
            "Vary",
        }
    for status_code in ("401", "409", "422"):
        error_schema = operation["responses"][status_code]["content"]["application/json"]["schema"]
        assert error_schema["$ref"].endswith("/ErrorResponse")

    assert operation["security"] == [{"ApiKeyAuth": []}]
    security_scheme = schema["components"]["securitySchemes"]["ApiKeyAuth"]
    assert security_scheme == {
        "type": "apiKey",
        "description": "API key for versioned product endpoints.",
        "in": "header",
        "name": "X-API-Key",
    }

    query_parameters = [
        parameter for parameter in operation["parameters"] if parameter["in"] == "query"
    ]
    assert [parameter["name"] for parameter in query_parameters] == ["end"]
    assert "not an availability as-of bound" in query_parameters[0]["description"]
    header_parameters = {
        parameter["name"] for parameter in operation["parameters"] if parameter["in"] == "header"
    }
    assert header_parameters == {"If-None-Match"}
    assert "current-snapshot" in operation["summary"]
    assert "not a point-in-time query" in operation["description"]
