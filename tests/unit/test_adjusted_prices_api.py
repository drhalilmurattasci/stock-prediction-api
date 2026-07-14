"""Public contract tests for factor-pinned adjusted-price reads."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1 import prices as prices_api
from app.config import Settings
from app.core.etag import strong_etag
from app.db.session import get_session
from app.main import create_app
from app.schemas.prices import (
    AdjustedPriceBar,
    AdjustedPriceFilters,
    AdjustedPriceLineage,
    AdjustedPricesResponse,
    PricePage,
)
from app.services.adjusted_price_store import AdjustedPriceEvidenceUnavailable

FACTOR_SET_ID = "sha256:" + "a" * 64
POLICY_HASH = "sha256:" + "b" * 64
SPLIT_COLLECTION_ID = "sha256:" + "c" * 64
DIVIDEND_COLLECTION_ID = "sha256:" + "d" * 64
ACTION_VERSION_ID = "sha256:" + "e" * 64


def _response() -> AdjustedPricesResponse:
    timestamp = datetime(2026, 7, 10, 20, tzinfo=UTC)
    factor_recorded = datetime(2026, 7, 10, 21, 1, tzinfo=UTC)
    factor_available = datetime(2026, 7, 10, 21, 2, tzinfo=UTC)
    lineage = AdjustedPriceLineage(
        factor_set_id=FACTOR_SET_ID,
        factor_set_recorded_at=factor_recorded,
        factor_set_available_at=factor_available,
        policy_version="split-dividend-gross-total-return-v1",
        policy_hash=POLICY_HASH,
        cutoff=datetime(2026, 7, 10, 21, tzinfo=UTC),
        anchor_date=date(2026, 7, 10),
        raw_coverage_start=timestamp,
        raw_coverage_end=timestamp,
        split_collection_id=SPLIT_COLLECTION_ID,
        split_collection_recorded_at=datetime(2026, 7, 10, 20, 7, tzinfo=UTC),
        split_collection_available_at=datetime(2026, 7, 10, 20, 8, tzinfo=UTC),
        dividend_collection_id=DIVIDEND_COLLECTION_ID,
        dividend_collection_recorded_at=datetime(2026, 7, 10, 20, 8, tzinfo=UTC),
        dividend_collection_available_at=datetime(2026, 7, 10, 20, 9, tzinfo=UTC),
        action_version_ids=(ACTION_VERSION_ID,),
        max_input_available_at=datetime(2026, 7, 10, 20, 9, tzinfo=UTC),
        data_available_at=factor_available,
        raw_input_count=1,
        adjustment_basis="split_dividend_adjusted",
    )
    bar = AdjustedPriceBar(
        raw_input_ordinal=0,
        timestamp=timestamp,
        open=99.0,
        high=102.0,
        low=98.0,
        close=101.0,
        volume=1000.0,
        vwap=100.5,
        trade_count=42,
        raw_version_recorded_at=datetime(2026, 7, 10, 20, 5, tzinfo=UTC),
        raw_available_at=datetime(2026, 7, 10, 20, 6, tzinfo=UTC),
        available_at=factor_available,
        price_factor_f64_be="3ff0000000000000",
        volume_factor_f64_be="3ff0000000000000",
    )
    return AdjustedPricesResponse(
        symbol="MSFT",
        source="polygon_open_close",
        timespan="day",
        multiplier=1,
        adjustment_basis="split_dividend_adjusted",
        factor_set_id=FACTOR_SET_ID,
        data_available_at=factor_available,
        count=1,
        page=PricePage(limit=100, has_more=False),
        lineage=lineage,
        bars=[bar],
    )


def _app() -> FastAPI:
    app = create_app(Settings(app_env="test", rate_limit_enabled=False))

    async def override_session() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, object())

    app.dependency_overrides[get_session] = override_session
    return app


def test_adjusted_route_requires_exact_factor_id_and_returns_receipt_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[AdjustedPriceFilters] = []

    async def fake_read(
        session: AsyncSession,
        symbol: str,
        filters: AdjustedPriceFilters,
    ) -> AdjustedPricesResponse:
        assert symbol == "msft"
        calls.append(filters)
        return _response()

    monkeypatch.setattr(prices_api, "read_adjusted_prices", fake_read)
    app = _app()
    with TestClient(app) as client:
        response = client.get(f"/v1/prices/msft/adjusted?factor_set_id={FACTOR_SET_ID}")

    assert response.status_code == 200
    assert response.headers["ETag"] == strong_etag(response.content)
    assert response.headers["Cache-Control"] == "private, no-cache"
    assert response.headers["Vary"] == "X-API-Key"
    assert calls == [AdjustedPriceFilters(factor_set_id=FACTOR_SET_ID)]
    body = response.json()
    assert body["adjustment_basis"] == "split_dividend_adjusted"
    assert body["factor_set_id"] == FACTOR_SET_ID
    assert body["data_available_at"] == "2026-07-10T21:02:00Z"
    assert body["lineage"]["split_collection_id"] == SPLIT_COLLECTION_ID
    assert body["lineage"]["dividend_collection_id"] == DIVIDEND_COLLECTION_ID
    assert body["lineage"]["action_version_ids"] == [ACTION_VERSION_ID]
    assert body["bars"][0]["raw_version_recorded_at"] == "2026-07-10T20:05:00Z"
    assert body["bars"][0]["raw_available_at"] == "2026-07-10T20:06:00Z"


@pytest.mark.parametrize(
    "query",
    [
        "",
        "?factor_set_id=latest",
        f"?factor_set_id={FACTOR_SET_ID}&start=2026-07-10T00:00:00",
    ],
)
def test_adjusted_route_rejects_implicit_or_malformed_factor_selection(query: str) -> None:
    app = _app()
    with TestClient(app) as client:
        response = client.get(f"/v1/prices/MSFT/adjusted{query}")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


def test_adjusted_route_supports_strong_conditional_revalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_read(
        session: AsyncSession,
        symbol: str,
        filters: AdjustedPriceFilters,
    ) -> AdjustedPricesResponse:
        return _response()

    monkeypatch.setattr(prices_api, "read_adjusted_prices", fake_read)
    app = _app()
    path = f"/v1/prices/MSFT/adjusted?factor_set_id={FACTOR_SET_ID}"
    with TestClient(app) as client:
        first = client.get(path)
        replay = client.get(path, headers={"If-None-Match": first.headers["ETag"]})

    assert replay.status_code == 304
    assert replay.content == b""
    assert replay.headers["ETag"] == first.headers["ETag"]


def test_adjusted_evidence_failure_is_structured_and_never_falls_back_to_raw(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_read(
        session: AsyncSession,
        symbol: str,
        filters: AdjustedPriceFilters,
    ) -> AdjustedPricesResponse:
        raise AdjustedPriceEvidenceUnavailable("Adjusted-price evidence is incomplete or invalid.")

    monkeypatch.setattr(prices_api, "read_adjusted_prices", fail_read)
    app = _app()
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/v1/prices/MSFT/adjusted?factor_set_id={FACTOR_SET_ID}")

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "adjusted_price_evidence_unavailable"
    assert "bars" not in response.json()


def test_openapi_keeps_raw_contract_stable_and_documents_explicit_adjusted_selection() -> None:
    app = _app()
    paths = app.openapi()["paths"]
    raw_operation: dict[str, Any] = paths["/v1/prices/{symbol}"]["get"]
    adjusted_operation: dict[str, Any] = paths["/v1/prices/{symbol}/adjusted"]["get"]

    raw_schema = raw_operation["responses"]["200"]["content"]["application/json"]["schema"]
    adjusted_schema = adjusted_operation["responses"]["200"]["content"]["application/json"][
        "schema"
    ]
    assert raw_schema["$ref"].endswith("/PricesResponse")
    assert adjusted_schema["$ref"].endswith("/AdjustedPricesResponse")
    raw_queries = {item["name"] for item in raw_operation["parameters"] if item["in"] == "query"}
    assert raw_queries == {
        "start",
        "end",
        "timespan",
        "multiplier",
        "source",
        "adjustment_basis",
        "limit",
    }
    adjusted_queries = {
        item["name"]: item for item in adjusted_operation["parameters"] if item["in"] == "query"
    }
    assert set(adjusted_queries) == {"factor_set_id", "start", "end", "limit"}
    assert adjusted_queries["factor_set_id"]["required"] is True
    assert {"401", "404", "409", "422", "503", "304"} <= adjusted_operation["responses"].keys()
