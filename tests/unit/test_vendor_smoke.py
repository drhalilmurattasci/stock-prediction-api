"""Fail-closed tests for the separately authorized live-vendor smoke harness."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime
from typing import Any

import pytest

from app.config import Settings
from data_sources.guards import AsyncPacingCostRateGuard
from scripts.vendor_smoke import (
    AUTHORIZATION_SENTINEL,
    SMOKE_SYMBOL,
    VendorSmokeRefused,
    _single_attempt_provider,
    run_vendor_smoke,
)

SMOKE_DATE = date(2026, 7, 10)
SMOKE_NOW = datetime(2026, 7, 13, 16, tzinfo=UTC)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "local",
        "database_url": (
            "postgresql+asyncpg://stockapi_app:test-secret@localhost:5432/stockapi_test"
        ),
        "polygon_api_key": "test-vendor-key",
        "polygon_max_calls_per_window": 5,
        "polygon_total_call_budget": 0,
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _successful_result() -> dict[str, Any]:
    return {
        "status": "ok",
        "provider": "polygon_open_close",
        "symbols": [SMOKE_SYMBOL],
        "rows_upserted": 1,
        "revisions": 0,
        "failures": 0,
        "retryable_failures": 0,
        "per_symbol": [
            {
                "symbol": SMOKE_SYMBOL,
                "status": "ok",
                "bars": 1,
                "rows_upserted": 1,
                "revisions": 0,
            }
        ],
    }


def _row_checks(*values: bool) -> Callable[[Settings, str, datetime], Awaitable[bool]]:
    remaining = iter(values)

    async def check(settings: Settings, symbol: str, observed_at: datetime) -> bool:
        del settings
        assert symbol == SMOKE_SYMBOL
        assert observed_at == datetime(2026, 7, 10, 20, tzinfo=UTC)
        return next(remaining)

    return check


def _receipt_check(value: bool) -> Callable[[Settings, str, datetime], Awaitable[bool]]:
    async def check(settings: Settings, symbol: str, observed_at: datetime) -> bool:
        del settings
        assert symbol == SMOKE_SYMBOL
        assert observed_at == datetime(2026, 7, 10, 20, tzinfo=UTC)
        return value

    return check


async def test_smoke_forces_one_attempt_and_proves_one_new_row() -> None:
    original = _settings()
    captured: dict[str, Any] = {}

    async def ingest(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return _successful_result()

    result = await run_vendor_smoke(
        session_date=SMOKE_DATE,
        authorization=AUTHORIZATION_SENTINEL,
        settings=original,
        clock=lambda: SMOKE_NOW,
        ingest_fn=ingest,
        row_exists_fn=_row_checks(False, True),
        receipt_exists_fn=_receipt_check(True),
    )

    assert result == {
        "status": "ok",
        "provider": "polygon_open_close",
        "symbol": "MSFT",
        "session": "2026-07-10",
        "outbound_attempt_budget": 1,
        "rows_persisted": 1,
    }
    assert captured["symbols"] == ["MSFT"]
    assert captured["start"] == captured["end"] == SMOKE_DATE
    assert captured["use_watermark"] is False
    assert captured["provider_factory"] is _single_attempt_provider
    guarded = captured["settings"]
    assert isinstance(guarded, Settings)
    assert guarded.polygon_max_calls_per_window == 1
    assert guarded.polygon_total_call_budget == 1
    assert original.polygon_max_calls_per_window == 5
    assert original.polygon_total_call_budget == 0


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"app_env": "test"}, "APP_ENV"),
        ({"polygon_api_key": ""}, "POLYGON_API_KEY"),
        (
            {
                "database_url": (
                    "postgresql+asyncpg://stockapi_app:test-secret@localhost:5432/stockapi"
                )
            },
            "DATABASE_URL",
        ),
        (
            {
                "database_url": (
                    "postgresql+asyncpg://stockapi_owner:test-secret@localhost:5432/stockapi_test"
                )
            },
            "DATABASE_URL",
        ),
        (
            {
                "database_url": (
                    "postgresql+asyncpg://stockapi_app:test-secret@db.example:5432/stockapi_test"
                )
            },
            "DATABASE_URL",
        ),
        (
            {
                "database_url": (
                    "postgresql+asyncpg://stockapi_app:test-secret@localhost/stockapi_test"
                )
            },
            "DATABASE_URL",
        ),
        (
            {
                "database_url": (
                    "postgresql+asyncpg://stockapi_app:test-secret@localhost:5432/"
                    "stockapi_test?sslmode=disable"
                )
            },
            "DATABASE_URL",
        ),
    ],
)
async def test_environment_scope_refuses_before_ingestion(
    overrides: dict[str, object],
    message: str,
) -> None:
    called = False

    async def ingest(**kwargs: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        return _successful_result()

    with pytest.raises(VendorSmokeRefused, match=message):
        await run_vendor_smoke(
            session_date=SMOKE_DATE,
            authorization=AUTHORIZATION_SENTINEL,
            settings=_settings(**overrides),
            clock=lambda: SMOKE_NOW,
            ingest_fn=ingest,
            row_exists_fn=_row_checks(False),
            receipt_exists_fn=_receipt_check(True),
        )
    assert called is False


@pytest.mark.parametrize(
    ("authorization", "session_date", "message"),
    [
        ("", SMOKE_DATE, "authorization"),
        ("almost", SMOKE_DATE, "authorization"),
        (AUTHORIZATION_SENTINEL, date(2026, 7, 9), "latest completed"),
    ],
)
async def test_authorization_and_exact_session_fail_closed(
    authorization: str,
    session_date: date,
    message: str,
) -> None:
    called = False

    async def ingest(**kwargs: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        return _successful_result()

    with pytest.raises(VendorSmokeRefused, match=message):
        await run_vendor_smoke(
            session_date=session_date,
            authorization=authorization,
            settings=_settings(),
            clock=lambda: SMOKE_NOW,
            ingest_fn=ingest,
            row_exists_fn=_row_checks(False),
            receipt_exists_fn=_receipt_check(True),
        )
    assert called is False


async def test_existing_target_refuses_without_ingestion() -> None:
    called = False

    async def ingest(**kwargs: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        return _successful_result()

    with pytest.raises(VendorSmokeRefused, match="already exists"):
        await run_vendor_smoke(
            session_date=SMOKE_DATE,
            authorization=AUTHORIZATION_SENTINEL,
            settings=_settings(),
            clock=lambda: SMOKE_NOW,
            ingest_fn=ingest,
            row_exists_fn=_row_checks(True),
            receipt_exists_fn=_receipt_check(True),
        )
    assert called is False


@pytest.mark.parametrize(
    "result",
    [
        {**_successful_result(), "status": "failed", "failures": 1},
        {**_successful_result(), "rows_upserted": 0},
        {**_successful_result(), "revisions": 1},
    ],
)
async def test_inexact_ingestion_result_fails_closed(result: dict[str, Any]) -> None:
    async def ingest(**kwargs: Any) -> dict[str, Any]:
        return result

    with pytest.raises(VendorSmokeRefused, match="exactly one new MSFT bar"):
        await run_vendor_smoke(
            session_date=SMOKE_DATE,
            authorization=AUTHORIZATION_SENTINEL,
            settings=_settings(),
            clock=lambda: SMOKE_NOW,
            ingest_fn=ingest,
            row_exists_fn=_row_checks(False),
            receipt_exists_fn=_receipt_check(True),
        )


async def test_reported_success_without_persisted_row_fails_closed() -> None:
    async def ingest(**kwargs: Any) -> dict[str, Any]:
        return _successful_result()

    with pytest.raises(VendorSmokeRefused, match="exact bar is absent"):
        await run_vendor_smoke(
            session_date=SMOKE_DATE,
            authorization=AUTHORIZATION_SENTINEL,
            settings=_settings(),
            clock=lambda: SMOKE_NOW,
            ingest_fn=ingest,
            row_exists_fn=_row_checks(False, False),
            receipt_exists_fn=_receipt_check(True),
        )


async def test_missing_exact_availability_receipt_fails_closed() -> None:
    async def ingest(**kwargs: Any) -> dict[str, Any]:
        return _successful_result()

    with pytest.raises(VendorSmokeRefused, match="post-commit availability receipt"):
        await run_vendor_smoke(
            session_date=SMOKE_DATE,
            authorization=AUTHORIZATION_SENTINEL,
            settings=_settings(),
            clock=lambda: SMOKE_NOW,
            ingest_fn=ingest,
            row_exists_fn=_row_checks(False, True),
            receipt_exists_fn=_receipt_check(False),
        )


async def test_smoke_provider_disables_retries_and_caps_guard() -> None:
    provider = _single_attempt_provider(_settings())
    try:
        assert provider._max_attempts == 1  # noqa: SLF001
        guard = provider._guard  # noqa: SLF001
        assert isinstance(guard, AsyncPacingCostRateGuard)
        assert guard.max_calls == 1
        assert guard.total_budget == 1
    finally:
        await provider.aclose()
