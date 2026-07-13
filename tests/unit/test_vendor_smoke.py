"""Fail-closed tests for the separately authorized live-vendor smoke harness."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import structlog

import scripts.vendor_smoke as vendor_smoke
from app.config import Settings
from data_sources.base import ProviderHTTPError
from data_sources.guards import AsyncPacingCostRateGuard
from scripts.vendor_smoke import (
    AUTHORIZATION_SENTINEL,
    SMOKE_LOCK_ID,
    SMOKE_SYMBOL,
    VendorSmokeRefused,
    _exclusive_smoke_run,
    _single_attempt_provider,
    run_vendor_smoke,
)

SMOKE_DATE = date(2026, 7, 10)
SMOKE_NOW = datetime(2026, 7, 13, 16, tzinfo=UTC)
REPO_ROOT = Path(__file__).resolve().parents[2]


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


@asynccontextmanager
async def _no_smoke_lock(settings: Settings) -> AsyncIterator[None]:
    del settings
    yield


class _FakeScalarResult:
    def __init__(self, value: bool) -> None:
        self.value = value

    def scalar_one(self) -> bool:
        return self.value


class _FakeSmokeConnection:
    def __init__(self, *, acquired: bool, events: list[str]) -> None:
        self.acquired = acquired
        self.events = events

    async def __aenter__(self) -> _FakeSmokeConnection:
        self.events.append("connect")
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        self.events.append("disconnect")

    async def execute(
        self,
        statement: object,
        parameters: dict[str, int],
    ) -> _FakeScalarResult:
        sql = str(statement)
        self.events.append(f"{sql}:{parameters['lock_id']}")
        return _FakeScalarResult(self.acquired if "pg_try_advisory_lock" in sql else True)

    async def commit(self) -> None:
        self.events.append("commit")


class _FakeSmokeEngine:
    def __init__(self, *, acquired: bool, events: list[str]) -> None:
        self.connection = _FakeSmokeConnection(acquired=acquired, events=events)
        self.events = events

    def connect(self) -> _FakeSmokeConnection:
        return self.connection

    async def dispose(self) -> None:
        self.events.append("dispose")


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
        lock_fn=_no_smoke_lock,
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
    assert captured["include_error_details"] is False
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
        ({"polygon_api_key": "fake-key\nsecond-line"}, "visible ASCII"),
        ({"polygon_api_key": "fake-key-ş"}, "visible ASCII"),
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
    db_called = False
    lock_called = False

    async def ingest(**kwargs: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        return _successful_result()

    async def row_exists(settings: Settings, symbol: str, observed_at: datetime) -> bool:
        nonlocal db_called
        del settings, symbol, observed_at
        db_called = True
        return False

    @asynccontextmanager
    async def lock(settings: Settings) -> AsyncIterator[None]:
        nonlocal lock_called
        del settings
        lock_called = True
        yield

    with pytest.raises(VendorSmokeRefused, match=message):
        await run_vendor_smoke(
            session_date=SMOKE_DATE,
            authorization=AUTHORIZATION_SENTINEL,
            settings=_settings(**overrides),
            clock=lambda: SMOKE_NOW,
            ingest_fn=ingest,
            row_exists_fn=row_exists,
            receipt_exists_fn=_receipt_check(True),
            lock_fn=lock,
        )
    assert called is False
    assert db_called is False
    assert lock_called is False


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
            lock_fn=_no_smoke_lock,
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
            lock_fn=_no_smoke_lock,
        )
    assert called is False


async def test_contended_lock_refuses_before_target_row_check() -> None:
    db_called = False
    ingest_called = False

    async def row_exists(settings: Settings, symbol: str, observed_at: datetime) -> bool:
        nonlocal db_called
        del settings, symbol, observed_at
        db_called = True
        return False

    async def ingest(**kwargs: Any) -> dict[str, Any]:
        nonlocal ingest_called
        del kwargs
        ingest_called = True
        return _successful_result()

    @asynccontextmanager
    async def contended(settings: Settings) -> AsyncIterator[None]:
        if settings:
            raise VendorSmokeRefused("another vendor smoke is already running")
        yield

    with pytest.raises(VendorSmokeRefused, match="already running"):
        await run_vendor_smoke(
            session_date=SMOKE_DATE,
            authorization=AUTHORIZATION_SENTINEL,
            settings=_settings(),
            clock=lambda: SMOKE_NOW,
            ingest_fn=ingest,
            row_exists_fn=row_exists,
            receipt_exists_fn=_receipt_check(True),
            lock_fn=contended,
        )

    assert db_called is False
    assert ingest_called is False


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
            lock_fn=_no_smoke_lock,
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
            lock_fn=_no_smoke_lock,
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
            lock_fn=_no_smoke_lock,
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


async def test_smoke_factory_makes_exactly_one_real_open_close_transport_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        assert request.url.path == "/v1/open-close/MSFT/2026-07-10"
        assert request.url.params["adjusted"] == "false"
        assert request.headers["Authorization"] == "Bearer test-vendor-key"
        return httpx.Response(503, json={"error": "temporarily unavailable"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(
        "data_sources.polygon.httpx.AsyncClient",
        lambda **_kwargs: client,
    )
    provider = _single_attempt_provider(_settings())
    try:
        with pytest.raises(ProviderHTTPError):
            await provider.get_daily_bars(SMOKE_SYMBOL, SMOKE_DATE, SMOKE_DATE)
    finally:
        await provider.aclose()

    assert attempts == 1


async def test_database_advisory_lock_wraps_the_entire_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    engine = _FakeSmokeEngine(acquired=True, events=events)
    monkeypatch.setattr(vendor_smoke, "build_engine", lambda _settings: engine)

    async with _exclusive_smoke_run(_settings()):
        events.append("operation")

    assert events == [
        "connect",
        f"SELECT pg_try_advisory_lock(:lock_id):{SMOKE_LOCK_ID}",
        "commit",
        "operation",
        f"SELECT pg_advisory_unlock(:lock_id):{SMOKE_LOCK_ID}",
        "commit",
        "disconnect",
        "dispose",
    ]


async def test_database_advisory_lock_contention_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    engine = _FakeSmokeEngine(acquired=False, events=events)
    monkeypatch.setattr(vendor_smoke, "build_engine", lambda _settings: engine)

    with pytest.raises(VendorSmokeRefused, match="already running"):
        async with _exclusive_smoke_run(_settings()):
            events.append("operation")

    assert "operation" not in events
    assert all("pg_advisory_unlock" not in event for event in events)
    assert events[-2:] == ["disconnect", "dispose"]


async def test_database_advisory_lock_releases_after_body_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    engine = _FakeSmokeEngine(acquired=True, events=events)
    monkeypatch.setattr(vendor_smoke, "build_engine", lambda _settings: engine)

    with pytest.raises(RuntimeError, match="synthetic body failure"):
        async with _exclusive_smoke_run(_settings()):
            raise RuntimeError("synthetic body failure")

    assert f"SELECT pg_advisory_unlock(:lock_id):{SMOKE_LOCK_ID}" in events
    assert events[-3:] == ["commit", "disconnect", "dispose"]


def test_main_never_renders_secret_bearing_exception_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_key = "FAKE_VENDOR_KEY_MUST_NOT_RENDER_7f92"
    original_structlog_config = structlog.get_config()

    async def fail_with_secret(**kwargs: object) -> dict[str, object]:
        del kwargs
        logger = structlog.get_logger("vendor-smoke-secret-regression")
        try:
            raise httpx.LocalProtocolError(f"Authorization: Bearer {fake_key}")
        except httpx.LocalProtocolError:
            logger.warning(
                "ingest_forecast_closes.symbol_failed",
                error_type="LocalProtocolError",
                exc_info=True,
            )
        raise RuntimeError("outer failure")

    monkeypatch.setattr(vendor_smoke, "run_vendor_smoke", fail_with_secret)
    try:
        result = vendor_smoke.main(
            [
                "--session",
                SMOKE_DATE.isoformat(),
                "--authorization",
                AUTHORIZATION_SENTINEL,
            ]
        )
        captured = capsys.readouterr()
    finally:
        structlog.configure(**original_structlog_config)

    rendered = captured.out + captured.err
    assert result == 1
    assert "ingest_forecast_closes.symbol_failed" in rendered
    assert "vendor smoke failed: RuntimeError" in rendered
    assert fake_key not in rendered
    assert "Authorization" not in rendered


def test_wrapper_scans_versioned_python_worker_names() -> None:
    wrapper = (REPO_ROOT / "run-vendor-smoke.ps1").read_text(encoding="utf-8")
    assert "python(?:w|[0-9]+(?:\\.[0-9]+)?)?" in wrapper
