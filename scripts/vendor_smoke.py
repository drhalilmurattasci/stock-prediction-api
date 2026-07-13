"""One-request Massive/Polygon credential, payload, and write-path smoke.

This utility is intentionally narrower than the ingestion task it invokes. It
accepts only the fixed ``MSFT`` raw-close lane, the latest completed XNYS
session, a local ``stockapi_test`` runtime database, and an explicit operator
sentinel. The provider receives a one-attempt total budget, and HTTP retries are
disabled as a second, independent bound.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, date, datetime
from typing import Any

import exchange_calendars as xcals
import pandas as pd
from sqlalchemy import and_, exists, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from app.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.models.bars import Bar, BarVersionAvailability
from app.db.session import build_engine
from data_sources.guards import AsyncPacingCostRateGuard
from data_sources.polygon_open_close import PolygonOpenCloseProvider
from ingestion.locks import vendor_operation_lock_id
from ingestion.tasks.ingest_forecast_closes import (
    ingest_forecast_closes_async,
    latest_completed_xnys_session,
)

AUTHORIZATION_SENTINEL = "stockapi-vendor-smoke-only"
SMOKE_SYMBOL = "MSFT"
SMOKE_SOURCE = "polygon_open_close"
SMOKE_TIMESPAN = "day"
SMOKE_ADJUSTMENT_BASIS = "raw"
SMOKE_MULTIPLIER = 1
SMOKE_ATTEMPT_BUDGET = 1
SMOKE_LOCK_ID = vendor_operation_lock_id()

IngestFn = Callable[..., Awaitable[dict[str, Any]]]
RowExistsFn = Callable[[Settings, str, datetime], Awaitable[bool]]
ReceiptExistsFn = Callable[[Settings, str, datetime], Awaitable[bool]]
SmokeLockFn = Callable[[Settings], AbstractAsyncContextManager[None]]


class VendorSmokeRefused(RuntimeError):
    """The requested live operation is outside the one-attempt smoke contract."""


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _safe_settings(settings: Settings) -> Settings:
    if settings.app_env != "local":
        raise VendorSmokeRefused("APP_ENV must be exactly local")
    key = settings.polygon_api_key.strip() if settings.polygon_api_key else ""
    if not key:
        raise VendorSmokeRefused("POLYGON_API_KEY must be non-empty in .env")
    if not key.isascii() or any(not 0x21 <= ord(character) <= 0x7E for character in key):
        raise VendorSmokeRefused("POLYGON_API_KEY must contain only visible ASCII characters")

    try:
        database_url = make_url(settings.database_url)
    except (ArgumentError, ValueError):
        raise VendorSmokeRefused("DATABASE_URL is not a valid SQLAlchemy URL") from None

    target = (
        database_url.drivername,
        database_url.username,
        (database_url.host or "").lower(),
        database_url.port,
        database_url.database,
    )
    allowed_targets = {
        ("postgresql+asyncpg", "stockapi_app", "localhost", 5432, "stockapi_test"),
        ("postgresql+asyncpg", "stockapi_app", "127.0.0.1", 5432, "stockapi_test"),
    }
    if target not in allowed_targets or not database_url.password or database_url.query:
        raise VendorSmokeRefused("DATABASE_URL must use stockapi_app on local stockapi_test:5432")

    # One guard acquisition precedes every HTTP attempt, including retries.
    # Force both the rolling window and cumulative process budget to one.
    return settings.model_copy(
        update={
            "polygon_api_key": key,
            "polygon_max_calls_per_window": SMOKE_ATTEMPT_BUDGET,
            "polygon_total_call_budget": SMOKE_ATTEMPT_BUDGET,
        }
    )


@asynccontextmanager
async def _exclusive_smoke_run(settings: Settings) -> AsyncIterator[None]:
    """Hold a cross-process DB lock across precheck, request, and receipt proof."""

    engine = build_engine(settings)
    try:
        async with engine.connect() as connection:
            acquired = bool(
                (
                    await connection.execute(
                        text("SELECT pg_try_advisory_lock(:lock_id)"),
                        {"lock_id": SMOKE_LOCK_ID},
                    )
                ).scalar_one()
            )
            # Session locks survive commit; do not hold an idle transaction open
            # while the bounded vendor request is in flight.
            await connection.commit()
            if not acquired:
                raise VendorSmokeRefused("another vendor smoke is already running")
            try:
                yield
            finally:
                await connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_id)"),
                    {"lock_id": SMOKE_LOCK_ID},
                )
                await connection.commit()
    finally:
        # Closing the dedicated connection is also a server-side unlock backstop.
        await engine.dispose()


@asynccontextmanager
async def _already_held_vendor_operation(settings: Settings) -> AsyncIterator[None]:
    """Mark the nested ingestion call as covered by the smoke's outer lock."""

    del settings
    yield


def _session_close(session_date: date) -> datetime:
    calendar = xcals.get_calendar("XNYS")
    label = pd.Timestamp(session_date)
    try:
        close = calendar.session_close(label).to_pydatetime()
    except Exception as exc:
        raise VendorSmokeRefused("session must be an XNYS trading date") from exc
    return close.astimezone(UTC)


async def _target_row_exists(
    settings: Settings,
    symbol: str,
    observed_at: datetime,
) -> bool:
    engine = build_engine(settings)
    try:
        statement = select(
            exists().where(
                Bar.symbol == symbol,
                Bar.ts == observed_at,
                Bar.timespan == SMOKE_TIMESPAN,
                Bar.multiplier == SMOKE_MULTIPLIER,
                Bar.source == SMOKE_SOURCE,
                Bar.adjustment_basis == SMOKE_ADJUSTMENT_BASIS,
            )
        )
        async with engine.connect() as connection:
            return bool((await connection.execute(statement)).scalar_one())
    finally:
        await engine.dispose()


async def _target_receipt_exists(
    settings: Settings,
    symbol: str,
    observed_at: datetime,
) -> bool:
    engine = build_engine(settings)
    try:
        key_match = (
            BarVersionAvailability.symbol == Bar.symbol,
            BarVersionAvailability.timespan == Bar.timespan,
            BarVersionAvailability.multiplier == Bar.multiplier,
            BarVersionAvailability.ts == Bar.ts,
            BarVersionAvailability.source == Bar.source,
            BarVersionAvailability.adjustment_basis == Bar.adjustment_basis,
            BarVersionAvailability.version_recorded_at == Bar.recorded_at,
        )
        receipt = (
            select(1)
            .select_from(BarVersionAvailability)
            .join(Bar, and_(*key_match))
            .where(
                Bar.symbol == symbol,
                Bar.ts == observed_at,
                Bar.timespan == SMOKE_TIMESPAN,
                Bar.multiplier == SMOKE_MULTIPLIER,
                Bar.source == SMOKE_SOURCE,
                Bar.adjustment_basis == SMOKE_ADJUSTMENT_BASIS,
                BarVersionAvailability.available_at >= Bar.recorded_at,
            )
            .exists()
        )
        async with engine.connect() as connection:
            return bool((await connection.execute(select(receipt))).scalar_one())
    finally:
        await engine.dispose()


def _single_attempt_provider(settings: Settings) -> PolygonOpenCloseProvider:
    key = settings.polygon_api_key.strip() if settings.polygon_api_key else ""
    if not key:  # Already validated; retained as a local type/safety boundary.
        raise VendorSmokeRefused("POLYGON_API_KEY must be non-empty in .env")
    return PolygonOpenCloseProvider(
        key,
        guard=AsyncPacingCostRateGuard(
            max_calls_per_window=SMOKE_ATTEMPT_BUDGET,
            window_seconds=settings.polygon_rate_window_seconds,
            total_budget=SMOKE_ATTEMPT_BUDGET,
        ),
        max_attempts=SMOKE_ATTEMPT_BUDGET,
    )


def _validate_result(result: dict[str, Any]) -> None:
    per_symbol = result.get("per_symbol")
    entry = per_symbol[0] if isinstance(per_symbol, list) and len(per_symbol) == 1 else None
    if not (
        result.get("status") == "ok"
        and result.get("provider") == SMOKE_SOURCE
        and result.get("symbols") == [SMOKE_SYMBOL]
        and result.get("rows_upserted") == 1
        and result.get("revisions") == 0
        and result.get("failures") == 0
        and result.get("retryable_failures") == 0
        and isinstance(entry, dict)
        and entry.get("symbol") == SMOKE_SYMBOL
        and entry.get("status") == "ok"
        and entry.get("bars") == 1
        and entry.get("rows_upserted") == 1
        and entry.get("revisions") == 0
    ):
        raise VendorSmokeRefused("ingestion did not persist exactly one new MSFT bar")


async def run_vendor_smoke(
    *,
    session_date: date,
    authorization: str,
    settings: Settings | None = None,
    clock: Callable[[], datetime] = _utcnow,
    ingest_fn: IngestFn = ingest_forecast_closes_async,
    row_exists_fn: RowExistsFn = _target_row_exists,
    receipt_exists_fn: ReceiptExistsFn = _target_receipt_exists,
    lock_fn: SmokeLockFn = _exclusive_smoke_run,
) -> dict[str, object]:
    """Make at most one outbound request, then prove the exact row persisted."""

    if authorization != AUTHORIZATION_SENTINEL:
        raise VendorSmokeRefused(f"authorization must be exactly {AUTHORIZATION_SENTINEL}")

    now = clock()
    if now.tzinfo is None:
        raise VendorSmokeRefused("clock must be timezone-aware")
    latest = latest_completed_xnys_session(now)
    if session_date != latest:
        raise VendorSmokeRefused(
            f"session must equal the latest completed XNYS session ({latest.isoformat()})"
        )

    observed_at = _session_close(session_date)
    guarded_settings = _safe_settings(settings or get_settings())
    async with lock_fn(guarded_settings):
        if await row_exists_fn(guarded_settings, SMOKE_SYMBOL, observed_at):
            raise VendorSmokeRefused("the exact MSFT smoke row already exists; refusing a replay")

        result = await ingest_fn(
            symbols=[SMOKE_SYMBOL],
            start=session_date,
            end=session_date,
            use_watermark=False,
            settings=guarded_settings,
            provider_factory=_single_attempt_provider,
            clock=clock,
            include_error_details=False,
            operation_lock_fn=_already_held_vendor_operation,
        )
        _validate_result(result)
        if not await row_exists_fn(guarded_settings, SMOKE_SYMBOL, observed_at):
            raise VendorSmokeRefused("ingestion reported success but the exact bar is absent")
        if not await receipt_exists_fn(guarded_settings, SMOKE_SYMBOL, observed_at):
            raise VendorSmokeRefused(
                "the exact bar lacks its DB-stamped post-commit availability receipt"
            )

    return {
        "status": "ok",
        "provider": SMOKE_SOURCE,
        "symbol": SMOKE_SYMBOL,
        "session": session_date.isoformat(),
        "outbound_attempt_budget": SMOKE_ATTEMPT_BUDGET,
        "rows_persisted": 1,
    }


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("session must be YYYY-MM-DD") from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", required=True, type=_iso_date)
    parser.add_argument("--authorization", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    # Remove exception payloads BEFORE any vendor work. Rich tracebacks render
    # exception locals, while transport failures can echo an invalid
    # Authorization value in their exception text. Structured error types remain.
    configure_logging("INFO", json_logs=False, exception_details=False)
    try:
        result = asyncio.run(
            run_vendor_smoke(
                session_date=args.session,
                authorization=args.authorization,
            )
        )
    except VendorSmokeRefused as exc:
        print(f"vendor smoke refused: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - never echo possibly sensitive exception text.
        print(f"vendor smoke failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
