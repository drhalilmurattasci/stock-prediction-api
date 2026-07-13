"""Dedicated Celery lane for privileged forecast-input snapshot creation."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.db.session import build_engine, build_sessionmaker
from app.services.forecast_snapshot_builder import (
    DEFAULT_AVAILABILITY_RULE_SET_HASH,
    DEFAULT_RESOLUTION_POLICY_HASH,
    DEFAULT_SNAPSHOT_BUILD_POLICY,
    ForecastSnapshotBuilder,
    SnapshotBuildError,
    SnapshotBuildSpec,
    SnapshotInputUnavailable,
    database_snapshot_cutoff,
    scheduled_snapshot_cutoff,
)
from ingestion.snapshot_celery_app import snapshot_celery_app

log = structlog.get_logger(__name__)


class SnapshotBatchTransientError(RuntimeError):
    """Carry the frozen cutoff across a Celery retry after partial progress."""

    def __init__(self, cutoff: datetime, cause: Exception) -> None:
        super().__init__(str(cause))
        self.cutoff = cutoff
        self.cause = cause


@snapshot_celery_app.task(
    name="forecasting.build_forecast_snapshots",
    bind=True,
    max_retries=3,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=300,
    time_limit=330,
)
def build_forecast_snapshots(
    self,
    symbols: list[str] | None = None,
    *,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Celery entrypoint; retries retain the first database-clock cutoff."""

    try:
        return asyncio.run(
            _run_owned_snapshot_batch(
                symbols=symbols,
                as_of=_parse_as_of(as_of) if as_of is not None else None,
            )
        )
    except SnapshotBatchTransientError as exc:
        raise self.retry(
            exc=exc.cause,
            args=(),
            kwargs={"symbols": symbols, "as_of": exc.cutoff.isoformat()},
        ) from exc.cause
    except SnapshotBuildError:
        raise
    except Exception as exc:
        # Failure before the database cutoff was acquired cannot have written
        # a snapshot, so retrying without a frozen cutoff is still idempotent.
        raise self.retry(
            exc=exc,
            args=(),
            kwargs={"symbols": symbols, "as_of": as_of},
        ) from exc


async def _run_owned_snapshot_batch(
    *,
    symbols: Sequence[str] | None,
    as_of: datetime | None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    engine = build_engine(settings)
    maker = build_sessionmaker(engine)
    cutoff: datetime | None = as_of
    try:
        cutoff = cutoff or scheduled_snapshot_cutoff(await database_snapshot_cutoff(maker))
        try:
            result = await build_forecast_snapshots_async(
                symbols=symbols,
                as_of=cutoff,
                settings=settings,
                sessionmaker=maker,
            )
            if result["failed"]:
                deferred_note = (
                    f"; {result['deferred']} additional series remain unavailable"
                    if result["deferred"]
                    else ""
                )
                raise SnapshotBuildError(
                    f"{result['failed']} snapshot series failed deterministic trust checks"
                    f"{deferred_note}"
                )
            if result["deferred"]:
                raise SnapshotBatchTransientError(
                    cutoff,
                    SnapshotInputUnavailable(
                        f"{result['deferred']} snapshot series remain unavailable"
                    ),
                )
            return result
        except SnapshotBuildError:
            raise
        except SnapshotBatchTransientError:
            raise
        except Exception as exc:
            raise SnapshotBatchTransientError(cutoff, exc) from exc
    finally:
        await engine.dispose()


async def build_forecast_snapshots_async(
    *,
    symbols: Sequence[str] | None = None,
    as_of: datetime | None = None,
    settings: Settings | None = None,
    sessionmaker: async_sessionmaker[AsyncSession] | None = None,
    engine: AsyncEngine | None = None,
) -> dict[str, Any]:
    """Build one raw-close/trading-day snapshot per pinned MVP symbol."""

    settings = settings or get_settings()
    policy = DEFAULT_SNAPSHOT_BUILD_POLICY
    policy.validate_configured_hashes(
        settings.forecast_resolution_policy_hash,
        settings.forecast_trusted_availability_rule_set_hash,
    )
    symbol_list = _normalize_symbols(symbols)
    owns_engine = engine is None and sessionmaker is None
    if sessionmaker is None:
        engine = engine or build_engine(settings)
        sessionmaker = build_sessionmaker(engine)
    cutoff = (
        _as_utc(as_of)
        if as_of is not None
        else scheduled_snapshot_cutoff(await database_snapshot_cutoff(sessionmaker))
    )
    builder = ForecastSnapshotBuilder(sessionmaker, policy)
    per_symbol: list[dict[str, Any]] = []
    created = replayed = deferred = failed = 0
    try:
        for symbol in symbol_list:
            try:
                result = await builder.build(
                    SnapshotBuildSpec(
                        symbol=symbol,
                        target="close",
                        horizon_unit="trading_day",
                        as_of=cutoff,
                    )
                )
            except SnapshotInputUnavailable as exc:
                deferred += 1
                per_symbol.append(
                    {
                        "symbol": symbol,
                        "status": "deferred",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
                log.info(
                    "forecast_snapshot.deferred",
                    symbol=symbol,
                    error=str(exc),
                )
            except SnapshotBuildError as exc:
                failed += 1
                per_symbol.append(
                    {
                        "symbol": symbol,
                        "status": "failed",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
                log.error(
                    "forecast_snapshot.failed",
                    symbol=symbol,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
            else:
                created += int(result.created)
                replayed += int(not result.created)
                per_symbol.append(
                    {
                        "symbol": symbol,
                        "status": "created" if result.created else "replayed",
                        "snapshot_id": result.snapshot_id,
                        "observations": result.observation_count,
                        "target_times": result.target_time_count,
                    }
                )
                log.info(
                    "forecast_snapshot.complete",
                    symbol=symbol,
                    snapshot_id=result.snapshot_id,
                    created=result.created,
                )
    finally:
        if owns_engine and engine is not None:
            await engine.dispose()

    status = "ok"
    if failed:
        status = "failed" if failed == len(symbol_list) else "degraded"
    elif deferred:
        status = "deferred" if deferred == len(symbol_list) else "degraded"
    return {
        "status": status,
        "as_of": cutoff.isoformat(),
        "resolution_policy_hash": policy.resolution_policy_hash,
        "availability_rule_set_hash": policy.availability_rule_set_hash,
        "created": created,
        "replayed": replayed,
        "deferred": deferred,
        "failed": failed,
        "per_symbol": per_symbol,
    }


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    source = DEFAULT_SNAPSHOT_BUILD_POLICY.allowed_symbols if symbols is None else symbols
    normalized = sorted({symbol.strip().upper() for symbol in source if symbol.strip()})
    if not normalized:
        raise SnapshotBuildError("at least one snapshot symbol is required")
    unsupported = set(normalized).difference(DEFAULT_SNAPSHOT_BUILD_POLICY.allowed_symbols)
    if unsupported:
        raise SnapshotBuildError(
            f"symbols outside the pinned snapshot universe: {', '.join(sorted(unsupported))}"
        )
    return normalized


def _parse_as_of(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise SnapshotBuildError("as_of must be an ISO-8601 timestamp") from exc
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise SnapshotBuildError("as_of must be timezone-aware")
    return value.astimezone(UTC)


def _main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--print-policy-hashes",
        action="store_true",
        help="print the exact v1 hashes operators must pin",
    )
    args = parser.parse_args(argv)
    if not args.print_policy_hashes:
        parser.error("--print-policy-hashes is required; run builds through Celery")
    print(f"FORECAST_RESOLUTION_POLICY_HASH={DEFAULT_RESOLUTION_POLICY_HASH}")
    print(f"FORECAST_TRUSTED_AVAILABILITY_RULE_SET_HASH={DEFAULT_AVAILABILITY_RULE_SET_HASH}")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised as an operator command.
    raise SystemExit(_main())
