"""One-shot, builder-role-only MSFT adjusted forecast snapshot seal.

The command performs no vendor I/O and is not a Celery task.  It resolves and
publishes one immutable adjustment-factor set from already-receipted local
evidence at an operator-bound cutoff, seals one adjusted-close forecast
snapshot at the factor receipt's stable timestamp, and exits.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import re
import sys
from collections.abc import Sequence
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, date, datetime
from pathlib import Path

import exchange_calendars as xcals
import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.db.models.forecast_snapshots import ForecastInputSnapshot
from app.db.session import build_engine, build_sessionmaker
from app.services.adjusted_forecast_snapshot_builder import (
    ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    ADJUSTED_RESOLUTION_POLICY_HASH,
    DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY,
    AdjustedForecastSnapshotBuilder,
    AdjustedSnapshotBuildSpec,
)
from app.services.adjustment_factor_builder import (
    AdjustmentFactorBuilder,
    AdjustmentFactorBuildSpec,
)
from app.services.adjustment_factor_store import SqlAdjustmentFactorSetStore
from app.services.forecast_snapshot_builder import database_snapshot_cutoff
from app.services.forecast_snapshots import (
    SnapshotValidationError,
    canonical_snapshot_payload,
    parse_snapshot_payload,
    snapshot_id_for_payload,
)
from app.services.market_calendar import latest_completed_xnys_session

AUTHORIZATION_SENTINEL = "stockapi-msft-adjusted-seal-only"
SYMBOL = "MSFT"
REQUIRED_SESSIONS = 258
ROLLOVER_GUARD_SECONDS = 600
BUILD_REVISION_FILE = Path("/app/.stockapi-build-revision")
_GIT_REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_CONTENT_ID_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


class OneShotAdjustedSealRefused(RuntimeError):
    """The invocation escaped the exact reviewed local adjusted-seal contract."""


def _attest_build_revision(
    tool_revision: str,
    revision_file: Path = BUILD_REVISION_FILE,
) -> None:
    if _GIT_REVISION_PATTERN.fullmatch(tool_revision) is None:
        raise OneShotAdjustedSealRefused("tool_revision must identify one reviewed Git commit")
    try:
        baked_revision = revision_file.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        raise OneShotAdjustedSealRefused(
            "the builder image has no trusted revision attestation"
        ) from None
    if baked_revision != tool_revision:
        raise OneShotAdjustedSealRefused("the builder image differs from the reviewed revision")


def _safe_settings(settings: Settings) -> Settings:
    if settings.app_env != "local":
        raise OneShotAdjustedSealRefused("APP_ENV must be exactly local")
    try:
        database_url = make_url(settings.database_url)
    except (ArgumentError, ValueError):
        raise OneShotAdjustedSealRefused("DATABASE_URL is invalid") from None
    target = (
        database_url.drivername,
        database_url.username,
        (database_url.host or "").lower(),
        database_url.port,
        database_url.database,
    )
    if (
        target
        != (
            "postgresql+asyncpg",
            "stockapi_snapshot_builder",
            "timescaledb",
            5432,
            "stockapi_test",
        )
        or not database_url.password
        or database_url.query
    ):
        raise OneShotAdjustedSealRefused(
            "DATABASE_URL must use stockapi_snapshot_builder on timescaledb/stockapi_test"
        )
    if settings.forecast_adjusted_close_resolution_policy_hash != ADJUSTED_RESOLUTION_POLICY_HASH:
        raise OneShotAdjustedSealRefused(
            "the adjusted resolution-policy hash is not pinned to this code"
        )
    if (
        settings.forecast_adjusted_close_trusted_availability_rule_set_hash
        != ADJUSTED_AVAILABILITY_RULE_SET_HASH
    ):
        raise OneShotAdjustedSealRefused(
            "the adjusted availability rule-set hash is not pinned to this code"
        )
    DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY.validate_configured_hashes(
        settings.forecast_adjusted_close_resolution_policy_hash,
        settings.forecast_adjusted_close_trusted_availability_rule_set_hash,
    )
    return settings


def _aware(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise OneShotAdjustedSealRefused(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("end must be YYYY-MM-DD") from None


def _parse_timestamp(value: str) -> datetime:
    try:
        return _aware(
            datetime.fromisoformat(value.replace("Z", "+00:00")),
            "factor_cutoff",
        )
    except (ValueError, OneShotAdjustedSealRefused):
        raise argparse.ArgumentTypeError(
            "factor-cutoff must be an aware ISO-8601 timestamp"
        ) from None


def _coverage_dates(end_session: date) -> tuple[date, ...]:
    calendar = xcals.get_calendar("XNYS")
    label = pd.Timestamp(end_session)
    if not calendar.is_session(label):
        raise OneShotAdjustedSealRefused("end must be an XNYS trading session")
    sessions = calendar.sessions_window(label, -REQUIRED_SESSIONS)
    dates = tuple(value.date() for value in sessions)
    if (
        len(dates) != REQUIRED_SESSIONS
        or not dates
        or dates[-1] != end_session
        or dates != tuple(sorted(set(dates)))
    ):
        raise OneShotAdjustedSealRefused("could not derive the exact 258-session XNYS window")
    return dates


def _next_session_close(end_session: date) -> datetime:
    calendar = xcals.get_calendar("XNYS")
    label = pd.Timestamp(end_session)
    if not calendar.is_session(label):
        raise OneShotAdjustedSealRefused("end must be an XNYS trading session")
    return _aware(
        calendar.session_close(calendar.next_session(label)).to_pydatetime(),
        "next XNYS session close",
    )


def _require_current_session(
    database_now: datetime,
    end_session: date,
    *,
    phase: str,
) -> None:
    current = _aware(database_now, f"{phase} database clock")
    if latest_completed_xnys_session(current) != end_session:
        raise OneShotAdjustedSealRefused(f"a newer XNYS session completed before the {phase}")
    if (_next_session_close(end_session) - current).total_seconds() < (ROLLOVER_GUARD_SECONDS):
        raise OneShotAdjustedSealRefused(
            f"too little time remains before session rollover at the {phase}"
        )


def _timestamp(value: datetime) -> str:
    utc = _aware(value, "result timestamp")
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T{utc.hour:02d}:"
        f"{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


async def _require_exact_factor_lineage(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    snapshot_id: str,
    snapshot_as_of: datetime,
    factor_set_id: str,
    factor_available_at: datetime,
) -> None:
    async with sessionmaker() as session:
        row = (
            await session.execute(
                select(ForecastInputSnapshot).where(
                    ForecastInputSnapshot.snapshot_id == snapshot_id
                )
            )
        ).scalar_one_or_none()
    if row is None:
        raise OneShotAdjustedSealRefused(
            "the adjusted snapshot is absent after the builder returned"
        )
    canonical = bytes(row.canonical_payload)
    try:
        payload = parse_snapshot_payload(canonical)
    except SnapshotValidationError:
        raise OneShotAdjustedSealRefused(
            "the persisted adjusted snapshot payload is invalid"
        ) from None
    factor_sources = tuple(
        source for source in payload.data_sources if source.name == "stockapi_adjustment_factors"
    )
    policy = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY
    if (
        canonical_snapshot_payload(payload) != canonical
        or snapshot_id_for_payload(canonical) != snapshot_id
        or payload.resolution_policy_hash != ADJUSTED_RESOLUTION_POLICY_HASH
        or payload.symbol != SYMBOL
        or payload.target != "adjusted_close"
        or payload.horizon_unit != "trading_day"
        or payload.series_basis != policy.series_basis
        or payload.as_of != snapshot_as_of
        or payload.availability.rule_set_hash != ADJUSTED_AVAILABILITY_RULE_SET_HASH
        or len(factor_sources) != 1
        or factor_sources[0].snapshot_id != factor_set_id
        or factor_sources[0].max_available_at != factor_available_at
        or factor_sources[0].fields != ("adjusted_close", "price_factor_f64")
    ):
        raise OneShotAdjustedSealRefused(
            "the persisted snapshot does not name the exact published factor set"
        )


async def seal_once(
    *,
    factor_cutoff: datetime,
    expected_factor_set_id: str,
    end_session: date,
    authorization: str,
    settings: Settings | None = None,
) -> dict[str, object]:
    """Build one factor artifact and one adjusted snapshot from local evidence."""

    if authorization != AUTHORIZATION_SENTINEL:
        raise OneShotAdjustedSealRefused(f"authorization must be exactly {AUTHORIZATION_SENTINEL}")
    if _CONTENT_ID_PATTERN.fullmatch(expected_factor_set_id) is None:
        raise OneShotAdjustedSealRefused(
            "expected_factor_set_id must be the exact planned sha256 identity"
        )
    safe_settings = _safe_settings(settings or get_settings())
    coverage_dates = _coverage_dates(end_session)
    planned_factor_cutoff = _aware(factor_cutoff, "factor_cutoff")
    _require_current_session(
        planned_factor_cutoff,
        end_session,
        phase="planned factor cutoff",
    )

    engine = build_engine(safe_settings)
    maker = build_sessionmaker(engine)
    try:
        preflight_database_now = _aware(
            await database_snapshot_cutoff(maker),
            "preflight database clock",
        )
        if planned_factor_cutoff > preflight_database_now:
            raise OneShotAdjustedSealRefused(
                "the planned factor cutoff is later than the preflight database clock"
            )
        _require_current_session(
            preflight_database_now,
            end_session,
            phase="preflight",
        )
        factor_builder = AdjustmentFactorBuilder(
            maker,
            SqlAdjustmentFactorSetStore(engine),
        )
        artifact = await factor_builder.prepare(
            AdjustmentFactorBuildSpec(
                symbol=SYMBOL,
                coverage_start=coverage_dates[0],
                coverage_end=coverage_dates[-1],
                cutoff=planned_factor_cutoff,
            )
        )
        if artifact.factor_set_id != expected_factor_set_id:
            raise OneShotAdjustedSealRefused(
                "prepared factor identity differs from the read-only plan"
            )
        factor_result = await factor_builder.publish(artifact)
        publication = factor_result.publication
        if (
            artifact.symbol != SYMBOL
            or artifact.cutoff != planned_factor_cutoff
            or artifact.anchor_date != end_session
            or len(artifact.raw_inputs) != REQUIRED_SESSIONS
            or artifact.raw_inputs[0].observation_date != coverage_dates[0]
            or artifact.raw_inputs[-1].observation_date != coverage_dates[-1]
            or publication.factor_set_id != artifact.factor_set_id
            or publication.input_count != REQUIRED_SESSIONS
            or _CONTENT_ID_PATTERN.fullmatch(artifact.factor_set_id) is None
        ):
            raise OneShotAdjustedSealRefused(
                "the factor builder escaped the exact one-shot evidence window"
            )

        database_now = _aware(
            await database_snapshot_cutoff(maker),
            "post-publication database clock",
        )
        if database_now <= preflight_database_now:
            raise OneShotAdjustedSealRefused(
                "the post-publication database clock did not advance past preflight"
            )
        _require_current_session(
            database_now,
            end_session,
            phase="post-publication visibility check",
        )
        factor_recorded_at = _aware(
            publication.factor_set_recorded_at,
            "factor recorded_at",
        )
        factor_available_at = _aware(
            publication.available_at,
            "factor available_at",
        )
        if not (
            planned_factor_cutoff <= factor_recorded_at <= factor_available_at <= database_now
            and factor_available_at > planned_factor_cutoff
        ):
            raise OneShotAdjustedSealRefused(
                "the factor publication is not visible at the later snapshot cutoff"
            )
        snapshot_as_of = factor_available_at
        _require_current_session(
            snapshot_as_of,
            end_session,
            phase="factor receipt cutoff",
        )

        snapshot_result = await AdjustedForecastSnapshotBuilder(maker).build(
            AdjustedSnapshotBuildSpec(
                symbol=SYMBOL,
                target="adjusted_close",
                horizon_unit="trading_day",
                as_of=snapshot_as_of,
            )
        )
        policy = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY
        snapshot_checked_at = _aware(
            snapshot_result.availability_checked_at,
            "snapshot availability_checked_at",
        )
        if (
            snapshot_result.as_of != snapshot_as_of
            or snapshot_checked_at < snapshot_as_of
            or not policy.minimum_observations
            <= snapshot_result.observation_count
            <= policy.observation_limit
            or snapshot_result.target_time_count != policy.target_time_count
            or _CONTENT_ID_PATTERN.fullmatch(snapshot_result.snapshot_id) is None
        ):
            raise OneShotAdjustedSealRefused(
                "the adjusted snapshot result escaped the pinned policy"
            )
        await _require_exact_factor_lineage(
            maker,
            snapshot_id=snapshot_result.snapshot_id,
            snapshot_as_of=snapshot_as_of,
            factor_set_id=artifact.factor_set_id,
            factor_available_at=factor_available_at,
        )

        return {
            "status": "ok",
            "symbol": SYMBOL,
            "end_session": end_session.isoformat(),
            "coverage_start": coverage_dates[0].isoformat(),
            "coverage_end": coverage_dates[-1].isoformat(),
            "factor_cutoff": _timestamp(planned_factor_cutoff),
            "factor_set_id": artifact.factor_set_id,
            "factor_set_recorded_at": _timestamp(factor_recorded_at),
            "factor_available_at": _timestamp(factor_available_at),
            "factor_input_count": publication.input_count,
            "snapshot_as_of": _timestamp(snapshot_as_of),
            "snapshot_id": snapshot_result.snapshot_id,
            "snapshot_status": "created" if snapshot_result.created else "replayed",
            "snapshot_availability_checked_at": _timestamp(snapshot_checked_at),
            "snapshot_observation_count": snapshot_result.observation_count,
            "snapshot_target_time_count": snapshot_result.target_time_count,
            "resolution_policy_hash": ADJUSTED_RESOLUTION_POLICY_HASH,
            "availability_rule_set_hash": ADJUSTED_AVAILABILITY_RULE_SET_HASH,
        }
    finally:
        await engine.dispose()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end", required=True, type=_parse_date)
    parser.add_argument("--factor-cutoff", required=True, type=_parse_timestamp)
    parser.add_argument("--expected-factor-set-id", required=True)
    parser.add_argument("--tool-revision", required=True)
    parser.add_argument("--authorization", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        _attest_build_revision(args.tool_revision)
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            result = asyncio.run(
                seal_once(
                    factor_cutoff=args.factor_cutoff,
                    expected_factor_set_id=args.expected_factor_set_id,
                    end_session=args.end,
                    authorization=args.authorization,
                )
            )
    except OneShotAdjustedSealRefused as exc:
        print(f"one-shot adjusted snapshot seal refused: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - never expose credential-bearing errors.
        print(
            f"one-shot adjusted snapshot seal failed: {type(exc).__name__}",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
