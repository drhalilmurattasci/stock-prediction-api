"""One-shot, builder-role-only MSFT snapshot seal for the local demo proof.

This is deliberately not a Celery worker: one invocation can create or replay
one semantic snapshot and then exits, so stale queue messages cannot widen the
write set. The surrounding host controller independently validates the row and
the authenticated API response.
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
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from app.config import Settings, get_settings
from app.db.session import build_engine, build_sessionmaker
from app.services.forecast_snapshot_builder import (
    DEFAULT_AVAILABILITY_RULE_SET_HASH,
    DEFAULT_RESOLUTION_POLICY_HASH,
    database_snapshot_cutoff,
)
from app.services.market_calendar import latest_completed_xnys_session
from ingestion.tasks.build_forecast_snapshots import build_forecast_snapshots_async

AUTHORIZATION_SENTINEL = "stockapi-msft-seal-serve-only"
SYMBOL = "MSFT"
ROLLOVER_GUARD_SECONDS = 600
BUILD_REVISION_FILE = Path("/app/.stockapi-build-revision")
_GIT_REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class OneShotSealRefused(RuntimeError):
    """The builder invocation escaped the exact reviewed local contract."""


def _attest_build_revision(tool_revision: str, revision_file: Path = BUILD_REVISION_FILE) -> None:
    if _GIT_REVISION_PATTERN.fullmatch(tool_revision) is None:
        raise OneShotSealRefused("tool_revision must identify one reviewed Git commit")
    try:
        baked_revision = revision_file.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        raise OneShotSealRefused("the builder image has no trusted revision attestation") from None
    if baked_revision != tool_revision:
        raise OneShotSealRefused("the builder image differs from the reviewed revision")


def _safe_settings(settings: Settings) -> Settings:
    if settings.app_env != "local":
        raise OneShotSealRefused("APP_ENV must be exactly local")
    try:
        database_url = make_url(settings.database_url)
    except (ArgumentError, ValueError):
        raise OneShotSealRefused("DATABASE_URL is invalid") from None
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
        raise OneShotSealRefused(
            "DATABASE_URL must use stockapi_snapshot_builder on timescaledb/stockapi_test"
        )
    if settings.forecast_resolution_policy_hash != DEFAULT_RESOLUTION_POLICY_HASH:
        raise OneShotSealRefused("the resolution-policy hash is not pinned to this code")
    if settings.forecast_trusted_availability_rule_set_hash != DEFAULT_AVAILABILITY_RULE_SET_HASH:
        raise OneShotSealRefused("the availability rule-set hash is not pinned to this code")
    return settings


def _aware(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise OneShotSealRefused(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_timestamp(value: str) -> datetime:
    try:
        return _aware(datetime.fromisoformat(value.replace("Z", "+00:00")), "as_of")
    except ValueError:
        raise argparse.ArgumentTypeError("as_of must be an aware ISO-8601 timestamp") from None


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("end must be YYYY-MM-DD") from None


def _next_session_close(end_session: date) -> datetime:
    calendar = xcals.get_calendar("XNYS")
    label = pd.Timestamp(end_session)
    if not calendar.is_session(label):
        raise OneShotSealRefused("end must be an XNYS trading session")
    return _aware(
        calendar.session_close(calendar.next_session(label)).to_pydatetime(),
        "next XNYS session close",
    )


async def seal_once(
    *,
    as_of: datetime,
    end_session: date,
    authorization: str,
    settings: Settings | None = None,
) -> dict[str, object]:
    if authorization != AUTHORIZATION_SENTINEL:
        raise OneShotSealRefused(f"authorization must be exactly {AUTHORIZATION_SENTINEL}")
    safe_settings = _safe_settings(settings or get_settings())
    cutoff = _aware(as_of, "as_of")
    if latest_completed_xnys_session(cutoff) != end_session:
        raise OneShotSealRefused("as_of is outside the exact authorized XNYS session")

    engine = build_engine(safe_settings)
    maker = build_sessionmaker(engine)
    try:
        database_now = _aware(await database_snapshot_cutoff(maker), "database clock")
        if cutoff > database_now:
            raise OneShotSealRefused("as_of is later than the database clock")
        if latest_completed_xnys_session(database_now) != end_session:
            raise OneShotSealRefused("a newer XNYS session has completed")
        if (
            _next_session_close(end_session) - database_now
        ).total_seconds() < ROLLOVER_GUARD_SECONDS:
            raise OneShotSealRefused("too little time remains before the next XNYS close")
        result = await build_forecast_snapshots_async(
            symbols=[SYMBOL],
            as_of=cutoff,
            settings=safe_settings,
            sessionmaker=maker,
            engine=engine,
        )
        return cast_result(result)
    finally:
        await engine.dispose()


def cast_result(value: dict[str, object]) -> dict[str, object]:
    """Keep the public one-shot boundary explicitly JSON-object shaped."""

    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True, type=_parse_timestamp)
    parser.add_argument("--end", required=True, type=_parse_date)
    parser.add_argument("--tool-revision", required=True)
    parser.add_argument("--authorization", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        _attest_build_revision(args.tool_revision)
        # Libraries may emit structured operational logs. Keep the machine
        # boundary to exactly one nonsecret JSON line on success and one
        # sanitized refusal on failure.
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            result = asyncio.run(
                seal_once(
                    as_of=args.as_of,
                    end_session=args.end,
                    authorization=args.authorization,
                )
            )
    except OneShotSealRefused as exc:
        print(f"one-shot snapshot seal refused: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - never expose credential-bearing errors.
        print(f"one-shot snapshot seal failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
