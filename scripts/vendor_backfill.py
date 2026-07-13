"""Fail-closed, resumable MSFT regular-session close backfill.

The ordinary forecast-close task is intentionally not used for the separately
authorized historical spend: it buffers a whole range before persistence and
Celery may renew an in-memory budget on retry. This operator lane plans the
exact final 258 XNYS sessions, calls only dates without a current post-commit
receipt, reserves every outbound attempt in a durable local ledger, and commits
each bar plus receipt before continuing. It never retries automatically.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast

import exchange_calendars as xcals
import pandas as pd
from sqlalchemy import and_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult, make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.models.bars import Bar, BarVersionAvailability
from app.db.session import build_engine, build_sessionmaker
from app.services.market_calendar import latest_completed_xnys_session
from data_sources.base import OHLCVBar
from data_sources.guards import AsyncPacingCostRateGuard
from ingestion.locks import vendor_operation_lock_id
from ingestion.tasks.ingest_prices import _advisory_xact_lock
from ingestion.upsert import BAR_VERSION_KEY, upsert_bars

AUTHORIZATION_SENTINEL = "stockapi-msft-backfill-only"
BACKFILL_SYMBOL = "MSFT"
BACKFILL_SOURCE = "polygon_open_close"
BACKFILL_TIMESPAN = "day"
BACKFILL_ADJUSTMENT_BASIS = "raw"
BACKFILL_MULTIPLIER = 1
REQUIRED_SESSIONS = 258
BACKFILL_MAX_CALLS_PER_WINDOW = 5
BACKFILL_RATE_WINDOW_SECONDS = 60.0
BACKFILL_LOCK_ID = vendor_operation_lock_id()
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER_PATH = REPO_ROOT / "data" / "vendor_backfill_attempts.jsonl"
_AUTHORIZATION_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")
_PLAN_ID_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_REVISION_PATTERN = re.compile(r"^[0-9a-f]{40,64}$")
_GIT_ROUTING_ENVIRONMENT = frozenset(
    {
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_WORK_TREE",
    }
)


class BackfillRefused(RuntimeError):
    """The requested operation is outside the reviewed backfill contract."""


class BackfillExecutionFailed(RuntimeError):
    """A fail-fast execution stopped after preserving prior checkpoints."""

    def __init__(self, result: dict[str, object]) -> None:
        super().__init__("backfill execution failed")
        self.result = result


@dataclass(frozen=True)
class ExistingCoverage:
    """Current rows split by exact post-commit receipt state."""

    complete_dates: tuple[date, ...]
    repairable_dates: tuple[date, ...]
    version_ids: tuple[tuple[date, str], ...]


@dataclass(frozen=True)
class BackfillPlan:
    """Content-addressed authorization scope for one database state."""

    end_session: date
    tool_revision: str
    expected_dates: tuple[date, ...]
    complete_dates: tuple[date, ...]
    repairable_dates: tuple[date, ...]
    missing_dates: tuple[date, ...]
    version_ids: tuple[tuple[date, str], ...]
    ambiguous_dates: tuple[date, ...]
    plan_id: str

    @property
    def smoke_anchor_present(self) -> bool:
        return self.end_session in self.complete_dates

    @property
    def required_outbound_attempts(self) -> int:
        return len(self.missing_dates)

    def public_result(self) -> dict[str, object]:
        if not self.smoke_anchor_present or self.ambiguous_dates:
            status = "blocked"
        elif self.missing_dates or self.repairable_dates:
            status = "ready"
        else:
            status = "complete"
        return {
            "status": status,
            "plan_id": self.plan_id,
            "tool_revision": self.tool_revision,
            "symbol": BACKFILL_SYMBOL,
            "source": BACKFILL_SOURCE,
            "window_start": self.expected_dates[0].isoformat(),
            "window_end": self.end_session.isoformat(),
            "required_sessions": len(self.expected_dates),
            "complete_sessions": len(self.complete_dates),
            "receipt_repairs_required": len(self.repairable_dates),
            "missing_sessions": len(self.missing_dates),
            "missing_sessions_sha256": _dates_digest(self.missing_dates),
            "required_outbound_attempts": self.required_outbound_attempts,
            "ambiguous_prior_attempts": len(self.ambiguous_dates),
            "max_calls_per_window": BACKFILL_MAX_CALLS_PER_WINDOW,
            "rate_window_seconds": BACKFILL_RATE_WINDOW_SECONDS,
            "smoke_anchor_present": self.smoke_anchor_present,
        }


class BackfillStore(Protocol):
    """Database seam used by the deterministic operator orchestration."""

    async def coverage(self, session_dates: tuple[date, ...]) -> ExistingCoverage: ...

    async def repair_receipts(self, session_dates: tuple[date, ...]) -> int: ...

    async def persist(self, bar: OHLCVBar) -> None: ...


class BackfillProvider(Protocol):
    name: str

    async def __aenter__(self) -> BackfillProvider: ...

    async def __aexit__(self, *exc_info: object) -> None: ...

    async def get_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjusted: bool = False,
    ) -> list[OHLCVBar]: ...


StoreFactory = Callable[[Settings], AbstractAsyncContextManager[BackfillStore]]
ProviderFactory = Callable[[Settings, AsyncPacingCostRateGuard], BackfillProvider]
LockFn = Callable[[Settings], AbstractAsyncContextManager[None]]
SessionsFn = Callable[[date], tuple[date, ...]]
Clock = Callable[[], datetime]
RevisionFn = Callable[[], str]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _calendar() -> Any:
    return xcals.get_calendar("XNYS")


def _expected_session_dates(end_session: date) -> tuple[date, ...]:
    calendar = _calendar()
    label = pd.Timestamp(end_session)
    if not calendar.is_session(label):
        raise BackfillRefused("end must be an XNYS trading session")
    sessions = calendar.sessions_window(label, -REQUIRED_SESSIONS)
    dates = tuple(value.date() for value in sessions)
    if len(dates) != REQUIRED_SESSIONS:
        raise BackfillRefused("could not derive the exact 258-session XNYS window")
    return dates


def _session_close(session_date: date) -> datetime:
    return _calendar().session_close(pd.Timestamp(session_date)).to_pydatetime().astimezone(UTC)


def _dates_digest(values: tuple[date, ...]) -> str:
    payload = "\n".join(value.isoformat() for value in values).encode("ascii")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _clean_git_revision() -> str:
    """Bind planning and execution to one reviewed, clean repository commit."""

    environment = os.environ.copy()
    for name in _GIT_ROUTING_ENVIRONMENT:
        environment.pop(name, None)
    try:
        repository = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=REPO_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        status = subprocess.run(
            [
                "git",
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--ignore-submodules=none",
            ],
            cwd=REPO_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        raise BackfillRefused("could not prove a clean Git revision") from None
    try:
        repository_root = Path(repository.stdout.strip()).resolve()
    except (OSError, ValueError):
        raise BackfillRefused("could not prove the repository root") from None
    if (
        repository.returncode != 0
        or repository.stderr.strip()
        or repository_root != REPO_ROOT.resolve()
    ):
        raise BackfillRefused("Git is not rooted at the reviewed repository")
    if status.returncode != 0 or status.stdout.strip() or status.stderr.strip():
        raise BackfillRefused("the backfill operator requires a clean Git worktree")
    value = revision.stdout.strip()
    if (
        revision.returncode != 0
        or revision.stderr.strip()
        or not _GIT_REVISION_PATTERN.fullmatch(value)
    ):
        raise BackfillRefused("could not identify the reviewed Git revision")
    return value


def _bar_version_id(bar: Bar, available_at: datetime | None) -> str:
    canonical = {
        "symbol": bar.symbol,
        "timespan": bar.timespan,
        "multiplier": bar.multiplier,
        "ts": bar.ts.astimezone(UTC).isoformat(),
        "source": bar.source,
        "adjustment_basis": bar.adjustment_basis,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "vwap": bar.vwap,
        "trade_count": bar.trade_count,
        "fetched_at": bar.fetched_at.astimezone(UTC).isoformat(),
        "as_of": bar.as_of.astimezone(UTC).isoformat(),
        "recorded_at": bar.recorded_at.astimezone(UTC).isoformat(),
        "available_at": available_at.astimezone(UTC).isoformat() if available_at else None,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _build_plan(
    end_session: date,
    expected_dates: tuple[date, ...],
    coverage: ExistingCoverage,
    *,
    tool_revision: str,
    ambiguous_dates: tuple[date, ...] = (),
) -> BackfillPlan:
    if not _GIT_REVISION_PATTERN.fullmatch(tool_revision):
        raise BackfillRefused("tool_revision must identify one Git commit")
    expected = set(expected_dates)
    complete = set(coverage.complete_dates)
    repairable = set(coverage.repairable_dates)
    if complete.intersection(repairable):
        raise BackfillRefused("database coverage classified one session twice")
    if not complete.issubset(expected) or not repairable.issubset(expected):
        raise BackfillRefused("database coverage escaped the authorized session window")
    versions = dict(coverage.version_ids)
    if set(versions) != complete.union(repairable):
        raise BackfillRefused("database coverage omitted a current bar version identity")
    missing = expected.difference(complete, repairable)
    ambiguous = set(ambiguous_dates).intersection(missing)
    canonical = {
        "version": 2,
        "tool_revision": tool_revision,
        "symbol": BACKFILL_SYMBOL,
        "source": BACKFILL_SOURCE,
        "end_session": end_session.isoformat(),
        "expected_dates": [value.isoformat() for value in expected_dates],
        "complete_dates": [value.isoformat() for value in sorted(complete)],
        "repairable_dates": [value.isoformat() for value in sorted(repairable)],
        "missing_dates": [value.isoformat() for value in sorted(missing)],
        "version_ids": [[value.isoformat(), versions[value]] for value in sorted(versions)],
        "ambiguous_dates": [value.isoformat() for value in sorted(ambiguous)],
        "max_calls_per_window": BACKFILL_MAX_CALLS_PER_WINDOW,
        "rate_window_seconds": BACKFILL_RATE_WINDOW_SECONDS,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return BackfillPlan(
        end_session=end_session,
        tool_revision=tool_revision,
        expected_dates=expected_dates,
        complete_dates=tuple(sorted(complete)),
        repairable_dates=tuple(sorted(repairable)),
        missing_dates=tuple(sorted(missing)),
        version_ids=tuple(sorted(versions.items())),
        ambiguous_dates=tuple(sorted(ambiguous)),
        plan_id="sha256:" + hashlib.sha256(encoded).hexdigest(),
    )


def _safe_settings(settings: Settings, *, require_key: bool) -> Settings:
    if settings.app_env != "local":
        raise BackfillRefused("APP_ENV must be exactly local")
    try:
        database_url = make_url(settings.database_url)
    except (ArgumentError, ValueError):
        raise BackfillRefused("DATABASE_URL is not a valid SQLAlchemy URL") from None
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
        raise BackfillRefused("DATABASE_URL must use stockapi_app on local stockapi_test:5432")

    key = settings.polygon_api_key.strip() if settings.polygon_api_key else ""
    if require_key and not key:
        raise BackfillRefused("POLYGON_API_KEY must be non-empty in .env")
    if key and (not key.isascii() or any(not 0x21 <= ord(character) <= 0x7E for character in key)):
        raise BackfillRefused("POLYGON_API_KEY must contain only visible ASCII characters")
    return settings.model_copy(update={"polygon_api_key": key or None})


def _validate_current_end(end_session: date, now: datetime) -> None:
    if now.tzinfo is None or now.utcoffset() is None:
        raise BackfillRefused("clock must be timezone-aware")
    latest = latest_completed_xnys_session(now)
    if end_session != latest:
        raise BackfillRefused(
            f"end must equal the latest completed XNYS session ({latest.isoformat()})"
        )


class SqlBackfillStore:
    """Runtime-role store that proves current rows and exact-version receipts."""

    def __init__(self, settings: Settings) -> None:
        self._engine: AsyncEngine = build_engine(settings)
        self._maker: async_sessionmaker[Any] = build_sessionmaker(self._engine)

    async def __aenter__(self) -> SqlBackfillStore:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._engine.dispose()

    async def coverage(self, session_dates: tuple[date, ...]) -> ExistingCoverage:
        if not session_dates:
            return ExistingCoverage(complete_dates=(), repairable_dates=(), version_ids=())
        close_to_date = {_session_close(value): value for value in session_dates}
        receipt_match = and_(
            BarVersionAvailability.symbol == Bar.symbol,
            BarVersionAvailability.timespan == Bar.timespan,
            BarVersionAvailability.multiplier == Bar.multiplier,
            BarVersionAvailability.ts == Bar.ts,
            BarVersionAvailability.source == Bar.source,
            BarVersionAvailability.adjustment_basis == Bar.adjustment_basis,
            BarVersionAvailability.version_recorded_at == Bar.recorded_at,
            BarVersionAvailability.available_at >= Bar.recorded_at,
        )
        lower = datetime.combine(session_dates[0], datetime.min.time(), tzinfo=UTC)
        upper = datetime.combine(
            session_dates[-1] + timedelta(days=1),
            datetime.min.time(),
            tzinfo=UTC,
        )
        statement = (
            select(Bar, BarVersionAvailability.available_at)
            .outerjoin(BarVersionAvailability, receipt_match)
            .where(
                Bar.symbol == BACKFILL_SYMBOL,
                Bar.timespan == BACKFILL_TIMESPAN,
                Bar.multiplier == BACKFILL_MULTIPLIER,
                Bar.source == BACKFILL_SOURCE,
                Bar.adjustment_basis == BACKFILL_ADJUSTMENT_BASIS,
                Bar.ts >= lower,
                Bar.ts < upper,
            )
        )
        async with self._maker() as session:
            rows = (await session.execute(statement)).all()
        complete: list[date] = []
        repairable: list[date] = []
        version_ids: list[tuple[date, str]] = []
        for bar, available_at in rows:
            session_date = close_to_date.get(bar.ts.astimezone(UTC))
            if session_date is None:
                raise BackfillRefused("noncanonical polygon_open_close timestamp in plan window")
            (complete if available_at is not None else repairable).append(session_date)
            version_ids.append((session_date, _bar_version_id(bar, available_at)))
        return ExistingCoverage(
            complete_dates=tuple(sorted(complete)),
            repairable_dates=tuple(sorted(repairable)),
            version_ids=tuple(sorted(version_ids)),
        )

    async def repair_receipts(self, session_dates: tuple[date, ...]) -> int:
        if not session_dates:
            return 0
        closes = tuple(_session_close(value) for value in session_dates)
        statement = select(Bar).where(
            Bar.symbol == BACKFILL_SYMBOL,
            Bar.timespan == BACKFILL_TIMESPAN,
            Bar.multiplier == BACKFILL_MULTIPLIER,
            Bar.source == BACKFILL_SOURCE,
            Bar.adjustment_basis == BACKFILL_ADJUSTMENT_BASIS,
            Bar.ts.in_(closes),
        )
        async with self._maker() as session, session.begin():
            await _advisory_xact_lock(
                session,
                BACKFILL_SYMBOL,
                BACKFILL_SOURCE,
                BACKFILL_TIMESPAN,
            )
            bars = (await session.execute(statement)).scalars().all()
            if len(bars) != len(session_dates):
                raise BackfillRefused("a repairable bar disappeared before receipt repair")
            raw_result = await session.execute(_exact_receipt_insert_statement(closes))
            return cast(CursorResult[Any], raw_result).rowcount

    async def persist(self, bar: OHLCVBar) -> None:
        async with self._maker() as session, session.begin():
            await _advisory_xact_lock(
                session,
                BACKFILL_SYMBOL,
                BACKFILL_SOURCE,
                BACKFILL_TIMESPAN,
            )
            plan = await upsert_bars(session, [bar])
            if len(plan.rows) != 1 or plan.revisions:
                raise BackfillRefused("one missing session did not persist as one new bar")
        async with self._maker() as session, session.begin():
            await _advisory_xact_lock(
                session,
                BACKFILL_SYMBOL,
                BACKFILL_SOURCE,
                BACKFILL_TIMESPAN,
            )
            await session.execute(_exact_receipt_insert_statement((bar.timestamp,)))


def _exact_receipt_insert_statement(closes: tuple[datetime, ...]) -> Any:
    """Insert receipts only for the requested committed current bar versions."""

    if not closes:
        raise BackfillRefused("at least one exact receipt timestamp is required")
    current_versions = select(
        Bar.symbol,
        Bar.timespan,
        Bar.multiplier,
        Bar.ts,
        Bar.source,
        Bar.adjustment_basis,
        Bar.recorded_at.label("version_recorded_at"),
    ).where(
        Bar.symbol == BACKFILL_SYMBOL,
        Bar.timespan == BACKFILL_TIMESPAN,
        Bar.multiplier == BACKFILL_MULTIPLIER,
        Bar.source == BACKFILL_SOURCE,
        Bar.adjustment_basis == BACKFILL_ADJUSTMENT_BASIS,
        Bar.ts.in_(closes),
    )
    return (
        insert(BarVersionAvailability)
        .from_select(BAR_VERSION_KEY, current_versions)
        .on_conflict_do_nothing()
    )


@asynccontextmanager
async def _sql_store(settings: Settings) -> AsyncIterator[BackfillStore]:
    async with SqlBackfillStore(settings) as store:
        yield store


@asynccontextmanager
async def _exclusive_backfill(settings: Settings) -> AsyncIterator[None]:
    engine = build_engine(settings)
    try:
        async with engine.connect() as connection:
            acquired = bool(
                (
                    await connection.execute(
                        text("SELECT pg_try_advisory_lock(:lock_id)"),
                        {"lock_id": BACKFILL_LOCK_ID},
                    )
                ).scalar_one()
            )
            await connection.commit()
            if not acquired:
                raise BackfillRefused("another vendor backfill is already running")
            try:
                yield
            finally:
                await connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_id)"),
                    {"lock_id": BACKFILL_LOCK_ID},
                )
                await connection.commit()
    finally:
        await engine.dispose()


class AttemptLedger:
    """Append-only local reservation ledger; reservations precede HTTP sends."""

    def __init__(self, path: Path = DEFAULT_LEDGER_PATH, *, clock: Clock = _utcnow) -> None:
        self.path = path
        self.clock = clock

    def begin_authorization(
        self,
        *,
        authorization_id: str,
        plan: BackfillPlan,
        max_calls: int,
    ) -> None:
        _validate_authorization_id(authorization_id)
        if any(record.get("authorization_id") == authorization_id for record in self._records()):
            raise BackfillRefused("authorization_id is already consumed; obtain a fresh grant")
        self._append(
            {
                "record_type": "authorization",
                "authorization_id": authorization_id,
                "plan_id": plan.plan_id,
                "tool_revision": plan.tool_revision,
                "symbol": BACKFILL_SYMBOL,
                "end_session": plan.end_session.isoformat(),
                "max_calls": max_calls,
                "recorded_at": _aware_iso(self.clock()),
            }
        )

    def reserve_attempt(
        self,
        *,
        authorization_id: str,
        plan_id: str,
        session_date: date,
    ) -> int:
        records = self._records()
        headers = [
            record
            for record in records
            if record.get("record_type") == "authorization"
            and record.get("authorization_id") == authorization_id
        ]
        if len(headers) != 1:
            raise BackfillRefused("authorization ledger header is missing or ambiguous")
        header = headers[0]
        if header.get("plan_id") != plan_id:
            raise BackfillRefused("authorization ledger plan does not match execution")
        attempts = [
            record
            for record in records
            if record.get("record_type") == "attempt"
            and record.get("authorization_id") == authorization_id
        ]
        if any(record.get("session_date") == session_date.isoformat() for record in attempts):
            raise BackfillRefused("this authorization already reserved the session attempt")
        max_calls = header.get("max_calls")
        if not isinstance(max_calls, int) or len(attempts) >= max_calls:
            raise BackfillRefused("authorization call budget is exhausted")
        attempt_number = len(attempts) + 1
        self._append(
            {
                "record_type": "attempt",
                "authorization_id": authorization_id,
                "plan_id": plan_id,
                "attempt_number": attempt_number,
                "session_date": session_date.isoformat(),
                "reserved_at": _aware_iso(self.clock()),
            }
        )
        return attempt_number

    def attempt_count(self, authorization_id: str) -> int:
        return sum(
            record.get("record_type") == "attempt"
            and record.get("authorization_id") == authorization_id
            for record in self._records()
        )

    def finish_attempt(
        self,
        *,
        authorization_id: str,
        plan_id: str,
        session_date: date,
        status: str,
        failure_type: str | None = None,
    ) -> None:
        if status not in {"checkpointed", "failed"}:
            raise BackfillRefused("attempt outcome status is invalid")
        records = self._records()
        matching_attempts = [
            record
            for record in records
            if record.get("record_type") == "attempt"
            and record.get("authorization_id") == authorization_id
            and record.get("plan_id") == plan_id
            and record.get("session_date") == session_date.isoformat()
        ]
        matching_outcomes = [
            record
            for record in records
            if record.get("record_type") == "outcome"
            and record.get("authorization_id") == authorization_id
            and record.get("plan_id") == plan_id
            and record.get("session_date") == session_date.isoformat()
        ]
        if len(matching_attempts) != 1 or matching_outcomes:
            raise BackfillRefused("attempt outcome does not identify one open reservation")
        self._append(
            {
                "record_type": "outcome",
                "authorization_id": authorization_id,
                "plan_id": plan_id,
                "session_date": session_date.isoformat(),
                "status": status,
                "failure_type": failure_type,
                "recorded_at": _aware_iso(self.clock()),
            }
        )

    def unresolved_dates(self) -> tuple[date, ...]:
        records = self._records()
        attempts = {
            (
                str(record.get("authorization_id")),
                str(record.get("plan_id")),
                str(record.get("session_date")),
            )
            for record in records
            if record.get("record_type") == "attempt"
        }
        outcomes = {
            (
                str(record.get("authorization_id")),
                str(record.get("plan_id")),
                str(record.get("session_date")),
            )
            for record in records
            if record.get("record_type") == "outcome"
        }
        unresolved: list[date] = []
        try:
            for _, _, session_date in sorted(attempts.difference(outcomes)):
                unresolved.append(date.fromisoformat(session_date))
        except ValueError as exc:
            raise BackfillRefused("attempt ledger contains an invalid session date") from exc
        return tuple(sorted(set(unresolved)))

    def _records(self) -> list[dict[str, object]]:
        if not self.path.exists():
            return []
        records: list[dict[str, object]] = []
        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
            for line_number, line in enumerate(lines, start=1):
                if not line:
                    raise ValueError(f"blank line {line_number}")
                decoded = json.loads(line)
                if not isinstance(decoded, dict):
                    raise ValueError(f"non-object line {line_number}")
                record_type = decoded.get("record_type")
                if not isinstance(record_type, str) or record_type not in {
                    "authorization",
                    "attempt",
                    "outcome",
                }:
                    raise ValueError(f"unknown record type on line {line_number}")
                records.append(decoded)
            self._validate_records(records)
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise BackfillRefused("attempt ledger is unreadable; stop for forensics") from exc
        return records

    @staticmethod
    def _validate_records(records: list[dict[str, object]]) -> None:
        schemas = {
            "authorization": {
                "record_type",
                "authorization_id",
                "plan_id",
                "tool_revision",
                "symbol",
                "end_session",
                "max_calls",
                "recorded_at",
            },
            "attempt": {
                "record_type",
                "authorization_id",
                "plan_id",
                "attempt_number",
                "session_date",
                "reserved_at",
            },
            "outcome": {
                "record_type",
                "authorization_id",
                "plan_id",
                "session_date",
                "status",
                "failure_type",
                "recorded_at",
            },
        }
        headers: dict[str, dict[str, object]] = {}
        attempts: dict[tuple[str, str], dict[str, object]] = {}
        outcomes: set[tuple[str, str]] = set()
        attempt_counts: dict[str, int] = {}

        for record in records:
            record_type = record["record_type"]
            if not isinstance(record_type, str) or set(record) != schemas[record_type]:
                raise ValueError("ledger record schema mismatch")
            authorization_id = record["authorization_id"]
            plan_id = record["plan_id"]
            if not isinstance(authorization_id, str) or not _AUTHORIZATION_ID_PATTERN.fullmatch(
                authorization_id
            ):
                raise ValueError("invalid ledger authorization id")
            if not isinstance(plan_id, str) or not _PLAN_ID_PATTERN.fullmatch(plan_id):
                raise ValueError("invalid ledger plan id")

            if record_type == "authorization":
                if authorization_id in headers:
                    raise ValueError("duplicate ledger authorization")
                tool_revision = record["tool_revision"]
                max_calls = record["max_calls"]
                if (
                    not isinstance(tool_revision, str)
                    or not _GIT_REVISION_PATTERN.fullmatch(tool_revision)
                    or record["symbol"] != BACKFILL_SYMBOL
                    or isinstance(max_calls, bool)
                    or not isinstance(max_calls, int)
                    or not 1 <= max_calls <= REQUIRED_SESSIONS
                ):
                    raise ValueError("invalid ledger authorization header")
                _parse_ledger_date(record["end_session"])
                _parse_ledger_timestamp(record["recorded_at"])
                headers[authorization_id] = record
                attempt_counts[authorization_id] = 0
                continue

            header = headers.get(authorization_id)
            if header is None or header["plan_id"] != plan_id:
                raise ValueError("ledger child precedes or mismatches its authorization")
            session_date = _parse_ledger_date(record["session_date"])
            if session_date > _parse_ledger_date(header["end_session"]):
                raise ValueError("ledger attempt is after its authorized end session")
            key = (authorization_id, session_date.isoformat())

            if record_type == "attempt":
                attempt_number = record["attempt_number"]
                expected_number = attempt_counts[authorization_id] + 1
                if (
                    key in attempts
                    or isinstance(attempt_number, bool)
                    or not isinstance(attempt_number, int)
                    or attempt_number != expected_number
                    or attempt_number > cast(int, header["max_calls"])
                ):
                    raise ValueError("invalid or duplicate ledger attempt")
                _parse_ledger_timestamp(record["reserved_at"])
                attempts[key] = record
                attempt_counts[authorization_id] = expected_number
                continue

            status = record["status"]
            failure_type = record["failure_type"]
            if key not in attempts or key in outcomes or status not in {"checkpointed", "failed"}:
                raise ValueError("invalid or duplicate ledger outcome")
            if (status == "checkpointed" and failure_type is not None) or (
                status == "failed"
                and (
                    not isinstance(failure_type, str)
                    or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,127}", failure_type)
                )
            ):
                raise ValueError("invalid ledger outcome detail")
            _parse_ledger_timestamp(record["recorded_at"])
            outcomes.add(key)

    def _append(self, record: dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
        ).encode("utf-8")
        descriptor = os.open(
            self.path,
            os.O_APPEND | os.O_CREAT | os.O_WRONLY,
            0o600,
        )
        try:
            offset = 0
            while offset < len(payload):
                offset += os.write(descriptor, payload[offset:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _aware_iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise BackfillRefused("ledger clock must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _parse_ledger_date(value: object) -> date:
    if not isinstance(value, str):
        raise ValueError("ledger date must be text")
    return date.fromisoformat(value)


def _parse_ledger_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("ledger timestamp must be text")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("ledger timestamp must be timezone-aware")
    return parsed


def _validate_authorization_id(value: str) -> None:
    if not _AUTHORIZATION_ID_PATTERN.fullmatch(value):
        raise BackfillRefused("authorization_id must be 3-64 lowercase safe characters")


def _default_provider(
    settings: Settings,
    guard: AsyncPacingCostRateGuard,
) -> BackfillProvider:
    # Keep provider code and vendor SDKs out of read-only planning and the
    # separate no-vendor forecast-demo controller.
    from data_sources.polygon_open_close import PolygonOpenCloseProvider

    key = settings.polygon_api_key or ""
    if not key:
        raise BackfillRefused("POLYGON_API_KEY must be non-empty in .env")
    return PolygonOpenCloseProvider(
        key,
        guard=guard,
        max_attempts=1,
    )


async def plan_backfill(
    *,
    end_session: date,
    settings: Settings | None = None,
    clock: Clock = _utcnow,
    store_factory: StoreFactory = _sql_store,
    sessions_fn: SessionsFn = _expected_session_dates,
    revision_fn: RevisionFn = _clean_git_revision,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
) -> BackfillPlan:
    safe_settings = _safe_settings(settings or get_settings(), require_key=False)
    tool_revision = revision_fn()
    _validate_current_end(end_session, clock())
    expected_dates = sessions_fn(end_session)
    ledger = AttemptLedger(ledger_path, clock=clock)
    async with store_factory(safe_settings) as store:
        coverage = await store.coverage(expected_dates)
    return _build_plan(
        end_session,
        expected_dates,
        coverage,
        tool_revision=tool_revision,
        ambiguous_dates=ledger.unresolved_dates(),
    )


async def repair_backfill(
    *,
    end_session: date,
    plan_id: str,
    settings: Settings | None = None,
    clock: Clock = _utcnow,
    store_factory: StoreFactory = _sql_store,
    lock_fn: LockFn = _exclusive_backfill,
    sessions_fn: SessionsFn = _expected_session_dates,
    revision_fn: RevisionFn = _clean_git_revision,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
) -> dict[str, object]:
    """Repair committed bars lacking receipts without any vendor credential or call."""

    if not _PLAN_ID_PATTERN.fullmatch(plan_id):
        raise BackfillRefused("plan_id must be a sha256 digest from the plan command")
    safe_settings = _safe_settings(settings or get_settings(), require_key=False)
    tool_revision = revision_fn()
    _validate_current_end(end_session, clock())
    expected_dates = sessions_fn(end_session)
    ledger = AttemptLedger(ledger_path, clock=clock)
    async with lock_fn(safe_settings), store_factory(safe_settings) as store:
        plan = _build_plan(
            end_session,
            expected_dates,
            await store.coverage(expected_dates),
            tool_revision=tool_revision,
            ambiguous_dates=ledger.unresolved_dates(),
        )
        if plan.plan_id != plan_id:
            raise BackfillRefused("database state no longer matches the repair plan_id")
        if not plan.repairable_dates:
            raise BackfillRefused("no receipt-only repairs are required")
        inserted = await store.repair_receipts(plan.repairable_dates)
        repaired_coverage = await store.coverage(expected_dates)
        if any(value not in repaired_coverage.complete_dates for value in plan.repairable_dates):
            raise BackfillRefused("receipt-only repair postflight is incomplete")
        final_plan = _build_plan(
            end_session,
            expected_dates,
            repaired_coverage,
            tool_revision=tool_revision,
            ambiguous_dates=ledger.unresolved_dates(),
        )
    return {
        "status": "ok",
        "original_plan_id": plan.plan_id,
        "new_plan_id": final_plan.plan_id,
        "symbol": BACKFILL_SYMBOL,
        "receipt_rows_inserted": inserted,
        "sessions_repaired": len(plan.repairable_dates),
        "remaining_missing_sessions": len(final_plan.missing_dates),
        "ambiguous_prior_attempts": len(final_plan.ambiguous_dates),
        "outbound_attempts": 0,
    }


async def execute_backfill(
    *,
    end_session: date,
    plan_id: str,
    max_calls: int,
    authorization: str,
    authorization_id: str,
    settings: Settings | None = None,
    clock: Clock = _utcnow,
    store_factory: StoreFactory = _sql_store,
    provider_factory: ProviderFactory = _default_provider,
    lock_fn: LockFn = _exclusive_backfill,
    sessions_fn: SessionsFn = _expected_session_dates,
    revision_fn: RevisionFn = _clean_git_revision,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
) -> dict[str, object]:
    """Execute exactly one freshly authorized, fail-fast missing-session plan."""

    if authorization != AUTHORIZATION_SENTINEL:
        raise BackfillRefused(f"authorization must be exactly {AUTHORIZATION_SENTINEL}")
    _validate_authorization_id(authorization_id)
    if not _PLAN_ID_PATTERN.fullmatch(plan_id):
        raise BackfillRefused("plan_id must be a sha256 digest from the plan command")
    if max_calls < 1 or max_calls > REQUIRED_SESSIONS:
        raise BackfillRefused("max_calls must be between 1 and 258")
    safe_settings = _safe_settings(settings or get_settings(), require_key=True)
    tool_revision = revision_fn()
    now = clock()
    _validate_current_end(end_session, now)
    expected_dates = sessions_fn(end_session)
    ledger = AttemptLedger(ledger_path, clock=clock)

    async with lock_fn(safe_settings), store_factory(safe_settings) as store:
        plan = _build_plan(
            end_session,
            expected_dates,
            await store.coverage(expected_dates),
            tool_revision=tool_revision,
            ambiguous_dates=ledger.unresolved_dates(),
        )
        if plan.plan_id != plan_id:
            raise BackfillRefused("database state no longer matches the authorized plan_id")
        if not plan.smoke_anchor_present:
            raise BackfillRefused("latest-session smoke bar and receipt must exist first")
        if plan.ambiguous_dates:
            raise BackfillRefused(
                "an unresolved prior attempt overlaps missing data; stop for forensics"
            )
        if not plan.missing_dates:
            raise BackfillRefused("no outbound vendor calls are required")
        if max_calls != plan.required_outbound_attempts:
            raise BackfillRefused(
                "max_calls must equal the plan's exact missing-session attempt count"
            )

        ledger.begin_authorization(
            authorization_id=authorization_id,
            plan=plan,
            max_calls=max_calls,
        )
        repaired = await store.repair_receipts(plan.repairable_dates)
        after_repair = await store.coverage(expected_dates)
        if any(value not in after_repair.complete_dates for value in plan.repairable_dates):
            raise BackfillExecutionFailed(
                _failure_result(
                    plan=plan,
                    authorization_id=authorization_id,
                    max_calls=max_calls,
                    ledger=ledger,
                    guard=None,
                    persisted=0,
                    remaining=len(plan.missing_dates),
                    failure_type="ReceiptRepairFailed",
                )
            )

        guard = AsyncPacingCostRateGuard(
            max_calls_per_window=BACKFILL_MAX_CALLS_PER_WINDOW,
            window_seconds=BACKFILL_RATE_WINDOW_SECONDS,
            total_budget=max_calls,
            admission_check=lambda: _validate_current_end(end_session, clock()),
        )
        persisted = 0
        try:
            provider = provider_factory(safe_settings, guard)
            if provider.name != BACKFILL_SOURCE:
                raise BackfillRefused("provider source is outside the backfill contract")
            async with provider:
                for session_date in plan.missing_dates:
                    current = await store.coverage((session_date,))
                    if session_date in current.complete_dates:
                        continue
                    if session_date in current.repairable_dates:
                        await store.repair_receipts((session_date,))
                        repaired += 1
                        repaired_state = await store.coverage((session_date,))
                        if session_date not in repaired_state.complete_dates:
                            raise BackfillRefused("concurrent receipt repair did not complete")
                        continue
                    ledger.reserve_attempt(
                        authorization_id=authorization_id,
                        plan_id=plan.plan_id,
                        session_date=session_date,
                    )
                    try:
                        bars = await provider.get_daily_bars(
                            BACKFILL_SYMBOL,
                            session_date,
                            session_date,
                            adjusted=False,
                        )
                        bar = _validate_one_session_bar(session_date, bars)
                        await store.persist(bar)
                        checkpoint = await store.coverage((session_date,))
                        if session_date not in checkpoint.complete_dates:
                            raise BackfillRefused(
                                "bar checkpoint lacks its exact post-commit receipt"
                            )
                    except Exception as exc:
                        ledger.finish_attempt(
                            authorization_id=authorization_id,
                            plan_id=plan.plan_id,
                            session_date=session_date,
                            status="failed",
                            failure_type=type(exc).__name__,
                        )
                        raise
                    ledger.finish_attempt(
                        authorization_id=authorization_id,
                        plan_id=plan.plan_id,
                        session_date=session_date,
                        status="checkpointed",
                    )
                    persisted += 1
        except Exception as exc:
            remaining = await _remaining_count(store, expected_dates, plan)
            raise BackfillExecutionFailed(
                _failure_result(
                    plan=plan,
                    authorization_id=authorization_id,
                    max_calls=max_calls,
                    ledger=ledger,
                    guard=guard,
                    persisted=persisted,
                    remaining=remaining,
                    failure_type=type(exc).__name__,
                )
            ) from None

        postflight = await store.coverage(expected_dates)
        final_plan = _build_plan(
            end_session,
            expected_dates,
            postflight,
            tool_revision=tool_revision,
        )
        if final_plan.missing_dates or final_plan.repairable_dates:
            raise BackfillExecutionFailed(
                _failure_result(
                    plan=plan,
                    authorization_id=authorization_id,
                    max_calls=max_calls,
                    ledger=ledger,
                    guard=guard,
                    persisted=persisted,
                    remaining=(len(final_plan.missing_dates) + len(final_plan.repairable_dates)),
                    failure_type="PostflightIncomplete",
                )
            )
        attempts_reserved = ledger.attempt_count(authorization_id)
        attempts_spent = guard.snapshot(BACKFILL_SOURCE)["spent"]
        if attempts_spent != attempts_reserved:
            raise BackfillExecutionFailed(
                _failure_result(
                    plan=plan,
                    authorization_id=authorization_id,
                    max_calls=max_calls,
                    ledger=ledger,
                    guard=guard,
                    persisted=persisted,
                    remaining=0,
                    failure_type="AttemptAccountingMismatch",
                )
            )
        return {
            "status": "ok",
            "plan_id": plan.plan_id,
            "tool_revision": plan.tool_revision,
            "authorization_id": authorization_id,
            "symbol": BACKFILL_SYMBOL,
            "window_start": plan.expected_dates[0].isoformat(),
            "window_end": plan.end_session.isoformat(),
            "required_sessions": len(plan.expected_dates),
            "receipt_repairs": repaired,
            "sessions_persisted": persisted,
            "authorized_max_calls": max_calls,
            "attempts_reserved": attempts_reserved,
            "attempts_spent": attempts_spent,
            "remaining_sessions": 0,
        }


async def _remaining_count(
    store: BackfillStore,
    expected_dates: tuple[date, ...],
    original_plan: BackfillPlan,
) -> int:
    try:
        current = _build_plan(
            original_plan.end_session,
            expected_dates,
            await store.coverage(expected_dates),
            tool_revision=original_plan.tool_revision,
        )
    except Exception:
        return len(original_plan.missing_dates)
    return len(current.missing_dates) + len(current.repairable_dates)


def _failure_result(
    *,
    plan: BackfillPlan,
    authorization_id: str,
    max_calls: int,
    ledger: AttemptLedger,
    guard: AsyncPacingCostRateGuard | None,
    persisted: int,
    remaining: int,
    failure_type: str,
) -> dict[str, object]:
    return {
        "status": "failed",
        "plan_id": plan.plan_id,
        "tool_revision": plan.tool_revision,
        "authorization_id": authorization_id,
        "symbol": BACKFILL_SYMBOL,
        "authorized_max_calls": max_calls,
        "attempts_reserved": ledger.attempt_count(authorization_id),
        "attempts_spent": (guard.snapshot(BACKFILL_SOURCE)["spent"] if guard is not None else 0),
        "sessions_persisted": persisted,
        "remaining_sessions": remaining,
        "failure_type": failure_type,
    }


def _validate_one_session_bar(
    session_date: date,
    bars: Sequence[OHLCVBar],
) -> OHLCVBar:
    if len(bars) != 1:
        raise BackfillRefused("one session request must return exactly one bar")
    bar = bars[0]
    expected_close = _session_close(session_date)
    if not (
        bar.symbol == BACKFILL_SYMBOL
        and bar.timestamp.astimezone(UTC) == expected_close
        and bar.timespan == BACKFILL_TIMESPAN
        and bar.multiplier == BACKFILL_MULTIPLIER
        and bar.source == BACKFILL_SOURCE
        and bar.adjustment_basis == BACKFILL_ADJUSTMENT_BASIS
    ):
        raise BackfillRefused("provider bar escaped the exact MSFT raw-close session scope")
    return bar


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="read-only exact missing-session plan")
    plan.add_argument("--end", required=True, type=_iso_date)
    repair = subparsers.add_parser("repair", help="repair receipts without vendor calls")
    repair.add_argument("--end", required=True, type=_iso_date)
    repair.add_argument("--plan-id", required=True)
    execute = subparsers.add_parser("execute", help="run one separately authorized plan")
    execute.add_argument("--end", required=True, type=_iso_date)
    execute.add_argument("--plan-id", required=True)
    execute.add_argument("--max-calls", required=True, type=int)
    execute.add_argument("--authorization", required=True)
    execute.add_argument("--authorization-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging("INFO", json_logs=False, exception_details=False)
    try:
        if args.command == "plan":
            result: dict[str, object] = asyncio.run(
                plan_backfill(end_session=args.end)
            ).public_result()
        elif args.command == "repair":
            result = asyncio.run(
                repair_backfill(
                    end_session=args.end,
                    plan_id=args.plan_id,
                )
            )
        else:
            result = asyncio.run(
                execute_backfill(
                    end_session=args.end,
                    plan_id=args.plan_id,
                    max_calls=args.max_calls,
                    authorization=args.authorization,
                    authorization_id=args.authorization_id,
                )
            )
    except BackfillExecutionFailed as exc:
        print(json.dumps(exc.result, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 1
    except BackfillRefused as exc:
        print(f"vendor backfill refused: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - never echo possibly sensitive exception text.
        print(f"vendor backfill failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
