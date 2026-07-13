"""Fail-closed local seal-and-serve proof for the MSFT forecast milestone.

``plan`` is read-only and binds one clean Git commit, the exact completed
258-session backfill, the database clock, the code-derived snapshot policies,
and the local-only runtime configuration. ``execute`` accepts only that exact
plan, runs one short-lived least-privilege builder container, then proves
API-key enforcement and parses the real localhost response contract.

This lane never imports a vendor provider and never receives a vendor key.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import math
import os
import re
import subprocess
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, Protocol, cast
from urllib.parse import urlsplit

import exchange_calendars as xcals
import httpx
import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import and_, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.core.logging import configure_logging
from app.core.security import API_KEY_HEADER
from app.db.models.bars import Bar, BarVersionAvailability
from app.db.session import build_engine, build_sessionmaker
from app.schemas.common import DISCLAIMER
from app.schemas.forecast import ForecastRequest, ForecastResponse
from app.services.forecast_serving import SqlForecastInputSnapshotRepository
from app.services.forecast_snapshot_builder import (
    DEFAULT_AVAILABILITY_RULE_SET_HASH,
    DEFAULT_RESOLUTION_POLICY_HASH,
    DEFAULT_SNAPSHOT_BUILD_POLICY,
)
from app.services.forecast_snapshots import (
    ForecastInputSnapshotRecord,
    validate_and_resolve_snapshot,
)
from app.services.market_calendar import latest_completed_xnys_session
from ingestion.locks import exclusive_vendor_operation
from ml.models.baselines import NaiveForecaster
from scripts.vendor_backfill import (
    BACKFILL_ADJUSTMENT_BASIS,
    BACKFILL_MULTIPLIER,
    BACKFILL_SOURCE,
    BACKFILL_SYMBOL,
    BACKFILL_TIMESPAN,
    DEFAULT_LEDGER_PATH,
    REQUIRED_SESSIONS,
    AttemptLedger,
    BackfillPlan,
    BackfillRefused,
    _clean_git_revision,
    _expected_session_dates,
    _session_close,
    plan_backfill,
)

AUTHORIZATION_SENTINEL = "stockapi-msft-seal-serve-only"
API_ORIGIN = "http://127.0.0.1:8000"
FORECAST_PATH = "/v1/forecast/MSFT"
FORECAST_HORIZON = 5
FORECAST_TARGET = "close"
FORECAST_HORIZON_UNIT = "trading_day"
FORECAST_MODEL = "baseline_naive"
FORECAST_COVERAGE = 0.8
SNAPSHOT_ONE_SHOT_MODULE = "ingestion.tasks.seal_forecast_demo_snapshot"
PLAN_CONTRACT_VERSION = 1
ROLLOVER_GUARD_SECONDS = 600
SNAPSHOT_CONTAINER_TIMEOUT_SECONDS = 360
HTTP_TIMEOUT_SECONDS = 10.0
API_HEALTH_ATTEMPTS = 12
REPO_ROOT = Path(__file__).resolve().parents[1]

_PLAN_ID_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IMAGE_ID_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_CONTAINER_ID_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_REVISION_LABEL = "org.opencontainers.image.revision"
_ATTESTED_REVISION_ENV = "STOCKAPI_FORECAST_DEMO_REVISION"
_ATTESTED_API_IMAGE_ENV = "STOCKAPI_FORECAST_DEMO_API_IMAGE_ID"
_ATTESTED_BUILDER_IMAGE_ENV = "STOCKAPI_FORECAST_DEMO_BUILDER_IMAGE_ID"
_ATTESTED_API_CONTAINER_ENV = "STOCKAPI_FORECAST_DEMO_API_CONTAINER_ID"
_API_IMAGE_OVERRIDE_ENV = "STOCKAPI_API_IMAGE"
_BUILDER_IMAGE_OVERRIDE_ENV = "STOCKAPI_SNAPSHOT_BUILDER_IMAGE"
_VENDOR_SECRET_VARIABLES = frozenset(
    {
        "ALPACA_API_KEY",
        "ALPACA_API_SECRET",
        "DATABENTO_API_KEY",
        "FINNHUB_API_KEY",
        "FMP_API_KEY",
        "NASDAQ_DATA_LINK_API_KEY",
        "POLYGON_API_KEY",
    }
)


class ForecastDemoRefused(RuntimeError):
    """The requested proof escaped or no longer matches the reviewed plan."""


class ForecastDemoEnvironment(BaseSettings):
    """Minimal no-vendor environment surface accepted by this controller."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: Literal["local", "test", "staging", "production"] = "local"
    api_v1_prefix: str = "/v1"
    database_url: str
    database_pool_size: int = 5
    database_max_overflow: int = 5
    database_pool_timeout: int = 30
    celery_broker_url: str = "redis://localhost:6380/0"
    celery_result_backend: str = "redis://localhost:6380/1"
    api_keys: str = ""
    jwt_secret: str = "change_me_random_64_chars"
    forecast_resolution_policy_hash: str | None = None
    forecast_trusted_availability_rule_set_hash: str | None = None
    forecast_seasonal_period: int = 5

    def runtime_settings(self) -> Settings:
        return Settings(
            _env_file=None,
            **self.model_dump(),
            polygon_api_key=None,
            fmp_api_key=None,
            finnhub_api_key=None,
            nasdaq_data_link_api_key=None,
            alpaca_api_key=None,
            alpaca_api_secret=None,
            databento_api_key=None,
        )


def _get_demo_settings() -> Settings:
    return ForecastDemoEnvironment().runtime_settings()


@dataclass(frozen=True)
class ForecastDemoDatabaseState:
    """Database-clock and exact-receipt facts used by one plan."""

    database_now: datetime
    exact_receipt_rows: int
    stable_cutoff: datetime | None


@dataclass(frozen=True)
class ForecastDemoPlan:
    """Content-addressed seal-and-serve authorization scope."""

    end_session: date
    tool_revision: str
    backfill_plan: BackfillPlan
    database_now: datetime
    stable_cutoff: datetime | None
    exact_receipt_rows: int
    ambiguous_prior_attempts: int
    api_key_count: int
    resolution_pin_matches: bool
    availability_pin_matches: bool
    blockers: tuple[str, ...]
    plan_id: str

    @property
    def ready(self) -> bool:
        return not self.blockers

    def public_result(self) -> dict[str, object]:
        return {
            "status": "ready" if self.ready else "blocked",
            "plan_id": self.plan_id,
            "tool_revision": self.tool_revision,
            "symbol": BACKFILL_SYMBOL,
            "source": BACKFILL_SOURCE,
            "window_start": self.backfill_plan.expected_dates[0].isoformat(),
            "window_end": self.end_session.isoformat(),
            "required_sessions": len(self.backfill_plan.expected_dates),
            "complete_sessions": len(self.backfill_plan.complete_dates),
            "missing_sessions": len(self.backfill_plan.missing_dates),
            "receipt_repairs_required": len(self.backfill_plan.repairable_dates),
            "ambiguous_prior_attempts": self.ambiguous_prior_attempts,
            "exact_receipt_rows": self.exact_receipt_rows,
            "stable_cutoff": _optional_timestamp(self.stable_cutoff),
            "database_now": _timestamp(self.database_now),
            "backfill_plan_id": self.backfill_plan.plan_id,
            "resolution_policy_hash": DEFAULT_RESOLUTION_POLICY_HASH,
            "availability_rule_set_hash": DEFAULT_AVAILABILITY_RULE_SET_HASH,
            "api_auth_configured": self.api_key_count == 1,
            "request": _public_request(),
            "blockers": list(self.blockers),
        }


class ForecastDemoStore(Protocol):
    """Runtime-role read seam for planning and independent verification."""

    async def database_state(
        self, session_dates: tuple[date, ...]
    ) -> ForecastDemoDatabaseState: ...

    async def database_now(self) -> datetime: ...

    async def get_snapshot(self, snapshot_id: str) -> ForecastInputSnapshotRecord | None: ...


@dataclass(frozen=True)
class RuntimeImageAttestation:
    """Immutable image/container facts handed off by the reviewed wrapper."""

    tool_revision: str
    api_image_id: str
    builder_image_id: str
    api_container_id: str


StoreFactory = Callable[[Settings], AbstractAsyncContextManager[ForecastDemoStore]]
Clock = Callable[[], datetime]
RevisionFn = Callable[[], str]
SessionsFn = Callable[[date], tuple[date, ...]]
HttpGet = Callable[[str, Sequence[tuple[str, str]], str | None], Awaitable["HttpResult"]]
SnapshotSealer = Callable[
    [datetime, date, str, RuntimeImageAttestation], Awaitable[dict[str, object]]
]
RuntimeAttestor = Callable[[str], RuntimeImageAttestation]
RuntimeRevalidator = Callable[[RuntimeImageAttestation], None]
LockFn = Callable[[Settings], AbstractAsyncContextManager[None]]


@dataclass(frozen=True)
class HttpResult:
    status_code: int
    content: bytes
    authenticate_header: str | None = None


@dataclass(frozen=True)
class ValidatedSnapshotEvidence:
    """Deterministic response facts independently resolved from sealed bytes."""

    target_times: tuple[datetime, ...]
    source_snapshot_id: str
    source_max_available_at: datetime
    max_available_at: datetime
    expected_points: tuple[float, ...]
    expected_quantiles: tuple[tuple[float, tuple[float, ...]], ...]


class SqlForecastDemoStore:
    """Read exact receipt/snapshot evidence through ``stockapi_app``."""

    def __init__(self, settings: Settings) -> None:
        self._engine: AsyncEngine = build_engine(settings)
        self._maker: async_sessionmaker[AsyncSession] = build_sessionmaker(self._engine)
        self._repository = SqlForecastInputSnapshotRepository(
            self._maker,
            trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        )

    async def __aenter__(self) -> SqlForecastDemoStore:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._engine.dispose()

    async def database_state(self, session_dates: tuple[date, ...]) -> ForecastDemoDatabaseState:
        closes = tuple(_session_close(value) for value in session_dates)
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
        statement = (
            select(
                func.count().label("receipt_count"),
                func.max(BarVersionAvailability.available_at).label("stable_cutoff"),
            )
            .select_from(Bar)
            .join(BarVersionAvailability, receipt_match)
            .where(
                Bar.symbol == BACKFILL_SYMBOL,
                Bar.timespan == BACKFILL_TIMESPAN,
                Bar.multiplier == BACKFILL_MULTIPLIER,
                Bar.source == BACKFILL_SOURCE,
                Bar.adjustment_basis == BACKFILL_ADJUSTMENT_BASIS,
                Bar.ts.in_(closes),
            )
        )
        async with self._maker() as session:
            row = (await session.execute(statement)).one()
            database_now = _aware(
                (await session.execute(select(func.clock_timestamp()))).scalar_one(),
                "database clock",
            )
        cutoff = row.stable_cutoff
        return ForecastDemoDatabaseState(
            database_now=database_now,
            exact_receipt_rows=int(row.receipt_count),
            stable_cutoff=(None if cutoff is None else _aware(cutoff, "stable cutoff")),
        )

    async def database_now(self) -> datetime:
        async with self._maker() as session:
            value = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        return _aware(value, "database clock")

    async def get_snapshot(self, snapshot_id: str) -> ForecastInputSnapshotRecord | None:
        return await self._repository.get(snapshot_id)


@asynccontextmanager
async def _sql_store(settings: Settings) -> AsyncIterator[ForecastDemoStore]:
    async with SqlForecastDemoStore(settings) as store:
        yield store


def _aware(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ForecastDemoRefused(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _aware(value, "timestamp").isoformat()


def _next_session_close(end_session: date) -> datetime:
    calendar = xcals.get_calendar("XNYS")
    label = pd.Timestamp(end_session)
    if not calendar.is_session(label):
        raise ForecastDemoRefused("end must be an XNYS trading session")
    return _aware(
        calendar.session_close(calendar.next_session(label)).to_pydatetime(),
        "next XNYS session close",
    )


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _sha256_document(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _public_request() -> dict[str, object]:
    return {
        "method": "GET",
        "origin": API_ORIGIN,
        "path": FORECAST_PATH,
        "horizon": FORECAST_HORIZON,
        "horizon_unit": FORECAST_HORIZON_UNIT,
        "target": FORECAST_TARGET,
        "model": FORECAST_MODEL,
        "coverage": FORECAST_COVERAGE,
        "authentication": API_KEY_HEADER,
    }


def _request_params(snapshot_id: str) -> tuple[tuple[str, str], ...]:
    return (
        ("horizon", str(FORECAST_HORIZON)),
        ("horizon_unit", FORECAST_HORIZON_UNIT),
        ("target", FORECAST_TARGET),
        ("snapshot_id", snapshot_id),
        ("model", FORECAST_MODEL),
        ("coverage", str(FORECAST_COVERAGE)),
    )


def _configured_api_keys(settings: Settings) -> tuple[str, ...]:
    keys = tuple(value.strip() for value in settings.api_keys.split(",") if value.strip())
    if any(
        not value.isascii() or any(not 0x21 <= ord(character) <= 0x7E for character in value)
        for value in keys
    ):
        raise ForecastDemoRefused("API_KEYS must contain only visible ASCII characters")
    return keys


def _api_key_binding(settings: Settings, api_keys: tuple[str, ...]) -> str | None:
    """Bind auth identity without publishing a reusable API-key fingerprint."""

    secret = settings.jwt_secret.strip()
    if len(api_keys) != 1 or not _valid_binding_secret(secret):
        return None
    digest = hmac.new(
        secret.encode("ascii"),
        b"stockapi-forecast-demo-api-key-v1\0" + api_keys[0].encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return "hmac-sha256:" + digest


def _valid_binding_secret(secret: str) -> bool:
    return len(secret) >= 32 and secret != "change_me_random_64_chars" and secret.isascii()


def _exact_local_redis(value: str, *, database: int) -> bool:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme == "redis"
        and parsed.hostname in {"localhost", "127.0.0.1"}
        and port == 6380
        and parsed.username is None
        and parsed.password is None
        and parsed.path == f"/{database}"
        and not parsed.query
        and not parsed.fragment
    )


def _safe_demo_settings(settings: Settings) -> Settings:
    if settings.app_env != "local":
        raise ForecastDemoRefused("APP_ENV must be exactly local")
    try:
        database_url = make_url(settings.database_url)
    except (ArgumentError, ValueError):
        raise ForecastDemoRefused("DATABASE_URL is not a valid SQLAlchemy URL") from None
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
        raise ForecastDemoRefused("DATABASE_URL must use stockapi_app on local stockapi_test:5432")
    safe = settings.model_copy(
        update={
            "polygon_api_key": None,
            "fmp_api_key": None,
            "finnhub_api_key": None,
            "nasdaq_data_link_api_key": None,
            "alpaca_api_key": None,
            "alpaca_api_secret": None,
            "databento_api_key": None,
        }
    )
    if safe.api_v1_prefix != "/v1":
        raise ForecastDemoRefused("API_V1_PREFIX must be exactly /v1")
    if not _exact_local_redis(safe.celery_broker_url, database=0):
        raise ForecastDemoRefused("CELERY_BROKER_URL must be local Redis 6380 database 0")
    if not _exact_local_redis(safe.celery_result_backend, database=1):
        raise ForecastDemoRefused("CELERY_RESULT_BACKEND must be local Redis 6380 database 1")
    _configured_api_keys(safe)
    return safe


def _plan_blockers(
    *,
    settings: Settings,
    api_key_count: int,
    backfill_plan: BackfillPlan,
    state: ForecastDemoDatabaseState,
    final_database_now: datetime,
    ambiguous_prior_attempts: int,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if api_key_count != 1:
        blockers.append("configure exactly one non-empty API_KEYS value")
    if not _valid_binding_secret(settings.jwt_secret.strip()):
        blockers.append("configure a non-default JWT_SECRET of at least 32 ASCII characters")
    if settings.forecast_resolution_policy_hash != DEFAULT_RESOLUTION_POLICY_HASH:
        blockers.append("pin the code-derived forecast resolution-policy hash")
    if settings.forecast_trusted_availability_rule_set_hash != DEFAULT_AVAILABILITY_RULE_SET_HASH:
        blockers.append("pin the code-derived trusted availability rule-set hash")
    if len(backfill_plan.expected_dates) != REQUIRED_SESSIONS:
        blockers.append("the backfill window is not exactly 258 XNYS sessions")
    if len(backfill_plan.complete_dates) != REQUIRED_SESSIONS:
        blockers.append("complete the exact 258-session MSFT backfill")
    if backfill_plan.missing_dates:
        blockers.append("backfill sessions are still missing")
    if backfill_plan.repairable_dates:
        blockers.append("repair exact-version availability receipts")
    if backfill_plan.ambiguous_dates or ambiguous_prior_attempts:
        blockers.append("resolve ambiguous prior vendor attempts")
    if state.exact_receipt_rows != REQUIRED_SESSIONS:
        blockers.append("the exact current-version receipt count is not 258")
    if state.stable_cutoff is None:
        blockers.append("no stable exact-version receipt cutoff exists")
    else:
        if state.stable_cutoff > final_database_now:
            blockers.append("the stable receipt cutoff is later than the database clock")
        if latest_completed_xnys_session(state.stable_cutoff) != backfill_plan.end_session:
            blockers.append("the stable receipt cutoff is outside the authorized XNYS session")
    if latest_completed_xnys_session(final_database_now) != backfill_plan.end_session:
        blockers.append("a newer XNYS session has completed; make a fresh plan")
    if (
        _next_session_close(backfill_plan.end_session) - final_database_now
    ).total_seconds() < ROLLOVER_GUARD_SECONDS:
        blockers.append("too little time remains before the next XNYS close")
    return tuple(blockers)


def _build_demo_plan(
    *,
    settings: Settings,
    backfill_plan: BackfillPlan,
    state: ForecastDemoDatabaseState,
    final_database_now: datetime,
    ambiguous_prior_attempts: int = 0,
) -> ForecastDemoPlan:
    api_keys = _configured_api_keys(settings)
    api_key_count = len(api_keys)
    api_key_binding = _api_key_binding(settings, api_keys)
    blockers = _plan_blockers(
        settings=settings,
        api_key_count=api_key_count,
        backfill_plan=backfill_plan,
        state=state,
        final_database_now=final_database_now,
        ambiguous_prior_attempts=ambiguous_prior_attempts,
    )
    resolution_matches = settings.forecast_resolution_policy_hash == DEFAULT_RESOLUTION_POLICY_HASH
    availability_matches = (
        settings.forecast_trusted_availability_rule_set_hash == DEFAULT_AVAILABILITY_RULE_SET_HASH
    )
    canonical = {
        "version": PLAN_CONTRACT_VERSION,
        "tool_revision": backfill_plan.tool_revision,
        "backfill_plan_id": backfill_plan.plan_id,
        "symbol": BACKFILL_SYMBOL,
        "source": BACKFILL_SOURCE,
        "end_session": backfill_plan.end_session.isoformat(),
        "stable_cutoff": _optional_timestamp(state.stable_cutoff),
        "exact_receipt_rows": state.exact_receipt_rows,
        "ambiguous_prior_attempts": ambiguous_prior_attempts,
        "resolution_policy_hash": DEFAULT_RESOLUTION_POLICY_HASH,
        "availability_rule_set_hash": DEFAULT_AVAILABILITY_RULE_SET_HASH,
        "resolution_pin_matches": resolution_matches,
        "availability_pin_matches": availability_matches,
        "api_key_count": api_key_count,
        "api_key_binding": api_key_binding,
        "rollover_guard_seconds": ROLLOVER_GUARD_SECONDS,
        "snapshot_container_timeout_seconds": SNAPSHOT_CONTAINER_TIMEOUT_SECONDS,
        "http_timeout_seconds": HTTP_TIMEOUT_SECONDS,
        "api_health_attempts": API_HEALTH_ATTEMPTS,
        "request": _public_request(),
        "blockers": list(blockers),
    }
    return ForecastDemoPlan(
        end_session=backfill_plan.end_session,
        tool_revision=backfill_plan.tool_revision,
        backfill_plan=backfill_plan,
        database_now=final_database_now,
        stable_cutoff=state.stable_cutoff,
        exact_receipt_rows=state.exact_receipt_rows,
        ambiguous_prior_attempts=ambiguous_prior_attempts,
        api_key_count=api_key_count,
        resolution_pin_matches=resolution_matches,
        availability_pin_matches=availability_matches,
        blockers=blockers,
        plan_id=_sha256_document(canonical),
    )


async def plan_forecast_demo(
    *,
    end_session: date,
    settings: Settings | None = None,
    store_factory: StoreFactory = _sql_store,
    sessions_fn: SessionsFn = _expected_session_dates,
    revision_fn: RevisionFn = _clean_git_revision,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
) -> ForecastDemoPlan:
    """Build a read-only content-addressed plan from database-clock evidence."""

    safe_settings = _safe_demo_settings(settings or _get_demo_settings())
    tool_revision = revision_fn()
    expected_dates = sessions_fn(end_session)
    expected_set = set(expected_dates)
    try:
        ambiguous_prior_attempts = sum(
            value in expected_set for value in AttemptLedger(ledger_path).unresolved_dates()
        )
    except BackfillRefused as exc:
        raise ForecastDemoRefused(str(exc)) from None
    async with store_factory(safe_settings) as store:
        state = await store.database_state(expected_dates)
    try:
        backfill_plan = await plan_backfill(
            end_session=end_session,
            settings=safe_settings,
            clock=lambda: state.database_now,
            sessions_fn=sessions_fn,
            revision_fn=lambda: tool_revision,
            ledger_path=ledger_path,
        )
    except BackfillRefused as exc:
        raise ForecastDemoRefused(str(exc)) from None
    async with store_factory(safe_settings) as store:
        final_database_now = await store.database_now()
    return _build_demo_plan(
        settings=safe_settings,
        backfill_plan=backfill_plan,
        state=state,
        final_database_now=final_database_now,
        ambiguous_prior_attempts=ambiguous_prior_attempts,
    )


async def _default_http_get(
    path: str,
    params: Sequence[tuple[str, str]],
    api_key: str | None,
) -> HttpResult:
    headers = {} if api_key is None else {API_KEY_HEADER: api_key}
    async with httpx.AsyncClient(
        base_url=API_ORIGIN,
        timeout=HTTP_TIMEOUT_SECONDS,
        trust_env=False,
    ) as client:
        response = await client.get(path, params=list(params), headers=headers)
    return HttpResult(
        status_code=response.status_code,
        content=response.content,
        authenticate_header=response.headers.get("WWW-Authenticate"),
    )


async def _wait_for_api(http_get: HttpGet) -> None:
    for _ in range(API_HEALTH_ATTEMPTS):
        try:
            result = await http_get("/healthz", (), None)
        except (httpx.HTTPError, OSError):
            result = None
        if result is not None and result.status_code == 200:
            return
        await asyncio.sleep(1)
    raise ForecastDemoRefused("the local API did not become healthy")


_SCRUBBED_SUBPROCESS_VARIABLES = _VENDOR_SECRET_VARIABLES | frozenset(
    {
        "COMPOSE_DISABLE_ENV_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_FILE",
        "COMPOSE_PROFILES",
        "COMPOSE_PROJECT_NAME",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        _API_IMAGE_OVERRIDE_ENV,
        _ATTESTED_API_CONTAINER_ENV,
        _ATTESTED_API_IMAGE_ENV,
        _ATTESTED_BUILDER_IMAGE_ENV,
        _ATTESTED_REVISION_ENV,
        _BUILDER_IMAGE_OVERRIDE_ENV,
    }
)


def _sanitized_subprocess_environment() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if key.upper() not in _SCRUBBED_SUBPROCESS_VARIABLES
    }


def _run_docker(
    arguments: Sequence[str],
    *,
    environment: dict[str, str],
    timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "--context", "desktop-linux", *arguments],
        cwd=REPO_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _validate_local_docker(environment: dict[str, str]) -> None:
    try:
        context = _run_docker(("context", "show"), environment=environment)
        context_name = context.stdout.strip()
        endpoint = _run_docker(
            (
                "context",
                "inspect",
                "desktop-linux",
                "--format",
                "{{.Endpoints.docker.Host}}",
            ),
            environment=environment,
        )
        info = _run_docker(
            ("info", "--format", "{{.Name}}|{{.OperatingSystem}}"),
            environment=environment,
        )
    except (OSError, subprocess.SubprocessError):
        raise ForecastDemoRefused("could not prove a local Docker Desktop daemon") from None
    if (
        context.returncode != 0
        or context_name != "desktop-linux"
        or endpoint.returncode != 0
        or endpoint.stdout.strip() != "npipe:////./pipe/dockerDesktopLinuxEngine"
        or info.returncode != 0
        or info.stdout.strip() != "docker-desktop|Docker Desktop"
    ):
        raise ForecastDemoRefused("Docker must target the local Docker Desktop Linux daemon")


def _compose_command(*arguments: str) -> tuple[str, ...]:
    env_file = (REPO_ROOT / ".env").resolve()
    compose_file = (REPO_ROOT / "docker-compose.yml").resolve()
    if not env_file.is_file() or not compose_file.is_file():
        raise ForecastDemoRefused("the pinned local Compose inputs are missing")
    return (
        "compose",
        "--ansi",
        "never",
        "--env-file",
        str(env_file),
        "--file",
        str(compose_file),
        "--project-directory",
        str(REPO_ROOT.resolve()),
        "--project-name",
        "stock-api",
        "--profile",
        "app",
        *arguments,
    )


def _image_revision(image_id: str, environment: dict[str, str]) -> str:
    inspected = _run_docker(
        (
            "image",
            "inspect",
            image_id,
            "--format",
            f'{{{{ index .Config.Labels "{_IMAGE_REVISION_LABEL}" }}}}',
        ),
        environment=environment,
    )
    if inspected.returncode != 0:
        raise ForecastDemoRefused("an attested forecast-demo image is unavailable")
    return inspected.stdout.strip()


def _api_container_facts(environment: dict[str, str]) -> tuple[str, str]:
    inspected = _run_docker(
        (
            "inspect",
            "stockapi-api",
            "--format",
            "{{.Id}}|{{.Image}}|{{.State.Running}}|"
            '{{ index .Config.Labels "com.docker.compose.project" }}|'
            '{{ index .Config.Labels "com.docker.compose.service" }}|{{ len .Mounts }}',
        ),
        environment=environment,
    )
    parts = inspected.stdout.strip().split("|")
    if (
        inspected.returncode != 0
        or len(parts) != 6
        or _CONTAINER_ID_PATTERN.fullmatch(parts[0]) is None
        or _IMAGE_ID_PATTERN.fullmatch(parts[1]) is None
        or parts[2] != "true"
        or parts[3] != "stock-api"
        or parts[4] != "api"
        or parts[5] != "0"
    ):
        raise ForecastDemoRefused("the attested local API container escaped its fixed scope")
    return parts[0], parts[1]


def _attest_runtime_images(tool_revision: str) -> RuntimeImageAttestation:
    """Require the wrapper's immutable build handoff and prove it independently."""

    if _GIT_REVISION_PATTERN.fullmatch(tool_revision) is None:
        raise ForecastDemoRefused("the reviewed tool revision is malformed")
    attested_revision = os.environ.get(_ATTESTED_REVISION_ENV, "")
    api_image_id = os.environ.get(_ATTESTED_API_IMAGE_ENV, "")
    builder_image_id = os.environ.get(_ATTESTED_BUILDER_IMAGE_ENV, "")
    attested_api_container_id = os.environ.get(_ATTESTED_API_CONTAINER_ENV, "")
    if (
        not hmac.compare_digest(attested_revision, tool_revision)
        or _IMAGE_ID_PATTERN.fullmatch(api_image_id) is None
        or _IMAGE_ID_PATTERN.fullmatch(builder_image_id) is None
        or _CONTAINER_ID_PATTERN.fullmatch(attested_api_container_id) is None
    ):
        raise ForecastDemoRefused("execute requires the wrapper's immutable image attestation")
    environment = _sanitized_subprocess_environment()
    _validate_local_docker(environment)
    api_container_id, actual_api_image_id = _api_container_facts(environment)
    if not hmac.compare_digest(
        api_container_id, attested_api_container_id
    ) or not hmac.compare_digest(actual_api_image_id, api_image_id):
        raise ForecastDemoRefused("the running API differs from the attested image")
    if not hmac.compare_digest(_image_revision(api_image_id, environment), tool_revision):
        raise ForecastDemoRefused("the API image is not bound to the reviewed revision")
    if not hmac.compare_digest(_image_revision(builder_image_id, environment), tool_revision):
        raise ForecastDemoRefused("the builder image is not bound to the reviewed revision")
    return RuntimeImageAttestation(
        tool_revision=tool_revision,
        api_image_id=api_image_id,
        builder_image_id=builder_image_id,
        api_container_id=attested_api_container_id,
    )


def _revalidate_api_container(attestation: RuntimeImageAttestation) -> None:
    environment = _sanitized_subprocess_environment()
    container_id, image_id = _api_container_facts(environment)
    if (
        not hmac.compare_digest(container_id, attestation.api_container_id)
        or not hmac.compare_digest(image_id, attestation.api_image_id)
        or not hmac.compare_digest(
            _image_revision(image_id, environment), attestation.tool_revision
        )
    ):
        raise ForecastDemoRefused("the attested API container changed during the proof")


def _cleanup_one_shot_container(
    name: str,
    plan_id: str,
    environment: dict[str, str],
) -> None:
    try:
        inspected = _run_docker(
            (
                "inspect",
                name,
                "--format",
                '{{.Id}}|{{ index .Config.Labels "stockapi.forecast-demo.plan-id" }}',
            ),
            environment=environment,
        )
        parts = inspected.stdout.strip().split("|", maxsplit=1)
        if (
            inspected.returncode == 0
            and len(parts) == 2
            and _CONTAINER_ID_PATTERN.fullmatch(parts[0]) is not None
            and hmac.compare_digest(parts[1], plan_id)
        ):
            _run_docker(("rm", "--force", parts[0]), environment=environment)
    except (OSError, subprocess.SubprocessError):
        # Cleanup is best effort; the deterministic name/label makes later
        # plans refuse instead of silently launching a second writer.
        return


async def _seal_snapshot_once(
    cutoff: datetime,
    end_session: date,
    plan_id: str,
    attestation: RuntimeImageAttestation,
) -> dict[str, object]:
    """Run exactly one builder-role process, never a persistent queue consumer."""

    if _PLAN_ID_PATTERN.fullmatch(plan_id) is None:
        raise ForecastDemoRefused("one-shot builder requires the exact plan_id")
    environment = _sanitized_subprocess_environment()
    environment[_API_IMAGE_OVERRIDE_ENV] = attestation.api_image_id
    environment[_BUILDER_IMAGE_OVERRIDE_ENV] = attestation.builder_image_id
    _validate_local_docker(environment)
    if not hmac.compare_digest(
        _image_revision(attestation.builder_image_id, environment),
        attestation.tool_revision,
    ):
        raise ForecastDemoRefused("the attested builder image changed before the seal")
    container_name = "stockapi-forecast-demo-" + plan_id.removeprefix("sha256:")[:16]
    existing = _run_docker(("inspect", container_name), environment=environment)
    if existing.returncode == 0:
        raise ForecastDemoRefused("a prior one-shot snapshot container still exists")
    command = [
        *_compose_command(),
        "run",
        "--pull",
        "never",
        "--rm",
        "--no-deps",
        "--name",
        container_name,
        "--label",
        f"stockapi.forecast-demo.plan-id={plan_id}",
        "snapshot-builder",
        "python",
        "-m",
        SNAPSHOT_ONE_SHOT_MODULE,
        "--as-of",
        _timestamp(cutoff),
        "--end",
        end_session.isoformat(),
        "--tool-revision",
        attestation.tool_revision,
        "--authorization",
        AUTHORIZATION_SENTINEL,
    ]

    def _run() -> subprocess.CompletedProcess[str]:
        return _run_docker(
            command,
            environment=environment,
            timeout=SNAPSHOT_CONTAINER_TIMEOUT_SECONDS,
        )

    try:
        try:
            completed = await asyncio.to_thread(_run)
        except (OSError, subprocess.SubprocessError):
            raise ForecastDemoRefused("the one-shot snapshot builder could not run") from None
        if completed.returncode != 0:
            raise ForecastDemoRefused("the one-shot snapshot builder failed")
        output_lines = [line for line in completed.stdout.splitlines() if line.strip()]
        if len(output_lines) != 1:
            raise ForecastDemoRefused("the one-shot snapshot builder output is malformed")
        try:
            value = json.loads(output_lines[0])
        except (ValueError, json.JSONDecodeError):
            raise ForecastDemoRefused("the one-shot snapshot builder output is malformed") from None
        if not isinstance(value, dict):
            raise ForecastDemoRefused("the one-shot snapshot builder returned a malformed result")
        return cast(dict[str, object], value)
    finally:
        await asyncio.to_thread(
            _cleanup_one_shot_container,
            container_name,
            plan_id,
            environment,
        )


def _strict_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ForecastDemoRefused(f"snapshot-builder {label} is malformed")
    return value


def _validated_task_result(result: dict[str, object], cutoff: datetime) -> tuple[str, str, int]:
    expected_keys = {
        "status",
        "as_of",
        "resolution_policy_hash",
        "availability_rule_set_hash",
        "created",
        "replayed",
        "deferred",
        "failed",
        "per_symbol",
    }
    if set(result) != expected_keys:
        raise ForecastDemoRefused("snapshot-builder result schema is malformed")
    created = _strict_integer(result["created"], "created count")
    replayed = _strict_integer(result["replayed"], "replayed count")
    deferred = _strict_integer(result["deferred"], "deferred count")
    failed = _strict_integer(result["failed"], "failed count")
    try:
        returned_cutoff = _aware(
            datetime.fromisoformat(str(result["as_of"]).replace("Z", "+00:00")),
            "snapshot-builder cutoff",
        )
    except (TypeError, ValueError):
        raise ForecastDemoRefused("snapshot-builder cutoff is malformed") from None
    if (
        result["status"] != "ok"
        or returned_cutoff != cutoff
        or result["resolution_policy_hash"] != DEFAULT_RESOLUTION_POLICY_HASH
        or result["availability_rule_set_hash"] != DEFAULT_AVAILABILITY_RULE_SET_HASH
        or (created, replayed) not in {(1, 0), (0, 1)}
        or deferred != 0
        or failed != 0
    ):
        raise ForecastDemoRefused("snapshot-builder result does not match the plan")
    per_symbol = result["per_symbol"]
    if not isinstance(per_symbol, list) or len(per_symbol) != 1:
        raise ForecastDemoRefused("snapshot-builder did not return exactly one symbol")
    entry = per_symbol[0]
    if not isinstance(entry, dict) or set(entry) != {
        "symbol",
        "status",
        "snapshot_id",
        "observations",
        "target_times",
    }:
        raise ForecastDemoRefused("snapshot-builder symbol result is malformed")
    snapshot_id = entry["snapshot_id"]
    observations = _strict_integer(entry["observations"], "observation count")
    target_times = _strict_integer(entry["target_times"], "target-time count")
    expected_entry_status = "created" if (created, replayed) == (1, 0) else "replayed"
    if (
        entry["symbol"] != BACKFILL_SYMBOL
        or entry["status"] != expected_entry_status
        or not isinstance(snapshot_id, str)
        or _PLAN_ID_PATTERN.fullmatch(snapshot_id) is None
        or not REQUIRED_SESSIONS <= observations <= DEFAULT_SNAPSHOT_BUILD_POLICY.observation_limit
        or target_times != DEFAULT_SNAPSHOT_BUILD_POLICY.target_time_count
    ):
        raise ForecastDemoRefused("snapshot-builder symbol result does not match the plan")
    return snapshot_id, cast(str, entry["status"]), observations


async def _absent_snapshot_id(store: ForecastDemoStore, plan_id: str) -> str:
    for counter in range(16):
        candidate = _sha256_document(
            {
                "purpose": "forecast-demo-authenticated-404-probe",
                "plan_id": plan_id,
                "counter": counter,
            }
        )
        if await store.get_snapshot(candidate) is None:
            return candidate
    raise ForecastDemoRefused("could not establish an absent snapshot probe")


def _wrong_api_key(api_key: str, plan_id: str) -> str:
    for counter in range(16):
        candidate = hashlib.sha256(
            f"forecast-demo-wrong-key:{plan_id}:{counter}".encode("ascii")
        ).hexdigest()
        if not hmac.compare_digest(candidate, api_key):
            return candidate
    raise ForecastDemoRefused("could not establish an invalid API-key probe")


def _validate_snapshot_record(
    record: ForecastInputSnapshotRecord | None,
    *,
    snapshot_id: str,
    cutoff: datetime,
) -> ValidatedSnapshotEvidence:
    if record is None:
        raise ForecastDemoRefused("the sealed snapshot is not readable through the runtime role")
    request = ForecastRequest(
        symbol=BACKFILL_SYMBOL,
        horizon=FORECAST_HORIZON,
        horizon_unit=FORECAST_HORIZON_UNIT,
        target=FORECAST_TARGET,
        snapshot_id=snapshot_id,
        model=FORECAST_MODEL,
        interval_coverages=[FORECAST_COVERAGE],
    )
    resolved = validate_and_resolve_snapshot(
        record,
        request,
        expected_series_basis=BACKFILL_ADJUSTMENT_BASIS,
        expected_resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
        expected_input_timespan=BACKFILL_TIMESPAN,
        expected_input_multiplier=BACKFILL_MULTIPLIER,
        trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
    )
    if (
        record.snapshot_id != snapshot_id
        or record.as_of != cutoff
        or record.availability_status != "passed"
        or record.availability_rule_set_hash != DEFAULT_AVAILABILITY_RULE_SET_HASH
        or record.observation_count < REQUIRED_SESSIONS
        or record.observation_count > DEFAULT_SNAPSHOT_BUILD_POLICY.observation_limit
        or record.target_time_count != DEFAULT_SNAPSHOT_BUILD_POLICY.target_time_count
        or record.max_available_at > cutoff
        or record.sealed_at < cutoff
        or not resolved.availability_verified
        or resolved.snapshot_id != snapshot_id
        or resolved.symbol != BACKFILL_SYMBOL
        or resolved.series_basis != BACKFILL_ADJUSTMENT_BASIS
        or len(resolved.observations) != record.observation_count
        or len(resolved.target_times) != FORECAST_HORIZON
        or len(resolved.data_sources) != 1
    ):
        raise ForecastDemoRefused("the sealed snapshot failed independent runtime validation")
    fitted = NaiveForecaster().fit([item.value for item in resolved.observations])
    expected_points = tuple(fitted.predict(FORECAST_HORIZON))
    quantile_levels = (0.1, 0.5, 0.9)
    raw_quantiles = fitted.predict_quantiles(FORECAST_HORIZON, quantile_levels)
    source = resolved.data_sources[0]
    max_available_at = max(
        [item.available_at for item in resolved.observations]
        + [item.max_available_at for item in resolved.data_sources]
    )
    return ValidatedSnapshotEvidence(
        target_times=resolved.target_times,
        source_snapshot_id=source.snapshot_id,
        source_max_available_at=source.max_available_at,
        max_available_at=max_available_at,
        expected_points=expected_points,
        expected_quantiles=tuple((level, tuple(raw_quantiles[level])) for level in quantile_levels),
    )


def _parse_forecast_response(content: bytes) -> ForecastResponse:
    try:
        return ForecastResponse.model_validate_json(content)
    except (ValueError, TypeError):
        raise ForecastDemoRefused("the authenticated forecast response is malformed") from None


def _validate_forecast_response(
    response: ForecastResponse,
    *,
    snapshot_id: str,
    cutoff: datetime,
    evidence: ValidatedSnapshotEvidence,
) -> None:
    provenance = response.provenance
    calibration = response.calibration
    if (
        response.symbol != BACKFILL_SYMBOL
        or response.target != FORECAST_TARGET
        or response.horizon != FORECAST_HORIZON
        or response.horizon_unit != FORECAST_HORIZON_UNIT
        or response.as_of != cutoff
        or response.currency != "USD"
        or len(response.forecasts) != FORECAST_HORIZON
        or provenance.snapshot_id != snapshot_id
        or provenance.feature_set_hash != snapshot_id
        or provenance.model_version != NaiveForecaster().model_version
        or provenance.series_basis != BACKFILL_ADJUSTMENT_BASIS
        or provenance.max_available_at != evidence.max_available_at
        or provenance.lookahead_check.status != "passed"
        or provenance.lookahead_check.violations
        or provenance.lookahead_check.max_feature_available_at != evidence.max_available_at
        or calibration.calibration_set_version != f"uncalibrated:{NaiveForecaster().model_version}"
        or calibration.method != "none"
        or calibration.sample_count != 0
        or calibration.window_start is not None
        or calibration.window_end is not None
        or calibration.by_interval
        or response.disclaimer != DISCLAIMER
    ):
        raise ForecastDemoRefused("the forecast response failed the milestone contract")
    if len(provenance.data_sources) != 1:
        raise ForecastDemoRefused("the forecast response lineage is not singular")
    source = provenance.data_sources[0]
    if (
        source.name != BACKFILL_SOURCE
        or source.snapshot_id != evidence.source_snapshot_id
        or source.fields != [FORECAST_TARGET]
        or source.max_available_at != evidence.source_max_available_at
    ):
        raise ForecastDemoRefused("the forecast response lineage escaped the source contract")
    expected_quantiles = dict(evidence.expected_quantiles)
    for index, step in enumerate(response.forecasts):
        if (
            step.target_time != evidence.target_times[index]
            or not math.isclose(
                step.point,
                evidence.expected_points[index],
                rel_tol=1e-12,
                abs_tol=1e-9,
            )
            or len(step.quantiles) != 3
            or len(step.intervals) != 1
        ):
            raise ForecastDemoRefused("the forecast path differs from the sealed snapshot")
        for expected_level, quantile in zip((0.1, 0.5, 0.9), step.quantiles, strict=True):
            if not math.isclose(quantile.level, expected_level, abs_tol=1e-12) or not math.isclose(
                quantile.value,
                expected_quantiles[expected_level][index],
                rel_tol=1e-12,
                abs_tol=1e-9,
            ):
                raise ForecastDemoRefused(
                    "the forecast quantiles differ from the deterministic baseline"
                )
        interval = step.intervals[0]
        if (
            not math.isclose(interval.coverage, FORECAST_COVERAGE, abs_tol=1e-12)
            or not math.isclose(interval.lower_quantile, 0.1, abs_tol=1e-12)
            or not math.isclose(interval.upper_quantile, 0.9, abs_tol=1e-12)
            or not math.isclose(
                interval.lower,
                expected_quantiles[0.1][index],
                rel_tol=1e-12,
                abs_tol=1e-9,
            )
            or not math.isclose(
                interval.upper,
                expected_quantiles[0.9][index],
                rel_tol=1e-12,
                abs_tol=1e-9,
            )
        ):
            raise ForecastDemoRefused("the forecast interval differs from the fixed request")


async def execute_forecast_demo(
    *,
    end_session: date,
    plan_id: str,
    authorization: str,
    settings: Settings | None = None,
    store_factory: StoreFactory = _sql_store,
    http_get: HttpGet = _default_http_get,
    snapshot_sealer: SnapshotSealer = _seal_snapshot_once,
    runtime_attestor: RuntimeAttestor = _attest_runtime_images,
    runtime_revalidator: RuntimeRevalidator = _revalidate_api_container,
    lock_fn: LockFn = exclusive_vendor_operation,
    sessions_fn: SessionsFn = _expected_session_dates,
    revision_fn: RevisionFn = _clean_git_revision,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
) -> dict[str, object]:
    """Seal/replay one local snapshot and prove the authenticated real route."""

    if authorization != AUTHORIZATION_SENTINEL:
        raise ForecastDemoRefused(f"authorization must be exactly {AUTHORIZATION_SENTINEL}")
    if _PLAN_ID_PATTERN.fullmatch(plan_id) is None:
        raise ForecastDemoRefused("plan_id must be a sha256 digest from plan mode")
    safe_settings = _safe_demo_settings(settings or _get_demo_settings())
    sealed_receipt: dict[str, object] | None = None
    proof_phase = "pre_seal"
    proof_http_status: int | None = None
    try:
        async with lock_fn(safe_settings):
            plan = await plan_forecast_demo(
                end_session=end_session,
                settings=safe_settings,
                store_factory=store_factory,
                sessions_fn=sessions_fn,
                revision_fn=revision_fn,
                ledger_path=ledger_path,
            )
            if plan.plan_id != plan_id:
                raise ForecastDemoRefused("database or configuration no longer matches plan_id")
            if not plan.ready or plan.stable_cutoff is None:
                raise ForecastDemoRefused("the seal-and-serve plan is not ready")
            api_keys = _configured_api_keys(safe_settings)
            if len(api_keys) != 1:
                raise ForecastDemoRefused("execute requires exactly one configured API key")
            api_key = api_keys[0]
            cutoff = plan.stable_cutoff
            attestation = await asyncio.to_thread(runtime_attestor, plan.tool_revision)

            await _wait_for_api(http_get)
            async with store_factory(safe_settings) as store:
                absent_id = await _absent_snapshot_id(store, plan.plan_id)
                unauthenticated = await http_get(
                    FORECAST_PATH,
                    _request_params(absent_id),
                    None,
                )
                if (
                    unauthenticated.status_code != 401
                    or unauthenticated.authenticate_header != API_KEY_HEADER
                ):
                    raise ForecastDemoRefused("the forecast route did not enforce API-key auth")
                wrong_key = await http_get(
                    FORECAST_PATH,
                    _request_params(absent_id),
                    _wrong_api_key(api_key, plan.plan_id),
                )
                if wrong_key.status_code != 401 or wrong_key.authenticate_header != API_KEY_HEADER:
                    raise ForecastDemoRefused("the forecast route accepted an invalid API key")
                missing = await http_get(
                    FORECAST_PATH,
                    _request_params(absent_id),
                    api_key,
                )
                if missing.status_code != 404:
                    raise ForecastDemoRefused(
                        "the authenticated missing-snapshot probe did not 404"
                    )

                task_result = await snapshot_sealer(
                    cutoff,
                    end_session,
                    plan.plan_id,
                    attestation,
                )
                snapshot_id, snapshot_status, observations = _validated_task_result(
                    task_result, cutoff
                )
                sealed_receipt = {
                    "plan_id": plan.plan_id,
                    "tool_revision": plan.tool_revision,
                    "symbol": BACKFILL_SYMBOL,
                    "end_session": end_session.isoformat(),
                    "stable_cutoff": _timestamp(cutoff),
                    "snapshot_id": snapshot_id,
                    "snapshot_status": snapshot_status,
                    "observation_count": observations,
                    "api_image_id": attestation.api_image_id,
                    "builder_image_id": attestation.builder_image_id,
                }
                proof_phase = "runtime_snapshot_read"
                record = await store.get_snapshot(snapshot_id)
                proof_phase = "snapshot_validation"
                evidence = _validate_snapshot_record(
                    record,
                    snapshot_id=snapshot_id,
                    cutoff=cutoff,
                )

            proof_phase = "authenticated_forecast_request"
            served = await http_get(
                FORECAST_PATH,
                _request_params(snapshot_id),
                api_key,
            )
            proof_http_status = served.status_code
            if served.status_code != 200:
                raise ForecastDemoRefused("the authenticated pinned forecast did not return 200")
            proof_phase = "forecast_response_parse"
            response = _parse_forecast_response(served.content)
            proof_phase = "forecast_response_validation"
            _validate_forecast_response(
                response,
                snapshot_id=snapshot_id,
                cutoff=cutoff,
                evidence=evidence,
            )
            proof_phase = "api_container_revalidation"
            await asyncio.to_thread(runtime_revalidator, attestation)
            proof_phase = "completion_database_clock"
            async with store_factory(safe_settings) as store:
                final_database_now = await store.database_now()
            session_still_current = latest_completed_xnys_session(final_database_now) == end_session

            proof_phase = "vendor_lock_release"
            return {
                "status": "ok" if session_still_current else "sealed_session_advanced",
                **sealed_receipt,
                "target_time_count": DEFAULT_SNAPSHOT_BUILD_POLICY.target_time_count,
                "unauthenticated_http_status": unauthenticated.status_code,
                "wrong_key_http_status": wrong_key.status_code,
                "missing_snapshot_http_status": missing.status_code,
                "authenticated_http_status": served.status_code,
                "model_version": response.provenance.model_version,
                "forecast_count": len(response.forecasts),
                "lookahead_status": response.provenance.lookahead_check.status,
                "calibration_method": response.calibration.method,
                "session_currency_at_completion": (
                    "current" if session_still_current else "advanced_after_seal"
                ),
            }
    except Exception as exc:
        if sealed_receipt is None:
            raise
        return {
            "status": "sealed_proof_failed",
            **sealed_receipt,
            "proof_phase": proof_phase,
            "failure_type": type(exc).__name__,
            "http_status": proof_http_status,
        }


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="read-only exact seal-and-serve plan")
    plan.add_argument("--end", required=True, type=_iso_date)
    execute = subparsers.add_parser("execute", help="run one exact local proof")
    execute.add_argument("--end", required=True, type=_iso_date)
    execute.add_argument("--plan-id", required=True)
    execute.add_argument("--authorization", required=True)
    return parser


def _assert_no_ambient_vendor_environment() -> None:
    if any(os.environ.get(name, "").strip() for name in _VENDOR_SECRET_VARIABLES):
        raise ForecastDemoRefused(
            "vendor credentials must be absent from the forecast-demo process environment"
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging("INFO", json_logs=False, exception_details=False)
    try:
        _assert_no_ambient_vendor_environment()
        if args.command == "plan":
            result = asyncio.run(plan_forecast_demo(end_session=args.end)).public_result()
        else:
            result = asyncio.run(
                execute_forecast_demo(
                    end_session=args.end,
                    plan_id=args.plan_id,
                    authorization=args.authorization,
                )
            )
    except (ForecastDemoRefused, BackfillRefused) as exc:
        print(f"forecast demo refused: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - never echo possibly sensitive exception text.
        print(f"forecast demo failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    if result.get("status") in {"sealed_session_advanced", "sealed_proof_failed"}:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 3
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
