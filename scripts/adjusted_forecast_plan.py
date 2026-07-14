"""Read-only, content-addressed plan for one adjusted MSFT forecast seal.

The plan resolves only local ``stockapi_test`` evidence through the runtime
role.  It performs no vendor calls and no writes.  Its factor cutoff is the
maximum selected input receipt, never a sampled clock, so unchanged evidence
produces the same plan identity across retries and partial-seal recovery.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, Protocol

import exchange_calendars as xcals
import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import and_, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.db.models.adjustment_factors import (
    AdjustmentFactorSetAvailability,
    AdjustmentFactorSetRecord,
)
from app.db.models.bars import Bar, BarVersionAvailability
from app.db.session import build_engine, build_sessionmaker
from app.services.adjusted_forecast_snapshot_builder import (
    ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    ADJUSTED_RESOLUTION_POLICY_HASH,
)
from app.services.adjustment_factor_builder import (
    AdjustmentFactorBuilder,
    AdjustmentFactorBuildError,
    AdjustmentFactorBuildSpec,
)
from app.services.adjustment_factors import (
    ADJUSTMENT_FACTOR_POLICY_HASH,
    ADJUSTMENT_FACTOR_POLICY_VERSION,
    ADJUSTMENT_FACTOR_SET_FORMAT,
    AdjustmentFactorSet,
)
from app.services.corporate_action_store import CorporateActionCollectionEvidence
from app.services.corporate_actions import CORPORATE_ACTION_QUERY_POLICY_HASH
from app.services.market_calendar import latest_completed_xnys_session
from scripts.vendor_acquisition import (
    DEFAULT_LEDGER_PATH,
    LEGACY_LEDGER_PATH,
    AcquisitionPlan,
    plan_acquisition,
)
from scripts.vendor_backfill import (
    BACKFILL_ADJUSTMENT_BASIS,
    BACKFILL_MULTIPLIER,
    BACKFILL_SOURCE,
    BACKFILL_SYMBOL,
    BACKFILL_TIMESPAN,
    REQUIRED_SESSIONS,
    BackfillRefused,
    _clean_git_revision,
    _expected_session_dates,
)

PLAN_CONTRACT_VERSION = 1
ROLLOVER_GUARD_SECONDS = 600
FORECAST_HORIZON = 5
FORECAST_HORIZON_UNIT = "trading_day"
FORECAST_TARGET = "adjusted_close"
FORECAST_MODEL = "baseline_naive"
FORECAST_INTERVAL_COVERAGES = (0.8,)
IDEMPOTENCY_KEY_DERIVATION_VERSION = "stockapi-adjusted-demo-idempotency-v1"
IDEMPOTENCY_KEY_PREFIX = "stockapi-adjusted-demo-"
API_ORIGIN = "http://127.0.0.1:8000"
API_PATH = "/v1/forecast"
API_KEY_HEADER = "X-API-Key"
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")

RevisionFn = Callable[[], str]
SessionsFn = Callable[[date], tuple[date, ...]]
AcquisitionPlanner = Callable[..., Awaitable[AcquisitionPlan]]


class AdjustedForecastPlanRefused(RuntimeError):
    """Planning escaped the exact local read-only contract."""


class AdjustedForecastPlanEnvironment(BaseSettings):
    """Minimal environment surface; vendor credentials are never loaded."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: Literal["local", "test", "staging", "production"] = "local"
    database_url: str
    database_pool_size: int = 5
    database_max_overflow: int = 5
    database_pool_timeout: int = 30
    api_v1_prefix: str = "/v1"
    api_keys: str = ""
    jwt_secret: str = "change_me_random_64_chars"
    forecast_adjusted_close_resolution_policy_hash: str | None = None
    forecast_adjusted_close_trusted_availability_rule_set_hash: str | None = None

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


def _get_plan_settings() -> Settings:
    return AdjustedForecastPlanEnvironment().runtime_settings()


@dataclass(frozen=True, slots=True)
class RawReceiptState:
    """Exact current-version raw receipts in the acquisition window."""

    receipt_count: int
    max_available_at: datetime | None


@dataclass(frozen=True, slots=True)
class ActionCollectionReceiptBinding:
    """One exact immutable action collection and its later receipt."""

    action_type: Literal["split", "dividend"]
    collection_id: str
    collection_recorded_at: datetime
    available_at: datetime
    event_count: int

    def public_result(self) -> dict[str, object]:
        return {
            "action_type": self.action_type,
            "collection_id": self.collection_id,
            "collection_recorded_at": _timestamp(self.collection_recorded_at),
            "available_at": _timestamp(self.available_at),
            "event_count": self.event_count,
        }


@dataclass(frozen=True, slots=True)
class PriorFactorState:
    """Existing expected factor status plus selector-conflicting identities."""

    expected_exists: bool
    expected_factor_set_recorded_at: datetime | None
    expected_available_at: datetime | None
    incompatible_factor_set_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AdjustedForecastSealPlan:
    """Stable, content-addressed adjusted seal and POST request plan."""

    end_session: date
    tool_revision: str
    acquisition_plan_id: str
    window_start: date
    database_now: datetime
    raw_receipt_count: int
    raw_max_available_at: datetime | None
    split_collection_receipt: ActionCollectionReceiptBinding | None
    dividend_collection_receipt: ActionCollectionReceiptBinding | None
    factor_cutoff: datetime | None
    expected_factor_set_id: str | None
    expected_factor_exists: bool
    expected_factor_set_recorded_at: datetime | None
    expected_factor_available_at: datetime | None
    incompatible_factor_set_ids: tuple[str, ...]
    api_key_count: int
    resolution_pin_matches: bool
    availability_pin_matches: bool
    blockers: tuple[str, ...]
    plan_id: str

    @property
    def ready(self) -> bool:
        return not self.blockers

    @property
    def idempotency_key(self) -> str:
        return IDEMPOTENCY_KEY_PREFIX + self.plan_id.removeprefix("sha256:")

    def public_result(self) -> dict[str, object]:
        return {
            "status": "ready" if self.ready else "blocked",
            "plan_id": self.plan_id,
            "tool_revision": self.tool_revision,
            "acquisition_plan_id": self.acquisition_plan_id,
            "symbol": BACKFILL_SYMBOL,
            "window_start": self.window_start.isoformat(),
            "window_end": self.end_session.isoformat(),
            "raw_receipt_count": self.raw_receipt_count,
            "raw_max_available_at": _optional_timestamp(self.raw_max_available_at),
            "split_collection_receipt": _optional_action(self.split_collection_receipt),
            "dividend_collection_receipt": _optional_action(self.dividend_collection_receipt),
            "factor_cutoff": _optional_timestamp(self.factor_cutoff),
            "expected_factor_set_id": self.expected_factor_set_id,
            "expected_factor_exists": self.expected_factor_exists,
            "expected_factor_set_recorded_at": _optional_timestamp(
                self.expected_factor_set_recorded_at
            ),
            "expected_factor_available_at": _optional_timestamp(self.expected_factor_available_at),
            "incompatible_factor_set_ids": list(self.incompatible_factor_set_ids),
            "database_now": _timestamp(self.database_now),
            "resolution_policy_hash": ADJUSTED_RESOLUTION_POLICY_HASH,
            "availability_rule_set_hash": ADJUSTED_AVAILABILITY_RULE_SET_HASH,
            "adjustment_factor_policy_hash": ADJUSTMENT_FACTOR_POLICY_HASH,
            "adjustment_factor_policy_version": ADJUSTMENT_FACTOR_POLICY_VERSION,
            "adjustment_factor_set_format": ADJUSTMENT_FACTOR_SET_FORMAT,
            "corporate_action_query_policy_hash": CORPORATE_ACTION_QUERY_POLICY_HASH,
            "api_auth_configured": self.api_key_count == 1,
            "request": _public_request(),
            "idempotency_key": self.idempotency_key,
            "blockers": list(self.blockers),
        }


class AdjustedForecastPlanStore(Protocol):
    """Read-only evidence seam for deterministic planning."""

    async def database_now(self) -> datetime: ...

    async def raw_receipts(self, session_dates: tuple[date, ...]) -> RawReceiptState: ...

    async def prepare_factor(
        self,
        spec: AdjustmentFactorBuildSpec,
    ) -> AdjustmentFactorSet: ...

    async def prior_factor_state(
        self,
        *,
        factor_cutoff: datetime,
        end_session: date,
        expected_factor_set_id: str,
    ) -> PriorFactorState: ...


StoreFactory = Callable[
    [Settings],
    AbstractAsyncContextManager[AdjustedForecastPlanStore],
]


class SqlAdjustedForecastPlanStore:
    """Runtime-role reads for raw receipts, preparation, and recovery state."""

    def __init__(self, settings: Settings) -> None:
        self._engine: AsyncEngine = build_engine(settings)
        self._maker: async_sessionmaker[AsyncSession] = build_sessionmaker(self._engine)

    async def __aenter__(self) -> SqlAdjustedForecastPlanStore:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._engine.dispose()

    async def database_now(self) -> datetime:
        async with self._maker() as session:
            value = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        return _aware(value, "database clock")

    async def raw_receipts(self, session_dates: tuple[date, ...]) -> RawReceiptState:
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
                func.max(BarVersionAvailability.available_at).label("max_available_at"),
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
        maximum = row.max_available_at
        return RawReceiptState(
            receipt_count=int(row.receipt_count),
            max_available_at=(None if maximum is None else _aware(maximum, "raw receipt maximum")),
        )

    async def prepare_factor(
        self,
        spec: AdjustmentFactorBuildSpec,
    ) -> AdjustmentFactorSet:
        return await AdjustmentFactorBuilder(self._maker).prepare(spec)

    async def prior_factor_state(
        self,
        *,
        factor_cutoff: datetime,
        end_session: date,
        expected_factor_set_id: str,
    ) -> PriorFactorState:
        statement = (
            select(
                AdjustmentFactorSetRecord.factor_set_id,
                AdjustmentFactorSetRecord.recorded_at,
                AdjustmentFactorSetAvailability.available_at,
            )
            .outerjoin(
                AdjustmentFactorSetAvailability,
                AdjustmentFactorSetAvailability.factor_set_id
                == AdjustmentFactorSetRecord.factor_set_id,
            )
            .where(
                AdjustmentFactorSetRecord.symbol == BACKFILL_SYMBOL,
                AdjustmentFactorSetRecord.policy_hash == ADJUSTMENT_FACTOR_POLICY_HASH,
                AdjustmentFactorSetRecord.anchor_date == end_session,
                AdjustmentFactorSetRecord.input_count >= REQUIRED_SESSIONS,
                AdjustmentFactorSetRecord.cutoff >= factor_cutoff,
            )
            .order_by(
                AdjustmentFactorSetRecord.cutoff,
                AdjustmentFactorSetRecord.factor_set_id,
            )
        )
        async with self._maker() as session:
            rows = (await session.execute(statement)).all()
        expected = [row for row in rows if row.factor_set_id == expected_factor_set_id]
        incompatible = tuple(
            sorted(row.factor_set_id for row in rows if row.factor_set_id != expected_factor_set_id)
        )
        if len(expected) > 1:
            incompatible = tuple(sorted((*incompatible, expected_factor_set_id)))
            expected = []
        row = expected[0] if expected else None
        return PriorFactorState(
            expected_exists=row is not None,
            expected_factor_set_recorded_at=(
                None if row is None else _aware(row.recorded_at, "existing factor recorded_at")
            ),
            expected_available_at=(
                None
                if row is None or row.available_at is None
                else _aware(row.available_at, "existing factor available_at")
            ),
            incompatible_factor_set_ids=incompatible,
        )


@asynccontextmanager
async def _sql_store(settings: Settings) -> AsyncIterator[AdjustedForecastPlanStore]:
    async with SqlAdjustedForecastPlanStore(settings) as store:
        yield store


def _safe_settings(settings: Settings) -> Settings:
    if settings.app_env != "local":
        raise AdjustedForecastPlanRefused("APP_ENV must be exactly local")
    try:
        database_url = make_url(settings.database_url)
    except (ArgumentError, ValueError):
        raise AdjustedForecastPlanRefused("DATABASE_URL is not a valid SQLAlchemy URL") from None
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
        raise AdjustedForecastPlanRefused(
            "DATABASE_URL must use stockapi_app on local stockapi_test:5432"
        )
    _configured_api_keys(settings)
    if settings.api_v1_prefix != "/v1":
        raise AdjustedForecastPlanRefused("API_V1_PREFIX must be exactly /v1")
    return settings.model_copy(
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


def _configured_api_keys(settings: Settings) -> tuple[str, ...]:
    keys = tuple(value.strip() for value in settings.api_keys.split(",") if value.strip())
    if any(
        not value.isascii() or any(not 0x21 <= ord(character) <= 0x7E for character in value)
        for value in keys
    ):
        raise AdjustedForecastPlanRefused("API_KEYS must contain only visible ASCII characters")
    return keys


def _valid_binding_secret(secret: str) -> bool:
    return len(secret) >= 32 and secret != "change_me_random_64_chars" and secret.isascii()


def _api_key_binding(settings: Settings, api_keys: tuple[str, ...]) -> str | None:
    secret = settings.jwt_secret.strip()
    if len(api_keys) != 1 or not _valid_binding_secret(secret):
        return None
    digest = hmac.new(
        secret.encode("ascii"),
        b"stockapi-adjusted-forecast-api-key-v1\0" + api_keys[0].encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return "hmac-sha256:" + digest


def _aware(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise AdjustedForecastPlanRefused(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _aware(value, "timestamp").isoformat()


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _session_close(session_date: date) -> datetime:
    calendar = xcals.get_calendar("XNYS")
    return _aware(
        calendar.session_close(pd.Timestamp(session_date)).to_pydatetime(),
        "session close",
    )


def _next_session_close(end_session: date) -> datetime:
    calendar = xcals.get_calendar("XNYS")
    label = pd.Timestamp(end_session)
    if not calendar.is_session(label):
        raise AdjustedForecastPlanRefused("end must be an XNYS trading session")
    return _aware(
        calendar.session_close(calendar.next_session(label)).to_pydatetime(),
        "next XNYS session close",
    )


def _one_action_binding(
    evidence: tuple[CorporateActionCollectionEvidence, ...],
    action_type: Literal["split", "dividend"],
) -> ActionCollectionReceiptBinding | None:
    complete = tuple(value for value in evidence if value.available_at is not None)
    if not complete:
        return None
    selected = max(
        complete,
        key=lambda value: (
            _aware(value.collection_recorded_at, "collection recorded_at"),
            value.collection_id,
        ),
    )
    return ActionCollectionReceiptBinding(
        action_type=action_type,
        collection_id=selected.collection_id,
        collection_recorded_at=_aware(
            selected.collection_recorded_at,
            f"{action_type} collection recorded_at",
        ),
        available_at=_aware(
            selected.available_at,
            f"{action_type} collection available_at",
        ),
        event_count=selected.event_count,
    )


def _optional_action(
    value: ActionCollectionReceiptBinding | None,
) -> dict[str, object] | None:
    return None if value is None else value.public_result()


def _public_request() -> dict[str, object]:
    return {
        "method": "POST",
        "origin": API_ORIGIN,
        "path": API_PATH,
        "target": FORECAST_TARGET,
        "authentication": API_KEY_HEADER,
        "body": {
            "symbol": BACKFILL_SYMBOL,
            "horizon": FORECAST_HORIZON,
            "horizon_unit": FORECAST_HORIZON_UNIT,
            "target": FORECAST_TARGET,
            "model": FORECAST_MODEL,
            "interval_coverages": list(FORECAST_INTERVAL_COVERAGES),
            "snapshot_id": "from_adjusted_seal_result",
        },
        "idempotency_key_derivation_version": (IDEMPOTENCY_KEY_DERIVATION_VERSION),
        "idempotency_key_prefix": IDEMPOTENCY_KEY_PREFIX,
        "idempotency_key_input": "plan_id_hex",
    }


def _sha256_document(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _factor_matches_plan(
    artifact: AdjustmentFactorSet,
    *,
    expected_dates: tuple[date, ...],
    factor_cutoff: datetime,
    raw_state: RawReceiptState,
    split: ActionCollectionReceiptBinding,
    dividend: ActionCollectionReceiptBinding,
) -> bool:
    return bool(
        artifact.symbol == BACKFILL_SYMBOL
        and artifact.format == ADJUSTMENT_FACTOR_SET_FORMAT
        and artifact.policy_hash == ADJUSTMENT_FACTOR_POLICY_HASH
        and artifact.policy_version == ADJUSTMENT_FACTOR_POLICY_VERSION
        and artifact.cutoff == factor_cutoff
        and artifact.anchor_date == expected_dates[-1]
        and len(artifact.raw_inputs) == REQUIRED_SESSIONS
        and tuple(value.observation_date for value in artifact.raw_inputs) == expected_dates
        and raw_state.max_available_at is not None
        and max(value.available_at for value in artifact.raw_inputs) == raw_state.max_available_at
        and artifact.split_collection_id == split.collection_id
        and artifact.dividend_collection_id == dividend.collection_id
        and _HASH_PATTERN.fullmatch(artifact.factor_set_id) is not None
    )


def _build_plan(
    *,
    settings: Settings,
    acquisition_plan: AcquisitionPlan,
    database_now: datetime,
    raw_state: RawReceiptState,
    split: ActionCollectionReceiptBinding | None,
    dividend: ActionCollectionReceiptBinding | None,
    factor_cutoff: datetime | None,
    artifact: AdjustmentFactorSet | None,
    prior: PriorFactorState,
    extra_blockers: tuple[str, ...] = (),
) -> AdjustedForecastSealPlan:
    api_keys = _configured_api_keys(settings)
    api_binding = _api_key_binding(settings, api_keys)
    blockers = list(extra_blockers)
    if len(api_keys) != 1:
        blockers.append("configure exactly one non-empty API_KEYS value")
    if not _valid_binding_secret(settings.jwt_secret.strip()):
        blockers.append("configure a non-default JWT_SECRET of at least 32 ASCII characters")
    resolution_matches = (
        settings.forecast_adjusted_close_resolution_policy_hash == ADJUSTED_RESOLUTION_POLICY_HASH
    )
    availability_matches = (
        settings.forecast_adjusted_close_trusted_availability_rule_set_hash
        == ADJUSTED_AVAILABILITY_RULE_SET_HASH
    )
    if not resolution_matches:
        blockers.append("pin the adjusted-close resolution-policy hash")
    if not availability_matches:
        blockers.append("pin the adjusted-close availability rule-set hash")
    if len(acquisition_plan.price_plan.expected_dates) != REQUIRED_SESSIONS:
        blockers.append("the acquisition window is not exactly 258 XNYS sessions")
    if len(acquisition_plan.price_plan.complete_dates) != REQUIRED_SESSIONS:
        blockers.append("complete all 258 exact raw bar receipts")
    if acquisition_plan.calls:
        blockers.append("finish the exact acquisition plan before adjusted sealing")
    if acquisition_plan.receipt_repairs_required:
        blockers.append("complete all acquisition receipt repairs")
    if acquisition_plan.ambiguous_call_ids or acquisition_plan.price_plan.ambiguous_dates:
        blockers.append("resolve ambiguous prior acquisition attempts")
    if raw_state.receipt_count != REQUIRED_SESSIONS or raw_state.max_available_at is None:
        blockers.append("the exact current raw receipt count is not 258")
    if split is None:
        blockers.append("require exactly one complete split collection and no repair state")
    if dividend is None:
        blockers.append("require exactly one complete dividend collection and no repair state")
    if factor_cutoff is None or artifact is None:
        blockers.append("the exact adjustment-factor artifact is not preparable")
    if prior.incompatible_factor_set_ids:
        blockers.append("incompatible same-or-newer adjustment-factor state exists")
    if latest_completed_xnys_session(database_now) != acquisition_plan.end_session:
        blockers.append("a newer XNYS session has completed; make a fresh plan")
    if (
        _next_session_close(acquisition_plan.end_session) - database_now
    ).total_seconds() < ROLLOVER_GUARD_SECONDS:
        blockers.append("too little time remains before the next XNYS close")
    if factor_cutoff is not None and factor_cutoff > database_now:
        blockers.append("the deterministic factor cutoff is later than the database clock")

    expected_factor_set_id = None if artifact is None else artifact.factor_set_id
    canonical = {
        "version": PLAN_CONTRACT_VERSION,
        "tool_revision": acquisition_plan.tool_revision,
        "acquisition_plan_id": acquisition_plan.plan_id,
        "symbol": BACKFILL_SYMBOL,
        "window_start": acquisition_plan.price_plan.expected_dates[0].isoformat(),
        "window_end": acquisition_plan.end_session.isoformat(),
        "raw_receipt_count": raw_state.receipt_count,
        "raw_max_available_at": _optional_timestamp(raw_state.max_available_at),
        "split_collection_receipt": _optional_action(split),
        "dividend_collection_receipt": _optional_action(dividend),
        "factor_cutoff": _optional_timestamp(factor_cutoff),
        "expected_factor_set_id": expected_factor_set_id,
        "incompatible_factor_set_ids": list(prior.incompatible_factor_set_ids),
        "adjustment_factor_policy_hash": ADJUSTMENT_FACTOR_POLICY_HASH,
        "adjustment_factor_policy_version": ADJUSTMENT_FACTOR_POLICY_VERSION,
        "adjustment_factor_set_format": ADJUSTMENT_FACTOR_SET_FORMAT,
        "corporate_action_query_policy_hash": CORPORATE_ACTION_QUERY_POLICY_HASH,
        "resolution_policy_hash": ADJUSTED_RESOLUTION_POLICY_HASH,
        "availability_rule_set_hash": ADJUSTED_AVAILABILITY_RULE_SET_HASH,
        "resolution_pin_matches": resolution_matches,
        "availability_pin_matches": availability_matches,
        "api_key_count": len(api_keys),
        "api_key_binding": api_binding,
        "rollover_guard_seconds": ROLLOVER_GUARD_SECONDS,
        "request": _public_request(),
        "blockers": blockers,
    }
    return AdjustedForecastSealPlan(
        end_session=acquisition_plan.end_session,
        tool_revision=acquisition_plan.tool_revision,
        acquisition_plan_id=acquisition_plan.plan_id,
        window_start=acquisition_plan.price_plan.expected_dates[0],
        database_now=database_now,
        raw_receipt_count=raw_state.receipt_count,
        raw_max_available_at=raw_state.max_available_at,
        split_collection_receipt=split,
        dividend_collection_receipt=dividend,
        factor_cutoff=factor_cutoff,
        expected_factor_set_id=expected_factor_set_id,
        expected_factor_exists=prior.expected_exists,
        expected_factor_set_recorded_at=prior.expected_factor_set_recorded_at,
        expected_factor_available_at=prior.expected_available_at,
        incompatible_factor_set_ids=prior.incompatible_factor_set_ids,
        api_key_count=len(api_keys),
        resolution_pin_matches=resolution_matches,
        availability_pin_matches=availability_matches,
        blockers=tuple(blockers),
        plan_id=_sha256_document(canonical),
    )


async def plan_adjusted_forecast_seal(
    *,
    end_session: date,
    settings: Settings | None = None,
    store_factory: StoreFactory = _sql_store,
    acquisition_planner: AcquisitionPlanner = plan_acquisition,
    sessions_fn: SessionsFn = _expected_session_dates,
    revision_fn: RevisionFn = _clean_git_revision,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    legacy_ledger_path: Path = LEGACY_LEDGER_PATH,
) -> AdjustedForecastSealPlan:
    """Resolve one stable adjusted seal plan without writes or vendor access."""

    safe_settings = _safe_settings(settings or _get_plan_settings())
    try:
        tool_revision = revision_fn()
        expected_dates = sessions_fn(end_session)
    except BackfillRefused as exc:
        raise AdjustedForecastPlanRefused(str(exc)) from None
    async with store_factory(safe_settings) as store:
        initial_database_now = _aware(
            await store.database_now(),
            "initial database clock",
        )
        try:
            acquisition_plan = await acquisition_planner(
                end_session=end_session,
                settings=safe_settings,
                clock=lambda: initial_database_now,
                sessions_fn=sessions_fn,
                revision_fn=lambda: tool_revision,
                ledger_path=ledger_path,
                legacy_ledger_path=legacy_ledger_path,
            )
        except BackfillRefused as exc:
            raise AdjustedForecastPlanRefused(str(exc)) from None
        raw_state = await store.raw_receipts(expected_dates)
        split = _one_action_binding(
            acquisition_plan.split_state.coverage.collections,
            "split",
        )
        dividend = _one_action_binding(
            acquisition_plan.dividend_state.coverage.collections,
            "dividend",
        )
        factor_cutoff = None
        artifact = None
        prior = PriorFactorState(False, None, None, ())
        extra_blockers: list[str] = []
        if (
            raw_state.receipt_count == REQUIRED_SESSIONS
            and raw_state.max_available_at is not None
            and split is not None
            and dividend is not None
        ):
            factor_cutoff = max(
                raw_state.max_available_at,
                split.available_at,
                dividend.available_at,
            )
            try:
                artifact = await store.prepare_factor(
                    AdjustmentFactorBuildSpec(
                        symbol=BACKFILL_SYMBOL,
                        coverage_start=expected_dates[0],
                        coverage_end=expected_dates[-1],
                        cutoff=factor_cutoff,
                    )
                )
            except AdjustmentFactorBuildError:
                extra_blockers.append(
                    "the selected receipts cannot prepare the exact factor artifact"
                )
            if artifact is not None and not _factor_matches_plan(
                artifact,
                expected_dates=expected_dates,
                factor_cutoff=factor_cutoff,
                raw_state=raw_state,
                split=split,
                dividend=dividend,
            ):
                extra_blockers.append(
                    "the prepared factor artifact differs from the acquisition evidence"
                )
                artifact = None
            if artifact is not None:
                prior = await store.prior_factor_state(
                    factor_cutoff=factor_cutoff,
                    end_session=end_session,
                    expected_factor_set_id=artifact.factor_set_id,
                )
        final_database_now = _aware(
            await store.database_now(),
            "final database clock",
        )
    return _build_plan(
        settings=safe_settings,
        acquisition_plan=acquisition_plan,
        database_now=final_database_now,
        raw_state=raw_state,
        split=split,
        dividend=dividend,
        factor_cutoff=factor_cutoff,
        artifact=artifact,
        prior=prior,
        extra_blockers=tuple(extra_blockers),
    )


__all__ = [
    "API_PATH",
    "ActionCollectionReceiptBinding",
    "AdjustedForecastPlanRefused",
    "AdjustedForecastSealPlan",
    "IDEMPOTENCY_KEY_DERIVATION_VERSION",
    "IDEMPOTENCY_KEY_PREFIX",
    "PriorFactorState",
    "RawReceiptState",
    "SqlAdjustedForecastPlanStore",
    "plan_adjusted_forecast_seal",
]
