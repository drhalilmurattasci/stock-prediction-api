"""Live-database gate: migrations, writes, reads, and immutable snapshots.

Skipped unless ``TEST_DATABASE_URL`` points at a **throwaway** TimescaleDB
through an owner/admin account. The fixture RESETS the project's database
objects (tables and alembic_version are dropped) before applying migrations.
It never creates or mutates the cluster-global runtime roles: bootstrap them
first and provide both least-privilege URLs. Never point this gate at shared
data. Run with::

    docker compose up -d timescaledb          # needs .env credentials
    $env:TEST_DATABASE_URL = "postgresql+asyncpg://<user>:<pass>@127.0.0.1:5432/<db>"
    # Optional when .env DATABASE_URL does not target the same throwaway DB:
    $env:TEST_RUNTIME_DATABASE_URL = "postgresql+asyncpg://stockapi_app:<pass>@127.0.0.1:5432/<db>"
    $env:TEST_SNAPSHOT_BUILDER_DATABASE_URL = "postgresql+asyncpg://stockapi_snapshot_builder:<pass>@127.0.0.1:5432/<db>"
    $env:TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET = "stockapi-test-only"
    uv run pytest tests/integration/test_bars_live_gate.py -v

This is the empirical proof the unit suite cannot give: the Alembic chain
applies against a real hypertable, ``upsert_bars`` replay is a no-op while a
restatement writes a revision row, a seeded two-page ``/v1/prices`` read has
no gaps or duplicates with TIMESTAMPTZ round-tripping, the finiteness CHECKs
actually reject NaN/Infinity under Postgres NaN ordering, and the API
statement-timeout cancels a pathological statement.
The same command also proves forecast-input snapshot SHA-256 enforcement,
idempotent insertion, semantic collision rejection, pure resolution, and
database-level UPDATE/DELETE/TRUNCATE refusal. Migrations ``0010``-``0015`` wire
the same gate to check content-addressed realized outcomes, immutable policy
registration, the direct receipt-writer cutoff fence, source-bound publication,
and cohort manifests whose availability can be sealed only after the manifest
commits and before its first target. Migration ``0015`` additionally exercises
runtime-only fitted-set and descriptive held-out publishers, post-commit release
availability, replay, normalized projections, run-scope binding, and append-only
database enforcement.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import threading
from dataclasses import dataclass, fields, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Self
from uuid import UUID

import exchange_calendars
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import ARRAY, LargeBinary, bindparam, create_engine, func, make_url, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    AsyncSessionTransaction,
    async_sessionmaker,
    create_async_engine,
)

from app.config import Settings
from app.core.exceptions import AppError
from app.db.models.adjustment_factors import (
    AdjustmentFactorEntry,
    AdjustmentFactorSetAvailability,
    AdjustmentFactorSetRecord,
)
from app.db.models.bars import Bar, BarRevision, BarVersionAvailability
from app.db.models.corporate_actions import (
    CorporateActionCollection,
    CorporateActionCollectionMember,
    CorporateActionVersion,
)
from app.db.models.forecast_calibration import (
    ForecastFittedCalibrationSet,
    ForecastHeldoutCoverageRelease,
    ForecastHeldoutCoverageReleaseAvailability,
    ForecastHeldoutCoverageReleaseBucket,
)
from app.db.models.forecast_evidence import (
    ForecastOutcomeCohortAvailability,
    ForecastOutcomeCohortManifest,
    ForecastOutcomeCohortMember,
    ForecastRealizedOutcome,
    ForecastRealizedOutcomePublication,
)
from app.db.models.forecast_snapshots import ForecastInputSnapshot
from app.db.models.predictions import ForecastRun
from app.db.session import build_engine, build_sessionmaker
from app.main import create_app
from app.schemas.forecast import (
    DataSourceLineage,
    ForecastCalibration,
    ForecastInterval,
    ForecastProvenance,
    ForecastQuantile,
    ForecastRequest,
    ForecastResponse,
    ForecastStep,
    LookaheadCheck,
)
from app.schemas.indicators import IndicatorFilters
from app.schemas.prices import AdjustedPriceFilters, PriceFilters
from app.services.adjusted_forecast_snapshot_builder import (
    ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    ADJUSTED_RESOLUTION_POLICY_HASH,
    AdjustedForecastSnapshotBuilder,
    AdjustedSnapshotBuildSpec,
)
from app.services.adjusted_price_store import read_adjusted_prices
from app.services.adjustment_factor_builder import (
    AdjustmentFactorBuilder,
    AdjustmentFactorBuildSpec,
)
from app.services.adjustment_factor_store import SqlAdjustmentFactorSetStore
from app.services.adjustment_factors import (
    AdjustmentFactorSet,
    DividendActionVersion,
    RawCloseVersion,
    build_adjustment_factor_set,
)
from app.services.corporate_action_store import SqlCorporateActionCollectionStore
from app.services.corporate_actions import (
    CORPORATE_ACTION_ORIGIN,
    DIVIDENDS_ENDPOINT,
    SPLITS_ENDPOINT,
    CorporateActionCollectionRecord,
    build_dividend_collection,
    build_split_collection,
)
from app.services.forecast_calibration_evidence import (
    CalibrationFitBucket,
    estimate_heldout_coverage,
    fit_empirical_residual_calibration_set,
)
from app.services.forecast_calibration_evidence_store import (
    SqlForecastCalibrationEvidenceReader,
)
from app.services.forecast_calibration_release_store import (
    SqlHeldoutCoverageReleaseStore,
)
from app.services.forecast_calibration_releases import (
    HELDOUT_COVERAGE_RELEASE_SCOPE,
    build_heldout_coverage_release,
)
from app.services.forecast_calibration_sets import (
    calibration_set_version_for,
    canonical_calibration_set,
)
from app.services.forecast_cohort_store import ForecastCohortProof, SqlForecastCohortStore
from app.services.forecast_cohorts import (
    CohortPurpose,
    ForecastCohortManifest,
    ForecastCohortMember,
    canonical_cohort_manifest,
    cohort_id_for_manifest,
    member_from_scheduled_run,
)
from app.services.forecast_outcome_policy_store import SqlForecastOutcomePolicyStore
from app.services.forecast_outcome_resolution import (
    ForecastOutcomeResolutionPolicy,
    SqlOutcomeBarVersionResolver,
)
from app.services.forecast_outcome_store import (
    ForecastOutcomePublicationSource,
    SqlForecastOutcomeStore,
)
from app.services.forecast_outcomes import (
    BarVersionEvidence,
    RealizedOutcomePayload,
    canonical_outcome_payload,
    outcome_id_for_payload,
)
from app.services.forecast_run_store import SqlForecastRunStore
from app.services.forecast_runs import (
    canonical_output,
    canonical_request,
    idempotency_digest,
    opportunity_hash,
    output_hash,
    parse_output,
    request_hash,
)
from app.services.forecast_serving import (
    ForecastServingPolicy,
    SnapshotForecastService,
    SqlForecastInputSnapshotRepository,
)
from app.services.forecast_snapshot_builder import (
    DEFAULT_AVAILABILITY_RULE_SET_HASH,
    DEFAULT_RESOLUTION_POLICY_HASH,
    ForecastSnapshotBuilder,
    SnapshotBuildSpec,
    database_snapshot_cutoff,
)
from app.services.forecast_snapshots import (
    ForecastInputSnapshotPayload,
    ForecastInputSnapshotRecord,
    SnapshotAvailabilityEvidence,
    SnapshotObservation,
    SnapshotSourceLineage,
    build_snapshot_record,
    parse_snapshot_payload,
    validate_and_resolve_snapshot,
)
from app.services.indicators import WINDOW_POLICY_HASH, read_indicators
from app.services.market_calendar import latest_completed_xnys_session
from app.services.prices import read_prices
from app.services.scheduled_evaluation import (
    ScheduledEvaluationService,
    ScheduledEvaluationSpec,
)
from data_sources.base import Dividend, DividendPage, OHLCVBar, Split, SplitPage
from ingestion.locks import (
    VendorOperationBusy,
    bar_series_lock_id,
    exclusive_vendor_operation,
    vendor_operation_lock_id,
)
from ingestion.upsert import (
    BarVersionConflictError,
    finalize_bar_version_availability,
    upsert_bars,
)
from scripts.vendor_backfill import (
    BackfillRefused,
    SqlBackfillStore,
    _exclusive_backfill,
    _session_close,
)

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")
if not TEST_DATABASE_URL:
    pytest.skip("TEST_DATABASE_URL not set - live-DB gate skipped", allow_module_level=True)
if os.environ.get("TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET") != "stockapi-test-only":
    raise RuntimeError(
        "set TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET=stockapi-test-only only for the "
        "owner-designated throwaway database"
    )

REPO_ROOT = Path(__file__).resolve().parents[2]

_PUBLISH_CORPORATE_ACTION_COLLECTION = text(
    "SELECT public.publish_corporate_action_collection(:manifest, :events)"
).bindparams(
    bindparam("manifest", type_=LargeBinary()),
    bindparam("events", type_=ARRAY(LargeBinary())),
)
_PUBLISH_CORPORATE_ACTION_RECEIPT = text(
    "SELECT collection_id, collection_recorded_at, available_at "
    "FROM public.publish_corporate_action_collection_receipt(:collection_id)"
)
_PUBLISH_ADJUSTMENT_FACTOR_SET = text(
    "SELECT public.publish_adjustment_factor_set(:payload)"
).bindparams(bindparam("payload", type_=LargeBinary()))
_PUBLISH_ADJUSTMENT_FACTOR_RECEIPT = text(
    "SELECT factor_set_id, factor_set_recorded_at, available_at "
    "FROM public.publish_adjustment_factor_set_receipt(:factor_set_id)"
)
_PUBLISH_VENDOR_ACQUISITION_ANCHOR = text(
    "SELECT checkpoint_number, ledger_sha256, campaign_id, "
    "campaign_checkpoint_number, campaign_ledger_sha256, base_calls, "
    "authorized_calls, reserved_calls "
    "FROM public.publish_vendor_acquisition_campaign_anchor("
    ":checkpoint_number, :ledger_sha256, :campaign_id, "
    ":campaign_checkpoint_number, :campaign_ledger_sha256, :base_calls, "
    ":authorized_calls, :reserved_calls)"
)


class _MissingReadBarrier:
    """Release two real sessions only after both observed one row missing."""

    def __init__(
        self,
        model: type[Any],
        *,
        rounds: int = 1,
        barrier_on_execute: bool = False,
    ) -> None:
        self.model = model
        self.barrier_on_execute = barrier_on_execute
        self.arrivals = 0
        self._rounds = rounds
        self._lock = asyncio.Lock()
        self._releases = [asyncio.Event() for _ in range(rounds)]

    async def arrive(self) -> None:
        async with self._lock:
            self.arrivals += 1
            round_index = (self.arrivals - 1) // 2
            if round_index >= self._rounds:
                return
            release = self._releases[round_index]
            if self.arrivals % 2 == 0:
                release.set()
        await asyncio.wait_for(release.wait(), timeout=5)


class _BarrierSession:
    """Minimal AsyncSession proxy used to force a live primary-key race."""

    def __init__(self, session: AsyncSession, barrier: _MissingReadBarrier) -> None:
        self._session = session
        self._barrier = barrier

    async def __aenter__(self) -> Self:
        await self._session.__aenter__()
        return self

    async def __aexit__(self, *error: object) -> bool | None:
        return await self._session.__aexit__(*error)

    def begin(self) -> AsyncSessionTransaction:
        return self._session.begin()

    async def get(self, model: type[Any], identity: object) -> Any:
        row = await self._session.get(model, identity)
        if model is self._barrier.model and row is None:
            await self._barrier.arrive()
        return row

    async def execute(self, *args: object, **kwargs: object) -> Any:
        result = await self._session.execute(*args, **kwargs)
        if self._barrier.barrier_on_execute:
            await self._barrier.arrive()
        return result

    def add(self, row: object) -> None:
        self._session.add(row)

    async def flush(self) -> None:
        await self._session.flush()

    async def refresh(self, row: object) -> None:
        await self._session.refresh(row)


class _BarrierSessionMaker:
    def __init__(
        self,
        maker: async_sessionmaker[AsyncSession],
        barrier: _MissingReadBarrier,
    ) -> None:
        self._maker = maker
        self._barrier = barrier

    def __call__(self) -> _BarrierSession:
        return _BarrierSession(self._maker(), self._barrier)


@dataclass(frozen=True, repr=False)
class LiveDatabaseUrls:
    owner: str
    runtime: str
    snapshot_builder: str


def _async_url(url: str) -> str:
    if "+asyncpg" in url:
        return url
    return url.replace("postgresql://", "postgresql+asyncpg://")


def _sync_url(url: str) -> str:
    return _async_url(url).replace("+asyncpg", "+psycopg")


def _validated_owner_url(url: str) -> str:
    """Bind destructive reset authority to the one local throwaway target."""

    parsed = make_url(_async_url(url))
    target = (
        parsed.drivername,
        parsed.username,
        (parsed.host or "").lower(),
        parsed.port or 5432,
        parsed.database,
    )
    allowed = {
        ("postgresql+asyncpg", "stockapi_owner", "localhost", 5432, "stockapi_test"),
        ("postgresql+asyncpg", "stockapi_owner", "127.0.0.1", 5432, "stockapi_test"),
    }
    if target not in allowed or not parsed.password or parsed.query:
        raise ValueError("TEST_DATABASE_URL must use stockapi_owner on local stockapi_test:5432")
    return parsed.render_as_string(hide_password=False)


def _runtime_url_for(owner_url: str) -> str:
    configured = os.environ.get("TEST_RUNTIME_DATABASE_URL") or Settings().database_url
    runtime_url = make_url(_async_url(configured))
    owner = make_url(owner_url)
    if runtime_url.username != "stockapi_app":
        raise ValueError("live gate runtime URL must use the non-owner stockapi_app role")
    owner_target = (owner.host, owner.port or 5432, owner.database)
    runtime_target = (runtime_url.host, runtime_url.port or 5432, runtime_url.database)
    if runtime_target != owner_target:
        raise ValueError("live gate owner and runtime URLs must target the same throwaway database")
    if owner.username == runtime_url.username:
        raise ValueError("TEST_DATABASE_URL must use a distinct owner/admin account")
    return runtime_url.render_as_string(hide_password=False)


def _snapshot_builder_url_for(owner_url: str) -> str:
    configured = os.environ.get("TEST_SNAPSHOT_BUILDER_DATABASE_URL", "")
    if not configured:
        raise ValueError(
            "TEST_SNAPSHOT_BUILDER_DATABASE_URL must name the dedicated throwaway-DB role"
        )
    builder_url = make_url(_async_url(configured))
    owner = make_url(owner_url)
    if builder_url.username != "stockapi_snapshot_builder":
        raise ValueError("live gate builder URL must use stockapi_snapshot_builder")
    owner_target = (owner.host, owner.port or 5432, owner.database)
    builder_target = (builder_url.host, builder_url.port or 5432, builder_url.database)
    if builder_target != owner_target:
        raise ValueError("live gate owner and builder URLs must target the same throwaway database")
    return builder_url.render_as_string(hide_password=False)


def _drop_project_schema(url: str) -> None:
    sync_engine = create_engine(_sync_url(url))
    try:
        with sync_engine.begin() as conn:
            conn.execute(
                text(
                    "DROP TABLE IF EXISTS "
                    "forecast_heldout_coverage_release_availability, "
                    "forecast_heldout_coverage_release_buckets, "
                    "forecast_heldout_coverage_releases, "
                    "forecast_fitted_calibration_sets, "
                    "adjustment_factor_set_availability, "
                    "adjustment_factor_entries, adjustment_factor_sets, "
                    "vendor_acquisition_campaign_anchors, "
                    "corporate_action_collection_availability, "
                    "corporate_action_collection_members, corporate_action_collections, "
                    "corporate_action_versions, forecast_outcome_cohort_availability, "
                    "forecast_outcome_cohort_members, forecast_outcome_cohort_manifests, "
                    "forecast_realized_outcome_publications, forecast_realized_outcomes, "
                    "forecast_outcome_resolution_policies, forecast_runs, "
                    "forecast_input_snapshots, "
                    "bar_version_availability, bars_revisions, bars, alembic_version CASCADE"
                )
            )
            for function_name in (
                "reject_forecast_input_snapshot_mutation",
                "stamp_forecast_input_snapshot_sealed_at",
                "reject_bar_history_mutation",
                "require_bar_revision_version_evidence",
                "version_bar_write",
                "stamp_bar_version_availability",
                "reject_forecast_run_mutation",
                "stamp_forecast_run_recorded_at",
                "reject_forecast_evidence_mutation",
                "stamp_forecast_realized_outcome",
                "stamp_forecast_outcome_cohort_manifest",
                "stamp_forecast_outcome_cohort_availability",
                "validate_forecast_outcome_cohort_member",
                "materialize_forecast_outcome_cohort_members",
                "fence_bar_version_availability",
                "stamp_forecast_outcome_resolution_policy",
                "validate_forecast_realized_outcome_policy",
                "stamp_corporate_action_evidence",
                "stamp_corporate_action_collection_member",
                "reject_corporate_action_mutation",
                "stamp_corporate_action_collection_availability",
                "stamp_adjustment_factor_set",
                "stamp_adjustment_factor_entry",
                "reject_adjustment_factor_mutation",
                "stamp_adjustment_factor_set_availability",
                "reject_vendor_acquisition_anchor_mutation",
                "reject_forecast_calibration_evidence_mutation",
                "stamp_forecast_heldout_coverage_release_availability",
            ):
                conn.execute(text(f"DROP FUNCTION IF EXISTS {function_name}()"))
            conn.execute(text("DROP FUNCTION IF EXISTS publish_fitted_calibration_set(bytea)"))
            conn.execute(
                text("DROP FUNCTION IF EXISTS publish_forecast_heldout_coverage_release(bytea)")
            )
            conn.execute(text("DROP FUNCTION IF EXISTS canonical_forecast_calibration_json(jsonb)"))
            conn.execute(
                text(
                    "DROP FUNCTION IF EXISTS "
                    "publish_forecast_heldout_coverage_release_receipt(varchar)"
                )
            )
            conn.execute(
                text("DROP FUNCTION IF EXISTS forecast_bar_series_fence_id(text, text, text)")
            )
            conn.execute(
                text("DROP FUNCTION IF EXISTS register_forecast_outcome_resolution_policy(bytea)")
            )
            conn.execute(
                text(
                    "DROP FUNCTION IF EXISTS publish_forecast_realized_outcome("
                    "varchar, uuid, smallint, varchar, bytea)"
                )
            )
            conn.execute(
                text("DROP FUNCTION IF EXISTS corporate_action_series_fence_id(text,text,text)")
            )
            conn.execute(
                text("DROP FUNCTION IF EXISTS publish_corporate_action_collection(bytea,bytea[])")
            )
            conn.execute(
                text("DROP FUNCTION IF EXISTS publish_corporate_action_collection_receipt(text)")
            )
            conn.execute(text("DROP FUNCTION IF EXISTS canonical_corporate_action_json(jsonb)"))
            conn.execute(text("DROP FUNCTION IF EXISTS parse_corporate_action_date(text)"))
            conn.execute(text("DROP FUNCTION IF EXISTS parse_corporate_action_timestamp(text)"))
            conn.execute(text("DROP FUNCTION IF EXISTS parse_corporate_action_decimal(text)"))
            for function_name in (
                "canonical_adjustment_factor_json(jsonb)",
                "adjustment_decimal34(numeric)",
                "adjustment_divide34(numeric,numeric)",
                "adjustment_decimal_text(numeric)",
                "parse_adjustment_timestamp(text)",
                "parse_adjustment_date(text)",
                "parse_adjustment_decimal(text)",
                "adjustment_factor_series_fence_id(text)",
                "publish_adjustment_factor_set(bytea)",
                "publish_adjustment_factor_set_receipt(text)",
            ):
                conn.execute(text(f"DROP FUNCTION IF EXISTS {function_name}"))
            conn.execute(
                text(
                    "DROP FUNCTION IF EXISTS publish_vendor_acquisition_campaign_anchor("
                    "bigint,text,text,bigint,text,integer,integer,integer)"
                )
            )
    finally:
        sync_engine.dispose()


def _invoke_alembic(url: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.pop("ALEMBIC_CONFIG", None)
    environment.update({"DATABASE_URL": url, "MIGRATION_DATABASE_URL": url})
    return subprocess.run(  # fresh process: env.py resolves DATABASE_URL uncached
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(REPO_ROOT / "alembic.ini"),
            *arguments,
        ],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=300,
    )


def _run_alembic(url: str, *arguments: str) -> None:
    result = _invoke_alembic(url, *arguments)
    assert result.returncode == 0, (
        f"alembic {' '.join(arguments)} failed:\n{result.stdout}\n{result.stderr}"
    )


@pytest.fixture(scope="module")
def migrated_database_url() -> LiveDatabaseUrls:
    """Reset/migrate the throwaway DB, then restore an empty migrated schema."""
    url = _validated_owner_url(TEST_DATABASE_URL)
    runtime_url = _runtime_url_for(url)
    snapshot_builder_url = _snapshot_builder_url_for(url)
    guard_engine = create_engine(_sync_url(url))
    guard_connection = guard_engine.connect()
    acquired = bool(
        guard_connection.execute(
            text("SELECT pg_try_advisory_lock(:lock_id)"),
            {"lock_id": vendor_operation_lock_id()},
        ).scalar_one()
    )
    guard_connection.commit()
    if not acquired:
        guard_connection.close()
        guard_engine.dispose()
        raise RuntimeError("another controlled vendor/operator lane is active")
    try:
        _drop_project_schema(url)
        _run_alembic(url, "upgrade", "head")
        for direction, target in (
            ("downgrade", "0007_snapshot_builder_privileges"),
            ("upgrade", "head"),
        ):
            _run_alembic(url, direction, target)
        urls = LiveDatabaseUrls(
            owner=url,
            runtime=runtime_url,
            snapshot_builder=snapshot_builder_url,
        )
        yield urls
    finally:
        # The gate may seed the exact vendor lane. Restore a clean migrated
        # throwaway database so a later one-request smoke still proves absence.
        try:
            _drop_project_schema(url)
            _run_alembic(url, "upgrade", "head")
        finally:
            guard_connection.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": vendor_operation_lock_id()},
            )
            guard_connection.commit()
            guard_connection.close()
            guard_engine.dispose()


@pytest.fixture
async def engine(migrated_database_url: LiveDatabaseUrls) -> AsyncEngine:
    engine = create_async_engine(migrated_database_url.runtime)
    yield engine
    await engine.dispose()


@pytest.fixture
async def owner_engine(migrated_database_url: LiveDatabaseUrls) -> AsyncEngine:
    engine = create_async_engine(migrated_database_url.owner)
    yield engine
    await engine.dispose()


@pytest.fixture
async def snapshot_builder_engine(
    migrated_database_url: LiveDatabaseUrls,
) -> AsyncEngine:
    engine = create_async_engine(migrated_database_url.snapshot_builder)
    yield engine
    await engine.dispose()


def _bar(
    day: int,
    *,
    symbol: str = "AAPL",
    close: float = 101.0,
    fetched_hour: int = 1,
) -> OHLCVBar:
    ts = datetime(2026, 7, day, tzinfo=UTC)
    return OHLCVBar(
        symbol=symbol,
        timestamp=ts,
        timespan="day",
        multiplier=1,
        open=close - 1.0,
        high=close + 1.0,
        low=close - 2.0,
        close=close,
        volume=1_000.0 + day,
        vwap=close - 0.5,
        trade_count=10 + day,
        source="polygon",
        fetched_at=ts + timedelta(hours=fetched_hour),
    )


def _dividend_collection(
    *,
    symbol: str,
    request_id: str,
    fetched_at: datetime,
    cash_amount: str | None,
) -> CorporateActionCollectionRecord:
    start = date(2026, 1, 1)
    end = date(2026, 7, 13)
    results: tuple[Dividend, ...] = ()
    if cash_amount is not None:
        amount = Decimal(cash_amount)
        results = (
            Dividend(
                provider_event_id="dividend-1",
                symbol=symbol,
                ex_dividend_date=date(2026, 5, 14),
                cash_amount=amount,
                split_adjusted_cash_amount=amount,
                historical_adjustment_factor=Decimal("0.9975"),
                currency="USD",
                declaration_date=date(2026, 3, 10),
                record_date=date(2026, 5, 15),
                pay_date=date(2026, 6, 12),
                frequency=4,
                distribution_type="recurring",
                source="polygon",
                fetched_at=fetched_at,
            ),
        )
    return build_dividend_collection(
        DividendPage(
            provider_request_id=request_id,
            provider_origin=CORPORATE_ACTION_ORIGIN,
            endpoint=DIVIDENDS_ENDPOINT,
            symbol=symbol,
            start=start,
            end=end,
            source="polygon",
            fetched_at=fetched_at,
            results=results,
        )
    )


def _split_collection(
    *,
    symbol: str,
    request_id: str,
    fetched_at: datetime,
) -> CorporateActionCollectionRecord:
    start = date(2026, 1, 1)
    end = date(2026, 7, 13)
    return build_split_collection(
        SplitPage(
            provider_request_id=request_id,
            provider_origin=CORPORATE_ACTION_ORIGIN,
            endpoint=SPLITS_ENDPOINT,
            symbol=symbol,
            start=start,
            end=end,
            source="polygon",
            fetched_at=fetched_at,
            results=(
                Split(
                    provider_event_id="split-1",
                    symbol=symbol,
                    execution_date=date(2026, 4, 15),
                    split_from=Decimal("1"),
                    split_to=Decimal("2"),
                    adjustment_type="forward_split",
                    historical_adjustment_factor=Decimal("0.5"),
                    source="polygon",
                    fetched_at=fetched_at,
                ),
            ),
        )
    )


async def _publish_corporate_action_content(
    engine: AsyncEngine,
    record: CorporateActionCollectionRecord,
) -> str:
    async with engine.begin() as conn:
        return (
            await conn.execute(
                _PUBLISH_CORPORATE_ACTION_COLLECTION,
                {
                    "manifest": record.canonical_manifest,
                    "events": [member.canonical_event for member in record.members],
                },
            )
        ).scalar_one()


def _canonical_bytes(document: object) -> bytes:
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _factor_split_collection(
    *,
    symbol: str,
    request_id: str,
    start: date,
    end: date,
    fetched_at: datetime,
) -> CorporateActionCollectionRecord:
    return build_split_collection(
        SplitPage(
            provider_request_id=request_id,
            provider_origin=CORPORATE_ACTION_ORIGIN,
            endpoint=SPLITS_ENDPOINT,
            symbol=symbol,
            start=start,
            end=end,
            source="polygon",
            fetched_at=fetched_at,
            results=(),
        )
    )


def _factor_dividend_collection(
    *,
    symbol: str,
    request_id: str,
    start: date,
    end: date,
    ex_dividend_date: date,
    cash_amount: str | None,
    fetched_at: datetime,
) -> CorporateActionCollectionRecord:
    results: tuple[Dividend, ...] = ()
    if cash_amount is not None:
        amount = Decimal(cash_amount)
        results = (
            Dividend(
                provider_event_id=f"{symbol.lower()}-dividend-1",
                symbol=symbol,
                ex_dividend_date=ex_dividend_date,
                cash_amount=amount,
                split_adjusted_cash_amount=amount,
                historical_adjustment_factor=Decimal("1"),
                currency="USD",
                declaration_date=start,
                record_date=ex_dividend_date,
                pay_date=end,
                frequency=4,
                distribution_type="recurring",
                source="polygon",
                fetched_at=fetched_at,
            ),
        )
    return build_dividend_collection(
        DividendPage(
            provider_request_id=request_id,
            provider_origin=CORPORATE_ACTION_ORIGIN,
            endpoint=DIVIDENDS_ENDPOINT,
            symbol=symbol,
            start=start,
            end=end,
            source="polygon",
            fetched_at=fetched_at,
            results=results,
        )
    )


def _factor_dividend_versions(
    collection: CorporateActionCollectionRecord,
) -> tuple[DividendActionVersion, ...]:
    versions: list[DividendActionVersion] = []
    for member in collection.members:
        assert member.cash_amount is not None
        assert member.currency is not None
        assert member.distribution_type is not None
        versions.append(
            DividendActionVersion(
                provider_event_id=member.provider_event_id,
                version_id=member.action_version_id,
                ex_dividend_date=member.effective_date,
                cash_amount=member.cash_amount,
                currency=member.currency,
                distribution_type=member.distribution_type,
            )
        )
    return tuple(versions)


async def _factor_database_now(engine: AsyncEngine) -> datetime:
    async with engine.connect() as conn:
        return (await conn.execute(select(func.clock_timestamp()))).scalar_one()


async def _factor_raw_versions(
    engine: AsyncEngine,
    symbol: str,
) -> tuple[RawCloseVersion, ...]:
    async with async_sessionmaker(engine, expire_on_commit=False)() as session:
        rows = (
            await session.execute(
                select(Bar, BarVersionAvailability.available_at)
                .join(
                    BarVersionAvailability,
                    (
                        (BarVersionAvailability.symbol == Bar.symbol)
                        & (BarVersionAvailability.timespan == Bar.timespan)
                        & (BarVersionAvailability.multiplier == Bar.multiplier)
                        & (BarVersionAvailability.ts == Bar.ts)
                        & (BarVersionAvailability.source == Bar.source)
                        & (BarVersionAvailability.adjustment_basis == Bar.adjustment_basis)
                        & (BarVersionAvailability.version_recorded_at == Bar.recorded_at)
                    ),
                )
                .where(
                    Bar.symbol == symbol,
                    Bar.timespan == "day",
                    Bar.multiplier == 1,
                    Bar.source == "polygon_open_close",
                    Bar.adjustment_basis == "raw",
                )
                .order_by(Bar.ts)
            )
        ).all()
    return tuple(
        RawCloseVersion(
            observation_date=bar.ts.astimezone(UTC).date(),
            observed_at=bar.ts,
            timespan=bar.timespan,
            multiplier=bar.multiplier,
            source=bar.source,
            adjustment_basis=bar.adjustment_basis,
            version_recorded_at=bar.recorded_at,
            available_at=available_at,
            close=Decimal(str(bar.close)),
        )
        for bar, available_at in rows
    )


async def _seed_factor_raw_series(
    engine: AsyncEngine,
    *,
    symbol: str,
    sessions: tuple[date, ...],
    closes: tuple[float, ...],
    fetched_at: datetime | None = None,
    allow_receipt_reconciliation: bool = False,
) -> tuple[RawCloseVersion, ...]:
    calendar = exchange_calendars.get_calendar(
        "XNYS",
        start="1990-01-01",
        end="2100-12-31",
    )
    bars = []
    for session_date, close in zip(sessions, closes, strict=True):
        observed_at = calendar.session_close(pd.Timestamp(session_date)).to_pydatetime()
        bars.append(
            OHLCVBar(
                symbol=symbol,
                timestamp=observed_at,
                timespan="day",
                multiplier=1,
                open=close - 1,
                high=close + 1,
                low=close - 2,
                close=close,
                volume=1_000,
                source="polygon_open_close",
                adjustment_basis="raw",
                fetched_at=fetched_at or observed_at + timedelta(minutes=1),
            )
        )
    maker = build_sessionmaker(engine)
    async with maker() as session, session.begin():
        plan = await upsert_bars(session, bars)
    async with maker() as session, session.begin():
        finalized = await finalize_bar_version_availability(session, plan.rows)
        if allow_receipt_reconciliation:
            assert finalized >= len(bars)
        else:
            assert finalized == len(bars)
    return await _factor_raw_versions(engine, symbol)


async def _restate_factor_bar(
    engine: AsyncEngine,
    *,
    symbol: str,
    session_date: date,
    close: float,
) -> tuple[RawCloseVersion, ...]:
    calendar = exchange_calendars.get_calendar(
        "XNYS",
        start="1990-01-01",
        end="2100-12-31",
    )
    observed_at = calendar.session_close(pd.Timestamp(session_date)).to_pydatetime()
    bar = OHLCVBar(
        symbol=symbol,
        timestamp=observed_at,
        timespan="day",
        multiplier=1,
        open=close - 1,
        high=close + 1,
        low=close - 2,
        close=close,
        volume=1_001,
        source="polygon_open_close",
        adjustment_basis="raw",
        fetched_at=observed_at + timedelta(minutes=2),
    )
    maker = build_sessionmaker(engine)
    async with maker() as session, session.begin():
        plan = await upsert_bars(session, [bar])
    async with maker() as session, session.begin():
        assert await finalize_bar_version_availability(session, plan.rows) == 1
    return await _factor_raw_versions(engine, symbol)


async def _factor_scenario(
    engine: AsyncEngine,
    *,
    symbol: str,
    cash_amount: str | None = "0.50",
) -> tuple[
    AdjustmentFactorSet,
    CorporateActionCollectionRecord,
    CorporateActionCollectionRecord,
]:
    sessions = (date(2026, 7, 8), date(2026, 7, 9), date(2026, 7, 10))
    raw = await _seed_factor_raw_series(
        engine,
        symbol=symbol,
        sessions=sessions,
        closes=(100.0, 101.0, 102.0),
    )
    split_collection = _factor_split_collection(
        symbol=symbol,
        request_id=f"{symbol.lower()}-splits",
        start=sessions[0],
        end=sessions[-1],
        fetched_at=datetime(2026, 7, 10, 21, tzinfo=UTC),
    )
    dividend_collection = _factor_dividend_collection(
        symbol=symbol,
        request_id=f"{symbol.lower()}-dividends",
        start=sessions[0],
        end=sessions[-1],
        ex_dividend_date=sessions[1],
        cash_amount=cash_amount,
        fetched_at=datetime(2026, 7, 10, 21, 1, tzinfo=UTC),
    )
    action_store = SqlCorporateActionCollectionStore(engine)
    await action_store.publish(split_collection)
    await action_store.publish(dividend_collection)
    cutoff = await _factor_database_now(engine)
    artifact = build_adjustment_factor_set(
        symbol=symbol,
        cutoff=cutoff,
        raw_closes=raw,
        split_collection_id=split_collection.collection_id,
        splits=(),
        dividend_collection_id=dividend_collection.collection_id,
        dividends=_factor_dividend_versions(dividend_collection),
    )
    return artifact, split_collection, dividend_collection


async def _assert_immutability_triggers(
    engine: AsyncEngine,
    tables: tuple[str, ...],
) -> None:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT relation.relname, trigger.tgname "
                    "FROM pg_trigger AS trigger "
                    "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
                    "WHERE relation.relname::text = ANY(CAST(:tables AS text[])) "
                    "AND NOT trigger.tgisinternal"
                ),
                {"tables": list(tables)},
            )
        ).all()
    actual = {(row.relname, row.tgname) for row in rows}
    for table_name in tables:
        assert (table_name, f"{table_name}_no_row_mutation") in actual
        assert (table_name, f"{table_name}_no_truncate") in actual


async def test_migration_chain_applies_and_bars_is_a_hypertable(
    owner_engine: AsyncEngine,
) -> None:
    async with owner_engine.connect() as conn:
        version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
        assert version == "0015_calibration_evidence"
        hypertables = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM timescaledb_information.hypertables "
                    "WHERE hypertable_name = 'bars'"
                )
            )
        ).scalar_one()
        assert hypertables == 1
        series_index = (
            await conn.execute(
                text("SELECT count(*) FROM pg_indexes WHERE indexname = 'ix_bars_series_ts'")
            )
        ).scalar_one()
        assert series_index == 1
        snapshot_index = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_indexes "
                    "WHERE indexname = 'ix_forecast_input_snapshots_resolve'"
                )
            )
        ).scalar_one()
        assert snapshot_index == 1
        run_indexes = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_indexes WHERE indexname IN "
                    "('ix_forecast_runs_opportunity_hash', "
                    "'uq_forecast_runs_scheduled_opportunity')"
                )
            )
        ).scalar_one()
        assert run_indexes == 2
        pgcrypto = (
            await conn.execute(text("SELECT count(*) FROM pg_extension WHERE extname = 'pgcrypto'"))
        ).scalar_one()
        assert pgcrypto == 1
        runtime_role = (
            await conn.execute(
                text(
                    "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, "
                    "rolbypassrls, rolinherit, rolcanlogin "
                    "FROM pg_roles WHERE rolname = 'stockapi_app'"
                )
            )
        ).one()
        assert tuple(runtime_role) == (False, False, False, False, False, False, True)
        memberships = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_auth_members AS membership "
                    "JOIN pg_roles AS role ON role.oid = membership.member "
                    "WHERE role.rolname = 'stockapi_app'"
                )
            )
        ).scalar_one()
        assert memberships == 0
        role_settings = (
            await conn.execute(
                text(
                    "SELECT setting.setdatabase, setting.setconfig "
                    "FROM pg_db_role_setting AS setting "
                    "JOIN pg_roles AS role ON role.oid = setting.setrole "
                    "WHERE role.rolname = 'stockapi_app'"
                )
            )
        ).one()
        assert role_settings.setdatabase == 0
        assert role_settings.setconfig == ["search_path=pg_catalog, public"]
        privileges = {
            name: (
                await conn.execute(
                    text("SELECT has_table_privilege('stockapi_app', :table_name, :privilege)"),
                    {"table_name": table_name, "privilege": privilege},
                )
            ).scalar_one()
            for name, table_name, privilege in (
                ("bars_select", "bars", "SELECT"),
                ("bars_insert", "bars", "INSERT"),
                ("bars_update", "bars", "UPDATE"),
                ("bars_delete", "bars", "DELETE"),
                ("bars_truncate", "bars", "TRUNCATE"),
                ("bars_references", "bars", "REFERENCES"),
                ("bars_trigger", "bars", "TRIGGER"),
                ("bars_maintain", "bars", "MAINTAIN"),
                ("revision_select", "bars_revisions", "SELECT"),
                ("revision_insert", "bars_revisions", "INSERT"),
                ("revision_update", "bars_revisions", "UPDATE"),
                ("revision_delete", "bars_revisions", "DELETE"),
                ("revision_truncate", "bars_revisions", "TRUNCATE"),
                ("availability_select", "bar_version_availability", "SELECT"),
                ("availability_insert", "bar_version_availability", "INSERT"),
                ("availability_update", "bar_version_availability", "UPDATE"),
                ("availability_delete", "bar_version_availability", "DELETE"),
                ("availability_references", "bar_version_availability", "REFERENCES"),
                ("availability_trigger", "bar_version_availability", "TRIGGER"),
                ("availability_maintain", "bar_version_availability", "MAINTAIN"),
                ("snapshot_select", "forecast_input_snapshots", "SELECT"),
                ("snapshot_insert", "forecast_input_snapshots", "INSERT"),
                ("snapshot_update", "forecast_input_snapshots", "UPDATE"),
                ("snapshot_delete", "forecast_input_snapshots", "DELETE"),
                ("snapshot_truncate", "forecast_input_snapshots", "TRUNCATE"),
                ("run_select", "forecast_runs", "SELECT"),
                ("run_insert", "forecast_runs", "INSERT"),
                ("run_update", "forecast_runs", "UPDATE"),
                ("run_delete", "forecast_runs", "DELETE"),
                ("run_truncate", "forecast_runs", "TRUNCATE"),
                ("run_references", "forecast_runs", "REFERENCES"),
                ("run_trigger", "forecast_runs", "TRIGGER"),
                ("run_maintain", "forecast_runs", "MAINTAIN"),
            )
        }
        assert privileges == {
            "bars_select": True,
            "bars_insert": True,
            "bars_update": True,
            "bars_delete": False,
            "bars_truncate": False,
            "bars_references": False,
            "bars_trigger": False,
            "bars_maintain": False,
            "revision_select": True,
            "revision_insert": False,
            "revision_update": False,
            "revision_delete": False,
            "revision_truncate": False,
            "availability_select": True,
            "availability_insert": True,
            "availability_update": False,
            "availability_delete": False,
            "availability_references": False,
            "availability_trigger": False,
            "availability_maintain": False,
            "snapshot_select": True,
            "snapshot_insert": False,
            "snapshot_update": False,
            "snapshot_delete": False,
            "snapshot_truncate": False,
            "run_select": True,
            "run_insert": True,
            "run_update": False,
            "run_delete": False,
            "run_truncate": False,
            "run_references": False,
            "run_trigger": False,
            "run_maintain": False,
        }
        sequence_usage = (
            await conn.execute(
                text(
                    "SELECT has_sequence_privilege("
                    "'stockapi_app', 'bars_revisions_id_seq', 'USAGE')"
                )
            )
        ).scalar_one()
        assert sequence_usage is False
        schema_privileges = (
            await conn.execute(
                text(
                    "SELECT has_schema_privilege('stockapi_app', 'public', 'USAGE'), "
                    "has_schema_privilege('stockapi_app', 'public', 'CREATE')"
                )
            )
        ).one()
        assert tuple(schema_privileges) == (True, False)
        can_execute_version_trigger = (
            await conn.execute(
                text(
                    "SELECT has_function_privilege("
                    "'stockapi_app', 'version_bar_write()', 'EXECUTE')"
                )
            )
        ).scalar_one()
        assert can_execute_version_trigger is False

        builder_role = (
            await conn.execute(
                text(
                    "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, "
                    "rolbypassrls, rolinherit, rolcanlogin "
                    "FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder'"
                )
            )
        ).one()
        assert tuple(builder_role) == (False, False, False, False, False, False, True)
        builder_memberships = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_auth_members AS membership "
                    "JOIN pg_roles AS member_role ON member_role.oid = membership.member "
                    "JOIN pg_roles AS granted_role ON granted_role.oid = membership.roleid "
                    "WHERE member_role.rolname = 'stockapi_snapshot_builder' "
                    "OR granted_role.rolname = 'stockapi_snapshot_builder'"
                )
            )
        ).scalar_one()
        assert builder_memberships == 0
        builder_settings = (
            await conn.execute(
                text(
                    "SELECT setting.setdatabase, setting.setconfig "
                    "FROM pg_db_role_setting AS setting "
                    "JOIN pg_roles AS role ON role.oid = setting.setrole "
                    "WHERE role.rolname = 'stockapi_snapshot_builder'"
                )
            )
        ).one()
        assert builder_settings.setdatabase == 0
        assert builder_settings.setconfig == ["search_path=pg_catalog, public"]
        builder_privileges = {
            name: (
                await conn.execute(
                    text(
                        "SELECT has_table_privilege("
                        "'stockapi_snapshot_builder', :table_name, :privilege)"
                    ),
                    {"table_name": table_name, "privilege": privilege},
                )
            ).scalar_one()
            for name, table_name, privilege in (
                ("bars_select", "bars", "SELECT"),
                ("bars_insert", "bars", "INSERT"),
                ("bars_update", "bars", "UPDATE"),
                ("revision_select", "bars_revisions", "SELECT"),
                ("revision_insert", "bars_revisions", "INSERT"),
                ("availability_select", "bar_version_availability", "SELECT"),
                ("availability_insert", "bar_version_availability", "INSERT"),
                ("availability_references", "bar_version_availability", "REFERENCES"),
                ("availability_trigger", "bar_version_availability", "TRIGGER"),
                ("availability_maintain", "bar_version_availability", "MAINTAIN"),
                ("snapshot_select", "forecast_input_snapshots", "SELECT"),
                ("snapshot_insert", "forecast_input_snapshots", "INSERT"),
                ("snapshot_update", "forecast_input_snapshots", "UPDATE"),
                ("snapshot_delete", "forecast_input_snapshots", "DELETE"),
                ("snapshot_truncate", "forecast_input_snapshots", "TRUNCATE"),
                ("run_select", "forecast_runs", "SELECT"),
                ("run_insert", "forecast_runs", "INSERT"),
                ("run_update", "forecast_runs", "UPDATE"),
                ("run_delete", "forecast_runs", "DELETE"),
                ("run_truncate", "forecast_runs", "TRUNCATE"),
            )
        }
        assert builder_privileges == {
            "bars_select": True,
            "bars_insert": False,
            "bars_update": False,
            "revision_select": True,
            "revision_insert": False,
            "availability_select": True,
            "availability_insert": False,
            "availability_references": False,
            "availability_trigger": False,
            "availability_maintain": False,
            "snapshot_select": True,
            "snapshot_insert": True,
            "snapshot_update": False,
            "snapshot_delete": False,
            "snapshot_truncate": False,
            "run_select": False,
            "run_insert": False,
            "run_update": False,
            "run_delete": False,
            "run_truncate": False,
        }
        for role in ("stockapi_app", "stockapi_snapshot_builder"):
            can_execute_receipt_trigger = (
                await conn.execute(
                    text(
                        "SELECT has_function_privilege("
                        ":role, 'stamp_bar_version_availability()', 'EXECUTE')"
                    ),
                    {"role": role},
                )
            ).scalar_one()
            assert can_execute_receipt_trigger is False
            for function_name in (
                "stamp_forecast_run_recorded_at()",
                "reject_forecast_run_mutation()",
            ):
                can_execute_run_trigger = (
                    await conn.execute(
                        text("SELECT has_function_privilege(:role, :function_name, 'EXECUTE')"),
                        {"role": role, "function_name": function_name},
                    )
                ).scalar_one()
            assert can_execute_run_trigger is False


async def test_vendor_acquisition_global_anchor_transitions_acl_and_immutability(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
) -> None:
    def checkpoint(
        number: int,
        *,
        ledger_character: str,
        campaign_character: str,
        campaign_checkpoint: int,
        campaign_ledger_character: str,
        base_calls: int,
        authorized_calls: int,
        reserved_calls: int,
    ) -> dict[str, object]:
        return {
            "checkpoint_number": number,
            "ledger_sha256": "sha256:" + ledger_character * 64,
            "campaign_id": "sha256:" + campaign_character * 64,
            "campaign_checkpoint_number": campaign_checkpoint,
            "campaign_ledger_sha256": "sha256:" + campaign_ledger_character * 64,
            "base_calls": base_calls,
            "authorized_calls": authorized_calls,
            "reserved_calls": reserved_calls,
        }

    first = checkpoint(
        1,
        ledger_character="a",
        campaign_character="b",
        campaign_checkpoint=1,
        campaign_ledger_character="c",
        base_calls=4,
        authorized_calls=4,
        reserved_calls=0,
    )
    second = checkpoint(
        2,
        ledger_character="d",
        campaign_character="b",
        campaign_checkpoint=2,
        campaign_ledger_character="e",
        base_calls=4,
        authorized_calls=4,
        reserved_calls=1,
    )
    outcome = checkpoint(
        3,
        ledger_character="f",
        campaign_character="b",
        campaign_checkpoint=3,
        campaign_ledger_character="0",
        base_calls=4,
        authorized_calls=4,
        reserved_calls=1,
    )
    recovery_authorization = checkpoint(
        4,
        ledger_character="1",
        campaign_character="b",
        campaign_checkpoint=4,
        campaign_ledger_character="3",
        base_calls=4,
        authorized_calls=5,
        reserved_calls=1,
    )
    other_campaign = checkpoint(
        5,
        ledger_character="4",
        campaign_character="6",
        campaign_checkpoint=1,
        campaign_ledger_character="5",
        base_calls=2,
        authorized_calls=2,
        reserved_calls=0,
    )

    async with engine.connect() as conn:
        transaction = await conn.begin()
        try:
            inserted = (await conn.execute(_PUBLISH_VENDOR_ACQUISITION_ANCHOR, first)).one()
            replayed = (await conn.execute(_PUBLISH_VENDOR_ACQUISITION_ANCHOR, first)).one()
            assert tuple(inserted) == tuple(replayed)
            await conn.execute(_PUBLISH_VENDOR_ACQUISITION_ANCHOR, second)
            await conn.execute(_PUBLISH_VENDOR_ACQUISITION_ANCHOR, outcome)
            await conn.execute(
                _PUBLISH_VENDOR_ACQUISITION_ANCHOR,
                recovery_authorization,
            )
            latest_replay = (
                await conn.execute(
                    _PUBLISH_VENDOR_ACQUISITION_ANCHOR,
                    recovery_authorization,
                )
            ).one()
            assert latest_replay.checkpoint_number == 4

            historical = await conn.begin_nested()
            with pytest.raises(DBAPIError, match="historical campaign checkpoint"):
                await conn.execute(_PUBLISH_VENDOR_ACQUISITION_ANCHOR, first)
            await historical.rollback()

            skipped = await conn.begin_nested()
            with pytest.raises(DBAPIError, match="global high-water"):
                await conn.execute(
                    _PUBLISH_VENDOR_ACQUISITION_ANCHOR,
                    {**other_campaign, "checkpoint_number": 6},
                )
            await skipped.rollback()

            await conn.execute(_PUBLISH_VENDOR_ACQUISITION_ANCHOR, other_campaign)
            count = (
                await conn.execute(text("SELECT count(*) FROM vendor_acquisition_campaign_anchors"))
            ).scalar_one()
            assert count == 5
        finally:
            await transaction.rollback()

    await _assert_immutability_triggers(
        owner_engine,
        ("vendor_acquisition_campaign_anchors",),
    )
    signature = (
        "publish_vendor_acquisition_campaign_anchor("
        "bigint,text,text,bigint,text,integer,integer,integer)"
    )
    async with owner_engine.connect() as conn:
        privileges = (
            await conn.execute(
                text(
                    "SELECT "
                    "has_table_privilege('stockapi_app', "
                    "'vendor_acquisition_campaign_anchors', 'SELECT'), "
                    "has_table_privilege('stockapi_app', "
                    "'vendor_acquisition_campaign_anchors', 'INSERT'), "
                    "has_table_privilege('stockapi_app', "
                    "'vendor_acquisition_campaign_anchors', 'UPDATE'), "
                    "has_table_privilege('stockapi_app', "
                    "'vendor_acquisition_campaign_anchors', 'DELETE'), "
                    "has_table_privilege('stockapi_app', "
                    "'vendor_acquisition_campaign_anchors', 'TRUNCATE'), "
                    "has_table_privilege('stockapi_snapshot_builder', "
                    "'vendor_acquisition_campaign_anchors', 'SELECT'), "
                    "has_function_privilege('stockapi_app', :signature, 'EXECUTE'), "
                    "has_function_privilege('stockapi_snapshot_builder', :signature, 'EXECUTE')"
                ),
                {"signature": signature},
            )
        ).one()
        assert tuple(privileges) == (True, False, False, False, False, False, True, False)

        await conn.rollback()
        transaction = await conn.begin()
        try:
            await conn.execute(_PUBLISH_VENDOR_ACQUISITION_ANCHOR, first)
            for mutation in (
                "UPDATE vendor_acquisition_campaign_anchors "
                "SET schema_version = schema_version WHERE checkpoint_number = 1",
                "DELETE FROM vendor_acquisition_campaign_anchors WHERE checkpoint_number = 1",
                "TRUNCATE vendor_acquisition_campaign_anchors",
            ):
                savepoint = await conn.begin_nested()
                with pytest.raises(DBAPIError, match="anchors are append-only"):
                    await conn.execute(text(mutation))
                await savepoint.rollback()
        finally:
            await transaction.rollback()


async def test_nonempty_policy_registry_refuses_downgrade(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
    migrated_database_url: LiveDatabaseUrls,
) -> None:
    policy = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=24 * 60 * 60)
    registered = await SqlForecastOutcomePolicyStore(build_sessionmaker(engine)).register(policy)

    noncanonical = policy.canonical_policy.replace(b"{", b"{ ", 1)
    async with engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(DBAPIError, match="exact supported canonical form"):
            await conn.execute(
                text("SELECT register_forecast_outcome_resolution_policy(:policy)"),
                {"policy": noncanonical},
            )
        await transaction.rollback()

    for mutation in (
        "UPDATE forecast_outcome_resolution_policies SET schema_version = schema_version "
        "WHERE policy_hash = :policy_hash",
        "DELETE FROM forecast_outcome_resolution_policies WHERE policy_hash = :policy_hash",
        "TRUNCATE forecast_outcome_resolution_policies CASCADE",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            with pytest.raises(DBAPIError, match="forecast evidence is insert-only"):
                await conn.execute(
                    text(mutation),
                    {"policy_hash": registered.record.policy_hash},
                )
            await transaction.rollback()

    result = await asyncio.to_thread(
        _invoke_alembic,
        migrated_database_url.owner,
        "downgrade",
        "0010_forecast_evidence",
    )
    combined = f"{result.stdout}\n{result.stderr}"
    assert result.returncode != 0
    assert "cannot downgrade nonempty outcome-policy evidence" in combined
    async with owner_engine.connect() as conn:
        version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
    assert version == "0015_calibration_evidence"


async def test_corporate_action_publication_replay_correction_and_withdrawal(
    engine: AsyncEngine,
) -> None:
    """Prove complete sets, exact versions, replay, and omission withdrawal."""

    first = _dividend_collection(
        symbol="CACT",
        request_id="ca-first",
        fetched_at=datetime(2026, 7, 13, 12, tzinfo=UTC),
        cash_amount="0.25",
    )
    corrected = _dividend_collection(
        symbol="CACT",
        request_id="ca-corrected",
        fetched_at=datetime(2026, 7, 13, 13, tzinfo=UTC),
        cash_amount="0.26",
    )
    withdrawn = _dividend_collection(
        symbol="CACT",
        request_id="ca-withdrawn",
        fetched_at=datetime(2026, 7, 13, 14, tzinfo=UTC),
        cash_amount=None,
    )
    assert first.members[0].action_version_id != corrected.members[0].action_version_id
    assert withdrawn.members == ()

    store = SqlCorporateActionCollectionStore(engine)
    first_proof = await store.publish(first)
    assert first_proof == await store.publish(first)
    corrected_proof = await store.publish(corrected)
    withdrawn_proof = await store.publish(withdrawn)
    assert (first_proof.event_count, corrected_proof.event_count, withdrawn_proof.event_count) == (
        1,
        1,
        0,
    )

    ids = (first.collection_id, corrected.collection_id, withdrawn.collection_id)
    async with async_sessionmaker(engine, expire_on_commit=False)() as session:
        collections = {
            value.collection_id: value
            for value in (
                await session.execute(
                    select(CorporateActionCollection).where(
                        CorporateActionCollection.collection_id.in_(ids)
                    )
                )
            ).scalars()
        }
        assert set(collections) == set(ids)
        for expected in (first, corrected, withdrawn):
            stored = collections[expected.collection_id]
            assert stored.canonical_manifest == expected.canonical_manifest
            assert stored.event_count == expected.event_count
            assert stored.provider_request_id == expected.provider_request_id

        members = tuple(
            (
                value.collection_id,
                value.ordinal,
                value.action_version_id,
            )
            for value in (
                await session.execute(
                    select(CorporateActionCollectionMember)
                    .where(CorporateActionCollectionMember.collection_id.in_(ids))
                    .order_by(
                        CorporateActionCollectionMember.collection_id,
                        CorporateActionCollectionMember.ordinal,
                    )
                )
            ).scalars()
        )
        assert members == tuple(
            sorted(
                (
                    (first.collection_id, 0, first.members[0].action_version_id),
                    (corrected.collection_id, 0, corrected.members[0].action_version_id),
                )
            )
        )
        stored_versions = {
            value.action_version_id: value
            for value in (
                await session.execute(
                    select(CorporateActionVersion).where(
                        CorporateActionVersion.action_version_id.in_(
                            (
                                first.members[0].action_version_id,
                                corrected.members[0].action_version_id,
                            )
                        )
                    )
                )
            ).scalars()
        }
        assert {key: value.canonical_event for key, value in stored_versions.items()} == {
            first.members[0].action_version_id: first.members[0].canonical_event,
            corrected.members[0].action_version_id: corrected.members[0].canonical_event,
        }
        stored_dividend = stored_versions[first.members[0].action_version_id]
        assert (
            stored_dividend.source,
            stored_dividend.action_type,
            stored_dividend.provider_event_id,
            stored_dividend.symbol,
            stored_dividend.effective_date,
            stored_dividend.status,
            stored_dividend.split_from,
            stored_dividend.split_to,
            stored_dividend.adjustment_type,
            stored_dividend.cash_amount,
            stored_dividend.split_adjusted_cash_amount,
            stored_dividend.currency,
            stored_dividend.declaration_date,
            stored_dividend.record_date,
            stored_dividend.pay_date,
            stored_dividend.frequency,
            stored_dividend.distribution_type,
            stored_dividend.historical_adjustment_factor,
        ) == (
            "polygon",
            "dividend",
            "dividend-1",
            "CACT",
            date(2026, 5, 14),
            "active",
            None,
            None,
            None,
            Decimal("0.25"),
            Decimal("0.25"),
            "USD",
            date(2026, 3, 10),
            date(2026, 5, 15),
            date(2026, 6, 12),
            4,
            "recurring",
            Decimal("0.9975"),
        )

    async with engine.connect() as conn:
        chronology = (
            await conn.execute(
                text(
                    "SELECT collection.recorded_at, collection.creator_xid, "
                    "member.creator_xid AS member_xid, "
                    "version.creator_xid AS version_xid, receipt.available_at, "
                    "(receipt.xmin::text)::bigint AS receipt_xid "
                    "FROM corporate_action_collections AS collection "
                    "JOIN corporate_action_collection_members AS member "
                    "ON member.collection_id = collection.collection_id "
                    "JOIN corporate_action_versions AS version "
                    "ON version.action_version_id = member.action_version_id "
                    "JOIN corporate_action_collection_availability AS receipt "
                    "ON receipt.collection_id = collection.collection_id "
                    "WHERE collection.collection_id = :collection_id"
                ),
                {"collection_id": first.collection_id},
            )
        ).one()
    assert chronology.creator_xid == chronology.member_xid == chronology.version_xid
    assert chronology.receipt_xid != chronology.creator_xid
    assert chronology.available_at >= chronology.recorded_at


async def test_corporate_action_runtime_boundary_and_canonical_bytes_fail_closed(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
) -> None:
    await _assert_immutability_triggers(
        engine,
        (
            "corporate_action_versions",
            "corporate_action_collections",
            "corporate_action_collection_members",
            "corporate_action_collection_availability",
        ),
    )
    record = _dividend_collection(
        symbol="CABOUND",
        request_id="ca-boundary",
        fetched_at=datetime(2026, 7, 13, 15, tzinfo=UTC),
        cash_amount="0.30",
    )
    other = _dividend_collection(
        symbol="CABOUND",
        request_id="ca-boundary-other",
        fetched_at=datetime(2026, 7, 13, 16, tzinfo=UTC),
        cash_amount="0.31",
    )
    noncanonical_event = record.members[0].canonical_event.replace(b"{", b"{ ", 1)
    noncanonical_event_id = "sha256:" + hashlib.sha256(noncanonical_event).hexdigest()
    noncanonical_event_manifest = json.loads(record.canonical_manifest)
    noncanonical_event_manifest["event_version_ids"] = [noncanonical_event_id]
    canonical_manifest_for_noncanonical_event = json.dumps(
        noncanonical_event_manifest,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")

    tables_and_keys = {
        "corporate_action_versions": "action_version_id",
        "corporate_action_collections": "collection_id",
        "corporate_action_collection_members": "collection_id",
        "corporate_action_collection_availability": "collection_id",
    }
    for table_name, key_column in tables_and_keys.items():
        for statement in (
            f"INSERT INTO {table_name} SELECT * FROM {table_name} WHERE false",
            f"UPDATE {table_name} SET {key_column} = {key_column} WHERE false",
            f"DELETE FROM {table_name} WHERE false",
            f"TRUNCATE {table_name}",
        ):
            async with engine.connect() as conn:
                transaction = await conn.begin()
                try:
                    with pytest.raises(DBAPIError, match="permission denied"):
                        await conn.execute(text(statement))
                finally:
                    await transaction.rollback()

    invalid_publications: tuple[tuple[bytes, list[bytes]], ...] = (
        (b"{}", []),
        (record.canonical_manifest, [other.members[0].canonical_event]),
        (
            record.canonical_manifest.replace(b"{", b"{ ", 1),
            [record.members[0].canonical_event],
        ),
        (canonical_manifest_for_noncanonical_event, [noncanonical_event]),
    )
    for manifest, events in invalid_publications:
        async with engine.connect() as conn:
            transaction = await conn.begin()
            try:
                with pytest.raises(DBAPIError):
                    await conn.execute(
                        _PUBLISH_CORPORATE_ACTION_COLLECTION,
                        {"manifest": manifest, "events": events},
                    )
            finally:
                await transaction.rollback()

    nan_payload = b'{"numeric_probe":"cash_nan"}'
    nan_identity = "sha256:" + hashlib.sha256(nan_payload).hexdigest()
    async with owner_engine.connect() as conn:
        transaction = await conn.begin()
        try:
            with pytest.raises(DBAPIError, match="cash_amount_finite_positive"):
                await conn.execute(
                    text(
                        "INSERT INTO corporate_action_versions ("
                        "action_version_id, schema_version, source, action_type, "
                        "provider_event_id, symbol, effective_date, status, "
                        "split_from, split_to, adjustment_type, cash_amount, "
                        "split_adjusted_cash_amount, currency, declaration_date, "
                        "record_date, pay_date, frequency, distribution_type, "
                        "historical_adjustment_factor, canonical_event, creator_xid"
                        ") VALUES ("
                        ":identity, 1, 'polygon', 'dividend', 'nan-probe', 'CANAN', "
                        "DATE '2026-07-09', 'active', NULL, NULL, NULL, "
                        "'NaN'::numeric, 1, 'USD', NULL, NULL, NULL, 4, "
                        "'recurring', 1, :payload, 1)"
                    ),
                    {"identity": nan_identity, "payload": nan_payload},
                )
        finally:
            await transaction.rollback()


async def test_corporate_action_membership_freeze_and_receipt_fence_contention(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
) -> None:
    record = _split_collection(
        symbol="CAFENCE",
        request_id="ca-fence",
        fetched_at=datetime(2026, 7, 13, 17, tzinfo=UTC),
    )
    assert await _publish_corporate_action_content(engine, record) == record.collection_id
    async with async_sessionmaker(engine, expire_on_commit=False)() as session:
        stored_split = (
            await session.execute(
                select(CorporateActionVersion).where(
                    CorporateActionVersion.action_version_id == record.members[0].action_version_id
                )
            )
        ).scalar_one()
    assert (
        stored_split.source,
        stored_split.action_type,
        stored_split.provider_event_id,
        stored_split.symbol,
        stored_split.effective_date,
        stored_split.status,
        stored_split.split_from,
        stored_split.split_to,
        stored_split.adjustment_type,
        stored_split.cash_amount,
        stored_split.split_adjusted_cash_amount,
        stored_split.currency,
        stored_split.declaration_date,
        stored_split.record_date,
        stored_split.pay_date,
        stored_split.frequency,
        stored_split.distribution_type,
        stored_split.historical_adjustment_factor,
    ) == (
        "polygon",
        "split",
        "split-1",
        "CAFENCE",
        date(2026, 4, 15),
        "active",
        Decimal("1"),
        Decimal("2"),
        "forward_split",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Decimal("0.5"),
    )

    async def assert_membership_frozen() -> None:
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            try:
                with pytest.raises(DBAPIError, match="must be inserted with their collection"):
                    await conn.execute(
                        text(
                            "INSERT INTO corporate_action_collection_members "
                            "(collection_id, ordinal, action_version_id, creator_xid) "
                            "VALUES (:collection_id, 99, :action_version_id, 1)"
                        ),
                        {
                            "collection_id": record.collection_id,
                            "action_version_id": record.members[0].action_version_id,
                        },
                    )
            finally:
                await transaction.rollback()

    await assert_membership_frozen()

    async with owner_engine.connect() as holder, engine.connect() as waiter:
        holder_transaction = await holder.begin()
        waiter_transaction = await waiter.begin()
        wait_task: asyncio.Task[Any] | None = None
        try:
            lock_id = (
                await holder.execute(
                    text(
                        "SELECT public.corporate_action_series_fence_id("
                        "'polygon', 'CAFENCE', 'split')"
                    )
                )
            ).scalar_one()
            await holder.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": lock_id},
            )
            waiter_pid = (await waiter.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            wait_task = asyncio.create_task(
                waiter.execute(
                    _PUBLISH_CORPORATE_ACTION_RECEIPT,
                    {"collection_id": record.collection_id},
                )
            )

            waiting = False
            for _ in range(40):
                async with owner_engine.connect() as probe:
                    waiting = bool(
                        (
                            await probe.execute(
                                text(
                                    "SELECT EXISTS (SELECT 1 FROM pg_locks "
                                    "WHERE pid = :pid AND locktype = 'advisory' "
                                    "AND NOT granted)"
                                ),
                                {"pid": waiter_pid},
                            )
                        ).scalar_one()
                    )
                if waiting:
                    break
                await asyncio.sleep(0.05)
            assert waiting
            assert not wait_task.done()

            await holder_transaction.commit()
            receipt = (await asyncio.wait_for(wait_task, timeout=5)).one()
            await waiter_transaction.commit()
        finally:
            if holder_transaction.is_active:
                await holder_transaction.rollback()
            if wait_task is not None and not wait_task.done():
                wait_task.cancel()
            if waiter_transaction.is_active:
                await waiter_transaction.rollback()

    assert receipt.collection_id == record.collection_id
    assert receipt.available_at >= receipt.collection_recorded_at
    await assert_membership_frozen()

    for mutation in (
        "UPDATE corporate_action_collections SET symbol = symbol "
        "WHERE collection_id = :collection_id",
        "DELETE FROM corporate_action_collection_members WHERE collection_id = :collection_id",
        "TRUNCATE corporate_action_collection_members",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            try:
                with pytest.raises(DBAPIError, match="corporate-action evidence is append-only"):
                    await conn.execute(text(mutation), {"collection_id": record.collection_id})
            finally:
                await transaction.rollback()


async def test_adjustment_factor_publication_replay_receipt_projection_and_lock(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
    migrated_database_url: LiveDatabaseUrls,
) -> None:
    artifact, _, _ = await _factor_scenario(engine, symbol="FACTOR")

    # Content and its availability receipt cannot be collapsed into one commit.
    async with snapshot_builder_engine.connect() as conn:
        transaction = await conn.begin()
        try:
            returned_id = (
                await conn.execute(
                    _PUBLISH_ADJUSTMENT_FACTOR_SET,
                    {"payload": artifact.canonical_payload},
                )
            ).scalar_one()
            assert returned_id == artifact.factor_set_id
            with pytest.raises(DBAPIError, match="requires a later transaction"):
                await conn.execute(
                    _PUBLISH_ADJUSTMENT_FACTOR_RECEIPT,
                    {"factor_set_id": artifact.factor_set_id},
                )
        finally:
            await transaction.rollback()

    factor_store = SqlAdjustmentFactorSetStore(snapshot_builder_engine)
    published = await factor_store.publish(artifact)
    assert await factor_store.publish(artifact) == published
    assert published.factor_set_id == artifact.factor_set_id
    assert published.input_count == len(artifact.raw_inputs)

    # The production resolver must reproduce the exact artifact from DB
    # receipts, release its read session, and replay the authoritative
    # publisher rather than relying on test-assembled inputs.
    builder = AdjustmentFactorBuilder(
        async_sessionmaker(snapshot_builder_engine, expire_on_commit=False),
        factor_store,
    )
    rebuilt = await builder.build(
        AdjustmentFactorBuildSpec(
            symbol=artifact.symbol,
            coverage_start=artifact.raw_inputs[0].observation_date,
            coverage_end=artifact.raw_inputs[-1].observation_date,
            cutoff=artifact.cutoff,
        )
    )
    assert rebuilt.artifact == artifact
    assert rebuilt.publication == published

    # Exercise the production runtime loader and adjusted read against the
    # real immutable tables. Pagination occurs only after the loader validates
    # all three exact raw versions and the complete factor window.
    async with async_sessionmaker(engine, expire_on_commit=False)() as runtime_session:
        first_page = await read_adjusted_prices(
            runtime_session,
            artifact.symbol,
            AdjustedPriceFilters(factor_set_id=artifact.factor_set_id, limit=2),
        )
        second_page = await read_adjusted_prices(
            runtime_session,
            artifact.symbol,
            AdjustedPriceFilters(
                factor_set_id=artifact.factor_set_id,
                end=first_page.page.next_end,
                limit=2,
            ),
        )
    assert first_page.count == 2
    assert first_page.page.has_more is True
    assert first_page.page.next_end == artifact.raw_inputs[1].observed_at
    assert [bar.timestamp for bar in first_page.bars] == [
        artifact.raw_inputs[1].observed_at,
        artifact.raw_inputs[2].observed_at,
    ]
    assert second_page.count == 1
    assert second_page.page.has_more is False
    assert second_page.bars[0].timestamp == artifact.raw_inputs[0].observed_at
    assert second_page.bars[0].close < float(artifact.raw_inputs[0].close)
    assert first_page.lineage.factor_set_id == artifact.factor_set_id

    document = json.loads(artifact.canonical_payload)
    async with async_sessionmaker(
        snapshot_builder_engine,
        expire_on_commit=False,
    )() as session:
        header = (
            await session.execute(
                select(AdjustmentFactorSetRecord).where(
                    AdjustmentFactorSetRecord.factor_set_id == artifact.factor_set_id
                )
            )
        ).scalar_one()
        entries = tuple(
            (
                await session.execute(
                    select(AdjustmentFactorEntry)
                    .where(AdjustmentFactorEntry.factor_set_id == artifact.factor_set_id)
                    .order_by(AdjustmentFactorEntry.ordinal)
                )
            )
            .scalars()
            .all()
        )
        receipt = (
            await session.execute(
                select(AdjustmentFactorSetAvailability).where(
                    AdjustmentFactorSetAvailability.factor_set_id == artifact.factor_set_id
                )
            )
        ).scalar_one()

    assert header.canonical_payload == artifact.canonical_payload
    assert (
        header.format,
        header.policy_version,
        header.policy_hash,
        header.symbol,
        header.cutoff,
        header.anchor_date,
        header.coverage_start,
        header.coverage_end,
        header.input_count,
        header.split_collection_id,
        header.dividend_collection_id,
    ) == (
        document["format"],
        artifact.policy_version,
        artifact.policy_hash,
        artifact.symbol,
        artifact.cutoff,
        artifact.anchor_date,
        artifact.raw_inputs[0].observation_date,
        artifact.raw_inputs[-1].observation_date,
        len(artifact.raw_inputs),
        artifact.split_collection_id,
        artifact.dividend_collection_id,
    )
    assert len(entries) == len(document["raw_inputs"]) == len(document["factors"])
    for ordinal, (entry, raw, factor) in enumerate(
        zip(entries, document["raw_inputs"], document["factors"], strict=True)
    ):
        assert (
            entry.ordinal,
            entry.symbol,
            entry.observation_date.isoformat(),
            entry.observed_at,
            entry.timespan,
            entry.multiplier,
            entry.source,
            entry.adjustment_basis,
            entry.version_recorded_at,
            entry.raw_available_at,
            entry.raw_close_decimal,
            entry.raw_close_f64_be,
            entry.price_factor_decimal,
            entry.price_factor_f64_be,
            entry.volume_factor_decimal,
            entry.volume_factor_f64_be,
        ) == (
            ordinal,
            artifact.symbol,
            raw["observation_date"],
            datetime.fromisoformat(raw["observed_at"].replace("Z", "+00:00")),
            raw["timespan"],
            raw["multiplier"],
            raw["source"],
            raw["adjustment_basis"],
            datetime.fromisoformat(raw["version_recorded_at"].replace("Z", "+00:00")),
            datetime.fromisoformat(raw["available_at"].replace("Z", "+00:00")),
            raw["close_decimal"],
            bytes.fromhex(raw["close_f64_be"]),
            factor["price_factor_decimal"],
            bytes.fromhex(factor["price_factor_f64_be"]),
            factor["volume_factor_decimal"],
            bytes.fromhex(factor["volume_factor_f64_be"]),
        )
    assert receipt.factor_set_recorded_at == header.recorded_at
    assert receipt.available_at >= receipt.factor_set_recorded_at
    assert header.max_input_available_at == max(
        *(value.available_at for value in artifact.raw_inputs),
        header.split_collection_available_at,
        header.dividend_collection_available_at,
    )

    async with owner_engine.connect() as conn:
        exact_fks = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_constraint WHERE conname = ANY(CAST(:names AS text[]))"
                ),
                {
                    "names": [
                        "fk_adjustment_factor_sets_exact_split_collection_receipt",
                        "fk_adjustment_factor_sets_exact_dividend_collection_receipt",
                        "fk_adjustment_factor_entries_exact_bar_receipt",
                    ]
                },
            )
        ).scalar_one()
        receipt_xid = (
            await conn.execute(
                text(
                    "SELECT (xmin::text)::bigint FROM adjustment_factor_set_availability "
                    "WHERE factor_set_id = :factor_set_id"
                ),
                {"factor_set_id": artifact.factor_set_id},
            )
        ).scalar_one()
    assert exact_fks == 3
    assert receipt_xid != header.creator_xid

    # The real publisher must wait behind the same raw-series fence as ingestion.
    async with owner_engine.connect() as holder, snapshot_builder_engine.connect() as waiter:
        holder_transaction = await holder.begin()
        waiter_transaction = await waiter.begin()
        wait_task: asyncio.Task[Any] | None = None
        try:
            lock_id = (
                await holder.execute(
                    text(
                        "SELECT public.forecast_bar_series_fence_id("
                        ":symbol, 'polygon_open_close', 'day')"
                    ),
                    {"symbol": artifact.symbol},
                )
            ).scalar_one()
            await holder.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": lock_id},
            )
            waiter_pid = (await waiter.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            wait_task = asyncio.create_task(
                waiter.execute(
                    _PUBLISH_ADJUSTMENT_FACTOR_SET,
                    {"payload": artifact.canonical_payload},
                )
            )
            waiting = False
            for _ in range(40):
                async with owner_engine.connect() as probe:
                    waiting = bool(
                        (
                            await probe.execute(
                                text(
                                    "SELECT EXISTS (SELECT 1 FROM pg_locks "
                                    "WHERE pid = :pid AND locktype = 'advisory' "
                                    "AND NOT granted)"
                                ),
                                {"pid": waiter_pid},
                            )
                        ).scalar_one()
                    )
                if waiting:
                    break
                await asyncio.sleep(0.05)
            assert waiting
            assert not wait_task.done()
            await holder_transaction.commit()
            replayed_id = (await asyncio.wait_for(wait_task, timeout=5)).scalar_one()
            await waiter_transaction.commit()
        finally:
            if holder_transaction.is_active:
                await holder_transaction.rollback()
            if wait_task is not None and not wait_task.done():
                wait_task.cancel()
            if waiter_transaction.is_active:
                await waiter_transaction.rollback()
    assert replayed_id == artifact.factor_set_id

    downgrade = await asyncio.to_thread(
        _invoke_alembic,
        migrated_database_url.owner,
        "downgrade",
        "0012_corporate_actions",
    )
    assert downgrade.returncode != 0
    assert "cannot downgrade nonempty adjustment-factor evidence" in (
        f"{downgrade.stdout}\n{downgrade.stderr}"
    )
    async with owner_engine.connect() as conn:
        version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
    assert version == "0015_calibration_evidence"


async def test_adjustment_decimal_kernel_is_exact_decimal34(
    owner_engine: AsyncEngine,
) -> None:
    decimal_cases = (
        (
            "1.2345678901234567890123456789012345",
            "1.234567890123456789012345678901234",
        ),
        (
            "1.2345678901234567890123456789012355",
            "1.234567890123456789012345678901236",
        ),
        (
            "1.2345678901234567890123456789012345001",
            "1.234567890123456789012345678901235",
        ),
        ("9.9999999999999999999999999999999995", "10"),
    )
    division_cases = (
        ("1", "3", "0.3333333333333333333333333333333333"),
        ("2", "3", "0.6666666666666666666666666666666667"),
        ("101", "101.5", "0.9950738916256157635467980295566502"),
    )
    async with owner_engine.connect() as conn:
        for value, expected in decimal_cases:
            actual = (
                await conn.execute(
                    text(
                        "SELECT adjustment_decimal_text("
                        "adjustment_decimal34(CAST(:value AS numeric)))"
                    ),
                    {"value": value},
                )
            ).scalar_one()
            assert actual == expected
        for numerator, denominator, expected in division_cases:
            actual = (
                await conn.execute(
                    text(
                        "SELECT adjustment_decimal_text(adjustment_divide34("
                        "CAST(:numerator AS numeric), CAST(:denominator AS numeric)))"
                    ),
                    {"numerator": numerator, "denominator": denominator},
                )
            ).scalar_one()
            assert actual == expected


async def test_adjustment_factor_acl_immutability_and_bytes_fail_closed(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
) -> None:
    artifact, _, _ = await _factor_scenario(engine, symbol="FACBOUND")
    published = await SqlAdjustmentFactorSetStore(snapshot_builder_engine).publish(artifact)
    await _assert_immutability_triggers(
        owner_engine,
        (
            "adjustment_factor_sets",
            "adjustment_factor_entries",
            "adjustment_factor_set_availability",
        ),
    )

    for table_name, key_column in {
        "adjustment_factor_sets": "factor_set_id",
        "adjustment_factor_entries": "factor_set_id",
        "adjustment_factor_set_availability": "factor_set_id",
    }.items():
        for statement in (
            f"INSERT INTO {table_name} SELECT * FROM {table_name} WHERE false",
            f"UPDATE {table_name} SET {key_column} = {key_column} WHERE false",
            f"DELETE FROM {table_name} WHERE false",
            f"TRUNCATE {table_name}",
        ):
            async with engine.connect() as conn:
                transaction = await conn.begin()
                try:
                    with pytest.raises(DBAPIError, match="permission denied"):
                        await conn.execute(text(statement))
                finally:
                    await transaction.rollback()

    for statement, parameters in (
        (_PUBLISH_ADJUSTMENT_FACTOR_SET, {"payload": artifact.canonical_payload}),
        (
            _PUBLISH_ADJUSTMENT_FACTOR_RECEIPT,
            {"factor_set_id": artifact.factor_set_id},
        ),
    ):
        async with engine.connect() as conn:
            transaction = await conn.begin()
            try:
                with pytest.raises(DBAPIError, match="permission denied"):
                    await conn.execute(statement, parameters)
            finally:
                await transaction.rollback()

    tampered = json.loads(artifact.canonical_payload)
    tampered["factors"][0]["price_factor_decimal"] = "0.25"
    tampered["factors"][0]["price_factor_f64_be"] = "3fd0000000000000"
    invalid_payloads = (
        b"{}",
        artifact.canonical_payload.replace(b"{", b"{ ", 1),
        _canonical_bytes(tampered),
    )
    for payload in invalid_payloads:
        async with snapshot_builder_engine.connect() as conn:
            transaction = await conn.begin()
            try:
                with pytest.raises(DBAPIError):
                    await conn.execute(
                        _PUBLISH_ADJUSTMENT_FACTOR_SET,
                        {"payload": payload},
                    )
            finally:
                await transaction.rollback()

    for mutation in (
        "UPDATE adjustment_factor_sets SET symbol = symbol WHERE factor_set_id = :factor_set_id",
        "DELETE FROM adjustment_factor_entries WHERE factor_set_id = :factor_set_id",
        "TRUNCATE adjustment_factor_set_availability",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            try:
                with pytest.raises(DBAPIError, match="adjustment-factor evidence is append-only"):
                    await conn.execute(
                        text(mutation),
                        {"factor_set_id": published.factor_set_id},
                    )
            finally:
                await transaction.rollback()


async def test_adjustment_factor_rejects_stale_and_omitted_raw_receipts(
    engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
) -> None:
    artifact, split_collection, dividend_collection = await _factor_scenario(
        engine,
        symbol="FACRAW",
        cash_amount=None,
    )

    omitted = json.loads(artifact.canonical_payload)
    omitted["raw_inputs"] = [omitted["raw_inputs"][0], omitted["raw_inputs"][2]]
    omitted["factors"] = [omitted["factors"][0], omitted["factors"][2]]
    for ordinal, factor in enumerate(omitted["factors"]):
        factor["raw_input_ordinal"] = ordinal
    async with snapshot_builder_engine.connect() as conn:
        transaction = await conn.begin()
        try:
            with pytest.raises(DBAPIError, match="omit newest cutoff-visible"):
                await conn.execute(
                    _PUBLISH_ADJUSTMENT_FACTOR_SET,
                    {"payload": _canonical_bytes(omitted)},
                )
        finally:
            await transaction.rollback()

    old_raw = artifact.raw_inputs
    await _restate_factor_bar(
        engine,
        symbol=artifact.symbol,
        session_date=date(2026, 7, 9),
        close=111.0,
    )
    stale_artifact = build_adjustment_factor_set(
        symbol=artifact.symbol,
        cutoff=await _factor_database_now(engine),
        raw_closes=old_raw,
        split_collection_id=split_collection.collection_id,
        splits=(),
        dividend_collection_id=dividend_collection.collection_id,
        dividends=(),
    )
    async with snapshot_builder_engine.connect() as conn:
        transaction = await conn.begin()
        try:
            with pytest.raises(DBAPIError, match="not the unique newest cutoff version"):
                await conn.execute(
                    _PUBLISH_ADJUSTMENT_FACTOR_SET,
                    {"payload": stale_artifact.canonical_payload},
                )
        finally:
            await transaction.rollback()


async def test_adjustment_factor_action_order_delayed_receipt_and_correction(
    engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
) -> None:
    symbol = "FACACT"
    sessions = (date(2026, 7, 8), date(2026, 7, 9), date(2026, 7, 10))
    raw = await _seed_factor_raw_series(
        engine,
        symbol=symbol,
        sessions=sessions,
        closes=(200.0, 201.0, 202.0),
    )
    split_collection = _factor_split_collection(
        symbol=symbol,
        request_id="facact-splits",
        start=sessions[0],
        end=sessions[-1],
        fetched_at=datetime(2026, 7, 10, 21, tzinfo=UTC),
    )
    old = _factor_dividend_collection(
        symbol=symbol,
        request_id="facact-old",
        start=sessions[0],
        end=sessions[-1],
        ex_dividend_date=sessions[1],
        cash_amount="0.40",
        fetched_at=datetime(2026, 7, 10, 21, 1, tzinfo=UTC),
    )
    newer = _factor_dividend_collection(
        symbol=symbol,
        request_id="facact-newer",
        start=sessions[0],
        end=sessions[-1],
        ex_dividend_date=sessions[1],
        cash_amount="0.41",
        fetched_at=datetime(2026, 7, 10, 21, 2, tzinfo=UTC),
    )
    action_store = SqlCorporateActionCollectionStore(engine)
    await action_store.publish(split_collection)
    assert await _publish_corporate_action_content(engine, old) == old.collection_id
    await asyncio.sleep(0.01)
    await action_store.publish(newer)
    async with engine.begin() as conn:
        delayed_old_receipt = (
            await conn.execute(
                _PUBLISH_CORPORATE_ACTION_RECEIPT,
                {"collection_id": old.collection_id},
            )
        ).one()

    cutoff = await _factor_database_now(engine)
    newer_artifact = build_adjustment_factor_set(
        symbol=symbol,
        cutoff=cutoff,
        raw_closes=raw,
        split_collection_id=split_collection.collection_id,
        splits=(),
        dividend_collection_id=newer.collection_id,
        dividends=_factor_dividend_versions(newer),
    )
    first_published = await SqlAdjustmentFactorSetStore(snapshot_builder_engine).publish(
        newer_artifact
    )
    assert delayed_old_receipt.available_at <= newer_artifact.cutoff

    stale_action_artifact = build_adjustment_factor_set(
        symbol=symbol,
        cutoff=cutoff,
        raw_closes=raw,
        split_collection_id=split_collection.collection_id,
        splits=(),
        dividend_collection_id=old.collection_id,
        dividends=_factor_dividend_versions(old),
    )
    async with snapshot_builder_engine.connect() as conn:
        transaction = await conn.begin()
        try:
            with pytest.raises(DBAPIError, match="not newest for the exact cutoff scope"):
                await conn.execute(
                    _PUBLISH_ADJUSTMENT_FACTOR_SET,
                    {"payload": stale_action_artifact.canonical_payload},
                )
        finally:
            await transaction.rollback()

    correction = _factor_dividend_collection(
        symbol=symbol,
        request_id="facact-correction",
        start=sessions[0],
        end=sessions[-1],
        ex_dividend_date=sessions[1],
        cash_amount="0.42",
        fetched_at=datetime(2026, 7, 10, 21, 3, tzinfo=UTC),
    )
    await asyncio.sleep(0.01)
    await action_store.publish(correction)
    corrected_artifact = build_adjustment_factor_set(
        symbol=symbol,
        cutoff=await _factor_database_now(engine),
        raw_closes=raw,
        split_collection_id=split_collection.collection_id,
        splits=(),
        dividend_collection_id=correction.collection_id,
        dividends=_factor_dividend_versions(correction),
    )
    corrected = await SqlAdjustmentFactorSetStore(snapshot_builder_engine).publish(
        corrected_artifact
    )
    assert corrected.factor_set_id != first_published.factor_set_id


async def test_forecast_evidence_schema_and_role_boundaries(
    owner_engine: AsyncEngine,
) -> None:
    tables = (
        "forecast_outcome_resolution_policies",
        "forecast_realized_outcomes",
        "forecast_realized_outcome_publications",
        "forecast_outcome_cohort_manifests",
        "forecast_outcome_cohort_members",
        "forecast_outcome_cohort_availability",
    )
    required_constraints = {
        "uq_bar_version_availability_exact_receipt",
        "ck_forecast_realized_outcomes_outcome_id_matches_payload",
        "uq_forecast_realized_outcomes_semantic_key",
        "ck_forecast_realized_outcomes_currency_usd",
        "fk_forecast_realized_outcomes_registered_policy",
        "pk_forecast_outcome_resolution_policies",
        "ck_forecast_outcome_resolution_policies_resolution_lag_bounded",
        "uq_forecast_outcome_resolution_policies_policy_rules",
        "pk_forecast_realized_outcome_publications",
        "ck_forecast_outcome_cohort_manifests_cohort_id_matches_payload",
        "fk_forecast_outcome_cohort_manifests_registered_policy",
        "pk_forecast_outcome_cohort_members",
        "uq_forecast_outcome_cohort_members_opportunity_step",
    }
    required_triggers = {
        ("forecast_realized_outcomes", "forecast_realized_outcomes_stamp"),
        ("forecast_realized_outcomes", "forecast_realized_outcomes_no_row_mutation"),
        ("forecast_realized_outcomes", "forecast_realized_outcomes_no_truncate"),
        ("forecast_realized_outcomes", "forecast_realized_outcomes_validate_policy"),
        (
            "forecast_outcome_resolution_policies",
            "forecast_outcome_resolution_policies_stamp",
        ),
        (
            "forecast_outcome_resolution_policies",
            "forecast_outcome_resolution_policies_no_row_mutation",
        ),
        (
            "forecast_outcome_resolution_policies",
            "forecast_outcome_resolution_policies_no_truncate",
        ),
        (
            "forecast_realized_outcome_publications",
            "forecast_realized_outcome_publications_no_row_mutation",
        ),
        (
            "forecast_realized_outcome_publications",
            "forecast_realized_outcome_publications_no_truncate",
        ),
        ("forecast_outcome_cohort_manifests", "forecast_outcome_cohorts_stamp"),
        (
            "forecast_outcome_cohort_manifests",
            "forecast_outcome_cohort_manifests_no_row_mutation",
        ),
        (
            "forecast_outcome_cohort_manifests",
            "forecast_outcome_cohort_manifests_no_truncate",
        ),
        (
            "forecast_outcome_cohort_manifests",
            "forecast_outcome_cohorts_materialize_members",
        ),
        (
            "forecast_outcome_cohort_members",
            "forecast_outcome_cohort_members_validate",
        ),
        (
            "forecast_outcome_cohort_members",
            "forecast_outcome_cohort_members_no_row_mutation",
        ),
        (
            "forecast_outcome_cohort_members",
            "forecast_outcome_cohort_members_no_truncate",
        ),
        (
            "forecast_outcome_cohort_availability",
            "forecast_outcome_cohort_availability_stamp",
        ),
        (
            "forecast_outcome_cohort_availability",
            "forecast_outcome_cohort_availability_no_row_mutation",
        ),
        (
            "forecast_outcome_cohort_availability",
            "forecast_outcome_cohort_availability_no_truncate",
        ),
    }

    async with owner_engine.connect() as conn:
        present_tables = set(
            (
                await conn.execute(
                    text(
                        "SELECT tablename FROM pg_tables "
                        "WHERE schemaname = 'public' AND tablename = ANY(:tables)"
                    ),
                    {"tables": list(tables)},
                )
            ).scalars()
        )
        assert present_tables == set(tables)

        constraints = set(
            (
                await conn.execute(
                    text(
                        "SELECT constraint_name FROM information_schema.table_constraints "
                        "WHERE table_schema = 'public' AND (table_name = ANY(:tables) "
                        "OR table_name = 'bar_version_availability')"
                    ),
                    {"tables": list(tables)},
                )
            ).scalars()
        )
        assert required_constraints <= constraints
        # PostgreSQL/SQLAlchemy may deterministically truncate long generated
        # FK names to the 63-byte identifier limit; assert their stable semantic
        # prefixes rather than coupling the gate to a compiler hash suffix.
        assert any(
            name.startswith("fk_forecast_realized_outcomes_exact_bar_receipt")
            for name in constraints
        )
        assert any(
            name.startswith("fk_forecast_outcome_cohort_availability_cohort_id")
            for name in constraints
        )
        assert any(
            name.startswith("fk_forecast_outcome_cohort_members_forecast_id")
            for name in constraints
        )
        assert any(
            name.startswith("ck_forecast_outcome_resolution_policies_canonical_polic")
            for name in constraints
        )

        indexes = set(
            (
                await conn.execute(
                    text(
                        "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' "
                        "AND indexname IN ('ix_forecast_realized_outcomes_target', "
                        "'ix_forecast_outcome_cohorts_target_window', "
                        "'ix_forecast_outcome_cohort_members_target')"
                    )
                )
            ).scalars()
        )
        assert indexes == {
            "ix_forecast_realized_outcomes_target",
            "ix_forecast_outcome_cohorts_target_window",
            "ix_forecast_outcome_cohort_members_target",
        }

        triggers = {
            tuple(row)
            for row in (
                await conn.execute(
                    text(
                        "SELECT relation.relname, trigger.tgname FROM pg_trigger AS trigger "
                        "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
                        "WHERE NOT trigger.tgisinternal AND relation.relname = ANY(:tables)"
                    ),
                    {"tables": list(tables)},
                )
            ).all()
        }
        assert triggers == required_triggers

        for table_name in tables:
            app_may_insert = table_name in {
                "forecast_outcome_cohort_manifests",
                "forecast_outcome_cohort_availability",
            }
            runtime = (
                await conn.execute(
                    text(
                        "SELECT has_table_privilege('stockapi_app', :table_name, 'SELECT'), "
                        "has_table_privilege('stockapi_app', :table_name, 'INSERT'), "
                        "has_table_privilege('stockapi_app', :table_name, 'UPDATE'), "
                        "has_table_privilege('stockapi_app', :table_name, 'DELETE'), "
                        "has_table_privilege('stockapi_app', :table_name, 'TRUNCATE'), "
                        "has_table_privilege('stockapi_app', :table_name, 'REFERENCES'), "
                        "has_table_privilege('stockapi_app', :table_name, 'TRIGGER'), "
                        "has_table_privilege('stockapi_app', :table_name, 'MAINTAIN'), "
                        "has_any_column_privilege('stockapi_app', :table_name, 'INSERT'), "
                        "has_any_column_privilege('stockapi_app', :table_name, 'UPDATE'), "
                        "has_any_column_privilege('stockapi_app', :table_name, 'REFERENCES')"
                    ),
                    {"table_name": table_name},
                )
            ).one()
            assert tuple(runtime) == (
                True,
                app_may_insert,
                False,
                False,
                False,
                False,
                False,
                False,
                app_may_insert,
                False,
                False,
            )

            builder = (
                await conn.execute(
                    text(
                        "SELECT has_table_privilege("
                        "'stockapi_snapshot_builder', :table_name, 'SELECT'), "
                        "has_table_privilege("
                        "'stockapi_snapshot_builder', :table_name, 'INSERT'), "
                        "has_table_privilege("
                        "'stockapi_snapshot_builder', :table_name, 'UPDATE'), "
                        "has_table_privilege("
                        "'stockapi_snapshot_builder', :table_name, 'DELETE'), "
                        "has_table_privilege("
                        "'stockapi_snapshot_builder', :table_name, 'TRUNCATE'), "
                        "has_any_column_privilege("
                        "'stockapi_snapshot_builder', :table_name, 'INSERT'), "
                        "has_any_column_privilege("
                        "'stockapi_snapshot_builder', :table_name, 'UPDATE'), "
                        "has_any_column_privilege("
                        "'stockapi_snapshot_builder', :table_name, 'REFERENCES')"
                    ),
                    {"table_name": table_name},
                )
            ).one()
            assert tuple(builder) == (False, False, False, False, False, False, False, False)

        for function_name in (
            "stamp_forecast_realized_outcome()",
            "stamp_forecast_outcome_cohort_manifest()",
            "validate_forecast_outcome_cohort_member()",
            "materialize_forecast_outcome_cohort_members()",
            "stamp_forecast_outcome_cohort_availability()",
            "reject_forecast_evidence_mutation()",
            "fence_bar_version_availability()",
            "stamp_forecast_outcome_resolution_policy()",
            "validate_forecast_realized_outcome_policy()",
            "forecast_bar_series_fence_id(text,text,text)",
        ):
            executable = (
                await conn.execute(
                    text(
                        "SELECT has_function_privilege('stockapi_app', :function_name, "
                        "'EXECUTE'), has_function_privilege('stockapi_snapshot_builder', "
                        ":function_name, 'EXECUTE')"
                    ),
                    {"function_name": function_name},
                )
            ).one()
            assert tuple(executable) == (False, False)

        for function_name in (
            "register_forecast_outcome_resolution_policy(bytea)",
            "publish_forecast_realized_outcome(varchar,uuid,smallint,varchar,bytea)",
        ):
            runtime, builder = (
                await conn.execute(
                    text(
                        "SELECT has_function_privilege('stockapi_app', :function_name, "
                        "'EXECUTE'), has_function_privilege('stockapi_snapshot_builder', "
                        ":function_name, 'EXECUTE')"
                    ),
                    {"function_name": function_name},
                )
            ).one()
            assert runtime is True
            assert builder is False


async def test_calibration_evidence_schema_and_role_boundaries(
    owner_engine: AsyncEngine,
) -> None:
    tables = (
        "forecast_fitted_calibration_sets",
        "forecast_heldout_coverage_releases",
        "forecast_heldout_coverage_release_buckets",
        "forecast_heldout_coverage_release_availability",
    )
    async with owner_engine.connect() as conn:
        present = set(
            (
                await conn.execute(
                    text(
                        "SELECT tablename FROM pg_tables "
                        "WHERE schemaname = 'public' AND tablename = ANY(:tables)"
                    ),
                    {"tables": list(tables)},
                )
            ).scalars()
        )
        assert present == set(tables)

        constraints = set(
            (
                await conn.execute(
                    text(
                        "SELECT constraint_name FROM information_schema.table_constraints "
                        "WHERE table_schema = 'public' AND table_name = ANY(:tables)"
                    ),
                    {"tables": list(tables)},
                )
            ).scalars()
        )
        assert {
            "pk_forecast_fitted_calibration_sets",
            "pk_forecast_heldout_coverage_releases",
            "pk_forecast_heldout_coverage_release_buckets",
            "pk_forecast_heldout_coverage_release_availability",
            "uq_fitted_calibration_sets_cohort_method",
        } <= constraints
        assert any(
            name.startswith("ck_forecast_fitted_calibration_sets_calibration_")
            for name in constraints
        )
        assert any(
            name.startswith("ck_forecast_heldout_coverage_releases_scope_") for name in constraints
        )
        assert any(
            name.startswith("ck_forecast_heldout_coverage_releases_release_")
            for name in constraints
        )

        publication_index = (
            await conn.execute(
                text(
                    "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' "
                    "AND tablename = 'forecast_realized_outcome_publications' "
                    "AND indexname = "
                    "'ix_forecast_realized_outcome_publications_cohort_member'"
                )
            )
        ).scalar_one()
        assert "(cohort_id, forecast_id, step, outcome_id)" in publication_index

        trigger_rows = (
            await conn.execute(
                text(
                    "SELECT relation.relname, trigger.tgname FROM pg_trigger AS trigger "
                    "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
                    "WHERE NOT trigger.tgisinternal AND relation.relname = ANY(:tables)"
                ),
                {"tables": list(tables)},
            )
        ).all()
        triggers = {tuple(row) for row in trigger_rows}
        for table_name in tables:
            assert (table_name, f"{table_name}_no_row_mutation") in triggers
            assert (table_name, f"{table_name}_no_truncate") in triggers
        assert (
            "forecast_heldout_coverage_release_availability",
            "forecast_heldout_coverage_release_availability_stamp",
        ) in triggers

        for table_name in tables:
            runtime = (
                await conn.execute(
                    text(
                        "SELECT has_table_privilege('stockapi_app', :table, 'SELECT'), "
                        "has_table_privilege('stockapi_app', :table, 'INSERT'), "
                        "has_table_privilege('stockapi_app', :table, 'UPDATE'), "
                        "has_table_privilege('stockapi_app', :table, 'DELETE'), "
                        "has_table_privilege('stockapi_app', :table, 'TRUNCATE'), "
                        "has_any_column_privilege('stockapi_app', :table, 'INSERT'), "
                        "has_any_column_privilege('stockapi_app', :table, 'UPDATE'), "
                        "has_any_column_privilege('stockapi_app', :table, 'REFERENCES')"
                    ),
                    {"table": table_name},
                )
            ).one()
            assert tuple(runtime) == (True, False, False, False, False, False, False, False)
            builder = (
                await conn.execute(
                    text(
                        "SELECT has_table_privilege("
                        "'stockapi_snapshot_builder', :table, 'SELECT'), "
                        "has_table_privilege("
                        "'stockapi_snapshot_builder', :table, 'INSERT'), "
                        "has_table_privilege("
                        "'stockapi_snapshot_builder', :table, 'UPDATE'), "
                        "has_table_privilege("
                        "'stockapi_snapshot_builder', :table, 'DELETE'), "
                        "has_table_privilege("
                        "'stockapi_snapshot_builder', :table, 'TRUNCATE'), "
                        "has_any_column_privilege("
                        "'stockapi_snapshot_builder', :table, 'SELECT')"
                    ),
                    {"table": table_name},
                )
            ).one()
            assert tuple(builder) == (False, False, False, False, False, False)

        for function_name in (
            "canonical_forecast_calibration_json(jsonb)",
            "reject_forecast_calibration_evidence_mutation()",
            "stamp_forecast_heldout_coverage_release_availability()",
        ):
            runtime, builder = (
                await conn.execute(
                    text(
                        "SELECT has_function_privilege("
                        "'stockapi_app', :function, 'EXECUTE'), "
                        "has_function_privilege("
                        "'stockapi_snapshot_builder', :function, 'EXECUTE')"
                    ),
                    {"function": function_name},
                )
            ).one()
            assert runtime is False
            assert builder is False
        for function_name in (
            "publish_fitted_calibration_set(bytea)",
            "publish_forecast_heldout_coverage_release(bytea)",
            "publish_forecast_heldout_coverage_release_receipt(varchar)",
        ):
            runtime, builder = (
                await conn.execute(
                    text(
                        "SELECT has_function_privilege("
                        "'stockapi_app', :function, 'EXECUTE'), "
                        "has_function_privilege("
                        "'stockapi_snapshot_builder', :function, 'EXECUTE')"
                    ),
                    {"function": function_name},
                )
            ).one()
            assert runtime is True
            assert builder is False


async def test_realized_outcome_binds_exact_receipt_and_is_immutable(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
) -> None:
    maker = build_sessionmaker(engine)
    symbol = "EVID"
    observed_at = datetime(2026, 7, 6, 20, tzinfo=UTC)
    source_bar = OHLCVBar(
        symbol=symbol,
        timestamp=observed_at,
        timespan="day",
        multiplier=1,
        open=122.5,
        high=124.0,
        low=122.0,
        close=123.5,
        volume=1_000.0,
        source="polygon_open_close",
        adjustment_basis="raw",
        fetched_at=observed_at + timedelta(minutes=1),
    )
    async with maker() as session, session.begin():
        plan = await upsert_bars(session, [source_bar])
    async with maker() as session, session.begin():
        assert await finalize_bar_version_availability(session, plan.rows) == 1

    async with maker() as session:
        bar = (await session.execute(select(Bar).where(Bar.symbol == symbol))).scalar_one()
        receipt = (
            await session.execute(
                select(BarVersionAvailability).where(
                    BarVersionAvailability.symbol == bar.symbol,
                    BarVersionAvailability.timespan == bar.timespan,
                    BarVersionAvailability.multiplier == bar.multiplier,
                    BarVersionAvailability.ts == bar.ts,
                    BarVersionAvailability.source == bar.source,
                    BarVersionAvailability.adjustment_basis == bar.adjustment_basis,
                    BarVersionAvailability.version_recorded_at == bar.recorded_at,
                )
            )
        ).scalar_one()

    async def policy_after(
        available_at: datetime,
    ) -> tuple[ForecastOutcomeResolutionPolicy, datetime]:
        # Pick an explicit integer lag whose deterministic cutoff is just after
        # this real DB receipt, then wait only for the DB clock to cross it.
        lag_seconds = int((available_at - observed_at).total_seconds()) + 2
        policy = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=lag_seconds)
        cutoff = policy.cutoff_for(observed_at)
        while True:
            async with maker() as session:
                database_now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            remaining = (cutoff - database_now).total_seconds()
            if remaining <= 0:
                return policy, cutoff
            await asyncio.sleep(min(remaining + 0.02, 0.25))

    policy, cutoff = await policy_after(receipt.available_at)
    resolver = SqlOutcomeBarVersionResolver(maker, policy)
    source = await resolver.resolve(
        symbol=symbol,
        target_time=observed_at,
        resolution_cutoff=cutoff,
    )
    assert source.value == bar.close
    assert source.version_recorded_at == bar.recorded_at
    assert source.available_at == receipt.available_at
    payload = RealizedOutcomePayload(
        outcome_resolution_policy_hash=policy.outcome_resolution_policy_hash,
        availability_rule_set_hash=policy.availability_rule_set_hash,
        resolution_cutoff=cutoff,
        symbol=bar.symbol,
        target="close",
        series_basis="raw",
        target_time=bar.ts,
        currency="USD",
        realized_value=bar.close,
        source_version=source,
    )
    canonical = canonical_outcome_payload(payload)
    outcome_id = outcome_id_for_payload(canonical)
    registered = await SqlForecastOutcomePolicyStore(maker).register(policy)
    assert registered.record.policy_hash == policy.outcome_resolution_policy_hash

    # Runtime cannot bypass the provenance boundary with a raw INSERT. The
    # function itself also fails closed after policy/max-version validation
    # when no prospectively sealed cohort member authorizes this old target.
    async with engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(DBAPIError, match="permission denied"):
            await conn.execute(
                pg_insert(ForecastRealizedOutcome).values(
                    outcome_id=outcome_id,
                    canonical_evidence=canonical,
                )
            )
        await transaction.rollback()

    # The callable boundary must reject oversized or semantically equivalent
    # but noncanonical JSON before it can occupy the immutable semantic key.
    for rejected_bytes, message in (
        (b"{" + b" " * 262_144, "invalid or exceeds its bound"),
        (
            canonical.replace(b'{"format"', b'{ "format"', 1),
            "not the exact canonical form",
        ),
    ):
        rejected_id = "sha256:" + hashlib.sha256(rejected_bytes).hexdigest()
        async with engine.connect() as conn:
            transaction = await conn.begin()
            with pytest.raises(DBAPIError, match=message):
                await conn.execute(
                    text(
                        "SELECT public.publish_forecast_realized_outcome("
                        ":cohort_id, :forecast_id, :step, :outcome_id, :canonical)"
                    ),
                    {
                        "cohort_id": "sha256:" + "f" * 64,
                        "forecast_id": UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                        "step": 1,
                        "outcome_id": rejected_id,
                        "canonical": rejected_bytes,
                    },
                )
            await transaction.rollback()

    non_usd = canonical.replace(b'"currency":"USD"', b'"currency":"EUR"', 1)
    async with engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(DBAPIError, match="registered cutoff policy"):
            await conn.execute(
                text(
                    "SELECT public.publish_forecast_realized_outcome("
                    ":cohort_id, :forecast_id, :step, :outcome_id, :canonical)"
                ),
                {
                    "cohort_id": "sha256:" + "f" * 64,
                    "forecast_id": UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                    "step": 1,
                    "outcome_id": "sha256:" + hashlib.sha256(non_usd).hexdigest(),
                    "canonical": non_usd,
                },
            )
        await transaction.rollback()
    async with engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(
            DBAPIError,
            match="outcome publication is not backed by an exact sealed cohort member",
        ):
            await conn.execute(
                text(
                    "SELECT public.publish_forecast_realized_outcome("
                    ":cohort_id, :forecast_id, :step, :outcome_id, :canonical)"
                ),
                {
                    "cohort_id": "sha256:" + "f" * 64,
                    "forecast_id": UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                    "step": 1,
                    "outcome_id": outcome_id,
                    "canonical": canonical,
                },
            )
        await transaction.rollback()
    async with maker() as session:
        persisted_count = (
            await session.execute(
                select(func.count())
                .select_from(ForecastRealizedOutcome)
                .where(ForecastRealizedOutcome.outcome_id == outcome_id)
            )
        ).scalar_one()
    assert persisted_count == 0

    # A restatement finalized after the frozen cutoff must not rewrite truth
    # under this policy. A separately hashed later-lag policy may select it.
    restated_close = bar.close + 3.0
    restatement = OHLCVBar(
        symbol=symbol,
        timestamp=observed_at,
        timespan="day",
        multiplier=1,
        open=restated_close - 1.0,
        high=restated_close + 1.0,
        low=restated_close - 2.0,
        close=restated_close,
        volume=1_100.0,
        source="polygon_open_close",
        adjustment_basis="raw",
        fetched_at=source_bar.fetched_at + timedelta(minutes=1),
    )
    async with maker() as session, session.begin():
        restated_plan = await upsert_bars(session, [restatement])
    async with maker() as session, session.begin():
        assert await finalize_bar_version_availability(session, restated_plan.rows) == 1
    async with maker() as session:
        restated_receipt = (
            (
                await session.execute(
                    select(BarVersionAvailability)
                    .where(
                        BarVersionAvailability.symbol == symbol,
                        BarVersionAvailability.version_recorded_at != source.version_recorded_at,
                    )
                    .order_by(BarVersionAvailability.version_recorded_at.desc())
                )
            )
            .scalars()
            .first()
        )
    assert restated_receipt is not None
    assert restated_receipt.available_at > cutoff
    still_original = await resolver.resolve(
        symbol=symbol,
        target_time=observed_at,
        resolution_cutoff=cutoff,
    )
    assert still_original == source
    later_policy, later_cutoff = await policy_after(restated_receipt.available_at)
    later_source = await SqlOutcomeBarVersionResolver(maker, later_policy).resolve(
        symbol=symbol,
        target_time=observed_at,
        resolution_cutoff=later_cutoff,
    )
    assert later_policy.outcome_resolution_policy_hash != policy.outcome_resolution_policy_hash
    assert later_source.value == restated_close
    assert later_source.version_recorded_at == restated_receipt.version_recorded_at
    await SqlForecastOutcomePolicyStore(maker).register(later_policy)

    lane_lock = bar_series_lock_id(symbol, "polygon_open_close", "day")

    async def owner_insert_rejected(
        candidate_id: str,
        candidate_canonical: bytes,
        message: str,
    ) -> None:
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            await conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": lane_lock},
            )
            with pytest.raises(DBAPIError, match=message):
                await conn.execute(
                    pg_insert(ForecastRealizedOutcome).values(
                        outcome_id=candidate_id,
                        canonical_evidence=candidate_canonical,
                    )
                )
            await transaction.rollback()

    await owner_insert_rejected(
        "sha256:" + "0" * 64,
        canonical,
        "ck_forecast_realized_outcomes_outcome_id_matches_payload",
    )

    wrong_value = bar.close + 1.0
    wrong_value_source = replace(source, value=wrong_value)
    wrong_value_payload = replace(
        payload,
        realized_value=wrong_value,
        source_version=wrong_value_source,
    )
    wrong_value_canonical = canonical_outcome_payload(wrong_value_payload)
    await owner_insert_rejected(
        outcome_id_for_payload(wrong_value_canonical),
        wrong_value_canonical,
        "does not match its exact bar version",
    )

    fake_receipt_time = receipt.available_at + timedelta(microseconds=1)
    wrong_receipt_source = replace(source, available_at=fake_receipt_time)
    wrong_receipt_payload = replace(
        payload,
        source_version=wrong_receipt_source,
    )
    wrong_receipt_canonical = canonical_outcome_payload(wrong_receipt_payload)
    await owner_insert_rejected(
        outcome_id_for_payload(wrong_receipt_canonical),
        wrong_receipt_canonical,
        "stored availability receipt",
    )

    wrong_cutoff_payload = replace(
        payload,
        resolution_cutoff=cutoff + timedelta(seconds=1),
    )
    wrong_cutoff_canonical = canonical_outcome_payload(wrong_cutoff_payload)
    await owner_insert_rejected(
        outcome_id_for_payload(wrong_cutoff_canonical),
        wrong_cutoff_canonical,
        "registered cutoff policy",
    )

    stale_source_payload = replace(
        payload,
        outcome_resolution_policy_hash=later_policy.outcome_resolution_policy_hash,
        availability_rule_set_hash=later_policy.availability_rule_set_hash,
        resolution_cutoff=later_cutoff,
    )
    stale_source_canonical = canonical_outcome_payload(stale_source_payload)
    await owner_insert_rejected(
        outcome_id_for_payload(stale_source_canonical),
        stale_source_canonical,
        "unique newest cutoff-visible version",
    )

    # Admin-only direct insertion is still forced through the byte, policy,
    # lock, and unique-maximum triggers. Use a rolled-back valid row to prove
    # every mutation path remains blocked without manufacturing durable truth.
    for mutation in (
        "UPDATE forecast_realized_outcomes SET symbol = symbol WHERE outcome_id = :outcome_id",
        "DELETE FROM forecast_realized_outcomes WHERE outcome_id = :outcome_id",
        "TRUNCATE forecast_realized_outcomes CASCADE",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            await conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": lane_lock},
            )
            await conn.execute(
                pg_insert(ForecastRealizedOutcome).values(
                    outcome_id=outcome_id,
                    canonical_evidence=canonical,
                )
            )
            with pytest.raises(DBAPIError, match="forecast evidence is insert-only"):
                await conn.execute(
                    text(mutation),
                    {"outcome_id": outcome_id} if ":outcome_id" in mutation else {},
                )
            await transaction.rollback()


async def test_receipt_fence_serializes_read_committed_outcome_resolution(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
    migrated_database_url: LiveDatabaseUrls,
) -> None:
    """Prove the DB receipt fence closes the resolver's pre-commit snapshot race."""

    maker = build_sessionmaker(engine)
    symbol = "FENCE"
    async with owner_engine.connect() as conn:
        database_now = (await conn.execute(select(func.clock_timestamp()))).scalar_one()
    observed_at = _session_close(latest_completed_xnys_session(database_now))
    source_bar = OHLCVBar(
        symbol=symbol,
        timestamp=observed_at,
        timespan="day",
        multiplier=1,
        open=210.0,
        high=212.0,
        low=209.0,
        close=211.5,
        volume=2_000.0,
        source="polygon_open_close",
        adjustment_basis="raw",
        fetched_at=observed_at,
    )
    async with maker() as session, session.begin():
        plan = await upsert_bars(session, [source_bar])
    async with maker() as session:
        bar = (await session.execute(select(Bar).where(Bar.symbol == symbol))).scalar_one()

    lane_lock = bar_series_lock_id(symbol, "polygon_open_close", "day")
    async with owner_engine.connect() as conn:
        database_lane_lock = (
            await conn.execute(
                text(
                    "SELECT public.forecast_bar_series_fence_id("
                    ":symbol, 'polygon_open_close', 'day')"
                ),
                {"symbol": symbol},
            )
        ).scalar_one()
    assert database_lane_lock == lane_lock

    release_writer = asyncio.Event()
    receipt_ready: asyncio.Future[datetime] = asyncio.get_running_loop().create_future()

    async def hold_uncommitted_receipt() -> datetime:
        try:
            async with maker() as session, session.begin():
                assert await finalize_bar_version_availability(session, plan.rows) == 1
                receipt = (
                    await session.execute(
                        select(BarVersionAvailability).where(
                            BarVersionAvailability.symbol == bar.symbol,
                            BarVersionAvailability.timespan == bar.timespan,
                            BarVersionAvailability.multiplier == bar.multiplier,
                            BarVersionAvailability.ts == bar.ts,
                            BarVersionAvailability.source == bar.source,
                            BarVersionAvailability.adjustment_basis == bar.adjustment_basis,
                            BarVersionAvailability.version_recorded_at == bar.recorded_at,
                        )
                    )
                ).scalar_one()
                receipt_ready.set_result(receipt.available_at)
                await release_writer.wait()
                return receipt.available_at
        except Exception as exc:
            if not receipt_ready.done():
                receipt_ready.set_exception(exc)
            raise

    repeatable_engine = create_async_engine(
        migrated_database_url.runtime,
        isolation_level="REPEATABLE READ",
    )
    writer_task = asyncio.create_task(hold_uncommitted_receipt())
    resolver_task: asyncio.Task[Any] | None = None
    tasks: list[asyncio.Task[Any]] = [writer_task]
    try:
        available_at = await asyncio.wait_for(asyncio.shield(receipt_ready), timeout=5)
        lag_seconds = int((available_at - observed_at).total_seconds()) + 5
        policy = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=lag_seconds)
        cutoff = policy.cutoff_for(observed_at)
        assert available_at < cutoff

        resolver = SqlOutcomeBarVersionResolver(
            build_sessionmaker(repeatable_engine),
            policy,
        )
        resolver_task = asyncio.create_task(
            resolver.resolve(
                symbol=symbol,
                target_time=observed_at,
                resolution_cutoff=cutoff,
            )
        )
        tasks.append(resolver_task)

        lock_state_statement = text(
            """
            WITH target AS (
                SELECT public.forecast_bar_series_fence_id(
                    :symbol, 'polygon_open_close', 'day'
                ) AS lock_id
            )
            SELECT count(*) FILTER (WHERE held.granted),
                   count(*) FILTER (WHERE NOT held.granted)
            FROM pg_catalog.pg_locks AS held
            CROSS JOIN target
            WHERE held.locktype = 'advisory'
              AND held.database = (
                  SELECT oid FROM pg_catalog.pg_database
                  WHERE datname = current_database()
              )
              AND held.objsubid = 1
              AND held.mode = 'ExclusiveLock'
              AND held.classid = ((target.lock_id >> 32) & 4294967295)::oid
              AND held.objid = (target.lock_id & 4294967295)::oid
            """
        )
        loop = asyncio.get_running_loop()
        lock_deadline = loop.time() + 5
        async with owner_engine.connect() as monitor:
            while True:
                granted, waiting = (
                    await monitor.execute(lock_state_statement, {"symbol": symbol})
                ).one()
                if (granted, waiting) == (1, 1):
                    break
                if resolver_task.done():
                    await resolver_task
                    pytest.fail("outcome resolver completed before the receipt fence released")
                if loop.time() >= lock_deadline:
                    pytest.fail(
                        "receipt writer and outcome resolver did not contend on one lane fence"
                    )
                await asyncio.sleep(0.01)

            assert not resolver_task.done()
            while True:
                database_now = (await monitor.execute(select(func.clock_timestamp()))).scalar_one()
                remaining = (cutoff - database_now).total_seconds()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(max(remaining, 0.01), 0.1))

        release_writer.set()
        committed_available_at = await asyncio.wait_for(writer_task, timeout=5)
        evidence = await asyncio.wait_for(resolver_task, timeout=5)
        assert committed_available_at == available_at
        assert evidence.symbol == symbol
        assert evidence.value == source_bar.close
        assert evidence.version_recorded_at == bar.recorded_at
        assert evidence.available_at == available_at
        assert evidence.available_at <= cutoff

        payload = RealizedOutcomePayload(
            outcome_resolution_policy_hash=policy.outcome_resolution_policy_hash,
            availability_rule_set_hash=policy.availability_rule_set_hash,
            resolution_cutoff=cutoff,
            symbol=symbol,
            target="close",
            series_basis="raw",
            target_time=observed_at,
            currency="USD",
            realized_value=evidence.value,
            source_version=evidence,
        )
        canonical = canonical_outcome_payload(payload)
        outcome_id = outcome_id_for_payload(canonical)
        await SqlForecastOutcomePolicyStore(maker).register(policy)

        # The owner cannot bypass the public publisher under a stale snapshot.
        # This reaches the table trigger with otherwise-valid canonical bytes.
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            await conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            with pytest.raises(DBAPIError, match="requires READ COMMITTED") as excinfo:
                await conn.execute(
                    pg_insert(ForecastRealizedOutcome).values(
                        outcome_id=outcome_id,
                        canonical_evidence=canonical,
                    )
                )
            assert getattr(excinfo.value.orig, "sqlstate", None) == "55000"
            await transaction.rollback()

        # The runtime's only publication capability independently enforces the
        # same isolation contract before parsing or provenance lookup.
        async with repeatable_engine.connect() as conn:
            transaction = await conn.begin()
            isolation = (await conn.execute(text("SHOW transaction_isolation"))).scalar_one()
            assert isolation == "repeatable read"
            with pytest.raises(DBAPIError, match="requires READ COMMITTED") as excinfo:
                await conn.execute(
                    text(
                        "SELECT public.publish_forecast_realized_outcome("
                        ":cohort_id, :forecast_id, :step, :outcome_id, :canonical)"
                    ),
                    {
                        "cohort_id": "sha256:" + "f" * 64,
                        "forecast_id": UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                        "step": 1,
                        "outcome_id": outcome_id,
                        "canonical": canonical,
                    },
                )
            assert getattr(excinfo.value.orig, "sqlstate", None) == "55000"
            await transaction.rollback()

        async with maker() as session:
            persisted_count = (
                await session.execute(
                    select(func.count())
                    .select_from(ForecastRealizedOutcome)
                    .where(ForecastRealizedOutcome.outcome_id == outcome_id)
                )
            ).scalar_one()
        assert persisted_count == 0
    finally:
        release_writer.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await repeatable_engine.dispose()


def _scheduled_response(
    *,
    forecast_id: UUID,
    snapshot: ForecastInputSnapshotRecord,
    target_time: datetime,
) -> ForecastResponse:
    quantiles = [
        ForecastQuantile(level=0.1, value=98.0),
        ForecastQuantile(level=0.5, value=100.0),
        ForecastQuantile(level=0.9, value=102.0),
    ]
    generated_at = snapshot.as_of + timedelta(minutes=2)
    return ForecastResponse(
        symbol=snapshot.symbol,
        target="close",
        horizon=1,
        horizon_unit=snapshot.horizon_unit,
        as_of=snapshot.as_of,
        currency=snapshot.currency,
        forecasts=[
            ForecastStep(
                step=1,
                target_time=target_time,
                point=100.0,
                quantiles=quantiles,
                intervals=[
                    ForecastInterval(
                        coverage=0.8,
                        lower_quantile=0.1,
                        upper_quantile=0.9,
                        lower=98.0,
                        upper=102.0,
                    )
                ],
            )
        ],
        provenance=ForecastProvenance(
            forecast_id=forecast_id,
            snapshot_id=snapshot.snapshot_id,
            model_version="baseline-naive@1",
            series_basis="raw",
            feature_set_hash=snapshot.snapshot_id,
            max_available_at=snapshot.max_available_at,
            generated_at=generated_at,
            code_version="live-gate",
            data_sources=[
                DataSourceLineage(
                    name="live-gate",
                    snapshot_id=snapshot.snapshot_id,
                    max_available_at=snapshot.max_available_at,
                    fields=["close"],
                )
            ],
            lookahead_check=LookaheadCheck(
                status="passed",
                checked_at=generated_at,
                max_feature_available_at=snapshot.max_available_at,
            ),
        ),
        calibration=ForecastCalibration(
            calibration_set_version="uncalibrated:baseline-naive@1",
            method="none",
            sample_count=0,
        ),
    )


async def test_cohort_requires_post_commit_seal_and_is_immutable(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
) -> None:
    async with engine.connect() as conn:
        database_now = (await conn.execute(select(func.clock_timestamp()))).scalar_one()
    earliest_target = database_now + timedelta(days=1)
    resolution_policy_hash = "sha256:" + "7" * 64
    availability_rule_set_hash = "sha256:" + "8" * 64
    snapshot = _forecast_snapshot_record(policy_hash=resolution_policy_hash)
    async with owner_engine.begin() as conn:
        await conn.execute(pg_insert(ForecastInputSnapshot).values(**_record_values(snapshot)))

    forecast_id = UUID("33333333-3333-3333-3333-333333333333")
    response = _scheduled_response(
        forecast_id=forecast_id,
        snapshot=snapshot,
        target_time=earliest_target,
    )
    request = ForecastRequest(
        symbol=snapshot.symbol,
        horizon=1,
        horizon_unit=snapshot.horizon_unit,
        target="close",
        snapshot_id=snapshot.snapshot_id,
        model="baseline_naive",
        interval_coverages=[0.8],
    )
    canonical_request_bytes = canonical_request(request)
    canonical_output_bytes = canonical_output(response)
    run_opportunity_hash = opportunity_hash(
        response,
        resolution_policy_hash=resolution_policy_hash,
        availability_rule_set_hash=availability_rule_set_hash,
        origin_kind="scheduled_evaluation",
    )
    run_values: dict[str, object] = {
        "forecast_id": forecast_id,
        "schema_version": 1,
        "origin_kind": "scheduled_evaluation",
        "idempotency_token_digest": None,
        "request_hash": request_hash(canonical_request_bytes),
        "opportunity_hash": run_opportunity_hash,
        "output_hash": output_hash(canonical_output_bytes),
        "snapshot_id": snapshot.snapshot_id,
        "resolution_policy_hash": resolution_policy_hash,
        "availability_rule_set_hash": availability_rule_set_hash,
        "symbol": response.symbol,
        "target": response.target,
        "horizon": response.horizon,
        "horizon_unit": response.horizon_unit,
        "series_basis": response.provenance.series_basis,
        "as_of": response.as_of,
        "max_available_at": response.provenance.max_available_at,
        "model_version": response.provenance.model_version,
        "feature_set_hash": snapshot.snapshot_id,
        "code_version": response.provenance.code_version,
        "calibration_set_version": response.calibration.calibration_set_version,
        "calibration_method": response.calibration.method,
        "generated_at": response.provenance.generated_at,
        "recorded_at": datetime(2000, 1, 1, tzinfo=UTC),
        "canonical_request": canonical_request_bytes,
        "canonical_output": canonical_output_bytes,
    }
    async with engine.begin() as conn:
        await conn.execute(pg_insert(ForecastRun).values(**run_values))

    runtime_maker = build_sessionmaker(engine)
    outcome_policy = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=24 * 60 * 60)
    await SqlForecastOutcomePolicyStore(runtime_maker).register(outcome_policy)
    async with runtime_maker() as session:
        scheduled_run = (
            await session.execute(select(ForecastRun).where(ForecastRun.forecast_id == forecast_id))
        ).scalar_one()
        member = member_from_scheduled_run(scheduled_run, step=1)

    async def _insert_scheduled_member(
        new_forecast_id: UUID,
        target_time: datetime,
    ) -> ForecastCohortMember:
        new_response = _scheduled_response(
            forecast_id=new_forecast_id,
            snapshot=snapshot,
            target_time=target_time,
        )
        new_output = canonical_output(new_response)
        new_values = {
            **run_values,
            "forecast_id": new_forecast_id,
            "opportunity_hash": opportunity_hash(
                new_response,
                resolution_policy_hash=resolution_policy_hash,
                availability_rule_set_hash=availability_rule_set_hash,
                origin_kind="scheduled_evaluation",
            ),
            "output_hash": output_hash(new_output),
            "canonical_output": new_output,
        }
        async with engine.begin() as conn:
            await conn.execute(pg_insert(ForecastRun).values(**new_values))
        async with runtime_maker() as session:
            row = await session.get(ForecastRun, new_forecast_id)
            assert row is not None
            return member_from_scheduled_run(row, step=1)

    manifest = ForecastCohortManifest(
        purpose="heldout_evaluation",
        selection_policy_hash="sha256:" + "9" * 64,
        outcome_resolution_policy_hash=outcome_policy.outcome_resolution_policy_hash,
        availability_rule_set_hash=outcome_policy.availability_rule_set_hash,
        members=(member,),
    )
    canonical = canonical_cohort_manifest(manifest)
    cohort_id = cohort_id_for_manifest(canonical)
    caller_timestamp = datetime(2000, 1, 1, tzinfo=UTC)

    # A self-consistent canonical manifest is not sufficient: its materialized
    # projection must match the exact scheduled archive bytes. This is the live
    # regression for the forged-header/member gap that existed in the draft
    # schema before the normalized member table and validation trigger.
    forged_manifest = replace(
        manifest,
        members=(replace(member, target_time=earliest_target + timedelta(minutes=1)),),
    )
    forged_canonical = canonical_cohort_manifest(forged_manifest)
    async with engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(DBAPIError, match="step does not match scheduled output"):
            await conn.execute(
                pg_insert(ForecastOutcomeCohortManifest).values(
                    cohort_id=cohort_id_for_manifest(forged_canonical),
                    canonical_manifest=forged_canonical,
                )
            )
        await transaction.rollback()

    manifest_insert = (
        pg_insert(ForecastOutcomeCohortManifest)
        .values(
            cohort_id=cohort_id,
            recorded_at=caller_timestamp,
            creator_xid=0,
            canonical_manifest=canonical,
        )
        .returning(
            ForecastOutcomeCohortManifest.recorded_at,
            ForecastOutcomeCohortManifest.creator_xid,
        )
    )

    async with engine.connect() as conn:
        transaction = await conn.begin()
        same_transaction_manifest = (await conn.execute(manifest_insert)).one()
        with pytest.raises(DBAPIError, match="requires a later transaction"):
            await conn.execute(
                pg_insert(ForecastOutcomeCohortAvailability).values(
                    cohort_id=cohort_id,
                    manifest_recorded_at=same_transaction_manifest.recorded_at,
                    sealed_at=caller_timestamp,
                    sealer_xid=0,
                )
            )
        await transaction.rollback()

    cohort_store = SqlForecastCohortStore(runtime_maker)
    proof = await cohort_store.publish(manifest)
    replayed_proof = await cohort_store.publish(manifest)
    assert replayed_proof == proof
    stored_manifest = proof.record
    assert stored_manifest.recorded_at != caller_timestamp
    assert stored_manifest.creator_xid > 0
    assert stored_manifest.recorded_at < earliest_target

    async with runtime_maker() as session:
        projected_member = (
            await session.execute(
                select(ForecastOutcomeCohortMember).where(
                    ForecastOutcomeCohortMember.cohort_id == cohort_id
                )
            )
        ).scalar_one()
    assert projected_member.forecast_id == forecast_id
    assert projected_member.step == 1
    assert projected_member.target_time == earliest_target
    assert projected_member.opportunity_hash == run_opportunity_hash
    assert projected_member.output_hash == output_hash(canonical_output_bytes)

    seal = proof.seal
    assert seal.sealer_xid != stored_manifest.creator_xid
    assert seal.manifest_recorded_at == stored_manifest.recorded_at
    assert stored_manifest.recorded_at <= seal.sealed_at < earliest_target
    assert seal.sealed_at != caller_timestamp

    expired_member = await _insert_scheduled_member(
        UUID("44444444-4444-4444-4444-444444444444"),
        database_now - timedelta(minutes=1),
    )
    expired_manifest = replace(
        manifest,
        selection_policy_hash="sha256:" + "b" * 64,
        members=(expired_member,),
    )
    expired_cohort_id = cohort_id_for_manifest(expired_manifest)
    with pytest.raises(AppError) as expired_error:
        await cohort_store.publish(expired_manifest)
    assert expired_error.value.code == "forecast_cohort_deadline_expired"
    assert expired_error.value.status_code == 409
    assert expired_error.value.details == {
        "retryable": False,
        "stage": "manifest",
    }
    async with runtime_maker() as session:
        assert (await session.get(ForecastOutcomeCohortManifest, expired_cohort_id)) is None

    manifest_race_member = await _insert_scheduled_member(
        UUID("55555555-5555-5555-5555-555555555555"),
        earliest_target + timedelta(minutes=10),
    )
    manifest_race = replace(
        manifest,
        selection_policy_hash="sha256:" + "c" * 64,
        members=(manifest_race_member,),
    )
    manifest_barrier = _MissingReadBarrier(ForecastOutcomeCohortManifest)
    manifest_race_maker = _BarrierSessionMaker(runtime_maker, manifest_barrier)
    manifest_race_results = await asyncio.wait_for(
        asyncio.gather(
            SqlForecastCohortStore(  # type: ignore[arg-type]
                manifest_race_maker
            ).publish(manifest_race),
            SqlForecastCohortStore(  # type: ignore[arg-type]
                manifest_race_maker
            ).publish(manifest_race),
        ),
        timeout=10,
    )
    assert manifest_barrier.arrivals == 2
    assert manifest_race_results[0] == manifest_race_results[1]

    seal_race_member = await _insert_scheduled_member(
        UUID("66666666-6666-6666-6666-666666666666"),
        earliest_target + timedelta(minutes=20),
    )
    seal_race = replace(
        manifest,
        selection_policy_hash="sha256:" + "d" * 64,
        members=(seal_race_member,),
    )
    seal_race_canonical = canonical_cohort_manifest(seal_race)
    seal_race_id = cohort_id_for_manifest(seal_race_canonical)
    async with engine.begin() as conn:
        await conn.execute(
            pg_insert(ForecastOutcomeCohortManifest).values(
                cohort_id=seal_race_id,
                recorded_at=caller_timestamp,
                creator_xid=0,
                canonical_manifest=seal_race_canonical,
            )
        )
    seal_barrier = _MissingReadBarrier(ForecastOutcomeCohortAvailability)
    seal_race_maker = _BarrierSessionMaker(runtime_maker, seal_barrier)
    seal_race_results = await asyncio.wait_for(
        asyncio.gather(
            SqlForecastCohortStore(  # type: ignore[arg-type]
                seal_race_maker
            ).publish(seal_race),
            SqlForecastCohortStore(  # type: ignore[arg-type]
                seal_race_maker
            ).publish(seal_race),
        ),
        timeout=10,
    )
    assert seal_barrier.arrivals == 2
    assert seal_race_results[0] == seal_race_results[1]

    async with runtime_maker() as session:
        for race_proof in (manifest_race_results[0], seal_race_results[0]):
            manifest_count = (
                await session.execute(
                    select(func.count())
                    .select_from(ForecastOutcomeCohortManifest)
                    .where(ForecastOutcomeCohortManifest.cohort_id == race_proof.record.cohort_id)
                )
            ).scalar_one()
            seal_count = (
                await session.execute(
                    select(func.count())
                    .select_from(ForecastOutcomeCohortAvailability)
                    .where(
                        ForecastOutcomeCohortAvailability.cohort_id == race_proof.record.cohort_id
                    )
                )
            ).scalar_one()
            assert manifest_count == seal_count == 1
            assert race_proof.record.creator_xid != race_proof.seal.sealer_xid

    async with engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(DBAPIError, match="permission denied"):
            await conn.execute(
                pg_insert(ForecastOutcomeCohortMember).values(
                    cohort_id=cohort_id,
                    forecast_id=forecast_id,
                    step=2,
                    target_time=earliest_target,
                    opportunity_hash=run_opportunity_hash,
                    output_hash=output_hash(canonical_output_bytes),
                )
            )
        await transaction.rollback()

    for mutation in (
        "UPDATE forecast_outcome_cohort_manifests SET purpose = purpose "
        "WHERE cohort_id = :cohort_id",
        "DELETE FROM forecast_outcome_cohort_manifests WHERE cohort_id = :cohort_id",
        "TRUNCATE forecast_outcome_cohort_manifests CASCADE",
        "UPDATE forecast_outcome_cohort_members SET step = step WHERE cohort_id = :cohort_id",
        "DELETE FROM forecast_outcome_cohort_members WHERE cohort_id = :cohort_id",
        "TRUNCATE forecast_outcome_cohort_members CASCADE",
        "UPDATE forecast_outcome_cohort_availability SET cohort_id = cohort_id "
        "WHERE cohort_id = :cohort_id",
        "DELETE FROM forecast_outcome_cohort_availability WHERE cohort_id = :cohort_id",
        "TRUNCATE forecast_outcome_cohort_availability",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            with pytest.raises(DBAPIError, match="forecast evidence is insert-only"):
                await conn.execute(
                    text(mutation),
                    {"cohort_id": cohort_id} if ":cohort_id" in mutation else {},
                )
            await transaction.rollback()


async def test_synthetic_publisher_path_binds_exact_snapshot_and_source(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
) -> None:
    """Exercise publication plumbing without claiming a real market outcome.

    The target is a short-lived synthetic timestamp in the owner-designated
    throwaway database.  This proves the callable DB boundary, provenance
    checks, and source-link replay; it deliberately does not substitute for a
    prospective XNYS forecast resolving over real vendor data.
    """

    runtime_maker = build_sessionmaker(engine)
    async with runtime_maker() as session:
        database_now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    target_time = database_now + timedelta(seconds=8)
    outcome_policy = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=10)
    await SqlForecastOutcomePolicyStore(runtime_maker).register(outcome_policy)

    snapshot = _forecast_snapshot_record(
        symbol="PUB",
        as_of=database_now - timedelta(minutes=5),
        target_times=(target_time,),
    )
    async with snapshot_builder_engine.begin() as conn:
        await conn.execute(pg_insert(ForecastInputSnapshot).values(**_record_values(snapshot)))

    forecast_policy_hash = "sha256:" + "1" * 64
    forecast_rule_set_hash = "sha256:" + "2" * 64
    cohort_store = SqlForecastCohortStore(runtime_maker)

    async def archive_and_seal(
        *,
        forecast_id: UUID,
        response_snapshot: ForecastInputSnapshotRecord,
        selection_hash: str,
    ) -> tuple[ForecastOutcomePublicationSource, ForecastCohortProof]:
        response = _scheduled_response(
            forecast_id=forecast_id,
            snapshot=response_snapshot,
            target_time=target_time,
        )
        request = ForecastRequest(
            symbol=snapshot.symbol,
            horizon=1,
            horizon_unit=snapshot.horizon_unit,
            target="close",
            snapshot_id=snapshot.snapshot_id,
            model="baseline_naive",
            interval_coverages=[0.8],
        )
        request_bytes = canonical_request(request)
        output_bytes = canonical_output(response)
        values = {
            "forecast_id": forecast_id,
            "schema_version": 1,
            "origin_kind": "scheduled_evaluation",
            "idempotency_token_digest": None,
            "request_hash": request_hash(request_bytes),
            "opportunity_hash": opportunity_hash(
                response,
                resolution_policy_hash=forecast_policy_hash,
                availability_rule_set_hash=forecast_rule_set_hash,
                origin_kind="scheduled_evaluation",
            ),
            "output_hash": output_hash(output_bytes),
            # Both rows reference the real immutable snapshot.  The forged
            # case differs only in the canonical output's claimed snapshot.
            "snapshot_id": snapshot.snapshot_id,
            "resolution_policy_hash": forecast_policy_hash,
            "availability_rule_set_hash": forecast_rule_set_hash,
            "symbol": response.symbol,
            "target": response.target,
            "horizon": response.horizon,
            "horizon_unit": response.horizon_unit,
            "series_basis": response.provenance.series_basis,
            "as_of": response.as_of,
            "max_available_at": response.provenance.max_available_at,
            "model_version": response.provenance.model_version,
            "feature_set_hash": response.provenance.feature_set_hash,
            "code_version": response.provenance.code_version,
            "calibration_set_version": response.calibration.calibration_set_version,
            "calibration_method": response.calibration.method,
            "generated_at": response.provenance.generated_at,
            "canonical_request": request_bytes,
            "canonical_output": output_bytes,
        }
        async with engine.begin() as conn:
            await conn.execute(pg_insert(ForecastRun).values(**values))
        async with runtime_maker() as session:
            run = await session.get(ForecastRun, forecast_id)
            assert run is not None
            member = member_from_scheduled_run(run, step=1)
        manifest = ForecastCohortManifest(
            purpose="heldout_evaluation",
            selection_policy_hash=selection_hash,
            outcome_resolution_policy_hash=outcome_policy.outcome_resolution_policy_hash,
            availability_rule_set_hash=outcome_policy.availability_rule_set_hash,
            members=(member,),
        )
        proof = await cohort_store.publish(manifest)
        return (
            ForecastOutcomePublicationSource(
                cohort_id=proof.record.cohort_id,
                forecast_id=forecast_id,
                step=1,
            ),
            proof,
        )

    valid_source, valid_cohort = await archive_and_seal(
        forecast_id=UUID("61616161-6161-6161-6161-616161616161"),
        response_snapshot=snapshot,
        selection_hash="sha256:" + "3" * 64,
    )
    forged_snapshot = replace(snapshot, snapshot_id="sha256:" + "e" * 64)
    forged_source, forged_cohort = await archive_and_seal(
        forecast_id=UUID("62626262-6262-6262-6262-626262626262"),
        response_snapshot=forged_snapshot,
        selection_hash="sha256:" + "4" * 64,
    )
    assert valid_cohort.seal.sealed_at < target_time
    assert forged_cohort.seal.sealed_at < target_time

    async def wait_for_database(moment: datetime) -> None:
        while True:
            async with runtime_maker() as session:
                now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            remaining = (moment - now).total_seconds()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining + 0.02, 0.25))

    await wait_for_database(target_time)
    bar_input = OHLCVBar(
        symbol="PUB",
        timestamp=target_time,
        timespan="day",
        multiplier=1,
        open=200.0,
        high=202.0,
        low=199.0,
        close=201.0,
        volume=1_000.0,
        source="polygon_open_close",
        adjustment_basis="raw",
        fetched_at=target_time,
    )
    async with runtime_maker() as session, session.begin():
        plan = await upsert_bars(session, [bar_input])
    async with runtime_maker() as session, session.begin():
        assert await finalize_bar_version_availability(session, plan.rows) == 1
    async with runtime_maker() as session:
        stored_bar = (await session.execute(select(Bar).where(Bar.symbol == "PUB"))).scalar_one()
        receipt = (
            await session.execute(
                select(BarVersionAvailability).where(
                    BarVersionAvailability.symbol == "PUB",
                    BarVersionAvailability.version_recorded_at == stored_bar.recorded_at,
                )
            )
        ).scalar_one()

    resolution_cutoff = outcome_policy.cutoff_for(target_time)
    assert receipt.available_at <= resolution_cutoff
    await wait_for_database(resolution_cutoff)
    version = BarVersionEvidence(
        symbol="PUB",
        timespan="day",
        multiplier=1,
        observed_at=target_time,
        source="polygon_open_close",
        adjustment_basis="raw",
        fetched_at=stored_bar.fetched_at,
        source_as_of=stored_bar.as_of,
        version_recorded_at=stored_bar.recorded_at,
        available_at=receipt.available_at,
        field="close",
        value=stored_bar.close,
    )
    payload = RealizedOutcomePayload(
        outcome_resolution_policy_hash=outcome_policy.outcome_resolution_policy_hash,
        availability_rule_set_hash=outcome_policy.availability_rule_set_hash,
        resolution_cutoff=resolution_cutoff,
        symbol="PUB",
        target="close",
        series_basis="raw",
        target_time=target_time,
        currency="USD",
        realized_value=stored_bar.close,
        source_version=version,
    )
    store = SqlForecastOutcomeStore(
        sessionmaker=runtime_maker,
        outcome_resolution_policy_hash=outcome_policy.outcome_resolution_policy_hash,
        availability_rule_set_hash=outcome_policy.availability_rule_set_hash,
    )

    with pytest.raises(AppError) as forged_error:
        await store.publish(payload, source=forged_source)
    assert forged_error.value.code == "forecast_outcome_integrity_failed"

    published = await store.publish(payload, source=valid_source)
    replayed = await store.publish(payload, source=valid_source)
    assert replayed == published
    assert published.publication.cohort_id == valid_source.cohort_id
    assert published.publication.forecast_id == valid_source.forecast_id
    assert published.publication.step == valid_source.step
    async with runtime_maker() as session:
        publications = (
            (
                await session.execute(
                    select(ForecastRealizedOutcomePublication).where(
                        ForecastRealizedOutcomePublication.outcome_id == published.record.outcome_id
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(publications) == 1
    assert publications[0].cohort_id == valid_source.cohort_id


async def test_upsert_replay_is_noop_and_restatement_writes_revision(engine: AsyncEngine) -> None:
    maker = build_sessionmaker(engine)
    symbol = "UPST"

    async with maker() as session, session.begin():
        await upsert_bars(session, [_bar(1, symbol=symbol, close=101.0)])

    # Identical values with a later fetched_at: the IS DISTINCT FROM guard must
    # leave the row byte-identical and write no revision.
    async with maker() as session:
        async with session.begin():
            replay = await upsert_bars(
                session,
                [_bar(1, symbol=symbol, close=101.0, fetched_hour=9)],
            )
        assert replay.revisions == []

    async with maker() as session:
        row = (await session.execute(select(Bar).where(Bar.symbol == symbol))).scalar_one()
        assert row.close == 101.0
        assert row.fetched_at == datetime(2026, 7, 1, 1, tzinfo=UTC)  # replay did not touch it
        revision_count = (
            await session.execute(
                select(func.count()).select_from(BarRevision).where(BarRevision.symbol == symbol)
            )
        ).scalar_one()
        assert revision_count == 0

    # A restated close must update the current row AND append the prior value.
    async with maker() as session:
        async with session.begin():
            restated = await upsert_bars(
                session,
                [_bar(1, symbol=symbol, close=102.5, fetched_hour=12)],
            )
        assert len(restated.revisions) == 1

    async with maker() as session:
        row = (await session.execute(select(Bar).where(Bar.symbol == symbol))).scalar_one()
        assert row.close == 102.5
        revision = (
            await session.execute(select(BarRevision).where(BarRevision.symbol == symbol))
        ).scalar_one()
        assert revision.previous_close == 101.0
        assert revision.incoming_close == 102.5
        assert revision.previous_recorded_at is not None
        assert revision.previous_recorded_at < row.recorded_at
    assert revision.incoming_recorded_at == revision.revised_at == row.recorded_at


async def test_receipt_rejects_version_written_in_same_outer_transaction_savepoint(
    engine: AsyncEngine,
) -> None:
    """A subxid must not bypass the top-level post-commit boundary."""

    maker = build_sessionmaker(engine)
    async with maker() as session, session.begin():
        async with session.begin_nested():
            plan = await upsert_bars(
                session,
                [_bar(day, symbol="RCPT", close=100.0 + day) for day in (1, 2, 3)],
            )
        with pytest.raises(DBAPIError, match="finalized after its write commits"):
            async with session.begin_nested():
                await finalize_bar_version_availability(session, plan.rows[-1:])

    async with maker() as session, session.begin():
        # A trailing-only retry reconciles the two earlier versions whose
        # hypothetical first finalizer never committed.
        assert await finalize_bar_version_availability(session, plan.rows[-1:]) == 3


async def test_backfill_store_repairs_only_exact_dates_and_shares_vendor_lock(
    engine: AsyncEngine,
    migrated_database_url: LiveDatabaseUrls,
) -> None:
    """Prove the operator's exact receipt write and cross-lane DB exclusion."""

    session_dates = (date(2026, 7, 8), date(2026, 7, 9), date(2026, 7, 10))
    out_of_window = date(2024, 1, 2)
    newly_persisted = date(2026, 7, 7)

    def close_bar(session_date: date) -> OHLCVBar:
        observed_at = _session_close(session_date)
        return OHLCVBar(
            symbol="MSFT",
            timestamp=observed_at,
            timespan="day",
            multiplier=1,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1_000.0,
            source="polygon_open_close",
            adjustment_basis="raw",
            fetched_at=observed_at + timedelta(minutes=1),
        )

    maker = build_sessionmaker(engine)
    async with maker() as session, session.begin():
        complete_plan = await upsert_bars(session, [close_bar(session_dates[0])])
    async with maker() as session, session.begin():
        assert await finalize_bar_version_availability(session, complete_plan.rows) == 1
    async with maker() as session, session.begin():
        await upsert_bars(
            session,
            [close_bar(value) for value in (*session_dates[1:], out_of_window)],
        )

    settings = Settings(app_env="local", database_url=migrated_database_url.runtime)
    async with SqlBackfillStore(settings) as store:
        before = await store.coverage(session_dates)
        assert before.complete_dates == (session_dates[0],)
        assert before.repairable_dates == session_dates[1:]

        assert await store.repair_receipts((session_dates[1],)) == 1
        after = await store.coverage(session_dates)
        assert after.complete_dates == session_dates[:2]
        assert after.repairable_dates == (session_dates[2],)

        await store.persist(close_bar(newly_persisted))
        exact_persist = await store.coverage((out_of_window, newly_persisted))
        assert exact_persist.complete_dates == (newly_persisted,)
        assert exact_persist.repairable_dates == (out_of_window,)

    with pytest.raises(VendorOperationBusy, match="already running"):
        async with exclusive_vendor_operation(settings):
            pytest.fail("contended generic vendor lock body must not run")
    with pytest.raises(BackfillRefused, match="already running"):
        async with _exclusive_backfill(settings):
            pytest.fail("contended backfill lock body must not run")


async def test_stale_conflicts_and_history_mutation_fail_closed(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
) -> None:
    maker = build_sessionmaker(engine)
    symbol = "GUARD"
    async with maker() as session, session.begin():
        await upsert_bars(session, [_bar(1, symbol=symbol, close=101.0, fetched_hour=1)])
    async with maker() as session, session.begin():
        await upsert_bars(session, [_bar(1, symbol=symbol, close=102.0, fetched_hour=2)])

    async with engine.begin() as conn:
        raw_update = await conn.execute(
            text(
                "UPDATE bars SET open = 102.0, high = 104.0, low = 101.0, close = 103.0, "
                "fetched_at = fetched_at + interval '1 hour', "
                "as_of = as_of + interval '1 hour', "
                "recorded_at = '2099-01-01T00:00:00+00' "
                "WHERE symbol = :symbol"
            ),
            {"symbol": symbol},
        )
        assert raw_update.rowcount == 1
        identical = await conn.execute(
            text("UPDATE bars SET close = close WHERE symbol = :symbol"),
            {"symbol": symbol},
        )
        assert identical.rowcount == 0

    async with maker() as session, session.begin():
        with pytest.raises(BarVersionConflictError, match="strictly newer"):
            await upsert_bars(
                session,
                [_bar(1, symbol=symbol, close=104.0, fetched_hour=3)],
            )

    direct_mutations = (
        (
            "UPDATE bars SET close = 104.0 WHERE symbol = :symbol",
            "changed bars require newer",
        ),
        (
            "UPDATE bars SET close = 104.0, fetched_at = fetched_at + interval '1 hour' "
            "WHERE symbol = :symbol",
            "changed bars require newer",
        ),
        (
            "UPDATE bars SET close = 104.0, as_of = as_of + interval '1 hour' "
            "WHERE symbol = :symbol",
            "changed bars require newer",
        ),
        (
            "DELETE FROM bars WHERE symbol = :symbol",
            "append-only",
        ),
        (
            "UPDATE bars_revisions SET incoming_close = 103.0 WHERE symbol = :symbol",
            "append-only",
        ),
        (
            "DELETE FROM bars_revisions WHERE symbol = :symbol",
            "append-only",
        ),
        ("TRUNCATE bars_revisions", "append-only"),
        ("TRUNCATE bars", "append-only"),
    )
    for mutation, message in direct_mutations:
        mutation_engine = engine if mutation.startswith("UPDATE bars SET") else owner_engine
        async with mutation_engine.connect() as conn:
            transaction = await conn.begin()
            parameters = {"symbol": symbol} if ":symbol" in mutation else {}
            with pytest.raises(DBAPIError, match=message):
                await conn.execute(text(mutation), parameters)
            await transaction.rollback()

    async with maker() as session:
        current = (await session.execute(select(Bar).where(Bar.symbol == symbol))).scalar_one()
        revision_count = (
            await session.execute(
                select(func.count()).select_from(BarRevision).where(BarRevision.symbol == symbol)
            )
        ).scalar_one()
    assert current.close == 103.0
    assert current.recorded_at.year != 2099
    assert revision_count == 2


async def test_two_page_read_covers_all_bars_without_gap_or_dup(engine: AsyncEngine) -> None:
    maker = build_sessionmaker(engine)
    symbol = "PAGE"
    async with maker() as session, session.begin():
        await upsert_bars(
            session,
            [_bar(day, symbol=symbol, close=100.0 + day) for day in (1, 2, 3, 4, 5)],
        )

    async with maker() as session:
        page_one = await read_prices(session, symbol, PriceFilters(limit=3))
        assert [bar.timestamp for bar in page_one.bars] == [
            datetime(2026, 7, day, tzinfo=UTC) for day in (3, 4, 5)
        ]
        assert page_one.page.has_more is True
        assert page_one.page.next_end == datetime(2026, 7, 3, tzinfo=UTC)

        page_two = await read_prices(
            session, symbol, PriceFilters(limit=3, end=page_one.page.next_end)
        )
        assert [bar.timestamp for bar in page_two.bars] == [
            datetime(2026, 7, day, tzinfo=UTC) for day in (1, 2)
        ]
        assert page_two.page.has_more is False
        assert page_two.page.next_end is None

    combined = [bar.timestamp for bar in (*page_two.bars, *page_one.bars)]
    assert combined == sorted(combined)  # chronological across pages
    assert len(set(combined)) == 5  # no duplicates, no gaps
    assert all(ts.utcoffset() == timedelta(0) for ts in combined)  # TIMESTAMPTZ round-trip


async def test_indicator_window_reads_real_postgres_and_validates_xnys_closes(
    engine: AsyncEngine,
) -> None:
    """Prove the bounded derived read over real stored TIMESTAMPTZ rows."""

    calendar = exchange_calendars.get_calendar("XNYS")
    labels = calendar.sessions_window(pd.Timestamp("2026-07-13"), -34)
    bars: list[OHLCVBar] = []
    for index, label in enumerate(labels):
        observed_at = calendar.session_close(label).to_pydatetime().astimezone(UTC)
        close = 100.0 + index / 10.0
        bars.append(
            OHLCVBar(
                symbol="INDI",
                timestamp=observed_at,
                timespan="day",
                multiplier=1,
                open=close - 0.5,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=1_000_000.0 + index,
                vwap=close - 0.1,
                trade_count=10_000 + index,
                source="polygon_open_close",
                adjustment_basis="raw",
                fetched_at=observed_at + timedelta(minutes=1),
            )
        )

    maker = build_sessionmaker(engine)
    async with maker() as session, session.begin():
        plan = await upsert_bars(session, bars)
        assert len(plan.rows) == 34
        assert plan.revisions == []

    async with maker() as session:
        response = await read_indicators(session, "INDI", IndicatorFilters())
        assert session.in_transaction() is False

    assert response.count == 34
    assert response.window.input_count == 34
    assert response.window.continuity == "exact_consecutive_regular_session_closes"
    assert response.window.input_start == bars[0].timestamp
    assert response.window.input_end == bars[-1].timestamp
    assert response.window.input_sha256 is not None
    assert response.window_policy_hash == WINDOW_POLICY_HASH
    assert response.observations[0].sma is None
    assert response.observations[-1].macd_signal is not None


_RAW_INSERT = text(
    "INSERT INTO bars (symbol, timespan, multiplier, ts, source, adjustment_basis, "
    "open, high, low, close, volume, vwap, trade_count, fetched_at, as_of) "
    "VALUES ('EVIL', 'day', 1, '2026-07-01T00:00:00+00', 'polygon', 'raw', "
    "1.0, 2.0, 0.5, 1.5, :volume, :vwap, 1, now(), now())"
)


@pytest.mark.parametrize("volume", [float("nan"), float("inf")])
async def test_non_finite_ohlcv_rejected_by_storage_check(
    engine: AsyncEngine, volume: float
) -> None:
    # Bypasses the DTO on purpose: proves the DB CHECK itself rejects what the
    # nonnegativity constraints cannot (NaN orders greater than 0 in Postgres).
    async with engine.connect() as conn:
        with pytest.raises(IntegrityError, match="ck_bars_ohlcv_finite"):
            await conn.execute(_RAW_INSERT, {"volume": volume, "vwap": None})


async def test_non_finite_vwap_rejected_by_storage_check(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        with pytest.raises(IntegrityError, match="ck_bars_vwap_finite"):
            await conn.execute(_RAW_INSERT, {"volume": 3.0, "vwap": float("inf")})


async def test_api_statement_timeout_cancels_pathological_statement(
    migrated_database_url: LiveDatabaseUrls,
) -> None:
    settings = Settings(app_env="test", database_url=migrated_database_url.runtime)
    capped = build_engine(settings, statement_timeout_ms=150)
    try:
        async with capped.connect() as conn:
            with pytest.raises(DBAPIError):
                await conn.execute(text("SELECT pg_sleep(2)"))
    finally:
        await capped.dispose()


def _forecast_snapshot_record(
    *,
    final_value: float = 102.0,
    policy_hash: str = "sha256:" + "a" * 64,
    symbol: str = "AAPL",
    as_of: datetime | None = None,
    target_times: tuple[datetime, ...] | None = None,
    horizon_unit: str = "calendar_day",
) -> ForecastInputSnapshotRecord:
    snapshot_as_of = as_of or datetime(2026, 7, 10, 21, tzinfo=UTC)
    return build_snapshot_record(
        ForecastInputSnapshotPayload(
            resolution_policy_hash=policy_hash,
            symbol=symbol,
            target="close",
            horizon_unit=horizon_unit,
            series_basis="raw",
            input_timespan="day",
            input_multiplier=1,
            as_of=snapshot_as_of,
            currency="USD",
            observations=(
                SnapshotObservation(
                    observed_at=snapshot_as_of - timedelta(days=2, hours=1),
                    available_at=snapshot_as_of - timedelta(days=2),
                    value=100.0,
                ),
                SnapshotObservation(
                    observed_at=snapshot_as_of - timedelta(days=1, hours=1),
                    available_at=snapshot_as_of - timedelta(days=1),
                    value=final_value,
                ),
            ),
            target_times=(
                target_times
                if target_times is not None
                else (
                    snapshot_as_of + timedelta(days=1),
                    snapshot_as_of + timedelta(days=2),
                )
            ),
            data_sources=(
                SnapshotSourceLineage(
                    name="live-gate",
                    snapshot_id="live-gate-source-v1",
                    max_available_at=snapshot_as_of - timedelta(hours=1),
                    fields=("close",),
                ),
            ),
            availability=SnapshotAvailabilityEvidence(status="not_run"),
        ),
        sealed_at=snapshot_as_of + timedelta(minutes=1),
    )


def _record_values(record: ForecastInputSnapshotRecord) -> dict[str, object]:
    return {field.name: getattr(record, field.name) for field in fields(record)}


async def test_snapshot_insert_hash_resolution_and_immutability(
    owner_engine: AsyncEngine,
) -> None:
    record = _forecast_snapshot_record()
    maker = build_sessionmaker(owner_engine)
    statement = (
        pg_insert(ForecastInputSnapshot)
        .values(**_record_values(record))
        .on_conflict_do_nothing(index_elements=["snapshot_id"])
    )
    async with maker() as session:
        async with session.begin():
            insert_before = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            first = await session.execute(statement)
            insert_after = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        assert first.rowcount == 1
        async with session.begin():
            replay = await session.execute(statement)
        assert replay.rowcount == 0

    async with maker() as session:
        stored = (
            await session.execute(
                select(ForecastInputSnapshot).where(
                    ForecastInputSnapshot.snapshot_id == record.snapshot_id
                )
            )
        ).scalar_one()
        loaded = ForecastInputSnapshotRecord(
            **{
                field.name: getattr(stored, field.name)
                for field in fields(ForecastInputSnapshotRecord)
            }
        )
    assert loaded.sealed_at != record.sealed_at  # caller value was overwritten by the DB trigger
    assert insert_before <= loaded.sealed_at <= insert_after
    request = ForecastRequest(
        symbol="AAPL",
        horizon=2,
        horizon_unit="calendar_day",
        target="close",
        snapshot_id=record.snapshot_id,
        model="baseline_naive",
        interval_coverages=[0.8],
    )
    resolved = validate_and_resolve_snapshot(
        loaded,
        request,
        expected_series_basis="raw",
        expected_resolution_policy_hash=record.resolution_policy_hash,
    )
    assert [item.value for item in resolved.observations] == [100.0, 102.0]
    assert resolved.availability_verified is False

    tamper_base = _forecast_snapshot_record(policy_hash="sha256:" + "c" * 64)
    tampered = replace(tamper_base, snapshot_id="sha256:" + "0" * 64)
    async with owner_engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(
            IntegrityError, match="ck_forecast_input_snapshots_payload_hash_matches_id"
        ):
            await conn.execute(pg_insert(ForecastInputSnapshot).values(**_record_values(tampered)))
        await transaction.rollback()

    # Same semantic selector/cutoff with different bytes is ambiguous and must
    # fail instead of silently becoming another "latest" snapshot.
    collision = _forecast_snapshot_record(final_value=103.0)
    async with owner_engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(IntegrityError, match="uq_forecast_input_snapshots_semantic_key"):
            await conn.execute(pg_insert(ForecastInputSnapshot).values(**_record_values(collision)))
        await transaction.rollback()

    for mutation in (
        "UPDATE forecast_input_snapshots SET symbol = 'MSFT' WHERE snapshot_id = :snapshot_id",
        "DELETE FROM forecast_input_snapshots WHERE snapshot_id = :snapshot_id",
        # The run archive now references snapshots. CASCADE reaches both
        # protected tables so their statement-level immutability triggers,
        # rather than the FK precheck, must refuse the operation.
        "TRUNCATE forecast_input_snapshots CASCADE",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            with pytest.raises(DBAPIError, match="insert-only"):
                parameters = (
                    {"snapshot_id": record.snapshot_id} if ":snapshot_id" in mutation else {}
                )
                await conn.execute(text(mutation), parameters)
            await transaction.rollback()


async def test_bars_to_verified_snapshot_to_served_forecast(
    engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
    owner_engine: AsyncEngine,
) -> None:
    """Prove the complete privileged-write/read-only-serving chain."""

    runtime_maker = build_sessionmaker(engine)
    builder_maker = build_sessionmaker(snapshot_builder_engine)
    initial_cutoff = await database_snapshot_cutoff(builder_maker)
    calendar = exchange_calendars.get_calendar("XNYS", start="1990-01-01", end="2100-12-31")
    latest = calendar.date_to_session(initial_cutoff.date(), direction="previous")
    if calendar.session_close(latest).to_pydatetime() > initial_cutoff:
        latest = calendar.previous_session(latest)
    sessions = calendar.sessions_window(latest, -258)
    bars = []
    for index, label in enumerate(sessions):
        timestamp = label.to_pydatetime().replace(tzinfo=UTC)
        fetched_at = calendar.session_close(label).to_pydatetime()
        close = 100.0 + index * 0.05 + (index % 7) * 0.2
        bars.append(
            OHLCVBar(
                symbol="AAPL",
                timestamp=timestamp,
                timespan="day",
                multiplier=1,
                open=close - 0.5,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=10_000.0 + index,
                source="polygon_open_close",
                adjustment_basis="raw",
                fetched_at=fetched_at,
            )
        )
    async with runtime_maker() as session, session.begin():
        plan = await upsert_bars(session, bars)
    async with runtime_maker() as session, session.begin():
        await finalize_bar_version_availability(session, plan.rows)

    historical_cutoff = await database_snapshot_cutoff(builder_maker)
    historical_spec = SnapshotBuildSpec(
        symbol="AAPL",
        target="close",
        horizon_unit="trading_day",
        as_of=historical_cutoff,
    )
    builder = ForecastSnapshotBuilder(builder_maker)
    historical_created = await builder.build(historical_spec)

    # Freeze the historical cutoff above, then publish and finalize a newer
    # version. Replaying the old semantic key must reconstruct the prior close
    # from bars_revisions, while a later cutoff must select the restatement.
    previous = bars[-1]
    restated_close = previous.close + 7.0
    restatement = OHLCVBar(
        symbol=previous.symbol,
        timestamp=previous.timestamp,
        timespan=previous.timespan,
        multiplier=previous.multiplier,
        open=restated_close - 0.5,
        high=restated_close + 1.0,
        low=restated_close - 1.0,
        close=restated_close,
        volume=previous.volume,
        source=previous.source,
        adjustment_basis=previous.adjustment_basis,
        fetched_at=previous.fetched_at + timedelta(minutes=1),
    )
    async with runtime_maker() as session, session.begin():
        restated_plan = await upsert_bars(session, [restatement])
    assert len(restated_plan.revisions) == 1
    async with runtime_maker() as session, session.begin():
        assert await finalize_bar_version_availability(session, restated_plan.rows) == 1

    historical_replayed = await builder.build(historical_spec)
    current_cutoff = await database_snapshot_cutoff(builder_maker)
    current_created = await builder.build(
        SnapshotBuildSpec(
            symbol="AAPL",
            target="close",
            horizon_unit="trading_day",
            as_of=current_cutoff,
        )
    )
    assert historical_created.created is True
    assert historical_replayed.created is False
    assert historical_replayed.snapshot_id == historical_created.snapshot_id
    assert historical_replayed.availability_checked_at == historical_created.availability_checked_at
    assert current_created.created is True
    assert current_created.snapshot_id != historical_created.snapshot_id

    async with builder_maker() as session:
        stored_snapshots = (
            (
                await session.execute(
                    select(ForecastInputSnapshot).where(
                        ForecastInputSnapshot.snapshot_id.in_(
                            (
                                historical_created.snapshot_id,
                                current_created.snapshot_id,
                            )
                        )
                    )
                )
            )
            .scalars()
            .all()
        )
    payloads = {
        row.snapshot_id: parse_snapshot_payload(bytes(row.canonical_payload))
        for row in stored_snapshots
    }
    assert payloads[historical_created.snapshot_id].observations[-1].value == previous.close
    assert payloads[current_created.snapshot_id].observations[-1].value == restated_close

    service = SnapshotForecastService(
        repository=SqlForecastInputSnapshotRepository(
            runtime_maker,
            trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        ),
        policy=ForecastServingPolicy(
            resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
            trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        ),
    )
    response = await service.forecast(
        ForecastRequest(
            symbol="AAPL",
            horizon=2,
            horizon_unit="trading_day",
            target="close",
            snapshot_id=current_created.snapshot_id,
            model="baseline_naive",
            interval_coverages=[0.8],
        )
    )
    assert response.symbol == "AAPL"
    assert response.provenance.snapshot_id == current_created.snapshot_id
    assert response.provenance.lookahead_check.status == "passed"
    assert len(response.forecasts) == 2

    # Exercise the default-off scheduling seam against the real runtime role:
    # the response is archived first, membership is derived from a validated
    # reread, and the content-addressed cohort is sealed in a later transaction.
    scheduled_store = SqlForecastRunStore(
        sessionmaker=runtime_maker,
        identity_secret="live-gate-scheduled-archive-secret",
        resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
        availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        origin_kind="scheduled_evaluation",
    )
    scheduled_forecast_service = SnapshotForecastService(
        repository=SqlForecastInputSnapshotRepository(
            runtime_maker,
            trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        ),
        policy=ForecastServingPolicy(
            resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
            trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        ),
        code_version="live-gate-scheduled-1",
        run_store=scheduled_store,
    )
    scheduled_request = ForecastRequest(
        symbol="AAPL",
        horizon=2,
        horizon_unit="trading_day",
        target="close",
        snapshot_id=current_created.snapshot_id,
        model="baseline_naive",
        interval_coverages=[0.8],
    )
    prospective_outcome_policy = ForecastOutcomeResolutionPolicy(
        resolution_lag_seconds=24 * 60 * 60
    )
    await SqlForecastOutcomePolicyStore(runtime_maker).register(prospective_outcome_policy)
    scheduled_spec = ScheduledEvaluationSpec(
        request=scheduled_request,
        purpose="heldout_evaluation",
        selected_steps=(1, 2),
        model_version="baseline-naive@1",
        code_version="live-gate-scheduled-1",
        forecast_resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
        forecast_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        selection_policy_hash="sha256:" + "d" * 64,
        outcome_resolution_policy_hash=(prospective_outcome_policy.outcome_resolution_policy_hash),
        outcome_availability_rule_set_hash=(prospective_outcome_policy.availability_rule_set_hash),
    )
    scheduled_evaluation = ScheduledEvaluationService(
        forecast_service=scheduled_forecast_service,
        run_store=scheduled_store,
        cohort_store=SqlForecastCohortStore(runtime_maker),
    )
    scheduled_proof = await scheduled_evaluation.publish(scheduled_spec)
    scheduled_replay = await scheduled_evaluation.publish(scheduled_spec)
    assert scheduled_replay == scheduled_proof
    assert scheduled_proof.run.origin_kind == "scheduled_evaluation"
    assert scheduled_proof.run.recorded_at is not None
    assert scheduled_proof.cohort_record.member_count == 2
    assert scheduled_proof.cohort_record.creator_xid != scheduled_proof.cohort_seal.sealer_xid
    assert (
        scheduled_proof.run.recorded_at
        <= scheduled_proof.cohort_record.recorded_at
        <= scheduled_proof.cohort_seal.sealed_at
        < scheduled_proof.cohort_record.earliest_target_time
    )

    # Complete the real HTTP chain as the runtime role: aggregate-router auth
    # must short-circuit first, then the same immutable row must serve through
    # app factory -> dependency wiring -> repository -> response validation.
    api_key = "live-gate-forecast-key"
    api_settings = Settings(
        app_env="test",
        database_url=engine.url.render_as_string(hide_password=False),
        redis_cache_url="redis://localhost:6379/0",
        rate_limit_enabled=False,
        api_keys=api_key,
        forecast_resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
        forecast_trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
    )
    app = create_app(api_settings)
    params = {
        "horizon": 5,
        "horizon_unit": "trading_day",
        "target": "close",
        "snapshot_id": current_created.snapshot_id,
        "model": "baseline_naive",
        "coverage": 0.8,
    }
    async with runtime_maker() as session:
        archive_before = (await session.execute(text("SELECT clock_timestamp()"))).scalar_one()
    with TestClient(app) as client:
        unauthenticated = client.get("/v1/forecast/AAPL", params=params)
        authenticated = client.get(
            "/v1/forecast/AAPL",
            params=params,
            headers={"X-API-Key": api_key},
        )
        post_payload = {
            "symbol": "AAPL",
            "horizon": 5,
            "horizon_unit": "trading_day",
            "target": "close",
            "snapshot_id": current_created.snapshot_id,
            "model": "baseline_naive",
            "interval_coverages": [0.8],
        }
        retry_headers = {
            "X-API-Key": api_key,
            "Idempotency-Key": "live-gate-retry-token",
        }
        created = client.post("/v1/forecast", json=post_payload, headers=retry_headers)
        replayed = client.post("/v1/forecast", json=post_payload, headers=retry_headers)
        changed_payload = {**post_payload, "horizon": 4}
        conflict = client.post(
            "/v1/forecast",
            json=changed_payload,
            headers=retry_headers,
        )
        contended_key = "live-gate-contended-token"
        contended_digest = idempotency_digest(
            principal=api_key,
            idempotency_key=contended_key,
            secret=api_settings.jwt_secret,
        )
        lock_key = int.from_bytes(
            bytes.fromhex(contended_digest.removeprefix("hmac-sha256:"))[:8],
            byteorder="big",
            signed=True,
        )
        async with runtime_maker() as lock_session, lock_session.begin():
            await lock_session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key)"),
                {"lock_key": lock_key},
            )
            contended = client.post(
                "/v1/forecast",
                json=post_payload,
                headers={
                    "X-API-Key": api_key,
                    "Idempotency-Key": contended_key,
                },
            )
    async with runtime_maker() as session:
        archive_after = (await session.execute(text("SELECT clock_timestamp()"))).scalar_one()

    assert unauthenticated.status_code == 401
    assert unauthenticated.headers["WWW-Authenticate"] == "X-API-Key"
    assert authenticated.status_code == 200
    served = ForecastResponse.model_validate(authenticated.json())
    assert served.symbol == "AAPL"
    assert served.provenance.snapshot_id == current_created.snapshot_id
    assert served.provenance.feature_set_hash == current_created.snapshot_id
    assert served.provenance.lookahead_check.status == "passed"
    assert len(served.forecasts) == 5
    assert created.status_code == 200
    assert replayed.status_code == 200
    assert replayed.content == created.content
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_key_conflict"
    assert "live-gate-retry-token" not in conflict.text
    assert contended.status_code == 409
    assert contended.json()["error"]["code"] == "idempotency_in_progress"
    assert contended_key not in contended.text

    async with runtime_maker() as session:
        # Scope to API-origin runs. forecast_runs is insert-only by design, so the
        # precommitted scheduled_evaluation run sealed by the cohort test earlier
        # in this module cannot be cleaned between tests -- and it also carries a
        # NULL idempotency digest, so an unscoped query would both inflate the
        # count and let ``unkeyed`` bind to the wrong row.
        archived = (
            (await session.execute(select(ForecastRun).where(ForecastRun.origin_kind == "api")))
            .scalars()
            .all()
        )
    assert len(archived) == 2  # one GET run plus one keyed POST; replay adds none
    keyed = next(row for row in archived if row.idempotency_token_digest is not None)
    unkeyed = next(row for row in archived if row.idempotency_token_digest is None)
    assert (
        keyed.forecast_id == ForecastResponse.model_validate(created.json()).provenance.forecast_id
    )
    assert unkeyed.forecast_id == served.provenance.forecast_id
    assert request_hash(bytes(keyed.canonical_request)) == keyed.request_hash
    assert output_hash(bytes(keyed.canonical_output)) == keyed.output_hash
    assert parse_output(bytes(keyed.canonical_output)).provenance.forecast_id == keyed.forecast_id
    assert archive_before <= keyed.generated_at <= keyed.recorded_at <= archive_after
    archived_material = (
        bytes(keyed.canonical_request)
        + bytes(keyed.canonical_output)
        + keyed.idempotency_token_digest.encode()
    )
    assert api_key.encode() not in archived_material
    assert b"live-gate-retry-token" not in archived_material

    for mutation in (
        "UPDATE forecast_runs SET symbol = symbol WHERE forecast_id = :forecast_id",
        "DELETE FROM forecast_runs WHERE forecast_id = :forecast_id",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            with pytest.raises(DBAPIError, match="insert-only"):
                parameters = (
                    {"forecast_id": keyed.forecast_id} if ":forecast_id" in mutation else {}
                )
                await conn.execute(text(mutation), parameters)
            await transaction.rollback()

    # The evidence substrate's FK (forecast_outcome_cohort_members -> forecast_runs)
    # now rejects a PLAIN truncate before any trigger can run, which would mask the
    # immutability guarantee. CASCADE is precisely how that obstacle is bypassed, so
    # it -- not the plain form -- is what actually proves the archive cannot be
    # emptied. The referencing evidence table must be insert-only in its own right,
    # or the cohort evidence could be destroyed even while the runs survive.
    async with owner_engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(DBAPIError, match="foreign key"):
            await conn.execute(text("TRUNCATE forecast_runs"))
        await transaction.rollback()

    for truncation in (
        "TRUNCATE forecast_runs CASCADE",
        "TRUNCATE forecast_outcome_cohort_members CASCADE",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            with pytest.raises(DBAPIError, match="insert-only"):
                await conn.execute(text(truncation))
            await transaction.rollback()

    # Block the real service after its snapshot SELECT. Under the former design
    # that SELECT belonged to the archive transaction and retained the only
    # pooled connection; the short repository now releases it before CPU work.
    single_engine = create_async_engine(
        engine.url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.5,
    )
    single_maker = build_sessionmaker(single_engine)
    run_store = SqlForecastRunStore(
        sessionmaker=single_maker,
        identity_secret=api_settings.jwt_secret,
        resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
        availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
    )
    snapshot_resolved = threading.Event()
    release_resolution = threading.Event()

    class _BlockingForecastService(SnapshotForecastService):
        def _resolve_record(self, record, request, series_basis):
            resolved = super()._resolve_record(record, request, series_basis)
            snapshot_resolved.set()
            if not release_resolution.wait(timeout=5):
                raise RuntimeError("live pool regression release timed out")
            return resolved

    future_app_time = archive_after + timedelta(days=1)
    blocked_service = _BlockingForecastService(
        repository=SqlForecastInputSnapshotRepository(
            single_maker,
            trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        ),
        policy=ForecastServingPolicy(
            resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
            trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        ),
        clock=lambda: future_app_time,
        run_store=run_store,
    )
    blocked_request = ForecastRequest(
        symbol="AAPL",
        horizon=2,
        horizon_unit="trading_day",
        target="close",
        snapshot_id=current_created.snapshot_id,
        model="baseline_naive",
        interval_coverages=[0.8],
    )
    async with single_maker() as session:
        pool_before = (await session.execute(text("SELECT clock_timestamp()"))).scalar_one()
    archive_task = asyncio.create_task(blocked_service.forecast(blocked_request))
    try:
        assert await asyncio.to_thread(snapshot_resolved.wait, 1)
        async with single_maker() as session:
            assert (
                await asyncio.wait_for(
                    session.execute(text("SELECT 1")),
                    timeout=1,
                )
            ).scalar_one() == 1
        release_resolution.set()
        archived_after_release = await asyncio.wait_for(archive_task, timeout=5)
        async with single_maker() as session:
            pool_after = (await session.execute(text("SELECT clock_timestamp()"))).scalar_one()
            pool_row = await session.get(
                ForecastRun,
                archived_after_release.provenance.forecast_id,
            )
        assert pool_row is not None
        stored_pool = parse_output(bytes(pool_row.canonical_output))
        assert (
            archived_after_release.provenance.generated_at
            == pool_row.generated_at
            == stored_pool.provenance.generated_at
        )
        assert pool_before <= pool_row.generated_at <= pool_row.recorded_at <= pool_after
        assert (
            archived_after_release.provenance.lookahead_check.checked_at
            == stored_pool.provenance.lookahead_check.checked_at
            == pool_row.generated_at
        )
        assert output_hash(bytes(pool_row.canonical_output)) == pool_row.output_hash
        assert archived_after_release.provenance.generated_at != future_app_time
    finally:
        release_resolution.set()
        if not archive_task.done():
            archive_task.cancel()
        await asyncio.gather(archive_task, return_exceptions=True)
        await single_engine.dispose()

    # Two first uses can both execute pure compute, but lock/recheck plus the
    # full-digest UNIQUE constraint must expose only one persisted winner.
    race_request = ForecastRequest(
        symbol="AAPL",
        horizon=2,
        horizon_unit="trading_day",
        target="close",
        snapshot_id=current_created.snapshot_id,
        model="baseline_naive",
        interval_coverages=[0.8],
    )
    candidate_a = await service.forecast(race_request)
    candidate_b = await service.forecast(race_request)
    assert candidate_a.provenance.forecast_id != candidate_b.provenance.forecast_id
    race_key = "live-gate-first-use-race"
    race_principal = "live-gate-race-principal"
    race_store_a = SqlForecastRunStore(
        sessionmaker=runtime_maker,
        identity_secret=api_settings.jwt_secret,
        resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
        availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
    )
    race_store_b = SqlForecastRunStore(
        sessionmaker=runtime_maker,
        identity_secret=api_settings.jwt_secret,
        resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
        availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
    )
    producers_started = 0
    both_producers_started = asyncio.Event()

    async def _racing_producer(candidate: ForecastResponse) -> ForecastResponse:
        nonlocal producers_started
        producers_started += 1
        if producers_started == 2:
            both_producers_started.set()
        await both_producers_started.wait()
        return candidate

    race_results = await asyncio.gather(
        race_store_a.execute(
            race_request,
            idempotency_key=race_key,
            principal=race_principal,
            producer=lambda: _racing_producer(candidate_a),
        ),
        race_store_b.execute(
            race_request,
            idempotency_key=race_key,
            principal=race_principal,
            producer=lambda: _racing_producer(candidate_b),
        ),
        return_exceptions=True,
    )
    race_responses = [item for item in race_results if isinstance(item, ForecastResponse)]
    race_errors = [item for item in race_results if isinstance(item, Exception)]
    assert producers_started == 2
    assert race_responses
    assert all(
        isinstance(item, AppError) and item.code == "idempotency_in_progress"
        for item in race_errors
    )

    async def _must_not_recompute() -> ForecastResponse:
        raise AssertionError("persisted keyed retry invoked the producer")

    race_replay = await race_store_a.execute(
        race_request,
        idempotency_key=race_key,
        principal=race_principal,
        producer=_must_not_recompute,
    )
    assert {item.provenance.forecast_id for item in race_responses} == {
        race_replay.provenance.forecast_id
    }
    race_digest = idempotency_digest(
        principal=race_principal,
        idempotency_key=race_key,
        secret=api_settings.jwt_secret,
    )
    async with runtime_maker() as session:
        assert (
            await session.execute(
                select(func.count())
                .select_from(ForecastRun)
                .where(ForecastRun.idempotency_token_digest == race_digest)
            )
        ).scalar_one() == 1


async def test_persisted_factor_to_adjusted_forecast_snapshot_live_chain(
    engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
) -> None:
    """Prove the exact action/factor/receipt evidence can seal and replay a snapshot."""

    symbol = "MSFT"
    runtime_maker = build_sessionmaker(engine)
    builder_maker = build_sessionmaker(snapshot_builder_engine)
    initial_cutoff = await database_snapshot_cutoff(builder_maker)
    calendar = exchange_calendars.get_calendar(
        "XNYS",
        start="1990-01-01",
        end="2100-12-31",
    )
    latest_session = latest_completed_xnys_session(initial_cutoff)
    labels = calendar.sessions_window(pd.Timestamp(latest_session), -258)
    sessions = tuple(label.date() for label in labels)
    closes = tuple(100.0 + index * 0.05 + (index % 7) * 0.2 for index in range(len(sessions)))
    assert len(sessions) == 258
    await _seed_factor_raw_series(
        engine,
        symbol=symbol,
        sessions=sessions,
        closes=closes,
        fetched_at=initial_cutoff,
        allow_receipt_reconciliation=True,
    )

    split_collection = _factor_split_collection(
        symbol=symbol,
        request_id="msft-adjusted-snapshot-splits",
        start=sessions[0],
        end=sessions[-1],
        fetched_at=initial_cutoff,
    )
    dividend_collection = _factor_dividend_collection(
        symbol=symbol,
        request_id="msft-adjusted-snapshot-dividends",
        start=sessions[0],
        end=sessions[-1],
        ex_dividend_date=sessions[-2],
        cash_amount=None,
        fetched_at=initial_cutoff,
    )
    action_store = SqlCorporateActionCollectionStore(engine)
    split_publication = await action_store.publish(split_collection)
    dividend_publication = await action_store.publish(dividend_collection)
    assert split_publication.event_count == dividend_publication.event_count == 0

    factor_cutoff = await _factor_database_now(engine)
    factor_result = await AdjustmentFactorBuilder(
        runtime_maker,
        SqlAdjustmentFactorSetStore(snapshot_builder_engine),
    ).build(
        AdjustmentFactorBuildSpec(
            symbol=symbol,
            coverage_start=sessions[0],
            coverage_end=sessions[-1],
            cutoff=factor_cutoff,
        )
    )
    assert factor_result.artifact.split_collection_id == split_collection.collection_id
    assert factor_result.artifact.dividend_collection_id == dividend_collection.collection_id
    assert factor_result.publication.input_count == 258

    snapshot_as_of = await database_snapshot_cutoff(builder_maker)
    assert latest_completed_xnys_session(snapshot_as_of) == sessions[-1]
    spec = AdjustedSnapshotBuildSpec(
        symbol=symbol,
        target="adjusted_close",
        horizon_unit="trading_day",
        as_of=snapshot_as_of,
    )
    snapshot_builder = AdjustedForecastSnapshotBuilder(builder_maker)
    created = await snapshot_builder.build(spec)
    replayed = await snapshot_builder.build(spec)
    assert created.created is True
    assert replayed.created is False
    assert replayed.snapshot_id == created.snapshot_id
    assert replayed.availability_checked_at == created.availability_checked_at
    assert created.observation_count == 258
    assert created.target_time_count == 252

    async with runtime_maker() as session:
        stored = (
            await session.execute(
                select(ForecastInputSnapshot).where(
                    ForecastInputSnapshot.snapshot_id == created.snapshot_id
                )
            )
        ).scalar_one()
    payload = parse_snapshot_payload(bytes(stored.canonical_payload))
    assert stored.target == payload.target == "adjusted_close"
    assert stored.series_basis == payload.series_basis == "split_dividend_adjusted"
    assert stored.resolution_policy_hash == ADJUSTED_RESOLUTION_POLICY_HASH
    assert stored.availability_status == "passed"
    assert stored.availability_rule_set_hash == ADJUSTED_AVAILABILITY_RULE_SET_HASH
    assert stored.max_available_at == factor_result.publication.available_at
    assert payload.availability.checked_at == created.availability_checked_at
    assert len(payload.observations) == 258
    assert (
        payload.observations[-1].observed_at == calendar.session_close(labels[-1]).to_pydatetime()
    )
    assert payload.observations[-1].value == closes[-1]
    assert all(
        observation.available_at == factor_result.publication.available_at
        for observation in payload.observations
    )

    sources = {source.name: source for source in payload.data_sources}
    assert set(sources) == {
        "polygon_dividends",
        "polygon_open_close",
        "polygon_splits",
        "stockapi_adjustment_factors",
    }
    assert sources["polygon_splits"].snapshot_id == split_collection.collection_id
    assert sources["polygon_splits"].max_available_at == split_publication.available_at
    assert sources["polygon_dividends"].snapshot_id == dividend_collection.collection_id
    assert sources["polygon_dividends"].max_available_at == dividend_publication.available_at
    assert sources["stockapi_adjustment_factors"].snapshot_id == (
        factor_result.artifact.factor_set_id
    )
    assert sources["stockapi_adjustment_factors"].max_available_at == (
        factor_result.publication.available_at
    )
    assert sources["polygon_open_close"].snapshot_id.startswith("sha256:")

    # Synthetic product-path proof only: serve the sealed adjusted snapshot
    # through the production baseline service and archive exactly one run. This
    # deliberately does not publish outcomes or calibration-cohort evidence.
    async with runtime_maker() as session:
        adjusted_runs_before = (
            await session.execute(
                select(func.count())
                .select_from(ForecastRun)
                .where(ForecastRun.snapshot_id == created.snapshot_id)
            )
        ).scalar_one()
    assert adjusted_runs_before == 0

    run_store = SqlForecastRunStore(
        sessionmaker=runtime_maker,
        identity_secret="live-gate-synthetic-adjusted-archive-secret",
        resolution_policy_hash=ADJUSTED_RESOLUTION_POLICY_HASH,
        availability_rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    )
    forecast_service = SnapshotForecastService(
        repository=SqlForecastInputSnapshotRepository(
            runtime_maker,
            trusted_availability_rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
        ),
        policy=ForecastServingPolicy(
            resolution_policy_hash=ADJUSTED_RESOLUTION_POLICY_HASH,
            trusted_availability_rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
            target="adjusted_close",
            series_basis="split_dividend_adjusted",
        ),
        code_version="live-gate-synthetic-adjusted-1",
        run_store=run_store,
    )
    synthetic_request = ForecastRequest(
        symbol=symbol,
        horizon=2,
        horizon_unit="trading_day",
        target="adjusted_close",
        snapshot_id=created.snapshot_id,
        model="baseline_naive",
        interval_coverages=[0.8],
    )
    synthetic_response = await forecast_service.forecast(synthetic_request)
    assert synthetic_response.symbol == symbol
    assert synthetic_response.target == "adjusted_close"
    assert synthetic_response.provenance.snapshot_id == created.snapshot_id
    assert synthetic_response.provenance.series_basis == "split_dividend_adjusted"
    assert synthetic_response.provenance.lookahead_check.status == "passed"
    assert len(synthetic_response.forecasts) == 2
    assert {source.name for source in synthetic_response.provenance.data_sources} == set(sources)

    async with runtime_maker() as session:
        archived_runs = (
            (
                await session.execute(
                    select(ForecastRun).where(ForecastRun.snapshot_id == created.snapshot_id)
                )
            )
            .scalars()
            .all()
        )
    assert len(archived_runs) == 1
    archived = archived_runs[0]
    assert archived.forecast_id == synthetic_response.provenance.forecast_id
    assert archived.target == "adjusted_close"
    assert archived.series_basis == "split_dividend_adjusted"
    assert archived.resolution_policy_hash == ADJUSTED_RESOLUTION_POLICY_HASH
    assert archived.availability_rule_set_hash == ADJUSTED_AVAILABILITY_RULE_SET_HASH
    assert archived.snapshot_id == created.snapshot_id

    # The serving role can read the sealed result but cannot mutate it.
    async with engine.connect() as conn:
        transaction = await conn.begin()
        try:
            with pytest.raises(DBAPIError, match="permission denied"):
                await conn.execute(
                    text(
                        "UPDATE forecast_input_snapshots SET symbol = symbol "
                        "WHERE snapshot_id = :snapshot_id"
                    ),
                    {"snapshot_id": created.snapshot_id},
                )
        finally:
            await transaction.rollback()


@dataclass(frozen=True, repr=False)
class _LiveCalibrationCohort:
    proof: ForecastCohortProof
    sources: tuple[ForecastOutcomePublicationSource, ...]
    target_time: datetime
    symbol: str


async def _archive_live_calibration_cohort(
    *,
    engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
    outcome_policy: ForecastOutcomeResolutionPolicy,
    purpose: CohortPurpose,
    selection_policy_hash: str,
    forecast_resolution_policy_hash: str,
    forecast_availability_rule_set_hash: str,
    symbol: str,
    target_time: datetime,
    forecast_ids: tuple[UUID, ...],
) -> _LiveCalibrationCohort:
    """Archive and prospectively seal one bounded synthetic cohort."""

    if len(forecast_ids) != 4:
        raise AssertionError("live calibration fixtures require exactly four members")
    runtime_maker = build_sessionmaker(engine)
    snapshots: list[ForecastInputSnapshotRecord] = []
    run_values: list[dict[str, object]] = []
    for index, forecast_id in enumerate(forecast_ids):
        snapshot = _forecast_snapshot_record(
            final_value=95.0 + index,
            policy_hash=forecast_resolution_policy_hash,
            symbol=symbol,
            as_of=target_time - timedelta(minutes=10, seconds=index),
            target_times=(target_time,),
            horizon_unit="trading_day",
        )
        snapshots.append(snapshot)
        response = _scheduled_response(
            forecast_id=forecast_id,
            snapshot=snapshot,
            target_time=target_time,
        )
        request = ForecastRequest(
            symbol=symbol,
            horizon=1,
            horizon_unit="trading_day",
            target="close",
            snapshot_id=snapshot.snapshot_id,
            model="baseline_naive",
            interval_coverages=[0.8],
        )
        request_bytes = canonical_request(request)
        output_bytes = canonical_output(response)
        run_values.append(
            {
                "forecast_id": forecast_id,
                "schema_version": 1,
                "origin_kind": "scheduled_evaluation",
                "idempotency_token_digest": None,
                "request_hash": request_hash(request_bytes),
                "opportunity_hash": opportunity_hash(
                    response,
                    resolution_policy_hash=forecast_resolution_policy_hash,
                    availability_rule_set_hash=forecast_availability_rule_set_hash,
                    origin_kind="scheduled_evaluation",
                ),
                "output_hash": output_hash(output_bytes),
                "snapshot_id": snapshot.snapshot_id,
                "resolution_policy_hash": forecast_resolution_policy_hash,
                "availability_rule_set_hash": forecast_availability_rule_set_hash,
                "symbol": response.symbol,
                "target": response.target,
                "horizon": response.horizon,
                "horizon_unit": response.horizon_unit,
                "series_basis": response.provenance.series_basis,
                "as_of": response.as_of,
                "max_available_at": response.provenance.max_available_at,
                "model_version": response.provenance.model_version,
                "feature_set_hash": response.provenance.feature_set_hash,
                "code_version": response.provenance.code_version,
                "calibration_set_version": response.calibration.calibration_set_version,
                "calibration_method": response.calibration.method,
                "generated_at": response.provenance.generated_at,
                "canonical_request": request_bytes,
                "canonical_output": output_bytes,
            }
        )

    async with snapshot_builder_engine.begin() as conn:
        for snapshot in snapshots:
            await conn.execute(pg_insert(ForecastInputSnapshot).values(**_record_values(snapshot)))
    async with engine.begin() as conn:
        for values in run_values:
            await conn.execute(pg_insert(ForecastRun).values(**values))

    async with runtime_maker() as session:
        rows = (
            (
                await session.execute(
                    select(ForecastRun)
                    .where(ForecastRun.forecast_id.in_(forecast_ids))
                    .order_by(ForecastRun.forecast_id)
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == len(forecast_ids)
    members = tuple(member_from_scheduled_run(row, step=1) for row in rows)
    manifest = ForecastCohortManifest(
        purpose=purpose,
        selection_policy_hash=selection_policy_hash,
        outcome_resolution_policy_hash=outcome_policy.outcome_resolution_policy_hash,
        availability_rule_set_hash=outcome_policy.availability_rule_set_hash,
        members=members,
    )
    proof = await SqlForecastCohortStore(runtime_maker).publish(manifest)
    assert proof.seal.sealed_at < target_time
    return _LiveCalibrationCohort(
        proof=proof,
        sources=tuple(
            ForecastOutcomePublicationSource(
                cohort_id=proof.record.cohort_id,
                forecast_id=member.forecast_id,
                step=member.step,
            )
            for member in proof.manifest.members
        ),
        target_time=target_time,
        symbol=symbol,
    )


async def _wait_for_live_database_time(
    maker: async_sessionmaker[AsyncSession],
    moment: datetime,
) -> None:
    while True:
        async with maker() as session:
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        remaining = (moment - now).total_seconds()
        if remaining <= 0:
            return
        await asyncio.sleep(min(remaining + 0.02, 0.25))


async def _live_calibration_outcome_payload(
    maker: async_sessionmaker[AsyncSession],
    *,
    outcome_policy: ForecastOutcomeResolutionPolicy,
    symbol: str,
    target_time: datetime,
) -> RealizedOutcomePayload:
    async with maker() as session:
        bar = (
            await session.execute(
                select(Bar).where(
                    Bar.symbol == symbol,
                    Bar.ts == target_time,
                    Bar.source == "polygon_open_close",
                    Bar.adjustment_basis == "raw",
                )
            )
        ).scalar_one()
        receipt = (
            await session.execute(
                select(BarVersionAvailability).where(
                    BarVersionAvailability.symbol == symbol,
                    BarVersionAvailability.ts == target_time,
                    BarVersionAvailability.source == "polygon_open_close",
                    BarVersionAvailability.adjustment_basis == "raw",
                    BarVersionAvailability.version_recorded_at == bar.recorded_at,
                )
            )
        ).scalar_one()
    source = BarVersionEvidence(
        symbol=symbol,
        timespan="day",
        multiplier=1,
        observed_at=target_time,
        source="polygon_open_close",
        adjustment_basis="raw",
        fetched_at=bar.fetched_at,
        source_as_of=bar.as_of,
        version_recorded_at=bar.recorded_at,
        available_at=receipt.available_at,
        field="close",
        value=bar.close,
    )
    cutoff = outcome_policy.cutoff_for(target_time)
    assert source.available_at <= cutoff
    return RealizedOutcomePayload(
        outcome_resolution_policy_hash=outcome_policy.outcome_resolution_policy_hash,
        availability_rule_set_hash=outcome_policy.availability_rule_set_hash,
        resolution_cutoff=cutoff,
        symbol=symbol,
        target="close",
        series_basis="raw",
        target_time=target_time,
        currency="USD",
        realized_value=bar.close,
        source_version=source,
    )


async def test_calibration_publishers_replay_fence_scope_and_immutability(
    engine: AsyncEngine,
    owner_engine: AsyncEngine,
    snapshot_builder_engine: AsyncEngine,
    migrated_database_url: LiveDatabaseUrls,
) -> None:
    """Prove migration 0015's callable boundary on real PostgreSQL.

    All evidence is labelled synthetic and remains confined to the throwaway
    database. The wrong-symbol cohort makes the database publisher, not only
    the Python reader, reject a held-out run-scope substitution.
    """

    runtime_maker = build_sessionmaker(engine)
    async with runtime_maker() as session:
        database_now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    fit_target = database_now + timedelta(seconds=12)
    heldout_target = fit_target + timedelta(seconds=1)
    outcome_policy = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=10)
    await SqlForecastOutcomePolicyStore(runtime_maker).register(outcome_policy)
    forecast_policy_hash = "sha256:" + "a" * 64
    forecast_rule_set_hash = "sha256:" + "b" * 64

    def ids(prefix: str) -> tuple[UUID, ...]:
        return tuple(UUID(f"{prefix}000000-0000-0000-0000-{number:012d}") for number in range(1, 5))

    fit = await _archive_live_calibration_cohort(
        engine=engine,
        snapshot_builder_engine=snapshot_builder_engine,
        outcome_policy=outcome_policy,
        purpose="calibration_fit",
        selection_policy_hash="sha256:" + "c" * 64,
        forecast_resolution_policy_hash=forecast_policy_hash,
        forecast_availability_rule_set_hash=forecast_rule_set_hash,
        symbol="CAL",
        target_time=fit_target,
        forecast_ids=ids("71"),
    )
    heldout = await _archive_live_calibration_cohort(
        engine=engine,
        snapshot_builder_engine=snapshot_builder_engine,
        outcome_policy=outcome_policy,
        purpose="heldout_evaluation",
        selection_policy_hash="sha256:" + "d" * 64,
        forecast_resolution_policy_hash=forecast_policy_hash,
        forecast_availability_rule_set_hash=forecast_rule_set_hash,
        symbol="CAL",
        target_time=heldout_target,
        forecast_ids=ids("72"),
    )
    wrong_scope = await _archive_live_calibration_cohort(
        engine=engine,
        snapshot_builder_engine=snapshot_builder_engine,
        outcome_policy=outcome_policy,
        purpose="heldout_evaluation",
        selection_policy_hash="sha256:" + "e" * 64,
        forecast_resolution_policy_hash=forecast_policy_hash,
        forecast_availability_rule_set_hash=forecast_rule_set_hash,
        symbol="BAD",
        target_time=heldout_target,
        forecast_ids=ids("73"),
    )

    await _wait_for_live_database_time(runtime_maker, heldout_target)
    bar_inputs = (
        OHLCVBar(
            symbol="CAL",
            timestamp=fit_target,
            timespan="day",
            multiplier=1,
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=1_000.0,
            source="polygon_open_close",
            adjustment_basis="raw",
            fetched_at=fit_target,
        ),
        OHLCVBar(
            symbol="CAL",
            timestamp=heldout_target,
            timespan="day",
            multiplier=1,
            open=102.0,
            high=104.0,
            low=101.0,
            close=103.0,
            volume=1_100.0,
            source="polygon_open_close",
            adjustment_basis="raw",
            fetched_at=heldout_target,
        ),
        OHLCVBar(
            symbol="BAD",
            timestamp=heldout_target,
            timespan="day",
            multiplier=1,
            open=98.0,
            high=100.0,
            low=97.0,
            close=99.0,
            volume=900.0,
            source="polygon_open_close",
            adjustment_basis="raw",
            fetched_at=heldout_target,
        ),
    )
    async with runtime_maker() as session, session.begin():
        write_plan = await upsert_bars(session, bar_inputs)
    async with runtime_maker() as session, session.begin():
        assert await finalize_bar_version_availability(session, write_plan.rows) == 3

    await _wait_for_live_database_time(
        runtime_maker,
        outcome_policy.cutoff_for(heldout_target),
    )
    payloads = {
        ("CAL", fit_target): await _live_calibration_outcome_payload(
            runtime_maker,
            outcome_policy=outcome_policy,
            symbol="CAL",
            target_time=fit_target,
        ),
        ("CAL", heldout_target): await _live_calibration_outcome_payload(
            runtime_maker,
            outcome_policy=outcome_policy,
            symbol="CAL",
            target_time=heldout_target,
        ),
        ("BAD", heldout_target): await _live_calibration_outcome_payload(
            runtime_maker,
            outcome_policy=outcome_policy,
            symbol="BAD",
            target_time=heldout_target,
        ),
    }
    outcome_store = SqlForecastOutcomeStore(
        sessionmaker=runtime_maker,
        outcome_resolution_policy_hash=outcome_policy.outcome_resolution_policy_hash,
        availability_rule_set_hash=outcome_policy.availability_rule_set_hash,
    )
    for cohort in (fit, heldout, wrong_scope):
        payload = payloads[(cohort.symbol, cohort.target_time)]
        for source in cohort.sources:
            published_outcome = await outcome_store.publish(payload, source=source)
            assert published_outcome.publication.cohort_id == cohort.proof.record.cohort_id
            assert published_outcome.publication.forecast_id == source.forecast_id

    evidence_reader = SqlForecastCalibrationEvidenceReader(runtime_maker)
    fit_dataset = await evidence_reader.read_validated(fit.proof.record.cohort_id)
    heldout_dataset = await evidence_reader.read_validated(heldout.proof.record.cohort_id)
    fitted_set = fit_empirical_residual_calibration_set(
        fit_dataset,
        buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
    )
    heldout_evidence = estimate_heldout_coverage(
        fitted_set,
        fit_dataset=fit_dataset,
        heldout_dataset=heldout_dataset,
        confidence_level=0.95,
    )
    release = build_heldout_coverage_release(fitted_set, heldout_evidence)
    canonical_set = canonical_calibration_set(fitted_set)

    # Content and public availability must be separate transactions. This
    # attempt is rolled back before exercising the store's intended path.
    async with engine.connect() as conn:
        transaction = await conn.begin()
        try:
            set_id = (
                await conn.execute(
                    text("SELECT publish_fitted_calibration_set(:canonical_set)"),
                    {"canonical_set": canonical_set},
                )
            ).scalar_one()
            release_id = (
                await conn.execute(
                    text("SELECT publish_forecast_heldout_coverage_release(:canonical_release)"),
                    {"canonical_release": release.canonical_release},
                )
            ).scalar_one()
            assert set_id == calibration_set_version_for(fitted_set)
            assert release_id == release.release_id
            with pytest.raises(DBAPIError, match="requires a later transaction") as excinfo:
                await conn.execute(
                    text(
                        "SELECT * FROM "
                        "publish_forecast_heldout_coverage_release_receipt(:release_id)"
                    ),
                    {"release_id": release.release_id},
                )
            assert getattr(excinfo.value.orig, "sqlstate", None) == "55000"
        finally:
            await transaction.rollback()

    release_store = SqlHeldoutCoverageReleaseStore(runtime_maker)
    published = await release_store.publish(
        fitted_set,
        heldout_cohort_id=heldout.proof.record.cohort_id,
        confidence_level=0.95,
    )
    replayed = await release_store.publish(
        fitted_set,
        heldout_cohort_id=heldout.proof.record.cohort_id,
        confidence_level=0.95,
    )
    assert replayed == published
    assert published.release.release_id == release.release_id
    assert published.release_record.evidence_scope == HELDOUT_COVERAGE_RELEASE_SCOPE
    assert published.availability.sealer_xid != published.release_record.creator_xid
    assert published.availability.available_at >= published.release_record.recorded_at

    async with runtime_maker() as session:
        fitted_row = await session.get(
            ForecastFittedCalibrationSet,
            calibration_set_version_for(fitted_set),
        )
        release_row = await session.get(ForecastHeldoutCoverageRelease, release.release_id)
        bucket_rows = (
            (
                await session.execute(
                    select(ForecastHeldoutCoverageReleaseBucket).where(
                        ForecastHeldoutCoverageReleaseBucket.release_id == release.release_id
                    )
                )
            )
            .scalars()
            .all()
        )
        receipt_row = await session.get(
            ForecastHeldoutCoverageReleaseAvailability,
            release.release_id,
        )
    assert fitted_row is not None
    assert bytes(fitted_row.canonical_set) == canonical_set
    assert fitted_row.cohort_id == fit.proof.record.cohort_id
    assert release_row is not None
    assert bytes(release_row.canonical_release) == release.canonical_release
    assert release_row.heldout_cohort_id == heldout.proof.record.cohort_id
    assert release_row.bucket_count == len(bucket_rows) == 1
    assert bucket_rows[0].sample_count == 4
    assert receipt_row is not None
    assert receipt_row.release_recorded_at == release_row.recorded_at

    # Rebind otherwise-valid release bytes to a complete held-out cohort whose
    # archived runs say BAD instead of CAL. The trusted DB publisher must not
    # rely on caller-side proof validation for this semantic boundary.
    forged_document = json.loads(release.canonical_release)
    forged_document["heldout_cohort_id"] = wrong_scope.proof.record.cohort_id
    forged_document["heldout_selection_policy_hash"] = (
        wrong_scope.proof.manifest.selection_policy_hash
    )
    forged_document["heldout_evidence_digest"] = "sha256:" + "f" * 64
    forged_release = json.dumps(
        forged_document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    async with engine.connect() as conn:
        transaction = await conn.begin()
        try:
            with pytest.raises(
                DBAPIError,
                match="held-out release scope differs from its forecast cohort",
            ) as excinfo:
                await conn.execute(
                    text("SELECT publish_forecast_heldout_coverage_release(:canonical_release)"),
                    {"canonical_release": forged_release},
                )
            assert getattr(excinfo.value.orig, "sqlstate", None) == "23000"
        finally:
            await transaction.rollback()

    async with engine.connect() as conn:
        transaction = await conn.begin()
        try:
            with pytest.raises(DBAPIError, match="permission denied"):
                await conn.execute(
                    text(
                        "INSERT INTO forecast_fitted_calibration_sets "
                        "SELECT * FROM forecast_fitted_calibration_sets WHERE false"
                    )
                )
        finally:
            await transaction.rollback()

    mutations = (
        (
            "UPDATE forecast_fitted_calibration_sets SET symbol = symbol "
            "WHERE calibration_set_version = :identity",
            calibration_set_version_for(fitted_set),
        ),
        (
            "DELETE FROM forecast_heldout_coverage_releases WHERE release_id = :identity",
            release.release_id,
        ),
        (
            "UPDATE forecast_heldout_coverage_release_buckets "
            "SET covered_count = covered_count WHERE release_id = :identity",
            release.release_id,
        ),
        (
            "DELETE FROM forecast_heldout_coverage_release_availability "
            "WHERE release_id = :identity",
            release.release_id,
        ),
    )
    for statement, identity in mutations:
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            try:
                with pytest.raises(
                    DBAPIError,
                    match="forecast calibration evidence is append-only",
                ):
                    await conn.execute(text(statement), {"identity": identity})
            finally:
                await transaction.rollback()
    for table_name in (
        "forecast_fitted_calibration_sets",
        "forecast_heldout_coverage_releases",
        "forecast_heldout_coverage_release_buckets",
        "forecast_heldout_coverage_release_availability",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            try:
                with pytest.raises(
                    DBAPIError,
                    match="forecast calibration evidence is append-only",
                ):
                    await conn.execute(text(f"TRUNCATE {table_name} CASCADE"))
            finally:
                await transaction.rollback()

    downgrade = await asyncio.to_thread(
        _invoke_alembic,
        migrated_database_url.owner,
        "downgrade",
        "0014_vendor_campaign_anchor",
    )
    assert downgrade.returncode != 0
    assert "cannot downgrade nonempty forecast calibration evidence" in (
        f"{downgrade.stdout}\n{downgrade.stderr}"
    )
    async with owner_engine.connect() as conn:
        version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
    assert version == "0015_calibration_evidence"
