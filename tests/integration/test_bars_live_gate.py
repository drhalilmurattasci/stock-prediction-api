"""Live-database gate: migrations, writes, reads, and immutable snapshots.

Skipped unless ``TEST_DATABASE_URL`` points at a **throwaway** TimescaleDB
through an owner/admin account. The fixture RESETS the target database (bars
tables and alembic_version are dropped) before applying migrations. It never
creates or mutates the cluster-global runtime roles: bootstrap them first and
provide both least-privilege URLs. Never point this gate at shared data. Run with::

    docker compose up -d timescaledb          # needs .env credentials
    $env:TEST_DATABASE_URL = "postgresql+asyncpg://<user>:<pass>@127.0.0.1:5432/<db>"
    # Optional when .env DATABASE_URL does not target the same throwaway DB:
    $env:TEST_RUNTIME_DATABASE_URL = "postgresql+asyncpg://stockapi_app:<pass>@127.0.0.1:5432/<db>"
    $env:TEST_SNAPSHOT_BUILDER_DATABASE_URL = "postgresql+asyncpg://stockapi_snapshot_builder:<pass>@127.0.0.1:5432/<db>"
    $env:TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET = "stockapi-test-only"
    uv run pytest tests/integration -v

This is the empirical proof the unit suite cannot give: the Alembic chain
applies against a real hypertable, ``upsert_bars`` replay is a no-op while a
restatement writes a revision row, a seeded two-page ``/v1/prices`` read has
no gaps or duplicates with TIMESTAMPTZ round-tripping, the finiteness CHECKs
actually reject NaN/Infinity under Postgres NaN ordering, and the API
statement-timeout cancels a pathological statement.
The same command also proves forecast-input snapshot SHA-256 enforcement,
idempotent insertion, semantic collision rejection, pure resolution, and
database-level UPDATE/DELETE/TRUNCATE refusal. Migration ``0010`` wires the same
gate to check content-addressed realized outcomes bound to an exact
bar-availability receipt and cohort manifests whose availability can be sealed
only after the manifest commits and before its first target.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import threading
from dataclasses import dataclass, fields, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Self
from uuid import UUID

import exchange_calendars
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, make_url, select, text
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
from app.db.models.bars import Bar, BarRevision, BarVersionAvailability
from app.db.models.forecast_evidence import (
    ForecastOutcomeCohortAvailability,
    ForecastOutcomeCohortManifest,
    ForecastOutcomeCohortMember,
    ForecastRealizedOutcome,
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
from app.schemas.prices import PriceFilters
from app.services.forecast_cohort_store import SqlForecastCohortStore
from app.services.forecast_cohorts import (
    ForecastCohortManifest,
    ForecastCohortMember,
    canonical_cohort_manifest,
    cohort_id_for_manifest,
    member_from_scheduled_run,
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
from app.services.prices import read_prices
from app.services.scheduled_evaluation import (
    ScheduledEvaluationService,
    ScheduledEvaluationSpec,
)
from data_sources.base import OHLCVBar
from ingestion.locks import (
    VendorOperationBusy,
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


class _MissingReadBarrier:
    """Release two real sessions only after both observed one row missing."""

    def __init__(self, model: type[Any]) -> None:
        self.model = model
        self.arrivals = 0
        self._lock = asyncio.Lock()
        self._release = asyncio.Event()

    async def arrive(self) -> None:
        async with self._lock:
            self.arrivals += 1
            if self.arrivals == 2:
                self._release.set()
        await asyncio.wait_for(self._release.wait(), timeout=5)


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


@dataclass(frozen=True)
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
                    "DROP TABLE IF EXISTS forecast_outcome_cohort_availability, "
                    "forecast_outcome_cohort_members, forecast_outcome_cohort_manifests, "
                    "forecast_realized_outcomes, forecast_runs, forecast_input_snapshots, "
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
            ):
                conn.execute(text(f"DROP FUNCTION IF EXISTS {function_name}()"))
    finally:
        sync_engine.dispose()


def _run_alembic(url: str, *arguments: str) -> None:
    environment = os.environ.copy()
    environment.pop("ALEMBIC_CONFIG", None)
    environment.update({"DATABASE_URL": url, "MIGRATION_DATABASE_URL": url})
    result = subprocess.run(  # fresh process: env.py resolves DATABASE_URL uncached
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


async def test_migration_chain_applies_and_bars_is_a_hypertable(
    owner_engine: AsyncEngine,
) -> None:
    async with owner_engine.connect() as conn:
        version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
        assert version == "0010_forecast_evidence"
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


async def test_forecast_evidence_schema_and_role_boundaries(
    owner_engine: AsyncEngine,
) -> None:
    tables = (
        "forecast_realized_outcomes",
        "forecast_outcome_cohort_manifests",
        "forecast_outcome_cohort_members",
        "forecast_outcome_cohort_availability",
    )
    required_constraints = {
        "uq_bar_version_availability_exact_receipt",
        "ck_forecast_realized_outcomes_outcome_id_matches_payload",
        "uq_forecast_realized_outcomes_semantic_key",
        "ck_forecast_outcome_cohort_manifests_cohort_id_matches_payload",
        "pk_forecast_outcome_cohort_members",
        "uq_forecast_outcome_cohort_members_opportunity_step",
    }
    required_triggers = {
        ("forecast_realized_outcomes", "forecast_realized_outcomes_stamp"),
        ("forecast_realized_outcomes", "forecast_realized_outcomes_no_row_mutation"),
        ("forecast_realized_outcomes", "forecast_realized_outcomes_no_truncate"),
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
            app_may_insert = table_name != "forecast_outcome_cohort_members"
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
                        "has_table_privilege('stockapi_app', :table_name, 'MAINTAIN')"
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
                        "'stockapi_snapshot_builder', :table_name, 'TRUNCATE')"
                    ),
                    {"table_name": table_name},
                )
            ).one()
            assert tuple(builder) == (False, False, False, False, False)

        for function_name in (
            "stamp_forecast_realized_outcome()",
            "stamp_forecast_outcome_cohort_manifest()",
            "validate_forecast_outcome_cohort_member()",
            "materialize_forecast_outcome_cohort_members()",
            "stamp_forecast_outcome_cohort_availability()",
            "reject_forecast_evidence_mutation()",
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

    source = BarVersionEvidence(
        symbol=bar.symbol,
        timespan=bar.timespan,
        multiplier=bar.multiplier,
        observed_at=bar.ts,
        source=bar.source,
        adjustment_basis=bar.adjustment_basis,
        fetched_at=bar.fetched_at,
        source_as_of=bar.as_of,
        version_recorded_at=bar.recorded_at,
        available_at=receipt.available_at,
        field="close",
        value=bar.close,
    )
    payload = RealizedOutcomePayload(
        outcome_resolution_policy_hash="sha256:" + "a" * 64,
        availability_rule_set_hash="sha256:" + "b" * 64,
        resolution_cutoff=receipt.available_at,
        symbol=bar.symbol,
        target="close",
        series_basis="raw",
        target_time=bar.ts,
        currency="USD",
        realized_value=bar.close,
        source_version=source,
    )
    canonical = canonical_outcome_payload(payload)
    caller_sealed_at = datetime(2000, 1, 1, tzinfo=UTC)
    outcome_id = outcome_id_for_payload(canonical)
    async with engine.begin() as conn:
        before = (await conn.execute(select(func.clock_timestamp()))).scalar_one()
        stored = (
            await conn.execute(
                pg_insert(ForecastRealizedOutcome)
                .values(
                    outcome_id=outcome_id,
                    sealed_at=caller_sealed_at,
                    canonical_evidence=canonical,
                )
                .returning(*ForecastRealizedOutcome.__table__.c)
            )
        ).one()
        after = (await conn.execute(select(func.clock_timestamp()))).scalar_one()
    assert before <= stored.sealed_at <= after
    assert stored.sealed_at != caller_sealed_at
    assert stored.outcome_id == outcome_id
    assert stored.bar_value == stored.realized_value == bar.close
    assert stored.bar_fetched_at == bar.fetched_at
    assert stored.bar_source_as_of == bar.as_of
    assert stored.bar_available_at == receipt.available_at

    mismatched_payload = replace(
        payload,
        outcome_resolution_policy_hash="sha256:" + "c" * 64,
    )
    mismatched_canonical = canonical_outcome_payload(mismatched_payload)
    async with engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(
            IntegrityError,
            match="ck_forecast_realized_outcomes_outcome_id_matches_payload",
        ):
            await conn.execute(
                pg_insert(ForecastRealizedOutcome).values(
                    outcome_id="sha256:" + "0" * 64,
                    canonical_evidence=mismatched_canonical,
                )
            )
        await transaction.rollback()

    wrong_value = bar.close + 1.0
    wrong_value_source = replace(source, value=wrong_value)
    wrong_value_payload = replace(
        payload,
        outcome_resolution_policy_hash="sha256:" + "d" * 64,
        realized_value=wrong_value,
        source_version=wrong_value_source,
    )
    wrong_value_canonical = canonical_outcome_payload(wrong_value_payload)
    async with engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(DBAPIError, match="does not match its exact bar version"):
            await conn.execute(
                pg_insert(ForecastRealizedOutcome).values(
                    outcome_id=outcome_id_for_payload(wrong_value_canonical),
                    canonical_evidence=wrong_value_canonical,
                )
            )
        await transaction.rollback()

    fake_receipt_time = receipt.available_at + timedelta(microseconds=1)
    wrong_receipt_source = replace(source, available_at=fake_receipt_time)
    wrong_receipt_payload = replace(
        payload,
        outcome_resolution_policy_hash="sha256:" + "e" * 64,
        resolution_cutoff=fake_receipt_time,
        source_version=wrong_receipt_source,
    )
    wrong_receipt_canonical = canonical_outcome_payload(wrong_receipt_payload)
    async with engine.connect() as conn:
        transaction = await conn.begin()
        with pytest.raises(DBAPIError, match="stored availability receipt"):
            await conn.execute(
                pg_insert(ForecastRealizedOutcome).values(
                    outcome_id=outcome_id_for_payload(wrong_receipt_canonical),
                    canonical_evidence=wrong_receipt_canonical,
                )
            )
        await transaction.rollback()

    for mutation in (
        "UPDATE forecast_realized_outcomes SET symbol = symbol WHERE outcome_id = :outcome_id",
        "DELETE FROM forecast_realized_outcomes WHERE outcome_id = :outcome_id",
        "TRUNCATE forecast_realized_outcomes",
    ):
        async with owner_engine.connect() as conn:
            transaction = await conn.begin()
            with pytest.raises(DBAPIError, match="forecast evidence is insert-only"):
                await conn.execute(
                    text(mutation),
                    {"outcome_id": outcome_id} if ":outcome_id" in mutation else {},
                )
            await transaction.rollback()


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
        outcome_resolution_policy_hash="sha256:" + "a" * 64,
        availability_rule_set_hash=availability_rule_set_hash,
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
        "TRUNCATE forecast_outcome_cohort_members",
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
) -> ForecastInputSnapshotRecord:
    as_of = datetime(2026, 7, 10, 21, tzinfo=UTC)
    return build_snapshot_record(
        ForecastInputSnapshotPayload(
            resolution_policy_hash=policy_hash,
            symbol="AAPL",
            target="close",
            horizon_unit="calendar_day",
            series_basis="raw",
            input_timespan="day",
            input_multiplier=1,
            as_of=as_of,
            currency="USD",
            observations=(
                SnapshotObservation(
                    observed_at=as_of - timedelta(days=2, hours=1),
                    available_at=as_of - timedelta(days=2),
                    value=100.0,
                ),
                SnapshotObservation(
                    observed_at=as_of - timedelta(days=1, hours=1),
                    available_at=as_of - timedelta(days=1),
                    value=final_value,
                ),
            ),
            target_times=(as_of + timedelta(days=1), as_of + timedelta(days=2)),
            data_sources=(
                SnapshotSourceLineage(
                    name="live-gate",
                    snapshot_id="live-gate-source-v1",
                    max_available_at=as_of - timedelta(hours=1),
                    fields=("close",),
                ),
            ),
            availability=SnapshotAvailabilityEvidence(status="not_run"),
        ),
        sealed_at=as_of + timedelta(minutes=1),
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
    scheduled_spec = ScheduledEvaluationSpec(
        request=scheduled_request,
        purpose="heldout_evaluation",
        selected_steps=(1, 2),
        model_version="baseline-naive@1",
        code_version="live-gate-scheduled-1",
        forecast_resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
        forecast_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        selection_policy_hash="sha256:" + "d" * 64,
        outcome_resolution_policy_hash="sha256:" + "e" * 64,
        outcome_availability_rule_set_hash="sha256:" + "f" * 64,
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
        "TRUNCATE forecast_outcome_cohort_members",
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
