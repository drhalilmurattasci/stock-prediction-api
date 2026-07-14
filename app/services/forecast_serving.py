"""Snapshot-backed forecast serving: the wiring behind ``/v1/forecast``.

Composes the three already-verified pure layers — the read-only snapshot
repository, ``validate_and_resolve_snapshot``, and
``assemble_baseline_forecast_response`` — into a :class:`ForecastService`.
Every gate fails closed:

* serving requires an explicitly configured resolution-policy hash AND a
  trusted availability rule-set hash (unset keeps the route at 501);
* only snapshots whose availability evidence verifies against the trusted
  rule set are served — a "not_run"/untrusted snapshot is refused, never
  emitted with a soft warning;
* every successful response is committed to the forecast-run archive before
  release, and ``Idempotency-Key`` retries replay its validated canonical model.
* snapshot loading and forecast computation finish before the archive opens its
  short lock/recheck/insert transaction, so CPU work cannot starve the DB pool.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import anyio.to_thread
from fastapi import status
from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import AppError, NotFoundError, NotImplementedYet
from app.db.models.forecast_snapshots import ForecastInputSnapshot
from app.schemas.forecast import ForecastRequest, ForecastResponse
from app.services.forecast_run_store import ForecastRunStore, SqlForecastRunStore
from app.services.forecast_snapshots import (
    SNAPSHOT_SCHEMA_VERSION,
    ForecastInputSnapshotRecord,
    ForecastInputSnapshotRepository,
    ForecastInputSnapshotSelector,
    SnapshotValidationError,
    validate_and_resolve_snapshot,
)
from app.services.forecasting import (
    ForecastRunIdentity,
    assemble_baseline_forecast_response,
)
from ml.models.base import Forecaster
from ml.models.baselines import DriftForecaster, NaiveForecaster, SeasonalNaiveForecaster

if TYPE_CHECKING:
    from app.config import Settings

_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_BUILD_REVISION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_BUILD_REVISION_PATH = Path("/app/.stockapi-build-revision")

#: Policy v1 deliberately serves raw closes only. Adjusted targets require the
#: separate corporate-action ledger promised by the project doctrine; vendor-
#: rewritten adjusted history is not relabelled as locally reproducible data.
_SERIES_BASIS_BY_TARGET = {
    "close": "raw",
}


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def read_build_revision(path: Path = _BUILD_REVISION_PATH) -> str | None:
    """Read the image-attested code identity without inventing a local value."""

    if not path.exists():
        return None
    try:
        revision = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise AppError(
            "Forecast build revision cannot be read.",
            code="forecast_build_revision_invalid",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        ) from exc
    if not revision or revision == "unattested":
        return None
    if _BUILD_REVISION_PATTERN.fullmatch(revision) is None:
        raise AppError(
            "Forecast build revision is malformed.",
            code="forecast_build_revision_invalid",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    return revision


@dataclass(frozen=True)
class ForecastServingPolicy:
    """Operator-pinned resolution and trust identity for served forecasts."""

    resolution_policy_hash: str
    trusted_availability_rule_set_hash: str
    input_timespan: str = "day"
    input_multiplier: int = 1
    seasonal_period: int = 5

    def series_basis_for(self, target: str) -> str:
        basis = _SERIES_BASIS_BY_TARGET.get(target)
        if basis is None:
            raise AppError(
                f"target {target!r} has no servable series basis",
                code="target_not_servable",
                status_code=status.HTTP_409_CONFLICT,
            )
        return basis


def build_latest_snapshot_statement(
    selector: ForecastInputSnapshotSelector,
    *,
    trusted_availability_rule_set_hash: str | None = None,
) -> Select[tuple[ForecastInputSnapshot]]:
    """Newest sealed snapshot for one exact semantic series, bounded by cutoff.

    With a trusted rule-set hash, only snapshots carrying that availability
    proof qualify: an unverified newer snapshot must not outage a series whose
    older snapshot is servable (the response honestly carries its ``as_of``).
    Byte/hash verification still happens in ``validate_and_resolve_snapshot``.
    """

    statement = select(ForecastInputSnapshot).where(
        ForecastInputSnapshot.schema_version == SNAPSHOT_SCHEMA_VERSION,
        ForecastInputSnapshot.resolution_policy_hash == selector.resolution_policy_hash,
        ForecastInputSnapshot.symbol == selector.symbol,
        ForecastInputSnapshot.target == selector.target,
        ForecastInputSnapshot.horizon_unit == selector.horizon_unit,
        ForecastInputSnapshot.series_basis == selector.series_basis,
        ForecastInputSnapshot.input_timespan == selector.input_timespan,
        ForecastInputSnapshot.input_multiplier == selector.input_multiplier,
    )
    if selector.cutoff is not None:
        statement = statement.where(ForecastInputSnapshot.as_of <= selector.cutoff)
    if trusted_availability_rule_set_hash is not None:
        statement = statement.where(
            ForecastInputSnapshot.availability_status == "passed",
            ForecastInputSnapshot.availability_rule_set_hash == trusted_availability_rule_set_hash,
        )
    # The semantic-key unique constraint makes as_of unique per series, so this
    # ordering is deterministic without a tie-breaker.
    return statement.order_by(ForecastInputSnapshot.as_of.desc()).limit(1)


def _record_from_row(row: ForecastInputSnapshot) -> ForecastInputSnapshotRecord:
    return ForecastInputSnapshotRecord(
        snapshot_id=row.snapshot_id,
        schema_version=row.schema_version,
        resolution_policy_hash=row.resolution_policy_hash,
        symbol=row.symbol,
        target=row.target,
        horizon_unit=row.horizon_unit,
        series_basis=row.series_basis,
        input_timespan=row.input_timespan,
        input_multiplier=row.input_multiplier,
        as_of=row.as_of,
        sealed_at=row.sealed_at,
        currency=row.currency,
        observation_count=row.observation_count,
        target_time_count=row.target_time_count,
        first_observed_at=row.first_observed_at,
        last_observed_at=row.last_observed_at,
        max_available_at=row.max_available_at,
        availability_status=row.availability_status,
        availability_rule_set_hash=row.availability_rule_set_hash,
        availability_checked_at=row.availability_checked_at,
        canonical_payload=bytes(row.canonical_payload),
    )


@dataclass(frozen=True)
class SessionForecastInputSnapshotRepository:
    """Read-only snapshot store bound to an existing transaction/session."""

    session: AsyncSession
    trusted_availability_rule_set_hash: str | None = None

    async def get(self, snapshot_id: str) -> ForecastInputSnapshotRecord | None:
        row = await self.session.get(ForecastInputSnapshot, snapshot_id)
        return None if row is None else _record_from_row(row)

    async def latest(
        self,
        selector: ForecastInputSnapshotSelector,
    ) -> ForecastInputSnapshotRecord | None:
        result = await self.session.execute(
            build_latest_snapshot_statement(
                selector,
                trusted_availability_rule_set_hash=(self.trusted_availability_rule_set_hash),
            )
        )
        row = result.scalars().first()
        return None if row is None else _record_from_row(row)


@dataclass(frozen=True)
class SqlForecastInputSnapshotRepository:
    """Read-only snapshot store that owns a short session per lookup."""

    sessionmaker: async_sessionmaker[AsyncSession]
    trusted_availability_rule_set_hash: str | None = None

    async def get(self, snapshot_id: str) -> ForecastInputSnapshotRecord | None:
        async with self.sessionmaker() as session:
            return await SessionForecastInputSnapshotRepository(
                session,
                self.trusted_availability_rule_set_hash,
            ).get(snapshot_id)

    async def latest(
        self,
        selector: ForecastInputSnapshotSelector,
    ) -> ForecastInputSnapshotRecord | None:
        async with self.sessionmaker() as session:
            return await SessionForecastInputSnapshotRepository(
                session,
                self.trusted_availability_rule_set_hash,
            ).latest(selector)


@dataclass(frozen=True)
class SnapshotForecastService:
    """Serve baseline forecasts strictly from verified immutable snapshots."""

    repository: ForecastInputSnapshotRepository
    policy: ForecastServingPolicy
    clock: Callable[[], datetime] = _utc_now
    id_factory: Callable[[], UUID] = uuid4
    code_version: str | None = None
    run_store: ForecastRunStore | None = None

    async def forecast(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None = None,
        principal: str | None = None,
    ) -> ForecastResponse:
        if self.run_store is not None:
            return await self.run_store.execute(
                request,
                idempotency_key=idempotency_key,
                principal=principal,
                producer=lambda: self._produce(request, self.repository),
            )
        if idempotency_key is not None:
            raise NotImplementedYet(
                "Retry-safe forecast creation requires a persisted forecast-run "
                "store; retry without an Idempotency-Key for one-shot creation.",
                details={"idempotency_requested": True},
            )
        return await self._produce(request, self.repository)

    async def _produce(
        self,
        request: ForecastRequest,
        repository: ForecastInputSnapshotRepository,
    ) -> ForecastResponse:
        if request.horizon_unit != "trading_day":
            raise AppError(
                "only trading_day forecast horizons are servable by policy v1",
                code="horizon_unit_not_servable",
                status_code=status.HTTP_409_CONFLICT,
            )
        series_basis = self.policy.series_basis_for(request.target)
        record = await self._load_record(request, series_basis, repository)
        try:
            # Byte re-canonicalization + SHA-256 of a payload up to 4 MiB is
            # pure CPU; keep it off the event loop.
            resolved = await anyio.to_thread.run_sync(
                self._resolve_record, record, request, series_basis
            )
        except SnapshotValidationError as exc:
            raise AppError(
                str(exc),
                code="snapshot_validation_failed",
                status_code=status.HTTP_409_CONFLICT,
                details={"snapshot_id": record.snapshot_id},
            ) from exc
        if not resolved.availability_verified:
            # The honesty gate: a forecast whose inputs lack a trusted
            # availability proof must be refused, not served with a footnote.
            raise AppError(
                "snapshot availability evidence is not verified by the trusted rule set",
                code="snapshot_not_verified",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                details={"snapshot_id": record.snapshot_id},
            )
        factory = self._forecaster_factory(request.model)
        identity = ForecastRunIdentity(
            forecast_id=self.id_factory(),
            # Clamp: host clock skew must not turn a verified snapshot into a
            # 500 via assemble's generated_at >= as_of invariant.
            generated_at=max(self.clock(), resolved.as_of),
            model_version=factory().model_version,
            feature_set_hash=record.snapshot_id,
            code_version=self.code_version,
        )

        def _assemble() -> ForecastResponse:
            return assemble_baseline_forecast_response(
                request,
                resolved,
                forecaster_factory=factory,
                identity=identity,
            )

        try:
            # Baseline fitting + empirical residual quantiles are seconds of
            # pure CPU at contract-maximal inputs; one request must not stall
            # the event loop (and with it health probes and every other client).
            return await anyio.to_thread.run_sync(_assemble)
        except ValueError as exc:
            # Data-driven infeasibility on a fully verified snapshot (horizon
            # beyond the usable residual history, series too short for the
            # requested model, degenerate/flat history) is a structured,
            # client-visible refusal — not an internal error.
            raise AppError(
                str(exc),
                code="forecast_not_computable",
                status_code=status.HTTP_409_CONFLICT,
                details={"snapshot_id": record.snapshot_id},
            ) from exc

    def _resolve_record(
        self,
        record: ForecastInputSnapshotRecord,
        request: ForecastRequest,
        series_basis: str,
    ):
        return validate_and_resolve_snapshot(
            record,
            request,
            expected_series_basis=series_basis,
            expected_resolution_policy_hash=self.policy.resolution_policy_hash,
            expected_input_timespan=self.policy.input_timespan,
            expected_input_multiplier=self.policy.input_multiplier,
            trusted_availability_rule_set_hash=(self.policy.trusted_availability_rule_set_hash),
        )

    async def _load_record(
        self,
        request: ForecastRequest,
        series_basis: str,
        repository: ForecastInputSnapshotRepository,
    ) -> ForecastInputSnapshotRecord:
        if request.snapshot_id is not None:
            record = await repository.get(request.snapshot_id)
        else:
            record = await repository.latest(
                ForecastInputSnapshotSelector(
                    resolution_policy_hash=self.policy.resolution_policy_hash,
                    symbol=request.symbol,
                    target=request.target,
                    horizon_unit=request.horizon_unit,
                    series_basis=series_basis,
                    input_timespan=self.policy.input_timespan,
                    input_multiplier=self.policy.input_multiplier,
                    cutoff=request.as_of,
                )
            )
        if record is None:
            raise NotFoundError(
                f"no sealed forecast-input snapshot is available for {request.symbol}",
                details={
                    "symbol": request.symbol,
                    "target": request.target,
                    "pinned_snapshot_id": request.snapshot_id,
                },
            )
        return record

    def _forecaster_factory(self, model: str) -> Callable[[], Forecaster]:
        # "auto" routes to the honest default until a promoted-champion
        # registry exists; the response's model_version states what ran.
        if model in ("auto", "baseline_naive"):
            return NaiveForecaster
        if model == "baseline_drift":
            return DriftForecaster
        if model == "baseline_seasonal_naive":
            period = self.policy.seasonal_period
            return lambda: SeasonalNaiveForecaster(period)
        # Schema-valid selectors without a serving implementation yet (e.g.
        # arima, chronos) are honestly 501, matching the codebase taxonomy.
        raise NotImplementedYet(
            f"model selector {model!r} is not implemented for serving yet",
            details={"model": model},
        )


def build_forecast_service(
    settings: Settings,
    sessionmaker: async_sessionmaker[AsyncSession],
) -> SnapshotForecastService | None:
    """Build the snapshot-backed service, or ``None`` when serving is not enabled.

    Unset hashes mean serving was never enabled (the route stays 501). A
    malformed configured hash is an operator error and raises loudly instead of
    silently downgrading to 501.
    """
    policy_hash = settings.forecast_resolution_policy_hash
    trusted_hash = settings.forecast_trusted_availability_rule_set_hash
    if policy_hash is None and trusted_hash is None:
        return None
    if (
        policy_hash is None
        or trusted_hash is None
        or _HASH_PATTERN.fullmatch(policy_hash) is None
        or _HASH_PATTERN.fullmatch(trusted_hash) is None
    ):
        raise AppError(
            "forecast serving is misconfigured: both the resolution-policy hash "
            "and the trusted availability rule-set hash must be sha256:<hex64>",
            code="forecast_serving_misconfigured",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    return SnapshotForecastService(
        repository=SqlForecastInputSnapshotRepository(
            sessionmaker,
            trusted_availability_rule_set_hash=trusted_hash,
        ),
        policy=ForecastServingPolicy(
            resolution_policy_hash=policy_hash,
            trusted_availability_rule_set_hash=trusted_hash,
            seasonal_period=settings.forecast_seasonal_period,
        ),
        code_version=read_build_revision(),
        run_store=SqlForecastRunStore(
            sessionmaker=sessionmaker,
            identity_secret=settings.jwt_secret,
            resolution_policy_hash=policy_hash,
            availability_rule_set_hash=trusted_hash,
        ),
    )
