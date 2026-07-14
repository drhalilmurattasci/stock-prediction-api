"""Transactional, append-only forecast-run persistence and retry replay."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import Protocol, runtime_checkable
from uuid import UUID

import anyio
from fastapi import status
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import AppError
from app.db.models.predictions import ForecastRun
from app.schemas.forecast import ForecastRequest, ForecastResponse
from app.services.forecast_runs import (
    RUN_SCHEMA_VERSION,
    ForecastRunValidationError,
    canonical_output,
    canonical_request,
    idempotency_digest,
    opportunity_hash,
    output_hash,
    parse_output,
    parse_request,
    request_hash,
)

ForecastProducer = Callable[[], Awaitable[ForecastResponse]]

_IDEMPOTENCY_UNIQUE = "uq_forecast_runs_idempotency_token_digest"
_SNAPSHOT_FOREIGN_KEY = "fk_forecast_runs_snapshot_id_forecast_input_snapshots"
_TIME_ORDER_CHECK = "ck_forecast_runs_time_order"


class _ForecastRunClockError(ForecastRunValidationError):
    """Archive database time contradicts the forecast input timeline."""


@dataclass(frozen=True)
class ArchivedForecastRun:
    """Detached scalar evidence safe to validate outside an ORM session."""

    forecast_id: UUID
    schema_version: int
    origin_kind: str
    idempotency_token_digest: str | None
    request_hash: str
    opportunity_hash: str
    output_hash: str
    snapshot_id: str
    resolution_policy_hash: str
    availability_rule_set_hash: str
    symbol: str
    target: str
    horizon: int
    horizon_unit: str
    series_basis: str
    as_of: datetime
    max_available_at: datetime
    model_version: str
    feature_set_hash: str
    code_version: str | None
    calibration_set_version: str
    calibration_method: str
    generated_at: datetime
    recorded_at: datetime | None
    canonical_request: bytes
    canonical_output: bytes


@runtime_checkable
class ForecastRunStore(Protocol):
    """Archive a successful run, or replay one bound to a retry key."""

    async def execute(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None,
        principal: str | None,
        producer: ForecastProducer,
    ) -> ForecastResponse: ...


@dataclass(frozen=True)
class SqlForecastRunStore:
    """PostgreSQL archive with connection-free compute and short finalization."""

    sessionmaker: async_sessionmaker[AsyncSession]
    identity_secret: str
    resolution_policy_hash: str
    availability_rule_set_hash: str
    origin_kind: str = "api"

    async def execute(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None,
        principal: str | None,
        producer: ForecastProducer,
    ) -> ForecastResponse:
        request_payload = canonical_request(request)
        request_identity = request_hash(request_payload)
        retry_identity = self._retry_identity(
            principal=principal,
            idempotency_key=idempotency_key,
        )

        existing = await self._preflight_retry(retry_identity)
        if existing is not None:
            return await self._replay_async(
                existing,
                expected_request=request_payload,
                expected_request_hash=request_identity,
            )

        # Snapshot lookup owns a short independent session and returns a frozen
        # record. Validation and model assembly then run without an archive
        # transaction or pooled connection pinned idle-in-transaction.
        provisional_response = await producer()
        completed_at = await self._database_completion_time()
        try:
            finalized_response, row = await anyio.to_thread.run_sync(
                partial(
                    self._finalized_row,
                    request_payload=request_payload,
                    request_identity=request_identity,
                    retry_identity=retry_identity,
                    response=provisional_response,
                    completed_at=completed_at,
                )
            )
        except _ForecastRunClockError as exc:
            raise AppError(
                "Forecast output could not be archived safely.",
                code="forecast_archive_clock_invalid",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                details={"retryable": False},
            ) from exc
        except (ForecastRunValidationError, ValueError, TypeError) as exc:
            raise AppError(
                "Forecast output could not be archived safely.",
                code="forecast_archive_validation_failed",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                details={"retryable": False},
            ) from exc

        return await self._commit_candidate(
            row,
            finalized_response=finalized_response,
            retry_identity=retry_identity,
            expected_request=request_payload,
            expected_request_hash=request_identity,
        )

    async def _preflight_retry(
        self,
        retry_identity: str | None,
    ) -> ArchivedForecastRun | None:
        if retry_identity is None:
            return None
        try:
            async with self.sessionmaker() as session:
                return await self._find_retry(session, retry_identity)
        except SQLAlchemyTimeoutError as exc:
            raise _archive_unavailable(retryable=True) from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc

    async def _database_completion_time(self) -> datetime:
        try:
            async with self.sessionmaker() as session:
                result = await session.execute(text("SELECT clock_timestamp()"))
                value = result.scalar_one()
        except SQLAlchemyTimeoutError as exc:
            raise _archive_unavailable(retryable=True) from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc
        if not isinstance(value, datetime):
            raise AppError(
                "Forecast archive clock returned an invalid value.",
                code="forecast_archive_clock_invalid",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                details={"retryable": False},
            )
        try:
            return _as_utc(value)
        except ForecastRunValidationError as exc:
            raise AppError(
                "Forecast archive clock returned an invalid value.",
                code="forecast_archive_clock_invalid",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                details={"retryable": False},
            ) from exc

    async def _commit_candidate(
        self,
        row: ForecastRun,
        *,
        finalized_response: ForecastResponse,
        retry_identity: str | None,
        expected_request: bytes,
        expected_request_hash: str,
    ) -> ForecastResponse:
        existing: ArchivedForecastRun | None = None
        flushed = False
        try:
            async with self.sessionmaker() as session, session.begin():
                if retry_identity is not None:
                    await self._lock_retry(session, retry_identity)
                    existing = await self._find_retry(session, retry_identity)
                if existing is None:
                    session.add(row)
                    await session.flush()
                    flushed = True
        except IntegrityError as exc:
            return await self._handle_integrity_error(
                exc,
                retry_identity=retry_identity,
                expected_request=expected_request,
                expected_request_hash=expected_request_hash,
            )
        except SQLAlchemyTimeoutError as exc:
            raise _archive_unavailable(retryable=True) from exc
        except DBAPIError as exc:
            sqlstate = _db_error_attribute(exc, "sqlstate")
            if _is_configuration_sqlstate(sqlstate):
                raise _database_error(exc) from exc
            if _is_known_rollback_sqlstate(sqlstate):
                raise _archive_unavailable(retryable=True) from exc
            if flushed:
                reconciled = await self._reconcile_unknown_commit(
                    row,
                    retry_identity=retry_identity,
                    expected_request=expected_request,
                    expected_request_hash=expected_request_hash,
                )
                if reconciled is not None:
                    return reconciled
                raise AppError(
                    "Forecast archive commit outcome is unknown.",
                    code="forecast_archive_commit_unknown",
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    details={
                        "outcome_unknown": True,
                        "retryable": retry_identity is not None,
                    },
                ) from exc
            raise _database_error(exc) from exc

        if existing is not None:
            return await self._replay_async(
                existing,
                expected_request=expected_request,
                expected_request_hash=expected_request_hash,
            )
        return finalized_response

    async def _handle_integrity_error(
        self,
        exc: IntegrityError,
        *,
        retry_identity: str | None,
        expected_request: bytes,
        expected_request_hash: str,
    ) -> ForecastResponse:
        constraint = _db_error_attribute(exc, "constraint_name")
        sqlstate = _db_error_attribute(exc, "sqlstate")
        if constraint == _IDEMPOTENCY_UNIQUE and retry_identity is not None:
            existing = await self._lookup_retry(retry_identity)
            if existing is not None:
                return await self._replay_async(
                    existing,
                    expected_request=expected_request,
                    expected_request_hash=expected_request_hash,
                )
            raise AppError(
                "Forecast idempotency conflict could not be reconciled.",
                code="forecast_archive_write_conflict",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                details={"retryable": True},
            ) from exc
        if constraint == _SNAPSHOT_FOREIGN_KEY or sqlstate == "23503":
            raise AppError(
                "The forecast snapshot was unavailable during archival.",
                code="forecast_snapshot_unavailable",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                details={"retryable": False},
            ) from exc
        if constraint == _TIME_ORDER_CHECK:
            raise AppError(
                "The forecast archive clock ordering is invalid.",
                code="forecast_archive_clock_invalid",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                details={"retryable": False},
            ) from exc
        raise AppError(
            "Forecast archive integrity validation failed.",
            code="forecast_archive_integrity_failed",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={"retryable": False},
        ) from exc

    async def _lookup_retry(self, retry_identity: str) -> ArchivedForecastRun | None:
        try:
            async with self.sessionmaker() as session:
                return await self._find_retry(session, retry_identity)
        except SQLAlchemyTimeoutError as exc:
            raise _archive_unavailable(retryable=True) from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc

    async def _reconcile_unknown_commit(
        self,
        row: ForecastRun,
        *,
        retry_identity: str | None,
        expected_request: bytes,
        expected_request_hash: str,
    ) -> ForecastResponse | None:
        try:
            async with self.sessionmaker() as session:
                existing = (
                    await self._find_retry(session, retry_identity)
                    if retry_identity is not None
                    else await self._find_forecast(session, row.forecast_id)
                )
        except (DBAPIError, SQLAlchemyTimeoutError):
            return None
        if existing is None:
            return None
        return await self._replay_async(
            existing,
            expected_request=expected_request,
            expected_request_hash=expected_request_hash,
        )

    def _retry_identity(
        self,
        *,
        principal: str | None,
        idempotency_key: str | None,
    ) -> str | None:
        if idempotency_key is None:
            return None
        if principal is None:
            raise AppError(
                "Authenticated principal is required for retry-safe forecast creation.",
                code="idempotency_principal_required",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        try:
            return idempotency_digest(
                principal=principal,
                idempotency_key=idempotency_key,
                secret=self.identity_secret,
            )
        except (ForecastRunValidationError, ValueError, TypeError) as exc:
            raise AppError(
                "Idempotency-Key is not valid.",
                code="invalid_idempotency_key",
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            ) from exc

    async def _lock_retry(self, session: AsyncSession, retry_identity: str) -> None:
        digest = bytes.fromhex(retry_identity.removeprefix("hmac-sha256:"))
        lock_key = int.from_bytes(digest[:8], byteorder="big", signed=True)
        result = await session.execute(
            text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
            {"lock_key": lock_key},
        )
        if result.scalar_one() is not True:
            raise AppError(
                "A forecast with this Idempotency-Key is already in progress.",
                code="idempotency_in_progress",
                status_code=status.HTTP_409_CONFLICT,
                details={"retryable": True},
            )

    async def _find_retry(
        self,
        session: AsyncSession,
        retry_identity: str,
    ) -> ArchivedForecastRun | None:
        result = await session.execute(
            select(ForecastRun).where(ForecastRun.idempotency_token_digest == retry_identity)
        )
        row = result.scalars().one_or_none()
        return None if row is None else _detach(row)

    async def _find_forecast(
        self,
        session: AsyncSession,
        forecast_id: UUID,
    ) -> ArchivedForecastRun | None:
        row = await session.get(ForecastRun, forecast_id)
        return None if row is None else _detach(row)

    async def _replay_async(
        self,
        row: ArchivedForecastRun,
        *,
        expected_request: bytes,
        expected_request_hash: str,
    ) -> ForecastResponse:
        return await anyio.to_thread.run_sync(
            partial(
                self._replay,
                row,
                expected_request=expected_request,
                expected_request_hash=expected_request_hash,
            )
        )

    def _replay(
        self,
        row: ArchivedForecastRun | ForecastRun,
        *,
        expected_request: bytes,
        expected_request_hash: str,
    ) -> ForecastResponse:
        evidence = _detach(row) if isinstance(row, ForecastRun) else row
        if evidence.request_hash != expected_request_hash:
            raise AppError(
                "Idempotency-Key was already used with a different forecast request.",
                code="idempotency_key_conflict",
                status_code=status.HTTP_409_CONFLICT,
            )
        try:
            stored_request = evidence.canonical_request
            if stored_request != expected_request:
                raise ForecastRunValidationError(
                    "stored request bytes do not match their retry request"
                )
            if request_hash(stored_request) != evidence.request_hash:
                raise ForecastRunValidationError("stored request hash is invalid")
            if parse_request(stored_request) != parse_request(expected_request):
                raise ForecastRunValidationError("stored request semantics are invalid")

            stored_output = evidence.canonical_output
            response = parse_output(stored_output)
            if output_hash(stored_output) != evidence.output_hash:
                raise ForecastRunValidationError("stored output hash is invalid")
            _validate_request_response(parse_request(stored_request), response)
            self._validate_headers(evidence, response)
            return response
        except (ForecastRunValidationError, ValueError, TypeError) as exc:
            raise AppError(
                "Persisted forecast replay evidence failed validation.",
                code="forecast_archive_corrupt",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                details={
                    "forecast_id": str(evidence.forecast_id),
                    "retryable": False,
                },
            ) from exc

    def _validate_headers(
        self,
        row: ArchivedForecastRun,
        response: ForecastResponse,
    ) -> None:
        provenance = response.provenance
        expected_opportunity = opportunity_hash(
            response,
            # A retry revalidates the policy epoch that produced the archived
            # response. Deploying a new policy must not relabel valid history
            # as corrupt or execute the forecast a second time.
            resolution_policy_hash=row.resolution_policy_hash,
            availability_rule_set_hash=row.availability_rule_set_hash,
            origin_kind=row.origin_kind,
        )
        actual = (
            row.schema_version,
            row.opportunity_hash,
            row.forecast_id,
            row.snapshot_id,
            row.resolution_policy_hash,
            row.availability_rule_set_hash,
            row.symbol,
            row.target,
            row.horizon,
            row.horizon_unit,
            row.series_basis,
            _as_utc(row.as_of),
            _as_utc(row.max_available_at),
            row.model_version,
            row.feature_set_hash,
            row.code_version,
            row.calibration_set_version,
            row.calibration_method,
            _as_utc(row.generated_at),
        )
        expected = (
            RUN_SCHEMA_VERSION,
            expected_opportunity,
            provenance.forecast_id,
            provenance.snapshot_id,
            row.resolution_policy_hash,
            row.availability_rule_set_hash,
            response.symbol,
            response.target,
            response.horizon,
            response.horizon_unit,
            provenance.series_basis,
            _as_utc(response.as_of),
            _as_utc(provenance.max_available_at),
            provenance.model_version,
            _feature_hash(provenance.feature_set_hash),
            provenance.code_version,
            response.calibration.calibration_set_version,
            response.calibration.method,
            _as_utc(provenance.generated_at),
        )
        if actual != expected:
            raise ForecastRunValidationError("forecast-run headers do not match canonical output")

    def _finalized_row(
        self,
        *,
        request_payload: bytes,
        request_identity: str,
        retry_identity: str | None,
        response: ForecastResponse,
        completed_at: datetime,
    ) -> tuple[ForecastResponse, ForecastRun]:
        finalized = _with_database_completion_time(response, completed_at)
        return finalized, self._row(
            request_payload=request_payload,
            request_identity=request_identity,
            retry_identity=retry_identity,
            response=finalized,
        )

    def _row(
        self,
        *,
        request_payload: bytes,
        request_identity: str,
        retry_identity: str | None,
        response: ForecastResponse,
    ) -> ForecastRun:
        provenance = response.provenance
        _validate_request_response(parse_request(request_payload), response)
        output_payload = canonical_output(response)
        return ForecastRun(
            forecast_id=provenance.forecast_id,
            schema_version=RUN_SCHEMA_VERSION,
            origin_kind=self.origin_kind,
            idempotency_token_digest=retry_identity,
            request_hash=request_identity,
            opportunity_hash=opportunity_hash(
                response,
                resolution_policy_hash=self.resolution_policy_hash,
                availability_rule_set_hash=self.availability_rule_set_hash,
                origin_kind=self.origin_kind,
            ),
            output_hash=output_hash(output_payload),
            snapshot_id=provenance.snapshot_id,
            resolution_policy_hash=self.resolution_policy_hash,
            availability_rule_set_hash=self.availability_rule_set_hash,
            symbol=response.symbol,
            target=response.target,
            horizon=response.horizon,
            horizon_unit=response.horizon_unit,
            series_basis=provenance.series_basis,
            as_of=_as_utc(response.as_of),
            max_available_at=_as_utc(provenance.max_available_at),
            model_version=provenance.model_version,
            feature_set_hash=_feature_hash(provenance.feature_set_hash),
            code_version=provenance.code_version,
            calibration_set_version=response.calibration.calibration_set_version,
            calibration_method=response.calibration.method,
            generated_at=_as_utc(provenance.generated_at),
            canonical_request=request_payload,
            canonical_output=output_payload,
        )


def _detach(row: ForecastRun) -> ArchivedForecastRun:
    """Copy all replay evidence while the ORM row is still session-bound."""

    return ArchivedForecastRun(
        forecast_id=row.forecast_id,
        schema_version=row.schema_version,
        origin_kind=row.origin_kind,
        idempotency_token_digest=row.idempotency_token_digest,
        request_hash=row.request_hash,
        opportunity_hash=row.opportunity_hash,
        output_hash=row.output_hash,
        snapshot_id=row.snapshot_id,
        resolution_policy_hash=row.resolution_policy_hash,
        availability_rule_set_hash=row.availability_rule_set_hash,
        symbol=row.symbol,
        target=row.target,
        horizon=row.horizon,
        horizon_unit=row.horizon_unit,
        series_basis=row.series_basis,
        as_of=row.as_of,
        max_available_at=row.max_available_at,
        model_version=row.model_version,
        feature_set_hash=row.feature_set_hash,
        code_version=row.code_version,
        calibration_set_version=row.calibration_set_version,
        calibration_method=row.calibration_method,
        generated_at=row.generated_at,
        recorded_at=row.recorded_at,
        canonical_request=bytes(row.canonical_request),
        canonical_output=bytes(row.canonical_output),
    )


def _with_database_completion_time(
    response: ForecastResponse,
    completed_at: datetime,
) -> ForecastResponse:
    """Replace provisional host time with the archive DB's observed completion time."""

    completed = _as_utc(completed_at)
    if completed < _as_utc(response.as_of) or completed < _as_utc(
        response.provenance.max_available_at
    ):
        raise _ForecastRunClockError(
            "database completion time precedes forecast input availability"
        )
    lookahead = response.provenance.lookahead_check.model_copy(update={"checked_at": completed})
    provenance = response.provenance.model_copy(
        update={
            "generated_at": completed,
            "lookahead_check": lookahead,
        }
    )
    candidate = response.model_copy(update={"provenance": provenance})
    try:
        return ForecastResponse.model_validate(candidate.model_dump(mode="python", round_trip=True))
    except (ValueError, TypeError) as exc:
        raise ForecastRunValidationError(
            "database-finalized forecast response fails validation"
        ) from exc


def _db_error_attribute(exc: DBAPIError, name: str) -> str | None:
    """Read structured driver metadata without parsing secret-bearing messages."""

    pending: list[object] = [exc, exc.orig]
    seen: set[int] = set()
    while pending:
        candidate = pending.pop(0)
        if candidate is None or id(candidate) in seen:
            continue
        seen.add(id(candidate))
        value = getattr(candidate, name, None)
        if isinstance(value, str) and value:
            return value
        pending.extend(
            (
                getattr(candidate, "__cause__", None),
                getattr(candidate, "__context__", None),
            )
        )
    return None


def _archive_unavailable(*, retryable: bool) -> AppError:
    return AppError(
        "Forecast archive storage is unavailable.",
        code="forecast_archive_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": retryable},
    )


def _database_error(exc: DBAPIError) -> AppError:
    """Classify structured SQLSTATEs without exposing SQL or driver text."""

    sqlstate = _db_error_attribute(exc, "sqlstate")
    if _is_configuration_sqlstate(sqlstate):
        return AppError(
            "Forecast archive database configuration is invalid.",
            code="forecast_archive_configuration_invalid",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={"retryable": False},
        )
    return _archive_unavailable(retryable=True)


def _is_configuration_sqlstate(sqlstate: str | None) -> bool:
    return sqlstate is not None and sqlstate.startswith(("0A", "22", "28", "3D", "3F", "42"))


def _is_known_rollback_sqlstate(sqlstate: str | None) -> bool:
    return sqlstate is not None and (sqlstate.startswith("40") or sqlstate == "57014")


def _feature_hash(value: str) -> str:
    normalized = value.lower()
    return normalized if normalized.startswith("sha256:") else f"sha256:{normalized}"


def _validate_request_response(
    request: ForecastRequest,
    response: ForecastResponse,
) -> None:
    if (
        response.symbol != request.symbol
        or response.target != request.target
        or response.horizon != request.horizon
        or response.horizon_unit != request.horizon_unit
    ):
        raise ForecastRunValidationError(
            "forecast output identity does not match its accepted request"
        )
    if request.snapshot_id is not None and response.provenance.snapshot_id != request.snapshot_id:
        raise ForecastRunValidationError(
            "forecast output snapshot does not match the pinned request"
        )
    if request.as_of is not None and _as_utc(response.as_of) > _as_utc(request.as_of):
        raise ForecastRunValidationError("forecast output exceeds the request cutoff")
    requested_coverages = set(request.interval_coverages)
    if any(
        {interval.coverage for interval in step.intervals} != requested_coverages
        for step in response.forecasts
    ):
        raise ForecastRunValidationError(
            "forecast output interval coverages do not match the request"
        )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ForecastRunValidationError("forecast-run timestamps must be timezone-aware")
    return value.astimezone(UTC)


__all__ = ["ForecastRunStore", "SqlForecastRunStore"]
