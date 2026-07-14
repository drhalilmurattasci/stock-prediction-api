"""Transactional, append-only forecast-run persistence and retry replay."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from fastapi import status
from sqlalchemy import select, text
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
from app.services.forecast_snapshots import ForecastInputSnapshotRepository

ForecastProducer = Callable[[ForecastInputSnapshotRepository], Awaitable[ForecastResponse]]
SnapshotRepositoryFactory = Callable[[AsyncSession], ForecastInputSnapshotRepository]


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
    """PostgreSQL-backed run archive with transaction-scoped retry locking."""

    sessionmaker: async_sessionmaker[AsyncSession]
    repository_factory: SnapshotRepositoryFactory
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

        async with self.sessionmaker() as session, session.begin():
            if retry_identity is not None:
                await self._lock_retry(session, retry_identity)
                existing = await self._find_retry(session, retry_identity)
                if existing is not None:
                    return self._replay(
                        existing,
                        expected_request=request_payload,
                        expected_request_hash=request_identity,
                    )

            response = await producer(self.repository_factory(session))
            try:
                row = self._row(
                    request_payload=request_payload,
                    request_identity=request_identity,
                    retry_identity=retry_identity,
                    response=response,
                )
            except (ForecastRunValidationError, ValueError, TypeError) as exc:
                raise AppError(
                    "Forecast output could not be archived safely.",
                    code="forecast_archive_validation_failed",
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                ) from exc
            session.add(row)
            # The response is not released until the insert, trigger, hashes,
            # FK, and privilege boundary have all succeeded and the surrounding
            # context commits.
            await session.flush()
            return response

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
    ) -> ForecastRun | None:
        result = await session.execute(
            select(ForecastRun).where(ForecastRun.idempotency_token_digest == retry_identity)
        )
        return result.scalars().one_or_none()

    def _replay(
        self,
        row: ForecastRun,
        *,
        expected_request: bytes,
        expected_request_hash: str,
    ) -> ForecastResponse:
        if row.request_hash != expected_request_hash:
            raise AppError(
                "Idempotency-Key was already used with a different forecast request.",
                code="idempotency_key_conflict",
                status_code=status.HTTP_409_CONFLICT,
            )
        try:
            stored_request = bytes(row.canonical_request)
            if stored_request != expected_request:
                raise ForecastRunValidationError(
                    "stored request bytes do not match their retry request"
                )
            if request_hash(stored_request) != row.request_hash:
                raise ForecastRunValidationError("stored request hash is invalid")
            if parse_request(stored_request) != parse_request(expected_request):
                raise ForecastRunValidationError("stored request semantics are invalid")

            stored_output = bytes(row.canonical_output)
            response = parse_output(stored_output)
            if output_hash(stored_output) != row.output_hash:
                raise ForecastRunValidationError("stored output hash is invalid")
            _validate_request_response(parse_request(stored_request), response)
            self._validate_headers(row, response)
            return response
        except (ForecastRunValidationError, ValueError, TypeError) as exc:
            raise AppError(
                "Persisted forecast replay evidence failed validation.",
                code="forecast_archive_corrupt",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                details={"forecast_id": str(row.forecast_id)},
            ) from exc

    def _validate_headers(self, row: ForecastRun, response: ForecastResponse) -> None:
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
