"""Append-only PostgreSQL persistence for realized forecast outcomes."""

from __future__ import annotations

import hmac
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from fastapi import status
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import AppError
from app.db.models.forecast_evidence import (
    ForecastRealizedOutcome,
    ForecastRealizedOutcomePublication,
)
from app.services.forecast_outcomes import (
    OutcomeValidationError,
    RealizedOutcomePayload,
    RealizedOutcomeRecord,
    canonical_outcome_payload,
    outcome_id_for_payload,
    parse_outcome_payload,
    validate_outcome_record,
)
from ingestion.locks import acquire_advisory_xact_lock, bar_series_lock_id

_OUTCOME_PRIMARY_KEY = "pk_forecast_realized_outcomes"
_SEMANTIC_UNIQUE = "uq_forecast_realized_outcomes_semantic_key"
_TIME_ORDER_CHECK = "ck_forecast_realized_outcomes_evidence_time_order"
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class ForecastOutcomeProof:
    """Detached realized-outcome evidence validated against trusted policies."""

    payload: RealizedOutcomePayload
    record: RealizedOutcomeRecord
    publication: ForecastOutcomePublicationRecord


@dataclass(frozen=True)
class ForecastOutcomePublicationRecord:
    """Detached database proof of the cohort member authorizing publication."""

    outcome_id: str
    cohort_id: str
    forecast_id: UUID
    step: int
    published_at: datetime
    publisher_xid: int


@dataclass(frozen=True)
class ForecastOutcomePublicationSource:
    """One exact precommitted cohort member authorizing publication."""

    cohort_id: str
    forecast_id: UUID
    step: int


@runtime_checkable
class ForecastOutcomeStore(Protocol):
    """Persist one policy-bound realized outcome, or replay its exact row."""

    async def publish(
        self,
        payload: RealizedOutcomePayload,
        *,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof: ...


@dataclass(frozen=True)
class _SemanticKey:
    outcome_resolution_policy_hash: str
    availability_rule_set_hash: str
    symbol: str
    target: str
    series_basis: str
    target_time: datetime


@dataclass(frozen=True)
class _PreparedOutcome:
    payload: RealizedOutcomePayload
    canonical: bytes
    outcome_id: str
    semantic_key: _SemanticKey


@dataclass(frozen=True)
class SqlForecastOutcomeStore:
    """Short-transaction outcome store with exact race reconciliation."""

    sessionmaker: async_sessionmaker[AsyncSession]
    outcome_resolution_policy_hash: str
    availability_rule_set_hash: str

    async def publish(
        self,
        payload: RealizedOutcomePayload,
        *,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof:
        prepared = _prepare(
            payload,
            expected_policy_hash=self.outcome_resolution_policy_hash,
            expected_rule_set_hash=self.availability_rule_set_hash,
        )
        publication_source = _publication_source(source)
        existing = await self._preflight(prepared, publication_source)
        if existing is not None:
            return existing
        return await self._commit(prepared, publication_source)

    async def _preflight(
        self,
        prepared: _PreparedOutcome,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof | None:
        try:
            async with self.sessionmaker() as session:
                return await self._find_prepared(session, prepared, source)
        except SQLAlchemyTimeoutError as exc:
            raise _unavailable() from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc

    async def _commit(
        self,
        prepared: _PreparedOutcome,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof:
        commit_pending = False
        try:
            async with self.sessionmaker() as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                await acquire_advisory_xact_lock(
                    session,
                    bar_series_lock_id(
                        prepared.payload.symbol,
                        prepared.payload.source_version.source,
                        prepared.payload.source_version.timespan,
                    ),
                )
                existing = await self._find_prepared(session, prepared, source)
                if existing is not None:
                    return existing
                result = await session.execute(
                    text(
                        "SELECT public.publish_forecast_realized_outcome("
                        ":cohort_id, :forecast_id, :forecast_step, "
                        ":outcome_id, :canonical_evidence)"
                    ),
                    {
                        "cohort_id": source.cohort_id,
                        "forecast_id": source.forecast_id,
                        "forecast_step": source.step,
                        "outcome_id": prepared.outcome_id,
                        "canonical_evidence": prepared.canonical,
                    },
                )
                if result.scalar_one() != prepared.outcome_id:
                    raise _corrupt()
                candidate = await session.get(
                    ForecastRealizedOutcome,
                    prepared.outcome_id,
                )
                if candidate is None:
                    raise _corrupt()
                publication = await session.get(
                    ForecastRealizedOutcomePublication,
                    _publication_identity(prepared.outcome_id, source),
                )
                if publication is None:
                    raise _corrupt()
                self._exact(candidate, publication, prepared, source)
                commit_pending = True
            return await self._read_committed(prepared, source)
        except IntegrityError as exc:
            if _is_race(exc):
                winner = await self._try_reconcile(prepared, source)
                if winner is not None:
                    return winner
                raise _write_conflict() from exc
            raise _integrity_error(exc) from exc
        except SQLAlchemyTimeoutError as exc:
            if commit_pending:
                return await self._reconcile_or_unknown(prepared, source, exc)
            raise _unavailable() from exc
        except DBAPIError as exc:
            if _is_integrity_state(exc):
                raise _integrity_error(exc) from exc
            if _is_configuration_error(exc):
                raise _configuration_invalid() from exc
            if _is_statement_completion_unknown(exc):
                return await self._reconcile_or_unknown(prepared, source, exc)
            if _is_known_rollback(exc):
                raise _unavailable() from exc
            if commit_pending:
                return await self._reconcile_or_unknown(prepared, source, exc)
            raise _database_error(exc) from exc

    async def _read_committed(
        self,
        prepared: _PreparedOutcome,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof:
        """Reread from a fresh session after the successful commit boundary."""

        try:
            async with self.sessionmaker() as session:
                row = await session.get(ForecastRealizedOutcome, prepared.outcome_id)
                publication = await session.get(
                    ForecastRealizedOutcomePublication,
                    _publication_identity(prepared.outcome_id, source),
                )
        except SQLAlchemyTimeoutError as exc:
            raise _unavailable() from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc
        if row is None:
            raise _unavailable()
        if publication is None:
            raise _corrupt()
        return self._exact(row, publication, prepared, source)

    async def _reconcile_or_unknown(
        self,
        prepared: _PreparedOutcome,
        source: ForecastOutcomePublicationSource,
        cause: BaseException,
    ) -> ForecastOutcomeProof:
        proof = await self._try_reconcile(prepared, source)
        if proof is not None:
            return proof
        raise _commit_unknown() from cause

    async def _try_reconcile(
        self,
        prepared: _PreparedOutcome,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof | None:
        """Reconcile only a content/semantic row with the exact provenance link."""

        try:
            async with self.sessionmaker() as session:
                row = await session.get(ForecastRealizedOutcome, prepared.outcome_id)
                if row is not None:
                    return await self._proof_if_published(
                        session,
                        row,
                        prepared,
                        source,
                    )
                row = await _find_semantic(session, prepared.semantic_key)
                if row is None:
                    return None
                return await self._proof_if_published(session, row, prepared, source)
        except (DBAPIError, SQLAlchemyTimeoutError):
            return None

    async def _find_prepared(
        self,
        session: AsyncSession,
        prepared: _PreparedOutcome,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof | None:
        row = await session.get(ForecastRealizedOutcome, prepared.outcome_id)
        if row is not None:
            return await self._proof_if_published(session, row, prepared, source)
        row = await _find_semantic(session, prepared.semantic_key)
        if row is None:
            return None
        return await self._proof_if_published(session, row, prepared, source)

    async def _proof_if_published(
        self,
        session: AsyncSession,
        row: ForecastRealizedOutcome,
        prepared: _PreparedOutcome,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof | None:
        payload, record = self._validated_outcome(row, prepared)
        publication = await session.get(
            ForecastRealizedOutcomePublication,
            _publication_identity(prepared.outcome_id, source),
        )
        if publication is None:
            # An outcome authorized by another cohort is valid truth, but it is
            # not proof for this caller. Re-enter the DB publisher so it can
            # validate and append the requested provenance link atomically.
            return None
        return _proof(payload, record, publication, source)

    def _exact(
        self,
        row: ForecastRealizedOutcome,
        publication: ForecastRealizedOutcomePublication,
        prepared: _PreparedOutcome,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof:
        payload, record = self._validated_outcome(row, prepared)
        return _proof(payload, record, publication, source)

    def _validated_outcome(
        self,
        row: ForecastRealizedOutcome,
        prepared: _PreparedOutcome,
    ) -> tuple[RealizedOutcomePayload, RealizedOutcomeRecord]:
        try:
            record = _detach(row)
            payload = validate_outcome_record(
                record,
                expected_outcome_resolution_policy_hash=(self.outcome_resolution_policy_hash),
                expected_availability_rule_set_hash=self.availability_rule_set_hash,
            )
        except (AttributeError, OutcomeValidationError, TypeError, ValueError) as exc:
            raise _corrupt() from exc
        if (
            record.outcome_id == prepared.outcome_id
            and record.canonical_evidence == prepared.canonical
            and payload == prepared.payload
        ):
            return payload, record
        if _semantic_key(payload) == prepared.semantic_key:
            raise _semantic_conflict()
        # A content-ID lookup returning other valid semantics would imply either
        # a broken digest invariant or inconsistent database evidence.
        raise _corrupt()


def _prepare(
    payload: RealizedOutcomePayload,
    *,
    expected_policy_hash: str,
    expected_rule_set_hash: str,
) -> _PreparedOutcome:
    if (
        not isinstance(expected_policy_hash, str)
        or _HASH_PATTERN.fullmatch(expected_policy_hash) is None
        or not isinstance(expected_rule_set_hash, str)
        or _HASH_PATTERN.fullmatch(expected_rule_set_hash) is None
    ):
        raise _configuration_invalid()
    try:
        canonical = canonical_outcome_payload(payload)
        normalized = parse_outcome_payload(canonical)
        outcome_id = outcome_id_for_payload(canonical)
    except (OutcomeValidationError, TypeError, ValueError) as exc:
        raise AppError(
            "Realized forecast outcome is invalid.",
            code="forecast_outcome_invalid",
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            details={"retryable": False},
        ) from exc
    if not hmac.compare_digest(
        normalized.outcome_resolution_policy_hash,
        expected_policy_hash,
    ) or not hmac.compare_digest(
        normalized.availability_rule_set_hash,
        expected_rule_set_hash,
    ):
        raise AppError(
            "Realized forecast outcome does not match the configured policies.",
            code="forecast_outcome_policy_mismatch",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={"retryable": False},
        )
    return _PreparedOutcome(
        payload=normalized,
        canonical=canonical,
        outcome_id=outcome_id,
        semantic_key=_semantic_key(normalized),
    )


def _semantic_key(payload: RealizedOutcomePayload) -> _SemanticKey:
    return _SemanticKey(
        outcome_resolution_policy_hash=payload.outcome_resolution_policy_hash,
        availability_rule_set_hash=payload.availability_rule_set_hash,
        symbol=payload.symbol,
        target=payload.target,
        series_basis=payload.series_basis,
        target_time=payload.target_time,
    )


def _publication_source(
    source: ForecastOutcomePublicationSource,
) -> ForecastOutcomePublicationSource:
    if (
        not isinstance(source, ForecastOutcomePublicationSource)
        or _HASH_PATTERN.fullmatch(source.cohort_id) is None
        or not isinstance(source.forecast_id, UUID)
        or type(source.step) is not int
        or not 1 <= source.step <= 252
    ):
        raise AppError(
            "Realized forecast outcome publication source is invalid.",
            code="forecast_outcome_publication_source_invalid",
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            details={"retryable": False},
        )
    return source


def _publication_identity(
    outcome_id: str,
    source: ForecastOutcomePublicationSource,
) -> tuple[str, str, UUID, int]:
    return outcome_id, source.cohort_id, source.forecast_id, source.step


def _proof(
    payload: RealizedOutcomePayload,
    record: RealizedOutcomeRecord,
    row: ForecastRealizedOutcomePublication,
    source: ForecastOutcomePublicationSource,
) -> ForecastOutcomeProof:
    try:
        publication = ForecastOutcomePublicationRecord(
            outcome_id=row.outcome_id,
            cohort_id=row.cohort_id,
            forecast_id=row.forecast_id,
            step=row.step,
            published_at=row.published_at,
            publisher_xid=row.publisher_xid,
        )
        if (
            not isinstance(publication.outcome_id, str)
            or publication.outcome_id != record.outcome_id
            or not isinstance(publication.cohort_id, str)
            or publication.cohort_id != source.cohort_id
            or not isinstance(publication.forecast_id, UUID)
            or publication.forecast_id != source.forecast_id
            or type(publication.step) is not int
            or publication.step != source.step
            or type(publication.publisher_xid) is not int
            or publication.publisher_xid <= 0
            or publication.published_at.tzinfo is None
            or publication.published_at.utcoffset() is None
            or publication.published_at < record.sealed_at
        ):
            raise ValueError("publication evidence does not match its source")
    except (AttributeError, TypeError, ValueError) as exc:
        raise _corrupt() from exc
    return ForecastOutcomeProof(
        payload=payload,
        record=record,
        publication=publication,
    )


async def _find_semantic(
    session: AsyncSession,
    key: _SemanticKey,
) -> ForecastRealizedOutcome | None:
    result = await session.execute(
        select(ForecastRealizedOutcome).where(
            ForecastRealizedOutcome.outcome_resolution_policy_hash
            == key.outcome_resolution_policy_hash,
            ForecastRealizedOutcome.availability_rule_set_hash == key.availability_rule_set_hash,
            ForecastRealizedOutcome.symbol == key.symbol,
            ForecastRealizedOutcome.target == key.target,
            ForecastRealizedOutcome.series_basis == key.series_basis,
            ForecastRealizedOutcome.target_time == key.target_time,
        )
    )
    return result.scalars().one_or_none()


def _detach(row: ForecastRealizedOutcome) -> RealizedOutcomeRecord:
    return RealizedOutcomeRecord(
        outcome_id=row.outcome_id,
        schema_version=row.schema_version,
        outcome_resolution_policy_hash=row.outcome_resolution_policy_hash,
        availability_rule_set_hash=row.availability_rule_set_hash,
        symbol=row.symbol,
        target=row.target,
        series_basis=row.series_basis,
        target_time=row.target_time,
        currency=row.currency,
        resolution_cutoff=row.resolution_cutoff,
        bar_timespan=row.bar_timespan,
        bar_multiplier=row.bar_multiplier,
        bar_observed_at=row.bar_observed_at,
        bar_source=row.bar_source,
        bar_adjustment_basis=row.bar_adjustment_basis,
        bar_version_recorded_at=row.bar_version_recorded_at,
        bar_fetched_at=row.bar_fetched_at,
        bar_source_as_of=row.bar_source_as_of,
        bar_available_at=row.bar_available_at,
        bar_field=row.bar_field,
        bar_value=row.bar_value,
        realized_value=row.realized_value,
        sealed_at=row.sealed_at,
        canonical_evidence=bytes(row.canonical_evidence),
    )


def _db_error_attribute(exc: DBAPIError, name: str) -> str | None:
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
            (getattr(candidate, "__cause__", None), getattr(candidate, "__context__", None))
        )
    return None


def _is_race(exc: IntegrityError) -> bool:
    if _db_error_attribute(exc, "sqlstate") != "23505":
        return False
    constraint = _db_error_attribute(exc, "constraint_name")
    return constraint in {_OUTCOME_PRIMARY_KEY, _SEMANTIC_UNIQUE}


def _is_configuration_error(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and sqlstate.startswith(("0A", "22", "28", "3D", "3F", "42"))


def _is_integrity_state(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and (sqlstate.startswith("23") or sqlstate == "55000")


def _is_known_rollback(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and (
        (sqlstate.startswith("40") and sqlstate != "40003") or sqlstate == "57014"
    )


def _is_statement_completion_unknown(exc: DBAPIError) -> bool:
    return _db_error_attribute(exc, "sqlstate") == "40003"


def _integrity_error(exc: DBAPIError) -> AppError:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    constraint = _db_error_attribute(exc, "constraint_name")
    if sqlstate == "23514" and constraint == _TIME_ORDER_CHECK:
        return AppError(
            "Realized forecast outcome is not ready at its resolution cutoff.",
            code="forecast_outcome_resolution_not_ready",
            status_code=status.HTTP_409_CONFLICT,
            details={"retryable": True},
        )
    if sqlstate == "23503":
        return AppError(
            "The exact bar evidence for this realized outcome is unavailable.",
            code="forecast_outcome_source_unavailable",
            status_code=status.HTTP_409_CONFLICT,
            details={"retryable": False},
        )
    return AppError(
        "Realized forecast outcome database integrity validation failed.",
        code="forecast_outcome_integrity_failed",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _database_error(exc: DBAPIError) -> AppError:
    return _configuration_invalid() if _is_configuration_error(exc) else _unavailable()


def _unavailable() -> AppError:
    return AppError(
        "Realized forecast outcome storage is unavailable.",
        code="forecast_outcome_store_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _configuration_invalid() -> AppError:
    return AppError(
        "Realized forecast outcome database configuration is invalid.",
        code="forecast_outcome_configuration_invalid",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _semantic_conflict() -> AppError:
    return AppError(
        "A different realized outcome already occupies this semantic identity.",
        code="forecast_outcome_semantic_conflict",
        status_code=status.HTTP_409_CONFLICT,
        details={"retryable": False},
    )


def _write_conflict() -> AppError:
    return AppError(
        "Concurrent realized outcome evidence could not be reconciled.",
        code="forecast_outcome_write_conflict",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _commit_unknown() -> AppError:
    return AppError(
        "Realized forecast outcome commit status is unknown.",
        code="forecast_outcome_commit_unknown",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"outcome_unknown": True, "retryable": True},
    )


def _corrupt() -> AppError:
    return AppError(
        "Persisted realized forecast outcome evidence failed validation.",
        code="forecast_outcome_evidence_corrupt",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


__all__ = [
    "ForecastOutcomeProof",
    "ForecastOutcomePublicationRecord",
    "ForecastOutcomePublicationSource",
    "ForecastOutcomeStore",
    "SqlForecastOutcomeStore",
]
