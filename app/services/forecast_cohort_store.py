"""Two-transaction PostgreSQL persistence for forecast-cohort evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from fastapi import status
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import AppError
from app.db.models.forecast_evidence import (
    ForecastOutcomeCohortAvailability,
    ForecastOutcomeCohortManifest,
)
from app.services.forecast_cohorts import (
    ForecastCohortManifest,
    ForecastCohortRecord,
    ForecastCohortSeal,
    ForecastCohortValidationError,
    canonical_cohort_manifest,
    cohort_id_for_manifest,
    parse_cohort_manifest,
    validate_cohort_record,
    validate_cohort_seal,
)

_MANIFEST_PRIMARY_KEY = "pk_forecast_outcome_cohort_manifests"
_SEAL_PRIMARY_KEY = "pk_forecast_outcome_cohort_availability"
_MANIFEST_TIME_ORDER_CHECK = "ck_forecast_outcome_cohort_manifests_time_order"


@dataclass(frozen=True)
class ForecastCohortProof:
    """Detached, independently validated manifest and availability evidence."""

    manifest: ForecastCohortManifest
    record: ForecastCohortRecord
    seal: ForecastCohortSeal


@runtime_checkable
class ForecastCohortStore(Protocol):
    """Persist and seal one explicit, content-addressed cohort manifest."""

    async def publish(self, manifest: ForecastCohortManifest) -> ForecastCohortProof: ...


@dataclass(frozen=True)
class _PreparedManifest:
    manifest: ForecastCohortManifest
    canonical: bytes
    cohort_id: str


@dataclass(frozen=True)
class SqlForecastCohortStore:
    """Create a manifest and its post-commit seal in distinct transactions."""

    sessionmaker: async_sessionmaker[AsyncSession]

    async def publish(self, manifest: ForecastCohortManifest) -> ForecastCohortProof:
        prepared = _prepare(manifest)
        record = await self._ensure_manifest(prepared)
        await self._ensure_seal(prepared, record)
        return await self._read_proof(prepared)

    async def _ensure_manifest(self, prepared: _PreparedManifest) -> ForecastCohortRecord:
        commit_pending = False
        candidate_record: ForecastCohortRecord | None = None
        try:
            async with self.sessionmaker() as session, session.begin():
                existing = await session.get(
                    ForecastOutcomeCohortManifest,
                    prepared.cohort_id,
                )
                if existing is not None:
                    return _exact_record(existing, prepared)
                candidate = _manifest_candidate(prepared)
                session.add(candidate)
                await session.flush()
                await session.refresh(candidate)
                candidate_record = _exact_record(candidate, prepared)
                commit_pending = True
            if candidate_record is None:  # pragma: no cover - construction invariant
                raise RuntimeError("manifest persistence produced no record")
            return candidate_record
        except IntegrityError as exc:
            if _is_duplicate(exc, _MANIFEST_PRIMARY_KEY):
                winner = await self._try_record(prepared)
                if winner is not None:
                    return winner
                raise _write_conflict("manifest") from exc
            raise _integrity_error(exc, "manifest") from exc
        except SQLAlchemyTimeoutError as exc:
            if commit_pending:
                return await self._reconcile_manifest_or_unknown(prepared, exc)
            raise _unavailable() from exc
        except DBAPIError as exc:
            if _is_integrity_state(exc):
                raise _integrity_error(exc, "manifest") from exc
            if _is_configuration_error(exc):
                raise _configuration_invalid() from exc
            if _is_known_rollback(exc):
                raise _unavailable() from exc
            if commit_pending:
                return await self._reconcile_manifest_or_unknown(prepared, exc)
            raise _database_error(exc) from exc

    async def _reconcile_manifest_or_unknown(
        self,
        prepared: _PreparedManifest,
        cause: BaseException,
    ) -> ForecastCohortRecord:
        record = await self._try_record(prepared)
        if record is not None:
            return record
        raise _commit_unknown("manifest") from cause

    async def _ensure_seal(
        self,
        prepared: _PreparedManifest,
        record: ForecastCohortRecord,
    ) -> ForecastCohortSeal:
        commit_pending = False
        candidate_seal: ForecastCohortSeal | None = None
        try:
            async with self.sessionmaker() as session, session.begin():
                existing = await session.get(
                    ForecastOutcomeCohortAvailability,
                    prepared.cohort_id,
                )
                if existing is not None:
                    return _exact_seal(existing, record)
                candidate = _seal_candidate(record)
                session.add(candidate)
                await session.flush()
                await session.refresh(candidate)
                candidate_seal = _exact_seal(candidate, record)
                commit_pending = True
            if candidate_seal is None:  # pragma: no cover - construction invariant
                raise RuntimeError("cohort sealing produced no receipt")
            return candidate_seal
        except IntegrityError as exc:
            if _is_duplicate(exc, _SEAL_PRIMARY_KEY):
                winner = await self._try_seal(record)
                if winner is not None:
                    return winner
                raise _write_conflict("seal") from exc
            raise _integrity_error(exc, "seal") from exc
        except SQLAlchemyTimeoutError as exc:
            if commit_pending:
                return await self._reconcile_seal_or_unknown(record, exc)
            raise _unavailable() from exc
        except DBAPIError as exc:
            if _is_integrity_state(exc):
                raise _integrity_error(exc, "seal") from exc
            if _is_configuration_error(exc):
                raise _configuration_invalid() from exc
            if _is_known_rollback(exc):
                raise _unavailable() from exc
            if commit_pending:
                return await self._reconcile_seal_or_unknown(record, exc)
            raise _database_error(exc) from exc

    async def _reconcile_seal_or_unknown(
        self,
        record: ForecastCohortRecord,
        cause: BaseException,
    ) -> ForecastCohortSeal:
        seal = await self._try_seal(record)
        if seal is not None:
            return seal
        raise _commit_unknown("seal") from cause

    async def _try_record(self, prepared: _PreparedManifest) -> ForecastCohortRecord | None:
        try:
            async with self.sessionmaker() as session:
                row = await session.get(ForecastOutcomeCohortManifest, prepared.cohort_id)
        except (DBAPIError, SQLAlchemyTimeoutError):
            return None
        return None if row is None else _exact_record(row, prepared)

    async def _try_seal(self, record: ForecastCohortRecord) -> ForecastCohortSeal | None:
        try:
            async with self.sessionmaker() as session:
                row = await session.get(ForecastOutcomeCohortAvailability, record.cohort_id)
        except (DBAPIError, SQLAlchemyTimeoutError):
            return None
        return None if row is None else _exact_seal(row, record)

    async def _read_proof(self, prepared: _PreparedManifest) -> ForecastCohortProof:
        try:
            async with self.sessionmaker() as session:
                manifest_row = await session.get(
                    ForecastOutcomeCohortManifest,
                    prepared.cohort_id,
                )
                seal_row = await session.get(
                    ForecastOutcomeCohortAvailability,
                    prepared.cohort_id,
                )
        except SQLAlchemyTimeoutError as exc:
            raise _unavailable() from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc
        if manifest_row is None or seal_row is None:
            raise AppError(
                "Forecast cohort evidence is incomplete.",
                code="forecast_cohort_evidence_incomplete",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                details={"retryable": True},
            )
        record = _exact_record(manifest_row, prepared)
        seal = _exact_seal(seal_row, record)
        return ForecastCohortProof(manifest=prepared.manifest, record=record, seal=seal)


def _prepare(manifest: ForecastCohortManifest) -> _PreparedManifest:
    try:
        canonical = canonical_cohort_manifest(manifest)
        normalized = parse_cohort_manifest(canonical)
        return _PreparedManifest(
            manifest=normalized,
            canonical=canonical,
            cohort_id=cohort_id_for_manifest(canonical),
        )
    except (ForecastCohortValidationError, TypeError, ValueError) as exc:
        raise AppError(
            "Forecast cohort manifest is invalid.",
            code="forecast_cohort_manifest_invalid",
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            details={"retryable": False},
        ) from exc


def _manifest_candidate(prepared: _PreparedManifest) -> ForecastOutcomeCohortManifest:
    manifest = prepared.manifest
    earliest = manifest.members[0].target_time
    latest = max(member.target_time for member in manifest.members)
    return ForecastOutcomeCohortManifest(
        cohort_id=prepared.cohort_id,
        schema_version=manifest.schema_version,
        selection_policy_hash=manifest.selection_policy_hash,
        outcome_resolution_policy_hash=manifest.outcome_resolution_policy_hash,
        availability_rule_set_hash=manifest.availability_rule_set_hash,
        purpose=manifest.purpose,
        member_count=len(manifest.members),
        earliest_target_time=earliest,
        latest_target_time=latest,
        # Invalid sentinels make a missing stamp trigger fail closed.
        recorded_at=earliest,
        creator_xid=0,
        canonical_manifest=prepared.canonical,
    )


def _seal_candidate(record: ForecastCohortRecord) -> ForecastOutcomeCohortAvailability:
    return ForecastOutcomeCohortAvailability(
        cohort_id=record.cohort_id,
        manifest_recorded_at=record.recorded_at,
        sealed_at=record.earliest_target_time,
        sealer_xid=0,
    )


def _exact_record(
    row: ForecastOutcomeCohortManifest,
    prepared: _PreparedManifest,
) -> ForecastCohortRecord:
    record = ForecastCohortRecord(
        cohort_id=row.cohort_id,
        schema_version=row.schema_version,
        purpose=row.purpose,  # type: ignore[arg-type]
        selection_policy_hash=row.selection_policy_hash,
        outcome_resolution_policy_hash=row.outcome_resolution_policy_hash,
        availability_rule_set_hash=row.availability_rule_set_hash,
        member_count=row.member_count,
        earliest_target_time=row.earliest_target_time,
        latest_target_time=row.latest_target_time,
        recorded_at=row.recorded_at,
        creator_xid=row.creator_xid,
        canonical_manifest=row.canonical_manifest,
    )
    try:
        stored = validate_cohort_record(record)
        if record.cohort_id != prepared.cohort_id:
            raise ForecastCohortValidationError("stored cohort identity differs")
        if record.canonical_manifest != prepared.canonical or stored != prepared.manifest:
            raise ForecastCohortValidationError("stored cohort content differs")
    except (ForecastCohortValidationError, TypeError, ValueError) as exc:
        raise _corrupt() from exc
    return record


def _exact_seal(
    row: ForecastOutcomeCohortAvailability,
    record: ForecastCohortRecord,
) -> ForecastCohortSeal:
    seal = ForecastCohortSeal(
        cohort_id=row.cohort_id,
        manifest_recorded_at=row.manifest_recorded_at,
        sealed_at=row.sealed_at,
        sealer_xid=row.sealer_xid,
    )
    try:
        validate_cohort_seal(record, seal)
    except (ForecastCohortValidationError, TypeError, ValueError) as exc:
        raise _corrupt() from exc
    return seal


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


def _is_duplicate(exc: IntegrityError, primary_key: str) -> bool:
    constraint = _db_error_attribute(exc, "constraint_name")
    # Reconcile only the exact content-addressed row race.  A bare 23505 could
    # instead come from materialized cohort-member uniqueness and must fail as
    # deterministic integrity corruption, not masquerade as a retryable race.
    return constraint == primary_key


def _is_configuration_error(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and sqlstate.startswith(("0A", "22", "28", "3D", "3F", "42"))


def _is_integrity_state(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and (sqlstate.startswith("23") or sqlstate == "55000")


def _is_known_rollback(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and (sqlstate.startswith("40") or sqlstate == "57014")


def _integrity_error(exc: DBAPIError, stage: str) -> AppError:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    constraint = _db_error_attribute(exc, "constraint_name")
    if sqlstate == "23514" and stage == "manifest" and constraint == _MANIFEST_TIME_ORDER_CHECK:
        return AppError(
            "The forecast cohort can no longer be recorded before its first target.",
            code="forecast_cohort_deadline_expired",
            status_code=status.HTTP_409_CONFLICT,
            details={"retryable": False, "stage": stage},
        )
    if sqlstate == "23503" and stage == "manifest":
        return AppError(
            "A scheduled forecast referenced by the cohort is unavailable.",
            code="forecast_cohort_source_unavailable",
            status_code=status.HTTP_409_CONFLICT,
            details={"retryable": False, "stage": stage},
        )
    if sqlstate == "55000" and stage == "seal":
        # _ensure_seal always opens a new session only after _ensure_manifest's
        # context committed. The live gate separately proves the trigger's
        # same-transaction 55000 branch, so a 55000 reachable here is the
        # pre-target deadline branch unless that transaction boundary regresses.
        return AppError(
            "The forecast cohort can no longer be sealed before its first target.",
            code="forecast_cohort_deadline_expired",
            status_code=status.HTTP_409_CONFLICT,
            details={"retryable": False, "stage": stage},
        )
    return AppError(
        "Forecast cohort database integrity validation failed.",
        code="forecast_cohort_integrity_failed",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False, "stage": stage},
    )


def _database_error(exc: DBAPIError) -> AppError:
    return _configuration_invalid() if _is_configuration_error(exc) else _unavailable()


def _unavailable() -> AppError:
    return AppError(
        "Forecast cohort storage is unavailable.",
        code="forecast_cohort_store_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _configuration_invalid() -> AppError:
    return AppError(
        "Forecast cohort database configuration is invalid.",
        code="forecast_cohort_configuration_invalid",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _write_conflict(stage: str) -> AppError:
    return AppError(
        "Concurrent forecast cohort evidence could not be reconciled.",
        code="forecast_cohort_write_conflict",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True, "stage": stage},
    )


def _commit_unknown(stage: str) -> AppError:
    return AppError(
        "Forecast cohort commit outcome is unknown.",
        code="forecast_cohort_commit_unknown",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"outcome_unknown": True, "retryable": True, "stage": stage},
    )


def _corrupt() -> AppError:
    return AppError(
        "Persisted forecast cohort evidence failed validation.",
        code="forecast_cohort_evidence_corrupt",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


__all__ = ["ForecastCohortProof", "ForecastCohortStore", "SqlForecastCohortStore"]
