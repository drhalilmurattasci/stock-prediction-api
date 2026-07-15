"""Append-only persistence for fitted calibration and descriptive coverage.

The store deliberately publishes *evidence*, not a serving decision.  A
successful release proves that exact fitted-set bytes and a descriptive
held-out measurement are durable and reproducible from the immutable forecast
cohort/outcome ledger.  It contains no acceptance threshold or activation
lookup; those belong to a later, prospectively committed policy artifact.
"""

from __future__ import annotations

import hmac
import re
import struct
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import partial
from typing import Protocol, runtime_checkable

import anyio
from fastapi import status
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import AppError
from app.db.models.forecast_calibration import (
    ForecastFittedCalibrationSet,
    ForecastHeldoutCoverageRelease,
    ForecastHeldoutCoverageReleaseAvailability,
    ForecastHeldoutCoverageReleaseBucket,
)
from app.services.forecast_calibration_evidence import (
    CalibrationEvidenceSet,
    ForecastCalibrationEvidenceError,
    estimate_heldout_coverage,
)
from app.services.forecast_calibration_evidence_store import (
    SqlForecastCalibrationEvidenceReader,
)
from app.services.forecast_calibration_releases import (
    HELDOUT_COVERAGE_RELEASE_SCHEMA_VERSION,
    HELDOUT_COVERAGE_RELEASE_SCOPE,
    ForecastCalibrationReleaseValidationError,
    HeldoutCoverageRelease,
    build_heldout_coverage_release,
    heldout_coverage_release_id_for,
    parse_heldout_coverage_release,
)
from app.services.forecast_calibration_sets import (
    FittedCalibrationSet,
    ForecastCalibrationSetValidationError,
    calibration_set_version_for,
    canonical_calibration_set,
    parse_calibration_set,
)

_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_READ_SNAPSHOT = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
_SET_PRIMARY_KEY = "pk_forecast_fitted_calibration_sets"
_RELEASE_PRIMARY_KEY = "pk_forecast_heldout_coverage_releases"
_RECEIPT_PRIMARY_KEY = "pk_forecast_heldout_coverage_release_availability"


@dataclass(frozen=True, slots=True)
class FittedCalibrationSetRecord:
    calibration_set_version: str
    schema_version: int
    model_version: str
    symbol: str
    target: str
    series_basis: str
    horizon_unit: str
    currency: str
    source_calibration_set_version: str
    source_calibration_method: str
    forecast_resolution_policy_hash: str
    forecast_availability_rule_set_hash: str
    fit_evidence_digest: str
    method: str
    window_start: date
    window_end: date
    sample_count: int
    cohort_id: str
    selection_policy_hash: str
    outcome_resolution_policy_hash: str
    outcome_availability_rule_set_hash: str
    interval_policy_version: str
    window_date_policy_version: str
    bucket_count: int
    recorded_at: datetime
    creator_xid: int
    canonical_set: bytes


@dataclass(frozen=True, slots=True)
class HeldoutCoverageReleaseRecord:
    release_id: str
    schema_version: int
    evidence_scope: str
    fitted_calibration_set_version: str
    method: str
    model_version: str
    symbol: str
    target: str
    series_basis: str
    horizon_unit: str
    currency: str
    fit_cohort_id: str
    fit_selection_policy_hash: str
    heldout_cohort_id: str
    heldout_selection_policy_hash: str
    outcome_resolution_policy_hash: str
    outcome_availability_rule_set_hash: str
    forecast_resolution_policy_hash: str
    forecast_availability_rule_set_hash: str
    fit_evidence_digest: str
    heldout_evidence_digest: str
    heldout_window_start: date
    heldout_window_end: date
    heldout_sample_count: int
    confidence_level_f64_be: bytes
    interval_policy_version: str
    window_date_policy_version: str
    estimator_policy_version: str
    bucket_count: int
    recorded_at: datetime
    creator_xid: int
    canonical_release: bytes


@dataclass(frozen=True, slots=True)
class HeldoutCoverageReleaseBucketRecord:
    release_id: str
    horizon: int
    coverage_millis: int
    covered_count: int
    sample_count: int
    empirical_coverage_f64_be: bytes
    confidence_low_f64_be: bytes
    confidence_high_f64_be: bytes


@dataclass(frozen=True, slots=True)
class HeldoutCoverageReleaseAvailability:
    release_id: str
    release_recorded_at: datetime
    available_at: datetime
    sealer_xid: int


@dataclass(frozen=True, slots=True)
class HeldoutCoverageReleaseProof:
    """Fully revalidated, post-commit descriptive evidence."""

    release: HeldoutCoverageRelease
    set_record: FittedCalibrationSetRecord
    release_record: HeldoutCoverageReleaseRecord
    bucket_records: tuple[HeldoutCoverageReleaseBucketRecord, ...]
    availability: HeldoutCoverageReleaseAvailability


@runtime_checkable
class HeldoutCoverageReleaseStore(Protocol):
    async def publish(
        self,
        fitted_set: FittedCalibrationSet,
        *,
        heldout_cohort_id: str,
        confidence_level: float,
    ) -> HeldoutCoverageReleaseProof: ...

    async def read_validated(self, release_id: str) -> HeldoutCoverageReleaseProof: ...


@dataclass(frozen=True, slots=True)
class _PreparedRelease:
    release: HeldoutCoverageRelease
    canonical_set: bytes


@dataclass(frozen=True, slots=True)
class _StoredRelease:
    set_record: FittedCalibrationSetRecord
    release_record: HeldoutCoverageReleaseRecord
    bucket_records: tuple[HeldoutCoverageReleaseBucketRecord, ...]
    availability: HeldoutCoverageReleaseAvailability | None


@dataclass(frozen=True)
class SqlHeldoutCoverageReleaseStore:
    """Publish and replay immutable descriptive calibration evidence."""

    sessionmaker: async_sessionmaker[AsyncSession]

    async def publish(
        self,
        fitted_set: FittedCalibrationSet,
        *,
        heldout_cohort_id: str,
        confidence_level: float,
    ) -> HeldoutCoverageReleaseProof:
        # Cohort IDs are the only caller-supplied evidence references.  Reload
        # both immutable source proofs before opening any publisher transaction;
        # otherwise a stale or forged in-memory dataset could permanently
        # occupy the append-only release identity/semantic slot.
        reader = SqlForecastCalibrationEvidenceReader(self.sessionmaker)
        try:
            fit_dataset = await reader.read_validated(fitted_set.cohort_id)
            heldout_dataset = await reader.read_validated(heldout_cohort_id)
        except AppError:
            raise
        except (AttributeError, TypeError, ValueError) as exc:
            raise _invalid() from exc
        try:
            prepared = await anyio.to_thread.run_sync(
                partial(
                    _prepare_release,
                    fitted_set,
                    fit_dataset=fit_dataset,
                    heldout_dataset=heldout_dataset,
                    confidence_level=confidence_level,
                )
            )
        except (
            ForecastCalibrationEvidenceError,
            ForecastCalibrationReleaseValidationError,
            ForecastCalibrationSetValidationError,
            TypeError,
            ValueError,
        ) as exc:
            raise _invalid() from exc

        await self._publish_content(prepared)
        await self._publish_receipt(prepared.release.release_id)
        return await self.read_validated(prepared.release.release_id)

    async def read_validated(self, release_id: str) -> HeldoutCoverageReleaseProof:
        identity = _release_identity(release_id)
        try:
            stored = await self._read_stored(identity)
            proof = await anyio.to_thread.run_sync(_validate_stored, stored)
        except AppError:
            raise
        except SQLAlchemyTimeoutError as exc:
            raise _unavailable() from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc
        except (
            ForecastCalibrationReleaseValidationError,
            ForecastCalibrationSetValidationError,
            AttributeError,
            TypeError,
            ValueError,
        ) as exc:
            raise _corrupt() from exc

        # A syntactically valid release is not trusted merely because bytes are
        # present.  Reload both exact source cohorts and reproduce the estimator.
        reader = SqlForecastCalibrationEvidenceReader(self.sessionmaker)
        try:
            fit_dataset = await reader.read_validated(proof.set_record.cohort_id)
            heldout_dataset = await reader.read_validated(proof.release_record.heldout_cohort_id)
        except AppError as exc:
            if exc.code == "forecast_calibration_evidence_store_unavailable":
                raise _unavailable() from exc
            if exc.code == "forecast_calibration_evidence_configuration_invalid":
                raise _configuration_invalid() from exc
            raise _corrupt() from exc
        try:
            reproduced = await anyio.to_thread.run_sync(
                partial(
                    _reproduce_release,
                    proof.release.fitted_set,
                    fit_dataset=fit_dataset,
                    heldout_dataset=heldout_dataset,
                    confidence_level=proof.release.evidence.confidence_level,
                )
            )
        except (
            ForecastCalibrationEvidenceError,
            ForecastCalibrationReleaseValidationError,
            ForecastCalibrationSetValidationError,
            TypeError,
            ValueError,
        ) as exc:
            raise _corrupt() from exc
        if not hmac.compare_digest(
            reproduced.canonical_release,
            proof.release.canonical_release,
        ):
            raise _corrupt()
        return proof

    async def _publish_content(self, prepared: _PreparedRelease) -> None:
        commit_pending = False
        try:
            async with self.sessionmaker() as session, session.begin():
                set_id = (
                    await session.execute(
                        text("SELECT publish_fitted_calibration_set(:canonical_set)"),
                        {"canonical_set": prepared.canonical_set},
                    )
                ).scalar_one()
                release_id = (
                    await session.execute(
                        text(
                            "SELECT publish_forecast_heldout_coverage_release(:canonical_release)"
                        ),
                        {"canonical_release": prepared.release.canonical_release},
                    )
                ).scalar_one()
                if (
                    set_id != calibration_set_version_for(prepared.canonical_set)
                    or release_id != prepared.release.release_id
                ):
                    raise _StoredCorrupt("publisher returned a different content identity")
                commit_pending = True
        except (IntegrityError, DBAPIError) as exc:
            if _configuration_failure(exc):
                raise _database_error(exc) from exc
            if _deterministic_rejection(exc):
                raise _conflict() from exc
            if commit_pending or _completion_unknown(exc):
                if await self._content_matches(prepared):
                    return
                raise _outcome_unknown("content") from exc
            raise _database_error(exc) from exc
        except SQLAlchemyTimeoutError as exc:
            if commit_pending and await self._content_matches(prepared):
                return
            if commit_pending:
                raise _outcome_unknown("content") from exc
            raise _unavailable() from exc
        except _StoredCorrupt as exc:
            raise _corrupt() from exc

    async def _publish_receipt(self, release_id: str) -> None:
        commit_pending = False
        try:
            async with self.sessionmaker() as session, session.begin():
                returned = (
                    await session.execute(
                        text(
                            "SELECT release_id FROM "
                            "publish_forecast_heldout_coverage_release_receipt(:release_id)"
                        ),
                        {"release_id": release_id},
                    )
                ).scalar_one()
                if returned != release_id:
                    raise _StoredCorrupt("receipt publisher returned a different identity")
                commit_pending = True
        except (IntegrityError, DBAPIError) as exc:
            if _configuration_failure(exc):
                raise _database_error(exc) from exc
            if _deterministic_rejection(exc):
                raise _conflict() from exc
            if commit_pending or _completion_unknown(exc):
                if await self._receipt_visible(release_id):
                    return
                raise _outcome_unknown("receipt") from exc
            raise _database_error(exc) from exc
        except SQLAlchemyTimeoutError as exc:
            if commit_pending and await self._receipt_visible(release_id):
                return
            if commit_pending:
                raise _outcome_unknown("receipt") from exc
            raise _unavailable() from exc
        except _StoredCorrupt as exc:
            raise _corrupt() from exc

    async def _content_matches(self, prepared: _PreparedRelease) -> bool:
        try:
            stored = await self._read_stored(
                prepared.release.release_id,
                require_receipt=False,
            )
            proof = await anyio.to_thread.run_sync(
                partial(
                    _validate_stored,
                    stored,
                    abandon_on_missing_receipt=True,
                )
            )
        except (AppError, DBAPIError, SQLAlchemyTimeoutError, ValueError, TypeError):
            return False
        return hmac.compare_digest(
            proof.release.canonical_release,
            prepared.release.canonical_release,
        ) and hmac.compare_digest(proof.set_record.canonical_set, prepared.canonical_set)

    async def _receipt_visible(self, release_id: str) -> bool:
        try:
            stored = await self._read_stored(release_id)
        except (AppError, DBAPIError, SQLAlchemyTimeoutError):
            return False
        return stored.availability is not None

    async def _read_stored(
        self,
        release_id: str,
        *,
        require_receipt: bool = True,
    ) -> _StoredRelease:
        async with self.sessionmaker() as session, session.begin():
            await session.execute(text(_READ_SNAPSHOT))
            release_row = await session.get(ForecastHeldoutCoverageRelease, release_id)
            if release_row is None:
                raise _missing(release_id)
            set_row = await session.get(
                ForecastFittedCalibrationSet,
                release_row.fitted_calibration_set_version,
            )
            bucket_result = await session.execute(
                select(ForecastHeldoutCoverageReleaseBucket)
                .where(ForecastHeldoutCoverageReleaseBucket.release_id == release_id)
                .order_by(
                    ForecastHeldoutCoverageReleaseBucket.horizon,
                    ForecastHeldoutCoverageReleaseBucket.coverage_millis,
                )
            )
            bucket_rows = tuple(bucket_result.scalars().all())
            receipt_row = await session.get(
                ForecastHeldoutCoverageReleaseAvailability,
                release_id,
            )
            if set_row is None:
                raise _StoredCorrupt("release has no fitted-set parent")
            if receipt_row is None and require_receipt:
                raise _incomplete(release_id)
            return _detach_stored(set_row, release_row, bucket_rows, receipt_row)


class _StoredCorrupt(ValueError):
    pass


def _prepare_release(
    fitted_set: FittedCalibrationSet,
    *,
    fit_dataset: CalibrationEvidenceSet,
    heldout_dataset: CalibrationEvidenceSet,
    confidence_level: float,
) -> _PreparedRelease:
    release = _reproduce_release(
        fitted_set,
        fit_dataset=fit_dataset,
        heldout_dataset=heldout_dataset,
        confidence_level=confidence_level,
    )
    return _PreparedRelease(
        release=release,
        canonical_set=canonical_calibration_set(release.fitted_set),
    )


def _reproduce_release(
    fitted_set: FittedCalibrationSet,
    *,
    fit_dataset: CalibrationEvidenceSet,
    heldout_dataset: CalibrationEvidenceSet,
    confidence_level: float,
) -> HeldoutCoverageRelease:
    evidence = estimate_heldout_coverage(
        fitted_set,
        fit_dataset=fit_dataset,
        heldout_dataset=heldout_dataset,
        confidence_level=confidence_level,
    )
    return build_heldout_coverage_release(fitted_set, evidence)


def _detach_stored(
    set_row: ForecastFittedCalibrationSet,
    release_row: ForecastHeldoutCoverageRelease,
    bucket_rows: tuple[ForecastHeldoutCoverageReleaseBucket, ...],
    receipt_row: ForecastHeldoutCoverageReleaseAvailability | None,
) -> _StoredRelease:
    try:
        set_record = FittedCalibrationSetRecord(
            **{
                field: getattr(set_row, field)
                for field in FittedCalibrationSetRecord.__dataclass_fields__
                if field != "canonical_set"
            },
            canonical_set=bytes(set_row.canonical_set),
        )
        release_record = HeldoutCoverageReleaseRecord(
            **{
                field: getattr(release_row, field)
                for field in HeldoutCoverageReleaseRecord.__dataclass_fields__
                if field not in {"canonical_release", "confidence_level_f64_be"}
            },
            confidence_level_f64_be=bytes(release_row.confidence_level_f64_be),
            canonical_release=bytes(release_row.canonical_release),
        )
        buckets = tuple(
            HeldoutCoverageReleaseBucketRecord(
                **{
                    field: getattr(row, field)
                    for field in HeldoutCoverageReleaseBucketRecord.__dataclass_fields__
                    if not field.endswith("_f64_be")
                },
                empirical_coverage_f64_be=bytes(row.empirical_coverage_f64_be),
                confidence_low_f64_be=bytes(row.confidence_low_f64_be),
                confidence_high_f64_be=bytes(row.confidence_high_f64_be),
            )
            for row in bucket_rows
        )
        availability = (
            None
            if receipt_row is None
            else HeldoutCoverageReleaseAvailability(
                release_id=receipt_row.release_id,
                release_recorded_at=receipt_row.release_recorded_at,
                available_at=receipt_row.available_at,
                sealer_xid=receipt_row.sealer_xid,
            )
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _StoredCorrupt("stored release rows cannot be detached") from exc
    return _StoredRelease(
        set_record=set_record,
        release_record=release_record,
        bucket_records=buckets,
        availability=availability,
    )


def _validate_stored(
    stored: _StoredRelease,
    *,
    abandon_on_missing_receipt: bool = False,
) -> HeldoutCoverageReleaseProof:
    set_record = stored.set_record
    fitted_set = parse_calibration_set(set_record.canonical_set)
    set_version = calibration_set_version_for(set_record.canonical_set)
    if set_record.calibration_set_version != set_version:
        raise _StoredCorrupt("stored fitted-set identity differs from its bytes")
    expected_set = (
        fitted_set.schema_version,
        fitted_set.model_version,
        fitted_set.symbol,
        fitted_set.target,
        fitted_set.series_basis,
        fitted_set.horizon_unit,
        fitted_set.currency,
        fitted_set.source_calibration_set_version,
        fitted_set.source_calibration_method,
        fitted_set.forecast_resolution_policy_hash,
        fitted_set.forecast_availability_rule_set_hash,
        fitted_set.fit_evidence_digest,
        fitted_set.method,
        fitted_set.window_start,
        fitted_set.window_end,
        fitted_set.sample_count,
        fitted_set.cohort_id,
        fitted_set.selection_policy_hash,
        fitted_set.outcome_resolution_policy_hash,
        fitted_set.outcome_availability_rule_set_hash,
        fitted_set.interval_policy_version,
        fitted_set.window_date_policy_version,
        len(fitted_set.buckets),
    )
    actual_set = (
        set_record.schema_version,
        set_record.model_version,
        set_record.symbol,
        set_record.target,
        set_record.series_basis,
        set_record.horizon_unit,
        set_record.currency,
        set_record.source_calibration_set_version,
        set_record.source_calibration_method,
        set_record.forecast_resolution_policy_hash,
        set_record.forecast_availability_rule_set_hash,
        set_record.fit_evidence_digest,
        set_record.method,
        set_record.window_start,
        set_record.window_end,
        set_record.sample_count,
        set_record.cohort_id,
        set_record.selection_policy_hash,
        set_record.outcome_resolution_policy_hash,
        set_record.outcome_availability_rule_set_hash,
        set_record.interval_policy_version,
        set_record.window_date_policy_version,
        set_record.bucket_count,
    )
    if actual_set != expected_set or not _valid_stamp(
        set_record.recorded_at,
        set_record.creator_xid,
    ):
        raise _StoredCorrupt("stored fitted-set projection differs from canonical bytes")

    release_record = stored.release_record
    evidence = parse_heldout_coverage_release(
        release_record.canonical_release,
        fitted_set=fitted_set,
    )
    release = build_heldout_coverage_release(fitted_set, evidence)
    if (
        release.release_id != release_record.release_id
        or heldout_coverage_release_id_for(release) != release_record.release_id
    ):
        raise _StoredCorrupt("stored release identity differs from its bytes")
    expected_release = (
        HELDOUT_COVERAGE_RELEASE_SCHEMA_VERSION,
        HELDOUT_COVERAGE_RELEASE_SCOPE,
        evidence.fitted_calibration_set_version,
        evidence.method,
        evidence.model_version,
        evidence.symbol,
        evidence.target,
        evidence.series_basis,
        evidence.horizon_unit,
        evidence.currency,
        evidence.fit_cohort_id,
        evidence.fit_selection_policy_hash,
        evidence.heldout_cohort_id,
        evidence.heldout_selection_policy_hash,
        evidence.outcome_resolution_policy_hash,
        evidence.outcome_availability_rule_set_hash,
        evidence.forecast_resolution_policy_hash,
        evidence.forecast_availability_rule_set_hash,
        evidence.fit_evidence_digest,
        evidence.heldout_evidence_digest,
        evidence.heldout_window_start,
        evidence.heldout_window_end,
        evidence.heldout_sample_count,
        _f64_bytes(evidence.confidence_level),
        evidence.interval_policy_version,
        evidence.window_date_policy_version,
        evidence.estimator_policy_version,
        len(evidence.buckets),
    )
    actual_release = (
        release_record.schema_version,
        release_record.evidence_scope,
        release_record.fitted_calibration_set_version,
        release_record.method,
        release_record.model_version,
        release_record.symbol,
        release_record.target,
        release_record.series_basis,
        release_record.horizon_unit,
        release_record.currency,
        release_record.fit_cohort_id,
        release_record.fit_selection_policy_hash,
        release_record.heldout_cohort_id,
        release_record.heldout_selection_policy_hash,
        release_record.outcome_resolution_policy_hash,
        release_record.outcome_availability_rule_set_hash,
        release_record.forecast_resolution_policy_hash,
        release_record.forecast_availability_rule_set_hash,
        release_record.fit_evidence_digest,
        release_record.heldout_evidence_digest,
        release_record.heldout_window_start,
        release_record.heldout_window_end,
        release_record.heldout_sample_count,
        release_record.confidence_level_f64_be,
        release_record.interval_policy_version,
        release_record.window_date_policy_version,
        release_record.estimator_policy_version,
        release_record.bucket_count,
    )
    if actual_release != expected_release or not _valid_stamp(
        release_record.recorded_at,
        release_record.creator_xid,
    ):
        raise _StoredCorrupt("stored release projection differs from canonical bytes")
    if _utc(release_record.recorded_at) < _utc(set_record.recorded_at):
        raise _StoredCorrupt("stored release predates its fitted calibration set")

    expected_buckets = tuple(
        (
            release.release_id,
            bucket.horizon,
            round(bucket.nominal_coverage * 1000),
            bucket.covered_count,
            bucket.sample_count,
            _f64_bytes(bucket.empirical_coverage),
            _f64_bytes(bucket.confidence_low),
            _f64_bytes(bucket.confidence_high),
        )
        for bucket in evidence.buckets
    )
    actual_buckets = tuple(
        (
            bucket.release_id,
            bucket.horizon,
            bucket.coverage_millis,
            bucket.covered_count,
            bucket.sample_count,
            bucket.empirical_coverage_f64_be,
            bucket.confidence_low_f64_be,
            bucket.confidence_high_f64_be,
        )
        for bucket in stored.bucket_records
    )
    if actual_buckets != expected_buckets:
        raise _StoredCorrupt("stored release buckets differ from canonical bytes")

    availability = stored.availability
    if availability is None:
        if abandon_on_missing_receipt:
            availability = HeldoutCoverageReleaseAvailability(
                release_id=release.release_id,
                release_recorded_at=release_record.recorded_at,
                available_at=release_record.recorded_at,
                sealer_xid=release_record.creator_xid,
            )
        else:
            raise _StoredCorrupt("stored release has no availability receipt")
    elif (
        availability.release_id != release.release_id
        or _utc(availability.release_recorded_at) != _utc(release_record.recorded_at)
        or _utc(availability.available_at) < _utc(release_record.recorded_at)
        or type(availability.sealer_xid) is not int
        or availability.sealer_xid <= 0
        or availability.sealer_xid == release_record.creator_xid
    ):
        raise _StoredCorrupt("stored release availability receipt is invalid")
    return HeldoutCoverageReleaseProof(
        release=release,
        set_record=set_record,
        release_record=release_record,
        bucket_records=stored.bucket_records,
        availability=availability,
    )


def _f64_bytes(value: float) -> bytes:
    return struct.pack(">d", value)


def _valid_stamp(recorded_at: datetime, creator_xid: int) -> bool:
    try:
        _utc(recorded_at)
    except (TypeError, ValueError):
        return False
    return type(creator_xid) is int and creator_xid > 0


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("stored timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _release_identity(value: object) -> str:
    if not isinstance(value, str) or _HASH_PATTERN.fullmatch(value) is None:
        raise AppError(
            "Forecast calibration release identity is invalid.",
            code="forecast_calibration_release_request_invalid",
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            details={"retryable": False},
        )
    return value


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


def _deterministic_rejection(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    if sqlstate is None:
        return False
    if sqlstate == "23505":
        constraint = _db_error_attribute(exc, "constraint_name")
        return constraint not in {
            _SET_PRIMARY_KEY,
            _RELEASE_PRIMARY_KEY,
            _RECEIPT_PRIMARY_KEY,
        }
    return sqlstate.startswith(("22", "23")) or sqlstate == "55000"


def _configuration_failure(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and sqlstate.startswith(("0A", "28", "3D", "3F", "42"))


def _completion_unknown(exc: DBAPIError) -> bool:
    return _db_error_attribute(exc, "sqlstate") == "40003"


def _database_error(exc: DBAPIError) -> AppError:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    if sqlstate is not None and sqlstate.startswith(("0A", "22", "28", "3D", "3F", "42")):
        return _configuration_invalid()
    return _unavailable()


def _configuration_invalid() -> AppError:
    return AppError(
        "Forecast calibration release database configuration is invalid.",
        code="forecast_calibration_release_configuration_invalid",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _invalid() -> AppError:
    return AppError(
        "Forecast calibration release evidence is invalid.",
        code="forecast_calibration_release_invalid",
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        details={"retryable": False},
    )


def _missing(release_id: str) -> AppError:
    return AppError(
        "The requested forecast calibration release does not exist.",
        code="forecast_calibration_release_missing",
        status_code=status.HTTP_404_NOT_FOUND,
        details={"release_id": release_id, "retryable": False},
    )


def _incomplete(release_id: str) -> AppError:
    return AppError(
        "Forecast calibration release is not yet post-commit available.",
        code="forecast_calibration_release_incomplete",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"release_id": release_id, "retryable": True},
    )


def _conflict() -> AppError:
    return AppError(
        "Forecast calibration release conflicts with persisted evidence.",
        code="forecast_calibration_release_conflict",
        status_code=status.HTTP_409_CONFLICT,
        details={"retryable": False},
    )


def _corrupt() -> AppError:
    return AppError(
        "Persisted forecast calibration release failed validation.",
        code="forecast_calibration_release_corrupt",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _unavailable() -> AppError:
    return AppError(
        "Forecast calibration release storage is unavailable.",
        code="forecast_calibration_release_store_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _outcome_unknown(stage: str) -> AppError:
    return AppError(
        "Forecast calibration release commit outcome is unknown.",
        code="forecast_calibration_release_outcome_unknown",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"outcome_unknown": True, "retryable": True, "stage": stage},
    )


__all__ = [
    "FittedCalibrationSetRecord",
    "HeldoutCoverageReleaseAvailability",
    "HeldoutCoverageReleaseBucketRecord",
    "HeldoutCoverageReleaseProof",
    "HeldoutCoverageReleaseRecord",
    "HeldoutCoverageReleaseStore",
    "SqlHeldoutCoverageReleaseStore",
]
