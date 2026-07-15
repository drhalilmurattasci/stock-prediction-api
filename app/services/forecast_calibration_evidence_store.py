"""Consistent, read-only PostgreSQL loading for calibration evidence.

The calibration fitter consumes a complete, prospectively sealed cohort.  A
cohort and its outcomes are published in several transactions, so composing
the existing per-record stores would both create an N+1 query pattern and risk
observing unrelated ``READ COMMITTED`` snapshots.  This reader instead takes
one short ``REPEATABLE READ, READ ONLY`` snapshot, detaches every scalar row,
then releases the connection before canonical parsing and proof validation.

Only an explicit realized-outcome publication authorizes a cohort member to
use an outcome.  The reader never falls back to a semantic symbol/time lookup.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol, runtime_checkable
from uuid import UUID

import anyio
from fastapi import status
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import AppError
from app.db.models.forecast_evidence import (
    ForecastOutcomeCohortAvailability,
    ForecastOutcomeCohortManifest,
    ForecastOutcomeCohortMember,
    ForecastRealizedOutcome,
    ForecastRealizedOutcomePublication,
)
from app.db.models.predictions import ForecastRun
from app.services.forecast_calibration_evidence import (
    CalibrationEvidenceSet,
    CalibrationJoinMemberProof,
    ForecastCalibrationEvidenceError,
    join_calibration_evidence,
)
from app.services.forecast_cohort_store import ForecastCohortProof
from app.services.forecast_cohorts import (
    ForecastCohortMember,
    ForecastCohortRecord,
    ForecastCohortSeal,
    ForecastCohortValidationError,
    validate_cohort_record,
    validate_cohort_seal,
)
from app.services.forecast_outcome_store import (
    ForecastOutcomeProof,
    ForecastOutcomePublicationRecord,
)
from app.services.forecast_outcomes import (
    OutcomeValidationError,
    RealizedOutcomeRecord,
    validate_outcome_record,
)
from app.services.forecast_run_store import ArchivedForecastRun

_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_SNAPSHOT_STATEMENT = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
_MAX_CANONICAL_EVIDENCE_BYTES = 128 * 1024 * 1024
_CANONICAL_EVIDENCE_SIZE_QUERY = text(
    """
    SELECT (octet_length(manifest.canonical_manifest)::bigint
         + COALESCE((
               SELECT sum(
                   octet_length(run.canonical_request)::bigint
                   + octet_length(run.canonical_output)::bigint
               )
               FROM public.forecast_runs AS run
               WHERE run.forecast_id IN (
                   SELECT DISTINCT member.forecast_id
                   FROM public.forecast_outcome_cohort_members AS member
                   WHERE member.cohort_id = :cohort_id
               )
           ), 0::bigint)
         + COALESCE((
               SELECT sum(octet_length(outcome.canonical_evidence)::bigint)
               FROM public.forecast_realized_outcome_publications AS publication
               JOIN public.forecast_realized_outcomes AS outcome
                 ON outcome.outcome_id = publication.outcome_id
               WHERE publication.cohort_id = :cohort_id
           ), 0::bigint))::bigint AS canonical_evidence_bytes
    FROM public.forecast_outcome_cohort_manifests AS manifest
    WHERE manifest.cohort_id = :cohort_id
    """
)


@runtime_checkable
class ForecastCalibrationEvidenceReader(Protocol):
    """Load one complete cohort as independently validated fit evidence."""

    async def read_validated(self, cohort_id: str) -> CalibrationEvidenceSet: ...


@dataclass(frozen=True)
class _PublishedOutcome:
    publication: ForecastOutcomePublicationRecord
    record: RealizedOutcomeRecord | None


@dataclass(frozen=True)
class _EvidenceSnapshot:
    cohort_record: ForecastCohortRecord
    cohort_seal: ForecastCohortSeal
    member_projection: tuple[ForecastCohortMember, ...]
    runs: tuple[ArchivedForecastRun, ...]
    published_outcomes: tuple[_PublishedOutcome, ...]


class _SnapshotCorrupt(ValueError):
    """Fetched rows cannot be represented at the detached proof boundary."""


class _EvidenceNotReady(ValueError):
    def __init__(self, missing_count: int) -> None:
        super().__init__("cohort outcomes are not complete")
        self.missing_count = missing_count


class _EvidenceTooLarge(ValueError):
    """The database snapshot exceeds the bounded trusted-reader budget."""


@dataclass(frozen=True)
class SqlForecastCalibrationEvidenceReader:
    """Bulk-load one sealed cohort from a single immutable database snapshot."""

    sessionmaker: async_sessionmaker[AsyncSession]

    async def read_validated(self, cohort_id: str) -> CalibrationEvidenceSet:
        identity = _cohort_identity(cohort_id)
        try:
            snapshot = await self._read_snapshot(identity)
        except AppError:
            raise
        except SQLAlchemyTimeoutError as exc:
            raise _unavailable() from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc
        except _EvidenceTooLarge as exc:
            raise _too_large() from exc
        except (AttributeError, _SnapshotCorrupt, TypeError, ValueError) as exc:
            raise _corrupt() from exc

        try:
            return await anyio.to_thread.run_sync(_assemble_evidence, snapshot)
        except _EvidenceNotReady as exc:
            raise _not_ready(identity, exc.missing_count) from exc
        except (
            ForecastCalibrationEvidenceError,
            ForecastCohortValidationError,
            OutcomeValidationError,
            _SnapshotCorrupt,
            AttributeError,
            TypeError,
            ValueError,
        ) as exc:
            raise _corrupt() from exc

    async def _read_snapshot(self, cohort_id: str) -> _EvidenceSnapshot:
        async with self.sessionmaker() as session, session.begin():
            # This must be the first statement after BEGIN.  Every following
            # query observes the same MVCC snapshot, and READ ONLY is a
            # mechanical guard against accidental writes in this repository.
            await session.execute(text(_SNAPSHOT_STATEMENT))

            header_result = await session.execute(
                select(
                    ForecastOutcomeCohortManifest,
                    ForecastOutcomeCohortAvailability,
                )
                .outerjoin(
                    ForecastOutcomeCohortAvailability,
                    ForecastOutcomeCohortAvailability.cohort_id
                    == ForecastOutcomeCohortManifest.cohort_id,
                )
                .where(ForecastOutcomeCohortManifest.cohort_id == cohort_id)
            )
            header = header_result.one_or_none()
            if header is None:
                raise _missing(cohort_id)
            manifest_row, seal_row = header
            if seal_row is None:
                raise _incomplete()

            canonical_size = (
                await session.execute(
                    _CANONICAL_EVIDENCE_SIZE_QUERY,
                    {"cohort_id": cohort_id},
                )
            ).scalar_one()
            if (
                type(canonical_size) is not int
                or canonical_size < 0
                or canonical_size > _MAX_CANONICAL_EVIDENCE_BYTES
            ):
                raise _EvidenceTooLarge("canonical evidence snapshot exceeds its read budget")

            member_result = await session.execute(
                select(ForecastOutcomeCohortMember)
                .where(ForecastOutcomeCohortMember.cohort_id == cohort_id)
                .order_by(
                    ForecastOutcomeCohortMember.target_time,
                    ForecastOutcomeCohortMember.forecast_id,
                    ForecastOutcomeCohortMember.step,
                )
            )
            member_rows = tuple(member_result.scalars().all())

            run_ids = (
                select(ForecastOutcomeCohortMember.forecast_id)
                .where(ForecastOutcomeCohortMember.cohort_id == cohort_id)
                .distinct()
            )
            run_result = await session.execute(
                select(ForecastRun)
                .where(ForecastRun.forecast_id.in_(run_ids))
                .order_by(ForecastRun.forecast_id)
            )
            run_rows = tuple(run_result.scalars().all())

            publication_result = await session.execute(
                select(
                    ForecastRealizedOutcomePublication,
                    ForecastRealizedOutcome,
                )
                .outerjoin(
                    ForecastRealizedOutcome,
                    ForecastRealizedOutcome.outcome_id
                    == ForecastRealizedOutcomePublication.outcome_id,
                )
                .where(ForecastRealizedOutcomePublication.cohort_id == cohort_id)
                .order_by(
                    ForecastRealizedOutcomePublication.forecast_id,
                    ForecastRealizedOutcomePublication.step,
                    ForecastRealizedOutcomePublication.outcome_id,
                )
            )
            publication_rows = tuple((row[0], row[1]) for row in publication_result.all())

            # Detach while scalar ORM attributes are still session-bound.  No
            # canonical JSON parsing or hashing occurs until after this
            # transaction and its pooled connection have been released.
            return _detach_snapshot(
                manifest_row,
                seal_row,
                member_rows,
                run_rows,
                publication_rows,
            )


def _detach_snapshot(
    manifest_row: ForecastOutcomeCohortManifest,
    seal_row: ForecastOutcomeCohortAvailability,
    member_rows: tuple[ForecastOutcomeCohortMember, ...],
    run_rows: tuple[ForecastRun, ...],
    publication_rows: tuple[
        tuple[ForecastRealizedOutcomePublication, ForecastRealizedOutcome | None], ...
    ],
) -> _EvidenceSnapshot:
    try:
        cohort_record = ForecastCohortRecord(
            cohort_id=manifest_row.cohort_id,
            schema_version=manifest_row.schema_version,
            purpose=manifest_row.purpose,  # type: ignore[arg-type]
            selection_policy_hash=manifest_row.selection_policy_hash,
            outcome_resolution_policy_hash=manifest_row.outcome_resolution_policy_hash,
            availability_rule_set_hash=manifest_row.availability_rule_set_hash,
            member_count=manifest_row.member_count,
            earliest_target_time=manifest_row.earliest_target_time,
            latest_target_time=manifest_row.latest_target_time,
            recorded_at=manifest_row.recorded_at,
            creator_xid=manifest_row.creator_xid,
            canonical_manifest=bytes(manifest_row.canonical_manifest),
        )
        cohort_seal = ForecastCohortSeal(
            cohort_id=seal_row.cohort_id,
            manifest_recorded_at=seal_row.manifest_recorded_at,
            sealed_at=seal_row.sealed_at,
            sealer_xid=seal_row.sealer_xid,
        )
        members = tuple(
            ForecastCohortMember(
                forecast_id=row.forecast_id,
                step=row.step,
                target_time=row.target_time,
                opportunity_hash=row.opportunity_hash,
                output_hash=row.output_hash,
            )
            for row in member_rows
        )
        runs = tuple(_detach_run(row) for row in run_rows)
        outcomes = tuple(
            _PublishedOutcome(
                publication=_detach_publication(publication),
                record=None if outcome is None else _detach_outcome(outcome),
            )
            for publication, outcome in publication_rows
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _SnapshotCorrupt("database rows cannot be detached safely") from exc
    return _EvidenceSnapshot(
        cohort_record=cohort_record,
        cohort_seal=cohort_seal,
        member_projection=members,
        runs=runs,
        published_outcomes=outcomes,
    )


def _detach_run(row: ForecastRun) -> ArchivedForecastRun:
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


def _detach_outcome(row: ForecastRealizedOutcome) -> RealizedOutcomeRecord:
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


def _detach_publication(
    row: ForecastRealizedOutcomePublication,
) -> ForecastOutcomePublicationRecord:
    return ForecastOutcomePublicationRecord(
        outcome_id=row.outcome_id,
        cohort_id=row.cohort_id,
        forecast_id=row.forecast_id,
        step=row.step,
        published_at=row.published_at,
        publisher_xid=row.publisher_xid,
    )


def _assemble_evidence(snapshot: _EvidenceSnapshot) -> CalibrationEvidenceSet:
    manifest = validate_cohort_record(snapshot.cohort_record)
    projected = tuple(
        sorted(
            snapshot.member_projection,
            key=lambda member: (
                member.target_time,
                str(member.forecast_id),
                member.step,
            ),
        )
    )
    if projected != manifest.members:
        raise _SnapshotCorrupt("cohort member projection differs from canonical evidence")
    sealed_manifest = validate_cohort_seal(snapshot.cohort_record, snapshot.cohort_seal)
    if sealed_manifest != manifest:
        raise _SnapshotCorrupt("cohort seal resolves to different manifest evidence")
    cohort = ForecastCohortProof(
        manifest=manifest,
        record=snapshot.cohort_record,
        seal=snapshot.cohort_seal,
    )

    expected_run_ids = {member.forecast_id for member in manifest.members}
    runs: dict[UUID, ArchivedForecastRun] = {}
    for run in snapshot.runs:
        if run.forecast_id in runs:
            raise _SnapshotCorrupt("archive snapshot contains a duplicate forecast run")
        runs[run.forecast_id] = run
    if set(runs) != expected_run_ids:
        raise _SnapshotCorrupt("archive snapshot does not exactly cover cohort runs")

    expected_members = {(member.forecast_id, member.step) for member in manifest.members}
    published: dict[tuple[UUID, int], ForecastOutcomeProof] = {}
    for item in snapshot.published_outcomes:
        publication = item.publication
        identity = (publication.forecast_id, publication.step)
        if identity not in expected_members or identity in published:
            raise _SnapshotCorrupt("outcome publications do not map one-to-one to cohort members")
        if item.record is None:
            raise _SnapshotCorrupt("outcome publication has no referenced outcome row")
        payload = validate_outcome_record(
            item.record,
            expected_outcome_resolution_policy_hash=manifest.outcome_resolution_policy_hash,
            expected_availability_rule_set_hash=manifest.availability_rule_set_hash,
        )
        published[identity] = ForecastOutcomeProof(
            payload=payload,
            record=item.record,
            publication=publication,
        )

    missing = expected_members - set(published)
    if missing:
        raise _EvidenceNotReady(len(missing))

    proofs = tuple(
        CalibrationJoinMemberProof(
            run=runs[member.forecast_id],
            outcome=published[(member.forecast_id, member.step)],
        )
        for member in manifest.members
    )
    return join_calibration_evidence(cohort, proofs)


def _cohort_identity(value: object) -> str:
    if not isinstance(value, str) or _HASH_PATTERN.fullmatch(value) is None:
        raise AppError(
            "Forecast calibration evidence identity is invalid.",
            code="forecast_calibration_evidence_request_invalid",
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


def _database_error(exc: DBAPIError) -> AppError:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    if sqlstate is not None and sqlstate.startswith(("0A", "22", "28", "3D", "3F", "42")):
        return AppError(
            "Forecast calibration evidence database configuration is invalid.",
            code="forecast_calibration_evidence_configuration_invalid",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={"retryable": False},
        )
    return _unavailable()


def _missing(cohort_id: str) -> AppError:
    return AppError(
        "The requested persisted forecast cohort does not exist.",
        code="forecast_cohort_evidence_missing",
        status_code=status.HTTP_404_NOT_FOUND,
        details={"cohort_id": cohort_id, "retryable": False},
    )


def _incomplete() -> AppError:
    return AppError(
        "Forecast cohort evidence is incomplete.",
        code="forecast_cohort_evidence_incomplete",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _not_ready(cohort_id: str, missing_count: int) -> AppError:
    return AppError(
        "Forecast calibration outcomes are not complete for this cohort.",
        code="forecast_calibration_evidence_not_ready",
        status_code=status.HTTP_409_CONFLICT,
        details={
            "cohort_id": cohort_id,
            "missing_member_count": missing_count,
            "retryable": True,
        },
    )


def _unavailable() -> AppError:
    return AppError(
        "Forecast calibration evidence storage is unavailable.",
        code="forecast_calibration_evidence_store_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _too_large() -> AppError:
    return AppError(
        "Forecast calibration evidence exceeds the trusted-reader size limit.",
        code="forecast_calibration_evidence_too_large",
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        details={
            "max_canonical_bytes": _MAX_CANONICAL_EVIDENCE_BYTES,
            "retryable": False,
        },
    )


def _corrupt() -> AppError:
    return AppError(
        "Persisted forecast calibration evidence failed validation.",
        code="forecast_calibration_evidence_corrupt",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


__all__ = [
    "ForecastCalibrationEvidenceReader",
    "SqlForecastCalibrationEvidenceReader",
]
