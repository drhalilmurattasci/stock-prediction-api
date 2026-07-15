"""Policy-neutral joins, fitting, and held-out evidence for conformal calibration.

This module is deliberately pure.  It selects no cohorts, resolves no outcomes,
writes no rows, and changes no serving behavior.  Its inputs are detached,
content-addressed proofs; every cohort member must be supplied exactly once.

The final stage reports Wilson coverage evidence only.  It does not decide
whether a calibration set is acceptable for serving: sample floors, tolerances,
multiplicity, temporal embargoes, and promotion remain a separate policy-bound
publication decision.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from statistics import NormalDist, StatisticsError
from typing import Literal
from uuid import UUID

from app.schemas.forecast import ForecastResponse
from app.services.forecast_calibration_sets import (
    INTERVAL_POLICY_VERSION,
    WINDOW_DATE_POLICY_VERSION,
    FittedCalibrationBucket,
    FittedCalibrationSet,
    ForecastCalibrationSetValidationError,
    calibration_set_version_for,
    canonical_calibration_set,
    parse_calibration_set,
)
from app.services.forecast_cohort_store import ForecastCohortProof
from app.services.forecast_cohorts import (
    CohortPurpose,
    ForecastCohortValidationError,
    member_from_scheduled_run,
    validate_cohort_seal,
)
from app.services.forecast_outcome_store import (
    ForecastOutcomeProof,
    ForecastOutcomePublicationRecord,
)
from app.services.forecast_outcomes import OutcomeValidationError, validate_outcome_record
from app.services.forecast_run_store import ArchivedForecastRun
from app.services.forecast_runs import (
    RUN_SCHEMA_VERSION,
    ForecastRunValidationError,
    opportunity_hash,
    output_hash,
    parse_output,
    parse_request,
    request_hash,
)
from ml.calibration.conformal import (
    AbsoluteResidualCalibration,
    ConformalValidationError,
    CQRCalibration,
    InsufficientCalibrationData,
    fit_absolute_residual,
    fit_cqr,
    interval_missed,
)

CENTRAL_INTERVAL_POLICY_VERSION = INTERVAL_POLICY_VERSION
CALIBRATION_EVIDENCE_FORMAT = "forecast-calibration-evidence-v1"
WILSON_COVERAGE_POLICY_VERSION = "wilson-score-two-sided-v1"
_UNCALIBRATED_METHOD = "none"
_COVERAGE_TOLERANCE = 1e-9
_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")

type FitMethod = Literal["empirical_residual", "conformal_quantile_regression"]


class ForecastCalibrationEvidenceError(ValueError):
    """Calibration evidence is incomplete, inconsistent, or unrepresentable."""


class InsufficientCalibrationFitData(ForecastCalibrationEvidenceError):
    """A requested finite-sample bucket has no finite order statistic."""


@dataclass(frozen=True, slots=True)
class CalibrationJoinMemberProof:
    """One archived scheduled forecast and its persisted realized-outcome proof."""

    run: ArchivedForecastRun
    outcome: ForecastOutcomeProof


@dataclass(frozen=True, slots=True)
class CalibrationIntervalObservation:
    coverage: float
    lower_quantile: float
    upper_quantile: float
    lower: float
    upper: float


@dataclass(frozen=True, slots=True)
class CalibrationObservation:
    forecast_id: UUID
    outcome_id: str
    opportunity_hash: str
    output_hash: str
    horizon: int
    target_time: datetime
    model_version: str
    source_calibration_set_version: str
    source_calibration_method: str
    forecast_resolution_policy_hash: str
    forecast_availability_rule_set_hash: str
    symbol: str
    target: str
    series_basis: str
    horizon_unit: str
    currency: str
    realized_value: float
    point: float
    intervals: tuple[CalibrationIntervalObservation, ...]


@dataclass(frozen=True, slots=True)
class CalibrationEvidenceSet:
    """Exact-cover evidence joined from one sealed prospective cohort."""

    cohort_id: str
    purpose: CohortPurpose
    selection_policy_hash: str
    outcome_resolution_policy_hash: str
    outcome_availability_rule_set_hash: str
    evidence_digest: str
    symbol: str
    target: str
    series_basis: str
    horizon_unit: str
    currency: str
    model_version: str
    source_calibration_set_version: str
    source_calibration_method: str
    forecast_resolution_policy_hash: str
    forecast_availability_rule_set_hash: str
    observations: tuple[CalibrationObservation, ...]
    source_cohort: ForecastCohortProof
    source_members: tuple[CalibrationJoinMemberProof, ...]


@dataclass(frozen=True, slots=True)
class CalibrationFitBucket:
    horizon: int
    coverage: float


@dataclass(frozen=True, slots=True)
class HeldoutCoverageBucket:
    horizon: int
    nominal_coverage: float
    covered_count: int
    sample_count: int
    empirical_coverage: float
    confidence_low: float
    confidence_high: float


@dataclass(frozen=True, slots=True)
class HeldoutCoverageEvidence:
    """Descriptive held-out coverage evidence; never a serving decision."""

    fitted_calibration_set_version: str
    method: FitMethod
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
    confidence_level: float
    interval_policy_version: str
    window_date_policy_version: str
    estimator_policy_version: str
    buckets: tuple[HeldoutCoverageBucket, ...]


def join_calibration_evidence(
    cohort: ForecastCohortProof,
    members: Sequence[CalibrationJoinMemberProof],
) -> CalibrationEvidenceSet:
    """Join one sealed cohort to exact archived forecasts and realized outcomes."""

    manifest = _validated_cohort(cohort)
    if isinstance(members, (str, bytes)):
        raise ForecastCalibrationEvidenceError("member proofs must be a sequence")
    try:
        supplied = tuple(members)
    except TypeError as exc:
        raise ForecastCalibrationEvidenceError("member proofs must be a sequence") from exc
    if len(supplied) != len(manifest.members):
        raise ForecastCalibrationEvidenceError(
            "calibration evidence must exactly cover every cohort member"
        )

    expected = {(member.forecast_id, member.step): member for member in manifest.members}
    seen: set[tuple[UUID, int]] = set()
    proofs_by_identity: dict[tuple[UUID, int], CalibrationJoinMemberProof] = {}
    observations: list[CalibrationObservation] = []
    for index, proof in enumerate(supplied):
        if not isinstance(proof, CalibrationJoinMemberProof):
            raise ForecastCalibrationEvidenceError(f"member proof {index} has the wrong type")
        if not isinstance(proof.outcome, ForecastOutcomeProof):
            raise ForecastCalibrationEvidenceError(
                f"member proof {index} has an invalid outcome proof"
            )
        publication = proof.outcome.publication
        if not isinstance(publication, ForecastOutcomePublicationRecord):
            raise ForecastCalibrationEvidenceError(
                f"member proof {index} has an invalid outcome publication"
            )
        identity = (publication.forecast_id, publication.step)
        if identity in seen:
            raise ForecastCalibrationEvidenceError(
                "calibration evidence contains a duplicate member"
            )
        seen.add(identity)
        proofs_by_identity[identity] = proof
        committed = expected.get(identity)
        if committed is None:
            raise ForecastCalibrationEvidenceError("calibration evidence contains an extra member")
        observations.append(
            _joined_observation(
                proof,
                cohort=cohort,
                committed=committed,
            )
        )
    if seen != set(expected):
        raise ForecastCalibrationEvidenceError(
            "calibration evidence must exactly cover every cohort member"
        )

    ordered = tuple(sorted(observations, key=_observation_key))
    first = ordered[0]
    scope = (
        first.symbol,
        first.target,
        first.series_basis,
        first.horizon_unit,
        first.currency,
        first.model_version,
        first.source_calibration_set_version,
        first.source_calibration_method,
        first.forecast_resolution_policy_hash,
        first.forecast_availability_rule_set_hash,
    )
    if any(
        (
            item.symbol,
            item.target,
            item.series_basis,
            item.horizon_unit,
            item.currency,
            item.model_version,
            item.source_calibration_set_version,
            item.source_calibration_method,
            item.forecast_resolution_policy_hash,
            item.forecast_availability_rule_set_hash,
        )
        != scope
        for item in ordered
    ):
        raise ForecastCalibrationEvidenceError(
            "calibration evidence mixes semantic scope, model, or source calibration"
        )
    if first.source_calibration_method != _UNCALIBRATED_METHOD or (
        first.source_calibration_set_version != f"uncalibrated:{first.model_version}"
    ):
        raise ForecastCalibrationEvidenceError(
            "calibration v1 requires uncalibrated source forecasts"
        )
    dataset = CalibrationEvidenceSet(
        cohort_id=cohort.record.cohort_id,
        purpose=manifest.purpose,
        selection_policy_hash=manifest.selection_policy_hash,
        outcome_resolution_policy_hash=manifest.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=manifest.availability_rule_set_hash,
        evidence_digest=_evidence_digest(cohort.record.cohort_id, ordered),
        symbol=first.symbol,
        target=first.target,
        series_basis=first.series_basis,
        horizon_unit=first.horizon_unit,
        currency=first.currency,
        model_version=first.model_version,
        source_calibration_set_version=first.source_calibration_set_version,
        source_calibration_method=first.source_calibration_method,
        forecast_resolution_policy_hash=first.forecast_resolution_policy_hash,
        forecast_availability_rule_set_hash=first.forecast_availability_rule_set_hash,
        observations=ordered,
        source_cohort=cohort,
        source_members=tuple(
            proofs_by_identity[(item.forecast_id, item.horizon)] for item in ordered
        ),
    )
    return _normalized_dataset_shape(dataset, purpose=manifest.purpose)


def fit_empirical_residual_calibration_set(
    dataset: CalibrationEvidenceSet,
    *,
    buckets: Sequence[CalibrationFitBucket],
) -> FittedCalibrationSet:
    """Fit symmetric absolute-residual corrections from one fit cohort."""

    return _fit_calibration_set(dataset, method="empirical_residual", buckets=buckets)


def fit_cqr_calibration_set(
    dataset: CalibrationEvidenceSet,
    *,
    buckets: Sequence[CalibrationFitBucket],
) -> FittedCalibrationSet:
    """Fit signed CQR corrections from one fit cohort."""

    return _fit_calibration_set(
        dataset,
        method="conformal_quantile_regression",
        buckets=buckets,
    )


def estimate_heldout_coverage(
    fitted_set: FittedCalibrationSet,
    *,
    fit_dataset: CalibrationEvidenceSet,
    heldout_dataset: CalibrationEvidenceSet,
    confidence_level: float,
) -> HeldoutCoverageEvidence:
    """Apply one reproducible fit to a disjoint cohort and report Wilson evidence."""

    try:
        normalized_set = parse_calibration_set(canonical_calibration_set(fitted_set))
    except (ForecastCalibrationSetValidationError, TypeError, ValueError) as exc:
        raise ForecastCalibrationEvidenceError("fitted calibration set is invalid") from exc
    fit_dataset = _require_dataset(fit_dataset, purpose="calibration_fit")
    heldout_dataset = _require_dataset(heldout_dataset, purpose="heldout_evaluation")
    confidence = _open_probability(confidence_level, "confidence_level")
    fit_specs = tuple(
        CalibrationFitBucket(
            horizon=bucket.horizon,
            coverage=bucket.calibration.selection.coverage,
        )
        for bucket in normalized_set.buckets
    )
    if normalized_set.method == "empirical_residual":
        reproduced = fit_empirical_residual_calibration_set(fit_dataset, buckets=fit_specs)
    else:
        reproduced = fit_cqr_calibration_set(fit_dataset, buckets=fit_specs)
    if canonical_calibration_set(reproduced) != canonical_calibration_set(normalized_set):
        raise ForecastCalibrationEvidenceError(
            "fitted calibration set is not reproducible from the supplied fit cohort"
        )
    _require_heldout_compatibility(normalized_set, fit_dataset, heldout_dataset)

    rows: list[HeldoutCoverageBucket] = []
    for bucket in normalized_set.buckets:
        coverage = bucket.calibration.selection.coverage
        population = [
            observation
            for observation in heldout_dataset.observations
            if observation.horizon == bucket.horizon
        ]
        hits = 0
        for observation in population:
            source_interval = _interval_for(observation, coverage)
            try:
                if isinstance(bucket.calibration, AbsoluteResidualCalibration):
                    lower, upper = bucket.calibration.interval(observation.point)
                elif isinstance(bucket.calibration, CQRCalibration):
                    lower, upper = bucket.calibration.interval(
                        source_interval.lower,
                        source_interval.upper,
                    )
                else:  # pragma: no cover - normalized set makes this unreachable
                    raise ForecastCalibrationEvidenceError(
                        "fitted calibration bucket has an unsupported correction"
                    )
                hits += int(not interval_missed(observation.realized_value, lower, upper))
            except (ConformalValidationError, TypeError, ValueError) as exc:
                raise ForecastCalibrationEvidenceError(
                    "fitted correction cannot be applied to held-out evidence"
                ) from exc
        low, high = wilson_interval(hits, len(population), confidence_level=confidence)
        rows.append(
            HeldoutCoverageBucket(
                horizon=bucket.horizon,
                nominal_coverage=coverage,
                covered_count=hits,
                sample_count=len(population),
                empirical_coverage=hits / len(population),
                confidence_low=low,
                confidence_high=high,
            )
        )
    targets = tuple(item.target_time for item in heldout_dataset.observations)
    return HeldoutCoverageEvidence(
        fitted_calibration_set_version=calibration_set_version_for(normalized_set),
        method=normalized_set.method,
        model_version=normalized_set.model_version,
        symbol=normalized_set.symbol,
        target=normalized_set.target,
        series_basis=normalized_set.series_basis,
        horizon_unit=normalized_set.horizon_unit,
        currency=normalized_set.currency,
        fit_cohort_id=fit_dataset.cohort_id,
        fit_selection_policy_hash=fit_dataset.selection_policy_hash,
        heldout_cohort_id=heldout_dataset.cohort_id,
        heldout_selection_policy_hash=heldout_dataset.selection_policy_hash,
        outcome_resolution_policy_hash=heldout_dataset.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=(heldout_dataset.outcome_availability_rule_set_hash),
        forecast_resolution_policy_hash=heldout_dataset.forecast_resolution_policy_hash,
        forecast_availability_rule_set_hash=(heldout_dataset.forecast_availability_rule_set_hash),
        fit_evidence_digest=normalized_set.fit_evidence_digest,
        heldout_evidence_digest=heldout_dataset.evidence_digest,
        heldout_window_start=min(targets).date(),
        heldout_window_end=max(targets).date(),
        heldout_sample_count=len(heldout_dataset.observations),
        confidence_level=confidence,
        interval_policy_version=CENTRAL_INTERVAL_POLICY_VERSION,
        window_date_policy_version=WINDOW_DATE_POLICY_VERSION,
        estimator_policy_version=WILSON_COVERAGE_POLICY_VERSION,
        buckets=tuple(rows),
    )


def _validated_cohort(cohort: ForecastCohortProof):
    if not isinstance(cohort, ForecastCohortProof):
        raise ForecastCalibrationEvidenceError("cohort proof has the wrong type")
    try:
        manifest = validate_cohort_seal(cohort.record, cohort.seal)
    except (ForecastCohortValidationError, TypeError, ValueError) as exc:
        raise ForecastCalibrationEvidenceError("cohort proof is invalid") from exc
    if cohort.manifest != manifest:
        raise ForecastCalibrationEvidenceError("cohort proof content is inconsistent")
    return manifest


def _joined_observation(proof, *, cohort, committed) -> CalibrationObservation:
    try:
        response = _validated_run(proof.run)
        derived_member = member_from_scheduled_run(proof.run, step=proof.outcome.publication.step)
    except (
        ForecastRunValidationError,
        ForecastCohortValidationError,
        TypeError,
        ValueError,
    ) as exc:
        raise ForecastCalibrationEvidenceError("archived forecast evidence is invalid") from exc
    if derived_member != committed:
        raise ForecastCalibrationEvidenceError(
            "archived forecast does not match its committed cohort member"
        )
    if _utc(proof.run.recorded_at, "recorded_at") > _utc(
        cohort.record.recorded_at,
        "cohort recorded_at",
    ):
        raise ForecastCalibrationEvidenceError(
            "archived forecast was not persisted before the cohort manifest"
        )
    payload = _validated_outcome(proof.outcome, cohort=cohort, committed=committed)
    step = next(item for item in response.forecasts if item.step == committed.step)
    if (
        payload.symbol != response.symbol
        or payload.target != response.target
        or payload.series_basis != response.provenance.series_basis
        or payload.currency != response.currency
        or payload.target_time != _utc(step.target_time, "target_time")
    ):
        raise ForecastCalibrationEvidenceError(
            "realized outcome does not match the forecast semantic scope"
        )
    intervals = tuple(
        sorted(
            (_interval_observation(interval) for interval in step.intervals),
            key=lambda item: _coverage_millis(item.coverage),
        )
    )
    return CalibrationObservation(
        forecast_id=proof.run.forecast_id,
        outcome_id=proof.outcome.record.outcome_id,
        opportunity_hash=proof.run.opportunity_hash,
        output_hash=proof.run.output_hash,
        horizon=committed.step,
        target_time=_utc(step.target_time, "target_time"),
        model_version=response.provenance.model_version,
        source_calibration_set_version=response.calibration.calibration_set_version,
        source_calibration_method=response.calibration.method,
        forecast_resolution_policy_hash=proof.run.resolution_policy_hash,
        forecast_availability_rule_set_hash=proof.run.availability_rule_set_hash,
        symbol=response.symbol,
        target=response.target,
        series_basis=response.provenance.series_basis,
        horizon_unit=response.horizon_unit,
        currency=payload.currency,
        realized_value=_finite(payload.realized_value, "realized_value"),
        point=_finite(step.point, "point"),
        intervals=intervals,
    )


def _validated_run(run: ArchivedForecastRun) -> ForecastResponse:
    if not isinstance(run, ArchivedForecastRun):
        raise ForecastCalibrationEvidenceError("run has the wrong type")
    if (
        run.schema_version != RUN_SCHEMA_VERSION
        or run.origin_kind != "scheduled_evaluation"
        or run.idempotency_token_digest is not None
    ):
        raise ForecastCalibrationEvidenceError("run is not a supported scheduled archive")
    recorded = _utc(run.recorded_at, "recorded_at")
    generated = _utc(run.generated_at, "generated_at")
    if recorded < generated:
        raise ForecastCalibrationEvidenceError("run persistence stamp predates generation")
    request = parse_request(run.canonical_request)
    response = parse_output(run.canonical_output)
    if request_hash(run.canonical_request) != run.request_hash:
        raise ForecastCalibrationEvidenceError("run request hash is invalid")
    if output_hash(run.canonical_output) != run.output_hash:
        raise ForecastCalibrationEvidenceError("run output hash is invalid")
    if (
        response.symbol != request.symbol
        or response.target != request.target
        or response.horizon != request.horizon
        or response.horizon_unit != request.horizon_unit
        or (
            request.snapshot_id is not None
            and response.provenance.snapshot_id != request.snapshot_id
        )
        or (
            request.as_of is not None
            and _utc(response.as_of, "as_of") > _utc(request.as_of, "request as_of")
        )
        or any(
            {item.coverage for item in step.intervals} != set(request.interval_coverages)
            for step in response.forecasts
        )
    ):
        raise ForecastCalibrationEvidenceError("run request and output identities differ")
    expected_opportunity = opportunity_hash(
        response,
        resolution_policy_hash=run.resolution_policy_hash,
        availability_rule_set_hash=run.availability_rule_set_hash,
        origin_kind=run.origin_kind,
    )
    provenance = response.provenance
    feature_hash = provenance.feature_set_hash.lower()
    if not feature_hash.startswith("sha256:"):
        feature_hash = f"sha256:{feature_hash}"
    actual = (
        run.opportunity_hash,
        run.forecast_id,
        run.snapshot_id,
        run.symbol,
        run.target,
        run.horizon,
        run.horizon_unit,
        run.series_basis,
        _utc(run.as_of, "run as_of"),
        _utc(run.max_available_at, "max_available_at"),
        run.model_version,
        run.feature_set_hash,
        run.code_version,
        run.calibration_set_version,
        run.calibration_method,
        generated,
    )
    expected = (
        expected_opportunity,
        provenance.forecast_id,
        provenance.snapshot_id,
        response.symbol,
        response.target,
        response.horizon,
        response.horizon_unit,
        provenance.series_basis,
        _utc(response.as_of, "response as_of"),
        _utc(provenance.max_available_at, "provenance max_available_at"),
        provenance.model_version,
        feature_hash,
        provenance.code_version,
        response.calibration.calibration_set_version,
        response.calibration.method,
        _utc(provenance.generated_at, "provenance generated_at"),
    )
    if actual != expected:
        raise ForecastCalibrationEvidenceError("run headers do not match canonical output")
    return response


def _validated_outcome(proof: ForecastOutcomeProof, *, cohort, committed):
    if not isinstance(proof, ForecastOutcomeProof):
        raise ForecastCalibrationEvidenceError("outcome proof has the wrong type")
    try:
        payload = validate_outcome_record(
            proof.record,
            expected_outcome_resolution_policy_hash=(
                cohort.manifest.outcome_resolution_policy_hash
            ),
            expected_availability_rule_set_hash=cohort.manifest.availability_rule_set_hash,
        )
    except (OutcomeValidationError, TypeError, ValueError) as exc:
        raise ForecastCalibrationEvidenceError("realized outcome evidence is invalid") from exc
    publication = proof.publication
    if (
        proof.payload != payload
        or publication.outcome_id != proof.record.outcome_id
        or publication.cohort_id != cohort.record.cohort_id
        or publication.forecast_id != committed.forecast_id
        or publication.step != committed.step
        or type(publication.publisher_xid) is not int
        or publication.publisher_xid <= 0
        or _utc(publication.published_at, "published_at")
        < _utc(proof.record.sealed_at, "outcome sealed_at")
    ):
        raise ForecastCalibrationEvidenceError(
            "outcome publication does not match its committed cohort member"
        )
    return payload


def _interval_observation(interval) -> CalibrationIntervalObservation:
    coverage = _canonical_coverage(interval.coverage)
    lower_quantile = _finite(interval.lower_quantile, "lower_quantile")
    upper_quantile = _finite(interval.upper_quantile, "upper_quantile")
    expected_lower = (1.0 - coverage) / 2.0
    expected_upper = (1.0 + coverage) / 2.0
    if (
        abs(lower_quantile - expected_lower) > _COVERAGE_TOLERANCE
        or abs(upper_quantile - expected_upper) > _COVERAGE_TOLERANCE
    ):
        raise ForecastCalibrationEvidenceError(
            "calibration v1 requires central equal-tailed source intervals"
        )
    lower = _finite(interval.lower, "interval lower")
    upper = _finite(interval.upper, "interval upper")
    if lower > upper:
        raise ForecastCalibrationEvidenceError("source interval is inverted")
    return CalibrationIntervalObservation(
        coverage=coverage,
        lower_quantile=lower_quantile,
        upper_quantile=upper_quantile,
        lower=lower,
        upper=upper,
    )


def _fit_calibration_set(dataset, *, method: FitMethod, buckets) -> FittedCalibrationSet:
    dataset = _require_dataset(dataset, purpose="calibration_fit")
    specs = _normalized_fit_buckets(buckets)
    observed_horizons = {item.horizon for item in dataset.observations}
    specified_horizons = {item.horizon for item in specs}
    if specified_horizons != observed_horizons:
        raise ForecastCalibrationEvidenceError(
            "fit buckets must exactly cover every observed horizon"
        )
    specified_buckets = {(item.horizon, _coverage_millis(item.coverage)) for item in specs}
    for observation in dataset.observations:
        emitted = {
            (observation.horizon, _coverage_millis(item.coverage)) for item in observation.intervals
        }
        required = {item for item in specified_buckets if item[0] == observation.horizon}
        if emitted != required:
            raise ForecastCalibrationEvidenceError(
                "fit buckets must exactly match every prospectively emitted interval"
            )
    fitted: list[FittedCalibrationBucket] = []
    for spec in specs:
        population = [
            observation
            for observation in dataset.observations
            if observation.horizon == spec.horizon
        ]
        # Require the interval to have been emitted prospectively even for the
        # point-residual method; no new nominal bucket may be invented after truth.
        source_intervals = [_interval_for(item, spec.coverage) for item in population]
        try:
            if method == "empirical_residual":
                calibration = fit_absolute_residual(
                    [item.realized_value for item in population],
                    [item.point for item in population],
                    coverage=spec.coverage,
                )
                fitted.append(
                    FittedCalibrationBucket(horizon=spec.horizon, calibration=calibration)
                )
            else:
                cqr_calibration = fit_cqr(
                    [item.realized_value for item in population],
                    [item.lower for item in source_intervals],
                    [item.upper for item in source_intervals],
                    coverage=spec.coverage,
                )
                fitted.append(
                    FittedCalibrationBucket(
                        horizon=spec.horizon,
                        calibration=cqr_calibration,
                    )
                )
        except InsufficientCalibrationData as exc:
            raise InsufficientCalibrationFitData(
                "insufficient fit data for "
                f"horizon={spec.horizon}, coverage_millis={_coverage_millis(spec.coverage)}, "
                f"sample_count={len(population)}"
            ) from exc
        except (ConformalValidationError, TypeError, ValueError) as exc:
            raise ForecastCalibrationEvidenceError("conformal fitting failed") from exc
    targets = tuple(item.target_time for item in dataset.observations)
    candidate = FittedCalibrationSet(
        model_version=dataset.model_version,
        method=method,
        symbol=dataset.symbol,
        target=dataset.target,
        series_basis=dataset.series_basis,
        horizon_unit=dataset.horizon_unit,
        currency=dataset.currency,
        source_calibration_set_version=dataset.source_calibration_set_version,
        source_calibration_method=dataset.source_calibration_method,
        forecast_resolution_policy_hash=dataset.forecast_resolution_policy_hash,
        forecast_availability_rule_set_hash=dataset.forecast_availability_rule_set_hash,
        fit_evidence_digest=dataset.evidence_digest,
        interval_policy_version=CENTRAL_INTERVAL_POLICY_VERSION,
        window_start=min(targets).date(),
        window_end=max(targets).date(),
        sample_count=len(dataset.observations),
        cohort_id=dataset.cohort_id,
        selection_policy_hash=dataset.selection_policy_hash,
        outcome_resolution_policy_hash=dataset.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=(dataset.outcome_availability_rule_set_hash),
        buckets=tuple(fitted),
    )
    try:
        return parse_calibration_set(canonical_calibration_set(candidate))
    except (ForecastCalibrationSetValidationError, TypeError, ValueError) as exc:
        raise ForecastCalibrationEvidenceError("fitted calibration set is invalid") from exc


def _require_dataset(
    dataset: CalibrationEvidenceSet,
    *,
    purpose: CohortPurpose,
) -> CalibrationEvidenceSet:
    normalized = _normalized_dataset_shape(dataset, purpose=purpose)
    try:
        rejoined = join_calibration_evidence(
            normalized.source_cohort,
            normalized.source_members,
        )
    except (ForecastCalibrationEvidenceError, TypeError, ValueError) as exc:
        raise ForecastCalibrationEvidenceError("dataset source proofs no longer validate") from exc
    if normalized != rejoined:
        raise ForecastCalibrationEvidenceError(
            "dataset content differs from its exact source proofs"
        )
    return rejoined


def _normalized_dataset_shape(
    dataset: CalibrationEvidenceSet,
    *,
    purpose: CohortPurpose,
) -> CalibrationEvidenceSet:
    if not isinstance(dataset, CalibrationEvidenceSet):
        raise ForecastCalibrationEvidenceError("dataset has the wrong type")
    if dataset.purpose != purpose:
        raise ForecastCalibrationEvidenceError(f"dataset purpose must be {purpose}")
    if not dataset.observations:
        raise ForecastCalibrationEvidenceError("dataset must contain observations")
    if not isinstance(dataset.source_cohort, ForecastCohortProof):
        raise ForecastCalibrationEvidenceError("dataset source cohort has the wrong type")
    if not isinstance(dataset.source_members, tuple) or not dataset.source_members:
        raise ForecastCalibrationEvidenceError("dataset source members must be a nonempty tuple")
    _sha256(dataset.cohort_id, "cohort_id")
    _sha256(dataset.selection_policy_hash, "selection_policy_hash")
    _sha256(dataset.outcome_resolution_policy_hash, "outcome_resolution_policy_hash")
    _sha256(
        dataset.outcome_availability_rule_set_hash,
        "outcome_availability_rule_set_hash",
    )
    _sha256(dataset.evidence_digest, "evidence_digest")
    _sha256(dataset.forecast_resolution_policy_hash, "forecast_resolution_policy_hash")
    _sha256(
        dataset.forecast_availability_rule_set_hash,
        "forecast_availability_rule_set_hash",
    )
    if (
        dataset.target != "close"
        or dataset.series_basis != "raw"
        or dataset.horizon_unit != "trading_day"
        or dataset.currency != "USD"
    ):
        raise ForecastCalibrationEvidenceError(
            "calibration evidence v1 supports raw daily USD closes only"
        )
    if (
        not isinstance(dataset.symbol, str)
        or not dataset.symbol
        or dataset.symbol != dataset.symbol.upper()
        or not isinstance(dataset.model_version, str)
        or not dataset.model_version
    ):
        raise ForecastCalibrationEvidenceError("dataset semantic identity is invalid")
    if dataset.source_calibration_method != _UNCALIBRATED_METHOD or (
        dataset.source_calibration_set_version != f"uncalibrated:{dataset.model_version}"
    ):
        raise ForecastCalibrationEvidenceError(
            "calibration v1 requires uncalibrated source forecasts"
        )
    normalized_observations = tuple(_normalized_observation(item) for item in dataset.observations)
    keys = [(item.forecast_id, item.horizon) for item in normalized_observations]
    if len(set(keys)) != len(keys):
        raise ForecastCalibrationEvidenceError("dataset contains duplicate forecast members")
    opportunity_keys = [(item.opportunity_hash, item.horizon) for item in normalized_observations]
    if len(set(opportunity_keys)) != len(opportunity_keys):
        raise ForecastCalibrationEvidenceError("dataset contains duplicate forecast opportunities")
    scope = (
        dataset.symbol,
        dataset.target,
        dataset.series_basis,
        dataset.horizon_unit,
        dataset.currency,
        dataset.model_version,
        dataset.source_calibration_set_version,
        dataset.source_calibration_method,
        dataset.forecast_resolution_policy_hash,
        dataset.forecast_availability_rule_set_hash,
    )
    for observation in normalized_observations:
        if (
            observation.symbol,
            observation.target,
            observation.series_basis,
            observation.horizon_unit,
            observation.currency,
            observation.model_version,
            observation.source_calibration_set_version,
            observation.source_calibration_method,
            observation.forecast_resolution_policy_hash,
            observation.forecast_availability_rule_set_hash,
        ) != scope:
            raise ForecastCalibrationEvidenceError("dataset scope differs from its observations")
        if not observation.intervals:
            raise ForecastCalibrationEvidenceError("observation contains no intervals")
    ordered = tuple(sorted(normalized_observations, key=_observation_key))
    expected_digest = _evidence_digest(dataset.cohort_id, ordered)
    if dataset.evidence_digest != expected_digest:
        raise ForecastCalibrationEvidenceError(
            "dataset evidence_digest does not match its exact observations"
        )
    return replace(dataset, observations=ordered)


def _normalized_observation(observation: CalibrationObservation) -> CalibrationObservation:
    if not isinstance(observation, CalibrationObservation):
        raise ForecastCalibrationEvidenceError("dataset observation has the wrong type")
    if not isinstance(observation.forecast_id, UUID):
        raise ForecastCalibrationEvidenceError("observation forecast_id must be a UUID")
    _sha256(observation.outcome_id, "outcome_id")
    _sha256(observation.opportunity_hash, "opportunity_hash")
    _sha256(observation.output_hash, "output_hash")
    if type(observation.horizon) is not int or not 1 <= observation.horizon <= 252:
        raise ForecastCalibrationEvidenceError("observation horizon must be within 1..252")
    if not isinstance(observation.intervals, tuple) or not observation.intervals:
        raise ForecastCalibrationEvidenceError("observation intervals must be a nonempty tuple")
    intervals = tuple(
        sorted(
            (_normalized_interval(item) for item in observation.intervals),
            key=lambda item: _coverage_millis(item.coverage),
        )
    )
    coverages = [_coverage_millis(item.coverage) for item in intervals]
    if len(set(coverages)) != len(coverages):
        raise ForecastCalibrationEvidenceError("observation contains a duplicate interval")
    realized_value = _finite(observation.realized_value, "realized_value")
    if realized_value < 0.0:
        raise ForecastCalibrationEvidenceError("raw close realized_value must be nonnegative")
    return replace(
        observation,
        target_time=_utc(observation.target_time, "target_time"),
        realized_value=realized_value,
        point=_finite(observation.point, "point"),
        intervals=intervals,
    )


def _normalized_interval(
    interval: CalibrationIntervalObservation,
) -> CalibrationIntervalObservation:
    if not isinstance(interval, CalibrationIntervalObservation):
        raise ForecastCalibrationEvidenceError("observation interval has the wrong type")
    coverage = _canonical_coverage(interval.coverage)
    lower_quantile = _finite(interval.lower_quantile, "lower_quantile")
    upper_quantile = _finite(interval.upper_quantile, "upper_quantile")
    if (
        abs(lower_quantile - (1.0 - coverage) / 2.0) > _COVERAGE_TOLERANCE
        or abs(upper_quantile - (1.0 + coverage) / 2.0) > _COVERAGE_TOLERANCE
    ):
        raise ForecastCalibrationEvidenceError(
            "calibration v1 requires central equal-tailed source intervals"
        )
    lower = _finite(interval.lower, "interval lower")
    upper = _finite(interval.upper, "interval upper")
    if lower > upper:
        raise ForecastCalibrationEvidenceError("observation interval is inverted")
    return CalibrationIntervalObservation(
        coverage=coverage,
        lower_quantile=lower_quantile,
        upper_quantile=upper_quantile,
        lower=lower,
        upper=upper,
    )


def _normalized_fit_buckets(buckets: Sequence[CalibrationFitBucket]):
    if isinstance(buckets, (str, bytes)):
        raise ForecastCalibrationEvidenceError("fit buckets must be a sequence")
    try:
        supplied = tuple(buckets)
    except TypeError as exc:
        raise ForecastCalibrationEvidenceError("fit buckets must be a sequence") from exc
    if not supplied:
        raise ForecastCalibrationEvidenceError("at least one fit bucket is required")
    normalized: list[CalibrationFitBucket] = []
    for bucket in supplied:
        if not isinstance(bucket, CalibrationFitBucket):
            raise ForecastCalibrationEvidenceError("fit bucket has the wrong type")
        if type(bucket.horizon) is not int or not 1 <= bucket.horizon <= 252:
            raise ForecastCalibrationEvidenceError("fit bucket horizon must be within 1..252")
        normalized.append(
            CalibrationFitBucket(
                horizon=bucket.horizon,
                coverage=_canonical_coverage(bucket.coverage),
            )
        )
    keys = [(item.horizon, _coverage_millis(item.coverage)) for item in normalized]
    if len(set(keys)) != len(keys):
        raise ForecastCalibrationEvidenceError("fit buckets contain a duplicate")
    return tuple(sorted(normalized, key=lambda item: (item.horizon, item.coverage)))


def _require_heldout_compatibility(fitted_set, fit_dataset, heldout_dataset) -> None:
    if fit_dataset.cohort_id == heldout_dataset.cohort_id:
        raise ForecastCalibrationEvidenceError("fit and held-out cohorts must be distinct")
    fit_members = {item.forecast_id for item in fit_dataset.observations}
    heldout_members = {item.forecast_id for item in heldout_dataset.observations}
    fit_opportunities = {item.opportunity_hash for item in fit_dataset.observations}
    heldout_opportunities = {item.opportunity_hash for item in heldout_dataset.observations}
    fit_outcomes = {item.outcome_id for item in fit_dataset.observations}
    heldout_outcomes = {item.outcome_id for item in heldout_dataset.observations}
    fit_truth = {
        (
            item.symbol,
            item.target,
            item.series_basis,
            item.target_time,
            item.currency,
        )
        for item in fit_dataset.observations
    }
    heldout_truth = {
        (
            item.symbol,
            item.target,
            item.series_basis,
            item.target_time,
            item.currency,
        )
        for item in heldout_dataset.observations
    }
    if (
        fit_members & heldout_members
        or fit_opportunities & heldout_opportunities
        or fit_outcomes & heldout_outcomes
        or fit_truth & heldout_truth
    ):
        raise ForecastCalibrationEvidenceError("fit and held-out evidence overlap")
    expected_scope = (
        fitted_set.symbol,
        fitted_set.target,
        fitted_set.series_basis,
        fitted_set.horizon_unit,
        fitted_set.currency,
        fitted_set.model_version,
        fitted_set.outcome_resolution_policy_hash,
        fitted_set.outcome_availability_rule_set_hash,
        fitted_set.forecast_resolution_policy_hash,
        fitted_set.forecast_availability_rule_set_hash,
    )
    actual_scope = (
        heldout_dataset.symbol,
        heldout_dataset.target,
        heldout_dataset.series_basis,
        heldout_dataset.horizon_unit,
        heldout_dataset.currency,
        heldout_dataset.model_version,
        heldout_dataset.outcome_resolution_policy_hash,
        heldout_dataset.outcome_availability_rule_set_hash,
        heldout_dataset.forecast_resolution_policy_hash,
        heldout_dataset.forecast_availability_rule_set_hash,
    )
    if actual_scope != expected_scope:
        raise ForecastCalibrationEvidenceError(
            "held-out evidence does not match the fitted semantic or truth-policy scope"
        )
    expected_horizons = {bucket.horizon for bucket in fitted_set.buckets}
    actual_horizons = {item.horizon for item in heldout_dataset.observations}
    if actual_horizons != expected_horizons:
        raise ForecastCalibrationEvidenceError(
            "held-out evidence must exactly cover every fitted horizon"
        )
    expected_coverages = {
        (bucket.horizon, _coverage_millis(bucket.calibration.selection.coverage))
        for bucket in fitted_set.buckets
    }
    for observation in heldout_dataset.observations:
        actual_coverages = {
            (observation.horizon, _coverage_millis(interval.coverage))
            for interval in observation.intervals
        }
        required = {item for item in expected_coverages if item[0] == observation.horizon}
        if actual_coverages != required:
            raise ForecastCalibrationEvidenceError(
                "held-out intervals do not exactly match the fitted buckets"
            )


def _interval_for(observation, coverage: float) -> CalibrationIntervalObservation:
    millis = _coverage_millis(coverage)
    matches = [item for item in observation.intervals if _coverage_millis(item.coverage) == millis]
    if len(matches) != 1:
        raise ForecastCalibrationEvidenceError(
            "observation does not contain exactly one required interval"
        )
    return matches[0]


def wilson_interval(
    successes: int,
    sample_count: int,
    *,
    confidence_level: float,
) -> tuple[float, float]:
    """Return the deterministic two-sided Wilson score interval."""

    confidence = _open_probability(confidence_level, "confidence_level")
    if (
        type(successes) is not int
        or type(sample_count) is not int
        or not 0 <= successes <= sample_count
    ):
        raise ForecastCalibrationEvidenceError("Wilson counts are invalid")
    if sample_count <= 0:
        raise ForecastCalibrationEvidenceError("Wilson sample_count must be positive")
    try:
        # Computing the upper tail as ``0.5 + confidence / 2`` rounds to 1.0
        # for the largest representable confidence below one.  The equivalent
        # lower-tail form stays strictly inside NormalDist's open domain.
        z = -NormalDist().inv_cdf((1.0 - confidence) / 2.0)
    except StatisticsError as exc:
        raise ForecastCalibrationEvidenceError(
            "confidence_level is too close to a boundary"
        ) from exc
    if not math.isfinite(z):
        raise ForecastCalibrationEvidenceError("confidence_level produces a nonfinite score")
    proportion = successes / sample_count
    z2 = z * z
    denominator = 1.0 + z2 / sample_count
    center = (proportion + z2 / (2.0 * sample_count)) / denominator
    margin = (
        z
        * math.sqrt(
            proportion * (1.0 - proportion) / sample_count
            + z2 / (4.0 * sample_count * sample_count)
        )
        / denominator
    )
    low = max(0.0, center - margin)
    high = min(1.0, center + margin)
    if successes == 0:
        low = 0.0
    if successes == sample_count:
        high = 1.0
    return low, high


def _observation_key(item: CalibrationObservation):
    return item.target_time, str(item.forecast_id), item.horizon


def _evidence_digest(
    cohort_id: str,
    observations: Sequence[CalibrationObservation],
) -> str:
    document = {
        "cohort_id": _sha256(cohort_id, "cohort_id"),
        "format": CALIBRATION_EVIDENCE_FORMAT,
        "members": [
            {
                "forecast_id": str(item.forecast_id),
                "horizon": item.horizon,
                "opportunity_hash": item.opportunity_hash,
                "outcome_id": item.outcome_id,
                "output_hash": item.output_hash,
                "target_time": item.target_time.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
            }
            for item in observations
        ],
    }
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _coverage_millis(value: float) -> int:
    coverage = _finite(value, "coverage")
    millis = round(coverage * 1000)
    if not 1 <= millis <= 999 or abs(coverage - millis / 1000) > 1e-12:
        raise ForecastCalibrationEvidenceError("coverage must be a canonical thousandth")
    return millis


def _canonical_coverage(value: object) -> float:
    converted = _finite(value, "coverage")
    millis = _coverage_millis(converted)
    return millis / 1000


def _open_probability(value: object, label: str) -> float:
    converted = _finite(value, label)
    if not 0.0 < converted < 1.0:
        raise ForecastCalibrationEvidenceError(f"{label} must be strictly between zero and one")
    return converted


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ForecastCalibrationEvidenceError(f"{label} must be a canonical sha256 hash")
    return value


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ForecastCalibrationEvidenceError(f"{label} must be finite")
    converted = float(value)
    if not math.isfinite(converted):
        raise ForecastCalibrationEvidenceError(f"{label} must be finite")
    return converted


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ForecastCalibrationEvidenceError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "CENTRAL_INTERVAL_POLICY_VERSION",
    "CALIBRATION_EVIDENCE_FORMAT",
    "WILSON_COVERAGE_POLICY_VERSION",
    "CalibrationEvidenceSet",
    "CalibrationFitBucket",
    "CalibrationIntervalObservation",
    "CalibrationJoinMemberProof",
    "CalibrationObservation",
    "FitMethod",
    "ForecastCalibrationEvidenceError",
    "HeldoutCoverageBucket",
    "HeldoutCoverageEvidence",
    "InsufficientCalibrationFitData",
    "estimate_heldout_coverage",
    "fit_cqr_calibration_set",
    "fit_empirical_residual_calibration_set",
    "join_calibration_evidence",
    "wilson_interval",
]
