"""Exact-cover fitting and held-out evidence for conformal calibration."""

from __future__ import annotations

import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

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
from app.services.forecast_calibration_evidence import (
    CalibrationEvidenceSet,
    CalibrationFitBucket,
    CalibrationIntervalObservation,
    CalibrationJoinMemberProof,
    ForecastCalibrationEvidenceError,
    InsufficientCalibrationFitData,
    estimate_heldout_coverage,
    fit_cqr_calibration_set,
    fit_empirical_residual_calibration_set,
    join_calibration_evidence,
)
from app.services.forecast_calibration_sets import (
    calibration_set_version_for,
    canonical_calibration_set,
)
from app.services.forecast_cohort_store import ForecastCohortProof
from app.services.forecast_cohorts import (
    ForecastCohortManifest,
    ForecastCohortSeal,
    build_cohort_record,
    member_from_scheduled_run,
)
from app.services.forecast_outcome_store import (
    ForecastOutcomeProof,
    ForecastOutcomePublicationRecord,
)
from app.services.forecast_outcomes import (
    BarVersionEvidence,
    RealizedOutcomePayload,
    build_outcome_record,
)
from app.services.forecast_run_store import ArchivedForecastRun
from app.services.forecast_runs import (
    RUN_SCHEMA_VERSION,
    canonical_output,
    canonical_request,
    opportunity_hash,
    output_hash,
    request_hash,
)

BASE = datetime(2026, 1, 2, 20, tzinfo=UTC)
SNAPSHOT = "sha256:" + "6" * 64
FORECAST_POLICY = "sha256:" + "1" * 64
FORECAST_RULES = "sha256:" + "2" * 64
SELECTION = "sha256:" + "3" * 64
OUTCOME_POLICY = "sha256:" + "4" * 64
OUTCOME_RULES = "sha256:" + "5" * 64


def _hash(number: int) -> str:
    return f"sha256:{number:064x}"


def _response(
    index: int,
    *,
    point: float,
    coverage: float = 0.8,
    half_width: float = 2.0,
    identity_offset: int = 0,
    target_offset_days: int = 0,
) -> ForecastResponse:
    forecast_id = UUID(int=identity_offset + index + 1)
    target = BASE + timedelta(days=target_offset_days + index + 1)
    lower_quantile = (1.0 - coverage) / 2.0
    upper_quantile = (1.0 + coverage) / 2.0
    return ForecastResponse(
        symbol="MSFT",
        target="close",
        horizon=1,
        horizon_unit="trading_day",
        as_of=BASE,
        currency="USD",
        forecasts=[
            ForecastStep(
                step=1,
                target_time=target,
                point=point,
                quantiles=[
                    ForecastQuantile(level=lower_quantile, value=point - half_width),
                    ForecastQuantile(level=0.5, value=point),
                    ForecastQuantile(level=upper_quantile, value=point + half_width),
                ],
                intervals=[
                    ForecastInterval(
                        coverage=coverage,
                        lower_quantile=lower_quantile,
                        upper_quantile=upper_quantile,
                        lower=point - half_width,
                        upper=point + half_width,
                    )
                ],
            )
        ],
        provenance=ForecastProvenance(
            forecast_id=forecast_id,
            snapshot_id=SNAPSHOT,
            model_version="baseline-naive@1",
            series_basis="raw",
            feature_set_hash=SNAPSHOT,
            max_available_at=BASE,
            generated_at=BASE + timedelta(minutes=2),
            code_version="fixture-1",
            data_sources=[
                DataSourceLineage(
                    name="sealed_snapshot",
                    snapshot_id=SNAPSHOT,
                    max_available_at=BASE,
                    fields=["close"],
                )
            ],
            lookahead_check=LookaheadCheck(
                status="passed",
                checked_at=BASE + timedelta(minutes=2),
                max_feature_available_at=BASE,
            ),
        ),
        calibration=ForecastCalibration(
            calibration_set_version="uncalibrated:baseline-naive@1",
            method="none",
            sample_count=0,
        ),
    )


def _request(coverage: float = 0.8) -> ForecastRequest:
    return ForecastRequest(
        symbol="MSFT",
        horizon=1,
        horizon_unit="trading_day",
        target="close",
        snapshot_id=SNAPSHOT,
        model="baseline_naive",
        interval_coverages=[coverage],
    )


def _run(
    index: int,
    *,
    point: float,
    coverage: float = 0.8,
    half_width: float = 2.0,
    identity_offset: int = 0,
    target_offset_days: int = 0,
    forecast_resolution_policy_hash: str = FORECAST_POLICY,
    forecast_availability_rule_set_hash: str = FORECAST_RULES,
) -> ArchivedForecastRun:
    response = _response(
        index,
        point=point,
        coverage=coverage,
        half_width=half_width,
        identity_offset=identity_offset,
        target_offset_days=target_offset_days,
    )
    request_bytes = canonical_request(_request(coverage))
    output_bytes = canonical_output(response)
    return ArchivedForecastRun(
        forecast_id=response.provenance.forecast_id,
        schema_version=RUN_SCHEMA_VERSION,
        origin_kind="scheduled_evaluation",
        idempotency_token_digest=None,
        request_hash=request_hash(request_bytes),
        opportunity_hash=opportunity_hash(
            response,
            resolution_policy_hash=forecast_resolution_policy_hash,
            availability_rule_set_hash=forecast_availability_rule_set_hash,
            origin_kind="scheduled_evaluation",
        ),
        output_hash=output_hash(output_bytes),
        snapshot_id=SNAPSHOT,
        resolution_policy_hash=forecast_resolution_policy_hash,
        availability_rule_set_hash=forecast_availability_rule_set_hash,
        symbol="MSFT",
        target="close",
        horizon=1,
        horizon_unit="trading_day",
        series_basis="raw",
        as_of=BASE,
        max_available_at=BASE,
        model_version="baseline-naive@1",
        feature_set_hash=SNAPSHOT,
        code_version="fixture-1",
        calibration_set_version="uncalibrated:baseline-naive@1",
        calibration_method="none",
        generated_at=BASE + timedelta(minutes=2),
        recorded_at=BASE + timedelta(minutes=3),
        canonical_request=request_bytes,
        canonical_output=output_bytes,
    )


def _join_material(
    actuals: list[float],
    *,
    purpose: str = "calibration_fit",
    points: list[float] | None = None,
    coverage: float = 0.8,
    half_width: float = 2.0,
    identity_offset: int = 0,
    target_offset_days: int = 0,
    selection_policy_hash: str = SELECTION,
    outcome_resolution_policy_hash: str = OUTCOME_POLICY,
    availability_rule_set_hash: str = OUTCOME_RULES,
    forecast_resolution_policy_hashes: list[str] | None = None,
) -> tuple[ForecastCohortProof, tuple[CalibrationJoinMemberProof, ...]]:
    predicted = points or [100.0 + index for index in range(len(actuals))]
    forecast_policies = forecast_resolution_policy_hashes or [FORECAST_POLICY] * len(actuals)
    runs = tuple(
        _run(
            index,
            point=point,
            coverage=coverage,
            half_width=half_width,
            identity_offset=identity_offset,
            target_offset_days=target_offset_days,
            forecast_resolution_policy_hash=forecast_policy,
        )
        for index, (point, forecast_policy) in enumerate(
            zip(predicted, forecast_policies, strict=True)
        )
    )
    members = tuple(member_from_scheduled_run(run, step=1) for run in runs)
    manifest = ForecastCohortManifest(
        purpose=purpose,  # type: ignore[arg-type]
        selection_policy_hash=selection_policy_hash,
        outcome_resolution_policy_hash=outcome_resolution_policy_hash,
        availability_rule_set_hash=availability_rule_set_hash,
        members=members,
    )
    record = build_cohort_record(
        manifest,
        recorded_at=BASE + timedelta(minutes=4),
        creator_xid=101,
    )
    cohort = ForecastCohortProof(
        manifest=manifest,
        record=record,
        seal=ForecastCohortSeal(
            cohort_id=record.cohort_id,
            manifest_recorded_at=record.recorded_at,
            sealed_at=BASE + timedelta(minutes=5),
            sealer_xid=102,
        ),
    )
    proofs: list[CalibrationJoinMemberProof] = []
    for run, actual, member in zip(runs, actuals, members, strict=True):
        available = member.target_time + timedelta(minutes=4)
        cutoff = member.target_time + timedelta(days=1)
        source = BarVersionEvidence(
            symbol="MSFT",
            timespan="day",
            multiplier=1,
            observed_at=member.target_time,
            source="polygon_open_close",
            adjustment_basis="raw",
            fetched_at=member.target_time + timedelta(minutes=1),
            source_as_of=member.target_time + timedelta(minutes=2),
            version_recorded_at=member.target_time + timedelta(minutes=3),
            available_at=available,
            field="close",
            value=actual,
        )
        payload = RealizedOutcomePayload(
            outcome_resolution_policy_hash=outcome_resolution_policy_hash,
            availability_rule_set_hash=availability_rule_set_hash,
            resolution_cutoff=cutoff,
            symbol="MSFT",
            target="close",
            series_basis="raw",
            target_time=member.target_time,
            currency="USD",
            realized_value=actual,
            source_version=source,
        )
        outcome_record = build_outcome_record(
            payload,
            sealed_at=cutoff + timedelta(minutes=1),
        )
        proofs.append(
            CalibrationJoinMemberProof(
                run=run,
                outcome=ForecastOutcomeProof(
                    payload=payload,
                    record=outcome_record,
                    publication=ForecastOutcomePublicationRecord(
                        outcome_id=outcome_record.outcome_id,
                        cohort_id=record.cohort_id,
                        forecast_id=run.forecast_id,
                        step=1,
                        published_at=cutoff + timedelta(minutes=2),
                        publisher_xid=103,
                    ),
                ),
            )
        )
    return cohort, tuple(proofs)


def _dataset(
    purpose: str,
    *,
    actuals: list[float],
    points: list[float] | None = None,
    half_width: float = 2.0,
    coverage: float = 0.8,
    cohort_number: int,
    identity_offset: int = 0,
    target_offset_days: int | None = None,
    outcome_resolution_policy_hash: str = OUTCOME_POLICY,
    availability_rule_set_hash: str = OUTCOME_RULES,
) -> CalibrationEvidenceSet:
    cohort, members = _join_material(
        actuals,
        purpose=purpose,
        points=points,
        coverage=coverage,
        half_width=half_width,
        identity_offset=identity_offset,
        target_offset_days=(identity_offset if target_offset_days is None else target_offset_days),
        selection_policy_hash=_hash(cohort_number + 100),
        outcome_resolution_policy_hash=outcome_resolution_policy_hash,
        availability_rule_set_hash=availability_rule_set_hash,
    )
    return join_calibration_evidence(cohort, members)


def test_join_exactly_covers_and_normalizes_canonical_evidence() -> None:
    cohort, proofs = _join_material([101.0, 103.0, 104.0, 105.0])

    forward = join_calibration_evidence(cohort, proofs)
    reverse = join_calibration_evidence(cohort, tuple(reversed(proofs)))

    assert forward == reverse
    assert forward.purpose == "calibration_fit"
    assert forward.symbol == "MSFT"
    assert forward.series_basis == "raw"
    assert forward.evidence_digest.startswith("sha256:")
    assert [item.realized_value for item in forward.observations] == [101.0, 103.0, 104.0, 105.0]
    assert all(item.intervals[0].coverage == 0.8 for item in forward.observations)


@pytest.mark.parametrize("shape", ["missing", "duplicate", "extra"])
def test_join_rejects_any_non_exact_member_cover(shape: str) -> None:
    cohort, proofs = _join_material([101.0, 102.0, 103.0, 104.0])
    if shape == "missing":
        supplied = proofs[:-1]
    elif shape == "duplicate":
        supplied = (proofs[0], proofs[0], proofs[2], proofs[3])
    else:
        supplied = (*proofs, proofs[0])
    with pytest.raises(ForecastCalibrationEvidenceError):
        join_calibration_evidence(cohort, supplied)


def test_join_rejects_tampered_run_header_and_outcome_publication() -> None:
    cohort, proofs = _join_material([101.0, 102.0, 103.0, 104.0])
    bad_run = replace(proofs[0], run=replace(proofs[0].run, model_version="wrong@1"))
    with pytest.raises(ForecastCalibrationEvidenceError, match="archived forecast"):
        join_calibration_evidence(cohort, (bad_run, *proofs[1:]))

    publication = replace(proofs[0].outcome.publication, cohort_id=_hash(99))
    bad_outcome = replace(proofs[0], outcome=replace(proofs[0].outcome, publication=publication))
    with pytest.raises(ForecastCalibrationEvidenceError):
        join_calibration_evidence(cohort, (bad_outcome, *proofs[1:]))


def test_join_revalidates_prospective_timing_and_outcome_content() -> None:
    cohort, proofs = _join_material([101.0, 102.0, 103.0, 104.0])
    late_run = replace(
        proofs[0],
        run=replace(proofs[0].run, recorded_at=cohort.record.recorded_at + timedelta(seconds=1)),
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="cohort manifest"):
        join_calibration_evidence(cohort, (late_run, *proofs[1:]))

    tampered_record = replace(
        proofs[0].outcome.record,
        realized_value=proofs[0].outcome.record.realized_value + 1.0,
    )
    tampered = replace(
        proofs[0],
        outcome=replace(proofs[0].outcome, record=tampered_record),
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="outcome"):
        join_calibration_evidence(cohort, (tampered, *proofs[1:]))

    bad_cohort = replace(
        cohort,
        seal=replace(cohort.seal, sealer_xid=cohort.record.creator_xid),
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="cohort proof"):
        join_calibration_evidence(bad_cohort, proofs)


def test_join_rejects_mixed_forecast_policy_epochs() -> None:
    cohort, proofs = _join_material(
        [101.0, 102.0, 103.0, 104.0],
        forecast_resolution_policy_hashes=[
            FORECAST_POLICY,
            FORECAST_POLICY,
            _hash(777),
            FORECAST_POLICY,
        ],
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="mixes semantic scope"):
        join_calibration_evidence(cohort, proofs)


def test_empirical_fit_is_deterministic_and_uses_member_count_once() -> None:
    dataset = _dataset(
        "calibration_fit",
        actuals=[101.0, 98.0, 103.0, 96.0, 105.0],
        points=[100.0] * 5,
        cohort_number=1,
    )
    buckets = [CalibrationFitBucket(horizon=1, coverage=0.8)]

    fitted = fit_empirical_residual_calibration_set(dataset, buckets=buckets)
    shuffled = replace(dataset, observations=tuple(reversed(dataset.observations)))
    shuffled_fit = fit_empirical_residual_calibration_set(shuffled, buckets=buckets)

    assert fitted.sample_count == 5
    assert fitted.buckets[0].calibration.selection.sample_count == 5
    assert fitted.buckets[0].calibration.selection.rank == 5
    assert fitted.buckets[0].calibration.selection.value == 5.0
    assert fitted.symbol == "MSFT"
    assert fitted.series_basis == "raw"
    assert fitted.fit_evidence_digest == dataset.evidence_digest
    assert canonical_calibration_set(fitted) == canonical_calibration_set(shuffled_fit)


def test_signed_cqr_fit_retains_negative_correction() -> None:
    dataset = _dataset(
        "calibration_fit",
        actuals=[100.0, 101.0, 102.0, 103.0, 104.0],
        points=[100.0, 101.0, 102.0, 103.0, 104.0],
        half_width=2.0,
        cohort_number=2,
    )
    fitted = fit_cqr_calibration_set(
        dataset,
        buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
    )
    assert fitted.buckets[0].calibration.selection.value == -2.0


def test_fit_fails_closed_on_wrong_purpose_missing_bucket_and_small_sample() -> None:
    heldout = _dataset(
        "heldout_evaluation",
        actuals=[100.0] * 5,
        cohort_number=3,
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="purpose"):
        fit_empirical_residual_calibration_set(
            heldout,
            buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
        )

    fit = _dataset(
        "calibration_fit",
        actuals=[100.0] * 5,
        cohort_number=31,
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="exactly cover"):
        fit_empirical_residual_calibration_set(
            fit,
            buckets=[CalibrationFitBucket(horizon=2, coverage=0.8)],
        )

    too_small = _dataset(
        "calibration_fit",
        actuals=[100.0, 100.0, 100.0, 100.0],
        coverage=0.9,
        cohort_number=4,
    )
    with pytest.raises(InsufficientCalibrationFitData, match="sample_count=4"):
        fit_empirical_residual_calibration_set(
            too_small,
            buckets=[CalibrationFitBucket(horizon=1, coverage=0.9)],
        )


def test_fit_rejects_scope_that_outcome_v1_cannot_prove() -> None:
    dataset = _dataset(
        "calibration_fit",
        actuals=[100.0] * 5,
        cohort_number=40,
    )
    adjusted = replace(
        dataset,
        target="adjusted_close",
        series_basis="split_dividend_adjusted",
        observations=tuple(
            replace(
                item,
                target="adjusted_close",
                series_basis="split_dividend_adjusted",
            )
            for item in dataset.observations
        ),
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="raw daily USD"):
        fit_empirical_residual_calibration_set(
            adjusted,
            buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
        )


def test_fit_revalidates_join_proofs_instead_of_trusting_detached_values() -> None:
    dataset = _dataset(
        "calibration_fit",
        actuals=[100.0] * 5,
        cohort_number=42,
    )
    first = dataset.observations[0]
    forged = replace(
        dataset,
        observations=(
            replace(first, realized_value=1_000.0),
            *dataset.observations[1:],
        ),
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="source proofs"):
        fit_empirical_residual_calibration_set(
            forged,
            buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
        )

    negative = replace(
        dataset,
        observations=(replace(first, realized_value=-1.0), *dataset.observations[1:]),
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="nonnegative"):
        fit_empirical_residual_calibration_set(
            negative,
            buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
        )


def test_fit_rejects_posthoc_bucket_selection_and_noncentral_intervals() -> None:
    dataset = _dataset(
        "calibration_fit",
        actuals=[100.0] * 5,
        cohort_number=41,
    )
    extra_interval = CalibrationIntervalObservation(
        coverage=0.5,
        lower_quantile=0.25,
        upper_quantile=0.75,
        lower=99.0,
        upper=101.0,
    )
    with_extra = replace(
        dataset,
        observations=tuple(
            replace(item, intervals=(*item.intervals, extra_interval))
            for item in dataset.observations
        ),
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="source proofs"):
        fit_empirical_residual_calibration_set(
            with_extra,
            buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
        )

    first = dataset.observations[0]
    noncentral = replace(
        first.intervals[0],
        lower_quantile=0.05,
        upper_quantile=0.85,
    )
    malformed = replace(
        dataset,
        observations=(replace(first, intervals=(noncentral,)), *dataset.observations[1:]),
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="central equal-tailed"):
        fit_empirical_residual_calibration_set(
            malformed,
            buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
        )


def test_heldout_estimator_applies_fit_and_reports_wilson_evidence() -> None:
    fit = _dataset(
        "calibration_fit",
        actuals=[101.0, 99.0, 101.0, 99.0, 101.0],
        points=[100.0] * 5,
        cohort_number=5,
    )
    fitted = fit_empirical_residual_calibration_set(
        fit,
        buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
    )
    heldout = _dataset(
        "heldout_evaluation",
        actuals=[101.0] * 8 + [102.0, 98.0],
        points=[100.0] * 10,
        cohort_number=6,
        identity_offset=100,
    )

    evidence = estimate_heldout_coverage(
        fitted,
        fit_dataset=fit,
        heldout_dataset=heldout,
        confidence_level=0.95,
    )

    bucket = evidence.buckets[0]
    assert evidence.fitted_calibration_set_version == calibration_set_version_for(fitted)
    assert evidence.fit_cohort_id == fit.cohort_id
    assert evidence.heldout_cohort_id == heldout.cohort_id
    assert evidence.fit_evidence_digest == fit.evidence_digest
    assert evidence.heldout_evidence_digest == heldout.evidence_digest
    assert bucket.covered_count == 8
    assert bucket.sample_count == 10
    assert bucket.empirical_coverage == 0.8
    assert bucket.confidence_low == pytest.approx(0.4901624715)
    assert bucket.confidence_high == pytest.approx(0.9433178485)


@pytest.mark.parametrize(
    ("actuals", "expected_hits", "expected_boundary"),
    [([100.0] * 5, 5, 1.0), ([110.0] * 5, 0, 0.0)],
)
def test_wilson_boundaries_are_exact(
    actuals: list[float], expected_hits: int, expected_boundary: float
) -> None:
    fit = _dataset(
        "calibration_fit",
        actuals=[100.0] * 5,
        points=[100.0] * 5,
        cohort_number=7,
    )
    fitted = fit_empirical_residual_calibration_set(
        fit,
        buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
    )
    heldout = _dataset(
        "heldout_evaluation",
        actuals=actuals,
        points=[100.0] * 5,
        cohort_number=8,
        identity_offset=200,
    )
    bucket = estimate_heldout_coverage(
        fitted,
        fit_dataset=fit,
        heldout_dataset=heldout,
    ).buckets[0]
    assert bucket.covered_count == expected_hits
    if expected_hits:
        assert bucket.confidence_high == expected_boundary
    else:
        assert bucket.confidence_low == expected_boundary


def test_heldout_rejects_overlap_scope_drift_and_forged_fit() -> None:
    fit = _dataset(
        "calibration_fit",
        actuals=[100.0] * 5,
        cohort_number=9,
    )
    fitted = fit_empirical_residual_calibration_set(
        fit,
        buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
    )
    overlapping = _dataset(
        "heldout_evaluation",
        actuals=[100.0] * 5,
        cohort_number=10,
        target_offset_days=0,
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="overlap"):
        estimate_heldout_coverage(
            fitted,
            fit_dataset=fit,
            heldout_dataset=overlapping,
        )

    heldout = _dataset(
        "heldout_evaluation",
        actuals=[100.0] * 5,
        cohort_number=11,
        identity_offset=300,
    )
    drifted = _dataset(
        "heldout_evaluation",
        actuals=[100.0] * 5,
        cohort_number=11,
        identity_offset=300,
        outcome_resolution_policy_hash=_hash(999),
    )
    with pytest.raises(ForecastCalibrationEvidenceError, match="scope"):
        estimate_heldout_coverage(
            fitted,
            fit_dataset=fit,
            heldout_dataset=drifted,
        )

    forged_bucket = replace(
        fitted.buckets[0],
        calibration=replace(
            fitted.buckets[0].calibration,
            selection=replace(fitted.buckets[0].calibration.selection, value=9.0),
        ),
    )
    forged = replace(fitted, buckets=(forged_bucket,))
    with pytest.raises(ForecastCalibrationEvidenceError, match="reproducible"):
        estimate_heldout_coverage(
            forged,
            fit_dataset=fit,
            heldout_dataset=heldout,
        )


def test_signed_cqr_correction_is_applied_on_heldout_rows() -> None:
    fit = _dataset(
        "calibration_fit",
        actuals=[100.0] * 5,
        points=[100.0] * 5,
        half_width=2.0,
        cohort_number=12,
    )
    fitted = fit_cqr_calibration_set(
        fit,
        buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
    )
    heldout = _dataset(
        "heldout_evaluation",
        actuals=[100.0] * 5,
        points=[100.0] * 5,
        half_width=2.0,
        cohort_number=13,
        identity_offset=400,
    )
    bucket = estimate_heldout_coverage(
        fitted,
        fit_dataset=fit,
        heldout_dataset=heldout,
    ).buckets[0]
    # A -2 correction shrinks [98, 102] to the inclusive singleton [100, 100].
    assert fitted.buckets[0].calibration.selection.value == -2.0
    assert bucket.covered_count == 5


def test_extreme_valid_confidence_uses_stable_lower_tail() -> None:
    fit = _dataset(
        "calibration_fit",
        actuals=[100.0] * 5,
        cohort_number=50,
    )
    fitted = fit_empirical_residual_calibration_set(
        fit,
        buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
    )
    heldout = _dataset(
        "heldout_evaluation",
        actuals=[100.0] * 5,
        cohort_number=51,
        identity_offset=500,
    )
    evidence = estimate_heldout_coverage(
        fitted,
        fit_dataset=fit,
        heldout_dataset=heldout,
        confidence_level=math.nextafter(1.0, 0.0),
    )
    assert math.isfinite(evidence.buckets[0].confidence_low)
    assert math.isfinite(evidence.buckets[0].confidence_high)
