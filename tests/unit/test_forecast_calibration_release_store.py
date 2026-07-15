"""Persistence-boundary tests for descriptive calibration releases."""

from __future__ import annotations

import struct
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

import app.services.forecast_calibration_release_store as store_module
from app.core.exceptions import AppError
from app.services.forecast_calibration_evidence import (
    CalibrationFitBucket,
    fit_empirical_residual_calibration_set,
)
from app.services.forecast_calibration_release_store import (
    FittedCalibrationSetRecord,
    HeldoutCoverageReleaseAvailability,
    HeldoutCoverageReleaseBucketRecord,
    HeldoutCoverageReleaseRecord,
    SqlHeldoutCoverageReleaseStore,
    _prepare_release,
    _release_identity,
    _StoredRelease,
    _validate_stored,
)
from app.services.forecast_calibration_releases import (
    HELDOUT_COVERAGE_RELEASE_SCHEMA_VERSION,
    HELDOUT_COVERAGE_RELEASE_SCOPE,
)
from app.services.forecast_calibration_sets import calibration_set_version_for
from tests.unit.test_forecast_calibration_evidence import _dataset


def _material():
    fit = _dataset(
        "calibration_fit",
        actuals=[101.0, 99.0, 101.0, 99.0, 101.0],
        points=[100.0] * 5,
        cohort_number=801,
    )
    fitted = fit_empirical_residual_calibration_set(
        fit,
        buckets=[CalibrationFitBucket(horizon=1, coverage=0.8)],
    )
    heldout = _dataset(
        "heldout_evaluation",
        actuals=[101.0, 101.0, 101.0, 102.0, 98.0],
        points=[100.0] * 5,
        cohort_number=802,
        identity_offset=600,
    )
    prepared = _prepare_release(
        fitted,
        fit_dataset=fit,
        heldout_dataset=heldout,
        confidence_level=0.95,
    )
    return fit, heldout, prepared


def _stored(*, with_receipt: bool = True) -> _StoredRelease:
    _fit, _heldout, prepared = _material()
    fitted = prepared.release.fitted_set
    evidence = prepared.release.evidence
    recorded_at = datetime(2026, 7, 15, 10, tzinfo=UTC)
    set_record = FittedCalibrationSetRecord(
        calibration_set_version=calibration_set_version_for(prepared.canonical_set),
        schema_version=fitted.schema_version,
        model_version=fitted.model_version,
        symbol=fitted.symbol,
        target=fitted.target,
        series_basis=fitted.series_basis,
        horizon_unit=fitted.horizon_unit,
        currency=fitted.currency,
        source_calibration_set_version=fitted.source_calibration_set_version,
        source_calibration_method=fitted.source_calibration_method,
        forecast_resolution_policy_hash=fitted.forecast_resolution_policy_hash,
        forecast_availability_rule_set_hash=(fitted.forecast_availability_rule_set_hash),
        fit_evidence_digest=fitted.fit_evidence_digest,
        method=fitted.method,
        window_start=fitted.window_start,
        window_end=fitted.window_end,
        sample_count=fitted.sample_count,
        cohort_id=fitted.cohort_id,
        selection_policy_hash=fitted.selection_policy_hash,
        outcome_resolution_policy_hash=fitted.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=(fitted.outcome_availability_rule_set_hash),
        interval_policy_version=fitted.interval_policy_version,
        window_date_policy_version=fitted.window_date_policy_version,
        bucket_count=len(fitted.buckets),
        recorded_at=recorded_at,
        creator_xid=101,
        canonical_set=prepared.canonical_set,
    )
    release_record = HeldoutCoverageReleaseRecord(
        release_id=prepared.release.release_id,
        schema_version=HELDOUT_COVERAGE_RELEASE_SCHEMA_VERSION,
        evidence_scope=HELDOUT_COVERAGE_RELEASE_SCOPE,
        fitted_calibration_set_version=evidence.fitted_calibration_set_version,
        method=evidence.method,
        model_version=evidence.model_version,
        symbol=evidence.symbol,
        target=evidence.target,
        series_basis=evidence.series_basis,
        horizon_unit=evidence.horizon_unit,
        currency=evidence.currency,
        fit_cohort_id=evidence.fit_cohort_id,
        fit_selection_policy_hash=evidence.fit_selection_policy_hash,
        heldout_cohort_id=evidence.heldout_cohort_id,
        heldout_selection_policy_hash=evidence.heldout_selection_policy_hash,
        outcome_resolution_policy_hash=evidence.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=evidence.outcome_availability_rule_set_hash,
        forecast_resolution_policy_hash=evidence.forecast_resolution_policy_hash,
        forecast_availability_rule_set_hash=evidence.forecast_availability_rule_set_hash,
        fit_evidence_digest=evidence.fit_evidence_digest,
        heldout_evidence_digest=evidence.heldout_evidence_digest,
        heldout_window_start=evidence.heldout_window_start,
        heldout_window_end=evidence.heldout_window_end,
        heldout_sample_count=evidence.heldout_sample_count,
        confidence_level_f64_be=struct.pack(">d", evidence.confidence_level),
        interval_policy_version=evidence.interval_policy_version,
        window_date_policy_version=evidence.window_date_policy_version,
        estimator_policy_version=evidence.estimator_policy_version,
        bucket_count=len(evidence.buckets),
        recorded_at=recorded_at + timedelta(seconds=1),
        creator_xid=102,
        canonical_release=prepared.release.canonical_release,
    )
    buckets = tuple(
        HeldoutCoverageReleaseBucketRecord(
            release_id=prepared.release.release_id,
            horizon=bucket.horizon,
            coverage_millis=round(bucket.nominal_coverage * 1000),
            covered_count=bucket.covered_count,
            sample_count=bucket.sample_count,
            empirical_coverage_f64_be=struct.pack(">d", bucket.empirical_coverage),
            confidence_low_f64_be=struct.pack(">d", bucket.confidence_low),
            confidence_high_f64_be=struct.pack(">d", bucket.confidence_high),
        )
        for bucket in evidence.buckets
    )
    receipt = (
        HeldoutCoverageReleaseAvailability(
            release_id=prepared.release.release_id,
            release_recorded_at=release_record.recorded_at,
            available_at=release_record.recorded_at + timedelta(seconds=1),
            sealer_xid=103,
        )
        if with_receipt
        else None
    )
    return _StoredRelease(
        set_record=set_record,
        release_record=release_record,
        bucket_records=buckets,
        availability=receipt,
    )


def test_stored_release_round_trips_every_projection_and_receipt() -> None:
    stored = _stored()

    proof = _validate_stored(stored)

    assert proof.release.release_id == stored.release_record.release_id
    assert proof.set_record == stored.set_record
    assert proof.release_record == stored.release_record
    assert proof.bucket_records == stored.bucket_records
    assert proof.availability == stored.availability


@pytest.mark.parametrize("projection", ["set", "release", "bucket", "receipt"])
def test_projection_drift_is_rejected(projection: str) -> None:
    stored = _stored()
    if projection == "set":
        stored = replace(
            stored,
            set_record=replace(stored.set_record, sample_count=999),
        )
    elif projection == "release":
        stored = replace(
            stored,
            release_record=replace(stored.release_record, symbol="AAPL"),
        )
    elif projection == "bucket":
        stored = replace(
            stored,
            bucket_records=(replace(stored.bucket_records[0], covered_count=0),),
        )
    else:
        assert stored.availability is not None
        stored = replace(
            stored,
            availability=replace(
                stored.availability,
                sealer_xid=stored.release_record.creator_xid,
            ),
        )

    with pytest.raises(ValueError):
        _validate_stored(stored)


def test_missing_receipt_is_not_a_valid_public_release() -> None:
    with pytest.raises(ValueError, match="availability"):
        _validate_stored(_stored(with_receipt=False))


def test_release_identity_preflight_is_strict_and_redacted() -> None:
    with pytest.raises(AppError) as caught:
        _release_identity("not-a-release")

    assert caught.value.code == "forecast_calibration_release_request_invalid"
    assert caught.value.status_code == 422
    assert caught.value.details == {"retryable": False}


def test_prepare_recomputes_from_exact_fit_and_heldout_proofs() -> None:
    fit, heldout, prepared = _material()
    forged = replace(fit, evidence_digest="sha256:" + "f" * 64)

    with pytest.raises(ValueError, match="evidence_digest"):
        _prepare_release(
            prepared.release.fitted_set,
            fit_dataset=forged,
            heldout_dataset=heldout,
            confidence_level=0.95,
        )


class _Result:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one(self) -> object:
        return self.value


class _Transaction:
    async def __aenter__(self) -> _Transaction:
        return self

    async def __aexit__(self, *_error: object) -> bool:
        return False


class _Session:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = iter(outcomes)
        self.calls: list[tuple[str, object | None]] = []

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_error: object) -> bool:
        return False

    def begin(self) -> _Transaction:
        return _Transaction()

    async def execute(self, statement: object, params: object | None = None) -> _Result:
        self.calls.append((str(statement), params))
        outcome = next(self.outcomes)
        if isinstance(outcome, BaseException):
            raise outcome
        return _Result(outcome)


class _Maker:
    def __init__(self, sessions: list[_Session]) -> None:
        self.sessions = iter(sessions)
        self.open_count = 0

    def __call__(self) -> _Session:
        self.open_count += 1
        return next(self.sessions)


async def test_content_and_receipt_use_distinct_short_transactions() -> None:
    _fit, _heldout, prepared = _material()
    content = _Session(
        [
            calibration_set_version_for(prepared.canonical_set),
            prepared.release.release_id,
        ]
    )
    receipt = _Session([prepared.release.release_id])
    maker = _Maker([content, receipt])
    store = SqlHeldoutCoverageReleaseStore(
        sessionmaker=cast(Any, maker),
    )

    await store._publish_content(prepared)
    await store._publish_receipt(prepared.release.release_id)

    assert maker.open_count == 2
    assert "publish_fitted_calibration_set" in content.calls[0][0]
    assert content.calls[0][1] == {"canonical_set": prepared.canonical_set}
    assert "publish_forecast_heldout_coverage_release" in content.calls[1][0]
    assert content.calls[1][1] == {"canonical_release": prepared.release.canonical_release}
    assert "publish_forecast_heldout_coverage_release_receipt" in receipt.calls[0][0]
    assert receipt.calls[0][1] == {"release_id": prepared.release.release_id}


async def test_publish_finishes_proof_preparation_before_opening_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fit, heldout, prepared = _material()
    fitted = prepared.release.fitted_set
    maker = _Maker([])
    store = SqlHeldoutCoverageReleaseStore(sessionmaker=cast(Any, maker))
    read_evidence = AsyncMock(side_effect=[fit, heldout])

    def guarded_prepare(*_args: object, **_kwargs: object):
        assert maker.open_count == 0
        assert read_evidence.await_count == 2
        return prepared

    publish_content = AsyncMock()
    publish_receipt = AsyncMock()
    read_validated = AsyncMock(return_value=_validate_stored(_stored()))
    monkeypatch.setattr(
        store_module.SqlForecastCalibrationEvidenceReader,
        "read_validated",
        read_evidence,
    )
    monkeypatch.setattr(store_module, "_prepare_release", guarded_prepare)
    monkeypatch.setattr(SqlHeldoutCoverageReleaseStore, "_publish_content", publish_content)
    monkeypatch.setattr(SqlHeldoutCoverageReleaseStore, "_publish_receipt", publish_receipt)
    monkeypatch.setattr(SqlHeldoutCoverageReleaseStore, "read_validated", read_validated)

    result = await store.publish(
        fitted,
        heldout_cohort_id=heldout.cohort_id,
        confidence_level=0.95,
    )

    assert result.release.release_id == prepared.release.release_id
    assert maker.open_count == 0
    publish_content.assert_awaited_once_with(prepared)
    publish_receipt.assert_awaited_once_with(prepared.release.release_id)


async def test_publish_reloads_trusted_fit_and_commits_nothing_when_it_does_not_reproduce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fit, heldout, prepared = _material()
    divergent_fit = _dataset(
        "calibration_fit",
        actuals=[105.0, 99.0, 101.0, 99.0, 101.0],
        points=[100.0] * 5,
        cohort_number=801,
    )
    store = SqlHeldoutCoverageReleaseStore(sessionmaker=cast(Any, _Maker([])))
    publish_content = AsyncMock()
    publish_receipt = AsyncMock()
    monkeypatch.setattr(
        store_module.SqlForecastCalibrationEvidenceReader,
        "read_validated",
        AsyncMock(side_effect=[divergent_fit, heldout]),
    )
    monkeypatch.setattr(SqlHeldoutCoverageReleaseStore, "_publish_content", publish_content)
    monkeypatch.setattr(SqlHeldoutCoverageReleaseStore, "_publish_receipt", publish_receipt)

    with pytest.raises(AppError) as caught:
        await store.publish(
            prepared.release.fitted_set,
            heldout_cohort_id=heldout.cohort_id,
            confidence_level=0.95,
        )

    assert caught.value.code == "forecast_calibration_release_invalid"
    publish_content.assert_not_awaited()
    publish_receipt.assert_not_awaited()


async def test_read_validated_reloads_both_cohorts_and_reproduces_exact_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fit, heldout, prepared = _material()
    stored = _stored()
    store = SqlHeldoutCoverageReleaseStore(sessionmaker=cast(Any, _Maker([])))
    read_stored = AsyncMock(return_value=stored)
    read_evidence = AsyncMock(side_effect=[fit, heldout])
    monkeypatch.setattr(SqlHeldoutCoverageReleaseStore, "_read_stored", read_stored)
    monkeypatch.setattr(
        store_module.SqlForecastCalibrationEvidenceReader,
        "read_validated",
        read_evidence,
    )

    proof = await store.read_validated(prepared.release.release_id)

    assert proof == _validate_stored(stored)
    read_stored.assert_awaited_once_with(prepared.release.release_id)
    assert [call.args[0] for call in read_evidence.await_args_list] == [
        fit.cohort_id,
        heldout.cohort_id,
    ]


async def test_read_validated_rejects_source_evidence_that_no_longer_reproduces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fit, _heldout, prepared = _material()
    divergent_heldout = _dataset(
        "heldout_evaluation",
        actuals=[103.0, 101.0, 101.0, 102.0, 98.0],
        points=[100.0] * 5,
        cohort_number=802,
        identity_offset=600,
    )
    store = SqlHeldoutCoverageReleaseStore(sessionmaker=cast(Any, _Maker([])))
    monkeypatch.setattr(
        SqlHeldoutCoverageReleaseStore,
        "_read_stored",
        AsyncMock(return_value=_stored()),
    )
    monkeypatch.setattr(
        store_module.SqlForecastCalibrationEvidenceReader,
        "read_validated",
        AsyncMock(side_effect=[fit, divergent_heldout]),
    )

    with pytest.raises(AppError) as caught:
        await store.read_validated(prepared.release.release_id)

    assert caught.value.code == "forecast_calibration_release_corrupt"
    assert caught.value.details == {"retryable": False}


class _DriverFailure(RuntimeError):
    def __init__(
        self,
        sqlstate: str,
        *,
        constraint_name: str | None = None,
    ) -> None:
        super().__init__("redacted driver failure")
        self.sqlstate = sqlstate
        self.constraint_name = constraint_name


def _operational(sqlstate: str) -> OperationalError:
    return OperationalError("secret statement", {}, _DriverFailure(sqlstate))


def _integrity(sqlstate: str, *, constraint_name: str | None = None) -> IntegrityError:
    return IntegrityError(
        "secret statement",
        {},
        _DriverFailure(sqlstate, constraint_name=constraint_name),
    )


@pytest.mark.parametrize("stage", ["content", "receipt"])
async def test_unknown_commit_reconciles_only_when_exact_stage_is_visible(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    _fit, _heldout, prepared = _material()
    store = SqlHeldoutCoverageReleaseStore(
        sessionmaker=cast(Any, _Maker([_Session([_operational("40003")])])),
    )
    content_matches = AsyncMock(return_value=True)
    receipt_visible = AsyncMock(return_value=True)
    monkeypatch.setattr(SqlHeldoutCoverageReleaseStore, "_content_matches", content_matches)
    monkeypatch.setattr(SqlHeldoutCoverageReleaseStore, "_receipt_visible", receipt_visible)

    if stage == "content":
        await store._publish_content(prepared)
        content_matches.assert_awaited_once_with(prepared)
        receipt_visible.assert_not_awaited()
    else:
        await store._publish_receipt(prepared.release.release_id)
        receipt_visible.assert_awaited_once_with(prepared.release.release_id)
        content_matches.assert_not_awaited()


@pytest.mark.parametrize("stage", ["content", "receipt"])
async def test_unknown_invisible_commit_reports_redacted_retryable_outcome(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    _fit, _heldout, prepared = _material()
    store = SqlHeldoutCoverageReleaseStore(
        sessionmaker=cast(Any, _Maker([_Session([_operational("40003")])])),
    )
    monkeypatch.setattr(
        SqlHeldoutCoverageReleaseStore,
        "_content_matches",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        SqlHeldoutCoverageReleaseStore,
        "_receipt_visible",
        AsyncMock(return_value=False),
    )

    with pytest.raises(AppError) as caught:
        if stage == "content":
            await store._publish_content(prepared)
        else:
            await store._publish_receipt(prepared.release.release_id)

    assert caught.value.code == "forecast_calibration_release_outcome_unknown"
    assert caught.value.details == {
        "outcome_unknown": True,
        "retryable": True,
        "stage": stage,
    }
    assert "secret" not in str(caught.value)


@pytest.mark.parametrize("stage", ["content", "receipt"])
async def test_deterministic_database_rejection_is_a_nonretryable_conflict(
    stage: str,
) -> None:
    _fit, _heldout, prepared = _material()
    store = SqlHeldoutCoverageReleaseStore(
        sessionmaker=cast(Any, _Maker([_Session([_integrity("23514")])])),
    )

    with pytest.raises(AppError) as caught:
        if stage == "content":
            await store._publish_content(prepared)
        else:
            await store._publish_receipt(prepared.release.release_id)

    assert caught.value.code == "forecast_calibration_release_conflict"
    assert caught.value.status_code == 409
    assert caught.value.details == {"retryable": False}


async def test_database_configuration_failure_is_redacted_and_nonretryable() -> None:
    _fit, _heldout, prepared = _material()
    store = SqlHeldoutCoverageReleaseStore(
        sessionmaker=cast(Any, _Maker([_Session([_operational("42P01")])])),
    )

    with pytest.raises(AppError) as caught:
        await store._publish_content(prepared)

    assert caught.value.code == "forecast_calibration_release_configuration_invalid"
    assert caught.value.status_code == 500
    assert caught.value.details == {"retryable": False}
    assert "secret" not in str(caught.value)


async def test_contradictory_fit_for_same_cohort_and_method_is_a_conflict() -> None:
    _fit, _heldout, prepared = _material()
    store = SqlHeldoutCoverageReleaseStore(
        sessionmaker=cast(
            Any,
            _Maker(
                [
                    _Session(
                        [
                            _integrity(
                                "23505",
                                constraint_name="uq_fitted_calibration_sets_cohort_method",
                            )
                        ]
                    )
                ]
            ),
        ),
    )

    with pytest.raises(AppError) as caught:
        await store._publish_content(prepared)

    assert caught.value.code == "forecast_calibration_release_conflict"
    assert caught.value.details == {"retryable": False}
