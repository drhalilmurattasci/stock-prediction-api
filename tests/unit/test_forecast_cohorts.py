from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from app.schemas.forecast import (
    DataSourceLineage,
    ForecastCalibration,
    ForecastInterval,
    ForecastProvenance,
    ForecastQuantile,
    ForecastResponse,
    ForecastStep,
    LookaheadCheck,
)
from app.services.forecast_cohorts import (
    COHORT_FORMAT,
    ForecastCohortManifest,
    ForecastCohortSeal,
    ForecastCohortValidationError,
    build_cohort_record,
    canonical_cohort_manifest,
    cohort_id_for_manifest,
    member_from_scheduled_run,
    parse_cohort_manifest,
    validate_cohort_record,
    validate_cohort_seal,
)
from app.services.forecast_runs import canonical_output, output_hash

AS_OF = datetime(2026, 7, 14, 17, tzinfo=UTC)
SELECTION_HASH = "sha256:" + "a" * 64
OUTCOME_HASH = "sha256:" + "b" * 64
AVAILABILITY_HASH = "sha256:" + "c" * 64
FIRST_FORECAST_ID = UUID("11111111-1111-1111-1111-111111111111")


@dataclass(frozen=True)
class _RunSource:
    forecast_id: UUID
    origin_kind: str
    opportunity_hash: str
    output_hash: str
    canonical_output: bytes


def _response(*, forecast_id: UUID, target_time: datetime) -> ForecastResponse:
    quantiles = [
        ForecastQuantile(level=0.1, value=98.0),
        ForecastQuantile(level=0.5, value=100.0),
        ForecastQuantile(level=0.9, value=102.0),
    ]
    return ForecastResponse(
        symbol="MSFT",
        target="close",
        horizon=1,
        horizon_unit="trading_day",
        as_of=AS_OF,
        currency="USD",
        forecasts=[
            ForecastStep(
                step=1,
                target_time=target_time,
                point=100.0,
                quantiles=quantiles,
                intervals=[
                    ForecastInterval(
                        coverage=0.8,
                        lower_quantile=0.1,
                        upper_quantile=0.9,
                        lower=98.0,
                        upper=102.0,
                    )
                ],
            )
        ],
        provenance=ForecastProvenance(
            forecast_id=forecast_id,
            snapshot_id="sha256:" + "d" * 64,
            model_version="baseline-naive@1",
            series_basis="raw",
            feature_set_hash="d" * 64,
            max_available_at=AS_OF,
            generated_at=AS_OF + timedelta(minutes=1),
            code_version="fixture-code@1",
            data_sources=[
                DataSourceLineage(
                    name="polygon_open_close",
                    snapshot_id="sha256:" + "e" * 64,
                    max_available_at=AS_OF,
                    fields=["close"],
                )
            ],
            lookahead_check=LookaheadCheck(
                status="passed",
                checked_at=AS_OF + timedelta(minutes=1),
                max_feature_available_at=AS_OF,
            ),
        ),
        calibration=ForecastCalibration(
            calibration_set_version="uncalibrated:baseline-naive@1",
            method="none",
            sample_count=0,
        ),
    )


def _source(
    *,
    forecast_id: UUID = FIRST_FORECAST_ID,
    target_time: datetime = AS_OF + timedelta(days=1),
    opportunity_digit: str = "1",
) -> _RunSource:
    response = _response(forecast_id=forecast_id, target_time=target_time)
    canonical = canonical_output(response)
    return _RunSource(
        forecast_id=forecast_id,
        origin_kind="scheduled_evaluation",
        opportunity_hash="sha256:" + opportunity_digit * 64,
        output_hash=output_hash(canonical),
        canonical_output=canonical,
    )


def _manifest(*, purpose: str = "heldout_evaluation") -> ForecastCohortManifest:
    first = member_from_scheduled_run(_source(), step=1)
    second = member_from_scheduled_run(
        _source(
            forecast_id=UUID("22222222-2222-2222-2222-222222222222"),
            target_time=AS_OF + timedelta(days=2),
            opportunity_digit="2",
        ),
        step=1,
    )
    return ForecastCohortManifest(
        purpose=purpose,  # type: ignore[arg-type]
        selection_policy_hash=SELECTION_HASH,
        outcome_resolution_policy_hash=OUTCOME_HASH,
        availability_rule_set_hash=AVAILABILITY_HASH,
        members=(second, first),
    )


def test_member_is_derived_from_exact_scheduled_canonical_output() -> None:
    source = _source(target_time=AS_OF + timedelta(days=1, hours=3))

    member = member_from_scheduled_run(source, step=1)

    assert member.forecast_id == source.forecast_id
    assert member.step == 1
    assert member.target_time == AS_OF + timedelta(days=1, hours=3)
    assert member.output_hash == source.output_hash
    assert member.opportunity_hash == source.opportunity_hash


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"origin_kind": "api"}, "scheduled_evaluation"),
        ({"output_hash": "sha256:" + "f" * 64}, "does not match"),
        ({"forecast_id": UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")}, "identifier"),
    ],
)
def test_member_refuses_non_scheduled_or_mismatched_archive_evidence(
    change: dict[str, object],
    message: str,
) -> None:
    source = replace(_source(), **change)

    with pytest.raises(ForecastCohortValidationError, match=message):
        member_from_scheduled_run(source, step=1)


def test_member_refuses_a_step_absent_from_the_archived_response() -> None:
    with pytest.raises(ForecastCohortValidationError, match="does not contain"):
        member_from_scheduled_run(_source(), step=2)


def test_manifest_is_strict_canonical_policy_bound_and_order_independent() -> None:
    manifest = _manifest()
    canonical = canonical_cohort_manifest(manifest)
    reordered = replace(manifest, members=tuple(reversed(manifest.members)))

    assert canonical_cohort_manifest(reordered) == canonical
    assert parse_cohort_manifest(canonical).members[0].forecast_id == UUID(
        "11111111-1111-1111-1111-111111111111"
    )
    document = json.loads(canonical)
    assert document["format"] == COHORT_FORMAT
    assert document["purpose"] == "heldout_evaluation"
    assert document["selection_policy_hash"] == SELECTION_HASH
    assert document["outcome_resolution_policy_hash"] == OUTCOME_HASH
    assert document["availability_rule_set_hash"] == AVAILABILITY_HASH
    assert cohort_id_for_manifest(canonical) == cohort_id_for_manifest(manifest)


def test_fit_and_heldout_purposes_have_distinct_artifact_identities() -> None:
    heldout = _manifest(purpose="heldout_evaluation")
    fit = _manifest(purpose="calibration_fit")

    assert cohort_id_for_manifest(heldout) != cohort_id_for_manifest(fit)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value + b"\n",
        lambda value: value.replace(
            b'"format":"forecast-outcome-cohort-v1"',
            b'"extra":1,"format":"forecast-outcome-cohort-v1"',
            1,
        ),
        lambda value: value.replace(
            b'"purpose":"heldout_evaluation"',
            b'"purpose":"heldout_evaluation","purpose":"heldout_evaluation"',
            1,
        ),
        lambda value: value.replace(b'"step":1', b'"step":true', 1),
        lambda value: value.replace(
            b'"target_time":"2026-07-15T17:00:00.000000Z"',
            b'"target_time":"2026-07-15T17:00:00Z"',
            1,
        ),
    ],
)
def test_parser_rejects_noncanonical_duplicate_unknown_or_wrong_typed_json(
    mutate: object,
) -> None:
    canonical = canonical_cohort_manifest(_manifest())
    changed = mutate(canonical)  # type: ignore[operator]

    with pytest.raises(ForecastCohortValidationError):
        parse_cohort_manifest(changed)


def test_manifest_refuses_duplicate_forecast_or_opportunity_steps() -> None:
    manifest = _manifest()
    first = manifest.members[0]

    with pytest.raises(ForecastCohortValidationError, match="duplicate forecast step"):
        canonical_cohort_manifest(replace(manifest, members=(first, first)))
    duplicate_opportunity = replace(
        manifest.members[1],
        opportunity_hash=first.opportunity_hash,
    )
    with pytest.raises(ForecastCohortValidationError, match="duplicate forecast opportunity"):
        canonical_cohort_manifest(replace(manifest, members=(first, duplicate_opportunity)))


def test_member_count_matches_the_database_storage_bound() -> None:
    member = _manifest().members[0]
    with pytest.raises(ForecastCohortValidationError, match="member count"):
        canonical_cohort_manifest(replace(_manifest(), members=(member,) * 10_001))


def test_record_headers_are_derived_and_independently_revalidated() -> None:
    manifest = _manifest()
    record = build_cohort_record(
        manifest,
        recorded_at=AS_OF + timedelta(hours=1),
        creator_xid=101,
    )

    assert record.member_count == 2
    assert record.earliest_target_time == AS_OF + timedelta(days=1)
    assert record.latest_target_time == AS_OF + timedelta(days=2)
    assert record.purpose == "heldout_evaluation"
    assert validate_cohort_record(record) == parse_cohort_manifest(record.canonical_manifest)

    with pytest.raises(ForecastCohortValidationError, match="headers"):
        validate_cohort_record(replace(record, member_count=3))


def test_record_must_be_created_before_its_earliest_target() -> None:
    with pytest.raises(ForecastCohortValidationError, match="recorded before"):
        build_cohort_record(
            _manifest(),
            recorded_at=AS_OF + timedelta(days=1),
            creator_xid=101,
        )


def test_seal_proves_a_distinct_transaction_strictly_before_first_target() -> None:
    record = build_cohort_record(
        _manifest(),
        recorded_at=AS_OF + timedelta(hours=1),
        creator_xid=101,
    )
    seal = ForecastCohortSeal(
        cohort_id=record.cohort_id,
        manifest_recorded_at=record.recorded_at,
        sealed_at=AS_OF + timedelta(hours=2),
        sealer_xid=102,
    )

    assert validate_cohort_seal(record, seal).purpose == "heldout_evaluation"


@pytest.mark.parametrize(
    "change",
    [
        {"sealer_xid": 101},
        {"manifest_recorded_at": AS_OF},
        {"sealed_at": AS_OF},
        {"sealed_at": AS_OF + timedelta(days=1)},
        {"cohort_id": "sha256:" + "f" * 64},
    ],
)
def test_seal_rejects_same_transaction_bad_time_or_wrong_manifest(
    change: dict[str, object],
) -> None:
    record = build_cohort_record(
        _manifest(),
        recorded_at=AS_OF + timedelta(hours=1),
        creator_xid=101,
    )
    seal = ForecastCohortSeal(
        cohort_id=record.cohort_id,
        manifest_recorded_at=record.recorded_at,
        sealed_at=AS_OF + timedelta(hours=2),
        sealer_xid=102,
    )

    with pytest.raises(ForecastCohortValidationError):
        validate_cohort_seal(record, replace(seal, **change))


def test_timezone_offsets_normalize_without_changing_manifest_identity() -> None:
    manifest = _manifest()
    offset = timezone(timedelta(hours=3))
    shifted = replace(
        manifest,
        members=tuple(
            replace(member, target_time=member.target_time.astimezone(offset))
            for member in manifest.members
        ),
    )

    assert canonical_cohort_manifest(shifted) == canonical_cohort_manifest(manifest)
