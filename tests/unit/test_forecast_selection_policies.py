from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from app.services.forecast_outcome_resolution import ForecastOutcomeResolutionPolicy
from app.services.forecast_selection_policies import (
    MAX_CANONICAL_SELECTION_POLICY_BYTES,
    MEMBERSHIP_AGGREGATION_RULE,
    MEMBERSHIP_COUNTING_UNIT,
    MINIMUM_SEAL_LEAD_SECONDS,
    ForecastPurposeAssignment,
    ForecastSelectionCandidate,
    ForecastSelectionPolicyValidationError,
    ForecastSelectionWindow,
    ProspectiveForecastSelectionPolicy,
    assign_selection_purposes,
    canonical_selection_policy,
    parse_selection_policy,
    purpose_for_target_time,
    selection_policy_hash_for,
    validate_selection_policy_outcome_epoch,
)

FORECAST_RESOLUTION_HASH = "sha256:" + "a" * 64
FORECAST_AVAILABILITY_HASH = "sha256:" + "b" * 64
OUTCOME_RESOLUTION_HASH = "sha256:ce8df5f3ad0256043d11f70170111594c7aefe0cc3150939091cbf00a4df801e"
OUTCOME_AVAILABILITY_HASH = (
    "sha256:cfd2d129386375b8663f71f5752b70630cf8dbde21cc18596985de41a58ca705"
)
LAG_SECONDS = 172_800
GOLDEN_POLICY_HASH = "sha256:ced49caba5f43b069256fb0c1ff848e163bf62f7f7997a919151694ac17d0b42"
GOLDEN_CANONICAL = (
    b'{"format":"forecast-prospective-selection-policy-v1",'
    b'"minimum_seal_lead_seconds":14400,"policy_epoch":{'
    b'"forecast_availability_rule_set_hash":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
    b'"forecast_resolution_policy_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"outcome_availability_rule_set_hash":"sha256:cfd2d129386375b8663f71f5752b70630cf8dbde21cc18596985de41a58ca705",'
    b'"outcome_resolution_policy_hash":"sha256:ce8df5f3ad0256043d11f70170111594c7aefe0cc3150939091cbf00a4df801e",'
    b'"resolution_lag_seconds":172800},"schema_version":1,"study":{'
    b'"cadence":"xnys_session_daily","currency":"USD","horizon":5,'
    b'"horizon_unit":"trading_day","interval_coverages_millis":[500,800,950],'
    b'"model_selector":"baseline_naive","model_version":"baseline-naive@1",'
    b'"selected_steps":[1,2,3,4,5],'
    b'"selection_rule":"complete_selected_step_bundle_within_one_utc_target_window",'
    b'"series_basis":"raw","snapshot_binding":"explicit_snapshot_id",'
    b'"symbols":["MSFT"],"target":"close"},"windows":{'
    b'"fit":{"end":"2026-09-30","minimum_member_count":200,'
    b'"start":"2026-07-20"},"heldout":{"end":"2026-10-30",'
    b'"minimum_member_count":100,"start":"2026-10-01"},'
    b'"membership_aggregation_rule":'
    b'"distinct_opportunity_steps_across_sealed_cohorts_by_policy_purpose_window",'
    b'"membership_counting_unit":"forecast_opportunity_step",'
    b'"window_date_policy_version":"utc-target-date-v1"}}'
)


def _policy(**updates: object) -> ProspectiveForecastSelectionPolicy:
    values: dict[str, object] = {
        "symbols": ("MSFT",),
        "target": "close",
        "series_basis": "raw",
        "horizon_unit": "trading_day",
        "currency": "USD",
        "model_selector": "baseline_naive",
        "model_version": "baseline-naive@1",
        "horizon": 5,
        "selected_steps": (1, 2, 3, 4, 5),
        "interval_coverages_millis": (500, 800, 950),
        "fit_window": ForecastSelectionWindow(date(2026, 7, 20), date(2026, 9, 30)),
        "heldout_window": ForecastSelectionWindow(date(2026, 10, 1), date(2026, 10, 30)),
        "minimum_fit_member_count": 200,
        "minimum_heldout_member_count": 100,
        "minimum_seal_lead_seconds": MINIMUM_SEAL_LEAD_SECONDS,
        "cadence": "xnys_session_daily",
        "snapshot_binding": "explicit_snapshot_id",
        "selection_rule": "complete_selected_step_bundle_within_one_utc_target_window",
        "resolution_lag_seconds": LAG_SECONDS,
        "forecast_resolution_policy_hash": FORECAST_RESOLUTION_HASH,
        "forecast_availability_rule_set_hash": FORECAST_AVAILABILITY_HASH,
        "outcome_resolution_policy_hash": OUTCOME_RESOLUTION_HASH,
        "outcome_availability_rule_set_hash": OUTCOME_AVAILABILITY_HASH,
    }
    values.update(updates)
    return ProspectiveForecastSelectionPolicy(**values)  # type: ignore[arg-type]


def _candidate(
    digit: str,
    step: int,
    target_time: datetime,
) -> ForecastSelectionCandidate:
    return ForecastSelectionCandidate(
        opportunity_hash="sha256:" + digit * 64,
        step=step,
        target_time=target_time,
    )


def _mutate(canonical: bytes, mutation) -> bytes:
    document = json.loads(canonical)
    mutation(document)
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def test_policy_has_pinned_golden_bytes_and_hash() -> None:
    policy = _policy()

    assert canonical_selection_policy(policy) == GOLDEN_CANONICAL
    assert policy.canonical_policy == GOLDEN_CANONICAL
    assert policy.selection_policy_hash == GOLDEN_POLICY_HASH
    assert selection_policy_hash_for(policy) == GOLDEN_POLICY_HASH
    assert selection_policy_hash_for(GOLDEN_CANONICAL) == GOLDEN_POLICY_HASH
    assert "sha256:" + hashlib.sha256(GOLDEN_CANONICAL).hexdigest() == GOLDEN_POLICY_HASH
    assert policy.selection_policy_document == json.loads(GOLDEN_CANONICAL)
    assert policy.selection_policy_document["windows"]["membership_counting_unit"] == (
        MEMBERSHIP_COUNTING_UNIT
    )
    assert policy.selection_policy_document["windows"]["membership_aggregation_rule"] == (
        MEMBERSHIP_AGGREGATION_RULE
    )


def test_policy_round_trip_is_exact_deterministic_and_frozen() -> None:
    policy = _policy()
    parsed = parse_selection_policy(policy.canonical_policy)

    assert parsed == policy
    assert parsed.canonical_policy == policy.canonical_policy
    assert parsed.selection_policy_hash == policy.selection_policy_hash
    with pytest.raises(FrozenInstanceError):
        policy.horizon = 6  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        policy.fit_window.end = date(2026, 10, 1)  # type: ignore[misc]


@pytest.mark.parametrize(
    "changed",
    [
        {"symbols": ("AAPL", "MSFT")},
        {"model_selector": "baseline_drift", "model_version": "baseline-drift@1"},
        {"model_version": "baseline-naive@2"},
        {"horizon": 6},
        {"selected_steps": (1, 2, 3, 4)},
        {"interval_coverages_millis": (500, 800)},
        {"fit_window": ForecastSelectionWindow(date(2026, 7, 21), date(2026, 9, 30))},
        {"heldout_window": ForecastSelectionWindow(date(2026, 10, 2), date(2026, 10, 30))},
        {"minimum_fit_member_count": 201},
        {"minimum_heldout_member_count": 101},
        {"minimum_seal_lead_seconds": MINIMUM_SEAL_LEAD_SECONDS + 1},
        {"forecast_resolution_policy_hash": "sha256:" + "c" * 64},
        {"forecast_availability_rule_set_hash": "sha256:" + "d" * 64},
    ],
)
def test_every_changeable_semantic_field_rotates_identity(changed: dict[str, object]) -> None:
    assert _policy(**changed).selection_policy_hash != GOLDEN_POLICY_HASH


def test_resolution_lag_and_derived_outcome_hashes_rotate_together() -> None:
    changed_lag = LAG_SECONDS + 1
    outcome = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=changed_lag)
    changed = _policy(
        resolution_lag_seconds=changed_lag,
        outcome_resolution_policy_hash=outcome.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=outcome.availability_rule_set_hash,
    )

    assert changed.selection_policy_hash != GOLDEN_POLICY_HASH


@pytest.mark.parametrize(
    "bad",
    [
        GOLDEN_CANONICAL + b"\n",
        json.dumps(json.loads(GOLDEN_CANONICAL), indent=2).encode(),
        b"[]",
        b"null",
        b'{"schema_version":NaN}',
        b"\xff",
        GOLDEN_CANONICAL[:-1],
    ],
)
def test_parser_rejects_noncanonical_or_invalid_json(bad: bytes) -> None:
    with pytest.raises(ForecastSelectionPolicyValidationError):
        parse_selection_policy(bad)


@pytest.mark.parametrize(
    "bad",
    [
        GOLDEN_CANONICAL.replace(
            b'"format":"forecast-prospective-selection-policy-v1"',
            b'"format":"forecast-prospective-selection-policy-v1","format":"forecast-prospective-selection-policy-v1"',
        ),
        GOLDEN_CANONICAL.replace(
            b'"fit":{"end"',
            b'"fit":{"extra":1,"end"',
        ),
        _mutate(GOLDEN_CANONICAL, lambda doc: doc.pop("study")),
        _mutate(GOLDEN_CANONICAL, lambda doc: doc.__setitem__("purpose", "calibration_fit")),
        _mutate(GOLDEN_CANONICAL, lambda doc: doc.__setitem__("schema_version", True)),
        _mutate(GOLDEN_CANONICAL, lambda doc: doc.__setitem__("format", "unsupported")),
        _mutate(
            GOLDEN_CANONICAL,
            lambda doc: doc["windows"].__setitem__(
                "membership_counting_unit",
                "forecast",
            ),
        ),
        _mutate(
            GOLDEN_CANONICAL,
            lambda doc: doc["windows"].__setitem__(
                "membership_aggregation_rule",
                "per_cohort",
            ),
        ),
    ],
)
def test_parser_rejects_duplicate_unknown_missing_or_wrong_version_keys(bad: bytes) -> None:
    with pytest.raises(ForecastSelectionPolicyValidationError):
        parse_selection_policy(bad)


@pytest.mark.parametrize(
    "bad",
    [
        b"",
        b"x" * (MAX_CANONICAL_SELECTION_POLICY_BYTES + 1),
        "not-bytes",
        bytearray(GOLDEN_CANONICAL),
    ],
    ids=["empty", "oversize", "string", "bytearray"],
)
def test_parser_and_hash_require_nonempty_bounded_bytes(bad: object) -> None:
    with pytest.raises(ForecastSelectionPolicyValidationError):
        parse_selection_policy(bad)  # type: ignore[arg-type]
    with pytest.raises(ForecastSelectionPolicyValidationError):
        selection_policy_hash_for(bad)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "updates",
    [
        {"symbols": ["MSFT"]},
        {"symbols": ("msft",)},
        {"symbols": ("MSFT", "AAPL")},
        {"symbols": ("MSFT", "MSFT")},
        {"model_selector": "auto"},
        {"model_selector": "unknown"},
        {"model_selector": "arima", "model_version": "arima@1"},
        {
            "model_selector": "baseline_seasonal_naive",
            "model_version": "baseline-seasonal-naive-s5@1",
        },
        {"model_version": " baseline-naive@1"},
        {"model_version": "baseline-drift@1"},
        {"horizon": True},
        {"horizon": 253},
        {"selected_steps": [1, 2, 3]},
        {"selected_steps": (1, 1)},
        {"selected_steps": (2, 1)},
        {"selected_steps": (1, 6)},
        {"interval_coverages_millis": [500, 800]},
        {"interval_coverages_millis": (0, 800)},
        {"interval_coverages_millis": (800, 500)},
        {
            "interval_coverages_millis": (
                100,
                200,
                300,
                400,
                500,
                600,
                700,
                800,
                900,
                950,
            )
        },
        {"minimum_fit_member_count": 0},
        {"minimum_heldout_member_count": True},
        {"minimum_seal_lead_seconds": MINIMUM_SEAL_LEAD_SECONDS - 1},
        {"target": "adjusted_close"},
        {"series_basis": "split_adjusted"},
        {"horizon_unit": "calendar_day"},
        {"currency": "EUR"},
        {"cadence": "weekly"},
        {"snapshot_binding": "latest"},
        {"selection_rule": "caller_supplied"},
        {"forecast_resolution_policy_hash": "not-a-hash"},
        {"resolution_lag_seconds": 0},
        {"schema_version": True},
    ],
)
def test_constructor_refuses_noncanonical_or_unsupported_fields(
    updates: dict[str, object],
) -> None:
    with pytest.raises(ForecastSelectionPolicyValidationError):
        _policy(**updates)


def test_constructor_rejects_datetime_window_dates_and_wrong_window_type() -> None:
    with pytest.raises(ForecastSelectionPolicyValidationError):
        ForecastSelectionWindow(
            datetime(2026, 7, 20, tzinfo=UTC),
            date(2026, 9, 30),
        )
    with pytest.raises(ForecastSelectionPolicyValidationError):
        _policy(fit_window=object())


def test_public_operations_revalidate_nested_frozen_windows() -> None:
    policy = _policy()
    object.__setattr__(policy.fit_window, "end", date(2026, 7, 19))

    for operation in (
        lambda: canonical_selection_policy(policy),
        lambda: selection_policy_hash_for(policy),
        lambda: purpose_for_target_time(policy, datetime(2026, 7, 20, tzinfo=UTC)),
        lambda: assign_selection_purposes(
            policy,
            (_candidate("0", 1, datetime(2026, 7, 20, tzinfo=UTC)),),
        ),
    ):
        with pytest.raises(ForecastSelectionPolicyValidationError):
            operation()


def test_historical_policy_parsing_is_independent_of_current_dependency_epoch() -> None:
    historical = _policy(
        resolution_lag_seconds=86_400,
        outcome_resolution_policy_hash="sha256:" + "e" * 64,
        outcome_availability_rule_set_hash="sha256:" + "f" * 64,
    )

    parsed = parse_selection_policy(historical.canonical_policy)

    assert parsed == historical


def test_outcome_epoch_match_is_an_explicit_composition_check() -> None:
    outcome = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=LAG_SECONDS)
    current_policy = _policy(
        outcome_resolution_policy_hash=outcome.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=outcome.availability_rule_set_hash,
    )
    validate_selection_policy_outcome_epoch(current_policy, outcome)

    with pytest.raises(
        ForecastSelectionPolicyValidationError,
        match="does not match",
    ):
        validate_selection_policy_outcome_epoch(
            _policy(outcome_resolution_policy_hash="sha256:" + "f" * 64),
            outcome,
        )


def test_windows_require_a_strict_forward_split() -> None:
    immediate = _policy(
        fit_window=ForecastSelectionWindow(date(2026, 7, 20), date(2026, 9, 30)),
        heldout_window=ForecastSelectionWindow(date(2026, 10, 1), date(2026, 10, 1)),
    )
    assert immediate.fit_window.end < immediate.heldout_window.start

    with pytest.raises(ForecastSelectionPolicyValidationError):
        ForecastSelectionWindow(date(2026, 9, 30), date(2026, 9, 29))
    with pytest.raises(ForecastSelectionPolicyValidationError, match="strictly before"):
        _policy(
            heldout_window=ForecastSelectionWindow(
                date(2026, 9, 30),
                date(2026, 10, 30),
            )
        )
    with pytest.raises(ForecastSelectionPolicyValidationError, match="strictly before"):
        _policy(
            heldout_window=ForecastSelectionWindow(
                date(2026, 9, 29),
                date(2026, 10, 30),
            )
        )


def test_purpose_assignment_uses_inclusive_utc_target_dates() -> None:
    policy = _policy()

    assert (
        purpose_for_target_time(
            policy,
            datetime(2026, 7, 20, 20, tzinfo=UTC),
        )
        == "calibration_fit"
    )
    assert (
        purpose_for_target_time(
            policy,
            datetime(2026, 9, 30, 20, tzinfo=UTC),
        )
        == "calibration_fit"
    )
    assert (
        purpose_for_target_time(
            policy,
            datetime(2026, 10, 1, 20, tzinfo=UTC),
        )
        == "heldout_evaluation"
    )
    assert (
        purpose_for_target_time(
            policy,
            datetime(2026, 10, 30, 20, tzinfo=UTC),
        )
        == "heldout_evaluation"
    )

    local_october = datetime(2026, 10, 1, 0, 30, tzinfo=timezone(timedelta(hours=3)))
    assert local_october.astimezone(UTC).date() == date(2026, 9, 30)
    assert purpose_for_target_time(policy, local_october) == "calibration_fit"


def test_purpose_assignment_refuses_uncovered_or_invalid_targets() -> None:
    policy = _policy(heldout_window=ForecastSelectionWindow(date(2026, 10, 5), date(2026, 10, 30)))
    gap = datetime(2026, 10, 2, 20, tzinfo=UTC)

    assert purpose_for_target_time(policy, gap) is None
    with pytest.raises(ForecastSelectionPolicyValidationError, match="outside"):
        assign_selection_purposes(policy, (_candidate("1", 1, gap),))
    with pytest.raises(ForecastSelectionPolicyValidationError, match="timezone-aware"):
        purpose_for_target_time(policy, datetime(2026, 9, 30, 20))


def test_batch_assignment_is_order_independent_and_policy_bound() -> None:
    policy = _policy(selected_steps=(1,))
    fit = _candidate("2", 1, datetime(2026, 9, 29, 20, tzinfo=UTC))
    heldout = _candidate("3", 1, datetime(2026, 10, 2, 20, tzinfo=UTC))

    forward = assign_selection_purposes(policy, (fit, heldout))
    reverse = assign_selection_purposes(policy, (heldout, fit))

    assert forward == reverse
    assert [item.purpose for item in forward] == ["calibration_fit", "heldout_evaluation"]
    assert {item.selection_policy_hash for item in forward} == {policy.selection_policy_hash}
    assert all(item.candidate.target_time.tzinfo is UTC for item in forward)


def test_exact_opportunity_step_cannot_cross_purposes() -> None:
    policy = _policy(selected_steps=(1,))
    candidates = (
        _candidate("4", 1, datetime(2026, 9, 30, 20, tzinfo=UTC)),
        _candidate("4", 1, datetime(2026, 10, 1, 20, tzinfo=UTC)),
    )

    with pytest.raises(ForecastSelectionPolicyValidationError, match="cannot cross"):
        assign_selection_purposes(policy, candidates)


def test_whole_opportunity_cannot_cross_purposes_on_different_steps() -> None:
    policy = _policy(selected_steps=(1, 2))
    candidates = (
        _candidate("5", 1, datetime(2026, 9, 30, 20, tzinfo=UTC)),
        _candidate("5", 2, datetime(2026, 10, 1, 20, tzinfo=UTC)),
    )

    with pytest.raises(ForecastSelectionPolicyValidationError, match="cannot cross"):
        assign_selection_purposes(policy, candidates)


def test_overlap_guard_is_not_broader_than_the_downstream_invariant() -> None:
    two_step_policy = _policy(selected_steps=(1, 2))
    fit_same_opportunity = (
        _candidate("6", 1, datetime(2026, 9, 29, 20, tzinfo=UTC)),
        _candidate("6", 2, datetime(2026, 9, 30, 20, tzinfo=UTC)),
    )
    one_step_policy = _policy(selected_steps=(1,))
    disjoint = (
        _candidate("7", 1, datetime(2026, 9, 30, 20, tzinfo=UTC)),
        _candidate("8", 1, datetime(2026, 10, 1, 20, tzinfo=UTC)),
    )

    assert len(assign_selection_purposes(two_step_policy, fit_same_opportunity)) == 2
    assert len(assign_selection_purposes(one_step_policy, disjoint)) == 2


def test_assignment_requires_complete_opportunity_bundles_on_every_call() -> None:
    policy = _policy(selected_steps=(1, 2))
    first = _candidate("b", 1, datetime(2026, 9, 29, 20, tzinfo=UTC))
    second = _candidate("b", 2, datetime(2026, 9, 30, 20, tzinfo=UTC))

    with pytest.raises(ForecastSelectionPolicyValidationError, match="exactly all selected"):
        assign_selection_purposes(policy, (first,))
    with pytest.raises(ForecastSelectionPolicyValidationError, match="exactly all selected"):
        assign_selection_purposes(policy, (second,))
    assert len(assign_selection_purposes(policy, (first, second))) == 2


def test_assignment_rejects_duplicate_unselected_or_malformed_candidates() -> None:
    policy = _policy(selected_steps=(1, 2))
    candidate = _candidate("9", 1, datetime(2026, 9, 30, 20, tzinfo=UTC))

    with pytest.raises(ForecastSelectionPolicyValidationError, match="duplicate"):
        assign_selection_purposes(policy, (candidate, candidate))
    with pytest.raises(ForecastSelectionPolicyValidationError, match="not selected"):
        assign_selection_purposes(
            policy,
            (_candidate("a", 3, datetime(2026, 9, 30, 20, tzinfo=UTC)),),
        )
    with pytest.raises(ForecastSelectionPolicyValidationError, match="nonempty bounded"):
        assign_selection_purposes(policy, ())
    with pytest.raises(ForecastSelectionPolicyValidationError, match="canonical sha256"):
        ForecastSelectionCandidate(
            opportunity_hash="SHA256:" + "a" * 64,
            step=1,
            target_time=datetime(2026, 9, 30, 20, tzinfo=UTC),
        )
    with pytest.raises(ForecastSelectionPolicyValidationError, match="integer"):
        ForecastSelectionCandidate(
            opportunity_hash="sha256:" + "a" * 64,
            step=True,
            target_time=datetime(2026, 9, 30, 20, tzinfo=UTC),
        )


def test_direct_assignment_construction_still_validates_shapes() -> None:
    candidate = _candidate("c", 1, datetime(2026, 9, 30, 20, tzinfo=UTC))

    with pytest.raises(ForecastSelectionPolicyValidationError, match="prospective"):
        ForecastPurposeAssignment(
            candidate=candidate,
            purpose="training",  # type: ignore[arg-type]
            selection_policy_hash=GOLDEN_POLICY_HASH,
        )
    with pytest.raises(ForecastSelectionPolicyValidationError, match="canonical sha256"):
        ForecastPurposeAssignment(
            candidate=candidate,
            purpose="calibration_fit",
            selection_policy_hash="not-a-hash",
        )
