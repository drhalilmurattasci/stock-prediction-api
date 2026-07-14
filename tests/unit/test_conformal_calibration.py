"""Golden and fail-closed tests for the offline conformal kernels."""

from __future__ import annotations

import math
from dataclasses import FrozenInstanceError

import pytest

from ml.calibration import (
    ABSOLUTE_RESIDUAL_POLICY_VERSION,
    ACI_POLICY_VERSION,
    CQR_POLICY_VERSION,
    FINITE_SAMPLE_POLICY_VERSION,
    AbsoluteResidualCalibration,
    ACIState,
    ConformalValidationError,
    CQRCalibration,
    InsufficientCalibrationData,
    QuantileSelection,
    absolute_residual_scores,
    aci_transition,
    cqr_scores,
    finite_sample_quantile,
    fit_absolute_residual,
    fit_cqr,
    interval_missed,
)


def _selection(*, coverage: float, value: float, sample_count: int = 4) -> QuantileSelection:
    rank = math.ceil((sample_count + 1) * coverage)
    return QuantileSelection(
        coverage=coverage,
        sample_count=sample_count,
        rank=rank,
        value=value,
    )


def _aci(**changes: object) -> ACIState:
    values: dict[str, object] = {
        "target_miscoverage": 0.2,
        "current_miscoverage": 0.2,
        "learning_rate": 0.05,
        "minimum_miscoverage": 0.01,
        "maximum_miscoverage": 0.99,
    }
    values.update(changes)
    return ACIState(**values)  # type: ignore[arg-type]


def test_algorithm_versions_are_explicit_and_pinned() -> None:
    assert FINITE_SAMPLE_POLICY_VERSION == "finite-sample-nearest-rank-v1"
    assert ABSOLUTE_RESIDUAL_POLICY_VERSION == "absolute-residual-v1"
    assert CQR_POLICY_VERSION == "signed-cqr-v1"
    assert ACI_POLICY_VERSION == "projected-aci-v1"
    assert _aci().algorithm_version == ACI_POLICY_VERSION


@pytest.mark.parametrize(
    ("coverage", "expected_rank", "expected_value"),
    [(0.8, 4, 4.0), (0.6, 3, 3.0)],
)
def test_finite_sample_quantile_uses_noninterpolated_corrected_rank(
    coverage: float,
    expected_rank: int,
    expected_value: float,
) -> None:
    scores = [4.0, 1.0, 3.0, 2.0]
    before = scores.copy()

    selected = finite_sample_quantile(scores, coverage=coverage)

    assert selected == QuantileSelection(
        coverage=coverage,
        sample_count=4,
        rank=expected_rank,
        value=expected_value,
    )
    assert scores == before


def test_finite_sample_quantile_refuses_augmented_infinity_instead_of_clipping() -> None:
    with pytest.raises(InsufficientCalibrationData, match="no finite order statistic"):
        finite_sample_quantile([4.0, 1.0, 3.0, 2.0], coverage=0.9)


def test_finite_sample_quantile_keeps_ties_and_normalizes_tolerance() -> None:
    selected = finite_sample_quantile([1.0, 2.0, 2.0, 9.0], coverage=0.6 + 5e-13)

    assert selected.coverage == 0.6
    assert selected.rank == 3
    assert selected.value == 2.0


@pytest.mark.parametrize(
    "coverage",
    [0.8001, 0.0, 1.0, -0.1, float("nan"), float("inf"), True],
)
def test_noncanonical_or_invalid_coverages_are_rejected(coverage: object) -> None:
    with pytest.raises(ConformalValidationError):
        finite_sample_quantile([1.0, 2.0, 3.0, 4.0], coverage=coverage)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "scores",
    [[], [1.0, float("nan")], [1.0, float("inf")], [1.0, True], "123", 123],
)
def test_score_sequence_must_be_nonempty_real_and_finite(scores: object) -> None:
    with pytest.raises(ConformalValidationError):
        finite_sample_quantile(scores, coverage=0.6)  # type: ignore[arg-type]


def test_extreme_real_conversion_fails_with_the_owned_validation_error() -> None:
    with pytest.raises(ConformalValidationError, match="finite real numbers"):
        finite_sample_quantile([10**10000], coverage=0.5)


def test_quantile_selection_rejects_forged_rank_policy_or_infeasible_sample() -> None:
    with pytest.raises(ConformalValidationError, match="rank does not match"):
        QuantileSelection(coverage=0.6, sample_count=4, rank=2, value=2.0)
    with pytest.raises(ConformalValidationError, match="policy_version"):
        QuantileSelection(
            coverage=0.6,
            sample_count=4,
            rank=3,
            value=2.0,
            policy_version="other",
        )
    with pytest.raises(InsufficientCalibrationData):
        QuantileSelection(coverage=0.9, sample_count=4, rank=5, value=4.0)


def test_absolute_residual_scores_and_fits_match_hand_calculated_goldens() -> None:
    actual = [10.0, 12.0, 9.0, 15.0]
    point = [9.0, 13.0, 10.0, 12.0]

    assert absolute_residual_scores(actual, point) == (1.0, 1.0, 1.0, 3.0)

    sixty = fit_absolute_residual(actual, point, coverage=0.6)
    eighty = fit_absolute_residual(actual, point, coverage=0.8)
    assert sixty.selection.rank == 3
    assert sixty.selection.value == 1.0
    assert sixty.interval(20.0) == (19.0, 21.0)
    assert eighty.selection.rank == 4
    assert eighty.selection.value == 3.0
    assert eighty.interval(20.0) == (17.0, 23.0)


def test_absolute_residual_validation_is_fail_closed() -> None:
    with pytest.raises(ConformalValidationError, match="equal lengths"):
        absolute_residual_scores([1.0, 2.0], [1.0])
    with pytest.raises(ConformalValidationError, match="overflowed"):
        absolute_residual_scores([1e308], [-1e308])
    with pytest.raises(ConformalValidationError, match="radius cannot be negative"):
        AbsoluteResidualCalibration(_selection(coverage=0.6, value=-1.0))
    with pytest.raises(ConformalValidationError, match="policy_version"):
        AbsoluteResidualCalibration(
            _selection(coverage=0.6, value=1.0),
            policy_version="other",
        )
    calibration = AbsoluteResidualCalibration(_selection(coverage=0.6, value=1e308))
    with pytest.raises(ConformalValidationError, match="overflowed"):
        calibration.interval(1e308)


def test_signed_cqr_scores_and_corrections_match_goldens_without_zero_clamp() -> None:
    actual = [10.0, 12.0, 9.0, 15.0]
    lower = [8.0, 11.0, 10.0, 12.0]
    upper = [11.0, 13.0, 12.0, 14.0]

    assert cqr_scores(actual, lower, upper) == (-1.0, -1.0, 1.0, 1.0)

    expanded = fit_cqr(actual, lower, upper, coverage=0.6)
    shrunk = fit_cqr(actual, lower, upper, coverage=0.2)
    assert expanded.selection.value == 1.0
    assert expanded.interval(18.0, 22.0) == (17.0, 23.0)
    assert shrunk.selection.value == -1.0
    assert shrunk.interval(18.0, 22.0) == (19.0, 21.0)


def test_cqr_score_is_zero_on_an_inclusive_base_bound() -> None:
    assert cqr_scores([10.0, 12.0], [10.0, 11.0], [11.0, 12.0]) == (0.0, 0.0)


def test_cqr_validation_rejects_inversion_crossing_mismatch_and_overflow() -> None:
    with pytest.raises(ConformalValidationError, match="equal lengths"):
        cqr_scores([1.0], [0.0], [2.0, 3.0])
    with pytest.raises(ConformalValidationError, match="lower cannot"):
        cqr_scores([1.0], [2.0], [0.0])
    with pytest.raises(ConformalValidationError, match="overflowed"):
        cqr_scores([1e308], [-1e308], [1e308])

    crossing = CQRCalibration(_selection(coverage=0.2, value=-2.0))
    with pytest.raises(ConformalValidationError, match="empty interval"):
        crossing.interval(0.0, 1.0)
    with pytest.raises(ConformalValidationError, match="policy_version"):
        CQRCalibration(_selection(coverage=0.6, value=1.0), policy_version="other")


def test_interval_missed_uses_inclusive_endpoints() -> None:
    assert interval_missed(1.0, 1.0, 2.0) is False
    assert interval_missed(2.0, 1.0, 2.0) is False
    assert interval_missed(0.99, 1.0, 2.0) is True
    assert interval_missed(2.01, 1.0, 2.0) is True
    with pytest.raises(ConformalValidationError, match="lower cannot"):
        interval_missed(1.0, 2.0, 1.0)


def test_projected_aci_golden_covered_and_missed_updates() -> None:
    state = _aci()

    covered = aci_transition(state, missed=False)
    missed = aci_transition(state, missed=True)

    assert covered.current_miscoverage == pytest.approx(0.21)
    assert covered.update_count == 1
    assert covered.miss_count == 0
    assert missed.current_miscoverage == pytest.approx(0.16)
    assert missed.update_count == 1
    assert missed.miss_count == 1
    assert state.current_miscoverage == 0.2
    assert state.update_count == 0


def test_projected_aci_clips_to_explicit_inclusive_bounds_and_counts_sequence() -> None:
    upper = aci_transition(
        _aci(
            current_miscoverage=0.2,
            learning_rate=1.0,
            minimum_miscoverage=0.05,
            maximum_miscoverage=0.25,
        ),
        missed=False,
    )
    lower = aci_transition(upper, missed=True)

    assert upper.current_miscoverage == 0.25
    assert lower.current_miscoverage == 0.05
    assert lower.update_count == 2
    assert lower.miss_count == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"target_miscoverage": 0.0},
        {"maximum_miscoverage": 1.0},
        {"minimum_miscoverage": 0.3},
        {"current_miscoverage": 0.001},
        {"learning_rate": 0.0},
        {"learning_rate": float("inf")},
        {"update_count": -1},
        {"update_count": True},
        {"update_count": 1, "miss_count": 2},
        {"algorithm_version": "other"},
    ],
)
def test_aci_state_rejects_invalid_or_ambiguous_configuration(
    changes: dict[str, object],
) -> None:
    with pytest.raises((ConformalValidationError, TypeError)):
        _aci(**changes)


@pytest.mark.parametrize("missed", [0, 1, None, "false"])
def test_aci_transition_requires_an_exact_boolean(missed: object) -> None:
    with pytest.raises(TypeError, match="missed must be a bool"):
        aci_transition(_aci(), missed=missed)  # type: ignore[arg-type]


def test_calibration_and_state_objects_are_immutable() -> None:
    calibration = fit_cqr(
        [10.0, 12.0, 9.0, 15.0],
        [8.0, 11.0, 10.0, 12.0],
        [11.0, 13.0, 12.0, 14.0],
        coverage=0.6,
    )
    state = _aci()

    with pytest.raises(FrozenInstanceError):
        calibration.policy_version = "changed"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        state.current_miscoverage = 0.3  # type: ignore[misc]
