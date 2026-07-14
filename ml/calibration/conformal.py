"""Dependency-free, offline conformal-calibration mechanics.

This module implements mathematical kernels only.  It cannot certify that its
inputs were prospective, point-in-time correct, exchangeable, selected without
leakage, or drawn from a precommitted calibration cohort.  Nothing here is wired
to serving, persistence, scheduling, or public calibration claims.

ACI state is deliberately not composed with the finite-sample selector here.
ACI may evolve to a non-thousandth miscoverage, while v1 finite-sample coverage
is restricted to canonical thousandths.  A future adapter must pin that mapping
instead of introducing an implicit rounding policy.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from numbers import Integral, Real

FINITE_SAMPLE_POLICY_VERSION = "finite-sample-nearest-rank-v1"
ABSOLUTE_RESIDUAL_POLICY_VERSION = "absolute-residual-v1"
CQR_POLICY_VERSION = "signed-cqr-v1"
ACI_POLICY_VERSION = "projected-aci-v1"

_COVERAGE_SCALE = 1_000
_COVERAGE_TOLERANCE = 1e-12


class ConformalValidationError(ValueError):
    """Conformal inputs or a requested operation violate the pinned policy."""


class InsufficientCalibrationData(ConformalValidationError):
    """A finite sample cannot supply the requested conformal order statistic."""


@dataclass(frozen=True, slots=True)
class QuantileSelection:
    """One finite-sample, non-interpolated conformal order statistic."""

    coverage: float
    sample_count: int
    rank: int
    value: float
    policy_version: str = FINITE_SAMPLE_POLICY_VERSION

    def __post_init__(self) -> None:
        coverage, coverage_millis = _coverage(self.coverage)
        sample_count = _positive_integer(self.sample_count, "sample_count")
        rank = _positive_integer(self.rank, "rank")
        expected_rank = _rank(sample_count, coverage_millis)
        if expected_rank > sample_count:
            raise InsufficientCalibrationData(
                "the requested coverage has no finite order statistic for this sample"
            )
        if rank != expected_rank:
            raise ConformalValidationError("rank does not match the finite-sample policy")
        value = _finite(self.value, "value")
        if self.policy_version != FINITE_SAMPLE_POLICY_VERSION:
            raise ConformalValidationError("quantile policy_version is not supported")
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(self, "sample_count", sample_count)
        object.__setattr__(self, "rank", rank)
        object.__setattr__(self, "value", value)


@dataclass(frozen=True, slots=True)
class AbsoluteResidualCalibration:
    """Symmetric split-conformal radius fitted from absolute residuals."""

    selection: QuantileSelection
    policy_version: str = ABSOLUTE_RESIDUAL_POLICY_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.selection, QuantileSelection):
            raise TypeError("selection must be a QuantileSelection")
        if self.selection.value < 0.0:
            raise ConformalValidationError("absolute-residual radius cannot be negative")
        if self.policy_version != ABSOLUTE_RESIDUAL_POLICY_VERSION:
            raise ConformalValidationError("absolute-residual policy_version is not supported")

    def interval(self, point: float) -> tuple[float, float]:
        """Apply the fitted radius without imposing a target-domain clamp."""

        center = _finite(point, "point")
        radius = self.selection.value
        lower = _finite_result(center - radius, "absolute-residual lower bound")
        upper = _finite_result(center + radius, "absolute-residual upper bound")
        return lower, upper


@dataclass(frozen=True, slots=True)
class CQRCalibration:
    """Signed split conformalized-quantile-regression correction."""

    selection: QuantileSelection
    policy_version: str = CQR_POLICY_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.selection, QuantileSelection):
            raise TypeError("selection must be a QuantileSelection")
        if self.policy_version != CQR_POLICY_VERSION:
            raise ConformalValidationError("CQR policy_version is not supported")

    def interval(self, lower: float, upper: float) -> tuple[float, float]:
        """Apply the signed correction, failing closed if it creates an empty set."""

        raw_lower, raw_upper = _ordered_interval(lower, upper)
        correction = self.selection.value
        calibrated_lower = _finite_result(raw_lower - correction, "CQR lower bound")
        calibrated_upper = _finite_result(raw_upper + correction, "CQR upper bound")
        if calibrated_lower > calibrated_upper:
            raise ConformalValidationError("signed CQR correction produces an empty interval")
        return calibrated_lower, calibrated_upper


@dataclass(frozen=True, slots=True)
class ACIState:
    """Immutable state for the explicitly projected ACI recurrence."""

    target_miscoverage: float
    current_miscoverage: float
    learning_rate: float
    minimum_miscoverage: float
    maximum_miscoverage: float
    update_count: int = 0
    miss_count: int = 0
    algorithm_version: str = ACI_POLICY_VERSION

    def __post_init__(self) -> None:
        target = _open_probability(self.target_miscoverage, "target_miscoverage")
        current = _open_probability(self.current_miscoverage, "current_miscoverage")
        learning_rate = _positive_finite(self.learning_rate, "learning_rate")
        minimum = _open_probability(self.minimum_miscoverage, "minimum_miscoverage")
        maximum = _open_probability(self.maximum_miscoverage, "maximum_miscoverage")
        if not minimum <= target <= maximum:
            raise ConformalValidationError(
                "target_miscoverage must be within the projection bounds"
            )
        if not minimum <= current <= maximum:
            raise ConformalValidationError(
                "current_miscoverage must be within the projection bounds"
            )
        updates = _nonnegative_integer(self.update_count, "update_count")
        misses = _nonnegative_integer(self.miss_count, "miss_count")
        if misses > updates:
            raise ConformalValidationError("miss_count cannot exceed update_count")
        if self.algorithm_version != ACI_POLICY_VERSION:
            raise ConformalValidationError("ACI algorithm_version is not supported")
        object.__setattr__(self, "target_miscoverage", target)
        object.__setattr__(self, "current_miscoverage", current)
        object.__setattr__(self, "learning_rate", learning_rate)
        object.__setattr__(self, "minimum_miscoverage", minimum)
        object.__setattr__(self, "maximum_miscoverage", maximum)
        object.__setattr__(self, "update_count", updates)
        object.__setattr__(self, "miss_count", misses)


def finite_sample_quantile(
    scores: Sequence[float],
    *,
    coverage: float,
) -> QuantileSelection:
    """Select rank ``ceil((n+1)*coverage)`` without interpolation or clipping.

    Public coverages are canonical thousandths.  If the rank is ``n+1``, the
    formal augmented order statistic is positive infinity; this finite-only
    implementation refuses instead of silently substituting the sample maximum.
    """

    normalized = _finite_sequence(scores, "scores")
    canonical_coverage, coverage_millis = _coverage(coverage)
    rank = _rank(len(normalized), coverage_millis)
    if rank > len(normalized):
        raise InsufficientCalibrationData(
            "the requested coverage has no finite order statistic for this sample"
        )
    value = sorted(normalized)[rank - 1]
    return QuantileSelection(
        coverage=canonical_coverage,
        sample_count=len(normalized),
        rank=rank,
        value=value,
    )


def absolute_residual_scores(
    actual: Sequence[float],
    point: Sequence[float],
) -> tuple[float, ...]:
    """Return finite ``abs(actual - point)`` split-conformal scores."""

    actual_values, point_values = _paired(actual, point, "actual", "point")
    return tuple(
        abs(_finite_result(observed - predicted, "absolute residual"))
        for observed, predicted in zip(actual_values, point_values, strict=True)
    )


def cqr_scores(
    actual: Sequence[float],
    lower: Sequence[float],
    upper: Sequence[float],
) -> tuple[float, ...]:
    """Return standard signed CQR scores ``max(lower-y, y-upper)``.

    Negative scores are retained deliberately: they permit conformal shrinkage.
    Clamping them at zero is a distinct conservative policy and is not v1 CQR.
    """

    actual_values = _finite_sequence(actual, "actual")
    lower_values = _finite_sequence(lower, "lower")
    upper_values = _finite_sequence(upper, "upper")
    if len({len(actual_values), len(lower_values), len(upper_values)}) != 1:
        raise ConformalValidationError("actual, lower, and upper must have equal lengths")
    scores: list[float] = []
    for observed, low, high in zip(
        actual_values,
        lower_values,
        upper_values,
        strict=True,
    ):
        if low > high:
            raise ConformalValidationError("lower cannot be greater than upper")
        below = _finite_result(low - observed, "CQR lower score")
        above = _finite_result(observed - high, "CQR upper score")
        scores.append(max(below, above))
    return tuple(scores)


def fit_absolute_residual(
    actual: Sequence[float],
    point: Sequence[float],
    *,
    coverage: float,
) -> AbsoluteResidualCalibration:
    """Fit one symmetric split-conformal radius."""

    return AbsoluteResidualCalibration(
        selection=finite_sample_quantile(
            absolute_residual_scores(actual, point),
            coverage=coverage,
        )
    )


def fit_cqr(
    actual: Sequence[float],
    lower: Sequence[float],
    upper: Sequence[float],
    *,
    coverage: float,
) -> CQRCalibration:
    """Fit one signed split-CQR correction."""

    return CQRCalibration(
        selection=finite_sample_quantile(
            cqr_scores(actual, lower, upper),
            coverage=coverage,
        )
    )


def interval_missed(actual: float, lower: float, upper: float) -> bool:
    """Return whether ``actual`` falls outside inclusive finite bounds."""

    observed = _finite(actual, "actual")
    low, high = _ordered_interval(lower, upper)
    return not low <= observed <= high


def aci_transition(state: ACIState, *, missed: bool) -> ACIState:
    """Apply one projected ACI update in an externally fixed outcome order.

    ``raw = alpha_t + gamma * (alpha_target - error_t)`` where ``error_t`` is
    one for a miss and zero for coverage.  The returned alpha is projected to
    the state's explicit inclusive bounds.  This function does not choose or
    validate the chronological update order.
    """

    if not isinstance(state, ACIState):
        raise TypeError("state must be an ACIState")
    if type(missed) is not bool:
        raise TypeError("missed must be a bool")
    error = 1.0 if missed else 0.0
    delta = _finite_result(
        state.learning_rate * (state.target_miscoverage - error),
        "ACI update",
    )
    raw = _finite_result(state.current_miscoverage + delta, "ACI update")
    projected = min(state.maximum_miscoverage, max(state.minimum_miscoverage, raw))
    return ACIState(
        target_miscoverage=state.target_miscoverage,
        current_miscoverage=projected,
        learning_rate=state.learning_rate,
        minimum_miscoverage=state.minimum_miscoverage,
        maximum_miscoverage=state.maximum_miscoverage,
        update_count=state.update_count + 1,
        miss_count=state.miss_count + int(missed),
        algorithm_version=state.algorithm_version,
    )


def _coverage(value: object) -> tuple[float, int]:
    coverage = _finite(value, "coverage")
    if not 0.0 < coverage < 1.0:
        raise ConformalValidationError("coverage must be strictly between zero and one")
    millis = round(coverage * _COVERAGE_SCALE)
    canonical = millis / _COVERAGE_SCALE
    if not 1 <= millis <= 999 or abs(coverage - canonical) > _COVERAGE_TOLERANCE:
        raise ConformalValidationError("coverage must be a canonical thousandth")
    return canonical, millis


def _rank(sample_count: int, coverage_millis: int) -> int:
    return ((sample_count + 1) * coverage_millis + _COVERAGE_SCALE - 1) // _COVERAGE_SCALE


def _paired(
    left: Sequence[float],
    right: Sequence[float],
    left_name: str,
    right_name: str,
) -> tuple[tuple[float, ...], tuple[float, ...]]:
    left_values = _finite_sequence(left, left_name)
    right_values = _finite_sequence(right, right_name)
    if len(left_values) != len(right_values):
        raise ConformalValidationError(f"{left_name} and {right_name} must have equal lengths")
    return left_values, right_values


def _finite_sequence(values: Sequence[float], name: str) -> tuple[float, ...]:
    if isinstance(values, (str, bytes)):
        raise ConformalValidationError(f"{name} must be a nonempty sequence")
    try:
        normalized = tuple(_finite(value, name) for value in values)
    except TypeError as exc:
        raise ConformalValidationError(f"{name} must be a nonempty sequence") from exc
    if not normalized:
        raise ConformalValidationError(f"{name} must be a nonempty sequence")
    return normalized


def _ordered_interval(lower: object, upper: object) -> tuple[float, float]:
    low = _finite(lower, "lower")
    high = _finite(upper, "upper")
    if low > high:
        raise ConformalValidationError("lower cannot be greater than upper")
    return low, high


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ConformalValidationError(f"{name} must contain only finite real numbers")
    try:
        converted = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ConformalValidationError(f"{name} must contain only finite real numbers") from exc
    if not math.isfinite(converted):
        raise ConformalValidationError(f"{name} must contain only finite real numbers")
    return converted


def _finite_result(value: float, name: str) -> float:
    if not math.isfinite(value):
        raise ConformalValidationError(f"{name} overflowed to a nonfinite value")
    return value


def _positive_finite(value: object, name: str) -> float:
    converted = _finite(value, name)
    if converted <= 0.0:
        raise ConformalValidationError(f"{name} must be positive")
    return converted


def _open_probability(value: object, name: str) -> float:
    converted = _finite(value, name)
    if not 0.0 < converted < 1.0:
        raise ConformalValidationError(f"{name} must be strictly between zero and one")
    return converted


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral) or value < 1:
        raise ConformalValidationError(f"{name} must be a positive integer")
    return int(value)


def _nonnegative_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral) or value < 0:
        raise ConformalValidationError(f"{name} must be a nonnegative integer")
    return int(value)


__all__ = [
    "ABSOLUTE_RESIDUAL_POLICY_VERSION",
    "ACI_POLICY_VERSION",
    "CQR_POLICY_VERSION",
    "FINITE_SAMPLE_POLICY_VERSION",
    "ACIState",
    "AbsoluteResidualCalibration",
    "CQRCalibration",
    "ConformalValidationError",
    "InsufficientCalibrationData",
    "QuantileSelection",
    "absolute_residual_scores",
    "aci_transition",
    "cqr_scores",
    "finite_sample_quantile",
    "fit_absolute_residual",
    "fit_cqr",
    "interval_missed",
]
