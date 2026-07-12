"""Deterministic expanding-window evaluation for forecast baselines.

This module can prove only that each model receives an outer prefix of the
supplied, already-resolved numeric sequence. It cannot prove that the sequence
was point-in-time correct, that revisions were isolated, or that a generic
forecaster's internal computations are leakage-free. Result metadata keeps
those checks explicitly unverified.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from numbers import Integral, Real
from typing import Literal

from ml.evaluation.metrics import (
    empirical_interval_coverage,
    mean_absolute_error,
    pinball_loss,
    root_mean_squared_error,
)
from ml.models.base import Forecaster

_LEVEL_TOLERANCE = 1e-12


class WalkForwardEvaluationError(ValueError):
    """A candidate failed at a specific expanding-window origin."""


@dataclass(frozen=True)
class WalkForwardConfig:
    """Shared origin and interval configuration for comparable candidates."""

    initial_train_size: int
    horizon: int
    stride: int = 1
    interval_coverages: tuple[float, ...] = (0.8,)

    def __post_init__(self) -> None:
        _positive_integer(self.initial_train_size, name="initial_train_size")
        _positive_integer(self.horizon, name="horizon")
        _positive_integer(self.stride, name="stride")
        normalized = _interval_coverages(self.interval_coverages)
        if normalized != self.interval_coverages:
            raise ValueError("interval_coverages must be sorted and canonical")

    @property
    def quantile_levels(self) -> tuple[float, ...]:
        """Median plus both central-interval endpoints, in stable order."""

        return _quantile_levels(self.interval_coverages)


@dataclass(frozen=True)
class BaselineCandidate:
    """A named model factory evaluated from a fresh template at every origin."""

    name: str
    model_factory: Callable[[], Forecaster] = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name or self.name.strip() != self.name:
            raise ValueError("candidate name must be a non-empty trimmed string")
        if not callable(self.model_factory):
            raise ValueError("model_factory must be callable")


@dataclass(frozen=True)
class QuantileScore:
    level: float
    pinball_loss: float


@dataclass(frozen=True)
class CoverageScore:
    nominal_coverage: float
    empirical_coverage: float


@dataclass(frozen=True)
class MetricSummary:
    """Point, quantile, and coverage scores for a declared aggregation rule."""

    sample_count: int
    mae: float
    rmse: float
    mean_pinball_loss: float
    by_quantile: tuple[QuantileScore, ...]
    by_coverage: tuple[CoverageScore, ...]
    aggregation: Literal["within_horizon", "equal_horizon_macro"]


@dataclass(frozen=True)
class HorizonMetrics:
    horizon_step: int
    metrics: MetricSummary


@dataclass(frozen=True)
class QuantilePath:
    level: float
    values: tuple[float, ...]


@dataclass(frozen=True)
class OriginForecast:
    """One frozen rolling-origin forecast and its realized full-horizon targets."""

    train_end_exclusive: int
    origin_index: int
    target_start_index: int
    actual: tuple[float, ...]
    point: tuple[float, ...]
    quantiles: tuple[QuantilePath, ...]


@dataclass(frozen=True)
class ModelEvaluation:
    candidate_name: str
    model_version: str
    origins: tuple[OriginForecast, ...]
    by_horizon: tuple[HorizonMetrics, ...]
    aggregate: MetricSummary
    evaluation_method: Literal["expanding_window_prefix_only"] = "expanding_window_prefix_only"
    point_in_time_verified: bool = False
    lookahead_status: Literal["not_run"] = "not_run"
    purged: bool = False
    embargoed: bool = False
    dependence_note: str = "Overlapping rolling-origin forecast cells are not independent samples."


@dataclass(frozen=True)
class LeaderboardEntry:
    rank: int
    evaluation: ModelEvaluation


@dataclass(frozen=True)
class BaselineLeaderboard:
    config: WalkForwardConfig
    entries: tuple[LeaderboardEntry, ...]
    ranking_metric: Literal["aggregate_mean_pinball_loss"] = "aggregate_mean_pinball_loss"
    ranking_order: tuple[str, ...] = (
        "aggregate_mean_pinball_loss",
        "aggregate_rmse",
        "aggregate_mae",
        "candidate_name",
        "model_version",
    )
    aggregation: Literal["equal_horizon_macro"] = "equal_horizon_macro"
    point_in_time_verified: bool = False
    lookahead_status: Literal["not_run"] = "not_run"
    selection_status: Literal["evaluation_only_not_post_selection_test"] = (
        "evaluation_only_not_post_selection_test"
    )


def evaluate_walk_forward(
    values: Sequence[float],
    candidate: BaselineCandidate,
    config: WalkForwardConfig,
) -> ModelEvaluation:
    """Evaluate one candidate on every eligible complete-horizon origin.

    For each ``t`` in
    ``range(initial_train_size, len(values) - horizon + 1, stride)``, the model
    sees exactly ``values[:t]`` and is scored against ``values[t:t+horizon]``.
    A failed origin fails the whole evaluation; origins are never silently
    dropped in a model-dependent way.
    """

    if not isinstance(candidate, BaselineCandidate):
        raise TypeError("candidate must be a BaselineCandidate")
    if not isinstance(config, WalkForwardConfig):
        raise TypeError("config must be a WalkForwardConfig")
    series = _finite_sequence(values)
    if len(series) < config.initial_train_size + config.horizon:
        raise ValueError("values must contain initial_train_size plus a complete forecast horizon")

    levels = config.quantile_levels
    origins: list[OriginForecast] = []
    model_instances: list[Forecaster] = []
    model_version: str | None = None
    last_train_size = len(series) - config.horizon
    for train_size in range(config.initial_train_size, last_train_size + 1, config.stride):
        try:
            template = candidate.model_factory()
            if not isinstance(template, Forecaster):
                raise TypeError("model_factory must return a Forecaster")
            if any(template is prior for prior in model_instances):
                raise TypeError("model_factory must return a fresh template at every origin")
            model_instances.append(template)
            fitted = template.fit(series[:train_size])
            if fitted is template:
                raise TypeError("Forecaster.fit must return a distinct origin-local instance")
            if not isinstance(fitted, Forecaster):
                raise TypeError("Forecaster.fit must return a Forecaster")
            if any(fitted is prior for prior in model_instances):
                raise TypeError(
                    "Forecaster.fit must return a fresh fitted instance at every origin"
                )
            model_instances.append(fitted)
            current_version = _model_version(fitted.model_version)
            if model_version is None:
                model_version = current_version
            elif current_version != model_version:
                raise ValueError("model_version must remain stable across origins")

            point = _forecast_path(fitted.predict(config.horizon), config.horizon, "point")
            quantiles = _quantile_paths(
                fitted.predict_quantiles(config.horizon, levels),
                levels,
                config.horizon,
            )
            _validate_distribution(point, quantiles, levels)
        except Exception as exc:
            raise WalkForwardEvaluationError(
                f"candidate {candidate.name!r} failed at train_size={train_size}: {exc}"
            ) from exc

        origins.append(
            OriginForecast(
                train_end_exclusive=train_size,
                origin_index=train_size - 1,
                target_start_index=train_size,
                actual=series[train_size : train_size + config.horizon],
                point=point,
                quantiles=tuple(
                    QuantilePath(level=level, values=quantiles[level]) for level in levels
                ),
            )
        )

    if model_version is None:  # Defensive: the length check guarantees at least one origin.
        raise RuntimeError("walk-forward evaluation produced no origins")
    by_horizon = _score_by_horizon(origins, levels, config.interval_coverages)
    return ModelEvaluation(
        candidate_name=candidate.name,
        model_version=model_version,
        origins=tuple(origins),
        by_horizon=by_horizon,
        aggregate=_macro_summary(by_horizon),
    )


def build_baseline_leaderboard(
    values: Sequence[float],
    candidates: Sequence[BaselineCandidate],
    config: WalkForwardConfig,
) -> BaselineLeaderboard:
    """Evaluate candidates on one shared origin grid and rank deterministically."""

    try:
        candidate_list = tuple(candidates)
    except TypeError as exc:
        raise ValueError("candidates must be a non-empty sequence") from exc
    if not candidate_list:
        raise ValueError("candidates must be a non-empty sequence")
    if any(not isinstance(candidate, BaselineCandidate) for candidate in candidate_list):
        raise TypeError("candidates must contain only BaselineCandidate values")
    names = [candidate.name for candidate in candidate_list]
    if len(set(names)) != len(names):
        raise ValueError("candidate names must be unique")

    series = _finite_sequence(values)
    evaluations = [evaluate_walk_forward(series, candidate, config) for candidate in candidate_list]
    identities = [evaluation.model_version for evaluation in evaluations]
    if len(set(identities)) != len(identities):
        raise ValueError("candidate model_version identities must be unique")
    ordered = sorted(
        evaluations,
        key=lambda evaluation: (
            evaluation.aggregate.mean_pinball_loss,
            evaluation.aggregate.rmse,
            evaluation.aggregate.mae,
            evaluation.candidate_name,
            evaluation.model_version,
        ),
    )
    entries = tuple(
        LeaderboardEntry(rank=index, evaluation=evaluation)
        for index, evaluation in enumerate(ordered, start=1)
    )
    return BaselineLeaderboard(config=config, entries=entries)


def _positive_integer(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return int(value)


def _finite_sequence(values: Sequence[float]) -> tuple[float, ...]:
    try:
        raw_values = tuple(values)
    except TypeError as exc:
        raise ValueError("values must be a finite sequence") from exc
    normalized: list[float] = []
    for value in raw_values:
        normalized.append(_finite(value, "values"))
    return tuple(normalized)


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{label} must contain only finite real numbers")
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{label} must contain only finite real numbers")
    return converted


def _interval_coverages(coverages: Sequence[float]) -> tuple[float, ...]:
    try:
        raw_coverages = tuple(coverages)
    except TypeError as exc:
        raise ValueError("interval_coverages must be a non-empty sequence") from exc
    if not raw_coverages:
        raise ValueError("interval_coverages must be a non-empty sequence")
    normalized: list[float] = []
    for raw in raw_coverages:
        coverage = _finite_probability(raw, label="interval_coverages")
        canonical = round(coverage, 3)
        if abs(coverage - canonical) > _LEVEL_TOLERANCE:
            raise ValueError("interval_coverages support at most three decimal places")
        normalized.append(canonical)
    if len(set(normalized)) != len(normalized):
        raise ValueError("interval_coverages must not contain duplicates")
    if normalized != sorted(normalized):
        raise ValueError("interval_coverages must be sorted")
    return tuple(normalized)


def _finite_probability(value: object, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{label} must contain probabilities strictly between 0 and 1")
    converted = float(value)
    if not math.isfinite(converted) or not 0.0 < converted < 1.0:
        raise ValueError(f"{label} must contain probabilities strictly between 0 and 1")
    return converted


def _quantile_levels(coverages: tuple[float, ...]) -> tuple[float, ...]:
    levels = {0.5}
    for coverage in coverages:
        levels.add(round((1.0 - coverage) / 2.0, 12))
        levels.add(round((1.0 + coverage) / 2.0, 12))
    return tuple(sorted(levels))


def _model_version(value: object) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError("model_version must be a non-empty trimmed string")
    return value


def _forecast_path(values: object, horizon: int, label: str) -> tuple[float, ...]:
    if not isinstance(values, Iterable):
        raise ValueError(f"{label} forecast must be a sequence")
    raw_values: tuple[object, ...] = tuple(values)
    if len(raw_values) != horizon:
        raise ValueError(f"{label} forecast length must match horizon")
    return tuple(_finite(value, f"{label} forecast") for value in raw_values)


def _quantile_paths(
    paths: object,
    levels: tuple[float, ...],
    horizon: int,
) -> dict[float, tuple[float, ...]]:
    if not isinstance(paths, dict):
        raise ValueError("quantile forecasts must be a dictionary")
    normalized: dict[float, tuple[float, ...]] = {}
    for raw_level, raw_path in paths.items():
        level = _finite_probability(raw_level, label="quantile levels")
        match = next(
            (requested for requested in levels if abs(requested - level) <= _LEVEL_TOLERANCE),
            None,
        )
        if match is None:
            raise ValueError(f"forecaster returned unexpected quantile {level}")
        if match in normalized:
            raise ValueError(f"forecaster returned duplicate quantile {match}")
        normalized[match] = _forecast_path(raw_path, horizon, f"quantile {match}")
    missing = [level for level in levels if level not in normalized]
    if missing:
        raise ValueError(f"forecaster omitted requested quantiles {missing}")
    return normalized


def _validate_distribution(
    point: tuple[float, ...],
    quantiles: dict[float, tuple[float, ...]],
    levels: tuple[float, ...],
) -> None:
    for step, point_value in enumerate(point):
        ordered = [quantiles[level][step] for level in levels]
        if any(
            current > following for current, following in zip(ordered, ordered[1:], strict=False)
        ):
            raise ValueError("forecaster returned crossing quantiles")
        if not math.isclose(quantiles[0.5][step], point_value, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError("forecaster point must equal its median quantile")


def _score_by_horizon(
    origins: Sequence[OriginForecast],
    levels: tuple[float, ...],
    coverages: tuple[float, ...],
) -> tuple[HorizonMetrics, ...]:
    rows: list[HorizonMetrics] = []
    horizon = len(origins[0].actual)
    for offset in range(horizon):
        actual = [origin.actual[offset] for origin in origins]
        point = [origin.point[offset] for origin in origins]
        quantile_values = {
            level: [
                next(path.values for path in origin.quantiles if path.level == level)[offset]
                for origin in origins
            ]
            for level in levels
        }
        quantile_scores = tuple(
            QuantileScore(
                level=level,
                pinball_loss=pinball_loss(actual, quantile_values[level], level),
            )
            for level in levels
        )
        coverage_scores = tuple(
            CoverageScore(
                nominal_coverage=coverage,
                empirical_coverage=empirical_interval_coverage(
                    actual,
                    quantile_values[round((1.0 - coverage) / 2.0, 12)],
                    quantile_values[round((1.0 + coverage) / 2.0, 12)],
                ),
            )
            for coverage in coverages
        )
        rows.append(
            HorizonMetrics(
                horizon_step=offset + 1,
                metrics=MetricSummary(
                    sample_count=len(origins),
                    mae=mean_absolute_error(actual, point),
                    rmse=root_mean_squared_error(actual, point),
                    mean_pinball_loss=_safe_mean([score.pinball_loss for score in quantile_scores]),
                    by_quantile=quantile_scores,
                    by_coverage=coverage_scores,
                    aggregation="within_horizon",
                ),
            )
        )
    return tuple(rows)


def _macro_summary(by_horizon: tuple[HorizonMetrics, ...]) -> MetricSummary:
    metrics = [row.metrics for row in by_horizon]
    levels = [score.level for score in metrics[0].by_quantile]
    coverages = [score.nominal_coverage for score in metrics[0].by_coverage]
    return MetricSummary(
        sample_count=sum(metric.sample_count for metric in metrics),
        mae=_safe_mean([metric.mae for metric in metrics]),
        rmse=_safe_mean([metric.rmse for metric in metrics]),
        mean_pinball_loss=_safe_mean([metric.mean_pinball_loss for metric in metrics]),
        by_quantile=tuple(
            QuantileScore(
                level=level,
                pinball_loss=_safe_mean(
                    [
                        next(
                            score.pinball_loss
                            for score in metric.by_quantile
                            if score.level == level
                        )
                        for metric in metrics
                    ]
                ),
            )
            for level in levels
        ),
        by_coverage=tuple(
            CoverageScore(
                nominal_coverage=coverage,
                empirical_coverage=_safe_mean(
                    [
                        next(
                            score.empirical_coverage
                            for score in metric.by_coverage
                            if score.nominal_coverage == coverage
                        )
                        for metric in metrics
                    ]
                ),
            )
            for coverage in coverages
        ),
        aggregation="equal_horizon_macro",
    )


def _safe_mean(values: Sequence[float]) -> float:
    scale = max(abs(value) for value in values)
    if scale == 0.0:
        return 0.0
    result = scale * math.fsum(value / scale / len(values) for value in values)
    if not math.isfinite(result):
        raise ValueError("metric aggregation produced a non-finite result")
    return result


__all__ = [
    "BaselineCandidate",
    "BaselineLeaderboard",
    "CoverageScore",
    "HorizonMetrics",
    "LeaderboardEntry",
    "MetricSummary",
    "ModelEvaluation",
    "OriginForecast",
    "QuantilePath",
    "QuantileScore",
    "WalkForwardConfig",
    "WalkForwardEvaluationError",
    "build_baseline_leaderboard",
    "evaluate_walk_forward",
]
