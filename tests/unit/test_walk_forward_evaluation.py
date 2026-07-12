from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from ml.evaluation.walk_forward import (
    BaselineCandidate,
    WalkForwardConfig,
    WalkForwardEvaluationError,
    build_baseline_leaderboard,
    evaluate_walk_forward,
)
from ml.models.baselines import DriftForecaster, NaiveForecaster, SeasonalNaiveForecaster


@dataclass
class Calls:
    factory_count: int = 0
    histories: list[tuple[float, ...]] = field(default_factory=list)
    predict_horizons: list[int] = field(default_factory=list)
    quantile_calls: list[tuple[int, tuple[float, ...]]] = field(default_factory=list)
    fitted_ids: list[int] = field(default_factory=list)


class RecordingForecaster:
    def __init__(
        self,
        calls: Calls,
        *,
        model_version: str = "fixture@1",
        bias: float = 0.0,
        spread: float = 2.0,
    ) -> None:
        self.calls = calls
        self.model_version = model_version
        self.bias = bias
        self.spread = spread
        self.history: tuple[float, ...] | None = None

    def fit(self, values: Sequence[float]) -> RecordingForecaster:
        history = tuple(values)
        self.calls.histories.append(history)
        fitted = RecordingForecaster(
            self.calls,
            model_version=self.model_version,
            bias=self.bias,
            spread=self.spread,
        )
        fitted.history = history
        self.calls.fitted_ids.append(id(fitted))
        return fitted

    def predict(self, horizon: int) -> list[float]:
        assert self.history is not None
        self.calls.predict_horizons.append(horizon)
        return [self.history[-1] + self.bias] * horizon

    def predict_quantiles(
        self,
        horizon: int,
        quantiles: Sequence[float],
    ) -> dict[float, list[float]]:
        assert self.history is not None
        levels = tuple(quantiles)
        self.calls.quantile_calls.append((horizon, levels))
        point = self.history[-1] + self.bias
        # Reverse insertion order to prove dictionary ordering is irrelevant.
        return {
            level: [
                point - self.spread
                if level < 0.5
                else point + self.spread
                if level > 0.5
                else point
            ]
            * horizon
            for level in reversed(levels)
        }


def recording_candidate(
    name: str = "fixture",
    *,
    version: str = "fixture@1",
    bias: float = 0.0,
) -> tuple[BaselineCandidate, Calls]:
    calls = Calls()

    def factory() -> RecordingForecaster:
        calls.factory_count += 1
        return RecordingForecaster(calls, model_version=version, bias=bias)

    return BaselineCandidate(name=name, model_factory=factory), calls


def test_walk_forward_golden_origins_metrics_and_honest_metadata() -> None:
    candidate, calls = recording_candidate()
    config = WalkForwardConfig(
        initial_train_size=3,
        horizon=2,
        interval_coverages=(0.5,),
    )

    result = evaluate_walk_forward([10, 12, 11, 15, 14, 18], candidate, config)

    assert calls.factory_count == 2
    assert calls.histories == [(10.0, 12.0, 11.0), (10.0, 12.0, 11.0, 15.0)]
    assert calls.predict_horizons == [2, 2]
    assert calls.quantile_calls == [(2, (0.25, 0.5, 0.75))] * 2
    assert len(set(calls.fitted_ids)) == 2

    first, second = result.origins
    assert (
        first.train_end_exclusive,
        first.origin_index,
        first.target_start_index,
        first.actual,
        first.point,
    ) == (3, 2, 3, (15.0, 14.0), (11.0, 11.0))
    assert [(path.level, path.values) for path in first.quantiles] == [
        (0.25, (9.0, 9.0)),
        (0.5, (11.0, 11.0)),
        (0.75, (13.0, 13.0)),
    ]
    assert (
        second.train_end_exclusive,
        second.origin_index,
        second.target_start_index,
        second.actual,
        second.point,
    ) == (4, 3, 4, (14.0, 18.0), (15.0, 15.0))

    horizon_one, horizon_two = result.by_horizon
    assert horizon_one.horizon_step == 1
    assert horizon_one.metrics.sample_count == 2
    assert horizon_one.metrics.mae == pytest.approx(2.5)
    assert horizon_one.metrics.rmse == pytest.approx(math.sqrt(8.5))
    assert [score.pinball_loss for score in horizon_one.metrics.by_quantile] == pytest.approx(
        [0.875, 1.25, 1.125]
    )
    assert horizon_one.metrics.mean_pinball_loss == pytest.approx(13 / 12)
    assert horizon_one.metrics.by_coverage[0].empirical_coverage == pytest.approx(0.5)

    assert horizon_two.horizon_step == 2
    assert horizon_two.metrics.sample_count == 2
    assert horizon_two.metrics.mae == pytest.approx(3.0)
    assert horizon_two.metrics.rmse == pytest.approx(3.0)
    assert [score.pinball_loss for score in horizon_two.metrics.by_quantile] == pytest.approx(
        [1.25, 1.5, 0.75]
    )
    assert horizon_two.metrics.mean_pinball_loss == pytest.approx(7 / 6)
    assert horizon_two.metrics.by_coverage[0].empirical_coverage == pytest.approx(0.0)

    assert result.aggregate.aggregation == "equal_horizon_macro"
    assert result.aggregate.sample_count == 4
    assert result.aggregate.mae == pytest.approx(2.75)
    assert result.aggregate.rmse == pytest.approx((math.sqrt(8.5) + 3.0) / 2.0)
    assert result.aggregate.mean_pinball_loss == pytest.approx(1.125)
    assert result.aggregate.by_coverage[0].empirical_coverage == pytest.approx(0.25)
    assert result.evaluation_method == "expanding_window_prefix_only"
    assert result.point_in_time_verified is False
    assert result.lookahead_status == "not_run"
    assert result.purged is False
    assert result.embargoed is False
    assert "not independent" in result.dependence_note


def test_tail_changes_cannot_change_an_earlier_origin_forecast() -> None:
    candidate_a, calls_a = recording_candidate()
    candidate_b, calls_b = recording_candidate()
    config = WalkForwardConfig(3, 2, interval_coverages=(0.5,))

    first = evaluate_walk_forward([10, 12, 11, 15, 14, 18], candidate_a, config)
    changed = evaluate_walk_forward([10, 12, 11, 900, -400, 700], candidate_b, config)

    assert calls_a.histories[0] == calls_b.histories[0] == (10.0, 12.0, 11.0)
    assert first.origins[0].point == changed.origins[0].point
    assert first.origins[0].quantiles == changed.origins[0].quantiles


def test_stride_and_last_complete_origin_are_explicit() -> None:
    candidate, calls = recording_candidate()
    result = evaluate_walk_forward(
        [1, 2, 3, 4, 5, 6, 7],
        candidate,
        WalkForwardConfig(3, 2, stride=2, interval_coverages=(0.5,)),
    )

    assert [origin.train_end_exclusive for origin in result.origins] == [3, 5]
    assert calls.histories == [(1.0, 2.0, 3.0), (1.0, 2.0, 3.0, 4.0, 5.0)]
    assert result.origins[-1].actual == (6.0, 7.0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial_train_size": True, "horizon": 1},
        {"initial_train_size": 0, "horizon": 1},
        {"initial_train_size": 1, "horizon": False},
        {"initial_train_size": 1, "horizon": 0},
        {"initial_train_size": 1, "horizon": 1, "stride": 0},
        {"initial_train_size": 1, "horizon": 1, "interval_coverages": ()},
        {"initial_train_size": 1, "horizon": 1, "interval_coverages": (0.8, 0.5)},
        {"initial_train_size": 1, "horizon": 1, "interval_coverages": (0.8, 0.8)},
        {"initial_train_size": 1, "horizon": 1, "interval_coverages": (0.8001,)},
        {"initial_train_size": 1, "horizon": 1, "interval_coverages": (0.0,)},
        {"initial_train_size": 1, "horizon": 1, "interval_coverages": (math.inf,)},
    ],
)
def test_config_rejects_ambiguous_or_noncanonical_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        WalkForwardConfig(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["", " ", " leading", "trailing ", 1])
def test_candidate_name_must_be_canonical(name: object) -> None:
    with pytest.raises(ValueError):
        BaselineCandidate(name=name, model_factory=NaiveForecaster)  # type: ignore[arg-type]


@pytest.mark.parametrize("values", [[], [1, 2], [1, math.nan, 3], [1, math.inf, 3], [1, True, 3]])
def test_evaluator_rejects_missing_or_nonfinite_series(values: list[object]) -> None:
    candidate, _ = recording_candidate()
    with pytest.raises(ValueError):
        evaluate_walk_forward(  # type: ignore[arg-type]
            values,
            candidate,
            WalkForwardConfig(2, 1, interval_coverages=(0.5,)),
        )


class BrokenForecaster(RecordingForecaster):
    def __init__(self, calls: Calls, mode: str, version: str = "broken@1") -> None:
        super().__init__(calls, model_version=version)
        self.mode = mode

    def fit(self, values: Sequence[float]) -> BrokenForecaster:
        if self.mode == "fit_returns_self":
            self.history = tuple(values)
            return self
        fitted = BrokenForecaster(self.calls, self.mode, self.model_version)
        fitted.history = tuple(values)
        return fitted

    def predict(self, horizon: int) -> list[float]:
        values = super().predict(horizon)
        if self.mode == "short_point":
            return values[:-1]
        if self.mode == "nonfinite_point":
            values[0] = math.inf
        return values

    def predict_quantiles(
        self,
        horizon: int,
        quantiles: Sequence[float],
    ) -> dict[float, list[float]]:
        paths = super().predict_quantiles(horizon, quantiles)
        if self.mode == "missing_quantile":
            paths.pop(max(paths))
        elif self.mode == "extra_quantile":
            paths[0.6] = list(paths[0.5])
        elif self.mode == "crossing":
            paths[min(paths)][0] = paths[max(paths)][0] + 1.0
        elif self.mode == "wrong_median":
            paths[0.5][0] += 1.0
        elif self.mode == "nonfinite_quantile":
            paths[min(paths)][0] = math.nan
        return paths


@pytest.mark.parametrize(
    "mode",
    [
        "fit_returns_self",
        "short_point",
        "nonfinite_point",
        "missing_quantile",
        "extra_quantile",
        "crossing",
        "wrong_median",
        "nonfinite_quantile",
    ],
)
def test_candidate_failures_are_contextual_and_never_skip_origins(mode: str) -> None:
    candidate = BaselineCandidate(
        "broken",
        lambda: BrokenForecaster(Calls(), mode),
    )

    with pytest.raises(
        WalkForwardEvaluationError,
        match=r"candidate 'broken' failed at train_size=3",
    ):
        evaluate_walk_forward(
            [10, 12, 11, 15, 14],
            candidate,
            WalkForwardConfig(3, 2, interval_coverages=(0.5,)),
        )


def test_model_version_must_be_stable_across_origins() -> None:
    calls = Calls()

    def factory() -> RecordingForecaster:
        calls.factory_count += 1
        return RecordingForecaster(calls, model_version=f"changing@{calls.factory_count}")

    with pytest.raises(WalkForwardEvaluationError, match="stable across origins"):
        evaluate_walk_forward(
            [10, 12, 11, 15, 14, 18],
            BaselineCandidate("changing", factory),
            WalkForwardConfig(3, 2, interval_coverages=(0.5,)),
        )


def test_factory_must_return_a_fresh_template_for_each_origin() -> None:
    shared = RecordingForecaster(Calls())

    with pytest.raises(WalkForwardEvaluationError, match="fresh template at every origin"):
        evaluate_walk_forward(
            [10, 12, 11, 15, 14, 18],
            BaselineCandidate("shared", lambda: shared),
            WalkForwardConfig(3, 2, interval_coverages=(0.5,)),
        )


def test_leaderboard_is_order_invariant_and_uses_declared_tiebreakers() -> None:
    zeta, _ = recording_candidate("zeta", version="zeta@1")
    worse, _ = recording_candidate("worse", version="worse@1", bias=10.0)
    alpha, _ = recording_candidate("alpha", version="alpha@1")
    config = WalkForwardConfig(3, 2, interval_coverages=(0.5,))
    values = [10, 12, 11, 15, 14, 18]

    first = build_baseline_leaderboard(values, [zeta, worse, alpha], config)
    second = build_baseline_leaderboard(values, [alpha, worse, zeta], config)

    assert [entry.evaluation.candidate_name for entry in first.entries] == [
        "alpha",
        "zeta",
        "worse",
    ]
    assert [entry.evaluation.candidate_name for entry in second.entries] == [
        "alpha",
        "zeta",
        "worse",
    ]
    assert [entry.rank for entry in first.entries] == [1, 2, 3]
    assert first.ranking_metric == "aggregate_mean_pinball_loss"
    assert first.aggregation == "equal_horizon_macro"
    assert first.point_in_time_verified is False
    assert first.lookahead_status == "not_run"
    assert first.selection_status == "evaluation_only_not_post_selection_test"


def test_leaderboard_rejects_duplicate_names_or_model_identities() -> None:
    first, _ = recording_candidate("same", version="first@1")
    second, _ = recording_candidate("same", version="second@1")
    config = WalkForwardConfig(3, 2, interval_coverages=(0.5,))
    values = [10, 12, 11, 15, 14]

    with pytest.raises(ValueError, match="names must be unique"):
        build_baseline_leaderboard(values, [first, second], config)

    one, _ = recording_candidate("one", version="duplicate@1")
    two, _ = recording_candidate("two", version="duplicate@1")
    with pytest.raises(ValueError, match="model_version identities must be unique"):
        build_baseline_leaderboard(values, [one, two], config)


def test_real_baselines_share_the_same_complete_origin_grid() -> None:
    values = [10, 12, 11, 16, 14, 20, 17, 23, 21, 27, 24, 31]
    config = WalkForwardConfig(7, 2, stride=2, interval_coverages=(0.5, 0.8))
    candidates = [
        BaselineCandidate("naive", NaiveForecaster),
        BaselineCandidate("drift", DriftForecaster),
        BaselineCandidate("seasonal", lambda: SeasonalNaiveForecaster(2)),
    ]

    leaderboard = build_baseline_leaderboard(values, candidates, config)

    assert len(leaderboard.entries) == 3
    assert {
        tuple(origin.train_end_exclusive for origin in entry.evaluation.origins)
        for entry in leaderboard.entries
    } == {(7, 9)}
    assert all(entry.evaluation.aggregate.sample_count == 4 for entry in leaderboard.entries)
