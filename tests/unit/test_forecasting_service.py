"""Pure ForecastResponse assembly and fail-closed service tests."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from app.core.exceptions import NotImplementedYet
from app.schemas.forecast import DataSourceLineage, ForecastRequest
from app.services.forecasting import (
    ForecastObservation,
    ForecastRunIdentity,
    ResolvedForecastInput,
    UnavailableForecastService,
    assemble_baseline_forecast_response,
)
from ml.models import NaiveForecaster

AS_OF = datetime(2026, 7, 10, 21, tzinfo=UTC)
GENERATED_AT = AS_OF + timedelta(minutes=1)


class FixedForecaster:
    """Deterministic contract fixture, not a claim about a production model."""

    model_version = "baseline-naive@fixture-1"

    def __init__(
        self,
        *,
        crossing: bool = False,
        median_offset: float = 0.0,
        fit_calls: list[list[float]] | None = None,
    ) -> None:
        self.crossing = crossing
        self.median_offset = median_offset
        self.fit_calls = fit_calls if fit_calls is not None else []
        self.fitted_values: list[float] = []

    def fit(self, values: Sequence[float]) -> FixedForecaster:
        fitted_values = list(values)
        self.fit_calls.append(fitted_values)
        fitted = FixedForecaster(
            crossing=self.crossing,
            median_offset=self.median_offset,
            fit_calls=self.fit_calls,
        )
        fitted.fitted_values = fitted_values
        return fitted

    def predict(self, horizon: int) -> list[float]:
        return [101.0 + step for step in range(horizon)]

    def predict_quantiles(
        self, horizon: int, quantiles: Sequence[float]
    ) -> dict[float, list[float]]:
        paths: dict[float, list[float]] = {}
        for level in quantiles:
            if abs(level - 0.5) < 1e-12:
                paths[level] = [101.0 + step + self.median_offset for step in range(horizon)]
            elif level < 0.5:
                paths[level] = [90.0 + step for step in range(horizon)]
            else:
                paths[level] = [110.0 + step for step in range(horizon)]
        if self.crossing:
            paths[min(quantiles)] = [120.0 + step for step in range(horizon)]
        return paths


class MutatingForecaster(FixedForecaster):
    def fit(self, values: Sequence[float]) -> MutatingForecaster:
        self.fitted_values = list(values)
        return self


def _request(**overrides: object) -> ForecastRequest:
    fields: dict[str, object] = {
        "symbol": "AAPL",
        "horizon": 2,
        "horizon_unit": "trading_day",
        "target": "close",
        "as_of": AS_OF,
        "snapshot_id": "fixture:snapshot:aapl-v1",
        "model": "baseline_naive",
        "interval_coverages": [0.8],
    }
    fields.update(overrides)
    return ForecastRequest.model_validate(fields)


def _resolved(**overrides: object) -> ResolvedForecastInput:
    observations = tuple(
        ForecastObservation(
            observed_at=AS_OF - timedelta(days=4 - index),
            available_at=AS_OF - timedelta(days=4 - index, hours=-1),
            value=value,
        )
        for index, value in enumerate((98.0, 100.0, 99.0, 101.0))
    )
    fields: dict[str, object] = {
        "symbol": "AAPL",
        "target": "close",
        "horizon_unit": "trading_day",
        "series_basis": "raw",
        "snapshot_id": "fixture:snapshot:aapl-v1",
        "as_of": AS_OF,
        "observations": observations,
        "target_times": (AS_OF + timedelta(days=1), AS_OF + timedelta(days=2)),
        "data_sources": (
            DataSourceLineage(
                name="fixture-market-data",
                snapshot_id="fixture:source:aapl-v1",
                max_available_at=AS_OF,
                fields=["close"],
            ),
        ),
        "currency": "USD",
        "availability_verified": True,
    }
    fields.update(overrides)
    return ResolvedForecastInput(**fields)  # type: ignore[arg-type]


def _identity(**overrides: object) -> ForecastRunIdentity:
    fields: dict[str, object] = {
        "forecast_id": UUID("22222222-2222-2222-2222-222222222222"),
        "generated_at": GENERATED_AT,
        "model_version": FixedForecaster.model_version,
        "feature_set_hash": "sha256:" + "b" * 64,
        "code_version": "fixture-code-version",
    }
    fields.update(overrides)
    return ForecastRunIdentity(**fields)  # type: ignore[arg-type]


def test_assembler_maps_exact_quantiles_intervals_and_provenance() -> None:
    forecaster = FixedForecaster()
    response = assemble_baseline_forecast_response(
        _request(),
        _resolved(),
        forecaster_factory=lambda: forecaster,
        identity=_identity(),
    )

    assert forecaster.fit_calls == [[98.0, 100.0, 99.0, 101.0]]
    assert response.symbol == "AAPL"
    assert response.as_of == AS_OF
    assert response.currency == "USD"
    assert [step.step for step in response.forecasts] == [1, 2]
    first = response.forecasts[0]
    assert [item.level for item in first.quantiles] == [0.1, 0.5, 0.9]
    assert first.point == first.quantiles[1].value == 101.0
    assert first.intervals[0].lower == first.quantiles[0].value == 90.0
    assert first.intervals[0].upper == first.quantiles[2].value == 110.0
    assert response.provenance.snapshot_id == "fixture:snapshot:aapl-v1"
    assert response.provenance.lookahead_check.status == "passed"
    assert response.provenance.max_available_at == AS_OF
    assert response.calibration.method == "none"
    assert response.calibration.sample_count == 0
    assert response.calibration.by_interval == []


def test_assembler_accepts_the_real_request_local_naive_baseline() -> None:
    response = assemble_baseline_forecast_response(
        _request(horizon=1),
        replace(_resolved(), target_times=(AS_OF + timedelta(days=1),)),
        forecaster_factory=NaiveForecaster,
        identity=_identity(model_version="baseline-naive@1"),
    )

    assert response.forecasts[0].point == 101.0
    assert response.provenance.model_version == "baseline-naive@1"
    assert response.forecasts[0].intervals[0].lower < response.forecasts[0].point


def test_assembler_is_byte_deterministic_for_identical_injected_identity() -> None:
    created: list[FixedForecaster] = []

    def factory() -> FixedForecaster:
        model = FixedForecaster()
        created.append(model)
        return model

    first = assemble_baseline_forecast_response(
        _request(), _resolved(), forecaster_factory=factory, identity=_identity()
    )
    second = assemble_baseline_forecast_response(
        _request(), _resolved(), forecaster_factory=factory, identity=_identity()
    )

    assert first.model_dump_json() == second.model_dump_json()
    assert len(created) == 2 and created[0] is not created[1]


def test_unverified_availability_is_never_reported_as_passed() -> None:
    response = assemble_baseline_forecast_response(
        _request(),
        replace(_resolved(), availability_verified=False),
        forecaster_factory=FixedForecaster,
        identity=_identity(),
    )

    check = response.provenance.lookahead_check
    assert check.status == "not_run"
    assert check.violations == [
        "availability timestamps were not verified by the snapshot resolver"
    ]


@pytest.mark.parametrize(
    "resolved",
    [
        replace(
            _resolved(),
            observations=(
                *_resolved().observations[:-1],
                replace(_resolved().observations[-1], available_at=AS_OF + timedelta(seconds=1)),
            ),
        ),
        replace(
            _resolved(),
            target_times=(AS_OF + timedelta(days=2), AS_OF + timedelta(days=1)),
        ),
    ],
)
def test_assembler_rejects_lookahead_or_unordered_calendar_input(
    resolved: ResolvedForecastInput,
) -> None:
    with pytest.raises(ValueError):
        assemble_baseline_forecast_response(
            _request(), resolved, forecaster_factory=FixedForecaster, identity=_identity()
        )


def test_assembler_rejects_crossing_quantiles_or_median_mismatch() -> None:
    with pytest.raises(ValueError, match="crossing"):
        assemble_baseline_forecast_response(
            _request(),
            _resolved(),
            forecaster_factory=lambda: FixedForecaster(crossing=True),
            identity=_identity(),
        )
    with pytest.raises(ValueError, match="median"):
        assemble_baseline_forecast_response(
            _request(),
            _resolved(),
            forecaster_factory=lambda: FixedForecaster(median_offset=1.0),
            identity=_identity(),
        )


def test_assembler_rejects_model_identity_mismatch() -> None:
    with pytest.raises(ValueError, match="model_version"):
        assemble_baseline_forecast_response(
            _request(),
            _resolved(),
            forecaster_factory=FixedForecaster,
            identity=_identity(model_version="different-model@1"),
        )


def test_assembler_rejects_mutating_shared_fit_lifecycle() -> None:
    shared = MutatingForecaster()
    with pytest.raises(TypeError, match="distinct request-local"):
        assemble_baseline_forecast_response(
            _request(),
            _resolved(),
            forecaster_factory=lambda: shared,
            identity=_identity(),
        )

    with pytest.raises(ValueError, match="requested model"):
        assemble_baseline_forecast_response(
            _request(model="baseline_drift"),
            _resolved(),
            forecaster_factory=FixedForecaster,
            identity=_identity(),
        )


def test_snapshot_id_overrides_request_as_of_but_unpinned_cutoff_does_not() -> None:
    response = assemble_baseline_forecast_response(
        _request(as_of=AS_OF - timedelta(days=1)),
        _resolved(),
        forecaster_factory=FixedForecaster,
        identity=_identity(),
    )
    assert response.as_of == AS_OF

    with pytest.raises(ValueError, match="later than the requested as_of"):
        assemble_baseline_forecast_response(
            _request(snapshot_id=None, as_of=AS_OF - timedelta(days=1)),
            _resolved(),
            forecaster_factory=FixedForecaster,
            identity=_identity(),
        )


@pytest.mark.parametrize(
    "resolved",
    [
        replace(_resolved(), symbol="MSFT"),
        replace(_resolved(), target="adjusted_close"),
        replace(_resolved(), horizon_unit="calendar_day"),
        replace(_resolved(), series_basis="split_adjusted"),
        replace(_resolved(), availability_verified="false"),  # type: ignore[arg-type]
        replace(
            _resolved(),
            observations=(
                *_resolved().observations[:-1],
                replace(_resolved().observations[-1], value=True),
            ),
        ),
    ],
)
def test_assembler_rejects_mismatched_or_coercible_resolver_facts(
    resolved: ResolvedForecastInput,
) -> None:
    with pytest.raises(ValueError):
        assemble_baseline_forecast_response(
            _request(),
            resolved,
            forecaster_factory=FixedForecaster,
            identity=_identity(),
        )


def test_lineage_order_fields_and_offsets_are_canonicalized() -> None:
    source_a = DataSourceLineage(
        name="a-source",
        snapshot_id="fixture:a",
        max_available_at=AS_OF,
        fields=["volume", "close", "close"],
    )
    source_b = DataSourceLineage(
        name="b-source",
        snapshot_id="fixture:b",
        max_available_at=AS_OF.astimezone(timezone(timedelta(hours=3))),
        fields=["close"],
    )
    first = assemble_baseline_forecast_response(
        _request(),
        replace(_resolved(), data_sources=(source_b, source_a)),
        forecaster_factory=FixedForecaster,
        identity=_identity(),
    )
    second = assemble_baseline_forecast_response(
        _request(),
        replace(_resolved(), data_sources=(source_a, source_b)),
        forecaster_factory=FixedForecaster,
        identity=_identity(),
    )

    assert first.model_dump_json() == second.model_dump_json()
    assert [source.name for source in first.provenance.data_sources] == [
        "a-source",
        "b-source",
    ]
    assert first.provenance.data_sources[0].fields == ["close", "volume"]


@pytest.mark.asyncio
async def test_default_service_remains_fail_closed() -> None:
    with pytest.raises(NotImplementedYet, match="not enabled") as excinfo:
        await UnavailableForecastService().forecast(_request(), idempotency_key="fixture-key")
    assert "fixture-key" not in str(excinfo.value.details)
    assert excinfo.value.details["idempotency_requested"] is True
