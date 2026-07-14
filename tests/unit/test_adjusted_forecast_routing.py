"""Target-policy isolation for raw and adjusted forecast serving."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from app.config import Settings
from app.core.exceptions import AppError, NotImplementedYet
from app.schemas.forecast import ForecastRequest, ForecastResponse
from app.services.forecast_serving import (
    ForecastServingPolicy,
    SnapshotForecastService,
    TargetRoutedForecastService,
    build_forecast_service,
)


def _hash(character: str) -> str:
    return "sha256:" + character * 64


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "test",
        "forecast_resolution_policy_hash": None,
        "forecast_trusted_availability_rule_set_hash": None,
        "forecast_adjusted_close_resolution_policy_hash": None,
        "forecast_adjusted_close_trusted_availability_rule_set_hash": None,
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def test_adjusted_only_service_uses_its_own_policy_epoch() -> None:
    service = build_forecast_service(
        _settings(
            forecast_adjusted_close_resolution_policy_hash=_hash("a"),
            forecast_adjusted_close_trusted_availability_rule_set_hash=_hash("b"),
        ),
        cast(Any, object()),
    )

    assert isinstance(service, SnapshotForecastService)
    assert service.policy == ForecastServingPolicy(
        resolution_policy_hash=_hash("a"),
        trusted_availability_rule_set_hash=_hash("b"),
        target="adjusted_close",
        series_basis="split_dividend_adjusted",
    )


def test_raw_and_adjusted_pins_build_two_isolated_children() -> None:
    service = build_forecast_service(
        _settings(
            forecast_resolution_policy_hash=_hash("1"),
            forecast_trusted_availability_rule_set_hash=_hash("2"),
            forecast_adjusted_close_resolution_policy_hash=_hash("3"),
            forecast_adjusted_close_trusted_availability_rule_set_hash=_hash("4"),
        ),
        cast(Any, object()),
    )

    assert isinstance(service, TargetRoutedForecastService)
    policies = {child.policy.target: child.policy for child in service.services}
    assert policies["close"].resolution_policy_hash == _hash("1")
    assert policies["close"].series_basis == "raw"
    assert policies["adjusted_close"].resolution_policy_hash == _hash("3")
    assert policies["adjusted_close"].series_basis == "split_dividend_adjusted"
    assert policies["close"].trusted_availability_rule_set_hash != (
        policies["adjusted_close"].trusted_availability_rule_set_hash
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"forecast_adjusted_close_resolution_policy_hash": _hash("a")},
        {"forecast_adjusted_close_trusted_availability_rule_set_hash": _hash("b")},
        {
            "forecast_adjusted_close_resolution_policy_hash": "sha256:not-hex",
            "forecast_adjusted_close_trusted_availability_rule_set_hash": _hash("b"),
        },
    ],
)
def test_adjusted_policy_pair_is_all_or_nothing(overrides: dict[str, object]) -> None:
    with pytest.raises(AppError, match="adjusted_close") as failure:
        build_forecast_service(_settings(**overrides), cast(Any, object()))

    assert failure.value.code == "forecast_serving_misconfigured"
    assert failure.value.status_code == 503


@dataclass
class _FakeChild:
    policy: ForecastServingPolicy
    calls: list[ForecastRequest] = field(default_factory=list)

    async def forecast(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None = None,
        principal: str | None = None,
    ) -> ForecastResponse:
        del idempotency_key, principal
        self.calls.append(request)
        return cast(ForecastResponse, request.target)


@pytest.mark.asyncio
async def test_router_dispatches_by_target_and_never_crosses_policy() -> None:
    raw = _FakeChild(ForecastServingPolicy(_hash("1"), _hash("2")))
    adjusted = _FakeChild(
        ForecastServingPolicy(
            _hash("3"),
            _hash("4"),
            target="adjusted_close",
            series_basis="split_dividend_adjusted",
        )
    )
    service = TargetRoutedForecastService(
        cast(tuple[SnapshotForecastService, ...], (raw, adjusted))
    )
    request = ForecastRequest(symbol="MSFT", horizon=1, target="adjusted_close")

    assert await service.forecast(request) == "adjusted_close"
    assert raw.calls == []
    assert adjusted.calls == [request]


@pytest.mark.asyncio
async def test_router_refuses_unconfigured_target() -> None:
    raw = _FakeChild(ForecastServingPolicy(_hash("1"), _hash("2")))
    service = TargetRoutedForecastService(cast(tuple[SnapshotForecastService, ...], (raw,)))
    request = ForecastRequest(symbol="MSFT", horizon=1, target="return")

    with pytest.raises(NotImplementedYet, match="not configured"):
        await service.forecast(request)
