"""Mechanical fences around every unattended Celery execution boundary."""

from __future__ import annotations

from types import ModuleType
from typing import Any

import pytest

from app.config import Settings
from ingestion.automation import AutomationRefused, require_automation_enabled
from ingestion.tasks import build_forecast_snapshots as snapshots
from ingestion.tasks import ingest_forecast_closes as closes
from ingestion.tasks import ingest_fundamentals as fundamentals
from ingestion.tasks import ingest_news as news
from ingestion.tasks import ingest_prices as prices


def _settings(*, enabled: bool = False, budget: int = 0) -> Settings:
    return Settings(
        _env_file=None,
        app_env="test",
        automation_enabled=enabled,
        polygon_total_call_budget=budget,
    )


def test_automation_guard_is_default_off_and_vendor_budgeted() -> None:
    with pytest.raises(AutomationRefused, match="disabled"):
        require_automation_enabled(_settings())
    with pytest.raises(AutomationRefused, match="positive total call budget"):
        require_automation_enabled(
            _settings(enabled=True),
            require_polygon_budget=True,
        )
    require_automation_enabled(_settings(enabled=True))
    require_automation_enabled(
        _settings(enabled=True, budget=1),
        require_polygon_budget=True,
    )


@pytest.mark.parametrize(
    ("module", "task"),
    [
        (prices, prices.ingest_prices),
        (closes, closes.ingest_forecast_closes),
        (snapshots, snapshots.build_forecast_snapshots),
        (fundamentals, fundamentals.ingest_fundamentals),
        (news, news.ingest_news),
    ],
)
def test_every_celery_entrypoint_refuses_while_automation_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    task: Any,
) -> None:
    monkeypatch.setattr(module, "get_settings", lambda: _settings())
    with pytest.raises(AutomationRefused, match="disabled"):
        task.run()


@pytest.mark.parametrize(
    ("module", "task"),
    [
        (prices, prices.ingest_prices),
        (closes, closes.ingest_forecast_closes),
    ],
)
def test_vendor_tasks_refuse_zero_budget_before_async_work(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    task: Any,
) -> None:
    called = False

    def forbidden_run(value: object) -> object:
        nonlocal called
        called = True
        raise AssertionError(f"async work escaped the budget gate: {value!r}")

    monkeypatch.setattr(module, "get_settings", lambda: _settings(enabled=True))
    monkeypatch.setattr(module.asyncio, "run", forbidden_run)
    with pytest.raises(AutomationRefused, match="positive total call budget"):
        task.run()
    assert called is False


@pytest.mark.parametrize(
    ("module", "task", "async_name", "result"),
    [
        (
            prices,
            prices.ingest_prices,
            "ingest_prices_async",
            {"status": "ok", "retryable_failures": 0},
        ),
        (
            closes,
            closes.ingest_forecast_closes,
            "ingest_forecast_closes_async",
            {"status": "ok", "retryable_failures": 0, "failures": 0},
        ),
    ],
)
def test_budgeted_vendor_tasks_pass_the_checked_settings_to_async_work(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    task: Any,
    async_name: str,
    result: dict[str, object],
) -> None:
    settings = _settings(enabled=True, budget=3)
    captured: dict[str, object] = {}

    async def fake_async(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return result

    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, async_name, fake_async)
    assert task.run() == result
    assert captured["settings"] is settings


def test_snapshot_refusal_is_not_converted_into_a_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retry_called = False

    def retry(*args: object, **kwargs: object) -> None:
        nonlocal retry_called
        del args, kwargs
        retry_called = True

    monkeypatch.setattr(snapshots, "get_settings", lambda: _settings())
    monkeypatch.setattr(snapshots.build_forecast_snapshots, "retry", retry)
    with pytest.raises(AutomationRefused):
        snapshots.build_forecast_snapshots.run()
    assert retry_called is False
