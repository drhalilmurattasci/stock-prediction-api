"""Celery configuration guardrails."""

from __future__ import annotations

from app.config import Settings
from ingestion.celery_app import build_beat_schedule, celery_app
from ingestion.snapshot_celery_app import snapshot_celery_app


def _automation_settings(*, polygon_budget: int = 0) -> Settings:
    return Settings(
        _env_file=None,
        app_env="test",
        automation_enabled=True,
        polygon_total_call_budget=polygon_budget,
    )


def test_redis_visibility_timeout_is_explicit():
    assert celery_app.conf.broker_transport_options["visibility_timeout"] == 6 * 60 * 60
    assert celery_app.conf.result_backend_transport_options["visibility_timeout"] == 6 * 60 * 60


def test_late_ack_is_per_ingestion_task_not_global():
    assert celery_app.conf.task_acks_late is False
    celery_app.loader.import_default_modules()
    for task_name in (
        "ingestion.ingest_prices",
        "ingestion.ingest_forecast_closes",
        "ingestion.ingest_fundamentals",
        "ingestion.ingest_news",
    ):
        task = celery_app.tasks[task_name]
        assert task.acks_late is True
        assert task.reject_on_worker_lost is True
    snapshot_celery_app.loader.import_default_modules()
    snapshot_task = snapshot_celery_app.tasks["forecasting.build_forecast_snapshots"]
    assert snapshot_task.acks_late is True
    assert snapshot_task.reject_on_worker_lost is True


def test_snapshot_builder_is_routed_to_its_dedicated_queue() -> None:
    route = celery_app.conf.task_routes["forecasting.build_forecast_snapshots"]
    assert route == {"queue": "snapshot-builder"}
    dedicated_route = snapshot_celery_app.amqp.router.route(
        {},
        "forecasting.build_forecast_snapshots",
    )
    assert dedicated_route["queue"].name == "snapshot-builder"
    schedule = build_beat_schedule(_automation_settings())["build-forecast-snapshots-eod"]
    assert schedule["task"] == "forecasting.build_forecast_snapshots"
    assert schedule["schedule"].hour == {17}
    assert schedule["schedule"].minute == {0}
    task = snapshot_celery_app.tasks["forecasting.build_forecast_snapshots"]
    assert task.soft_time_limit == 300
    assert task.time_limit == 330


def test_privileged_worker_app_imports_only_the_builder_task_module() -> None:
    assert snapshot_celery_app.conf.include == ("ingestion.tasks.build_forecast_snapshots",)
    assert snapshot_celery_app.conf.accept_content == ["json"]


def test_regular_session_closes_run_before_the_snapshot_build() -> None:
    schedule = build_beat_schedule(_automation_settings(polygon_budget=25))[
        "ingest-forecast-closes-eod"
    ]
    assert schedule["task"] == "ingestion.ingest_forecast_closes"
    assert schedule["schedule"].hour == {16}
    assert schedule["schedule"].minute == {0}


def test_beat_schedule_is_empty_by_default() -> None:
    settings = Settings(_env_file=None, app_env="test")
    assert settings.automation_enabled is False
    assert build_beat_schedule(settings) == {}


def test_vendor_schedules_require_a_positive_finite_budget() -> None:
    snapshot_only = build_beat_schedule(_automation_settings())
    assert set(snapshot_only) == {"build-forecast-snapshots-eod"}

    budgeted = build_beat_schedule(_automation_settings(polygon_budget=25))
    assert set(budgeted) == {
        "build-forecast-snapshots-eod",
        "ingest-forecast-closes-eod",
        "ingest-prices-eod",
    }
    assert "ingest-fundamentals-daily" not in budgeted
    assert "ingest-news-hourly" not in budgeted
