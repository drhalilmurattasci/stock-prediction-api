"""Celery configuration guardrails."""

from __future__ import annotations

from ingestion.celery_app import celery_app
from ingestion.snapshot_celery_app import snapshot_celery_app


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
    schedule = celery_app.conf.beat_schedule["build-forecast-snapshots-eod"]
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
    schedule = celery_app.conf.beat_schedule["ingest-forecast-closes-eod"]
    assert schedule["task"] == "ingestion.ingest_forecast_closes"
    assert schedule["schedule"].hour == {16}
    assert schedule["schedule"].minute == {0}
