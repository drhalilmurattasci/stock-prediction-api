"""Celery configuration guardrails."""

from __future__ import annotations

from ingestion.celery_app import celery_app


def test_redis_visibility_timeout_is_explicit():
    assert celery_app.conf.broker_transport_options["visibility_timeout"] == 6 * 60 * 60
    assert celery_app.conf.result_backend_transport_options["visibility_timeout"] == 6 * 60 * 60


def test_late_ack_is_per_ingestion_task_not_global():
    assert celery_app.conf.task_acks_late is False
    celery_app.loader.import_default_modules()
    for task_name in (
        "ingestion.ingest_prices",
        "ingestion.ingest_fundamentals",
        "ingestion.ingest_news",
    ):
        task = celery_app.tasks[task_name]
        assert task.acks_late is True
        assert task.reject_on_worker_lost is True
