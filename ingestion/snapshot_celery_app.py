"""Least-privilege Celery app for the snapshot-builder worker only."""

from __future__ import annotations

from celery import Celery

from app.config import get_settings

settings = get_settings()

snapshot_celery_app = Celery(
    "stockapi_snapshot_builder",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=("ingestion.tasks.build_forecast_snapshots",),
)
snapshot_celery_app.conf.update(
    broker_transport_options={"visibility_timeout": 6 * 60 * 60},
    result_backend_transport_options={"visibility_timeout": 6 * 60 * 60},
    task_routes={
        "forecasting.build_forecast_snapshots": {"queue": "snapshot-builder"},
    },
    task_acks_late=False,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,
    accept_content=["json"],
    task_serializer="json",
    result_serializer="json",
)

__all__ = ["snapshot_celery_app"]
