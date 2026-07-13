"""Celery application: broker/back-end config and Beat schedule.

Run locally::

    celery -A ingestion.celery_app.celery_app worker --loglevel=INFO
    celery -A ingestion.celery_app.celery_app beat   --loglevel=INFO

Rationale (see STOCK_API_MASTER_PLAN.md §4): one Redis-backed task system —
Celery for async jobs (ingestion, backtests, heavy inference) and Celery Beat
for scheduled pulls. Prefect is intentionally deferred.
"""

from __future__ import annotations

from celery import Celery
from celery.schedules import crontab

from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "stockapi",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        "ingestion.tasks.ingest_prices",
        "ingestion.tasks.ingest_forecast_closes",
        "ingestion.tasks.ingest_fundamentals",
        "ingestion.tasks.ingest_news",
    ],
)

celery_app.conf.update(
    broker_transport_options={"visibility_timeout": 6 * 60 * 60},
    result_backend_transport_options={"visibility_timeout": 6 * 60 * 60},
    task_acks_late=False,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,
    task_routes={
        "forecasting.build_forecast_snapshots": {"queue": "snapshot-builder"},
    },
)

# Placeholder cadences — real windows/universe finalized in Phase 1.
celery_app.conf.beat_schedule = {
    "ingest-forecast-closes-eod": {
        "task": "ingestion.ingest_forecast_closes",
        # At 16:00 UTC the task resolves the latest *completed* XNYS session;
        # during the US session that is deliberately the preceding close.
        "schedule": crontab(hour=16, minute=0),
    },
    "ingest-prices-eod": {
        "task": "ingestion.ingest_prices",
        "schedule": crontab(hour=22, minute=30),  # after US close (UTC)
    },
    "build-forecast-snapshots-eod": {
        "task": "forecasting.build_forecast_snapshots",
        "schedule": crontab(hour=17, minute=0),
    },
    "ingest-fundamentals-daily": {
        "task": "ingestion.ingest_fundamentals",
        "schedule": crontab(hour=6, minute=0),
    },
    "ingest-news-hourly": {
        "task": "ingestion.ingest_news",
        "schedule": crontab(minute=0),
    },
}
