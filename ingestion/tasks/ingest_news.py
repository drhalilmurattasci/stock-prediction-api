"""Celery task: ingest news and sentiment aggregates.

Placeholder — real vendor pull lands in Phase 1. Only derived aggregates are ever
stored/served, never raw licensed article text.
Design reference: STOCK_API_MASTER_PLAN.md
"""

from __future__ import annotations

import structlog
from celery import shared_task

from app.config import get_settings
from ingestion.automation import require_automation_enabled

log = structlog.get_logger(__name__)


@shared_task(
    name="ingestion.ingest_news",
    bind=True,
    max_retries=3,
    acks_late=True,
    reject_on_worker_lost=True,
)
def ingest_news(self, symbols: list[str] | None = None) -> dict:
    require_automation_enabled(get_settings())
    log.info("ingest_news.start", symbols=symbols)
    # TODO(P1): pull news/sentiment, aggregate, upsert idempotently.
    return {"status": "not_implemented", "symbols": symbols or []}
