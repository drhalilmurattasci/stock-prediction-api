"""Celery task: ingest fundamentals (statements, ratios, metrics).

Placeholder — real vendor pull with point-in-time ``available_at`` lagging lands
in Phase 1.
Design reference: STOCK_API_MASTER_PLAN.md
"""

from __future__ import annotations

import structlog
from celery import shared_task

from app.config import get_settings
from ingestion.automation import require_automation_enabled

log = structlog.get_logger(__name__)


@shared_task(
    name="ingestion.ingest_fundamentals",
    bind=True,
    max_retries=3,
    acks_late=True,
    reject_on_worker_lost=True,
)
def ingest_fundamentals(self, symbols: list[str] | None = None) -> dict:
    require_automation_enabled(get_settings())
    log.info("ingest_fundamentals.start", symbols=symbols)
    # TODO(P1): pull fundamentals, lag by publication delay, upsert idempotently.
    return {"status": "not_implemented", "symbols": symbols or []}
