"""Celery task: ingest OHLCV/price bars into TimescaleDB.

Placeholder — the real vendor pull, corporate-action adjustment, and idempotent
upsert land in Phase 1.
Design reference: STOCK_API_MASTER_PLAN.md
"""

from __future__ import annotations

import structlog
from celery import shared_task

log = structlog.get_logger(__name__)


@shared_task(name="ingestion.ingest_prices", bind=True, max_retries=3)
def ingest_prices(self, symbols: list[str] | None = None) -> dict:
    log.info("ingest_prices.start", symbols=symbols)
    # TODO(P1): pull bars via a data_sources adapter, adjust, upsert idempotently.
    return {"status": "not_implemented", "symbols": symbols or []}
