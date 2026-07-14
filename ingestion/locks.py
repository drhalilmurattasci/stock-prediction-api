"""Stable PostgreSQL advisory-lock identities shared by data pipelines."""

from __future__ import annotations

import hashlib
import struct
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.db.session import build_engine

POLYGON_VENDOR = "polygon"
_BAR_SERIES_FENCE_DOMAIN = "stockapi.bar-series-fence.v1"


class VendorOperationBusy(RuntimeError):
    """Another controlled process owns the vendor-wide outbound-call lane."""


def stable_lock_id(namespace: str, *parts: str) -> int:
    """Return one signed 64-bit lock id for a canonical logical resource."""

    if not namespace or any(not part for part in parts):
        raise ValueError("advisory-lock identity parts must not be empty")
    identity = ":".join((namespace, *parts))
    digest = hashlib.blake2b(identity.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


def bar_series_lock_id(symbol: str, source: str, timespan: str) -> int:
    """Return the DB-reproducible identity for one bar-series fence.

    Unlike the process-owned generic lock identity, this SHA-256 construction
    is intentionally simple to reproduce in PostgreSQL.  Receipt triggers,
    ingestion writers, snapshot builders, and outcome resolvers therefore all
    serialize on the same database-enforced lane even when a caller bypasses
    the Python write path.
    """

    parts = (source, timespan, symbol)
    if any(not isinstance(part, str) or not part for part in parts):
        raise ValueError("bar-series fence identity parts must be canonical")
    identity = bytearray(_BAR_SERIES_FENCE_DOMAIN.encode("utf-8"))
    for part in parts:
        encoded = part.encode("utf-8")
        identity.extend(struct.pack("!I", len(encoded)))
        identity.extend(encoded)
    digest = hashlib.sha256(identity).digest()[:8]
    return int.from_bytes(digest, byteorder="big", signed=True)


def vendor_operation_lock_id(vendor: str = POLYGON_VENDOR) -> int:
    """Machine/process-independent exclusion for every call to one vendor."""

    return stable_lock_id("vendor-operation", vendor)


@asynccontextmanager
async def exclusive_vendor_operation(
    settings: Settings,
    vendor: str = POLYGON_VENDOR,
) -> AsyncIterator[None]:
    """Try to hold a vendor-wide PostgreSQL session lock without waiting."""

    engine = build_engine(settings)
    try:
        async with engine.connect() as connection:
            lock_id = vendor_operation_lock_id(vendor)
            acquired = bool(
                (
                    await connection.execute(
                        text("SELECT pg_try_advisory_lock(:lock_id)"),
                        {"lock_id": lock_id},
                    )
                ).scalar_one()
            )
            await connection.commit()
            if not acquired:
                raise VendorOperationBusy(f"another {vendor} operation is already running")
            try:
                yield
            finally:
                await connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_id)"),
                    {"lock_id": lock_id},
                )
                await connection.commit()
    finally:
        await engine.dispose()


async def acquire_advisory_xact_lock(session: AsyncSession, lock_id: int) -> None:
    """Hold one bounded transaction-scoped PostgreSQL advisory lock."""

    # A dead/hung writer must not pin a dedicated worker forever. PostgreSQL
    # applies lock_timeout to advisory-lock acquisition and aborts the statement;
    # the task wrapper classifies that database error as retryable.
    await session.execute(text("SET LOCAL lock_timeout = '30s'"))
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_id)"),
        {"lock_id": lock_id},
    )
