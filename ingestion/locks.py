"""Stable PostgreSQL advisory-lock identities shared by data pipelines."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.db.session import build_engine

POLYGON_VENDOR = "polygon"


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
    """Identity used by both ingestion and point-in-time snapshot reads."""

    return stable_lock_id(source, timespan, symbol)


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
