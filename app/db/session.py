"""Async SQLAlchemy engine and session factory (TimescaleDB/Postgres)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import Settings


def statement_timeout_connect_args(statement_timeout_ms: int | None) -> dict[str, object]:
    """asyncpg connect args enforcing a per-statement server-side timeout.

    Returns ``{}`` when disabled (``None`` or non-positive). Postgres then
    cancels any single statement exceeding the budget, so a pathological scan
    (e.g. a filter the indexes cannot bound) cannot pin the database.
    """
    if statement_timeout_ms is None or statement_timeout_ms <= 0:
        return {}
    return {"server_settings": {"statement_timeout": str(statement_timeout_ms)}}


def build_engine(settings: Settings, *, statement_timeout_ms: int | None = None) -> AsyncEngine:
    """Build an async SQLAlchemy engine from runtime settings.

    ``statement_timeout_ms`` is opt-in per engine: the API's request-serving
    engine passes its budget, while ingestion/migration engines omit it so
    long-running batch transactions are never capped by the read budget.
    """
    return create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout,
        connect_args=statement_timeout_connect_args(statement_timeout_ms),
        future=True,
    )


def build_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build a session factory bound to the app-owned engine."""
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding an async DB session."""
    sessionmaker = request.app.state.sessionmaker
    async with sessionmaker() as session:
        yield session


async def check_db(engine: AsyncEngine) -> None:
    """Raise if the database is unreachable (used by ``/readyz``)."""
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
