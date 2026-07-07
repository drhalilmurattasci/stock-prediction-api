"""Async SQLAlchemy engine and session factory (TimescaleDB/Postgres)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings

_settings = get_settings()

engine: AsyncEngine = create_async_engine(
    _settings.database_url,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding an async DB session."""
    async with SessionLocal() as session:
        yield session


async def check_db() -> None:
    """Raise if the database is unreachable (used by ``/readyz``)."""
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
