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


def build_engine(settings: Settings) -> AsyncEngine:
    """Build an async SQLAlchemy engine from runtime settings."""
    return create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout,
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
