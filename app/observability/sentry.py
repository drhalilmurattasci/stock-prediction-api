"""Sentry initialization (no-op unless ``SENTRY_DSN`` is set)."""

from __future__ import annotations

from app.config import Settings


def init_sentry(settings: Settings) -> bool:
    """Initialize Sentry if a DSN is configured. Returns True when enabled."""
    if not settings.sentry_dsn:
        return False

    import sentry_sdk

    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.app_env,
        traces_sample_rate=0.0 if settings.app_env == "local" else 0.1,
    )
    return True
