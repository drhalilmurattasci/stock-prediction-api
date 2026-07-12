"""Authentication: scoped API keys (P0) + JWT helpers.

P0 provides API-key gating only; hashed keys, scopes, quotas, and JWT sessions
are Phase 4/5 work per STOCK_API_MASTER_PLAN.md.
"""

from __future__ import annotations

from fastapi import Depends, Header, HTTPException, status

from app.config import Settings, get_settings

API_KEY_HEADER = "X-API-Key"


async def require_api_key(
    x_api_key: str | None = Header(default=None, alias=API_KEY_HEADER),
    settings: Settings = Depends(get_settings),
) -> str:
    """Validate the ``X-API-Key`` header against configured keys.

    When no keys are configured, anonymous access is allowed only in local/test
    environments. Staging and production fail closed instead of silently
    exposing product endpoints when deployment configuration is incomplete.
    """
    allowed = settings.api_key_set
    if not allowed:
        if settings.app_env in {"staging", "production"}:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API key authentication is not configured.",
                headers={"WWW-Authenticate": API_KEY_HEADER},
            )
        return x_api_key or "anonymous"
    if x_api_key is None or x_api_key not in allowed:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key.",
            headers={"WWW-Authenticate": API_KEY_HEADER},
        )
    return x_api_key
