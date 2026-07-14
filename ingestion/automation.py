"""Fail-closed guardrails for unattended Celery execution."""

from __future__ import annotations

from app.config import Settings


class AutomationRefused(RuntimeError):
    """Raised before an unattended task can perform any work."""


def require_automation_enabled(
    settings: Settings,
    *,
    require_polygon_budget: bool = False,
) -> None:
    """Refuse unattended work unless its independent gates are explicit."""

    if not settings.automation_enabled:
        raise AutomationRefused("unattended automation is disabled")
    if require_polygon_budget and settings.polygon_total_call_budget <= 0:
        raise AutomationRefused(
            "unattended Polygon automation requires a positive total call budget"
        )


__all__ = ["AutomationRefused", "require_automation_enabled"]
