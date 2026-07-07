"""Liveness (``/healthz``) and readiness (``/readyz``) probes."""

from __future__ import annotations

from fastapi import APIRouter, Response, status

from app import __version__
from app.db.session import check_db
from app.dependencies import check_redis
from app.schemas.common import HealthResponse, ReadinessCheck, ReadinessResponse

router = APIRouter(tags=["health"])

SERVICE_NAME = "stock-prediction-api"


@router.get("/healthz", response_model=HealthResponse, summary="Liveness probe")
async def healthz() -> HealthResponse:
    return HealthResponse(service=SERVICE_NAME, version=__version__)


@router.get("/readyz", response_model=ReadinessResponse, summary="Readiness probe")
async def readyz(response: Response) -> ReadinessResponse:
    checks: list[ReadinessCheck] = []
    for name, probe in (("database", check_db), ("redis", check_redis)):
        try:
            await probe()
            checks.append(ReadinessCheck(name=name, ok=True))
        except Exception as exc:  # noqa: BLE001 - report failure, never crash the probe
            checks.append(ReadinessCheck(name=name, ok=False, detail=str(exc)))

    all_ok = all(c.ok for c in checks)
    if not all_ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(status="ok" if all_ok else "degraded", checks=checks)
