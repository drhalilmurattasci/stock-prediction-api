"""Liveness (``/healthz``) and readiness (``/readyz``) probes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Request, Response, status

from app import __version__
from app.db.session import check_db
from app.dependencies import check_redis
from app.schemas.common import HealthResponse, ReadinessCheck, ReadinessResponse

router = APIRouter(tags=["health"])

SERVICE_NAME = "stock-prediction-api"
ReadinessProbe = Callable[[Request], Awaitable[None]]


async def _database_probe(request: Request) -> None:
    await check_db(request.app.state.engine)


async def _redis_probe(request: Request) -> None:
    await check_redis(request.app.state.redis_cache)


DEFAULT_READINESS_PROBES: tuple[tuple[str, ReadinessProbe], ...] = (
    ("database", _database_probe),
    ("redis", _redis_probe),
)


@router.get("/healthz", response_model=HealthResponse, summary="Liveness probe")
async def healthz() -> HealthResponse:
    return HealthResponse(service=SERVICE_NAME, version=__version__)


@router.get("/readyz", response_model=ReadinessResponse, summary="Readiness probe")
async def readyz(request: Request, response: Response) -> ReadinessResponse:
    checks: list[ReadinessCheck] = []
    probes = getattr(request.app.state, "readiness_probes", DEFAULT_READINESS_PROBES)
    for name, probe in probes:
        try:
            await probe(request)
            checks.append(ReadinessCheck(name=name, ok=True))
        except Exception as exc:  # noqa: BLE001 - report failure, never crash the probe
            checks.append(ReadinessCheck(name=name, ok=False, detail=str(exc)))

    all_ok = all(c.ok for c in checks)
    if not all_ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(status="ok" if all_ok else "degraded", checks=checks)
