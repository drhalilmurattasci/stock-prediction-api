"""Liveness (``/healthz``) and readiness (``/readyz``) probes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import structlog
from fastapi import APIRouter, Request, Response, status

from app import __version__
from app.db.session import check_db
from app.dependencies import check_redis
from app.schemas.common import HealthResponse, ReadinessCheck, ReadinessResponse

router = APIRouter(tags=["health"])
log = structlog.get_logger(__name__)

SERVICE_NAME = "stock-prediction-api"
#: ``/readyz`` is public and unauthenticated. A dependency's own exception text
#: discloses internal topology and credentials -- asyncpg, for instance, renders
#: 'connection to server at "timescaledb" (172.18.0.2), port 5432 failed: FATAL:
#: password authentication failed for user "stockapi_app"'. Probes therefore
#: report only WHICH check failed; logs retain a safe failure class for
#: correlation without copying driver messages that may contain credentials.
UNAVAILABLE_DETAIL = "dependency check failed"
ReadinessProbe = Callable[[Request], Awaitable[None]]


async def _database_probe(request: Request) -> None:
    await check_db(request.app.state.engine)


async def _redis_probe(request: Request) -> None:
    await check_redis(request.app.state.redis_cache)


async def _rate_limit_probe(request: Request) -> None:
    await request.app.state.rate_limiter.backend.check()


DEFAULT_READINESS_PROBES: tuple[tuple[str, ReadinessProbe], ...] = (
    ("database", _database_probe),
    ("redis", _redis_probe),
    ("rate_limit", _rate_limit_probe),
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
            # Never reflect the dependency's exception text to an unauthenticated
            # caller (see UNAVAILABLE_DETAIL), including through logs. The
            # request-id is already bound for correlation; the exception class
            # is sufficient to route diagnosis without copying a driver message
            # that may itself contain a DSN or credential.
            log.warning(
                "readiness_probe_failed",
                check=name,
                error_type=type(exc).__name__,
            )
            checks.append(ReadinessCheck(name=name, ok=False, detail=UNAVAILABLE_DETAIL))

    all_ok = all(c.ok for c in checks)
    if not all_ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(status="ok" if all_ok else "degraded", checks=checks)
