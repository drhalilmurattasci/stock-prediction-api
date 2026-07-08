"""FastAPI application entrypoint: app factory, middleware, lifespan, routers."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, cast

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import Response
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app import __version__
from app.api.router import api_router
from app.api.v1 import health
from app.config import get_settings
from app.core.exceptions import install_exception_handlers
from app.core.logging import configure_logging
from app.core.rate_limit import limiter
from app.db.session import engine
from app.observability.metrics import PrometheusMiddleware, metrics_endpoint
from app.observability.sentry import init_sentry
from app.schemas.common import DISCLAIMER

log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level, json_logs=settings.is_production)
    init_sentry(settings)
    log.info("startup", env=settings.app_env, version=__version__)
    try:
        yield
    finally:
        await engine.dispose()
        log.info("shutdown")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.project_name,
        version=__version__,
        summary="Probabilistic stock-market analytics & forecasts with calibrated intervals.",
        description=f"{DISCLAIMER}\n\nSee STOCK_API_MASTER_PLAN.md for scope and doctrine.",
        lifespan=lifespan,
    )

    # --- rate limiting (slowapi) ---
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, cast(Any, _rate_limit_exceeded_handler))
    app.add_middleware(SlowAPIMiddleware)

    # --- metrics ---
    app.add_middleware(PrometheusMiddleware)

    # --- request correlation id (outermost) ---
    @app.middleware("http")
    async def request_id_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        request.state.request_id = request_id
        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            response = await call_next(request)
        finally:
            structlog.contextvars.clear_contextvars()
        response.headers["X-Request-ID"] = request_id
        return response

    install_exception_handlers(app)

    app.add_route("/metrics", metrics_endpoint, include_in_schema=False)
    app.include_router(health.router)
    app.include_router(api_router, prefix=settings.api_v1_prefix)

    return app


app = create_app()
