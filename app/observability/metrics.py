"""Prometheus metrics and request instrumentation middleware."""

from __future__ import annotations

import time

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests.",
    ["method", "path", "status"],
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds.",
    ["method", "path"],
)


class PrometheusMiddleware:
    """Pure-ASGI middleware recording request counts and latency."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        # NOTE: raw path is fine while routes have no path params; switch to the
        # matched route template before exposing high-cardinality endpoints (P2).
        path = scope.get("path", "")
        start = time.perf_counter()
        status_code = 500

        async def send_wrapper(message: dict) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            REQUEST_LATENCY.labels(method, path).observe(time.perf_counter() - start)
            REQUEST_COUNT.labels(method, path, str(status_code)).inc()


async def metrics_endpoint(_request: Request) -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
