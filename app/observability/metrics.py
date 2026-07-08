"""Prometheus metrics and request instrumentation middleware."""

from __future__ import annotations

import time

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

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

UNMATCHED_ROUTE = "<unmatched>"


class PrometheusMiddleware:
    """Pure-ASGI middleware recording request counts and latency."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        start = time.perf_counter()
        status_code = 500

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            path = _route_template(scope)
            REQUEST_LATENCY.labels(method, path).observe(time.perf_counter() - start)
            REQUEST_COUNT.labels(method, path, str(status_code)).inc()


async def metrics_endpoint(_request: Request) -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _route_template(scope: Scope) -> str:
    route = scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str):
        return _with_mount_prefix(scope, path)
    return UNMATCHED_ROUTE


def _with_mount_prefix(scope: Scope, route_path: str) -> str:
    raw_path = scope.get("path", "")
    if not isinstance(raw_path, str) or raw_path == route_path:
        return route_path

    route_parts = route_path.strip("/").split("/")
    raw_parts = raw_path.strip("/").split("/")
    if not route_parts or len(raw_parts) < len(route_parts):
        return route_path

    prefix_parts = raw_parts[: len(raw_parts) - len(route_parts)]
    return "/" + "/".join([*prefix_parts, *route_parts])
