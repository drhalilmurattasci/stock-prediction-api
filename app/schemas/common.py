"""Shared schemas: error envelope, health, readiness, disclaimer."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

DISCLAIMER = (
    "Not investment advice. Outputs are probabilistic, informational analytics "
    "only — not a recommendation to buy or sell any security. Past performance "
    "does not guarantee future results."
)


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: Any = None


class ErrorResponse(BaseModel):
    error: ErrorBody


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str
    version: str


class ReadinessCheck(BaseModel):
    name: str
    ok: bool
    detail: str | None = None


class ReadinessResponse(BaseModel):
    status: str
    checks: list[ReadinessCheck]
