"""Structured logging setup (structlog over stdlib)."""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def _drop_exception_details(
    _logger: object,
    _method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Remove traceback payloads for commands that may hold live credentials."""

    event_dict.pop("exc_info", None)
    event_dict.pop("exception", None)
    return event_dict


def configure_logging(
    level: str = "INFO",
    *,
    json_logs: bool = False,
    exception_details: bool = True,
) -> None:
    """Configure structlog with ISO timestamps, log levels, and contextvars.

    ``json_logs=True`` emits machine-readable JSON (production); otherwise a
    human-friendly console renderer is used. Credential-bearing one-shot tools
    can set ``exception_details=False`` to retain structured error fields while
    dropping exception text and tracebacks.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    exception_processor = (
        structlog.processors.format_exc_info if exception_details else _drop_exception_details
    )
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        exception_processor,
    ]
    renderer = structlog.processors.JSONRenderer() if json_logs else structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=log_level)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
