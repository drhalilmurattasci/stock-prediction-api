"""Application exceptions and FastAPI handlers producing a stable error envelope."""

from __future__ import annotations

from collections.abc import Mapping

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.schemas.common import ErrorBody, ErrorResponse


class AppError(Exception):
    """Base class for expected, structured application errors."""

    status_code: int = status.HTTP_400_BAD_REQUEST
    code: str = "app_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        status_code: int | None = None,
        details: object | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = details
        if code is not None:
            self.code = code
        if status_code is not None:
            self.status_code = status_code


class NotFoundError(AppError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "not_found"


class NotImplementedYet(AppError):
    status_code = status.HTTP_501_NOT_IMPLEMENTED
    code = "not_implemented"


def _envelope(
    request: Request,
    *,
    code: str,
    message: str,
    status_code: int,
    details: object | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    body = ErrorResponse(
        error=ErrorBody(code=code, message=message, request_id=request_id, details=details)
    )
    return JSONResponse(
        status_code=status_code,
        content=body.model_dump(mode="json"),
        headers=headers,
    )


def install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(request: Request, exc: AppError) -> JSONResponse:
        return _envelope(
            request,
            code=exc.code,
            message=exc.message,
            status_code=exc.status_code,
            details=exc.details,
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return _envelope(
            request,
            code="http_error",
            message=str(exc.detail),
            status_code=exc.status_code,
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return _envelope(
            request,
            code="validation_error",
            message="Request validation failed.",
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            details=jsonable_encoder(exc.errors()),
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        return _envelope(
            request,
            code="internal_error",
            message="Internal server error.",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
