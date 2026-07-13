# syntax=docker/dockerfile:1
# API / ordinary worker / snapshot-builder worker / beat image. Requires a
# committed uv.lock (run `uv lock` first); the build installs from the lockfile
# for reproducibility.

FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"
WORKDIR /app

# uv (fast, lockfile-driven installs)
COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /usr/local/bin/uv

# --- dependency layer (cached until pyproject/uv.lock change) ---
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# --- application code ---
COPY app ./app
COPY data_sources ./data_sources
COPY ingestion ./ingestion
COPY ml ./ml
COPY alembic.ini ./
COPY migrations ./migrations

EXPOSE 8000
# Default command runs the API; worker/snapshot-builder/beat override it in Compose.
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "2"]
