.PHONY: help install install-ml lock up up-app down logs api worker snapshot-builder beat lint fmt type test migrate revision

help:
	@echo "Targets:"
	@echo "  install     uv sync (core + dev)"
	@echo "  install-ml  uv sync (core + dev + ml)"
	@echo "  lock        uv lock (generate uv.lock)"
	@echo "  up          docker compose up -d (infra: timescaledb, redis, mlflow)"
	@echo "  up-app      docker compose --profile app up -d --build (full stack; needs uv.lock)"
	@echo "  down        docker compose down"
	@echo "  api         run the API with reload"
	@echo "  worker      run the ordinary Celery worker (one process)"
	@echo "  snapshot-builder  run the least-privilege snapshot worker (one process)"
	@echo "  beat        run the Celery Beat scheduler"
	@echo "  lint/fmt/type/test   ruff / ruff format / mypy / pytest"
	@echo "  migrate     alembic upgrade head"
	@echo "  revision    alembic revision --autogenerate m=\"message\""

install:
	uv sync --extra dev

install-ml:
	uv sync --extra dev --extra ml

lock:
	uv lock

up:
	docker compose up -d

up-app:
	docker compose --profile app up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

api:
	uvicorn app.main:app --reload

worker:
	celery -A ingestion.celery_app.celery_app worker --loglevel=INFO --concurrency=1

snapshot-builder:
	celery -A ingestion.snapshot_celery_app.snapshot_celery_app worker --loglevel=INFO --concurrency=1 --queues=snapshot-builder

beat:
	celery -A ingestion.celery_app.celery_app beat --loglevel=INFO

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

type:
	uv run mypy

test:
	uv run pytest

migrate:
	uv run alembic upgrade head

revision:
	uv run alembic revision --autogenerate -m "$(m)"
