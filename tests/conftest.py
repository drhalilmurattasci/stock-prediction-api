"""Pytest fixtures: FastAPI test client.

Uses Starlette's TestClient, which runs the app lifespan. Liveness/metrics/docs
tests need no external services; readiness (DB/Redis) is covered by integration
tests once infra is available.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture
def client() -> TestClient:
    app = create_app(Settings(app_env="test", rate_limit_enabled=False))
    with TestClient(app) as test_client:
        yield test_client
