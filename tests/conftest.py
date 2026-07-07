"""Pytest fixtures: FastAPI test client.

Uses Starlette's TestClient, which runs the app lifespan. Liveness/metrics/docs
tests need no external services; readiness (DB/Redis) is covered by integration
tests once infra is available.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(scope="session")
def client() -> TestClient:
    with TestClient(app) as test_client:
        yield test_client
