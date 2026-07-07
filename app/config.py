"""Application settings via pydantic-settings (env-driven config)."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, loaded from environment / ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- app ---
    app_env: Literal["local", "staging", "production"] = "local"
    log_level: str = "INFO"
    project_name: str = "Stock Prediction API"
    api_v1_prefix: str = "/v1"

    # --- database (async driver) ---
    database_url: str = "postgresql+asyncpg://stockapi:change_me_strong@localhost:5432/stockapi"

    # --- redis (cache) ---
    redis_url: str = "redis://localhost:6379/0"

    # --- celery (broker + result backend) ---
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # --- rate limiting ---
    rate_limit_storage_uri: str = "memory://"
    rate_limit_default: str = "120/minute"

    # --- services ---
    mlflow_tracking_uri: str = "http://localhost:5000"

    # --- auth ---
    jwt_secret: str = "change_me_random_64_chars"
    api_keys: str = ""  # comma-separated; empty = allow anonymous (dev only)

    # --- observability ---
    sentry_dsn: str | None = None

    # --- vendor keys ---
    polygon_api_key: str | None = None
    fmp_api_key: str | None = None
    finnhub_api_key: str | None = None
    nasdaq_data_link_api_key: str | None = None
    alpaca_api_key: str | None = None
    alpaca_api_secret: str | None = None
    databento_api_key: str | None = None

    @property
    def api_key_set(self) -> set[str]:
        return {k.strip() for k in self.api_keys.split(",") if k.strip()}

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def sync_database_url(self) -> str:
        """Sync SQLAlchemy URL (psycopg) for non-async contexts."""
        return self.database_url.replace("+asyncpg", "+psycopg")


@lru_cache
def get_settings() -> Settings:
    return Settings()
