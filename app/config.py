"""Application settings via pydantic-settings (env-driven config)."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import AliasChoices, Field
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
    app_env: Literal["local", "test", "staging", "production"] = "local"
    log_level: str = "INFO"
    project_name: str = "Stock Prediction API"
    api_v1_prefix: str = "/v1"

    # --- database (async driver) ---
    database_url: str = (
        "postgresql+asyncpg://stockapi_app:change_me_app_strong@localhost:5432/stockapi"
    )
    migration_database_url: str | None = None
    database_pool_size: int = Field(default=5, ge=1)
    database_max_overflow: int = Field(default=5, ge=0)
    database_pool_timeout: int = Field(default=30, ge=1)
    # Server-side per-statement budget for the API's request-serving engine
    # only (ingestion/migrations are never capped by it); 0 disables.
    api_statement_timeout_ms: int = Field(default=5_000, ge=0)

    # --- redis (cache) ---
    redis_cache_url: str = Field(
        default="redis://localhost:6379/0",
        validation_alias=AliasChoices("REDIS_CACHE_URL", "REDIS_URL"),
    )

    # --- celery (broker + result backend) ---
    celery_broker_url: str = "redis://localhost:6380/0"
    celery_result_backend: str = "redis://localhost:6380/1"

    # --- rate limiting ---
    rate_limit_storage_uri: str = "memory://"
    rate_limit_default: str = "120/minute"
    rate_limit_enabled: bool = True

    # --- services ---
    mlflow_tracking_uri: str = "http://localhost:5000"

    # --- auth ---
    jwt_secret: str = "change_me_random_64_chars"
    api_keys: str = ""  # comma-separated; empty = allow anonymous (dev only)

    # --- observability ---
    sentry_dsn: str | None = None

    # --- vendor keys ---
    polygon_api_key: str | None = None
    # Temporary single-process guard for the default Polygon ingestion path.
    # A positive total budget is cumulative for the worker process lifetime;
    # zero disables that non-renewing cap.
    polygon_max_calls_per_window: int = Field(default=5, ge=1)
    polygon_rate_window_seconds: float = Field(default=60.0, gt=0)
    polygon_total_call_budget: int = Field(default=0, ge=0)
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
        """Sync runtime URL (psycopg) for non-async contexts."""
        return self.database_url.replace("+asyncpg", "+psycopg")

    @property
    def effective_migration_database_url(self) -> str:
        """Owner URL used only by Alembic, falling back for external setups."""

        return self.migration_database_url or self.database_url

    @property
    def redis_url(self) -> str:
        """Backward-compatible alias for the cache Redis URL."""
        return self.redis_cache_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
