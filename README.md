# Stock Market Analysis & Price Prediction API

A REST/WebSocket API that ingests market data, computes structured analysis (trend, volatility, momentum, regime, risk), and serves **probabilistic price forecasts with confidence intervals**.

> ⚠️ **Not investment advice.** Markets are near-efficient; this project ships *calibrated probabilistic forecasts*, never "accurate predictions" or trading/investment advice.

## Status

🚧 **Phase 0 — Foundations.** The FastAPI application spine is runnable: `/healthz`, `/readyz`, `/metrics`, `/v1` router surface (endpoints stubbed with `501` until later phases), structured logging with request IDs, error envelope, API-key auth + rate-limit wiring, Celery app + Beat schedule, and Alembic wired to an async engine. Data ingestion and forecasting land in Phases 1–3.

## Quickstart

```bash
cp .env.example .env          # fill in vendor keys
uv sync --frozen --extra dev  # install core + dev deps from uv.lock
docker compose up -d          # infra: timescaledb, redis-cache, redis-celery, mlflow
uv run alembic upgrade head   # apply migrations
make api                      # uvicorn app.main:app --reload  ->  http://localhost:8000/docs
```

See [INSTALL.md](INSTALL.md) for the full Windows/WSL2 setup. Run the workers with `make worker` / `make beat`.

## Documentation

| Doc | What's inside |
|---|---|
| [STOCK_API_MASTER_PLAN.md](STOCK_API_MASTER_PLAN.md) | Master plan — overview, doctrine, not-to-do list, tech stack, full feature catalog of 14 APIs/frameworks, phased roadmap |
| [INSTALL.md](INSTALL.md) | Start-to-finish installation guide (Windows) — WSL2, Docker, uv, Python 3.12, infra stack, smoke tests |

## Tech stack (committed)

- **Core:** Python 3.12 · FastAPI · Pydantic v2 · httpx + tenacity
- **Data:** TimescaleDB / PostgreSQL · Redis cache + dedicated Redis Celery broker
- **Orchestration:** Celery + Beat (Redis broker)
- **Modeling:** Chronos-2 · StatsForecast · LightGBM · statsmodels / scikit-learn
- **ML lifecycle:** MLflow · Feast *(optional, later)* · BentoML *(scaling escape hatch)*
- **Backtesting/evaluation:** owned walk-forward harness (bias-free discipline)
- **Ops:** Docker · GitHub Actions · Prometheus + Grafana · Sentry

**Data sources:** Polygon/Massive (prices) · FMP (fundamentals) · Finnhub (news/sentiment) · Sharadar (point-in-time fundamentals); Databento US Equities Mini as redistribution-safe upgrade.

## License

Proprietary — all rights reserved. See [LICENSE](LICENSE).
