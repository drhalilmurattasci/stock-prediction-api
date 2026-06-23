# Stock Market Analysis & Price Prediction API

A REST/WebSocket API that ingests market data, computes structured analysis (trend, volatility, momentum, regime, risk), and serves **probabilistic price forecasts with confidence intervals**.

> ⚠️ **Not investment advice.** Markets are near-efficient; this project ships *calibrated probabilistic forecasts*, never "accurate predictions" or trading/investment advice.

## Status

📐 **Planning & setup phase.** The architecture, full API/vendor feature catalog, engineering doctrine, committed tech stack, and phased roadmap are documented. App scaffolding is next.

## Documentation

| Doc | What's inside |
|---|---|
| [STOCK_API_MASTER_PLAN.md](STOCK_API_MASTER_PLAN.md) | Master plan — overview, doctrine, not-to-do list, tech stack, full feature catalog of 14 APIs/frameworks, phased roadmap |
| [INSTALL.md](INSTALL.md) | Start-to-finish installation guide (Windows) — WSL2, Docker, uv, Python 3.12, infra stack, smoke tests |

## Tech stack (committed)

- **Core:** Python 3.12 · FastAPI · Pydantic v2 · httpx
- **Data:** TimescaleDB / PostgreSQL · Redis
- **Orchestration:** Prefect
- **Modeling:** Chronos-2 · Nixtla · Darts / PyTorch Forecasting · XGBoost / LightGBM · statsmodels
- **ML lifecycle:** MLflow · Feast · BentoML
- **Backtesting:** vectorbt · backtrader (bias-free discipline)
- **Ops:** Docker · GitHub Actions · Prometheus + Grafana · Sentry

**Data sources:** Polygon/Massive (prices) · FMP (fundamentals) · Finnhub (news/sentiment) · Sharadar (point-in-time fundamentals); Databento US Equities Mini as redistribution-safe upgrade.

## License

TBD.
