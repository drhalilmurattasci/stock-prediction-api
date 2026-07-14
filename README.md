# Stock Market Price Forecast API

A REST API that ingests versioned daily market data and serves baseline price
forecasts with central **prediction intervals** and point-in-time provenance.

> ⚠️ **Not investment advice.** Markets are near-efficient; this project reports
> explicit uncertainty and never markets a forecast as an "accurate prediction"
> or as trading/investment advice. Current baseline intervals are deliberately
> labelled uncalibrated until held-out coverage validation is persisted.

## Status

🚧 **First honest forecast vertical slice is code-complete; migration `0010`
is proven on the live PostgreSQL 17 integration gate.**
The repository now has API-key auth, bounded `/v1/prices` reads, versioned Polygon
daily-bar ingestion, append-only restatement history, leakage-aware baselines, an
immutable point-in-time snapshot builder, snapshot-backed `/v1/forecast`, and an
insert-only content-hashed forecast-run archive with retry-safe POST idempotency.
Migration `0010` also establishes the policy-explicit forecast-evidence
substrate: realized raw-close outcomes bind one exact post-commit bar receipt,
while immutable cohort manifests materialize members validated against exact
scheduled forecast outputs and receive a distinct post-commit seal before their
first target so later evaluation cannot silently cherry-pick membership.

The initial builder policy is intentionally narrow: Massive/Polygon raw
regular-session closes from `/v1/open-close` (source `polygon_open_close`) for
`AAPL`, `MSFT`, `NVDA`, `QQQ`, and `SPY`; XNYS `trading_day` horizons; USD; 512
observation capacity with a 258-observation minimum; and 252 real
exchange-session target closes. Adjusted
prices remain refused until the separate corporate-action ledger exists. The
route also stays `501` until an operator explicitly pins the code-derived policy
and availability hashes.

Unit/static gates cover the evidence substrate. The destructive TimescaleDB
integration gate now passes through migration `0010` on real PostgreSQL 17.
`run-live-gate.ps1` is hard-bound to the designated `stockapi_test` throwaway
database and checks the complete migration chain, exact
runtime/builder role boundaries, restatement history, historical point-in-time
snapshot reconstruction, archived serving, schema-validated keyed replay, and
the outcome/cohort hash, exact-receipt, immutability, role-boundary, and
pre-outcome sealing constraints. Ordinary test runs still skip that gate when
its explicit live-database environment is absent. The first real
Massive/Polygon call remains a separate credentialed smoke gate.
That gate now has a one-attempt, fail-closed operator command documented in
`INSTALL.md`; no vendor request runs as part of ordinary verification. The next
history step is also scaffolded but has not run: a clean-commit-bound MSFT plan
derives the exact final 258 XNYS sessions, then a separately authorized,
no-auto-retry backfill checkpoints one bar and exact availability receipt per
request at a hard 5/60 pace. Its append-only local ledger makes late failures
resumable and ambiguous crashes fail closed. All controlled Polygon ingestion,
smoke, and backfill paths share one vendor-wide PostgreSQL operation lock.
The final no-vendor step is also scaffolded: a read-only plan binds the completed
backfill, clean commit, database clock, policy hashes, and API auth configuration;
its separately authorized execution uses one short-lived least-privilege builder
container, then proves the real loopback route returns `401` without a key and
`200` with the configured key. It never starts the ordinary worker or Beat.

## Quickstart

```bash
cp .env.example .env          # fill in vendor keys
uv sync --frozen --extra dev  # install core + dev deps from uv.lock
docker compose up -d          # infra: timescaledb, redis-cache, redis-celery, mlflow
uv run alembic upgrade head   # apply migrations
make api                      # uvicorn app.main:app --reload  ->  http://localhost:8000/docs
```

Before enabling snapshot creation/forecast serving, print and pin the exact
policy identities:

```bash
uv run python -m ingestion.tasks.build_forecast_snapshots --print-policy-hashes
```

The `app` Compose profile serves the API only; it never starts a worker, the
privileged snapshot builder, or Beat. Persistent actors live behind the separate
`automation` profile, the default-off `AUTOMATION_ENABLED` task gate, and (for
Polygon lanes) a positive finite process budget. See [INSTALL.md](INSTALL.md)
for role bootstrap and the live database gate. For the bounded milestone proof,
use `run-forecast-demo.ps1`; its one-shot builder does not enable Celery
automation. Compose publishes the API on loopback only. That proof builds from
the exact reviewed Git commit and pins both API and one-shot builder execution
to revision-labelled immutable image IDs.

The evidence schema and strict canonical validators are a foundation, not a
claim of calibration: no unattended outcome resolver, cohort publisher,
scoreboard, or interval recalibrator is enabled yet. Those actors must bind an
explicit resolution/selection policy and remain behind the same automation and
budget controls before they can publish evidence.

See [INSTALL.md](INSTALL.md) for the full Windows/WSL2 setup. Persistent workers
and Beat are intentionally not safe-by-default startup conveniences: inspect or
purge the durable queue, set the explicit automation gates and finite vendor
budget, then use `make up-automation` only under an approved runbook.

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
