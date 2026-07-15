# INSTALL — Stock Market Price Forecast API

> Start-to-finish installation guide for the stack defined in [STOCK_API_MASTER_PLAN.md](STOCK_API_MASTER_PLAN.md).
> **Target machine:** Windows 11 Pro (build 26200), PowerShell. **Detected 2026-06-23.**
> Infra (TimescaleDB, Redis, MLflow) runs in **Docker containers**; dev tooling
> (Git, Python, uv), the ordinary Celery worker, the least-privilege snapshot
> worker, and Beat can run **natively**.

---

## 0. What this installs

| Layer | Component | Method | Runs as |
|---|---|---|---|
| Package mgr | winget | (present) | native |
| VCS | Git | (present) | native |
| Runtime | Python **3.12** | uv-managed | native venv |
| Env/pkg mgr | **uv** | winget | native |
| Containers | **Docker Desktop** + WSL2 | winget + GUI | native host |
| Database | **TimescaleDB** (PostgreSQL 17) | Docker image | container |
| Cache/limits + broker | **Redis 7** (split instances) | Docker image | container |
| Orchestration | **Celery workers + Beat** (Redis broker) | uv pip | native processes |
| ML tracking | **MLflow** | Docker image | container |
| API + ML libs | FastAPI, Pydantic, SQLAlchemy, pandas, torch, LightGBM, StatsForecast, Chronos | uv pip | native venv |

**Estimated time:** 30–60 min (most of it Docker image pulls + Python wheel downloads). One reboot required (WSL2).

---

## 1. Machine baseline (detected on this host)

| Tool | Status | Version / path |
|---|---|---|
| winget | ✅ present | v1.28.240 |
| Git | ✅ present | 2.54.0 |
| Python (system) | ✅ present | 3.13.13 — *project will pin 3.12 instead* |
| pip | ✅ present | 26.0.1 |
| Node / npm | ✅ present | 24.16.0 / 11.13.0 |
| WSL | ✅ present | platform installed (backend confirmed in Step 2) |
| uv | ❌ missing | install in Step 3 |
| Docker | ❌ missing | install in Step 2 |
| Hardware | ✅ | 63 GB RAM, 16 logical CPUs — ample for local ML |

> ⚠️ **Admin note:** the shell used for the survey was **non-elevated**. Steps **2 (WSL2)** and the **Docker Desktop install** require an **Administrator PowerShell** (UAC). Open one via: Start → type `powershell` → right-click → **Run as administrator**.

---

## 2. Step 1 — Enable WSL2 + install Docker Desktop

Docker Desktop on Windows uses the **WSL2** backend. Enable it first.

### 2.1 Enable WSL2 (Administrator PowerShell, one-time)

```powershell
# Enables the WSL + Virtual Machine Platform features and sets WSL2 as default.
wsl --install --no-distribution
wsl --set-default-version 2
wsl --status
```

> If `wsl --install` reports features were enabled, **reboot now** before continuing.
> Docker Desktop ships its own WSL distro, so `--no-distribution` is enough (no Ubuntu needed). Want a full Linux env too? Use `wsl --install -d Ubuntu`.

**Verify (after reboot):**
```powershell
wsl --status          # should show "Default Version: 2"
wsl --version         # WSL kernel version
```

### 2.2 Install Docker Desktop (Administrator PowerShell)

```powershell
winget install -e --id Docker.DockerDesktop --accept-source-agreements --accept-package-agreements
```

### 2.3 First-run configuration (GUI, one-time, manual)

1. Launch **Docker Desktop** (Start menu). Accept the service agreement.
2. You can **skip sign-in** (not required for local use).
3. **Settings → General →** ensure *“Use the WSL 2 based engine”* is checked.
4. **Settings → Resources → WSL Integration →** leave defaults (Docker manages its own distro).
5. Wait until the whale icon shows **“Engine running.”**

**Verify:**
```powershell
docker version
docker run --rm hello-world      # prints "Hello from Docker!"
docker compose version
```

> 🔧 If `docker` is “not recognized,” open a **new** terminal (PATH was updated by the installer).

---

## 3. Step 2 — Install uv (Python env & package manager)

```powershell
winget install -e --id astral-sh.uv --accept-source-agreements --accept-package-agreements
```

**Verify** (open a new terminal so PATH refreshes):
```powershell
uv --version
```

> Fallback if winget can’t find it: `powershell -ExecutionPolicy Bypass -c "irm https://astral.sh/uv/install.ps1 | iex"`

---

## 4. Step 3 — Install project Python 3.12

The system has Python 3.13, but the project pins **Python 3.12** for repeatable dependency resolution and ML wheel compatibility. Let `uv` manage an isolated 3.12 — it won’t touch your system Python.

```powershell
uv python install 3.12
uv python list            # confirms 3.12 is available to uv
```

---

## 5. Step 4 — Create the project files

All commands below run from the project root **`A:\tansel`**.

### 5.1 `pyproject.toml`

Create **`A:\tansel\pyproject.toml`** with dependency groups (core always; `ml` and `dev` optional):

```toml
[project]
name = "stock-prediction-api"
version = "0.1.0"
description = "Versioned market-data ingestion and baseline price-forecast REST API"
requires-python = ">=3.12,<3.13"
dependencies = [
  # --- API & web ---
  "fastapi>=0.115",
  "uvicorn[standard]>=0.34",
  "pydantic>=2.9",
  "pydantic-settings>=2.6",
  "httpx>=0.28",
  "tenacity>=9.0",
  "slowapi>=0.1.9",
  "PyJWT[crypto]>=2.10",
  "bcrypt>=4.2",
  "python-dotenv>=1.0",
  # --- data layer ---
  "sqlalchemy>=2.0",
  "psycopg[binary]>=3.2",
  "asyncpg>=0.30",
  "alembic>=1.14",
  "redis>=5.2",
  # --- orchestration (task queue + scheduler) ---
  "celery[redis]>=5.4",
  # --- analytics core ---
  "pandas>=2.2",
  "numpy>=1.26",
  "exchange-calendars==4.13.2",
  "scikit-learn>=1.5",
  "statsmodels>=0.14",
  # --- tracking client ---
  "mlflow-skinny>=2.19",
  # --- observability ---
  "prometheus-client>=0.21",
  "sentry-sdk>=2.19",
  "structlog>=24.4",
  # --- vendor SDKs ---
  "polygon-api-client>=1.14",
  "finnhub-python>=2.4",
]

[project.optional-dependencies]
ml = [
  "torch>=2.5",                 # CPU build by default; see GPU note in Appendix
  "lightgbm>=4.5",
  "statsforecast>=2.0",
  "chronos-forecasting>=2.0",
]
dev = [
  "pytest>=8.3",
  "pytest-asyncio>=0.24",
  "ruff>=0.8",
  "mypy>=1.13",
  "ipython>=8.30",
]

[tool.uv]
package = false

[tool.ruff]
target-version = "py312"
line-length = 100
src = ["app", "data_sources", "ingestion", "ml", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC", "C4", "SIM"]

[tool.ruff.lint.flake8-bugbear]
# FastAPI uses callables in argument defaults by design (Depends/Header/...).
extend-immutable-calls = [
  "fastapi.Depends",
  "fastapi.Header",
  "fastapi.Query",
  "fastapi.Path",
  "fastapi.Body",
  "fastapi.Security",
  "fastapi.Cookie",
  "fastapi.Form",
  "fastapi.File",
]

[tool.ruff.lint.isort]
known-first-party = ["app", "data_sources", "ingestion", "ml", "tests"]

[tool.mypy]
python_version = "3.12"
plugins = ["pydantic.mypy"]
ignore_missing_imports = true
warn_redundant_casts = true
warn_unused_ignores = true
check_untyped_defs = true
files = ["app", "data_sources", "ingestion", "ml"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
addopts = "-q"
```

### 5.2 `.env.example`

Create **`A:\tansel\.env.example`** (copy to `.env` and fill in real keys — never commit `.env`):

```dotenv
# ---- Database (TimescaleDB) ----
POSTGRES_USER=stockapi_owner
POSTGRES_PASSWORD=change_me_owner_strong
POSTGRES_DB=stockapi
POSTGRES_APP_PASSWORD=change_me_app_strong
POSTGRES_SNAPSHOT_BUILDER_PASSWORD=change_me_snapshot_builder_strong
# Percent-encoded form for compose DATABASE_URL interpolation.
POSTGRES_APP_URL_PASSWORD=change_me_app_strong
POSTGRES_SNAPSHOT_BUILDER_URL_PASSWORD=change_me_snapshot_builder_strong
# Runtime is non-owner; only Alembic uses the owner credential.
# Percent-encode reserved characters in each password embedded in these URLs.
DATABASE_URL=postgresql+asyncpg://stockapi_app:change_me_app_strong@localhost:5432/stockapi
MIGRATION_DATABASE_URL=postgresql+asyncpg://stockapi_owner:change_me_owner_strong@localhost:5432/stockapi
DATABASE_POOL_SIZE=5
DATABASE_MAX_OVERFLOW=5
DATABASE_POOL_TIMEOUT=30
API_STATEMENT_TIMEOUT_MS=5000

# Each target epoch requires its own complete pair. Blank keeps that target's
# snapshot creation/forecast serving fail-closed. Print exact values below.
FORECAST_RESOLUTION_POLICY_HASH=
FORECAST_TRUSTED_AVAILABILITY_RULE_SET_HASH=
FORECAST_ADJUSTED_CLOSE_RESOLUTION_POLICY_HASH=
FORECAST_ADJUSTED_CLOSE_TRUSTED_AVAILABILITY_RULE_SET_HASH=
FORECAST_SEASONAL_PERIOD=5

# ---- Redis (cache + rate-limit counters) ----
REDIS_CACHE_URL=redis://localhost:6379/0

# ---- Celery (task queue + Beat scheduler) ----
CELERY_BROKER_URL=redis://localhost:6380/0
CELERY_RESULT_BACKEND=redis://localhost:6380/1

# ---- Rate limiting ----
# memory:// is fine for a single worker in dev; use redis://localhost:6379/1 so
# limits are shared across workers in production.
RATE_LIMIT_STORAGE_URI=memory://
RATE_LIMIT_DEFAULT=120/minute
RATE_LIMIT_ENABLED=true
RATE_LIMIT_STORAGE_TIMEOUT_SECONDS=1

# ---- Services ----
MLFLOW_TRACKING_URI=http://localhost:5000

# ---- Vendor API keys (fill these in) ----
POLYGON_API_KEY=
POLYGON_MAX_CALLS_PER_WINDOW=5
POLYGON_RATE_WINDOW_SECONDS=60
# Optional process-lifetime cap; 0 disables.
POLYGON_TOTAL_CALL_BUDGET=0
FMP_API_KEY=
FINNHUB_API_KEY=
NASDAQ_DATA_LINK_API_KEY=
# Optional / expansion sources:
# ALPACA_API_KEY=
# ALPACA_API_SECRET=
# DATABENTO_API_KEY=

# ---- App ----
APP_ENV=local
LOG_LEVEL=INFO
JWT_SECRET=change_me_random_64_chars
# Comma-separated API keys accepted by the API. Empty = allow anonymous (dev only).
API_KEYS=

# ---- Observability ----
SENTRY_DSN=
```

### 5.3 `.gitignore`

Create **`A:\tansel\.gitignore`**:

```gitignore
.venv/
__pycache__/
*.pyc
.env
.mlflow/
mlruns/
data/
.pytest_cache/
.ruff_cache/
.mypy_cache/
```

### 5.4 `docker-compose.yml`

Create **`A:\tansel\docker-compose.yml`**:

```yaml
name: stock-api

services:
  timescaledb:
    image: timescale/timescaledb:2.28.2-pg17
    container_name: stockapi-timescaledb
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_APP_PASSWORD: ${POSTGRES_APP_PASSWORD}
      POSTGRES_SNAPSHOT_BUILDER_PASSWORD: ${POSTGRES_SNAPSHOT_BUILDER_PASSWORD}
    ports:
      - "127.0.0.1:5432:5432"
    volumes:
      - ./data/pgdata:/var/lib/postgresql/data
      - ./scripts/db-init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis-cache:
    image: redis:7-alpine
    container_name: stockapi-redis-cache
    command:
      [
        "redis-server",
        "--save",
        "",
        "--appendonly",
        "no",
        "--maxmemory",
        "256mb",
        "--maxmemory-policy",
        "noeviction",
      ]
    ports:
      - "127.0.0.1:6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis-celery:
    image: redis:7-alpine
    container_name: stockapi-redis-celery
    command:
      [
        "redis-server",
        "--appendonly",
        "yes",
        "--maxmemory-policy",
        "noeviction",
      ]
    ports:
      - "127.0.0.1:6380:6379"
    volumes:
      - ./data/redis-celery:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  mlflow:
    image: python:3.12-slim
    container_name: stockapi-mlflow
    working_dir: /mlflow
    # Local-dev deviation: sqlite backend + local artifact dir. Production keeps
    # filesystem artifacts with offsite backups until a managed object store is needed.
    command: >
      bash -c "pip install --no-cache-dir mlflow-skinny>=2.19 &&
               mlflow server --host 0.0.0.0 --port 5000
               --backend-store-uri sqlite:////mlflow/mlflow.db
               --artifacts-destination /mlflow/artifacts"
    ports:
      - "5000:5000"
    volumes:
      - ./data/mlflow:/mlflow

  # The API lives under profile `app`; persistent actors (ordinary worker,
  # snapshot-builder, Beat) live under `automation`. Both default Compose and
  # `--profile app` therefore start no unattended work. Automation additionally
  # requires AUTOMATION_ENABLED=true and a positive finite Polygon budget.
  # See docker-compose.yml for the exact service boundaries.
```

### 5.5 TimescaleDB init script

Create **`A:\tansel\scripts\db-init\01-extensions.sql`** (auto-run on first DB boot):

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- Example hypertable (uncomment once the bars table exists):
-- CREATE TABLE IF NOT EXISTS bars (
--   symbol text NOT NULL, ts timestamptz NOT NULL,
--   open double precision, high double precision, low double precision,
--   close double precision, volume double precision,
--   PRIMARY KEY (symbol, ts)
-- );
-- SELECT create_hypertable('bars', 'ts', if_not_exists => TRUE);
```

---

## 6. Step 5 — Create the virtual env & install Python deps

From **`A:\tansel`**:

```powershell
# Create the .env from the template, then edit it with your real keys
Copy-Item .env.example .env

# Resolve + lock dependencies (writes uv.lock), then install core + dev into .venv
uv lock
uv sync --frozen --extra dev

# Add the ML stack (torch, LightGBM, StatsForecast, Chronos-2) — takes longer
uv sync --frozen --extra dev --extra ml

# Activate the uv-managed venv (PowerShell)
.\.venv\Scripts\Activate.ps1
```

**Verify the environment:**
```powershell
python --version                       # Python 3.12.x
python -c "import fastapi, pandas, sqlalchemy, redis, celery, mlflow; print('core OK')"
python -c "import torch, lightgbm, statsforecast; import chronos; print('ml OK')"
```

> 🔧 If `Activate.ps1` is blocked: `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`, then retry.

---

## 7. Step 6 — Bring up the infrastructure

Make sure **Docker Desktop is running** (whale icon → “Engine running”), then from **`A:\tansel`**:

```powershell
docker compose up -d
docker compose ps           # all services should be "running"/"healthy"
```

First run pulls images (TimescaleDB, Redis) and the MLflow container pip-installs MLflow on boot (~1–2 min the first time).

Fresh databases create the fixed, non-owner `stockapi_app` and
`stockapi_snapshot_builder` roles through `scripts/db-init/02-runtime-role.sh`.
Existing initialized database directories do not rerun Docker init scripts;
bootstrap both roles once before applying migrations `0006` through the current
head, `0014_vendor_campaign_anchor`:

```powershell
docker compose exec timescaledb sh /docker-entrypoint-initdb.d/02-runtime-role.sh
alembic upgrade head
```

The migration fails with a clear error if this one-time bootstrap was missed.
No Compose service receives the owner URL. The host-run Alembic command selects
`MIGRATION_DATABASE_URL`; Compose API and ingestion services use `stockapi_app`,
the queue-isolated snapshot worker alone receives the builder credential, and
Beat receives neither database credential.

The native `.env` workflow is a convenience boundary, not hard secret
isolation: every local Python process can read that file. Use Compose or inject
per-process environments when validating credential separation.

Print the content-derived policy identities. The command emits separate raw and
adjusted pairs; copy only a complete pair into `.env` when intentionally enabling
that target epoch, then start the serving-only API tier:

```powershell
uv run python -m ingestion.tasks.build_forecast_snapshots --print-policy-hashes
docker compose --profile app up -d --build
```

That command starts no worker, snapshot builder, or Beat. Do not add the
`automation` profile merely as a convenience. Before deliberately enabling it,
inspect or purge the durable Celery queue, scope the symbol/window work, set
`AUTOMATION_ENABLED=true`, and set a positive `POLYGON_TOTAL_CALL_BUDGET`.
That cap is per Polygon lane and worker process, resets on restart, and is not a
durable vendor-spend ledger. Stop `worker`, `snapshot-builder`, and `beat`
immediately to disable a running tier; changing `.env` does not reconfigure an
existing container, so recreate it to apply a new flag or budget.

For an existing volume, retain the `POSTGRES_USER`, `POSTGRES_DB`, and owner
password that originally initialized that database (older local volumes often
use `stockapi`). Changing Docker environment variables does not rename or reset
an existing PostgreSQL owner. If the old local data is disposable, recreating
the database directory is the alternative; do not delete it merely to upgrade.

**Watch logs if needed:**
```powershell
docker compose logs -f timescaledb
docker compose logs -f mlflow
```

---

## 8. Step 7 — Verify every component (smoke tests)

| Component | Command | Expected |
|---|---|---|
| Docker engine | `docker version` | client + server versions |
| TimescaleDB up | `docker compose exec timescaledb sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'` | `accepting connections` |
| Timescale ext (owner check) | `docker compose exec timescaledb sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT extversion FROM pg_extension WHERE extname=''timescaledb'';"'` | a version row |
| Redis cache | `docker exec stockapi-redis-cache redis-cli ping` | `PONG` |
| Redis Celery | `docker exec stockapi-redis-celery redis-cli ping` | `PONG` |
| MLflow UI | open `http://localhost:5000` | MLflow dashboard |
| Ordinary Celery worker | `celery -A ingestion.celery_app.celery_app inspect ping` (after starting it) | `pong` from one node |
| Snapshot-builder worker | `celery -A ingestion.snapshot_celery_app.snapshot_celery_app inspect ping` (after starting it with its isolated environment) | `pong` from one node |
| DB from Python | see snippet below | `db OK` |

```powershell
python -c "import os,sqlalchemy as sa; from dotenv import load_dotenv; load_dotenv(); url=os.environ['DATABASE_URL'].replace('+asyncpg','+psycopg'); e=sa.create_engine(url); c=e.connect(); print('db OK', c.execute(sa.text('select 1')).scalar())"
```

The empirical database gate is intentionally destructive and must only target
a specifically designated throwaway TimescaleDB. It drops/recreates the project
tables before proving migrations, role ACLs, bar revisions, snapshot creation,
read-only serving, exact-receipt realized-outcome resolution/persistence, and
pre-outcome cohort sealing:

```powershell
.\run-live-gate.ps1
```

The runner refuses any database/user except `stockapi_test` owned by
`stockapi_owner`, requires distinct owner/runtime/snapshot-builder passwords in
`.env`, starts TimescaleDB, waits at most five minutes for health, supplies the
destructive-test sentinel only for the test process, and removes all test URLs
afterward. Both wrapper and test module independently enforce the exact local
owner target; the wrapper also refuses to reset while API/Celery/uvicorn
processes could race it. All mutating operator wrappers share one machine-wide
mutex, and the fixture holds the same PostgreSQL vendor-operation advisory lock
used by direct smoke/acquisition/backfill/demo lanes across reset and teardown. The module
fixture then drops its seeded test data and reapplies
migrations, leaving an empty schema at migration head `0014_vendor_campaign_anchor` so the later vendor smoke
still proves absence. It never makes a vendor call.

CI proves the same database boundary on every push and pull request in the
dedicated `live-postgres` job. `scripts/ci-live-database-gate.sh` refuses any
non-GitHub-hosted runner, checked-out `.env`, vendor credential, enabled
automation, or reusable Postgres data directory. It creates three distinct
masked passwords, starts the digest-pinned TimescaleDB image on loopback with a
fresh anonymous volume, runs the repository's extension and role init scripts,
replays the transactional role bootstrap once, and executes only
`tests/integration/test_bars_live_gate.py`. Cleanup removes the container and
anonymous volume on success, failure, or cancellation. The job references no
GitHub secret and does not authorize or perform vendor I/O. Ordinary local
`pytest` remains skip-capable when the four explicit `TEST_*` values are absent;
use `run-live-gate.ps1` for the corresponding owner-controlled Windows proof.

✅ When all rows pass, the database migration, privilege, revision, immutable
snapshot, runtime-role serving, API-key short circuit, authenticated HTTP
forecast, realized-outcome hash/exact-receipt, resolver cutoff, exact race
replay, post-cutoff restatement, and cohort post-commit sealing boundaries are
proven. The gate also proves immutable corporate-action collections and
receipts, Decimal34 adjustment-factor publication, exact raw-version binding,
and factor-table role/immutability boundaries. Polygon credentials and
Celery/Beat remain outside this destructive
gate. A labelled synthetic throwaway target exercises the successful
publisher/store/source-link path and forged-snapshot refusal, but is not a real
market outcome. The gate does not pretend that a newly sealed real XNYS cohort
has already matured; that final composition requires a forecast made before the
target and elapsed market time. These checks do not enable an unattended
collector, cohort publisher, or calibrator.

### Separately authorized first vendor request

The first Massive/Polygon request is a distinct owner gate. After placing the
key in ignored `.env`, authorize an exact session and run only the bounded
smoke below (replace the date with the latest completed XNYS session named in
that authorization):

```powershell
.\run-vendor-smoke.ps1 `
  -Session YYYY-MM-DD `
  -Authorization stockapi-vendor-smoke-only
```

`YYYY-MM-DD` is deliberately non-executable documentation: replace it only
with the exact session date named in the current owner authorization. Examples
never carry a date forward automatically.

The harness is hard-bound to `MSFT`, `stockapi_app`, and the local
`stockapi_test` database. It refuses a pre-existing target row and forces one
total HTTP attempt with retries disabled. Keep the ordinary ingestion worker and
Beat stopped; the wrapper checks the local process/container state before
starting, including Celery launched through versioned Python executables. A
machine-wide Windows mutex serializes wrapper invocations, and the Python module
holds a non-blocking PostgreSQL advisory lock from the absence precheck through
the post-commit receipt proof, so direct module invocations also fail closed on
contention. Success requires both the exact bar and its DB-stamped post-commit
availability receipt. It does not build a snapshot or serve a forecast; those
remain behind the separately budgeted typed acquisition and local build gates
below.

### Separately authorized typed corporate-action + price acquisition

The one-request smoke authorizes only that one bar. The adjusted-data history
lane has a separate read-only planner and a typed campaign budget covering both
corporate actions and missing open-close sessions. Planning needs no vendor key
and cannot make a vendor request:

```powershell
.\run-vendor-acquisition.ps1 -Mode plan -End YYYY-MM-DD
```

`-End` must be the latest completed XNYS session and must be named again in any
later authorization. Every wrapper mode requires the clean local `main` branch.
The plan binds that commit, exact 258-session
MSFT price window, both exact corporate-action query scopes and query-policy
hash, current price/action receipts, both attempt ledgers, ordered typed calls,
and the hard 5-calls-per-60-seconds pace into `plan_id` and `calls_sha256`. It
does not require the commit to be pushed and never performs a push.

Interpret the result before asking for authorization:

- `blocked`: the latest-session smoke receipt is absent, an action scope has
  ambiguous repair candidates, or a prior unresolved reservation overlaps the
  required call set. Do not execute.
- `ready`: the plan identifies an exact nonempty call set or receipt-only
  repairs.
- `complete`: all 258 price receipts and both complete action-collection
  receipts exist. Do not execute.

Immediately after the destructive live gate, the cleaned throwaway database has
no smoke anchor, so an acquisition plan is correctly `blocked`. Once the
separately authorized one-bar smoke succeeds, and assuming no other data exists,
the expected acquisition allocation is exactly **259 outbound attempts**:

| call kind | exact ceiling | purpose |
|---|---:|---|
| `split_page` | 1 | one complete, bounded MSFT split collection |
| `dividend_page` | 1 | one complete, bounded MSFT dividend collection |
| `open_close` | 257 | the remaining sessions in the 258-session window |
| **initial campaign** | **259** | exact base; zero implicit retry headroom |

At 5 calls per 60 seconds this is roughly 52 minutes plus network/database time.
The owner grant must name MSFT, `window_start`, `window_end`, `tool_revision`,
`plan_id`, `campaign_id`, the campaign-journal digest/count, the global
Postgres-anchored journal digest/count, and cumulative counters,
`calls_sha256`, the exact three-part allocation, the required campaign budget
delta, the current-run ceiling, the 5/60 pace, and a fresh lowercase
`authorization_id`. The first full plan's delta and current-run ceiling are both
259. The fixed sentinel below is only a mechanical check; it cannot substitute
for that grant.

With those exact freshly reviewed values, execution is:

```powershell
.\run-vendor-acquisition.ps1 `
  -Mode execute `
  -End YYYY-MM-DD `
  -PlanId sha256:<64-hex-plan-id> `
  -CampaignId sha256:<64-hex-campaign-id> `
  -CampaignBudgetDelta 259 `
  -MaxCalls 259 `
  -SplitCalls 1 `
  -DividendCalls 1 `
  -OpenCloseCalls 257 `
  -Authorization stockapi-msft-acquisition-only `
  -AuthorizationId msft-acquisition-YYYYMMDD-a
```

The wrapper scrubs ambient vendor credentials so the key can come only from
ignored `.env`, disables ambient HTTP proxy inheritance, pins and health-checks
the local Docker Desktop database at migration head `0014_vendor_campaign_anchor`, and refuses the API,
worker, Beat, snapshot-builder, native Celery/uvicorn, or another mutating
operator. Execute revalidates
the exact one-line plan and runs the reviewed code from a detached Git worktree;
the canonical ignored ledger remains in the primary workspace.
The Python lane holds the vendor-wide PostgreSQL lock, independently re-plans,
requires the exact campaign delta and typed ceilings to equal the current plan,
and reserves each call in `data/vendor_acquisition_attempts.jsonl` before HTTP.
Every authorization, reservation, and outcome is fsynced locally and then
advanced through an immutable Postgres high-water function before execution can
continue. Any file/DB count or digest mismatch, including a valid suffix
rollback from an older campaign, is a forensic stop.
Split and dividend pages run first; each page must be complete and rejects any
`next_url` rather than issuing a follow-up request.
Missing open-close sessions follow in ascending order. HTTP retries are disabled,
session currency is rechecked after pacing, and exact content plus its later
database receipt is verified before the next call.

Preserve both ignored ledgers: the combined lane also imports unresolved state
from the older `data/vendor_backfill_attempts.jsonl`. Recovery is fail-closed:

- If only receipts are absent for already committed content, re-plan and run:

  ```powershell
  .\run-vendor-acquisition.ps1 `
    -Mode repair `
    -End YYYY-MM-DD `
    -PlanId sha256:<current-plan-id>
  ```

  This performs database receipt writes but makes zero vendor calls.
- A caught failed attempt remains a cumulative campaign debit. Re-plan; if the
  new plan reports a positive `campaign_required_budget_delta`, obtain a new
  grant naming that exact delta and a fresh authorization ID. No recovery call
  is implicit, and the entire campaign is hard-limited to five explicitly
  granted recovery calls beyond its base.
- An unresolved reservation overlapping a required call is an unknown
  request/checkpoint outcome. Stop for independent vendor/database forensics;
  there is no automatic or destructive clear switch.

Success requires zero remaining calls and repairs, exactly 258 price receipts,
and one complete receipted collection for each action type. Acquisition does not
itself publish an adjustment-factor set, seal a forecast snapshot, or authorize
an outcome/cohort write.

### Lower-level price-only backfill (not the adjusted-data runbook)

The older price-only lane remains useful for focused recovery and its coverage
planner is reused by the typed acquisition implementation. Do **not** use it as
the initial adjusted-data milestone: it cannot acquire or authorize corporate
actions. The combined lane above is the owner runbook.

The history pull is not covered by the one-request smoke authorization. Run it
only from a reviewed, clean commit with the ordinary worker and Beat stopped.
First produce a read-only plan; this mode does not require `POLYGON_API_KEY` and
cannot make a vendor call:

```powershell
.\run-vendor-backfill.ps1 -Mode plan -End YYYY-MM-DD
```

`-End` must still be the latest completed XNYS session. The plan is hard-bound
to MSFT, raw `polygon_open_close` bars, the local `stockapi_test` runtime role,
exactly 258 sessions, and 5 calls per 60 seconds. It also binds the clean Git
`tool_revision`, every current bar/receipt version, the missing-date digest, and
the durable attempt ledger state into `plan_id`. Any session rollover, database
restatement, receipt change, ledger ambiguity, code change, or dirty worktree
invalidates the plan instead of drifting.

Interpret `status` before asking for authorization:

- `blocked`: the latest-session smoke receipt is absent, or an unresolved prior
  attempt overlaps a still-missing date. Do not execute.
- `ready`: one or more exact receipt repairs or vendor calls remain.
- `complete`: all 258 exact receipts exist; do not execute.

After a successful one-bar latest-session smoke, the first ordinary plan should
show 257 missing sessions and 257 required outbound attempts. At the hard 5/60
pace that takes roughly 52 minutes plus network/database time. The owner grant
must explicitly name MSFT, `window_end`, `tool_revision`, `plan_id`,
`missing_sessions_sha256`, the exact `required_outbound_attempts`, the 5/60
pace, and a new lowercase `authorization_id`. The fixed sentinel passed to the
program is a mechanical check; it does not substitute for that owner grant.

With those exact reviewed values, run:

```powershell
.\run-vendor-backfill.ps1 `
  -Mode execute `
  -End YYYY-MM-DD `
  -PlanId sha256:<64-hex-plan-id> `
  -MaxCalls 257 `
  -Authorization stockapi-msft-backfill-only `
  -AuthorizationId msft-YYYYMMDD-a
```

The wrapper never accepts the API key on argv. The Python lane disables HTTP
retries, rechecks session currency at each post-pacing admission, reserves the
date durably before sending, then commits and re-reads that date's exact
post-commit receipt before continuing. A vendor-wide PostgreSQL lock excludes
the smoke and both ordinary Polygon ingestion lanes; the machine-wide wrapper
mutex and worker/Beat process checks are additional defenses.

Audit history lives at `data/vendor_backfill_attempts.jsonl` (ignored by Git).
Preserve it; never delete or edit it to clear a refusal, and never reuse an
authorization ID. Recovery is fail-closed:

- A caught failed attempt has a terminal ledger outcome. Re-run `plan`, obtain
  a fresh owner grant for only the remaining digest/count, and use a new ID.
- A committed bar lacking only its receipt is repaired with zero vendor calls:

  ```powershell
  .\run-vendor-backfill.ps1 `
    -Mode repair `
    -End YYYY-MM-DD `
    -PlanId sha256:<current-plan-id>
  ```

  `repair` still mutates the throwaway database, so its wrapper also requires
  the worker and Beat to be stopped.
- An unresolved reservation for a still-missing date means the process died in
  the unknown request/checkpoint window. Stop for independent vendor/DB
  forensics; the harness intentionally has no automatic or destructive
  "clear" switch.

Success reports 258 required sessions, zero remaining sessions, and equal
`attempts_reserved`/`attempts_spent`. Re-run `plan` to independently obtain
`status: complete`; only then proceed to an explicitly reviewed factor or raw
snapshot build.

### Factor publication and adjusted-serving boundary

A complete typed acquisition supplies inputs; it does not silently manufacture
derived evidence. The implemented factor path is:

1. `AdjustmentFactorBuilder` resolves one exact cutoff, 258-session raw series,
   and exact complete split/dividend collections with their receipts.
2. The pure pinned policy computes canonical Decimal34 price/volume factors and
   publishes the exact IEEE-754 bits consumers must use.
3. `SqlAdjustmentFactorSetStore` publishes immutable content and entries, then a
   distinct later `adjustment_factor_set_availability` receipt.
4. `GET /v1/prices/{symbol}/adjusted` accepts only the resulting explicit
   `factor_set_id`; it validates every exact raw version before filtering or
   paginating and fails closed instead of selecting “latest” or returning raw.

The implemented low-level primitive is
`ingestion.tasks.seal_adjusted_forecast_snapshot`. Its exact interface contract
is shown here for review, **not as a direct host runbook**:

```text
python -m ingestion.tasks.seal_adjusted_forecast_snapshot \
  --end YYYY-MM-DD \
  --factor-cutoff <aware-ISO-8601-plan-cutoff> \
  --expected-factor-set-id sha256:<64-hex-plan-identity> \
  --tool-revision <40-hex-reviewed-commit> \
  --authorization stockapi-msft-adjusted-seal-only
```

The primitive makes no vendor call and is not a Celery task. It refuses any
database except `stockapi_snapshot_builder@timescaledb:5432/stockapi_test`, any
non-local environment, or an image whose baked `/app/.stockapi-build-revision`
does not exactly equal `--tool-revision`. It requires both adjusted hashes to
match the running code, an exact 258-session MSFT/current-XNYS window, and the
operator-plan-bound factor cutoff. Fresh database-clock checks before and after
factor publication reject future cutoffs, session rollover, and insufficient
rollover margin. The factor's later receipt becomes the deterministic adjusted
snapshot `as_of`; exact factor and snapshot replays are allowed, and persisted
snapshot lineage is reread and verified before success. Its sanitized JSON
names the factor/snapshot IDs, cutoff and receipt times, counts, and policy
hashes.

The complete adjusted host lane is the `adjusted_close` target of
`run-forecast-demo.ps1`. Its read-only plan derives and binds the factor cutoff
and expected factor-set identity; execute builds the detached reviewed revision,
pins the immutable image, excludes competing actors, revalidates the plan, and
surfaces the adjusted forecast over authenticated HTTP. Use
`-Target adjusted_close` with the distinct
`stockapi-msft-adjusted-seal-serve-only` authorization. The low-level sentinel
above is only a refusal check, and neither it nor vendor-acquisition authority
authorizes the factor/snapshot writes.

Once that reviewed controller has published an exact factor ID, the
adjusted-price read shape is:

```text
GET /v1/prices/MSFT/adjusted?factor_set_id=sha256:<64-hex-id>&limit=100
```

The response includes the factor/policy identities, exact split and dividend
collection receipts, action version IDs, full raw coverage, raw-version receipt
identity per returned row, receipt-derived availability, and a keyset `next_end`.
The existing raw `/v1/prices/{symbol}` response remains unchanged.

Adjusted-close target-routed forecast serving uses the snapshot above and its
own pair of hashes printed by `--print-policy-hashes`. The outcome resolver,
scheduled-evaluation cohorts, and calibration evidence remain raw-close-only,
so an adjusted forecast must not be represented as a matured/scored cohort or
calibration result.

### Separately authorized local seal-and-serve proof

This step makes **no vendor request**. It does make one idempotent insert into
`forecast_input_snapshots` (or proves an exact replay) and starts the local API,
so review a fresh read-only plan before authorizing it. Prerequisites are:

- the typed acquisition reports complete and the shared price planner confirms
  all 258 exact MSFT receipts;
- `.env` pins both hashes printed by the policy command;
- `.env` contains exactly one non-empty `API_KEYS` value; and
- `.env` contains a non-default ASCII `JWT_SECRET` of at least 32 characters
  (used only as the HMAC key for a nonpublic API-key plan binding); and
- the worktree is clean at the reviewed commit.

Plan without starting the API or writing a snapshot:

```powershell
.\run-forecast-demo.ps1 -Mode plan -End YYYY-MM-DD
```

`status: ready` binds the exact acquired price/version state, stable maximum receipt
cutoff, database-clock session, clean `tool_revision`, policy identities, local
runtime/broker targets, secret-safe API-key identity, bounded session-rollover
margin, and fixed five-step `baseline_naive` request into `plan_id`. A newer
completed XNYS session, data restatement, receipt change, credential/policy
change, code change, or dirty worktree invalidates it. The HMAC binding is never
printed; only the outer content-addressed `plan_id` is public.

After the owner explicitly authorizes that exact plan, execute:

```powershell
.\run-forecast-demo.ps1 `
  -Mode execute `
  -End YYYY-MM-DD `
  -PlanId sha256:<64-hex-plan-id> `
  -Authorization stockapi-msft-seal-serve-only
```

The wrapper first rechecks the plan before changing service state, then builds
one shared API/builder image from an exact detached worktree of the reviewed
Git commit (never the mutable checkout), explicitly starts the two local Redis
dependencies, publishes the API at `127.0.0.1:8000`, and refuses ordinary
worker, Beat, persistent snapshot worker, or native Celery/uvicorn contention.
It pins the repository Compose file/project and the local Docker Desktop Linux
daemon; ambient Compose/Docker overrides are refused. The image carries the
reviewed revision as an OCI label and baked file. The wrapper hands immutable
image and freshly recreated API-container IDs to the controller; API startup
uses `--no-build --pull never`, and the builder is overridden to the exact
image ID and run with `--pull never --rm --no-deps`. Container project/service,
running state, and zero mounts are checked before and after the write. Snapshot
creation is one short-lived process, not a queue consumer, so stale Redis
messages cannot widen the write set. A deterministic plan-labelled container
name permits immutable-ID cleanup after interruption. The host controller holds
the shared PostgreSQL vendor lock, recomputes the exact plan, proves
unauthenticated and wrong-key `401` plus authenticated missing-snapshot `404`, validates
the sealed bytes/header/availability evidence through `stockapi_app`, then
requires an authenticated `200` parsed as `ForecastResponse` with five ordered
XNYS steps exactly matching the sealed schedule, deterministic naïve
points/quantiles, the requested 0.8 intervals, exact Polygon source-manifest
lineage, passed lookahead evidence, and honest uncalibrated metadata. HTTPX
ignores ambient proxies, vendor variables are removed from the controller and
one-shot environments (then restored in the caller), and no database password
or API key appears on argv or in proof JSON. This local attestation assumes the
Git object store, OS user, and Docker Desktop daemon are trusted; production
provenance additionally requires digest-pinned bases and signed artifacts.

Success is the demo milestone: one immutable point-in-time snapshot and one
real authenticated forecast response. The local API remains available on
loopback for inspection; vendor acquisition/backfill authorization is neither
implied nor consumed by this step. Planning/execution refuse inside a ten-minute guard band
before the next XNYS close. If the session nevertheless advances after the
snapshot commits, the command exits `3` with an explicit nonsecret
`sealed_session_advanced` receipt (including the snapshot ID) instead of hiding
the committed write behind a generic failure. Any runtime-row, HTTP, response,
container-revalidation, final-clock, or lock-release failure after a validated
seal likewise exits `3` with `sealed_proof_failed`, the snapshot ID/status,
immutable image IDs, fixed failure phase, and exception type/HTTP status—but
never exception text or response bodies.

---

## 9. Step 8 — Run the API and workers

Apply all migrations, then run the API, ordinary Celery worker, least-privilege
snapshot worker, and Beat scheduler in separate terminals:

```powershell
.\.venv\Scripts\Activate.ps1
alembic upgrade head                 # apply the complete migration chain
uvicorn app.main:app --reload --port 8000
# Swagger docs: http://localhost:8000/docs
# Liveness:     http://localhost:8000/healthz
# Readiness:    http://localhost:8000/readyz    (checks DB + Redis)

# In separate terminals only under an approved automation runbook. These task
# entrypoints refuse while AUTOMATION_ENABLED is false; Polygon entrypoints also
# refuse unless POLYGON_TOTAL_CALL_BUDGET is positive.
celery -A ingestion.celery_app.celery_app worker --loglevel=INFO --concurrency=1
celery -A ingestion.celery_app.celery_app beat   --loglevel=INFO

# Snapshot creation must be a separate process with DATABASE_URL temporarily
# set to the stockapi_snapshot_builder URL; never give this URL to the API or
# ordinary worker. Compose's `snapshot-builder` service wires this safely.
celery -A ingestion.snapshot_celery_app.snapshot_celery_app worker --loglevel=INFO --concurrency=1 --queues=snapshot-builder
```

---

## 10. Daily start / stop

```powershell
# Start a work session
docker compose up -d                 # infra
.\.venv\Scripts\Activate.ps1         # python env
uvicorn app.main:app --reload        # api
# Persistent workers and Beat remain stopped during ordinary development.
# Separately authorized smoke/acquisition/backfill/demo wrappers use bounded one-shot paths.

# Stop
docker compose stop                  # stop containers, keep data
deactivate                           # leave the venv
```

---

## 11. Troubleshooting

| Symptom | Fix |
|---|---|
| `winget` install needs admin / silently fails | Run PowerShell **as Administrator**. |
| Docker Desktop won’t start / “WSL 2 not installed” | Re-run `wsl --install`, **reboot**, ensure Virtualization is **Enabled in BIOS/UEFI** (Intel VT-x / AMD-V). |
| Virtualization disabled | Reboot → BIOS/UEFI → enable **Intel VT-x / SVM Mode** → save & exit. |
| `docker` not recognized after install | Open a **new** terminal (PATH refresh) or sign out/in. |
| Port already in use (5432/6379/5000/8000) | Find owner: `Get-NetTCPConnection -LocalPort 5432 \| Select OwningProcess` then `Get-Process -Id <pid>`; stop it or change the host port in `docker-compose.yml`. |
| `Activate.ps1` blocked | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`. |
| TimescaleDB extension missing | Confirm `scripts/db-init/01-extensions.sql` mounted; it only runs on a **fresh** volume — `docker compose down -v` then `up` to re-init (destroys local data). |
| MLflow container slow first boot | Expected — it pip-installs MLflow on start. Subsequent boots reuse the layer cache. |
| `torch` install huge / slow | Default is CPU build. For GPU see Appendix A. |
| `TA-Lib` build error | TA-Lib is **optional**. The default path is owned indicator functions with golden-value tests. |

---

## 12. Uninstall / rollback

```powershell
# Stop and remove containers + volumes (DESTROYS local DB/Redis/MLflow data)
docker compose down -v

# Remove the Python env
Remove-Item -Recurse -Force .\.venv

# Remove installed apps (optional)
winget uninstall -e --id Docker.DockerDesktop
winget uninstall -e --id astral-sh.uv
```

---

## Appendix A — GPU (CUDA) PyTorch (optional)

This host has ample RAM; if you also have an NVIDIA GPU and want CUDA acceleration:

```powershell
uv pip uninstall torch
uv pip install torch --index-url https://download.pytorch.org/whl/cu124
python -c "import torch; print('CUDA:', torch.cuda.is_available())"
```

## Appendix B — TA-Lib on Windows (optional)

The default path is owned indicator functions over pandas/numpy. To add the faster C-backed **TA-Lib** for specific formulas:

```powershell
# Prebuilt wheel (preferred on Windows — no C toolchain needed)
uv pip install TA-Lib
# If no wheel resolves for 3.12, grab a prebuilt wheel matching cp312/win_amd64
# from a trusted wheel mirror and: uv pip install path\to\TA_Lib-...-cp312-win_amd64.whl
```

## Appendix C — VS Code (optional editor)

```powershell
winget install -e --id Microsoft.VisualStudioCode --accept-source-agreements --accept-package-agreements
# Recommended extensions: ms-python.python, ms-python.vscode-pylance, ms-azuretools.vscode-docker, charliermarsh.ruff
```

## Appendix D — Version-pinning policy

Image tags (`latest-pg17`) and `>=` ranges favor a smooth first install. **Before production**, pin every image to an exact digest/tag and commit a `uv.lock` (`uv lock`) for reproducible builds. Verify current versions at each vendor’s site — they change frequently.

---

## Install checklist

- [ ] WSL2 enabled + rebooted (`wsl --status` → version 2)
- [ ] Docker Desktop installed, engine running (`docker run --rm hello-world`)
- [ ] uv installed (`uv --version`)
- [ ] Python 3.12 available to uv (`uv python list`)
- [ ] Project files created (`pyproject.toml`, `.env`, `docker-compose.yml`, init SQL)
- [ ] `uv sync --frozen --extra dev` (+ `--extra ml`) installed, import checks pass
- [ ] `docker compose up -d` → all services healthy
- [ ] Smoke-test matrix all green (DB, Redis, MLflow)
- [ ] `alembic upgrade head` → `uvicorn` serves `/docs`, `/healthz` returns 200
- [ ] `--profile app` starts the API without worker, snapshot-builder, or Beat
- [ ] If unattended automation is explicitly approved: queue inspected, finite
      budget scoped, default-off gate enabled, and separate profile rehearsed
