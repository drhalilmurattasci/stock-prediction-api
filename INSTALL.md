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

# Blank keeps snapshot creation and /v1/forecast fail-closed. Print the exact
# current values with the command documented below before enabling.
FORECAST_RESOLUTION_POLICY_HASH=
FORECAST_TRUSTED_AVAILABILITY_RULE_SET_HASH=
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
      - "5432:5432"
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
        "allkeys-lru",
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

  # The app tier (api / worker / snapshot-builder / beat) lives under the
  # `app` compose profile so
  # `docker compose up` stays infra-only. Start the full stack with:
  #   docker compose --profile app up -d --build   (requires a committed uv.lock)
  # See docker-compose.yml in the repo for all app-tier service definitions.
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
bootstrap both roles once before applying migrations `0007` and `0008`:

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

Print the content-derived policy identities, copy both values into `.env`, then
start the full app tier when you are ready to enable raw-close forecasts:

```powershell
uv run python -m ingestion.tasks.build_forecast_snapshots --print-policy-hashes
docker compose --profile app up -d --build
```

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
and read-only serving:

```powershell
$env:TEST_DATABASE_URL = 'postgresql+asyncpg://<owner>:<encoded-secret>@localhost:5432/<throwaway-db>'
$env:TEST_RUNTIME_DATABASE_URL = 'postgresql+asyncpg://stockapi_app:<encoded-secret>@localhost:5432/<throwaway-db>'
$env:TEST_SNAPSHOT_BUILDER_DATABASE_URL = 'postgresql+asyncpg://stockapi_snapshot_builder:<encoded-secret>@localhost:5432/<throwaway-db>'
$env:TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET = 'stockapi-test-only'
uv run pytest tests/integration -v
```

✅ When all rows pass, the database migration, privilege, revision, snapshot,
and read-only serving boundaries are proven. Polygon credentials, Celery/Beat,
and the authenticated HTTP route still require their separate smoke checks.

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

# In separate terminals (needed for scheduled Polygon price ingestion):
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
# (optional) celery -A ingestion.celery_app.celery_app worker --loglevel=INFO --concurrency=1
# (optional, isolated builder env) celery -A ingestion.snapshot_celery_app.snapshot_celery_app worker --loglevel=INFO --concurrency=1 --queues=snapshot-builder

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
- [ ] Ordinary worker, isolated snapshot-builder worker, and Beat are running
