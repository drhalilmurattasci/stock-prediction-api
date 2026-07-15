# Stock Market Price Forecast API

A REST API that ingests versioned daily market data and serves baseline price
forecasts with central **prediction intervals** and point-in-time provenance.

> ⚠️ **Not investment advice.** Markets are near-efficient; this project reports
> explicit uncertainty and never markets a forecast as an "accurate prediction"
> or as trading/investment advice. Current baseline intervals are deliberately
> labelled uncalibrated until held-out coverage validation is persisted.

## Status

🚧 **The fail-closed data/forecast evidence substrate is code-complete through
migration `0015_calibration_evidence`, but the product has not yet served a
forecast over real vendor data.**
The repository now has API-key auth, bounded `/v1/prices` and `/v1/indicators`
reads, versioned Polygon daily-bar ingestion, append-only restatement history, an
owned causal indicator library, leakage-aware baselines, an
immutable point-in-time snapshot builder, snapshot-backed `/v1/forecast`, and an
insert-only content-hashed forecast-run archive with retry-safe POST idempotency.
Migrations `0010`-`0011` establish the policy-explicit forecast-evidence
substrate: realized raw-close outcomes bind one exact post-commit bar receipt,
while immutable cohort manifests materialize members validated against exact
scheduled forecast outputs and receive a distinct post-commit seal before their
first target so later evaluation cannot silently cherry-pick membership. An
immutable policy registry and database-enforced receipt fence close the cutoff
race; runtime outcome writes are possible only through a bounded canonical
publisher that records the exact authorizing cohort member.

Migration `0015` adds the policy-neutral calibration persistence boundary: one
read-only repeatable snapshot reconstructs an exact cohort/run/outcome proof,
with a database-computed 128 MiB cumulative canonical-byte ceiling before heavy
rows are materialized. Publication reloads both source cohorts through that
trusted reader before any write; fitted conformal sets and held-out coverage
releases are content-addressed, append-only, and independently replayable from
those proofs. Held-out releases
are schema-labelled `descriptive-only` and contain no acceptance, promotion, or
serving field. A separate prospective evaluation and promotion policy is still
required before any calibration set can become serving-eligible.

Migrations `0012`-`0013` add the missing corporate-action and adjustment
boundary: append-only content-addressed split/dividend collections and exact
post-commit receipts, followed by immutable Decimal34 adjustment-factor sets,
one factor per exact receipted raw bar version, and a later factor-set receipt.
`GET /v1/prices/{symbol}/adjusted` requires an explicit immutable
`factor_set_id`; it reconstructs every bound current-or-revision raw version,
validates the complete factor window before applying a range or page, and
returns split/dividend-adjusted OHLCV with factor, policy, action-collection,
and raw-version lineage. It never resolves a mutable “latest” set or falls back
to raw values.

The initial builder policy is intentionally narrow: Massive/Polygon raw
regular-session closes from `/v1/open-close` (source `polygon_open_close`) for
`AAPL`, `MSFT`, `NVDA`, `QQQ`, and `SPY`; XNYS `trading_day` horizons; USD; 512
observation capacity with a 258-observation minimum; and 252 real
exchange-session target closes. A separate adjusted-close snapshot policy can
derive a sealed snapshot only from a receipted factor set and has independently
pin-gated forecast serving. Raw and adjusted targets cannot cross policy epochs.
The raw route stays `501` until its two code-derived identities are pinned; the
`adjusted_close` target stays fail-closed until its own two identities are
pinned and a compatible adjusted snapshot exists. The current outcome resolver,
scheduled-evaluation cohorts, and calibration evidence remain deliberately
raw-close-only.

**Acceptance status — 2026-07-14.** The local empirical state was empty: the
throwaway database was reset to zero market-data, corporate-action, factor,
snapshot, forecast, outcome, and cohort evidence rows; `POLYGON_API_KEY` was
absent; and no vendor request had ever run. There were therefore no real
adjusted rows, real forecasts, matured outcomes, or calibration claims despite
the code-complete paths above.

`/v1/indicators/{symbol}` is the second implemented Phase 2 read surface. Version
one is intentionally fixed to the raw daily `polygon_open_close` lane and the
newest 258 stored XNYS closes before an optional exclusive timestamp. It rejects
nonconsecutive session windows and nonpositive or otherwise invalid calculation
inputs, discloses structural warm-up nulls and window-relative recursive seeds,
and separates formula-policy, window-policy, and exact-input hashes. The endpoint
is a current-snapshot calculation, not an availability-as-of reconstruction;
later restatements can change a historical request, raw values can include
unresolved corporate-action discontinuities, and it does not assert that the
newest stored row is the latest completed session. At the dated acceptance
checkpoint above the database was empty; this API surface alone is not evidence
of a real-data forecast.

Unit/static gates cover the evidence substrate. On 2026-07-15 the separately
controlled local TimescaleDB gate passed all 31 tests through migration
`0015_calibration_evidence`, including the fitted/release publishers, later-
transaction receipt, replay, scope rejection, ACLs, append-only triggers, and
nonempty downgrade refusal. The last remote CI execution still proves `0014`;
remote validation of `0015` requires an explicit later push. The dedicated
`live-postgres` GitHub Actions job provisions a
fresh digest-pinned TimescaleDB for every push and pull request, initializes the
fixed runtime roles from the repository bootstrap, and runs only the destructive
Postgres module with generated ephemeral credentials. It supplies no vendor
secret, keeps automation disabled, and removes the container and anonymous
database volume even after failure.
`run-live-gate.ps1` is hard-bound to the designated `stockapi_test` throwaway
database and checks a fresh empty-schema upgrade to head plus the gate's empty
`0015` to `0007` to `0015` downgrade/upgrade cycle, exact
runtime/builder role boundaries, restatement history, historical point-in-time
snapshot reconstruction, archived serving, schema-validated keyed replay, and
the outcome/cohort hash, exact-receipt, immutability, role-boundary, and
pre-outcome sealing constraints. It also proves immutable corporate-action
collections, exact action receipts, Decimal34 factor publication, exact raw-bar
receipt binding, and factor-table immutability. It drives the owned outcome resolver
through a direct receipt-writer race across the cutoff, proves fresh
READ-COMMITTED visibility, and tests frozen-cutoff restatement behavior. A
clearly labelled synthetic throwaway target exercises the successful DB
publisher/store/source-link path and rejects forged snapshot provenance; it is
not evidence of a real market outcome.
Ordinary local test runs and the ordinary CI pytest job still skip that module
when its explicit live-database environment is absent; the dedicated CI job is
the per-build opt-in. The first real
Massive/Polygon call remains a separate credentialed smoke gate.
That gate has a one-attempt, fail-closed operator command documented in
`INSTALL.md`; no vendor request runs as part of ordinary verification. The next
history step is now a typed combined acquisition lane, not an unscoped
backfill. Its read-only plan covers the exact final 258 XNYS sessions plus one
complete split page and one complete dividend page over the same window. From
the empty state after a successful one-bar smoke, the initial campaign
authorization is exactly **259 outbound attempts: 1 split + 1 dividend + 257
open-close**. The executor sends actions first, disables HTTP retries,
checkpoints every exact receipt, enforces one shared 5/60 pacer, and durably
debits every reservation across fresh authorization IDs in an ignored
append-only campaign journal. Every journal record is also chained to an
immutable global Postgres high-water checkpoint; planning and execution require
the full-file count and digest to match, including across campaign/date changes.
The initial grant carries no retry headroom. A
later retry requires a new content-addressed plan and an explicit recovery
budget delta; recovery is hard-limited to five additional calls for the entire
campaign. Corporate-action responses are one page only and fail closed instead
of following `next_url`. A code revision, session rollover, database receipt,
campaign-ledger change, or unresolved prior attempt invalidates or blocks the
plan.
All controlled Polygon vendor-ingestion operator paths share one vendor-wide
PostgreSQL operation lock.

The acquisition plan itself is safe to inspect before any vendor credential or
grant:

```powershell
.\run-vendor-acquisition.ps1 -Mode plan -End YYYY-MM-DD
```

It accepts no authorization fields, cannot call the vendor, and binds the clean
local commit without requiring or performing a push. With no smoke anchor it
reports `blocked`; it never substitutes another session date.

After acquisition is complete, adjusted evidence has a separate low-level
builder primitive: `ingestion.tasks.seal_adjusted_forecast_snapshot`. Inside an
exact revision-attested `stockapi_snapshot_builder` image, it can publish or
exact-replay one MSFT factor set at an operator-plan-bound cutoff and seal or
replay one adjusted-close snapshot at the later factor-receipt time. It performs
no vendor I/O and does not enable Celery. Do not invoke the primitive ad hoc.
The adjusted lane of `run-forecast-demo.ps1` prepares that exact factor artifact
read-only, binds its content ID and stable evidence-derived cutoff, then uses the
same detached-image, actor-exclusion, and API attestation controls as the raw
lane. Its authenticated POST uses a plan-derived idempotency key and must replay
the same archived forecast on retry. Adjusted outcomes, cohorts, and calibration
remain separately excluded.

The raw-close, no-vendor lane is also scaffolded: a read-only plan binds the
completed price acquisition, clean commit, database clock, raw policy hashes,
and API auth configuration;
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

Before enabling snapshot creation/forecast serving, print the exact raw and
adjusted policy identities and pin only the target epochs intentionally being
enabled:

```bash
uv run python -m ingestion.tasks.build_forecast_snapshots --print-policy-hashes
```

The `app` Compose profile serves the API only; it never starts a worker, the
privileged snapshot builder, or Beat. Persistent actors live behind the separate
`automation` profile, the default-off `AUTOMATION_ENABLED` task gate, and (for
Polygon lanes) a positive finite process budget. See [INSTALL.md](INSTALL.md)
for role bootstrap and the live database gate. For the bounded raw or adjusted
milestone proof, run `run-forecast-demo.ps1` in plan mode with an exact end date
and either `-Target close` (the default) or `-Target adjusted_close`. Its
one-shot builder does not enable Celery automation. Compose publishes the API
on loopback only. The proof builds from
the exact reviewed Git commit and pins both API and one-shot builder execution
to revision-labelled immutable image IDs.

The evidence schema and strict canonical validators are a foundation, not a
claim of calibration. A default-off internal scheduled-evaluation seam can now
archive one explicitly versioned forecast and publish its selected cohort with
a distinct post-commit seal. A second internal seam rereads that sealed cohort
and the historical run, derives the requested member only from persisted bytes,
resolves one XNYS raw close at a deterministic policy-hashed lag, and persists
or exactly replays the immutable outcome. Resolution shares the ingestion
series lock, so an eligible post-commit receipt cannot still be in flight when
the cutoff is declared closed. Neither seam is wired to Celery, Beat, an API
route, or a default scientific policy.

The throwaway PostgreSQL gate proves the resolver against historical versions,
including a real receipt-writer race and frozen-cutoff restatement behavior. It
also proves the successful publication machinery with explicitly synthetic
throwaway evidence. That does not manufacture the final real-market
cohort-to-outcome lifecycle: a valid cohort must be sealed before a real XNYS
target, and resolution occurs only after that target plus its committed lag.
That empirical proof needs an actual precommitted forecast and elapsed market
time. No unattended collector,
scoreboard, or interval recalibrator is enabled; those actors still require
explicit policy artifacts and the same automation controls before use.

The repository now includes dependency-free, versioned offline kernels for
finite-sample split conformal, signed CQR, and projected ACI, with golden and
fail-closed tests. This is math infrastructure, not calibration evidence. It is
not connected to serving or persistence; there is no immutable fitted
calibration artifact, and ACI-to-quantile discretization remains deliberately
undefined. Forecast responses therefore continue to report
`calibration.method=none` until prospective fit and held-out cohorts mature and
the resulting artifact is durably bound to the served forecast.

See [INSTALL.md](INSTALL.md) for the full Windows/WSL2 setup. Persistent workers
and Beat are intentionally not safe-by-default startup conveniences: inspect or
purge the durable queue, set the explicit automation gates and finite vendor
budget, then use `make up-automation` only under an approved runbook.

## Documentation

| Doc | What's inside |
|---|---|
| [STOCK_API_MASTER_PLAN.md](STOCK_API_MASTER_PLAN.md) | Master plan — overview, doctrine, not-to-do list, tech stack, full feature catalog of 14 APIs/frameworks, phased roadmap |
| [INSTALL.md](INSTALL.md) | Start-to-finish installation guide (Windows) — WSL2, Docker, uv, Python 3.12, infra stack, smoke tests |
| [docs/architecture.md](docs/architecture.md) | Implemented trust boundaries for raw bars, corporate actions, factors, snapshots, serving, and evidence |
| [scripts/README.md](scripts/README.md) | Exact operator planning, authorization, budget, and recovery contracts |

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
